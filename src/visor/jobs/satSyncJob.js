const cron   = require('node-cron');
const config = require('../../config/env');
const CFDI = require('../models/CFDI');
const Comparison = require('../models/Comparison');
const Discrepancy = require('../models/Discrepancy');
const ComparisonSession = require('../models/ComparisonSession');
const { batchCompareCFDIs, formatSessionName } = require('../services/comparisonEngine');
const entityRepo = require('../repositories/entity.repository');
const SatJobCheckpoint = require('../models/SatJobCheckpoint');
const { compareCFDI } = require('../services/comparisonEngine');
const { compararArrays } = require('../services/comparisonEngine');
const { parseCFDI, normalizarCFDI } = require('../services/cfdiParser');
const { solicitar, verificar, descargarPaquete, descargarPaqueteMetadata } = require('../sat/download');
const { obtener, tieneCredenciales } = require('../sat/credenciales');
const { puedeIniciar, registrarInicio, registrarFin } = require('../sat/rateLimiter');
const { derivarPeriodoDesdeFecha, resolverPeriodo, resolverOCrearPeriodo } = require('../services/periodoFiscal.service');
const { logger } = require('../../shared/utils/logger');
const SatDescargaLog = require('../models/SatDescargaLog');
const { aplicarReclasificacion } = require('../services/reclasificacionGlobal.service');
const { verifyCFDIWithSAT }      = require('../services/satVerification');
const { fetchTodasLasFacturas } = require('../services/erp.service');
const { transformarTolerante } = require('../services/erp-transformer.service');
const { upsertFromERP } = require('../repositories/cfdi.repository');

const CRON_HORA = config.sat.cronHora;

// Protege el estatus de conciliación manual (`lastComparisonStatus:
// 'conciliado'`, ver comparison.controller.js `conciliarNotInErp`) durante la
// reingesta de CFDIs SAT — los `bulkWrite` de más abajo pisan
// `lastComparisonStatus` a 'not_in_erp'/'match' sin saber si ya fue conciliado
// a mano. Confirmado con el usuario 2026-08-14: SOLO debe protegerse este
// campo para los conciliados manualmente, el resto de la reingesta (subTotal,
// xmlContent, satStatus, etc.) debe seguir comportándose exactamente igual.
// Patrón: leer ANTES cuáles ya estaban conciliados, dejar correr el bulkWrite
// tal cual, y restaurar el campo después solo para esos UUIDs — así no hay
// que tocar los `$set` grandes de cada bulkWrite (menor riesgo de romper algún
// otro campo).
const _uuidsConciliadosPrevios = async (uuidsCandidatos) => {
  if (!uuidsCandidatos.length) return new Set();
  const previos = await CFDI.find(
    { uuid: { $in: uuidsCandidatos }, source: 'SAT', lastComparisonStatus: 'conciliado' },
    'uuid',
  ).lean();
  return new Set(previos.map(d => d.uuid.toUpperCase()));
};
const _restaurarConciliados = async (uuidsConciliadosPrevios) => {
  if (!uuidsConciliadosPrevios.size) return;
  await CFDI.updateMany(
    { uuid: { $in: [...uuidsConciliadosPrevios] }, source: 'SAT' },
    { $set: { lastComparisonStatus: 'conciliado' } },
  );
};

/**
 * Re-parsea el xmlContent de CFDIs que tienen XML guardado pero subTotal = 0.
 * Se llama en background después de cada sync para recuperar datos sobrescritos por metadata.
 */
const repararSubtotalesDesdeXml = async ({ source, ejercicio, periodo } = {}) => {
  const filter = { isActive: true, xmlHash: { $exists: true, $ne: null }, subTotal: 0 };
  if (source)    filter.source    = source.toUpperCase();
  if (ejercicio) filter.ejercicio = parseInt(ejercicio);
  if (periodo)   filter.periodo   = parseInt(periodo);

  const cfdis = await CFDI.find(filter).select('+xmlContent').lean();
  if (cfdis.length === 0) return;

  let reparados = 0;
  for (const cfdi of cfdis) {
    if (!cfdi.xmlContent) continue;
    try {
      const parsed = await parseCFDI(cfdi.xmlContent);
      if (parsed.subTotal === 0) continue;
      await CFDI.updateOne(
        { _id: cfdi._id },
        {
          $set: {
            subTotal:        parsed.subTotal,
            descuento:       parsed.descuento,
            total:           parsed.total,
            moneda:          parsed.moneda,
            tipoCambio:      parsed.tipoCambio,
            conceptos:       parsed.conceptos,
            impuestos:       parsed.impuestos,
            complementoPago: parsed.complementoPago,
            origenDescarga:  'xml',
          },
        },
      );
      reparados++;
    } catch (_) { /* ignorar errores individuales */ }
  }
  if (reparados > 0) logger.info(`[SatSyncJob] repararSubtotalesDesdeXml: ${reparados}/${cfdis.length} CFDIs reparados.`);
};

// ── Helper: derivar fechas de inicio/fin de un periodo ────────────────────────
const derivarFechasERP = (ejercicio, periodo) => {
  const mes       = String(periodo).padStart(2, '0');
  const ultimoDia = new Date(Date.UTC(ejercicio, periodo, 0)).getUTCDate();
  return {
    fechaInicio: `${ejercicio}-${mes}-01T06:00:00Z`,
    fechaFin:    `${ejercicio}-${mes}-${String(ultimoDia).padStart(2, '0')}T06:00:00Z`,
  };
};

/**
 * Job nocturno de Descarga ERP.
 * Descarga automáticamente las facturas del ERP para el mes indicado
 * (o el mes actual si no se especifica) y las persiste en MongoDB.
 *
 * @param {object} [opts]
 * @param {number} [opts.ejercicioParam]  — Año fiscal; si se omite usa el mes actual CDMX.
 * @param {number} [opts.periodoParam]    — Mes (1-12); si se omite usa el mes actual CDMX.
 */
const ejecutarDescargaERP = async ({ ejercicioParam, periodoParam } = {}) => {
  logger.info('[ERPSyncJob] Iniciando descarga automática ERP...');

  let ejercicio, periodo;
  if (ejercicioParam && periodoParam) {
    ejercicio = ejercicioParam;
    periodo   = periodoParam;
  } else {
    // Periodo actual en hora de México
    const fmtMX = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Mexico_City', year: 'numeric', month: '2-digit', day: '2-digit' });
    const hoyMX = fmtMX.format(new Date());
    const [anoStr, mesStr] = hoyMX.split('-');
    ejercicio = parseInt(anoStr, 10);
    periodo   = parseInt(mesStr, 10);
  }

  // Crear log de inicio
  let logEntry = null;
  try {
    logEntry = await SatDescargaLog.create({
      rfc: 'SISTEMA',
      tipo: 'erp_automatica',
      ejercicio,
      periodo,
      estado: 'en_proceso',
      inicio: new Date(),
    });
  } catch (logErr) {
    logger.warn(`[ERPSyncJob] No se pudo crear log de descarga: ${logErr.message}`);
  }

  const actualizarLog = async (campos) => {
    if (!logEntry) return;
    await SatDescargaLog.updateOne({ _id: logEntry._id }, { $set: campos }).catch(() => {});
  };

  // Verificar que el periodo fiscal exista, o crearlo automáticamente
  try {
    const { creado } = await resolverOCrearPeriodo(ejercicio, periodo);
    if (creado) logger.info(`[ERPSyncJob] Periodo ${periodo}/${ejercicio} creado automáticamente.`);
  } catch (err) {
    logger.error(`[ERPSyncJob] No se pudo resolver el periodo ${periodo}/${ejercicio}: ${err.message}. Descarga cancelada.`);
    await actualizarLog({ estado: 'error', error: err.message, fin: new Date() });
    return;
  }

  const { fechaInicio, fechaFin } = derivarFechasERP(ejercicio, periodo);
  logger.info(`[ERPSyncJob] Periodo: ${ejercicio}/${periodo} | ${fechaInicio} → ${fechaFin}`);

  // Descargar facturas del ERP
  let facturas;
  try {
    facturas = await fetchTodasLasFacturas({ fechaInicio, fechaFin });
  } catch (err) {
    logger.error(`[ERPSyncJob] Error conectando con ERP: ${err.message}`);
    await actualizarLog({ estado: 'error', error: err.message, fin: new Date() });
    return;
  }

  if (facturas.length === 0) {
    logger.info(`[ERPSyncJob] ERP no devolvió registros para ${ejercicio}/${periodo}`);
    await actualizarLog({ estado: 'completado', totalSAT: 0, fin: new Date() });
    return;
  }

  logger.info(`[ERPSyncJob] ${facturas.length} factura(s) recibidas del ERP. Procesando...`);

  let guardadas = 0, duplicadas = 0, omitidas = 0, conErrores = 0;

  for (let i = 0; i < facturas.length; i++) {
    const factura = facturas[i];
    let doc, erroresTransform = [];
    try {
      ({ doc, errores: erroresTransform } = transformarTolerante(factura, { ejercicio, periodo, uploadedBy: 'system' }));
    } catch (err) {
      logger.error(`[ERPSyncJob] Error transformando factura [${i + 1}]: ${err.message}`);
      conErrores++;
      continue;
    }

    if (doc.tipoDeComprobante === 'T') { omitidas++; continue; }

    try {
      const { isNew, isDuplicate } = await upsertFromERP(doc);
      if (isDuplicate) { duplicadas++; }
      else { isNew ? guardadas++ : duplicadas++; }
      if (erroresTransform.length > 0) conErrores++;
    } catch (err) {
      logger.error(`[ERPSyncJob] Error guardando UUID ${doc.uuid}: ${err.message}`);
      conErrores++;
    }
  }

  // Reclasificación automática de facturas globales
  if (guardadas > 0) {
    try {
      const reclass = await aplicarReclasificacion({ ejercicio, periodo, source: 'ERP' });
      if (reclass.totalModificados > 0) {
        logger.info(`[ERPSyncJob] Reclasificación: ${reclass.totalModificados} CFDI(s) corregidos`);
      }
    } catch (reclassErr) {
      logger.warn(`[ERPSyncJob] Reclasificación falló (no crítico): ${reclassErr.message}`);
    }
  }

  logger.info(
    `[ERPSyncJob] Completado | recibidas=${facturas.length} guardadas=${guardadas} ` +
    `duplicadas=${duplicadas} omitidas=${omitidas} conErrores=${conErrores}`
  );

  await actualizarLog({
    estado: 'completado',
    totalERP: guardadas + duplicadas,
    fin: new Date(),
  });
};

/**
 * Job nocturno de Comparación automática ERP vs SAT.
 * Compara todos los CFDIs ERP + SAT del periodo indicado
 * (o el mes actual si no se especifica).
 *
 * @param {object} [opts]
 * @param {number} [opts.ejercicioParam]  — Año fiscal; si se omite usa el mes actual CDMX.
 * @param {number} [opts.periodoParam]    — Mes (1-12); si se omite usa el mes actual CDMX.
 */
const ejecutarComparacionAuto = async ({ ejercicioParam, periodoParam } = {}) => {
  logger.info('[CompJobAuto] Iniciando comparación automática ERP vs SAT...');

  let ejercicio, periodo;
  if (ejercicioParam && periodoParam) {
    ejercicio = ejercicioParam;
    periodo   = periodoParam;
  } else {
    const fmtMX = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Mexico_City', year: 'numeric', month: '2-digit', day: '2-digit' });
    const hoyMX = fmtMX.format(new Date());
    const [anoStr, mesStr] = hoyMX.split('-');
    ejercicio = parseInt(anoStr, 10);
    periodo   = parseInt(mesStr, 10);
  }

  try {
    const { creado } = await resolverOCrearPeriodo(ejercicio, periodo);
    if (creado) logger.info(`[CompJobAuto] Periodo ${periodo}/${ejercicio} creado automáticamente.`);
  } catch (err) {
    logger.error(`[CompJobAuto] No se pudo resolver el periodo ${periodo}/${ejercicio}: ${err.message}. Comparación cancelada.`);
    return;
  }

  const baseFilter = { isActive: true, ejercicio, periodo, tipoDeComprobante: { $ne: 'T' } };

  // Recibidos SAT es solo para descarga/archivo — NO debe entrar al motor de
  // comparación ERP vs SAT. Se filtra al lado Emitidos exigiendo que el emisor
  // sea una de nuestras propias entidades (si el emisor es un tercero, el CFDI
  // es un recibido y se excluye de este query, sin importar su "source").
  const entidadesRfcs = (await entityRepo.findAll()).map(e => e.rfc?.toUpperCase()).filter(Boolean);
  // Excluye los ya conciliados manualmente — ver comentario en
  // compareSATOnlyCFDI (comparisonEngine.js).
  const satFilter = { ...baseFilter, source: { $in: ['SAT', 'MANUAL'] }, 'emisor.rfc': { $in: entidadesRfcs }, lastComparisonStatus: { $ne: 'conciliado' } };

  const [erpCfdis, satCfdis, allErpUuids] = await Promise.all([
    CFDI.find({ ...baseFilter, source: 'ERP' }, '_id uuid').lean(),
    CFDI.find(satFilter, '_id uuid').lean(),
    CFDI.find({ ...baseFilter, source: 'ERP' }, 'uuid').lean(),
  ]);

  const erpUuidSet   = new Set(allErpUuids.map(c => c.uuid.toUpperCase()));
  const satOnlyCfdis = satCfdis.filter(c => !erpUuidSet.has(c.uuid.toUpperCase()));

  const totalCFDIs = erpCfdis.length + satOnlyCfdis.length;
  if (totalCFDIs === 0) {
    logger.info('[CompJobAuto] Sin CFDIs para comparar en este periodo.');
    return;
  }

  logger.info(`[CompJobAuto] ${erpCfdis.length} ERP + ${satOnlyCfdis.length} solo-SAT = ${totalCFDIs} CFDIs`);

  const session = await ComparisonSession.create({
    name:        formatSessionName(new Date()) + ' (auto)',
    triggeredBy: null,
    totalCFDIs,
    status:      'running',
    filters:     { ejercicio, periodo, auto: true },
  });

  try {
    await batchCompareCFDIs(
      erpCfdis.map(c => c._id.toString()),
      {
        concurrency: 5,
        triggeredBy: null,
        sessionId:   session._id,
        satOnlyIds:  satOnlyCfdis.map(c => c._id.toString()),
      },
    );
    logger.info(`[CompJobAuto] Comparación completada. Sesión: ${session._id}`);
  } catch (err) {
    logger.error(`[CompJobAuto] Error en comparación: ${err.message}`);
  }
};

/**
 * Job nocturno de Descarga Masiva SAT.
 *
 * Para cada entidad con descarga nocturna habilitada:
 *  1. Verifica si hay credenciales e.firma registradas.
 *  2. Descarga CFDIs del día anterior desde el SAT.
 *  3. Compara contra CFDIs del ERP en MongoDB.
 *  4. Guarda resultados en Comparison y Discrepancy.
 *  5. Elimina las credenciales al terminar (éxito o fallo).
 */
/**
 * Reintenta checkpoints marcados como 'incompleto' de los últimos 45 días.
 * Se llama al inicio de cada descarga masiva nocturna.
 */
const reintentarIncompletos = async () => {
  const fmtMX = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Mexico_City', year: 'numeric', month: '2-digit', day: '2-digit' });
  const hoyMXStr = fmtMX.format(new Date());
  const hace45 = new Date(`${hoyMXStr}T12:00:00`);
  hace45.setDate(hace45.getDate() - 45);
  const hace45Str = fmtMX.format(hace45);

  const incompletos = await SatJobCheckpoint.find({
    status: 'incompleto',
    fecha:  { $gte: hace45Str },
  }).lean();

  if (incompletos.length === 0) {
    logger.info('[SatSyncJob] reintentarIncompletos: no hay checkpoints incompletos pendientes.');
    return;
  }

  logger.info(`[SatSyncJob] reintentarIncompletos: ${incompletos.length} checkpoint(s) incompleto(s) encontrado(s) — reintentando...`);

  for (const cp of incompletos) {
    const { rfc, fecha, tipoComprobante, ejercicio, periodo, reintentos = 0 } = cp;

    // Máximo 3 reintentos para no desperdiciar solicitudes SAT en días sin CFDIs
    if (reintentos >= 3) {
      if (!cp.alertaPendiente) {
        await SatJobCheckpoint.updateOne(
          { _id: cp._id },
          { $set: { alertaPendiente: true, updatedAt: new Date() } },
        ).catch(() => {});
      }
      logger.error(
        `[SatSyncJob] ⛔ ALERTA: RFC ${rfc} | ${tipoComprobante} | ${fecha} ` +
        `agotó ${reintentos} reintentos sin éxito — CFDIs potencialmente perdidos. ` +
        `Acción requerida: DELETE /api/sat/checkpoint/${rfc}?fecha=${fecha}&tipo=${tipoComprobante}`
      );
      continue;
    }

    const limitCheck = await puedeIniciar(rfc, 1);
    if (!limitCheck.puede) {
      logger.warn(`[SatSyncJob] reintentarIncompletos: RFC ${rfc} bloqueado — ${limitCheck.razon}`);
      break; // si no hay cupo, dejar de intentar más
    }

    let creds = null;
    try {
      creds = await obtener(rfc);
    } catch {
      logger.warn(`[SatSyncJob] reintentarIncompletos: sin credenciales para RFC ${rfc} — omitiendo.`);
      continue;
    }
    if (!creds) {
      logger.warn(`[SatSyncJob] reintentarIncompletos: credenciales nulas para RFC ${rfc} — omitiendo.`);
      continue;
    }

    // Incrementar contador de reintentos ANTES de la descarga (sin cambiar status —
    // descargarPorSubtipo necesita ver 'incompleto' para hacer una nueva solicitud SAT).
    await SatJobCheckpoint.updateOne(
      { _id: cp._id },
      { $inc: { reintentos: 1 }, $set: { updatedAt: new Date() } },
    ).catch(() => {});

    // Determinar si el checkpoint es una mitad (_M1/_M2) o el día completo
    const yaTieneMitad   = /_M[12]$/.test(tipoComprobante);
    const tipoBase       = tipoComprobante.replace(/_M[12]$/, '');
    const cpSufijoActual = yaTieneMitad ? tipoComprobante.slice(tipoBase.length) : '';
    const horaIni        = tipoComprobante.endsWith('_M2') ? '12:00:00' : '00:00:00';
    const horaFin        = tipoComprobante.endsWith('_M1') ? '11:59:59' : '23:59:59';
    const fechaFin       = cp.fechaFin ?? fecha;

    logger.info(`[SatSyncJob] reintentarIncompletos: reintentando RFC ${rfc} ${tipoComprobante} ${fecha} (intento ${reintentos + 1}/3)`);

    let iniciado = false;
    try {
      await registrarInicio(rfc, 1);
      iniciado = true;

      if (reintentos >= 1 && !yaTieneMitad) {
        // Segunda+ retry de un checkpoint de día completo: dividir en 2 mitades
        logger.info(
          `[SatSyncJob] reintentarIncompletos: RFC ${rfc} ${tipoBase} ${fecha} — ` +
          `dividiendo día en 2 mitades (00:00-11:59 y 12:00-23:59)...`
        );

        // Mitad 1 (usa el slot ya registrado)
        await procesarDescarga({
          rfc, tipoSolicitud: 'CFDI', tipoComprobante: tipoBase, creds, ejercicio, periodo,
          fechaInicio: `${fecha}T00:00:00`,
          fechaFin:    `${fecha}T11:59:59`,
          tipo: 'reintento_incompleto', cpSufijo: '_M1',
        });
        registrarFin(rfc);
        iniciado = false;

        // Mitad 2 (verificar cuota antes de iniciar)
        const limitM2 = await puedeIniciar(rfc, 1);
        if (limitM2.puede) {
          await registrarInicio(rfc, 1);
          iniciado = true;
          await procesarDescarga({
            rfc, tipoSolicitud: 'CFDI', tipoComprobante: tipoBase, creds, ejercicio, periodo,
            fechaInicio: `${fecha}T12:00:00`,
            fechaFin:    `${fecha}T23:59:59`,
            tipo: 'reintento_incompleto', cpSufijo: '_M2',
          });
        } else {
          logger.warn(`[SatSyncJob] reintentarIncompletos M2: RFC ${rfc} sin cuota para segunda mitad — ${limitM2.razon}`);
        }

        // El checkpoint original ya fue reemplazado por M1/M2 — marcarlo completado
        await SatJobCheckpoint.updateOne(
          { _id: cp._id },
          { $set: { status: 'completado', updatedAt: new Date() } }
        ).catch(() => {});

      } else {
        // Primer reintento (día completo) o reintento de una mitad ya existente
        await procesarDescarga({
          rfc,
          fechaInicio:     `${fecha}T${horaIni}`,
          fechaFin:        `${fechaFin}T${horaFin}`,
          tipoSolicitud:   'CFDI',
          tipoComprobante: tipoBase,
          cpSufijo:        cpSufijoActual,
          creds,
          ejercicio,
          periodo,
          tipo:            'reintento_incompleto',
        });
      }
    } catch (err) {
      logger.error(`[SatSyncJob] reintentarIncompletos: error en RFC ${rfc} ${tipoComprobante} ${fecha}: ${err.message}`);
    } finally {
      if (iniciado) registrarFin(rfc);
    }
  }
};

const ejecutarDescargaMasiva = async ({ tipos: tiposObjetivo = ['Emitidos'] } = {}) => {
  logger.info(`[SatSyncJob] Iniciando descarga masiva nocturna (${tiposObjetivo.join(', ')})...`);

  // Fechas en hora de México (el SAT usa CDMX como referencia)
  const fmtMX = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Mexico_City', year: 'numeric', month: '2-digit', day: '2-digit' });
  const hoyMXStr = fmtMX.format(new Date());
  const [anoHoy, mesHoy, diaHoy] = hoyMXStr.split('-').map(Number);

  // Ayer
  const ayerDate = new Date(`${hoyMXStr}T12:00:00`);
  ayerDate.setDate(ayerDate.getDate() - 1);
  const ayerMXStr = fmtMX.format(ayerDate);

  // Hace 3 días
  const tresDate = new Date(`${hoyMXStr}T12:00:00`);
  tresDate.setDate(tresDate.getDate() - 3);
  const tresMXStr = fmtMX.format(tresDate);

  // ejercicio/periodo derivados de ayer (mes del fin del rango diario)
  const ejercicio = parseInt(ayerMXStr.split('-')[0], 10);
  const periodo   = parseInt(ayerMXStr.split('-')[1], 10);

  // ── Rangos de descarga ───────────────────────────────────────────────────────
  // Siempre: últimos 3 días, XMLs completos (tipoSolicitud forzado a 'CFDI')
  const rangos = [
    {
      fechaInicio:    `${tresMXStr}T00:00:00`,
      fechaFin:       `${ayerMXStr}T23:59:59`,
      tipoSolicitud:  'CFDI',
      label: `últimos 3 días (${tresMXStr} → ${ayerMXStr})`,
    },
  ];

  // Del día 15 al último día del mes: repaso completo desde el 1ro, XMLs completos
  if (diaHoy >= 15) {
    const mesStrPad = String(mesHoy).padStart(2, '0');
    const primero   = `${anoHoy}-${mesStrPad}-01`;
    rangos.push({
      fechaInicio:   `${primero}T00:00:00`,
      fechaFin:      `${ayerMXStr}T23:59:59`,
      tipoSolicitud: 'CFDI',
      label: `repaso mensual (${primero} → ${ayerMXStr})`,
    });
    logger.info(`[SatSyncJob] Día ${diaHoy}: se agrega repaso mensual desde ${primero}.`);
  }

  try {
    const { creado } = await resolverOCrearPeriodo(ejercicio, periodo);
    if (creado) logger.info(`[SatSyncJob] Periodo ${periodo}/${ejercicio} creado automáticamente.`);
  } catch (err) {
    logger.error(`[SatSyncJob] No se pudo resolver el periodo ${periodo}/${ejercicio}: ${err.message}. Descarga cancelada.`);
    return;
  }
  logger.info(`[SatSyncJob] Periodo fiscal validado: ${ejercicio}/${periodo} | ${rangos.length} rango(s) programado(s).`);

  // Entidades con descarga nocturna habilitada (PostgreSQL)
  const entidades = await entityRepo.findWithAutoSync();

  if (entidades.length === 0) {
    logger.info('[SatSyncJob] No hay entidades con descarga nocturna habilitada.');
    return;
  }

  logger.info(`[SatSyncJob] Procesando ${entidades.length} entidad(es)...`);

  // Tipos del diario: Ingresos, Egresos, Pagos y Nómina
  const TIPOS_DIARIO = ['Ingresos', 'Egresos', 'Pagos' /*, 'Nomina' */];

  for (const entidad of entidades) {
    const rfc = entidad.rfc;
    logger.info(`[SatSyncJob] Procesando RFC: ${rfc}`);

    let creds = null;
    try {
      // ── 1. Verificar credenciales ────────────────────────────────────────
      const estado = await tieneCredenciales(rfc);
      if (!estado.tiene) {
        logger.warn(`[SatSyncJob] Sin credenciales e.firma para RFC ${rfc}. Omitiendo.`);
        continue;
      }

      creds = await obtener(rfc);
      if (!creds) {
        logger.warn(`[SatSyncJob] No se pudieron obtener credenciales para RFC ${rfc}. Omitiendo.`);
        continue;
      }

      // ── 2. Procesar cada rango de fechas ─────────────────────────────────
      for (const rango of rangos) {
        logger.info(`[SatSyncJob] RFC ${rfc}: procesando rango "${rango.label}"`);

        // Filtra los tipos pedidos por el caller según la config de sincronización de la entidad.
        // Emitidos y Recibidos corren en jobs/horarios separados (ver reprogramarJobs) para no
        // saturar al SAT con solicitudes simultáneas del mismo RFC.
        const tipos = tiposObjetivo.filter(t => {
          if (t === 'Emitidos')  return entidad.syncConfig?.syncEmitidos  !== false;
          if (t === 'Recibidos') return entidad.syncConfig?.syncRecibidos !== false;
          return true;
        });

        for (const tipoComprobante of tipos) {
          // Cada Emitidos se divide en 4 sub-solicitudes SAT
          const solicitudesNecesarias = tipoComprobante === 'Emitidos' ? TIPOS_DIARIO.length : 1;
          const limitCheck = await puedeIniciar(rfc, solicitudesNecesarias);
          if (!limitCheck.puede) {
            logger.warn(`[SatSyncJob] RFC ${rfc} (${tipoComprobante} / ${rango.label}): bloqueado — ${limitCheck.razon}`);
            continue;
          }
          let iniciado = false;
          try {
            await registrarInicio(rfc, solicitudesNecesarias);
            iniciado = true;
            await procesarDescarga({
              rfc,
              fechaInicio:   rango.fechaInicio,
              fechaFin:      rango.fechaFin,
              tipoSolicitud: rango.tipoSolicitud,
              tipoComprobante,
              creds,
              ejercicio,
              periodo,
              tiposEmitidosSplit: tipoComprobante === 'Emitidos' ? TIPOS_DIARIO : undefined,
            });
          } catch (descErr) {
            logger.error(`[SatSyncJob] RFC ${rfc} (${tipoComprobante} / ${rango.label}): ${descErr.message}`);
            throw descErr;
          } finally {
            if (iniciado) registrarFin(rfc);
          }
        }
      }

      // Actualizar fecha de última sincronización (Sequelize — PostgreSQL)
      await entityRepo.update(entidad.id, {
        syncConfig: { ...entidad.syncConfig, lastSync: new Date() },
      });

    } catch (err) {
      logger.error(`[SatSyncJob] Error procesando RFC ${rfc}: ${err.message}`);
    }
    // Las credenciales expiran automáticamente a las 8 horas via TTL de MongoDB.
  }

  // Reintentar checkpoints incompletos de días anteriores — se hace AL FINAL
  // para no comprometer la cuota SAT del día actual.
  await reintentarIncompletos().catch(err =>
    logger.error(`[SatSyncJob] reintentarIncompletos falló: ${err.message}`)
  );

  logger.info('[SatSyncJob] Descarga masiva nocturna completada.');
};

// Mapa de efecto SAT (metadata) a tipoDeComprobante (XML)
const EFECTO_MAP = { I: 'I', Ingreso: 'I', E: 'E', Egreso: 'E', T: 'T', Traslado: 'T', N: 'N', 'Nómina': 'N', Nomina: 'N', P: 'P', Pago: 'P' };

/**
 * Convierte una fila de metadatos SAT al mismo formato que normalizarCFDI
 * para que compararArrays() pueda procesarla sin cambios.
 */
const normalizarMetadato = (row) => {
  const tipo = EFECTO_MAP[row.efecto] || row.efecto || '';
  const subTotalVal = parseFloat(row.subTotal || '0') || 0;
  const monedaVal   = row.moneda || 'MXN';
  return {
    uuid:              (row.uuid || '').toUpperCase().trim(),
    rfcEmisor:         (row.rfcEmisor || '').toUpperCase().trim(),
    rfcReceptor:       (row.rfcReceptor || '').toUpperCase().trim(),
    total:             parseFloat(row.total || '0') || 0,
    subtotal:          subTotalVal, // campo en minúsculas para detectarDiferencias
    fecha:             new Date(row.fecha || ''),
    tipoDeComprobante: tipo,
    tipoComprobante:   tipo, // alias en minúsculas requerido por detectarDiferencias / normalizarCFDI
    moneda:            monedaVal,
    satStatus:         row.estado || 'Vigente',
    estatus:           row.estado || 'Vigente',
    // Campos usados al guardar en MongoDB
    emisor:            { rfc: (row.rfcEmisor || '').toUpperCase().trim(), nombre: row.nombreEmisor || '' },
    receptor:          { rfc: (row.rfcReceptor || '').toUpperCase().trim(), nombre: row.nombreReceptor || '' },
    subTotal:          subTotalVal,
    serie:             '',
    folio:             '',
    version:           row.version || '4.0',
    xmlContent:        null,
    xmlHash:           null,
    conceptos:         [],
    impuestos:         {},
    timbreFiscalDigital: null,
    complementoPago:   null,
  };
};

/**
 * Descarga y parsea los CFDIs o metadatos del SAT para un tipo/solicitud.
 * Maneja su propio checkpoint independiente.
 * @param {string} [tipoSolicitud='CFDI']  — 'CFDI' para XMLs completos, 'Metadata' para metadatos TXT.
 * Retorna { rows: [], paquetes: number, totalReportado: number, esMetadata: boolean }
 */
const descargarPorSubtipo = async ({ rfc, fechaInicio, fechaFin, ejercicio, periodo, tipoComprobante, creds, tipoSolicitud = 'CFDI', folioFiscalUUID, cpSufijo = '' }) => {
  const esMetadata  = tipoSolicitud === 'Metadata';
  // Incluir modo y sufijo en la clave del checkpoint para no mezclar XML con metadata ni mitades
  const cpTipo = (esMetadata ? `${tipoComprobante}_Metadata` : tipoComprobante) + cpSufijo;
  const fecha  = fechaInicio.slice(0, 10);
  let checkpoint = await SatJobCheckpoint.findOne({ rfc: rfc.toUpperCase(), fecha, tipoComprobante: cpTipo });

  // Si ya hay una solicitud en vuelo (no completada ni con error), no duplicar
  const enVuelo = checkpoint?.status === 'solicitando' || checkpoint?.status === 'verificando';
  if (enVuelo && (Date.now() - new Date(checkpoint.updatedAt).getTime()) < CHECKPOINT_MAX_AGE_MS) {
    logger.warn(`[SatSyncJob] ${tipoComprobante} ${fecha} ya tiene solicitud activa (${checkpoint.status}) — omitiendo para evitar rechazo SAT.`);
    return { rows: [], paquetes: 0, totalReportado: 0, esMetadata };
  }

  let idSolicitud, idsPaquetes;

  // Los paquetes SAT caducan a las 72 horas. Si el checkpoint tiene más de 72 horas,
  // se descarta y se hace una nueva solicitud para evitar descargar un paquete ya expirado.
  const CHECKPOINT_MAX_AGE_MS = 72 * 60 * 60 * 1000; // 72 horas
  const checkpointVigente =
    checkpoint?.status === 'descargando' &&
    checkpoint.idsPaquetes?.length > 0 &&
    checkpoint.updatedAt &&
    (Date.now() - new Date(checkpoint.updatedAt).getTime()) < CHECKPOINT_MAX_AGE_MS;

  // Edge case: el proceso cayó entre solicitar() y verificar() — el checkpoint quedó
  // en 'verificando' con idSolicitud ya guardado. Se reutiliza esa solicitud en vez
  // de hacer una nueva (que daría SAT [5005] solicitud duplicada).
  const checkpointVerificando =
    checkpoint?.status === 'verificando' &&
    checkpoint.idSolicitud &&
    checkpoint.updatedAt &&
    (Date.now() - new Date(checkpoint.updatedAt).getTime()) < CHECKPOINT_MAX_AGE_MS;

  // Flag: el checkpoint estaba en 'verificando' pero el SAT rechazó esa solicitud;
  // hay que hacer una nueva solicitud pasando al bloque de nueva-solicitud con retry.
  let rechazadaCheckpointVerif = false;

  if (checkpointVerificando) {
    logger.warn(`[SatSyncJob] Checkpoint ${tipoComprobante} quedó en 'verificando' — reanudando verificación de solicitud ${checkpoint.idSolicitud}`);
    let totalReportadoSAT = 0;
    try {
      ({ idsPaquetes, totalCfdis: totalReportadoSAT } = await verificar(checkpoint.idSolicitud, rfc, creds));
      logger.info(`[SatSyncJob] ${tipoComprobante}: ${idsPaquetes.length} paquete(s), ${totalReportadoSAT} CFDIs reportados por SAT`);
      if (idsPaquetes.length === 0) {
        await SatJobCheckpoint.updateOne({ _id: checkpoint._id }, { $set: { status: 'completado', updatedAt: new Date() } });
        return { rows: [], paquetes: 0, totalReportado: totalReportadoSAT, esMetadata };
      }
      await SatJobCheckpoint.updateOne({ _id: checkpoint._id }, { $set: { idsPaquetes, totalReportadoSAT, status: 'descargando', updatedAt: new Date() } });
      checkpoint.totalReportadoSAT = totalReportadoSAT;
      idSolicitud = checkpoint.idSolicitud;
    } catch (rechazadaErr) {
      // Cualquier error al re-verificar la solicitud previa (SAT_RECHAZADA, 5004, red, etc.)
      // implica que esa solicitud ya no es recuperable — limpiar checkpoint y reintentar con solicitud nueva.
      logger.warn(
        `[SatSyncJob] Solicitud previa ${checkpoint.idSolicitud} (${tipoComprobante}) no recuperable: ${rechazadaErr.message} — ` +
        `descartando checkpoint y solicitando de nuevo...`
      );
      await SatJobCheckpoint.updateOne(
        { _id: checkpoint._id },
        { $set: { status: 'error', idSolicitud: null, error: rechazadaErr.message, updatedAt: new Date() } }
      ).catch(() => {});
      rechazadaCheckpointVerif = true;
    }
  }

  if (!checkpointVerificando || rechazadaCheckpointVerif) {
    if (checkpointVigente && !rechazadaCheckpointVerif) {
    idSolicitud = checkpoint.idSolicitud;
    // ── Re-verificar con el SAT para asegurar que tenemos TODOS los IDs de paquetes.
    // Protege contra checkpoints guardados con código anterior que extraía IDs incompletos,
    // y también actualiza totalReportadoSAT si faltaba (checkpoints sin ese campo).
    try {
      let totalFresh;
      ({ idsPaquetes, totalCfdis: totalFresh } = await verificar(idSolicitud, rfc, creds));
      if (idsPaquetes.length !== checkpoint.idsPaquetes.length) {
        logger.warn(
          `[SatSyncJob] ⚠ Checkpoint tenía ${checkpoint.idsPaquetes.length} paquete(s) pero SAT reporta ` +
          `${idsPaquetes.length} — checkpoint corregido.`
        );
      }
      await SatJobCheckpoint.updateOne(
        { _id: checkpoint._id },
        { $set: { idsPaquetes, totalReportadoSAT: totalFresh, updatedAt: new Date() } }
      );
      checkpoint.totalReportadoSAT = totalFresh;
    } catch (verifErr) {
      // Si la re-verificación falla (ej. solicitud expirada o error de red),
      // se usan los IDs del checkpoint como fallback — mejor descargar algo que nada.
      logger.warn(`[SatSyncJob] No se pudo re-verificar ${idSolicitud}: ${verifErr.message} — usando IDs del checkpoint.`);
      idsPaquetes = checkpoint.idsPaquetes;
    }
    const ya = checkpoint.paquetesProcesados?.length ?? 0;
    logger.info(
      `[SatSyncJob] Reanudando ${tipoComprobante}: ${ya}/${idsPaquetes.length} paquete(s) procesados, ` +
      `${checkpoint.totalReportadoSAT ?? 0} CFDIs reportados por SAT.`
    );
  } else {
    if (checkpoint?.status === 'descargando') {
      logger.warn(`[SatSyncJob] Checkpoint ${tipoComprobante} caducado (>72h) — descartando y haciendo nueva solicitud.`);
    }

    // Retry si el SAT rechaza la solicitud (SAT_RECHAZADA): esperar 5 min y reintentar.
    // Causa habitual: solicitud activa previa del mismo RFC aún no cerrada por el SAT.
    const MAX_REINTENTOS_RECHAZADA = 2;
    const ESPERA_RECHAZADA_MS = 5 * 60 * 1000; // 5 minutos

    let totalReportadoSATLocal = 0;

    for (let intento = 1; intento <= MAX_REINTENTOS_RECHAZADA; intento++) {
      checkpoint = await SatJobCheckpoint.findOneAndUpdate(
        { rfc: rfc.toUpperCase(), fecha, tipoComprobante: cpTipo },
        { $set: { ejercicio, periodo, fechaFin: fechaFin.slice(0, 10), status: 'solicitando', idSolicitud: null, idsPaquetes: [], paquetesProcesados: [], paquetesFallidos: [], cfdisDescargados: 0, error: null, updatedAt: new Date() } },
        { upsert: true, new: true },
      );
      try {
        idSolicitud = await solicitar({ rfcSolicitante: rfc, fechaInicio, fechaFin, tipoComprobante, tipoSolicitud, creds, folioFiscalUUID });
        await SatJobCheckpoint.updateOne({ _id: checkpoint._id }, { $set: { idSolicitud, status: 'verificando', updatedAt: new Date() } });

        ({ idsPaquetes, totalCfdis: totalReportadoSATLocal } = await verificar(idSolicitud, rfc, creds));
        break; // solicitud aceptada y terminada — salir del loop de reintentos
      } catch (rechazadaErr) {
        const esRechazada     = rechazadaErr.message.startsWith('SAT_RECHAZADA');
        const esErrorInterno  = rechazadaErr.message.includes('SAT [5006]');
        if ((esRechazada || esErrorInterno) && intento < MAX_REINTENTOS_RECHAZADA) {
          logger.warn(
            `[SatSyncJob] ${tipoComprobante} ${esErrorInterno ? 'error interno SAT [5006]' : 'rechazada'} (intento ${intento}/${MAX_REINTENTOS_RECHAZADA}) — ` +
            `esperando ${ESPERA_RECHAZADA_MS / 60000} min antes de reintentar...`
          );
          await new Promise(r => setTimeout(r, ESPERA_RECHAZADA_MS));
          continue;
        }
        // Reintentos agotados o error distinto a Rechazada.
        // Marcar checkpoint como 'error' para que el siguiente run del job
        // no intente re-verificar este idSolicitud (que ya está rechazado).
        await SatJobCheckpoint.updateOne(
          { _id: checkpoint._id },
          { $set: { status: 'error', error: rechazadaErr.message, updatedAt: new Date() } }
        ).catch(() => {});
        throw rechazadaErr;
      }
    }

    logger.info(`[SatSyncJob] ${tipoComprobante}: ${idsPaquetes.length} paquete(s), ${totalReportadoSATLocal} CFDIs reportados por SAT`);

    if (idsPaquetes.length === 0) {
      await SatJobCheckpoint.updateOne({ _id: checkpoint._id }, { $set: { status: 'completado', updatedAt: new Date() } });
      return { rows: [], paquetes: 0, totalReportado: totalReportadoSATLocal, esMetadata };
    }
    await SatJobCheckpoint.updateOne({ _id: checkpoint._id }, { $set: { idsPaquetes, totalReportadoSAT: totalReportadoSATLocal, status: 'descargando', updatedAt: new Date() } });
    checkpoint.totalReportadoSAT = totalReportadoSATLocal;
    }
  }

  // Recuperar totalReportadoSAT del checkpoint (ya actualizado arriba si es nueva solicitud)
  const totalReportadoSAT = checkpoint.totalReportadoSAT ?? 0;

  const yaProcessados = new Set(checkpoint.paquetesProcesados ?? []);
  const pendientes    = idsPaquetes.filter(id => !yaProcessados.has(id));

  const rows = [];
  let paquetesFallidos = 0;
  for (const idPaquete of pendientes) {
    try {
      if (esMetadata) {
        const registros = await descargarPaqueteMetadata(idPaquete, rfc, creds);
        rows.push(...registros);
      } else {
        let xmls = await descargarPaquete(idPaquete, rfc, creds);
        for (const xml of xmls) {
          try {
            const parsed = await parseCFDI(xml);
            rows.push(parsed);
          } catch (parseErr) {
            logger.warn(`[SatSyncJob] Error parseando XML en ${idPaquete}: ${parseErr.message}`);
          }
        }
        xmls = null;
      }
      await SatJobCheckpoint.updateOne(
        { _id: checkpoint._id },
        { $addToSet: { paquetesProcesados: idPaquete }, $set: { updatedAt: new Date() } },
      );
      logger.info(`[SatSyncJob] Paquete ${idPaquete} procesado (${esMetadata ? 'metadata' : 'XML'}).`);
    } catch (pkgErr) {
      // Un paquete fallido no cancela los demás — se registra el error y continúa.
      // NOTA: El SAT solo permite 2 descargas por paquete. Si ambos intentos fallaron,
      // este paquete ya NO se puede reintentar — se necesita una nueva solicitud SAT.
      paquetesFallidos++;
      logger.error(`[SatSyncJob] ⚠ Paquete ${idPaquete} falló (se omite): ${pkgErr.message}`);
      logger.error(`[SatSyncJob]   → El SAT permite máx 2 descargas por paquete. Si ambos intentos fallaron, elimina el checkpoint para hacer una nueva solicitud.`);
      // Marcar como fallido permanente para no volver a intentarlo en las re-verificaciones
      await SatJobCheckpoint.updateOne(
        { _id: checkpoint._id },
        { $addToSet: { paquetesFallidos: idPaquete }, $set: { updatedAt: new Date() } },
      ).catch(() => {});
    }
  }
  // ── Re-verificar si hay más paquetes disponibles ─────────────────────────
  // El SAT a veces retorna estado=3 (Terminada) con paquetes parciales y agrega
  // los restantes poco después. Para datasets grandes el SAT puede tardar 20-30 min
  // en generar todos los paquetes — MAX_REVERIF escala según NumeroCFDIs reportados.
  const MAX_REVERIF = totalReportadoSAT > 5000 ? 30 : totalReportadoSAT > 1000 ? 15 : 10;
  // Número de re-verificaciones vacías consecutivas antes de rendirse.
  // El SAT puede tardar varios minutos en generar paquetes adicionales — no romper al primer vacío.
  const MAX_VACIOS_CONSECUTIVOS = totalReportadoSAT > 1000 ? 5 : 3;
  if (!esMetadata && totalReportadoSAT > 0 && rows.length < totalReportadoSAT * 0.95) {
    let vaciosConsecutivos = 0;
    for (let rv = 1; rv <= MAX_REVERIF; rv++) {
      logger.warn(
        `[SatSyncJob] ⚠ DESCARGA INCOMPLETA (${tipoComprobante}): ` +
        `${rows.length}/${totalReportadoSAT} CFDIs — re-verificando en 60s (intento ${rv}/${MAX_REVERIF})...`
      );
      await new Promise(r => setTimeout(r, 60_000));

      let paquetesActualizados;
      try {
        ({ idsPaquetes: paquetesActualizados } = await verificar(idSolicitud, rfc, creds));
      } catch (reverErr) {
        logger.warn(`[SatSyncJob] Re-verificación ${rv} fallida: ${reverErr.message} — se detiene la búsqueda de paquetes adicionales.`);
        break;
      }

      // Refetch checkpoint para obtener paquetesProcesados actualizados en esta ejecución.
      // Sin este refetch, el checkpoint en memoria es el inicial (vacío para nueva solicitud)
      // y todos los paquetes ya descargados aparecerían como "nuevos", agotando el límite
      // de 2 descargas por paquete que impone el SAT.
      const cpFresh = await SatJobCheckpoint.findById(checkpoint._id).lean();
      if (cpFresh) checkpoint = cpFresh;

      // Excluir tanto los exitosos como los permanentemente fallidos (límite 2 del SAT)
      const yaProcessados2 = new Set([
        ...(checkpoint.paquetesProcesados ?? []),
        ...(checkpoint.paquetesFallidos   ?? []),
      ]);
      const nuevos = paquetesActualizados.filter(id => !yaProcessados2.has(id));

      if (nuevos.length === 0) {
        vaciosConsecutivos++;
        logger.info(`[SatSyncJob] Re-verificación ${rv}: el SAT no reportó paquetes adicionales (vacío ${vaciosConsecutivos}/${MAX_VACIOS_CONSECUTIVOS}).`);
        if (vaciosConsecutivos >= MAX_VACIOS_CONSECUTIVOS) break;
        continue;
      }
      vaciosConsecutivos = 0;

      logger.info(`[SatSyncJob] Re-verificación ${rv}: ${nuevos.length} paquete(s) nuevo(s) encontrado(s) — descargando...`);
      for (const idPaquete of nuevos) {
        try {
          let xmls = await descargarPaquete(idPaquete, rfc, creds);
          for (const xml of xmls) {
            try { rows.push(await parseCFDI(xml)); }
            catch (parseErr) { logger.warn(`[SatSyncJob] Error parseando XML en ${idPaquete}: ${parseErr.message}`); }
          }
          xmls = null;
          await SatJobCheckpoint.updateOne(
            { _id: checkpoint._id },
            { $addToSet: { paquetesProcesados: idPaquete }, $set: { updatedAt: new Date() } },
          );
          logger.info(`[SatSyncJob] Paquete adicional ${idPaquete} procesado (XML).`);
        } catch (pkgErr) {
          logger.error(`[SatSyncJob] ⚠ Paquete adicional ${idPaquete} falló: ${pkgErr.message}`);
          await SatJobCheckpoint.updateOne(
            { _id: checkpoint._id },
            { $addToSet: { paquetesFallidos: idPaquete }, $set: { updatedAt: new Date() } },
          ).catch(() => {});
        }
      }

      if (rows.length >= totalReportadoSAT * 0.95) {
        logger.info(`[SatSyncJob] Re-verificación ${rv}: descarga completa tras encontrar paquetes adicionales.`);
        break;
      }
    }
  }

  // Marcar checkpoint según resultado final
  const esIncompleto = !esMetadata && totalReportadoSAT > 0 && rows.length < totalReportadoSAT * 0.95;
  if (paquetesFallidos > 0) {
    logger.warn(`[SatSyncJob] ${paquetesFallidos} de ${pendientes.length} paquetes fallaron — la descarga puede estar incompleta.`);
  }
  const nuevoStatus = esIncompleto ? 'incompleto' : 'completado';
  await SatJobCheckpoint.updateOne(
    { _id: checkpoint._id },
    { $set: { status: nuevoStatus, cfdisDescargados: rows.length, updatedAt: new Date() } },
  );

  // Aviso final de completitud
  if (!esMetadata && totalReportadoSAT > 0 && rows.length < totalReportadoSAT * 0.95) {
    logger.warn(
      `[SatSyncJob] ⚠ DESCARGA INCOMPLETA FINAL (${tipoComprobante}): SAT reportó ${totalReportadoSAT} CFDIs ` +
      `pero solo se descargaron ${rows.length} (${Math.round((rows.length / totalReportadoSAT) * 100)}%). ` +
      `El SAT no añadió más paquetes tras ${MAX_REVERIF} re-verificaciones. ` +
      `Elimina el checkpoint de MongoDB para reintentar la solicitud completa.`
    );
  } else if (!esMetadata && totalReportadoSAT === 0 && rows.length > 0) {
    logger.warn(
      `[SatSyncJob] ⚠ No se puede verificar completitud de ${tipoComprobante}: ` +
      `SAT no reportó total de CFDIs (descargados=${rows.length}). ` +
      `Si esperas más CFDIs, elimina el checkpoint de MongoDB y reintenta.`
    );
  }

  return { rows, paquetes: idsPaquetes.length, totalReportado: totalReportadoSAT, esMetadata };
};

/**
 * Ejecuta la descarga, parseo y comparación para un RFC/tipo/rango.
 * @param {number}   ejercicio  — Año fiscal al que vincular los CFDIs descargados.
 * @param {number}   periodo    — Mes fiscal (1–12) al que vincular los CFDIs descargados.
 * @param {Function} [onPaso]   — Callback opcional (paso: number) para reportar progreso al frontend.
 *                                Pasos: 1=Autenticando, 3=Verificando, 4=Descargando, 5=Procesando.
 */
const procesarDescarga = async ({ rfc, fechaInicio, fechaFin, tipoComprobante, tipoSolicitud, creds, ayer, ejercicio, periodo, onPaso, tipo = 'automatica', tiposEmitidosSplit, folioFiscalUUID, cpSufijo = '' }) => {
  logger.info(`[SatSyncJob] RFC ${rfc} — solicitando ${tipoComprobante} ${fechaInicio.slice(0, 10)}`);

  const fecha = fechaInicio.slice(0, 10); // YYYY-MM-DD

  // Crear entrada de log al inicio
  let logId = null;
  try {
    const logEntry = await SatDescargaLog.create({
      rfc: rfc.toUpperCase(),
      tipo,
      tipoComprobante,
      fechaInicio: fecha,
      fechaFin: fechaFin.slice(0, 10),
      ejercicio,
      periodo,
      estado: 'en_proceso',
      inicio: new Date(),
    });
    logId = logEntry._id;
  } catch (logErr) {
    logger.warn(`[SatSyncJob] No se pudo crear log de descarga: ${logErr.message}`);
  }

  const actualizarLog = async (campos) => {
    if (!logId) return;
    await SatDescargaLog.updateOne({ _id: logId }, { $set: campos }).catch(() => {});
  };

  try {
    // ── Determinar modo: se respeta el parámetro tipoSolicitud si viene del caller.
    // Para el job nocturno (1 día) se usa CFDI por defecto.
    // Para descarga manual el usuario puede elegir CFDI o Metadata desde el frontend.
    const diffDias   = Math.round((new Date(fechaFin) - new Date(fechaInicio)) / (1000 * 60 * 60 * 24));
    const modoFinal  = tipoSolicitud ?? (diffDias > 5 ? 'Metadata' : 'CFDI');

    logger.info(`[SatSyncJob] RFC ${rfc}: modo=${modoFinal}, diffDias=${diffDias}${tipoSolicitud ? ' (elegido por usuario)' : ' (auto)'}`);

    onPaso?.(1);
    let totalPaquetes     = 0;
    let totalReportadoSAT = 0;
    let esMetadata        = false;
    let incompleta        = false;
    let reclasificacionResultado = null;

    // En modo XML + Emitidos: dividir por sub-tipo.
    // tiposEmitidosSplit permite al caller restringir qué sub-tipos se solicitan
    // (ej. el job diario solo pide Ingresos, Egresos y Pagos).
    const TIPOS_SPLIT_EMITIDOS = tiposEmitidosSplit ?? ['Ingresos', 'Egresos', 'Pagos', 'Nomina'];
    const tiposADescargar = (modoFinal === 'CFDI' && tipoComprobante === 'Emitidos')
      ? TIPOS_SPLIT_EMITIDOS
      : [tipoComprobante];

    // Mapa tipo → letra SAT (usado para filtrar el ERP por tipo dentro del loop)
    const TIPO_LETRA = {
      Ingresos: 'I', Egresos: 'E', Traslados: 'T', Nomina: 'N', Pagos: 'P',
      RecibidosIngresos: 'I', RecibidosEgresos: 'E', RecibidosTraslados: 'T', RecibidosNomina: 'N', RecibidosPagos: 'P',
    };

    const inicioDelDia = new Date(fechaInicio);
    const finDelDia    = new Date(fechaFin);
    const esRecibidos  = tipoComprobante === 'Recibidos' || tipoComprobante.startsWith('Recibidos');
    const campoRfc     = esRecibidos ? 'receptor.rfc' : 'emisor.rfc';

    // Resultados acumulados de todos los sub-tipos para guardarResultados al final
    const allCoinc    = [];
    const allSoloSAT  = [];
    const allSoloERP  = [];
    const allConDiff  = [];
    const allSinUuid  = [];
    const tiposFallidos = [];

    onPaso?.(3);

    for (let ti = 0; ti < tiposADescargar.length; ti++) {
      const tipoActual = tiposADescargar[ti];

      try {
        // ── 1. Descargar paquetes (espera completa antes de continuar) ────────
        const { rows: r, paquetes, totalReportado, esMetadata: modoMeta } = await descargarPorSubtipo({
          rfc, fechaInicio, fechaFin, ejercicio, periodo,
          tipoComprobante: tipoActual, creds, tipoSolicitud: modoFinal,
          folioFiscalUUID, cpSufijo,
        });

        totalPaquetes     += paquetes;
        totalReportadoSAT += (totalReportado ?? 0);
        esMetadata         = modoMeta;

        // Verificar completitud de este sub-tipo
        if (!esMetadata && totalReportado > 0 && r.length < totalReportado * 0.95) {
          incompleta = true;
          logger.warn(
            `[SatSyncJob] ⚠ INCOMPLETO (${tipoActual}): SAT reportó ${totalReportado} pero se descargaron ` +
            `${r.length} (${Math.round((r.length / totalReportado) * 100)}%)`
          );
        }

        if (r.length === 0) {
          logger.info(`[SatSyncJob] RFC ${rfc} (${tipoActual}): sin CFDIs nuevos.`);
        } else {
          // ── 2. Normalizar ──────────────────────────────────────────────────
          const cfdisSATTipo = esMetadata ? r.map(normalizarMetadato) : r.map(normalizarCFDI);

          // ── 3. Comparar con ERP (scoped al tipo actual) ───────────────────
          const tipoFiltroERP = TIPO_LETRA[tipoActual]
            ? { tipoDeComprobante: TIPO_LETRA[tipoActual] }
            : { tipoDeComprobante: { $ne: 'T' } };

          const cfdisERPDocs = await CFDI.find({
            source: 'ERP', isActive: true,
            ...tipoFiltroERP,
            [campoRfc]: rfc.toUpperCase(),
            fecha: { $gte: inicioDelDia, $lte: finDelDia },
          }, 'uuid serie folio fecha emisor receptor subTotal total moneda tipoDeComprobante satStatus impuestos complementoPago').lean();

          const cfdisERPTipo = cfdisERPDocs.map(normalizarCFDI);
          const { coinciden, soloEnSAT, soloEnERP, conDiferencia, sinUuid } = compararArrays(cfdisSATTipo, cfdisERPTipo);

          logger.info(
            `[SatSyncJob] RFC ${rfc} (${tipoActual}): ` +
            `coinciden=${coinciden.length}, soloSAT=${soloEnSAT.length}, soloERP=${soloEnERP.length}, diffs=${conDiferencia.length}`
          );

          // ── 4. Guardar CFDIs nuevos INMEDIATAMENTE en MongoDB ─────────────
          // Se guarda aquí (no al final) para que si el siguiente tipo falla,
          // los CFDIs de este tipo ya estén persistidos y no se pierdan.
          onPaso?.(4);
          if (soloEnSAT.length > 0) {
            const soloEnSATUuids = new Set(soloEnSAT.map(c => c.uuid.toUpperCase()));

            if (esMetadata) {
              const registrosMeta = r.filter(row => soloEnSATUuids.has((row.uuid || '').toUpperCase()));
              if (registrosMeta.length > 0) {
                const _conciliadosPrevios = await _uuidsConciliadosPrevios(registrosMeta.map(row => row.uuid.toUpperCase()));
                await CFDI.bulkWrite(registrosMeta.map(row => ({
                  updateOne: {
                    filter: { uuid: row.uuid.toUpperCase(), source: 'SAT', origenDescarga: { $ne: 'xml' } },
                    update: {
                      $set: {
                        uuid:                 row.uuid.toUpperCase(),
                        source:               'SAT',
                        origenDescarga:       'metadata',
                        satStatus:            row.estado === 'Cancelado' ? 'Cancelado' : 'Vigente',
                        isActive:             true,
                        version:              row.version || '4.0',
                        fecha:                new Date(row.fecha || ''),
                        total:                parseFloat(row.total    || '0') || 0,
                        subTotal:             parseFloat(row.subTotal || '0') || 0,
                        descuento:            parseFloat(row.descuento || '0') || 0,
                        moneda:               row.moneda    || 'MXN',
                        tipoCambio:           parseFloat(row.tipoCambio || '1') || 1,
                        metodoPago:           row.metodoPago || undefined,
                        formaPago:            row.formaPago  || undefined,
                        tipoDeComprobante:    EFECTO_MAP[row.efecto] || row.efecto || '',
                        emisor:               { rfc: (row.rfcEmisor   || '').toUpperCase(), nombre: row.nombreEmisor   || '' },
                        receptor:             { rfc: (row.rfcReceptor || '').toUpperCase(), nombre: row.nombreReceptor || '', usoCFDI: row.usoCFDI || '' },
                        lastComparisonStatus: 'not_in_erp',
                        lastComparisonAt:     new Date(),
                      },
                      // ejercicio/periodo solo en inserción — preserva reclasificaciones previas
                      $setOnInsert: { ejercicio, periodo },
                    },
                    upsert: true,
                  },
                })));
                await _restaurarConciliados(_conciliadosPrevios);
                logger.info(`[SatSyncJob] ✓ Tipo ${tipoActual}: ${registrosMeta.length} registros metadata guardados en MongoDB.`);
              }
            } else {
              const cfdisNuevos = r.filter(c => soloEnSATUuids.has((c.uuid || '').toUpperCase()));
              if (cfdisNuevos.length > 0) {
                const _conciliadosPreviosXml = await _uuidsConciliadosPrevios(cfdisNuevos.map(c => c.uuid.toUpperCase()));
                await CFDI.bulkWrite(cfdisNuevos.map(c => ({
                  updateOne: {
                    filter: { uuid: c.uuid.toUpperCase(), source: 'SAT' },
                    update: {
                      $set: {
                        uuid:                 c.uuid.toUpperCase(),
                        source:               'SAT',
                        origenDescarga:       'xml',
                        satStatus:            'Vigente',
                        isActive:             true,
                        version:              c.version,
                        serie:                c.serie,
                        folio:                c.folio,
                        fecha:                c.fecha,
                        subTotal:             c.subTotal,
                        total:                c.total,
                        moneda:               c.moneda,
                        tipoDeComprobante:    c.tipoDeComprobante,
                        emisor:               c.emisor,
                        receptor:             c.receptor,
                        conceptos:            c.conceptos,
                        impuestos:            c.impuestos,
                        xmlContent:           c.xmlContent,
                        xmlHash:              c.xmlHash,
                        timbreFiscalDigital:  c.timbreFiscalDigital,
                        complementoPago:      c.complementoPago,
                        lastComparisonStatus: 'not_in_erp',
                        lastComparisonAt:     new Date(),
                      },
                      // ejercicio/periodo solo en inserción — preserva reclasificaciones previas
                      $setOnInsert: { ejercicio, periodo },
                    },
                    upsert: true,
                  },
                })));
                await _restaurarConciliados(_conciliadosPreviosXml);
                logger.info(`[SatSyncJob] ✓ Tipo ${tipoActual}: ${cfdisNuevos.length} CFDIs XML guardados en MongoDB.`);

                // Reclasificación inmediata por tipo (solo XML — tiene InformacionGlobal)
                try {
                  const rec = await aplicarReclasificacion({ rfc, ejercicio, source: 'SAT' });
                  if (rec.totalModificados > 0) {
                    logger.info(`[SatSyncJob] Reclasificación (${tipoActual}): ${rec.totalModificados} CFDI(s) corregidos para RFC ${rfc}`);
                  }
                  reclasificacionResultado = rec;
                } catch (reclassErr) {
                  logger.warn(`[SatSyncJob] Reclasificación (${tipoActual}) falló (no crítico): ${reclassErr.message}`);
                }
              }

              // ── Upgrade metadata→xml para coinciden ──────────────────────
              // CFDIs que ya coinciden con ERP pero fueron guardados como metadata
              // en una descarga anterior: promover a XML con los datos completos.
              if (coinciden.length > 0) {
                const coincidenUuids = new Set(coinciden.map(c => (c.uuid || '').toUpperCase()));
                const xmlParaCoinc   = r.filter(c => coincidenUuids.has((c.uuid || '').toUpperCase()));
                if (xmlParaCoinc.length > 0) {
                  const upgResult = await CFDI.bulkWrite(xmlParaCoinc.map(c => ({
                    updateOne: {
                      filter: { uuid: c.uuid.toUpperCase(), source: 'SAT', origenDescarga: 'metadata' },
                      update: {
                        $set: {
                          origenDescarga:      'xml',
                          xmlContent:          c.xmlContent,
                          xmlHash:             c.xmlHash,
                          serie:               c.serie,
                          folio:               c.folio,
                          subTotal:            c.subTotal,
                          total:               c.total,
                          moneda:              c.moneda,
                          conceptos:           c.conceptos,
                          impuestos:           c.impuestos,
                          timbreFiscalDigital: c.timbreFiscalDigital,
                          complementoPago:     c.complementoPago,
                        },
                      },
                    },
                  })));
                  const upgraded = upgResult.modifiedCount ?? 0;
                  if (upgraded > 0) {
                    logger.info(`[SatSyncJob] ✓ Tipo ${tipoActual}: ${upgraded} CFDIs promovidos de metadata→xml.`);
                  }
                }
              }
            }
          }

          // ── 5. Acumular resultados para guardarResultados al final ─────────
          allCoinc.push(...coinciden);
          allSoloSAT.push(...soloEnSAT);
          allSoloERP.push(...soloEnERP);
          allConDiff.push(...conDiferencia);
          allSinUuid.push(...(sinUuid ?? []));
        }

        // ── 6. Fallback automático a Metadata si XML quedó incompleto ─────────
        // El SAT a veces genera solo una fracción de los paquetes XML pero sí
        // entrega el 100% en modo Metadata (mucho más ligero).  Usamos el fallback
        // para recuperar al menos UUID + RFC + total + fecha de los CFDIs faltantes.
        const xmlIncompleto = !modoMeta && totalReportado > 0 && r.length < totalReportado * 0.95;
        if (xmlIncompleto) {
          const limitMeta = await puedeIniciar(rfc, 1);
          if (!limitMeta.puede) {
            logger.warn(`[SatSyncJob] Fallback Metadata (${tipoActual}): sin cuota SAT disponible (${limitMeta.razon}) — omitido.`);
          } else {
            let metaIniciado = false;
            try {
              logger.info(
                `[SatSyncJob] XML incompleto (${r.length}/${totalReportado}) — ` +
                `esperando 2 min antes de fallback a Metadata para ${tipoActual}...`
              );
              await new Promise(res => setTimeout(res, 2 * 60_000));

              await registrarInicio(rfc, 1);
              metaIniciado = true;

              const { rows: rMeta } = await descargarPorSubtipo({
                rfc, fechaInicio, fechaFin, ejercicio, periodo,
                tipoComprobante: tipoActual, creds,
                tipoSolicitud: 'Metadata',
              });

              if (rMeta.length > 0) {
                // Solo guardar los UUIDs que NO llegaron como XML completo
                const yaUuids = new Set(r.map(c => (c.uuid || '').toUpperCase()));
                const soloMetaRows = rMeta.filter(m => !yaUuids.has((m.uuid || '').toUpperCase()));

                if (soloMetaRows.length > 0) {
                  // Obtener UUIDs del ERP para clasificar coincidencias
                  const tipoFiltroMeta = TIPO_LETRA[tipoActual]
                    ? { tipoDeComprobante: TIPO_LETRA[tipoActual] }
                    : { tipoDeComprobante: { $ne: 'T' } };

                  const erpMetaDocs = await CFDI.find({
                    source: 'ERP', isActive: true,
                    ...tipoFiltroMeta,
                    [campoRfc]: rfc.toUpperCase(),
                    fecha: { $gte: inicioDelDia, $lte: finDelDia },
                  }, 'uuid').lean();
                  const erpUuids = new Set(erpMetaDocs.map(c => c.uuid.toUpperCase()));

                  // Guardar en MongoDB — solo actualizar si NO ya existe como XML
                  // (evita sobrescribir subTotal/origenDescarga de CFDIs descargados como XML en sesiones previas)
                  const _conciliadosPreviosMeta2 = await _uuidsConciliadosPrevios(soloMetaRows.map(row => row.uuid.toUpperCase()));
                  await CFDI.bulkWrite(soloMetaRows.map(row => ({
                    updateOne: {
                      filter: { uuid: row.uuid.toUpperCase(), source: 'SAT', origenDescarga: { $ne: 'xml' } },
                      update: {
                        $set: {
                          uuid:                 row.uuid.toUpperCase(),
                          source:               'SAT',
                          origenDescarga:       'metadata',
                          satStatus:            row.estado === 'Cancelado' ? 'Cancelado' : 'Vigente',
                          isActive:             true,
                          version:              row.version || '4.0',
                          fecha:                new Date(row.fecha || ''),
                          total:                parseFloat(row.total    || '0') || 0,
                          subTotal:             parseFloat(row.subTotal || '0') || 0,
                          descuento:            parseFloat(row.descuento || '0') || 0,
                          moneda:               row.moneda    || 'MXN',
                          tipoCambio:           parseFloat(row.tipoCambio || '1') || 1,
                          metodoPago:           row.metodoPago || undefined,
                          formaPago:            row.formaPago  || undefined,
                          tipoDeComprobante:    EFECTO_MAP[row.efecto] || row.efecto || '',
                          emisor:               { rfc: (row.rfcEmisor   || '').toUpperCase(), nombre: row.nombreEmisor   || '' },
                          receptor:             { rfc: (row.rfcReceptor || '').toUpperCase(), nombre: row.nombreReceptor || '', usoCFDI: row.usoCFDI || '' },
                          lastComparisonStatus: erpUuids.has((row.uuid || '').toUpperCase()) ? 'match' : 'not_in_erp',
                          lastComparisonAt:     new Date(),
                        },
                        $setOnInsert: { ejercicio, periodo },
                      },
                      upsert: true,
                    },
                  })));
                  await _restaurarConciliados(_conciliadosPreviosMeta2);

                  // Acumular en resultados de comparación
                  const metaCoinc   = soloMetaRows.filter(m =>  erpUuids.has((m.uuid || '').toUpperCase())).map(normalizarMetadato);
                  const metaSoloSAT = soloMetaRows.filter(m => !erpUuids.has((m.uuid || '').toUpperCase())).map(normalizarMetadato);
                  // Quitar de soloERP los que ahora encontramos en metadata
                  const metaCoincUuids = new Set(metaCoinc.map(c => c.uuid.toUpperCase()));
                  for (let i = allSoloERP.length - 1; i >= 0; i--) {
                    if (metaCoincUuids.has((allSoloERP[i].uuid || '').toUpperCase())) allSoloERP.splice(i, 1);
                  }
                  allCoinc.push(...metaCoinc);
                  allSoloSAT.push(...metaSoloSAT);

                  logger.info(
                    `[SatSyncJob] ✓ Fallback Metadata (${tipoActual}): ${soloMetaRows.length} CFDIs recuperados ` +
                    `(coinciden=${metaCoinc.length}, soloSAT=${metaSoloSAT.length}). ` +
                    `Total recuperado: ${r.length + soloMetaRows.length}/${totalReportado}.`
                  );
                  incompleta = (r.length + soloMetaRows.length) < totalReportado * 0.95;
                  if (incompleta) {
                    logger.warn(
                      `[SatSyncJob] ⚠ Metadata también incompleto (${tipoActual}): ` +
                      `${r.length + soloMetaRows.length}/${totalReportado} CFDIs recuperados. ` +
                      `El checkpoint quedará como 'incompleto' y se reintentará mañana dividiendo el día en 2 mitades.`
                    );
                  }
                } else {
                  logger.info(`[SatSyncJob] Fallback Metadata (${tipoActual}): todos los UUIDs ya estaban descargados como XML.`);
                  incompleta = false;
                }
              } else {
                logger.warn(`[SatSyncJob] Fallback Metadata (${tipoActual}): el SAT no devolvió registros.`);
              }
            } catch (metaErr) {
              logger.warn(`[SatSyncJob] Fallback Metadata (${tipoActual}) falló (no crítico): ${metaErr.message}`);
            } finally {
              if (metaIniciado) registrarFin(rfc);
            }
          }
        }

      } catch (tipoErr) {
        // Un tipo fallido NO aborta los demás — sus datos ya están guardados si llegaron.
        // Los tipos anteriores ya fueron persistidos en MongoDB en el paso 4.
        tiposFallidos.push({ tipo: tipoActual, error: tipoErr.message });
        logger.error(`[SatSyncJob] ⚠ Tipo ${tipoActual} falló (se omite y continúa): ${tipoErr.message}`);
      }

      // ── Cooldown: esperar a que el SAT cierre la solicitud actual ──────────
      // Solo se aplica cuando hay un siguiente tipo; el tipo actual ya completó
      // su ciclo completo (solicitud → verificación → descarga → guardado)
      // antes de que empiece el cooldown.
      // 60s mínimo — el SAT necesita ese tiempo para "cerrar" la solicitud
      // anterior antes de aceptar una nueva del mismo RFC.
      if (ti < tiposADescargar.length - 1) {
        const siguiente = tiposADescargar[ti + 1];
        logger.info(`[SatSyncJob] Tipo ${tipoActual} completado. Cooldown 3min antes de solicitar ${siguiente}...`);
        await new Promise(r => setTimeout(r, 3 * 60_000));
      }
    }

    // ── Post-loop: validación y resultado ────────────────────────────────────
    if (tiposFallidos.length > 0) {
      const msgFallidos =
        `[SatSyncJob] ⚠ ${tiposFallidos.length} tipo(s) fallaron: ` +
        tiposFallidos.map(t => `${t.tipo} (${t.error})`).join(', ');

      if (totalPaquetes === 0 && allCoinc.length === 0 && allSoloSAT.length === 0 && allSoloERP.length === 0) {
        throw new Error(
          `Todos los tipos de comprobante fallaron. ` +
          tiposFallidos.map(t => `${t.tipo}: ${t.error}`).join(' | ')
        );
      }
      logger.warn(msgFallidos + '. Los CFDIs de los tipos exitosos ya fueron guardados en MongoDB.');
    }

    if (totalPaquetes === 0 && allCoinc.length === 0 && allSoloSAT.length === 0) {
      logger.info(`[SatSyncJob] RFC ${rfc}: no hay paquetes que descargar.`);
      await actualizarLog({ estado: 'completado', fin: new Date(), totalSAT: 0, totalERP: 0, coinciden: 0, soloSAT: 0, soloERP: 0, diferencias: 0, paquetes: 0, totalReportadoSAT: 0, incompleta: false });
      return { totalSAT: 0, totalERP: 0, coinciden: 0, soloEnSAT: 0, soloEnERP: 0, conDiferencia: 0, paquetes: 0, totalReportadoSAT: 0, incompleta: false };
    }

    // ── Guardar resultados de comparación en Comparison/Discrepancy/CFDI status
    onPaso?.(5);
    await guardarResultados({ rfc, tipoComprobante, coinciden: allCoinc, soloEnSAT: allSoloSAT, soloEnERP: allSoloERP, conDiferencia: allConDiff, sinUuid: allSinUuid, ejercicio, periodo });

    const totalSAT = allCoinc.length + allSoloSAT.length + allConDiff.length;
    const totalERP = allCoinc.length + allSoloERP.length + allConDiff.length;

    const resultado = {
      totalSAT,
      totalERP,
      coinciden:         allCoinc.length,
      soloEnSAT:         allSoloSAT.length,
      soloEnERP:         allSoloERP.length,
      conDiferencia:     allConDiff.length,
      paquetes:          totalPaquetes,
      totalReportadoSAT,
      incompleta,
      reclasificacion: reclasificacionResultado
        ? {
            totalCorregidos: reclasificacionResultado.totalModificados,
            motivos:         reclasificacionResultado.resumen?.motivoConteo ?? {},
            detalle:         reclasificacionResultado.modificadas ?? [],
          }
        : null,
    };

    await actualizarLog({
      estado:            'completado',
      fin:               new Date(),
      totalSAT:          resultado.totalSAT,
      totalERP:          resultado.totalERP,
      coinciden:         resultado.coinciden,
      soloSAT:           resultado.soloEnSAT,
      soloERP:           resultado.soloEnERP,
      diferencias:       resultado.conDiferencia,
      paquetes:          resultado.paquetes,
      totalReportadoSAT: resultado.totalReportadoSAT,
      incompleta:        resultado.incompleta,
    });

    // Reparar en background CFDIs que tengan XML guardado pero subTotal=0
    setImmediate(() => repararSubtotalesDesdeXml({ source: 'SAT', ejercicio, periodo })
      .catch(err => logger.warn(`[SatSyncJob] repararSubtotalesDesdeXml falló: ${err.message}`))
    );

    return resultado;

  } catch (err) {
    await actualizarLog({ estado: 'error', error: err.message, fin: new Date() });
    throw err;
  }
};

/**
 * Guarda los resultados de la comparación en MongoDB.
 * Recibe ejercicio y periodo explícitamente para garantizar que todos los
 * registros queden vinculados al periodo fiscal correcto (seleccionado por el
 * usuario o derivado de la fecha del job automático).
 */
const guardarResultados = async ({ rfc, tipoComprobante, coinciden, soloEnSAT, soloEnERP, conDiferencia, sinUuid = [], ejercicio, periodo }) => {
  const ahora = new Date();
  const fp    = { ejercicio, periodo };

  // ── Coinciden — bulkWrite (puede ser la mayoría de 30k CFDIs) ─────────────
  if (coinciden.length > 0) {
    await Comparison.bulkWrite(coinciden.map(cfdi => ({
      updateOne: {
        filter: { uuid: cfdi.uuid },
        update: { $set: { uuid: cfdi.uuid, status: 'match', differences: [], totalDifferences: 0, criticalCount: 0, warningCount: 0, comparedAt: ahora, comparedBy: 'scheduled', hasLocalSATCopy: true, ...fp } },
        upsert: true,
      },
    })));
    // ERP: solo actualizar lastComparisonStatus — NO sobreescribir ejercicio/periodo porque
    // el ERP puede tener un periodo reclasificado distinto al rango de la descarga SAT.
    await CFDI.bulkWrite(coinciden.map(cfdi => ({
      updateOne: {
        filter: { uuid: cfdi.uuid, source: 'ERP' },
        update: { $set: { lastComparisonStatus: 'match', lastComparisonAt: ahora } },
      },
    })));
    // SAT/MANUAL: solo status — NO sobreescribir ejercicio/periodo porque
    // aplicarReclasificacion ya corrigió el periodo de facturas globales antes de guardarResultados.
    await CFDI.bulkWrite(coinciden.map(cfdi => ({
      updateOne: {
        filter: { uuid: cfdi.uuid, source: { $in: ['SAT', 'MANUAL'] } },
        update: { $set: { lastComparisonStatus: 'match', lastComparisonAt: ahora } },
      },
    })));
  }

  // ── Solo en SAT — bulkWrite Comparison + Discrepancy ─────────────────────
  if (soloEnSAT.length > 0) {
    await Comparison.bulkWrite(soloEnSAT.map(cfdi => ({
      updateOne: {
        filter: { uuid: cfdi.uuid },
        update: { $set: { uuid: cfdi.uuid, status: 'not_in_erp', differences: [{ field: 'source', erpValue: 'No encontrado', satValue: 'Presente', severity: 'critical' }], totalDifferences: 1, criticalCount: 1, warningCount: 0, comparedAt: ahora, comparedBy: 'scheduled', hasLocalSATCopy: true, ...fp } },
        upsert: true,
      },
    })));
    // comparisonId omitido intencionalmente en bulk — Discrepancy se consulta por { uuid, type }
    await Discrepancy.bulkWrite(soloEnSAT.map(cfdi => ({
      updateOne: {
        filter: { uuid: cfdi.uuid, type: 'MISSING_IN_ERP' },
        update: { $set: { uuid: cfdi.uuid, type: 'MISSING_IN_ERP', severity: 'critical', description: `CFDI ${cfdi.uuid} existe en SAT pero no en ERP`, status: 'open', rfcEmisor: cfdi.rfcEmisor, rfcReceptor: cfdi.rfcReceptor, ...fp } },
        upsert: true,
      },
    })));
    // SAT/MANUAL: solo status — NO sobreescribir ejercicio/periodo (preservar reclasificaciones)
    await CFDI.bulkWrite(soloEnSAT.map(cfdi => ({
      updateOne: {
        filter: { uuid: cfdi.uuid, source: { $in: ['SAT', 'MANUAL'] } },
        update: { $set: { lastComparisonStatus: 'not_in_erp', lastComparisonAt: ahora } },
      },
    })));
  }

  // ── Solo en ERP — bulkWrite Comparison + Discrepancy + CFDI ──────────────
  if (soloEnERP.length > 0) {
    await Comparison.bulkWrite(soloEnERP.map(cfdi => ({
      updateOne: {
        filter: { uuid: cfdi.uuid },
        update: { $set: { uuid: cfdi.uuid, status: 'not_in_sat', differences: [{ field: 'source', erpValue: 'Presente', satValue: 'No encontrado', severity: 'critical' }], totalDifferences: 1, criticalCount: 1, warningCount: 0, comparedAt: ahora, comparedBy: 'scheduled', hasLocalSATCopy: false, ...fp } },
        upsert: true,
      },
    })));
    await Discrepancy.bulkWrite(soloEnERP.map(cfdi => ({
      updateOne: {
        filter: { uuid: cfdi.uuid, type: 'UUID_NOT_FOUND_SAT' },
        update: { $set: { uuid: cfdi.uuid, type: 'UUID_NOT_FOUND_SAT', severity: 'critical', description: `CFDI ${cfdi.uuid} existe en ERP pero no fue encontrado en SAT`, status: 'open', rfcEmisor: cfdi.rfcEmisor, rfcReceptor: cfdi.rfcReceptor, ...fp } },
        upsert: true,
      },
    })));
    await CFDI.bulkWrite(soloEnERP.map(cfdi => ({
      updateOne: {
        // Solo actualizar status — no tocar ejercicio/periodo del ERP
        filter: { uuid: cfdi.uuid, source: 'ERP' },
        update: { $set: { lastComparisonStatus: 'not_in_sat', lastComparisonAt: ahora } },
      },
    })));
  }

  // ── Sin UUID — ERP sin timbre real, no generan discrepancia ──────────────
  if (sinUuid.length > 0) {
    await CFDI.bulkWrite(sinUuid.map(cfdi => ({
      updateOne: {
        filter: { uuid: cfdi.uuid, source: 'ERP' },
        update: { $set: { lastComparisonStatus: 'sin_uuid', lastComparisonAt: ahora } },
      },
    })));
    await Comparison.bulkWrite(sinUuid.map(cfdi => ({
      updateOne: {
        filter: { uuid: cfdi.uuid },
        update: { $set: { uuid: cfdi.uuid, status: 'sin_uuid', differences: [], totalDifferences: 0, criticalCount: 0, warningCount: 0, comparedAt: ahora, comparedBy: 'scheduled', hasLocalSATCopy: false, ...fp } },
        upsert: true,
      },
    })));
  }

  // ── Con diferencias de campo — secuencial (lógica compleja, set pequeño) ──
  for (const { sat, erp, diferencias } of conDiferencia) {
    const camposCriticos = ['rfcEmisor', 'rfcReceptor', 'total'];
    const criticalCount  = diferencias.filter(d => camposCriticos.includes(d.campo)).length;
    const warningCount   = diferencias.length - criticalCount;

    const differences = diferencias.map(d => ({
      field:    d.campo,
      erpValue: String(d.valorERP ?? ''),
      satValue: String(d.valorSAT ?? ''),
      severity: camposCriticos.includes(d.campo) ? 'critical' : 'warning',
    }));

    const comp = await Comparison.findOneAndUpdate(
      { uuid: sat.uuid },
      { $set: { uuid: sat.uuid, status: 'discrepancy', differences, totalDifferences: diferencias.length, criticalCount, warningCount, comparedAt: ahora, comparedBy: 'scheduled', hasLocalSATCopy: true, ...fp } },
      { upsert: true, new: true }
    );

    await Discrepancy.deleteMany({ comparisonId: comp._id });
    await Promise.all(diferencias.map(d => Discrepancy.create({
      comparisonId: comp._id,
      uuid:         sat.uuid,
      type:         mapCampoToType(d.campo),
      severity:     camposCriticos.includes(d.campo) ? 'critical' : 'warning',
      description:  `Campo '${d.campo}': ERP="${d.valorERP}", SAT="${d.valorSAT}"`,
      erpValue:     String(d.valorERP ?? ''),
      satValue:     String(d.valorSAT ?? ''),
      rfcEmisor:    sat.rfcEmisor,
      rfcReceptor:  sat.rfcReceptor,
      status:       'open',
      ...fp,
    })));

    // ERP: solo status, no tocar ejercicio/periodo reclasificado
    await CFDI.findOneAndUpdate(
      { uuid: sat.uuid, source: 'ERP' },
      { $set: { lastComparisonStatus: 'discrepancy', lastComparisonAt: ahora } },
    );
  }
};

const mapCampoToType = (campo) => {
  if (campo === 'total' || campo === 'subtotal') return 'AMOUNT_MISMATCH';
  if (campo.includes('rfc')) return 'RFC_MISMATCH';
  if (campo === 'fecha') return 'DATE_MISMATCH';
  return 'OTHER';
};

// ── Tareas de verificación y descarga masiva (reprogramables dinámicamente) ───

/**
 * Verifica el estado SAT (Vigente/Cancelado) de los CFDIs ERP de un periodo concreto.
 * Útil para meses anteriores donde el estado puede haber cambiado.
 *
 * @param {number} ejercicio
 * @param {number} periodo
 * @returns {Promise<{ verificados: number, errores: number }>}
 */
const ejecutarVerificacionPeriodo = async (ejercicio, periodo) => {
  logger.info(`[VerifJob] Verificando estado SAT para ${ejercicio}/${periodo}...`);

  const cfdis = await CFDI.find({
    source: 'ERP', isActive: true, ejercicio, periodo,
  }, '_id uuid emisor receptor total version sello timbreFiscalDigital tipoDeComprobante').lean();

  logger.info(`[VerifJob] ${cfdis.length} CFDIs ERP para verificar en ${ejercicio}/${periodo}`);

  let verificados = 0, errores = 0;
  for (const cfdi of cfdis) {
    try {
      if (!cfdi.emisor?.rfc || !cfdi.receptor?.rfc) {
        logger.warn(`[VerifJob] CFDI ${cfdi.uuid} sin emisor/receptor — omitido`);
        errores++;
        continue;
      }
      const sello  = cfdi.timbreFiscalDigital?.selloCFD || cfdi.sello || '';
      const result = await verifyCFDIWithSAT(
        cfdi.uuid, cfdi.emisor.rfc, cfdi.receptor.rfc,
        cfdi.total, sello, cfdi.version || '4.0', cfdi.tipoDeComprobante,
      );
      await CFDI.updateMany({ uuid: cfdi.uuid }, { $set: { satStatus: result.state, satLastCheck: new Date() } });
      verificados++;
      await new Promise(r => setTimeout(r, 500)); // respetar rate SAT
    } catch (err) {
      errores++;
      logger.error(`[VerifJob] Error verificando ${cfdi.uuid}: ${err.message}`);
    }
  }
  logger.info(`[VerifJob] Completado: ${verificados} verificados, ${errores} errores`);
  return { verificados, errores, total: cfdis.length };
};

const jobVerificacionSAT = async () => {
  logger.info('[SatSyncJob] Iniciando verificación de estado SAT...');
  const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000);
  const cfdis = await CFDI.find({
    source: 'ERP',
    isActive: true,
    $or: [
      { satStatus: null },
      { satLastCheck: { $lt: yesterday } },
      { satStatus: 'Pendiente' },
    ],
  }, '_id').limit(2000).lean();
  logger.info(`[SatSyncJob] ${cfdis.length} CFDIs por verificar`);
  let success = 0, failed = 0;
  for (const cfdi of cfdis) {
    try {
      await compareCFDI(cfdi._id.toString(), { triggeredBy: null });
      success++;
      await new Promise(r => setTimeout(r, 600));
    } catch (err) {
      failed++;
      logger.error(`[SatSyncJob] Error CFDI ${cfdi._id}:`, err.message);
    }
  }
  logger.info(`[SatSyncJob] Verificación completada: ${success} exitosos, ${failed} fallidos`);
};

// Verifica el estado SAT de CFDIs Recibidos (source: 'SAT', receptor = entidad propia)
// que nunca se han verificado o cuya última verificación tiene más de un día.
// A diferencia de jobVerificacionSAT (Emitidos), aquí no se llama a compareCFDI —
// Recibidos no tiene motor de comparación contra ERP, solo se actualiza satStatus.
const jobVerificacionSATRecibidos = async () => {
  logger.info('[SatSyncJob] Iniciando verificación de estado SAT para Recibidos...');

  const entidadesRfcs = (await entityRepo.findAll()).map(e => e.rfc?.toUpperCase()).filter(Boolean);
  if (entidadesRfcs.length === 0) {
    logger.warn('[SatSyncJob] Sin entidades registradas — se omite verificación de Recibidos.');
    return;
  }

  const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000);
  const cfdis = await CFDI.find({
    source:          'SAT',
    isActive:        { $ne: false },
    'receptor.rfc':  { $in: entidadesRfcs },
    $or: [
      { satStatus: null },
      { satLastCheck: { $lt: yesterday } },
      { satStatus: 'Pendiente' },
    ],
  }, '_id uuid emisor receptor total version sello timbreFiscalDigital tipoDeComprobante').limit(2000).lean();

  logger.info(`[SatSyncJob] ${cfdis.length} CFDIs Recibidos por verificar`);

  let success = 0, failed = 0, omitidos = 0;
  for (const cfdi of cfdis) {
    // RFCs con & no son consultables vía SOAP (el servicio no decodifica %26)
    const rfcConAmpersand = (cfdi.emisor?.rfc || '').includes('&') || (cfdi.receptor?.rfc || '').includes('&');
    if (rfcConAmpersand || !cfdi.emisor?.rfc || !cfdi.receptor?.rfc) {
      omitidos++;
      continue;
    }
    try {
      const sello  = cfdi.timbreFiscalDigital?.selloCFD || cfdi.sello || '';
      const result = await verifyCFDIWithSAT(
        cfdi.uuid, cfdi.emisor.rfc, cfdi.receptor.rfc,
        cfdi.total, sello, cfdi.version || '4.0', cfdi.tipoDeComprobante,
      );
      await CFDI.updateMany({ uuid: cfdi.uuid }, { $set: { satStatus: result.state, satLastCheck: new Date() } });
      success++;
      await new Promise(r => setTimeout(r, 600));
    } catch (err) {
      failed++;
      logger.error(`[SatSyncJob] Error CFDI Recibido ${cfdi.uuid}: ${err.message}`);
    }
  }
  logger.info(`[SatSyncJob] Verificación Recibidos completada: ${success} exitosos, ${failed} fallidos, ${omitidos} omitidos`);
};

const jobDescargaMasiva = async () => {
  try {
    await ejecutarDescargaMasiva({ tipos: ['Emitidos'] });
  } catch (err) {
    logger.error(`[SatSyncJob] Error fatal en descarga masiva: ${err.message}`);
  }
};

// Job separado para Recibidos — corre en un horario distinto al de Emitidos
// (ver reprogramarJobs) para no disparar solicitudes simultáneas del mismo RFC.
// Comparte el mismo limitador de cuota SAT (rateLimiter) que Emitidos, así que
// ambos jobs nunca exceden juntos el máximo diario ni el máximo de activas.
const jobDescargaMasivaRecibidos = async () => {
  try {
    await ejecutarDescargaMasiva({ tipos: ['Recibidos'] });
  } catch (err) {
    logger.error(`[SatSyncJob] Error fatal en descarga masiva de Recibidos: ${err.message}`);
  }
};

/**
 * Convierte "HH:MM" → expresión cron "MM HH * * *"
 * Si el formato es inválido, usa 01:00 como fallback y loguea un error.
 */
const horaACron = (hora) => {
  const match = (hora ?? '').match(/^(\d{1,2}):(\d{2})$/);
  if (!match) {
    logger.error(`[SatSyncJob] Formato de hora inválido: "${hora}" — usando 01:00 como fallback`);
    return '0 1 * * *';
  }
  const hh = parseInt(match[1], 10);
  const mm = parseInt(match[2], 10);
  if (hh < 0 || hh > 23 || mm < 0 || mm > 59) {
    logger.error(`[SatSyncJob] Hora fuera de rango: "${hora}" — usando 01:00 como fallback`);
    return '0 1 * * *';
  }
  return `${mm} ${hh} * * *`;
};

// Instancias actuales de los jobs (para poder destruirlas y recrearlas)
let _jobVerif             = null;
let _jobVerifRecibidos    = null;
let _jobDescarga          = null;
let _jobDescargaRecibidos = null;
let _jobERP               = null;
let _jobComparacion       = null;

const jobDescargaERP = async () => {
  try { await ejecutarDescargaERP(); }
  catch (err) { logger.error(`[ERPSyncJob] Error fatal en descarga ERP: ${err.message}`); }
};

const jobComparacionAuto = async () => {
  try { await ejecutarComparacionAuto(); }
  catch (err) { logger.error(`[CompJobAuto] Error fatal en comparación: ${err.message}`); }
};

const jobVerificacionRecibidos = async () => {
  try { await jobVerificacionSATRecibidos(); }
  catch (err) { logger.error(`[SatSyncJob] Error fatal en verificación de Recibidos: ${err.message}`); }
};

/**
 * Job horario: re-consulta el estado SAT de CFDIs que en el ERP aparecen como
 * "Cancelacion Pendiente", "Cancelado" o "Deshabilitado" pero cuyo satStatus
 * sigue siendo "Vigente".
 *
 * Escenario típico: el cliente cancela la factura en el ERP pero el SAT aún
 * no ha procesado la solicitud (o el campo satStatus nunca se actualizó).
 * Este job garantiza que el satStatus refleje la realidad del SAT.
 */
const ejecutarVerificacionEstadosCriticos = async () => {
  logger.info('[VerifCriticos] Iniciando verificación horaria de estados críticos...');

  const cfdis = await CFDI.find(
    {
      source:    'ERP',
      isActive:  true,
      erpStatus: { $in: ['Cancelacion Pendiente', 'Cancelado', 'Deshabilitado'] },
      satStatus: 'Vigente',
    },
    '_id uuid emisor receptor total version sello timbreFiscalDigital tipoDeComprobante erpStatus',
  ).lean();

  if (cfdis.length === 0) {
    logger.info('[VerifCriticos] Sin CFDIs con estado crítico pendiente de verificar.');
    return;
  }

  logger.info(`[VerifCriticos] ${cfdis.length} CFDI(s) a verificar contra SAT.`);

  let actualizados = 0, sinCambio = 0, omitidos = 0, errores = 0;

  for (const cfdi of cfdis) {
    // RFCs con & no son consultables vía SOAP (el servicio no decodifica %26)
    const rfcConAmpersand = (cfdi.emisor?.rfc || '').includes('&') || (cfdi.receptor?.rfc || '').includes('&');
    if (rfcConAmpersand) {
      omitidos++;
      continue;
    }

    if (!cfdi.emisor?.rfc || !cfdi.receptor?.rfc) {
      logger.warn(`[VerifCriticos] CFDI ${cfdi.uuid} sin emisor/receptor — omitido`);
      omitidos++;
      continue;
    }

    try {
      const sello  = cfdi.timbreFiscalDigital?.selloCFD || cfdi.sello || '';
      const result = await verifyCFDIWithSAT(
        cfdi.uuid,
        cfdi.emisor.rfc,
        cfdi.receptor.rfc,
        cfdi.total,
        sello,
        cfdi.version || '4.0',
        cfdi.tipoDeComprobante,
      );

      const nuevoEstado = result.state; // 'Vigente' | 'Cancelado' | 'No Encontrado' | ...

      if (nuevoEstado !== 'Vigente') {
        // El SAT ya no lo reporta como Vigente — actualizar ambos documentos (ERP y SAT)
        await CFDI.updateMany(
          { uuid: cfdi.uuid },
          { $set: { satStatus: nuevoEstado, satLastCheck: new Date() } },
        );
        logger.info(
          `[VerifCriticos] ${cfdi.uuid} (ERP: ${cfdi.erpStatus}) → SAT: ${nuevoEstado} ` +
          `(antes: Vigente) — actualizado`,
        );
        actualizados++;
      } else {
        // El SAT sigue reportando Vigente; solo actualizar la fecha de chequeo
        await CFDI.updateMany(
          { uuid: cfdi.uuid },
          { $set: { satLastCheck: new Date() } },
        );
        sinCambio++;
      }

      await new Promise(r => setTimeout(r, 500)); // respetar rate del SAT
    } catch (err) {
      errores++;
      logger.error(`[VerifCriticos] Error verificando ${cfdi.uuid}: ${err.message}`);
    }
  }

  logger.info(
    `[VerifCriticos] Completado — actualizados: ${actualizados}, sin cambio: ${sinCambio}, ` +
    `omitidos: ${omitidos}, errores: ${errores}`,
  );
};

// Job horario fijo — no configurable por el usuario (se programa independientemente
// de reprogramarJobs para que no sea destruido al cambiar los horarios nocturnos).
cron.schedule('0 * * * *', async () => {
  try { await ejecutarVerificacionEstadosCriticos(); }
  catch (err) { logger.error(`[VerifCriticos] Error fatal: ${err.message}`); }
}, { timezone: 'America/Mexico_City' });

/**
 * ejecutarVerificacionTimbradosSATCancelado
 * ─────────────────────────────────────────────────────────────────────────────
 * Job que corre cada 5 minutos. Busca CFDIs donde el ERP los sigue marcando
 * como "Timbrado" (activo) pero el SAT ya los reporta como "Cancelado".
 *
 * Para cada CFDI en ese estado consulta el ERP en tiempo real para ver si el
 * contador/ERP ya procesó la cancelación y actualizó su propio estado.
 * Si el ERP ahora reporta un estado distinto a "Timbrado", actualiza erpStatus
 * en MongoDB para que el reporte de conciliación refleje la realidad.
 *
 * Procesa máximo LOTE_MAX CFDIs por ejecución (priorizando los que llevan más
 * tiempo sin checar) para no saturar la API del ERP.
 */
const LOTE_MAX_TIMBRADOS = 20;

const ejecutarVerificacionTimbradosSATCancelado = async () => {
  const { fetchEstadoCfdi } = require('../services/erp.service');

  // Solo ejecutar si la integración ERP está configurada
  let erpHabilitado = false;
  try {
    const AppConfig = require('../models/AppConfig');
    const cfg = await AppConfig.findOne({ key: 'erpApiUrl' }).lean();
    erpHabilitado = !!(cfg?.value);
  } catch { /* si no hay AppConfig, intentamos de todas formas */ erpHabilitado = true; }

  if (!erpHabilitado) return;

  // Buscar CFDIs: ERP=Timbrado pero SAT=Cancelado
  // Ordenar por satLastCheck ASC para procesar primero los que llevan más tiempo sin revisar
  const cfdis = await CFDI.find(
    {
      source:    'ERP',
      isActive:  true,
      erpStatus: 'Timbrado',
      satStatus: 'Cancelado',
    },
    '_id uuid fecha erpStatus satStatus satLastCheck',
  )
    .sort({ satLastCheck: 1 }) // los más antiguos primero
    .limit(LOTE_MAX_TIMBRADOS)
    .lean();

  if (cfdis.length === 0) return; // nada que hacer, no loguear para no saturar

  logger.info(`[VerifTimbrados] ${cfdis.length} CFDI(s) con ERP=Timbrado / SAT=Cancelado — consultando ERP...`);

  let actualizados = 0, sinCambio = 0, noEncontrados = 0, errores = 0;

  for (const cfdi of cfdis) {
    if (!cfdi.uuid || !cfdi.fecha) {
      await CFDI.updateOne({ _id: cfdi._id }, { $set: { satLastCheck: new Date() } });
      continue;
    }

    try {
      const { erpStatus: nuevoEstado, encontrado } = await fetchEstadoCfdi(cfdi.uuid, cfdi.fecha);

      if (!encontrado) {
        // No encontrado en ERP — puede haberse dado de baja; registrar sin cambio
        await CFDI.updateOne({ _id: cfdi._id }, { $set: { satLastCheck: new Date() } });
        noEncontrados++;
      } else if (nuevoEstado && nuevoEstado !== 'Timbrado') {
        // El ERP ya cambió su estado (ej: 'Cancelado', 'Cancelacion Pendiente')
        await CFDI.updateMany(
          { uuid: cfdi.uuid },
          { $set: { erpStatus: nuevoEstado, satLastCheck: new Date() } },
        );
        logger.info(
          `[VerifTimbrados] ${cfdi.uuid} — ERP actualizado: Timbrado → ${nuevoEstado}`,
        );
        actualizados++;
      } else {
        // ERP sigue reportando Timbrado — actualizar fecha de último chequeo
        await CFDI.updateOne({ _id: cfdi._id }, { $set: { satLastCheck: new Date() } });
        sinCambio++;
      }

      await new Promise(r => setTimeout(r, 300)); // respetar rate del ERP
    } catch (err) {
      errores++;
      logger.warn(`[VerifTimbrados] Error consultando ERP para ${cfdi.uuid}: ${err.message}`);
    }
  }

  if (actualizados > 0 || errores > 0) {
    logger.info(
      `[VerifTimbrados] Completado — actualizados: ${actualizados}, sin cambio: ${sinCambio}, ` +
      `no encontrados: ${noEncontrados}, errores: ${errores}`,
    );
  }
};

// Job cada 5 minutos — detecta cancelaciones SAT que el ERP aún no ha procesado
cron.schedule('*/5 * * * *', async () => {
  try { await ejecutarVerificacionTimbradosSATCancelado(); }
  catch (err) { logger.error(`[VerifTimbrados] Error fatal: ${err.message}`); }
}, { timezone: 'America/Mexico_City' });

/**
 * (Re)programa los cuatro jobs con los horarios indicados.
 * Llamado al arrancar la app y cuando el usuario cambia el horario via API.
 */
const reprogramarJobs = ({
  satDescarga = '01:00', satDescargaRecibidos = '22:00',
  erpDescarga = '03:00', erpVerificacion = '02:00', comparacion = '04:00',
  satVerificacionRecibidos = '06:00',
} = {}) => {
  if (_jobVerif)             { _jobVerif.stop();             _jobVerif             = null; }
  if (_jobVerifRecibidos)    { _jobVerifRecibidos.stop();    _jobVerifRecibidos    = null; }
  if (_jobDescarga)          { _jobDescarga.stop();          _jobDescarga          = null; }
  if (_jobDescargaRecibidos) { _jobDescargaRecibidos.stop(); _jobDescargaRecibidos = null; }
  if (_jobERP)               { _jobERP.stop();               _jobERP               = null; }
  if (_jobComparacion)       { _jobComparacion.stop();       _jobComparacion       = null; }

  _jobDescarga          = cron.schedule(horaACron(satDescarga),             jobDescargaMasiva,          { timezone: 'America/Mexico_City' });
  _jobDescargaRecibidos = cron.schedule(horaACron(satDescargaRecibidos),    jobDescargaMasivaRecibidos, { timezone: 'America/Mexico_City' });
  _jobERP               = cron.schedule(horaACron(erpDescarga),             jobDescargaERP,             { timezone: 'America/Mexico_City' });
  _jobVerif             = cron.schedule(horaACron(erpVerificacion),         jobVerificacionSAT,         { timezone: 'America/Mexico_City' });
  _jobVerifRecibidos    = cron.schedule(horaACron(satVerificacionRecibidos), jobVerificacionRecibidos,  { timezone: 'America/Mexico_City' });
  _jobComparacion       = cron.schedule(horaACron(comparacion),             jobComparacionAuto,         { timezone: 'America/Mexico_City' });

  logger.info(
    `[SatSyncJob] Jobs programados — Descarga SAT Emitidos: ${satDescarga} | Descarga SAT Recibidos: ${satDescargaRecibidos} | ` +
    `Descarga ERP: ${erpDescarga} | Verificación: ${erpVerificacion} | Verificación Recibidos: ${satVerificacionRecibidos} | ` +
    `Comparación: ${comparacion} (America/Mexico_City)`
  );
};

// ── Arranque inicial: leer horario guardado en BD o usar defaults ─────────────
(async () => {
  try {
    const AppConfig = require('../models/AppConfig');
    const configs   = await AppConfig.find({ key: { $in: ['satDescarga', 'satDescargaRecibidos', 'erpDescarga', 'erpVerificacion', 'comparacion', 'satVerificacionRecibidos'] } }).lean();
    const map       = Object.fromEntries(configs.map(c => [c.key, c.value]));
    reprogramarJobs({
      satDescarga:              map.satDescarga              ?? '01:00',
      satDescargaRecibidos:     map.satDescargaRecibidos     ?? '22:00',
      erpDescarga:              map.erpDescarga              ?? '03:00',
      erpVerificacion:          map.erpVerificacion          ?? '02:00',
      comparacion:              map.comparacion              ?? '04:00',
      satVerificacionRecibidos: map.satVerificacionRecibidos ?? '06:00',
    });
  } catch (err) {
    logger.error(`[SatSyncJob] No se pudo leer horario de BD al arrancar — usando defaults (01:00/22:00/03:00/02:00/04:00/06:00). Error: ${err.message}`);
    reprogramarJobs();
  }

  // Restaurar programaciones de meses anteriores pendientes en BD
  try {
    const { restaurarProgramados } = require('../controllers/schedule.controller');
    await restaurarProgramados();
  } catch (err) {
    logger.warn(`[Restore] No se pudieron restaurar programaciones: ${err.message}`);
  }
})();

module.exports = { ejecutarDescargaMasiva, ejecutarDescargaERP, ejecutarComparacionAuto, ejecutarVerificacionPeriodo, ejecutarVerificacionEstadosCriticos, procesarDescarga, reprogramarJobs, _uuidsConciliadosPrevios, _restaurarConciliados };
