const cron   = require('node-cron');
const config = require('../../config/env');
const CFDI = require('../models/CFDI');
const Comparison = require('../models/Comparison');
const Discrepancy = require('../models/Discrepancy');
const entityRepo = require('../repositories/entity.repository');
const SatJobCheckpoint = require('../models/SatJobCheckpoint');
const { compareCFDI } = require('../services/comparisonEngine');
const { compararArrays } = require('../services/comparisonEngine');
const { parseCFDI, normalizarCFDI } = require('../services/cfdiParser');
const { solicitar, verificar, descargarPaquete, descargarPaqueteMetadata } = require('../sat/download');
const { obtener, eliminar, tieneCredenciales } = require('../sat/credenciales');
const { puedeIniciar, registrarInicio, registrarFin } = require('../sat/rateLimiter');
const { derivarPeriodoDesdeFecha, resolverPeriodo } = require('../services/periodoFiscal.service');
const { logger } = require('../../shared/utils/logger');
const SatDescargaLog = require('../models/SatDescargaLog');
const { aplicarReclasificacion } = require('../services/reclasificacionGlobal.service');

const CRON_HORA = config.sat.cronHora;

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
const ejecutarDescargaMasiva = async () => {
  logger.info('[SatSyncJob] Iniciando descarga masiva nocturna...');

  // Rango: día anterior completo
  const ayer = new Date();
  ayer.setDate(ayer.getDate() - 1);
  const fechaInicio = `${ayer.toISOString().slice(0, 10)}T00:00:00`;
  const fechaFin    = `${ayer.toISOString().slice(0, 10)}T23:59:59`;

  // Periodo fiscal: se deriva de la fecha procesada y se valida contra BD.
  const { ejercicio, periodo } = derivarPeriodoDesdeFecha(ayer);
  try {
    await resolverPeriodo(ejercicio, periodo);
  } catch {
    logger.error(
      `[SatSyncJob] El periodo ${periodo}/${ejercicio} no existe en PeriodoFiscal. ` +
      `Créalo en la sección Ejercicios antes de que corra el job nocturno. Descarga cancelada.`,
    );
    return;
  }
  logger.info(`[SatSyncJob] Periodo fiscal validado: ${ejercicio}/${periodo}`);

  // Entidades con descarga nocturna habilitada (PostgreSQL)
  const entidades = await entityRepo.findWithAutoSync();

  if (entidades.length === 0) {
    logger.info('[SatSyncJob] No hay entidades con descarga nocturna habilitada.');
    return;
  }

  logger.info(`[SatSyncJob] Procesando ${entidades.length} entidad(es)...`);

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

      // ── 2. Descargar emitidos y/o recibidos ─────────────────────────────
      const tipos = [];
      if (entidad.syncConfig?.syncEmitidos !== false) tipos.push('Emitidos');
      if (entidad.syncConfig?.syncRecibidos) tipos.push('Recibidos');
      if (tipos.length === 0) tipos.push('Emitidos');

      for (const tipoComprobante of tipos) {
        const limitCheck = await puedeIniciar(rfc);
        if (!limitCheck.puede) {
          logger.warn(`[SatSyncJob] RFC ${rfc} (${tipoComprobante}): descarga nocturna bloqueada — ${limitCheck.razon}`);
          continue;
        }
        await registrarInicio(rfc);
        try {
          await procesarDescarga({ rfc, fechaInicio, fechaFin, tipoComprobante, creds, ayer, ejercicio, periodo });
        } catch (descErr) {
          // Marcar checkpoint con error para diagnóstico
          const fecha = fechaInicio.slice(0, 10);
          await SatJobCheckpoint.findOneAndUpdate(
            { rfc: rfc.toUpperCase(), fecha, tipoComprobante },
            { $set: { status: 'error', error: descErr.message, updatedAt: new Date() } },
          ).catch(() => {});
          throw descErr;
        } finally {
          registrarFin(rfc);
        }
      }

      // Actualizar fecha de última sincronización
      await Entity.findOneAndUpdate({ rfc }, { $set: { 'syncConfig.lastSync': new Date() } });

    } catch (err) {
      logger.error(`[SatSyncJob] Error procesando RFC ${rfc}: ${err.message}`);
    } finally {
      // ── 5. Eliminar credenciales siempre ──────────────────────────────
      try {
        await eliminar(rfc);
      } catch (delErr) {
        logger.error(`[SatSyncJob] Error eliminando credenciales de ${rfc}: ${delErr.message}`);
      }
    }
  }

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
  return {
    uuid:              (row.uuid || '').toUpperCase().trim(),
    rfcEmisor:         (row.rfcEmisor || '').toUpperCase().trim(),
    rfcReceptor:       (row.rfcReceptor || '').toUpperCase().trim(),
    total:             parseFloat(row.total || '0') || 0,
    subtotal:          0, // metadata no incluye subtotal — campo en minúsculas para detectarDiferencias
    fecha:             new Date(row.fecha || ''),
    tipoDeComprobante: tipo,
    tipoComprobante:   tipo, // alias en minúsculas requerido por detectarDiferencias / normalizarCFDI
    moneda:            'MXN',
    satStatus:         row.estado || 'Vigente',
    estatus:           row.estado || 'Vigente',
    // Campos usados al guardar en MongoDB
    emisor:            { rfc: (row.rfcEmisor || '').toUpperCase().trim(), nombre: row.nombreEmisor || '' },
    receptor:          { rfc: (row.rfcReceptor || '').toUpperCase().trim(), nombre: row.nombreReceptor || '' },
    subTotal:          0,
    serie:             '',
    folio:             '',
    version:           '4.0',
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
const descargarPorSubtipo = async ({ rfc, fechaInicio, fechaFin, ejercicio, periodo, tipoComprobante, creds, tipoSolicitud = 'CFDI' }) => {
  const esMetadata  = tipoSolicitud === 'Metadata';
  // Incluir modo en la clave del checkpoint para no mezclar XML con metadata
  const cpTipo = esMetadata ? `${tipoComprobante}_Metadata` : tipoComprobante;
  const fecha  = fechaInicio.slice(0, 10);
  let checkpoint = await SatJobCheckpoint.findOne({ rfc: rfc.toUpperCase(), fecha, tipoComprobante: cpTipo });

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

  if (checkpointVerificando) {
    logger.warn(`[SatSyncJob] Checkpoint ${tipoComprobante} quedó en 'verificando' — reanudando verificación de solicitud ${checkpoint.idSolicitud}`);
    let totalReportadoSAT = 0;
    ({ idsPaquetes, totalCfdis: totalReportadoSAT } = await verificar(checkpoint.idSolicitud, rfc, creds));
    logger.info(`[SatSyncJob] ${tipoComprobante}: ${idsPaquetes.length} paquete(s), ${totalReportadoSAT} CFDIs reportados por SAT`);
    if (idsPaquetes.length === 0) {
      await SatJobCheckpoint.updateOne({ _id: checkpoint._id }, { $set: { status: 'completado', updatedAt: new Date() } });
      return { rows: [], paquetes: 0, totalReportado: totalReportadoSAT, esMetadata };
    }
    await SatJobCheckpoint.updateOne({ _id: checkpoint._id }, { $set: { idsPaquetes, totalReportadoSAT, status: 'descargando', updatedAt: new Date() } });
    checkpoint.totalReportadoSAT = totalReportadoSAT;
    idSolicitud = checkpoint.idSolicitud;
  } else if (checkpointVigente) {
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
        { $set: { ejercicio, periodo, status: 'solicitando', idSolicitud: null, idsPaquetes: [], paquetesProcesados: [], error: null, updatedAt: new Date() } },
        { upsert: true, new: true },
      );
      idSolicitud = await solicitar({ rfcSolicitante: rfc, fechaInicio, fechaFin, tipoComprobante, tipoSolicitud, creds });
      await SatJobCheckpoint.updateOne({ _id: checkpoint._id }, { $set: { idSolicitud, status: 'verificando', updatedAt: new Date() } });

      try {
        ({ idsPaquetes, totalCfdis: totalReportadoSATLocal } = await verificar(idSolicitud, rfc, creds));
        break; // solicitud aceptada y terminada — salir del loop de reintentos
      } catch (rechazadaErr) {
        if (rechazadaErr.message.startsWith('SAT_RECHAZADA') && intento < MAX_REINTENTOS_RECHAZADA) {
          logger.warn(
            `[SatSyncJob] ${tipoComprobante} rechazada por SAT (intento ${intento}/${MAX_REINTENTOS_RECHAZADA}) — ` +
            `esperando ${ESPERA_RECHAZADA_MS / 60000} min antes de reintentar...`
          );
          await new Promise(r => setTimeout(r, ESPERA_RECHAZADA_MS));
          continue;
        }
        throw rechazadaErr; // sin más reintentos o error distinto a Rechazada
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
    }
  }
  // ── Re-verificar si hay más paquetes disponibles ─────────────────────────
  // El SAT a veces retorna estado=3 (Terminada) con paquetes parciales y agrega
  // los restantes poco después. Si la descarga está incompleta y no hubo paquetes
  // fallidos, se re-verifica la solicitud hasta 2 veces esperando 60s entre intentos.
  const MAX_REVERIF = 2;
  if (!esMetadata && paquetesFallidos === 0 && totalReportadoSAT > 0 && rows.length < totalReportadoSAT * 0.95) {
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

      const yaProcessados2 = new Set(checkpoint.paquetesProcesados ?? []);
      const nuevos = paquetesActualizados.filter(id => !yaProcessados2.has(id));

      if (nuevos.length === 0) {
        logger.info(`[SatSyncJob] Re-verificación ${rv}: el SAT no reportó paquetes adicionales.`);
        break;
      }

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
        }
      }

      if (rows.length >= totalReportadoSAT * 0.95) {
        logger.info(`[SatSyncJob] Re-verificación ${rv}: descarga completa tras encontrar paquetes adicionales.`);
        break;
      }
    }
  }

  // Marcar checkpoint según resultado final
  if (paquetesFallidos > 0) {
    logger.warn(`[SatSyncJob] ${paquetesFallidos} de ${pendientes.length} paquetes fallaron — la descarga puede estar incompleta.`);
    await SatJobCheckpoint.updateOne({ _id: checkpoint._id }, { $set: { status: 'descargando', updatedAt: new Date() } });
  } else {
    await SatJobCheckpoint.updateOne({ _id: checkpoint._id }, { $set: { status: 'completado', updatedAt: new Date() } });
  }

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
const procesarDescarga = async ({ rfc, fechaInicio, fechaFin, tipoComprobante, tipoSolicitud, creds, ayer, ejercicio, periodo, onPaso, tipo = 'automatica' }) => {
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

    // En modo XML + Emitidos: dividir por tipo para cubrir todos los comprobantes.
    const TIPOS_SPLIT_EMITIDOS = ['Ingresos', 'Egresos', 'Pagos', 'Nomina', 'Traslados'];
    const tiposADescargar = (modoFinal === 'CFDI' && tipoComprobante === 'Emitidos')
      ? TIPOS_SPLIT_EMITIDOS
      : [tipoComprobante];

    onPaso?.(3);
    const rows = [];
    const tiposFallidos = [];
    for (let ti = 0; ti < tiposADescargar.length; ti++) {
      const tipo = tiposADescargar[ti];
      try {
        const { rows: r, paquetes, totalReportado, esMetadata: modoMeta } = await descargarPorSubtipo({
          rfc, fechaInicio, fechaFin, ejercicio, periodo, tipoComprobante: tipo, creds, tipoSolicitud: modoFinal,
        });
        rows.push(...r);
        totalPaquetes     += paquetes;
        totalReportadoSAT += (totalReportado ?? 0);
        esMetadata         = modoMeta;
      } catch (tipoErr) {
        // Un tipo fallido NO aborta los demás — se acumula el error y se continúa.
        // Los CFDIs de los tipos exitosos se guardarán igualmente al final.
        tiposFallidos.push({ tipo, error: tipoErr.message });
        logger.error(`[SatSyncJob] ⚠ Tipo ${tipo} falló (se omite y continúa): ${tipoErr.message}`);
      }

      // Cooldown entre tipos: el SAT puede rechazar la siguiente solicitud si la anterior
      // aún no está "cerrada" en su backend. 30 s es suficiente en la mayoría de casos;
      // si la solicitud llega a ser rechazada de todos modos, descargarPorSubtipo reintenta.
      if (ti < tiposADescargar.length - 1) {
        logger.info(`[SatSyncJob] Cooldown 30s antes de solicitar el siguiente tipo (${tiposADescargar[ti + 1]})...`);
        await new Promise(r => setTimeout(r, 30_000));
      }
    }

    if (tiposFallidos.length > 0) {
      const msgFallidos =
        `[SatSyncJob] ⚠ ${tiposFallidos.length} tipo(s) fallaron: ` +
        tiposFallidos.map(t => `${t.tipo} (${t.error})`).join(', ');

      if (rows.length === 0 && totalPaquetes === 0) {
        // Todos los tipos fallaron — no hay nada que guardar, propagar como error.
        throw new Error(
          `Todos los tipos de comprobante fallaron. ` +
          tiposFallidos.map(t => `${t.tipo}: ${t.error}`).join(' | ')
        );
      }
      logger.warn(msgFallidos + `. Los CFDIs de los tipos exitosos sí se guardarán.`);
    }

    onPaso?.(4);

    if (rows.length === 0 && totalPaquetes === 0) {
      logger.info(`[SatSyncJob] RFC ${rfc}: no hay paquetes que descargar.`);
      await actualizarLog({ estado: 'completado', fin: new Date(), totalSAT: 0, totalERP: 0, coinciden: 0, soloSAT: 0, soloERP: 0, diferencias: 0, paquetes: 0, totalReportadoSAT: 0, incompleta: false });
      return { totalSAT: 0, totalERP: 0, coinciden: 0, soloEnSAT: 0, soloEnERP: 0, conDiferencia: 0, paquetes: 0, totalReportadoSAT: 0, incompleta: false };
    }

    // En modo XML (CFDI), verificar si la descarga fue incompleta
    const incompleta = !esMetadata && totalReportadoSAT > 0 && rows.length < totalReportadoSAT * 0.95;
    if (incompleta) {
      logger.warn(
        `[SatSyncJob] ⚠ DESCARGA INCOMPLETA RFC ${rfc}: SAT reportó ${totalReportadoSAT} CFDIs ` +
        `pero se descargaron ${rows.length} (${Math.round((rows.length / totalReportadoSAT) * 100)}%). ` +
        `Usa modo Metadata para rangos >5 días.`
      );
    }

    logger.info(`[SatSyncJob] RFC ${rfc}: ${rows.length} registros del SAT (modo=${modoFinal}, ${totalPaquetes} paquete(s), reportados=${totalReportadoSAT})`);

    // Normalizar según modo: metadata usa normalizarMetadato, XML usa normalizarCFDI
    const cfdisSAT = esMetadata ? rows.map(normalizarMetadato) : rows.map(normalizarCFDI);

    const inicioDelDia = new Date(fechaInicio);
    const finDelDia    = new Date(fechaFin);

    // Subtipos de recibidos usan receptor.rfc para buscar en el ERP
    const esRecibidos = tipoComprobante === 'Recibidos' || tipoComprobante.startsWith('Recibidos');
    const campoRfc = esRecibidos ? 'receptor.rfc' : 'emisor.rfc';

    // Mapa de tipo de comprobante (seleccionado en UI) a letra SAT — incluye subtipos de recibidos
    const TIPO_LETRA = {
      Ingresos: 'I', Egresos: 'E', Traslados: 'T', Nomina: 'N', Pagos: 'P',
      RecibidosIngresos: 'I', RecibidosEgresos: 'E', RecibidosTraslados: 'T', RecibidosNomina: 'N', RecibidosPagos: 'P',
    };
    // Si se descargó un tipo específico, filtrar el ERP por ese tipo para no comparar
    // CFDIs de otros tipos que no están en la descarga SAT (evita falsos soloEnERP).
    // Para 'Emitidos' / 'Recibidos' (todos los tipos) se excluyen solo traslados (sin impacto fiscal).
    const tipoFiltroERP = TIPO_LETRA[tipoComprobante]
      ? { tipoDeComprobante: TIPO_LETRA[tipoComprobante] }
      : { tipoDeComprobante: { $ne: 'T' } };

    const cfdisERPDocs = await CFDI.find({
      source: 'ERP',
      isActive: true,
      ...tipoFiltroERP,
      [campoRfc]: rfc.toUpperCase(),
      fecha: { $gte: inicioDelDia, $lte: finDelDia },
    }, 'uuid serie folio fecha emisor receptor subTotal total moneda tipoDeComprobante satStatus').lean();

    const cfdisERP = cfdisERPDocs.map(normalizarCFDI);

    const { coinciden, soloEnSAT, soloEnERP, conDiferencia } = compararArrays(cfdisSAT, cfdisERP);

    logger.info(`[SatSyncJob] RFC ${rfc} (${tipoComprobante}): coinciden=${coinciden.length}, soloSAT=${soloEnSAT.length}, soloERP=${soloEnERP.length}, diffs=${conDiferencia.length}`);

    onPaso?.(5);
    let reclasificacionResultado = null;
    if (soloEnSAT.length > 0) {
      const soloEnSATUuids = new Set(soloEnSAT.map(c => c.uuid.toUpperCase()));

      if (esMetadata) {
        // Modo metadata: guardar con campos básicos (sin xmlContent)
        const registrosMeta = rows.filter(r => soloEnSATUuids.has((r.uuid || '').toUpperCase()));
        if (registrosMeta.length > 0) {
          await CFDI.bulkWrite(registrosMeta.map(r => ({
            updateOne: {
              filter: { uuid: r.uuid.toUpperCase(), source: 'SAT' },
              update: { $set: {
                uuid:                 r.uuid.toUpperCase(),
                source:               'SAT',
                ejercicio,
                periodo,
                satStatus:            r.estado === 'Cancelado' ? 'Cancelado' : 'Vigente',
                isActive:             true,
                version:              '4.0',
                fecha:                new Date(r.fecha || ''),
                total:                parseFloat(r.total || '0') || 0,
                subTotal:             0,
                moneda:               'MXN',
                tipoDeComprobante:    EFECTO_MAP[r.efecto] || r.efecto || '',
                emisor:               { rfc: (r.rfcEmisor || '').toUpperCase(), nombre: r.nombreEmisor || '' },
                receptor:             { rfc: (r.rfcReceptor || '').toUpperCase(), nombre: r.nombreReceptor || '' },
                lastComparisonStatus: 'not_in_erp',
                lastComparisonAt:     new Date(),
              }},
              upsert: true,
            },
          })));
          logger.info(`[SatSyncJob] RFC ${rfc}: ${registrosMeta.length} registros SAT (metadata) guardados`);
        }
      } else {
        // Modo XML: guardar con xmlContent completo
        const cfdisNuevos = rows.filter(c => soloEnSATUuids.has((c.uuid || '').toUpperCase()));
        if (cfdisNuevos.length > 0) {
          await CFDI.bulkWrite(cfdisNuevos.map(c => ({
            updateOne: {
              filter: { uuid: c.uuid.toUpperCase(), source: 'SAT' },
              update: { $set: {
                uuid:                 c.uuid.toUpperCase(),
                source:               'SAT',
                ejercicio,
                periodo,
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
              }},
              upsert: true,
            },
          })));
          logger.info(`[SatSyncJob] RFC ${rfc}: ${cfdisNuevos.length} CFDIs SAT (XML) guardados`);

          // Reclasificar facturas globales: corrige periodo/ejercicio según fecha de emisión,
          // no fecha de timbrado. Solo aplica a CFDIs con cfdi:InformacionGlobal.
          try {
            reclasificacionResultado = await aplicarReclasificacion({ rfc, source: 'SAT' });
            if (reclasificacionResultado.totalModificados > 0) {
              logger.info(`[SatSyncJob] Reclasificación global SAT: ${reclasificacionResultado.totalModificados} CFDI(s) corregidos para RFC ${rfc}`);
            }
          } catch (reclassErr) {
            logger.warn(`[SatSyncJob] Reclasificación global SAT falló (no crítico): ${reclassErr.message}`);
          }
        }
      }
    }

    await guardarResultados({ rfc, tipoComprobante, coinciden, soloEnSAT, soloEnERP, conDiferencia, ejercicio, periodo });

    const resultado = {
      totalSAT:          cfdisSAT.length,
      totalERP:          cfdisERP.length,
      coinciden:         coinciden.length,
      soloEnSAT:         soloEnSAT.length,
      soloEnERP:         soloEnERP.length,
      conDiferencia:     conDiferencia.length,
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
const guardarResultados = async ({ rfc, tipoComprobante, coinciden, soloEnSAT, soloEnERP, conDiferencia, ejercicio, periodo }) => {
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
    await CFDI.bulkWrite(coinciden.map(cfdi => ({
      updateOne: {
        filter: { uuid: cfdi.uuid },
        update: { $set: { lastComparisonStatus: 'match', lastComparisonAt: ahora, ...fp } },
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
    await CFDI.bulkWrite(soloEnSAT.map(cfdi => ({
      updateOne: {
        filter: { uuid: cfdi.uuid, source: { $in: ['SAT', 'MANUAL'] } },
        update: { $set: { lastComparisonStatus: 'not_in_erp', lastComparisonAt: ahora, ...fp } },
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
        filter: { uuid: cfdi.uuid },
        update: { $set: { lastComparisonStatus: 'not_in_sat', lastComparisonAt: ahora, ...fp } },
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

    await CFDI.findOneAndUpdate(
      { uuid: sat.uuid },
      { $set: { lastComparisonStatus: 'discrepancy', lastComparisonAt: ahora, ...fp } },
    );
  }
};

const mapCampoToType = (campo) => {
  if (campo === 'total' || campo === 'subtotal') return 'AMOUNT_MISMATCH';
  if (campo.includes('rfc')) return 'RFC_MISMATCH';
  if (campo === 'fecha') return 'DATE_MISMATCH';
  return 'OTHER';
};

// ── Job anterior: verificación de estado SAT para CFDIs del ERP ──────────────
// Se mantiene con su horario original (2 AM) para no romper funcionalidad existente.
cron.schedule('0 2 * * *', async () => {
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
  }, '_id').limit(500).lean();

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

  logger.info(`[SatSyncJob] Completado: ${success} exitosos, ${failed} fallidos`);
}, {
  timezone: 'America/Mexico_City',
});

// ── Job de Descarga Masiva: 1:00 AM hora de México ───────────────────────────
cron.schedule(CRON_HORA, async () => {
  try {
    await ejecutarDescargaMasiva();
  } catch (err) {
    logger.error(`[SatSyncJob] Error fatal en descarga masiva: ${err.message}`);
  }
}, {
  timezone: 'America/Mexico_City',
});

logger.info(`[SatSyncJob] Jobs registrados: verificación SAT 2AM, descarga masiva ${CRON_HORA} (America/Mexico_City)`);

module.exports = { ejecutarDescargaMasiva, procesarDescarga };
