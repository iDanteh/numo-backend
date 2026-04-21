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
const { solicitar, verificar, descargarPaquete } = require('../sat/download');
const { obtener, eliminar, tieneCredenciales } = require('../sat/credenciales');
const { puedeIniciar, registrarInicio, registrarFin } = require('../sat/rateLimiter');
const { derivarPeriodoDesdeFecha, resolverPeriodo } = require('../services/periodoFiscal.service');
const { logger } = require('../../shared/utils/logger');
const SatDescargaLog = require('../models/SatDescargaLog');

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

/**
 * Ejecuta la descarga, parseo y comparación para un RFC/tipo/rango.
 * @param {number}   ejercicio  — Año fiscal al que vincular los CFDIs descargados.
 * @param {number}   periodo    — Mes fiscal (1–12) al que vincular los CFDIs descargados.
 * @param {Function} [onPaso]   — Callback opcional (paso: number) para reportar progreso al frontend.
 *                                Pasos: 1=Autenticando, 3=Verificando, 4=Descargando, 5=Procesando.
 */
const procesarDescarga = async ({ rfc, fechaInicio, fechaFin, tipoComprobante, creds, ayer, ejercicio, periodo, onPaso, tipo = 'automatica' }) => {
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
    // ── Checkpoint: buscar descarga parcialmente completada ──────────────────
    let checkpoint = await SatJobCheckpoint.findOne({ rfc: rfc.toUpperCase(), fecha, tipoComprobante });

    let idSolicitud, idsPaquetes, totalCfdis;

    if (checkpoint && checkpoint.status === 'descargando' && checkpoint.idsPaquetes?.length > 0) {
      idSolicitud  = checkpoint.idSolicitud;
      idsPaquetes  = checkpoint.idsPaquetes;
      totalCfdis   = idsPaquetes.length;
      const ya = checkpoint.paquetesProcesados?.length ?? 0;
      logger.info(`[SatSyncJob] Reanudando desde checkpoint: ${ya}/${idsPaquetes.length} paquetes ya procesados.`);
      onPaso?.(4);
    } else {
      checkpoint = await SatJobCheckpoint.findOneAndUpdate(
        { rfc: rfc.toUpperCase(), fecha, tipoComprobante },
        { $set: { ejercicio, periodo, status: 'solicitando', idSolicitud: null, idsPaquetes: [], paquetesProcesados: [], error: null, updatedAt: new Date() } },
        { upsert: true, new: true },
      );

      onPaso?.(1);
      idSolicitud = await solicitar({ rfcSolicitante: rfc, fechaInicio, fechaFin, tipoComprobante, creds });
      await SatJobCheckpoint.updateOne({ _id: checkpoint._id }, { $set: { idSolicitud, status: 'verificando', updatedAt: new Date() } });

      onPaso?.(3);
      ({ idsPaquetes, totalCfdis } = await verificar(idSolicitud, rfc, creds));
      logger.info(`[SatSyncJob] RFC ${rfc}: ${totalCfdis} CFDIs en ${idsPaquetes.length} paquete(s)`);

      if (idsPaquetes.length === 0) {
        logger.info(`[SatSyncJob] RFC ${rfc}: no hay paquetes que descargar.`);
        await SatJobCheckpoint.updateOne({ _id: checkpoint._id }, { $set: { status: 'completado', updatedAt: new Date() } });
        await actualizarLog({ estado: 'completado', fin: new Date(), totalSAT: 0, totalERP: 0, coinciden: 0, soloSAT: 0, soloERP: 0, diferencias: 0, paquetes: 0 });
        return { totalSAT: 0, totalERP: 0, coinciden: 0, soloEnSAT: 0, soloEnERP: 0, conDiferencia: 0, paquetes: 0 };
      }

      await SatJobCheckpoint.updateOne(
        { _id: checkpoint._id },
        { $set: { idsPaquetes, status: 'descargando', updatedAt: new Date() } },
      );

      onPaso?.(4);
    }

    const yaProcessados = new Set(checkpoint.paquetesProcesados ?? []);
    const pendientes    = idsPaquetes.filter(id => !yaProcessados.has(id));

    const cfdisSATRaw = [];
    for (const idPaquete of pendientes) {
      let xmls = await descargarPaquete(idPaquete, rfc, creds);
      for (const xml of xmls) {
        try {
          const parsed = await parseCFDI(xml);
          cfdisSATRaw.push(parsed);
        } catch (parseErr) {
          logger.warn(`[SatSyncJob] Error parseando XML de paquete ${idPaquete}: ${parseErr.message}`);
        }
      }
      xmls = null;

      await SatJobCheckpoint.updateOne(
        { _id: checkpoint._id },
        { $addToSet: { paquetesProcesados: idPaquete }, $set: { updatedAt: new Date() } },
      );
      logger.info(`[SatSyncJob] Paquete ${idPaquete} procesado y guardado en checkpoint.`);
    }

    logger.info(`[SatSyncJob] RFC ${rfc}: ${cfdisSATRaw.length} CFDIs parseados del SAT`);

    const cfdisSAT = cfdisSATRaw.map(normalizarCFDI);

    const inicioDelDia = new Date(fechaInicio);
    const finDelDia    = new Date(fechaFin);

    const campoRfc = tipoComprobante === 'Recibidos' ? 'receptor.rfc' : 'emisor.rfc';
    const cfdisERPDocs = await CFDI.find({
      source: 'ERP',
      isActive: true,
      tipoDeComprobante: { $ne: 'T' },
      [campoRfc]: rfc.toUpperCase(),
      fecha: { $gte: inicioDelDia, $lte: finDelDia },
    }, 'uuid serie folio fecha emisor receptor subTotal total moneda tipoDeComprobante satStatus').lean();

    const cfdisERP = cfdisERPDocs.map(normalizarCFDI);

    const { coinciden, soloEnSAT, soloEnERP, conDiferencia } = compararArrays(cfdisSAT, cfdisERP);

    logger.info(`[SatSyncJob] RFC ${rfc} (${tipoComprobante}): coinciden=${coinciden.length}, soloSAT=${soloEnSAT.length}, soloERP=${soloEnERP.length}, diffs=${conDiferencia.length}`);

    onPaso?.(5);
    if (soloEnSAT.length > 0) {
      const soloEnSATUuids = new Set(soloEnSAT.map(c => c.uuid.toUpperCase()));
      const cfdisNuevos = cfdisSATRaw.filter(c => soloEnSATUuids.has((c.uuid || '').toUpperCase()));
      if (cfdisNuevos.length > 0) {
        await CFDI.bulkWrite(cfdisNuevos.map(c => ({
          updateOne: {
            filter: { uuid: c.uuid.toUpperCase(), source: 'SAT' },
            update: { $set: {
              uuid:               c.uuid.toUpperCase(),
              source:             'SAT',
              ejercicio,
              periodo,
              satStatus:          'Vigente',
              isActive:           true,
              version:            c.version,
              serie:              c.serie,
              folio:              c.folio,
              fecha:              c.fecha,
              subTotal:           c.subTotal,
              total:              c.total,
              moneda:             c.moneda,
              tipoDeComprobante:  c.tipoDeComprobante,
              emisor:             c.emisor,
              receptor:           c.receptor,
              conceptos:          c.conceptos,
              impuestos:          c.impuestos,
              xmlContent:         c.xmlContent,
              xmlHash:            c.xmlHash,
              timbreFiscalDigital: c.timbreFiscalDigital,
              complementoPago:    c.complementoPago,
              lastComparisonStatus: 'not_in_erp',
              lastComparisonAt:   new Date(),
            }},
            upsert: true,
          },
        })));
        logger.info(`[SatSyncJob] RFC ${rfc}: ${cfdisNuevos.length} CFDIs SAT guardados en colección`);
      }
    }

    await guardarResultados({ rfc, tipoComprobante, coinciden, soloEnSAT, soloEnERP, conDiferencia, ejercicio, periodo });

    await SatJobCheckpoint.updateOne(
      { rfc: rfc.toUpperCase(), fecha, tipoComprobante },
      { $set: { status: 'completado', updatedAt: new Date() } },
    );

    const resultado = {
      totalSAT:      cfdisSAT.length,
      totalERP:      cfdisERP.length,
      coinciden:     coinciden.length,
      soloEnSAT:     soloEnSAT.length,
      soloEnERP:     soloEnERP.length,
      conDiferencia: conDiferencia.length,
      paquetes:      idsPaquetes.length,
    };

    await actualizarLog({
      estado:      'completado',
      fin:         new Date(),
      totalSAT:    resultado.totalSAT,
      totalERP:    resultado.totalERP,
      coinciden:   resultado.coinciden,
      soloSAT:     resultado.soloEnSAT,
      soloERP:     resultado.soloEnERP,
      diferencias: resultado.conDiferencia,
      paquetes:    resultado.paquetes,
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
