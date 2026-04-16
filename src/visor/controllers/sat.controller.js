const { validationResult } = require('express-validator');
const { verifyCFDIWithSAT } = require('../services/satVerification');
const { procesarDescarga } = require('../jobs/satSyncJob');
const { resolverPeriodo } = require('../services/periodoFiscal.service');
const { guardar, tieneCredenciales, obtener, eliminar, limpiarBuffers } = require('../sat/credenciales');
const { puedeIniciar, registrarInicio, registrarFin, getEstado } = require('../sat/rateLimiter');
const Comparison = require('../models/Comparison');
const CFDI = require('../models/CFDI');
const { asyncHandler } = require('../middleware/errorHandler');
const { RFC_REGEX } = require('../utils/validators');
const { logger } = require('../utils/logger');

/** Estado en memoria de jobs de descarga manual (jobId → estado). */
const jobsManales = new Map();

// ─────────────────────────────────────────────────────────────────────────────
// Helpers de fechas
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Normaliza cualquier string de fecha (YYYY-MM-DD, ISO 8601, etc.)
 * y devuelve solo la parte YYYY-MM-DD.
 * Lanza un Error con mensaje descriptivo si el valor no es una fecha válida.
 */
const normalizarFecha = (valor, nombre) => {
  if (!valor || typeof valor !== 'string') {
    throw new Error(`${nombre} es requerida y debe ser un string.`);
  }

  const limpio = valor.trim();

  let year, month, day;

  // 1️⃣ Formato YYYY-MM-DD o ISO
  const isoMatch = limpio.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (isoMatch) {
    year  = isoMatch[1];
    month = isoMatch[2];
    day   = isoMatch[3];
  }

  // 2️⃣ Formato DD/MM/YYYY
  const mxMatch = limpio.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (mxMatch) {
    day   = mxMatch[1];
    month = mxMatch[2];
    year  = mxMatch[3];
  }

  if (!year || !month || !day) {
    throw new Error(
      `${nombre} tiene un formato inválido ("${valor}"). Use YYYY-MM-DD, ISO 8601 o DD/MM/YYYY.`,
    );
  }

  // Validar que sea fecha real
  const fecha = new Date(`${year}-${month}-${day}T00:00:00`);
  if (isNaN(fecha.getTime())) {
    throw new Error(`${nombre} no es una fecha real ("${valor}").`);
  }

  // Validación adicional para evitar cosas como 31/02/2026
  if (
    fecha.getUTCFullYear().toString() !== year ||
    (fecha.getUTCMonth() + 1).toString().padStart(2, '0') !== month ||
    fecha.getUTCDate().toString().padStart(2, '0') !== day
  ) {
    throw new Error(`${nombre} no es una fecha válida ("${valor}").`);
  }

  return `${year}-${month}-${day}`;
};

/**
 * Valida que el rango de fechas sea coherente y no supere 1 año.
 * @param {string} fi  — "YYYY-MM-DDT00:00:00"
 * @param {string} ff  — "YYYY-MM-DDT23:59:59"
 */
const validarRango = (fi, ff) => {
  const inicio     = new Date(fi);
  const fin        = new Date(ff);
  const hoy        = new Date();
  const UN_ANIO_MS = 365 * 24 * 60 * 60 * 1000;

  if (fin < inicio) {
    throw new Error('fechaFin debe ser mayor o igual a fechaInicio.');
  }
  if (fin > hoy) {
    throw new Error('fechaFin no puede ser una fecha futura.');
  }
  if (fin - inicio > UN_ANIO_MS) {
    throw new Error('El rango máximo permitido por el SAT es de 1 año por solicitud.');
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// Controllers
// ─────────────────────────────────────────────────────────────────────────────

/**
 * POST /api/sat/verify
 */
const verify = asyncHandler(async (req, res) => {
  const errors = validationResult(req);
  if (!errors.isEmpty()) return res.status(400).json({ errors: errors.array() });

  const { uuid, rfcEmisor, rfcReceptor, total, sello = '', version = '4.0' } = req.body;
  const result = await verifyCFDIWithSAT(uuid, rfcEmisor, rfcReceptor, parseFloat(total), sello, version);

  await CFDI.findOneAndUpdate(
    { uuid: uuid.toUpperCase() },
    { $set: { satStatus: result.state, satLastCheck: new Date() } },
  );

  res.json(result);
});

/**
 * POST /api/sat/verify-batch
 */
const verifyBatch = asyncHandler(async (req, res) => {
  const { uuids } = req.body;
  if (!Array.isArray(uuids) || uuids.length === 0)
    return res.status(400).json({ error: 'Se requiere un array de UUIDs' });
  if (uuids.length > 100)
    return res.status(400).json({ error: 'Máximo 100 UUIDs por lote' });

  const cfdis   = await CFDI.find({ uuid: { $in: uuids.map(u => u.toUpperCase()) } }).lean();
  const cfdiMap = Object.fromEntries(cfdis.map(c => [c.uuid, c]));

  res.status(202).json({
    message:  'Verificación en lote iniciada',
    total:    uuids.length,
    found:    cfdis.length,
    notFound: uuids.length - cfdis.length,
  });

  (async () => {
    for (const uuid of uuids) {
      const cfdi = cfdiMap[uuid.toUpperCase()];
      if (!cfdi) continue;
      try {
        const sello = cfdi.timbreFiscalDigital?.selloCFD || cfdi.sello || '';
        const result = await verifyCFDIWithSAT(cfdi.uuid, cfdi.emisor.rfc, cfdi.receptor.rfc, cfdi.total, sello, cfdi.version || '4.0', cfdi.tipoDeComprobante);
        await CFDI.findOneAndUpdate({ uuid: cfdi.uuid }, { $set: { satStatus: result.state, satLastCheck: new Date() } });
        await new Promise(r => setTimeout(r, 500));
      } catch (err) {
        logger.error(`Error verificando ${uuid}:`, err.message);
      }
    }
  })();
});

/**
 * GET /api/sat/status/:uuid
 */
const getStatus = asyncHandler(async (req, res) => {
  const uuid = req.params.uuid.toUpperCase();
  const cfdi = await CFDI.findOne({ uuid }, 'uuid satStatus satLastCheck emisor receptor total version sello timbreFiscalDigital tipoDeComprobante');
  if (!cfdi) return res.status(404).json({ error: 'CFDI no encontrado en base local' });

  const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
  if (cfdi.satLastCheck && cfdi.satLastCheck > oneHourAgo) {
    return res.json({ uuid, satStatus: cfdi.satStatus, satLastCheck: cfdi.satLastCheck, cached: true });
  }

  const sello = cfdi.timbreFiscalDigital?.selloCFD || cfdi.sello || '';
  const result = await verifyCFDIWithSAT(cfdi.uuid, cfdi.emisor.rfc, cfdi.receptor.rfc, cfdi.total, sello, cfdi.version || '4.0', cfdi.tipoDeComprobante);
  await CFDI.findOneAndUpdate({ uuid }, { $set: { satStatus: result.state, satLastCheck: new Date() } });

  res.json({ uuid, satStatus: result.state, satLastCheck: new Date(), cached: false });
});

/**
 * POST /api/sat/credenciales
 */
const registerCredentials = asyncHandler(async (req, res) => {
  const errors = validationResult(req);
  if (!errors.isEmpty()) return res.status(400).json({ errors: errors.array() });

  const { rfc, password } = req.body;
  const cerFile = req.files?.cer?.[0];
  const keyFile = req.files?.key?.[0];

  if (!cerFile) return res.status(400).json({ error: 'Archivo .cer requerido' });
  if (!keyFile) return res.status(400).json({ error: 'Archivo .key requerido' });

  if (!RFC_REGEX.test(rfc.trim()))
    return res.status(400).json({ error: 'Formato de RFC inválido' });

  await guardar(rfc, {
    cerB64:   cerFile.buffer.toString('base64'),
    keyB64:   keyFile.buffer.toString('base64'),
    password,
  });

  // Limpiar buffers de multer de la memoria
  cerFile.buffer.fill(0);
  keyFile.buffer.fill(0);

  const { ttlSegundos } = await tieneCredenciales(rfc);
  logger.info(`[SAT] Credenciales registradas para RFC ${rfc.toUpperCase()}`);

  res.status(201).json({
    message:   'Credenciales registradas correctamente',
    rfc:       rfc.toUpperCase(),
    ttlSegundos,
    expiraEn:  new Date(Date.now() + ttlSegundos * 1000).toISOString(),
    aviso:     'Los archivos nunca se almacenan en el servidor. Solo se guarda la versión cifrada.',
  });
});

/**
 * GET /api/sat/credenciales/estado/:rfc
 */
const getCredentialStatus = asyncHandler(async (req, res) => {
  const rfc    = req.params.rfc.toUpperCase().trim();
  const estado = await tieneCredenciales(rfc);

  res.json({
    rfc,
    tieneCredenciales: estado.tiene,
    ttlSegundos:       estado.ttlSegundos,
    expiraEn:          estado.ttlSegundos
      ? new Date(Date.now() + estado.ttlSegundos * 1000).toISOString()
      : null,
  });
});

/**
 * POST /api/sat/descarga-manual
 */
const startDownload = asyncHandler(async (req, res) => {
  const errors = validationResult(req);
  if (!errors.isEmpty()) return res.status(400).json({ errors: errors.array() });

  const { rfc, fechaInicio, fechaFin, tipoComprobante = 'Emitidos' } = req.body;
  const ejercicio = parseInt(req.body.ejercicio, 10);
  const periodo   = parseInt(req.body.periodo,   10);
  const rfcNorm   = rfc.toUpperCase().trim();

  if (!Number.isFinite(ejercicio) || ejercicio < 2000 || ejercicio > 2100) {
    return res.status(400).json({ error: 'ejercicio debe ser un año válido (ej. 2026).' });
  }
  if (!Number.isFinite(periodo) || periodo < 1 || periodo > 12) {
    return res.status(400).json({ error: 'periodo debe ser un número entre 1 y 12.' });
  }

  // ── Validar y normalizar fechas ANTES de cualquier otra operación ──────────
  let fi, ff;
  try {
    const soloInicio = normalizarFecha(fechaInicio, 'fechaInicio');
    const soloFin    = normalizarFecha(fechaFin,    'fechaFin');
    fi = `${soloInicio}T00:00:00`;
    ff = `${soloFin}T23:59:59`;
    validarRango(fi, ff);
  } catch (err) {
    logger.warn(`[SAT] Fechas inválidas en descarga-manual: ${err.message}`);
    return res.status(400).json({ error: err.message });
  }
  // ──────────────────────────────────────────────────────────────────────────

  // ── Validar que el PeriodoFiscal exista en BD ──────────────────────────────
  try {
    await resolverPeriodo(ejercicio, periodo);
  } catch (err) {
    return res.status(err.status || 400).json({ error: err.message });
  }
  // ──────────────────────────────────────────────────────────────────────────

  const estado = await tieneCredenciales(rfcNorm);
  if (!estado.tiene) {
    return res.status(400).json({
      error: 'No hay credenciales e.firma registradas para este RFC',
      rfc:   rfcNorm,
    });
  }

  // ── Validar límites SAT antes de iniciar el job ────────────────────────────
  const limitCheck = await puedeIniciar(rfcNorm);
  if (!limitCheck.puede) {
    logger.warn(`[SAT] Descarga bloqueada por límite (${limitCheck.codigo}) para RFC ${rfcNorm}`);
    return res.status(429).json({
      error:   limitCheck.razon,
      codigo:  limitCheck.codigo,
      limites: await getEstado(rfcNorm),
    });
  }
  // ──────────────────────────────────────────────────────────────────────────

  const jobId = `manual-${rfcNorm}-${Date.now()}`;
  jobsManales.set(jobId, {
    jobId, rfc: rfcNorm, estado: 'en_proceso',
    inicio: new Date(),
    fechaInicio: fi,
    fechaFin:    ff,
    tipoComprobante,
    ejercicio,
    periodo,
    paso: 0,  // progreso real: 0=credenciales, 1=autenticando, 3=verificando, 4=descargando, 5=procesando
  });

  // Registrar solicitud activa ANTES de lanzar el async (el contador debe
  // reflejarse de inmediato para que otras peticiones concurrentes lo vean)
  await registrarInicio(rfcNorm);

  logger.info(`[SAT] Job manual ${jobId} iniciado | rfc=${rfcNorm} fi=${fi} ff=${ff} tipo=${tipoComprobante} periodo=${ejercicio}/${periodo}`);
  res.status(202).json({ message: 'Descarga iniciada', jobId, rfc: rfcNorm });

  // ── Job async con limpieza garantizada en finally ──────────────────────────
  (async () => {
    let creds = null;
    try {
      creds = await obtener(rfcNorm);
      if (!creds) throw new Error('No se pudieron recuperar las credenciales');

      const resultado = await procesarDescarga({
        rfc: rfcNorm,
        fechaInicio: fi,
        fechaFin:    ff,
        tipoComprobante,
        creds,
        ayer: new Date(fi),
        ejercicio,
        periodo,
        onPaso: (n) => {
          const j = jobsManales.get(jobId);
          if (j) jobsManales.set(jobId, { ...j, paso: n });
        },
      });

      jobsManales.set(jobId, {
        ...jobsManales.get(jobId),
        estado: 'completado',
        fin:    new Date(),
        resultado: resultado ?? {
          totalSAT: 0, totalERP: 0, coinciden: 0,
          soloEnSAT: 0, soloEnERP: 0, conDiferencia: 0, paquetes: 0,
        },
      });
      logger.info(`[SAT] Job manual ${jobId} completado`);

    } catch (err) {
      logger.error(`[SAT] Job manual ${jobId} error: ${err.message}`);
      jobsManales.set(jobId, {
        ...jobsManales.get(jobId),
        estado: 'error',
        error:  err.message,
        fin:    new Date(),
      });

    } finally {
      // Decrementar contador de activas siempre (éxito o fallo)
      registrarFin(rfcNorm);

      // Limpiar Buffers en memoria
      limpiarBuffers(creds);

      // Eliminar credenciales de MongoDB siempre (éxito o fallo)
      try {
        await eliminar(rfcNorm);
      } catch (delErr) {
        logger.error(`[SAT] Error eliminando credenciales de ${rfcNorm}: ${delErr.message}`);
      }

      // Limpiar entry del Map después de 30 min — el frontend tiene tiempo de leer
      // el estado final, pero el Map no crece indefinidamente
      setTimeout(() => jobsManales.delete(jobId), 30 * 60 * 1000);
    }
  })();
});

/**
 * GET /api/sat/descarga-manual/status/:jobId
 */
const getDownloadStatus = asyncHandler(async (req, res) => {
  const job = jobsManales.get(req.params.jobId);
  if (!job) return res.status(404).json({ error: 'Job no encontrado' });
  res.json(job);
});

/**
 * GET /api/sat/limites/:rfc
 * Retorna el estado actual de límites de descarga para un RFC.
 */
const getLimitesEstado = asyncHandler(async (req, res) => {
  const rfc    = req.params.rfc.toUpperCase().trim();
  const estado = await getEstado(rfc);
  res.json({ rfc, ...estado });
});

/**
 * GET /api/sat/historial/:rfc
 */
const getHistory = asyncHandler(async (req, res) => {
  const rfc = req.params.rfc.toUpperCase().trim();

  const comparaciones = await Comparison.find({
    comparedBy: 'scheduled',
    comparedAt: { $exists: true },
    $or: [{ rfcEmisor: rfc }, { rfcReceptor: rfc }],
  })
    .sort({ comparedAt: -1 })
    .limit(200)
    .lean();

  const porDia = new Map();
  for (const comp of comparaciones) {
    const dia = comp.comparedAt.toISOString().slice(0, 10);
    if (!porDia.has(dia)) {
      porDia.set(dia, { fecha: dia, total: 0, coinciden: 0, diferencias: 0, errores: 0, soloSAT: 0, soloERP: 0 });
    }
    const bucket = porDia.get(dia);
    bucket.total++;
    if      (comp.status === 'match')                                              bucket.coinciden++;
    else if (comp.status === 'discrepancy')                                        bucket.diferencias++;
    else if (comp.status === 'not_in_sat')  bucket.soloERP++;
    else if (comp.status === 'not_in_erp')  bucket.soloSAT++;
    else if (comp.status === 'error')                                              bucket.errores++;
  }

  const historial = [...porDia.values()].slice(0, 7).map(dia => ({
    ...dia,
    estado: dia.diferencias > 0 ? 'con_diferencias' : dia.errores > 0 ? 'error' : 'ok',
  }));

  res.json({ rfc, historial });
});

module.exports = {
  verify, verifyBatch, getStatus,
  registerCredentials, getCredentialStatus,
  startDownload, getDownloadStatus,
  getLimitesEstado, getHistory,
};