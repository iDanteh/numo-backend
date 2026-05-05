'use strict';

const AppConfig           = require('../models/AppConfig');
const ScheduledJob        = require('../models/ScheduledJob');
const { asyncHandler }    = require('../../shared/middleware/error-handler');
const { logger }          = require('../../shared/utils/logger');
const {
  reprogramarJobs,
  ejecutarDescargaERP,
  ejecutarComparacionAuto,
  ejecutarVerificacionPeriodo,
} = require('../jobs/satSyncJob');

// Locks simples para evitar ejecuciones concurrentes del mismo job/periodo
const _locks = new Set();

// Map en memoria solo para guardar el timeoutId (no persiste, se recrea al arrancar)
const _timeouts = new Map();

const KEYS     = ['satDescarga', 'erpDescarga', 'erpVerificacion', 'comparacion'];
const DEFAULTS = {
  satDescarga:     '01:00',
  erpDescarga:     '03:00',
  erpVerificacion: '02:00',
  comparacion:     '04:00',
};
const TIME_RE = /^([01]\d|2[0-3]):([0-5]\d)$/;

// ─────────────────────────────────────────────────────────────────────────────
// Helper: ejecutar secuencia y actualizar DB
// ─────────────────────────────────────────────────────────────────────────────
const _ejecutarSecuencia = async (docId, ejercicio, periodo) => {
  await ScheduledJob.findByIdAndUpdate(docId, { estado: 'en_proceso' });
  logger.info(`[ProgramarMes] Iniciando secuencia ${ejercicio}/${periodo}...`);
  try {
    await ejecutarDescargaERP({ ejercicioParam: ejercicio, periodoParam: periodo });
    await ejecutarVerificacionPeriodo(ejercicio, periodo);
    await ejecutarComparacionAuto({ ejercicioParam: ejercicio, periodoParam: periodo });
    logger.info(`[ProgramarMes] Secuencia ${ejercicio}/${periodo} completada.`);
    await ScheduledJob.findByIdAndUpdate(docId, { estado: 'completado', fin: new Date() });
  } catch (err) {
    logger.error(`[ProgramarMes] ${ejercicio}/${periodo}: ${err.message}`);
    await ScheduledJob.findByIdAndUpdate(docId, { estado: 'error', error: err.message, fin: new Date() });
  } finally {
    _timeouts.delete(String(docId));
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// Restaurar programaciones pendientes al arrancar el servidor
// ─────────────────────────────────────────────────────────────────────────────
const restaurarProgramados = async () => {
  const pendientes = await ScheduledJob.find({ estado: 'pendiente' }).lean();
  const ahora = Date.now();

  for (const job of pendientes) {
    const ejecutaEn = new Date(job.ejecutaEn).getTime();
    const delayMs   = Math.max(0, ejecutaEn - ahora);

    // Si ya pasó la hora programada (reinicio tardío) → ejecutar de inmediato
    const tid = setTimeout(
      () => _ejecutarSecuencia(job._id, job.ejercicio, job.periodo),
      delayMs,
    );
    _timeouts.set(String(job._id), tid);
    logger.info(`[Restore] Programación restaurada: ${job.ejercicio}/${job.periodo} a las ${job.hora} (en ${Math.round(delayMs / 60000)} min)`);
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// Schedule config (horarios nocturnos)
// ─────────────────────────────────────────────────────────────────────────────

const getSchedule = asyncHandler(async (_req, res) => {
  const configs = await AppConfig.find({ key: { $in: KEYS } }).lean();
  const map     = Object.fromEntries(configs.map(c => [c.key, c.value]));
  res.json({
    satDescarga:     map.satDescarga     ?? DEFAULTS.satDescarga,
    erpDescarga:     map.erpDescarga     ?? DEFAULTS.erpDescarga,
    erpVerificacion: map.erpVerificacion ?? DEFAULTS.erpVerificacion,
    comparacion:     map.comparacion     ?? DEFAULTS.comparacion,
  });
});

const updateSchedule = asyncHandler(async (req, res) => {
  const campos = ['satDescarga', 'erpDescarga', 'erpVerificacion', 'comparacion'];

  for (const campo of campos) {
    const valor = req.body[campo];
    if (valor === undefined) continue;
    if (!TIME_RE.test(valor)) {
      return res.status(400).json({ error: `${campo} debe tener formato HH:MM (ej. 01:30)` });
    }
    await AppConfig.findOneAndUpdate({ key: campo }, { value: valor }, { upsert: true, new: true });
  }

  const configs = await AppConfig.find({ key: { $in: KEYS } }).lean();
  const map     = Object.fromEntries(configs.map(c => [c.key, c.value]));

  const resultado = {
    satDescarga:     map.satDescarga     ?? DEFAULTS.satDescarga,
    erpDescarga:     map.erpDescarga     ?? DEFAULTS.erpDescarga,
    erpVerificacion: map.erpVerificacion ?? DEFAULTS.erpVerificacion,
    comparacion:     map.comparacion     ?? DEFAULTS.comparacion,
  };

  reprogramarJobs(resultado);

  res.json({ ...resultado, mensaje: 'Horarios actualizados y jobs reprogramados' });
});

// ─────────────────────────────────────────────────────────────────────────────
// Ejecución manual por periodo
// ─────────────────────────────────────────────────────────────────────────────

const _validarPeriodo = (req, res) => {
  const ejercicio = parseInt(req.body.ejercicio, 10);
  const periodo   = parseInt(req.body.periodo,   10);
  if (!Number.isFinite(ejercicio) || ejercicio < 2000 || ejercicio > 2100) {
    res.status(400).json({ error: 'ejercicio inválido (ej. 2025)' });
    return null;
  }
  if (!Number.isFinite(periodo) || periodo < 1 || periodo > 12) {
    res.status(400).json({ error: 'periodo debe ser 1–12' });
    return null;
  }
  return { ejercicio, periodo };
};

const runErp = asyncHandler(async (req, res) => {
  const parsed = _validarPeriodo(req, res);
  if (!parsed) return;
  const { ejercicio, periodo } = parsed;

  const key = `erp-${ejercicio}-${periodo}`;
  if (_locks.has(key))
    return res.status(409).json({ error: `Descarga ERP ${ejercicio}/${periodo} ya está en proceso.` });

  _locks.add(key);
  res.status(202).json({ message: 'Descarga ERP iniciada', ejercicio, periodo });

  setImmediate(async () => {
    try {
      await ejecutarDescargaERP({ ejercicioParam: ejercicio, periodoParam: periodo });
    } catch (err) {
      logger.error(`[RunErp] ${ejercicio}/${periodo}: ${err.message}`);
    } finally {
      _locks.delete(key);
    }
  });
});

const runVerificacion = asyncHandler(async (req, res) => {
  const parsed = _validarPeriodo(req, res);
  if (!parsed) return;
  const { ejercicio, periodo } = parsed;

  const key = `verif-${ejercicio}-${periodo}`;
  if (_locks.has(key))
    return res.status(409).json({ error: `Verificación ${ejercicio}/${periodo} ya está en proceso.` });

  _locks.add(key);
  res.status(202).json({ message: 'Verificación de estado SAT iniciada', ejercicio, periodo });

  setImmediate(async () => {
    try {
      await ejecutarVerificacionPeriodo(ejercicio, periodo);
    } catch (err) {
      logger.error(`[RunVerif] ${ejercicio}/${periodo}: ${err.message}`);
    } finally {
      _locks.delete(key);
    }
  });
});

const runComparacion = asyncHandler(async (req, res) => {
  const parsed = _validarPeriodo(req, res);
  if (!parsed) return;
  const { ejercicio, periodo } = parsed;

  const key = `comp-${ejercicio}-${periodo}`;
  if (_locks.has(key))
    return res.status(409).json({ error: `Comparación ${ejercicio}/${periodo} ya está en proceso.` });

  _locks.add(key);
  res.status(202).json({ message: 'Comparación iniciada', ejercicio, periodo });

  setImmediate(async () => {
    try {
      await ejecutarComparacionAuto({ ejercicioParam: ejercicio, periodoParam: periodo });
    } catch (err) {
      logger.error(`[RunComp] ${ejercicio}/${periodo}: ${err.message}`);
    } finally {
      _locks.delete(key);
    }
  });
});

const getLocks = asyncHandler(async (_req, res) => {
  res.json({ activos: [..._locks] });
});

// ─────────────────────────────────────────────────────────────────────────────
// Programación de un mes completo — persiste en MongoDB
// ─────────────────────────────────────────────────────────────────────────────

const programarMes = asyncHandler(async (req, res) => {
  const parsed = _validarPeriodo(req, res);
  if (!parsed) return;
  const { ejercicio, periodo } = parsed;

  const { hora } = req.body;
  if (!TIME_RE.test(hora))
    return res.status(400).json({ error: 'hora debe tener formato HH:MM (ej. 22:00)' });

  const ahoraMs    = Date.now();
  const hoyMX      = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Mexico_City' });
  const fakeMXNow  = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/Mexico_City' }));
  const mxOffsetMs = ahoraMs - fakeMXNow.getTime();
  const targetFake = new Date(`${hoyMX}T${hora}:00`);
  const objetivo   = new Date(targetFake.getTime() + mxOffsetMs);
  if (objetivo.getTime() <= ahoraMs) objetivo.setDate(objetivo.getDate() + 1);
  const delayMs = objetivo.getTime() - ahoraMs;

  const doc = await ScheduledJob.create({ ejercicio, periodo, hora, ejecutaEn: objetivo });

  const tid = setTimeout(() => _ejecutarSecuencia(doc._id, ejercicio, periodo), delayMs);
  _timeouts.set(String(doc._id), tid);

  res.status(201).json({
    id:        String(doc._id),
    ejercicio, periodo, hora,
    ejecutaEn: objetivo.toISOString(),
    estado:    'pendiente',
  });
});

const getProgramados = asyncHandler(async (_req, res) => {
  const lista = await ScheduledJob.find().sort({ ejecutaEn: 1 }).lean();
  res.json({
    programados: lista.map(d => ({
      id:        String(d._id),
      ejercicio: d.ejercicio,
      periodo:   d.periodo,
      hora:      d.hora,
      ejecutaEn: d.ejecutaEn,
      estado:    d.estado,
      fin:       d.fin,
      error:     d.error,
    })),
  });
});

const cancelarProgramado = asyncHandler(async (req, res) => {
  const doc = await ScheduledJob.findById(req.params.id);
  if (!doc) return res.status(404).json({ error: 'Programación no encontrada' });
  if (doc.estado !== 'pendiente')
    return res.status(409).json({ error: 'Solo se pueden cancelar programaciones pendientes' });

  const tid = _timeouts.get(req.params.id);
  if (tid) clearTimeout(tid);
  _timeouts.delete(req.params.id);

  await ScheduledJob.findByIdAndDelete(req.params.id);
  res.json({ message: 'Programación cancelada' });
});

const actualizarProgramado = asyncHandler(async (req, res) => {
  const doc = await ScheduledJob.findById(req.params.id);
  if (!doc) return res.status(404).json({ error: 'Programación no encontrada' });
  if (doc.estado !== 'pendiente')
    return res.status(409).json({ error: 'Solo se puede editar una programación pendiente' });

  const { hora } = req.body;
  if (!TIME_RE.test(hora))
    return res.status(400).json({ error: 'hora debe tener formato HH:MM (ej. 22:00)' });

  // Cancelar el timeout anterior
  const tidAnterior = _timeouts.get(req.params.id);
  if (tidAnterior) clearTimeout(tidAnterior);

  // Recalcular ejecutaEn con la nueva hora
  const ahoraMs    = Date.now();
  const hoyMX      = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Mexico_City' });
  const fakeMXNow  = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/Mexico_City' }));
  const mxOffsetMs = ahoraMs - fakeMXNow.getTime();
  const targetFake = new Date(`${hoyMX}T${hora}:00`);
  const objetivo   = new Date(targetFake.getTime() + mxOffsetMs);
  if (objetivo.getTime() <= ahoraMs) objetivo.setDate(objetivo.getDate() + 1);
  const delayMs = objetivo.getTime() - ahoraMs;

  await ScheduledJob.findByIdAndUpdate(req.params.id, { hora, ejecutaEn: objetivo });

  const tid = setTimeout(() => _ejecutarSecuencia(doc._id, doc.ejercicio, doc.periodo), delayMs);
  _timeouts.set(req.params.id, tid);

  res.json({ id: req.params.id, hora, ejecutaEn: objetivo.toISOString(), estado: 'pendiente' });
});

module.exports = {
  getSchedule, updateSchedule,
  runErp, runVerificacion, runComparacion, getLocks,
  programarMes, getProgramados, cancelarProgramado, actualizarProgramado,
  restaurarProgramados,
};
