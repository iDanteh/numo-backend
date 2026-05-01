'use strict';

const AppConfig           = require('../models/AppConfig');
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

// Programaciones pendientes: Map<id, { id, ejercicio, periodo, hora, ejecutaEn, estado, timeoutId }>
const _programados = new Map();

const KEYS     = ['satDescarga', 'erpDescarga', 'erpVerificacion', 'comparacion'];
const DEFAULTS = {
  satDescarga:     '01:00',
  erpDescarga:     '03:00',
  erpVerificacion: '02:00',
  comparacion:     '04:00',
};
const TIME_RE = /^([01]\d|2[0-3]):([0-5]\d)$/;

/**
 * GET /api/schedule
 */
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

/**
 * PUT /api/schedule
 * Body: { satDescarga?, erpDescarga?, erpVerificacion?, comparacion? }
 */
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

/**
 * POST /api/schedule/run/erp
 * Ejecuta la descarga ERP para el ejercicio/periodo indicado.
 */
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

/**
 * POST /api/schedule/run/verificacion
 * Verifica el estado SAT de todos los CFDIs ERP del periodo indicado.
 */
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

/**
 * POST /api/schedule/run/comparacion
 * Ejecuta la comparación ERP vs SAT para el periodo indicado.
 */
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

/**
 * GET /api/schedule/locks
 * Retorna los jobs actualmente en ejecución (para que el frontend sepa el estado).
 */
const getLocks = asyncHandler(async (_req, res) => {
  res.json({ activos: [..._locks] });
});

// ─────────────────────────────────────────────────────────────────────────────
// Programación de un mes completo (ERP → Verificación → Comparación en secuencia)
// ─────────────────────────────────────────────────────────────────────────────

/**
 * POST /api/schedule/programar-mes
 * Programa la ejecución secuencial de los 3 jobs para un periodo a una hora dada.
 */
const programarMes = asyncHandler(async (req, res) => {
  const parsed = _validarPeriodo(req, res);
  if (!parsed) return;
  const { ejercicio, periodo } = parsed;

  const { hora } = req.body;
  if (!TIME_RE.test(hora))
    return res.status(400).json({ error: 'hora debe tener formato HH:MM (ej. 22:00)' });

  // Calcular delay en hora de México (correcto para servidores UTC/Docker)
  // Truco: comparar Date.now() con la misma hora interpretada como "local del server"
  // para obtener el offset MX→UTC y aplicarlo al objetivo.
  const ahoraMs     = Date.now();
  const hoyMX       = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Mexico_City' });
  const fakeMXNow   = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/Mexico_City' }));
  const mxOffsetMs  = ahoraMs - fakeMXNow.getTime(); // diferencia UTC - horaLocal(MX) en ms
  const targetFake  = new Date(`${hoyMX}T${hora}:00`); // hora MX tratada como local del servidor
  const objetivo    = new Date(targetFake.getTime() + mxOffsetMs); // corregida a UTC real
  if (objetivo.getTime() <= ahoraMs) {
    // Si ya pasó hoy en México, programar para mañana
    objetivo.setDate(objetivo.getDate() + 1);
  }
  const delayMs = objetivo.getTime() - ahoraMs;

  const id = `prog-${ejercicio}-${periodo}-${Date.now()}`;

  const timeoutId = setTimeout(async () => {
    const prog = _programados.get(id);
    if (prog) _programados.set(id, { ...prog, estado: 'en_proceso' });
    logger.info(`[ProgramarMes] Iniciando secuencia ${ejercicio}/${periodo}...`);
    try {
      await ejecutarDescargaERP({ ejercicioParam: ejercicio, periodoParam: periodo });
      await ejecutarVerificacionPeriodo(ejercicio, periodo);
      await ejecutarComparacionAuto({ ejercicioParam: ejercicio, periodoParam: periodo });
      logger.info(`[ProgramarMes] Secuencia ${ejercicio}/${periodo} completada.`);
      if (_programados.has(id))
        _programados.set(id, { ..._programados.get(id), estado: 'completado', fin: new Date().toISOString() });
    } catch (err) {
      logger.error(`[ProgramarMes] ${ejercicio}/${periodo}: ${err.message}`);
      if (_programados.has(id))
        _programados.set(id, { ..._programados.get(id), estado: 'error', error: err.message, fin: new Date().toISOString() });
    }
  }, delayMs);

  _programados.set(id, {
    id, ejercicio, periodo, hora,
    ejecutaEn: objetivo.toISOString(),
    estado:    'pendiente',
    timeoutId,
  });

  res.status(201).json({ id, ejercicio, periodo, hora, ejecutaEn: objetivo.toISOString() });
});

/**
 * GET /api/schedule/programados
 */
const getProgramados = asyncHandler(async (_req, res) => {
  const lista = [..._programados.values()].map(({ timeoutId, ...rest }) => rest);
  res.json({ programados: lista });
});

/**
 * DELETE /api/schedule/programados/:id
 */
const cancelarProgramado = asyncHandler(async (req, res) => {
  const prog = _programados.get(req.params.id);
  if (!prog) return res.status(404).json({ error: 'Programación no encontrada' });
  if (prog.estado !== 'pendiente')
    return res.status(409).json({ error: 'Solo se pueden cancelar programaciones pendientes' });
  clearTimeout(prog.timeoutId);
  _programados.delete(req.params.id);
  res.json({ message: 'Programación cancelada' });
});

module.exports = { getSchedule, updateSchedule, runErp, runVerificacion, runComparacion, getLocks, programarMes, getProgramados, cancelarProgramado };
