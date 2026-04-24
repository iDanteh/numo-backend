'use strict';

const AppConfig           = require('../models/AppConfig');
const { asyncHandler }    = require('../../shared/middleware/error-handler');
const { reprogramarJobs } = require('../jobs/satSyncJob');

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

module.exports = { getSchedule, updateSchedule };
