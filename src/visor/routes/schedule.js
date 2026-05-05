'use strict';

const express = require('express');
const { body } = require('express-validator');
const { authenticate, permit } = require('../../shared/middleware/auth');
const { getSchedule, updateSchedule, runErp, runVerificacion, runComparacion, getLocks, programarMes, getProgramados, cancelarProgramado, actualizarProgramado } = require('../controllers/schedule.controller');

const router = express.Router();

const validarPeriodoBody = [
  body('ejercicio').isInt({ min: 2000, max: 2100 }).withMessage('ejercicio inválido'),
  body('periodo').isInt({ min: 1, max: 12 }).withMessage('periodo debe ser 1-12'),
];

router.get('/',  authenticate,                          getSchedule);
router.put('/',  authenticate, permit('entities:write'), updateSchedule);

// Ejecución manual por periodo
router.get('/locks',            authenticate, permit('entities:write'), getLocks);
router.post('/run/erp',         authenticate, permit('entities:write'), validarPeriodoBody, runErp);
router.post('/run/verificacion',authenticate, permit('entities:write'), validarPeriodoBody, runVerificacion);
router.post('/run/comparacion', authenticate, permit('entities:write'), validarPeriodoBody, runComparacion);

// Programación de mes completo
router.get('/programados',                    authenticate, permit('entities:write'), getProgramados);
router.post('/programar-mes',                 authenticate, permit('entities:write'),
  [...validarPeriodoBody, body('hora').matches(/^([01]\d|2[0-3]):([0-5]\d)$/).withMessage('hora inválida (HH:MM)')],
  programarMes,
);
router.delete('/programados/:id',             authenticate, permit('entities:write'), cancelarProgramado);
router.patch('/programados/:id',              authenticate, permit('entities:write'),
  body('hora').matches(/^([01]\d|2[0-3]):([0-5]\d)$/).withMessage('hora inválida (HH:MM)'),
  actualizarProgramado,
);

module.exports = router;
