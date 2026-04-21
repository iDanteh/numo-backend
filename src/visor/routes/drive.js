'use strict';

const express = require('express');
const { body } = require('express-validator');
const { authenticate, permit } = require('../../shared/middleware/auth');
const { getFolders, importFromDrive } = require('../controllers/drive.controller');

const router = express.Router();

router.get('/folders', authenticate, getFolders);

router.post('/import',
  authenticate,
  permit('drive:import'),
  [
    body('source').isIn(['ERP', 'SAT', 'MANUAL']).withMessage('source debe ser ERP, SAT o MANUAL'),
    body('folderId').if(body('source').not().equals('ERP')).notEmpty().withMessage('folderId requerido'),
    body('ejercicio').isInt({ min: 2000, max: 2100 }).withMessage('ejercicio inválido'),
    body('periodo').isInt({ min: 1, max: 12 }).withMessage('periodo debe ser 1-12'),
  ],
  importFromDrive,
);

module.exports = router;
