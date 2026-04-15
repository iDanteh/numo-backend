'use strict';

const express = require('express');
const multer  = require('multer');
const { body } = require('express-validator');
const { authenticate, authorize } = require('../middleware/auth');
const {
  verify, verifyBatch, getStatus,
  registerCredentials, getCredentialStatus,
  startDownload, getDownloadStatus,
  getLimitesEstado, getHistory,
} = require('../controllers/sat.controller');

const router = express.Router();

// ── Multer: credenciales e.firma (memoria — 2 MB, solo .cer/.key) ────────────
const credUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 2 * 1024 * 1024 },
});

// ── Rutas existentes (sin cambios) ────────────────────────────────────────────

router.post('/verify',
  authenticate,
  [
    body('uuid').isUUID().withMessage('UUID inválido'),
    body('rfcEmisor').notEmpty().withMessage('RFC Emisor requerido'),
    body('rfcReceptor').notEmpty().withMessage('RFC Receptor requerido'),
    body('total').isNumeric().withMessage('Total debe ser numérico'),
  ],
  verify,
);

router.post('/verify-batch', authenticate, authorize('admin', 'contador'), verifyBatch);
router.get('/status/:uuid', authenticate, getStatus);

router.post('/credenciales',
  authenticate,
  authorize('admin', 'contador'),
  credUpload.fields([{ name: 'cer', maxCount: 1 }, { name: 'key', maxCount: 1 }]),
  [
    body('rfc').notEmpty().withMessage('RFC requerido'),
    body('password').notEmpty().withMessage('Contraseña requerida'),
  ],
  registerCredentials,
);

router.get('/credenciales/estado/:rfc', authenticate, getCredentialStatus);

router.post('/descarga-manual',
  authenticate,
  authorize('admin', 'contador'),
  [
    body('rfc').notEmpty().withMessage('RFC requerido'),
    body('fechaInicio').isISO8601().withMessage('fechaInicio debe ser fecha válida'),
    body('fechaFin').isISO8601().withMessage('fechaFin debe ser fecha válida'),
    body('tipoComprobante').optional().isIn(['Emitidos', 'Recibidos', 'Ingresos', 'Egresos', 'Traslados', 'Nomina', 'Pagos']).withMessage('tipoComprobante debe ser uno de: Emitidos, Recibidos, Ingresos, Egresos, Traslados, Nomina, Pagos'),
    body('ejercicio').isInt({ min: 2000, max: 2100 }).withMessage('ejercicio debe ser un año válido (ej. 2024)'),
    body('periodo').isInt({ min: 1, max: 12 }).withMessage('periodo debe ser un mes válido (1–12)'),
  ],
  startDownload,
);

router.get('/descarga-manual/status/:jobId', authenticate, getDownloadStatus);
router.get('/limites/:rfc', authenticate, getLimitesEstado);
router.get('/historial/:rfc', authenticate, getHistory);

module.exports = router;
