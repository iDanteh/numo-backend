'use strict';

const express    = require('express');
const multer     = require('multer');
const { body }   = require('express-validator');
const rateLimit  = require('express-rate-limit');
const { authenticate, permit } = require('../../shared/middleware/auth');
const {
  list, getById, getXml,
  upload, importExcel, importFromErpApi,
  create, compare, remove, exportExcel,
} = require('../controllers/cfdi.controller');

const router = express.Router();

const listLimiter = rateLimit({
  windowMs: 60 * 1000,
  max:      150,
  standardHeaders: true,
  legacyHeaders:   false,
  handler: (_req, res) => res.status(429).json({
    success: false,
    error:   'Demasiadas peticiones, espera un momento.',
    code:    'RATE_LIMIT_EXCEEDED',
    retryAfter: 60,
  }),
});

const xmlUpload   = multer({ storage: multer.memoryStorage(), limits: { fileSize: 50 * 1024 * 1024 } });
const excelUpload = multer({
  storage:    multer.memoryStorage(),
  limits:     { fileSize: 20 * 1024 * 1024 },
  fileFilter: (_req, file, cb) => cb(null, /\.(xlsx|xls)$/i.test(file.originalname)),
});

/** Convierte errores de multer en respuestas 400 legibles. */
const handleXmlUpload = (req, res, next) => {
  xmlUpload.array('xmlFiles', 500)(req, res, (err) => {
    if (err?.name === 'MulterError') {
      if (err.code === 'LIMIT_UNEXPECTED_FILE') {
        return res.status(400).json({
          error: `Campo de archivo inesperado: "${err.field}". Usa el campo "xmlFiles".`,
        });
      }
      return res.status(400).json({ error: `Error al procesar archivos: ${err.message}` });
    }
    next(err);
  });
};

// ── Lectura ───────────────────────────────────────────────────────────────────
router.get('/',          authenticate, listLimiter, list);
router.get('/export',    authenticate, exportExcel);
router.get('/:id/xml',   authenticate, getXml);
router.get('/:id',       authenticate, getById);

// ── Escritura ─────────────────────────────────────────────────────────────────
router.post('/upload',         authenticate, permit('visor:write'), handleXmlUpload, upload);
router.post('/import-excel',   authenticate, permit('visor:write'), excelUpload.single('excelFile'), importExcel);
router.post('/import-erp-api', authenticate, permit('visor:write'), importFromErpApi);
router.post('/:id/compare',    authenticate, permit('visor:write'), compare);
router.post('/',
  authenticate,
  permit('visor:write'),
  [
    body('uuid').isUUID().withMessage('UUID inválido'),
    body('emisor.rfc').notEmpty(),
    body('receptor.rfc').notEmpty(),
    body('total').isNumeric(),
    body('fecha').isISO8601(),
    body('tipoDeComprobante').isIn(['I', 'E', 'T', 'N', 'P']),
  ],
  create,
);

// ── Admin ─────────────────────────────────────────────────────────────────────
router.delete('/:id', authenticate, permit('users:manage'), remove);

module.exports = router;
