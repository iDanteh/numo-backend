const express = require('express');
const multer = require('multer');
const { body } = require('express-validator');
const rateLimit = require('express-rate-limit');
const { authenticate, authorize } = require('../middleware/auth');
const {
  list, getById, getXml,
  upload, importExcel, importFromErpApi,
  create, compare, remove, exportExcel,
} = require('../controllers/cfdi.controller');

const router = express.Router();

const listLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 150,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => {
    res.status(429).json({
      success: false,
      error: 'Demasiadas peticiones, espera un momento.',
      code: 'RATE_LIMIT_EXCEEDED',
      retryAfter: 60,
    });
  },
});

const xmlUpload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 50 * 1024 * 1024 } });
const excelUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 20 * 1024 * 1024 },
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

router.get('/', authenticate, listLimiter, list);
router.get('/export', authenticate, exportExcel);
router.get('/:id/xml', authenticate, getXml);
router.get('/:id', authenticate, getById);

router.post('/upload', authenticate, authorize('admin', 'contador'), handleXmlUpload, upload);
router.post('/import-excel', authenticate, authorize('admin', 'contador'), excelUpload.single('excelFile'), importExcel);
router.post('/import-erp-api', authenticate, authorize('admin', 'contador'), importFromErpApi);
router.post('/:id/compare', authenticate, authorize('admin', 'contador', 'auditor'), compare);
router.post('/',
  authenticate,
  authorize('admin', 'contador'),
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

router.delete('/:id', authenticate, authorize('admin'), remove);

module.exports = router;
