'use strict';

const express = require('express');
const multer  = require('multer');
const { authenticate, authorize } = require('../../shared/middleware/auth.real');
const { asyncHandler }            = require('../../shared/middleware/error-handler');
const service                     = require('./collection-request.service');

const router = express.Router();

const upload = multer({
  storage: multer.memoryStorage(),
  limits:  { fileSize: 20 * 1024 * 1024 },
  fileFilter(_req, file, cb) {
    const allowed = ['image/jpeg', 'image/png', 'image/webp', 'image/gif', 'application/pdf'];
    if (allowed.includes(file.mimetype)) return cb(null, true);
    cb(new Error(`Tipo no soportado: ${file.mimetype}. Usa JPG, PNG, WEBP o PDF.`));
  },
});

// POST /api/collection-requests/analyze
router.post('/analyze',
  authenticate,
  upload.single('comprobante'),
  asyncHandler(async (req, res) => {
    if (!req.file) return res.status(400).json({ error: 'Se requiere una imagen en el campo "comprobante"' });
    res.json(await service.analyzeReceipt(req.file.buffer, req.file.mimetype));
  }),
);

// GET /api/collection-requests
router.get('/', authenticate, asyncHandler(async (req, res) => {
  res.json(await service.list(req.query));
}));

// POST /api/collection-requests
router.post('/',
  authenticate,
  authorize('admin', 'contador'),
  asyncHandler(async (req, res) => {
    res.status(201).json(await service.create(req.body, req.user._id));
  }),
);

// GET /api/collection-requests/:id
router.get('/:id', authenticate, asyncHandler(async (req, res) => {
  res.json(await service.getById(req.params.id));
}));

// PATCH /api/collection-requests/:id/confirmar
router.patch('/:id/confirmar',
  authenticate,
  authorize('admin', 'contador'),
  asyncHandler(async (req, res) => {
    res.json(await service.confirm(req.params.id, req.body, req.user._id));
  }),
);

// PATCH /api/collection-requests/:id/rechazar
router.patch('/:id/rechazar',
  authenticate,
  authorize('admin', 'contador'),
  asyncHandler(async (req, res) => {
    res.json(await service.reject(req.params.id, req.body.notas));
  }),
);

module.exports = router;
