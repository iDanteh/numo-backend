'use strict';

const express = require('express');
const multer  = require('multer');
const { authenticate, permit }    = require('../../shared/middleware/auth.real');
const { asyncHandler }            = require('../../shared/middleware/error-handler');
const service                     = require('./account-plan.service');

const router = express.Router();

const upload = multer({
  storage: multer.memoryStorage(),
  limits:  { fileSize: 20 * 1024 * 1024 },
  fileFilter: (_req, file, cb) => {
    const ok = /\.(xlsx|xls)$/i.test(file.originalname);
    cb(ok ? null : new Error('Solo se aceptan archivos Excel (.xlsx, .xls)'), ok);
  },
});

// GET /api/account-plan
router.get('/', authenticate, asyncHandler(async (req, res) => {
  res.json(await service.list(req.query));
}));

// GET /api/account-plan/tree
router.get('/tree', authenticate, asyncHandler(async (req, res) => {
  res.json(await service.tree());
}));

// GET /api/account-plan/search
router.get('/search', authenticate, asyncHandler(async (req, res) => {
  res.json(await service.search(req.query.q, req.query.tipo));
}));

// GET /api/account-plan/:id
router.get('/:id', authenticate, asyncHandler(async (req, res) => {
  res.json(await service.getById(req.params.id));
}));

// POST /api/account-plan
router.post('/',
  authenticate,
  permit('account-plan:write'),
  asyncHandler(async (req, res) => {
    res.status(201).json(await service.create(req.body));
  }),
);

// PATCH /api/account-plan/:id
router.patch('/:id',
  authenticate,
  permit('account-plan:write'),
  asyncHandler(async (req, res) => {
    res.json(await service.update(req.params.id, req.body));
  }),
);

// DELETE /api/account-plan/:id
router.delete('/:id',
  authenticate,
  permit('users:manage'),
  asyncHandler(async (req, res) => {
    res.json(await service.softDelete(req.params.id));
  }),
);

// POST /api/account-plan/import
router.post('/import',
  authenticate,
  permit('account-plan:write'),
  upload.single('excelFile'),
  asyncHandler(async (req, res) => {
    if (!req.file) return res.status(400).json({ error: 'No se envió ningún archivo Excel' });

    let columnMap;
    if (req.body.columnMap) {
      try { columnMap = JSON.parse(req.body.columnMap); } catch (_) { /* ignorar */ }
    }

    const result = await service.importFile(req.file.buffer, { columnMap });
    const statusCode = result.errores.length > 0 ? 207 : 200;
    res.status(statusCode).json({
      message:       `${result.importados} importadas, ${result.actualizados} actualizadas, ${result.omitidos} omitidas`,
      importados:    result.importados,
      actualizados:  result.actualizados,
      omitidos:      result.omitidos,
      total:         result.total,
      errores:       result.errores,
    });
  }),
);

module.exports = router;
