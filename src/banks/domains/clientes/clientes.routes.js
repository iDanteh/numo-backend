'use strict';

const express = require('express');
const multer  = require('multer');
const ExcelJS = require('exceljs');
const { authenticate, permit }    = require('../../shared/middleware/auth.real');
const { asyncHandler }            = require('../../shared/middleware/error-handler');
const { BadRequestError }         = require('../../../shared/errors/AppError');
const service                     = require('./clientes.service');

const router = express.Router();

const upload = multer({
  storage: multer.memoryStorage(),
  limits:  { fileSize: 10 * 1024 * 1024 },
  fileFilter: (_req, file, cb) => {
    const ok = /\.(xlsx|xls|csv)$/i.test(file.originalname);
    cb(ok ? null : new Error('Solo se aceptan archivos .xlsx, .xls o .csv'), ok);
  },
});

// GET /api/clientes
router.get('/', authenticate, asyncHandler(async (req, res) => {
  res.json(await service.list(req.query));
}));

// POST /api/clientes/import  — debe ir antes de /:id para que Express no lo capture como { id: 'import' }
router.post('/import',
  authenticate,
  permit('account-plan:write'),
  upload.single('file'),
  asyncHandler(async (req, res) => {
    if (!req.file) throw new BadRequestError('No se recibió ningún archivo');

    const rows = [];
    const name = req.file.originalname.toLowerCase();

    if (name.endsWith('.csv')) {
      const text  = req.file.buffer.toString('utf8');
      const lines = text.split(/\r?\n/).map(l => l.trim()).filter(Boolean);
      if (lines.length < 2) throw new BadRequestError('El CSV está vacío o no tiene filas de datos');
      const headers = lines[0].split(',').map(h => h.replace(/"/g, '').trim().toLowerCase());
      for (let i = 1; i < lines.length; i++) {
        const vals = lines[i].split(',').map(v => v.replace(/"/g, '').trim());
        const obj  = {};
        headers.forEach((h, idx) => { obj[h] = vals[idx] ?? ''; });
        rows.push(obj);
      }
    } else {
      const workbook = new ExcelJS.Workbook();
      await workbook.xlsx.load(req.file.buffer);
      const sheet = workbook.worksheets[0];
      if (!sheet) throw new BadRequestError('El archivo Excel no tiene hojas de datos');

      const headers = [];
      sheet.getRow(1).eachCell((cell, col) => {
        headers[col] = String(cell.value ?? '').trim().toLowerCase();
      });

      sheet.eachRow((row, rowNum) => {
        if (rowNum === 1) return;
        const obj = {};
        row.eachCell((cell, col) => {
          const key = headers[col];
          if (key) obj[key] = cell.value != null ? String(cell.value).trim() : '';
        });
        if (Object.values(obj).some(v => v !== '')) rows.push(obj);
      });
    }

    if (rows.length === 0) throw new BadRequestError('No se encontraron filas de datos en el archivo');
    res.json(await service.importBulk(rows));
  }),
);

// GET /api/clientes/:id
router.get('/:id', authenticate, asyncHandler(async (req, res) => {
  res.json(await service.getById(req.params.id));
}));

// POST /api/clientes
router.post('/',
  authenticate,
  permit('account-plan:write'),
  asyncHandler(async (req, res) => {
    res.status(201).json(await service.create(req.body));
  }),
);

// PATCH /api/clientes/:id
router.patch('/:id',
  authenticate,
  permit('account-plan:write'),
  asyncHandler(async (req, res) => {
    res.json(await service.update(req.params.id, req.body));
  }),
);

// DELETE /api/clientes/:id
router.delete('/:id',
  authenticate,
  permit('account-plan:write'),
  asyncHandler(async (req, res) => {
    res.json(await service.softDelete(req.params.id));
  }),
);

module.exports = router;
