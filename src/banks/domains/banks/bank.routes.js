'use strict';

const express = require('express');
const multer  = require('multer');
const { authenticate, permit }     = require('../../shared/middleware/auth.real');
const { asyncHandler }             = require('../../shared/middleware/error-handler');
const service                      = require('./bank.service');
const {
  parseAuxiliaryFile,
  applyAuxiliaryMatching,
  resumenAuxiliarClientes,
  listMovimientosAuxiliar,
} = require('./bank-auxiliary.parser');
const rulesService          = require('./bank-rules.service');
const { matchAutorizaciones, matchAutorizacionesDesdeErp } = require('./bank-autorizaciones.service');

const router = express.Router();

const upload = multer({
  storage: multer.memoryStorage(),
  limits:  { fileSize: 50 * 1024 * 1024 },
  fileFilter: (_req, file, cb) => {
    const ok = /\.(xlsx|xls)$/i.test(file.originalname);
    cb(ok ? null : new Error('Solo se aceptan archivos Excel (.xlsx, .xls)'), ok);
  },
});

// GET /api/banks/cards
router.get('/cards', authenticate, asyncHandler(async (req, res) => {
  res.json(await service.getCards());
}));

// GET /api/banks/categories?banco=BBVA
router.get('/categories', authenticate, asyncHandler(async (req, res) => {
  if (!req.query.banco) return res.status(400).json({ error: 'banco requerido' });
  res.json(await service.listCategories(req.query.banco));
}));

// GET /api/banks/identificadores?banco=BBVA
router.get('/identificadores', authenticate, asyncHandler(async (req, res) => {
  if (!req.query.banco) return res.status(400).json({ error: 'banco requerido' });
  res.json(await service.listIdentificadores(req.query.banco));
}));

// GET /api/banks/movements/export  — descarga Excel respetando filtros activos
router.get('/movements/export', authenticate, asyncHandler(async (req, res) => {
  const query = { ...req.query };
  // cobranza no puede exportar movimientos identificados
  if (req.user.role === 'cobranza') {
    if (query.status === 'identificado') query.status = undefined;
    if (!query.status) query.status = 'no_identificado';
  }
  const buffer = await service.exportMovements(query);
  const banco  = req.query.banco || 'movimientos';
  const fecha  = new Date().toISOString().slice(0, 10);
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', `attachment; filename="movimientos-${banco}-${fecha}.xlsx"`);
  res.send(Buffer.from(buffer));
}));

// GET /api/banks/movements
router.get('/movements', authenticate, asyncHandler(async (req, res) => {
  const query = { ...req.query };
  // cobranza no puede ver movimientos identificados
  if (req.user.role === 'cobranza') {
    if (query.status === 'identificado') {
      return res.json({ data: [], pagination: { total: 0, page: 1, limit: Number(query.limit) || 50, pages: 0 } });
    }
    if (!query.status) query.status = 'no_identificado';
  }
  res.json(await service.listMovements(query));
}));

// GET /api/banks/summary
router.get('/summary', authenticate, asyncHandler(async (req, res) => {
  const { fechaInicio, fechaFin } = req.query;
  res.json(await service.getSummary(fechaInicio, fechaFin));
}));

// POST /api/banks/upload
router.post('/upload',
  authenticate,
  permit('banks:import'),
  upload.single('excelFile'),
  asyncHandler(async (req, res) => {
    if (!req.file) return res.status(400).json({ error: 'No se envió ningún archivo Excel' });
    const result = await service.importFile(req.file.buffer, req.body.banco, req.user._id, { auth0Sub: req.user._id });
    res.status(207).json(result);
  }),
);

// POST /api/banks/import-individual
router.post(
  '/import-individual',
  authenticate,
  permit('banks:import'),
  asyncHandler(async (req, res) => {
    const { movimiento, banco } = req.body;

    if (!movimiento) {
      return res.status(400).json({ error: 'No se envió el movimiento' });
    }

    const result = await service.importIndividual(
      movimiento,
      banco,
      req.user._id,
      { auth0Sub: req.user._id }
    );

    res.status(201).json(result);
  })
);

// PATCH /api/banks/movements/:id/status
router.patch('/movements/:id/status',
  authenticate,
  permit('banks:update'),
  asyncHandler(async (req, res) => {
    res.json(await service.updateStatus(req.params.id, req.body.status, req.user));
  }),
);

// PATCH /api/banks/movements/:id/erp-ids  (remove individual)
router.patch('/movements/:id/erp-ids',
  authenticate,
  permit('banks:update'),
  asyncHandler(async (req, res) => {
    res.json(await service.updateErpIds(req.params.id, req.body.action, req.body.erpId, req.user));
  }),
);

// PUT /api/banks/movements/:id/erp-ids  (replace full array)
router.put('/movements/:id/erp-ids',
  authenticate,
  permit('banks:update'),
  asyncHandler(async (req, res) => {
    res.json(await service.setErpIds(req.params.id, req.body.erpLinks, req.user));
  }),
);

// ── Reglas de categorización ─────────────────────────────────────────────────

// GET /api/banks/rules?banco=BBVA
router.get('/rules', authenticate, asyncHandler(async (req, res) => {
  if (!req.query.banco) return res.status(400).json({ error: 'banco requerido' });
  res.json(await rulesService.listRules(req.query.banco));
}));

// POST /api/banks/rules
router.post('/rules',
  authenticate, permit('banks:rules'),
  asyncHandler(async (req, res) => {
    const { banco, ...data } = req.body;
    if (!banco) return res.status(400).json({ error: 'banco requerido' });
    res.status(201).json(await rulesService.createRule(banco, data));
  }),
);

// PUT /api/banks/rules/reorder
router.put('/rules/reorder',
  authenticate, permit('banks:rules'),
  asyncHandler(async (req, res) => {
    res.json(await rulesService.reorderRules(req.body.ids));
  }),
);

// PUT /api/banks/rules/:id
router.put('/rules/:id',
  authenticate, permit('banks:rules'),
  asyncHandler(async (req, res) => {
    res.json(await rulesService.updateRule(req.params.id, req.body));
  }),
);

// DELETE /api/banks/rules/:id
router.delete('/rules/:id',
  authenticate, permit('banks:rules'),
  asyncHandler(async (req, res) => {
    res.json(await rulesService.deleteRule(req.params.id));
  }),
);

// POST /api/banks/rules/apply
router.post('/rules/apply',
  authenticate, permit('banks:rules'),
  asyncHandler(async (req, res) => {
    const { banco, soloSinCategoria = false } = req.body;
    if (!banco) return res.status(400).json({ error: 'banco requerido' });
    res.json(await rulesService.applyRules(banco, soloSinCategoria));
  }),
);

// POST /api/banks/auxiliar/import
router.post('/auxiliar/import',
  authenticate,
  permit('banks:import'),
  upload.single('excelFile'),
  asyncHandler(async (req, res) => {
    if (!req.file) return res.status(400).json({ error: 'No se envió ningún archivo Excel' });
    const result = await parseAuxiliaryFile(req.file.buffer);
    res.status(207).json(result);
  }),
);

// POST /api/banks/auxiliar/aplicar  — cruza catálogo con movimientos
router.post('/auxiliar/aplicar',
  authenticate,
  permit('banks:update'),
  asyncHandler(async (_req, res) => {
    const result = await applyAuxiliaryMatching();
    res.json(result);
  }),
);

// GET /api/banks/auxiliar/clientes  — resumen agrupado por cliente
router.get('/auxiliar/clientes',
  authenticate,
  asyncHandler(async (req, res) => {
    const result = await resumenAuxiliarClientes(req.query);
    res.json(result);
  }),
);

// GET /api/banks/auxiliar/movimientos  — lista paginada de movimientos identificados
router.get('/auxiliar/movimientos',
  authenticate,
  asyncHandler(async (req, res) => {
    const result = await listMovimientosAuxiliar(req.query);
    res.json(result);
  }),
);

// GET /api/banks/config/:banco
router.get('/config/:banco', authenticate, asyncHandler(async (req, res) => {
  res.json(await service.getConfig(req.params.banco));
}));

// PATCH /api/banks/config/:banco
router.patch('/config/:banco',
  authenticate,
  permit('banks:config'),
  asyncHandler(async (req, res) => {
    res.json(await service.saveConfig(req.params.banco, req.body));
  }),
);

// POST /api/banks/autorizaciones/match  — match por número de autorización (vía Excel)
router.post('/autorizaciones/match',
  authenticate,
  permit('banks:import'),
  upload.single('excelFile'),
  asyncHandler(async (req, res) => {
    if (!req.file) return res.status(400).json({ error: 'No se envió ningún archivo Excel' });
    const result = await matchAutorizaciones(req.file.buffer);
    res.json(result);
  }),
);

// POST /api/banks/autorizaciones/match-erp  — match desde CxCs del ERP (sin Excel)
// Body opcional: { banco: 'BBVA' }  — si se omite, busca en todos los bancos.
router.post('/autorizaciones/match-erp',
  authenticate,
  permit('banks:import'),
  asyncHandler(async (req, res) => {
    const result = await matchAutorizacionesDesdeErp({ banco: req.body.banco });
    res.json(result);
  }),
);

module.exports = router;
