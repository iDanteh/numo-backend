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

/**
 * Aplica las restricciones de visibilidad para el rol cobranza.
 * Solo depósitos no identificados (o los que ese mismo usuario identificó).
 *
 * @param {object}  query     - query params originales
 * @param {string}  userId    - auth0 sub del usuario
 * @param {boolean} forExport - en exportación no se devuelve vacío; 'otros' → 'no_identificado'
 * @returns {{ query: object, empty: boolean }}
 */
function applyCobranzaRestrictions(query, userId, forExport = false) {
  const q = { ...query };
  if (q.status === 'otros') {
    if (!forExport) return { query: q, empty: true };
    q.status = undefined; // en export: quita el filtro de status → luego cae en el default
  }
  if (q.status === 'identificado') q.identificadoPorUsuario = userId;
  if (!q.status) q.status = 'no_identificado';
  q.tipo = 'deposito';
  return { query: q, empty: false };
}

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
  let query = { ...req.query };
  if (req.user.role === 'cobranza') {
    ({ query } = applyCobranzaRestrictions(query, req.user._id, true));
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
  // Cuando viene movId (navegación desde OCR) cobranza puede ver ese movimiento
  // sin restricciones de status/tipo para que la navegación funcione correctamente.
  let query = { ...req.query };
  if (req.user.role === 'cobranza' && !query.movId) {
    const { query: restricted, empty } = applyCobranzaRestrictions(query, req.user._id);
    if (empty) {
      return res.json({ data: [], pagination: { total: 0, page: 1, limit: Number(query.limit) || 50, pages: 0 } });
    }
    query = restricted;
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
    const result = await service.importFile(req.file.buffer, req.body.banco, req.user._id, { auth0Sub: req.user._id, nombre: req.user.nombre });
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

// PATCH /api/banks/movements/:id/ficha  — requiere permiso banks:ficha (contabilidad y admin)
router.patch('/movements/:id/ficha',
  authenticate,
  permit('banks:ficha'),
  asyncHandler(async (req, res) => {
    res.json(await service.setFicha(req.params.id, req.body.ficha, req.user));
  }),
);

// DELETE /api/banks/movements/:id/ficha  — requiere permiso banks:ficha; el service valida autoría
router.delete('/movements/:id/ficha',
  authenticate,
  permit('banks:ficha'),
  asyncHandler(async (req, res) => {
    res.json(await service.deleteFicha(req.params.id, req.user));
  }),
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

// POST /api/banks/config/:banco/saldo-inicial  — registro único, solo admin
router.post('/config/:banco/saldo-inicial',
  authenticate,
  permit('banks:admin'),
  asyncHandler(async (req, res) => {
    const monto = Number(req.body.monto);
    if (isNaN(monto)) return res.status(400).json({ error: 'monto debe ser un número' });
    const cfg = await service.setSaldoInicial(req.params.banco, monto);
    res.json({
      banco:                 req.params.banco,
      saldoInicial:          Number(cfg.saldoInicial),
      saldoInicialFechaCorte: cfg.saldoInicialFechaCorte,
    });
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
// Body opcional: { banco: 'BBVA', fechaDesde: '2026-01-01' }
//   banco     → si se omite, busca en todos los bancos.
//   fechaDesde → filtra CxCs cuya fechaAfectacion/fechaRealPago >= esta fecha.
//                Recomendado para evitar procesar histórico muy antiguo.
router.post('/autorizaciones/match-erp',
  authenticate,
  permit('banks:import'),
  asyncHandler(async (req, res) => {
    const result = await matchAutorizacionesDesdeErp({
      banco:      req.body.banco,
      fechaDesde: req.body.fechaDesde,
    });
    res.json(result);
  }),
);

// PATCH /api/banks/movements/:id  — edición de campos del movimiento
router.patch('/movements/:id',
  authenticate,
  permit('banks:update'),
  asyncHandler(async (req, res) => {
    res.json(await service.updateMovement(req.params.id, req.body, req.user));
  }),
);

// DELETE /api/banks/movements  — eliminación masiva, solo admin
router.delete('/movements',
  authenticate,
  permit('banks:admin'),
  asyncHandler(async (req, res) => {
    const ids = req.body.ids;
    if (!Array.isArray(ids) || ids.length === 0) {
      return res.status(400).json({ error: 'Se requiere un array de IDs en body.ids' });
    }
    res.json(await service.deleteMovements(ids));
  }),
);

// GET /api/banks/template  — descarga la plantilla Excel oficial
router.get('/template', authenticate, asyncHandler(async (_req, res) => {
  const buffer = await service.generateTemplate();
  const fecha  = new Date().toISOString().slice(0, 10);
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', `attachment; filename="plantilla-bancos-${fecha}.xlsx"`);
  res.send(Buffer.from(buffer));
}));

module.exports = router;
