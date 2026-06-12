'use strict';

const express = require('express');
const multer  = require('multer');
const { authenticate, permit }     = require('../../shared/middleware/auth.real');
const { asyncHandler }             = require('../../shared/middleware/error-handler');
const rbacStore                    = require('../../../shared/services/rbac-store');
const { PERMISSIONS, MOVEMENT_SCOPE, getMovementScope } = require('../../../shared/config/rbac');
const service                      = require('./bank.service');
const {
  parseAuxiliaryFile,
  applyAuxiliaryMatching,
  resumenAuxiliarClientes,
  listMovimientosAuxiliar,
} = require('./bank-auxiliary.parser');
const rulesService          = require('./bank-rules.service');
const { matchAutorizaciones, matchAutorizacionesDesdeErp } = require('./bank-autorizaciones.service');
const { emitToUser } = require('../../shared/socket');

const router = express.Router();

/**
 * Aplica restricciones de visibilidad para usuarios sin acceso completo a movimientos.
 * Limita a depósitos; el comportamiento sobre los identificados depende del `scope`
 * declarado en rbac.js para el rol del usuario.
 *
 * Statuses visibles sin filtro explícito (default): no_identificado + reclasificado.
 * Statuses bloqueados para roles restringidos: 'otros'.
 *
 * NOTA DE ESCALA: al agregar un nuevo status, evalúa si debe aparecer en DEFAULT_STATUSES
 * y si requiere un bloque explícito aquí (como 'otros' y 'identificado').
 *
 * @param {object} query               - query params originales
 * @param {string} userId              - auth0 sub del usuario
 * @param {object} [opts]
 * @param {string} [opts.scope]        - MOVEMENT_SCOPE.OWN | ALL (default: OWN)
 * @param {boolean} [opts.forExport]   - en export 'otros' → cae en default en vez de vacío
 * @returns {{ query: object, empty: boolean }}
 */
const RESTRICTED_DEFAULT_STATUSES = 'no_identificado,reclasificado';

function applyMovementRestrictions(query, userId, { scope = MOVEMENT_SCOPE.OWN, forExport = false } = {}) {
  const q = { ...query };
  if (q.status === 'otros') {
    if (!forExport) return { query: q, empty: true };
    q.status = undefined; // en export: cae en el default a continuación
  }
  if (q.status === 'identificado' && scope === MOVEMENT_SCOPE.OWN) {
    q.identificadoPorUsuario = userId;
  }
  if (!q.status) q.status = RESTRICTED_DEFAULT_STATUSES;
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

// GET /api/banks/categories?banco=BBVA  (banco opcional; sin banco → todos)
router.get('/categories', authenticate, asyncHandler(async (req, res) => {
  res.json(await service.listCategories(req.query.banco ?? null));
}));

// GET /api/banks/identificadores?banco=BBVA  (banco opcional; sin banco → todos)
router.get('/identificadores', authenticate, asyncHandler(async (req, res) => {
  res.json(await service.listIdentificadores(req.query.banco ?? null));
}));

// GET /api/banks/movements/export  — descarga Excel respetando filtros activos
router.get('/movements/export', authenticate, permit(PERMISSIONS.BANKS_EXPORT), asyncHandler(async (req, res) => {
  let query = { ...req.query };
  const hasFullAccess = await rbacStore.hasPermission(req.user.role, PERMISSIONS.BANKS_CONFIG);
  if (!hasFullAccess) {
    // banks:export:all → puede exportar depósitos de TODOS los usuarios identificadores,
    // pero sigue sin acceso a retiros ni a "otros" (no tiene banks:config).
    const hasExportAll = await rbacStore.hasPermission(req.user.role, PERMISSIONS.BANKS_EXPORT_ALL);
    const scope = hasExportAll ? MOVEMENT_SCOPE.ALL : getMovementScope(req.user.role);
    ({ query } = applyMovementRestrictions(query, req.user._id, { scope, forExport: true }));
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
  // Cuando viene movId (navegación desde OCR) el usuario puede ver ese movimiento
  // sin restricciones de status/tipo para que la navegación funcione correctamente.
  let query = { ...req.query };
  const hasFullAccess = await rbacStore.hasPermission(req.user.role, PERMISSIONS.BANKS_CONFIG);
  if (!hasFullAccess && !query.movId) {
    const scope = getMovementScope(req.user.role);
    const { query: restricted, empty } = applyMovementRestrictions(query, req.user._id, { scope });
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

// GET /api/banks/stats — conteos globales por estado con filtro de año/mes opcional
router.get('/stats', authenticate, asyncHandler(async (req, res) => {
  const { year, month } = req.query;
  res.json(await service.getStatusStats(year, month));
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

// Mutex: impide que dos cargas de Excel corran en paralelo y generen escrituras
// duplicadas sobre los mismos movimientos. Comportamiento idéntico al del motor ERP.
let autMatchRunning = false;

// POST /api/banks/autorizaciones/match  — match por número de autorización (vía Excel)
router.post('/autorizaciones/match',
  authenticate,
  permit('banks:import'),
  upload.single('excelFile'),
  asyncHandler(async (req, res) => {
    if (autMatchRunning) {
      return res.status(409).json({ error: 'Ya hay un match de autorizaciones en progreso. Espera a que termine antes de cargar otro archivo.' });
    }
    if (!req.file) return res.status(400).json({ error: 'No se envió ningún archivo Excel' });
    autMatchRunning = true;
    try {
      const result = await matchAutorizaciones(req.file.buffer, {
        userId: req.user._id,
        nombre: req.user.nombre,
      });
      res.json(result);
    } finally {
      autMatchRunning = false;
    }
  }),
);

// ── Job store en memoria para match-erp ───────────────────────────────────────
// Guarda estado de cada corrida. Los resultados expiran en 15 min para no
// acumular memoria en procesos de larga ejecución.
const erpMatchJobs   = new Map(); // jobId → { status, auth0Sub, result?, error? }
const ERP_JOB_TTL_MS = 15 * 60 * 1000;
// Mutex: impide que dos corridas corran en paralelo y generen escrituras duplicadas.
let erpMatchRunning  = false;

// POST /api/banks/autorizaciones/match-erp  — inicia job en background, devuelve jobId
// Body opcional: { banco: 'BBVA', fechaDesde: '2026-01-01' }
router.post('/autorizaciones/match-erp',
  authenticate,
  permit('banks:import'),
  asyncHandler(async (req, res) => {
    if (erpMatchRunning) {
      return res.status(409).json({ error: 'Ya hay un match ERP en progreso. Espera a que termine antes de iniciar otro.' });
    }

    const { banco, fechaDesde } = req.body;
    if (fechaDesde != null && fechaDesde !== '' && isNaN(Date.parse(fechaDesde))) {
      return res.status(400).json({ error: 'fechaDesde debe ser una fecha válida (ISO 8601, ej. 2026-01-01)' });
    }

    const jobId    = `erp-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
    const auth0Sub = req.user._id;

    erpMatchRunning = true;
    erpMatchJobs.set(jobId, { status: 'running', auth0Sub });
    res.status(202).json({ jobId });

    // Corre en background: no await, respuesta ya enviada al cliente.
    matchAutorizacionesDesdeErp(
      { banco, fechaDesde },
      {
        onProgress: (progress) => {
          emitToUser(auth0Sub, 'bank:erp:match:progress', { jobId, ...progress });
        },
      },
    )
      .then(result => {
        erpMatchJobs.set(jobId, { status: 'done', auth0Sub, result });
        emitToUser(auth0Sub, 'bank:erp:match:done', { jobId, ...result });
        setTimeout(() => erpMatchJobs.delete(jobId), ERP_JOB_TTL_MS);
      })
      .catch(err => {
        const error = err?.message || 'Error desconocido en el motor ERP';
        erpMatchJobs.set(jobId, { status: 'error', auth0Sub, error });
        emitToUser(auth0Sub, 'bank:erp:match:error', { jobId, error });
        setTimeout(() => erpMatchJobs.delete(jobId), ERP_JOB_TTL_MS);
      })
      .finally(() => { erpMatchRunning = false; });
  }),
);

// GET /api/banks/autorizaciones/match-erp/job/:jobId  — polling de estado (fallback socket)
router.get('/autorizaciones/match-erp/job/:jobId',
  authenticate,
  asyncHandler(async (req, res) => {
    const job = erpMatchJobs.get(req.params.jobId);
    if (!job) return res.status(404).json({ error: 'Job no encontrado o expirado' });
    if (job.auth0Sub !== req.user._id) return res.status(403).json({ error: 'No autorizado' });
    const { auth0Sub: _, ...jobResponse } = job;
    res.json(jobResponse);
  }),
);

// PATCH /api/banks/movements/reclasify — reclasificación manual masiva (admin y contabilidad)
router.patch('/movements/reclasify',
  authenticate,
  permit('banks:config'),
  asyncHandler(async (req, res) => {
    const ids = req.body.ids;
    if (!Array.isArray(ids) || ids.length === 0) {
      return res.status(400).json({ error: 'Se requiere un array de IDs en body.ids' });
    }
    res.json(await service.reclasifyMovements(ids));
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

// POST /api/banks/admin/identificar-anteriores
// Marca como 'identificado' todos los movimientos con status 'no_identificado'
// y fecha anterior al 1 de mayo. Solo admin. Idempotente: no toca 'otros' ni
// movimientos ya identificados.
router.post('/admin/identificar-anteriores',
  authenticate,
  permit('banks:admin'),
  asyncHandler(async (_req, res) => {
    res.json(await service.identificarAnterioresAMayo());
  }),
);

// POST /api/banks/admin/revertir-anteriores
// Deshace exclusivamente lo aplicado por identificar-anteriores.
// Preserva identificaciones manuales de usuarios humanos.
router.post('/admin/revertir-anteriores',
  authenticate,
  permit('banks:admin'),
  asyncHandler(async (_req, res) => {
    res.json(await service.revertirAnterioresAMayo());
  }),
);

// POST /api/banks/admin/importar-conciliacion
// Recibe un Excel (fecha_deposito, banco, monto_deposito) y marca como
// 'identificado' cada movimiento que coincida con status 'no_identificado'.
// No toca movimientos ya identificados ni manipulados por humanos.
// Devuelve runId para el revert selectivo.
router.post('/admin/importar-conciliacion',
  authenticate,
  permit('banks:admin'),
  upload.single('excelFile'),
  asyncHandler(async (req, res) => {
    if (!req.file) return res.status(400).json({ error: 'Se requiere un archivo Excel (.xlsx / .xls)' });
    res.json(await service.importarConciliacion(req.file.buffer, req.user));
  }),
);

// POST /api/banks/admin/revertir-conciliacion
// Deshace exactamente lo aplicado por importar-conciliacion para el runId dado.
// Preserva identificaciones manuales del mismo usuario y de otros motores.
router.post('/admin/revertir-conciliacion',
  authenticate,
  permit('banks:admin'),
  asyncHandler(async (req, res) => {
    const { runId } = req.body;
    if (!runId) return res.status(400).json({ error: 'Se requiere el runId de la importación' });
    res.json(await service.revertirConciliacion(runId, req.user._id));
  }),
);

// GET /api/banks/duplicates  — análisis de movimientos potencialmente duplicados (solo admin)
// Detecta grupos de movimientos que comparten importe+saldo+fecha o número de
// autorización, lo que sugiere que la misma transacción fue importada más de una
// vez con conceptos ligeramente diferentes (distinto hash → pasaron la dedup).
router.get('/duplicates',
  authenticate,
  permit('banks:admin'),
  asyncHandler(async (_req, res) => {
    res.json(await service.findPotentialDuplicates());
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
