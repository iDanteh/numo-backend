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
const indicadoresService    = require('./bank-indicadores.service');
const { matchAutorizaciones, matchAutorizacionesDesdeErp } = require('./bank-autorizaciones.service');
const {
  matchTraspasosInternos,
  revertirTraspasosInternos,
  generarExcelTraspasosInternos,
} = require('./traspasos-internos.service');
const { emitToUser } = require('../../shared/socket');
// erp.routes expone _sincronizarConRetry/_rangoDesdeFollo (mismo helper que ya usan los
// scripts de backfill y collection-request.service.js) — sin ciclo: erp.routes.js requiere
// bank.service.js, NUNCA bank.routes.js, así que esta ruta sí puede requerir erp.routes.
const erpRoutes = require('../erp/erp.routes');
const { logger } = require('../../shared/utils/logger');
const CFDI = require('../../../visor/models/CFDI');

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
router.get('/cards', authenticate, permit(PERMISSIONS.BANKS_READ), asyncHandler(async (req, res) => {
  const hasFullAccess  = await rbacStore.hasPermission(req.user.role, PERMISSIONS.BANKS_CONFIG, req.user.extraPermissions);
  // El dashboard muestra "Identificados" de TODO el equipo (scope ALL) aunque el rol tenga
  // scope OWN en la tabla de movimientos — decisión explícita del usuario, independiente de
  // getMovementScope()/rbac.js; la tabla de abajo sigue restringida a lo propio por defecto.
  // No se calcula/pasa rolActual (2026-07-31): el dashboard ya no excluye por ocultar-por-rol,
  // ver comentario en bank.service.js#getCards().
  const restrictions = hasFullAccess ? null : { scope: MOVEMENT_SCOPE.ALL, userId: req.user._id };
  const { year, month } = req.query;
  res.json(await service.getCards(restrictions, year, month));
}));

// GET /api/banks/indicadores — tiempo de identificación (dashboard de Bancos).
// Mismo cálculo de restrictions que /cards: el promedio general y el backlog son de
// TODO el equipo (scope ALL) aunque el rol tenga scope OWN en la tabla de movimientos;
// ver bank-indicadores.service.js#getIndicadoresIdentificacion para el criterio completo.
router.get('/indicadores', authenticate, permit(PERMISSIONS.BANKS_READ), asyncHandler(async (req, res) => {
  const hasFullAccess = await rbacStore.hasPermission(req.user.role, PERMISSIONS.BANKS_CONFIG, req.user.extraPermissions);
  const restrictions = hasFullAccess ? null : { scope: MOVEMENT_SCOPE.ALL, userId: req.user._id };
  const { banco, categoria, year, month } = req.query;
  res.json(await indicadoresService.getIndicadoresIdentificacion({ banco, categoria, year, month, restrictions }));
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
  const query = { ...req.query };
  // Reporte de movimientos: todas las opciones (retiros, "otros"/"reclasificado",
  // identificadoPor de cualquier usuario) disponibles sin restricción por rol —
  // a diferencia del listado principal (GET /movements), que sigue restringido.
  // Ocultamiento por rol (regla 'ocultar' con ocultarRoles): independiente de banks:config
  // — solo banks:admin ve movimientos ocultos para otros roles. Se sobreescribe siempre
  // para que el cliente no pueda mandar su propio rolActual y saltarse el filtro.
  const hasAdminAccess = await rbacStore.hasPermission(req.user.role, PERMISSIONS.BANKS_ADMIN, req.user.extraPermissions);
  query.rolActual = hasAdminAccess ? null : req.user.role;
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
  const hasFullAccess = await rbacStore.hasPermission(req.user.role, PERMISSIONS.BANKS_CONFIG, req.user.extraPermissions);
  if (!hasFullAccess && !query.movId) {
    const scope = getMovementScope(req.user.role);
    const { query: restricted, empty } = applyMovementRestrictions(query, req.user._id, { scope });
    if (empty) {
      return res.json({ data: [], pagination: { total: 0, page: 1, limit: Number(query.limit) || 50, pages: 0 } });
    }
    query = restricted;
  }
  // Ocultamiento por rol (regla 'ocultar' con ocultarRoles): independiente de banks:config
  // — solo banks:admin ve movimientos ocultos para otros roles. Se sobreescribe siempre
  // para que el cliente no pueda mandar su propio rolActual y saltarse el filtro.
  const hasAdminAccess = await rbacStore.hasPermission(req.user.role, PERMISSIONS.BANKS_ADMIN, req.user.extraPermissions);
  query.rolActual = hasAdminAccess ? null : req.user.role;
  res.json(await service.listMovements(query));
}));

// GET /api/banks/summary
router.get('/summary', authenticate, asyncHandler(async (req, res) => {
  const { fechaInicio, fechaFin } = req.query;
  res.json(await service.getSummary(fechaInicio, fechaFin));
}));

// GET /api/banks/stats — conteos por estado con filtro de año/mes/banco opcional, restringido por rol
router.get('/stats', authenticate, permit(PERMISSIONS.BANKS_READ), asyncHandler(async (req, res) => {
  const { year, month, banco } = req.query;
  const hasFullAccess  = await rbacStore.hasPermission(req.user.role, PERMISSIONS.BANKS_CONFIG, req.user.extraPermissions);
  const hasAdminAccess = await rbacStore.hasPermission(req.user.role, PERMISSIONS.BANKS_ADMIN, req.user.extraPermissions);
  // Mismo criterio que /cards: "Identificados" del dashboard es de todo el equipo, no solo del
  // usuario logueado, aunque su rol tenga scope OWN en la tabla de movimientos.
  const restrictions = hasFullAccess ? null : { scope: MOVEMENT_SCOPE.ALL, userId: req.user._id };
  const rolActual     = hasAdminAccess ? null : req.user.role;
  res.json(await service.getStatusStats(year, month, restrictions, rolActual, banco || null));
}));

// GET /api/banks/years — años con al menos un movimiento, opcionalmente acotado a un banco.
// 2026-07-31: separado de /stats para que el combo de año del dashboard no pague la agregación
// completa de estadísticas solo para descartarla — mismo criterio de restricciones/rol que /stats.
router.get('/years', authenticate, permit(PERMISSIONS.BANKS_READ), asyncHandler(async (req, res) => {
  const { banco } = req.query;
  const hasFullAccess  = await rbacStore.hasPermission(req.user.role, PERMISSIONS.BANKS_CONFIG, req.user.extraPermissions);
  const hasAdminAccess = await rbacStore.hasPermission(req.user.role, PERMISSIONS.BANKS_ADMIN, req.user.extraPermissions);
  const restrictions = hasFullAccess ? null : { scope: MOVEMENT_SCOPE.ALL, userId: req.user._id };
  const rolActual     = hasAdminAccess ? null : req.user.role;
  res.json({ years: await service.getAvailableYears(banco || null, rolActual, restrictions) });
}));

// POST /api/banks/upload
router.post('/upload',
  authenticate,
  permit('banks:import'),
  upload.single('excelFile'),
  asyncHandler(async (req, res) => {
    if (!req.file) return res.status(400).json({ error: 'No se envió ningún archivo Excel' });
    const result = await service.importFile(req.file.buffer, req.body.banco, req.user._id, { auth0Sub: req.user._id, nombre: req.user.nombre, filename: req.file.originalname });
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

// GET /api/banks/cfdis/buscar — busca CFDIs por serie/folio, SOLO source='ERP'
// (colección `cfdis`, modelo del dominio visor — cross-domain, mismo patrón ya
// usado por cfdi-poliza-generator.service.js). Usada por el input nuevo de la
// sección de ficha del modal ERP (2026-08-07, permiso propio banks:cfdi:read,
// sin asignar a ningún rol todavía). Match exacto case-insensitive: son
// identificadores precisos, no texto libre — ninguno de los 2 params es
// obligatorio por sí solo, pero al menos uno debe venir para no traer la
// colección entera.
//
// CORRECCIÓN 2026-08-12: la primera versión usaba regex `^...$` con flag `i` — Mongo NO
// puede usar un índice B-tree normal para acotar un regex case-insensitive, así que caía
// en un scan completo (confirmado en producción: "operation exceeded time limit" contra
// maxTimeMS). Cambiado a igualdad exacta + `.collation({strength:2})`, respaldado por el
// índice parcial de CFDI.js (serie+folio, solo documentos source='ERP') — con esto Mongo
// SÍ puede resolver la búsqueda con un IXSCAN case-insensitive real.
router.get('/cfdis/buscar',
  authenticate,
  permit(PERMISSIONS.BANKS_CFDI_READ),
  asyncHandler(async (req, res) => {
    const serie = (req.query.serie ?? '').toString().trim();
    const folio = (req.query.folio ?? '').toString().trim();
    if (!serie && !folio) return res.json([]);

    const filter = { source: 'ERP' };
    if (serie) filter.serie = serie;
    if (folio) filter.folio = folio;

    // maxTimeMS se conserva como red de seguridad (evita retener una conexión del pool
    // compartido de Mongo si algo más deja de usar el índice), pero ya no es lo único que
    // sostiene la performance de esta búsqueda — ver índice parcial+collation en CFDI.js.
    const resultados = await CFDI.find(filter)
      .select('uuid serie folio fecha total')
      .collation({ locale: 'en', strength: 2 })
      .sort({ fecha: -1 })
      .limit(20)
      .maxTimeMS(5000)
      .lean();

    res.json(resultados);
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

router.patch('/movements/:id/categoria',
  authenticate,
  permit('banks:movement:categoria'),
  asyncHandler(async (req, res) => {
    res.json(await service.updateCategoria(req.params.id, req.body.categoria, req.user));
  })
)

// PATCH /api/banks/movements/categoria/bulk — recategorización manual masiva
router.patch('/movements/categoria/bulk',
  authenticate,
  permit('banks:movement:categoria'),
  asyncHandler(async (req, res) => {
    const { ids, categoria } = req.body;
    if (!Array.isArray(ids) || ids.length === 0) {
      return res.status(400).json({ error: 'Se requiere un array de IDs en body.ids' });
    }
    res.json(await service.bulkUpdateCategoria(ids, categoria ?? null, req.user));
  }),
);

// PATCH /api/banks/movements/:id/erp-ids  (remove individual)
// Bug real 2026-07-30: este endpoint es la ÚNICA acción real de "desvincular" (el botón
// del modal ERP solo edita el arreglo en memoria, ver erp-modal.component.ts) — estaba
// guardado con 'banks:update' en vez de 'banks:erp:unlink', contradiciendo el comentario
// explícito en rbac.js ("Restringido a admin — ningún otro rol puede eliminar una
// vinculación existente"). El frontend ya filtraba el botón por 'banks:erp:unlink'
// correctamente, así que este fix no cambia nada para roles existentes (nunca vieron el
// botón) — solo hace que otorgar el permiso extra realmente funcione, y cierra el hueco de
// que cualquiera con 'banks:update' pudiera desvincular llamando la API directo.
router.patch('/movements/:id/erp-ids',
  authenticate,
  permit('banks:erp:unlink'),
  asyncHandler(async (req, res) => {
    res.json(await service.updateErpIds(req.params.id, req.body.action, req.body.erpId, req.user));
  }),
);

// PUT /api/banks/movements/:id/erp-ids  (replace full array)
// Sin permit() propio a propósito: este PUT puede representar un alta (vincular —
// 'banks:erp:link' O 'banks:cobro', ver bug 2026-07-31 en setErpIds()) y/o una baja
// ('banks:erp:unlink') — no se puede saber acá en la ruta cuál de los dos es sin leer el
// movimiento actual primero, así que ambos chequeos viven dentro del servicio (mismo
// patrón ya usado en el resto del archivo para lógica condicional de alcance/permiso).
router.put('/movements/:id/erp-ids',
  authenticate,
  asyncHandler(async (req, res) => {
    await _recuperarFolioFiscalFaltante(req.body.erpLinks);
    res.json(await service.setErpIds(req.params.id, req.body.erpLinks, req.user));
  }),
);

// Best-effort 2026-07-31 (mismo bug de fondo que en Solicitudes de Cobro, ver
// collection-request.service.js#identificar paso 2b): el panel de cobros manual
// (cobro-panel.component.ts + erp-modal.component.ts#confirmErp) arma erpLinks con el
// folioFiscal que ya tenía CACHEADO en el navegador desde que se buscó/paginó la CxC — si
// Kore timbra el CFDI entre esa búsqueda y el clic en "Aplicar Cobro"/"Guardar", ese
// folioFiscal llega null y se queda así para siempre (ventana normalmente de segundos/
// minutos, mucho más chica que en Solicitudes de Cobro, pero el mismo hueco). Antes de
// persistir, se reintenta por cada link sin folioFiscal que sí traiga serie+folioExterno.
// Nunca bloquea el guardado: si el ERP no responde o no lo encuentra, sigue sin ese dato.
async function _recuperarFolioFiscalFaltante(erpLinks) {
  if (!Array.isArray(erpLinks)) return;
  for (const link of erpLinks) {
    if (!link || link.folioFiscal || !link.serie || !link.folioExterno) continue;
    const rango = erpRoutes._rangoDesdeFollo(link.folioExterno);
    if (!rango) continue;
    try {
      const { raw } = await erpRoutes._sincronizarConRetry({
        serieExterna: link.serie, folioExterno: String(link.folioExterno),
        fechaDesde: rango.fechaDesde, fechaHasta: rango.fechaHasta,
      });
      const encontrada = raw.find(c =>
        String(c.folioExterno) === String(link.folioExterno) && String(c.serieExterna) === String(link.serie),
      );
      if (encontrada?.folioFiscal) link.folioFiscal = encontrada.folioFiscal;
    } catch (err) {
      logger.warn(`[banks] PUT erp-ids: no se pudo recuperar folioFiscal para erpId=${link.erpId} vía /cuentas-pendientes (se continúa sin él): ${err.message}`);
    }
  }
}

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
  permit('banks:movement:edit'),
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

// POST /api/banks/admin/traspasos-internos
// Motor de detección de traspasos entre cuentas propias (BBVA depósito categorizado por
// el usuario ↔ retiro real en el banco contraparte, mismo día UTC + mismo monto,
// exactamente 1 candidato de cada lado). El banco contraparte NO es fijo — se determina
// por movimiento a partir del concepto del depósito BBVA (ver
// traspasos-internos.service.js#_extraerBancoContraparte); si no se puede determinar,
// el movimiento se reporta aparte en `sinBancoDetectado`, nunca se asume un banco al azar.
// body: { categoriaBbva, dryRun }. dryRun=true (default): solo devuelve la clasificación,
// sin escribir en Mongo.
router.post('/admin/traspasos-internos',
  authenticate,
  permit('banks:admin'),
  asyncHandler(async (req, res) => {
    const { categoriaBbva, dryRun } = req.body;
    if (!categoriaBbva) return res.status(400).json({ error: 'Se requiere categoriaBbva' });
    res.json(await matchTraspasosInternos({ categoriaBbva, dryRun }, req.user));
  }),
);

// POST /api/banks/admin/traspasos-internos/revertir
// Deshace exactamente lo aplicado por traspasos-internos para el runId dado.
// Preserva identificaciones manuales humanas posteriores del mismo movimiento.
router.post('/admin/traspasos-internos/revertir',
  authenticate,
  permit('banks:admin'),
  asyncHandler(async (req, res) => {
    const { runId } = req.body;
    if (!runId) return res.status(400).json({ error: 'Se requiere el runId de la operación' });
    res.json(await revertirTraspasosInternos(runId, req.user._id));
  }),
);

// GET /api/banks/admin/traspasos-internos/reporte?categoriaBbva=...
// Corre el mismo motor en dry-run y devuelve el Excel directo como descarga —
// operación síncrona corta, sin persistir jobId (mismo criterio que /movements/export).
router.get('/admin/traspasos-internos/reporte',
  authenticate,
  permit('banks:admin'),
  asyncHandler(async (req, res) => {
    const { categoriaBbva } = req.query;
    if (!categoriaBbva) return res.status(400).json({ error: 'Se requiere categoriaBbva' });
    const resultado = await matchTraspasosInternos({ categoriaBbva, dryRun: true }, req.user);
    const buffer    = await generarExcelTraspasosInternos(resultado);
    const fecha     = new Date().toISOString().slice(0, 10);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="traspasos-internos-${fecha}.xlsx"`);
    res.send(Buffer.from(buffer));
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
