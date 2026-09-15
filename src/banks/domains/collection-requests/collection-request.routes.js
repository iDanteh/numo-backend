'use strict';

const express = require('express');
const multer  = require('multer');
const { authenticate, permit }    = require('../../shared/middleware/auth.real');
const { asyncHandler }            = require('../../shared/middleware/error-handler');
const { verifyKoreApiKey }        = require('../../../shared/middleware/kore-api-key-auth');
const { PERMISSIONS }             = require('../../../shared/config/rbac');
const rbacStore                   = require('../../../shared/services/rbac-store');
const { ForbiddenError }          = require('../../shared/errors/AppError');
const service                     = require('./collection-request.service');
const indicadoresService          = require('./collection-request-indicadores.service');

// Resuelve el `forceStatus` a pasarle al service para las rutas de Solicitudes de Cobro
// que muestran datos de una solicitud puntual (bandeja, detalle, comprobantes) — 2026-09-15,
// permiso collections:read:identificadas. Todas estas rutas ya exigen collections:read
// (permit(), antes de llegar acá); esto decide CUÁNTO ve dentro de ese universo:
//   - collections:write → null (acceso completo, todos los status — comportamiento de siempre).
//   - collections:read:identificadas (sin write) → 'identificada' (fuerza el filtro,
//     ignora lo que pida la query/el id).
//   - ninguno de los dos → 403 (el rol solo tiene collections:read "pelado", que hoy
//     nadie usa contra estas rutas — Tienda, el único rol así, usa /mias).
async function _resolverForceStatus(user) {
  const tieneAccesoCompleto = await rbacStore.hasPermission(user.role, PERMISSIONS.COLLECTIONS_WRITE, user.extraPermissions);
  if (tieneAccesoCompleto) return null;
  const soloIdentificadas = await rbacStore.hasPermission(user.role, PERMISSIONS.COLLECTIONS_READ_IDENTIFICADAS, user.extraPermissions);
  if (soloIdentificadas) return 'identificada';
  throw new ForbiddenError('No tenés permiso suficiente para ver solicitudes de cobro.');
}

const router = express.Router();

const upload = multer({
  storage: multer.memoryStorage(),
  limits:  { fileSize: 20 * 1024 * 1024 },
  fileFilter(_req, file, cb) {
    const allowed = ['image/jpeg', 'image/jpg', 'image/png', 'image/webp', 'image/gif', 'application/pdf'];
    if (allowed.includes(file.mimetype)) return cb(null, true);
    cb(new Error(`Tipo no soportado: ${file.mimetype}. Usa JPG, JPEG, PNG, WEBP o PDF.`));
  },
});

// Los comprobantes de una solicitud de cobro se suben a Google Drive (ver
// drive-comprobantes.service.js), no a Mongo — sin el límite de 5MB que antes
// era necesario para no acercarse al máximo de 16MB por documento de MongoDB.
// Hasta 6 archivos por solicitud (uno por depósito bancario distinto, típico
// de Modo 1 con transferencia + efectivo + cheque).
const MAX_COMPROBANTES = 6;
const uploadComprobante = multer({
  storage: multer.memoryStorage(),
  limits:  { fileSize: 15 * 1024 * 1024 },
  fileFilter(_req, file, cb) {
    const allowed = ['image/jpeg', 'image/jpg', 'image/png', 'image/webp', 'image/gif', 'application/pdf'];
    if (allowed.includes(file.mimetype)) return cb(null, true);
    cb(new Error(`Tipo no soportado: ${file.mimetype}. Usa JPG, JPEG, PNG, WEBP o PDF.`));
  },
});

// POST /api/collection-requests/analyze
router.post('/analyze',
  authenticate,
  upload.single('comprobante'),
  asyncHandler(async (req, res) => {
    if (!req.file) return res.status(400).json({ error: 'Se requiere una imagen en el campo "comprobante"' });
    res.json(await service.analyzeReceipt(req.file.buffer, req.file.mimetype, req.file.originalname));
  }),
);

// POST /api/collection-requests — crea una solicitud de cobro. Lo llama el ERP
// (Kore) directamente, autenticado con API key, no con sesión Numo. El campo
// multipart es "comprobantes" (repetido una vez por archivo) — antes era
// "comprobante" (singular); Kore debe actualizar su integración.
router.post('/',
  verifyKoreApiKey,
  uploadComprobante.array('comprobantes', MAX_COMPROBANTES),
  asyncHandler(async (req, res) => {
    res.status(201).json(await service.create(req.body, req.files));
  }),
);

// GET /api/collection-requests/mias — solicitudes creadas por el usuario autenticado
// (rol tienda revisando el estatus de lo que ha solicitado). Debe ir antes de /:id.
router.get('/mias', authenticate, permit('collections:read'), asyncHandler(async (req, res) => {
  res.json(await service.listMine(req.user._id, req.query));
}));

// GET /api/collection-requests/stats — conteos por status + "hoy" + monto
// pendiente total, para las tarjetas superiores y los badges de las pestañas.
// Aparte de list(): con paginación real por status, el arreglo de list() ya no
// trae todos los estatus a la vez. Debe ir antes de /:id.
router.get('/stats', authenticate, permit('collections:read'), asyncHandler(async (req, res) => {
  res.json(await service.stats());
}));

// GET /api/collection-requests/mias/stats — mismo propósito, acotado a las
// solicitudes del usuario autenticado (rol tienda en "mis solicitudes").
router.get('/mias/stats', authenticate, permit('collections:read'), asyncHandler(async (req, res) => {
  res.json(await service.statsMine(req.user._id));
}));

// GET /api/collection-requests/indicadores — tiempo de identificación ACOTADO a
// Solicitudes de Cobro (total + fase banco/Kore + fase contador), siempre de
// TODO el equipo — ver collection-request-indicadores.service.js para el
// criterio completo. Debe ir antes de /:id.
router.get('/indicadores', authenticate, permit('collections:read'), asyncHandler(async (req, res) => {
  const { year, month } = req.query;
  res.json(await indicadoresService.getIndicadoresSolicitudesCobro({ year, month }));
}));

// GET /api/collection-requests/report — reporte Excel de TODAS las solicitudes
// resueltas (Autorizadas + Rechazadas, nunca pendientes — ver buildReport en el
// service). Requiere collections:write: solo cobranza/contabilidad/admin ven el
// universo completo. Debe ir antes de /:id.
router.get('/report', authenticate, permit('collections:write'), asyncHandler(async (req, res) => {
  const buffer = await service.buildReport(req.query);
  const fecha = new Date().toISOString().slice(0, 10);
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', `attachment; filename="Solicitudes-Cobro-${fecha}.xlsx"`);
  res.send(buffer);
}));

// GET /api/collection-requests/mias/report — mismo reporte, acotado a las
// solicitudes resueltas del usuario autenticado (rol tienda). Requiere solo
// collections:read: es lo único que tienda tiene. Debe ir antes de /:id.
router.get('/mias/report', authenticate, permit('collections:read'), asyncHandler(async (req, res) => {
  const buffer = await service.buildReportMine(req.user._id, req.query);
  const fecha = new Date().toISOString().slice(0, 10);
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', `attachment; filename="Mis-Solicitudes-Cobro-${fecha}.xlsx"`);
  res.send(buffer);
}));

// GET /api/collection-requests/erp/:solicitudIdErp — el ERP (Kore) consulta el
// estado de la solicitud que él mismo creó, autenticado con su API key (no hay
// sesión Numo). Debe ir antes de /:id para que Express no intente matchear
// "erp" como si fuera un _id de Mongo.
router.get('/erp/:solicitudIdErp', verifyKoreApiKey, asyncHandler(async (req, res) => {
  res.json(await service.getByErpId(req.params.solicitudIdErp));
}));

// POST /api/collection-requests/erp/:solicitudIdErp/cancelar — Kore avisa que
// canceló la CxC de su lado (ej. CAC) mientras la solicitud seguía pendiente en
// Numo. Mismo mecanismo de autenticación que el resto de las llamadas de Kore
// en este router (API key, no sesión Auth0/Numo). Body: { canceladoPorUserId,
// canceladoPorNombre } — la identidad del usuario de Kore que confirmó la
// cancelación, para mostrar "Cancelado por el usuario X" en la bandeja.
router.post('/erp/:solicitudIdErp/cancelar', verifyKoreApiKey, asyncHandler(async (req, res) => {
  res.json(await service.cancelarPorErp(req.params.solicitudIdErp, req.body));
}));

// GET /api/collection-requests — bandeja para revisión (cobranza/contabilidad/admin)
router.get('/', authenticate, permit('collections:read'), asyncHandler(async (req, res) => {
  const forceStatus = await _resolverForceStatus(req.user);
  res.json(await service.list(req.query, { forceStatus }));
}));

// GET /api/collection-requests/:id
router.get('/:id', authenticate, permit('collections:read'), asyncHandler(async (req, res) => {
  const forceStatus = await _resolverForceStatus(req.user);
  res.json(await service.getById(req.params.id, { forceStatus }));
}));

// GET /api/collection-requests/:id/comprobante — imagen/PDF del PRIMER
// comprobante (compat con solicitudes de un solo archivo, viejas o nuevas).
router.get('/:id/comprobante', authenticate, permit('collections:read'), asyncHandler(async (req, res) => {
  const forceStatus = await _resolverForceStatus(req.user);
  const { data, mimetype, originalName } = await service.getComprobante(req.params.id, 0, { forceStatus });
  res.set('Content-Type', mimetype || 'application/octet-stream');
  res.set('Content-Disposition', `inline; filename="${originalName || 'comprobante'}"`);
  res.send(data);
}));

// GET /api/collection-requests/:id/comprobantes/:index — imagen/PDF del
// comprobante en esa posición (0-based) — para solicitudes con varios.
// Proxy autenticado: el archivo vive en Drive, nunca se expone un link público.
router.get('/:id/comprobantes/:index', authenticate, permit('collections:read'), asyncHandler(async (req, res) => {
  const forceStatus = await _resolverForceStatus(req.user);
  const { data, mimetype, originalName } = await service.getComprobante(req.params.id, parseInt(req.params.index, 10) || 0, { forceStatus });
  res.set('Content-Type', mimetype || 'application/octet-stream');
  res.set('Content-Disposition', `inline; filename="${originalName || 'comprobante'}"`);
  res.send(data);
}));

// GET /api/collection-requests/:id/analyze-comprobante — corre OCR + matching
// sobre CADA comprobante ya guardado (mismo motor que /analyze, sin volver a
// subir los archivos) — regresa un resultado por comprobante, nunca combinados,
// para ayudar a ubicar el movimiento bancario correspondiente a cada uno.
router.get('/:id/analyze-comprobante', authenticate, permit('collections:read'), asyncHandler(async (req, res) => {
  const forceStatus = await _resolverForceStatus(req.user);
  res.json(await service.analyzeStoredComprobantes(req.params.id, { forceStatus }));
}));

// PATCH /api/collection-requests/:id/identificar — vincula la solicitud a uno o
// varios movimientos bancarios encontrados manualmente. Body: { bankMovementId }
// (atajo escalar, expande a todas las formasPago) o { asignaciones: [{
// formaPagoDocId, bankMovementId }] } (una asignación por forma de pago,
// multi-bank-movement — ver design/sdd/collection-request-multi-bank-movement).
// Se pasa el body COMPLETO — resolverAsignaciones() en el service decide cuál
// forma aplica.
router.patch('/:id/identificar',
  authenticate,
  permit('collections:write'),
  asyncHandler(async (req, res) => {
    res.json(await service.identificar(req.params.id, req.body, req.user));
  }),
);

// PATCH /api/collection-requests/:id/rechazar
router.patch('/:id/rechazar',
  authenticate,
  permit('collections:write'),
  asyncHandler(async (req, res) => {
    res.json(await service.rechazar(req.params.id, req.body.motivo, req.user));
  }),
);

module.exports = router;
