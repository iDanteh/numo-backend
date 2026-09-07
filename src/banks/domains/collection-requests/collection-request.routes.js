'use strict';

const express = require('express');
const multer  = require('multer');
const { authenticate, permit }    = require('../../shared/middleware/auth.real');
const { asyncHandler }            = require('../../shared/middleware/error-handler');
const { verifyKoreApiKey }        = require('../../../shared/middleware/kore-api-key-auth');
const service                     = require('./collection-request.service');
const indicadoresService          = require('./collection-request-indicadores.service');

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

// Admin puede acotar /indicadores(/distribucion) a uno o varios contadores específicos
// vía ?userIds=id1,id2 (coma-separado) — 2026-09-07, pedido explícito del usuario. Sin
// ese query param, admin sigue viendo TODO el equipo (undefined), comportamiento de
// siempre. Para cualquier otro rol se ignora (siempre su propio _id, sin importar qué
// venga en la query). Los ids acá son el auth0 sub (resueltoPorUserId es String en
// CollectionRequest.model.js, NO un ObjectId de Mongo ni el id entero de Postgres de
// AppUserRecord) — mismo criterio que `identificadoPor` en bank.service.js#_buildFilter:
// split(',') + trim + filter(Boolean), sin validar contra ObjectId (no aplica, el campo
// nunca es de ese tipo, así que un id inválido no puede tirar CastError, simplemente no
// matchea nada).
function _resolveScopeUserId(req) {
  if (req.user.role !== 'admin') return req.user._id;
  const userIds = String(req.query.userIds || '').split(',').map(s => s.trim()).filter(Boolean);
  return userIds.length ? userIds : undefined;
}

// GET /api/collection-requests/indicadores — tiempo de identificación ACOTADO a
// Solicitudes de Cobro (total + fase banco/Kore + fase contador). Admin ve TODO el
// equipo (o solo los contadores elegidos vía ?userIds=); cualquier otro rol con
// collections:read (contadores) ve SOLO lo que él mismo resolvió — mismo criterio que
// scopeUserId de abajo, pedido explícito del usuario (2026-09-03, ampliado 2026-09-07).
// Debe ir antes de /:id.
router.get('/indicadores', authenticate, permit('collections:read'), asyncHandler(async (req, res) => {
  const { year, month } = req.query;
  const scopeUserId = _resolveScopeUserId(req);
  res.json(await indicadoresService.getIndicadoresSolicitudesCobro({ year, month, scopeUserId }));
}));

// GET /api/collection-requests/indicadores/distribucion — distribución por franja de
// tiempo del bloque "Distribución por franja de tiempo" del panel de arriba, ACOTADA
// al día actual (hora de México) por defecto, o al rango fechaInicio/fechaFin cuando
// el usuario usa el selector de rango — ver getDistribucionSolicitudesCobro() para el
// criterio completo. Mismo scoping por rol que /indicadores (admin: todo el equipo o
// los contadores elegidos vía ?userIds=; resto: solo lo propio). Debe ir antes de /:id.
router.get('/indicadores/distribucion', authenticate, permit('collections:read'), asyncHandler(async (req, res) => {
  const { fechaInicio, fechaFin } = req.query;
  const scopeUserId = _resolveScopeUserId(req);
  res.json(await indicadoresService.getDistribucionSolicitudesCobro({ desde: fechaInicio, hasta: fechaFin, scopeUserId }));
}));

// GET /api/collection-requests/indicadores/contadores — auth0Subs de los usuarios que
// alguna vez identificaron una solicitud de cobro (CollectionRequest.resueltoPorUserId
// real), a diferencia de GET /api/users (UserService.listUsers()) que trae TODOS los
// usuarios con rol contabilidad/cobranza sin importar si resolvieron algo. Fix real
// (2026-09-07, reportado por el admin probando el filtro en el navegador): el <select>
// del panel ofrecía usuarios que nunca habían resuelto nada. Mismo permiso que
// /indicadores (collections:read) — no es un dato sensible propio de admin, cualquier
// rol con acceso al panel puede pedirlo. Debe ir antes de /:id.
router.get('/indicadores/contadores', authenticate, permit('collections:read'), asyncHandler(async (req, res) => {
  res.json({ userIds: await indicadoresService.listContadoresConSolicitudesIdentificadas() });
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
  res.json(await service.list(req.query));
}));

// GET /api/collection-requests/:id
router.get('/:id', authenticate, permit('collections:read'), asyncHandler(async (req, res) => {
  res.json(await service.getById(req.params.id));
}));

// GET /api/collection-requests/:id/comprobante — imagen/PDF del PRIMER
// comprobante (compat con solicitudes de un solo archivo, viejas o nuevas).
router.get('/:id/comprobante', authenticate, permit('collections:read'), asyncHandler(async (req, res) => {
  const { data, mimetype, originalName } = await service.getComprobante(req.params.id, 0);
  res.set('Content-Type', mimetype || 'application/octet-stream');
  res.set('Content-Disposition', `inline; filename="${originalName || 'comprobante'}"`);
  res.send(data);
}));

// GET /api/collection-requests/:id/comprobantes/:index — imagen/PDF del
// comprobante en esa posición (0-based) — para solicitudes con varios.
// Proxy autenticado: el archivo vive en Drive, nunca se expone un link público.
router.get('/:id/comprobantes/:index', authenticate, permit('collections:read'), asyncHandler(async (req, res) => {
  const { data, mimetype, originalName } = await service.getComprobante(req.params.id, parseInt(req.params.index, 10) || 0);
  res.set('Content-Type', mimetype || 'application/octet-stream');
  res.set('Content-Disposition', `inline; filename="${originalName || 'comprobante'}"`);
  res.send(data);
}));

// GET /api/collection-requests/:id/analyze-comprobante — corre OCR + matching
// sobre CADA comprobante ya guardado (mismo motor que /analyze, sin volver a
// subir los archivos) — regresa un resultado por comprobante, nunca combinados,
// para ayudar a ubicar el movimiento bancario correspondiente a cada uno.
router.get('/:id/analyze-comprobante', authenticate, permit('collections:read'), asyncHandler(async (req, res) => {
  res.json(await service.analyzeStoredComprobantes(req.params.id));
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
