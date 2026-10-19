'use strict';

/**
 * loyalty/domains/import/loyalty-import.routes.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — migración de clientes legados ("Club Tuberos"). Router
 * de STAFF, montado en `/api/loyalty/import` (ver app.js), gateado por el
 * permiso angosto y exclusivo de admin `loyalty:importar` (rbac.js).
 *
 * SOLO expone la previsualización/concentrado (lectura, sin I/O a base de
 * datos): sube clientes.csv y/o puntos.pdf, los parsea y cruza, y devuelve el
 * reporte para revisión humana. Aplicar la migración (crear tarjetas/ledger)
 * queda fuera de alcance — es un paso posterior, una vez decidido el mapeo
 * Categoria→Nivel (ver loyalty-import.service.js).
 *
 * Multer — nota sobre "dos instancias": el patrón de referencia
 * (bank.routes.js `upload`/`uploadFichaImagen`) usa dos instancias de multer,
 * pero cada una en una ruta DISTINTA. Acá ambos archivos (clientes y puntos)
 * viajan en la MISMA request multipart/form-data, y multer solo puede
 * consumir el stream de la request una vez — por eso se usa una sola llamada
 * a `.fields([...])`, con un fileFilter que decide la validación de
 * extensión/mimetype según `file.fieldname` (equivalente funcional a "dos
 * validadores distintos", sin duplicar el parseo del stream). El límite de
 * tamaño de multer es único por request (25MB, el mayor de los dos); el tope
 * más estricto de clientes.csv (15MB) se revalida a mano en el handler.
 */
const express = require('express');
const multer = require('multer');
const { authenticate, permit } = require('../../../shared/middleware/auth');
const { asyncHandler } = require('../../../shared/middleware/error-handler');
const { PERMISSIONS } = require('../../../shared/config/rbac');
const { BadRequestError } = require('../../../shared/errors/AppError');
const { parseClientesCsv, parsePuntosPdf, parseCorreccionesXlsx } = require('./loyalty-import.parser');
const { construirConcentrado } = require('./loyalty-import.service');
const applyService = require('./loyalty-import-apply.service');

const router = express.Router();

const MAX_CLIENTES_BYTES = 15 * 1024 * 1024;
const MAX_PUNTOS_BYTES = 25 * 1024 * 1024;

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: MAX_PUNTOS_BYTES }, // tope único de multer = el mayor de los dos
  fileFilter: (_req, file, cb) => {
    if (file.fieldname === 'clientes') {
      const ok = /\.csv$/i.test(file.originalname);
      return cb(ok ? null : new Error('El campo "clientes" solo acepta archivos .csv'), ok);
    }
    if (file.fieldname === 'puntos') {
      const ok = /\.pdf$/i.test(file.originalname) || file.mimetype === 'application/pdf';
      return cb(ok ? null : new Error('El campo "puntos" solo acepta archivos .pdf'), ok);
    }
    cb(new Error(`Campo de archivo inesperado: "${file.fieldname}"`), false);
  },
});

const uploadConcentrado = upload.fields([
  { name: 'clientes', maxCount: 1 },
  { name: 'puntos', maxCount: 1 },
]);

// Excel de correcciones (re-subida de filas bloqueantes corregidas a mano, ver
// descargarBloqueantes() en importar-clientes.component.ts) — un único campo, instancia de
// multer separada porque es un flujo independiente del par clientes.csv/puntos.pdf (no viaja en
// la misma request multipart).
const MAX_CORRECCIONES_BYTES = 15 * 1024 * 1024; // mismo tope que clientes.csv — el Excel de
// correcciones es siempre un subconjunto chico (las filas bloqueantes de una sola corrida).

const uploadCorrecciones = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: MAX_CORRECCIONES_BYTES },
  fileFilter: (_req, file, cb) => {
    const ok = /\.xlsx$/i.test(file.originalname);
    return cb(ok ? null : new Error('El campo "correcciones" solo acepta archivos .xlsx'), ok);
  },
}).single('correcciones');

// POST /api/loyalty/import/concentrado
router.post('/concentrado', authenticate, permit(PERMISSIONS.LOYALTY_IMPORTAR), uploadConcentrado, asyncHandler(async (req, res) => {
  const clientesFile = req.files?.clientes?.[0];
  const puntosFile = req.files?.puntos?.[0];

  if (!clientesFile && !puntosFile) {
    throw new BadRequestError('Debes subir al menos un archivo (clientes.csv o puntos.pdf).');
  }
  if (clientesFile && clientesFile.size > MAX_CLIENTES_BYTES) {
    throw new BadRequestError('clientes.csv supera el tamaño máximo permitido (15MB).');
  }

  const clientesRows = clientesFile ? await parseClientesCsv(clientesFile.buffer) : [];
  const puntosRows = puntosFile ? await parsePuntosPdf(puntosFile.buffer) : [];

  res.json(construirConcentrado({ clientesRows, puntosRows }));
}));

// POST /api/loyalty/import/aplicar?confirmar=true
//
// Sin `?confirmar=true` (o con cualquier otro valor): dry-run — parsea,
// clasifica contra la DB real (loyalty-import-apply.service#clasificar) y
// devuelve el reporte, SIN escribir nada. Con `?confirmar=true`: arranca la
// migración en BACKGROUND (loyalty-import-apply.service#iniciarAplicarJob) y
// responde 202+jobId de inmediato — ver el comentario de esa función sobre
// por qué (bcrypt del NIP: ~22min para un batch real en una sola transacción
// HTTP). Decisión de negocio 2026-10-19: las filas bloqueantes (duplicado/
// invalido/conflicto) YA NO abortan nada — quedan excluidas del job
// (`excluidasBloqueantes` en el resultado final) y el resto de la corrida se
// aplica igual. El admin las corrige en el Excel exportado por el frontend y
// las re-sube por separado vía POST /correcciones.
router.post('/aplicar', authenticate, permit(PERMISSIONS.LOYALTY_IMPORTAR), uploadConcentrado, asyncHandler(async (req, res) => {
  const clientesFile = req.files?.clientes?.[0];
  const puntosFile = req.files?.puntos?.[0];

  if (!clientesFile && !puntosFile) {
    throw new BadRequestError('Debes subir al menos un archivo (clientes.csv o puntos.pdf).');
  }
  if (clientesFile && clientesFile.size > MAX_CLIENTES_BYTES) {
    throw new BadRequestError('clientes.csv supera el tamaño máximo permitido (15MB).');
  }

  const clientesRows = clientesFile ? await parseClientesCsv(clientesFile.buffer) : [];
  const puntosRows = puntosFile ? await parsePuntosPdf(puntosFile.buffer) : [];

  const confirmar = req.query.confirmar === 'true';
  if (!confirmar) {
    return res.json(await applyService.clasificar({ clientesRows, puntosRows }));
  }

  const { jobId, total, totalLotes } = await applyService.iniciarAplicarJob({ clientesRows, puntosRows }, req.user);
  res.status(202).json({ jobId, total, totalLotes });
}));

// GET /api/loyalty/import/aplicar-job/:jobId
//
// Fallback de recuperación tras un reload de página (mismo criterio que GET
// /netpay/reporte/upload-job/:jobId en erp.routes.js) — el socket
// (`lealtad-import:apply:progress|done|error`) es la vía normal de enterarse
// del progreso; esto es solo para cuando alguien recarga a mitad del job y
// pierde la suscripción en memoria. SÍ hay chequeo de dueño (userId).
router.get('/aplicar-job/:jobId', authenticate, permit(PERMISSIONS.LOYALTY_IMPORTAR), asyncHandler(async (req, res) => {
  const estado = applyService.obtenerEstadoAplicarJob(req.params.jobId, req.user._id);
  if (!estado) return res.status(404).json({ error: 'Job de aplicar no encontrado o expirado' });
  res.json(estado);
}));

// POST /api/loyalty/import/correcciones?confirmar=true
//
// Re-aplica las filas bloqueantes que el admin corrigió a mano en el Excel exportado por
// descargarBloqueantes() (frontend) — flujo independiente del par clientes.csv/puntos.pdf.
// Mismo contrato que /aplicar: sin `?confirmar=true` es dry-run (clasificarCorrecciones, SIN
// escribir nada); con `?confirmar=true` arranca la MISMA máquina de job en background
// (iniciarAplicarCorreccionesJob → _iniciarJobDesdeFilasClasificadas) y responde 202+jobId de
// inmediato — el progreso/resultado llegan por los mismos eventos de socket
// (`lealtad-import:apply:progress|done|error`, genéricos por jobId).
router.post('/correcciones', authenticate, permit(PERMISSIONS.LOYALTY_IMPORTAR), uploadCorrecciones, asyncHandler(async (req, res) => {
  if (!req.file) {
    throw new BadRequestError('Debes subir un archivo .xlsx de correcciones.');
  }

  const filasCrudas = await parseCorreccionesXlsx(req.file.buffer);

  const confirmar = req.query.confirmar === 'true';
  if (!confirmar) {
    return res.json(await applyService.clasificarCorrecciones(filasCrudas));
  }

  const { jobId, total, totalLotes } = await applyService.iniciarAplicarCorreccionesJob(filasCrudas, req.user);
  res.status(202).json({ jobId, total, totalLotes });
}));

// GET /api/loyalty/import/pendientes
router.get('/pendientes', authenticate, permit(PERMISSIONS.LOYALTY_IMPORTAR), asyncHandler(async (req, res) => {
  const { page, limit } = req.query;
  res.json(await applyService.listarPendientes({ page, limit }));
}));

// POST /api/loyalty/import/pendientes/:id/completar
router.post('/pendientes/:id/completar', authenticate, permit(PERMISSIONS.LOYALTY_IMPORTAR), asyncHandler(async (req, res) => {
  const { telefono } = req.body ?? {};
  res.json(await applyService.completarRegistroPendiente(req.params.id, { telefono }, req.user));
}));

// POST /api/loyalty/import/pendientes/:id/rechazar — decisión de negocio 2026-10-19: el admin
// decide que un cliente legado sin teléfono ya no se va a migrar. Nunca borra la fila.
router.post('/pendientes/:id/rechazar', authenticate, permit(PERMISSIONS.LOYALTY_IMPORTAR), asyncHandler(async (req, res) => {
  res.json(await applyService.rechazarRegistroPendiente(req.params.id, req.user));
}));

// POST /api/loyalty/import/pendientes/rechazar-todos — un solo UPDATE masivo (sin bcrypt/NIP de
// por medio, a diferencia de /aplicar), no hace falta job ni lotes.
router.post('/pendientes/rechazar-todos', authenticate, permit(PERMISSIONS.LOYALTY_IMPORTAR), asyncHandler(async (req, res) => {
  res.json(await applyService.rechazarTodosPendientes(req.user));
}));

module.exports = router;
