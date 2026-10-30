'use strict';

/**
 * loyalty/loyalty.routes.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — router de STAFF, montado en `/api/loyalty` (PR4/8). Cada
 * ruta usa `authenticate, permit(PERMISSIONS.LOYALTY_X), asyncHandler`, mismo
 * patrón que `system-monitor.routes.js`/`bank.routes.js:117`. El lookup público
 * (anónimo) vive en un router aparte (`loyalty-public.routes.js`) — JAMÁS se
 * monta acá, y este router nunca importa `authenticate`/`permit` de forma
 * condicional: es staff-only de punta a punta (D9, ver design.md).
 *
 * La lógica de negocio vive por completo en los servicios de dominio
 * (PR2/PR3): este archivo solo valida permisos y hace el mapeo HTTP↔servicio.
 */
const express = require('express');
const multer  = require('multer');
const { authenticate, permit } = require('../shared/middleware/auth');
const { asyncHandler } = require('../shared/middleware/error-handler');
const { BadRequestError } = require('../shared/errors/AppError');
const { PERMISSIONS } = require('../shared/config/rbac');
const cardService = require('./domains/cards/loyalty-card.service');
const ledgerService = require('./domains/ledger/loyalty-ledger.service');
const configService = require('./domains/config/loyalty-config.service');

const router = express.Router();

// Multer dedicado a la foto del cliente — mismo patrón que uploadFichaImagen en
// bank.routes.js, sin el tipo application/pdf (acá siempre es una foto tomada con cámara).
const uploadFotoCliente = multer({
  storage: multer.memoryStorage(),
  limits:  { fileSize: 15 * 1024 * 1024 },
  fileFilter: (_req, file, cb) => {
    const ok = /^image\/(jpeg|png|webp)$/.test(file.mimetype);
    cb(ok ? null : new Error('Solo se aceptan imágenes (jpg/png/webp)'), ok);
  },
});

// ── Tarjetas ──────────────────────────────────────────────────────────────────
router.get('/cards', authenticate, permit(PERMISSIONS.LOYALTY_READ), asyncHandler(async (req, res) => {
  const { search, nivel, activa, origen, page, limit, sort, sortDir, fechaAltaInicio, fechaAltaFin } = req.query;
  const exportar = req.query.exportar === 'true';
  const revisionPendiente = req.query.revisionPendiente === 'true'
    ? true
    : (req.query.revisionPendiente === 'false' ? false : undefined);
  res.json(await cardService.listar({
    search, nivel, activa, origen, page, limit, sort, sortDir,
    exportar, fechaAltaInicio, fechaAltaFin, revisionPendiente,
  }));
}));

router.post('/cards', authenticate, permit(PERMISSIONS.LOYALTY_TARJETAS_CREAR), asyncHandler(async (req, res) => {
  const {
    telefono, nombre, nivel,
    apellidos, apodo, sexo, fechaNacimiento, especialidades, sucursal, calle,
    numExterior, numInterior, colonia, municipio, estado, cp, comentarios,
  } = req.body ?? {};
  const resultado = await cardService.crearTarjeta({
    telefono, nombre, nivel,
    apellidos, apodo, sexo, fechaNacimiento, especialidades, sucursal, calle,
    numExterior, numInterior, colonia, municipio, estado, cp, comentarios,
  }, req.user);
  res.status(201).json(resultado);
}));

// ── Rutas estáticas ANTES que /:id para no colisionar con el parámetro ───────
// Pendientes de revisión Kore (2026-10-29) — altas creadas por el webhook de catálogo de clientes
// (`_procesarAlta` en loyalty-kore-cliente.service.js), marcadas `revisionPendiente: true`. Ruta
// estática antes de 'GET /cards/:id' por el mismo criterio que movimientos/niveles/editar abajo.
router.get('/cards/pendientes-revision-kore', authenticate, permit(PERMISSIONS.LOYALTY_READ), asyncHandler(async (req, res) => {
  const { page, limit, search } = req.query;
  res.json(await cardService.listarPendientesRevisionKore({ page, limit, search }));
}));

router.get('/cards/:id/movimientos', authenticate, permit(PERMISSIONS.LOYALTY_READ), asyncHandler(async (req, res) => {
  const { page, limit } = req.query;
  res.json(await ledgerService.listarMovimientos(req.params.id, { page, limit }));
}));

router.get('/cards/:id/niveles', authenticate, permit(PERMISSIONS.LOYALTY_READ), asyncHandler(async (req, res) => {
  res.json(await ledgerService.listarNiveles(req.params.id));
}));

// Reporte "Movimientos" (2026-10-29) — ledger completo del programa (todas las tarjetas), sin
// paginar, para exportar/auditar. Mismo permiso que el resto de lectura de ledger (loyalty:read).
// Ruta estática, agrupada junto al resto de rutas de /cards/:id/* de este dominio.
router.get('/ledger/export', authenticate, permit(PERMISSIONS.LOYALTY_READ), asyncHandler(async (req, res) => {
  const {
    fechaInicio, fechaFin, tipo, nivelAplicado, origen, reasonId, usuarioNombre, puntosMin, puntosMax,
  } = req.query;
  const movimientos = await ledgerService.listarMovimientosGlobal({
    fechaInicio, fechaFin, tipo, nivelAplicado, origen, reasonId, usuarioNombre, puntosMin, puntosMax,
  });
  res.json({ movimientos });
}));

// Precarga del formulario de edición (2026-10-23) — trae los campos PII que `GET /cards/:id`
// excluye por defaultScope. Gateada por loyalty:tarjetas:editar (el mismo permiso que ya hace
// falta para el PATCH de abajo), no loyalty:read — ruta estática antes de '/:id' por el mismo
// criterio que movimientos/niveles arriba.
router.get('/cards/:id/editar', authenticate, permit(PERMISSIONS.LOYALTY_TARJETAS_EDITAR), asyncHandler(async (req, res) => {
  res.json(await cardService.obtenerParaEditar(req.params.id));
}));

router.get('/cards/:id', authenticate, permit(PERMISSIONS.LOYALTY_READ), asyncHandler(async (req, res) => {
  res.json(await cardService.obtener(req.params.id));
}));

router.patch('/cards/:id', authenticate, permit(PERMISSIONS.LOYALTY_TARJETAS_EDITAR), asyncHandler(async (req, res) => {
  const {
    nombre, telefono, activa,
    apellidos, apodo, sexo, fechaNacimiento, especialidades, sucursal, calle,
    numExterior, numInterior, colonia, municipio, estado, cp, comentarios,
  } = req.body ?? {};
  res.json(await cardService.editarTarjeta(req.params.id, {
    nombre, telefono, activa,
    apellidos, apodo, sexo, fechaNacimiento, especialidades, sucursal, calle,
    numExterior, numInterior, colonia, municipio, estado, cp, comentarios,
  }, req.user));
}));

router.post('/cards/:id/nip', authenticate, permit(PERMISSIONS.LOYALTY_TARJETAS_EDITAR), asyncHandler(async (req, res) => {
  res.json(await cardService.cambiarNip(req.params.id, req.user));
}));

// Foto del cliente (2026-10-29) — tomada con la cámara del dispositivo del staff, 100% capturada
// en Numo (nunca viene de Kore). Mismo permiso que el PATCH de arriba (loyalty:tarjetas:editar).
router.post('/cards/:id/foto', authenticate, permit(PERMISSIONS.LOYALTY_TARJETAS_EDITAR),
  uploadFotoCliente.single('foto'),
  asyncHandler(async (req, res) => {
    if (!req.file) throw new BadRequestError('No se recibió ningún archivo.');
    res.json(await cardService.subirFoto(req.params.id, req.file, req.user));
  }),
);

// GET /cards/:id/foto (2026-10-29) — binario autenticado de la foto del cliente, proxy hacia
// Drive (bug real corregido: `fotoUrl` es el webViewLink de la página visor, nunca embebible en
// un <img src>). Permiso loyalty:read (no loyalty:tarjetas:editar): ver la foto es lectura, mismo
// criterio que GET /movements/:id/ficha/imagen en bank.routes.js (banks:read, no banks:ficha).
router.get('/cards/:id/foto', authenticate, permit(PERMISSIONS.LOYALTY_READ), asyncHandler(async (req, res) => {
  const { data, mimetype } = await cardService.obtenerFoto(req.params.id);
  res.set('Content-Type', mimetype || 'application/octet-stream');
  res.set('Content-Disposition', 'inline; filename="foto"');
  res.send(data);
}));

// Marcar como revisada (2026-10-29) — contraparte de GET /cards/pendientes-revision-kore de
// arriba. Mismo permiso que el resto de edición de tarjeta (loyalty:tarjetas:editar).
router.patch('/cards/:id/marcar-revisada', authenticate, permit(PERMISSIONS.LOYALTY_TARJETAS_EDITAR), asyncHandler(async (req, res) => {
  res.json(await cardService.marcarRevisada(req.params.id, req.user));
}));

router.put('/cards/:id/nivel', authenticate, permit(PERMISSIONS.LOYALTY_TARJETAS_NIVEL), asyncHandler(async (req, res) => {
  const { nivel, motivo } = req.body ?? {};
  res.json(await cardService.cambiarNivel(req.params.id, nivel, { motivo }, req.user));
}));

// ── Movimientos ───────────────────────────────────────────────────────────────
router.post('/cards/:id/acumulaciones', authenticate, permit(PERMISSIONS.LOYALTY_MOVIMIENTOS), asyncHandler(async (req, res) => {
  const { serieFolio } = req.body ?? {};
  const resultado = await ledgerService.registrarAcumulacion({ cardId: req.params.id, serieFolio }, req.user);
  res.status(201).json(resultado);
}));

router.post('/cards/:id/canjes', authenticate, permit(PERMISSIONS.LOYALTY_MOVIMIENTOS), asyncHandler(async (req, res) => {
  const { puntos, serieFolio } = req.body ?? {};
  const resultado = await ledgerService.registrarCanje({ cardId: req.params.id, puntos, serieFolio }, req.user);
  res.status(201).json(resultado);
}));

// AJUSTES: permiso independiente de LOYALTY_TARJETAS_NIVEL (specs/loyalty-permissions
// "Ajustes and Tarjetas:Nivel Do Not Imply Each Other") — ninguno de los dos
// sustituye al otro, cada uno gatea exclusivamente su propia ruta.
router.post('/cards/:id/ajustes', authenticate, permit(PERMISSIONS.LOYALTY_AJUSTES), asyncHandler(async (req, res) => {
  const { puntos, reasonId, justificacion } = req.body ?? {};
  const resultado = await ledgerService.registrarAjuste({ cardId: req.params.id, puntos, reasonId, justificacion }, req.user);
  res.status(201).json(resultado);
}));

// ── Configuración ─────────────────────────────────────────────────────────────
// NOTA (D10): esto es LOYALTY_CONFIG/LOYALTY_READ, nunca el genérico
// config:manage — ese es admin-only y expone todas las secciones de
// Configuraciones Globales, no solo 'lealtad'.
router.get('/config', authenticate, permit(PERMISSIONS.LOYALTY_READ), asyncHandler(async (req, res) => {
  res.json(await configService.obtenerPorcentajes());
}));

router.put('/config/porcentajes', authenticate, permit(PERMISSIONS.LOYALTY_CONFIG), asyncHandler(async (req, res) => {
  res.json(await configService.guardarPorcentajes(req.body, req.user));
}));

// Especialidades (2026-10-23) — catálogo de referencia, mismo criterio de permisos que arriba:
// LOYALTY_READ para leer, LOYALTY_CONFIG para editar. Fusionado acá (en vez del config:manage
// genérico) para que toda la configuración de Lealtad viva en una sola pantalla.
router.get('/config/especialidades', authenticate, permit(PERMISSIONS.LOYALTY_READ), asyncHandler(async (req, res) => {
  res.json(await configService.obtenerEspecialidades());
}));

router.put('/config/especialidades', authenticate, permit(PERMISSIONS.LOYALTY_CONFIG), asyncHandler(async (req, res) => {
  res.json(await configService.guardarEspecialidades(req.body?.especialidades, req.user));
}));

// Foto de clientes (2026-10-29) — Folder ID de Drive consumido por drive-loyalty-foto.service.js,
// gestionado desde acá por el mismo motivo que Especialidades arriba (sección 'lealtad' oculta del
// riel genérico de Configuraciones Globales).
router.get('/config/foto-folder', authenticate, permit(PERMISSIONS.LOYALTY_READ), asyncHandler(async (req, res) => {
  res.json({ folderId: await configService.obtenerFotoFolderId() });
}));

router.put('/config/foto-folder', authenticate, permit(PERMISSIONS.LOYALTY_CONFIG), asyncHandler(async (req, res) => {
  res.json({ folderId: await configService.guardarFotoFolderId(req.body?.folderId, req.user) });
}));

// Historial de un valor de configuración de Lealtad (2026-10-29) — el audit log YA se escribe en
// cada guardado (ver global-config.service.js#setValue), esto solo expone la UI para verlo desde
// Lealtad sin pedirle config:manage al staff. Mismo permiso que YA gatea guardar estos valores
// (LOYALTY_CONFIG), nunca el config:manage genérico de GET /api/config/:configId/audit.
router.get('/config/historial/:clave', authenticate, permit(PERMISSIONS.LOYALTY_CONFIG), asyncHandler(async (req, res) => {
  res.json(await configService.obtenerHistorial(req.params.clave));
}));

router.get('/reasons', authenticate, permit(PERMISSIONS.LOYALTY_READ), asyncHandler(async (req, res) => {
  res.json(await configService.listarReasons({ soloActivos: req.query.soloActivos === 'true' }));
}));

router.post('/reasons', authenticate, permit(PERMISSIONS.LOYALTY_CONFIG), asyncHandler(async (req, res) => {
  res.status(201).json(await configService.crearReason(req.body ?? {}));
}));

router.patch('/reasons/:id', authenticate, permit(PERMISSIONS.LOYALTY_CONFIG), asyncHandler(async (req, res) => {
  res.json(await configService.editarReason(req.params.id, req.body ?? {}));
}));

router.get('/sucursales', authenticate, permit(PERMISSIONS.LOYALTY_READ), asyncHandler(async (req, res) => {
  res.json(await configService.listarSucursales());
}));

router.patch('/sucursales/:id', authenticate, permit(PERMISSIONS.LOYALTY_CONFIG), asyncHandler(async (req, res) => {
  res.json(await configService.actualizarSucursal(req.params.id, req.body ?? {}));
}));

// Promociones (bono extra sobre acumulación, por periodo — ej. "Buen Fin").
router.get('/promociones', authenticate, permit(PERMISSIONS.LOYALTY_READ), asyncHandler(async (req, res) => {
  res.json(await configService.listarPromociones({ soloActivas: req.query.soloActivas === 'true' }));
}));

router.post('/promociones', authenticate, permit(PERMISSIONS.LOYALTY_CONFIG), asyncHandler(async (req, res) => {
  res.status(201).json(await configService.crearPromocion(req.body ?? {}));
}));

router.patch('/promociones/:id', authenticate, permit(PERMISSIONS.LOYALTY_CONFIG), asyncHandler(async (req, res) => {
  res.json(await configService.editarPromocion(req.params.id, req.body ?? {}));
}));

// Catálogos de Kore (conceptos/formas de pago) espejados para configurar qué
// ventas acumulan puntos (2026-10-26) — `acumulaPuntos`/`esPuntos` son
// SIEMPRE manuales, un sync nunca los pisa (ver loyalty-config.service.js).
router.get('/config/conceptos-kore', authenticate, permit(PERMISSIONS.LOYALTY_READ), asyncHandler(async (req, res) => {
  res.json(await configService.listarConceptosKore({ soloActivos: req.query.soloActivos === 'true' }));
}));

router.post('/config/conceptos-kore/sync', authenticate, permit(PERMISSIONS.LOYALTY_CONFIG), asyncHandler(async (req, res) => {
  res.json(await configService.sincronizarConceptosKore());
}));

router.patch('/config/conceptos-kore/:id', authenticate, permit(PERMISSIONS.LOYALTY_CONFIG), asyncHandler(async (req, res) => {
  res.json(await configService.actualizarConceptoKore(req.params.id, req.body ?? {}));
}));

router.get('/config/formas-pago-kore', authenticate, permit(PERMISSIONS.LOYALTY_READ), asyncHandler(async (req, res) => {
  res.json(await configService.listarFormasPagoKore({ soloActivos: req.query.soloActivos === 'true' }));
}));

router.post('/config/formas-pago-kore/sync', authenticate, permit(PERMISSIONS.LOYALTY_CONFIG), asyncHandler(async (req, res) => {
  res.json(await configService.sincronizarFormasPagoKore());
}));

router.patch('/config/formas-pago-kore/:id', authenticate, permit(PERMISSIONS.LOYALTY_CONFIG), asyncHandler(async (req, res) => {
  res.json(await configService.actualizarFormaPagoKore(req.params.id, req.body ?? {}));
}));

module.exports = router;
