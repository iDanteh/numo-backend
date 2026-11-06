'use strict';

// Reporte "Por facturar" (Reportes, 2026-10-22) — ver reportes-por-facturar.service.js.
const express = require('express');
const { authenticate, permit } = require('../../shared/middleware/auth.real');
const { asyncHandler }         = require('../../shared/middleware/error-handler');
const service                  = require('./reportes-por-facturar.service');
const estadistica              = require('./reportes-por-facturar.estadistica');
const fichas                   = require('./reportes-por-facturar.fichas');
const fichasGuardadas          = require('./reportes-por-facturar.fichas-guardadas');
const rbacStore                = require('../../../shared/services/rbac-store');
const { BadRequestError, ForbiddenError, NotFoundError } = require('../../../shared/errors/AppError');

const router = express.Router();

// Permiso propio del reporte (ver rbac.js REPORTES_POR_FACTURAR) — `visor:reports`
// ya no basta (2026-10-23, pedido del usuario).
const PERM = 'reportes:por-facturar';

// GET /api/reportes-por-facturar?rfc=&page=&limit= — historial paginado (sin los datos)
router.get('/', authenticate, permit(PERM), asyncHandler(async (req, res) => {
  const { rfc, page, limit } = req.query;
  res.json(await service.listar({ rfc, page, limit }));
}));

// POST /api/reportes-por-facturar { rfc, centroCostoId?, fechaInicio, fechaFin? }
// → el reporte guardado con estado 'calculando'; se completa en segundo plano (2026-10-29).
router.post('/', authenticate, permit(PERM), asyncHandler(async (req, res) => {
  const { rfc, centroCostoId, fechaInicio, fechaFin } = req.body ?? {};
  res.status(202).json(await service.generar({ rfc, centroCostoId, fechaInicio, fechaFin }, req.user));
}));

// ── Estadística por facturar (2026-10-29) — antes de /:id ────────────────────
// POST /api/reportes-por-facturar/estadistica { rfc, centroCostoId?, fechaInicio, fechaFin? } → { id } (corre en segundo plano)
router.post('/estadistica', authenticate, permit(PERM), asyncHandler(async (req, res) => {
  const { rfc, centroCostoId, fechaInicio, fechaFin } = req.body ?? {};
  estadistica.validar({ rfc, fechaInicio, fechaFin });
  res.status(202).json(estadistica.iniciar({ rfc, centroCostoId, fechaInicio, fechaFin }, req.user));
}));

// Mes en curso ya calculado (2026-10-30): se ve al instante al entrar.
// GET /api/reportes-por-facturar/estadistica/mes-en-curso?rfc=&centroCostoId= → { resultado, calculadoEn, calculando, avance, error }
router.get('/estadistica/mes-en-curso', authenticate, permit(PERM), asyncHandler(async (req, res) => {
  res.json(await estadistica.fotoActual(req.query.rfc, req.query.centroCostoId));
}));

// POST /api/reportes-por-facturar/estadistica/mes-en-curso/actualizar { rfc, centroCostoId? } — recalcular ya
router.post('/estadistica/mes-en-curso/actualizar', authenticate, permit(PERM), asyncHandler(async (req, res) => {
  const { rfc, centroCostoId } = req.body ?? {};
  estadistica.recalcularFoto(rfc, centroCostoId);
  res.status(202).json(await estadistica.fotoActual(rfc, centroCostoId));
}));

// GET /api/reportes-por-facturar/estadistica/mes-en-curso/excel?rfc=&centroCostoId=
router.get('/estadistica/mes-en-curso/excel', authenticate, permit(PERM), asyncHandler(async (req, res) => {
  const { buffer, filename } = await estadistica.excelFoto(req.query.rfc, req.query.centroCostoId);
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  res.send(Buffer.from(buffer));
}));

// GET /api/reportes-por-facturar/estadistica/:id — avance o resultado
router.get('/estadistica/:id', authenticate, permit(PERM), asyncHandler(async (req, res) => {
  res.json(estadistica.estado(req.params.id));
}));

// GET /api/reportes-por-facturar/estadistica/:id/excel
router.get('/estadistica/:id/excel', authenticate, permit(PERM), asyncHandler(async (req, res) => {
  const { buffer, filename } = await estadistica.excel(req.params.id);
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  res.send(Buffer.from(buffer));
}));

// ── Fichas de efectivo (2026-11-04) — antes de /:id ──────────────────────────
// Permiso propio que solo un administrador puede asignar (rbac.js REPORTES_FICHAS_EFECTIVO).
const PERM_FICHAS = 'reportes:fichas-efectivo';
// Anexo (detalle de cada venta): solo administrador ('*') o este permiso (2026-11-05).
const PERM_ANEXO = 'reportes:fichas-efectivo:anexo';
const _puedeAnexo = req => rbacStore.hasPermission(req.user?.role, PERM_ANEXO, req.user?.extraPermissions);
/**
 * Sucursales que puede consultar el usuario en Fichas de efectivo (2026-11-05):
 * null = todas (casilla "Todas"); si no, Set de ids — vacío = NINGUNA (2026-11-06,
 * pedido del usuario). Aplica también al administrador.
 */
async function _sucursalesPermitidas(req) {
  if (req.user?.todasSucursales === true) return null;
  return new Set((req.user?.sucursalIds ?? []).map(Number).filter(Number.isInteger));
}
/** 403 si la sucursal pedida no es de las suyas. */
async function _exigirSucursal(req, centroCostoId) {
  const permitidas = await _sucursalesPermitidas(req);
  if (permitidas && !permitidas.size) throw new ForbiddenError('No tienes sucursales asignadas; pide al administrador que te asigne');
  if (permitidas && !permitidas.has(Number(centroCostoId))) {
    throw new ForbiddenError('No tienes acceso a esa sucursal');
  }
  return permitidas;
}

/** Quita el anexo (detalle / coincidencias) si el usuario no puede verlo. */
async function _sinAnexoSiNoPuede(req, data) {
  if (await _puedeAnexo(req)) return data;
  const quitar = d => (d && typeof d === 'object') ? (({ detalle, coincidencias, ...resto }) => resto)(d) : d;
  return Array.isArray(data) ? data.map(quitar) : quitar(data);
}

// GET /api/reportes-por-facturar/fichas/globales?rfc=&centroCostoId=&fechaInicio=&fechaFin= → facturas globales del periodo
router.get('/fichas/globales', authenticate, permit(PERM_FICHAS), asyncHandler(async (req, res) => {
  const { rfc, centroCostoId, fechaInicio, fechaFin } = req.query;
  await _exigirSucursal(req, centroCostoId);
  res.json(await fichas.listarGlobales({ rfc, centroCostoId: centroCostoId || null, fechaInicio, fechaFin }));
}));

// GET /api/reportes-por-facturar/fichas/bancos → bancos dados de alta en Bancos (con número de cuenta)
router.get('/fichas/bancos', authenticate, permit(PERM_FICHAS), asyncHandler(async (req, res) => {
  res.json(await fichas.listarBancos());
}));

// POST /api/reportes-por-facturar/fichas { rfc, centroCostoId, fechaInicio, fechaFin } → efectivo de la caja de la sucursal en cada día del rango y la suma
router.post('/fichas', authenticate, permit(PERM_FICHAS), asyncHandler(async (req, res) => {
  const { rfc, centroCostoId, fechaInicio, fechaFin } = req.body ?? {};
  await _exigirSucursal(req, centroCostoId);
  res.json(await _sinAnexoSiNoPuede(req, await fichas.calcularFicha({ rfc, centroCostoId, fechaInicio, fechaFin })));
}));

// ── Fichas guardadas (2026-11-05): rastro venta → factura / global → ficha ──
// POST /api/reportes-por-facturar/fichas/guardar { rfc, centroCostoId, fechaInicio, fechaFin, bancos, sucursalBanco, denominaciones }
router.post('/fichas/guardar', authenticate, permit(PERM_FICHAS), asyncHandler(async (req, res) => {
  await _exigirSucursal(req, req.body?.centroCostoId);
  res.status(201).json(await _sinAnexoSiNoPuede(req, await fichasGuardadas.guardar(req.body ?? {}, req.user)));
}));

// GET /api/reportes-por-facturar/fichas/guardadas?rfc=&centroCostoId=&q= (q = folio de ficha, remisión o factura / global)
router.get('/fichas/guardadas', authenticate, permit(PERM_FICHAS), asyncHandler(async (req, res) => {
  const { rfc, centroCostoId, q } = req.query;
  const permitidas = centroCostoId ? await _exigirSucursal(req, centroCostoId) : await _sucursalesPermitidas(req);
  const centroCostoIds = permitidas ? [...permitidas] : null;
  res.json(await _sinAnexoSiNoPuede(req, await fichasGuardadas.listar({ rfc, centroCostoId, centroCostoIds, q })));
}));

// GET /api/reportes-por-facturar/fichas/guardadas/:id?rfc=
router.get('/fichas/guardadas/:id', authenticate, permit(PERM_FICHAS), asyncHandler(async (req, res) => {
  const ficha = await fichasGuardadas.obtener({ rfc: req.query.rfc, id: req.params.id });
  const permitidas = await _sucursalesPermitidas(req);
  if (permitidas && !permitidas.has(Number(ficha.centroCostoId))) throw new NotFoundError('Ficha no encontrada');
  res.json(await _sinAnexoSiNoPuede(req, ficha));
}));

// Anexo › Actualizar (2026-11-06): ¿las ventas sin factura ya se facturaron? Solo quien ve el anexo.
// POST /api/reportes-por-facturar/fichas/anexo/actualizar { rfc, centroCostoId, detalle } — ficha generada sin guardar
router.post('/fichas/anexo/actualizar', authenticate, permit(PERM_FICHAS), asyncHandler(async (req, res) => {
  if (!(await _puedeAnexo(req))) throw new ForbiddenError('No tienes acceso al anexo');
  const { rfc, centroCostoId, detalle } = req.body ?? {};
  await _exigirSucursal(req, centroCostoId);
  if (!Array.isArray(detalle) || detalle.length > 20000) throw new BadRequestError('Detalle inválido');
  res.json(await fichas.actualizarFacturasDetalle({ rfc, detalle }));
}));

// POST /api/reportes-por-facturar/fichas/guardadas/:id/actualizar-anexo { rfc } — ficha guardada (se guarda la factura nueva)
router.post('/fichas/guardadas/:id/actualizar-anexo', authenticate, permit(PERM_FICHAS), asyncHandler(async (req, res) => {
  if (!(await _puedeAnexo(req))) throw new ForbiddenError('No tienes acceso al anexo');
  const ficha = await fichasGuardadas.obtener({ rfc: req.body?.rfc, id: req.params.id });
  const permitidas = await _sucursalesPermitidas(req);
  if (permitidas && !permitidas.has(Number(ficha.centroCostoId))) throw new NotFoundError('Ficha no encontrada');
  res.json(await fichasGuardadas.actualizarAnexo({ rfc: req.body?.rfc, id: req.params.id }));
}));

// POST /api/reportes-por-facturar/fichas/guardadas/:id/cancelar { rfc, motivo } — solo administrador
router.post('/fichas/guardadas/:id/cancelar', authenticate, permit(PERM_FICHAS), asyncHandler(async (req, res) => {
  const { rfc, motivo } = req.body ?? {};
  res.json(await _sinAnexoSiNoPuede(req, await fichasGuardadas.cancelar({ rfc, id: req.params.id, motivo }, req.user)));
}));

// GET /api/reportes-por-facturar/:id — reporte con sus tablas
router.get('/:id', authenticate, permit(PERM), asyncHandler(async (req, res) => {
  res.json(await service.obtener(req.params.id));
}));

// POST /api/reportes-por-facturar/:id/actualizar — recalcula el mismo reporte (misma sucursal y fechas)
router.post('/:id/actualizar', authenticate, permit(PERM), asyncHandler(async (req, res) => {
  res.json(await service.actualizar(req.params.id, req.user));
}));

// DELETE /api/reportes-por-facturar/:id — borrado lógico (guarda quién lo eliminó)
router.delete('/:id', authenticate, permit(PERM), asyncHandler(async (req, res) => {
  res.json(await service.eliminar(req.params.id, req.user));
}));

// GET /api/reportes-por-facturar/:id/excel?seccion= — una pestaña (mostrador, ref, especiales, ficha, efectivo) o todas
router.get('/:id/excel', authenticate, permit(PERM), asyncHandler(async (req, res) => {
  const { buffer, filename } = await service.excel(req.params.id, req.query.seccion || undefined);
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  res.send(Buffer.from(buffer));
}));

module.exports = router;
