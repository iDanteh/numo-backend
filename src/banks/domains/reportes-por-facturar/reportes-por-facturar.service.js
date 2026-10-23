'use strict';

/**
 * reportes-por-facturar.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Reporte "Por facturar" (Reportes, 2026-10-22, pedido del usuario): lo mismo
 * que la pestaña MOSTRADOR de la póliza de Ingreso, pero sin generar póliza —
 * por día o rango de fechas y por sucursal, con historial de quién lo creó.
 *
 *   • EN FACTURA GLOBAL / PPD POR FACTURAR YA FACTURADO A NOMBRE DEL CLIENTE:
 *     facturas de Ingreso del rango (misma regla de fecha que la póliza,
 *     `_uuidsPorFechaEfectiva`) pagadas con depósitos de Bancos de DÍAS
 *     ANTERIORES a la factura — el equivalente sin póliza de la "Venta Sin
 *     Cobro" que alimenta la hoja (ver mostrador.service.js). Puede variar
 *     un poco respecto a la póliza (allá decide el generador).
 *   • PENDIENTE GENERAR FACTURA: depósitos del rango con remisiones PPD,
 *     saldo 0, "POR FACTURAR" y sin factura (detectarDepositosPendientesFactura).
 *     Incluye los de fecha anterior identificados dentro del rango (columna
 *     IDENTIFICADO EL, 2026-10-23); lo mismo CON PENDIENTES DE PAGO.
 */

const ExcelJS = require('exceljs');
const { ReportePorFacturar, User } = require('../../../shared/models/postgres');
const CFDI         = require('../../../visor/models/CFDI');
const BankMovement = require('../banks/BankMovement.model');
const centrosSvc   = require('../centros-costo/centros-costo.service');
const { _uuidsPorFechaEfectiva } = require('../cfdi-mapping/cfdi-poliza-generator.service');
const { detectarMovimientosMostrador, detectarDepositosPendientesFactura, detectarDepositosEfectivoSinAsociar } = require('../cfdi-mapping/mostrador.service');
const { agregarHojaMostrador, agregarHojaDepositosEfectivo, agregarHojaPendientesPago, tablasMostrador } = require('../cfdi-mapping/mostrador-hoja');
const { BadRequestError, NotFoundError } = require('../../shared/errors/AppError');

const MAX_DIAS_RANGO = 31;
const FECHA_RE = /^\d{4}-\d{2}-\d{2}$/;

const _r2 = n => Math.round((Number(n) || 0) * 100) / 100;

function _diasDelRango(fechaInicio, fechaFin) {
  const dias = [];
  for (let d = new Date(`${fechaInicio}T00:00:00Z`); d <= new Date(`${fechaFin}T00:00:00Z`); d = new Date(d.getTime() + 86400000)) {
    dias.push(d.toISOString().slice(0, 10));
  }
  return dias;
}

// Día "ingenuo" (YYYY-MM-DD) de la fecha de un depósito de Bancos — `fecha` es
// el día del estado de cuenta a medianoche UTC (ver cargarFichasCobranza).
const _diaDeposito = f => (f ? new Date(f).toISOString().slice(0, 10) : null);

/**
 * Facturas de Ingreso de la sucursal en el rango, con el día efectivo de cada
 * una — mismo universo por fecha que la póliza (`_uuidsPorFechaEfectiva`).
 */
async function _facturasDelRango({ rfc, series, fechaInicio, fechaFin }) {
  const diaPorUuid = new Map();
  for (const dia of _diasDelRango(fechaInicio, fechaFin)) {
    const [ejercicio, periodo] = dia.split('-').map(Number);
    const uuids = await _uuidsPorFechaEfectiva({ rfc, ejercicio, periodo, tipoCfdi: 'I', fechaInicio: dia, fechaFin: dia });
    for (const u of uuids) if (!diaPorUuid.has(u)) diaPorUuid.set(u, dia);
  }
  if (!diaPorUuid.size) return [];

  const docs = await CFDI.find({ uuid: { $in: [...diaPorUuid.keys()] } })
    .select('uuid serie folio fecha total receptor satStatus source').lean();
  const seriesSet = new Set(series);
  const porUuid = new Map();
  for (const c of docs) {
    const u = c.uuid.toUpperCase();
    if (!seriesSet.has(String(c.serie ?? '').trim().toUpperCase())) continue;
    // Una por UUID; la copia ERP manda (es la que trae la fecha real).
    if (!porUuid.has(u) || c.source === 'ERP') porUuid.set(u, { ...c, uuid: u, dia: diaPorUuid.get(u) });
  }
  return [...porUuid.values()].filter(c => c.satStatus !== 'Cancelado');
}

/**
 * Facturas pagadas con depósitos de Bancos de días anteriores a la factura
 * (las "de mostrador") → mismas entradas que guarda la póliza en
 * `movimientosMostrador`.
 */
async function _movimientosMostradorSinPoliza({ rfc, facturas }) {
  if (!facturas.length) return [];
  const uuids = facturas.map(f => f.uuid);
  const bancos = await BankMovement.find({
    'erpLinks.folioFiscal': { $in: [...uuids, ...uuids.map(u => u.toLowerCase())] },
    isActive: { $ne: false }, deposito: { $gt: 0 },
  }).select('fecha erpLinks').lean();

  const diaPorUuid = new Map(facturas.map(f => [f.uuid, f.dia]));
  const conDepositoAnterior = new Set();
  for (const b of bancos) {
    const diaDep = _diaDeposito(b.fecha);
    for (const l of b.erpLinks ?? []) {
      const u = (l.folioFiscal ?? '').toUpperCase();
      if (diaPorUuid.has(u) && diaDep && diaDep < diaPorUuid.get(u)) conDepositoAnterior.add(u);
    }
  }
  if (!conDepositoAnterior.size) return [];

  // Mismo formato que la póliza: una línea "Venta Sin Cobro" por factura.
  const movimientos = [...conDepositoAnterior].map(u => ({ tipoOrigen: 'Venta Sin Cobro', debe: 1, cfdiUuid: u }));
  return detectarMovimientosMostrador({ rfc, cfdis: facturas.filter(f => conDepositoAnterior.has(f.uuid)), movimientos });
}

function _resumen(datos) {
  const t = tablasMostrador(datos);
  const deps = fs => fs.flatMap(f => f.depositos ?? []);
  const suma = ds => _r2(ds.reduce((s, d) => s + (Number(d.importe) || 0), 0));
  return {
    facturasGlobales:    { facturas: t.facturasGlobales.length, depositos: deps(t.facturasGlobales).length, importe: suma(deps(t.facturasGlobales)) },
    ppdFacturadoCliente: { facturas: t.ppdFacturadoCliente.length, depositos: deps(t.ppdFacturadoCliente).length, importe: suma(deps(t.ppdFacturadoCliente)) },
    depositosPendientes: { depositos: t.depositosPendientes.length, importe: suma(t.depositosPendientes) },
    depositosEfectivo:   { depositos: (datos.depositosEfectivoSinAsociar ?? []).length, importe: suma(datos.depositosEfectivoSinAsociar ?? []) },
    pendientesPago:      { depositos: (datos.depositosPendientesPago ?? []).length, importe: suma(datos.depositosPendientesPago ?? []) },
  };
}

// Calcula el contenido del reporte (sin guardarlo) — lo usan generar y actualizar.
async function _calcular({ rfc, centroCostoId, fechaInicio, fechaFin }) {
  fechaFin = fechaFin || fechaInicio;
  if (!rfc) throw new BadRequestError('RFC requerido');
  if (!FECHA_RE.test(fechaInicio ?? '') || !FECHA_RE.test(fechaFin ?? '')) throw new BadRequestError('Fechas inválidas (formato YYYY-MM-DD)');
  if (fechaFin < fechaInicio) throw new BadRequestError('La fecha final no puede ser anterior a la inicial');
  if (_diasDelRango(fechaInicio, fechaFin).length > MAX_DIAS_RANGO) throw new BadRequestError(`El rango no puede pasar de ${MAX_DIAS_RANGO} días`);

  const ccBySerieMap = await centrosSvc.resolveBySerieMap();
  const series = Object.entries(ccBySerieMap)
    .filter(([, c]) => !centroCostoId || String(c.id) === String(centroCostoId))
    .map(([serie]) => serie.trim().toUpperCase());
  if (!series.length) throw new BadRequestError('La sucursal no tiene serie de facturación configurada');
  const sucursal = centroCostoId
    ? Object.values(ccBySerieMap).find(c => String(c.id) === String(centroCostoId))?.sucursal ?? null
    : 'Todas';

  const advertencias = [];
  const facturas = await _facturasDelRango({ rfc, series, fechaInicio, fechaFin });
  const movimientosMostrador = await _movimientosMostradorSinPoliza({ rfc, facturas });
  // También los de fecha anterior identificados dentro del rango (2026-10-23).
  const pendientes = await detectarDepositosPendientesFactura({ rfc, series, fechaInicio, fechaFin, incluirIdentificados: true });
  if (pendientes.remisionesSinRespuesta) {
    advertencias.push(`Kore no respondió ${pendientes.remisionesSinRespuesta} remisión(es) — "PENDIENTE GENERAR FACTURA" puede estar incompleta; vuelve a generarlo más tarde.`);
  }

  const depositosEfectivoSinAsociar = await detectarDepositosEfectivoSinAsociar({ fechaInicio, fechaFin });
  const datos = {
    movimientosMostrador,
    depositosPendientesFactura: pendientes.depositos,
    depositosPendientesPago:    pendientes.depositosConSaldo ?? [],
    depositosEfectivoSinAsociar,
  };
  return { sucursal, fechaFin, datos, resumen: _resumen(datos), advertencias: advertencias.length ? advertencias : null };
}

const _userId = user => (user?.dbId ? parseInt(user.dbId, 10) : null);

async function generar({ rfc, centroCostoId, fechaInicio, fechaFin }, user) {
  const calc = await _calcular({ rfc, centroCostoId, fechaInicio, fechaFin });
  const reporte = await ReportePorFacturar.create({
    rfc, centroCostoId: centroCostoId ? Number(centroCostoId) : null, fechaInicio, ...calc,
    creadoPorId: _userId(user),
  });
  return obtener(reporte.id);
}

/**
 * Actualizar (2026-10-23, pedido del usuario): vuelve a consultar Bancos y
 * Kore con la misma sucursal y fechas y reemplaza los datos del MISMO reporte
 * (no crea otro); guarda quién y cuándo lo actualizó.
 */
async function actualizar(id, user) {
  const r = await ReportePorFacturar.findByPk(id);
  if (!r || r.eliminadoEn) throw new NotFoundError('Reporte');
  const { sucursal, datos, resumen, advertencias } = await _calcular({
    rfc: r.rfc, centroCostoId: r.centroCostoId, fechaInicio: r.fechaInicio, fechaFin: r.fechaFin,
  });
  await r.update({ sucursal, datos, resumen, advertencias, actualizadoPorId: _userId(user), actualizadoEn: new Date() });
  return obtener(r.id);
}

const _incluirCreador = [
  { model: User, as: 'creadoPor', attributes: ['nombre', 'email'] },
  { model: User, as: 'actualizadoPor', attributes: ['nombre', 'email'] },
];

// Historial paginado, más recientes primero (5 por página en la pantalla).
async function listar({ rfc, page = 1, limit = 5 } = {}) {
  const lim  = Math.min(Math.max(parseInt(limit, 10) || 5, 1), 100);
  const pag  = Math.max(parseInt(page, 10) || 1, 1);
  const { rows, count } = await ReportePorFacturar.findAndCountAll({
    where:      { ...(rfc ? { rfc } : {}), eliminadoEn: null },
    attributes: { exclude: ['datos'] },
    include:    _incluirCreador,
    order:      [['createdAt', 'DESC'], ['id', 'DESC']],
    limit:      lim,
    offset:     (pag - 1) * lim,
  });
  return { data: rows, total: count, page: pag, limit: lim };
}

async function obtener(id) {
  const r = await ReportePorFacturar.findByPk(id, { include: _incluirCreador });
  if (!r || r.eliminadoEn) throw new NotFoundError('Reporte');
  const json = r.toJSON();
  return {
    ...json,
    tablas: {
      ...tablasMostrador(json.datos),
      pendientesPago:    json.datos?.depositosPendientesPago ?? [],
      depositosEfectivo: json.datos?.depositosEfectivoSinAsociar ?? [],
    },
  };
}

/** Borrado lógico: deja de verse en el historial, guarda quién y cuándo. */
async function eliminar(id, user) {
  const r = await ReportePorFacturar.findByPk(id);
  if (!r || r.eliminadoEn) throw new NotFoundError('Reporte');
  await r.update({ eliminadoEn: new Date(), eliminadoPorId: _userId(user) });
  return { ok: true, id: r.id };
}

async function excel(id) {
  const r = await obtener(id);
  const workbook = new ExcelJS.Workbook();
  if (!agregarHojaMostrador(workbook, r.datos, 'POR FACTURAR', { columnaIdentificado: true })) {
    workbook.addWorksheet('POR FACTURAR').addRow(['Sin facturas de mostrador ni depósitos pendientes de facturar en el rango.']);
  }
  agregarHojaPendientesPago(workbook, r.datos?.depositosPendientesPago);
  agregarHojaDepositosEfectivo(workbook, r.datos?.depositosEfectivoSinAsociar);
  const rango = r.fechaInicio === r.fechaFin ? r.fechaInicio : `${r.fechaInicio}_${r.fechaFin}`;
  const sucursal = String(r.sucursal ?? 'Todas').replace(/[^\w-]+/g, '_');
  return { buffer: await workbook.xlsx.writeBuffer(), filename: `Por_facturar_${sucursal}_${rango}.xlsx` };
}

module.exports = { generar, actualizar, listar, obtener, eliminar, excel, _movimientosMostradorSinPoliza, _facturasDelRango };
