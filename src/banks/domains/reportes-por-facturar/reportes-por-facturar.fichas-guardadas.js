'use strict';

/**
 * reportes-por-facturar.fichas-guardadas.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Fichas de efectivo GUARDADAS (2026-11-05): al imprimir, la ficha se vuelve a
 * calcular en el servidor (no se confía en lo que manda la pantalla), se
 * guarda con folio
 * propio y el detalle de cada venta. Historial con buscador por folio de
 * ficha, remisión o factura / global; cancelar (solo administrador) libera
 * las cajas para volver a generarlas.
 */

const FichaEfectivo = require('./FichaEfectivo.model');
const Counter = require('../../shared/models/Counter');
const { esAdmin } = require('../users/permisos-solo-admin');
const { BadRequestError, NotFoundError, ConflictError, ForbiddenError } = require('../../shared/errors/AppError');

/**
 * Folio de 5 dígitos = MES (2) + CONSECUTIVO DEL MES (3) — 2026-11-05, decisión
 * del usuario: noviembre 11001, 11002…; diciembre 12001…; enero 01001…
 * Hasta 999 fichas por mes. Con 5 dígitos no cabe el año: el folio es único
 * dentro de cada año (índice único año + folio) y nunca se reusa — una ficha
 * cancelada conserva el suyo. El mes es el de México al guardar.
 */
const POR_MES_MAX = 999;
const folioTexto = n => String(n ?? '').padStart(5, '0');
const _anioMesMx = (d = new Date()) => {
  const [anio, mes] = d.toLocaleDateString('en-CA', { timeZone: 'America/Mexico_City' }).split('-').map(Number);
  return { anio, mes };
};

// Índices de versiones previas que impedirían repetir el folio en otro año.
let _indicesViejosQuitados = false;
async function _quitarIndicesViejos() {
  if (_indicesViejosQuitados) return;
  _indicesViejosQuitados = true;
  for (const nombre of ['rfc_1_folio_1', 'folio_1']) {
    await FichaEfectivo.collection.dropIndex(nombre).catch(() => {});
  }
}

/** Siguiente folio del mes: MM001, MM002… */
async function _siguienteFolio(ahora = new Date()) {
  await _quitarIndicesViejos();
  const { anio, mes } = _anioMesMx(ahora);
  const { seq } = await Counter.nextSeq(`fichaEfectivo_${anio}${String(mes).padStart(2, '0')}`);
  if (seq > POR_MES_MAX) throw new ConflictError(`Se acabaron los folios de este mes (máximo ${POR_MES_MAX} fichas por mes)`);
  return { anio, folio: mes * 1000 + seq };
}

const _usuario = u => ({ id: u?.dbId ?? u?._id ?? null, nombre: u?.nombre ?? u?.email ?? null });

/** Fichas vigentes que ya tienen alguna de estas cajas (`SERIE|DIA`). */
async function cajasYaGuardadas({ rfc, cajas }) {
  if (!cajas?.length) return [];
  const docs = await FichaEfectivo.find({ rfc, estado: 'vigente', cajas: { $in: cajas } }).select('folio cajas').lean();
  const quiero = new Set(cajas);
  return docs.flatMap(d => d.cajas.filter(c => quiero.has(c)).map(caja => ({ caja, folio: d.folio, id: String(d._id) })));
}

/**
 * Recalcula la ficha y la guarda con folio. 409 si alguna caja ya está en otra
 * ficha vigente o si Kore no respondió alguna caja.
 * 2026-11-17 (pedido del usuario): ya no se elige banco ni se captura la
 * denominación — la ficha lleva TODOS los bancos de Bancos (con su cuenta, sin
 * importe; se llena a mano).
 */
async function guardar({ rfc, centroCostoId, fechaInicio, fechaFin }, user) {
  const { calcularFicha, listarBancos } = require('./reportes-por-facturar.fichas');
  const bancos = (await listarBancos()).map(b => ({ banco: b.banco, numeroCuenta: b.numeroCuenta ?? null, importe: null }));

  const ficha = await calcularFicha({ rfc, centroCostoId, fechaInicio, fechaFin });
  if (ficha.incompleta) throw new ConflictError('Kore no respondió alguna caja; vuelve a generar la ficha antes de guardarla.');
  if (!ficha.cortes.length) throw new BadRequestError('No hay efectivo en la caja en esas fechas');
  if (ficha.yaEnFicha.length) {
    const folios = [...new Set(ficha.yaEnFicha.map(x => folioTexto(x.folio)))].join(', ');
    throw new ConflictError(`Ya hay días de esta sucursal en la ficha ${folios}. Cancélala o cambia las fechas.`);
  }

  // Índice único en folio: si dos se guardan al mismo tiempo y chocan, se toma el siguiente.
  let doc;
  for (let intento = 1; !doc; intento++) {
    const { anio, folio } = await _siguienteFolio();
    try {
      doc = await FichaEfectivo.create({
    folio, anio, rfc, centroCostoId: ficha.centroCostoId, sucursal: ficha.sucursal, series: ficha.series,
    fechaInicio: ficha.fechaInicio, fechaFin: ficha.fechaFin,
    cajas: ficha.cortes.map(c => `${c.serie}|${c.dia}`),
    bancos, sucursalBanco: null,
    denominaciones: [], totalContado: null, diferencia: null,
    totales: ficha.totales, cortes: ficha.cortes, detalle: ficha.detalle,
    creadoPor: _usuario(user),
      });
    } catch (err) {
      if (err?.code !== 11000 || intento >= 3) throw err;
    }
  }
  return doc.toObject();
}

/**
 * Historial. `q`: folio de ficha, remisión (C0-261100086) o factura / global
 * (A0-261100676); sin q, las últimas.
 */
async function listar({ rfc, centroCostoId, centroCostoIds = null, q, limite = 100 }) {
  if (!rfc) throw new BadRequestError('RFC requerido');
  const filtro = { rfc };
  if (centroCostoId != null && centroCostoId !== '') filtro.centroCostoId = Number(centroCostoId);
  // Usuario con sucursales asignadas (2026-11-05): solo las suyas.
  else if (Array.isArray(centroCostoIds)) filtro.centroCostoId = { $in: centroCostoIds.map(Number) };
  const texto = String(q ?? '').trim().toUpperCase().replace(/\s+/g, '');
  if (texto) {
    const or = [{ 'detalle.remision': texto }, { 'detalle.factura': texto }];
    if (/^\d+$/.test(texto)) or.push({ folio: Number(texto) });
    filtro.$or = or;
  }
  const docs = await FichaEfectivo.find(filtro).sort({ createdAt: -1 }).limit(Math.min(Number(limite) || 100, 500))
    .select('folio sucursal series fechaInicio fechaFin bancos totales.totalEntregar totalContado estado creadoPor createdAt canceladaEn motivoCancelacion' + (texto ? ' detalle' : ''))
    .lean();
  return docs.map(d => {
    const { detalle, ...resto } = d;
    const coincidencias = texto ? (detalle ?? []).filter(x => x.remision === texto || x.factura === texto) : undefined;
    return { ...resto, id: String(d._id), ...(coincidencias ? { coincidencias } : {}) };
  });
}

async function obtener({ rfc, id }) {
  const doc = await FichaEfectivo.findOne({ _id: id, rfc }).lean().catch(() => null);
  if (!doc) throw new NotFoundError('Ficha no encontrada');
  return { ...doc, id: String(doc._id) };
}

/**
 * Anexo › Actualizar de una ficha guardada (2026-11-06): pone la factura a las
 * ventas que ya se facturaron después y lo guarda (el buscador del historial
 * las encuentra por esa factura). Los importes no cambian.
 */
async function actualizarAnexo({ rfc, id }) {
  const { actualizarFacturasDetalle } = require('./reportes-por-facturar.fichas');
  const doc = await FichaEfectivo.findOne({ _id: id, rfc }).catch(() => null);
  if (!doc) throw new NotFoundError('Ficha no encontrada');
  const detalle = (doc.detalle ?? []).map(d => (d.toObject ? d.toObject() : { ...d }));
  const r = await actualizarFacturasDetalle({ rfc, detalle });
  doc.detalle = r.detalle;
  doc.anexoActualizadoEn = r.actualizadoEn;
  await doc.save();
  const obj = doc.toObject();
  return { ...obj, id: String(obj._id), resultadoActualizar: { nuevas: r.nuevas, siguenSinFactura: r.siguenSinFactura, advertencias: r.advertencias } };
}

/** Solo administrador; libera las cajas para volver a generarlas. */
async function cancelar({ rfc, id, motivo }, user) {
  if (!(await esAdmin(user))) throw new ForbiddenError('Solo un administrador puede cancelar una ficha');
  const m = String(motivo ?? '').trim();
  if (m.length < 5) throw new BadRequestError('Escribe el motivo de la cancelación');
  const doc = await FichaEfectivo.findOneAndUpdate(
    { _id: id, rfc, estado: 'vigente' },
    { $set: { estado: 'cancelada', canceladaEn: new Date(), canceladaPor: _usuario(user), motivoCancelacion: m.slice(0, 300) } },
    { new: true },
  ).lean().catch(() => null);
  if (!doc) throw new NotFoundError('Ficha no encontrada o ya cancelada');
  return { ...doc, id: String(doc._id) };
}

module.exports = { folioTexto, cajasYaGuardadas, guardar, listar, obtener, cancelar, actualizarAnexo };
