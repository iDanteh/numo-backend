'use strict';

// ── Pólizas › "Sin timbrar" (2026-11-09) ─────────────────────────────────────
// Facturas/NC que YA existen en Kore (copia ERP "SINUUID-…", sin UUID) pero que
// todavía no se timbran en el SAT. Son las que después llegan tarde y quedan
// fuera de la póliza de Ingreso del día (caso real CEDIS, reporte "NO
// REGISTRADOS COMPAQ"): CEDIS factura con la fecha de la venta y timbra 1–3
// días después. Solo ACTIVAS en Kore (erpStatus Habilitado/Pendiente) — las
// "Deshabilitado" son facturas dadas de baja que nunca se timbraron (decisión
// del usuario 2026-11-09). Se descartan las que ya tienen cualquier copia con
// UUID real (ERP o SAT) con la misma serie-folio.

const CFDI = require('../../../visor/models/CFDI');
const { Poliza } = require('../../../shared/models/postgres');
const { Op } = require('sequelize');
const centrosSvc = require('../centros-costo/centros-costo.service');
const { BadRequestError } = require('../../shared/errors/AppError');

const MAX_DIAS = 92;
const DIA_MS = 86400000;

// México sin horario de verano (UTC-6) — mismo criterio que _medianocheMx del generador.
const inicioDiaMx = (ymd) => new Date(`${ymd}T06:00:00.000Z`);
function ymdMx(fecha) {
  // Fecha ERP justo a medianoche UTC = factura con día sin hora → es SU día
  // (mismo criterio que _uuidsPorFechaEfectiva).
  if (fecha.getTime() % DIA_MS === 0) return fecha.toISOString().slice(0, 10);
  return new Date(fecha.getTime() - 6 * 3600000).toISOString().slice(0, 10);
}
const fmtDMY = (ymd) => { const [y, m, d] = ymd.split('-'); return `${d}/${m}/${y}`; };

async function listarSinTimbrar({ rfc, desde, hasta, centroCostoId }) {
  if (!rfc) throw new BadRequestError('RFC requerido');
  const re = /^\d{4}-\d{2}-\d{2}$/;
  if (!re.test(desde || '') || !re.test(hasta || '')) throw new BadRequestError('Rango de fechas inválido');
  if (desde > hasta) throw new BadRequestError('La fecha inicial es mayor que la final');
  const dias = (new Date(hasta) - new Date(desde)) / DIA_MS + 1;
  if (dias > MAX_DIAS) throw new BadRequestError(`El rango máximo es de ${MAX_DIAS} días`);

  const ini = new Date(`${desde}T00:00:00.000Z`);                 // incluye medianoches exactas del primer día
  const fin = new Date(inicioDiaMx(hasta).getTime() + DIA_MS - 1);  // fin del último día en hora de México

  const sinUuid = await CFDI.find({
    source: 'ERP',
    'emisor.rfc': rfc,
    uuid: /^SINUUID-/,
    erpStatus: { $in: ['Habilitado', 'Pendiente'] },
    tipoDeComprobante: { $in: ['I', 'E'] },
    isActive: { $ne: false },
    fecha: { $gte: ini, $lte: fin },
  }).select('serie folio fecha tipoDeComprobante metodoPago formaPago total subTotal receptor erpStatus satStatus createdAt').lean();

  const ccBySerie = await centrosSvc.resolveBySerieMap();
  let candidatos = sinUuid
    .map(c => ({ ...c, dia: ymdMx(new Date(c.fecha)) }))
    .filter(c => c.dia >= desde && c.dia <= hasta)
    .filter(c => !centroCostoId || String(ccBySerie[c.serie]?.id ?? '') === String(centroCostoId));
  if (!candidatos.length) return { desde, hasta, total: 0, facturas: [], resumen: [] };

  // Descartar las que ya tienen copia con UUID real (ya timbradas). Puede haber
  // varias copias SINUUID de la misma serie-folio (importes distintos): se deja una.
  const series = [...new Set(candidatos.map(c => c.serie))];
  const folios = [...new Set(candidatos.map(c => String(c.folio)))];
  const timbradas = await CFDI.find({
    'emisor.rfc': rfc, serie: { $in: series }, folio: { $in: folios }, uuid: { $not: /^SINUUID-/ },
  }).select('serie folio').lean();
  const yaTimbrada = new Set(timbradas.map(t => `${t.serie}-${t.folio}`));
  const vistos = new Set();
  candidatos = candidatos.filter(c => {
    const k = `${c.serie}-${c.folio}`;
    if (yaTimbrada.has(k) || vistos.has(k)) return false;
    vistos.add(k);
    return true;
  });

  // Póliza de Ingreso vigente de esa sucursal y día (si ya se generó, esta
  // factura le va a faltar cuando se timbre).
  const conceptos = new Map();
  for (const c of candidatos) {
    const suc = ccBySerie[c.serie]?.sucursal;
    if (suc) conceptos.set(`Ingresos por Ventas Suc. ${suc} Día: ${fmtDMY(c.dia)}`, null);
  }
  if (conceptos.size) {
    const pols = await Poliza.findAll({
      where: { rfc, tipo: 'I', estado: { [Op.ne]: 'cancelada' }, concepto: { [Op.in]: [...conceptos.keys()] } },
      attributes: ['id', 'numero', 'concepto', 'estado'], order: [['id', 'DESC']], raw: true,
    });
    for (const p of pols) if (!conceptos.get(p.concepto)) conceptos.set(p.concepto, p);
  }

  const hoy = ymdMx(new Date());
  const facturas = candidatos.map(c => {
    const cc = ccBySerie[c.serie];
    const pol = cc ? conceptos.get(`Ingresos por Ventas Suc. ${cc.sucursal} Día: ${fmtDMY(c.dia)}`) : null;
    return {
      serieFolio: `${c.serie}-${c.folio}`,
      dia: c.dia,
      diasSinTimbrar: Math.max(0, Math.round((new Date(hoy) - new Date(c.dia)) / DIA_MS)),
      tipo: c.tipoDeComprobante,
      metodoPago: c.metodoPago || null,
      formaPago: c.formaPago || null,
      total: c.total,
      subTotal: c.subTotal,
      cliente: c.receptor?.nombre || null,
      rfcCliente: c.receptor?.rfc || null,
      sucursal: cc?.sucursal || null,
      centroCostoId: cc?.id ?? null,
      erpStatus: c.erpStatus,
      polizaId: pol?.id ?? null,
      polizaNumero: pol?.numero ?? null,
      polizaEstado: pol?.estado ?? null,
    };
  }).sort((a, b) => a.dia.localeCompare(b.dia) || a.serieFolio.localeCompare(b.serieFolio));

  const porSuc = new Map();
  for (const f of facturas) {
    const k = f.sucursal || '(sin sucursal)';
    const r = porSuc.get(k) || { sucursal: k, cantidad: 0, total: 0, conPoliza: 0 };
    r.cantidad += 1; r.total += Number(f.total) || 0; if (f.polizaId) r.conPoliza += 1;
    porSuc.set(k, r);
  }

  return {
    desde, hasta, total: facturas.length, facturas,
    resumen: [...porSuc.values()].sort((a, b) => b.cantidad - a.cantidad),
  };
}

// Por póliza de Ingreso (no cancelada): las sin timbrar de SU sucursal y día
// (pedido del usuario 2026-11-09: aviso "⏳ N sin timbrar" en cada póliza).
// Response: { [polizaId]: { total, desde, hasta, centroCostoId, facturas: [...] } }
async function sinTimbrarDePolizas(polizaIds) {
  const ids = [...new Set((polizaIds || []).map(Number).filter(Number.isFinite))].slice(0, 100);
  if (!ids.length) return {};
  const { _parametrosRegeneracionIngreso } = require('./cfdi-poliza-generator.service');
  const polizas = await Poliza.findAll({
    where: { id: { [Op.in]: ids }, tipo: 'I', estado: { [Op.ne]: 'cancelada' } },
    attributes: ['id', 'rfc', 'concepto'],
  });
  const cache = new Map();
  const resultado = {};
  for (const p of polizas) {
    let params;
    try { params = await _parametrosRegeneracionIngreso(p); } catch (_) { continue; }
    const k = [p.rfc, params.fechaInicio, params.fechaFin, params.centroCostoId].join('|');
    if (!cache.has(k)) {
      cache.set(k, await listarSinTimbrar({
        rfc: p.rfc, desde: params.fechaInicio, hasta: params.fechaFin, centroCostoId: params.centroCostoId,
      }).catch(() => null));
    }
    const r = cache.get(k);
    if (!r || !r.total) continue;
    resultado[p.id] = {
      total: r.total, desde: params.fechaInicio, hasta: params.fechaFin, centroCostoId: params.centroCostoId,
      facturas: r.facturas.map(f => ({ serieFolio: f.serieFolio, tipo: f.tipo, total: f.total, cliente: f.cliente })),
    };
  }
  return resultado;
}

module.exports = { listarSinTimbrar, sinTimbrarDePolizas };
