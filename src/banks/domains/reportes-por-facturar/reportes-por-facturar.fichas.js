'use strict';

/**
 * reportes-por-facturar.fichas.js
 * ─────────────────────────────────────────────────────────────────────────────
 * "Fichas de efectivo" (Reportes, 2026-11-04, pedido del usuario con la foto
 * de su ficha de entrega de efectivo a traslado de valores). 2026-11-05: ya
 * no se eligen facturas globales — solo SUCURSAL y RANGO de fechas (más banco
 * y denominaciones, que captura la pantalla). La ficha suma el efectivo de la
 * CAJA de la sucursal en cada día del rango (corte de caja de Kore — lo que
 * ampara la global no incluye la cobranza y no cuadraba):
 *
 *   cobros con claveCentro = serie de la sucursal y fecha (día MX) = ese día
 *   (`/desgloses-cobro/almacen?centro=`), solo la parte en EFECTIVO:
 *     • Efectivo facturado:    cobro del ticket (CBT) de una venta ya facturada.
 *     • Efectivo por facturar: cobros (no abonos) de ventas SIN factura todavía.
 *     • Cobranza en efectivo:  otros cobros (CFC, CPF, APS, MIS…) de ventas ya facturadas.
 *     • Abonos en efectivo:    abonos (ABO).
 *     • APA = aplicación de saldo a favor: no es dinero nuevo → fuera.
 *   menos salidas de la caja ese día (`/desgloses-salidas/caja`):
 *     • Devoluciones en efectivo (RETD) y faltante en el cierre (RPF).
 *   Informativos (no suman): depósito en efectivo (no pasa por la caja) y
 *   saldo a favor aplicado.
 *
 * Verificado contra la ficha física de CONSTRUCASA (C0): 31-oct $83,405.41 −
 * RETD $215.47 − RPF $0.55 = $83,189.39 (ficha $83,189.00); 1-nov $17,039.26 −
 * RPF $0.01 = $17,039.25 (ficha $17,039).
 */

const CFDI       = require('../../../visor/models/CFDI');
const centrosSvc = require('../centros-costo/centros-costo.service');
const { obtenerDesglosesCobroAlmacenPorCentro, obtenerDesglosesSalidasCajaPorAlmacen } = require('../erp/erp-sync.service');
const { traerFacturasRecientes } = require('./reportes-por-facturar.facturas-kore');
const { BadRequestError } = require('../../shared/errors/AppError');

const MAX_DIAS_RANGO = 31;
const FECHA_RE = /^\d{4}-\d{2}-\d{2}$/;
const RFC_PUBLICO_GENERAL = 'XAXX010101000';
// Suman al total a entregar (las salidas restan).
const ENTRADAS = ['efectivoFacturado', 'efectivoPorFacturar', 'cobranzaEfectivo', 'abonosEfectivo'];
const SALIDAS = ['devoluciones', 'faltante'];
const INFORMATIVOS = ['depositoEfectivo', 'saldoFavor'];
const CONCEPTOS = [...ENTRADAS, ...SALIDAS, ...INFORMATIVOS];

const _r2 = n => Math.round((Number(n) || 0) * 100) / 100;
const _serie = v => String(v ?? '').trim().toUpperCase();
const _diaMx = f => new Date(new Date(f).getTime() - 6 * 3600000).toISOString().slice(0, 10);
const _dias = (a, b) => Math.round((new Date(`${b}T00:00:00Z`) - new Date(`${a}T00:00:00Z`)) / 86400000) + 1;

// Misma clasificación de formas que el reporte "Por facturar" (mostrador.service._formaPago).
function _forma(fp) {
  const nombre = fp?.nombre ?? fp?.nombreFormaPago ?? '';
  if (/saldo\s*a\s*favor|anticipo/i.test(nombre)) return 'saldoFavor';
  // Puntos de lealtad: Kore a veces los manda con clave 01, pero no son efectivo
  // (C0 1-nov: $50.78 de PUNTOS hacían que no cuadrara con la ficha).
  if (/puntos/i.test(nombre)) return null;
  if (/dep[oó]sito/i.test(nombre)) return 'depositoEfectivo';
  if (String(fp?.claveSat ?? '').trim() === '01' || /efectivo/i.test(nombre)) return 'efectivo';
  return null; // tarjeta, transferencia, cheque, puntos… no van en la ficha
}

/** Facturas globales vigentes (una por UUID; manda la copia ERP). */
function _vigentesPorUuid(docs) {
  const porUuid = new Map();
  for (const c of docs) {
    const u = String(c.uuid ?? '').toUpperCase();
    if (!u) continue;
    if (!porUuid.has(u) || c.source === 'ERP') porUuid.set(u, c);
  }
  return [...porUuid.entries()]
    .filter(([, c]) => c.satStatus !== 'Cancelado' && !['Cancelado', 'Cancelacion Pendiente'].includes(c.erpStatus));
}

const _orden = (a, b) => a.dia.localeCompare(b.dia) || a.serie.localeCompare(b.serie) || String(a.folio ?? '').localeCompare(String(b.folio ?? ''), undefined, { numeric: true });

/**
 * Facturas globales (receptor público en general) vigentes de la sucursal en
 * el periodo — día en hora de México. Hoy / ayer se traen antes de Kore (la
 * descarga nocturna aún no las tiene), igual que el reporte "Por facturar".
 */
async function listarGlobales({ rfc, centroCostoId, fechaInicio, fechaFin }) {
  fechaFin = fechaFin || fechaInicio;
  if (!rfc) throw new BadRequestError('RFC requerido');
  if (!FECHA_RE.test(fechaInicio ?? '') || !FECHA_RE.test(fechaFin ?? '')) throw new BadRequestError('Fechas inválidas (formato YYYY-MM-DD)');
  if (fechaFin < fechaInicio) throw new BadRequestError('La fecha final no puede ser anterior a la inicial');
  if (_dias(fechaInicio, fechaFin) > MAX_DIAS_RANGO) throw new BadRequestError(`El rango no puede pasar de ${MAX_DIAS_RANGO} días`);

  const ccBySerieMap = await centrosSvc.resolveBySerieMap();
  const entradas = Object.entries(ccBySerieMap).filter(([, c]) => !centroCostoId || String(c.id) === String(centroCostoId));
  if (!entradas.length) throw new BadRequestError('La sucursal no tiene serie de facturación configurada');
  const series = entradas.map(([s]) => _serie(s));
  const sucursalDe = new Map(Object.entries(ccBySerieMap).map(([s, c]) => [_serie(s), c?.sucursal ?? null]));

  const advertencias = [];
  const recientes = await traerFacturasRecientes({ series, fechaInicio, fechaFin });
  if (recientes.error) {
    advertencias.push(`No se pudieron traer de Kore las facturas del ${recientes.dias.join(' y ')} (${recientes.error}) — pueden faltar las de ese día hasta la descarga de la noche.`);
  }

  const docs = await CFDI.find({
    'emisor.rfc': rfc,
    tipoDeComprobante: 'I',
    'receptor.rfc': RFC_PUBLICO_GENERAL,
    serie: { $in: series },
    fecha: { $gte: new Date(`${fechaInicio}T00:00:00.000-06:00`), $lte: new Date(`${fechaFin}T23:59:59.999-06:00`) },
  }).select('uuid serie folio fecha total satStatus erpStatus source').lean();

  const globales = _vigentesPorUuid(docs)
    .map(([uuid, c]) => ({
      uuid, serie: _serie(c.serie), folio: String(c.folio ?? '').trim(), fecha: c.fecha, dia: _diaMx(c.fecha),
      total: _r2(c.total), sucursal: sucursalDe.get(_serie(c.serie)) ?? null,
    }))
    .sort(_orden);

  return { sucursal: centroCostoId ? entradas[0][1]?.sucursal ?? null : 'Todas', fechaInicio, fechaFin, globales, advertencias: advertencias.length ? advertencias : null };
}

/** Efectivo de la caja de una sucursal (serie) en un día. */
async function _corteDeCaja({ rfc, serie, dia }) {
  const montos = Object.fromEntries(CONCEPTOS.map(k => [k, 0]));
  const fechaDesde = new Date(`${dia}T00:00:00.000-06:00`).toISOString();
  const fechaHasta = new Date(`${dia}T23:59:59.999-06:00`).toISOString();
  const [cuentas, salidas] = await Promise.all([
    obtenerDesglosesCobroAlmacenPorCentro({ rfc, centro: serie, fechaDesde, fechaHasta }),
    obtenerDesglosesSalidasCajaPorAlmacen({ rfc, almacen: serie, fechaDesde, fechaHasta }),
  ]);

  for (const c of cuentas ?? []) {
    const facturada = !!c.serieFactura;
    for (const k of c.cobros ?? []) {
      if (_serie(k.claveCentro) !== serie || !k.fecha || _diaMx(k.fecha) !== dia) continue;
      const origen = _serie(k.serieOrigen);
      if (origen === 'APA') continue; // aplicación de saldo a favor: no es dinero nuevo
      const fps = k.formasPago ?? [];
      // Kore repite en cada ticket el total de un pago que cerró varios: con
      // una forma se usa |monto| del cobro; con varias que lo exceden, se
      // escalan (salvo saldo a favor, donde el monto ya es solo esa parte).
      const montoReal = k.monto != null ? Math.abs(Number(k.monto) || 0) : null;
      const suma = fps.reduce((s, f) => s + (Number(f.monto) || 0), 0);
      const conSaldoFavor = fps.some(f => _forma(f) === 'saldoFavor');
      const factor = (!conSaldoFavor && fps.length > 1 && montoReal != null && suma > montoReal + 0.05) ? montoReal / suma : 1;
      for (const fp of fps) {
        const forma = _forma(fp);
        if (!forma) continue;
        const monto = (fps.length === 1 && montoReal != null) ? montoReal : (Number(fp.monto) || 0) * factor;
        if (forma !== 'efectivo') montos[forma] += monto;
        else if (origen === 'ABO') montos.abonosEfectivo += monto;
        else if (!facturada) montos.efectivoPorFacturar += monto;
        else if (origen === 'CBT') montos.efectivoFacturado += monto;
        else montos.cobranzaEfectivo += monto;
      }
    }
  }
  for (const s of salidas ?? []) {
    if (!s.fecha || _diaMx(s.fecha) !== dia) continue;
    const tipo = _serie(s.tipoMovimiento?.abreviatura);
    if (tipo === 'RETD') montos.devoluciones += Number(s.montoRetirado) || 0;
    else if (tipo === 'RPF') montos.faltante += Number(s.montoRetirado) || 0;
  }
  for (const k of CONCEPTOS) montos[k] = _r2(montos[k]);
  return montos;
}

const _total = m => _r2(ENTRADAS.reduce((s, k) => s + m[k], 0) - SALIDAS.reduce((s, k) => s + m[k], 0));

/**
 * Ficha de una sucursal en un rango de fechas (2026-11-05: el usuario ya no
 * quiere elegir globales — solo sucursal, rango, banco y denominaciones): el
 * efectivo de la caja de cada serie de la sucursal en cada día del rango, y la
 * suma. Los días sin ningún movimiento de efectivo no salen en el desglose.
 * Las cajas se consultan una por una para no provocar 429 en Kore.
 */
async function calcularFicha({ rfc, centroCostoId, fechaInicio, fechaFin }) {
  fechaFin = fechaFin || fechaInicio;
  if (!rfc) throw new BadRequestError('RFC requerido');
  if (centroCostoId == null || centroCostoId === '') throw new BadRequestError('Elige la sucursal');
  if (!FECHA_RE.test(fechaInicio ?? '') || !FECHA_RE.test(fechaFin ?? '')) throw new BadRequestError('Fechas inválidas (formato YYYY-MM-DD)');
  if (fechaFin < fechaInicio) throw new BadRequestError('La fecha final no puede ser anterior a la inicial');
  if (_dias(fechaInicio, fechaFin) > MAX_DIAS_RANGO) throw new BadRequestError(`El rango no puede pasar de ${MAX_DIAS_RANGO} días`);

  const ccBySerieMap = await centrosSvc.resolveBySerieMap();
  const entradas = Object.entries(ccBySerieMap).filter(([, c]) => String(c?.id) === String(centroCostoId));
  if (!entradas.length) throw new BadRequestError('La sucursal no tiene serie de facturación configurada');
  const series = [...new Set(entradas.map(([s]) => _serie(s)))].sort();
  const sucursal = entradas[0][1]?.sucursal ?? null;

  const dias = [];
  for (let d = fechaInicio; d <= fechaFin; d = new Date(new Date(`${d}T00:00:00Z`).getTime() + 86400000).toISOString().slice(0, 10)) dias.push(d);

  const advertencias = [];
  const filas = [];
  for (const dia of dias) {
    for (const serie of series) {
      try {
        const montos = await _corteDeCaja({ rfc, serie, dia });
        if (!CONCEPTOS.some(k => montos[k])) continue; // día sin movimiento de efectivo
        filas.push({ serie, dia, sucursal, ...montos, totalEntregar: _total(montos) });
      } catch (err) {
        advertencias.push(`Kore no respondió la caja ${serie} del ${dia} (${err.message}); no se incluyó en la ficha — vuelve a generarla más tarde.`);
      }
    }
  }

  const totales = Object.fromEntries([...CONCEPTOS, 'totalEntregar'].map(k => [k, _r2(filas.reduce((s, f) => s + f[k], 0))]));
  return {
    generadoEn: new Date(),
    sucursal,
    series,
    fechaInicio,
    fechaFin,
    cortes: filas,
    totales,
    advertencias: advertencias.length ? advertencias : null,
  };
}

/**
 * Bancos dados de alta en Bancos (los que tienen movimientos o configuración),
 * con su número de cuenta para la ficha.
 */
async function listarBancos() {
  const BankMovement = require('../banks/BankMovement.model');
  const bankConfigRepo = require('../banks/repositories/bank-config.repository');
  const [nombres, configs] = await Promise.all([
    BankMovement.distinct('banco'),
    bankConfigRepo.findAllAsMap().catch(() => new Map()),
  ]);
  const todos = [...new Set([...nombres, ...configs.keys()].filter(Boolean))].sort((a, b) => a.localeCompare(b));
  return todos.map(banco => ({ banco, numeroCuenta: configs.get(banco)?.numeroCuenta ?? null }));
}

module.exports = { listarGlobales, calcularFicha, listarBancos, _corteDeCaja };
