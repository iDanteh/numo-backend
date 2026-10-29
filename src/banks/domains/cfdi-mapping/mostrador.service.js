'use strict';

/**
 * mostrador.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Hoja "MOSTRADOR" de la póliza de Ingreso (2026-10-21, pedido del usuario con
 * su PDF "MOSTRADOR 20 OCT 2026", factura A0-261009872).
 *
 * Una factura de mostrador agrupa remisiones que se cobraron DÍAS ANTES
 * (depósitos y transferencias ya identificados en Bancos) y se factura
 * después. El generador solo busca cobros del día de la factura, así que la
 * deja como "Venta Sin Cobro" — y esa línea no sale en el export (ver
 * poliza.service.js), por lo que nada de ella aparecía en el Excel.
 *
 * Aquí se listan, por cada factura con "Venta Sin Cobro", los depósitos de
 * Bancos ligados a ella (`erpLinks[].folioFiscal` = UUID de la factura) —
 * informativo, NUNCA se contabiliza (mismo criterio que "Movimientos PPD por
 * facturar").
 *
 * 2026-10-22 (pedido del usuario): la hoja solo enseña (a) los depósitos de
 * facturas GLOBALES (Público en General), juntos en una tabla, y (b) los de
 * remisiones que Kore sigue teniendo como PPD "POR FACTURAR" — el resto (p.
 * ej. factura PUE a nombre del cliente, ya FACTURADO) no sale. El estatus de
 * cada depósito es el de Kore (`nombreTipoMovimiento` de su remisión).
 */

const BankMovement = require('../banks/BankMovement.model');
const { obtenerDesglosesCobroAlmacen, obtenerCuentaTicket } = require('../erp/erp-sync.service');

const TIPO_ORIGEN_VENTA_SIN_COBRO = 'Venta Sin Cobro';
const ESTATUS_POR_FACTURAR = 'POR FACTURAR';
const RFC_PUBLICO_GENERAL = 'XAXX010101000';

const _r2 = n => Math.round((Number(n) || 0) * 100) / 100;

// ── Formas de pago del cobro de cada remisión (reporte "Por facturar",
// 2026-10-28, pedido del usuario: "si es un cobro mixto y cuánto tienen de
// cada uno") ────────────────────────────────────────────────────────────────
// 2026-10-29 (pedido del usuario: "se están sumando al efectivo depósitos en
// efectivo y transferencias … y los abonos"): Kore manda "DEPOSITO EN
// EFECTIVO" con la misma clave 01 que el efectivo → columna propia; y un
// abono (origen ABO) en efectivo solo cuenta como EFECTIVO si la nota ya está
// facturada y liquidada (saldo 0) — si no, va a ABONOS.
const FORMAS_PAGO = ['efectivo', 'depositoEfectivo', 'transferencia', 'tarjeta', 'cheque', 'abonos', 'otras'];

// c_FormaPago SAT: 01 efectivo, 02 cheque, 03 transferencia, 04/28 tarjeta.
// Saldo a favor, anticipo y puntos (no son dinero nuevo) → otras.
function _formaPago(fp) {
  const nombre = fp?.nombre ?? fp?.nombreFormaPago ?? '';
  if (/saldo\s*a\s*favor|anticipo|puntos/i.test(nombre)) return 'otras';
  if (/dep[oó]sito/i.test(nombre)) return 'depositoEfectivo';
  const clave = String(fp?.claveSat ?? '').trim();
  if (clave === '01') return 'efectivo';
  if (clave === '02') return 'cheque';
  if (clave === '03') return 'transferencia';
  if (clave === '04' || clave === '28') return 'tarjeta';
  if (/efectivo/i.test(nombre)) return 'efectivo';
  if (/tarjeta|d[eé]bito|cr[eé]dito|amex/i.test(nombre)) return 'tarjeta';
  if (/transfer|spei/i.test(nombre)) return 'transferencia';
  if (/cheque/i.test(nombre)) return 'cheque';
  return 'otras';
}

/**
 * Suma por forma de pago de todos los cobros de las cuentas de Kore
 * (`/desgloses-cobro/almacen`) de un depósito. Kore repite en cada ticket el
 * total de un pago que cerró varios (ver cfdi-poliza-generator, "Bug del
 * ERP"): con una sola forma se usa `|cobro.monto|`; con varias que exceden
 * ese monto se escalan — salvo saldo a favor / anticipo, donde `cobro.monto`
 * es solo esa porción.
 * @param {(cuenta) => boolean} [liquidada]  la nota ya está facturada y con
 *   saldo 0 → sus abonos en efectivo suman a EFECTIVO (si no, a ABONOS).
 * @returns {{ efectivo, depositoEfectivo, transferencia, tarjeta, cheque, abonos, otras, mixto: boolean } | null}
 *   null si Kore no regresó las cuentas o no traen cobros.
 */
function formasPagoDeCuentas(cuentas, liquidada = () => false) {
  const presentes = (cuentas ?? []).filter(Boolean);
  if (!presentes.length) return null;
  const total = Object.fromEntries(FORMAS_PAGO.map(f => [f, 0]));
  let conCobros = false;
  for (const cuenta of presentes) {
    const abonoEsEfectivo = liquidada(cuenta);
    for (const cobro of cuenta.cobros ?? []) {
      const esAbono = String(cobro.serieOrigen ?? '').trim().toUpperCase() === 'ABO';
      const fps = cobro.formasPago ?? [];
      if (fps.length) conCobros = true;
      const montoReal = cobro.monto != null ? Math.abs(Number(cobro.monto) || 0) : null;
      const suma = fps.reduce((s, f) => s + (Number(f.monto) || 0), 0);
      const conSaldoFavor = fps.some(f => /saldo\s*a\s*favor|anticipo/i.test(f.nombre ?? f.nombreFormaPago ?? ''));
      const factor = (!conSaldoFavor && fps.length > 1 && montoReal != null && suma > montoReal + 0.05) ? montoReal / suma : 1;
      for (const fp of fps) {
        const monto = (fps.length === 1 && montoReal != null) ? montoReal : (Number(fp.monto) || 0) * factor;
        const forma = _formaPago(fp);
        total[forma === 'efectivo' && esAbono && !abonoEsEfectivo ? 'abonos' : forma] += monto;
      }
    }
  }
  if (!conCobros) return null;
  for (const f of FORMAS_PAGO) total[f] = _r2(total[f]);
  return { ...total, mixto: FORMAS_PAGO.filter(f => total[f] > 0.005).length > 1 };
}

// Cuenta de Kore de cada remisión (`serie|folio` → { nombre, tipoPago,
// nombreTipoMovimiento }) vía `/cuentas-pendientes`, que necesita la fecha de
// creación de la remisión (sale de `/desgloses-cobro/almacen`). Si Kore falla,
// la remisión queda sin datos. `desglose` = la cuenta de `/desgloses-cobro/
// almacen` de cada remisión (sus cobros con formas de pago).
// 2026-10-29 (caso real 044114 en la global A0-261010907, reporte "Por
// facturar" del 20 al 29-oct, todas las sucursales): antes se pedían TODAS
// las remisiones en una sola llamada (con cientos Kore responde 414 "URI Too
// Large" y no regresaba ninguna) y todas las cuentas a la vez (429 → sin
// estatus → el depósito quedaba "no PPD" y se escondía). Ahora en lotes de
// LOTE_DESGLOSES y de CONCURRENCIA_CUENTAS en CONCURRENCIA_CUENTAS, igual que
// detectarDepositosPendientesFactura.
async function _cuentasPorRemision(rfc, remisiones) {
  const cuentasKore = new Map();
  const desglose = new Map();
  if (!remisiones.length) return { cuentasKore, desglose };
  for (let i = 0; i < remisiones.length; i += LOTE_DESGLOSES) {
    const lote = remisiones.slice(i, i + LOTE_DESGLOSES);
    try {
      const cuentas = await obtenerDesglosesCobroAlmacen({ rfc, series: lote.map(r => r.serie), folios: lote.map(r => r.folio) });
      for (const c of cuentas ?? []) desglose.set(`${c.serieVenta}|${c.folioVenta}`, c);
    } catch {
      // Ese lote queda sin datos (sus depósitos salen con ppdPorFacturar null).
    }
  }
  const cuentas = [...desglose.values()];
  for (let i = 0; i < cuentas.length; i += CONCURRENCIA_CUENTAS) {
    await Promise.all(cuentas.slice(i, i + CONCURRENCIA_CUENTAS).map(async c => {
      const info = await obtenerCuentaTicket({ rfc, serie: c.serieVenta, folio: c.folioVenta, fechaCreacion: c.fechaCreacion }).catch(() => null);
      if (info) cuentasKore.set(`${c.serieVenta}|${c.folioVenta}`, info);
    }));
  }
  return { cuentasKore, desglose };
}

// Nota facturada (serieFactura en /desgloses-cobro/almacen) y con saldo 0 en
// Kore. Sin respuesta de Kore no se sabe el saldo → no liquidada.
const _facturadaYLiquidada = cuentasKore => cuenta => {
  const saldo = cuentasKore.get(`${cuenta.serieVenta}|${cuenta.folioVenta}`)?.saldoActual;
  return !!cuenta.serieFactura && saldo != null && Math.abs(Number(saldo)) < 0.005;
};

/**
 * @param {object}   p
 * @param {string}   p.rfc
 * @param {object[]} p.cfdis        CFDIs de la póliza (uuid, serie, folio, fecha, total)
 * @param {object[]} p.movimientos  movimientos generados (se usan los "Venta Sin Cobro")
 * @returns {Promise<object[]>} una entrada por factura:
 *   { uuid, serie, folio, fechaFactura, esGlobal, totalFactura, totalAplicado,
 *     depositos: [{ fecha, descripcion, importe, aplicado, banco, remisiones, cliente, idNumo, estatus, ppdPorFacturar }] }
 */
async function detectarMovimientosMostrador({ rfc, cfdis, movimientos }) {
  const uuidsSinCobro = new Set(movimientos
    .filter(m => m.tipoOrigen === TIPO_ORIGEN_VENTA_SIN_COBRO && Number(m.debe) > 0 && m.cfdiUuid)
    .map(m => m.cfdiUuid.toUpperCase()));
  if (!uuidsSinCobro.size) return [];

  const cfdiPorUuid = new Map(cfdis.filter(c => c.uuid).map(c => [c.uuid.toUpperCase(), c]));
  const variantes = [...uuidsSinCobro].flatMap(u => [u, u.toLowerCase()]);
  const bancos = await BankMovement.find({ 'erpLinks.folioFiscal': { $in: variantes }, isActive: { $ne: false } }).lean();
  if (!bancos.length) return [];

  const porFactura = new Map();
  for (const b of bancos) {
    const linksPorUuid = new Map();
    for (const l of b.erpLinks ?? []) {
      const u = (l.folioFiscal ?? '').toUpperCase();
      if (!uuidsSinCobro.has(u)) continue;
      if (!linksPorUuid.has(u)) linksPorUuid.set(u, []);
      linksPorUuid.get(u).push(l);
    }
    for (const [u, links] of linksPorUuid) {
      if (!porFactura.has(u)) porFactura.set(u, []);
      porFactura.get(u).push({
        _tipoPagoPorRem: new Map(links.map(l => [`${l.serie ?? ''}-${l.folioExterno ?? ''}`, l.tipoPago ?? null])),
        fecha:       b.fecha ?? null,
        descripcion: b.concepto ?? '',
        importe:     _r2(b.deposito),
        aplicado:    _r2(links.reduce((s, l) => s + (Number(l.saldoPagado ?? l.saldoPagadoTotal) || 0), 0)),
        banco:       b.banco ?? '',
        remisiones:  [...new Set(links.map(l => `${l.serie ?? ''}-${l.folioExterno ?? ''}`))],
        idNumo:      b.folio ?? '',
      });
    }
  }

  const remisiones = [...new Set([...porFactura.values()].flat().flatMap(d => d.remisiones))]
    .map(r => { const i = r.indexOf('-'); return { serie: r.slice(0, i), folio: r.slice(i + 1) }; })
    .filter(r => r.serie && r.folio);
  const { cuentasKore, desglose } = await _cuentasPorRemision(rfc, remisiones);

  const resultado = [];
  for (const [u, depositos] of porFactura) {
    const cfdi = cfdiPorUuid.get(u) ?? {};
    for (const d of depositos) {
      const infos = d.remisiones.map(r => ({ ...cuentasKore.get(r.replace('-', '|')), tipoPagoLink: d._tipoPagoPorRem.get(r) }));
      const unicos = vals => [...new Set(vals.filter(Boolean))];
      d.cliente = unicos(infos.map(i => i.nombre)).join(' / ');
      d.estatus = unicos(infos.map(i => i.nombreTipoMovimiento)).join(' / ');
      // PPD "POR FACTURAR" en Kore (mismo criterio que `esPpdPorFacturar` del
      // generador); si Kore no regresó la cuenta, tipoPago cae al del link.
      d.ppdPorFacturar = infos.some(i => (i.tipoPago ?? i.tipoPagoLink) === 'PPD' && i.nombreTipoMovimiento === ESTATUS_POR_FACTURAR);
      // Kore no regresó NINGUNA de sus remisiones: no se sabe → null (no se
      // esconde; el reporte avisa). Antes quedaba false y desaparecía.
      if (!d.ppdPorFacturar && infos.every(i => !i.nombreTipoMovimiento) && infos.some(i => (i.tipoPagoLink ?? 'PPD') === 'PPD')) {
        d.ppdPorFacturar = null;
        d.estatus = 'SIN RESPUESTA DE KORE';
      }
      d.formasPago = formasPagoDeCuentas(d.remisiones.map(r => desglose.get(r.replace('-', '|'))), _facturadaYLiquidada(cuentasKore));
      delete d._tipoPagoPorRem;
    }
    depositos.sort((a, b) => new Date(a.fecha ?? 0) - new Date(b.fecha ?? 0) || String(a.idNumo).localeCompare(String(b.idNumo)));
    resultado.push({
      uuid:          u,
      serie:         cfdi.serie ?? null,
      folio:         cfdi.folio ?? null,
      fechaFactura:  cfdi.fecha ?? null,
      esGlobal:      (cfdi.receptor?.rfc ?? '').trim().toUpperCase() === RFC_PUBLICO_GENERAL,
      totalFactura:  _r2(cfdi.total),
      totalAplicado: _r2(depositos.reduce((s, d) => s + d.aplicado, 0)),
      depositos,
    });
  }
  return resultado.sort((a, b) => String(a.folio).localeCompare(String(b.folio)));
}

/**
 * Tabla "PENDIENTE GENERAR FACTURA" (2026-10-22, pedido del usuario con su
 * lista manual de CEDIS): depósitos de Bancos DEL DÍA de la póliza que pagan
 * remisiones de la sucursal que, según Kore, son PPD, ya están pagadas
 * (saldo 0), siguen "POR FACTURAR" y NO tienen ninguna factura. Sale el
 * depósito solo si TODAS sus remisiones de la sucursal cumplen. Verificado
 * en prod: 31 de los 32 depósitos de la lista (el 32º no tiene remisión
 * ligada en Bancos) — Kore no cambia el estatus al facturar, por eso la
 * factura se confirma con `/desgloses-cobro/almacen` (serieFactura).
 *
 * @param {object}   p
 * @param {string}   p.rfc
 * @param {string[]} p.series    series de la sucursal (A0, …)
 * @param {string}   p.fechaInicio  YYYY-MM-DD — día de la póliza
 * @param {string}   [p.fechaFin]   YYYY-MM-DD (default = fechaInicio)
 * @param {boolean}  [p.incluirIdentificados]  también los depósitos de fecha
 *   anterior que se IDENTIFICARON / VINCULARON dentro del rango (día MX), con
 *   su `identificadoEl` — solo el reporte "Por facturar" (2026-10-23, pedido
 *   del usuario: depósitos de ago/sep/oct identificados hoy no salían en el
 *   reporte de hoy). La póliza no lo usa.
 * @returns {Promise<{ depositos: object[], depositosConSaldo: object[], remisionesSinRespuesta: number }>}
 *   depositos: [{ fecha, descripcion, importe, aplicado, banco, remisiones, cliente, idNumo, estatus }]
 *   depositosConSaldo: mismos campos + `saldo` — PPD, "POR FACTURAR" y sin
 *   factura pero alguna remisión todavía debe algo (pestaña "CON PENDIENTES
 *   DE PAGO" del reporte, 2026-10-23; caso real 053960 / A0-261002980, saldo
 *   $47.79 = retención del 4% sin registrar en Kore).
 */
const LOTE_DESGLOSES = 50;
// Kore limita /cuentas-pendientes (429 con 8 a la vez en prod, 2026-10-22).
const CONCURRENCIA_CUENTAS = 2;

// Último momento en que se identificó / vinculó el depósito (fechas reales,
// con hora): vinculaciones del historial, identificadoPor y la primera
// identificación.
function _identificadoEl(b) {
  const fechas = [
    b.primeraIdentificacionAt,
    ...(b.identificadoPor ?? []).map(i => i.fechaId),
    ...(b.historialVinculacion ?? []).filter(h => h.accion === 'vinculado').map(h => h.at),
  ].map(f => (f ? new Date(f) : null)).filter(f => f && !isNaN(f));
  return fechas.length ? new Date(Math.max(...fechas)) : null;
}

async function detectarDepositosPendientesFactura({ rfc, series, fechaInicio, fechaFin, incluirIdentificados = false }) {
  const seriesSet = new Set((series ?? []).map(s => String(s).trim().toUpperCase()).filter(Boolean));
  if (!rfc || !seriesSet.size || !fechaInicio) return { depositos: [], depositosConSaldo: [], remisionesSinRespuesta: 0 };

  // `fecha` de Bancos es el día del estado de cuenta sin hora (medianoche
  // "ingenua" UTC) — mismo rango que cargarFichasCobranza.
  const desde = new Date(`${fechaInicio}T00:00:00.000Z`);
  const hasta = new Date(`${fechaFin || fechaInicio}T23:59:59.999Z`);
  const linkSinFactura = l => seriesSet.has(String(l.serie ?? '').trim().toUpperCase()) && !l.folioFiscal && l.folioExterno;
  const porFechaBanco = { fecha: { $gte: desde, $lte: hasta } };
  // Identificación: instante real → día de México (UTC-6, sin horario de verano).
  const enRangoId = {
    $gte: new Date(`${fechaInicio}T00:00:00.000-06:00`),
    $lte: new Date(`${fechaFin || fechaInicio}T23:59:59.999-06:00`),
  };
  const bancos = await BankMovement.find({
    isActive: { $ne: false },
    deposito: { $gt: 0 },
    ...(incluirIdentificados
      ? { $or: [
        porFechaBanco,
        { primeraIdentificacionAt: enRangoId },
        { 'identificadoPor.fechaId': enRangoId },
        { historialVinculacion: { $elemMatch: { accion: 'vinculado', at: enRangoId } } },
      ] }
      : porFechaBanco),
    erpLinks: { $elemMatch: { serie: { $in: [...seriesSet] }, $or: [{ folioFiscal: null }, { folioFiscal: '' }] } },
  }).lean();
  if (!bancos.length) return { depositos: [], depositosConSaldo: [], remisionesSinRespuesta: 0 };

  const clave = l => `${String(l.serie).trim().toUpperCase()}|${String(l.folioExterno).trim()}`;
  const remisiones = [...new Map(bancos.flatMap(b => (b.erpLinks ?? []).filter(linkSinFactura))
    .map(l => [clave(l), { serie: String(l.serie).trim().toUpperCase(), folio: String(l.folioExterno).trim() }])).values()];

  // Factura que ampara cada remisión (y su fecha de creación, que pide /cuentas-pendientes).
  const desglose = new Map();
  for (let i = 0; i < remisiones.length; i += LOTE_DESGLOSES) {
    const lote = remisiones.slice(i, i + LOTE_DESGLOSES);
    const cuentas = await obtenerDesglosesCobroAlmacen({ rfc, series: lote.map(r => r.serie), folios: lote.map(r => r.folio) });
    for (const c of cuentas ?? []) desglose.set(`${c.serieVenta}|${c.folioVenta}`, c);
  }

  // Tipo de pago, estatus y saldo de Kore — solo de las que no tienen factura.
  const cuentaKore = new Map();
  let remisionesSinRespuesta = 0;
  const pendientes = remisiones.filter(r => { const d = desglose.get(`${r.serie}|${r.folio}`); return d && !d.serieFactura; });
  for (let i = 0; i < pendientes.length; i += CONCURRENCIA_CUENTAS) {
    await Promise.all(pendientes.slice(i, i + CONCURRENCIA_CUENTAS).map(async r => {
      const d = desglose.get(`${r.serie}|${r.folio}`);
      const info = await obtenerCuentaTicket({ rfc, serie: r.serie, folio: r.folio, fechaCreacion: d.fechaCreacion }).catch(() => null);
      if (info) cuentaKore.set(`${r.serie}|${r.folio}`, info);
      else remisionesSinRespuesta++;
    }));
  }

  // PPD, "POR FACTURAR" y sin factura (sin importar el saldo).
  const porFacturar = l => {
    const k = clave(l);
    const d = desglose.get(k);
    const c = cuentaKore.get(k);
    return !!(d && !d.serieFactura && c && !c.factura
      && c.tipoPago === 'PPD'
      && c.nombreTipoMovimiento === ESTATUS_POR_FACTURAR);
  };
  const saldoDe = l => Number(cuentaKore.get(clave(l))?.saldoActual) || 0;
  const pagada = l => Math.abs(saldoDe(l)) < 0.005;

  const depositos = [];
  const depositosConSaldo = [];
  for (const b of bancos) {
    const links = (b.erpLinks ?? []).filter(linkSinFactura);
    if (!links.length || !links.every(porFacturar)) continue;
    // Todas pagadas → PENDIENTE GENERAR FACTURA; si alguna debe algo → CON PENDIENTES DE PAGO.
    const todasPagadas = links.every(pagada);
    const remisionesUnicas = [...new Map(links.map(l => [clave(l), l])).values()];
    (todasPagadas ? depositos : depositosConSaldo).push({
      fecha:       b.fecha ?? null,
      descripcion: b.concepto ?? '',
      importe:     _r2(b.deposito),
      aplicado:    _r2(links.reduce((s, l) => s + (Number(l.saldoPagado ?? l.saldoPagadoTotal) || 0), 0)),
      banco:       b.banco ?? '',
      remisiones:  [...new Set(links.map(l => `${String(l.serie).trim().toUpperCase()}-${String(l.folioExterno).trim()}`))],
      cliente:     [...new Set(links.map(l => cuentaKore.get(clave(l))?.nombre).filter(Boolean))].join(' / '),
      idNumo:      b.folio ?? '',
      estatus:     ESTATUS_POR_FACTURAR,
      ...(incluirIdentificados ? { identificadoEl: _identificadoEl(b) } : {}),
      formasPago:  formasPagoDeCuentas(remisionesUnicas.map(l => desglose.get(clave(l)))),
      ...(todasPagadas ? {} : { saldo: _r2(remisionesUnicas.reduce((s, l) => s + saldoDe(l), 0)) }),
    });
  }
  const porFecha = (a, b) => new Date(a.fecha ?? 0) - new Date(b.fecha ?? 0) || String(a.idNumo).localeCompare(String(b.idNumo));
  depositos.sort(porFecha);
  depositosConSaldo.sort(porFecha);
  return { depositos, depositosConSaldo, remisionesSinRespuesta };
}

/**
 * Hoja "DEPÓSITOS EN EFECTIVO" del reporte "Por facturar" (2026-10-22, pedido
 * del usuario): depósitos en efectivo del periodo que todavía NO están
 * asociados a ninguna venta en Bancos (sin `erpLinks`), p. ej. el 051499
 * ($1,618.95, 8-oct) — sin deducir a qué remisión corresponden. Sin vínculo
 * no se sabe de qué sucursal son, así que salen todos los del periodo.
 *
 * @returns {Promise<object[]>} [{ fecha, descripcion, importe, banco, ficha, idNumo }]
 */
async function detectarDepositosEfectivoSinAsociar({ fechaInicio, fechaFin }) {
  if (!fechaInicio) return [];
  const bancos = await BankMovement.find({
    isActive: { $ne: false },
    deposito: { $gt: 0 },
    // Mismo rango "ingenuo" que detectarDepositosPendientesFactura.
    fecha:    { $gte: new Date(`${fechaInicio}T00:00:00.000Z`), $lte: new Date(`${fechaFin || fechaInicio}T23:59:59.999Z`) },
    concepto: { $regex: 'efectivo', $options: 'i' },
    $or:      [{ erpLinks: { $exists: false } }, { erpLinks: { $size: 0 } }],
  }).select('fecha concepto deposito banco ficha folio').lean();
  return bancos
    .map(b => ({
      fecha:       b.fecha ?? null,
      descripcion: b.concepto ?? '',
      importe:     _r2(b.deposito),
      banco:       b.banco ?? '',
      ficha:       b.ficha ?? '',
      idNumo:      b.folio ?? '',
    }))
    .sort((a, b) => new Date(a.fecha ?? 0) - new Date(b.fecha ?? 0) || String(a.idNumo).localeCompare(String(b.idNumo)));
}

module.exports = { detectarMovimientosMostrador, detectarDepositosPendientesFactura, detectarDepositosEfectivoSinAsociar, formasPagoDeCuentas, FORMAS_PAGO, _identificadoEl };
