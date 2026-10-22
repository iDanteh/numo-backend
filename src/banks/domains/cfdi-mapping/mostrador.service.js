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

// Cuenta de Kore de cada remisión (`serie|folio` → { nombre, tipoPago,
// nombreTipoMovimiento }) vía `/cuentas-pendientes`, que necesita la fecha de
// creación de la remisión (sale de `/desgloses-cobro/almacen`). Si Kore falla,
// la remisión queda sin datos.
async function _cuentasPorRemision(rfc, remisiones) {
  const cuentasKore = new Map();
  if (!remisiones.length) return cuentasKore;
  let cuentas = [];
  try {
    cuentas = await obtenerDesglosesCobroAlmacen({ rfc, series: remisiones.map(r => r.serie), folios: remisiones.map(r => r.folio) });
  } catch {
    return cuentasKore;
  }
  await Promise.all((cuentas ?? []).map(async c => {
    const info = await obtenerCuentaTicket({ rfc, serie: c.serieVenta, folio: c.folioVenta, fechaCreacion: c.fechaCreacion }).catch(() => null);
    if (info) cuentasKore.set(`${c.serieVenta}|${c.folioVenta}`, info);
  }));
  return cuentasKore;
}

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
  const cuentasKore = await _cuentasPorRemision(rfc, remisiones);

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
 * @returns {Promise<{ depositos: object[], remisionesSinRespuesta: number }>}
 *   depositos: [{ fecha, descripcion, importe, banco, remisiones, cliente, idNumo }]
 */
const LOTE_DESGLOSES = 50;
// Kore limita /cuentas-pendientes (429 con 8 a la vez en prod, 2026-10-22).
const CONCURRENCIA_CUENTAS = 2;

async function detectarDepositosPendientesFactura({ rfc, series, fechaInicio, fechaFin }) {
  const seriesSet = new Set((series ?? []).map(s => String(s).trim().toUpperCase()).filter(Boolean));
  if (!rfc || !seriesSet.size || !fechaInicio) return { depositos: [], remisionesSinRespuesta: 0 };

  // `fecha` de Bancos es el día del estado de cuenta sin hora (medianoche
  // "ingenua" UTC) — mismo rango que cargarFichasCobranza.
  const desde = new Date(`${fechaInicio}T00:00:00.000Z`);
  const hasta = new Date(`${fechaFin || fechaInicio}T23:59:59.999Z`);
  const linkSinFactura = l => seriesSet.has(String(l.serie ?? '').trim().toUpperCase()) && !l.folioFiscal && l.folioExterno;
  const bancos = await BankMovement.find({
    isActive: { $ne: false },
    deposito: { $gt: 0 },
    fecha:    { $gte: desde, $lte: hasta },
    erpLinks: { $elemMatch: { serie: { $in: [...seriesSet] }, $or: [{ folioFiscal: null }, { folioFiscal: '' }] } },
  }).lean();
  if (!bancos.length) return { depositos: [], remisionesSinRespuesta: 0 };

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

  const cumple = l => {
    const k = clave(l);
    const d = desglose.get(k);
    const c = cuentaKore.get(k);
    return !!(d && !d.serieFactura && c && !c.factura
      && c.tipoPago === 'PPD'
      && c.nombreTipoMovimiento === ESTATUS_POR_FACTURAR
      && Math.abs(Number(c.saldoActual)) < 0.005);
  };

  const depositos = [];
  for (const b of bancos) {
    const links = (b.erpLinks ?? []).filter(linkSinFactura);
    if (!links.length || !links.every(cumple)) continue;
    depositos.push({
      fecha:       b.fecha ?? null,
      descripcion: b.concepto ?? '',
      importe:     _r2(b.deposito),
      aplicado:    _r2(links.reduce((s, l) => s + (Number(l.saldoPagado ?? l.saldoPagadoTotal) || 0), 0)),
      banco:       b.banco ?? '',
      remisiones:  [...new Set(links.map(l => `${String(l.serie).trim().toUpperCase()}-${String(l.folioExterno).trim()}`))],
      cliente:     [...new Set(links.map(l => cuentaKore.get(clave(l))?.nombre).filter(Boolean))].join(' / '),
      idNumo:      b.folio ?? '',
      estatus:     ESTATUS_POR_FACTURAR,
    });
  }
  depositos.sort((a, b) => new Date(a.fecha ?? 0) - new Date(b.fecha ?? 0) || String(a.idNumo).localeCompare(String(b.idNumo)));
  return { depositos, remisionesSinRespuesta };
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

module.exports = { detectarMovimientosMostrador, detectarDepositosPendientesFactura, detectarDepositosEfectivoSinAsociar };
