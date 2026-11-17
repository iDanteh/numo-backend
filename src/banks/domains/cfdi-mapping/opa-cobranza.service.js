'use strict';

/**
 * OPA (anticipo) que nace del SOBRANTE de un depósito de cobranza — el mismo
 * depósito liquida cuentas PPD (con complemento de pago) y lo que sobra Kore lo
 * deja como anticipo. Regla del usuario (2026-10-13): "si la OPA viene de un PPD
 * o un complemento de pago va a Cobranza y no a Ingresos". Caso real MINI
 * ABASTOS dep 046486 (11-sep, $7,479.42): B0-260803365 $491.35 + B0-260803238
 * $573.64 + B0-260803492 $6,345.52 (complemento B0-261000927) + OPA-01106
 * $68.91 (anticipo timbrado B0-261000851, que Ingreso registraba aparte con
 * cargo a "Bancos por identificar").
 *
 * La liga sale de Numo Bancos (`bank_movements.erpLinks`): el link del anticipo
 * trae serie 'OPA' + folioExterno; los de las cuentas PPD traen `tipoPago: 'PPD'`
 * y el uuid de su factura en `folioFiscal`.
 */

const BankMovement = require('../banks/BankMovement.model');
const CFDI = require('../../../visor/models/CFDI');

const esLinkOpa = l => String(l?.serie ?? '').toUpperCase() === 'OPA' && !!l?.folioExterno;
const esLinkPpd = l => String(l?.tipoPago ?? '').toUpperCase() === 'PPD';
const montoLink = l => (typeof l.saldoPagado === 'number' ? l.saldoPagado : Number(l.total) || 0);

/**
 * Depósitos ligados a estas facturas (uuid) que además traen una OPA.
 * @returns {Promise<{ idDeposito: string, fecha: Date|null, opas: { folio: string, referencia: string, monto: number }[], facturasPpd: Set<string> }[]>}
 */
async function depositosConOpaPorFacturas(facturaUuids) {
  const uuids = [...new Set((facturaUuids ?? []).filter(Boolean).map(u => String(u).toUpperCase()))];
  if (!uuids.length) return [];
  const movs = await BankMovement.find({
    'erpLinks.folioFiscal': { $in: uuids.flatMap(u => [u, u.toLowerCase()]) },
    'erpLinks.serie': 'OPA',
  }).select('fecha erpLinks').lean();
  return movs.map(m => ({
    idDeposito: String(m._id),
    fecha: m.fecha ?? null,
    opas: (m.erpLinks ?? []).filter(esLinkOpa)
      .map(l => ({ folio: l.folioExterno, referencia: `OPA-${l.folioExterno}`, monto: Math.round(montoLink(l) * 100) / 100 }))
      .filter(o => o.monto > 0),
    facturasPpd: new Set((m.erpLinks ?? []).filter(esLinkPpd).map(l => String(l.folioFiscal ?? '').toUpperCase()).filter(Boolean)),
  })).filter(d => d.opas.length && d.facturasPpd.size);
}

/**
 * ¿Este anticipo timbrado (CFDI tipo I) es una OPA que salió de un depósito de
 * cobranza? Primero por su folio OPA ya conocido (`refOpa`, ej. "OPA-01106", de
 * Cuentas Pendientes); si no se conoce, por monto exacto + mismo cliente (RFC de
 * alguna factura PPD del depósito) en un depósito de hasta 60 días antes.
 * @returns {Promise<string|null>} la referencia OPA si va a Cobranza; null si no.
 */
async function referenciaOpaDeCobranza(cfdi, refOpa, cache = new Map()) {
  const clave = `${cfdi?.uuid}|${refOpa ?? ''}`;
  if (cache.has(clave)) return cache.get(clave);
  let resultado = null;
  const folio = /^OPA-(.+)$/i.exec(refOpa ?? '')?.[1] ?? null;
  if (folio) {
    const movs = await BankMovement.find({ erpLinks: { $elemMatch: { serie: 'OPA', folioExterno: folio } } }).select('erpLinks').lean();
    if (movs.some(m => (m.erpLinks ?? []).some(esLinkPpd))) resultado = `OPA-${folio}`;
  } else if (cfdi?.fecha && Number(cfdi.total) > 0 && cfdi.receptor?.rfc) {
    const total = Number(cfdi.total);
    const fecha = new Date(cfdi.fecha).getTime();
    const movs = await BankMovement.find({
      fecha: { $gte: new Date(fecha - 60 * 86400000), $lte: new Date(fecha + 5 * 86400000) },
      erpLinks: { $elemMatch: { serie: 'OPA', total: { $gte: total - 0.01, $lte: total + 0.01 } } },
    }).select('erpLinks').lean();
    for (const m of movs) {
      const opa = (m.erpLinks ?? []).find(l => esLinkOpa(l) && Math.abs(Number(l.total) - total) < 0.01);
      const uuidsPpd = (m.erpLinks ?? []).filter(esLinkPpd).map(l => String(l.folioFiscal ?? '').toUpperCase()).filter(Boolean);
      if (!opa || !uuidsPpd.length) continue;
      const mismoCliente = await CFDI.exists({ uuid: { $in: uuidsPpd }, 'receptor.rfc': String(cfdi.receptor.rfc).toUpperCase() });
      if (mismoCliente) { resultado = `OPA-${opa.folioExterno}`; break; }
    }
  }
  cache.set(clave, resultado);
  return resultado;
}

/**
 * Sobrante mínimo para registrarlo como saldo a favor (2026-11-17, decisión del
 * usuario): con $1.00 o menos son centavos y el export ajusta el banco y Clientes
 * al monto del depósito (regla del 27-oct, caso OSCAR $0.25). Mismo valor en
 * poliza.service.js (`MINIMO_SOBRANTE_SF`).
 */
const MINIMO_SOBRANTE_SF = 1;

/**
 * Depósitos (identificados) ligados a estas facturas cuyo monto es MAYOR que lo
 * que aplicaron a cuentas en Kore — el sobrante va como SALDO A FAVOR en la
 * póliza de Cobranza (2026-11-17, pedido del usuario: transferencia de $5,000
 * que solo abona $2,100 → $2,900 de saldo a favor). Los que traen una OPA se
 * quedan fuera: su sobrante ya es esa OPA (`depositosConOpaPorFacturas`).
 * @returns {Promise<{ idDeposito: string, folio: string|null, fecha: Date|null, deposito: number, aplicado: number, sobrante: number, facturas: Set<string> }[]>}
 */
async function depositosConSobrantePorFacturas(facturaUuids) {
  const uuids = [...new Set((facturaUuids ?? []).filter(Boolean).map(u => String(u).toUpperCase()))];
  if (!uuids.length) return [];
  const movs = await BankMovement.find({
    'erpLinks.folioFiscal': { $in: uuids.flatMap(u => [u, u.toLowerCase()]) },
    status: 'identificado',
  }).select('fecha folio deposito erpLinks').lean();
  const r2 = n => Math.round(n * 100) / 100;
  const out = [];
  for (const m of movs) {
    const links = m.erpLinks ?? [];
    if (typeof m.deposito !== 'number' || links.some(esLinkOpa)) continue;
    const aplicado = r2(links.reduce((s, l) => s + montoLink(l), 0));
    const sobrante = r2(m.deposito - aplicado);
    if (sobrante <= MINIMO_SOBRANTE_SF) continue;
    out.push({
      idDeposito: String(m._id), folio: m.folio ?? null, fecha: m.fecha ?? null,
      deposito: m.deposito, aplicado, sobrante,
      facturas: new Set(links.map(l => String(l.folioFiscal ?? '').toUpperCase()).filter(Boolean)),
    });
  }
  return out;
}

module.exports = { depositosConOpaPorFacturas, depositosConSobrantePorFacturas, referenciaOpaDeCobranza, MINIMO_SOBRANTE_SF };
