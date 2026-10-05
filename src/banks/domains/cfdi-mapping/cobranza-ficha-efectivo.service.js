'use strict';

/**
 * cobranza-ficha-efectivo.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Efectivo de la Cobranza CEDIS contra la ficha de depósito (2026-10-05,
 * pedido del usuario con sus pólizas manuales 19/21/22/24/25/26/29-sep).
 *
 * Contabilidad no carga el efectivo cobrado en la caja A0 renglón por renglón:
 * pone UN cargo al banco por el monto de la ficha "dd/mm COBRANZA" que
 * capturan en Bancos (`BankMovement.ficha`, ej. "297368 29/09 COBRANZA"):
 *   M1 1102011001 | EFECTIVO | 0 | 35,656.58 | EFECTIVO A0
 * (Banamex: 1102012001 | AUT.<ficha>). Si un pago en efectivo del día NO
 * entró a esa ficha, va a Caja 1101010001 "PXA" (cargo) y se abona el día
 * en que aparece en otra ficha:
 *   21-sep: 1101010001 | PXA | 0 | 23,566.69 | JACINTO CONDOY … (cargo)
 *   22-sep: 1101010001 | PXA | 1 | 23,566.69 | JACINTO CONDOY … (abono)
 *
 * La ficha solo trae el total; qué pago se quedó fuera se deduce por suma:
 * solo pagos COMPLETOS (un pago puede liquidar varios tickets) de los
 * complementos de este día cobrados en la caja A0. Si no hay exactamente
 * una combinación que cuadre, no se toca nada y se deja un aviso.
 */

const BankMovement = require('../banks/BankMovement.model');
const { Poliza, PolizaMovimiento, AccountPlan } = require('../../../shared/models/postgres');
const { Op } = require('sequelize');

const CAJA_COBRANZA_CEDIS     = 'A0';
const CODIGO_CUENTA_CAJA      = '1101010003';
const CODIGO_CUENTA_PXA       = '1101010001';
const CODIGO_CUENTA_FALTANTE  = '5202990001';
const ETIQUETA_PXA            = 'PXA';
// Mismo mapeo que `BANCO_A_CODIGO_CUENTA` en poliza.service.js — duplicado a
// propósito (ver docstring de cobranza-poliza-generator.service.js).
const BANCO_A_CODIGO_CUENTA = {
  Banamex: '1102012001', BBVA: '1102011001', Santander: '1102013001',
  Banorte: '1102014001', Scotiabank: '1102015001', Azteca: '1102016001',
};
// Diferencia máxima (centavos de redondeo) que se acepta como cuadre y va a
// FALTANTE 5202990001 — en las pólizas manuales nunca pasa de $1.20.
const TOLERANCIA_CUADRE = 2.00;
const MAX_PAGOS_PXA     = 3;   // pagos que se pueden quedar fuera de una ficha
const MAX_PENDIENTES    = 8;   // PXA de días anteriores que se consideran

const r2 = n => Math.round(n * 100) / 100;

/** Fichas "dd/mm COBRANZA" (exacto, no "GLOBAL CEDIS" ni por ticket) del día. */
async function cargarFichasCobranza(dia) {
  const [, m, d] = dia.split('-');
  const desde = new Date(`${dia}T00:00:00.000Z`);
  const hasta = new Date(desde.getTime() + 8 * 86400000);
  const re = new RegExp(`^\\s*\\d+\\s+${d}/${m}\\s+COBRANZA\\s*$`, 'i');
  const rows = await BankMovement.find({
    isActive: true, deposito: { $gt: 0 }, fecha: { $gte: desde, $lt: hasta }, ficha: { $regex: `${d}/${m}\\s+COBRANZA`, $options: 'i' },
  }).select('banco deposito ficha folio fecha').lean();
  return rows.filter(r => re.test(r.ficha ?? '')).map(r => ({
    banco: r.banco, monto: r2(Number(r.deposito) || 0), ficha: r.ficha.trim(),
    numero: (r.ficha.trim().match(/^\d+/) ?? [null])[0], folio: r.folio ?? null,
  }));
}

/**
 * Depósitos que Bancos ya ligó a cada factura: uuid → [montos]. Sirve para
 * reconocer el efectivo que el cliente depositó directo al banco (ej. PABLO
 * GOMEZ ROJAS 28-sep, $30,330.15) y que por eso no viene en la ficha.
 */
async function cargarDepositosPorFactura(facturaUuids) {
  const mapa = new Map();
  if (!facturaUuids.length) return mapa;
  const uuids = [...new Set(facturaUuids.flatMap(u => [u.toUpperCase(), u.toLowerCase()]))];
  const rows = await BankMovement.find({ isActive: true, deposito: { $gt: 0 }, 'erpLinks.folioFiscal': { $in: uuids } })
    .select('deposito erpLinks.folioFiscal').lean();
  for (const r of rows) {
    for (const l of r.erpLinks ?? []) {
      if (!l.folioFiscal) continue;
      const k = String(l.folioFiscal).toUpperCase();
      mapa.set(k, [...(mapa.get(k) ?? []), r2(Number(r.deposito) || 0)]);
    }
  }
  return mapa;
}

/** PXA cargados en Cobranzas anteriores (no canceladas) que todavía no se abonan. */
async function cargarPxaPendientes({ rfc, dia }) {
  const cuenta = await AccountPlan.findOne({ where: { codigo: CODIGO_CUENTA_PXA }, attributes: ['id'], raw: true });
  if (!cuenta) return [];
  const rows = await PolizaMovimiento.findAll({
    where: { cuentaId: cuenta.id, reglaNombre: ETIQUETA_PXA },
    attributes: ['concepto', 'debe', 'haber', 'centroCosto', 'centroCostoId'],
    include: [{ model: Poliza, as: 'poliza', attributes: ['fecha'], required: true,
      where: { rfc, tipo: 'D', estado: { [Op.ne]: 'cancelada' }, fecha: { [Op.lt]: dia } } }],
    raw: true,
  });
  const porConcepto = new Map();
  for (const r of rows) {
    const p = porConcepto.get(r.concepto) ?? { concepto: r.concepto, monto: 0, centroCosto: r.centroCosto, centroCostoId: r.centroCostoId };
    p.monto = r2(p.monto + (Number(r.debe) || 0) - (Number(r.haber) || 0));
    porConcepto.set(r.concepto, p);
  }
  return [...porConcepto.values()].filter(p => p.monto > 0.009);
}

function* combinaciones(n, k) {
  const idx = [];
  function* rec(desde) {
    if (idx.length === k) { yield [...idx]; return; }
    for (let i = desde; i < n; i++) { idx.push(i); yield* rec(i + 1); idx.pop(); }
  }
  yield* rec(0);
}

/**
 * Elige qué pagos del día se quedaron fuera (PXA cargo) y qué pendientes
 * entraron (PXA abono) para que efectivo − fuera + pendientesQueEntraron =
 * fichas. Prefiere abonar todos los pendientes y dejar fuera los menos pagos
 * posibles; si dos soluciones empatan, es ambigua.
 */
function resolverPxa({ pagos, pendientes, montoFichas }) {
  const totalPagos = r2(pagos.reduce((s, p) => s + p.monto, 0));
  const soluciones = [];
  const nPend = Math.min(pendientes.length, MAX_PENDIENTES);
  for (let maskQ = (1 << nPend) - 1; maskQ >= 0; maskQ--) {
    const entran = pendientes.slice(0, nPend).filter((_, i) => maskQ & (1 << i));
    const sumaEntran = entran.reduce((s, p) => s + p.monto, 0);
    const objetivoFuera = r2(totalPagos + sumaEntran - montoFichas);
    if (objetivoFuera < -TOLERANCIA_CUADRE) continue;
    const omitidos = nPend - entran.length;
    for (let k = 0; k <= Math.min(MAX_PAGOS_PXA, pagos.length); k++) {
      for (const comb of combinaciones(pagos.length, k)) {
        const fuera = comb.map(i => pagos[i]);
        const residuo = r2(objetivoFuera - fuera.reduce((s, p) => s + p.monto, 0));
        if (Math.abs(residuo) <= TOLERANCIA_CUADRE) {
          soluciones.push({ entran, fuera, residuo, costo: omitidos * 10 + k });
        }
      }
    }
  }
  if (!soluciones.length) return { estado: 'sin-solucion', totalPagos };
  soluciones.sort((a, b) => a.costo - b.costo || Math.abs(a.residuo) - Math.abs(b.residuo));
  const mejor = soluciones[0];
  const empatadas = soluciones.filter(s => s.costo === mejor.costo && Math.abs(Math.abs(s.residuo) - Math.abs(mejor.residuo)) < 0.005);
  if (empatadas.length > 1) return { estado: 'ambigua', totalPagos, opciones: empatadas.slice(0, 5) };
  return { estado: 'ok', totalPagos, ...mejor };
}

/**
 * Reemplaza, dentro de `movs`, los cargos de efectivo cobrado en la caja A0
 * por el cargo de la ficha + PXA. Muta `movs`; regresa avisos.
 */
async function aplicarFichaEfectivoCobranza({ movs, rfc, dia, cuentaMap, centroCostoCedis }) {
  const avisos = [];
  // Solo pagos de facturas de CEDIS: un ticket de otra sucursal cobrado en la
  // caja A0 (ej. MUNICIPIO DE SAN ANDRES ZAUTLA, N0, 24-sep) se deposita aparte.
  const esCandidato = m => m.cuentaId === cuentaMap[CODIGO_CUENTA_CAJA] && Number(m.debe) > 0
    && m._formaPagoReal === '01' && m._claveCentroCobro === CAJA_COBRANZA_CEDIS && m._grupoPago
    && (!centroCostoCedis || m.centroCosto === centroCostoCedis);
  const candidatos = movs.filter(esCandidato);
  if (!candidatos.length) return avisos;

  const svc = module.exports; // vía exports para poder simular cada carga
  const fichas = await svc.cargarFichasCobranza(dia);
  if (!fichas.length) {
    avisos.push(`ℹ Sin ficha "${dia.slice(8, 10)}/${dia.slice(5, 7)} COBRANZA" capturada en Bancos — el efectivo de la caja ${CAJA_COBRANZA_CEDIS} queda en Caja por identificar`);
    return avisos;
  }

  // Un pago completo = mismo cliente + mismo cobro en caja (ver `_grupoPago`).
  const pagosMap = new Map();
  for (const m of candidatos) {
    const key = `${m.rfcTercero ?? ''}|${m._grupoPago}`;
    const p = pagosMap.get(key) ?? { key, monto: 0, movs: [], concepto: null, centroCosto: m.centroCosto, centroCostoId: m.centroCostoId };
    p.monto = r2(p.monto + Number(m.debe));
    p.movs.push(m);
    pagosMap.set(key, p);
  }
  // Fuera los pagos que el cliente depositó directo: un depósito ligado a su
  // factura por el MISMO monto (del pago o de esa factura). Un depósito ligado
  // por otro monto es de otro cobro (ej. AUTOOBRA 24-sep, EDUARDO MERARDO 25-sep).
  const depositos = await svc.cargarDepositosPorFactura(candidatos.map(m => m.facturaUuid).filter(Boolean));
  const mismoMonto = (montos, objetivo) => montos.some(d => Math.abs(d - objetivo) <= 0.05);
  const pagos = [...pagosMap.values()].filter(p => !p.movs.some(m => {
    const montos = m.facturaUuid ? depositos.get(String(m.facturaUuid).toUpperCase()) : null;
    if (!montos?.length) return false;
    const montoFactura = r2(p.movs.filter(x => x.facturaUuid === m.facturaUuid).reduce((s, x) => s + Number(x.debe), 0));
    return mismoMonto(montos, p.monto) || mismoMonto(montos, montoFactura);
  }));
  const enPagos = new Set(pagos.flatMap(p => p.movs));
  for (const p of pagos) {
    const [cliente] = (p.movs[0].concepto ?? '').split(' / ');
    const tickets = [...new Set(p.movs.flatMap(m => (m.concepto ?? '').split(' / ').slice(1)))];
    p.concepto = [cliente, ...tickets].filter(Boolean).join(' / ').slice(0, 500);
  }
  const pendientes = await svc.cargarPxaPendientes({ rfc, dia });

  // Fichas a usar: siempre la principal (la más grande); las chicas del mismo
  // día suelen ser cobros de otra sucursal hechos en la caja A0, depositados
  // aparte (ej. "26/09 COBRANZA" $1.68 de I0). Gana la combinación con menos
  // PXA y que cuadre más exacto.
  const fichasOrdenadas = [...fichas].sort((a, b) => b.monto - a.monto).slice(0, 4);
  const intentos = [];
  for (let mask = 1; mask < (1 << fichasOrdenadas.length); mask += 2) {
    const usadas = fichasOrdenadas.filter((_, i) => mask & (1 << i));
    const r = resolverPxa({ pagos, pendientes, montoFichas: r2(usadas.reduce((s, f) => s + f.monto, 0)) });
    if (r.estado !== 'sin-solucion') intentos.push({ r, usadas });
  }
  const clave = ({ r }) => [r.estado === 'ok' ? r.costo : Infinity, r.estado === 'ok' ? Math.abs(r.residuo) : Infinity];
  intentos.sort((a, b) => clave(a)[0] - clave(b)[0] || clave(a)[1] - clave(b)[1]);
  const empate = intentos.length > 1 && intentos[1].r.estado === 'ok'
    && clave(intentos[1])[0] === clave(intentos[0])[0] && Math.abs(clave(intentos[1])[1] - clave(intentos[0])[1]) < 0.005;
  const resultado = intentos[0] && !empate ? intentos[0].r : (intentos[0] ? { estado: 'ambigua' } : null);
  const fichasUsadas = intentos[0]?.usadas ?? null;
  const montoTodas = r2(fichas.reduce((s, f) => s + f.monto, 0));
  if (!resultado || resultado.estado !== 'ok') {
    const totalPagos = r2(pagos.reduce((s, p) => s + p.monto, 0));
    const totalPend  = r2(pendientes.reduce((s, p) => s + p.monto, 0));
    avisos.push(`⚠ Efectivo caja ${CAJA_COBRANZA_CEDIS} vs ficha COBRANZA no se pudo cuadrar automático: efectivo del día $${totalPagos.toFixed(2)}`
      + `${totalPend ? ` + PXA pendientes $${totalPend.toFixed(2)}` : ''} vs ficha(s) $${montoTodas.toFixed(2)} (${fichas.map(f => f.ficha).join(', ')})`
      + `${resultado?.estado === 'ambigua' ? ' — más de una combinación posible' : ''}. El efectivo queda en Caja por identificar.`);
    return avisos;
  }

  // Ficha principal: banco y referencia del renglón "EFECTIVO A0".
  const principal = fichasUsadas[0];
  const codigoBanco = BANCO_A_CODIGO_CUENTA[principal.banco];
  const ids = await AccountPlan.findAll({ where: { codigo: [codigoBanco, CODIGO_CUENTA_PXA, CODIGO_CUENTA_FALTANTE].filter(Boolean) }, attributes: ['id', 'codigo'], raw: true });
  const idPor = Object.fromEntries(ids.map(c => [c.codigo, c.id]));
  if (!idPor[codigoBanco] || !idPor[CODIGO_CUENTA_PXA]) {
    avisos.push(`⚠ Falta la cuenta ${!idPor[codigoBanco] ? codigoBanco : CODIGO_CUENTA_PXA} en el catálogo — el efectivo queda en Caja por identificar`);
    return avisos;
  }

  const base = {
    centroCosto: candidatos[0].centroCosto, centroCostoId: candidatos[0].centroCostoId ?? null,
    ventaFecha: dia, cfdiUuid: null, facturaUuid: null, rfcTercero: null,
    tipoComprobante: 'P', metodoPago: null, formaPago: null, folio: null, rfcEmisor: null, rfcReceptor: null,
    reglaId: null, cuentaFaltante: false,
  };
  const quitar = enPagos;
  const nuevos = [];
  const montoFichas = r2(fichasUsadas.reduce((s, f) => s + f.monto, 0));
  nuevos.push({
    ...base, cuentaId: idPor[codigoBanco], debe: montoFichas, haber: 0,
    serie: principal.banco === 'Banamex' && principal.numero ? `AUT.${principal.numero}` : 'EFECTIVO',
    concepto: `EFECTIVO ${CAJA_COBRANZA_CEDIS}`, tipoOrigen: 'Pago', reglaNombre: 'EFECTIVO-FICHA',
  });
  for (const p of resultado.fuera) {
    nuevos.push({ ...base, centroCosto: p.centroCosto, centroCostoId: p.centroCostoId, cuentaId: idPor[CODIGO_CUENTA_PXA],
      debe: p.monto, haber: 0, serie: ETIQUETA_PXA, concepto: p.concepto, tipoOrigen: 'Cargo Especial', reglaNombre: ETIQUETA_PXA });
  }
  for (const p of resultado.entran) {
    nuevos.push({ ...base, centroCosto: p.centroCosto ?? base.centroCosto, centroCostoId: p.centroCostoId ?? base.centroCostoId, cuentaId: idPor[CODIGO_CUENTA_PXA],
      debe: 0, haber: p.monto, serie: ETIQUETA_PXA, concepto: p.concepto, tipoOrigen: 'Cargo Especial', reglaNombre: ETIQUETA_PXA });
  }
  // Centavos: lo que la ficha trae de menos (o de más) contra los cobros.
  if (Math.abs(resultado.residuo) >= 0.01 && idPor[CODIGO_CUENTA_FALTANTE]) {
    nuevos.push({ ...base, cuentaId: idPor[CODIGO_CUENTA_FALTANTE], serie: 'FALTANTE', concepto: `COBRANZA SUC. CEDIS`,
      debe: resultado.residuo > 0 ? resultado.residuo : 0, haber: resultado.residuo < 0 ? -resultado.residuo : 0,
      tipoOrigen: 'Pago', reglaNombre: 'FALTANTE' });
  }

  for (let i = movs.length - 1; i >= 0; i--) if (quitar.has(movs[i])) movs.splice(i, 1);
  movs.push(...nuevos);

  avisos.push(`ℹ Efectivo caja ${CAJA_COBRANZA_CEDIS}: ficha ${fichasUsadas.map(f => f.ficha).join(' + ')} $${montoFichas.toFixed(2)}`
    + `${resultado.fuera.length ? ` · PXA cargo ${resultado.fuera.map(p => `${p.concepto} $${p.monto.toFixed(2)}`).join('; ')}` : ''}`
    + `${resultado.entran.length ? ` · PXA abono ${resultado.entran.map(p => `${p.concepto} $${p.monto.toFixed(2)}`).join('; ')}` : ''}`);
  const fichasFuera = fichas.filter(f => !fichasUsadas.includes(f));
  if (fichasFuera.length) avisos.push(`ℹ Ficha(s) no incluidas en EFECTIVO ${CAJA_COBRANZA_CEDIS}: ${fichasFuera.map(f => `${f.ficha} $${f.monto.toFixed(2)}`).join(', ')}`);
  return avisos;
}

module.exports = {
  aplicarFichaEfectivoCobranza, resolverPxa,
  cargarFichasCobranza, cargarPxaPendientes, cargarDepositosPorFactura,
  CAJA_COBRANZA_CEDIS, CODIGO_CUENTA_PXA, CODIGO_CUENTA_FALTANTE, BANCO_A_CODIGO_CUENTA,
};
