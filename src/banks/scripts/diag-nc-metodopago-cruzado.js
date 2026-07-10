'use strict';

/**
 * diag-nc-metodopago-cruzado.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Para cada Nota de Crédito (tipo E, tipoRelacion 01/03 — bonificación,
 * descuento, devolución) de un período, compara el `metodoPago` que la NC
 * usa para clasificarse en el bloque Contado/Crédito de la póliza (ver
 * `poliza.service.js:595-596`) contra el `metodoPago` REAL de la factura de
 * Ingreso que está ajustando (resuelto vía `cfdiRelacionados`).
 *
 * Por qué importa: el split Contado/Crédito y la selección de regla de mapeo
 * (Clientes vs Bancos/Caja, IVA Trasladado vs IVA Por Trasladar PPD) dependen
 * únicamente del `metodoPago` propio de la NC — nunca se cruza contra la
 * factura original. Dos parches lo corrigen ANTES de llegar aquí:
 *   - `_normalizarEgresoPue99`: PUE+formaPago=99, inválido por regla del SAT.
 *   - `_normalizarEgresoCondonacion`: E+formaPago=15 (Condonación) → usa el
 *     metodoPago real de la factura relacionada (fix aplicado tras encontrar
 *     1,936 NCs con este patrón exacto en 2026-2, $674,901.62 de subtotal).
 * Este script aplica AMBOS antes de comparar, así que en una corrida limpia
 * el conteo de discrepancias debería ser 0 — si aparece alguna, es un caso
 * nuevo no cubierto por los dos parches y hay que investigarlo aparte.
 *
 * Uso:
 *   node src/banks/scripts/diag-nc-metodopago-cruzado.js
 */

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const { sequelize } = require('../../config/database.postgres');
const CFDI = require('../../visor/models/CFDI');
const { _normalizarEgresoPue99, _normalizarEgresoCondonacion } = require('../domains/cfdi-mapping/balanza-preliminar.service');

// ── Ajusta estos valores ──────────────────────────────────────────────────────
const RFC       = 'CCO011113663';
const EJERCICIO = 2026;
const PERIODO   = 2;
// ─────────────────────────────────────────────────────────────────────────────

const TIPOS_RELACION_AJUSTE = ['01', '03']; // bonificación/descuento/devolución normal — 04 (sustitución) y 07 (anticipo) tienen su propio manejo, se excluyen aquí

function primeraDescripcion(cfdi) {
  return (cfdi.conceptos?.[0]?.descripcion ?? cfdi.conceptos?.[0]?.Descripcion ?? '(sin descripción)').substring(0, 50);
}

function uuidsRelacionados(cfdi, tipos) {
  return (cfdi.cfdiRelacionados ?? [])
    .filter(r => tipos.includes(r.tipoRelacion))
    .flatMap(r => r.uuids ?? (r.uuid ? [r.uuid] : []))
    .filter(Boolean)
    .map(u => u.trim().toUpperCase());
}

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  // ── 1. Cargar NCs SAT del período con tipoRelacion 01/03 ──────────────────
  const ncsRaw = await CFDI.find({
    $or:               [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }],
    ejercicio:         EJERCICIO,
    periodo:           PERIODO,
    tipoDeComprobante: 'E',
    source:            'SAT',
    satStatus:         'Vigente',
    isActive:          true,
    'cfdiRelacionados.tipoRelacion': { $in: TIPOS_RELACION_AJUSTE },
  })
    .select('uuid serie folio metodoPago formaPago subTotal total conceptos cfdiRelacionados receptor.nombre')
    .lean();

  console.log(`\nTotal NCs tipo E (tipoRelacion 01/03) en ${RFC} ${EJERCICIO}-${PERIODO}: ${ncsRaw.length}`);
  if (!ncsRaw.length) { await cerrar(); return; }

  // ── 2. Enriquecer con ERP (mismo patrón que diag-notas-credito.js) ────────
  const ncUuids = ncsRaw.map(c => c.uuid).filter(Boolean);
  const ncErp   = await CFDI.find({ uuid: { $in: ncUuids }, source: 'ERP' })
    .select('uuid formaPago metodoPago').lean();
  const ncErpMap = Object.fromEntries(ncErp.map(c => [c.uuid, c]));

  const ncs = ncsRaw.map(c => {
    const erp = ncErpMap[c.uuid];
    return {
      ...c,
      tipoDeComprobante: 'E',
      formaPago:  c.formaPago  || erp?.formaPago  || null,
      metodoPago: c.metodoPago || erp?.metodoPago || null,
    };
  });

  // Misma normalización que usa el pipeline real antes de clasificar Contado/Crédito.
  _normalizarEgresoPue99(ncs);

  // ── 3. Resolver la factura original de cada NC vía cfdiRelacionados ───────
  const facturaUuids = [...new Set(ncs.flatMap(nc => uuidsRelacionados(nc, TIPOS_RELACION_AJUSTE)))];

  const facturasSat = await CFDI.find({ uuid: { $in: facturaUuids }, source: 'SAT' })
    .select('uuid serie folio metodoPago formaPago tipoDeComprobante periodo ejercicio').lean();
  const facturaMap = Object.fromEntries(facturasSat.map(f => [f.uuid.toUpperCase(), f]));

  const faltantes = facturaUuids.filter(u => !facturaMap[u]);
  if (faltantes.length) {
    const facturasErp = await CFDI.find({ uuid: { $in: faltantes }, source: 'ERP' })
      .select('uuid serie folio metodoPago formaPago tipoDeComprobante periodo ejercicio').lean();
    for (const f of facturasErp) facturaMap[f.uuid.toUpperCase()] = f;
  }

  // Misma normalización que usa el pipeline real para NC formaPago=15 (Condonación).
  const relMetodoPagoMap = Object.fromEntries(facturaUuids.map(u => [u, facturaMap[u]?.metodoPago ?? null]));
  _normalizarEgresoCondonacion(ncs, relMetodoPagoMap);

  // ── 4. Comparar metodoPago(NC) vs metodoPago(factura original) ────────────
  const coincide   = [];
  const discrepa    = [];
  const sinFactura = [];
  const noEsI      = []; // el uuid relacionado no es una factura de Ingreso (raro, pero se reporta)

  for (const nc of ncs) {
    const relUuids = uuidsRelacionados(nc, TIPOS_RELACION_AJUSTE);
    if (!relUuids.length) { sinFactura.push({ nc, motivo: 'NC sin uuid en cfdiRelacionados' }); continue; }

    const facturas = relUuids.map(u => facturaMap[u]).filter(Boolean);
    if (!facturas.length) { sinFactura.push({ nc, motivo: `factura(s) relacionada(s) no encontrada(s) en BD: ${relUuids.join(', ')}` }); continue; }

    const noIngreso = facturas.filter(f => f.tipoDeComprobante !== 'I');
    if (noIngreso.length) noEsI.push({ nc, facturas: noIngreso });

    const facturasIngreso = facturas.filter(f => f.tipoDeComprobante === 'I');
    if (!facturasIngreso.length) continue;

    for (const factura of facturasIngreso) {
      const item = { nc, factura };
      if ((nc.metodoPago || null) === (factura.metodoPago || null)) coincide.push(item);
      else discrepa.push(item);
    }
  }

  // ── 5. Reporte ──────────────────────────────────────────────────────────
  console.log('\n── RESUMEN ─────────────────────────────────────────────────────────────');
  console.log(`  Coinciden (NC.metodoPago === factura.metodoPago): ${coincide.length}`);
  console.log(`  DISCREPAN (posible bloque/cuenta equivocada):     ${discrepa.length}`);
  console.log(`  Sin factura relacionada localizable:              ${sinFactura.length}`);
  if (noEsI.length) console.log(`  uuid relacionado no es tipo I (revisar aparte):   ${noEsI.length}`);

  if (discrepa.length) {
    const porFormaPago = {};
    const porDireccion  = {};
    let montoTotal = 0;
    for (const { nc, factura } of discrepa) {
      const fp = nc.formaPago ?? '(vacío)';
      const dir = `${nc.metodoPago ?? '(vacío)'} → ${factura.metodoPago ?? '(vacío)'}`;
      porFormaPago[fp] = (porFormaPago[fp] ?? 0) + 1;
      porDireccion[dir] = (porDireccion[dir] ?? 0) + 1;
      montoTotal += Number(nc.subTotal ?? 0);
    }
    console.log('\n── DISCREPANCIAS por formaPago de la NC ──────────────────────────────');
    Object.entries(porFormaPago).sort((a, b) => b[1] - a[1])
      .forEach(([k, v]) => console.log(`  ${String(v).padStart(5)}  formaPago=${k}`));
    console.log('\n── DISCREPANCIAS por dirección (NC.metodoPago → factura.metodoPago) ──');
    Object.entries(porDireccion).sort((a, b) => b[1] - a[1])
      .forEach(([k, v]) => console.log(`  ${String(v).padStart(5)}  ${k}`));
    console.log(`\n  Monto subTotal total en discrepancias: $${montoTotal.toLocaleString('es-MX', { minimumFractionDigits: 2 })}`);

    console.log('\n── DISCREPANCIAS (muestra de 10, revisar manualmente) ────────────────');
    for (const { nc, factura } of discrepa.slice(0, 10)) {
      console.log({
        cliente:          nc.receptor?.nombre?.substring(0, 30) ?? '—',
        nc:               `${nc.serie ?? ''}-${nc.folio ?? nc.uuid?.substring(0, 8)}`,
        nc_metodoPago:    nc.metodoPago ?? '(vacío)',
        nc_formaPago:     nc.formaPago ?? '(vacío)',
        nc_concepto:      primeraDescripcion(nc),
        factura:          `${factura.serie ?? ''}-${factura.folio ?? factura.uuid?.substring(0, 8)}`,
        factura_metodoPago: factura.metodoPago ?? '(vacío)',
        monto_nc:         Number(nc.subTotal ?? 0).toFixed(2),
      });
    }
  }

  if (sinFactura.length) {
    console.log('\n── SIN FACTURA RELACIONADA LOCALIZABLE (revisar por separado) ────────');
    sinFactura.slice(0, 30).forEach(({ nc, motivo }) =>
      console.log(`  ${nc.serie ?? ''}-${nc.folio ?? nc.uuid?.substring(0, 8)}  metodoPago=${nc.metodoPago ?? '(vacío)'}  → ${motivo}`));
    if (sinFactura.length > 30) console.log(`  ... y ${sinFactura.length - 30} más`);
  }

  console.log('\n──────────────────────────────────────────────────────────────────────');
  console.log('INTERPRETACIÓN:');
  console.log('  Una fila en DISCREPANCIAS significa que esa NC cayó en el bloque');
  console.log('  Contado/Crédito y usó la cuenta (Clientes vs Bancos/Caja, IVA');
  console.log('  Trasladado vs IVA Por Trasladar PPD) que correspondería a SU PROPIO');
  console.log('  metodoPago declarado, no al de la factura que realmente ajusta.');
  console.log('  Antes de corregir el motor de reglas, verifica manualmente 3-5 de');
  console.log('  estos casos contra el ERP para confirmar cuál dato es el correcto.');

  await cerrar();
}

async function cerrar() {
  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(err => { console.error(err); process.exit(1); });
