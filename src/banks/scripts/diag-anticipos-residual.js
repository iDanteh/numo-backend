'use strict';
/**
 * diag-anticipos-residual.js
 * Identifica NCs tipo E (tipoRelacion=07) que NO fueron filtradas por el fix
 * de doble-contabilización y siguen sumando DEBE en 2103010001.
 *
 * Un NC se filtra si su cfdiRelacionados[tipoRelacion=07].uuids[0] apunta a
 * una factura tipo I, formaPago=30 que existe en el MISMO período/ejercicio.
 * Las que NO se filtran son el residual.
 *
 * Uso:
 *   node src/banks/scripts/diag-anticipos-residual.js --rfc XAXX010101000 --ejercicio 2026 --periodo 2
 */

require('dotenv').config();

const mongoose = require('mongoose');
const CFDI     = require('../../visor/models/CFDI');

const args = process.argv.slice(2);
const get  = (flag) => { const i = args.indexOf(flag); return i >= 0 ? args[i + 1] : null; };

const RFC      = get('--rfc')      || process.env.DIAG_RFC;
const EJERCICIO = Number(get('--ejercicio') || 2026);
const PERIODO   = Number(get('--periodo')   || 2);

if (!RFC) { console.error('Falta --rfc'); process.exit(1); }

async function run() {
  await mongoose.connect(process.env.MONGODB_URI);
  console.log(`MongoDB conectado.\nRFC=${RFC}  Ejercicio=${EJERCICIO}  Período=${PERIODO}\n`);

  // 1. UUIDs de facturas PUE (formaPago=30) en el período
  const facturasPue = await CFDI.find({
    $or:               [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }],
    ejercicio:         EJERCICIO,
    periodo:           PERIODO,
    tipoDeComprobante: 'I',
    formaPago:         '30',
    source:            'SAT',
    satStatus:         'Vigente',
    isActive:          true,
  }).select('uuid folio subTotal').lean();

  const setFacturasPue = new Set(facturasPue.map(f => f.uuid.toUpperCase()));
  console.log(`Facturas PUE formaPago=30 en período: ${facturasPue.length}`);
  facturasPue.forEach(f =>
    console.log(`  ${f.uuid}  folio=${f.folio}  subTotal=${f.subTotal}`),
  );

  // 2. NCs tipo E con tipoRelacion=07 en el período
  const ncs = await CFDI.find({
    $or:               [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }],
    ejercicio:         EJERCICIO,
    periodo:           PERIODO,
    tipoDeComprobante: 'E',
    'cfdiRelacionados.tipoRelacion': '07',
    source:            'SAT',
    satStatus:         'Vigente',
    isActive:          true,
  }).select('uuid folio subTotal cfdiRelacionados').lean();

  console.log(`\nNCs tipo E tipoRelacion=07 en período: ${ncs.length}`);

  let filtradas = 0;
  let residuales = 0;
  let subTotalResidual = 0;

  console.log('\n── NCs FILTRADAS (su factura PUE está en el período) ──────────────────');
  for (const nc of ncs) {
    const rel07  = (nc.cfdiRelacionados || []).find(r => r.tipoRelacion === '07');
    const uuid07 = (rel07?.uuids?.[0] ?? '').toUpperCase();
    if (uuid07 && setFacturasPue.has(uuid07)) {
      filtradas++;
      console.log(`  FILTRADA  folio=${nc.folio}  subTotal=${nc.subTotal}  → factura ${uuid07.slice(0,8)}...`);
    }
  }

  console.log('\n── NCs RESIDUALES (su factura PUE NO está en el período) ──────────────');
  for (const nc of ncs) {
    const rel07  = (nc.cfdiRelacionados || []).find(r => r.tipoRelacion === '07');
    const uuid07 = (rel07?.uuids?.[0] ?? '').toUpperCase();
    if (!uuid07 || !setFacturasPue.has(uuid07)) {
      residuales++;
      subTotalResidual += Number(nc.subTotal || 0);
      const motivo = !uuid07 ? 'sin UUID en cfdiRelacionados' : `factura ${uuid07.slice(0,8)}... no en período`;
      console.log(`  RESIDUAL  folio=${nc.folio}  subTotal=${nc.subTotal}  uuid=${nc.uuid.slice(0,8)}...  motivo: ${motivo}`);
      if (uuid07 && !setFacturasPue.has(uuid07)) {
        // Buscar la factura en otros períodos
        const factOtro = await CFDI.findOne({
          uuid:              { $regex: new RegExp(`^${uuid07}$`, 'i') },
          tipoDeComprobante: 'I',
          source:            'SAT',
        }).select('uuid folio ejercicio periodo formaPago satStatus isActive').lean();
        if (factOtro) {
          console.log(`         → factura encontrada en ejercicio=${factOtro.ejercicio} período=${factOtro.periodo} formaPago=${factOtro.formaPago} satStatus=${factOtro.satStatus} isActive=${factOtro.isActive}`);
        } else {
          console.log(`         → factura NO encontrada en MongoDB`);
        }
      }
    }
  }

  console.log(`\n── RESUMEN ────────────────────────────────────────────────────────────`);
  console.log(`  NCs totales:     ${ncs.length}`);
  console.log(`  Filtradas:       ${filtradas}`);
  console.log(`  Residuales:      ${residuales}`);
  console.log(`  SubTotal residual (≈ impacto en DEBE 2103010001): ${subTotalResidual.toFixed(2)}`);

  await mongoose.disconnect();
  process.exit(0);
}

run().catch(err => { console.error(err); process.exit(1); });
