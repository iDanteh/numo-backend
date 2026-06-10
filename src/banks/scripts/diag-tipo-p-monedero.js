'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const CFDI = require('../../visor/models/CFDI');

const RFC       = 'CCO011113663';
const EJERCICIO = 2026;

async function main() {
  await connectMongo();

  // Ver estructura real del tipo P formaPago=05
  const pagos = await CFDI.find({
    $or: [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }],
    ejercicio:         EJERCICIO,
    tipoDeComprobante: 'P',
    formaPago:         '05',
    source:            'SAT',
    satStatus:         'Vigente',
    isActive:          true,
  }).lean();

  console.log(`\nTipo P formaPago=05: ${pagos.length} pagos\n`);

  for (const p of pagos) {
    console.log('UUID:', p.uuid?.substring(0, 8));
    console.log('Periodo:', p.periodo);
    console.log('Complemento keys:', Object.keys(p.complementoPago ?? {}));
    const comp = p.complementoPago ?? {};
    // Intentar múltiples estructuras
    console.log('  totales:', comp.totales ?? '—');
    console.log('  pagos[]:', (comp.pagos ?? comp.pago ?? comp.Pagos ?? comp.Pago ?? []).slice(0, 1));
    console.log('  subTotal SAT:', p.subTotal);
    console.log('  total SAT:', p.total);
    console.log('---');
  }

  // También buscar tipo P formaPago=05 en TODOS los periodos del ejercicio
  const todosP05 = await CFDI.find({
    $or: [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }],
    ejercicio:         EJERCICIO,
    tipoDeComprobante: 'P',
    formaPago:         '05',
    source:            'SAT',
    satStatus:         'Vigente',
    isActive:          true,
  }).select('uuid periodo subTotal total complementoPago').lean();

  console.log(`\nTipo P formaPago=05 todo el ejercicio ${EJERCICIO}: ${todosP05.length}`);
  for (const p of todosP05) {
    // Extraer monto con múltiples estrategias
    const m40 = Number(p.complementoPago?.totales?.montoTotalPagos || 0);
    const pagArr = p.complementoPago?.pagos ?? p.complementoPago?.pago ?? p.complementoPago?.Pagos ?? p.complementoPago?.Pago ?? [];
    const m33 = pagArr.reduce((s, pg) => s + Number(pg.monto || pg.Monto || pg.importe || 0), 0);
    console.log(`  Periodo ${p.periodo} | uuid: ${p.uuid?.substring(0, 8)} | subTotal: ${p.subTotal} | total: ${p.total} | monto40: ${m40} | monto33: ${m33}`);
  }

  await disconnectMongo();
  process.exit(0);
}
main().catch(err => { console.error(err); process.exit(1); });
