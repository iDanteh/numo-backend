'use strict';
require('dotenv').config();

const { sequelize } = require('../../config/database.postgres');
const { CfdiMappingRule } = require('../../shared/models/postgres');

// Fix (2026-09-04): "Reg CC-BON-ERP-16/0/M" (ids 1394/1395/1396, "cancela CxC")
// tenian TODOS sus filtros de matching en null salvo tipoComprobante/tasaIva/
// tipoOrigen -- exactamente los mismos que usa TODA la familia TO-BON-*
// (Contado, prioridad 71). Como el motor de reglas hace ganar primero por
// PRIORIDAD NUMERICA (menor gana) y solo usa "mas especifica" como
// desempate, esta regla (prioridad 69) le ganaba SIEMPRE a TO-BON-*, sin
// importar si la venta origen era de Contado (PUE) o credito (PPD).
// Confirmado con datos reales: 788 usos historicos de 1394, contra 0 usos de
// las 16 variantes TO-BON-16/0/M (con y sin Efectivo) -- nunca se disparo la
// regla de Contado ni una sola vez desde que existen (2026-06-09).
// Fix: agregar metodoPago='PPD' para que "cancela CxC" solo aplique cuando
// la venta original SI fue a credito real -- las de Contado (PUE) ahora
// caeran solas en TO-BON-* (Saldo a Favor/Efectivo), como debe ser.
const FIXES = [
  { id: 1394, metodoPago: 'PPD' }, // Reg CC-BON-ERP-16
  { id: 1395, metodoPago: 'PPD' }, // Reg CC-BON-ERP-0
  { id: 1396, metodoPago: 'PPD' }, // Reg CC-BON-ERP-M
];

async function run() {
  await sequelize.authenticate();

  console.log('── Estado ANTES ──\n');
  const before = await CfdiMappingRule.findAll({ where: { id: FIXES.map(f => f.id) }, raw: true, order: [['id', 'ASC']] });
  for (const r of before) {
    console.log(`id=${r.id}  ${r.nombre}  metodoPago=${r.metodoPago} formaPago=${r.formaPago} prioridad=${r.prioridad} vecesUsada=${r.vecesUsada}`);
  }

  console.log('\n── Aplicando UPDATEs ──\n');
  for (const fix of FIXES) {
    const { id, ...vals } = fix;
    const [count] = await CfdiMappingRule.update(vals, { where: { id } });
    console.log(`id=${id}: ${count} fila(s) actualizada(s)`);
  }

  console.log('\n── Estado DESPUÉS ──\n');
  const after = await CfdiMappingRule.findAll({ where: { id: FIXES.map(f => f.id) }, raw: true, order: [['id', 'ASC']] });
  for (const r of after) {
    console.log(`id=${r.id}  ${r.nombre}  metodoPago=${r.metodoPago} formaPago=${r.formaPago} prioridad=${r.prioridad} vecesUsada=${r.vecesUsada}`);
  }

  await sequelize.close();
  process.exit(0);
}

run().catch(e => { console.error(e); process.exit(1); });
