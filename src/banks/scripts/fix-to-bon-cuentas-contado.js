'use strict';
require('dotenv').config();

const { sequelize } = require('../../config/database.postgres');
const { CfdiMappingRule } = require('../../shared/models/postgres');

// Fix (2026-08-25): reglas TO-BON-* con formaPago='99'/'15' (creadas fuera
// del seed, edición manual del panel — mismo patrón ya corregido para
// TO-CAN-*, ver fix-to-can-cuentas-cancelacion.js) mandaban el Abono a
// Clientes (1103010001/1103010002) cuando sus reglas hermanas (mismo tipo,
// sin formaPago específico o Efectivo) correctamente usan Saldo a Favor
// (2103090001) o Caja (1101010003). Una Bonificación de Contado nunca debe
// afectar Clientes (no hay CxC que cerrar) — confirmado con el usuario:
// debe ir a Saldo a Favor, igual que sus reglas hermanas.
const FIXES = [
  { id: 1264, cuentaAbono: '2103090001' }, // TO-BON-16 fP99
  { id: 1265, cuentaAbono: '2103090001' }, // TO-BON-0 fP99
  { id: 1266, cuentaAbono: '2103090001' }, // TO-BON-M fP99
  { id: 1298, cuentaAbono: '2103090001' }, // TO-BON-16 fP15
  { id: 1299, cuentaAbono: '2103090001' }, // TO-BON-0 fP15
  { id: 1300, cuentaAbono: '2103090001' }, // TO-BON-M fP15
];

async function run() {
  await sequelize.authenticate();

  console.log('── Estado ANTES ──\n');
  const before = await CfdiMappingRule.findAll({ where: { id: FIXES.map(f => f.id) }, raw: true, order: [['id', 'ASC']] });
  for (const r of before) {
    console.log(`id=${r.id}  ${r.nombre}  cargo=${r.cuentaCargo} abono=${r.cuentaAbono} iva=${r.cuentaIva}`);
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
    console.log(`id=${r.id}  ${r.nombre}  cargo=${r.cuentaCargo} abono=${r.cuentaAbono} iva=${r.cuentaIva}`);
  }

  await sequelize.close();
  process.exit(0);
}

run().catch(e => { console.error(e); process.exit(1); });
