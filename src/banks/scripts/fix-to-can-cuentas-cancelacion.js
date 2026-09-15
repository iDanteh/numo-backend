'use strict';
require('dotenv').config();

const { sequelize } = require('../../config/database.postgres');
const { CfdiMappingRule } = require('../../shared/models/postgres');

// Fix: NC tipoOrigen='Cancelación' (documentosRelacionados.Serie='CANCELACION')
// deben quedar iguales sin importar formaPago:
//   DEBE Devoluciones s/Ventas 16% (4200010001) o 0% (4200010002) + DEBE IVA Trasladado (2104010001)
//   HABER Caja por identificar (1101010003) — nunca Efectivo/Bancos/Clientes según formaPago.
// Mixto: cuentaAbono2 = Devoluciones 0% (4200010002), no Ingresos 0%.
const FIXES = [
  { id: 201, cuentaCargo: '4200010001', cuentaAbono: '1101010003', cuentaIva: '2104010001' },
  { id: 203, cuentaCargo: '4200010001', cuentaAbono: '1101010003', cuentaIva: '2104010001' },
  { id: 205, cuentaCargo: '4200010002', cuentaAbono: '1101010003', cuentaIva: null },
  { id: 207, cuentaCargo: '4200010002', cuentaAbono: '1101010003', cuentaIva: null },
  { id: 209, cuentaCargo: '4200010001', cuentaAbono: '1101010003', cuentaIva: '2104010001', cuentaAbono2: '4200010002' },
  { id: 211, cuentaCargo: '4200010001', cuentaAbono: '1101010003', cuentaIva: '2104010001', cuentaAbono2: '4200010002' },
  { id: 289, cuentaCargo: '4200010001', cuentaAbono: '1101010003', cuentaIva: '2104010001' },
  { id: 290, cuentaCargo: '4200010001', cuentaAbono: '1101010003', cuentaIva: '2104010001', cuentaAbono2: '4200010002' },
  { id: 291, cuentaCargo: '4200010002', cuentaAbono: '1101010003', cuentaIva: null },
  { id: 325, cuentaCargo: '4200010001', cuentaAbono: '1101010003', cuentaIva: '2104010001' },
  { id: 326, cuentaCargo: '4200010001', cuentaAbono: '1101010003', cuentaIva: '2104010001', cuentaAbono2: '4200010002' },
  { id: 327, cuentaCargo: '4200010002', cuentaAbono: '1101010003', cuentaIva: null },
];

async function run() {
  await sequelize.authenticate();

  console.log('── Estado ANTES ──\n');
  const before = await CfdiMappingRule.findAll({ where: { id: FIXES.map(f => f.id) }, raw: true, order: [['id', 'ASC']] });
  for (const r of before) {
    console.log(`id=${r.id}  ${r.nombre}  cargo=${r.cuentaCargo} abono=${r.cuentaAbono} iva=${r.cuentaIva} abono2=${r.cuentaAbono2}`);
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
    console.log(`id=${r.id}  ${r.nombre}  cargo=${r.cuentaCargo} abono=${r.cuentaAbono} iva=${r.cuentaIva} abono2=${r.cuentaAbono2}`);
  }

  await sequelize.close();
  process.exit(0);
}

run().catch(e => { console.error(e); process.exit(1); });
