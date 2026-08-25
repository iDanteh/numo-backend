'use strict';
require('dotenv').config();
const { CfdiMappingRule } = require('./src/shared/models/postgres');
const { Op } = require('sequelize');

async function main() {
  const reglas = await CfdiMappingRule.findAll({
    where: { nombre: { [Op.iLike]: '%bonific%' } },
    raw: true,
  });
  console.log(`Encontradas: ${reglas.length}`);
  for (const r of reglas) {
    console.log(JSON.stringify({
      id: r.id, nombre: r.nombre, tipoComprobante: r.tipoComprobante, metodoPago: r.metodoPago,
      formaPago: r.formaPago, tipoOrigen: r.tipoOrigen, cuentaCargo: r.cuentaCargo, cuentaAbono: r.cuentaAbono,
      cuentaIva: r.cuentaIva, cuentaIvaPPD: r.cuentaIvaPPD, prioridad: r.prioridad, isActive: r.isActive,
    }));
  }
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
