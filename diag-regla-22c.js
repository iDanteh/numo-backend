'use strict';
require('dotenv').config();
const { CfdiMappingRule } = require('./src/shared/models/postgres');
const { Op } = require('sequelize');

async function main() {
  const reglas = await CfdiMappingRule.findAll({
    where: { nombre: { [Op.iLike]: '%Factura Final Anticipo%' } },
    raw: true,
  });
  console.log(`Encontradas: ${reglas.length}`);
  for (const r of reglas) {
    console.log(JSON.stringify(r));
  }
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
