'use strict';
require('dotenv').config();
const { sequelize } = require('./src/config/database.postgres');
const CfdiMappingRule = require('./src/shared/models/postgres/CfdiMappingRule');

async function run() {
  await sequelize.authenticate();
  const deleted = await CfdiMappingRule.destroy({
    where: { nombre: [
      'Reg CC-CAN-01-16-EF — NC Cancelación NC01 16% Efectivo',
      'Reg CC-CAN-01-16 — NC Cancelación NC01 16%',
      'Reg CC-CAN-01-M-EF — NC Cancelación NC01 Mixta Efectivo',
      'Reg CC-CAN-01-M — NC Cancelación NC01 Mixta',
    ]},
  });
  console.log(`Eliminadas: ${deleted} reglas CC-CAN-01`);
  await sequelize.close();
  process.exit(0);
}
run().catch(e => { console.error(e.message); process.exit(1); });
