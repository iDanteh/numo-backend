'use strict';
require('dotenv').config();
const { sequelize } = require('./src/config/database.postgres');
const { Op } = require('sequelize');
const PolizaMovimiento = require('./src/shared/models/postgres/PolizaMovimiento');
const AccountPlan = require('./src/shared/models/postgres/AccountPlan');
const Poliza = require('./src/shared/models/postgres/Poliza');
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const CFDI = require('./src/visor/models/CFDI');

const MONTO = process.env.DIAG_MONTO || '618.81';
const SERIEFOLIO = process.env.DIAG_SERIEFOLIO || 'B0-260801859';

async function main() {
  await sequelize.authenticate();
  await connectMongo();

  const poliza = await Poliza.findOne({ where: { estado: 'borrador' }, order: [['createdAt', 'DESC']], attributes: ['id', 'createdAt'], raw: true });
  console.log('Poliza mas reciente: id=', poliza?.id, poliza?.createdAt);

  const movs = await PolizaMovimiento.findAll({
    where: {
      polizaId: poliza.id,
      reglaNombre: { [Op.like]: '%COS%' },
      debe: MONTO,
    },
    attributes: ['id', 'cuentaId', 'debe', 'haber', 'tipoOrigen', 'reglaNombre', 'concepto', 'serie', 'cfdiUuid', 'formaPago', 'centroCosto'],
    raw: true,
  });
  console.log(`\nMovimientos con reglaNombre LIKE '%COS%' y debe=${MONTO}:`, movs.length);
  for (const m of movs) console.log(JSON.stringify(m, null, 2));

  // Tambien buscar TODOS los COS de esta poliza, para dar contexto
  const todosCos = await PolizaMovimiento.findAll({
    where: { polizaId: poliza.id, reglaNombre: { [Op.like]: '%COS%' } },
    attributes: ['debe', 'haber', 'tipoOrigen', 'reglaNombre', 'concepto', 'serie', 'cfdiUuid', 'formaPago'],
    raw: true,
  });
  console.log(`\nTotal de movimientos COS en esta poliza: ${todosCos.length}`);
  for (const m of todosCos) console.log(JSON.stringify(m));

  // Si encontramos cfdiUuid, buscar ese CFDI real
  const uuids = [...new Set(movs.map(m => m.cfdiUuid).filter(Boolean))];
  if (uuids.length) {
    const cfdis = await CFDI.find({ uuid: { $in: uuids } }).select('uuid serie folio total fecha receptor.nombre tipoDeComprobante source satStatus').lean();
    console.log('\nCFDIs reales ligados a estos movimientos:');
    for (const c of cfdis) console.log(JSON.stringify(c));
  }

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
