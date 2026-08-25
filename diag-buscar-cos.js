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

  // Buscar SIN filtrar por poliza (puede que se hayan regenerado otras
  // sucursales despues de Hidalgo/B0) -- el monto exacto es suficientemente
  // especifico para encontrarlo entre todas las polizas recientes.
  // OJO: en el layout M1, la columna D es CARGO y la E es ABONO -- la linea
  // pegada por el usuario ("... 0 618.81 0 0 ...") trae 0 en cargo y 618.81
  // en abono, asi que puede ser un HABER, no un DEBE. Tambien buscamos por
  // concepto = "B0-260801859" (columna H, ese es el concepto real del M1,
  // no la regla).
  const movs = await PolizaMovimiento.findAll({
    where: {
      [Op.or]: [
        { reglaNombre: { [Op.like]: '%COS%' }, [Op.or]: [{ debe: MONTO }, { haber: MONTO }] },
        { concepto: { [Op.like]: `%${SERIEFOLIO}%` } },
      ],
    },
    attributes: ['id', 'polizaId', 'cuentaId', 'debe', 'haber', 'tipoOrigen', 'reglaNombre', 'concepto', 'serie', 'cfdiUuid', 'formaPago', 'centroCosto'],
    raw: true,
    limit: 20,
  });
  console.log(`\nMovimientos con reglaNombre LIKE '%COS%' (debe o haber=${MONTO}) O concepto LIKE '%${SERIEFOLIO}%' (todas las polizas):`, movs.length);
  for (const m of movs) console.log(JSON.stringify(m, null, 2));

  if (movs.length) {
    const polizaIdsEncontrados = [...new Set(movs.map(m => m.polizaId))];
    const polizasEncontradas = await Poliza.findAll({ where: { id: { [Op.in]: polizaIdsEncontrados } }, attributes: ['id', 'createdAt', 'estado'], raw: true });
    console.log('\nPolizas donde aparece este monto:', JSON.stringify(polizasEncontradas));

    // Mostrar TODOS los COS de esa(s) poliza(s) para contexto completo
    const todosCos = await PolizaMovimiento.findAll({
      where: { polizaId: { [Op.in]: polizaIdsEncontrados }, reglaNombre: { [Op.like]: '%COS%' } },
      attributes: ['polizaId', 'debe', 'haber', 'tipoOrigen', 'reglaNombre', 'concepto', 'serie', 'cfdiUuid', 'formaPago'],
      raw: true,
    });
    console.log(`\nTotal de movimientos COS en esa(s) poliza(s): ${todosCos.length}`);
    for (const m of todosCos) console.log(JSON.stringify(m));
  } else {
    console.log('\nNo se encontro ningun movimiento con ese monto exacto -- puede que el monto tenga mas decimales o el nombre de regla sea distinto. Buscando variantes cercanas...');
    const rango = [Number(MONTO) - 0.02, Number(MONTO) + 0.02];
    const cercanos = await PolizaMovimiento.findAll({
      where: {
        reglaNombre: { [Op.like]: '%COS%' },
        [Op.or]: [{ debe: { [Op.between]: rango } }, { haber: { [Op.between]: rango } }],
      },
      attributes: ['polizaId', 'debe', 'haber', 'tipoOrigen', 'reglaNombre', 'concepto', 'serie', 'cfdiUuid', 'formaPago'],
      raw: true,
    });
    console.log('Cercanos encontrados:', cercanos.length);
    for (const m of cercanos) console.log(JSON.stringify(m));
  }

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
