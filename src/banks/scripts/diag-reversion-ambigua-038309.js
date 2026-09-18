'use strict';
/** Diagnóstico de solo lectura — inspecciona el BankMovement + su erpLink relevante
 *  para el caso reportado por el usuario (reversión "atribución ambigua" sobre
 *  A0-260703646, erpId 6a623ad30c57b7000171373b, movimiento 6a988ce009507619d8b2526e).
 *  No escribe nada. */
require('dotenv').config();
const mongoose     = require('mongoose');
const BankMovement = require('../domains/banks/BankMovement.model');

async function run() {
  await mongoose.connect(process.env.MONGODB_URI);
  console.log('DB conectada:', mongoose.connection.name, ' host:', mongoose.connection.host);
  const mov = await BankMovement.findById('6a988ce009507619d8b2526e').lean();
  if (!mov) { console.log('No se encontró el movimiento.'); process.exit(1); }

  console.log('folio:', mov.folio, ' banco:', mov.banco, ' deposito:', mov.deposito);
  console.log('status:', mov.status, ' saldoErp:', mov.saldoErp, ' saldoErpSyncedAt:', mov.saldoErpSyncedAt);
  console.log('isActive:', mov.isActive, ' oculto:', mov.oculto, ' createdAt:', mov.createdAt, ' updatedAt:', mov.updatedAt);
  console.log('erpIds:', mov.erpIds);
  console.log('identificadoPor:', JSON.stringify(mov.identificadoPor ?? []));
  console.log('\nerpLinks:');
  for (const l of mov.erpLinks ?? []) {
    console.log(`  erpId=${l.erpId} folioExterno=${l.folioExterno} saldoActual=${l.saldoActual} saldoErpAportado=${l.saldoErpAportado} saldoPagado=${l.saldoPagado} saldoPagadoTotal=${l.saldoPagadoTotal} conciliacionFinalizadaAt=${l.conciliacionFinalizadaAt}`);
  }
  console.log('\n_changelog (últimas 8):');
  for (const c of (mov._changelog ?? []).slice(-8)) {
    console.log(`  at=${c.at} via=${c.via} campo=${c.campo} de=${JSON.stringify(c.de)} a=${JSON.stringify(c.a)} revertedAt=${c.revertedAt}`);
  }
  console.log('\n--- Movimientos vinculados a erpId 6a623ad30c57b7000171373b (A0-260703646) ---');
  const vinculados = await BankMovement.find({ erpIds: '6a623ad30c57b7000171373b' }).lean();
  console.log(`Encontrados: ${vinculados.length}`);
  for (const m of vinculados) {
    const link = m.erpLinks.find(l => l.erpId === '6a623ad30c57b7000171373b');
    console.log(`  _id=${m._id} folio=${m.folio} deposito=${m.deposito} status=${m.status} saldoErp=${m.saldoErp}`);
    console.log(`    link: saldoErpAportado=${link?.saldoErpAportado} saldoActual=${link?.saldoActual}`);
  }

  console.log('\n--- ErpReversion por erpId ---');
  const ErpReversion = require('../domains/erp/ErpReversion.model');
  const reversiones = await ErpReversion.find({ erpId: '6a623ad30c57b7000171373b' }).lean();
  for (const r of reversiones) console.log(JSON.stringify(r, null, 2));

  console.log('\n--- ErpReversion por folioExterno=260703646 ---');
  const porFolio = await ErpReversion.find({ folioExterno: '260703646' }).lean();
  for (const r of porFolio) console.log(JSON.stringify(r, null, 2));

  console.log('\n--- ErpReversion por referencia=6aac451e8faa5c000173e53a ---');
  const porReferencia = await ErpReversion.find({ referencia: '6aac451e8faa5c000173e53a' }).lean();
  for (const r of porReferencia) console.log(JSON.stringify(r, null, 2));

  const total = await ErpReversion.countDocuments({});
  const masVieja = await ErpReversion.findOne({}).sort({ createdAt: 1 }).lean();
  const masNueva = await ErpReversion.findOne({}).sort({ createdAt: -1 }).lean();
  console.log(`\nTotal ErpReversion en la colección: ${total}`);
  console.log('Más vieja:', masVieja?.createdAt, ' Más nueva:', masNueva?.createdAt);

  console.log('\n--- Últimas 5 ErpReversion (cualquier erpId, para ver el shape real) ---');
  const ultimas = await ErpReversion.find({}).sort({ createdAt: -1 }).limit(5).lean();
  for (const r of ultimas) console.log(`  erpId=${r.erpId} folioExterno=${r.folioExterno} createdAt=${r.createdAt} atribucionConfiable=${r.atribucionConfiable} movimientosAfectados=${r.movimientosAfectados.length}`);

  await mongoose.connection.close();
}
run().catch(e => { console.error(e); process.exit(1); });
