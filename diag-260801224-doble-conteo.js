'use strict';
require('dotenv').config();
const { PolizaMovimiento, Poliza, AccountPlan } = require('./src/shared/models/postgres');
const { Op } = require('sequelize');

async function main() {
  // Buscar el CFDI real con folio 260801224 (SAT) para obtener su uuid.
  const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
  const CFDI = require('./src/visor/models/CFDI');
  await connectMongo();
  const cfdi = await CFDI.findOne({ serie: 'B0', folio: '260801224', source: 'SAT' }).select('uuid folio serie total fecha receptor.nombre').lean();
  console.log('CFDI 260801224:', JSON.stringify(cfdi));
  await disconnectMongo();

  if (!cfdi?.uuid) { process.exit(0); }

  const movs = await PolizaMovimiento.findAll({
    where: { cfdiUuid: cfdi.uuid },
    order: [['polizaId', 'DESC'], ['orden', 'ASC']],
    raw: true,
  });
  console.log(`Total movimientos con este cfdiUuid (todas las polizas, todos los intentos): ${movs.length}`);

  const polizaIds = [...new Set(movs.map(m => m.polizaId))];
  const polizas = await Poliza.findAll({ where: { id: polizaIds }, attributes: ['id', 'fecha', 'estado'], raw: true });
  const polizaPorId = new Map(polizas.map(p => [p.id, p]));

  const cuentaIds = [...new Set(movs.map(m => m.cuentaId).filter(Boolean))];
  const cuentas = await AccountPlan.findAll({ where: { id: cuentaIds }, raw: true });
  const cuentaPorId = new Map(cuentas.map(c => [c.id, c]));

  for (const m of movs) {
    const p = polizaPorId.get(m.polizaId);
    const c = cuentaPorId.get(m.cuentaId);
    console.log(JSON.stringify({
      polizaId: m.polizaId, polizaFecha: p?.fecha, polizaEstado: p?.estado,
      cuenta: c?.codigo, cuentaNombre: c?.nombre, debe: m.debe, haber: m.haber,
      serie: m.serie, concepto: m.concepto, tipoOrigen: m.tipoOrigen, reglaNombre: m.reglaNombre,
    }));
  }
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
