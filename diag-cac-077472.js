'use strict';
require('dotenv').config();
const { PolizaMovimiento, AccountPlan } = require('./src/shared/models/postgres');
const { Op } = require('sequelize');

async function main() {
  const movs = await PolizaMovimiento.findAll({
    where: { concepto: { [Op.iLike]: '%CAC-077472%' } },
    order: [['polizaId', 'DESC'], ['orden', 'ASC']],
    raw: true,
  });
  console.log(`Encontrados: ${movs.length}`);
  const polizaIds = [...new Set(movs.map(m => m.polizaId))];
  console.log(`Polizas distintas: ${polizaIds.length} -> ${polizaIds.slice(0, 10).join(',')}`);
  const ultimaPolizaId = polizaIds[0];
  const movsUltima = movs.filter(m => m.polizaId === ultimaPolizaId);
  console.log(`\nMostrando solo la poliza mas reciente (id=${ultimaPolizaId}), ${movsUltima.length} lineas:`);
  const cuentaIds = [...new Set(movsUltima.map(m => m.cuentaId).filter(Boolean))];
  const cuentas = await AccountPlan.findAll({ where: { id: cuentaIds }, raw: true });
  const cuentaPorId = new Map(cuentas.map(c => [c.id, c]));
  for (const p of movsUltima) {
    const c = cuentaPorId.get(p.cuentaId);
    console.log(JSON.stringify({
      id: p.id, polizaId: p.polizaId, cuenta: c?.codigo, cuentaNombre: c?.nombre,
      debe: p.debe, haber: p.haber, serie: p.serie, concepto: p.concepto,
      tipoOrigen: p.tipoOrigen, reglaNombre: p.reglaNombre, cfdiUuid: p.cfdiUuid,
    }));
  }

  // Verificar si el ticket B0-260801094 tiene movimientos de OTRAS facturas
  // (distinto cfdiUuid) en la misma poliza -- confirmaria/descartaria la
  // hipotesis de interaccion con el fix de "ticket compartido".
  const todosDelTicket = await PolizaMovimiento.findAll({
    where: { polizaId: ultimaPolizaId, serie: 'B0-260801094' },
    order: [['orden', 'ASC']],
    raw: true,
  });
  const uuidsDistintos = [...new Set(todosDelTicket.map(m => m.cfdiUuid))];
  console.log(`\nTotal movimientos con serie='B0-260801094' en poliza ${ultimaPolizaId}: ${todosDelTicket.length}`);
  console.log(`cfdiUuid distintos: ${uuidsDistintos.length} -> ${uuidsDistintos.join(', ')}`);
  const cuentaIds2 = [...new Set(todosDelTicket.map(m => m.cuentaId).filter(Boolean))];
  const cuentas2 = await AccountPlan.findAll({ where: { id: cuentaIds2 }, raw: true });
  const cuentaPorId2 = new Map(cuentas2.map(c => [c.id, c]));
  for (const p of todosDelTicket) {
    const c = cuentaPorId2.get(p.cuentaId);
    console.log(JSON.stringify({
      id: p.id, cuenta: c?.codigo, cuentaNombre: c?.nombre,
      debe: p.debe, haber: p.haber, concepto: p.concepto,
      tipoOrigen: p.tipoOrigen, reglaNombre: p.reglaNombre, cfdiUuid: p.cfdiUuid,
    }));
  }
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
