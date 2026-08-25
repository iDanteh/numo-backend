'use strict';
require('dotenv').config();
const { PolizaMovimiento, Poliza, AccountPlan } = require('./src/shared/models/postgres');

const UUID_FACTURA = '23503D5C-99D0-481C-9D6F-82C052EEAE50';

async function main() {
  const p = await Poliza.findOne({ where: { fecha: '2026-08-11' }, order: [['id', 'DESC']] });
  console.log('Poliza activa mas reciente 11-ago:', p ? { id: p.id, estado: p.estado } : null);
  if (!p) process.exit(0);

  const movs = await PolizaMovimiento.findAll({ where: { polizaId: p.id, cfdiUuid: UUID_FACTURA }, raw: true, order: [['orden', 'ASC']] });
  const cuentaIds = [...new Set(movs.map(m => m.cuentaId).filter(Boolean))];
  const cuentas = await AccountPlan.findAll({ where: { id: cuentaIds }, raw: true });
  const cuentaPorId = new Map(cuentas.map(c => [c.id, c]));

  console.log(`\nTotal movimientos de esta factura en poliza ${p.id}: ${movs.length}`);
  let totalDebe = 0, totalHaber = 0;
  const porTipoOrigenReglaCuenta = new Map();
  for (const m of movs) {
    totalDebe += Number(m.debe) || 0;
    totalHaber += Number(m.haber) || 0;
    const c = cuentaPorId.get(m.cuentaId);
    const key = `${m.tipoOrigen}|${m.reglaNombre}|${c?.codigo}|${m.formaPago}`;
    const acc = porTipoOrigenReglaCuenta.get(key) ?? { debe: 0, haber: 0, n: 0 };
    acc.debe += Number(m.debe) || 0;
    acc.haber += Number(m.haber) || 0;
    acc.n += 1;
    porTipoOrigenReglaCuenta.set(key, acc);
  }
  console.log(`Total debe: ${totalDebe.toFixed(2)}  Total haber: ${totalHaber.toFixed(2)}`);
  console.log('\nAgrupado por tipoOrigen|reglaNombre|cuenta|formaPago:');
  for (const [key, acc] of [...porTipoOrigenReglaCuenta.entries()].sort((a, b) => b[1].debe - a[1].debe)) {
    console.log(`  ${key} -> debe=${acc.debe.toFixed(2)} haber=${acc.haber.toFixed(2)} (n=${acc.n})`);
  }

  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
