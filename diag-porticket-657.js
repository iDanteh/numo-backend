'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { sequelize } = require('./src/config/database.postgres');
const CFDI = require('./src/visor/models/CFDI');
const { obtenerDesglosesCobroAlmacen } = require('./src/banks/domains/erp/erp-sync.service');
const { SERIES_CON_AUTH } = require('./src/banks/domains/erp/erp-auth.utils');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const SERIE = process.env.DIAG_SERIE || 'C0';
const FOLIO_FACTURA = process.env.DIAG_FOLIO || '260800657';

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  const cfdi = await CFDI.findOne({ serie: SERIE, folio: FOLIO_FACTURA, source: 'ERP' })
    .select('documentosRelacionados').lean();
  const docs = (cfdi.documentosRelacionados ?? []).filter(d => d.Serie && d.Folio);
  console.log('Total documentosRelacionados (esperado):', docs.length);

  const LOTE = 150;
  const cuentasPorTicket = new Map(); // ticketKey -> [cuenta,...]
  let totalCuentasDevueltas = 0;
  for (let i = 0; i < docs.length; i += LOTE) {
    const lote = docs.slice(i, i + LOTE);
    const resultado = await obtenerDesglosesCobroAlmacen({
      rfc: RFC, series: lote.map(d => d.Serie), folios: lote.map(d => d.Folio),
    });
    totalCuentasDevueltas += resultado.length;
    for (const cuenta of resultado) {
      const key = `${cuenta.serieVenta}-${cuenta.folioVenta}`;
      if (!cuentasPorTicket.has(key)) cuentasPorTicket.set(key, []);
      cuentasPorTicket.get(key).push(cuenta);
    }
  }
  console.log('Total cuentas devueltas por el ERP:', totalCuentasDevueltas);
  console.log('Tickets únicos (por serieVenta-folioVenta):', cuentasPorTicket.size);

  // Tickets con MÁS de una "cuenta" devuelta (posible duplicado/doble conteo).
  const duplicados = [...cuentasPorTicket.entries()].filter(([, cuentas]) => cuentas.length > 1);
  console.log(`\nTickets con más de 1 cuenta devuelta: ${duplicados.length}`);
  for (const [key, cuentas] of duplicados) {
    console.log(key, '->', cuentas.length, 'cuentas, cuentaIds:', cuentas.map(c => c.cuentaId));
  }

  // Total cobrado real por ticket (Efectivo/Tarjeta/Transferencia, sin SF/Puntos).
  const totalPorTicket = [];
  for (const [key, cuentas] of cuentasPorTicket) {
    let total = 0;
    for (const cuenta of cuentas) {
      for (const cobro of (cuenta.cobros ?? [])) {
        const origen = (cobro.serieOrigen ?? '').toUpperCase();
        if (origen !== 'CBT' && !SERIES_CON_AUTH.includes(origen)) continue;
        const fps = (cobro.formasPago ?? []).filter(fp => !/puntos|saldo\s*a\s*favor/i.test(fp.nombre ?? ''));
        if (!fps.length) continue;
        const monto = fps.length === 1 ? Math.abs(Number(cobro.monto) || 0) : fps.reduce((s, fp) => s + (Number(fp.monto) || 0), 0);
        total += monto;
      }
    }
    totalPorTicket.push({ ticket: key, total: Math.round(total * 100) / 100 });
  }
  totalPorTicket.sort((a, b) => b.total - a.total);
  console.log('\nTop 15 tickets por monto cobrado real:');
  for (const t of totalPorTicket.slice(0, 15)) console.log(t.ticket, '$' + t.total.toFixed(2));

  const sumaTotal = totalPorTicket.reduce((s, t) => s + t.total, 0);
  console.log('\nSuma total (debe coincidir con el diagnostico anterior, 138129.24):', sumaTotal.toFixed(2));

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
