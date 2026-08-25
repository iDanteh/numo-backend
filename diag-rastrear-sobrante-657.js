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

  const LOTE = 150;
  let totalUsandoMontoRepetido = 0;
  let totalUsandoMontoCobro = 0;
  const sospechosos = [];
  for (let i = 0; i < docs.length; i += LOTE) {
    const lote = docs.slice(i, i + LOTE);
    const resultado = await obtenerDesglosesCobroAlmacen({
      rfc: RFC, series: lote.map(d => d.Serie), folios: lote.map(d => d.Folio),
    });
    for (const cuenta of resultado) {
      for (const cobro of (cuenta.cobros ?? [])) {
        const origen = (cobro.serieOrigen ?? '').toUpperCase();
        if (origen !== 'CBT' && !SERIES_CON_AUTH.includes(origen)) continue;
        const fps = (cobro.formasPago ?? []).filter(fp => !/puntos|saldo\s*a\s*favor/i.test(fp.nombre ?? ''));
        if (!fps.length) continue;
        const sumaFp = fps.reduce((s, fp) => s + (Number(fp.monto) || 0), 0);
        const montoCobro = Math.abs(Number(cobro.monto) || 0);
        totalUsandoMontoRepetido += sumaFp;
        totalUsandoMontoCobro += (fps.length === 1 ? montoCobro : sumaFp);
        // Sospechoso: 2+ formasPago Y su suma NO coincide con cobro.monto
        // (indicio del bug de monto repetido en varios tickets a la vez).
        if (fps.length > 1 && Math.abs(sumaFp - montoCobro) > 1) {
          sospechosos.push({
            ticket: `${cuenta.serieVenta}-${cuenta.folioVenta}`,
            folioOrigen: cobro.folioOrigen, montoCobro, sumaFormasPago: sumaFp,
            formasPago: fps.map(fp => ({ nombre: fp.nombre, monto: fp.monto })),
          });
        }
      }
    }
  }

  console.log('Total usando fp.monto tal cual (posible bug repetido):', totalUsandoMontoRepetido.toFixed(2));
  console.log('Total usando cobro.monto cuando hay 1 solo fp (ya corregido):', totalUsandoMontoCobro.toFixed(2));
  console.log('Diferencia entre ambos métodos:', (totalUsandoMontoRepetido - totalUsandoMontoCobro).toFixed(2));
  console.log(`\nCobros con 2+ formasPago y suma≠cobro.monto: ${sospechosos.length}`);
  for (const s of sospechosos) console.log(JSON.stringify(s, null, 2));

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
