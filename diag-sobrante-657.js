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
    .select('uuid serie folio total subTotal impuestos.totalImpuestosTrasladados documentosRelacionados').lean();
  if (!cfdi) { console.log('CFDI no encontrado'); process.exit(1); }

  const iva = Number(cfdi.impuestos?.totalImpuestosTrasladados || 0);
  const subtotal = Number(cfdi.subTotal || 0);
  const total = Number(cfdi.total || 0);
  console.log(`CFDI ${SERIE}-${FOLIO_FACTURA}: subTotal=${subtotal}, iva=${iva}, total=${total}`);

  const docs = (cfdi.documentosRelacionados ?? []).filter(d => d.Serie && d.Folio);
  console.log('Total tickets (documentosRelacionados):', docs.length);

  const LOTE = 150;
  let totalCobrosReales = 0;
  let totalPuntos = 0;
  const porFormaPago = {};
  for (let i = 0; i < docs.length; i += LOTE) {
    const lote = docs.slice(i, i + LOTE);
    const resultado = await obtenerDesglosesCobroAlmacen({
      rfc: RFC, series: lote.map(d => d.Serie), folios: lote.map(d => d.Folio),
    });
    for (const cuenta of resultado) {
      for (const cobro of (cuenta.cobros ?? [])) {
        const origen = (cobro.serieOrigen ?? '').toUpperCase();
        if (origen === 'CBT') {
          for (const fp of (cobro.formasPago ?? [])) {
            if (/puntos/i.test(fp.nombre ?? '')) { totalPuntos += Number(fp.monto) || 0; continue; }
            if (/saldo\s*a\s*favor/i.test(fp.nombre ?? '')) continue;
            const monto = (cobro.formasPago.length === 1 && cobro.monto != null)
              ? Math.abs(Number(cobro.monto) || 0)
              : (Number(fp.monto) || 0);
            totalCobrosReales += monto;
            const clave = fp.claveSat ?? '??';
            porFormaPago[clave] = (porFormaPago[clave] || 0) + monto;
          }
        } else if (SERIES_CON_AUTH.includes(origen)) {
          for (const fp of (cobro.formasPago ?? [])) {
            if (/puntos/i.test(fp.nombre ?? '')) { totalPuntos += Number(fp.monto) || 0; continue; }
            if (/saldo\s*a\s*favor/i.test(fp.nombre ?? '')) continue;
            const monto = (cobro.formasPago.length === 1 && cobro.monto != null)
              ? Math.abs(Number(cobro.monto) || 0)
              : (Number(fp.monto) || 0);
            totalCobrosReales += monto;
            const clave = fp.claveSat ?? '??';
            porFormaPago[clave] = (porFormaPago[clave] || 0) + monto;
          }
        }
      }
    }
  }

  console.log('\nTotal cobros reales (Efectivo/Tarjeta/etc, excluye SF/Puntos):', totalCobrosReales.toFixed(2));
  console.log('Por forma de pago (claveSat):', porFormaPago);
  console.log('Total Puntos:', totalPuntos.toFixed(2));
  console.log('\nmontoCargo esperado (subtotal, tipo I normal):', subtotal.toFixed(2));
  console.log('Diferencia (subtotal - cobrosReales):', (subtotal - totalCobrosReales).toFixed(2));
  console.log('Diferencia (total - cobrosReales):', (total - totalCobrosReales).toFixed(2));

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
