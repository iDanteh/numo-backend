'use strict';

/**
 * diag-desglose-pago-real-cfdi.js
 * Llama DIRECTO a `_prefetchAjustesFacturaPropia` (cfdi-poliza-generator.service.js)
 * para UN solo CFDI, replicando exactamente el mismo camino que usa la
 * generación real de pólizas (mismo centro/rango de fechas), y vuelca el
 * `desglosePagoReal` resultante — para ver si el claveSat/monto por forma de
 * pago real sobrevive hasta ahí. Solo lectura: no crea ni toca ninguna
 * póliza, no escribe en Postgres.
 *
 * Uso:
 *   node src/banks/scripts/diag-desglose-pago-real-cfdi.js <uuid> <rfc> <centro> <fechaYYYY-MM-DD>
 */

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const CFDI = require('../../visor/models/CFDI');
const { _prefetchAjustesFacturaPropia } = require('../domains/cfdi-mapping/cfdi-poliza-generator.service');

const CODIGO_CUENTA_BANCOS = '1102011005';

const [uuid, rfc, centro, fecha] = process.argv.slice(2);
if (!uuid || !rfc || !centro || !fecha) {
  console.error('Uso: node diag-desglose-pago-real-cfdi.js <uuid> <rfc> <centro> <fechaYYYY-MM-DD>');
  process.exit(1);
}

async function main() {
  await connectMongo();

  const cfdi = await CFDI.findOne({ uuid, source: 'SAT' }).lean()
    || await CFDI.findOne({ uuid }).lean();
  if (!cfdi) {
    console.error('CFDI no encontrado:', uuid);
    process.exit(1);
  }
  console.log('CFDI:', { source: cfdi.source, serie: cfdi.serie, folio: cfdi.folio, tipoDeComprobante: cfdi.tipoDeComprobante, formaPago: cfdi.formaPago, metodoPago: cfdi.metodoPago });

  const cfdiConRegla = [{ cfdi, rule: { cuentaCargo: CODIGO_CUENTA_BANCOS } }];

  // Mismo rango que usaria la generacion real de ese dia (Mexico, UTC-6).
  const fechaDesde = new Date(`${fecha}T06:00:00.000Z`);
  const fechaHasta = new Date(new Date(fechaDesde).getTime() + 24 * 3600 * 1000);

  const resultado = await _prefetchAjustesFacturaPropia(cfdiConRegla, rfc, {
    centroPropioClave: centro,
    fechaDesde,
    fechaHasta,
  });

  const key = `${cfdi.serie}|${cfdi.folio}`;
  console.log('\ndesglosePagoReal para', key, ':');
  console.log(JSON.stringify(resultado.desglosePagoReal.get(key) ?? null, null, 2));

  console.log('\nTodas las claves presentes en desglosePagoReal:', [...resultado.desglosePagoReal.keys()]);

  process.exit(0);
}

main().catch(err => {
  console.error('ERROR:', err.stack || err.message);
  process.exit(1);
});
