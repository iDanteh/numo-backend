'use strict';

/**
 * diag-desglose-cobro-almacen.js
 * Llama DIRECTO a `obtenerDesglosesCobroAlmacen` (erp-sync.service.js) para
 * una serie/folio de venta puntual y vuelca el `formasPago` crudo que
 * devuelve el ERP (Kore), tal cual llega antes de cualquier transformación
 * interna — para confirmar si `claveSat` viene poblado y con qué valor.
 * Solo lectura (GET al ERP, sin tocar Mongo/Postgres).
 *
 * Uso:
 *   node src/banks/scripts/diag-desglose-cobro-almacen.js <rfc> <serie> <folio>
 */

require('dotenv').config();

const { obtenerDesglosesCobroAlmacen } = require('../domains/erp/erp-sync.service');

const rfc   = process.argv[2];
const serie = process.argv[3];
const folio = process.argv[4];

if (!rfc || !serie || !folio) {
  console.error('Uso: node diag-desglose-cobro-almacen.js <rfc> <serie> <folio>');
  process.exit(1);
}

async function main() {
  const cuentas = await obtenerDesglosesCobroAlmacen({ rfc, series: [serie], folios: [folio] });
  console.log(`Cuentas devueltas por el ERP: ${cuentas.length}`);
  console.log(JSON.stringify(cuentas, null, 2));
  process.exit(0);
}

main().catch(err => {
  console.error('ERROR:', err.message);
  process.exit(1);
});
