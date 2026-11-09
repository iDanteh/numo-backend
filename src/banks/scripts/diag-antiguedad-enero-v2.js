'use strict';

/**
 * diag-antiguedad-enero-v2.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Verificación post-fix (2026-11-06, cuarta ronda): confirma contra la base real
 * que el bucket "Más de 90 días" de enero 2026 (modo Año de Antigüedad) ya NO
 * incluye los depósitos `status:'identificado'` sin rastro de vinculación
 * (identificados por ficha o editados a mano) que causaban el falso positivo.
 *
 * Uso: node src/banks/scripts/diag-antiguedad-enero-v2.js
 */

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const { getAntiguedadDepositosPorAnio } = require('../domains/banks/bank-indicadores.service');

async function main() {
  await connectMongo();

  const { meses } = await getAntiguedadDepositosPorAnio({ anio: 2026 });
  const enero = meses.find((m) => m.periodo === '2026-01');

  console.log('=== ene 26, agregado real (post-fix) ===');
  console.log(JSON.stringify(enero, null, 2));

  await disconnectMongo();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
