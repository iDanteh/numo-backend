'use strict';

/**
 * diag-cruce-efectivo-salidas-caja.js
 *
 * Diagnostico de SOLO LECTURA: compara, para un dia+almacen puntual, el
 * "Depositos consolidados (Efectivo)" de la poliza activa contra la suma
 * real de salidas de caja reportadas por Kore (/desgloses-salidas/caja,
 * ver obtenerDesglosesSalidasCajaPorAlmacen en erp-sync.service.js).
 *
 * IMPORTANTE (confirmar antes de confiar en el numero): las salidas son
 * RETIROS PARCIALES acumulados durante el dia (una caja puede tener varios
 * "Salida por Transferencia" + un "CIERRE CAJA" final, cada uno con su
 * propio montoRetirado/montoRestante) -- este script solo SUMA todos los
 * montoRetirado del dia por almacen, sin distinguir tipo de movimiento.
 * Sirve para verificar manualmente varios casos reales antes de decidir si
 * esta suma simple es la comparacion correcta, o si hace falta excluir
 * algun tipo (ej. "RETIRO POR FALTANTE DE EFECTIVO" podria no deber
 * contarse igual que un deposito real a banco).
 *
 * No toca ninguna poliza ni el export -- solo lee e imprime.
 *
 * Uso:
 *   node src/banks/scripts/diag-cruce-efectivo-salidas-caja.js <rfc> <almacen> <fecha YYYY-MM-DD>
 */

require('dotenv').config();

const { connectMongo } = require('../../config/database.mongo');
const { sequelize } = require('../../config/database.postgres');
const { QueryTypes } = require('sequelize');
const { obtenerDesglosesSalidasCajaPorAlmacen } = require('../domains/erp/erp-sync.service');
const { exportContpaqXlsx } = require('../domains/polizas/poliza.service');

const [rfc, almacen, fecha] = process.argv.slice(2);
if (!rfc || !almacen || !fecha) {
  console.error('Uso: node diag-cruce-efectivo-salidas-caja.js <rfc> <almacen> <fecha YYYY-MM-DD>');
  process.exit(1);
}

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  // "Depósitos consolidados (Efectivo)" NO existe como fila propia en
  // poliza_movimientos -- se calcula en el momento del export
  // (consolidarCargos, poliza.service.js), sumando todos los Cargos de
  // venta normal a la cuenta de Caja. Hay que llamar al export real para
  // obtener el número, no se puede leer directo de Postgres.
  const [polizaRow] = await sequelize.query(`
    SELECT id FROM polizas
     WHERE rfc = :rfc AND fecha = :fecha AND estado != 'cancelada'
     ORDER BY created_at DESC LIMIT 1
  `, { replacements: { rfc, fecha }, type: QueryTypes.SELECT });

  let efectivoConsolidadoPoliza = 0;
  if (polizaRow) {
    const { workbooks } = await exportContpaqXlsx(polizaRow.id);
    for (const { workbook } of workbooks) {
      for (const ws of workbook.worksheets) {
        ws.eachRow({ includeEmpty: false }, (row) => {
          const vals = row.values.slice(1).map(v => (v && v.result !== undefined ? v.result : v));
          if (vals[7] === 'Depósitos consolidados (Efectivo)') {
            efectivoConsolidadoPoliza += Number(vals[4]) || 0;
          }
        });
      }
    }
  }
  console.log(`Efectivo consolidado en póliza activa [${polizaRow?.id ?? 'ninguna'}]: $${efectivoConsolidadoPoliza.toFixed(2)}`);

  const salidas = await obtenerDesglosesSalidasCajaPorAlmacen({
    rfc, almacen,
    fechaDesde: `${fecha}T00:00:00Z`, fechaHasta: `${fecha}T23:59:59Z`,
  });

  const porTipo = {};
  let totalSalidas = 0;
  for (const s of salidas) {
    const tipo = s.tipoMovimiento?.nombre ?? 'SIN TIPO';
    porTipo[tipo] = (porTipo[tipo] ?? 0) + (Number(s.montoRetirado) || 0);
    totalSalidas += Number(s.montoRetirado) || 0;
  }

  console.log(`\nSalidas de caja reales (${salidas.length} movimientos), por tipo:`);
  for (const [tipo, monto] of Object.entries(porTipo)) {
    console.log(`  ${tipo}: $${monto.toFixed(2)}`);
  }
  console.log(`  TOTAL: $${totalSalidas.toFixed(2)}`);

  console.log(`\nDiferencia (Efectivo consolidado - suma de salidas): $${(efectivoConsolidadoPoliza - totalSalidas).toFixed(2)}`);

  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e.stack || e.message); process.exit(1); });
