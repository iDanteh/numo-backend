'use strict';

/**
 * diag-preview-opa.js
 * Llama a generarPropuesta (preview, NO persiste nada) para un dia/centro
 * puntual, con DEBUG_OPA_UUID activo para ver por que un CFDI no resuelve
 * su cierre de anticipo (OPA). Requiere que el CFDI de destino NO tenga ya
 * una poliza activa (cancelar primero con diag-cancelar-poliza.js).
 *
 * Uso:
 *   DEBUG_OPA_UUID=<uuid> node src/banks/scripts/diag-preview-opa.js <rfc> <centroCostoId> <fechaInicio> <fechaFin>
 */

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const { sequelize } = require('../../config/database.postgres');
const { generarPropuesta } = require('../domains/cfdi-mapping/cfdi-poliza-generator.service');

const [rfc, centroCostoIdArg, fechaInicio, fechaFin] = process.argv.slice(2);
if (!rfc || !centroCostoIdArg || !fechaInicio || !fechaFin) {
  console.error('Uso: node diag-preview-opa.js <rfc> <centroCostoId> <fechaInicio> <fechaFin>');
  process.exit(1);
}

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  const [ejercicio, mes] = fechaInicio.split('-').map(Number);

  const resultado = await generarPropuesta({
    rfc,
    ejercicio,
    periodo: mes,
    tipoCfdi: 'I',
    centroCostoId: Number(centroCostoIdArg),
    fechaInicio,
    fechaFin,
  });

  console.log('Advertencias:', resultado._meta?.advertencias);
  console.log('Total movimientos generados (preview):', resultado.movimientos?.length);

  const targetUuid = (process.env.DEBUG_OPA_UUID || '').toUpperCase();
  if (targetUuid) {
    const lineasTarget = resultado.movimientos.filter(m => (m.cfdiUuid || '').toUpperCase() === targetUuid);
    console.log(`\nLineas del preview para ${targetUuid}: ${lineasTarget.length}`);
    console.log(JSON.stringify(lineasTarget.map(m => ({
      cuenta: m.cuentaId, debe: m.debe, haber: m.haber, serie: m.serie, tipoOrigen: m.tipoOrigen, reglaNombre: m.reglaNombre,
    })), null, 2));
  }

  process.exit(0);
}

main().catch(err => { console.error('ERROR:', err.stack || err.message); process.exit(1); });
