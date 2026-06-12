'use strict';

/**
 * migrate-cfdi-mapping-pago-generico.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Migración: agrega Reg 7Z — regla genérica de cobro tipo P.
 *
 * Problema que resuelve:
 *   Los CFDIs de pago CFDI 3.3 (Complemento de Pago 1.0) no tienen el nodo
 *   <Totales> con totalTrasladosImpuestoIVA16. El motor no puede determinar
 *   la tasa IVA y retorna null. Sin esta regla, esos pagos quedan sin regla
 *   de mapeo y no generan movimientos contables.
 *
 * Regla insertada:
 *   Reg 7Z — Cobro Genérico (CP 1.0 / forma de pago no clasificada)
 *   tipoComprobante=P, formaPago=null, tasaIva=null, prioridad=99
 *   Las reglas específicas (Reg 7A–7G, prioridad 70–74) siempre ganan primero.
 *
 * Es idempotente: no hace nada si la regla ya existe.
 *
 * Uso:
 *   node src/banks/scripts/migrate-cfdi-mapping-pago-generico.js
 *   docker exec numo-backend node src/banks/scripts/migrate-cfdi-mapping-pago-generico.js
 */

require('dotenv').config();

const { sequelize }   = require('../../config/database.postgres');
const CfdiMappingRule = require('../../shared/models/postgres/CfdiMappingRule');

async function run() {
  await sequelize.authenticate();
  console.log('PostgreSQL conectado.');

  const nombre = 'Reg 7Z — Cobro Genérico (CP 1.0 / forma de pago no clasificada)';

  const existe = await CfdiMappingRule.findOne({ where: { nombre } });
  if (existe) {
    console.log(`OK (ya existía): "${nombre}"`);
  } else {
    await CfdiMappingRule.create({
      nombre,
      tipoComprobante: 'P',
      formaPago:       null,
      tasaIva:         null,
      cuentaCargo:     '1101010003',  // Caja por identificar
      cuentaAbono:     '1103010001',  // Clientes Nac Gral 16% (default)
      cuentaIva:       '2104010001',  // IVA Trasladado definitivo
      cuentaIvaPPD:    '2105010001',  // IVA Por Trasladar PPD
      conceptoContiene: null,
      prioridad:       99,
      isActive:        true,
    });
    console.log(`Insertada: "${nombre}"`);
  }

  console.log('\nMigracion completada.');
  await sequelize.close();
  process.exit(0);
}

run().catch((err) => {
  console.error('Error en migración:', err);
  process.exit(1);
});
