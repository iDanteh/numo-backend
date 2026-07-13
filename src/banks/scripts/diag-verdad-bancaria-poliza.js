'use strict';

/**
 * diag-verdad-bancaria-poliza.js
 * Para una poliza especifica: lista sus movimientos de cargo (Caja/Bancos)
 * con cfdiUuid, y cruza cada uno contra BankMovement (Mongo) replicando
 * construirVerdadBancaria, para ver si el sistema SI detecta una referencia
 * bancaria real que se esta perdiendo en el export, o si de plano no hay
 * match en BankMovement. Solo lectura.
 *
 * Uso:
 *   node src/banks/scripts/diag-verdad-bancaria-poliza.js <polizaId>
 */

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const { sequelize } = require('../../config/database.postgres');
const { PolizaMovimiento, AccountPlan } = require('../../shared/models/postgres');
const BankMovement = require('../domains/banks/BankMovement.model');

const polizaId = process.argv[2];
if (!polizaId) {
  console.error('Uso: node diag-verdad-bancaria-poliza.js <polizaId>');
  process.exit(1);
}

const CATEGORIAS_TRANSFERENCIA_BANCO = ['SPEI', 'TRASPASO'];

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  const movs = await PolizaMovimiento.findAll({
    where: { polizaId: polizaId },
    include: [{ model: AccountPlan, as: 'cuenta', attributes: ['codigo', 'nombre'] }],
    order: [['id', 'ASC']],
  });

  console.log('Total movimientos en la poliza: ' + movs.length);

  const cargos = movs.filter(function (m) {
    return Number(m.debe) > 0 && m.cfdiUuid;
  });

  console.log('Movimientos de cargo con cfdiUuid: ' + cargos.length);

  const uuids = [...new Set(cargos.map(function (m) { return (m.cfdiUuid || '').toUpperCase(); }))];

  const bankMovs = await BankMovement.find(
    { uuidXML: { $in: uuids } },
    { uuidXML: 1, categoria: 1, numeroAutorizacion: 1, referenciaNumerica: 1, formaPago: 1 },
  ).lean();

  console.log('Registros encontrados en BankMovement para estos uuids: ' + bankMovs.length + ' (de ' + uuids.length + ' uuids unicos)');

  const bankMap = {};
  for (const bm of bankMovs) {
    bankMap[bm.uuidXML.toUpperCase()] = bm;
  }

  console.log('--- Detalle por movimiento de cargo ---');
  for (const m of cargos) {
    const uuid = (m.cfdiUuid || '').toUpperCase();
    const bm = bankMap[uuid];
    const cat = bm ? (bm.categoria || '').toUpperCase() : null;
    const esTransferencia = bm ? CATEGORIAS_TRANSFERENCIA_BANCO.some(function (c) { return cat.includes(c); }) : false;
    const referencia = bm ? (bm.numeroAutorizacion || bm.referenciaNumerica || null) : null;
    console.log({
      cuenta: m.cuenta ? m.cuenta.codigo : m.cuentaId,
      debe: m.debe,
      descripcion: (m.descripcion || '').substring(0, 40),
      cfdiUuid: uuid,
      encontrado_en_BankMovement: !!bm,
      bm_categoria: bm ? bm.categoria : null,
      bm_formaPago: bm ? bm.formaPago : null,
      bm_numeroAutorizacion: bm ? bm.numeroAutorizacion : null,
      bm_referenciaNumerica: bm ? bm.referenciaNumerica : null,
      esTransferencia_calculado: esTransferencia,
      referencia_final: referencia,
    });
  }

  await cerrar();
}

async function cerrar() {
  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(function (err) { console.error(err); process.exit(1); });
