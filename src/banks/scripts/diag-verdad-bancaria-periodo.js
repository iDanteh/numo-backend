'use strict';

/**
 * diag-verdad-bancaria-periodo.js
 * Version corregida de diag-verdad-bancaria-poliza.js: ese script viejo
 * cruzaba por BankMovement.uuidXML (campo legado, ~13% de cobertura). El
 * export real (`construirVerdadBancaria` en poliza.service.js) cruza por
 * `erpLinks.folioFiscal` (~59% de cobertura) — este script replica ESE
 * cruce para poder diagnosticar por que ciertos periodos no muestran pagos
 * identificados aunque el banco si tenga el deposito real. Solo lectura.
 *
 * Ademas separa dos causas posibles:
 *   1) No hay bank_movements con fecha en ese mes (extracto no importado).
 *   2) Si hay bank_movements, pero no tienen erpLinks (conciliacion pendiente
 *      — nadie ha vinculado esos depositos contra las CxC del ERP todavia).
 *
 * Uso:
 *   node src/banks/scripts/diag-verdad-bancaria-periodo.js <rfc> <ejercicio> <periodo>
 */

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const { sequelize } = require('../../config/database.postgres');
const { Poliza, PolizaMovimiento, AccountPlan } = require('../../shared/models/postgres');
const BankMovement = require('../domains/banks/BankMovement.model');

const rfc       = process.argv[2];
const ejercicio = parseInt(process.argv[3], 10);
const periodo   = parseInt(process.argv[4], 10);

if (!rfc || !ejercicio || !periodo) {
  console.error('Uso: node diag-verdad-bancaria-periodo.js <rfc> <ejercicio> <periodo>');
  process.exit(1);
}

const CATEGORIAS_TRANSFERENCIA_BANCO = ['SPEI', 'TRASPASO'];

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  // ── 1) Bank statement importado para ese mes? ──────────────────────────────
  const desde = new Date(Date.UTC(ejercicio, periodo - 1, 1));
  const hasta = new Date(Date.UTC(ejercicio, periodo, 1));
  const totalBankMovsPeriodo = await BankMovement.countDocuments({
    fecha: { $gte: desde, $lt: hasta }, isActive: true,
  });
  const conErpLinksPeriodo = await BankMovement.countDocuments({
    fecha: { $gte: desde, $lt: hasta }, isActive: true, 'erpLinks.0': { $exists: true },
  });
  console.log(`[1] BankMovements con fecha en ${ejercicio}-${String(periodo).padStart(2, '0')}: ${totalBankMovsPeriodo} totales, ${conErpLinksPeriodo} con al menos un erpLink` +
    (totalBankMovsPeriodo > 0 ? ` (${((conErpLinksPeriodo / totalBankMovsPeriodo) * 100).toFixed(1)}% conciliados)` : ' — SIN DATOS: el extracto de este mes no parece estar importado.'));

  // ── 2) De las polizas de Ingreso de este RFC/periodo, cuantos CFDIs de   ──
  //      cargo tienen match real contra BankMovement.erpLinks.folioFiscal? ──
  const polizas = await Poliza.findAll({
    where: { rfc, ejercicio, periodo, tipo: 'I' },
    attributes: ['id', 'numero', 'fecha', 'concepto'],
  });
  console.log(`\n[2] Polizas de Ingreso para ${rfc} ${ejercicio}-${periodo}: ${polizas.length}`);
  if (polizas.length === 0) { await cerrar(); return; }

  const polizaIds = polizas.map((p) => p.id);
  const movs = await PolizaMovimiento.findAll({
    where: { polizaId: polizaIds },
    include: [{ model: AccountPlan, as: 'cuenta', attributes: ['codigo', 'nombre'] }],
    order:  [['id', 'ASC']],
  });
  console.log('Total movimientos en esas polizas: ' + movs.length);

  const cargos = movs.filter((m) => Number(m.debe) > 0 && m.cfdiUuid);
  console.log('Movimientos de cargo (Caja/Bancos) con cfdiUuid: ' + cargos.length);

  const uuids = [...new Set(cargos.map((m) => (m.cfdiUuid || '').toUpperCase()))];
  console.log('CFDIs unicos: ' + uuids.length);
  if (uuids.length === 0) { await cerrar(); return; }

  const uuidsSet = new Set(uuids);
  const bankMovs = await BankMovement.find(
    { 'erpLinks.folioFiscal': { $in: uuids.map((u) => new RegExp(`^${u}$`, 'i')) } },
    { erpLinks: 1, categoria: 1, numeroAutorizacion: 1, referenciaNumerica: 1 },
  ).lean();
  console.log('Registros de BankMovement cuyo erpLinks matchea alguno de estos CFDIs: ' + bankMovs.length);

  const mapa = new Map();
  for (const bm of bankMovs) {
    const cat = (bm.categoria || '').toUpperCase();
    const esTransferencia = CATEGORIAS_TRANSFERENCIA_BANCO.some((c) => cat.includes(c));
    const referencia = bm.numeroAutorizacion || bm.referenciaNumerica || null;
    for (const link of (bm.erpLinks ?? [])) {
      const folioFiscalUpper = (link.folioFiscal || '').toUpperCase();
      if (!uuidsSet.has(folioFiscalUpper)) continue;
      const actual = mapa.get(folioFiscalUpper);
      if (!actual || (!actual.esTransferencia && esTransferencia)) {
        mapa.set(folioFiscalUpper, { esTransferencia, referencia });
      }
    }
  }

  console.log(`\n[3] CFDIs con match real (erpLinks.folioFiscal): ${mapa.size} de ${uuids.length} (${((mapa.size / uuids.length) * 100).toFixed(1)}%)`);

  console.log('\n--- Primeros 20 CFDIs SIN match (candidatos a "conciliacion pendiente") ---');
  let mostrados = 0;
  for (const uuid of uuids) {
    if (mapa.has(uuid)) continue;
    if (mostrados >= 20) break;
    console.log(uuid);
    mostrados++;
  }

  console.log('\n--- CFDIs CON match (deberian aparecer como "Deposito identificado" en el export) ---');
  for (const [uuid, info] of mapa) {
    console.log({ uuid, ...info });
  }

  await cerrar();
}

async function cerrar() {
  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch((err) => { console.error(err); process.exit(1); });
