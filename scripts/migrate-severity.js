'use strict';

require('dotenv').config();
const mongoose = require('mongoose');

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/cfdi_comparator';

async function main() {
  await mongoose.connect(MONGODB_URI);
  console.log('Conectado a MongoDB\n');

  const Discrepancy = require('../src/models/Discrepancy');

  // ── Diagnóstico completo ──────────────────────────────────────────────────────
  const totalCriticas = await Discrepancy.countDocuments({ severity: 'critical' });
  console.log(`Total discrepancias críticas: ${totalCriticas}`);

  if (totalCriticas === 0) {
    console.log('No hay discrepancias críticas. Nada que migrar.');
    await mongoose.disconnect();
    return;
  }

  // Desglose por tipo
  const porTipo = await Discrepancy.aggregate([
    { $match: { severity: 'critical' } },
    { $group: { _id: '$type', count: { $sum: 1 }, conImpacto: { $sum: { $cond: [{ $gt: ['$fiscalImpact.amount', 0] }, 1, 0] } } } },
    { $sort: { count: -1 } },
  ]);
  console.log('\nCríticas por tipo:');
  porTipo.forEach(t => console.log(`  ${t._id || 'sin_tipo'}: ${t.count} total, ${t.conImpacto} con fiscalImpact`));

  // Rango de importes en críticas con fiscalImpact
  const rangos = await Discrepancy.aggregate([
    { $match: { severity: 'critical', 'fiscalImpact.amount': { $gt: 0 } } },
    {
      $group: {
        _id: null,
        min: { $min: '$fiscalImpact.amount' },
        max: { $max: '$fiscalImpact.amount' },
        total: { $sum: 1 },
        menorOIgual001: { $sum: { $cond: [{ $lte: ['$fiscalImpact.amount', 0.01] }, 1, 0] } },
      },
    },
  ]);
  if (rangos.length) {
    const r = rangos[0];
    console.log(`\nRango fiscalImpact.amount en críticas: min=${r.min}  max=${r.max}`);
    console.log(`Críticas con amount > 0: ${r.total}  de las cuales amount <= 0.01: ${r.menorOIgual001}`);
  } else {
    console.log('\nNinguna crítica tiene fiscalImpact.amount > 0');
  }

  // Muestra de críticas con fiscalImpact (para ver valores reales)
  const muestra = await Discrepancy.find(
    { severity: 'critical', 'fiscalImpact.amount': { $exists: true, $gt: 0 } },
    { type: 1, severity: 1, fiscalImpact: 1, description: 1 }
  ).limit(5).lean();

  if (muestra.length) {
    console.log('\nMuestra de críticas con fiscalImpact:');
    muestra.forEach(d => console.log(`  type=${d.type}  amount=${d.fiscalImpact?.amount}  desc=${d.description?.slice(0, 70)}`));
  }

  // Muestra de críticas SIN fiscalImpact
  const sinImpacto = await Discrepancy.find(
    { severity: 'critical', $or: [{ fiscalImpact: { $exists: false } }, { 'fiscalImpact.amount': { $exists: false } }] },
    { type: 1, severity: 1, description: 1 }
  ).limit(5).lean();

  if (sinImpacto.length) {
    console.log('\nMuestra de críticas SIN fiscalImpact:');
    sinImpacto.forEach(d => console.log(`  type=${d.type}  desc=${d.description?.slice(0, 70)}`));
  }

  // ── Migración ────────────────────────────────────────────────────────────────
  console.log('\n──────────────────────────────────────────────────');
  console.log('Ejecutando migración...');

  // 1. Degrada críticas con fiscalImpact.amount entre 0 y $0.01
  // Se usa 0.0101 como umbral para absorber imprecisión de punto flotante
  // (0.01 se almacena como 0.010000000000000009 en IEEE-754)
  const res = await Discrepancy.updateMany(
    { severity: 'critical', 'fiscalImpact.amount': { $gt: 0, $lte: 0.0101 } },
    { $set: { severity: 'warning' } }
  );
  console.log(`[1] fiscalImpact.amount <= 0.01 → warning: ${res.modifiedCount} actualizadas`);

  // 2. TAX_CALCULATION_ERROR sin impacto o con importe 0 → también son advertencias
  const resCero = await Discrepancy.updateMany(
    {
      severity: 'critical',
      type: 'TAX_CALCULATION_ERROR',
      $or: [
        { 'fiscalImpact.amount': 0 },
        { 'fiscalImpact.amount': { $exists: false } },
        { fiscalImpact: { $exists: false } },
      ],
    },
    { $set: { severity: 'warning' } }
  );
  console.log(`[2] TAX_CALCULATION_ERROR sin impacto → warning: ${resCero.modifiedCount} actualizadas`);

  const totalMigradas = res.modifiedCount + resCero.modifiedCount;
  console.log(`\nTotal discrepancias migradas: ${totalMigradas}`);

  // ── Migrar lastComparisonStatus de CFDIs ─────────────────────────────────────
  // CFDIs con lastComparisonStatus='discrepancy' cuyas discrepancias son todas 'warning'
  // deben pasar a lastComparisonStatus='warning' para mostrar fila amarilla en UI.
  console.log('\nBuscando CFDIs con solo advertencias...');
  const CFDI = require('../src/models/CFDI');

  // UUIDs que aún tienen al menos una discrepancia crítica abierta
  const uuidsConCriticas = await Discrepancy.distinct('uuid', {
    severity: 'critical',
    status: { $nin: ['resolved', 'ignored', 'accepted'] },
  });

  // CFDIs con status 'discrepancy' que NO están en la lista de críticas
  const cfdisMigrar = await CFDI.updateMany(
    {
      lastComparisonStatus: 'discrepancy',
      uuid: { $nin: uuidsConCriticas },
    },
    { $set: { lastComparisonStatus: 'warning' } }
  );
  console.log(`CFDIs actualizados a 'warning': ${cfdisMigrar.modifiedCount}`);

  await mongoose.disconnect();
  console.log('\nListo.');
}

main().catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
