'use strict';

/**
 * diag-cortes-ledger-dia-mes.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Diagnóstico de consistencia entre el tab "Diario" y el tab "Mensual" del panel
 * de Cortes, tras el fix de _primeraIdentificacionEfectiva (fallback a fichaAt,
 * 2026-11-10). Script de SOLO LECTURA, no modifica nada.
 *
 * Qué revisa:
 *   1. getLedgerMensual() de los últimos meses (incluye el mes en curso)
 *   2. getLedgerDiario() de los últimos 14 días
 *   3. Si el "total" (backlog) del día MÁS RECIENTE de getLedgerDiario coincide
 *      con el "total" del mes en curso de getLedgerMensual (misma fecha de
 *      corte real: ambos cierran "ahora mismo" si el periodo incluye hoy).
 *   4. Conteo crudo en Mongo por status, y cuántos identificados dependen
 *      únicamente de fichaAt (para dimensionar el impacto real del fix).
 *
 * Uso:
 *   node src/banks/scripts/diag-cortes-ledger-dia-mes.js
 */

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const BankMovement = require('../domains/banks/BankMovement.model');
const { getLedgerMensual, getLedgerDiario } = require('../domains/banks/bank-indicadores.service');

async function main() {
  await connectMongo();

  console.log('=== 1) getLedgerMensual (últimos 7 meses, incluye mes en curso) ===');
  const { meses } = await getLedgerMensual({});
  for (const m of meses) {
    console.log(
      `${m.mes}  iniciales=${m.iniciales.count}/$${m.iniciales.saldo}  ` +
      `nuevos=${m.nuevos.count}/$${m.nuevos.saldo}  ` +
      `identificados=${m.identificados.count}/$${m.identificados.saldo}  ` +
      `total=${m.total.count}/$${m.total.saldo}`
    );
  }

  console.log('\n=== 2) getLedgerDiario (últimos 14 días) ===');
  const { dias } = await getLedgerDiario({});
  for (const d of dias) {
    const fechaStr = d.fecha.toISOString().slice(0, 10);
    console.log(
      `${fechaStr}  iniciales=${d.iniciales.count}/$${d.iniciales.saldo}  ` +
      `nuevos=${d.nuevos.count}/$${d.nuevos.saldo}  ` +
      `identificados=${d.identificados.count}/$${d.identificados.saldo}  ` +
      `total=${d.total.count}/$${d.total.saldo}`
    );
  }

  console.log('\n=== 3) Consistencia día más reciente (Diario) vs mes en curso (Mensual) ===');
  const ultimoDia = dias[dias.length - 1];
  const mesEnCurso = meses[meses.length - 1];
  console.log(`Último día (${ultimoDia.fecha.toISOString().slice(0, 10)}): total=${ultimoDia.total.count}/$${ultimoDia.total.saldo}`);
  console.log(`Mes en curso (${mesEnCurso.mes}): total=${mesEnCurso.total.count}/$${mesEnCurso.total.saldo}`);
  console.log(
    ultimoDia.total.count === mesEnCurso.total.count
      ? '-> COINCIDEN en count.'
      : `-> NO COINCIDEN en count (diferencia: ${mesEnCurso.total.count - ultimoDia.total.count}).`
  );

  console.log('\n=== 4) Conteo crudo en Mongo (sanity check, sin pasar por el service) ===');
  const porStatus = await BankMovement.aggregate([
    { $group: { _id: '$status', count: { $sum: 1 } } },
  ]);
  console.log('Por status:', porStatus);

  const identificadosSoloFicha = await BankMovement.countDocuments({
    status: 'identificado',
    primeraIdentificacionAt: null,
    $or: [{ identificadoPor: { $size: 0 } }, { identificadoPor: { $exists: false } }],
    fichaAt: { $ne: null },
  });
  console.log(`Identificados que SOLO tienen fichaAt (sin primeraIdentificacionAt ni identificadoPor): ${identificadosSoloFicha}`);

  await disconnectMongo();
}

main().catch(async (err) => {
  console.error(err);
  await disconnectMongo().catch(() => {});
  process.exit(1);
});
