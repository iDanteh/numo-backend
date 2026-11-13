'use strict';

/**
 * diag-cortes-drift-ids.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Investiga, a nivel de documento, por qué `total` de un mes/día no coincide
 * EXACTO con `iniciales` del periodo siguiente en el ledger de Cortes (debería,
 * según el propio JSDoc de _calcularLedgerBacklog). Script de SOLO LECTURA.
 *
 * Uso:
 *   node src/banks/scripts/diag-cortes-drift-ids.js
 */

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const BankMovement = require('../domains/banks/BankMovement.model');
const {
  _enumerarMesesParaLedger,
  _estadoAlCierre,
} = require('../domains/banks/bank-indicadores.service');

const baseMatch = { isActive: true, oculto: { $ne: true }, deposito: { $gt: 0 } };

async function backlogIdsAlCorte(corte) {
  const candidatos = await BankMovement.find({
    ...baseMatch,
    createdAt: { $lte: corte },
    status: { $in: ['no_identificado', 'identificado'] },
  }).select('_id createdAt status primeraIdentificacionAt historialVinculacion identificadoPor fichaAt').lean();

  return new Set(
    candidatos
      .filter((mov) => !_estadoAlCierre(mov, corte).identificado)
      .map((mov) => String(mov._id))
  );
}

async function main() {
  await connectMongo();

  const meses = await _enumerarMesesParaLedger({ cantidad: 7 });
  const oct = meses.find((m) => m.mes === '2026-10');
  const nov = meses.find((m) => m.mes === '2026-11');
  console.log(`Corte fin de Octubre: ${oct.fin.toISOString()}`);
  console.log(`Corte inicio de Noviembre: ${nov.inicio.toISOString()}`);
  console.log(`Diferencia (ms): ${nov.inicio.getTime() - oct.fin.getTime()}`);

  const backlogFinOct = await backlogIdsAlCorte(oct.fin);
  const backlogInicioNov = await backlogIdsAlCorte(nov.inicio);

  console.log(`\nBacklog a fin de Octubre: ${backlogFinOct.size}`);
  console.log(`Backlog a inicio de Noviembre: ${backlogInicioNov.size}`);

  const soloEnOct = [...backlogFinOct].filter((id) => !backlogInicioNov.has(id));
  const soloEnNov = [...backlogInicioNov].filter((id) => !backlogFinOct.has(id));
  console.log(`IDs solo en el backlog de fin-Oct (ya no en inicio-Nov): ${soloEnOct.length}`);
  console.log(`IDs solo en el backlog de inicio-Nov (no estaban en fin-Oct): ${soloEnNov.length}`);

  const idsRevisar = [...soloEnOct, ...soloEnNov].slice(0, 10);
  if (idsRevisar.length) {
    console.log('\n--- Detalle de los primeros movimientos que difieren ---');
    const docs = await BankMovement.find({ _id: { $in: idsRevisar } })
      .select('_id createdAt status primeraIdentificacionAt historialVinculacion identificadoPor fichaAt ultimoCambioStatusAt')
      .lean();
    for (const d of docs) {
      console.log(JSON.stringify({
        _id: String(d._id),
        createdAt: d.createdAt,
        status: d.status,
        primeraIdentificacionAt: d.primeraIdentificacionAt,
        identificadoPor: d.identificadoPor,
        fichaAt: d.fichaAt,
        historialVinculacion: d.historialVinculacion,
        ultimoCambioStatusAt: d.ultimoCambioStatusAt,
        enFinOct: backlogFinOct.has(String(d._id)),
        enInicioNov: backlogInicioNov.has(String(d._id)),
      }, null, 2));
    }
  }

  await disconnectMongo();
}

main().catch(async (err) => {
  console.error(err);
  await disconnectMongo().catch(() => {});
  process.exit(1);
});
