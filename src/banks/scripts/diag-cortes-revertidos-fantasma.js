'use strict';

/**
 * diag-cortes-revertidos-fantasma.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Hipótesis a confirmar: _calcularLedgerBacklog excluye (continue) un movimiento
 * de TODO un periodo si estaba identificado al `periodoInicio` — sin revisar si
 * se REVIRTIÓ (desvinculado) más tarde, DENTRO de ese mismo periodo. Si eso pasa,
 * el movimiento queda invisible para el `total` de ese periodo, pero reaparece
 * como `iniciales` "de la nada" en el periodo siguiente (o en un corte diario
 * más fino que cae después de la reversión) — rompiendo la garantía documentada
 * de "total(A) == iniciales(B)". Script de SOLO LECTURA.
 *
 * Uso:
 *   node src/banks/scripts/diag-cortes-revertidos-fantasma.js
 */

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const BankMovement = require('../domains/banks/BankMovement.model');
const {
  _enumerarMesesParaLedger,
  _estadoAlCierre,
} = require('../domains/banks/bank-indicadores.service');

const baseMatch = { isActive: true, oculto: { $ne: true }, deposito: { $gt: 0 } };

async function main() {
  await connectMongo();

  const meses = await _enumerarMesesParaLedger({ cantidad: 7 });
  const oct = meses.find((m) => m.mes === '2026-10');
  const nov = meses.find((m) => m.mes === '2026-11');

  // Candidatos EXACTOS que usaría _calcularLedgerBacklog para Octubre (createdAt <= finOct).
  const candidatos = await BankMovement.find({
    ...baseMatch,
    createdAt: { $lte: oct.fin },
    status: { $in: ['no_identificado', 'identificado'] },
  }).select('_id createdAt status primeraIdentificacionAt historialVinculacion identificadoPor fichaAt ultimoCambioStatusAt').lean();

  const fantasmas = [];
  for (const mov of candidatos) {
    const esNuevoOct = mov.createdAt >= oct.inicio;
    if (esNuevoOct) continue; // solo nos interesan los "iniciales" de Octubre (createdAt previo)

    const identificadoAlInicioOct = _estadoAlCierre(mov, oct.inicio).identificado;
    if (!identificadoAlInicioOct) continue; // esos SÍ entran a Octubre normalmente, no son el caso

    // Estaba resuelto al empezar Octubre -> _calcularLedgerBacklog lo salta (continue) para
    // TODO Octubre. ¿Pero sigue resuelto al EMPEZAR Noviembre (periodoInicio de Nov == periodoFin de Oct)?
    const identificadoAlInicioNov = _estadoAlCierre(mov, nov.inicio).identificado;
    if (!identificadoAlInicioNov) {
      fantasmas.push(mov); // resuelto al inicio de Oct, NO resuelto al inicio de Nov -> reapareció
    }
  }

  console.log(`Candidatos "iniciales" de Octubre evaluados: ${candidatos.filter(m => m.createdAt < oct.inicio).length}`);
  console.log(`Movimientos FANTASMA (resueltos al inicio-Oct, otra vez pendientes al inicio-Nov): ${fantasmas.length}`);

  for (const mov of fantasmas.slice(0, 10)) {
    console.log(JSON.stringify({
      _id: String(mov._id),
      createdAt: mov.createdAt,
      status: mov.status,
      primeraIdentificacionAt: mov.primeraIdentificacionAt,
      identificadoPor: mov.identificadoPor,
      fichaAt: mov.fichaAt,
      historialVinculacion: mov.historialVinculacion,
      ultimoCambioStatusAt: mov.ultimoCambioStatusAt,
    }, null, 2));
  }

  await disconnectMongo();
}

main().catch(async (err) => {
  console.error(err);
  await disconnectMongo().catch(() => {});
  process.exit(1);
});
