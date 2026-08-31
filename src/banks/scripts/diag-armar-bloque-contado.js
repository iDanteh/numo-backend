'use strict';

/**
 * diag-armar-bloque-contado.js
 * Reproduce el pipeline REAL completo que usa `exportContpaqXlsx` para el
 * bloque de Contado — `construirVerdadBancaria`, `construirBancoRealPorTicket`,
 * `_extraerCobrosSucursal`, `armarBloqueContado` — con los movimientos YA
 * PERSISTIDOS de una poliza, SIN llamar a `exportContpaqXlsx` (que persiste
 * cambios de cuenta banco-real). Solo lectura, no modifica nada.
 *
 * Uso:
 *   node src/banks/scripts/diag-armar-bloque-contado.js <polizaId> <cfdiUuid>
 */

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const { sequelize } = require('../../config/database.postgres');
const { Poliza, PolizaMovimiento, AccountPlan, CentroCosto, CfdiMappingRule } = require('../../shared/models/postgres');
const {
  _construirVerdadBancaria, _construirBancoRealPorTicket, _extraerCobrosSucursal, _armarBloqueContado,
} = require('../domains/polizas/poliza.service');

const [polizaIdArg, cfdiUuid] = process.argv.slice(2);
if (!polizaIdArg || !cfdiUuid) {
  console.error('Uso: node diag-armar-bloque-contado.js <polizaId> <cfdiUuid>');
  process.exit(1);
}
const polizaId = Number(polizaIdArg);

const MOVIMIENTOS_INCLUDE = {
  model: PolizaMovimiento, as: 'movimientos',
  include: [
    { model: AccountPlan, as: 'cuenta', attributes: ['id', 'codigo', 'nombre', 'tipo', 'naturaleza'] },
    { model: CentroCosto, as: 'centroCostoObj', attributes: ['id', 'clave', 'sucursal', 'serieFacturacion'], required: false },
    { model: CfdiMappingRule, as: 'regla', attributes: ['id', 'nombre'], required: false },
  ],
  order: [['orden', 'ASC']],
};

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  const poliza = await Poliza.findByPk(polizaId, { include: [MOVIMIENTOS_INCLUDE] });
  if (!poliza) { console.error('Poliza no encontrada:', polizaId); process.exit(1); }

  let movimientos = poliza.movimientos ?? [];
  console.log(`Poliza ${polizaId} (${poliza.estado}): ${movimientos.length} movimientos totales.`);

  const verdadBancaria = await _construirVerdadBancaria(movimientos.map(m => ({ cfdiUuid: m.cfdiUuid, serie: m.serie })));
  console.log('\nverdadBancaria para nuestro uuid:', JSON.stringify(verdadBancaria.get(cfdiUuid.toUpperCase()) ?? null, null, 2));

  const bancoRealPorTicket = await _construirBancoRealPorTicket(
    movimientos.map(m => ({ serieVentaTicket: m.serieVentaTicket, folioVentaTicket: m.folioVentaTicket })),
  );
  console.log('bancoRealPorTicket para M0|260802850:', JSON.stringify(bancoRealPorTicket.get('M0|260802850') ?? null, null, 2));

  const { resto: movimientosSinCobroSucursal } = _extraerCobrosSucursal(movimientos);
  movimientos = movimientosSinCobroSucursal;

  const contado = movimientos.filter(m => m.metodoPago !== 'PPD');
  console.log(`\nContado (post cobro-sucursal): ${contado.length} movimientos.`);

  const ventas = _armarBloqueContado(contado, verdadBancaria, new Map(), { bancoRealPorTicket });

  console.log(`\nTotal lineas en "ventas" (lo que se exporta): ${ventas.length}`);

  const match = ventas.filter(v => {
    if ((v.cfdiUuid || '').toUpperCase() === cfdiUuid.toUpperCase()) return true;
    return (v._detalle ?? []).some(x => (x.cfdiUuid || '').toUpperCase() === cfdiUuid.toUpperCase());
  });
  console.log(`\nLineas de "ventas" relacionadas con el CFDI ${cfdiUuid}: ${match.length}`);
  console.log(JSON.stringify(match, null, 2));

  process.exit(0);
}

main().catch(err => { console.error('ERROR:', err.stack || err.message); process.exit(1); });
