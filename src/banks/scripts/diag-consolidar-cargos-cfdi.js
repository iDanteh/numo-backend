'use strict';

/**
 * diag-consolidar-cargos-cfdi.js
 * Llama DIRECTO a `_consolidarCargos` (poliza.service.js, exportado para
 * diagnostico) con los movimientos REALES de una poliza (cargados tal cual
 * los carga `exportContpaqXlsx`, mismo include), para ver en que bucket cae
 * cada linea de un CFDI puntual — sin llamar a `exportContpaqXlsx` (que
 * persiste cambios de cuenta banco-real) y sin tocar nada. Solo lectura.
 *
 * verdadBancaria/bancoRealPorTicket se pasan VACIOS a proposito: si el CFDI
 * en cuestion no tiene match real en BankMovement con `categoria` puesta
 * (confirmado por separado contra Mongo), el resultado es identico a pasar
 * los mapas reales — el gate cae al mismo fallback (`m.formaPago`). Si aqui
 * el resultado no coincide con lo que muestra el export real, el problema
 * esta en `construirVerdadBancaria`/`construirBancoRealPorTicket`, no en el
 * gate de `consolidarCargos` en si.
 *
 * Uso:
 *   node src/banks/scripts/diag-consolidar-cargos-cfdi.js <polizaId> <cfdiUuid>
 */

require('dotenv').config();

const { sequelize } = require('../../config/database.postgres');
const { Poliza, PolizaMovimiento, AccountPlan, CentroCosto, CfdiMappingRule } = require('../../shared/models/postgres');
const { _consolidarCargos } = require('../domains/polizas/poliza.service');

const [polizaIdArg, cfdiUuid] = process.argv.slice(2);
if (!polizaIdArg || !cfdiUuid) {
  console.error('Uso: node diag-consolidar-cargos-cfdi.js <polizaId> <cfdiUuid>');
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
  await sequelize.authenticate();

  const poliza = await Poliza.findByPk(polizaId, { include: [MOVIMIENTOS_INCLUDE] });
  if (!poliza) { console.error('Poliza no encontrada:', polizaId); process.exit(1); }

  const movimientos = poliza.movimientos ?? [];
  const contadoNormal = movimientos.filter(m => m.metodoPago !== 'PPD');

  console.log(`Poliza ${polizaId} (${poliza.estado}): ${movimientos.length} movimientos totales, ${contadoNormal.length} de Contado.`);

  const soloEsteCfdi = contadoNormal.filter(m => (m.cfdiUuid || '').toUpperCase() === cfdiUuid.toUpperCase());
  console.log('\n--- Corrida AISLADA: solo las lineas de este CFDI ---');
  const resultadoAislado = _consolidarCargos(soloEsteCfdi, 21, false, new Map(), null, new Map());
  console.log('consolidados:', JSON.stringify(resultadoAislado.consolidados, null, 2));
  console.log('depositosIdentificados:', JSON.stringify(resultadoAislado.depositosIdentificados, null, 2));
  console.log('porCategoria:', JSON.stringify(resultadoAislado.porCategoria, null, 2));
  console.log('anticipos:', JSON.stringify(resultadoAislado.anticipos, null, 2));

  const resultado = _consolidarCargos(contadoNormal, 21, false, new Map(), null, new Map());

  console.log('\n--- Buscando lineas del CFDI', cfdiUuid, '---');

  const enConsolidados = [];
  for (const c of resultado.consolidados) {
    for (const d of (c._detalle ?? [])) {
      if ((d.cfdiUuid || '').toUpperCase() === cfdiUuid.toUpperCase()) {
        enConsolidados.push({ grupoLabel: c.serie, grupoConcepto: c.concepto, detalle: d });
      }
    }
  }
  console.log(`\nEn CONSOLIDADOS (bucket generico): ${enConsolidados.length} lineas`);
  console.log(JSON.stringify(enConsolidados, null, 2));

  const enIdentificados = resultado.depositosIdentificados.filter(d => {
    if ((d.cfdiUuid || '').toUpperCase() === cfdiUuid.toUpperCase()) return true;
    return (d._detalle ?? []).some(x => (x.cfdiUuid || '').toUpperCase() === cfdiUuid.toUpperCase());
  });
  console.log(`\nEn DEPOSITOS IDENTIFICADOS (individual o agrupado, con referencia real): ${enIdentificados.length} lineas`);
  console.log(JSON.stringify(enIdentificados, null, 2));

  for (const cat of Object.keys(resultado.porCategoria)) {
    const match = resultado.porCategoria[cat].filter(d => (d.cfdiUuid || '').toUpperCase() === cfdiUuid.toUpperCase());
    if (match.length) console.log(`\nEn porCategoria.${cat}: ${match.length} lineas`, JSON.stringify(match, null, 2));
  }
  const enAnticipos = resultado.anticipos.filter(d => (d.cfdiUuid || '').toUpperCase() === cfdiUuid.toUpperCase());
  if (enAnticipos.length) console.log(`\nEn ANTICIPOS: ${enAnticipos.length} lineas`, JSON.stringify(enAnticipos, null, 2));

  console.log('\n--- Totales generales del resultado (todas las facturas) ---');
  console.log('consolidados (grupos):', resultado.consolidados.length, resultado.consolidados.map(c => ({ serie: c.serie, concepto: c.concepto, debe: c.debe, nDetalle: c._detalle?.length })));
  console.log('depositosIdentificados:', resultado.depositosIdentificados.length);
  console.log('porCategoria counts:', Object.fromEntries(Object.entries(resultado.porCategoria).map(([k, v]) => [k, v.length])));
  console.log('anticipos:', resultado.anticipos.length);

  // Tambien las lineas crudas de entrada para ese CFDI, para comparar formaPago tal cual llego.
  const crudas = contadoNormal.filter(m => (m.cfdiUuid || '').toUpperCase() === cfdiUuid.toUpperCase());
  console.log(`\nLineas CRUDAS de entrada para este CFDI: ${crudas.length}`);
  console.log(crudas.map(m => ({ id: m.id, cuenta: m.cuenta?.codigo, debe: m.debe, haber: m.haber, formaPago: m.formaPago, metodoPago: m.metodoPago, tipoOrigen: m.tipoOrigen, reglaNombre: m.reglaNombre })));

  process.exit(0);
}

main().catch(err => { console.error('ERROR:', err.stack || err.message); process.exit(1); });
