'use strict';

// LoyaltyPointsLedger.test.js — verifica el esquema Sequelize (sistema-lealtad,
// PR1/8, design.md "Data Model" → LoyaltyPointsLedger). Append-only: updatedAt
// false, índice compuesto (card_id,created_at) e índice único PARCIAL
// (origen,referencia_externa) donde referencia_externa no sea null (D-less,
// ver design.md snippet literal).
const LoyaltyPointsLedger = require('./LoyaltyPointsLedger');

describe('LoyaltyPointsLedger — modelo Sequelize (loyalty_points_ledger)', () => {
  test('tableName/underscored/updatedAt:false', () => {
    expect(LoyaltyPointsLedger.getTableName()).toBe('loyalty_points_ledger');
    expect(LoyaltyPointsLedger.options.underscored).toBe(true);
    expect(LoyaltyPointsLedger.options.timestamps).not.toBe(false);
    expect(LoyaltyPointsLedger.options.updatedAt).toBe(false);
  });

  test('cardId: FK a loyalty_cards, NOT NULL, onDelete RESTRICT', () => {
    const attr = LoyaltyPointsLedger.getAttributes().cardId;
    expect(attr.allowNull).toBe(false);
    expect(attr.references.model).toBe('loyalty_cards');
    expect(attr.references.key).toBe('id');
    expect(attr.onDelete).toBe('RESTRICT');
  });

  test("tipo: STRING(20) NOT NULL, isIn [ACUMULACION,CANJE,AJUSTE,MIGRACION]", () => {
    const attr = LoyaltyPointsLedger.getAttributes().tipo;
    expect(attr.allowNull).toBe(false);
    expect(attr.validate.isIn).toEqual([['ACUMULACION', 'CANJE', 'AJUSTE', 'MIGRACION']]);
  });

  test('puntos: DECIMAL(10,2) NOT NULL (con signo); saldoResultante: DECIMAL(12,2) NOT NULL', () => {
    const puntos = LoyaltyPointsLedger.getAttributes().puntos;
    expect(puntos.type.options.precision).toBe(10);
    expect(puntos.type.options.scale).toBe(2);
    expect(puntos.allowNull).toBe(false);

    const saldoResultante = LoyaltyPointsLedger.getAttributes().saldoResultante;
    expect(saldoResultante.type.options.precision).toBe(12);
    expect(saldoResultante.type.options.scale).toBe(2);
    expect(saldoResultante.allowNull).toBe(false);
  });

  test('montoVenta/porcentajeAplicado/nivelAplicado: snapshots nullable (solo ACUMULACION)', () => {
    expect(LoyaltyPointsLedger.getAttributes().montoVenta.allowNull).toBe(true);
    expect(LoyaltyPointsLedger.getAttributes().porcentajeAplicado.allowNull).toBe(true);
    expect(LoyaltyPointsLedger.getAttributes().nivelAplicado.allowNull).toBe(true);
  });

  test('montoVentaTotal: nullable, solo auditoría (nunca se muestra en la UI)', () => {
    expect(LoyaltyPointsLedger.getAttributes().montoVentaTotal.allowNull).toBe(true);
  });

  test('centroCostoId: FK a centros_costo nullable; reasonId: FK a loyalty_adjustment_reasons nullable', () => {
    const cc = LoyaltyPointsLedger.getAttributes().centroCostoId;
    expect(cc.allowNull).toBe(true);
    expect(cc.references.model).toBe('centros_costo');

    const reason = LoyaltyPointsLedger.getAttributes().reasonId;
    expect(reason.allowNull).toBe(true);
    expect(reason.references.model).toBe('loyalty_adjustment_reasons');
  });

  test("origen: STRING(20) NOT NULL default 'manual'; referenciaExterna: STRING(100) nullable", () => {
    const origen = LoyaltyPointsLedger.getAttributes().origen;
    expect(origen.allowNull).toBe(false);
    expect(origen.defaultValue).toBe('manual');

    const ref = LoyaltyPointsLedger.getAttributes().referenciaExterna;
    expect(ref.allowNull).toBe(true);
  });

  test('índice compuesto (card_id,created_at) e índice único parcial (origen,referencia_externa)', () => {
    const indexes = LoyaltyPointsLedger.options.indexes;
    const compound = indexes.find((i) => i.name === 'loyalty_ledger_card_created');
    expect(compound.fields).toEqual(['card_id', 'created_at']);

    const partialUnique = indexes.find((i) => i.name === 'loyalty_ledger_origen_ref_uq');
    expect(partialUnique.unique).toBe(true);
    expect(partialUnique.fields).toEqual(['origen', 'referencia_externa']);
    expect(partialUnique.where).toBeDefined();
  });

  test('justificacion: TEXT nullable; usuarioId/usuarioNombre: STRING(200) nullable', () => {
    expect(LoyaltyPointsLedger.getAttributes().justificacion.allowNull).toBe(true);
    expect(LoyaltyPointsLedger.getAttributes().usuarioId.allowNull).toBe(true);
    expect(LoyaltyPointsLedger.getAttributes().usuarioNombre.allowNull).toBe(true);
  });
});
