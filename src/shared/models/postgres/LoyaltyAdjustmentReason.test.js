'use strict';

// LoyaltyAdjustmentReason.test.js — verifica el esquema Sequelize
// (sistema-lealtad, PR1/8, design.md "Data Model" → LoyaltyAdjustmentReason).
// Rediseño 2026-10-23: signoPermitido/porcentaje se reemplazaron por puntosFijos
// (ver comentario del modelo).
const LoyaltyAdjustmentReason = require('./LoyaltyAdjustmentReason');

describe('LoyaltyAdjustmentReason — modelo Sequelize (loyalty_adjustment_reasons)', () => {
  test('tableName/underscored', () => {
    expect(LoyaltyAdjustmentReason.getTableName()).toBe('loyalty_adjustment_reasons');
    expect(LoyaltyAdjustmentReason.options.underscored).toBe(true);
  });

  test('clave: STRING(50) NOT NULL UNIQUE', () => {
    const attr = LoyaltyAdjustmentReason.getAttributes().clave;
    expect(attr.type.options.length).toBe(50);
    expect(attr.allowNull).toBe(false);
    expect(attr.unique).toBe(true);
  });

  test('nombre: STRING(150) NOT NULL', () => {
    const attr = LoyaltyAdjustmentReason.getAttributes().nombre;
    expect(attr.type.options.length).toBe(150);
    expect(attr.allowNull).toBe(false);
  });

  test('puntosFijos: INTEGER nullable (null = motivo manual)', () => {
    const attr = LoyaltyAdjustmentReason.getAttributes().puntosFijos;
    expect(attr.allowNull).toBe(true);
    expect(attr.type.constructor.name).toBe('INTEGER');
  });

  test('activo: BOOLEAN NOT NULL default true', () => {
    const attr = LoyaltyAdjustmentReason.getAttributes().activo;
    expect(attr.allowNull).toBe(false);
    expect(attr.defaultValue).toBe(true);
  });
});
