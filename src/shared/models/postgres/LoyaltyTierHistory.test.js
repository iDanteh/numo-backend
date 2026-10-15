'use strict';

// LoyaltyTierHistory.test.js — verifica el esquema Sequelize (sistema-lealtad,
// PR1/8, design.md "Data Model" → LoyaltyTierHistory). `timestamps:false` +
// `at` default NOW, mismo patrón que ConfigAuditLog.fecha.
const LoyaltyTierHistory = require('./LoyaltyTierHistory');

describe('LoyaltyTierHistory — modelo Sequelize (loyalty_tier_history)', () => {
  test('tableName/underscored/timestamps:false', () => {
    expect(LoyaltyTierHistory.getTableName()).toBe('loyalty_tier_history');
    expect(LoyaltyTierHistory.options.underscored).toBe(true);
    expect(LoyaltyTierHistory.options.timestamps).toBe(false);
  });

  test('cardId: FK a loyalty_cards NOT NULL, onDelete RESTRICT', () => {
    const attr = LoyaltyTierHistory.getAttributes().cardId;
    expect(attr.allowNull).toBe(false);
    expect(attr.references.model).toBe('loyalty_cards');
    expect(attr.onDelete).toBe('RESTRICT');
  });

  test('nivelAnterior/nivelNuevo: STRING(10) NOT NULL', () => {
    const anterior = LoyaltyTierHistory.getAttributes().nivelAnterior;
    const nuevo = LoyaltyTierHistory.getAttributes().nivelNuevo;
    expect(anterior.type.options.length).toBe(10);
    expect(anterior.allowNull).toBe(false);
    expect(nuevo.type.options.length).toBe(10);
    expect(nuevo.allowNull).toBe(false);
  });

  test('motivo: TEXT nullable; usuarioId/usuarioNombre: STRING(200) nullable', () => {
    expect(LoyaltyTierHistory.getAttributes().motivo.allowNull).toBe(true);
    expect(LoyaltyTierHistory.getAttributes().usuarioId.allowNull).toBe(true);
    expect(LoyaltyTierHistory.getAttributes().usuarioNombre.allowNull).toBe(true);
  });

  test('at: DATE default NOW', () => {
    const attr = LoyaltyTierHistory.getAttributes().at;
    expect(attr.defaultValue).toBeDefined();
  });
});
