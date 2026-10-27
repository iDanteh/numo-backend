'use strict';

// LoyaltyKoreVentaFallida.test.js — verifica el esquema Sequelize (registro de procesos
// fallidos del webhook Kore→Numo de venta, 2026-10-26).
const LoyaltyKoreVentaFallida = require('./LoyaltyKoreVentaFallida');

describe('LoyaltyKoreVentaFallida — modelo Sequelize (loyalty_kore_ventas_fallidas)', () => {
  test('tableName/underscored', () => {
    expect(LoyaltyKoreVentaFallida.getTableName()).toBe('loyalty_kore_ventas_fallidas');
    expect(LoyaltyKoreVentaFallida.options.underscored).toBe(true);
  });

  test('serieFolio: STRING(100) NOT NULL, no unique (puede haber un historial resuelto + uno nuevo)', () => {
    const attr = LoyaltyKoreVentaFallida.getAttributes().serieFolio;
    expect(attr.type.options.length).toBe(100);
    expect(attr.allowNull).toBe(false);
    expect(attr.unique).toBeFalsy();
  });

  test('motivo: STRING(300) NOT NULL', () => {
    const attr = LoyaltyKoreVentaFallida.getAttributes().motivo;
    expect(attr.type.options.length).toBe(300);
    expect(attr.allowNull).toBe(false);
  });

  test('intentos: INTEGER NOT NULL default 1', () => {
    const attr = LoyaltyKoreVentaFallida.getAttributes().intentos;
    expect(attr.allowNull).toBe(false);
    expect(attr.defaultValue).toBe(1);
  });

  test('resuelto: BOOLEAN NOT NULL default false', () => {
    const attr = LoyaltyKoreVentaFallida.getAttributes().resuelto;
    expect(attr.allowNull).toBe(false);
    expect(attr.defaultValue).toBe(false);
  });

  test('resueltoPorUserId/resueltoPorNombre/resueltoEn nullable', () => {
    const attrs = LoyaltyKoreVentaFallida.getAttributes();
    expect(attrs.resueltoPorUserId.allowNull).toBe(true);
    expect(attrs.resueltoPorNombre.allowNull).toBe(true);
    expect(attrs.resueltoEn.allowNull).toBe(true);
  });
});
