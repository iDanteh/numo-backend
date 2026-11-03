'use strict';

// LoyaltySecurityEvent.test.js — verifica el esquema Sequelize
// (loyalty_security_events, auditoría append-only del flujo OTP de la ruta
// pública de consulta, ver loyalty-recovery.service.js).
const LoyaltySecurityEvent = require('./LoyaltySecurityEvent');

describe('LoyaltySecurityEvent — modelo Sequelize (loyalty_security_events)', () => {
  test('tableName/underscored, sin timestamps gestionados por Sequelize', () => {
    expect(LoyaltySecurityEvent.getTableName()).toBe('loyalty_security_events');
    expect(LoyaltySecurityEvent.options.underscored).toBe(true);
    expect(LoyaltySecurityEvent.options.timestamps).toBe(false);
  });

  test('telefono: STRING(10) nullable', () => {
    const attr = LoyaltySecurityEvent.getAttributes().telefono;
    expect(attr.type.options.length).toBe(10);
    expect(attr.allowNull).toBe(true);
  });

  test('tipo: STRING(30) NOT NULL', () => {
    const attr = LoyaltySecurityEvent.getAttributes().tipo;
    expect(attr.type.options.length).toBe(30);
    expect(attr.allowNull).toBe(false);
  });

  test('resultado: STRING(10) NOT NULL', () => {
    const attr = LoyaltySecurityEvent.getAttributes().resultado;
    expect(attr.type.options.length).toBe(10);
    expect(attr.allowNull).toBe(false);
  });

  test('motivo: STRING(50) nullable (null cuando resultado=exito)', () => {
    const attr = LoyaltySecurityEvent.getAttributes().motivo;
    expect(attr.type.options.length).toBe(50);
    expect(attr.allowNull).toBe(true);
  });

  test('ip: STRING(45) nullable (cabe IPv6)', () => {
    const attr = LoyaltySecurityEvent.getAttributes().ip;
    expect(attr.type.options.length).toBe(45);
    expect(attr.allowNull).toBe(true);
  });

  test('createdAt: DATE NOT NULL con default NOW', () => {
    const attr = LoyaltySecurityEvent.getAttributes().createdAt;
    expect(attr.allowNull).toBe(false);
    expect(attr.defaultValue).toBeDefined();
  });

  test('índices por telefono e ip', () => {
    const indexNames = LoyaltySecurityEvent.options.indexes.map((i) => i.name);
    expect(indexNames).toContain('loyalty_security_events_telefono');
    expect(indexNames).toContain('loyalty_security_events_ip');
  });
});
