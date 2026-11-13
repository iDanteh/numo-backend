'use strict';

// LoyaltyCard.test.js — verifica el esquema Sequelize (sistema-lealtad, PR1/8,
// design.md "Data Model" → LoyaltyCard). No requiere conexión real a Postgres:
// `sequelize.define` solo registra metadata (rawAttributes/options), nunca
// abre una conexión. Mismo criterio que NetpayFolioRegistro.model.test.js
// (ahí con Mongoose `schema.path`, aquí con `Model.getAttributes()`/`options`).
const LoyaltyCard = require('./LoyaltyCard');

describe('LoyaltyCard — modelo Sequelize (loyalty_cards)', () => {
  test('tableName/underscored', () => {
    expect(LoyaltyCard.getTableName()).toBe('loyalty_cards');
    expect(LoyaltyCard.options.underscored).toBe(true);
  });

  test('telefono: STRING(10), NOT NULL, UNIQUE, valida 10 dígitos', () => {
    const attr = LoyaltyCard.getAttributes().telefono;
    expect(attr.type.constructor.name).toBe('STRING');
    expect(attr.type.options.length).toBe(10);
    expect(attr.allowNull).toBe(false);
    expect(attr.unique).toBe(true);
    expect(attr.validate.is.toString()).toContain('\\d{10}');
  });

  test('nipHash: STRING(100) NOT NULL, excluido del defaultScope', () => {
    const attr = LoyaltyCard.getAttributes().nipHash;
    expect(attr.type.constructor.name).toBe('STRING');
    expect(attr.type.options.length).toBe(100);
    expect(attr.allowNull).toBe(false);

    expect(LoyaltyCard.options.defaultScope.attributes.exclude).toContain('nipHash');
  });

  test('nombre: STRING(200) NOT NULL', () => {
    const attr = LoyaltyCard.getAttributes().nombre;
    expect(attr.type.constructor.name).toBe('STRING');
    expect(attr.type.options.length).toBe(200);
    expect(attr.allowNull).toBe(false);
  });

  test("nivel: STRING(10) default 'Bronce', isIn [Bronce,Plata,Oro,Azul]", () => {
    const attr = LoyaltyCard.getAttributes().nivel;
    expect(attr.allowNull).toBe(false);
    expect(attr.defaultValue).toBe('Bronce');
    expect(attr.validate.isIn).toEqual([['Bronce', 'Plata', 'Oro', 'Azul']]);
  });

  test('saldoPuntos: DECIMAL(12,2) NOT NULL default 0', () => {
    const attr = LoyaltyCard.getAttributes().saldoPuntos;
    expect(attr.type.constructor.name).toBe('DECIMAL');
    expect(attr.type.options.precision).toBe(12);
    expect(attr.type.options.scale).toBe(2);
    expect(attr.allowNull).toBe(false);
    expect(attr.defaultValue).toBe(0);
  });

  test('activa: BOOLEAN default true', () => {
    const attr = LoyaltyCard.getAttributes().activa;
    expect(attr.allowNull).toBe(false);
    expect(attr.defaultValue).toBe(true);
  });

  test('nipIntentosFallidos: INTEGER NOT NULL default 0; nipBloqueadoHasta: DATE NULL', () => {
    const intentos = LoyaltyCard.getAttributes().nipIntentosFallidos;
    expect(intentos.allowNull).toBe(false);
    expect(intentos.defaultValue).toBe(0);

    const bloqueado = LoyaltyCard.getAttributes().nipBloqueadoHasta;
    expect(bloqueado.allowNull).toBe(true);
  });

  test('createdById/createdByNombre STRING(200); updatedById/updatedByNombre STRING(200) NULL', () => {
    const createdById = LoyaltyCard.getAttributes().createdById;
    const createdByNombre = LoyaltyCard.getAttributes().createdByNombre;
    const updatedById = LoyaltyCard.getAttributes().updatedById;
    const updatedByNombre = LoyaltyCard.getAttributes().updatedByNombre;

    expect(createdById.type.options.length).toBe(200);
    expect(createdByNombre.type.options.length).toBe(200);
    expect(updatedById.allowNull).toBe(true);
    expect(updatedByNombre.allowNull).toBe(true);
    expect(updatedById.type.options.length).toBe(200);
  });

  test('timestamps de Sequelize (created_at/updated_at) activos — no declara timestamps:false', () => {
    expect(LoyaltyCard.options.timestamps).not.toBe(false);
  });

  describe('sesión de consulta + email de contacto + contadores (2026-11-09)', () => {
    test('consultaSesionTokenHash: STRING(100) NULL, excluido del defaultScope, con scope propio', () => {
      const attr = LoyaltyCard.getAttributes().consultaSesionTokenHash;
      expect(attr.type.constructor.name).toBe('STRING');
      expect(attr.type.options.length).toBe(100);
      expect(attr.allowNull).toBe(true);

      expect(LoyaltyCard.options.defaultScope.attributes.exclude).toContain('consultaSesionTokenHash');
      expect(LoyaltyCard.options.scopes.withConsultaSesionTokenHash.attributes.include).toContain('consultaSesionTokenHash');
    });

    test('consultaSesionUsosRestantes: INTEGER NULL; consultaSesionExpiraEn: DATE NULL', () => {
      const usos = LoyaltyCard.getAttributes().consultaSesionUsosRestantes;
      expect(usos.allowNull).toBe(true);

      const expira = LoyaltyCard.getAttributes().consultaSesionExpiraEn;
      expect(expira.allowNull).toBe(true);
    });

    test('emailContacto: STRING(200) NULL', () => {
      const attr = LoyaltyCard.getAttributes().emailContacto;
      expect(attr.type.constructor.name).toBe('STRING');
      expect(attr.type.options.length).toBe(200);
      expect(attr.allowNull).toBe(true);
    });

    test('ultimaConsultaAt: DATE NULL; totalConsultas: INTEGER NOT NULL default 0', () => {
      const ultima = LoyaltyCard.getAttributes().ultimaConsultaAt;
      expect(ultima.allowNull).toBe(true);

      const total = LoyaltyCard.getAttributes().totalConsultas;
      expect(total.allowNull).toBe(false);
      expect(total.defaultValue).toBe(0);
    });
  });

  describe('límite mensual de consultas con código (2026-11-09)', () => {
    test('consultaCodigoMesPeriodo: STRING(7) NULL, NO excluido del defaultScope (no es secreto)', () => {
      const attr = LoyaltyCard.getAttributes().consultaCodigoMesPeriodo;
      expect(attr.type.constructor.name).toBe('STRING');
      expect(attr.type.options.length).toBe(7);
      expect(attr.allowNull).toBe(true);

      expect(LoyaltyCard.options.defaultScope.attributes.exclude).not.toContain('consultaCodigoMesPeriodo');
    });

    test('consultaCodigoMesContador: INTEGER NOT NULL default 0, NO excluido del defaultScope', () => {
      const attr = LoyaltyCard.getAttributes().consultaCodigoMesContador;
      expect(attr.allowNull).toBe(false);
      expect(attr.defaultValue).toBe(0);

      expect(LoyaltyCard.options.defaultScope.attributes.exclude).not.toContain('consultaCodigoMesContador');
    });
  });
});
