'use strict';

// index.test.js — nuevo, cubre únicamente las adiciones de Sistema de Lealtad
// (sistema-lealtad, PR1/8, design.md "index.js additions"): exports de los 4
// modelos nuevos y las asociaciones hasMany/belongsTo con onDelete RESTRICT.
// `syncModels()` (DDL: ALTER TABLE/CHECK/sync de las tablas nuevas) requiere
// una conexión real a Postgres y NO se ejecuta aquí — queda como verificación
// manual explícita en tasks.md (Fase 1, tarea 1.7), igual que ya documentaba
// el propio tasks.md para esta tarea ("DDL, not unit-testable"). Las
// asociaciones sí son metadata síncrona de Sequelize y se verifican aquí.
const {
  LoyaltyCard,
  LoyaltyPointsLedger,
  LoyaltyTierHistory,
  LoyaltyAdjustmentReason,
  CentroCosto,
  syncModels,
} = require('./index');

describe('postgres/index.js — adiciones de Sistema de Lealtad', () => {
  test('exporta los 4 modelos nuevos', () => {
    expect(LoyaltyCard).toBeDefined();
    expect(LoyaltyPointsLedger).toBeDefined();
    expect(LoyaltyTierHistory).toBeDefined();
    expect(LoyaltyAdjustmentReason).toBeDefined();
  });

  test('LoyaltyCard hasMany LoyaltyPointsLedger (as movimientos, RESTRICT) y LoyaltyTierHistory (as historialNiveles, RESTRICT)', () => {
    const movimientos = LoyaltyCard.associations.movimientos;
    expect(movimientos).toBeDefined();
    expect(movimientos.target).toBe(LoyaltyPointsLedger);
    expect(movimientos.options.onDelete).toBe('RESTRICT');

    const historialNiveles = LoyaltyCard.associations.historialNiveles;
    expect(historialNiveles).toBeDefined();
    expect(historialNiveles.target).toBe(LoyaltyTierHistory);
    expect(historialNiveles.options.onDelete).toBe('RESTRICT');
  });

  test('LoyaltyPointsLedger belongsTo LoyaltyCard (as card), CentroCosto (as centroCosto), LoyaltyAdjustmentReason (as reason)', () => {
    expect(LoyaltyPointsLedger.associations.card.target).toBe(LoyaltyCard);
    expect(LoyaltyPointsLedger.associations.centroCosto.target).toBe(CentroCosto);
    expect(LoyaltyPointsLedger.associations.reason.target).toBe(LoyaltyAdjustmentReason);
  });

  test('LoyaltyTierHistory belongsTo LoyaltyCard (as card)', () => {
    expect(LoyaltyTierHistory.associations.card.target).toBe(LoyaltyCard);
  });

  test('CentroCosto hasMany LoyaltyPointsLedger (as movimientosLealtad)', () => {
    const assoc = CentroCosto.associations.movimientosLealtad;
    expect(assoc).toBeDefined();
    expect(assoc.target).toBe(LoyaltyPointsLedger);
  });

  test('syncModels sigue siendo exportado como función (sin ejecutarla: requiere Postgres real)', () => {
    expect(typeof syncModels).toBe('function');
  });
});
