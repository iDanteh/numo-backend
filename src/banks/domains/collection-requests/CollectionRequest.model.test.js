'use strict';

// CollectionRequest.model.test.js — verifica los cambios de esquema del PR1
// (multi-bank-movement): formasPago[].bankMovementId nuevo, campo raíz
// bankMovementId sin romper (back-compat, D1), e inconsistenciaPostKore
// nuevo (D4). No requiere conexión real a Mongo: mongoose.model() solo
// compila el esquema, no abre sesión.
const CollectionRequest = require('./CollectionRequest.model');

describe('CollectionRequest schema — multi-bank-movement (PR1)', () => {
  test('formasPago[] gana bankMovementId (ObjectId ref BankMovement, default null, indexado)', () => {
    const formasPagoSchema = CollectionRequest.schema.path('formasPago').schema;
    const path = formasPagoSchema.path('bankMovementId');

    expect(path).toBeDefined();
    expect(path.instance).toBe('ObjectId');
    expect(path.options.ref).toBe('BankMovement');
    expect(path.options.default ?? null).toBeNull();
  });

  test('el campo raíz bankMovementId se conserva sin cambios (back-compat, D1)', () => {
    const path = CollectionRequest.schema.path('bankMovementId');

    expect(path).toBeDefined();
    expect(path.instance).toBe('ObjectId');
    expect(path.options.ref).toBe('BankMovement');
  });

  test('inconsistenciaPostKore existe con at/mensaje/movimientosPendientes, default null', () => {
    const doc = new CollectionRequest({
      cxcs: [{ erpId: '1' }],
      formasPago: [{ formaPagoId: 'f1', formaPagoDescripcion: 'Transferencia', importe: 100 }],
      monto: 100,
      solicitanteUserId: 'user-1',
    });

    // Sin asignar nada, debe quedar null por default — nunca un objeto vacío.
    expect(doc.inconsistenciaPostKore).toBeNull();

    doc.inconsistenciaPostKore = {
      at: new Date('2026-08-06T00:00:00Z'),
      mensaje: 'Kore aceptó pero el commit de Mongo falló',
      movimientosPendientes: [],
    };

    expect(doc.inconsistenciaPostKore.mensaje).toBe('Kore aceptó pero el commit de Mongo falló');
    expect(Array.isArray(doc.inconsistenciaPostKore.movimientosPendientes)).toBe(true);
  });
});
