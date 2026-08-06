'use strict';

// collection-request-asignaciones.test.js — resolverAsignaciones/
// calcularReconciliacion/movimientosDe: funciones puras, sin Kore ni Mongo
// real (mismo patrón que collection-request-erp-links.js). Los "formasPago"
// de prueba solo necesitan _id + formaPagoDescripcion + bankMovementId,
// como llegarían tras un populate real de Mongoose.
const {
  resolverAsignaciones,
  calcularReconciliacion,
  movimientosDe,
} = require('./collection-request-asignaciones');
const { BadRequestError } = require('../../shared/errors/AppError');

function forma(id, descripcion, extra = {}) {
  return { _id: id, formaPagoDescripcion: descripcion, formaPagoId: 'fp-generico', importe: 0, ...extra };
}

describe('resolverAsignaciones', () => {
  test('atajo escalar {bankMovementId}: expande el mismo movimiento a TODAS las formasPago', () => {
    const cr = { formasPago: [forma('f1', 'Transferencia'), forma('f2', 'Efectivo')] };

    const mapa = resolverAsignaciones(cr, { bankMovementId: 'mov-1' });

    expect(mapa.size).toBe(1);
    expect(mapa.get('mov-1')).toHaveLength(2);
    expect(mapa.get('mov-1').map(f => f._id)).toEqual(['f1', 'f2']);
  });

  test('arreglo explícito de asignaciones: agrupa por movimiento distinto, orden de aparición', () => {
    const cr = {
      formasPago: [forma('f1', 'Transferencia'), forma('f2', 'Cheque'), forma('f3', 'Efectivo')],
    };
    const body = {
      asignaciones: [
        { formaPagoDocId: 'f1', bankMovementId: 'mov-A' },
        { formaPagoDocId: 'f2', bankMovementId: 'mov-B' },
        { formaPagoDocId: 'f3', bankMovementId: 'mov-A' },
      ],
    };

    const mapa = resolverAsignaciones(cr, body);

    expect([...mapa.keys()]).toEqual(['mov-A', 'mov-B']);
    expect(mapa.get('mov-A').map(f => f._id)).toEqual(['f1', 'f3']);
    expect(mapa.get('mov-B').map(f => f._id)).toEqual(['f2']);
  });

  test('formaPagoDocId desconocido: BadRequestError, ningún cálculo se realiza', () => {
    const cr = { formasPago: [forma('f1', 'Transferencia')] };
    const body = { asignaciones: [{ formaPagoDocId: 'no-existe', bankMovementId: 'mov-1' }] };

    expect(() => resolverAsignaciones(cr, body)).toThrow(BadRequestError);
    expect(() => resolverAsignaciones(cr, body)).toThrow(/formaPagoDocId desconocido/);
  });

  test('todo-o-nada: falta 1 de 2 formasPago sin asignar -> BadRequestError con detalle', () => {
    const cr = {
      formasPago: [forma('f1', 'Transferencia'), forma('f2', 'Cheque')],
    };
    const body = { asignaciones: [{ formaPagoDocId: 'f1', bankMovementId: 'mov-1' }] };

    expect(() => resolverAsignaciones(cr, body)).toThrow(BadRequestError);
    expect(() => resolverAsignaciones(cr, body)).toThrow(
      /Faltan 1 de 2 formas de pago sin movimiento bancario asignado \(Cheque\)/,
    );
  });

  test('todo-o-nada: 0 de 3 asignadas -> BadRequestError lista las 3 descripciones', () => {
    const cr = {
      formasPago: [forma('f1', 'Efectivo'), forma('f2', 'Cheque'), forma('f3', 'Transferencia')],
    };

    expect(() => resolverAsignaciones(cr, {})).toThrow(
      /Faltan 3 de 3 formas de pago sin movimiento bancario asignado \(Efectivo, Cheque, Transferencia\)/,
    );
  });
});

describe('calcularReconciliacion', () => {
  test('faltante (abono parcial): mensaje informativo, cubreParcial=true, NUNCA lanza', () => {
    const cr = { monto: 100000 };
    const movs = [{ _id: 'm1', deposito: 40000 }, { _id: 'm2', deposito: 30000 }];

    const r = calcularReconciliacion(cr, movs);

    expect(r.cubreParcial).toBe(true);
    expect(r.montoDepositado).toBe(70000);
    expect(r.diferencia).toBe(30000);
    expect(r.mensaje).toBe('cubre $70,000 de $100,000 — quedan $30,000 pendientes');
  });

  test('monto exacto: sin mensaje (null), cubreParcial=false', () => {
    const cr = { monto: 50000 };
    const movs = [{ _id: 'm1', deposito: 50000 }];

    const r = calcularReconciliacion(cr, movs);

    expect(r.cubreParcial).toBe(false);
    expect(r.mensaje).toBeNull();
  });

  test('exceso (depósito cubre más de lo solicitado): silencioso, sin mensaje', () => {
    const cr = { monto: 50000 };
    const movs = [{ _id: 'm1', deposito: 80000 }];

    const r = calcularReconciliacion(cr, movs);

    expect(r.cubreParcial).toBe(false);
    expect(r.mensaje).toBeNull();
    expect(r.diferencia).toBe(-30000);
  });

  test('movimiento compartido por 2 formasPago cuenta UNA sola vez', () => {
    const cr = { monto: 100000 };
    const movCompartido = { _id: 'm1', deposito: 60000 };
    // el mismo movimiento aparece 2 veces en la lista (una por cada formaPago
    // que lo referencia) — no debe duplicarse en montoDepositado.
    const movs = [movCompartido, movCompartido, { _id: 'm2', deposito: 10000 }];

    const r = calcularReconciliacion(cr, movs);

    expect(r.montoDepositado).toBe(70000);
    expect(r.diferencia).toBe(30000);
  });
});

describe('movimientosDe', () => {
  test('post-backfill: lee formasPago[].bankMovementId, deduplicado, orden de aparición', () => {
    const movA = { _id: 'mov-A' };
    const movB = { _id: 'mov-B' };
    const cr = {
      bankMovementId: 'legacy-no-debe-usarse',
      formasPago: [
        { bankMovementId: movA },
        { bankMovementId: movB },
        { bankMovementId: movA }, // repetido -> no debe duplicarse
      ],
    };

    const movs = movimientosDe(cr);

    expect(movs).toEqual([movA, movB]);
  });

  test('pre-backfill: formasPago[].bankMovementId todos null -> fallback al campo raíz (D1, ÚNICO punto)', () => {
    const cr = {
      bankMovementId: 'legacy-mov-1',
      formasPago: [{ bankMovementId: null }, { bankMovementId: null }],
    };

    const movs = movimientosDe(cr);

    expect(movs).toEqual(['legacy-mov-1']);
  });
});
