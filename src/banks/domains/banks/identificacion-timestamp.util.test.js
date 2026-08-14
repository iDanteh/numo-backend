'use strict';

const { resolvePrimeraIdentificacion } = require('./identificacion-timestamp.util');

describe('resolvePrimeraIdentificacion', () => {
  const NOW = new Date('2026-08-13T10:00:00.000Z');

  beforeEach(() => {
    jest.useFakeTimers().setSystemTime(NOW);
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  test('no_identificado → identificado, sin valor previo: setea ahora + usuario actual', () => {
    const result = resolvePrimeraIdentificacion(
      'identificado',
      { primeraIdentificacionAt: null, primeraIdentificacionPor: null },
      { _id: 'user-1', nombre: 'Ana' },
    );
    expect(result).toEqual({
      primeraIdentificacionAt: NOW,
      primeraIdentificacionPor: { userId: 'user-1', nombre: 'Ana' },
    });
  });

  test('no_identificado → identificado, ya había valor previo (re-identificación tras revert): no lo toca', () => {
    const previo = new Date('2026-01-01T00:00:00.000Z');
    const result = resolvePrimeraIdentificacion(
      'identificado',
      { primeraIdentificacionAt: previo, primeraIdentificacionPor: { userId: 'user-viejo', nombre: 'Beto' } },
      { _id: 'user-2', nombre: 'Carla' },
    );
    expect(result).toEqual({
      primeraIdentificacionAt: previo,
      primeraIdentificacionPor: { userId: 'user-viejo', nombre: 'Beto' },
    });
  });

  test('identificado → no_identificado (desvinculación): preserva el valor existente, nunca lo limpia', () => {
    const previo = new Date('2026-01-01T00:00:00.000Z');
    const result = resolvePrimeraIdentificacion(
      'no_identificado',
      { primeraIdentificacionAt: previo, primeraIdentificacionPor: { userId: 'user-1', nombre: 'Ana' } },
      { _id: 'user-2', nombre: 'Carla' },
    );
    expect(result).toEqual({
      primeraIdentificacionAt: previo,
      primeraIdentificacionPor: { userId: 'user-1', nombre: 'Ana' },
    });
  });

  test('identificado → identificado (no-op de status): preserva el valor existente', () => {
    const previo = new Date('2026-01-01T00:00:00.000Z');
    const result = resolvePrimeraIdentificacion(
      'identificado',
      { primeraIdentificacionAt: previo, primeraIdentificacionPor: { userId: 'user-1', nombre: 'Ana' } },
      { _id: 'user-1', nombre: 'Ana' },
    );
    expect(result).toEqual({
      primeraIdentificacionAt: previo,
      primeraIdentificacionPor: { userId: 'user-1', nombre: 'Ana' },
    });
  });

  test('sin valor previo y sin usuario real (motor automático): no explota, userId/nombre quedan null', () => {
    const result = resolvePrimeraIdentificacion(
      'identificado',
      { primeraIdentificacionAt: null, primeraIdentificacionPor: null },
      null,
    );
    expect(result).toEqual({
      primeraIdentificacionAt: NOW,
      primeraIdentificacionPor: { userId: null, nombre: null },
    });
  });

  test('actual undefined (doc nuevo sin los campos todavía): se comporta como sin valor previo', () => {
    const result = resolvePrimeraIdentificacion('identificado', undefined, { _id: 'user-1', nombre: 'Ana' });
    expect(result).toEqual({
      primeraIdentificacionAt: NOW,
      primeraIdentificacionPor: { userId: 'user-1', nombre: 'Ana' },
    });
  });
});
