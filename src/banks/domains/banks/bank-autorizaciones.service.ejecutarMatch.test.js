'use strict';

// bank-autorizaciones.service.ejecutarMatch.test.js — test de regresión para
// ejecutarMatch() ANTES/DESPUÉS del refactor idsAIdentificar: Set<string> →
// Map<string, {primeraIdentificacionAt, primeraIdentificacionPor}> (necesario
// para poder llamar resolvePrimeraIdentificacion() por movimiento antes de
// armar el updateOne del bulkWrite).
//
// Este archivo se corrió primero contra el código SIN el refactor (capturando
// la forma exacta de `ops` que generaba el bulkWrite: status + identificadoPor,
// sin primeraIdentificacionAt/Por) y después se amplió para exigir también los
// 2 campos nuevos, corriendo de nuevo para confirmar que el refactor no cambió
// nada del comportamiento previo (mismo _id, mismo filter ACID, mismo status,
// mismo identificadoPor) y sí agrega los campos nuevos correctamente.
jest.mock('./BankMovement.model');
// DATE_WINDOW_DAYS ahora viene de Configuraciones Globales (sección erp-caja) en vez
// de process.env.ERP_DATE_WINDOW_DAYS — se mockea para no depender de Postgres real
// en este test y reproducir el mismo default (30 días) que tenía el código viejo.
jest.mock('../../../shared/services/global-config.service');

const BankMovement = require('./BankMovement.model');
const globalConfigService = require('../../../shared/services/global-config.service');
const { ejecutarMatch } = require('./bank-autorizaciones.service');

beforeEach(() => {
  globalConfigService.getValue.mockResolvedValue('30');
});

// Mongoose Query real es chainable: find().select().lean() — se replica la
// cadena con jest.fn() que devuelve el mismo objeto hasta el .lean() final,
// que resuelve la promesa con el array de movimientos.
function fakeFindQuery(movimientos) {
  const q = {};
  q.select = jest.fn(() => q);
  q.lean   = jest.fn().mockResolvedValue(movimientos);
  return q;
}

function fakeMov(overrides = {}) {
  return {
    _id: 'mov-1',
    numeroAutorizacion: '123456',
    referenciaNumerica: null,
    concepto: 'DEPOSITO SPEI',
    deposito: 500,
    retiro: null,
    status: 'no_identificado',
    banco: 'BBVA',
    fecha: null,
    ...overrides,
  };
}

beforeEach(() => {
  jest.clearAllMocks();
});

describe('ejecutarMatch — bulkWrite de idsAIdentificar (Tier 1a: numeroAutorizacion)', () => {
  test('movimiento no_identificado con auth exacto: se identifica y el updateOne conserva filter/status/identificadoPor de siempre', async () => {
    const mov = fakeMov();
    BankMovement.find.mockReturnValue(fakeFindQuery([mov]));
    BankMovement.bulkWrite.mockResolvedValue({ modifiedCount: 1 });

    const user = { userId: 'auth0|user-1', nombre: 'Ana' };
    const rows = [{ autNorm: '123456', importe: 500, banco: 'BBVA', fecha: null }];

    const resultado = await ejecutarMatch(rows, user);

    expect(resultado.matcheados).toBe(1);
    expect(resultado.identificados).toBe(1);

    // El primer bulkWrite es el de idsAIdentificar (identificación); puede haber
    // un segundo bulkWrite para idsActualizarAuth, pero en este caso numeroAutorizacion
    // ya estaba presente en el mov, así que no debería dispararse.
    expect(BankMovement.bulkWrite).toHaveBeenCalledTimes(1);

    const ops = BankMovement.bulkWrite.mock.calls[0][0];
    expect(ops).toHaveLength(1);

    const { filter, update } = ops[0].updateOne;
    expect(filter._id).toBe('mov-1');
    expect(filter.status).toBe('no_identificado');
    expect(update.$set.status).toBe('identificado');
    expect(update.$set.identificadoPor).toEqual([
      { userId: 'auth0|user-1', nombre: 'Ana', fechaId: expect.any(Date) },
    ]);

    // ── Campos nuevos (post-refactor): primeraIdentificacionAt/Por ──────────
    // Transición no_identificado → identificado, sin valor previo → se setea ahora.
    expect(update.$set.primeraIdentificacionAt).toBeInstanceOf(Date);
    expect(update.$set.primeraIdentificacionPor).toEqual({
      userId: 'auth0|user-1',
      nombre: 'Ana',
    });
  });

  test('movimiento no_identificado que YA tenía primeraIdentificacionAt (re-identificación tras revert): no lo pisa', async () => {
    const previo = new Date('2026-01-01T00:00:00.000Z');
    const mov = fakeMov({
      primeraIdentificacionAt: previo,
      primeraIdentificacionPor: { userId: 'user-viejo', nombre: 'Beto' },
    });
    BankMovement.find.mockReturnValue(fakeFindQuery([mov]));
    BankMovement.bulkWrite.mockResolvedValue({ modifiedCount: 1 });

    const user = { userId: 'auth0|user-2', nombre: 'Carla' };
    const rows = [{ autNorm: '123456', importe: 500, banco: 'BBVA', fecha: null }];

    await ejecutarMatch(rows, user);

    const ops = BankMovement.bulkWrite.mock.calls[0][0];
    const { update } = ops[0].updateOne;

    expect(update.$set.primeraIdentificacionAt).toEqual(previo);
    expect(update.$set.primeraIdentificacionPor).toEqual({ userId: 'user-viejo', nombre: 'Beto' });
    // El resto del comportamiento no cambia: sigue identificando con el usuario actual.
    expect(update.$set.identificadoPor[0].userId).toBe('auth0|user-2');
  });

  test('movimiento ya identificado (Fase 4, ya_identificado sin auth): no entra a idsAIdentificar, no se le toca primeraIdentificacionAt', async () => {
    const mov = fakeMov({ status: 'identificado', numeroAutorizacion: null, deposito: 500 });
    BankMovement.find.mockReturnValue(fakeFindQuery([mov]));
    BankMovement.bulkWrite.mockResolvedValue({ modifiedCount: 0 });

    const user = { userId: 'auth0|user-1', nombre: 'Ana' };
    // autNorm no coincide con nada estructurado; cae a Fase 4 por banco+monto.
    const rows = [{ autNorm: '999999', importe: 500, banco: 'BBVA', fecha: null }];

    const resultado = await ejecutarMatch(rows, user);

    expect(resultado.yaIdentificados).toBe(1);
    // idsAIdentificar queda vacío → bulkWrite de identificación no se llama.
    // (Puede llamarse bulkWrite para idsActualizarAuth, que es un array distinto.)
    const idsAIdentificarCalls = BankMovement.bulkWrite.mock.calls.filter(
      ([ops]) => ops.some(op => op.updateOne.update.$set?.status === 'identificado'),
    );
    expect(idsAIdentificarCalls).toHaveLength(0);
  });
});
