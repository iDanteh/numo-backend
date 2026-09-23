'use strict';

// bank.service.getCards.rangoExplicito.test.js — getCards() (dashboard "Estatus", 2026-09-23):
// rango explícito fechaInicio/fechaFin (combinar meses de forma continua) debe ganar por
// completo sobre year/month, mismo criterio de precedencia que _resolverMatchTiempo() en
// bank-indicadores.service.js. Solo se prueba el `$match` armado — no la agregación completa.
//
// Mismo patrón de mocks que bank.service.rangoAnioMesMexico.test.js/bank.service.ficha.test.js
// — solo dependencias con I/O.
jest.mock('./BankMovement.model');
jest.mock('../../shared/socket');
jest.mock('./drive-fichas.service');
jest.mock('./repositories/bank-config.repository');

const BankMovement   = require('./BankMovement.model');
const bankConfigRepo = require('./repositories/bank-config.repository');
const { getCards }   = require('./bank.service');

describe('getCards() — precedencia de rango explícito sobre year/month', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    BankMovement.aggregate.mockResolvedValue([]);
    bankConfigRepo.findAllAsMap.mockResolvedValue(new Map());
  });

  function matchDeLaPrimeraAgregacion() {
    // Primera llamada a aggregate() = la agregación principal (por banco); la firma de
    // aggregate es (pipeline, opts) — pipeline[0] siempre es el $match.
    return BankMovement.aggregate.mock.calls[0][0][0].$match;
  }

  test('con fechaInicio+fechaFin: match.fecha usa ese rango exacto, ignorando year/month aunque también vengan', async () => {
    await getCards(null, '2026', '2', '2026-01-01', '2026-03-31');

    const match = matchDeLaPrimeraAgregacion();
    expect(match.fecha.$gte.toISOString()).toBe('2026-01-01T06:00:00.000Z');
    expect(match.fecha.$lte.toISOString()).toBe('2026-04-01T05:59:59.999Z');
  });

  test('con rango parcial (falta fechaFin): se ignora el rango y se cae al comportamiento de year/month de siempre', async () => {
    await getCards(null, '2026', '2', '2026-01-01', null);

    const match = matchDeLaPrimeraAgregacion();
    // _rangoAnioMesMexico('2026','2') — mismo criterio ya probado en
    // bank.service.rangoAnioMesMexico.test.js.
    expect(match.fecha.$gte.toISOString()).toBe('2026-02-01T06:00:00.000Z');
    expect(match.fecha.$lt.toISOString()).toBe('2026-03-01T06:00:00.000Z');
    expect(match.fecha.$lte).toBeUndefined();
  });

  test('con rango parcial (falta fechaInicio): mismo fallback a year/month', async () => {
    await getCards(null, '2026', '2', null, '2026-03-31');

    const match = matchDeLaPrimeraAgregacion();
    expect(match.fecha.$gte.toISOString()).toBe('2026-02-01T06:00:00.000Z');
    expect(match.fecha.$lt.toISOString()).toBe('2026-03-01T06:00:00.000Z');
  });

  test('sin fechaInicio/fechaFin ni year: match no lleva `fecha` (comportamiento previo sin cambios)', async () => {
    await getCards(null, null, null);

    const match = matchDeLaPrimeraAgregacion();
    expect(match.fecha).toBeUndefined();
  });

  test('sin fechaInicio/fechaFin, solo year: comportamiento previo intacto (regresión)', async () => {
    await getCards(null, '2026', null);

    const match = matchDeLaPrimeraAgregacion();
    expect(match.fecha.$gte.toISOString()).toBe('2026-01-01T06:00:00.000Z');
    expect(match.fecha.$lt.toISOString()).toBe('2027-01-01T06:00:00.000Z');
  });
});
