'use strict';

// bank.service.rangoAnioMesMexico.test.js — _rangoAnioMesMexico() (getCards()/getStatusStats()).
// Bug real 2026-09-09 (auditoría de TZ): el contenedor de producción corre en UTC, sin `TZ`
// fijado. `new Date(y, m-1, 1)` construye medianoche en la hora LOCAL DEL PROCESO — en un
// contenedor UTC eso corre el límite de mes 6hs (México es UTC-6 fijo, sin horario de verano
// desde 2022), pudiendo contar un movimiento de fin de mes en el mes equivocado. El fix usa
// Date.UTC(...,6,0,0) para representar medianoche en México como instante UTC real, inmune al
// TZ del proceso — estos tests corren forzando process.env.TZ='UTC' para probar exactamente el
// escenario que rompía antes del fix.
//
// Mismo patrón de mocks que bank.service.ficha.test.js — solo dependencias con I/O.
jest.mock('./BankMovement.model');
jest.mock('../../shared/socket');
jest.mock('./drive-fichas.service');

describe('_rangoAnioMesMexico (forzando process.env.TZ = "UTC", como el contenedor real)', () => {
  let originalTz;
  let _rangoAnioMesMexico;

  beforeAll(() => {
    originalTz = process.env.TZ;
    process.env.TZ = 'UTC';
    jest.resetModules();
    ({ _rangoAnioMesMexico } = require('./bank.service'));
  });

  afterAll(() => {
    process.env.TZ = originalTz;
  });

  test('rango de mes: un movimiento del 31-ene a las 21:00 hora México sigue contando en enero', () => {
    const rango = _rangoAnioMesMexico('2026', '1');
    // 21:00 del 31-ene en México (UTC-6) = 2026-02-01T03:00:00Z
    const finDeMesMexico = new Date('2026-02-01T03:00:00Z');

    expect(finDeMesMexico >= rango.$gte).toBe(true);
    expect(finDeMesMexico < rango.$lt).toBe(true);
  });

  test('el límite $lt (inicio de febrero en México) es exactamente 2026-02-01T06:00:00Z, no medianoche UTC', () => {
    const rango = _rangoAnioMesMexico('2026', '1');
    expect(rango.$lt.toISOString()).toBe('2026-02-01T06:00:00.000Z');
    expect(rango.$gte.toISOString()).toBe('2026-01-01T06:00:00.000Z');
  });

  test('un movimiento justo a medianoche México del día 1 (00:00) SÍ entra en el mes, no en el anterior', () => {
    const rango = _rangoAnioMesMexico('2026', '2');
    const medianocheMexicoDia1 = new Date('2026-02-01T06:00:00Z');
    expect(medianocheMexicoDia1 >= rango.$gte).toBe(true);
    expect(medianocheMexicoDia1 < rango.$lt).toBe(true);
  });

  test('sin mes (solo año): rango de todo el año en hora de México', () => {
    const rango = _rangoAnioMesMexico('2026', null);
    expect(rango.$gte.toISOString()).toBe('2026-01-01T06:00:00.000Z');
    expect(rango.$lt.toISOString()).toBe('2027-01-01T06:00:00.000Z');
  });
});
