'use strict';

// bank.service.diaMexico.test.js — unit tests directos de _inicioDiaMx/_finDiaMx
// (helpers agregados 2026-09-11 para el fix de huso horario en los 3 filtros de
// fecha de exportMovements: fecha, fechaAplicacion, fechaImportacion). Mismo
// criterio que _medianocheMx (collection-request-indicadores.service.js /
// cfdi-poliza-generator.service.js): offset fijo UTC-6, México sin DST desde 2022.
jest.mock('./BankMovement.model');
jest.mock('../../shared/socket', () => ({ emitToUser: jest.fn(), emitToBanco: jest.fn(), emitToAll: jest.fn() }));

const { _inicioDiaMx, _finDiaMx } = require('./bank.service');

describe('_inicioDiaMx / _finDiaMx', () => {
  test('_inicioDiaMx: medianoche de México = 06:00:00.000 UTC del mismo día calendario', () => {
    expect(_inicioDiaMx('2026-09-11')).toEqual(new Date('2026-09-11T06:00:00.000Z'));
  });

  test('_finDiaMx: 23:59:59.999 de México = 05:59:59.999 UTC del día calendario SIGUIENTE', () => {
    expect(_finDiaMx('2026-09-11')).toEqual(new Date('2026-09-12T05:59:59.999Z'));
  });

  test('el rango [_inicioDiaMx, _finDiaMx] de un mismo día dura exactamente 24h (menos 1ms)', () => {
    const inicio = _inicioDiaMx('2026-09-11').getTime();
    const fin    = _finDiaMx('2026-09-11').getTime();
    expect(fin - inicio).toBe(24 * 60 * 60 * 1000 - 1);
  });

  test('bug real: un createdAt de 2026-09-11T01:00:00.000Z (19:00 hora MX del 10-sep) cae en el rango del día MX 10, no en el 11', () => {
    const createdAt = new Date('2026-09-11T01:00:00.000Z').getTime();
    expect(createdAt).toBeGreaterThanOrEqual(_inicioDiaMx('2026-09-10').getTime());
    expect(createdAt).toBeLessThanOrEqual(_finDiaMx('2026-09-10').getTime());
    expect(createdAt).toBeLessThan(_inicioDiaMx('2026-09-11').getTime());
  });
});
