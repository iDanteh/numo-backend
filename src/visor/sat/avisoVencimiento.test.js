'use strict';

jest.mock('../repositories/entity.repository', () => ({ findWithAutoSync: jest.fn() }));
jest.mock('./credenciales', () => ({ tieneCredenciales: jest.fn() }));

const entityRepo = require('../repositories/entity.repository');
const { tieneCredenciales } = require('./credenciales');
const { avisosVigentes, inicioAviso } = require('./avisoVencimiento');

// Caso real: credenciales subidas 19-oct 9:20 MX → vencen 24-oct 9:20 MX (15:20 UTC).
const VENCE = new Date('2026-10-24T15:20:00Z');
const enHorasAntes = (ahora) => ({ tiene: true, ttlSegundos: Math.floor((VENCE - ahora) / 1000) });

describe('inicioAviso', () => {
  test('48 h antes del vencimiento (mismo momento que el correo de 2 días)', () => {
    expect(inicioAviso(VENCE).toISOString()).toBe('2026-10-22T15:20:00.000Z');
  });
});

describe('avisosVigentes', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    entityRepo.findWithAutoSync.mockResolvedValue([{ rfc: 'CCO011113663', nombre: 'TUBOS' }]);
  });

  test('faltando 48 h y 1 min: todavía no aparece', async () => {
    const ahora = new Date('2026-10-22T15:19:00Z');
    tieneCredenciales.mockResolvedValue(enHorasAntes(ahora));
    expect(await avisosVigentes(ahora)).toEqual([]);
  });

  test('faltando 48 h: aparece como por vencer', async () => {
    const ahora = new Date('2026-10-22T15:20:00Z');
    tieneCredenciales.mockResolvedValue(enHorasAntes(ahora));
    const [aviso] = await avisosVigentes(ahora);
    expect(aviso).toMatchObject({ rfc: 'CCO011113663', estado: 'por_vencer' });
    expect(aviso.expiraEn).toBe(VENCE.toISOString());
  });

  test('ya vencidas / nunca subidas: sigue apareciendo como faltan', async () => {
    tieneCredenciales.mockResolvedValue({ tiene: false, ttlSegundos: null });
    expect(await avisosVigentes(new Date('2026-10-25T18:00:00Z'))).toEqual([
      { rfc: 'CCO011113663', nombre: 'TUBOS', estado: 'faltan', expiraEn: null },
    ]);
  });

  test('recién subidas: no aparece', async () => {
    const ahora = new Date('2026-10-19T15:30:00Z');
    tieneCredenciales.mockResolvedValue(enHorasAntes(ahora));
    expect(await avisosVigentes(ahora)).toEqual([]);
  });
});
