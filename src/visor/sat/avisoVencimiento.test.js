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
  test('12:00 MX del día anterior al vencimiento', () => {
    expect(inicioAviso(VENCE).toISOString()).toBe('2026-10-23T18:00:00.000Z');
  });

  test('vencimiento en la noche MX (ya es otro día en UTC) usa el día MX', () => {
    // 24-oct 23:30 MX = 25-oct 05:30 UTC → aviso desde 23-oct 12:00 MX
    expect(inicioAviso(new Date('2026-10-25T05:30:00Z')).toISOString()).toBe('2026-10-23T18:00:00.000Z');
  });
});

describe('avisosVigentes', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    entityRepo.findWithAutoSync.mockResolvedValue([{ rfc: 'CCO011113663', nombre: 'TUBOS' }]);
  });

  test('23-oct 11:59 MX: todavía no aparece', async () => {
    const ahora = new Date('2026-10-23T17:59:00Z');
    tieneCredenciales.mockResolvedValue(enHorasAntes(ahora));
    expect(await avisosVigentes(ahora)).toEqual([]);
  });

  test('23-oct 12:00 MX: aparece como por vencer', async () => {
    const ahora = new Date('2026-10-23T18:00:00Z');
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
