'use strict';

/**
 * loyalty-kore-match.service.test.js
 * ─────────────────────────────────────────────────────────────────────────────
 * LoyaltyCard + el cliente HTTP de Kore mockeados — `normalizarTarjeta` se usa
 * REAL (función pura) para verificar el cruce de punta a punta, mismo criterio
 * que loyalty-import-apply.service.test.js. `emitToUser` mockeado (igual que
 * loyalty-import-apply.service.test.js) para probar `iniciarMatchKoreJob`/
 * `_correrMatchKoreJob`/`obtenerEstadoMatchKoreJob` sin sockets reales.
 */

jest.mock('../../../shared/models/postgres', () => ({
  LoyaltyCard: { findAll: jest.fn() },
}));
jest.mock('./loyalty-kore-monedero.client', () => ({
  obtenerClientesConMonedero: jest.fn(),
}));
jest.mock('../../../banks/shared/socket', () => ({ emitToUser: jest.fn() }));
jest.mock('../../../shared/utils/logger', () => ({
  logger: { info: jest.fn(), warn: jest.fn(), error: jest.fn() },
}));

const { Op } = require('sequelize');
const { LoyaltyCard } = require('../../../shared/models/postgres');
const { obtenerClientesConMonedero } = require('./loyalty-kore-monedero.client');
const { emitToUser } = require('../../../banks/shared/socket');
const { logger } = require('../../../shared/utils/logger');
const {
  correrMatchKore, iniciarMatchKoreJob, _correrMatchKoreJob, obtenerEstadoMatchKoreJob,
} = require('./loyalty-kore-match.service');

// Mismo criterio que loyalty-import-apply.service.test.js: `_id` (auth0Sub) es lo que usan la sala
// de Socket.IO y el chequeo de dueño del job, no `dbId`.
const USER = { dbId: 7, _id: 'auth0|test-admin', nombre: 'Admin' };

function card(overrides = {}) {
  return {
    id: 1, telefono: '5551234567', nombre: 'CLIENTE', tarjetaLegada: '9005',
    update: jest.fn(async function update(changes) { Object.assign(this, changes); }),
    ...overrides,
  };
}

beforeEach(() => {
  jest.clearAllMocks();
});

describe('correrMatchKore', () => {
  test('solo trae LoyaltyCard con tarjetaLegada no nulo', async () => {
    LoyaltyCard.findAll.mockResolvedValue([]);
    obtenerClientesConMonedero.mockResolvedValue([]);

    await correrMatchKore();

    expect(LoyaltyCard.findAll).toHaveBeenCalledWith({ where: { tarjetaLegada: { [Op.ne]: null } } });
  });

  test('match exacto por tarjetaLegada === normalizarTarjeta(monedero): UPDATEa koreClienteId/koreMonedero', async () => {
    const c = card({ tarjetaLegada: '44' }); // ya normalizado al persistirse (ver loyalty-import-apply.service.js)
    LoyaltyCard.findAll.mockResolvedValue([c]);
    obtenerClientesConMonedero.mockResolvedValue([
      { id: '1066bvfaevunm', codigo: 18595, nombre: 'ALBERTO', razonSocial: 'ALBERTO ANGEL', monedero: '044' }, // normaliza a '44'
    ]);

    const resumen = await correrMatchKore();

    expect(c.update).toHaveBeenCalledWith({ koreClienteId: '1066bvfaevunm', koreMonedero: '044' });
    expect(resumen).toMatchObject({
      totalKore: 1,
      totalCardsConTarjetaLegada: 1,
      matcheados: 1,
      sinMatchEnKoreCount: 0,
      sinMatchEnLealtadCount: 0,
    });
    // matcheadosDetalle (2026-10-23): alimenta el Excel que genera el frontend al terminar el
    // job — NUNCA se trunca, a diferencia de las listas de error.
    expect(resumen.matcheadosDetalle).toEqual([
      { telefono: '5551234567', nombre: 'CLIENTE', tarjetaLegada: '44', koreClienteId: '1066bvfaevunm', koreMonedero: '044' },
    ]);
  });

  test('cliente de Kore sin match en Lealtad: se reporta en sinMatchEnLealtad, nunca escribe nada', async () => {
    LoyaltyCard.findAll.mockResolvedValue([card({ tarjetaLegada: '1' })]);
    obtenerClientesConMonedero.mockResolvedValue([
      { id: 'kore-2', nombre: 'SIN MATCH', razonSocial: 'SIN MATCH SA', monedero: '999' },
    ]);

    const resumen = await correrMatchKore();

    expect(resumen.matcheados).toBe(0);
    expect(resumen.sinMatchEnLealtadCount).toBe(1);
    expect(resumen.sinMatchEnLealtad).toEqual([
      { koreClienteId: 'kore-2', nombre: 'SIN MATCH', razonSocial: 'SIN MATCH SA', monedero: '999' },
    ]);
    expect(resumen.sinMatchEnKoreCount).toBe(1); // la única card tampoco matcheó
  });

  test('tarjeta de Lealtad sin match en Kore: se reporta en sinMatchEnKore', async () => {
    const c = card({ id: 7, telefono: '5559999999', nombre: 'HUERFANA', tarjetaLegada: 'CF-3615' });
    LoyaltyCard.findAll.mockResolvedValue([c]);
    obtenerClientesConMonedero.mockResolvedValue([
      { id: 'kore-1', nombre: 'OTRO', monedero: '1' }, // no matchea con CF-3615
    ]);

    const resumen = await correrMatchKore();

    expect(c.update).not.toHaveBeenCalled();
    expect(resumen.sinMatchEnKoreCount).toBe(1);
    expect(resumen.sinMatchEnKore).toEqual([
      { cardId: 7, telefono: '5559999999', nombre: 'HUERFANA', tarjetaLegada: 'CF-3615' },
    ]);
    // sinMatchEnKoreDetalle (2026-10-23): alimenta la 2da pestaña del Excel — mismo shape que
    // sinMatchEnKore, pero nunca se trunca (ver siguiente test).
    expect(resumen.sinMatchEnKoreDetalle).toEqual(resumen.sinMatchEnKore);
  });

  test('2+ LoyaltyCard con la MISMA tarjetaLegada (no debería pasar en operación normal, pero no debe perder ninguna): ambas se actualizan', async () => {
    const c1 = card({ id: 1, tarjetaLegada: '5' });
    const c2 = card({ id: 2, tarjetaLegada: '5' });
    LoyaltyCard.findAll.mockResolvedValue([c1, c2]);
    obtenerClientesConMonedero.mockResolvedValue([{ id: 'kore-5', monedero: '5' }]);

    const resumen = await correrMatchKore();

    expect(c1.update).toHaveBeenCalledWith({ koreClienteId: 'kore-5', koreMonedero: '5' });
    expect(c2.update).toHaveBeenCalledWith({ koreClienteId: 'kore-5', koreMonedero: '5' });
    expect(resumen.matcheados).toBe(2);
  });

  test('idempotente: correrlo de nuevo con el mismo universo no cambia el resultado (se puede volver a llamar sin efectos destructivos)', async () => {
    const c = card({ tarjetaLegada: '44' });
    LoyaltyCard.findAll.mockResolvedValue([c]);
    obtenerClientesConMonedero.mockResolvedValue([{ id: 'kore-1', monedero: '044' }]);

    const primero = await correrMatchKore();
    const segundo = await correrMatchKore();

    expect(primero).toEqual(segundo);
    expect(c.update).toHaveBeenCalledTimes(2); // se vuelve a llamar, pero con el MISMO valor — no destructivo
  });

  test('conteos truncan la lista de ejemplos a 100 pero el *_count siempre es completo', async () => {
    const cards = Array.from({ length: 150 }, (_, i) => card({ id: i, telefono: `55500000${String(i).padStart(2, '0')}`, tarjetaLegada: `legada-${i}` }));
    LoyaltyCard.findAll.mockResolvedValue(cards);
    obtenerClientesConMonedero.mockResolvedValue([]); // ninguna matchea

    const resumen = await correrMatchKore();

    expect(resumen.sinMatchEnKoreCount).toBe(150);
    expect(resumen.sinMatchEnKore).toHaveLength(100);
    // sinMatchEnKoreDetalle NO se trunca — a diferencia de sinMatchEnKore, alimenta el Excel.
    expect(resumen.sinMatchEnKoreDetalle).toHaveLength(150);
  });

  // Fix reliability (revisión 4-lens 2026-10-22): 2 clientes DISTINTOS de Kore comparten el mismo
  // `monedero` normalizado (dato externo sucio) — antes, el segundo pisaba en silencio el match
  // del primero. Ahora el primero gana, el segundo se cuenta como conflictivo y NUNCA escribe.
  test('monedero duplicado en Kore: el primer cliente gana, el segundo NO pisa el match y se cuenta en monederosDuplicadosEnKore', async () => {
    const c = card({ tarjetaLegada: '44' });
    LoyaltyCard.findAll.mockResolvedValue([c]);
    obtenerClientesConMonedero.mockResolvedValue([
      { id: 'kore-primero', nombre: 'PRIMERO', razonSocial: 'PRIMERO SA', monedero: '044' },
      { id: 'kore-segundo', nombre: 'SEGUNDO', razonSocial: 'SEGUNDO SA', monedero: '44' }, // mismo normalizado
    ]);

    const resumen = await correrMatchKore();

    expect(c.update).toHaveBeenCalledTimes(1);
    expect(c.update).toHaveBeenCalledWith({ koreClienteId: 'kore-primero', koreMonedero: '044' });
    expect(resumen.matcheados).toBe(1);
    expect(resumen.monederosDuplicadosEnKoreCount).toBe(1);
    expect(resumen.monederosDuplicadosEnKore).toEqual([
      { koreClienteId: 'kore-segundo', nombre: 'SEGUNDO', razonSocial: 'SEGUNDO SA', monedero: '44' },
    ]);
    expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining('monedero duplicado en Kore'));
  });

  // Fix reliability/resilience (revisión 4-lens 2026-10-22): un error transitorio en UN
  // `card.update(...)` ya no aborta el resto de la corrida.
  test('card.update que falla no detiene el resto de la corrida: se registra en erroresActualizacion y se sigue', async () => {
    const c1 = card({ id: 1, telefono: '5551111111', tarjetaLegada: '1' });
    const c2 = card({ id: 2, telefono: '5552222222', tarjetaLegada: '2' });
    c1.update.mockRejectedValueOnce(new Error('timeout de DB'));
    LoyaltyCard.findAll.mockResolvedValue([c1, c2]);
    obtenerClientesConMonedero.mockResolvedValue([
      { id: 'kore-1', monedero: '1' },
      { id: 'kore-2', monedero: '2' },
    ]);

    const resumen = await correrMatchKore();

    expect(c1.update).toHaveBeenCalledTimes(1);
    expect(c2.update).toHaveBeenCalledWith({ koreClienteId: 'kore-2', koreMonedero: '2' });
    expect(resumen.matcheados).toBe(1); // solo c2 se aplicó de verdad
    expect(resumen.erroresActualizacionCount).toBe(1);
    expect(resumen.erroresActualizacion).toEqual([
      { cardId: 1, telefono: '5551111111', error: 'timeout de DB' },
    ]);
    expect(logger.error).toHaveBeenCalledWith(expect.stringContaining('se omite y se continúa'));
  });

  test('onProgress: se invoca con procesados/total crecientes a medida que se recorre clientesKore', async () => {
    const cards = [card({ id: 1, tarjetaLegada: '1' })];
    LoyaltyCard.findAll.mockResolvedValue(cards);
    obtenerClientesConMonedero.mockResolvedValue([{ id: 'kore-1', monedero: '1' }]);

    const onProgress = jest.fn();
    await correrMatchKore({ onProgress });

    expect(onProgress).toHaveBeenCalledWith({ procesados: 1, total: 1 });
  });
});

describe('iniciarMatchKoreJob / _correrMatchKoreJob (match con Kore en background)', () => {
  test('_correrMatchKoreJob: corre correrMatchKore completo, emite progreso + done, y el resultado queda disponible vía obtenerEstadoMatchKoreJob', async () => {
    const c = card({ tarjetaLegada: '44' });
    LoyaltyCard.findAll.mockResolvedValue([c]);
    obtenerClientesConMonedero.mockResolvedValue([{ id: 'kore-1', monedero: '044' }]);

    await _correrMatchKoreJob('job-kore-1', USER);

    const estado = obtenerEstadoMatchKoreJob('job-kore-1', USER._id);
    expect(estado.status).toBe('done');
    expect(estado.resultado).toMatchObject({ totalKore: 1, matcheados: 1 });

    expect(emitToUser).toHaveBeenCalledWith('auth0|test-admin', 'lealtad-import:kore-match:progress', expect.objectContaining({ jobId: 'job-kore-1', procesados: 1, total: 1 }));
    expect(emitToUser).toHaveBeenCalledWith('auth0|test-admin', 'lealtad-import:kore-match:done', expect.objectContaining({ jobId: 'job-kore-1', matcheados: 1 }));
  });

  test('_correrMatchKoreJob: un error catastrófico (ej. Kore inalcanzable) pasa el job a "error" sin dejarlo colgado en "running"', async () => {
    LoyaltyCard.findAll.mockResolvedValue([]);
    obtenerClientesConMonedero.mockRejectedValue(new Error('Kore no responde'));

    await _correrMatchKoreJob('job-kore-error', USER);

    const estado = obtenerEstadoMatchKoreJob('job-kore-error', USER._id);
    expect(estado.status).toBe('error');
    expect(estado.error).toBe('Kore no responde');
    expect(emitToUser).toHaveBeenCalledWith('auth0|test-admin', 'lealtad-import:kore-match:error', expect.objectContaining({ jobId: 'job-kore-error', error: 'Kore no responde' }));
    expect(logger.error).toHaveBeenCalledWith(expect.stringContaining('job-kore-error'));
  });

  test('iniciarMatchKoreJob: devuelve jobId de inmediato (fire-and-forget) y el job arranca en "running"', async () => {
    LoyaltyCard.findAll.mockResolvedValue([]);
    obtenerClientesConMonedero.mockResolvedValue([]);

    const { jobId } = iniciarMatchKoreJob(USER);

    expect(jobId).toMatch(/^lealtad-kore-match-/);
    // Antes de drenar el microtask queue, el job ya quedó registrado como 'running' (mismo
    // criterio que iniciarAplicarJob/_iniciarJobDesdeFilasClasificadas: no se espera el fire-and-forget).
    const estadoInicial = obtenerEstadoMatchKoreJob(jobId, USER._id);
    expect(estadoInicial.status).toBe('running');

    await new Promise((resolve) => setImmediate(resolve));
    expect(obtenerEstadoMatchKoreJob(jobId, USER._id).status).toBe('done');
  });

  describe('obtenerEstadoMatchKoreJob', () => {
    test('jobId inexistente: null', () => {
      expect(obtenerEstadoMatchKoreJob('no-existe', USER._id)).toBeNull();
    });

    test('userId distinto al dueño del job: null (no filtra el resultado a otro usuario)', async () => {
      LoyaltyCard.findAll.mockResolvedValue([]);
      obtenerClientesConMonedero.mockResolvedValue([]);

      await _correrMatchKoreJob('job-ajeno', USER);

      expect(obtenerEstadoMatchKoreJob('job-ajeno', 'auth0|otro-usuario')).toBeNull();
    });
  });
});
