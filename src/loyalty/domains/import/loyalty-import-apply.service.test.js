'use strict';

/**
 * domains/import/loyalty-import-apply.service.test.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Mismo criterio de mocking que loyalty-card.service.test.js: modelos +
 * conexión Postgres mockeados, nunca se toca una DB real. `construirConcentrado`
 * y `normalizarTelefono` se usan REALES (son funciones puras) para verificar
 * la clasificación de punta a punta.
 */

const { Transaction } = require('sequelize');

jest.mock('../../../shared/models/postgres', () => ({
  LoyaltyCard:              { findOne: jest.fn(), findByPk: jest.fn() },
  LoyaltyPointsLedger:      { findOne: jest.fn() },
  LoyaltyPendingEnrollment: {
    create: jest.fn(), findByPk: jest.fn(), findAndCountAll: jest.fn(), update: jest.fn(),
  },
}));
jest.mock('../../../config/database.postgres', () => ({
  sequelize: { transaction: jest.fn() },
}));
jest.mock('../cards/loyalty-card.service', () => ({
  crearTarjeta: jest.fn(),
}));
jest.mock('../ledger/loyalty-ledger.service', () => ({
  _aplicarMovimiento: jest.fn(),
}));
jest.mock('../../../banks/shared/socket', () => ({ emitToUser: jest.fn() }));

const {
  LoyaltyCard, LoyaltyPointsLedger, LoyaltyPendingEnrollment,
} = require('../../../shared/models/postgres');
const { sequelize } = require('../../../config/database.postgres');
const cardService = require('../cards/loyalty-card.service');
const ledgerService = require('../ledger/loyalty-ledger.service');
const { emitToUser } = require('../../../banks/shared/socket');
const { logger } = require('../../../shared/utils/logger');
const applyService = require('./loyalty-import-apply.service');

// _id: necesario para los tests de job/socket (iniciarAplicarJob/_correrAplicarJob) — la sala de
// Socket.IO y el chequeo de dueño del job usan `user._id` (auth0Sub), NO `dbId` (ver
// loyalty-import-apply.service.js#_userLabel vs. banks/shared/socket.js#emitToUser).
const USER = { dbId: 7, _id: 'auth0|test-admin', nombre: 'Admin' };

function cliente(overrides = {}) {
  return {
    tarjetaRaw: '9005',
    tarjetaNormalizada: '9005',
    nombre: 'CRISTINA DIAZ GUTIERREZ',
    celular: '5559876543',
    telefono: '',
    correo: 'cristina@test.com',
    categoriaOriginal: 'Oro',
    ...overrides,
  };
}

beforeEach(() => {
  jest.clearAllMocks();
  sequelize.transaction.mockImplementation(async (cb) => cb({ fake: true }));
});

describe('clasificar', () => {
  test('tarjeta_duplicada_en_clientes → duplicado_bloqueante en ambas filas, gana sobre cualquier otra condición', async () => {
    const resultado = await applyService.clasificar({
      clientesRows: [
        cliente({ tarjetaRaw: '3569', tarjetaNormalizada: '3569', nombre: 'UNO', celular: '5551111111' }),
        cliente({ tarjetaRaw: '3569', tarjetaNormalizada: '3569', nombre: 'DOS', celular: '5552222222' }),
      ],
    });

    expect(resultado.filas.every((f) => f.categoria === 'duplicado_bloqueante')).toBe(true);
    expect(resultado.clasificacionResumen.duplicadoBloqueante).toBe(2);
    // Precedencia: ni siquiera consulta la DB para una fila ya bloqueante por duplicado.
    expect(LoyaltyCard.findOne).not.toHaveBeenCalled();
  });

  test('sin_contacto → pendiente_sin_telefono, NO bloquea y no consulta la DB', async () => {
    const resultado = await applyService.clasificar({
      clientesRows: [cliente({ celular: '', telefono: '' })],
    });

    expect(resultado.filas[0].categoria).toBe('pendiente_sin_telefono');
    expect(resultado.clasificacionResumen.pendienteSinTelefono).toBe(1);
    expect(LoyaltyCard.findOne).not.toHaveBeenCalled();
  });

  test('contacto que no normaliza a 10 dígitos → invalido_bloqueante', async () => {
    const resultado = await applyService.clasificar({
      clientesRows: [cliente({ celular: '123' })],
    });

    expect(resultado.filas[0].categoria).toBe('invalido_bloqueante');
    expect(resultado.clasificacionResumen.invalidoBloqueante).toBe(1);
  });

  test('teléfono válido sin tarjeta previa → ok', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);

    const resultado = await applyService.clasificar({ clientesRows: [cliente()] });

    expect(resultado.filas[0].categoria).toBe('ok');
    expect(resultado.filas[0].telefonoNormalizado).toBe('5559876543');
    expect(resultado.clasificacionResumen.ok).toBe(1);
  });

  test('tarjeta existente CON movimiento de migración previo → ya_migrada (no bloquea)', async () => {
    LoyaltyCard.findOne.mockResolvedValue({ id: 99 });
    LoyaltyPointsLedger.findOne.mockResolvedValue({ id: 1, origen: 'migracion' });

    const resultado = await applyService.clasificar({ clientesRows: [cliente()] });

    expect(resultado.filas[0].categoria).toBe('ya_migrada');
    expect(resultado.clasificacionResumen.yaMigrada).toBe(1);
    expect(LoyaltyPointsLedger.findOne).toHaveBeenCalledWith({ where: { cardId: 99, origen: 'migracion' } });
  });

  test('tarjeta existente SIN movimiento de migración previo → conflicto_bloqueante', async () => {
    LoyaltyCard.findOne.mockResolvedValue({ id: 99 });
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);

    const resultado = await applyService.clasificar({ clientesRows: [cliente()] });

    expect(resultado.filas[0].categoria).toBe('conflicto_bloqueante');
    expect(resultado.clasificacionResumen.conflictoBloqueante).toBe(1);
  });

  test('caso real 2026-10-19: dos Tarjetas legadas DISTINTAS con el MISMO teléfono → telefono_duplicado_bloqueante en ambas, sin consultar la DB', async () => {
    const resultado = await applyService.clasificar({
      clientesRows: [
        cliente({ tarjetaRaw: '1001', tarjetaNormalizada: '1001', nombre: 'UNO', celular: '5559999999' }),
        cliente({ tarjetaRaw: '1002', tarjetaNormalizada: '1002', nombre: 'DOS', celular: '5559999999' }),
      ],
    });

    expect(resultado.filas.every((f) => f.categoria === 'telefono_duplicado_bloqueante')).toBe(true);
    expect(resultado.clasificacionResumen.telefonoDuplicadoBloqueante).toBe(2);
    // Precedencia: el chequeo de teléfono duplicado gana antes de tocar la DB.
    expect(LoyaltyCard.findOne).not.toHaveBeenCalled();
  });

  test('mismo teléfono pero una de las dos filas ya es duplicado_bloqueante por Tarjeta: esa NO se recuenta como telefono_duplicado_bloqueante', async () => {
    const resultado = await applyService.clasificar({
      clientesRows: [
        cliente({ tarjetaRaw: '3569', tarjetaNormalizada: '3569', nombre: 'UNO', celular: '5551111111' }),
        cliente({ tarjetaRaw: '3569', tarjetaNormalizada: '3569', nombre: 'DOS', celular: '5551111111' }),
      ],
    });

    expect(resultado.filas.every((f) => f.categoria === 'duplicado_bloqueante')).toBe(true);
    expect(resultado.clasificacionResumen.telefonoDuplicadoBloqueante).toBe(0);
  });
});

describe('aplicar', () => {
  test('decisión de negocio 2026-10-19: YA NO aborta con filas bloqueantes — las excluye y aplica el resto en la MISMA corrida', async () => {
    // Fila 1: conflicto_bloqueante (ya existe tarjeta, sin migración previa). Fila 2: 'ok'.
    LoyaltyCard.findOne.mockImplementation(async ({ where }) => (
      where.telefono === '5551111111' ? { id: 99 } : null
    ));
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    cardService.crearTarjeta.mockResolvedValue({ card: { id: 1 }, nip: '1234' });
    LoyaltyCard.findByPk.mockResolvedValue({ id: 1, activa: true });

    const resultado = await applyService.aplicar({
      clientesRows: [
        cliente({ tarjetaRaw: '1001', tarjetaNormalizada: '1001', nombre: 'BLOQUEANTE', celular: '5551111111' }),
        cliente({ tarjetaRaw: '1002', tarjetaNormalizada: '1002', nombre: 'OK', celular: '5552222222' }),
      ],
    }, USER);

    // La fila bloqueante NUNCA llega a `_aplicarFila` — solo se crea UNA tarjeta, la 'ok'.
    expect(cardService.crearTarjeta).toHaveBeenCalledTimes(1);
    expect(cardService.crearTarjeta).toHaveBeenCalledWith(
      expect.objectContaining({ telefono: '5552222222', nombre: 'OK' }),
      USER,
      expect.objectContaining({ transaction: expect.anything() }),
    );
    expect(sequelize.transaction).toHaveBeenCalledTimes(1); // sigue siendo UNA sola transacción

    expect(resultado.creadas).toHaveLength(1);
    expect(resultado.excluidasBloqueantes).toBe(1);
    expect(resultado.clasificacionResumen.conflictoBloqueante).toBe(1);
    expect(resultado.clasificacionResumen.ok).toBe(1);
  });

  test('camino feliz: crea tarjeta+MIGRACION para "ok" y un pendiente para "sin_contacto", en una sola transacción', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null); // la fila 'ok' no tiene tarjeta previa
    cardService.crearTarjeta.mockResolvedValue({ card: { id: 1 }, nip: '1234' });
    LoyaltyCard.findByPk.mockResolvedValue({ id: 1, activa: true });

    const resultado = await applyService.aplicar({
      clientesRows: [
        cliente({ tarjetaRaw: '9005', tarjetaNormalizada: '9005' }),
        cliente({
          tarjetaRaw: '9006', tarjetaNormalizada: '9006', nombre: 'SIN CONTACTO',
          celular: '', telefono: '', correo: null,
        }),
      ],
    }, USER);

    expect(sequelize.transaction).toHaveBeenCalledTimes(1);
    expect(cardService.crearTarjeta).toHaveBeenCalledTimes(1);
    expect(cardService.crearTarjeta).toHaveBeenCalledWith(
      expect.objectContaining({ telefono: '5559876543', nombre: 'CRISTINA DIAZ GUTIERREZ' }),
      USER,
      expect.objectContaining({ transaction: expect.anything() }),
    );
    expect(ledgerService._aplicarMovimiento).toHaveBeenCalledTimes(1);

    expect(LoyaltyPendingEnrollment.create).toHaveBeenCalledTimes(1);
    expect(LoyaltyPendingEnrollment.create).toHaveBeenCalledWith(
      expect.objectContaining({ nombre: 'SIN CONTACTO', estado: 'pendiente', correo: null }),
      expect.objectContaining({ transaction: expect.anything() }),
    );

    expect(resultado.creadas).toHaveLength(1);
    expect(resultado.creadas[0]).toEqual(expect.objectContaining({ telefono: '5559876543', nip: '1234' }));
    expect(resultado.pendientesCreadas).toBe(1);
    expect(resultado.omitidasYaMigradas).toBe(0);
    expect(resultado.excluidasBloqueantes).toBe(0);
  });

  test('"ya_migrada" se cuenta y se omite sin crear nada', async () => {
    LoyaltyCard.findOne.mockResolvedValue({ id: 99 });
    LoyaltyPointsLedger.findOne.mockResolvedValue({ id: 1, origen: 'migracion' });

    const resultado = await applyService.aplicar({ clientesRows: [cliente()] }, USER);

    expect(cardService.crearTarjeta).not.toHaveBeenCalled();
    expect(LoyaltyPendingEnrollment.create).not.toHaveBeenCalled();
    expect(resultado.omitidasYaMigradas).toBe(1);
    expect(resultado.creadas).toHaveLength(0);
    expect(resultado.excluidasBloqueantes).toBe(0);
  });
});

describe('iniciarAplicarJob / _correrAplicarJob (migración en background, por lotes)', () => {
  function filaClienteOk(i) {
    return cliente({
      tarjetaRaw: `T${i}`, tarjetaNormalizada: `T${i}`,
      nombre: `CLIENTE ${i}`, celular: `555${String(i).padStart(7, '0')}`,
    });
  }

  // Filas ya clasificadas, construidas a mano — usadas para llamar `_correrAplicarJob`
  // directamente (mismo criterio que netpay-reporte.service.test.js#_correrCargaReporteJob:
  // probar el job de fondo sin pasar por el fire-and-forget de `iniciarAplicarJob`).
  function filaOk(telefono) {
    return { categoria: 'ok', telefonoNormalizado: telefono, nombre: `CLIENTE ${telefono}`, nivelSugerido: 'Bronce', saldo: '10.00' };
  }

  test('decisión de negocio 2026-10-19: iniciarAplicarJob YA NO aborta con bloqueantes — el job arranca igual, excluyéndolas', async () => {
    // Fila 1: conflicto_bloqueante. Fila 2: 'ok'.
    LoyaltyCard.findOne.mockImplementation(async ({ where }) => (
      where.telefono === '5551111111' ? { id: 99 } : null
    ));
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    cardService.crearTarjeta.mockResolvedValue({ card: { id: 1 }, nip: '1234' });
    LoyaltyCard.findByPk.mockResolvedValue({ id: 1, activa: true });

    const { jobId, total, totalLotes } = await applyService.iniciarAplicarJob({
      clientesRows: [
        cliente({ tarjetaRaw: '1001', tarjetaNormalizada: '1001', nombre: 'BLOQUEANTE', celular: '5551111111' }),
        cliente({ tarjetaRaw: '1002', tarjetaNormalizada: '1002', nombre: 'OK', celular: '5552222222' }),
      ],
    }, USER);

    expect(jobId).toMatch(/^lealtad-aplicar-/);
    expect(total).toBe(1); // solo la fila 'ok' es accionable, la bloqueante queda excluida
    expect(totalLotes).toBe(1);

    await new Promise((resolve) => setImmediate(resolve));
    const estado = applyService.obtenerEstadoAplicarJob(jobId, USER._id);
    expect(estado.status).toBe('done');
    expect(estado.resultado.creadas).toHaveLength(1);
    expect(estado.resultado.excluidasBloqueantes).toBe(1);
    expect(cardService.crearTarjeta).toHaveBeenCalledTimes(1);
  });

  test('parte las filas accionables en lotes de BATCH_SIZE (200) y responde con jobId+total+totalLotes de inmediato', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null); // todas las filas → 'ok'
    cardService.crearTarjeta.mockResolvedValue({ card: { id: 1 }, nip: '1234' });
    LoyaltyCard.findByPk.mockResolvedValue({ id: 1, activa: true });

    const clientesRows = Array.from({ length: 250 }, (_, i) => filaClienteOk(i));
    const { jobId, total, totalLotes } = await applyService.iniciarAplicarJob({ clientesRows }, USER);

    expect(jobId).toMatch(/^lealtad-aplicar-/);
    expect(total).toBe(250);
    expect(totalLotes).toBe(2); // ceil(250 / 200) → un lote de 200 + uno de 50

    // `_correrAplicarJob` corre sin await (fire-and-forget, a propósito — ver comentario en el
    // servicio). Es pura cadena de promesas ya resueltas (mocks), sin timers reales de por
    // medio, así que un solo `setImmediate` alcanza para drenarla ANTES del siguiente test —
    // evita que este job en vuelo golpee los mocks que el próximo test resetea en su `beforeEach`.
    await new Promise((resolve) => setImmediate(resolve));
    expect(applyService.obtenerEstadoAplicarJob(jobId, USER._id).status).toBe('done');
  });

  test('_correrAplicarJob: emite progreso una vez por lote completado, con procesados/loteActual crecientes, y termina en "done"', async () => {
    cardService.crearTarjeta.mockResolvedValue({ card: { id: 1 }, nip: '1234' });
    LoyaltyCard.findByPk.mockResolvedValue({ id: 1, activa: true });

    const lote1 = Array.from({ length: 3 }, (_, i) => filaOk(`555000000${i}`));
    const lote2 = Array.from({ length: 2 }, (_, i) => filaOk(`555111111${i}`));
    const clasificacionResumen = { ok: 5, yaMigrada: 1, pendienteSinTelefono: 0, duplicadoBloqueante: 0, invalidoBloqueante: 0, conflictoBloqueante: 0, telefonoDuplicadoBloqueante: 0 };

    await applyService._correrAplicarJob('job-lotes', [lote1, lote2], USER, {}, clasificacionResumen);

    // Cada FILA es su propia transacción (ver docstring de _correrAplicarJob) — 5 filas, 5
    // transacciones, sin importar cómo se repartan entre los 2 lotes.
    expect(sequelize.transaction).toHaveBeenCalledTimes(5);
    expect(cardService.crearTarjeta).toHaveBeenCalledTimes(5);

    const progresos = emitToUser.mock.calls
      .filter((c) => c[1] === 'lealtad-import:apply:progress')
      .map((c) => c[2]);
    expect(progresos).toEqual([
      { jobId: 'job-lotes', procesados: 3, total: 5, loteActual: 1, totalLotes: 2, pct: 60 },
      { jobId: 'job-lotes', procesados: 5, total: 5, loteActual: 2, totalLotes: 2, pct: 100 },
    ]);

    const estado = applyService.obtenerEstadoAplicarJob('job-lotes', USER._id);
    expect(estado.status).toBe('done');
    expect(estado.procesados).toBe(5);
    expect(estado.loteActual).toBe(2);
    expect(estado.resultado.creadas).toHaveLength(5);
    expect(estado.resultado.omitidasYaMigradas).toBe(1); // tomado de clasificacionResumen.yaMigrada, no recorrido

    expect(emitToUser).toHaveBeenCalledWith('auth0|test-admin', 'lealtad-import:apply:done', expect.objectContaining({
      jobId: 'job-lotes', omitidasYaMigradas: 1,
    }));
  });

  test('decisión de negocio 2026-10-19: una fila que falla al escribir NUNCA detiene el job — se registra en erroresFilas y se sigue con las demás, status final "done"', async () => {
    jest.spyOn(logger, 'error').mockImplementation(() => {});

    let llamada = 0;
    cardService.crearTarjeta.mockImplementation(async () => {
      llamada += 1;
      if (llamada === 4) throw new Error('Ya existe una tarjeta registrada con ese teléfono.'); // falla justo en el lote 2
      return { card: { id: llamada }, nip: '1234' };
    });
    LoyaltyCard.findByPk.mockImplementation(async (id) => ({ id, activa: true }));

    const lote1 = Array.from({ length: 3 }, (_, i) => filaOk(`555000000${i}`)); // las 3 se aplican bien
    const lote2 = [filaOk('5551111110'), filaOk('5551111111')]; // la primera revienta, la segunda SÍ debe aplicarse
    const clasificacionResumen = { ok: 5, yaMigrada: 0, pendienteSinTelefono: 0, duplicadoBloqueante: 0, invalidoBloqueante: 0, conflictoBloqueante: 0, telefonoDuplicadoBloqueante: 0 };

    await applyService._correrAplicarJob('job-falla', [lote1, lote2], USER, {}, clasificacionResumen);

    // Las 5 filas se INTENTAN — ninguna detiene a las que vienen después, ni siquiera dentro del
    // mismo lote: cada una corre en su propia transacción.
    expect(cardService.crearTarjeta).toHaveBeenCalledTimes(5);
    expect(sequelize.transaction).toHaveBeenCalledTimes(5);

    const estado = applyService.obtenerEstadoAplicarJob('job-falla', USER._id);
    expect(estado.status).toBe('done'); // NUNCA 'error' por una fila individual
    expect(estado.procesados).toBe(5); // las 5 se cuentan como procesadas (4 ok + 1 con error)
    expect(estado.loteActual).toBe(2);
    expect(estado.resultado.creadas).toHaveLength(4); // 4 tarjetas creadas, la 5ta falló
    expect(estado.resultado.erroresFilas).toEqual([
      expect.objectContaining({ telefono: '5551111110', error: expect.stringContaining('Ya existe una tarjeta') }),
    ]);

    expect(emitToUser).toHaveBeenCalledWith('auth0|test-admin', 'lealtad-import:apply:done', expect.objectContaining({
      jobId: 'job-falla', erroresFilas: expect.arrayContaining([expect.objectContaining({ telefono: '5551111110' })]),
    }));
    expect(logger.error).toHaveBeenCalledWith(expect.stringContaining('se omite y se continúa'));
  });

  describe('obtenerEstadoAplicarJob', () => {
    test('jobId inexistente: null', () => {
      expect(applyService.obtenerEstadoAplicarJob('no-existe', USER._id)).toBeNull();
    });
  });
});

describe('construirFilasDesdeCorrecciones', () => {
  function corregida(overrides = {}) {
    return {
      tarjetaRaw: '9005',
      nombre: 'CRISTINA DIAZ GUTIERREZ',
      celular: '5559876543',
      telefono: '',
      categoriaOriginal: 'Oro',
      saldo: 5134,
      ...overrides,
    };
  }

  test('detecta tarjeta duplicada DENTRO del set de correcciones (misma normalizarTarjeta)', () => {
    const filas = applyService.construirFilasDesdeCorrecciones([
      corregida({ tarjetaRaw: '044', nombre: 'UNO', celular: '5551111111' }),
      corregida({ tarjetaRaw: '44', nombre: 'DOS', celular: '5552222222' }), // normaliza igual a '044'
    ]);

    expect(filas.every((f) => f.advertencias.includes('tarjeta_duplicada_en_clientes'))).toBe(true);
    expect(filas[0].tarjetaNormalizada).toBe('44');
    expect(filas[1].tarjetaNormalizada).toBe('44');
  });

  test('no marca duplicado cuando las tarjetas son distintas', () => {
    const filas = applyService.construirFilasDesdeCorrecciones([
      corregida({ tarjetaRaw: '1001' }),
      corregida({ tarjetaRaw: '1002' }),
    ]);

    expect(filas.every((f) => !f.advertencias.includes('tarjeta_duplicada_en_clientes'))).toBe(true);
  });

  test('resuelve nivelSugerido vía Categoria, con default Bronce + info cuando no reconoce', () => {
    const [reconocida, noReconocida] = applyService.construirFilasDesdeCorrecciones([
      corregida({ tarjetaRaw: '2001', categoriaOriginal: 'Plata' }),
      corregida({ tarjetaRaw: '2002', categoriaOriginal: 'Profesionales' }),
    ]);

    expect(reconocida.nivelSugerido).toBe('Plata');
    expect(reconocida.info).not.toContain('nivel_asignado_por_default');
    expect(noReconocida.nivelSugerido).toBe('Bronce');
    expect(noReconocida.info).toContain('nivel_asignado_por_default');
  });

  test('fila sin celular ni telefono: sin_contacto, contacto vacío', () => {
    const [fila] = applyService.construirFilasDesdeCorrecciones([
      corregida({ celular: '', telefono: '' }),
    ]);

    expect(fila.advertencias).toContain('sin_contacto');
    expect(fila.contacto).toBe('');
  });

  test('tarjetaRawPuntos siempre null y correo siempre vacío (el Excel de correcciones no los trae)', () => {
    const [fila] = applyService.construirFilasDesdeCorrecciones([corregida()]);

    expect(fila.tarjetaRawPuntos).toBeNull();
    expect(fila.correo).toBe('');
  });
});

describe('clasificarCorrecciones', () => {
  function corregida(overrides = {}) {
    return {
      tarjetaRaw: '9005',
      nombre: 'CRISTINA DIAZ GUTIERREZ',
      celular: '5559876543',
      telefono: '',
      categoriaOriginal: 'Oro',
      saldo: 5134,
      ...overrides,
    };
  }

  test('clasifica contra la DB real igual que clasificar(), sin un `resumen` de tipo concentrado', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);

    const resultado = await applyService.clasificarCorrecciones([corregida()]);

    expect(resultado.resumen).toBeUndefined();
    expect(resultado.filas[0].categoria).toBe('ok');
    expect(resultado.clasificacionResumen.ok).toBe(1);
  });

  test('duplicado dentro del set de correcciones → duplicado_bloqueante, sin consultar la DB', async () => {
    const resultado = await applyService.clasificarCorrecciones([
      corregida({ tarjetaRaw: '3569', nombre: 'UNO', celular: '5551111111' }),
      corregida({ tarjetaRaw: '3569', nombre: 'DOS', celular: '5552222222' }),
    ]);

    expect(resultado.filas.every((f) => f.categoria === 'duplicado_bloqueante')).toBe(true);
    expect(resultado.clasificacionResumen.duplicadoBloqueante).toBe(2);
    expect(LoyaltyCard.findOne).not.toHaveBeenCalled();
  });
});

describe('iniciarAplicarCorreccionesJob', () => {
  function corregida(overrides = {}) {
    return {
      tarjetaRaw: '9005',
      nombre: 'CRISTINA DIAZ GUTIERREZ',
      celular: '5559876543',
      telefono: '',
      categoriaOriginal: 'Oro',
      saldo: 5134,
      ...overrides,
    };
  }

  test('camino feliz: un lote, crea las tarjetas igual que el job principal', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);
    cardService.crearTarjeta.mockResolvedValue({ card: { id: 1 }, nip: '1234' });
    LoyaltyCard.findByPk.mockResolvedValue({ id: 1, activa: true });

    const filasCrudas = [
      corregida({ tarjetaRaw: '9005', celular: '5559876543' }),
      corregida({ tarjetaRaw: '9006', nombre: 'OTRO', celular: '5551112222' }),
    ];

    const { jobId, total, totalLotes } = await applyService.iniciarAplicarCorreccionesJob(filasCrudas, USER);

    expect(jobId).toMatch(/^lealtad-aplicar-/);
    expect(total).toBe(2);
    expect(totalLotes).toBe(1);

    await new Promise((resolve) => setImmediate(resolve));
    const estado = applyService.obtenerEstadoAplicarJob(jobId, USER._id);
    expect(estado.status).toBe('done');
    expect(estado.resultado.creadas).toHaveLength(2);
    expect(estado.resultado.excluidasBloqueantes).toBe(0);
    expect(cardService.crearTarjeta).toHaveBeenCalledTimes(2);
  });
});

describe('completarRegistroPendiente', () => {
  function makePendiente(overrides = {}) {
    const p = {
      id: 5, nombre: 'Juan', saldoLegado: '100.00', nivelSugerido: 'Bronce', estado: 'pendiente',
      ...overrides,
    };
    p.update = jest.fn(async (changes) => { Object.assign(p, changes); return p; });
    return p;
  }

  test('camino feliz: crea la tarjeta, aplica MIGRACION con el saldo legado y marca completado', async () => {
    const pendiente = makePendiente();
    LoyaltyPendingEnrollment.findByPk.mockResolvedValue(pendiente);
    cardService.crearTarjeta.mockResolvedValue({ card: { id: 10 }, nip: '4321' });
    LoyaltyCard.findByPk.mockResolvedValue({ id: 10, activa: true });

    const resultado = await applyService.completarRegistroPendiente(5, { telefono: '5551234567' }, USER);

    expect(cardService.crearTarjeta).toHaveBeenCalledWith(
      { telefono: '5551234567', nombre: 'Juan', nivel: 'Bronce' },
      USER,
      expect.objectContaining({ transaction: expect.anything() }),
    );
    expect(ledgerService._aplicarMovimiento).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ id: 10 }),
      expect.objectContaining({ tipo: 'MIGRACION', puntos: '100.00', origen: 'migracion' }),
    );
    expect(pendiente.update).toHaveBeenCalledWith(
      expect.objectContaining({ estado: 'completado', cardIdResultante: 10, resueltoPorId: '7' }),
      expect.objectContaining({ transaction: expect.anything() }),
    );
    expect(resultado).toEqual({ card: { id: 10 }, nip: '4321' });
  });

  test('rechaza con 400 si el registro ya fue completado (no crea nada)', async () => {
    LoyaltyPendingEnrollment.findByPk.mockResolvedValue(makePendiente({ estado: 'completado' }));

    await expect(
      applyService.completarRegistroPendiente(5, { telefono: '5551234567' }, USER),
    ).rejects.toMatchObject({ statusCode: 400 });

    expect(cardService.crearTarjeta).not.toHaveBeenCalled();
  });

  test('404 si el registro no existe', async () => {
    LoyaltyPendingEnrollment.findByPk.mockResolvedValue(null);

    await expect(
      applyService.completarRegistroPendiente(999, { telefono: '5551234567' }, USER),
    ).rejects.toMatchObject({ statusCode: 404 });
  });
});

describe('rechazarRegistroPendiente', () => {
  function makePendiente(overrides = {}) {
    const p = { id: 5, nombre: 'Juan', estado: 'pendiente', ...overrides };
    p.update = jest.fn(async (changes) => { Object.assign(p, changes); return p; });
    return p;
  }

  test('camino feliz: marca "rechazado" y registra quién/cuándo, sin crear ninguna tarjeta', async () => {
    const pendiente = makePendiente();
    LoyaltyPendingEnrollment.findByPk.mockResolvedValue(pendiente);

    const resultado = await applyService.rechazarRegistroPendiente(5, USER);

    expect(pendiente.update).toHaveBeenCalledWith(expect.objectContaining({
      estado: 'rechazado', resueltoPorId: '7', resueltoPorNombre: 'Admin',
    }));
    expect(cardService.crearTarjeta).not.toHaveBeenCalled();
    expect(resultado.estado).toBe('rechazado');
  });

  test('400 si el registro ya fue resuelto (completado o rechazado)', async () => {
    LoyaltyPendingEnrollment.findByPk.mockResolvedValue(makePendiente({ estado: 'completado' }));

    await expect(applyService.rechazarRegistroPendiente(5, USER)).rejects.toMatchObject({ statusCode: 400 });
  });

  test('404 si el registro no existe', async () => {
    LoyaltyPendingEnrollment.findByPk.mockResolvedValue(null);

    await expect(applyService.rechazarRegistroPendiente(999, USER)).rejects.toMatchObject({ statusCode: 404 });
  });
});

describe('rechazarTodosPendientes', () => {
  test('un solo UPDATE masivo sobre estado:pendiente, sin crear nada', async () => {
    LoyaltyPendingEnrollment.update.mockResolvedValue([37]);

    const resultado = await applyService.rechazarTodosPendientes(USER);

    expect(LoyaltyPendingEnrollment.update).toHaveBeenCalledWith(
      expect.objectContaining({ estado: 'rechazado', resueltoPorId: '7' }),
      { where: { estado: 'pendiente' } },
    );
    expect(resultado).toEqual({ rechazados: 37 });
    expect(cardService.crearTarjeta).not.toHaveBeenCalled();
  });
});

describe('listarPendientes', () => {
  test('pagina con el mismo contrato que loyalty-card.service.js#listar', async () => {
    LoyaltyPendingEnrollment.findAndCountAll.mockResolvedValue({ count: 3, rows: [{ id: 1 }] });

    const resultado = await applyService.listarPendientes({ page: 1, limit: 2 });

    expect(LoyaltyPendingEnrollment.findAndCountAll).toHaveBeenCalledWith(
      expect.objectContaining({ where: { estado: 'pendiente' }, limit: 2, offset: 0 }),
    );
    expect(resultado).toEqual({ total: 3, page: 1, limit: 2, pages: 2, pendientes: [{ id: 1 }] });
  });
});

describe('Transaction.LOCK import (sanity)', () => {
  test('la lib de sequelize expone LOCK.UPDATE (usado internamente por el servicio)', () => {
    expect(Transaction.LOCK.UPDATE).toBeDefined();
  });
});
