'use strict';

jest.mock('../shared/utils/logger', () => ({ logger: { warn: jest.fn(), error: jest.fn() } }));
jest.mock('../shared/services/email.service', () => ({ enviarCorreo: jest.fn() }));
jest.mock('../shared/services/global-config.service', () => ({ getValue: jest.fn() }));

const { logger } = require('../shared/utils/logger');
const emailSvc = require('../shared/services/email.service');
const globalConfigSvc = require('../shared/services/global-config.service');
const { procesarAlerta, _resetParaTests } = require('./system-monitor-alerta.job');

function snapshotFixture(overrides = {}) {
  return {
    generadoEn: '2026-09-24T18:00:00.000Z',
    estadoGeneral: 'normal',
    tasaErrorPct: 0,
    eventLoopLagMs: 5,
    salud: { mongo: 'conectado', postgres: 'conectado' },
    cpu: { load1: 0.5, load5: 0.4, load15: 0.3, cores: 4 },
    ...overrides,
  };
}

describe('procesarAlerta', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    _resetParaTests();
    globalConfigSvc.getValue.mockResolvedValue('admin@numo.mx, otro@numo.mx');
    emailSvc.enviarCorreo.mockResolvedValue(true);
  });

  test('normal → normal: no manda correo', async () => {
    await procesarAlerta(snapshotFixture({ estadoGeneral: 'normal' }));
    expect(emailSvc.enviarCorreo).not.toHaveBeenCalled();
  });

  test('normal → degradado: correo INMEDIATO', async () => {
    await procesarAlerta(snapshotFixture({ estadoGeneral: 'degradado' }));

    expect(emailSvc.enviarCorreo).toHaveBeenCalledTimes(1);
    const { to, subject } = emailSvc.enviarCorreo.mock.calls[0][0];
    expect(to).toEqual(['admin@numo.mx', 'otro@numo.mx']);
    expect(subject).toContain('DEGRADADO');
  });

  test('normal → caido: correo inmediato, distinto asunto que degradado', async () => {
    await procesarAlerta(snapshotFixture({ estadoGeneral: 'caido' }));
    expect(emailSvc.enviarCorreo.mock.calls[0][0].subject).toContain('CAIDO');
  });

  test('se mantiene degradado: NO manda un correo por cada tick, solo cada 15min', async () => {
    const base = Date.parse('2026-09-24T18:00:00.000Z');
    const nowSpy = jest.spyOn(Date, 'now');

    nowSpy.mockReturnValue(base);
    await procesarAlerta(snapshotFixture({ estadoGeneral: 'degradado' })); // transición → envía
    expect(emailSvc.enviarCorreo).toHaveBeenCalledTimes(1);

    nowSpy.mockReturnValue(base + 5 * 60 * 1000); // 5 min después — todavía no
    await procesarAlerta(snapshotFixture({ estadoGeneral: 'degradado' }));
    expect(emailSvc.enviarCorreo).toHaveBeenCalledTimes(1);

    nowSpy.mockReturnValue(base + 14 * 60 * 1000); // 14 min — todavía no (falta 1 min)
    await procesarAlerta(snapshotFixture({ estadoGeneral: 'degradado' }));
    expect(emailSvc.enviarCorreo).toHaveBeenCalledTimes(1);

    nowSpy.mockReturnValue(base + 15 * 60 * 1000); // 15 min exactos — recordatorio
    await procesarAlerta(snapshotFixture({ estadoGeneral: 'degradado' }));
    expect(emailSvc.enviarCorreo).toHaveBeenCalledTimes(2);
    expect(emailSvc.enviarCorreo.mock.calls[1][0].subject).toContain('recordatorio');

    nowSpy.mockRestore();
  });

  test('degradado → normal: correo de "recuperado"', async () => {
    await procesarAlerta(snapshotFixture({ estadoGeneral: 'degradado' }));
    await procesarAlerta(snapshotFixture({ estadoGeneral: 'normal' }));

    expect(emailSvc.enviarCorreo).toHaveBeenCalledTimes(2);
    expect(emailSvc.enviarCorreo.mock.calls[1][0].subject).toContain('recuperado');
  });

  test('sin emails-alerta configurado (getValue tira, sección/clave no existe): loguea warning, NO manda correo, no tira', async () => {
    globalConfigSvc.getValue.mockRejectedValue(new Error("No existe la configuración 'system-monitor.emails-alerta'"));

    await expect(procesarAlerta(snapshotFixture({ estadoGeneral: 'degradado' }))).resolves.not.toThrow();

    expect(emailSvc.enviarCorreo).not.toHaveBeenCalled();
    expect(logger.warn).toHaveBeenCalledTimes(1);
  });

  test('emails-alerta configurado pero vacío: mismo criterio, se omite sin tirar', async () => {
    globalConfigSvc.getValue.mockResolvedValue('');

    await procesarAlerta(snapshotFixture({ estadoGeneral: 'degradado' }));

    expect(emailSvc.enviarCorreo).not.toHaveBeenCalled();
    expect(logger.warn).toHaveBeenCalledTimes(1);
  });

  test('si enviarCorreo falla, NO se marca como enviado — el próximo tick reintenta antes de los 15min', async () => {
    const base = Date.parse('2026-09-24T18:00:00.000Z');
    const nowSpy = jest.spyOn(Date, 'now').mockReturnValue(base);
    emailSvc.enviarCorreo.mockResolvedValueOnce(false); // falla el envío inicial

    await procesarAlerta(snapshotFixture({ estadoGeneral: 'degradado' }));
    expect(emailSvc.enviarCorreo).toHaveBeenCalledTimes(1);

    nowSpy.mockReturnValue(base + 60 * 1000); // apenas 1 min después — no 15
    await procesarAlerta(snapshotFixture({ estadoGeneral: 'degradado' }));
    expect(emailSvc.enviarCorreo).toHaveBeenCalledTimes(2); // reintenta igual, porque el anterior no se contó como enviado

    nowSpy.mockRestore();
  });

  test('el asunto/detalle menciona qué está fallando (Mongo/tasa de error/lag/CPU)', async () => {
    await procesarAlerta(snapshotFixture({
      estadoGeneral: 'caido',
      salud: { mongo: 'desconectado', postgres: 'conectado' },
    }));

    const { html } = emailSvc.enviarCorreo.mock.calls[0][0];
    expect(html).toContain('MongoDB desconectado');
  });
});
