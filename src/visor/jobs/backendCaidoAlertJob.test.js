'use strict';

jest.mock('node-cron', () => ({ schedule: jest.fn() }));
jest.mock('../../shared/utils/logger', () => ({ logger: { warn: jest.fn(), error: jest.fn(), info: jest.fn() } }));
jest.mock('../../shared/services/email.service', () => ({ enviarCorreo: jest.fn() }));

const mockColeccion = { findOne: jest.fn(), updateOne: jest.fn() };
jest.mock('mongoose', () => ({ connection: { db: { collection: () => mockColeccion } } }));

const emailSvc = require('../../shared/services/email.service');
const { revisarCaidaAlArrancar } = require('./backendCaidoAlertJob');

const haceMinutos = m => new Date(Date.now() - m * 60000);

describe('revisarCaidaAlArrancar', () => {
  const envOriginal = { ...process.env };

  beforeEach(() => {
    jest.clearAllMocks();
    process.env.NODE_ENV = 'production';
    delete process.env.BACKEND_CAIDO_ALERTA;
    mockColeccion.updateOne.mockResolvedValue({});
    emailSvc.enviarCorreo.mockResolvedValue(true);
  });
  afterAll(() => { process.env = envOriginal; });

  test('primer arranque (sin latido previo): no avisa, deja el latido', async () => {
    mockColeccion.findOne.mockResolvedValue(null);
    await revisarCaidaAlArrancar();
    expect(emailSvc.enviarCorreo).not.toHaveBeenCalled();
    expect(mockColeccion.updateOne).toHaveBeenCalledTimes(1);
  });

  test('reinicio normal (latido de hace 3 min): no avisa', async () => {
    mockColeccion.findOne.mockResolvedValue({ ultimoLatido: haceMinutos(3) });
    await revisarCaidaAlArrancar();
    expect(emailSvc.enviarCorreo).not.toHaveBeenCalled();
  });

  test('caído 2.6 días: avisa a los destinatarios técnicos', async () => {
    mockColeccion.findOne.mockResolvedValue({ ultimoLatido: haceMinutos(2.6 * 24 * 60) });
    await revisarCaidaAlArrancar();
    expect(emailSvc.enviarCorreo).toHaveBeenCalledTimes(1);
    const { to, subject } = emailSvc.enviarCorreo.mock.calls[0][0];
    expect(to).toEqual(['programador6@tubosyconexiones.mx', 'sistemas4@tubosyconexiones.mx']);
    expect(subject).toContain('2.6 día(s)');
  });

  test('fuera de producción no hace nada', async () => {
    process.env.NODE_ENV = 'development';
    mockColeccion.findOne.mockResolvedValue({ ultimoLatido: haceMinutos(5000) });
    await revisarCaidaAlArrancar();
    expect(mockColeccion.findOne).not.toHaveBeenCalled();
    expect(emailSvc.enviarCorreo).not.toHaveBeenCalled();
  });

  test('BACKEND_CAIDO_ALERTA=false lo apaga', async () => {
    process.env.BACKEND_CAIDO_ALERTA = 'false';
    mockColeccion.findOne.mockResolvedValue({ ultimoLatido: haceMinutos(5000) });
    await revisarCaidaAlArrancar();
    expect(emailSvc.enviarCorreo).not.toHaveBeenCalled();
  });

  test('si Mongo falla no lanza', async () => {
    mockColeccion.findOne.mockRejectedValue(new Error('sin conexión'));
    await expect(revisarCaidaAlArrancar()).resolves.toBeUndefined();
  });
});
