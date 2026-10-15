'use strict';

/**
 * domains/config/loyalty-config.service.test.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad (PR3) — dominio de configuración: % por nivel (envoltorio
 * sobre `global-config.service.setValue/getValue`, D10/D11), catálogo de
 * motivos de ajuste y flags de sucursal (`participaLealtad`/`permiteCanjeLealtad`,
 * ya agregados a `CentroCosto` en PR1).
 *
 * `global-config.service` se mockea completo (mismo patrón que
 * `loyalty-ledger.service.test.js`) — este archivo NO repite la cobertura de
 * `global-config.service.test.js` (cifrado, caché, auditoría, emit de
 * `config:updated`, ya probados ahí contra Postgres real). Acá solo se
 * verifica el envoltorio: que esta capa llama a `setValue`/`getValue` con la
 * sección/clave/tipo correctos y que la validación de rango/formato del %
 * ocurre ANTES de llamar a `setValue`.
 */

jest.mock('../../../shared/models/postgres', () => ({
  LoyaltyAdjustmentReason: { findAll: jest.fn(), findByPk: jest.fn(), create: jest.fn() },
  CentroCosto:             { findAll: jest.fn(), findByPk: jest.fn() },
}));
jest.mock('../../../shared/services/global-config.service', () => ({
  getValue: jest.fn(),
  setValue: jest.fn(),
}));

const { LoyaltyAdjustmentReason, CentroCosto } = require('../../../shared/models/postgres');
const globalConfigService = require('../../../shared/services/global-config.service');
const configService = require('./loyalty-config.service');

const USER = { dbId: 1, nombre: 'Admin' };

beforeEach(() => {
  jest.clearAllMocks();
});

describe('guardarPorcentajes', () => {
  test('llama a setValue(\'lealtad\', PORCENTAJE_<NIVEL>, valor, {tipo:"numero"}) por cada nivel enviado', async () => {
    await configService.guardarPorcentajes({ Bronce: '1.5', Plata: '3', Oro: '5.25' }, USER);

    expect(globalConfigService.setValue).toHaveBeenCalledWith(
      'lealtad', 'PORCENTAJE_BRONCE', '1.5',
      expect.objectContaining({ tipo: 'numero', usuarioId: '1', usuarioNombre: 'Admin' }),
    );
    expect(globalConfigService.setValue).toHaveBeenCalledWith(
      'lealtad', 'PORCENTAJE_PLATA', '3',
      expect.objectContaining({ tipo: 'numero' }),
    );
    expect(globalConfigService.setValue).toHaveBeenCalledWith(
      'lealtad', 'PORCENTAJE_ORO', '5.25',
      expect.objectContaining({ tipo: 'numero' }),
    );
    expect(globalConfigService.setValue).toHaveBeenCalledTimes(3);
  });

  test('solo escribe los niveles presentes en el payload (parcial)', async () => {
    await configService.guardarPorcentajes({ Plata: '4' }, USER);

    expect(globalConfigService.setValue).toHaveBeenCalledTimes(1);
    expect(globalConfigService.setValue).toHaveBeenCalledWith(
      'lealtad', 'PORCENTAJE_PLATA', '4', expect.anything(),
    );
  });

  test.each([
    ['0', 'cero'],
    ['-1', 'negativo'],
    ['100.01', 'mayor a 100'],
    ['101', 'mayor a 100'],
    ['3.456', 'más de 2 decimales'],
    ['abc', 'no numérico'],
  ])('% fuera de rango/formato inválido (%s, %s) → 400, no llama a setValue', async (valor) => {
    await expect(
      configService.guardarPorcentajes({ Bronce: valor }, USER),
    ).rejects.toMatchObject({ statusCode: 400 });

    expect(globalConfigService.setValue).not.toHaveBeenCalled();
  });

  test('100 y 0.01 son válidos (límites inclusivos/positivos)', async () => {
    await configService.guardarPorcentajes({ Bronce: '100', Plata: '0.01' }, USER);
    expect(globalConfigService.setValue).toHaveBeenCalledTimes(2);
  });
});

describe('obtenerPorcentajes', () => {
  test('devuelve null para el nivel cuando getValue tira (D11: no hay default en 0)', async () => {
    globalConfigService.getValue.mockImplementation(async (section, clave) => {
      if (clave === 'PORCENTAJE_BRONCE') return '2.5';
      throw new Error('no existe');
    });

    const resultado = await configService.obtenerPorcentajes();

    expect(resultado).toEqual({ Bronce: 2.5, Plata: null, Oro: null });
  });
});

describe('listarReasons', () => {
  test('soloActivos filtra por activo=true (para el selector de un nuevo AJUSTE)', async () => {
    LoyaltyAdjustmentReason.findAll.mockResolvedValue([]);

    await configService.listarReasons({ soloActivos: true });

    expect(LoyaltyAdjustmentReason.findAll).toHaveBeenCalledWith(
      expect.objectContaining({ where: { activo: true } }),
    );
  });

  test('sin soloActivos trae todos (para la pantalla de catálogo, incluye inactivos)', async () => {
    LoyaltyAdjustmentReason.findAll.mockResolvedValue([]);

    await configService.listarReasons();

    expect(LoyaltyAdjustmentReason.findAll).toHaveBeenCalledWith(
      expect.objectContaining({ where: {} }),
    );
  });
});

describe('crearReason', () => {
  test('signoPermitido inválido → 400, no inserta', async () => {
    await expect(
      configService.crearReason({ clave: 'x', nombre: 'X', signoPermitido: 'MAS' }),
    ).rejects.toMatchObject({ statusCode: 400 });

    expect(LoyaltyAdjustmentReason.create).not.toHaveBeenCalled();
  });

  test('signoPermitido válido → crea con activo=true', async () => {
    LoyaltyAdjustmentReason.create.mockResolvedValue({ id: 1 });

    await configService.crearReason({ clave: 'bono', nombre: 'Bono', signoPermitido: 'POSITIVO' });

    expect(LoyaltyAdjustmentReason.create).toHaveBeenCalledWith({
      clave: 'bono', nombre: 'Bono', signoPermitido: 'POSITIVO', activo: true,
    });
  });
});

describe('editarReason', () => {
  function makeReason(overrides = {}) {
    const reason = { id: 1, clave: 'bono', nombre: 'Bono', signoPermitido: 'POSITIVO', activo: true, ...overrides };
    reason.update = jest.fn(async (changes) => { Object.assign(reason, changes); return reason; });
    return reason;
  }

  test('404 si el motivo no existe', async () => {
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue(null);

    await expect(configService.editarReason(999, { activo: false })).rejects.toMatchObject({ statusCode: 404 });
  });

  test('desactivar (activo:false) solo actualiza el flag — nunca borra la fila (historial intacto)', async () => {
    const reason = makeReason();
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue(reason);

    const resultado = await configService.editarReason(1, { activo: false });

    expect(reason.update).toHaveBeenCalledWith({ activo: false });
    expect(resultado.activo).toBe(false);
    expect(LoyaltyAdjustmentReason.create).not.toHaveBeenCalled();
  });

  test('signoPermitido inválido en edición → 400, no actualiza', async () => {
    const reason = makeReason();
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue(reason);

    await expect(
      configService.editarReason(1, { signoPermitido: 'RARO' }),
    ).rejects.toMatchObject({ statusCode: 400 });
    expect(reason.update).not.toHaveBeenCalled();
  });
});

describe('listarSucursales', () => {
  test('incluye ambos flags de lealtad en la proyección', async () => {
    CentroCosto.findAll.mockResolvedValue([]);

    await configService.listarSucursales();

    expect(CentroCosto.findAll).toHaveBeenCalledWith(
      expect.objectContaining({
        attributes: expect.arrayContaining(['participaLealtad', 'permiteCanjeLealtad']),
      }),
    );
  });
});

describe('actualizarSucursal', () => {
  function makeCentro(overrides = {}) {
    const centro = { id: 10, clave: 'D0', participaLealtad: true, permiteCanjeLealtad: true, ...overrides };
    centro.update = jest.fn(async (changes) => { Object.assign(centro, changes); return centro; });
    return centro;
  }

  test('404 si el centro de costo no existe', async () => {
    CentroCosto.findByPk.mockResolvedValue(null);

    await expect(configService.actualizarSucursal(999, { participaLealtad: false })).rejects.toMatchObject({ statusCode: 404 });
  });

  test('actualiza participaLealtad=false dejando permiteCanjeLealtad intacto (independientes)', async () => {
    const centro = makeCentro();
    CentroCosto.findByPk.mockResolvedValue(centro);

    const resultado = await configService.actualizarSucursal(10, { participaLealtad: false });

    expect(centro.update).toHaveBeenCalledWith({ participaLealtad: false });
    expect(resultado.participaLealtad).toBe(false);
    expect(resultado.permiteCanjeLealtad).toBe(true);
  });

  test('actualiza permiteCanjeLealtad=false dejando participaLealtad intacto (independientes, sentido inverso)', async () => {
    const centro = makeCentro();
    CentroCosto.findByPk.mockResolvedValue(centro);

    const resultado = await configService.actualizarSucursal(10, { permiteCanjeLealtad: false });

    expect(centro.update).toHaveBeenCalledWith({ permiteCanjeLealtad: false });
    expect(resultado.permiteCanjeLealtad).toBe(false);
    expect(resultado.participaLealtad).toBe(true);
  });

  test('ambos flags a la vez', async () => {
    const centro = makeCentro();
    CentroCosto.findByPk.mockResolvedValue(centro);

    await configService.actualizarSucursal(10, { participaLealtad: false, permiteCanjeLealtad: false });

    expect(centro.update).toHaveBeenCalledWith({ participaLealtad: false, permiteCanjeLealtad: false });
  });
});
