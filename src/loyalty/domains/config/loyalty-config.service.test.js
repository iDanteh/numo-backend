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
  LoyaltyPromotion:        { findAll: jest.fn(), findByPk: jest.fn(), create: jest.fn(), findOne: jest.fn() },
  CentroCosto:             { findAll: jest.fn(), findByPk: jest.fn() },
}));
jest.mock('../../../shared/services/global-config.service', () => ({
  getValue: jest.fn(),
  setValue: jest.fn(),
}));

const { LoyaltyAdjustmentReason, LoyaltyPromotion, CentroCosto } = require('../../../shared/models/postgres');
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

  test('signoPermitido válido → crea con activo=true, porcentaje:null (sin % en el payload)', async () => {
    LoyaltyAdjustmentReason.create.mockResolvedValue({ id: 1 });

    await configService.crearReason({ clave: 'bono', nombre: 'Bono', signoPermitido: 'POSITIVO' });

    expect(LoyaltyAdjustmentReason.create).toHaveBeenCalledWith({
      clave: 'bono', nombre: 'Bono', signoPermitido: 'POSITIVO', activo: true, porcentaje: null,
    });
  });

  test('porcentaje + signoPermitido AMBOS → 400, no inserta (el signo del % automático no puede ser ambiguo)', async () => {
    await expect(
      configService.crearReason({ clave: 'comision', nombre: 'Comisión', signoPermitido: 'AMBOS', porcentaje: '5' }),
    ).rejects.toMatchObject({ statusCode: 400 });

    expect(LoyaltyAdjustmentReason.create).not.toHaveBeenCalled();
  });

  test.each([
    ['0'], ['-1'], ['100.01'], ['101'], ['3.456'], ['abc'],
  ])('porcentaje con formato/rango inválido (%s) → 400, no inserta', async (valor) => {
    await expect(
      configService.crearReason({ clave: 'comision', nombre: 'Comisión', signoPermitido: 'POSITIVO', porcentaje: valor }),
    ).rejects.toMatchObject({ statusCode: 400 });

    expect(LoyaltyAdjustmentReason.create).not.toHaveBeenCalled();
  });

  test('porcentaje válido + signo POSITIVO → crea con el % guardado', async () => {
    LoyaltyAdjustmentReason.create.mockResolvedValue({ id: 5 });

    await configService.crearReason({ clave: 'comision', nombre: 'Comisión', signoPermitido: 'POSITIVO', porcentaje: '7.5' });

    expect(LoyaltyAdjustmentReason.create).toHaveBeenCalledWith({
      clave: 'comision', nombre: 'Comisión', signoPermitido: 'POSITIVO', activo: true, porcentaje: '7.5',
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

  test('porcentaje con signoPermitido AMBOS ya guardado (no viene en el payload) → 400', async () => {
    const reason = makeReason({ signoPermitido: 'AMBOS' });
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue(reason);

    await expect(
      configService.editarReason(1, { porcentaje: '5' }),
    ).rejects.toMatchObject({ statusCode: 400 });
    expect(reason.update).not.toHaveBeenCalled();
  });

  test('porcentaje válido junto con signoPermitido POSITIVO en el mismo payload → actualiza ambos', async () => {
    const reason = makeReason({ signoPermitido: 'AMBOS' });
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue(reason);

    const resultado = await configService.editarReason(1, { porcentaje: '5', signoPermitido: 'POSITIVO' });

    expect(reason.update).toHaveBeenCalledWith({ signoPermitido: 'POSITIVO', porcentaje: '5' });
    expect(resultado.porcentaje).toBe('5');
  });

  test('porcentaje: null explícito quita el % (vuelve el motivo manual)', async () => {
    const reason = makeReason({ porcentaje: '5.00' });
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue(reason);

    await configService.editarReason(1, { porcentaje: null });

    expect(reason.update).toHaveBeenCalledWith({ porcentaje: null });
  });

  test('porcentaje inválido en edición → 400, no actualiza', async () => {
    const reason = makeReason();
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue(reason);

    await expect(
      configService.editarReason(1, { porcentaje: '200' }),
    ).rejects.toMatchObject({ statusCode: 400 });
    expect(reason.update).not.toHaveBeenCalled();
  });
});

describe('listarPromociones / crearPromocion / editarPromocion / obtenerPromocionVigente', () => {
  function makePromo(overrides = {}) {
    const promo = {
      id: 1, nombre: 'Buen Fin', fechaInicio: '2026-11-01', fechaFin: '2026-11-17',
      porcentajeBono: '5.00', activa: true, ...overrides,
    };
    promo.update = jest.fn(async (changes) => { Object.assign(promo, changes); return promo; });
    return promo;
  }

  test('listarPromociones soloActivas filtra por activa:true', async () => {
    LoyaltyPromotion.findAll.mockResolvedValue([]);

    await configService.listarPromociones({ soloActivas: true });

    expect(LoyaltyPromotion.findAll).toHaveBeenCalledWith(expect.objectContaining({ where: { activa: true } }));
  });

  test('listarPromociones sin soloActivas trae todas (incluye inactivas, para histórico)', async () => {
    LoyaltyPromotion.findAll.mockResolvedValue([]);

    await configService.listarPromociones();

    expect(LoyaltyPromotion.findAll).toHaveBeenCalledWith(expect.objectContaining({ where: {} }));
  });

  test('crearPromocion: nombre vacío → 400, no inserta', async () => {
    await expect(
      configService.crearPromocion({ nombre: '   ', fechaInicio: '2026-11-01', fechaFin: '2026-11-17', porcentajeBono: '5' }),
    ).rejects.toMatchObject({ statusCode: 400 });
    expect(LoyaltyPromotion.create).not.toHaveBeenCalled();
  });

  test('crearPromocion: fechaFin < fechaInicio → 400, no inserta', async () => {
    await expect(
      configService.crearPromocion({ nombre: 'Buen Fin', fechaInicio: '2026-11-17', fechaFin: '2026-11-01', porcentajeBono: '5' }),
    ).rejects.toMatchObject({ statusCode: 400 });
    expect(LoyaltyPromotion.create).not.toHaveBeenCalled();
  });

  test.each([
    ['0'], ['-1'], ['100.01'], ['abc'],
  ])('crearPromocion: porcentajeBono inválido (%s) → 400, no inserta', async (valor) => {
    await expect(
      configService.crearPromocion({ nombre: 'Buen Fin', fechaInicio: '2026-11-01', fechaFin: '2026-11-17', porcentajeBono: valor }),
    ).rejects.toMatchObject({ statusCode: 400 });
    expect(LoyaltyPromotion.create).not.toHaveBeenCalled();
  });

  test('crearPromocion válida → crea con activa:true', async () => {
    LoyaltyPromotion.create.mockResolvedValue({ id: 1 });

    await configService.crearPromocion({ nombre: 'Buen Fin', fechaInicio: '2026-11-01', fechaFin: '2026-11-17', porcentajeBono: '5' });

    expect(LoyaltyPromotion.create).toHaveBeenCalledWith({
      nombre: 'Buen Fin', fechaInicio: '2026-11-01', fechaFin: '2026-11-17', porcentajeBono: '5', activa: true,
    });
  });

  test('editarPromocion: 404 si no existe', async () => {
    LoyaltyPromotion.findByPk.mockResolvedValue(null);

    await expect(configService.editarPromocion(999, { activa: false })).rejects.toMatchObject({ statusCode: 404 });
  });

  test('editarPromocion: cambiar solo fechaFin valida contra la fechaInicio ya guardada', async () => {
    const promo = makePromo({ fechaInicio: '2026-11-01', fechaFin: '2026-11-17' });
    LoyaltyPromotion.findByPk.mockResolvedValue(promo);

    await expect(
      configService.editarPromocion(1, { fechaFin: '2026-10-01' }),
    ).rejects.toMatchObject({ statusCode: 400 });
    expect(promo.update).not.toHaveBeenCalled();
  });

  test('editarPromocion: cambiar solo fechaInicio valida contra la fechaFin ya guardada', async () => {
    const promo = makePromo({ fechaInicio: '2026-11-01', fechaFin: '2026-11-17' });
    LoyaltyPromotion.findByPk.mockResolvedValue(promo);

    await expect(
      configService.editarPromocion(1, { fechaInicio: '2026-12-01' }),
    ).rejects.toMatchObject({ statusCode: 400 });
    expect(promo.update).not.toHaveBeenCalled();
  });

  test('editarPromocion: activa:false solo cambia el flag (nunca borra la fila)', async () => {
    const promo = makePromo();
    LoyaltyPromotion.findByPk.mockResolvedValue(promo);

    const resultado = await configService.editarPromocion(1, { activa: false });

    expect(promo.update).toHaveBeenCalledWith({ activa: false });
    expect(resultado.activa).toBe(false);
  });

  test('obtenerPromocionVigente: sin promos → null', async () => {
    LoyaltyPromotion.findOne.mockResolvedValue(null);

    const resultado = await configService.obtenerPromocionVigente(new Date('2026-11-05'));

    expect(resultado).toBeNull();
  });

  test('obtenerPromocionVigente: filtra por rango activa y ordena por % descendente (gana la mayor si hay solape)', async () => {
    LoyaltyPromotion.findOne.mockResolvedValue(makePromo());

    const resultado = await configService.obtenerPromocionVigente(new Date('2026-11-05'));

    expect(resultado.nombre).toBe('Buen Fin');
    expect(LoyaltyPromotion.findOne).toHaveBeenCalledWith(expect.objectContaining({
      where: expect.objectContaining({ activa: true }),
      order: [['porcentajeBono', 'DESC']],
    }));
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

  test('incluye serieFacturacion (2026-10-19, pedido explícito del usuario — "Sucursal → CEDIS (A0)")', async () => {
    CentroCosto.findAll.mockResolvedValue([]);

    await configService.listarSucursales();

    expect(CentroCosto.findAll).toHaveBeenCalledWith(
      expect.objectContaining({
        attributes: expect.arrayContaining(['serieFacturacion']),
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
