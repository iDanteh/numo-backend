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
  LoyaltyKoreConcepto:     { findAll: jest.fn(), findByPk: jest.fn(), findOrCreate: jest.fn() },
  LoyaltyKoreFormaPago:    { findAll: jest.fn(), findByPk: jest.fn(), findOrCreate: jest.fn(), update: jest.fn() },
  ConfigSection:           { findOne: jest.fn() },
  GlobalConfig:            { findOne: jest.fn() },
}));
jest.mock('../../../shared/services/global-config.service', () => ({
  getValue: jest.fn(),
  setValue: jest.fn(),
  listAuditLog: jest.fn(),
}));
jest.mock('../../../config/database.postgres', () => ({
  sequelize: { transaction: jest.fn() },
}));
jest.mock('./loyalty-kore-catalogos.client', () => ({
  obtenerConceptosKore: jest.fn(),
  obtenerFormasPagoKore: jest.fn(),
}));

const {
  LoyaltyAdjustmentReason, LoyaltyPromotion, CentroCosto,
  LoyaltyKoreConcepto, LoyaltyKoreFormaPago,
  ConfigSection, GlobalConfig,
} = require('../../../shared/models/postgres');
const globalConfigService = require('../../../shared/services/global-config.service');
const { sequelize } = require('../../../config/database.postgres');
const koreCatalogosClient = require('./loyalty-kore-catalogos.client');
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

describe('obtenerFotoFolderId / guardarFotoFolderId', () => {
  test('obtenerFotoFolderId lee lealtad.FOTO_CLIENTE_FOLDER_ID y recorta espacios', async () => {
    globalConfigService.getValue.mockResolvedValue('  abc123  ');
    const valor = await configService.obtenerFotoFolderId();
    expect(globalConfigService.getValue).toHaveBeenCalledWith('lealtad', 'FOTO_CLIENTE_FOLDER_ID');
    expect(valor).toBe('abc123');
  });

  test('obtenerFotoFolderId devuelve string vacío si la clave no existe (sin fallback inventado)', async () => {
    globalConfigService.getValue.mockRejectedValue(new Error('no existe'));
    await expect(configService.obtenerFotoFolderId()).resolves.toBe('');
  });

  test('guardarFotoFolderId llama a setValue con tipo:"texto" y recorta espacios', async () => {
    const resultado = await configService.guardarFotoFolderId('  nuevo-id  ', USER);
    expect(globalConfigService.setValue).toHaveBeenCalledWith(
      'lealtad', 'FOTO_CLIENTE_FOLDER_ID', 'nuevo-id',
      expect.objectContaining({ tipo: 'texto', usuarioId: '1', usuarioNombre: 'Admin' }),
    );
    expect(resultado).toBe('nuevo-id');
  });

  test('guardarFotoFolderId acepta vacío ("no configurado")', async () => {
    const resultado = await configService.guardarFotoFolderId('   ', USER);
    expect(globalConfigService.setValue).toHaveBeenCalledWith(
      'lealtad', 'FOTO_CLIENTE_FOLDER_ID', '', expect.anything(),
    );
    expect(resultado).toBe('');
  });
});

describe('obtenerHistorial', () => {
  test('resuelve sección lealtad + clave a la fila de GlobalConfig y delega en listAuditLog(configId)', async () => {
    ConfigSection.findOne.mockResolvedValue({ id: 7, clave: 'lealtad' });
    GlobalConfig.findOne.mockResolvedValue({ id: 42, clave: 'PORCENTAJE_BRONCE' });
    const logs = [{ id: 1, accion: 'editado' }];
    globalConfigService.listAuditLog.mockResolvedValue(logs);

    const resultado = await configService.obtenerHistorial('PORCENTAJE_BRONCE');

    expect(ConfigSection.findOne).toHaveBeenCalledWith({ where: { clave: 'lealtad' } });
    expect(GlobalConfig.findOne).toHaveBeenCalledWith({ where: { sectionId: 7, clave: 'PORCENTAJE_BRONCE' } });
    expect(globalConfigService.listAuditLog).toHaveBeenCalledWith(42);
    expect(resultado).toBe(logs);
  });

  test('clave nunca guardada (sin fila en GlobalConfig todavía) → array vacío, no tira error', async () => {
    ConfigSection.findOne.mockResolvedValue({ id: 7, clave: 'lealtad' });
    GlobalConfig.findOne.mockResolvedValue(null);

    const resultado = await configService.obtenerHistorial('FOTO_CLIENTE_FOLDER_ID');

    expect(resultado).toEqual([]);
    expect(globalConfigService.listAuditLog).not.toHaveBeenCalled();
  });

  test('sección lealtad inexistente en el catálogo (caso defensivo) → array vacío', async () => {
    ConfigSection.findOne.mockResolvedValue(null);

    const resultado = await configService.obtenerHistorial('ESPECIALIDADES');

    expect(resultado).toEqual([]);
    expect(GlobalConfig.findOne).not.toHaveBeenCalled();
    expect(globalConfigService.listAuditLog).not.toHaveBeenCalled();
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
  test('nombre vacío → 400, no inserta', async () => {
    await expect(
      configService.crearReason({ clave: 'x', nombre: '   ' }),
    ).rejects.toMatchObject({ statusCode: 400 });

    expect(LoyaltyAdjustmentReason.create).not.toHaveBeenCalled();
  });

  test('sin puntosFijos → crea en modo manual (puntosFijos: null)', async () => {
    LoyaltyAdjustmentReason.create.mockResolvedValue({ id: 1 });

    await configService.crearReason({ clave: 'correccion', nombre: 'Corrección' });

    expect(LoyaltyAdjustmentReason.create).toHaveBeenCalledWith({
      clave: 'correccion', nombre: 'Corrección', activo: true, puntosFijos: null,
    });
  });

  test.each([
    [0], ['abc'], [5.5],
  ])('puntosFijos inválido (%s) → 400, no inserta', async (valor) => {
    await expect(
      configService.crearReason({ clave: 'bono', nombre: 'Bono', puntosFijos: valor }),
    ).rejects.toMatchObject({ statusCode: 400 });

    expect(LoyaltyAdjustmentReason.create).not.toHaveBeenCalled();
  });

  test('puntosFijos positivo → crea el motivo con ese valor (ej. "Bono" = 100)', async () => {
    LoyaltyAdjustmentReason.create.mockResolvedValue({ id: 5 });

    await configService.crearReason({ clave: 'bono', nombre: 'Bono', puntosFijos: 100 });

    expect(LoyaltyAdjustmentReason.create).toHaveBeenCalledWith({
      clave: 'bono', nombre: 'Bono', activo: true, puntosFijos: 100,
    });
  });

  test('puntosFijos negativo → crea el motivo con ese valor (ej. "Comisión por mora" = -200)', async () => {
    LoyaltyAdjustmentReason.create.mockResolvedValue({ id: 6 });

    await configService.crearReason({ clave: 'comision_mora', nombre: 'Comisión por mora', puntosFijos: -200 });

    expect(LoyaltyAdjustmentReason.create).toHaveBeenCalledWith({
      clave: 'comision_mora', nombre: 'Comisión por mora', activo: true, puntosFijos: -200,
    });
  });
});

describe('editarReason', () => {
  function makeReason(overrides = {}) {
    const reason = { id: 1, clave: 'bono', nombre: 'Bono', activo: true, puntosFijos: null, ...overrides };
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

  test('puntosFijos inválido en edición → 400, no actualiza', async () => {
    const reason = makeReason();
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue(reason);

    await expect(
      configService.editarReason(1, { puntosFijos: 0 }),
    ).rejects.toMatchObject({ statusCode: 400 });
    expect(reason.update).not.toHaveBeenCalled();
  });

  test('puntosFijos válido → actualiza el valor fijo', async () => {
    const reason = makeReason();
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue(reason);

    const resultado = await configService.editarReason(1, { puntosFijos: 100 });

    expect(reason.update).toHaveBeenCalledWith({ puntosFijos: 100 });
    expect(resultado.puntosFijos).toBe(100);
  });

  test('puntosFijos: null explícito vuelve el motivo manual', async () => {
    const reason = makeReason({ puntosFijos: 100 });
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue(reason);

    await configService.editarReason(1, { puntosFijos: null });

    expect(reason.update).toHaveBeenCalledWith({ puntosFijos: null });
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

// ── Catálogos de Kore (conceptos/formas de pago, 2026-10-26) ──────────────────
// `acumulaPuntos`/`esPuntos` son SIEMPRE manuales — un sync nunca los pisa, solo
// nombre/abreviatura/tipo (conceptos) o los hints informativos de Kore (formas
// de pago). Ver loyalty-config.service.js para el detalle de la decisión.

function makeConcepto(overrides = {}) {
  return { id: 1, koreId: 'c1', nombre: 'COBRO CONTRA ENTREGA', acumulaPuntos: false, activo: true, update: jest.fn(), ...overrides };
}

function makeFormaPago(overrides = {}) {
  return {
    id: 1, koreId: 'f1', nombre: 'EFECTIVO', acumulaPuntos: false, esPuntos: false,
    koreAcumulaPuntosHint: null, koreEsPuntosHint: null, activo: true, update: jest.fn(), ...overrides,
  };
}

describe('sincronizarConceptosKore', () => {
  test('concepto nuevo: se crea con acumulaPuntos:false, nunca pisa un valor manual existente', async () => {
    koreCatalogosClient.obtenerConceptosKore.mockResolvedValue([
      { Id: 'c1', Nombre: 'COBRO CONTRA ENTREGA', Abreviatura: 'CCE', Tipo: 'ENTRADA' },
    ]);
    LoyaltyKoreConcepto.findOrCreate.mockResolvedValue([makeConcepto(), true]);
    LoyaltyKoreConcepto.findAll.mockResolvedValue([]);

    const resultado = await configService.sincronizarConceptosKore();

    expect(LoyaltyKoreConcepto.findOrCreate).toHaveBeenCalledWith({
      where:    { koreId: 'c1' },
      defaults: expect.objectContaining({ koreId: 'c1', nombre: 'COBRO CONTRA ENTREGA', acumulaPuntos: false }),
    });
    expect(resultado).toEqual({ nuevos: 1, actualizados: 0, desactivados: 0 });
  });

  test('concepto existente: actualiza nombre/abreviatura/tipo, nunca toca acumulaPuntos', async () => {
    const existente = makeConcepto({ acumulaPuntos: true });
    koreCatalogosClient.obtenerConceptosKore.mockResolvedValue([
      { Id: 'c1', Nombre: 'NUEVO NOMBRE', Abreviatura: 'NN', Tipo: 'SALIDA' },
    ]);
    LoyaltyKoreConcepto.findOrCreate.mockResolvedValue([existente, false]);
    LoyaltyKoreConcepto.findAll.mockResolvedValue([existente]);

    const resultado = await configService.sincronizarConceptosKore();

    expect(existente.update).toHaveBeenCalledWith({ nombre: 'NUEVO NOMBRE', abreviatura: 'NN', tipo: 'SALIDA', activo: true });
    expect(existente.update).not.toHaveBeenCalledWith(expect.objectContaining({ acumulaPuntos: expect.anything() }));
    expect(resultado.actualizados).toBe(1);
  });

  test('concepto que ya no viene de Kore: se marca activo:false, nunca se borra', async () => {
    const yaNoViene = makeConcepto({ koreId: 'viejo' });
    koreCatalogosClient.obtenerConceptosKore.mockResolvedValue([]);
    LoyaltyKoreConcepto.findAll.mockResolvedValue([yaNoViene]);

    const resultado = await configService.sincronizarConceptosKore();

    expect(yaNoViene.update).toHaveBeenCalledWith({ activo: false });
    expect(resultado.desactivados).toBe(1);
  });
});

describe('sincronizarFormasPagoKore', () => {
  test('forma de pago nueva: se crea con acumulaPuntos/esPuntos en false, guarda los hints de Kore', async () => {
    koreCatalogosClient.obtenerFormasPagoKore.mockResolvedValue([
      { ID: 'f1', Nombre: 'EFECTIVO', AcumulaPuntosTuberos: true, EsPuntos: false },
    ]);
    LoyaltyKoreFormaPago.findOrCreate.mockResolvedValue([makeFormaPago(), true]);
    LoyaltyKoreFormaPago.findAll.mockResolvedValue([]);

    const resultado = await configService.sincronizarFormasPagoKore();

    expect(LoyaltyKoreFormaPago.findOrCreate).toHaveBeenCalledWith({
      where:    { koreId: 'f1' },
      defaults: expect.objectContaining({
        koreId: 'f1', nombre: 'EFECTIVO', acumulaPuntos: false, esPuntos: false,
        koreAcumulaPuntosHint: true, koreEsPuntosHint: false,
      }),
    });
    expect(resultado).toEqual({ nuevos: 1, actualizados: 0, desactivados: 0 });
  });

  test('forma de pago existente: refresca los hints de Kore, nunca toca acumulaPuntos/esPuntos reales', async () => {
    const existente = makeFormaPago({ acumulaPuntos: true, esPuntos: true, koreAcumulaPuntosHint: false });
    koreCatalogosClient.obtenerFormasPagoKore.mockResolvedValue([
      { ID: 'f1', Nombre: 'EFECTIVO', AcumulaPuntosTuberos: true, EsPuntos: false },
    ]);
    LoyaltyKoreFormaPago.findOrCreate.mockResolvedValue([existente, false]);
    LoyaltyKoreFormaPago.findAll.mockResolvedValue([existente]);

    await configService.sincronizarFormasPagoKore();

    expect(existente.update).toHaveBeenCalledWith({
      nombre: 'EFECTIVO', koreAcumulaPuntosHint: true, koreEsPuntosHint: false, activo: true,
    });
    expect(existente.update).not.toHaveBeenCalledWith(expect.objectContaining({ acumulaPuntos: expect.anything() }));
    expect(existente.update).not.toHaveBeenCalledWith(expect.objectContaining({ esPuntos: expect.anything() }));
  });
});

describe('actualizarConceptoKore', () => {
  test('actualiza acumulaPuntos', async () => {
    const concepto = makeConcepto();
    LoyaltyKoreConcepto.findByPk.mockResolvedValue(concepto);

    await configService.actualizarConceptoKore(1, { acumulaPuntos: true });

    expect(concepto.update).toHaveBeenCalledWith({ acumulaPuntos: true });
  });

  test('id inexistente: 404', async () => {
    LoyaltyKoreConcepto.findByPk.mockResolvedValue(null);
    await expect(configService.actualizarConceptoKore(999, { acumulaPuntos: true })).rejects.toMatchObject({ statusCode: 404 });
  });
});

describe('actualizarFormaPagoKore', () => {
  beforeEach(() => {
    sequelize.transaction.mockImplementation(async (cb) => cb({ fake: true }));
  });

  test('actualiza acumulaPuntos sin tocar esPuntos', async () => {
    const forma = makeFormaPago();
    LoyaltyKoreFormaPago.findByPk.mockResolvedValue(forma);

    await configService.actualizarFormaPagoKore(1, { acumulaPuntos: true });

    expect(forma.update).toHaveBeenCalledWith({ acumulaPuntos: true }, { transaction: { fake: true } });
    expect(LoyaltyKoreFormaPago.update).not.toHaveBeenCalled();
  });

  test('esPuntos:true limpia cualquier otra fila que ya lo tuviera, ANTES de guardar la propia', async () => {
    const forma = makeFormaPago({ id: 2 });
    LoyaltyKoreFormaPago.findByPk.mockResolvedValue(forma);

    await configService.actualizarFormaPagoKore(2, { esPuntos: true });

    const { Op } = require('sequelize');
    expect(LoyaltyKoreFormaPago.update).toHaveBeenCalledWith(
      { esPuntos: false },
      { where: { esPuntos: true, id: { [Op.ne]: 2 } }, transaction: { fake: true } },
    );
    expect(forma.update).toHaveBeenCalledWith({ esPuntos: true }, { transaction: { fake: true } });
  });

  test('id inexistente: 404', async () => {
    LoyaltyKoreFormaPago.findByPk.mockResolvedValue(null);
    await expect(configService.actualizarFormaPagoKore(999, { esPuntos: true })).rejects.toMatchObject({ statusCode: 404 });
  });
});
