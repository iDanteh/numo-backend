'use strict';

// reset-netpay-comisiones-config.test.js (2026-10-07) — mockea los modelos Postgres y
// sequelize.transaction, NUNCA toca Postgres real. Cubre: idempotencia (sección vieja no
// existe), dry-run (cero borrados), y apply (borra en orden FK dentro de una transacción
// todo-o-nada).
jest.mock('../src/shared/models/postgres', () => ({
  ConfigSection: { findOne: jest.fn() },
  GlobalConfig: { findAll: jest.fn(), destroy: jest.fn() },
  ConfigAuditLog: { count: jest.fn(), destroy: jest.fn() },
}));
jest.mock('../src/config/database.postgres', () => ({
  sequelize: { transaction: jest.fn() },
  connectPostgres: jest.fn(),
  disconnectPostgres: jest.fn(),
}));

const { ConfigSection, GlobalConfig, ConfigAuditLog } = require('../src/shared/models/postgres');
const { sequelize } = require('../src/config/database.postgres');

const { parseArgs, runDryRun, runApply, CLAVE_SECCION_VIEJA } = require('./reset-netpay-comisiones-config');

beforeEach(() => {
  jest.clearAllMocks();
  jest.spyOn(console, 'log').mockImplementation(() => {});
  // sequelize.transaction(cb) — ejecuta el callback con un objeto `t` cualquiera, mismo
  // patrón que el resto de los tests de scripts que usan transacciones Sequelize.
  sequelize.transaction.mockImplementation((cb) => cb({}));
});

describe('parseArgs', () => {
  test('sin flags: modo dry-run por default', () => {
    expect(parseArgs([])).toEqual({ modo: 'dry-run' });
  });

  test('--apply: modo apply', () => {
    expect(parseArgs(['--apply'])).toEqual({ modo: 'apply' });
  });

  test('flag desconocido: arroja', () => {
    expect(() => parseArgs(['--migrar'])).toThrow(/no reconocido/);
  });

  test('2 modos a la vez: arroja', () => {
    expect(() => parseArgs(['--dry-run', '--apply'])).toThrow(/Solo se permite un modo/);
  });
});

describe('runDryRun', () => {
  test('sección vieja no existe: informa "nada que limpiar", no cuenta nada', async () => {
    ConfigSection.findOne.mockResolvedValue(null);

    const resultado = await runDryRun();

    expect(ConfigSection.findOne).toHaveBeenCalledWith({ where: { clave: CLAVE_SECCION_VIEJA } });
    expect(resultado).toEqual({ existeSeccion: false, totalConfigs: 0, totalAudits: 0 });
    expect(GlobalConfig.findAll).not.toHaveBeenCalled();
  });

  test('sección vieja existe con configs: cuenta GlobalConfig y ConfigAuditLog, no borra nada', async () => {
    ConfigSection.findOne.mockResolvedValue({ id: 7 });
    GlobalConfig.findAll.mockResolvedValue([{ id: 1 }, { id: 2 }]);
    ConfigAuditLog.count.mockResolvedValue(5);

    const resultado = await runDryRun();

    expect(GlobalConfig.findAll).toHaveBeenCalledWith({ where: { sectionId: 7 }, attributes: ['id'] });
    expect(ConfigAuditLog.count).toHaveBeenCalledWith({ where: { configId: [1, 2] } });
    expect(resultado).toEqual({ existeSeccion: true, totalConfigs: 2, totalAudits: 5 });
    expect(GlobalConfig.destroy).not.toHaveBeenCalled();
    expect(ConfigAuditLog.destroy).not.toHaveBeenCalled();
  });

  test('sección vieja existe sin configs: 0 audits, sin llamar a ConfigAuditLog.count', async () => {
    ConfigSection.findOne.mockResolvedValue({ id: 7 });
    GlobalConfig.findAll.mockResolvedValue([]);

    const resultado = await runDryRun();

    expect(ConfigAuditLog.count).not.toHaveBeenCalled();
    expect(resultado).toEqual({ existeSeccion: true, totalConfigs: 0, totalAudits: 0 });
  });
});

describe('runApply', () => {
  test('sección vieja no existe: no abre transacción, nada que borrar', async () => {
    ConfigSection.findOne.mockResolvedValue(null);

    const resultado = await runApply();

    expect(sequelize.transaction).not.toHaveBeenCalled();
    expect(resultado).toEqual({ existeSeccion: false, configsBorrados: 0, auditsBorrados: 0 });
  });

  test('borra en orden FK dentro de UNA transacción: primero audits, después configs, después la sección', async () => {
    const destroySeccion = jest.fn().mockResolvedValue(undefined);
    ConfigSection.findOne.mockResolvedValue({ id: 7, destroy: destroySeccion });
    GlobalConfig.findAll.mockResolvedValue([{ id: 1 }, { id: 2 }]);
    ConfigAuditLog.destroy.mockResolvedValue(5);
    GlobalConfig.destroy.mockResolvedValue(2);

    const resultado = await runApply();

    expect(sequelize.transaction).toHaveBeenCalledTimes(1);
    expect(ConfigAuditLog.destroy).toHaveBeenCalledWith(expect.objectContaining({ where: { configId: [1, 2] } }));
    expect(GlobalConfig.destroy).toHaveBeenCalledWith(expect.objectContaining({ where: { sectionId: 7 } }));
    expect(destroySeccion).toHaveBeenCalledTimes(1);
    expect(resultado).toEqual({ existeSeccion: true, configsBorrados: 2, auditsBorrados: 5 });
  });

  test('sección vieja sin configs: no llama a ConfigAuditLog.destroy, igual borra la sección', async () => {
    const destroySeccion = jest.fn().mockResolvedValue(undefined);
    ConfigSection.findOne.mockResolvedValue({ id: 7, destroy: destroySeccion });
    GlobalConfig.findAll.mockResolvedValue([]);
    GlobalConfig.destroy.mockResolvedValue(0);

    const resultado = await runApply();

    expect(ConfigAuditLog.destroy).not.toHaveBeenCalled();
    expect(destroySeccion).toHaveBeenCalledTimes(1);
    expect(resultado).toEqual({ existeSeccion: true, configsBorrados: 0, auditsBorrados: 0 });
  });
});
