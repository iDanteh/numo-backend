'use strict';

// Passphrase de prueba, solo para este proceso de Jest — no toca el .env real.
// Se define ANTES de requerir el service porque _passphrase() la lee en cada
// llamada (no hay caché de módulo que pueda quedar con un valor viejo).
process.env.CONFIG_MASTER_KEY = 'jest-test-master-key-no-usar-en-real';

const { sequelize } = require('../../config/database.postgres');
const { ConfigSection, GlobalConfig, ConfigAuditLog } = require('../models/postgres');
const svc = require('./global-config.service');

/**
 * global-config.service.test.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Integración contra Postgres REAL (no hay mock de pgcrypto posible con
 * sentido — el punto de estos tests es verificar que el cifrado/descifrado
 * real funciona). Usa el mismo POSTGRES_URI que el resto de la app (cargado
 * vía config/env.js → dotenv), con una sección de prueba con clave
 * claramente namespaced y limpieza completa en beforeAll/afterAll para no
 * dejar basura ni chocar con datos reales.
 */

const SECTION_CLAVE = '__test_global_config_service__';

async function limpiarSeccionDePrueba() {
  const section = await ConfigSection.findOne({ where: { clave: SECTION_CLAVE } });
  if (!section) return;
  const configs   = await GlobalConfig.findAll({ where: { sectionId: section.id }, attributes: ['id'] });
  const configIds = configs.map((c) => c.id);
  if (configIds.length) await ConfigAuditLog.destroy({ where: { configId: configIds } });
  await GlobalConfig.destroy({ where: { sectionId: section.id } });
  await section.destroy();
}

describe('global-config.service (integración Postgres)', () => {
  beforeAll(async () => {
    await sequelize.authenticate();
    await sequelize.query('CREATE EXTENSION IF NOT EXISTS pgcrypto');
    // Solo las 3 tablas de este dominio (no el syncModels() completo de la
    // app, que también migra columnas de otras 15+ tablas sin relación).
    await ConfigSection.sync({ force: false });
    await GlobalConfig.sync({ force: false });
    await ConfigAuditLog.sync({ force: false });

    await limpiarSeccionDePrueba();
    await svc.createSection({
      clave: SECTION_CLAVE,
      nombre: 'Sección de prueba (Jest)',
      modulosAfectados: ['ninguno — solo test'],
    });
  });

  afterAll(async () => {
    await limpiarSeccionDePrueba();
    await sequelize.close();
  });

  test('setValue + getValue con valor NO secreto: se guarda en texto plano y se lee igual', async () => {
    const configId = await svc.setValue(SECTION_CLAVE, 'URL_PLANA', 'https://ejemplo.test/api', {
      esSecreto: false, tipo: 'url', usuarioId: '1', usuarioNombre: 'Tester',
    });

    const fila = await GlobalConfig.findByPk(configId);
    expect(fila.valor).toBe('https://ejemplo.test/api');
    expect(fila.valorCifrado).toBeNull();
    expect(fila.esSecreto).toBe(false);

    const leido = await svc.getValue(SECTION_CLAVE, 'URL_PLANA');
    expect(leido).toBe('https://ejemplo.test/api');
  });

  test('setValue + getValue con valor SECRETO: se guarda cifrado (valor queda null) y se descifra igual al original', async () => {
    const original = 'super-secreto-123!ñ';
    const configId = await svc.setValue(SECTION_CLAVE, 'API_KEY', original, {
      esSecreto: true, tipo: 'texto', usuarioId: '1', usuarioNombre: 'Tester',
    });

    const fila = await GlobalConfig.findByPk(configId);
    expect(fila.valor).toBeNull();
    expect(fila.esSecreto).toBe(true);
    expect(fila.valorCifrado).not.toBeNull();
    expect(Buffer.isBuffer(fila.valorCifrado)).toBe(true);
    // El bytea cifrado no debe contener el texto plano en ningún lado.
    expect(fila.valorCifrado.toString('latin1')).not.toContain(original);

    const leido = await svc.getValue(SECTION_CLAVE, 'API_KEY');
    expect(leido).toBe(original);
  });

  test('invalida la caché en memoria al hacer setValue de la misma clave', async () => {
    await svc.setValue(SECTION_CLAVE, 'CACHE_KEY', 'valor-viejo', { usuarioNombre: 'Tester' });
    expect(await svc.getValue(SECTION_CLAVE, 'CACHE_KEY')).toBe('valor-viejo'); // puebla la caché

    await svc.setValue(SECTION_CLAVE, 'CACHE_KEY', 'valor-nuevo', { usuarioNombre: 'Tester' });
    expect(await svc.getValue(SECTION_CLAVE, 'CACHE_KEY')).toBe('valor-nuevo'); // no debe servir el viejo cacheado
  });

  test('audit log: un config esSecreto=true NUNCA guarda valorAnterior/valorNuevo (ni al crear ni al editar)', async () => {
    const configId = await svc.setValue(SECTION_CLAVE, 'AUDIT_SECRETO', 'v1', {
      esSecreto: true, usuarioNombre: 'Tester',
    });
    await svc.setValue(SECTION_CLAVE, 'AUDIT_SECRETO', 'v2', { esSecreto: true, usuarioNombre: 'Tester' });

    const logs = await ConfigAuditLog.findAll({ where: { configId }, order: [['id', 'ASC']] });
    expect(logs).toHaveLength(2);
    expect(logs[0].accion).toBe('creado');
    expect(logs[1].accion).toBe('editado');
    for (const log of logs) {
      expect(log.valorAnterior).toBeNull();
      expect(log.valorNuevo).toBeNull();
    }
  });

  test('audit log: un config esSecreto=false SÍ guarda valorAnterior/valorNuevo reales en una edición', async () => {
    const configId = await svc.setValue(SECTION_CLAVE, 'AUDIT_PLANO', 'p1', {
      esSecreto: false, usuarioNombre: 'Tester',
    });
    await svc.setValue(SECTION_CLAVE, 'AUDIT_PLANO', 'p2', { esSecreto: false, usuarioNombre: 'Tester' });

    const logs = await ConfigAuditLog.findAll({ where: { configId }, order: [['id', 'ASC']] });
    expect(logs).toHaveLength(2);
    expect(logs[0].accion).toBe('creado');
    expect(logs[0].valorAnterior).toBeNull(); // nada antes de crearlo
    expect(logs[0].valorNuevo).toBe('p1');
    expect(logs[1].accion).toBe('editado');
    expect(logs[1].valorAnterior).toBe('p1'); // valor real previo
    expect(logs[1].valorNuevo).toBe('p2');
  });

  test('revealSecret descifra correctamente y registra accion="secreto_revelado" sin valores en el audit log', async () => {
    const original  = 'valor-a-revelar-456';
    const configId  = await svc.setValue(SECTION_CLAVE, 'REVEAL_ME', original, {
      esSecreto: true, usuarioNombre: 'Tester',
    });

    const revelado = await svc.revealSecret(configId, { usuarioId: '2', usuarioNombre: 'OtroUsuario' });
    expect(revelado).toBe(original);

    const logs = await ConfigAuditLog.findAll({ where: { configId, accion: 'secreto_revelado' } });
    expect(logs).toHaveLength(1);
    expect(logs[0].valorAnterior).toBeNull();
    expect(logs[0].valorNuevo).toBeNull();
    expect(logs[0].usuarioNombre).toBe('OtroUsuario');
  });

  test('getValue sobre una clave inexistente tira error explícito (no undefined, no silencioso)', async () => {
    await expect(svc.getValue(SECTION_CLAVE, 'NO_EXISTE_JAMAS')).rejects.toThrow(/No existe la configuración/);
  });

  test('setValue sobre una sección que no existe en el catálogo tira error explícito y NO la auto-crea', async () => {
    const seccionInexistente = '__seccion_que_no_existe_jamas__';
    await expect(
      svc.setValue(seccionInexistente, 'X', 'y', { usuarioNombre: 'Tester' }),
    ).rejects.toThrow(/no existe en el catálogo/);

    const section = await ConfigSection.findOne({ where: { clave: seccionInexistente } });
    expect(section).toBeNull();
  });
});
