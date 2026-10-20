'use strict';

/**
 * scripts/migrate-lealtad-csv.test.js — Sistema de Lealtad (PR8, design.md "Migration /
 * Rollout" §3 + specs/loyalty-migration/spec.md). File reading (`leerFilas`) goes through the
 * REAL `exceljs` library against temp CSV fixtures created per test (os.tmpdir(), deleted in
 * afterEach) — no mocked file parser, same spirit as `migrate-netpay-v2.test.js` never
 * touching a live DB. The DB layer (models + the two PR2 services) is fully mocked, same
 * jest.mock() convention as `loyalty-ledger.service.test.js`/`loyalty-card.service.test.js`.
 *
 * ABSOLUTELY no test in this file opens a real Postgres connection, and the real CSV from the
 * business owner is never used here — every fixture is synthetic, built inline per test.
 * `--apply` against real data is explicitly deferred (see tasks.md Phase 8, task 8.4).
 */

const fs = require('fs');
const os = require('os');
const path = require('path');

jest.mock('../src/shared/models/postgres', () => ({
  LoyaltyCard: { findOne: jest.fn(), findByPk: jest.fn() },
  LoyaltyPointsLedger: { findOne: jest.fn() },
}));
jest.mock('../src/config/database.postgres', () => ({
  sequelize: { transaction: jest.fn(), authenticate: jest.fn() },
}));
jest.mock('../src/loyalty/domains/cards/loyalty-card.service');
jest.mock('../src/loyalty/domains/ledger/loyalty-ledger.service');

const { LoyaltyCard, LoyaltyPointsLedger } = require('../src/shared/models/postgres');
const { sequelize } = require('../src/config/database.postgres');
const cardService = require('../src/loyalty/domains/cards/loyalty-card.service');
const ledgerService = require('../src/loyalty/domains/ledger/loyalty-ledger.service');

const {
  parseArgs,
  leerFilas,
  clasificar,
  reportar,
  construirMensaje,
  escribirDataset,
  runDryRun,
  runApply,
} = require('./migrate-lealtad-csv');

let dir;

beforeEach(() => {
  jest.clearAllMocks();
  sequelize.transaction.mockImplementation(async (cb) => cb({ fake: true }));
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lealtad-migracion-'));
});

afterEach(() => {
  fs.rmSync(dir, { recursive: true, force: true });
});

function escribirCsv(nombreArchivo, contenido) {
  const ruta = path.join(dir, nombreArchivo);
  fs.writeFileSync(ruta, contenido, 'utf8');
  return ruta;
}

describe('parseArgs', () => {
  test('sin modo: default dry-run; --file es requerido', () => {
    expect(() => parseArgs(['--file=./x.csv'])).not.toThrow();
    expect(parseArgs(['--file=./x.csv'])).toMatchObject({ modo: 'dry-run', file: './x.csv' });
  });

  test('sin --file: arroja', () => {
    expect(() => parseArgs(['--dry-run'])).toThrow(/--file/);
  });

  test('--apply sin --out: arroja', () => {
    expect(() => parseArgs(['--file=./x.csv', '--apply'])).toThrow(/--out/);
  });

  test('--apply con --out: ok', () => {
    const r = parseArgs(['--file=./x.csv', '--apply', '--out=./salida.csv']);
    expect(r).toMatchObject({ modo: 'apply', file: './x.csv', out: './salida.csv' });
  });

  test('--url y --col-* se parsean en colOverrides/url', () => {
    const r = parseArgs([
      '--file=./x.csv', '--url=https://x/#/lealtad/consulta',
      '--col-telefono=Cel', '--col-saldo=Puntos Acumulados',
    ]);
    expect(r.url).toBe('https://x/#/lealtad/consulta');
    expect(r.colOverrides.telefono).toEqual(['cel']);
    expect(r.colOverrides.saldo).toEqual(['puntos acumulados']);
  });

  test('dos modos a la vez: arroja', () => {
    expect(() => parseArgs(['--file=./x.csv', '--dry-run', '--apply'])).toThrow(/un modo a la vez/);
  });

  test('flag desconocido: arroja', () => {
    expect(() => parseArgs(['--file=./x.csv', '--foo'])).toThrow(/no reconocido/);
  });

  test('--file sin "=valor": arroja (flag no reconocido)', () => {
    expect(() => parseArgs(['--file'])).toThrow(/no reconocido/);
  });
});

describe('leerFilas (exceljs real contra un archivo temporal)', () => {
  test('lee un CSV con encabezados estándar (telefono, nombre, saldo)', async () => {
    const ruta = escribirCsv('ok.csv', 'telefono,nombre,saldo\n5512345678,Juan Perez,150.00\n');
    const filas = await leerFilas(ruta, {});
    expect(filas).toHaveLength(1);
    expect(filas[0]).toMatchObject({ telefonoRaw: '5512345678', nombreRaw: 'Juan Perez' });
    expect(String(filas[0].saldoRaw)).toBe('150');
  });

  test('detecta encabezados alternativos (celular, cliente, points) sin overrides', async () => {
    const ruta = escribirCsv('alt.csv', 'celular,cliente,points\n5512345678,Ana Ruiz,75.50\n');
    const filas = await leerFilas(ruta, {});
    expect(filas).toHaveLength(1);
    expect(filas[0].telefonoRaw).toBe('5512345678');
    expect(filas[0].nombreRaw).toBe('Ana Ruiz');
  });

  test('--col-* overrides tienen precedencia sobre los alias por default', async () => {
    const ruta = escribirCsv('custom.csv', 'Cel,Cliente,Puntos Acumulados\n5512345678,Ana,20\n');
    const filas = await leerFilas(ruta, {
      telefono: ['cel'], nombre: ['cliente'], saldo: ['puntos acumulados'],
    });
    expect(filas).toHaveLength(1);
    expect(filas[0].telefonoRaw).toBe('5512345678');
  });

  test('columna de teléfono ausente: arroja', async () => {
    const ruta = escribirCsv('sin-tel.csv', 'nombre,saldo\nJuan,10\n');
    await expect(leerFilas(ruta, {})).rejects.toThrow(/teléfono/i);
  });

  test('filas completamente vacías se ignoran', async () => {
    const ruta = escribirCsv('conhuecos.csv', 'telefono,nombre,saldo\n5512345678,Juan,10\n,,\n');
    const filas = await leerFilas(ruta, {});
    expect(filas).toHaveLength(1);
  });
});

describe('clasificar (clasificación pura + chequeo contra DB mockeada)', () => {
  function fila(overrides = {}) {
    return {
      numeroFila: 2, telefonoRaw: '5512345678', nombreRaw: 'Juan', saldoRaw: '150.00', nivelRaw: '', ...overrides,
    };
  }

  test('fila válida sin conflicto en DB → ok, con saldo normalizado a 2 decimales y nivel Bronce por default', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);
    const r = await clasificar([fila()]);
    expect(r.ok).toHaveLength(1);
    expect(r.ok[0]).toMatchObject({ telefono: '5512345678', nombre: 'Juan', saldo: '150.00', nivel: 'Bronce' });
    expect(r.invalido).toHaveLength(0);
  });

  test('teléfono con guiones se normaliza antes de clasificar (spec: "55-1234-5678" → "5512345678")', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);
    const r = await clasificar([fila({ telefonoRaw: '55-1234-5678' })]);
    expect(r.ok).toHaveLength(1);
    expect(r.ok[0].telefono).toBe('5512345678');
  });

  test('teléfono con menos de 10 dígitos tras normalizar → invalido, no crea nada', async () => {
    const r = await clasificar([fila({ telefonoRaw: '12345' })]);
    expect(r.invalido).toHaveLength(1);
    expect(r.ok).toHaveLength(0);
    expect(LoyaltyCard.findOne).not.toHaveBeenCalled();
  });

  test('teléfonos duplicados dentro del mismo CSV: AMBAS filas rechazadas, ninguna se importa', async () => {
    const filas = [
      fila({ numeroFila: 2, telefonoRaw: '5512345678' }),
      fila({ numeroFila: 3, telefonoRaw: '55 1234 5678', nombreRaw: 'Otro' }),
    ];
    const r = await clasificar(filas);
    expect(r.duplicadoEnCsv).toHaveLength(2);
    expect(r.ok).toHaveLength(0);
    expect(LoyaltyCard.findOne).not.toHaveBeenCalled();
  });

  test('nombre vacío → invalido', async () => {
    const r = await clasificar([fila({ nombreRaw: '   ' })]);
    expect(r.invalido).toHaveLength(1);
  });

  test('saldo con más de 2 decimales → invalido', async () => {
    const r = await clasificar([fila({ saldoRaw: '150.123' })]);
    expect(r.invalido).toHaveLength(1);
  });

  test('saldo negativo → invalido', async () => {
    const r = await clasificar([fila({ saldoRaw: '-5.00' })]);
    expect(r.invalido).toHaveLength(1);
  });

  test('saldo no numérico → invalido', async () => {
    const r = await clasificar([fila({ saldoRaw: 'abc' })]);
    expect(r.invalido).toHaveLength(1);
  });

  test('nivel desconocido → invalido', async () => {
    const r = await clasificar([fila({ nivelRaw: 'Diamante' })]);
    expect(r.invalido).toHaveLength(1);
  });

  test('nivel reconocido case-insensitive ("oro") se normaliza a "Oro"', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);
    const r = await clasificar([fila({ nivelRaw: 'oro' })]);
    expect(r.ok[0].nivel).toBe('Oro');
  });

  test('teléfono ya existe CON entrada MIGRACION previa → ya_migrada, no bloquea (idempotencia de re-run)', async () => {
    LoyaltyCard.findOne.mockResolvedValue({ id: 5 });
    LoyaltyPointsLedger.findOne.mockResolvedValue({ id: 1, origen: 'migracion' });
    const r = await clasificar([fila()]);
    expect(r.yaMigrada).toHaveLength(1);
    expect(r.ok).toHaveLength(0);
    expect(r.conflicto).toHaveLength(0);
  });

  test('teléfono ya existe SIN entrada MIGRACION previa → conflicto (bloqueante)', async () => {
    LoyaltyCard.findOne.mockResolvedValue({ id: 5 });
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    const r = await clasificar([fila()]);
    expect(r.conflicto).toHaveLength(1);
    expect(r.ok).toHaveLength(0);
  });
});

describe('reportar', () => {
  test('totalBloqueante = 0 cuando todo es ok/ya_migrada', () => {
    const r = reportar({ ok: [1], invalido: [], duplicadoEnCsv: [], yaMigrada: [1], conflicto: [] });
    expect(r.totalBloqueante).toBe(0);
  });

  test('totalBloqueante suma invalido + duplicadoEnCsv + conflicto (ya_migrada NUNCA bloquea)', () => {
    const r = reportar({
      ok: [], invalido: [1], duplicadoEnCsv: [1, 2], yaMigrada: [1, 1, 1], conflicto: [1],
    });
    expect(r.totalBloqueante).toBe(4);
  });
});

describe('construirMensaje', () => {
  test('incluye siempre nombre y nip', () => {
    const msg = construirMensaje({ nombre: 'Juan', nip: '1234', url: null });
    expect(msg).toContain('Juan');
    expect(msg).toContain('1234');
  });

  test('incluye la URL cuando se provee, la omite si no', () => {
    const conUrl = construirMensaje({ nombre: 'Juan', nip: '1234', url: 'https://x/#/lealtad/consulta' });
    const sinUrl = construirMensaje({ nombre: 'Juan', nip: '1234', url: null });
    expect(conUrl).toContain('https://x/#/lealtad/consulta');
    expect(sinUrl).not.toContain('https://');
  });
});

describe('escribirDataset', () => {
  test('escribe encabezado + filas en formato telefono,nip,mensaje', () => {
    const ruta = path.join(dir, 'out.csv');
    escribirDataset(ruta, [{ telefono: '5512345678', nip: '1234', mensaje: 'Hola Juan' }]);
    const contenido = fs.readFileSync(ruta, 'utf8');
    expect(contenido.split('\n')[0]).toBe('telefono,nip,mensaje');
    expect(contenido).toContain('5512345678,1234,Hola Juan');
  });

  test('escapa comas/comillas dentro del mensaje', () => {
    const ruta = path.join(dir, 'out.csv');
    escribirDataset(ruta, [{ telefono: '5512345678', nip: '1234', mensaje: 'Hola, "Juan"' }]);
    const contenido = fs.readFileSync(ruta, 'utf8');
    expect(contenido).toContain('"Hola, ""Juan"""');
  });

  test('dataset vacío escribe solo el encabezado', () => {
    const ruta = path.join(dir, 'out.csv');
    escribirDataset(ruta, []);
    const contenido = fs.readFileSync(ruta, 'utf8').trim();
    expect(contenido).toBe('telefono,nip,mensaje');
  });
});

describe('runDryRun', () => {
  test('no escribe nada y no toca crearTarjeta/sequelize.transaction', async () => {
    const ruta = escribirCsv('ok.csv', 'telefono,nombre,saldo\n5512345678,Juan,150.00\n');
    LoyaltyCard.findOne.mockResolvedValue(null);

    const r = await runDryRun({ file: ruta, colOverrides: {} });

    expect(r.ok).toHaveLength(1);
    expect(cardService.crearTarjeta).not.toHaveBeenCalled();
    expect(ledgerService._aplicarMovimiento).not.toHaveBeenCalled();
    expect(sequelize.transaction).not.toHaveBeenCalled();
  });
});

describe('runApply', () => {
  test('aborta con CERO escrituras si hay filas inválidas/duplicadas/conflicto', async () => {
    const ruta = escribirCsv('mixed.csv', 'telefono,nombre,saldo\n123,Juan,10\n5512345679,Ana,20.00\n');
    LoyaltyCard.findOne.mockResolvedValue(null);
    const outPath = path.join(dir, 'out.csv');

    await expect(runApply({ file: ruta, out: outPath, url: null, colOverrides: {} })).rejects.toThrow(/abortado/);

    expect(cardService.crearTarjeta).not.toHaveBeenCalled();
    expect(sequelize.transaction).not.toHaveBeenCalled();
    expect(fs.existsSync(outPath)).toBe(false);
  });

  test('fila válida produce exactamente una tarjeta + una entrada MIGRACION con saldoResultante = saldo del CSV', async () => {
    const ruta = escribirCsv('ok.csv', 'telefono,nombre,saldo\n5512345678,Juan,150.00\n');
    LoyaltyCard.findOne.mockResolvedValue(null);
    const cardCreada = { id: 42 };
    cardService.crearTarjeta.mockResolvedValue({ card: cardCreada, nip: '7777' });
    LoyaltyCard.findByPk.mockResolvedValue(cardCreada);
    ledgerService._aplicarMovimiento.mockImplementation(async (t, card, mov) => ({
      ...mov, saldoResultante: mov.puntos, cardId: card.id,
    }));

    const outPath = path.join(dir, 'out.csv');
    const r = await runApply({ file: ruta, out: outPath, url: 'https://x/#/lealtad/consulta', colOverrides: {} });

    expect(cardService.crearTarjeta).toHaveBeenCalledWith(
      { telefono: '5512345678', nombre: 'Juan', nivel: 'Bronce' },
      expect.any(Object),
      { transaction: { fake: true } },
    );
    expect(ledgerService._aplicarMovimiento).toHaveBeenCalledWith(
      { fake: true },
      cardCreada,
      expect.objectContaining({ tipo: 'MIGRACION', puntos: '150.00', origen: 'migracion' }),
    );
    expect(r.creadas).toBe(1);
    expect(r.dataset).toEqual([{ telefono: '5512345678', nip: '7777', mensaje: expect.stringContaining('7777') }]);

    const contenido = fs.readFileSync(outPath, 'utf8');
    expect(contenido).toContain('telefono,nip,mensaje');
    expect(contenido).toContain('5512345678,7777,');
  });

  test('2 filas ok producen EXACTAMENTE 2 tuplas en el dataset de salida (una por tarjeta importada)', async () => {
    const ruta = escribirCsv(
      'dos.csv',
      'telefono,nombre,saldo\n5512345678,Juan,150.00\n5512345679,Ana,20.00\n',
    );
    LoyaltyCard.findOne.mockResolvedValue(null);
    let nextId = 1;
    cardService.crearTarjeta.mockImplementation(async () => ({ card: { id: nextId++ }, nip: '0000' }));
    LoyaltyCard.findByPk.mockImplementation(async (id) => ({ id }));
    ledgerService._aplicarMovimiento.mockResolvedValue({});

    const outPath = path.join(dir, 'out.csv');
    const r = await runApply({ file: ruta, out: outPath, url: null, colOverrides: {} });

    expect(r.creadas).toBe(2);
    expect(r.dataset).toHaveLength(2);
    expect(cardService.crearTarjeta).toHaveBeenCalledTimes(2);
  });

  test('falla en la escritura del ledger revierte la transacción: no deja tarjeta huérfana ni archivo de salida', async () => {
    const ruta = escribirCsv('ok.csv', 'telefono,nombre,saldo\n5512345678,Juan,150.00\n');
    LoyaltyCard.findOne.mockResolvedValue(null);
    cardService.crearTarjeta.mockResolvedValue({ card: { id: 1 }, nip: '1111' });
    LoyaltyCard.findByPk.mockResolvedValue({ id: 1 });
    ledgerService._aplicarMovimiento.mockRejectedValue(new Error('fallo simulado del ledger'));

    const outPath = path.join(dir, 'out.csv');
    await expect(
      runApply({ file: ruta, out: outPath, url: null, colOverrides: {} }),
    ).rejects.toThrow(/fallo simulado/);

    expect(fs.existsSync(outPath)).toBe(false);
  });

  test('si el archivo de salida ya se escribió pero la transacción aun así revierte, el archivo se borra', async () => {
    const ruta = escribirCsv('ok.csv', 'telefono,nombre,saldo\n5512345678,Juan,150.00\n');
    LoyaltyCard.findOne.mockResolvedValue(null);
    cardService.crearTarjeta.mockResolvedValue({ card: { id: 1 }, nip: '1111' });
    LoyaltyCard.findByPk.mockResolvedValue({ id: 1 });
    ledgerService._aplicarMovimiento.mockResolvedValue({});
    // Simula que sequelize.transaction corre el callback completo (incluida la escritura del
    // archivo) y LUEGO el intento de COMMIT falla — el archivo quedó escrito en disco, pero la
    // transacción nunca se confirmó, así que debe borrarse (design.md "deleted if the
    // transaction rolls back").
    sequelize.transaction.mockImplementation(async (cb) => {
      await cb({ fake: true });
      throw new Error('fallo simulado de COMMIT');
    });

    const outPath = path.join(dir, 'out.csv');
    await expect(
      runApply({ file: ruta, out: outPath, url: null, colOverrides: {} }),
    ).rejects.toThrow(/fallo simulado de COMMIT/);

    expect(fs.existsSync(outPath)).toBe(false);
  });

  test('re-run idempotente: fila ya migrada se omite, cero tarjetas creadas, no arroja', async () => {
    const ruta = escribirCsv('ok.csv', 'telefono,nombre,saldo\n5512345678,Juan,150.00\n');
    LoyaltyCard.findOne.mockResolvedValue({ id: 9 });
    LoyaltyPointsLedger.findOne.mockResolvedValue({ id: 1, origen: 'migracion' });

    const outPath = path.join(dir, 'out.csv');
    const r = await runApply({ file: ruta, out: outPath, url: null, colOverrides: {} });

    expect(r.creadas).toBe(0);
    expect(cardService.crearTarjeta).not.toHaveBeenCalled();
  });

  test('el dataset de salida solo expone {telefono, nip, mensaje} — nunca un hash/campo interno de la tarjeta', async () => {
    const ruta = escribirCsv('ok.csv', 'telefono,nombre,saldo\n5512345678,Juan,150.00\n');
    LoyaltyCard.findOne.mockResolvedValue(null);
    cardService.crearTarjeta.mockResolvedValue({ card: { id: 1, nipHash: 'secreto-no-debe-salir' }, nip: '1111' });
    LoyaltyCard.findByPk.mockResolvedValue({ id: 1 });
    ledgerService._aplicarMovimiento.mockResolvedValue({});

    const outPath = path.join(dir, 'out.csv');
    const r = await runApply({ file: ruta, out: outPath, url: null, colOverrides: {} });

    expect(Object.keys(r.dataset[0]).sort()).toEqual(['mensaje', 'nip', 'telefono']);
    const contenido = fs.readFileSync(outPath, 'utf8');
    expect(contenido).not.toContain('secreto-no-debe-salir');
  });

  test('ningún módulo de mensajería (provider) es requerido por el script — no envía nada', () => {
    const mod = require('./migrate-lealtad-csv');
    expect(Object.keys(mod).join(',')).not.toMatch(/brevo|whatsapp|sms|enviar/i);
  });
});
