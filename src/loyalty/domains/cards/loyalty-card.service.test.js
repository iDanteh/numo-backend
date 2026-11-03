'use strict';

/**
 * domains/cards/loyalty-card.service.test.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad (PR2) — alta/edición de tarjetas y cambio de nivel.
 * `nip.util` se usa REAL (no mockeado): así se verifica que el NIP que vuelve
 * en la respuesta nunca es el mismo valor que se persiste como `nipHash`.
 */

const { Transaction } = require('sequelize');

jest.mock('../../../shared/models/postgres', () => {
  // `unscoped()` devuelve el MISMO mock (no un modelo scoped real) a propósito: alcanza para que
  // los tests de `obtenerParaEditar` mockeen `LoyaltyCard.findByPk` como cualquier otro test de
  // este archivo, sin duplicar el mock de Sequelize.
  const LoyaltyCard = {
    findOne: jest.fn(), findByPk: jest.fn(), create: jest.fn(), findAndCountAll: jest.fn(), findAll: jest.fn(),
  };
  LoyaltyCard.unscoped = jest.fn(() => LoyaltyCard);
  return {
    LoyaltyCard,
    LoyaltyTierHistory: { create: jest.fn() },
  };
});
jest.mock('../../../config/database.postgres', () => ({
  sequelize: { transaction: jest.fn() },
}));
jest.mock('./drive-loyalty-foto.service', () => ({
  subirFotoCliente: jest.fn(),
  descargarFotoCliente: jest.fn(),
}));

const { LoyaltyCard, LoyaltyTierHistory } = require('../../../shared/models/postgres');
const { sequelize } = require('../../../config/database.postgres');
const { subirFotoCliente, descargarFotoCliente } = require('./drive-loyalty-foto.service');
const cardService = require('./loyalty-card.service');

function makeCard(overrides = {}) {
  const card = {
    id: 1,
    telefono: '5512345678',
    nombre: 'Juan Pérez',
    nivel: 'Bronce',
    saldoPuntos: '0.00',
    activa: true,
    nipHash: 'hash-existente',
    ...overrides,
  };
  card.update = jest.fn(async (changes) => {
    Object.assign(card, changes);
    return card;
  });
  return card;
}

const USER = { dbId: 7, nombre: 'Admin' };

beforeEach(() => {
  jest.clearAllMocks();
  sequelize.transaction.mockImplementation(async (cb) => cb({ fake: true }));
  LoyaltyCard.create.mockImplementation(async (data) => ({ ...makeCard(), ...data }));
});

describe('crearTarjeta', () => {
  test('teléfono duplicado (tarjeta activa o inactiva) → 409, no inserta', async () => {
    LoyaltyCard.findOne.mockResolvedValue(makeCard({ activa: false }));

    await expect(
      cardService.crearTarjeta({ telefono: '5512345678', nombre: 'Otro' }, USER),
    ).rejects.toMatchObject({ statusCode: 409 });

    expect(LoyaltyCard.create).not.toHaveBeenCalled();
  });

  test('teléfono único crea la tarjeta con nivel Bronce por default, saldo 0, activa', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);

    const { card, nip } = await cardService.crearTarjeta({ telefono: '5512345678', nombre: 'Juan Pérez' }, USER);

    expect(LoyaltyCard.create).toHaveBeenCalledWith(expect.objectContaining({
      telefono: '5512345678', nombre: 'Juan Pérez', nivel: 'Bronce', saldoPuntos: 0, activa: true,
    }));
    expect(nip).toMatch(/^\d{4}$/);
    expect(card).toBeDefined();
  });

  test('el NIP devuelto nunca es igual al hash que se persiste', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);

    const { nip } = await cardService.crearTarjeta({ telefono: '5512345678', nombre: 'Juan Pérez' }, USER);
    const [[createArgs]] = LoyaltyCard.create.mock.calls;

    expect(createArgs.nipHash).toBeDefined();
    expect(createArgs.nipHash).not.toBe(nip);
  });

  test('staff puede fijar el nivel inicial explícitamente', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);

    await cardService.crearTarjeta({ telefono: '5512345678', nombre: 'Juan Pérez', nivel: 'Oro' }, USER);

    expect(LoyaltyCard.create).toHaveBeenCalledWith(expect.objectContaining({ nivel: 'Oro' }));
  });

  test('normaliza el teléfono antes de buscar/crear (+52, espacios, guiones)', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);

    await cardService.crearTarjeta({ telefono: '+52 55-1234-5678', nombre: 'Juan Pérez' }, USER);

    expect(LoyaltyCard.findOne).toHaveBeenCalledWith({ where: { telefono: '5512345678' } });
    expect(LoyaltyCard.create).toHaveBeenCalledWith(expect.objectContaining({ telefono: '5512345678' }));
  });

  test('captura SequelizeUniqueConstraintError como 409 (carrera entre findOne y create)', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);
    const err = new Error('duplicate');
    err.name = 'SequelizeUniqueConstraintError';
    LoyaltyCard.create.mockRejectedValue(err);

    await expect(
      cardService.crearTarjeta({ telefono: '5512345678', nombre: 'Juan Pérez' }, USER),
    ).rejects.toMatchObject({ statusCode: 409 });
  });

  // PR8 (migración): crearTarjeta se reutiliza dentro de la transacción única del
  // script de migración (design.md "Apply. One Postgres transaction for all ok rows") —
  // necesita poder recibir y propagar esa transacción externa a findOne/create en vez
  // de abrir su propio connection slot implícito, igual que cambiarNivel ya hace con su
  // propia transacción interna.
  test('con transaction explícito (3er argumento), la propaga a findOne y create', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);
    const fakeT = { fake: true };

    await cardService.crearTarjeta(
      { telefono: '5512345678', nombre: 'Juan Pérez' },
      USER,
      { transaction: fakeT },
    );

    expect(LoyaltyCard.findOne).toHaveBeenCalledWith({ where: { telefono: '5512345678' }, transaction: fakeT });
    expect(LoyaltyCard.create).toHaveBeenCalledWith(expect.objectContaining({ telefono: '5512345678' }), { transaction: fakeT });
  });

  test('sin transaction explícito, el llamado a rutas existente sigue invocando findOne/create con un solo argumento', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);

    await cardService.crearTarjeta({ telefono: '5512345678', nombre: 'Juan Pérez' }, USER);

    expect(LoyaltyCard.findOne.mock.calls[0]).toHaveLength(1);
    expect(LoyaltyCard.create.mock.calls[0]).toHaveLength(1);
  });

  // "Datos adicionales" (2026-10-23) — alta manual, mismos campos que el enriquecimiento del
  // import (ver loyalty-import-apply.service.js), ahora también opcionales acá.
  test('acepta "datos adicionales" opcionales y los persiste trimeados', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);

    await cardService.crearTarjeta({
      telefono: '5512345678', nombre: 'Juan Pérez', apellidos: ' Pérez López ', sucursal: 'Matriz',
    }, USER);

    expect(LoyaltyCard.create).toHaveBeenCalledWith(expect.objectContaining({
      apellidos: 'Pérez López', sucursal: 'Matriz',
    }));
  });

  test('sin "datos adicionales" en el payload, no manda ninguno de esos campos a create', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);

    await cardService.crearTarjeta({ telefono: '5512345678', nombre: 'Juan Pérez' }, USER);

    const [[createArgs]] = LoyaltyCard.create.mock.calls;
    expect(createArgs.apellidos).toBeUndefined();
    expect(createArgs.calle).toBeUndefined();
  });
});

describe('editarTarjeta', () => {
  test('editar solo nombre deja teléfono/saldo/nivel sin cambios', async () => {
    const card = makeCard();
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await cardService.editarTarjeta(1, { nombre: 'Juan P. Editado' }, USER);

    expect(card.nombre).toBe('Juan P. Editado');
    expect(card.telefono).toBe('5512345678');
    expect(card.saldoPuntos).toBe('0.00');
    expect(card.nivel).toBe('Bronce');
  });

  test('cambiar teléfono a uno ya usado por otra tarjeta → 409, no se aplica', async () => {
    const card = makeCard();
    LoyaltyCard.findByPk.mockResolvedValue(card);
    LoyaltyCard.findOne.mockResolvedValue(makeCard({ id: 2, telefono: '5599999999' }));

    await expect(
      cardService.editarTarjeta(1, { telefono: '5599999999' }, USER),
    ).rejects.toMatchObject({ statusCode: 409 });

    expect(card.telefono).toBe('5512345678');
  });

  test('cambiar teléfono a uno libre se aplica', async () => {
    const card = makeCard();
    LoyaltyCard.findByPk.mockResolvedValue(card);
    LoyaltyCard.findOne.mockResolvedValue(null);

    await cardService.editarTarjeta(1, { telefono: '5599999999' }, USER);

    expect(card.telefono).toBe('5599999999');
  });

  test('registra updatedById/updatedByNombre en cualquier edición', async () => {
    const card = makeCard();
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await cardService.editarTarjeta(1, { nombre: 'Nuevo Nombre' }, USER);

    expect(card.updatedById).toBe('7');
    expect(card.updatedByNombre).toBe('Admin');
  });

  test('edita "datos adicionales" sin tocar nombre/teléfono/activa', async () => {
    const card = makeCard();
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await cardService.editarTarjeta(1, { apodo: 'Juancho', colonia: 'Centro' }, USER);

    expect(card.apodo).toBe('Juancho');
    expect(card.colonia).toBe('Centro');
    expect(card.nombre).toBe('Juan Pérez');
  });

  test('un campo de "datos adicionales" no enviado (undefined) no se toca', async () => {
    const card = makeCard({ apodo: 'Apodo viejo' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await cardService.editarTarjeta(1, { nombre: 'Solo nombre' }, USER);

    expect(card.update).toHaveBeenCalledWith(expect.not.objectContaining({ apodo: expect.anything() }));
  });
});

describe('obtenerParaEditar', () => {
  test('trae la tarjeta sin nipHash/recuperacionCodigoHash, vía unscoped()', async () => {
    const card = makeCard({ apodo: 'Juancho' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    const resultado = await cardService.obtenerParaEditar(1);

    expect(LoyaltyCard.unscoped).toHaveBeenCalled();
    expect(LoyaltyCard.findByPk).toHaveBeenCalledWith(1, {
      attributes: { exclude: ['nipHash', 'recuperacionCodigoHash'] },
    });
    expect(resultado.apodo).toBe('Juancho');
  });

  test('tarjeta inexistente → 404', async () => {
    LoyaltyCard.findByPk.mockResolvedValue(null);

    await expect(cardService.obtenerParaEditar(999)).rejects.toMatchObject({ statusCode: 404 });
  });
});

describe('cambiarNip', () => {
  test('genera un nuevo hash y resetea los contadores de intentos/bloqueo', async () => {
    const card = makeCard({ nipHash: 'hash-viejo', nipIntentosFallidos: 3, nipBloqueadoHasta: new Date() });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    const { nip } = await cardService.cambiarNip(1, USER);

    expect(nip).toMatch(/^\d{4}$/);
    expect(card.nipHash).not.toBe('hash-viejo');
    expect(card.nipHash).not.toBe(nip);
    expect(card.nipIntentosFallidos).toBe(0);
    expect(card.nipBloqueadoHasta).toBeNull();
  });

  test('bug real corregido (2026-10-19): registra quién hizo el cambio (updatedById/updatedByNombre)', async () => {
    const card = makeCard({ updatedById: null, updatedByNombre: null });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await cardService.cambiarNip(1, USER);

    expect(card.updatedById).toBe(String(USER.dbId));
    expect(card.updatedByNombre).toBe(USER.nombre);
  });
});

describe('cambiarNivel', () => {
  test('actualiza el nivel y escribe LoyaltyTierHistory en la misma transacción', async () => {
    const card = makeCard({ nivel: 'Bronce' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await cardService.cambiarNivel(1, 'Plata', { motivo: 'promoción manual' }, USER);

    expect(card.nivel).toBe('Plata');
    expect(LoyaltyTierHistory.create).toHaveBeenCalledWith(
      expect.objectContaining({ cardId: 1, nivelAnterior: 'Bronce', nivelNuevo: 'Plata', motivo: 'promoción manual' }),
      expect.objectContaining({ transaction: expect.anything() }),
    );
  });

  test('no mueve saldoPuntos', async () => {
    const card = makeCard({ nivel: 'Bronce', saldoPuntos: '123.45' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await cardService.cambiarNivel(1, 'Plata', {}, USER);

    expect(card.saldoPuntos).toBe('123.45');
  });

  test('nivel sin cambio real → 400', async () => {
    const card = makeCard({ nivel: 'Plata' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await expect(cardService.cambiarNivel(1, 'Plata', {}, USER)).rejects.toMatchObject({ statusCode: 400 });
    expect(LoyaltyTierHistory.create).not.toHaveBeenCalled();
  });

  test('nivel inválido → 400', async () => {
    const card = makeCard({ nivel: 'Bronce' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await expect(cardService.cambiarNivel(1, 'Diamante', {}, USER)).rejects.toMatchObject({ statusCode: 400 });
  });

  test('usa lock de fila (Transaction.LOCK.UPDATE) sobre la tarjeta', async () => {
    const card = makeCard({ nivel: 'Bronce' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await cardService.cambiarNivel(1, 'Plata', {}, USER);

    expect(LoyaltyCard.findByPk).toHaveBeenCalledWith(1, expect.objectContaining({ lock: Transaction.LOCK.UPDATE }));
  });
});

describe('listar — orden (sort/sortDir)', () => {
  beforeEach(() => {
    LoyaltyCard.findAndCountAll.mockResolvedValue({ count: 0, rows: [] });
  });

  test('sin sort → default nombre ASC (comportamiento previo intacto)', async () => {
    await cardService.listar({});

    expect(LoyaltyCard.findAndCountAll).toHaveBeenCalledWith(
      expect.objectContaining({ order: [['nombre', 'ASC']] }),
    );
  });

  test('sort="saldo" → ordena por la columna real saldoPuntos, no por "saldo"', async () => {
    await cardService.listar({ sort: 'saldo', sortDir: 'desc' });

    expect(LoyaltyCard.findAndCountAll).toHaveBeenCalledWith(
      expect.objectContaining({ order: [['saldoPuntos', 'DESC']] }),
    );
  });

  test('sort="creado" → ordena por la columna real createdAt, no por "creado"', async () => {
    await cardService.listar({ sort: 'creado', sortDir: 'desc' });

    expect(LoyaltyCard.findAndCountAll).toHaveBeenCalledWith(
      expect.objectContaining({ order: [['createdAt', 'DESC']] }),
    );
  });

  test.each(['telefono', 'nombre', 'nivel', 'activa'])('sort="%s" es una columna permitida, ASC por default', async (col) => {
    await cardService.listar({ sort: col });

    expect(LoyaltyCard.findAndCountAll).toHaveBeenCalledWith(
      expect.objectContaining({ order: [[col, 'ASC']] }),
    );
  });

  test('sortDir inválido/ausente con sort válido → ASC, nunca se cuela un valor crudo', async () => {
    await cardService.listar({ sort: 'nombre', sortDir: 'xyz' });

    expect(LoyaltyCard.findAndCountAll).toHaveBeenCalledWith(
      expect.objectContaining({ order: [['nombre', 'ASC']] }),
    );
  });

  test('columna no permitida (fuera de whitelist) → cae al default, nunca se interpola directo', async () => {
    await cardService.listar({ sort: 'nipHash', sortDir: 'DESC' });

    expect(LoyaltyCard.findAndCountAll).toHaveBeenCalledWith(
      expect.objectContaining({ order: [['nombre', 'ASC']] }),
    );
  });

  test('intento de inyección vía sort (nombre de columna con SQL crudo) → ignorado, cae al default', async () => {
    await cardService.listar({ sort: "nombre; DROP TABLE loyalty_cards;--", sortDir: 'ASC' });

    expect(LoyaltyCard.findAndCountAll).toHaveBeenCalledWith(
      expect.objectContaining({ order: [['nombre', 'ASC']] }),
    );
  });
});

describe('listar — filtro por origen (2 fuentes: proceso existente de Numo vs. webhook de Kore)', () => {
  beforeEach(() => {
    LoyaltyCard.findAndCountAll.mockResolvedValue({ count: 0, rows: [] });
  });

  test('origen="kore" → filtra createdById="kore-webhook"', async () => {
    await cardService.listar({ origen: 'kore' });

    expect(LoyaltyCard.findAndCountAll).toHaveBeenCalledWith(
      expect.objectContaining({ where: expect.objectContaining({ createdById: 'kore-webhook' }) }),
    );
  });

  test('origen="numo" → excluye "kore-webhook" PERO incluye createdById nulo (tarjetas migradas)', async () => {
    await cardService.listar({ origen: 'numo' });

    const { where } = LoyaltyCard.findAndCountAll.mock.calls[0][0];
    const Sequelize = require('sequelize');
    const grupoOr = where[Sequelize.Op.and][0][Sequelize.Op.or];
    expect(grupoOr).toEqual(expect.arrayContaining([
      { createdById: { [Sequelize.Op.ne]: 'kore-webhook' } },
      { createdById: { [Sequelize.Op.is]: null } },
    ]));
  });

  test('sin origen (u otro valor cualquiera) → no agrega ningún filtro de origen', async () => {
    await cardService.listar({});
    const { where: whereSinOrigen } = LoyaltyCard.findAndCountAll.mock.calls[0][0];
    expect(whereSinOrigen.createdById).toBeUndefined();

    await cardService.listar({ origen: 'otracosa' });
    const { where: whereOtroValor } = LoyaltyCard.findAndCountAll.mock.calls[1][0];
    expect(whereOtroValor.createdById).toBeUndefined();
  });

  test('origen="numo" + search combinados: ambos filtros sobreviven (no se pisan)', async () => {
    await cardService.listar({ origen: 'numo', search: 'Juan' });

    const Sequelize = require('sequelize');
    const { where } = LoyaltyCard.findAndCountAll.mock.calls[0][0];
    const grupos = where[Sequelize.Op.and];
    expect(grupos).toHaveLength(2); // grupo de origen + grupo de search, ninguno pisó al otro
  });
});

// subirFoto (2026-10-29) — foto del cliente, 100% capturada en Numo. `subirFotoCliente`
// (drive-loyalty-foto.service.js) se mockea completo: la lógica de Drive en sí ya se prueba en
// drive-loyalty-foto.service.test.js, acá solo se cubre el wiring (nombre de archivo, guardado
// del link, quién/cuándo) — mismo criterio que bank.service.js#adjuntarImagenFicha.
describe('subirFoto', () => {
  test('tarjeta inexistente → 404, no sube nada a Drive', async () => {
    LoyaltyCard.findByPk.mockResolvedValue(null);

    await expect(
      cardService.subirFoto(999, { buffer: Buffer.from('img'), mimetype: 'image/png', originalname: 'foto.png' }, USER),
    ).rejects.toMatchObject({ statusCode: 404 });

    expect(subirFotoCliente).not.toHaveBeenCalled();
  });

  test('sube la foto con un nombre de archivo "teléfono - nombre" y guarda el link devuelto', async () => {
    const card = makeCard({ id: 1, telefono: '5512345678', nombre: 'Juan Pérez' });
    LoyaltyCard.findByPk.mockResolvedValue(card);
    subirFotoCliente.mockResolvedValue({ driveFileId: 'file-1', driveWebViewLink: 'https://drive/file-1' });

    const resultado = await cardService.subirFoto(
      1, { buffer: Buffer.from('img'), mimetype: 'image/png', originalname: 'foto.png' }, USER,
    );

    expect(subirFotoCliente).toHaveBeenCalledWith(
      expect.any(Buffer), 'image/png', '5512345678 - Juan Pérez.png',
    );
    expect(card.update).toHaveBeenCalledWith(expect.objectContaining({
      fotoUrl: 'https://drive/file-1', updatedById: '7', updatedByNombre: 'Admin',
    }));
    expect(resultado.fotoUrl).toBe('https://drive/file-1');
  });

  // Bug real corregido (2026-10-29): `driveWebViewLink`/`fotoUrl` es la página VISOR de Drive,
  // nunca embebible en un <img> — `fotoDriveFileId`/`fotoMimeType` son lo que de verdad permite
  // descargar el binario después (ver obtenerFoto() más abajo). Antes se descartaba `driveFileId`.
  test('persiste también fotoDriveFileId y fotoMimeType (no solo el link)', async () => {
    const card = makeCard({ id: 1, telefono: '5512345678', nombre: 'Juan Pérez' });
    LoyaltyCard.findByPk.mockResolvedValue(card);
    subirFotoCliente.mockResolvedValue({ driveFileId: 'file-1', driveWebViewLink: 'https://drive/file-1' });

    await cardService.subirFoto(
      1, { buffer: Buffer.from('img'), mimetype: 'image/png', originalname: 'foto.png' }, USER,
    );

    expect(card.update).toHaveBeenCalledWith(expect.objectContaining({
      fotoDriveFileId: 'file-1', fotoMimeType: 'image/png',
    }));
  });
});

// obtenerFoto (2026-10-29) — bug real corregido: card.fotoUrl es el webViewLink de Drive (página
// visor), nunca embebible en un <img>. Esto trae el binario real para servirlo vía un proxy
// autenticado (GET /cards/:id/foto). `descargarFotoCliente` se mockea completo (la lógica de
// Drive en sí ya se prueba en drive-loyalty-foto.service.test.js).
describe('obtenerFoto', () => {
  test('tarjeta inexistente → 404, no pide nada a Drive', async () => {
    LoyaltyCard.findByPk.mockResolvedValue(null);

    await expect(cardService.obtenerFoto(999)).rejects.toMatchObject({ statusCode: 404 });
    expect(descargarFotoCliente).not.toHaveBeenCalled();
  });

  test('tarjeta sin fotoDriveFileId (nunca subió foto, o es de antes de esta columna) → 404', async () => {
    const card = makeCard({ fotoDriveFileId: null });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await expect(cardService.obtenerFoto(1)).rejects.toMatchObject({ statusCode: 404 });
    expect(descargarFotoCliente).not.toHaveBeenCalled();
  });

  test('con fotoDriveFileId, descarga el binario y devuelve data+mimetype', async () => {
    const card = makeCard({ fotoDriveFileId: 'file-1', fotoMimeType: 'image/png' });
    LoyaltyCard.findByPk.mockResolvedValue(card);
    descargarFotoCliente.mockResolvedValue(Buffer.from('contenido-binario'));

    const resultado = await cardService.obtenerFoto(1);

    expect(descargarFotoCliente).toHaveBeenCalledWith('file-1');
    expect(resultado).toEqual({ data: Buffer.from('contenido-binario'), mimetype: 'image/png' });
  });
});

describe('listarPendientesRevisionKore', () => {
  test('filtra por revisionPendiente:true, ordena por createdAt ASC y pagina', async () => {
    LoyaltyCard.findAndCountAll.mockResolvedValue({ count: 3, rows: [{ id: 1 }, { id: 2 }, { id: 3 }] });

    const resultado = await cardService.listarPendientesRevisionKore({ page: 2, limit: 2 });

    expect(LoyaltyCard.findAndCountAll).toHaveBeenCalledWith({
      where:  { revisionPendiente: true },
      order:  [['createdAt', 'ASC']],
      limit:  2,
      offset: 2,
    });
    expect(resultado).toEqual({ total: 3, page: 2, limit: 2, pages: 2, cards: [{ id: 1 }, { id: 2 }, { id: 3 }] });
  });

  test('usa page/limit por default cuando no se mandan', async () => {
    LoyaltyCard.findAndCountAll.mockResolvedValue({ count: 0, rows: [] });

    await cardService.listarPendientesRevisionKore();

    expect(LoyaltyCard.findAndCountAll).toHaveBeenCalledWith(expect.objectContaining({
      where: { revisionPendiente: true }, limit: 50, offset: 0,
    }));
  });

  // search (2026-10-29) — mismo criterio que listar(): prefijo de teléfono O ILIKE sobre nombre.
  test('search filtra por prefijo de teléfono O nombre (ILIKE), sin perder revisionPendiente:true', async () => {
    LoyaltyCard.findAndCountAll.mockResolvedValue({ count: 0, rows: [] });

    await cardService.listarPendientesRevisionKore({ search: 'Juan' });

    const Sequelize = require('sequelize');
    const { where } = LoyaltyCard.findAndCountAll.mock.calls[0][0];
    expect(where.revisionPendiente).toBe(true);
    const grupoOr = where[Sequelize.Op.and][0][Sequelize.Op.or];
    expect(grupoOr).toEqual(expect.arrayContaining([
      { telefono: { [Sequelize.Op.like]: 'Juan%' } },
      { nombre:   { [Sequelize.Op.iLike]: '%Juan%' } },
    ]));
  });

  test('sin search → no agrega Op.and', async () => {
    LoyaltyCard.findAndCountAll.mockResolvedValue({ count: 0, rows: [] });

    await cardService.listarPendientesRevisionKore({});

    const { where } = LoyaltyCard.findAndCountAll.mock.calls[0][0];
    expect(where).toEqual({ revisionPendiente: true });
  });
});

// Reporte "Tarjetas" (2026-10-29): exportar / fechaAlta / revisionPendiente en listar().
describe('listar — reporte "Tarjetas" (exportar, fechaAlta, revisionPendiente)', () => {
  beforeEach(() => {
    LoyaltyCard.findAndCountAll.mockResolvedValue({ count: 0, rows: [] });
    LoyaltyCard.findAll.mockResolvedValue([{ id: 1 }, { id: 2 }]);
  });

  test('sin exportar (comportamiento previo intacto): usa findAndCountAll con limit/offset', async () => {
    await cardService.listar({ page: 2, limit: 10 });

    expect(LoyaltyCard.findAndCountAll).toHaveBeenCalledWith(expect.objectContaining({
      limit: 10, offset: 10,
    }));
    expect(LoyaltyCard.findAll).not.toHaveBeenCalled();
  });

  test('exportar:true trae TODAS las filas vía findAll, sin limit/offset', async () => {
    const resultado = await cardService.listar({ exportar: true, page: 3, limit: 5 });

    expect(LoyaltyCard.findAll).toHaveBeenCalledWith(expect.objectContaining({
      where: expect.any(Object), order: expect.any(Array),
    }));
    expect(LoyaltyCard.findAll.mock.calls[0][0]).not.toHaveProperty('limit');
    expect(LoyaltyCard.findAll.mock.calls[0][0]).not.toHaveProperty('offset');
    expect(LoyaltyCard.findAndCountAll).not.toHaveBeenCalled();
    expect(resultado).toEqual({ total: 2, page: 1, limit: 2, pages: 1, cards: [{ id: 1 }, { id: 2 }] });
  });

  test('fechaAltaInicio/fechaAltaFin acotan createdAt (Op.gte/Op.lte), fin de día inclusivo', async () => {
    const Sequelize = require('sequelize');
    await cardService.listar({ fechaAltaInicio: '2026-10-01', fechaAltaFin: '2026-10-29' });

    const { where } = LoyaltyCard.findAndCountAll.mock.calls[0][0];
    expect(where.createdAt[Sequelize.Op.gte]).toEqual(new Date('2026-10-01'));
    expect(where.createdAt[Sequelize.Op.lte]).toEqual(new Date('2026-10-29T23:59:59.999Z'));
  });

  test('solo fechaAltaInicio (sin fin) agrega únicamente Op.gte', async () => {
    const Sequelize = require('sequelize');
    await cardService.listar({ fechaAltaInicio: '2026-10-01' });

    const { where } = LoyaltyCard.findAndCountAll.mock.calls[0][0];
    expect(where.createdAt[Sequelize.Op.gte]).toEqual(new Date('2026-10-01'));
    expect(where.createdAt[Sequelize.Op.lte]).toBeUndefined();
  });

  test('sin fechaAlta → no agrega createdAt al where', async () => {
    await cardService.listar({});

    const { where } = LoyaltyCard.findAndCountAll.mock.calls[0][0];
    expect(where.createdAt).toBeUndefined();
  });

  test('revisionPendiente:true filtra where.revisionPendiente', async () => {
    await cardService.listar({ revisionPendiente: true });

    expect(LoyaltyCard.findAndCountAll).toHaveBeenCalledWith(
      expect.objectContaining({ where: expect.objectContaining({ revisionPendiente: true }) }),
    );
  });

  test('revisionPendiente:false filtra where.revisionPendiente en false (no se confunde con "sin filtro")', async () => {
    await cardService.listar({ revisionPendiente: false });

    expect(LoyaltyCard.findAndCountAll).toHaveBeenCalledWith(
      expect.objectContaining({ where: expect.objectContaining({ revisionPendiente: false }) }),
    );
  });

  test('sin revisionPendiente → no agrega el filtro', async () => {
    await cardService.listar({});

    const { where } = LoyaltyCard.findAndCountAll.mock.calls[0][0];
    expect(where.revisionPendiente).toBeUndefined();
  });
});

describe('marcarRevisada', () => {
  test('tarjeta inexistente → 404', async () => {
    LoyaltyCard.findByPk.mockResolvedValue(null);

    await expect(cardService.marcarRevisada(999, USER)).rejects.toMatchObject({ statusCode: 404 });
  });

  test('idempotente: si ya estaba revisada (revisionPendiente:false), devuelve la tarjeta sin tocarla', async () => {
    const card = makeCard({ revisionPendiente: false });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    const resultado = await cardService.marcarRevisada(1, USER);

    expect(card.update).not.toHaveBeenCalled();
    expect(resultado).toBe(card);
  });

  test('happy path: marca revisionPendiente:false y setea revisadoPorId/Nombre/revisadoAt', async () => {
    const card = makeCard({ revisionPendiente: true });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    const resultado = await cardService.marcarRevisada(1, USER);

    expect(card.update).toHaveBeenCalledWith({
      revisionPendiente: false,
      revisadoPorId:     '7',
      revisadoPorNombre: 'Admin',
      revisadoAt:        expect.any(Date),
    });
    expect(resultado.revisionPendiente).toBe(false);
  });
});
