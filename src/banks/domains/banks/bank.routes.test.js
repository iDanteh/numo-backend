'use strict';

// bank.routes.test.js — primer test de este router (no existía ninguno). Cobertura
// mínima de la ruta nueva GET /cfdis/buscar (2026-08-07, permiso banks:cfdi:read):
// gate de permiso, filtro source='ERP' siempre fijo, match exacto vía collation
// (2026-08-12: reemplazó al regex case-insensitive original, que no podía usar
// ningún índice y terminó agotando maxTimeMS en producción), y el short-circuit
// de "sin serie ni folio". Requerir el router real no pega a Mongo — los modelos
// solo declaran esquemas, las llamadas HTTP viven dentro de handlers, nunca a
// nivel de módulo (mismo criterio ya documentado en erp.routes.test.js).
jest.mock('../../shared/middleware/auth.real', () => ({
  authenticate: (req, _res, next) => {
    req.user = {
      _id:  'user-test',
      role: 'test-role',
      extraPermissions: [],
    };
    next();
  },
  permit: (...perms) => (req, res, next) => {
    const granted = JSON.parse(req.headers['x-test-permissions'] || '[]');
    const ok = perms.every(p => granted.includes(p));
    if (!ok) {
      return res.status(403).json({ error: 'Permisos insuficientes para esta acción.', required: perms });
    }
    next();
  },
}));

// CFDI (dominio visor) mockeado — límite de I/O real de este endpoint. El mock
// replica la cadena .select().sort().limit().maxTimeMS().lean() que usa la ruta.
const mockLean = jest.fn();
jest.mock('../../../visor/models/CFDI', () => ({
  find: jest.fn(() => ({
    select:     jest.fn().mockReturnThis(),
    collation:  jest.fn().mockReturnThis(),
    sort:       jest.fn().mockReturnThis(),
    limit:      jest.fn().mockReturnThis(),
    maxTimeMS:  jest.fn().mockReturnThis(),
    lean:       mockLean,
  })),
}));

// GET /indicadores: bank-indicadores.service.js completo mockeado — esta ruta solo se
// prueba a nivel de gate de permiso + paso de query params, no de agregación (esa lógica
// tiene su propio test: bank-indicadores.service.test.js).
jest.mock('../../../shared/services/rbac-store');
jest.mock('./bank-indicadores.service', () => ({
  getIndicadoresIdentificacion: jest.fn(),
}));

const express = require('express');
const request = require('supertest');
const router  = require('./bank.routes');
const service = require('./bank.service');
const CFDI    = require('../../../visor/models/CFDI');
const rbacStore = require('../../../shared/services/rbac-store');
const indicadoresService = require('./bank-indicadores.service');
const { PERMISSIONS, MOVEMENT_SCOPE } = require('../../../shared/config/rbac');

describe('GET /cfdis/buscar', () => {
  let app;

  beforeEach(() => {
    jest.clearAllMocks();
    mockLean.mockResolvedValue([]);
    app = express();
    app.use(express.json());
    app.use('/', router);
  });

  test('responde 403 sin banks:cfdi:read', async () => {
    const res = await request(app)
      .get('/cfdis/buscar')
      .query({ serie: 'A0', folio: '123' })
      .set('x-test-permissions', JSON.stringify([]));

    expect(res.status).toBe(403);
    expect(res.body.required).toEqual([PERMISSIONS.BANKS_CFDI_READ]);
    expect(CFDI.find).not.toHaveBeenCalled();
  });

  test('sin serie ni folio: responde [] sin consultar Mongo', async () => {
    const res = await request(app)
      .get('/cfdis/buscar')
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_CFDI_READ]));

    expect(res.status).toBe(200);
    expect(res.body).toEqual([]);
    expect(CFDI.find).not.toHaveBeenCalled();
  });

  test('filtra siempre por source=ERP, match exacto (no regex) para serie/folio', async () => {
    await request(app)
      .get('/cfdis/buscar')
      .query({ serie: 'A0', folio: '123' })
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_CFDI_READ]));

    expect(CFDI.find).toHaveBeenCalledTimes(1);
    const filtroUsado = CFDI.find.mock.calls[0][0];
    expect(filtroUsado.source).toBe('ERP');
    // Igualdad exacta, sin regex — el case-insensitive lo resuelve la collation de Mongo
    // (ver .collation() abajo), no un patrón armado en JS.
    expect(filtroUsado.serie).toBe('A0');
    expect(filtroUsado.folio).toBe('123');
  });

  test('usa collation case-insensitive (strength:2) para aprovechar el índice parcial de CFDI.js', async () => {
    await request(app)
      .get('/cfdis/buscar')
      .query({ serie: 'A0', folio: '123' })
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_CFDI_READ]));

    // El mock de CFDI.find() crea una cadena nueva por invocación — hay que leer la
    // instancia real que devolvió la llamada dentro del handler, no crear una propia.
    const cadenaUsada = CFDI.find.mock.results[0].value;
    expect(cadenaUsada.collation).toHaveBeenCalledWith({ locale: 'en', strength: 2 });
  });

  test('caracteres especiales en serie/folio viajan literales (sin interpretarse como patrón)', async () => {
    await request(app)
      .get('/cfdis/buscar')
      .query({ serie: 'A.*', folio: '1(2)3' })
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_CFDI_READ]));

    const filtroUsado = CFDI.find.mock.calls[0][0];
    // Al ser igualdad exacta (no regex), no hay nada que escapar ni forma de inyectar un
    // patrón — el string se compara tal cual, literal.
    expect(filtroUsado.serie).toBe('A.*');
    expect(filtroUsado.folio).toBe('1(2)3');
  });

  test('acepta solo serie o solo folio (el otro no se agrega al filtro)', async () => {
    await request(app)
      .get('/cfdis/buscar')
      .query({ folio: '456' })
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_CFDI_READ]));

    const filtroUsado = CFDI.find.mock.calls[0][0];
    expect(filtroUsado.serie).toBeUndefined();
    expect(filtroUsado.folio).toBe('456');
  });

  test('devuelve los resultados de Mongo tal cual (uuid/serie/folio/fecha/total)', async () => {
    mockLean.mockResolvedValue([
      { uuid: 'U1', serie: 'A0', folio: '123', fecha: '2026-08-01T00:00:00.000Z', total: 1500.5 },
    ]);

    const res = await request(app)
      .get('/cfdis/buscar')
      .query({ serie: 'A0', folio: '123' })
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_CFDI_READ]));

    expect(res.status).toBe(200);
    expect(res.body).toEqual([
      { uuid: 'U1', serie: 'A0', folio: '123', fecha: '2026-08-01T00:00:00.000Z', total: 1500.5 },
    ]);
  });
});

describe('GET /indicadores', () => {
  let app;

  const FAKE_RESULT = {
    promedioHoras: 12.5,
    totalIdentificadosConDato: 3,
    backlog: { menos24h: 1, de1a3d: 0, de3a7d: 0, mas7d: 0 },
    porUsuario: [{ userId: 'user-1', nombre: 'Ana', promedioHoras: 12.5, count: 3 }],
  };

  beforeEach(() => {
    jest.clearAllMocks();
    rbacStore.hasPermission = jest.fn().mockResolvedValue(false); // sin banks:config por defecto
    indicadoresService.getIndicadoresIdentificacion.mockResolvedValue(FAKE_RESULT);
    app = express();
    app.use(express.json());
    app.use('/', router);
  });

  test('responde 403 sin banks:read', async () => {
    const res = await request(app)
      .get('/indicadores')
      .set('x-test-permissions', JSON.stringify([]));

    expect(res.status).toBe(403);
    expect(res.body.required).toEqual([PERMISSIONS.BANKS_READ]);
    expect(indicadoresService.getIndicadoresIdentificacion).not.toHaveBeenCalled();
  });

  test('con banks:read pasa banco/categoria/year/month tal cual al service', async () => {
    const res = await request(app)
      .get('/indicadores')
      .query({ banco: 'BBVA', categoria: 'Renta', year: '2026', month: '8' })
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_READ]));

    expect(res.status).toBe(200);
    expect(indicadoresService.getIndicadoresIdentificacion).toHaveBeenCalledTimes(1);
    const args = indicadoresService.getIndicadoresIdentificacion.mock.calls[0][0];
    expect(args.banco).toBe('BBVA');
    expect(args.categoria).toBe('Renta');
    expect(args.year).toBe('2026');
    expect(args.month).toBe('8');
  });

  test('no distingue scope por rol: la ruta nunca pasa restrictions al service (siempre equipo completo)', async () => {
    await request(app)
      .get('/indicadores')
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_READ]));

    const args = indicadoresService.getIndicadoresIdentificacion.mock.calls[0][0];
    expect(args.restrictions).toBeUndefined();
    expect(rbacStore.hasPermission).not.toHaveBeenCalled();
  });

  test('devuelve el shape básico del resultado del service tal cual', async () => {
    const res = await request(app)
      .get('/indicadores')
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_READ]));

    expect(res.status).toBe(200);
    expect(res.body).toEqual(FAKE_RESULT);
  });
});

// POST /movements/:id/ficha/imagen (2026-09-03) — adjunta la foto/documento de respaldo de una
// ficha ya registrada. `service.adjuntarImagenFicha` se mockea directo sobre el módulo real de
// bank.service.js (no hay jest.mock('./bank.service') a nivel de archivo, mismo criterio que el
// resto de este test file: requerir el service real es seguro, no pega a Mongo a nivel de módulo).
describe('POST /movements/:id/ficha/imagen', () => {
  let app;

  beforeEach(() => {
    jest.clearAllMocks();
    service.adjuntarImagenFicha = jest.fn();
    app = express();
    app.use(express.json());
    app.use('/', router);
  });

  test('responde 403 sin banks:ficha', async () => {
    const res = await request(app)
      .post('/movements/mov-1/ficha/imagen')
      .attach('imagen', Buffer.from('fake-image'), { filename: 'ficha.png', contentType: 'image/png' })
      .set('x-test-permissions', JSON.stringify([]));

    expect(res.status).toBe(403);
    expect(service.adjuntarImagenFicha).not.toHaveBeenCalled();
  });

  test('responde 400 si no se envía ningún archivo', async () => {
    const res = await request(app)
      .post('/movements/mov-1/ficha/imagen')
      .set('x-test-permissions', JSON.stringify(['banks:ficha']));

    expect(res.status).toBe(400);
    expect(service.adjuntarImagenFicha).not.toHaveBeenCalled();
  });

  test('con archivo y permiso, delega en service.adjuntarImagenFicha con el archivo recibido', async () => {
    service.adjuntarImagenFicha.mockResolvedValue({
      _id: 'mov-1', fichaDriveFileId: 'file-1', fichaDriveWebViewLink: 'https://drive/file-1',
    });

    const res = await request(app)
      .post('/movements/mov-1/ficha/imagen')
      .attach('imagen', Buffer.from('fake-image'), { filename: 'ficha.png', contentType: 'image/png' })
      .set('x-test-permissions', JSON.stringify(['banks:ficha']));

    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      _id: 'mov-1', fichaDriveFileId: 'file-1', fichaDriveWebViewLink: 'https://drive/file-1',
    });
    expect(service.adjuntarImagenFicha).toHaveBeenCalledTimes(1);
    const [id, file] = service.adjuntarImagenFicha.mock.calls[0];
    expect(id).toBe('mov-1');
    expect(file.originalname).toBe('ficha.png');
    expect(file.mimetype).toBe('image/png');
    expect(Buffer.isBuffer(file.buffer)).toBe(true);
  });
});

// GET /movements/:id/ficha/imagen (2026-09-03) — proxy autenticado de la imagen/PDF de
// respaldo de la ficha, permiso banks:read (no banks:ficha, ver comentario en bank.routes.js).
describe('GET /movements/:id/ficha/imagen', () => {
  let app;

  beforeEach(() => {
    jest.clearAllMocks();
    service.obtenerImagenFicha = jest.fn();
    app = express();
    app.use(express.json());
    app.use('/', router);
  });

  test('responde 403 sin banks:read', async () => {
    const res = await request(app)
      .get('/movements/mov-1/ficha/imagen')
      .set('x-test-permissions', JSON.stringify([]));

    expect(res.status).toBe(403);
    expect(service.obtenerImagenFicha).not.toHaveBeenCalled();
  });

  test('con banks:read, responde 200 con el Content-Type correcto y delega en el service', async () => {
    service.obtenerImagenFicha.mockResolvedValue({ data: Buffer.from('contenido'), mimetype: 'image/png' });

    const res = await request(app)
      .get('/movements/mov-1/ficha/imagen')
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_READ]));

    expect(res.status).toBe(200);
    expect(res.headers['content-type']).toBe('image/png');
    expect(service.obtenerImagenFicha).toHaveBeenCalledWith('mov-1');
    expect(res.body).toEqual(Buffer.from('contenido'));
  });
});
