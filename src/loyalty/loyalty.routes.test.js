'use strict';

// loyalty.routes.test.js — PR4/8 sistema-lealtad. Solo gate de permiso + wiring
// (mismo criterio que system-monitor.routes.test.js/bank.routes.test.js): cada
// servicio se mockea completo, la lógica real vive y se prueba en sus propios
// *.service.test.js (PR2/PR3). Este archivo cubre la matriz de independencia de
// permisos exigida por specs/loyalty-permissions/spec.md — cada uno de los 7
// `loyalty:*` debe gatear SOLO su propia acción.
jest.mock('../shared/middleware/auth', () => ({
  authenticate: (req, _res, next) => {
    req.user = { _id: 'user-test', dbId: '1', nombre: 'Tester', role: 'test-role', extraPermissions: [] };
    next();
  },
  permit: (...perms) => (req, res, next) => {
    const granted = JSON.parse(req.headers['x-test-permissions'] || '[]');
    const ok = perms.every((p) => granted.includes(p));
    if (!ok) {
      return res.status(403).json({ error: 'Permisos insuficientes para esta acción.', required: perms });
    }
    next();
  },
}));

jest.mock('./domains/cards/loyalty-card.service', () => ({
  crearTarjeta: jest.fn(),
  listar: jest.fn(),
  obtener: jest.fn(),
  obtenerParaEditar: jest.fn(),
  editarTarjeta: jest.fn(),
  cambiarNip: jest.fn(),
  cambiarNivel: jest.fn(),
  subirFoto: jest.fn(),
  obtenerFoto: jest.fn(),
  listarPendientesRevisionKore: jest.fn(),
  marcarRevisada: jest.fn(),
}));
jest.mock('./domains/ledger/loyalty-ledger.service', () => ({
  registrarAcumulacion: jest.fn(),
  registrarCanje: jest.fn(),
  registrarAjuste: jest.fn(),
  listarMovimientos: jest.fn(),
  listarNiveles: jest.fn(),
  listarMovimientosGlobal: jest.fn(),
}));
jest.mock('./domains/config/loyalty-config.service', () => ({
  obtenerPorcentajes: jest.fn(),
  guardarPorcentajes: jest.fn(),
  obtenerEspecialidades: jest.fn(),
  guardarEspecialidades: jest.fn(),
  obtenerFotoFolderId: jest.fn(),
  guardarFotoFolderId: jest.fn(),
  listarReasons: jest.fn(),
  crearReason: jest.fn(),
  editarReason: jest.fn(),
  listarSucursales: jest.fn(),
  actualizarSucursal: jest.fn(),
  listarPromociones: jest.fn(),
  crearPromocion: jest.fn(),
  editarPromocion: jest.fn(),
  listarConceptosKore: jest.fn(),
  sincronizarConceptosKore: jest.fn(),
  actualizarConceptoKore: jest.fn(),
  listarFormasPagoKore: jest.fn(),
  sincronizarFormasPagoKore: jest.fn(),
  actualizarFormaPagoKore: jest.fn(),
}));

const express = require('express');
const request = require('supertest');
const router = require('./loyalty.routes');
const cardService = require('./domains/cards/loyalty-card.service');
const ledgerService = require('./domains/ledger/loyalty-ledger.service');
const configService = require('./domains/config/loyalty-config.service');
const { PERMISSIONS } = require('../shared/config/rbac');

function perms(...list) {
  return { 'x-test-permissions': JSON.stringify(list) };
}

let app;
beforeEach(() => {
  jest.clearAllMocks();
  // Default sano para la matriz de permisos genérica de abajo — a diferencia del resto de rutas
  // mockeadas (que solo hacen res.json(await ...), inofensivo con undefined), GET /cards/:id/foto
  // desestructura { data, mimetype } del resultado.
  cardService.obtenerFoto.mockResolvedValue({ data: Buffer.from('img'), mimetype: 'image/png' });
  app = express();
  app.use(express.json());
  app.use('/', router);
});

// ── Matriz: cada ruta exige EXACTAMENTE su permiso, ningún otro lo sustituye ──
describe('matriz de permisos — independencia (specs/loyalty-permissions)', () => {
  const casos = [
    { metodo: 'get',   ruta: '/cards',                    permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'get',   ruta: '/cards/pendientes-revision-kore', permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'patch', ruta: '/cards/1/marcar-revisada',  permiso: PERMISSIONS.LOYALTY_TARJETAS_EDITAR },
    { metodo: 'post',  ruta: '/cards',                     permiso: PERMISSIONS.LOYALTY_TARJETAS_CREAR, body: { telefono: '5512345678', nombre: 'Ana' } },
    { metodo: 'get',   ruta: '/cards/1',                   permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'get',   ruta: '/cards/1/movimientos',       permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'get',   ruta: '/cards/1/niveles',           permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'get',   ruta: '/ledger/export',             permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'patch', ruta: '/cards/1',                   permiso: PERMISSIONS.LOYALTY_TARJETAS_EDITAR, body: { nombre: 'Ana' } },
    { metodo: 'post',  ruta: '/cards/1/nip',                permiso: PERMISSIONS.LOYALTY_TARJETAS_EDITAR },
    { metodo: 'get',   ruta: '/cards/1/foto',               permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'put',   ruta: '/cards/1/nivel',              permiso: PERMISSIONS.LOYALTY_TARJETAS_NIVEL, body: { nivel: 'Plata' } },
    { metodo: 'post',  ruta: '/cards/1/acumulaciones',      permiso: PERMISSIONS.LOYALTY_MOVIMIENTOS, body: { montoVenta: 100, serieFolio: 'A0-123' } },
    { metodo: 'post',  ruta: '/cards/1/canjes',             permiso: PERMISSIONS.LOYALTY_MOVIMIENTOS, body: { puntos: 10, serieFolio: 'A0-123' } },
    { metodo: 'post',  ruta: '/cards/1/ajustes',            permiso: PERMISSIONS.LOYALTY_AJUSTES, body: { puntos: 10, reasonId: 1, justificacion: 'bono' } },
    { metodo: 'get',   ruta: '/config',                     permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'put',   ruta: '/config/porcentajes',         permiso: PERMISSIONS.LOYALTY_CONFIG, body: { Bronce: 1 } },
    { metodo: 'get',   ruta: '/config/especialidades',      permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'put',   ruta: '/config/especialidades',      permiso: PERMISSIONS.LOYALTY_CONFIG, body: { especialidades: ['a'] } },
    { metodo: 'get',   ruta: '/config/foto-folder',         permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'put',   ruta: '/config/foto-folder',         permiso: PERMISSIONS.LOYALTY_CONFIG, body: { folderId: 'abc123' } },
    { metodo: 'get',   ruta: '/reasons',                    permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'post',  ruta: '/reasons',                    permiso: PERMISSIONS.LOYALTY_CONFIG, body: { clave: 'x', nombre: 'x' } },
    { metodo: 'patch', ruta: '/reasons/1',                  permiso: PERMISSIONS.LOYALTY_CONFIG, body: { activo: false } },
    { metodo: 'get',   ruta: '/sucursales',                 permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'patch', ruta: '/sucursales/1',                permiso: PERMISSIONS.LOYALTY_CONFIG, body: { participaLealtad: false } },
    { metodo: 'get',   ruta: '/promociones',                 permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'post',  ruta: '/promociones',                 permiso: PERMISSIONS.LOYALTY_CONFIG, body: { nombre: 'Buen Fin', fechaInicio: '2026-11-01', fechaFin: '2026-11-17', porcentajeBono: 5 } },
    { metodo: 'patch', ruta: '/promociones/1',                permiso: PERMISSIONS.LOYALTY_CONFIG, body: { activa: false } },
    { metodo: 'get',   ruta: '/config/conceptos-kore',         permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'post',  ruta: '/config/conceptos-kore/sync',    permiso: PERMISSIONS.LOYALTY_CONFIG },
    { metodo: 'patch', ruta: '/config/conceptos-kore/1',       permiso: PERMISSIONS.LOYALTY_CONFIG, body: { acumulaPuntos: true } },
    { metodo: 'get',   ruta: '/config/formas-pago-kore',       permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'post',  ruta: '/config/formas-pago-kore/sync',  permiso: PERMISSIONS.LOYALTY_CONFIG },
    { metodo: 'patch', ruta: '/config/formas-pago-kore/1',     permiso: PERMISSIONS.LOYALTY_CONFIG, body: { esPuntos: true } },
  ];

  const TODOS = Object.values(PERMISSIONS).filter((p) => p.startsWith('loyalty:'));

  test.each(casos)('$metodo $ruta exige $permiso y rechaza con cualquier OTRO permiso de loyalty', async ({ metodo, ruta, permiso, body }) => {
    // Con TODOS los demás permisos de loyalty menos el requerido → 403
    const otros = TODOS.filter((p) => p !== permiso);
    let req = request(app)[metodo](ruta).set(perms(...otros));
    if (body) req = req.send(body);
    const sinElSuyo = await req;
    expect(sinElSuyo.status).toBe(403);

    // Con el permiso exacto → nunca 403 (puede ser 200/201 según el mock)
    let req2 = request(app)[metodo](ruta).set(perms(permiso));
    if (body) req2 = req2.send(body);
    const conElSuyo = await req2;
    expect(conElSuyo.status).not.toBe(403);
  });
});

describe('loyalty:ajustes y loyalty:tarjetas:nivel no se implican entre sí', () => {
  test('ajustes NO permite cambiar nivel', async () => {
    const res = await request(app).put('/cards/1/nivel')
      .set(perms(PERMISSIONS.LOYALTY_AJUSTES))
      .send({ nivel: 'Plata' });
    expect(res.status).toBe(403);
  });

  test('tarjetas:nivel NO permite crear un ajuste', async () => {
    const res = await request(app).post('/cards/1/ajustes')
      .set(perms(PERMISSIONS.LOYALTY_TARJETAS_NIVEL))
      .send({ puntos: 10, reasonId: 1, justificacion: 'bono' });
    expect(res.status).toBe(403);
  });
});

describe('read-only (solo loyalty:read) no puede escribir nada', () => {
  test('no puede crear tarjeta', async () => {
    const res = await request(app).post('/cards')
      .set(perms(PERMISSIONS.LOYALTY_READ))
      .send({ telefono: '5512345678', nombre: 'Ana' });
    expect(res.status).toBe(403);
  });

  test('no puede registrar acumulación ni canje', async () => {
    const r1 = await request(app).post('/cards/1/acumulaciones').set(perms(PERMISSIONS.LOYALTY_READ)).send({ serieFolio: 'A0-1' });
    const r2 = await request(app).post('/cards/1/canjes').set(perms(PERMISSIONS.LOYALTY_READ)).send({ puntos: 1, serieFolio: 'A0-1' });
    expect(r1.status).toBe(403);
    expect(r2.status).toBe(403);
  });

  test('no puede ajustar, cambiar nivel, ni escribir config', async () => {
    const r1 = await request(app).post('/cards/1/ajustes').set(perms(PERMISSIONS.LOYALTY_READ)).send({ puntos: 1, reasonId: 1, justificacion: 'bono' });
    const r2 = await request(app).put('/cards/1/nivel').set(perms(PERMISSIONS.LOYALTY_READ)).send({ nivel: 'Plata' });
    const r3 = await request(app).put('/config/porcentajes').set(perms(PERMISSIONS.LOYALTY_READ)).send({ Bronce: 1 });
    expect(r1.status).toBe(403);
    expect(r2.status).toBe(403);
    expect(r3.status).toBe(403);
  });
});

describe('wiring hacia los servicios (happy path)', () => {
  test('POST /cards llama a crearTarjeta y devuelve 201', async () => {
    cardService.crearTarjeta.mockResolvedValue({ card: { id: 1 }, nip: '5678' });
    const res = await request(app).post('/cards')
      .set(perms(PERMISSIONS.LOYALTY_TARJETAS_CREAR))
      .send({ telefono: '5512345678', nombre: 'Ana' });
    expect(res.status).toBe(201);
    expect(res.body).toEqual({ card: { id: 1 }, nip: '5678' });
    expect(cardService.crearTarjeta).toHaveBeenCalledWith(
      { telefono: '5512345678', nombre: 'Ana' },
      expect.objectContaining({ dbId: '1' }),
    );
  });

  test('GET /cards/:id/editar llama a obtenerParaEditar y exige loyalty:tarjetas:editar, no loyalty:read', async () => {
    cardService.obtenerParaEditar.mockResolvedValue({ id: 42, apodo: 'Juancho' });

    const sinPermiso = await request(app).get('/cards/42/editar').set(perms(PERMISSIONS.LOYALTY_READ));
    expect(sinPermiso.status).toBe(403);

    const res = await request(app).get('/cards/42/editar').set(perms(PERMISSIONS.LOYALTY_TARJETAS_EDITAR));
    expect(res.status).toBe(200);
    expect(cardService.obtenerParaEditar).toHaveBeenCalledWith('42');
  });

  test('GET /cards/:id/movimientos llama a listarMovimientos con el id', async () => {
    ledgerService.listarMovimientos.mockResolvedValue({ movimientos: [] });
    const res = await request(app).get('/cards/42/movimientos').set(perms(PERMISSIONS.LOYALTY_READ));
    expect(res.status).toBe(200);
    expect(ledgerService.listarMovimientos).toHaveBeenCalledWith('42', expect.any(Object));
  });

  // Reporte "Movimientos" (2026-10-29).
  test('GET /ledger/export pasa los filtros de query a listarMovimientosGlobal y responde { movimientos }', async () => {
    ledgerService.listarMovimientosGlobal.mockResolvedValue([{ id: 1 }, { id: 2 }]);

    const res = await request(app).get(
      '/ledger/export?fechaInicio=2026-10-01&fechaFin=2026-10-29&tipo=ACUMULACION,CANJE'
      + '&nivelAplicado=Oro&origen=kore&reasonId=7&usuarioNombre=Ana&puntosMin=-50&puntosMax=100',
    ).set(perms(PERMISSIONS.LOYALTY_READ));

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ movimientos: [{ id: 1 }, { id: 2 }] });
    expect(ledgerService.listarMovimientosGlobal).toHaveBeenCalledWith({
      fechaInicio: '2026-10-01', fechaFin: '2026-10-29', tipo: 'ACUMULACION,CANJE',
      nivelAplicado: 'Oro', origen: 'kore', reasonId: '7', usuarioNombre: 'Ana',
      puntosMin: '-50', puntosMax: '100',
    });
  });

  // Reporte "Tarjetas" (2026-10-29).
  test('GET /cards pasa exportar/fechaAlta/revisionPendiente al servicio', async () => {
    cardService.listar.mockResolvedValue({ total: 0, cards: [] });

    const res = await request(app).get(
      '/cards?exportar=true&fechaAltaInicio=2026-10-01&fechaAltaFin=2026-10-29&revisionPendiente=true',
    ).set(perms(PERMISSIONS.LOYALTY_READ));

    expect(res.status).toBe(200);
    expect(cardService.listar).toHaveBeenCalledWith(expect.objectContaining({
      exportar: true, fechaAltaInicio: '2026-10-01', fechaAltaFin: '2026-10-29', revisionPendiente: true,
    }));
  });

  test('GET /cards con revisionPendiente=false pasa false explícito (no undefined)', async () => {
    cardService.listar.mockResolvedValue({ total: 0, cards: [] });

    await request(app).get('/cards?revisionPendiente=false').set(perms(PERMISSIONS.LOYALTY_READ));

    expect(cardService.listar).toHaveBeenCalledWith(expect.objectContaining({ revisionPendiente: false }));
  });

  test('GET /cards sin exportar ni revisionPendiente: exportar=false, revisionPendiente=undefined', async () => {
    cardService.listar.mockResolvedValue({ total: 0, cards: [] });

    await request(app).get('/cards').set(perms(PERMISSIONS.LOYALTY_READ));

    expect(cardService.listar).toHaveBeenCalledWith(expect.objectContaining({
      exportar: false, revisionPendiente: undefined,
    }));
  });

  test('PUT /config/porcentajes llama a guardarPorcentajes', async () => {
    configService.guardarPorcentajes.mockResolvedValue({ Bronce: 1 });
    const res = await request(app).put('/config/porcentajes')
      .set(perms(PERMISSIONS.LOYALTY_CONFIG))
      .send({ Bronce: 1 });
    expect(res.status).toBe(200);
    expect(configService.guardarPorcentajes).toHaveBeenCalledWith({ Bronce: 1 }, expect.any(Object));
  });

  test('POST /cards/:id/ajustes pasa puntos/reasonId/justificacion al servicio', async () => {
    ledgerService.registrarAjuste.mockResolvedValue({ movimiento: { id: 1 } });
    const res = await request(app).post('/cards/1/ajustes')
      .set(perms(PERMISSIONS.LOYALTY_AJUSTES))
      .send({ puntos: '20.00', reasonId: 10, justificacion: 'ajuste manual' });
    expect(res.status).toBe(201);
    expect(ledgerService.registrarAjuste).toHaveBeenCalledWith(
      { cardId: '1', puntos: '20.00', reasonId: 10, justificacion: 'ajuste manual' },
      expect.any(Object),
    );
  });

  test('GET /config/foto-folder llama a obtenerFotoFolderId', async () => {
    configService.obtenerFotoFolderId.mockResolvedValue('abc123');
    const res = await request(app).get('/config/foto-folder').set(perms(PERMISSIONS.LOYALTY_READ));
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ folderId: 'abc123' });
    expect(configService.obtenerFotoFolderId).toHaveBeenCalled();
  });

  test('PUT /config/foto-folder llama a guardarFotoFolderId con folderId y el usuario', async () => {
    configService.guardarFotoFolderId.mockResolvedValue('nuevo-id');
    const res = await request(app).put('/config/foto-folder')
      .set(perms(PERMISSIONS.LOYALTY_CONFIG))
      .send({ folderId: 'nuevo-id' });
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ folderId: 'nuevo-id' });
    expect(configService.guardarFotoFolderId).toHaveBeenCalledWith('nuevo-id', expect.objectContaining({ dbId: '1' }));
  });

  test('GET /cards/pendientes-revision-kore pasa search al servicio', async () => {
    cardService.listarPendientesRevisionKore.mockResolvedValue({ total: 0, cards: [] });
    const res = await request(app).get('/cards/pendientes-revision-kore?search=juan')
      .set(perms(PERMISSIONS.LOYALTY_READ));
    expect(res.status).toBe(200);
    expect(cardService.listarPendientesRevisionKore).toHaveBeenCalledWith(
      expect.objectContaining({ search: 'juan' }),
    );
  });

  test('GET /promociones llama a listarPromociones con soloActivas', async () => {
    configService.listarPromociones.mockResolvedValue([]);
    const res = await request(app).get('/promociones?soloActivas=true').set(perms(PERMISSIONS.LOYALTY_READ));
    expect(res.status).toBe(200);
    expect(configService.listarPromociones).toHaveBeenCalledWith({ soloActivas: true });
  });

  test('POST /promociones llama a crearPromocion y devuelve 201', async () => {
    configService.crearPromocion.mockResolvedValue({ id: 1 });
    const res = await request(app).post('/promociones')
      .set(perms(PERMISSIONS.LOYALTY_CONFIG))
      .send({ nombre: 'Buen Fin', fechaInicio: '2026-11-01', fechaFin: '2026-11-17', porcentajeBono: 5 });
    expect(res.status).toBe(201);
    expect(configService.crearPromocion).toHaveBeenCalledWith(
      expect.objectContaining({ nombre: 'Buen Fin' }),
    );
  });

  test('PATCH /promociones/:id llama a editarPromocion', async () => {
    configService.editarPromocion.mockResolvedValue({ id: 1, activa: false });
    const res = await request(app).patch('/promociones/1')
      .set(perms(PERMISSIONS.LOYALTY_CONFIG))
      .send({ activa: false });
    expect(res.status).toBe(200);
    expect(configService.editarPromocion).toHaveBeenCalledWith('1', { activa: false });
  });

  test('GET /config/conceptos-kore llama a listarConceptosKore con soloActivos', async () => {
    configService.listarConceptosKore.mockResolvedValue([]);
    const res = await request(app).get('/config/conceptos-kore?soloActivos=true')
      .set(perms(PERMISSIONS.LOYALTY_READ));
    expect(res.status).toBe(200);
    expect(configService.listarConceptosKore).toHaveBeenCalledWith({ soloActivos: true });
  });

  test('POST /config/conceptos-kore/sync llama a sincronizarConceptosKore', async () => {
    configService.sincronizarConceptosKore.mockResolvedValue({ nuevos: 1, actualizados: 0, desactivados: 0 });
    const res = await request(app).post('/config/conceptos-kore/sync')
      .set(perms(PERMISSIONS.LOYALTY_CONFIG));
    expect(res.status).toBe(200);
    expect(configService.sincronizarConceptosKore).toHaveBeenCalled();
  });

  test('PATCH /config/conceptos-kore/:id llama a actualizarConceptoKore', async () => {
    configService.actualizarConceptoKore.mockResolvedValue({ id: 1, acumulaPuntos: true });
    const res = await request(app).patch('/config/conceptos-kore/1')
      .set(perms(PERMISSIONS.LOYALTY_CONFIG))
      .send({ acumulaPuntos: true });
    expect(res.status).toBe(200);
    expect(configService.actualizarConceptoKore).toHaveBeenCalledWith('1', { acumulaPuntos: true });
  });

  test('GET /config/formas-pago-kore llama a listarFormasPagoKore con soloActivos', async () => {
    configService.listarFormasPagoKore.mockResolvedValue([]);
    const res = await request(app).get('/config/formas-pago-kore')
      .set(perms(PERMISSIONS.LOYALTY_READ));
    expect(res.status).toBe(200);
    expect(configService.listarFormasPagoKore).toHaveBeenCalledWith({ soloActivos: false });
  });

  test('POST /config/formas-pago-kore/sync llama a sincronizarFormasPagoKore', async () => {
    configService.sincronizarFormasPagoKore.mockResolvedValue({ nuevos: 0, actualizados: 1, desactivados: 0 });
    const res = await request(app).post('/config/formas-pago-kore/sync')
      .set(perms(PERMISSIONS.LOYALTY_CONFIG));
    expect(res.status).toBe(200);
    expect(configService.sincronizarFormasPagoKore).toHaveBeenCalled();
  });

  test('PATCH /config/formas-pago-kore/:id llama a actualizarFormaPagoKore', async () => {
    configService.actualizarFormaPagoKore.mockResolvedValue({ id: 1, esPuntos: true });
    const res = await request(app).patch('/config/formas-pago-kore/1')
      .set(perms(PERMISSIONS.LOYALTY_CONFIG))
      .send({ esPuntos: true });
    expect(res.status).toBe(200);
    expect(configService.actualizarFormaPagoKore).toHaveBeenCalledWith('1', { esPuntos: true });
  });
});

// POST /cards/:id/foto (2026-10-29) — foto del cliente, 100% capturada en Numo (cámara del
// staff). Mismo criterio de test que POST /movements/:id/ficha/imagen en bank.routes.test.js:
// requiere .attach() (multipart), separado de la matriz de permisos de arriba (esa manda JSON).
describe('POST /cards/:id/foto', () => {
  test('responde 403 sin loyalty:tarjetas:editar', async () => {
    const res = await request(app)
      .post('/cards/1/foto')
      .attach('foto', Buffer.from('fake-image'), { filename: 'foto.png', contentType: 'image/png' })
      .set(perms());

    expect(res.status).toBe(403);
    expect(cardService.subirFoto).not.toHaveBeenCalled();
  });

  test('loyalty:read NO sustituye a loyalty:tarjetas:editar', async () => {
    const res = await request(app)
      .post('/cards/1/foto')
      .attach('foto', Buffer.from('fake-image'), { filename: 'foto.png', contentType: 'image/png' })
      .set(perms(PERMISSIONS.LOYALTY_READ));

    expect(res.status).toBe(403);
    expect(cardService.subirFoto).not.toHaveBeenCalled();
  });

  test('responde 400 si no se envía ningún archivo', async () => {
    const res = await request(app)
      .post('/cards/1/foto')
      .set(perms(PERMISSIONS.LOYALTY_TARJETAS_EDITAR));

    expect(res.status).toBe(400);
    expect(cardService.subirFoto).not.toHaveBeenCalled();
  });

  test('con archivo y permiso, delega en cardService.subirFoto con el archivo recibido', async () => {
    cardService.subirFoto.mockResolvedValue({ id: 1, fotoUrl: 'https://drive/file-1' });

    const res = await request(app)
      .post('/cards/1/foto')
      .attach('foto', Buffer.from('fake-image'), { filename: 'foto.png', contentType: 'image/png' })
      .set(perms(PERMISSIONS.LOYALTY_TARJETAS_EDITAR));

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ id: 1, fotoUrl: 'https://drive/file-1' });
    expect(cardService.subirFoto).toHaveBeenCalledTimes(1);
    const [id, file] = cardService.subirFoto.mock.calls[0];
    expect(id).toBe('1');
    expect(file.originalname).toBe('foto.png');
    expect(file.mimetype).toBe('image/png');
    expect(Buffer.isBuffer(file.buffer)).toBe(true);
  });
});

// GET /cards/:id/foto (2026-10-29) — bug real corregido: card.fotoUrl es el webViewLink de la
// página visor de Drive, nunca embebible en un <img src>. Este proxy autenticado sirve el binario
// real. Permiso loyalty:read (no loyalty:tarjetas:editar) — ver la foto es lectura, mismo criterio
// que GET /movements/:id/ficha/imagen en bank.routes.js (banks:read, no banks:ficha).
describe('GET /cards/:id/foto', () => {
  test('responde 403 sin loyalty:read', async () => {
    const res = await request(app).get('/cards/1/foto').set(perms());

    expect(res.status).toBe(403);
    expect(cardService.obtenerFoto).not.toHaveBeenCalled();
  });

  test('con loyalty:read, sirve el binario con el Content-Type correcto', async () => {
    cardService.obtenerFoto.mockResolvedValue({ data: Buffer.from('contenido-binario'), mimetype: 'image/png' });

    const res = await request(app).get('/cards/1/foto').set(perms(PERMISSIONS.LOYALTY_READ));

    expect(res.status).toBe(200);
    expect(res.headers['content-type']).toBe('image/png');
    expect(res.body).toEqual(Buffer.from('contenido-binario'));
    expect(cardService.obtenerFoto).toHaveBeenCalledWith('1');
  });

  test('sin mimetype guardado, cae a application/octet-stream', async () => {
    cardService.obtenerFoto.mockResolvedValue({ data: Buffer.from('x'), mimetype: null });

    const res = await request(app).get('/cards/1/foto').set(perms(PERMISSIONS.LOYALTY_READ));

    expect(res.status).toBe(200);
    expect(res.headers['content-type']).toBe('application/octet-stream');
  });
});

// GET /cards/pendientes-revision-kore + PATCH /cards/:id/marcar-revisada (2026-10-29) — altas del
// webhook de catálogo de clientes de Kore marcadas `revisionPendiente: true`
// (loyalty-kore-cliente.service.js#_procesarAlta).
describe('GET /cards/pendientes-revision-kore', () => {
  test('exige loyalty:read y delega en listarPendientesRevisionKore', async () => {
    cardService.listarPendientesRevisionKore.mockResolvedValue({ total: 0, page: 1, limit: 50, pages: 0, cards: [] });

    const sinPermiso = await request(app).get('/cards/pendientes-revision-kore').set(perms());
    expect(sinPermiso.status).toBe(403);

    const res = await request(app).get('/cards/pendientes-revision-kore').set(perms(PERMISSIONS.LOYALTY_READ));
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ total: 0, page: 1, limit: 50, pages: 0, cards: [] });
    expect(cardService.listarPendientesRevisionKore).toHaveBeenCalledWith(
      expect.objectContaining({ page: undefined, limit: undefined }),
    );
  });

  test('ruta estática: no la captura GET /cards/:id (obtener no se llama)', async () => {
    cardService.listarPendientesRevisionKore.mockResolvedValue({ total: 0, page: 1, limit: 50, pages: 0, cards: [] });

    await request(app).get('/cards/pendientes-revision-kore').set(perms(PERMISSIONS.LOYALTY_READ));

    expect(cardService.obtener).not.toHaveBeenCalled();
  });
});

describe('PATCH /cards/:id/marcar-revisada', () => {
  test('exige loyalty:tarjetas:editar, no loyalty:read', async () => {
    cardService.marcarRevisada.mockResolvedValue({ id: 42, revisionPendiente: false });

    const sinPermiso = await request(app).patch('/cards/42/marcar-revisada').set(perms(PERMISSIONS.LOYALTY_READ));
    expect(sinPermiso.status).toBe(403);

    const res = await request(app).patch('/cards/42/marcar-revisada').set(perms(PERMISSIONS.LOYALTY_TARJETAS_EDITAR));
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ id: 42, revisionPendiente: false });
    expect(cardService.marcarRevisada).toHaveBeenCalledWith('42', expect.objectContaining({ dbId: '1' }));
  });
});
