'use strict';

// Fix de seguridad 2026-10-14: el canal de Socket.IO no exigía ningún token para
// conectarse. Estos tests cubren el shim que adapta `authenticate` (middleware
// Express real, sin reimplementar verificación de JWT) a Socket.IO vía
// `_io.use(_authenticateSocket)`.
jest.mock('../../shared/middleware/auth', () => ({
  authenticate: jest.fn(),
}));

// Adapter Redis (consultoría de desempeño 2026-11-11, cluster nativo + Redis) — mockea
// `redis`/`@socket.io/redis-adapter`/`socket.io` para que `init()` siga siendo testeable
// sin un Redis ni un Server real. `createClient()` devuelve un cliente nuevo cada vez
// (y `.duplicate()` devuelve OTRO cliente nuevo), igual que el real: `_pubClient` y
// `_subClient` son objetos distintos.
jest.mock('redis', () => {
  function makeFakeClient() {
    const client = {
      connect: jest.fn().mockResolvedValue(undefined),
      quit: jest.fn().mockResolvedValue(undefined),
      on: jest.fn(),
      sendCommand: jest.fn(),
    };
    client.duplicate = jest.fn(() => makeFakeClient());
    return client;
  }
  return { createClient: jest.fn(() => makeFakeClient()) };
});

jest.mock('@socket.io/redis-adapter', () => ({
  // El contenido real del adapter no le importa a este test — solo que sea el
  // valor que termina en `adapter:` del constructor de `Server`.
  createAdapter: jest.fn((pubClient, subClient) => ({ __fakeAdapter: true, pubClient, subClient })),
}));

jest.mock('socket.io', () => {
  const ServerMock = jest.fn().mockImplementation(function ServerCtor(httpServer, opts) {
    this.httpServer = httpServer;
    this.opts = opts;
    this.use = jest.fn();
    this.on = jest.fn();
    this.to = jest.fn().mockReturnThis();
    this.emit = jest.fn();
  });
  return { Server: ServerMock };
});

const { authenticate } = require('../../shared/middleware/auth');
const { createClient } = require('redis');
const { createAdapter } = require('@socket.io/redis-adapter');
const { Server } = require('socket.io');
const socketModule = require('./socket');
const { _authenticateSocket } = socketModule;

function buildSocket(token) {
  return { handshake: { auth: { token } }, data: {} };
}

describe('_authenticateSocket', () => {
  afterEach(() => jest.clearAllMocks());

  test('token válido: puebla socket.data.user y llama a next() sin argumentos', () => {
    const fakeUser = { _id: 'auth0|123', role: 'cobranza', extraPermissions: [] };
    authenticate.mockImplementation((req, _res, next) => {
      req.user = fakeUser;
      next();
    });

    const socket = buildSocket('token-valido');
    const next   = jest.fn();
    _authenticateSocket(socket, next);

    expect(socket.data.user).toEqual(fakeUser);
    expect(next).toHaveBeenCalledWith();
    expect(authenticate).toHaveBeenCalledWith(
      expect.objectContaining({ headers: { authorization: 'Bearer token-valido' } }),
      expect.anything(),
      expect.any(Function),
    );
  });

  test('token inválido/ausente: authenticate responde con error vía res.json() → next(Error)', () => {
    authenticate.mockImplementation((_req, res, _next) => {
      // Mismo contrato que el authenticate real: en fallo responde, nunca llama a next().
      res.status(401).json({ error: 'Token inválido' });
    });

    const socket = buildSocket(undefined);
    const next   = jest.fn();
    _authenticateSocket(socket, next);

    expect(socket.data.user).toBeUndefined();
    expect(next).toHaveBeenCalledTimes(1);
    const errArg = next.mock.calls[0][0];
    expect(errArg).toBeInstanceOf(Error);
    expect(errArg.message).toBe('Token inválido');
  });

  // Bug real 2026-10-14 (reproducido en consola, fuera de este mock): el shim
  // no tenía `req.is()` ni `req.method` — authenticate() en sí pasó el mock de
  // arriba, pero la librería REAL de Auth0 (express-oauth2-jwt-bearer) los usa
  // internamente y tiraba "TypeError: req.is is not a function" /
  // "Invalid HTTP method received in request". Este mock no llama a la
  // librería real, así que no lo detecta por sí solo — por eso se afirma acá
  // el contrato completo del shim en vez de solo lo que authenticate() toca.
  test('el req shim expone is()/method/get() — contrato que exige express-oauth2-jwt-bearer, no solo authenticate()', () => {
    authenticate.mockImplementation((req, _res, next) => {
      expect(typeof req.is).toBe('function');
      expect(req.is('urlencoded')).toBe(false);
      expect(typeof req.method).toBe('string');
      expect(req.method.length).toBeGreaterThan(0);
      expect(typeof req.get).toBe('function');
      req.user = { _id: 'auth0|123', role: 'cobranza', extraPermissions: [] };
      next();
    });

    _authenticateSocket(buildSocket('token-valido'), jest.fn());
    expect(authenticate).toHaveBeenCalled();
  });
});

describe('init() — adapter Redis (cluster nativo + Redis, 2026-11-11)', () => {
  afterEach(() => jest.clearAllMocks());

  test('es async, conecta pubClient y subClient (un `duplicate()` distinto), y pasa el adapter ya armado al constructor de Server', async () => {
    const fakeHttpServer = {};

    const resultado = socketModule.init(fakeHttpServer);
    expect(resultado).toBeInstanceOf(Promise);
    const io = await resultado;

    expect(createClient).toHaveBeenCalledTimes(1);
    const pubClient = createClient.mock.results[0].value;
    expect(pubClient.duplicate).toHaveBeenCalledTimes(1);
    const subClient = pubClient.duplicate.mock.results[0].value;
    expect(subClient).not.toBe(pubClient);

    expect(pubClient.connect).toHaveBeenCalledTimes(1);
    expect(subClient.connect).toHaveBeenCalledTimes(1);

    expect(createAdapter).toHaveBeenCalledWith(pubClient, subClient);
    const adapterDevuelto = createAdapter.mock.results[0].value;

    expect(Server).toHaveBeenCalledTimes(1);
    const [httpServerArg, opts] = Server.mock.calls[0];
    expect(httpServerArg).toBe(fakeHttpServer);
    expect(opts.adapter).toBe(adapterDevuelto);

    // init() sigue registrando el middleware de auth y el handler de conexión,
    // igual que antes de agregar el adapter.
    expect(io.use).toHaveBeenCalledWith(socketModule._authenticateSocket);
    expect(io.on).toHaveBeenCalledWith('connection', expect.any(Function));
    expect(socketModule.getIo()).toBe(io);
  });

  test('closeRedisAdapter() cierra pubClient y subClient', async () => {
    await socketModule.init({});
    const pubClient = createClient.mock.results[0].value;
    const subClient = pubClient.duplicate.mock.results[0].value;

    await socketModule.closeRedisAdapter();

    expect(pubClient.quit).toHaveBeenCalledTimes(1);
    expect(subClient.quit).toHaveBeenCalledTimes(1);
  });
});
