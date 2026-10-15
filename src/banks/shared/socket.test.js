'use strict';

// Fix de seguridad 2026-10-14: el canal de Socket.IO no exigía ningún token para
// conectarse. Estos tests cubren el shim que adapta `authenticate` (middleware
// Express real, sin reimplementar verificación de JWT) a Socket.IO vía
// `_io.use(_authenticateSocket)`.
jest.mock('../../shared/middleware/auth', () => ({
  authenticate: jest.fn(),
}));

const { authenticate } = require('../../shared/middleware/auth');
const { _authenticateSocket } = require('./socket');

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
