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
});
