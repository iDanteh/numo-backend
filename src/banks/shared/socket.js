'use strict';

const { Server } = require('socket.io');
const { createClient } = require('redis');
const { createAdapter } = require('@socket.io/redis-adapter');
const { authenticate }       = require('../../shared/middleware/auth');
const rbacStore              = require('../../shared/services/rbac-store');
const { PERMISSIONS }        = require('../../shared/config/rbac');
const config                 = require('../../config/env');
const { logger }             = require('../shared/utils/logger');

let _io = null;
// Clientes dedicados del adapter Redis (pub/sub cross-worker) — separados a
// propósito del redisClient compartido de shared/utils/redis-client.js: un
// cliente en modo *subscribe* deja de aceptar otros comandos, así que el
// adapter necesita los suyos propios (uno para publicar, su `duplicate()`
// para suscribirse).
let _pubClient = null;
let _subClient = null;

// Adapta el middleware Express `authenticate` (src/shared/middleware/auth.js) a
// Socket.IO vía un shim mínimo de req/res — mismo criterio que usan desde hace
// años libs como passport.socketio para reusar middleware de Express sin
// reimplementar verificación de JWT. `authenticate` en éxito puebla `req.user` y
// llama a `next()` SIN argumentos; en fallo SIEMPRE hace `res.status().json()` y
// nunca llama a `next()` — por eso `res.json()` acá dispara el `next(Error)` de
// Socket.IO en vez de intentar "responder" nada.
function _authenticateSocket(socket, next) {
  const token = socket.handshake.auth?.token;
  const req = {
    headers: { authorization: token ? `Bearer ${token}` : '' },
    get(name) { return this.headers[String(name).toLowerCase()] || ''; },
    // express-oauth2-jwt-bearer llama req.is('urlencoded') para decidir si busca
    // el token también en el body (fallback de RFC 6750) — el handshake de
    // Socket.IO nunca manda un body url-encoded, así que `false` es la respuesta
    // correcta, no solo un stub para evitar el crash (bug real 2026-10-14,
    // reproducido en consola local: "TypeError: req.is is not a function").
    is() { return false; },
    // También valida req.method ("Invalid HTTP method received in request" si
    // falta) — el handshake de Socket.IO es, en los hechos, un GET.
    method: 'GET',
  };
  const res = {
    statusCode: 200,
    status(code) { this.statusCode = code; return this; },
    json(body) {
      next(new Error(body?.error || 'No autorizado'));
      return this;
    },
  };
  authenticate(req, res, (err) => {
    if (err) return next(err);
    socket.data.user = req.user;
    next();
  });
}

/**
 * init — adjunta Socket.IO al servidor HTTP y registra eventos base.
 * Salas disponibles:
 *   user:{auth0Sub}      — notificaciones personales (roles, importación)
 *   banco:{banco}        — actualizaciones en tiempo real del banco activo
 *   lealtad-tarjeta:{id} — actualizaciones en tiempo real de UNA tarjeta de Lealtad
 *    (2026-11-10, ver `emitToLoyaltyCard`: igual patrón que `banco:{banco}`/`bank:join`,
 *    gateado por `loyalty:read` en vez de `banks:read` — los webhooks de Kore no emitían
 *    nada hasta hoy, ver el commit que agrega esto).
 *
 * Fix de seguridad (2026-10-14, hallazgo de auditoría): antes cualquiera con
 * acceso de red podía conectarse sin sesión y pedir cualquier canal (incluido
 * hacerse pasar por otro usuario vía `identify(auth0Sub)`, que confiaba
 * ciegamente en lo que mandaba el cliente). Ahora `_io.use(_authenticateSocket)`
 * exige el mismo JWT que ya valida el resto del backend ANTES de aceptar la
 * conexión, la sala personal se une sola desde la identidad ya verificada
 * (`socket.data.user`, nunca del parámetro que manda el cliente), y unirse a un
 * canal de banco exige `banks:read` igual que las rutas REST.
 *
 * Adapter Redis (consultoría de desempeño 2026-11-11, cluster nativo + Redis,
 * ver src/cluster.js): `_pubClient`/`_subClient` se conectan ANTES de crear el
 * `Server`, y el adapter ya armado se pasa en el constructor de `Server` — no
 * se llama `_io.adapter(...)` después, para evitar la condición de carrera de
 * "el adapter se reemplaza cuando ya hay conexiones aceptadas". Con esto,
 * `.to(...).emit()`/`.emit()` (ver `emitToUser`/`emitToBanco`/etc. abajo)
 * cruzan workers solos — cada worker del cluster solo tiene en memoria los
 * sockets que él mismo aceptó, el adapter es quien reenvía el evento al resto
 * vía pub/sub de Redis. `init` pasa a ser `async` por este motivo: conectar
 * ambos clientes es una operación async que debe completarse ANTES de poder
 * construir el `Server` con el adapter ya listo.
 *
 * Degradación SIN Redis (hallazgo real corriendo el proceso sin Redis
 * levantado, 2026-11-11): si `connect()` rechaza (acotado por
 * `reconnectStrategy` abajo — sin tope, colgaría este `await` para siempre),
 * `init()` NO propaga ese error. Socket.IO arranca igual, sin adapter — mismo
 * comportamiento que hoy sin cluster (sockets solo locales a este proceso,
 * sin cross-worker). Dejar que este error tumbara `init()` tumbaría
 * `startServer()` entero en app.js (sin try/catch alrededor de
 * `await socketMgr.init(server)`) — Redis caído NUNCA debe bajar el servidor
 * HTTP completo, exactamente el criterio que ya pide la sección de Redis en
 * config/env.js.
 */
async function init(httpServer) {
  // `reconnectStrategy` acotada (mismo motivo que shared/utils/redis-client.js): sin
  // Redis levantado, un cliente sin tope reintenta reconectar para siempre — acá
  // además dejaría colgado el `await connect()` de abajo esperando una conexión que
  // nunca llega. Con el tope, `connect()` rechaza después de ~10 intentos (ver el
  // try/catch de abajo, que por eso nunca deja que ese rechazo tumbe `init()`).
  const redisSocketOpts = {
    reconnectStrategy: (retries) => {
      if (retries > 10) return false;
      return Math.min(retries * 100, 3000);
    },
  };
  _pubClient = createClient({ url: config.redis.url, socket: redisSocketOpts });
  _subClient = _pubClient.duplicate();
  _pubClient.on('error', (err) => logger.error(`[socket][redis] pubClient error: ${err.message}`));
  _subClient.on('error', (err) => logger.error(`[socket][redis] subClient error: ${err.message}`));

  let adapter;
  try {
    await Promise.all([_pubClient.connect(), _subClient.connect()]);
    adapter = createAdapter(_pubClient, _subClient);
  } catch (err) {
    logger.error(`[socket][redis] No se pudo conectar el adapter — Socket.IO sigue sin cross-worker: ${err.message}`);
  }

  _io = new Server(httpServer, {
    cors: {
      origin:  process.env.CORS_ORIGIN || 'http://localhost:4200',
      methods: ['GET', 'POST'],
    },
    // Sin `adapter` (Redis no disponible) Socket.IO usa su adapter in-memory
    // default — mismo comportamiento que antes de esta consultoría.
    ...(adapter ? { adapter } : {}),
  });

  _io.use(_authenticateSocket);

  _io.on('connection', (socket) => {
    // Identidad ya verificada por el middleware de arriba — se une solo,
    // nunca a partir de un valor que mande el cliente.
    socket.join(`user:${socket.data.user._id}`);

    // Se mantiene registrado por compatibilidad con el frontend actual (que
    // todavía emite este evento), pero el auth0Sub que manda el cliente se
    // ignora por completo — ya no decide a qué sala se une.
    socket.on('identify', () => {});

    socket.on('bank:join', async (banco) => {
      if (typeof banco !== 'string' || !banco.trim()) return;
      const puede = await rbacStore.hasPermission(
        socket.data.user.role, PERMISSIONS.BANKS_READ, socket.data.user.extraPermissions,
      );
      if (!puede) {
        logger.debug(`[socket] bank:join rechazado (sin banks:read): user=${socket.data.user._id} banco=${banco}`);
        return;
      }
      socket.join(`banco:${banco.trim()}`);
    });

    socket.on('bank:leave', (banco) => {
      if (typeof banco === 'string' && banco.trim()) {
        socket.leave(`banco:${banco.trim()}`);
      }
    });

    // Sala por tarjeta de Lealtad (2026-11-10) — mismo patrón que `bank:join`/`bank:leave`
    // de arriba, gateado por `loyalty:read` (el mismo permiso que ya protege `GET /cards/:id`
    // y `/cards/:id/historial`) en vez de `banks:read`.
    socket.on('lealtad-tarjeta:join', async (cardId) => {
      if (typeof cardId !== 'string' || !cardId.trim()) return;
      const puede = await rbacStore.hasPermission(
        socket.data.user.role, PERMISSIONS.LOYALTY_READ, socket.data.user.extraPermissions,
      );
      if (!puede) {
        logger.debug(`[socket] lealtad-tarjeta:join rechazado (sin loyalty:read): user=${socket.data.user._id} cardId=${cardId}`);
        return;
      }
      socket.join(`lealtad-tarjeta:${cardId.trim()}`);
    });

    socket.on('lealtad-tarjeta:leave', (cardId) => {
      if (typeof cardId === 'string' && cardId.trim()) {
        socket.leave(`lealtad-tarjeta:${cardId.trim()}`);
      }
    });
  });

  return _io;
}

/** Devuelve la instancia de Socket.IO; null si aún no se ha inicializado. */
function getIo() {
  return _io;
}

/** Cierra los clientes Redis del adapter — llamado desde el shutdown de app.js. */
async function closeRedisAdapter() {
  if (_pubClient) await _pubClient.quit();
  if (_subClient) await _subClient.quit();
}

/** Emite un evento solo al usuario identificado por su auth0Sub. */
function emitToUser(auth0Sub, event, data) {
  if (_io && auth0Sub) _io.to(`user:${auth0Sub}`).emit(event, data);
}

/** Emite un evento a todos los clientes suscritos a un banco. */
function emitToBanco(banco, event, data) {
  if (_io && banco) _io.to(`banco:${banco}`).emit(event, data);
}

/** Emite un evento a todos los clientes suscritos a una tarjeta de Lealtad (2026-11-10) —
 *  cualquier código del servidor puede invocarla sin tener identidad propia (igual que
 *  `emitToBanco`/`emitToUser`): un webhook server-to-server autenticado por `X-Api-Key`
 *  (sin usuario humano detrás) la llama tal cual, la autenticación de socket solo le
 *  importa a quien SE CONECTA (JWT), nunca a quien emite. */
function emitToLoyaltyCard(cardId, event, data) {
  if (_io && cardId) _io.to(`lealtad-tarjeta:${cardId}`).emit(event, data);
}

/** Emite un evento a todos los clientes conectados. */
function emitToAll(event, data) {
  if (_io) _io.emit(event, data);
}

module.exports = {
  init, getIo, closeRedisAdapter, emitToUser, emitToBanco, emitToLoyaltyCard, emitToAll, _authenticateSocket,
};
