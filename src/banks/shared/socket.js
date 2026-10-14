'use strict';

const { Server } = require('socket.io');
const { authenticate }       = require('../../shared/middleware/auth');
const rbacStore              = require('../../shared/services/rbac-store');
const { PERMISSIONS }        = require('../../shared/config/rbac');
const { logger }             = require('../shared/utils/logger');

let _io = null;

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
 *   user:{auth0Sub}  — notificaciones personales (roles, importación)
 *   banco:{banco}    — actualizaciones en tiempo real del banco activo
 *
 * Fix de seguridad (2026-10-14, hallazgo de auditoría): antes cualquiera con
 * acceso de red podía conectarse sin sesión y pedir cualquier canal (incluido
 * hacerse pasar por otro usuario vía `identify(auth0Sub)`, que confiaba
 * ciegamente en lo que mandaba el cliente). Ahora `_io.use(_authenticateSocket)`
 * exige el mismo JWT que ya valida el resto del backend ANTES de aceptar la
 * conexión, la sala personal se une sola desde la identidad ya verificada
 * (`socket.data.user`, nunca del parámetro que manda el cliente), y unirse a un
 * canal de banco exige `banks:read` igual que las rutas REST.
 */
function init(httpServer) {
  _io = new Server(httpServer, {
    cors: {
      origin:  process.env.CORS_ORIGIN || 'http://localhost:4200',
      methods: ['GET', 'POST'],
    },
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
  });

  return _io;
}

/** Devuelve la instancia de Socket.IO; null si aún no se ha inicializado. */
function getIo() {
  return _io;
}

/** Emite un evento solo al usuario identificado por su auth0Sub. */
function emitToUser(auth0Sub, event, data) {
  if (_io && auth0Sub) _io.to(`user:${auth0Sub}`).emit(event, data);
}

/** Emite un evento a todos los clientes suscritos a un banco. */
function emitToBanco(banco, event, data) {
  if (_io && banco) _io.to(`banco:${banco}`).emit(event, data);
}

/** Emite un evento a todos los clientes conectados. */
function emitToAll(event, data) {
  if (_io) _io.emit(event, data);
}

module.exports = { init, getIo, emitToUser, emitToBanco, emitToAll, _authenticateSocket };
