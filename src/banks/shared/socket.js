'use strict';

const { Server } = require('socket.io');

let _io = null;

/**
 * init — adjunta Socket.IO al servidor HTTP y registra eventos base.
 * Salas disponibles:
 *   user:{auth0Sub}  — notificaciones personales (roles, importación)
 *   banco:{banco}    — actualizaciones en tiempo real del banco activo
 */
function init(httpServer) {
  _io = new Server(httpServer, {
    cors: {
      origin:  process.env.CORS_ORIGIN || 'http://localhost:4200',
      methods: ['GET', 'POST'],
    },
  });

  _io.on('connection', (socket) => {
    socket.on('identify', (auth0Sub) => {
      if (typeof auth0Sub === 'string' && auth0Sub.trim()) {
        socket.join(`user:${auth0Sub.trim()}`);
      }
    });

    socket.on('bank:join', (banco) => {
      if (typeof banco === 'string' && banco.trim()) {
        socket.join(`banco:${banco.trim()}`);
      }
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

module.exports = { init, getIo, emitToUser, emitToBanco, emitToAll };
