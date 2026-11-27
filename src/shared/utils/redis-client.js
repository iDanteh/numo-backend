'use strict';

/**
 * shared/utils/redis-client.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Cliente Redis compartido de propósito general — hoy lo usan los rate
 * limiters (RedisStore de rate-limit-redis) para que el límite sea real
 * cross-worker bajo cluster nativo (ver src/cluster.js). NO lo reutilices
 * para pub/sub de Socket.IO — un cliente en modo *subscribe* deja de aceptar
 * otros comandos; socket.js maneja sus propios clientes dedicados.
 *
 * Conecta al cargarse el módulo (mismo criterio que los cron jobs de
 * app.js: side-effect en el require). Un fallo de conexión al arrancar NO
 * tumba el proceso — solo loguea, igual que el resto de la infraestructura
 * no crítica de este proyecto.
 *
 * `reconnectStrategy` ACOTADA (hallazgo real corriendo el proceso sin Redis
 * levantado, 2026-11-11): sin esto, el cliente reintenta reconectar PARA
 * SIEMPRE con backoff — cada intento fallido además rechaza cualquier
 * comando en cola (p. ej. los SCRIPT LOAD que dispara `RedisStore` al
 * construirse, ver app.js/loyalty-public.routes.js), generando un goteo
 * infinito de `[unhandledRejection]` mientras Redis siga caído. Después de
 * 10 intentos (~sub-segundo a 3s de backoff cada uno) se deja de intentar
 * — el cliente queda desconectado pero el proceso sigue vivo y logueando
 * normal; un restart del proceso (o de Redis) vuelve a intentar desde cero.
 */
const { createClient } = require('redis');
const config = require('../../config/env');
const { logger } = require('./logger');

const redisClient = createClient({
  url: config.redis.url,
  socket: {
    reconnectStrategy: (retries) => {
      if (retries > 10) return false;
      return Math.min(retries * 100, 3000);
    },
  },
});

redisClient.on('error', (err) => {
  logger.error(`[redis] Error de conexión: ${err.message}`);
});

redisClient.connect().catch((err) => {
  logger.error(`[redis] No se pudo conectar al arrancar: ${err.message}`);
});

module.exports = redisClient;
