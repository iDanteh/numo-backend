'use strict';

/**
 * loyalty-gateway/app.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — gateway independiente de los webhooks de Kore (venta,
 * catálogo de clientes, catálogo de niveles), para que la acumulación/canje de
 * puntos siga funcionando aunque el backend principal de Numo esté caído. Ver
 * `architecture/microservicios-evaluacion-lealtad` (Engram) para el análisis de
 * acoplamiento completo que llevó a este diseño.
 *
 * Reusa TAL CUAL los 3 routers de Kore y todo lo que ya existe debajo de ellos
 * (servicios, modelos) — nunca se duplica lógica de negocio, nunca se cambia una
 * URL que Kore ya conoce (sigue siendo /api/loyalty/kore/* una vez Nginx rutea
 * acá, ver infra/servidor). A propósito deja afuera todo lo que no necesita:
 *
 *   - MongoDB: el dominio loyalty es 100% Postgres, nunca lo toca. Bono real: si
 *     Mongo se cae, este gateway sigue funcionando (el backend principal no).
 *   - syncModels()/migraciones: solo el backend principal es dueño del esquema —
 *     este proceso solo CONECTA (sequelize.authenticate()), nunca migra. Debe
 *     arrancar DESPUÉS de que el backend principal haya migrado al menos una vez
 *     (siempre cierto en un ambiente ya existente).
 *   - Auth0/RBAC completo: los 3 routers son 100% server-to-server
 *     (verifyKoreApiKey, ver cada *.routes.js), nunca necesitan sesión de staff.
 *   - Un Server de Socket.IO real: este proceso nunca acepta conexiones de
 *     socket, solo necesita poder AVISAR eventos a los clientes que sí están
 *     conectados al backend principal — ver socketMgr.initEmitterOnly()
 *     (banks/shared/socket.js), que publica al mismo canal Redis que el backend
 *     principal ya escucha, sin levantar un Server acá.
 */
require('dotenv').config();

const http = require('http');
const express = require('express');
const helmet = require('helmet');
const cors = require('cors');
const compression = require('compression');
const morgan = require('morgan');
const swaggerUi = require('swagger-ui-express');
const swaggerSpec = require('./swagger');
const { checkPostgresOk } = require('../shared/utils/db-health');
const socketMgr = require('../banks/shared/socket');
const errorHandler = require('../shared/middleware/error-handler');
const { logger } = require('../shared/utils/logger');

const loyaltyKoreVentaRoutes = require('../loyalty/domains/ledger/loyalty-kore-venta.routes');
const loyaltyKoreClienteRoutes = require('../loyalty/domains/kore-match/loyalty-kore-cliente.routes');
const loyaltyKoreNivelRoutes = require('../loyalty/domains/kore-match/loyalty-kore-nivel.routes');

process.on('unhandledRejection', (reason) => {
  logger.error(`[loyalty-gateway][unhandledRejection] ${reason?.message || reason}`, { stack: reason?.stack });
});

const app = express();

// Confiar en el proxy inverso (Nginx) para leer X-Forwarded-For correctamente —
// mismo motivo que app.js (sin esto, express-rate-limit de loyalty-kore-venta.routes.js
// lanza ERR_ERL_UNEXPECTED_X_FORWARDED_FOR).
app.set('trust proxy', 1);

app.use(helmet());
app.use(cors({ origin: process.env.CORS_ORIGIN || 'http://localhost:4200', credentials: true }));
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));
app.use(compression());
app.use(morgan('combined', { stream: { write: (msg) => logger.info(msg.trim()) } }));

// Mismo prefijo que app.js — Kore nunca se entera de que cambió de proceso
// (ver docstring de arriba). Los 3 routers, sin tocarlos.
app.use('/api/loyalty/kore', loyaltyKoreVentaRoutes);
app.use('/api/loyalty/kore', loyaltyKoreClienteRoutes);
app.use('/api/loyalty/kore', loyaltyKoreNivelRoutes);

// Swagger UI (2026-11-18, pedido explícito del usuario): entorno interactivo para que Kore
// pueda probar los webhooks en vez de depender solo de Insomnia/Postman o el PDF estático.
// `res.removeHeader` quita el CSP restrictivo que `helmet()` ya puso arriba SOLO para esta
// ruta — swagger-ui-express sirve HTML/JS inline que ese CSP por defecto bloquea. El resto del
// gateway (los webhooks JSON) sigue con el CSP de helmet intacto.
app.use('/api-docs', (_req, res, next) => {
  res.removeHeader('Content-Security-Policy');
  next();
}, swaggerUi.serve, swaggerUi.setup(swaggerSpec));

// Health check propio — solo Postgres, nunca Mongo (este proceso no la usa).
// Docker usa este endpoint para marcar el contenedor como healthy/unhealthy,
// mismo criterio que GET /health en app.js.
app.get('/health', async (_req, res) => {
  const pgOk = await checkPostgresOk();
  res.status(pgOk ? 200 : 503).json({ status: pgOk ? 'ok' : 'degraded', postgres: pgOk });
});

app.use((_req, res) => {
  res.status(404).json({ error: 'Ruta no encontrada' });
});

app.use(errorHandler);

const PORT = process.env.LOYALTY_GATEWAY_PORT || 3001;

const startServer = async () => {
  // Conecta sin migrar — ver docstring de arriba. Si Postgres no responde, el
  // boot falla fuerte (igual que app.js con connectDB()): sin base de datos no
  // hay nada que este proceso pueda hacer.
  const { sequelize } = require('../config/database.postgres');
  await sequelize.authenticate();
  logger.info('[loyalty-gateway] PostgreSQL conectado');

  // Best-effort, nunca bloquea el arranque (mismo criterio que init() en
  // banks/shared/socket.js) — sin Redis, este proceso sigue sirviendo los
  // webhooks, solo no avisa nada por socket a quien esté conectado al backend
  // principal.
  await socketMgr.initEmitterOnly();

  const server = http.createServer(app);
  server.listen(PORT, () => {
    logger.info(`[loyalty-gateway] Escuchando en puerto ${PORT}`);
  });

  const shutdown = async (signal) => {
    logger.warn(`[loyalty-gateway] ${signal} recibido — cerrando`);
    server.close(() => process.exit(0));
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
};

startServer().catch((err) => {
  logger.error('[loyalty-gateway] Error iniciando servidor:', err);
  process.exit(1);
});

module.exports = app;
