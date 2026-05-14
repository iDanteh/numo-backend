require('dotenv').config();
const config = require('./config/env');
const http        = require('http');
const express     = require('express');
const helmet      = require('helmet');
const cors        = require('cors');
const morgan      = require('morgan');
const compression = require('compression');
const rateLimit   = require('express-rate-limit');
const socketMgr   = require('./banks/shared/socket');

const mongoose         = require('mongoose');
const { sequelize }    = require('./config/database.postgres');
const { connectDB, disconnectDB } = require('./config/database');
const seed             = require('./banks/scripts/seed');
const { logger }   = require('./shared/utils/logger');
const errorHandler = require('./shared/middleware/error-handler');

// Domain routers — Banks module
const bankRoutes              = require('./banks/domains/banks/bank.routes');
const accountPlanRoutes       = require('./banks/domains/account-plan/account-plan.routes');
const collectionRequestRoutes = require('./banks/domains/collection-requests/collection-request.routes');
const bankErpRoutes           = require('./banks/domains/erp/erp.routes');
const userRoutes              = require('./banks/domains/users/user.routes');
const polizaRoutes            = require('./banks/domains/polizas/poliza.routes');
const cfdiMappingRoutes       = require('./banks/domains/cfdi-mapping/cfdi-mapping.routes');

// Domain routers — Visor module
const authRoutes             = require('./visor/routes/auth');
const cfdiRoutes             = require('./visor/routes/cfdis');
const comparisonRoutes       = require('./visor/routes/comparisons');
const discrepancyRoutes      = require('./visor/routes/discrepancies');
const reportRoutes           = require('./visor/routes/reports');
const satRoutes              = require('./visor/routes/sat');
const entityRoutes           = require('./visor/routes/entities');
const driveRoutes            = require('./visor/routes/drive');
const periodosFiscalesRoutes = require('./visor/routes/periodos-fiscales');
const visorErpRoutes         = require('./visor/routes/erp');
const scheduleRoutes         = require('./visor/routes/schedule');

const app = express();

// Confiar en el proxy inverso (nginx) para leer X-Forwarded-For correctamente.
// Sin esto, express-rate-limit lanza ERR_ERL_UNEXPECTED_X_FORWARDED_FOR.
app.set('trust proxy', 1);

// ── Security ──────────────────────────────────────────────────────────────────
app.use(helmet());
app.use(cors({
  origin:      process.env.CORS_ORIGIN || 'http://localhost:4200',
  credentials: true,
}));

// Rate limiting — global: 600 req / 15 min (40/min promedio)
const limiter = rateLimit({
  windowMs: config.rateLimit.windowMs,
  max:      config.rateLimit.max,
  standardHeaders: true,
  legacyHeaders:   false,
  handler: (req, res) => {
    const resetMs = req.rateLimit.resetTime instanceof Date
      ? req.rateLimit.resetTime.getTime()
      : (req.rateLimit.resetTime || Date.now() + 60000);
    const retryAfter = Math.max(1, Math.ceil((resetMs - Date.now()) / 1000));
    res.status(429).json({
      success:    false,
      error:      'Demasiadas solicitudes, intenta más tarde.',
      code:       'RATE_LIMIT_EXCEEDED',
      retryAfter,
    });
  },
});
app.use('/api/', limiter);

// ── Body parsing & compression ────────────────────────────────────────────────
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));
app.use(compression());

// ── Logging ───────────────────────────────────────────────────────────────────
app.use(morgan('combined', {
  stream: { write: (msg) => logger.info(msg.trim()) },
}));

// ── Health check ──────────────────────────────────────────────────────────────
// Verifica el estado real de las conexiones a MongoDB y PostgreSQL.
// Docker usa este endpoint para marcar el contenedor como healthy/unhealthy.
app.get('/health', async (_req, res) => {
  const mongoOk = mongoose.connection.readyState === 1; // 1 = connected

  let pgOk = false;
  try {
    await sequelize.authenticate();
    pgOk = true;
  } catch {
    pgOk = false;
  }

  const allOk  = mongoOk && pgOk;
  const status = allOk ? 'ok' : 'degraded';

  res.status(allOk ? 200 : 503).json({
    status,
    timestamp: new Date().toISOString(),
    version:   '3.0.0',
    databases: {
      mongo:    mongoOk ? 'connected' : 'disconnected',
      postgres: pgOk    ? 'connected' : 'disconnected',
    },
  });
});

// ── API routes ────────────────────────────────────────────────────────────────
// Banks module
app.use('/api/banks',                bankRoutes);
app.use('/api/account-plan',         accountPlanRoutes);
app.use('/api/collection-requests',  collectionRequestRoutes);
app.use('/api/erp',                  bankErpRoutes);
app.use('/api/users',                userRoutes);
app.use('/api/polizas',             polizaRoutes);
app.use('/api/cfdi-mapping',        cfdiMappingRoutes);

// Visor module
app.use('/api/auth',             authRoutes);
app.use('/api/cfdis',            cfdiRoutes);
app.use('/api/comparisons',      comparisonRoutes);
app.use('/api/discrepancies',    discrepancyRoutes);
app.use('/api/reports',          reportRoutes);
app.use('/api/sat',              satRoutes);
app.use('/api/entities',         entityRoutes);
app.use('/api/drive',            driveRoutes);
app.use('/api/periodos-fiscales', periodosFiscalesRoutes);
app.use('/api/erp',              visorErpRoutes);
app.use('/api/schedule',         scheduleRoutes);

// ── 404 ───────────────────────────────────────────────────────────────────────
app.use((_req, res) => {
  res.status(404).json({ error: 'Ruta no encontrada' });
});

// ── Error handler (must be last) ──────────────────────────────────────────────
app.use(errorHandler);

// ── Start server ──────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;

const startServer = async () => {
  await connectDB();

  // Limpiar sesiones de comparación que quedaron en 'running' por un reinicio anterior.
  // Si el proceso fue interrumpido (deploy, OOM, SIGKILL) mientras corría un batch,
  // la sesión nunca llegó a marcarse 'completed' o 'failed' → queda en 'running' para siempre.
  try {
    const ComparisonSession = require('./visor/models/ComparisonSession');
    const { modifiedCount } = await ComparisonSession.updateMany(
      { status: 'running' },
      { $set: { status: 'failed', completedAt: new Date(), failureReason: 'Servidor reiniciado mientras el proceso estaba en curso' } },
    );
    if (modifiedCount > 0) {
      logger.warn(`[startup] ${modifiedCount} sesión(es) de comparación quedaron en 'running' — marcadas como 'failed'`);
    }
  } catch (err) {
    logger.error('[startup] No se pudieron limpiar sesiones zombie:', err.message);
  }

  require('./visor/jobs/satSyncJob');
  try {
    await seed();
  } catch (err) {
    logger.error('[seed] Error en seed (no fatal):', err.message);
  }
  const server = http.createServer(app);
  socketMgr.init(server);
  server.listen(PORT, () => {
    logger.info(`Servidor corriendo en puerto ${PORT} [${process.env.NODE_ENV || 'development'}]`);
  });

  // ── Graceful shutdown ────────────────────────────────────────────────────────
  // Docker envía SIGTERM antes de matar el contenedor. Tenemos ~10 s para:
  //  1. Dejar de aceptar conexiones nuevas.
  //  2. Eliminar credenciales e.firma de jobs SAT activos (seguridad).
  //  3. Cerrar conexiones a BD para evitar conexiones huérfanas.
  // Si no terminamos en SHUTDOWN_TIMEOUT_MS, process.exit(1) fuerza el cierre.
  const SHUTDOWN_TIMEOUT_MS = 9_000;

  const shutdown = async (signal) => {
    logger.warn(`[Shutdown] ${signal} recibido — iniciando graceful shutdown...`);

    // Timeout de seguridad: si algo se cuelga, matar igual
    const forceExit = setTimeout(() => {
      logger.error('[Shutdown] Timeout — forzando process.exit(1)');
      process.exit(1);
    }, SHUTDOWN_TIMEOUT_MS);
    forceExit.unref(); // no impedir que el event loop cierre si termina antes

    // 1. Dejar de aceptar nuevas conexiones HTTP
    server.close();

    // 2. Limpiar credenciales SAT de jobs activos en memoria
    try {
      const { cleanupActiveJobs } = require('./visor/controllers/sat.controller');
      await cleanupActiveJobs();
      logger.info('[Shutdown] Credenciales SAT activas eliminadas');
    } catch (cleanErr) {
      logger.error(`[Shutdown] Error limpiando credenciales SAT: ${cleanErr.message}`);
    }

    // 3. Cerrar conexiones a bases de datos
    try {
      await disconnectDB();
      logger.info('[Shutdown] Bases de datos desconectadas');
    } catch (dbErr) {
      logger.error(`[Shutdown] Error cerrando DBs: ${dbErr.message}`);
    }

    clearTimeout(forceExit);
    logger.info('[Shutdown] Completado — saliendo con código 0');
    process.exit(0);
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT',  () => shutdown('SIGINT'));

  // Capturar excepciones no manejadas para logearlas antes de morir
  process.on('uncaughtException', (err) => {
    logger.error(`[uncaughtException] ${err.message}`, err);
    shutdown('uncaughtException');
  });
  process.on('unhandledRejection', (reason) => {
    logger.error(`[unhandledRejection] ${reason}`);
  });
};

startServer().catch((err) => {
  logger.error('Error iniciando servidor:', err);
  process.exit(1);
});

module.exports = app;
