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

const { connectDB }    = require('./config/database');
const seed             = require('./banks/scripts/seed');
const { logger }   = require('./shared/utils/logger');
const errorHandler = require('./shared/middleware/error-handler');

// Domain routers — Banks module
const bankRoutes              = require('./banks/domains/banks/bank.routes');
const accountPlanRoutes       = require('./banks/domains/account-plan/account-plan.routes');
const collectionRequestRoutes = require('./banks/domains/collection-requests/collection-request.routes');
const bankErpRoutes           = require('./banks/domains/erp/erp.routes');
const userRoutes              = require('./banks/domains/users/user.routes');

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
app.get('/health', (_req, res) => {
  res.json({
    status:    'ok',
    timestamp: new Date().toISOString(),
    version:   '3.0.0',
    databases: { mongo: 'connected', postgres: 'connected' },
  });
});

// ── API routes ────────────────────────────────────────────────────────────────
// Banks module
app.use('/api/banks',                bankRoutes);
app.use('/api/account-plan',         accountPlanRoutes);
app.use('/api/collection-requests',  collectionRequestRoutes);
app.use('/api/erp',                  bankErpRoutes);
app.use('/api/users',                userRoutes);

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
};

startServer().catch((err) => {
  logger.error('Error iniciando servidor:', err);
  process.exit(1);
});

module.exports = app;
