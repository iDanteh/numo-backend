'use strict';

// Corrida automática diaria: Sync Saldo ERP → (solo si terminó OK) Sync Histórico Kore.
// Mismo patrón que numo-backend/src/visor/jobs/satSyncJob.js — node-cron con timezone
// explícito, registrado como side-effect al ser require-ado desde app.js.
const cron = require('node-cron');
const erpRoutes = require('../domains/erp/erp.routes');

cron.schedule('0 7 * * *', async () => {
  try {
    await erpRoutes.runErpSyncAutomatico();
  } catch (err) {
    console.error(`[CronErpSync] Error fatal: ${err.message}`);
  }
}, { timezone: 'America/Mexico_City' });
