'use strict';

// Corrida automática diaria: SOLO recuperar folio fiscal de CxC con erpLink pendiente de
// timbrar, sin importar el status del movimiento (2026-09-21, ampliado 2026-11-18 — ver
// _recuperarFolioFiscalJob en erp.routes.js para el motivo completo: ningún job automático
// debe volver a recalcular saldoErp/status de ningún movimiento). Antes disparaba
// runErpSyncAutomatico() (Sync Saldo ERP → Recalcular saldo ERP encadenado) — esa función
// queda intacta en erp.routes.js sin usarse desde acá.
// Mismo patrón que numo-backend/src/visor/jobs/satSyncJob.js — node-cron con timezone
// explícito, registrado como side-effect al ser require-ado desde app.js.
const cron = require('node-cron');
const erpRoutes = require('../domains/erp/erp.routes');

cron.schedule('0 7 * * *', async () => {
  try {
    await erpRoutes.runRecuperarFolioFiscalAutomatico();
  } catch (err) {
    console.error(`[CronErpSync] Error fatal: ${err.message}`);
  }
}, { timezone: 'America/Mexico_City' });
