'use strict';

// Corrida automática diaria: "Congelamiento de puntos" por inactividad (Sistema de Lealtad,
// 2026-11-03, feature confirmada por el usuario). Mismo patrón que
// banks/jobs/cajaTransferenciaSyncCron.js — node-cron con timezone explícito, registrado como
// side-effect desde app.js, try/catch propio para que un error acá nunca tumbe el proceso.
//
// Horario: 4:10am hora de México — fuera de los horarios ya tomados por otros crons diarios del
// backend (erpSyncCron 7:00, cajaTransferenciaSyncCron 7:05, credencialesAlertJob 8:00,
// cfdisNoRecuperadosAlertJob 3:00), con margen suficiente para no competir por el mismo minuto.
const cron = require('node-cron');
const { congelarTarjetasInactivas } = require('./congelamiento-puntos.service');

cron.schedule('10 4 * * *', async () => {
  try {
    await congelarTarjetasInactivas();
  } catch (err) {
    console.error(`[CronCongelamientoPuntos] Error fatal: ${err.message}`);
  }
}, { timezone: 'America/Mexico_City' });
