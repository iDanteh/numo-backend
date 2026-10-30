'use strict';

// "Estadística por facturar" del mes en curso siempre lista (2026-10-30, pedido
// del usuario: "ya debe verse en cuanto se entra al módulo") — recalcula cada
// hora, de 7:00 a 21:00, las que alguien abrió en los últimos 7 días. Mismo
// patrón que erpSyncCron.js (registrado como side-effect desde app.js).
const cron = require('node-cron');
const estadistica = require('../domains/reportes-por-facturar/reportes-por-facturar.estadistica');

cron.schedule('20 7-21 * * *', async () => {
  try {
    await estadistica.recalcularFotosEnUso();
  } catch (err) {
    console.error(`[CronEstadisticaPorFacturar] Error: ${err.message}`);
  }
}, { timezone: 'America/Mexico_City' });
