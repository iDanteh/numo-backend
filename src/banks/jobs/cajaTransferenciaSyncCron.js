'use strict';

// Corrida automática diaria: sincroniza transferencias entre cajas (Fase A del
// proceso de matching de Depósito en efectivo huérfanos). Mismo patrón que
// erpSyncCron.js — node-cron con timezone explícito, registrado como side-effect
// desde app.js. 8am (una hora después de erpSyncCron) para no competir por el
// mismo horario.
//
// 2026-09-02: se quitó el paso de detectarHuerfanas() (pedido explícito del
// usuario) — el mecanismo de "marcar huérfana" se elimina por completo, va a
// reemplazarse por algo distinto todavía no definido.
const cron = require('node-cron');
const { sincronizarTransferenciasCajas,
        reaplicarFiltro }                = require('../domains/erp/caja-transferencia-sync.service');

cron.schedule('0 8 * * *', async () => {
  try {
    await sincronizarTransferenciasCajas();
  } catch (err) {
    console.error(`[CronCajaTransferencias] Error fatal en sync: ${err.message}`);
  }

  // Respaldo del hook de config:updated (caja-transferencia-sync.service.js#init) — si el
  // proceso estuvo caído cuando cambió la config, o el hook falló, esta corrida diaria lo
  // reconcilia igual. Se corre SIEMPRE, mismo criterio que los pasos anteriores.
  try {
    await reaplicarFiltro({ dryRun: false });
  } catch (err) {
    console.error(`[CronCajaTransferencias] Error fatal reaplicando filtro: ${err.message}`);
  }
}, { timezone: 'America/Mexico_City' });
