'use strict';

// Corrida automática diaria: sincroniza transferencias entre cajas (Fase A del
// proceso de matching de depósitos huérfanos). Mismo patrón que erpSyncCron.js
// — node-cron con timezone explícito, registrado como side-effect desde app.js.
// 8am (una hora después de erpSyncCron) para no competir por el mismo horario.
const cron = require('node-cron');
const { sincronizarTransferenciasCajas,
        reaplicarFiltro }                = require('../domains/erp/caja-transferencia-sync.service');
const { detectarHuerfanas }              = require('../domains/erp/caja-transferencia-match.service');

cron.schedule('0 8 * * *', async () => {
  try {
    await sincronizarTransferenciasCajas();
  } catch (err) {
    console.error(`[CronCajaTransferencias] Error fatal en sync: ${err.message}`);
  }

  // Se corre SIEMPRE, incluso si el sync falló — detectarHuerfanas revisa transferencias
  // YA guardadas de corridas anteriores, un fallo del sync de hoy no debe bloquearlo.
  try {
    await detectarHuerfanas();
  } catch (err) {
    console.error(`[CronCajaTransferencias] Error fatal detectando huérfanas: ${err.message}`);
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
