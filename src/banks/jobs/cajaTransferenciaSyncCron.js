'use strict';

// Corrida automática diaria: sincroniza transferencias entre cajas (Fase A del
// proceso de matching de Depósito en efectivo huérfanos). Mismo patrón que
// erpSyncCron.js — node-cron con timezone explícito, registrado como side-effect
// desde app.js. 7:05am (erpSyncCron corre a las 7:00 en punto) para arrancar
// temprano sin competir por el mismo minuto exacto.
//
// 2026-09-02: se quitó el paso de detectarHuerfanas() (pedido explícito del
// usuario) — el mecanismo de "marcar huérfana" se elimina por completo, va a
// reemplazarse por algo distinto todavía no definido.
// 2026-09-09: el reemplazo es reclasificarHistoricasDescartadas() — limpia las
// transferencias anteriores al corte histórico que ya fueron resueltas por otra vía
// (ver caja-transferencia-match.service.js), corriendo cada noche para que el backlog
// ya sincronizado se limpie solo, sin script manual aparte.
const cron = require('node-cron');
const { sincronizarTransferenciasCajas,
        reaplicarFiltro }                = require('../domains/erp/caja-transferencia-sync.service');
const { reclasificarHistoricasDescartadas } = require('../domains/erp/caja-transferencia-match.service');

cron.schedule('5 7 * * *', async () => {
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

  try {
    await reclasificarHistoricasDescartadas();
  } catch (err) {
    console.error(`[CronCajaTransferencias] Error fatal reclasificando históricas: ${err.message}`);
  }
}, { timezone: 'America/Mexico_City' });
