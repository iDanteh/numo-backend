'use strict';

// Job de respaldo (Fase 2) para la correlación de anticipos generados por Kore —
// ver el comentario completo en anticipo-generado.service.js (reconciliarAnticiposPendientes)
// sobre la carrera de tiempos real que lo disparó. Mismo patrón que
// cajaTransferenciaSyncCron.js — node-cron con timezone explícito, registrado
// como side-effect desde app.js, try/catch para que un error acá jamás escale a
// uncaughtException.
//
// Cada 5 minutos: sobra margen contra los ~7.5s reales de la carrera documentada,
// y no vale la pena correrlo más seguido — es un job de reconciliación/auditoría,
// no de tiempo real (el webhook ya intenta correlacionar al momento; esto solo
// reintenta lo que quedó pendiente).
const cron = require('node-cron');
const { reconciliarAnticiposPendientes } = require('../domains/collection-requests/anticipo-generado.service');

cron.schedule('*/5 * * * *', async () => {
  try {
    await reconciliarAnticiposPendientes();
  } catch (err) {
    console.error(`[CronAnticipoGeneradoReconciliacion] Error fatal: ${err.message}`);
  }
}, { timezone: 'America/Mexico_City' });
