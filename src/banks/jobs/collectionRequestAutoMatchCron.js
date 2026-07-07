'use strict';

// Reintenta el auto-match (OCR ≥95% + monto exacto → identificar y aplicar el
// cobro solo, ver intentarAutoMatch() en collection-request.service.js) sobre
// solicitudes que sigan "pendiente" con comprobante. Hace falta porque el
// depósito bancario muchas veces se importa a Numo DESPUÉS de que Kore avisó
// la solicitud — el intento que se dispara al crear (fire-and-forget) no
// alcanza para esos casos. Mismo patrón que erpSyncCron.js: node-cron con
// timezone explícito, registrado como side-effect al ser require-ado desde app.js.
const cron = require('node-cron');
const CollectionRequest = require('../domains/collection-requests/CollectionRequest.model');
const { intentarAutoMatch } = require('../domains/collection-requests/collection-request.service');

// No reintentar para siempre — una solicitud pendiente con comprobante que
// lleva más de este tiempo sin encontrar match probablemente nunca lo va a
// encontrar solo (o de plano no hay depósito que corresponda), y no vale la
// pena seguir gastando cuota de OCR/CPU en ella en cada corrida del cron.
const AUTO_MATCH_MAX_ANTIGUEDAD_DIAS = 14;

async function runAutoMatchPendientes() {
  const desde = new Date(Date.now() - AUTO_MATCH_MAX_ANTIGUEDAD_DIAS * 86_400_000);
  const pendientes = await CollectionRequest.find({
    status: 'pendiente',
    'comprobante.data': { $ne: null },
    conceptoId: { $ne: null },
    createdAt: { $gte: desde },
  }).select('_id').lean();

  if (pendientes.length === 0) return;

  console.log(`[CronAutoMatch] ${pendientes.length} solicitud(es) pendiente(s) con comprobante — reintentando auto-match...`);
  let aplicadas = 0;
  // Secuencial, no en paralelo — cada intento le pega a Kore (sesión de caja +
  // saldo + aplicar cobro) y comparte el mismo rate limit que el resto de las
  // integraciones ERP; no hay prisa, es un job de fondo.
  for (const { _id } of pendientes) {
    try {
      const resultado = await intentarAutoMatch(_id.toString());
      if (resultado.aplicado) aplicadas++;
    } catch (err) {
      console.error(`[CronAutoMatch] ${_id}: error inesperado —`, err.message);
    }
  }
  console.log(`[CronAutoMatch] Terminado — ${aplicadas} de ${pendientes.length} aplicada(s) automáticamente.`);
}

cron.schedule('*/20 * * * *', async () => {
  try {
    await runAutoMatchPendientes();
  } catch (err) {
    console.error(`[CronAutoMatch] Error fatal: ${err.message}`);
  }
}, { timezone: 'America/Mexico_City' });

module.exports = { runAutoMatchPendientes };
