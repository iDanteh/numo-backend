'use strict';

/**
 * visor/jobs/backendCaidoAlertJob.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Avisa por correo cuando el backend vuelve a arrancar después de haber estado
 * apagado más de UMBRAL_MINUTOS (2026-10-19, pedido del usuario: el backend
 * estuvo caído del 16-oct 14:48 al 19-oct 5:30 MX y nadie se enteró — esas
 * noches no corrió la descarga del SAT, la sincronización con Kore ni los
 * avisos de vencimiento de credenciales).
 *
 * Mecanismo: un "latido" en Mongo (colección `backend_heartbeat`, un solo
 * documento) que se actualiza cada 5 minutos. Al arrancar se compara el último
 * latido con la hora actual: un deploy/reinicio normal tarda unos minutos y no
 * avisa; un hueco largo sí. Mismos destinatarios técnicos que
 * cfdisNoRecuperadosAlertJob.js (no la lista de negocio de la entidad).
 *
 * Solo corre con NODE_ENV=production (una máquina de desarrollo apagada en la
 * noche no debe mandar correos); BACKEND_CAIDO_ALERTA=false lo apaga.
 */

const os       = require('os');
const cron     = require('node-cron');
const mongoose = require('mongoose');
const { logger } = require('../../shared/utils/logger');
const emailSvc = require('../../shared/services/email.service');

const DESTINATARIOS  = ['programador6@tubosyconexiones.mx', 'sistemas4@tubosyconexiones.mx'];
const UMBRAL_MINUTOS = 30;
const COLECCION      = 'backend_heartbeat';
const DOC_ID         = 'backend';

const habilitado = () => process.env.NODE_ENV === 'production' && process.env.BACKEND_CAIDO_ALERTA !== 'false';
const coleccion  = () => mongoose.connection.db.collection(COLECCION);

// Dentro de Docker os.hostname() es el id del contenedor — la URL pública
// (CORS_ORIGIN) distingue producción de testnumo.
const nombreServidor = () => (process.env.CORS_ORIGIN || '').split(',')[0].trim() || os.hostname();

const fmtMX = fecha => new Date(fecha).toLocaleString('es-MX', { timeZone: 'America/Mexico_City' });

function formatearDuracion(minutos) {
  if (minutos < 60) return `${Math.round(minutos)} minuto(s)`;
  const horas = minutos / 60;
  if (horas < 48) return `${horas.toFixed(1)} hora(s)`;
  return `${(horas / 24).toFixed(1)} día(s)`;
}

function construirCorreo({ desde, hasta, minutos }) {
  const duracion = formatearDuracion(minutos);
  return {
    subject: `⚠ Numo estuvo apagado ${duracion} — ${nombreServidor()}`,
    html: `
      <div style="font-family:Arial,sans-serif;max-width:520px;margin:0 auto">
        <h2 style="color:#dc2626">⚠ El backend de Numo estuvo apagado</h2>
        <p>El servidor volvió a arrancar después de estar sin funcionar <strong>${duracion}</strong>.</p>
        <p>
          <strong>Último registro antes de apagarse:</strong> ${fmtMX(desde)}<br>
          <strong>Volvió a arrancar:</strong> ${fmtMX(hasta)}<br>
          <span style="color:#6b7280;font-size:.85rem">(hora de México; el inicio es aproximado, ±5 minutos)</span>
        </p>
        <p>Durante ese tiempo no corrieron los procesos automáticos. Conviene revisar:</p>
        <ul>
          <li>Descarga nocturna del SAT de esos días (Historial &gt; Descarga SAT).</li>
          <li>Sincronización con Kore ERP.</li>
          <li>Avisos de vencimiento de credenciales SAT que hayan tocado en ese lapso.</li>
        </ul>
        <p style="color:#6b7280;font-size:.85rem;margin-top:1.5rem">
          Servidor: ${nombreServidor()} — aviso automático de Numo, no responder a este correo.
        </p>
      </div>
    `,
  };
}

async function latir() {
  await coleccion().updateOne({ _id: DOC_ID }, { $set: { ultimoLatido: new Date() } }, { upsert: true });
}

/**
 * Se llama UNA vez al arrancar el servidor (app.js), con Mongo ya conectado.
 * Nunca lanza.
 */
async function revisarCaidaAlArrancar() {
  if (!habilitado()) return;
  try {
    const anterior = await coleccion().findOne({ _id: DOC_ID });
    const ahora = new Date();
    await coleccion().updateOne({ _id: DOC_ID }, { $set: { ultimoLatido: ahora, iniciadoEn: ahora } }, { upsert: true });

    if (!anterior?.ultimoLatido) return; // primer arranque con esta función: no hay contra qué comparar
    const minutos = (ahora - new Date(anterior.ultimoLatido)) / 60000;
    if (minutos < UMBRAL_MINUTOS) return;

    logger.warn(`[BackendCaidoAlert] El backend estuvo sin latido ${formatearDuracion(minutos)} (desde ${new Date(anterior.ultimoLatido).toISOString()})`);
    const { subject, html } = construirCorreo({ desde: anterior.ultimoLatido, hasta: ahora, minutos });
    await emailSvc.enviarCorreo({ to: DESTINATARIOS, subject, html });
  } catch (err) {
    logger.error(`[BackendCaidoAlert] No se pudo revisar la caída al arrancar: ${err.message}`);
  }
}

cron.schedule('*/5 * * * *', async () => {
  if (!habilitado()) return;
  try { await latir(); }
  catch (err) { logger.error(`[BackendCaidoAlert] Error al registrar latido: ${err.message}`); }
}, { timezone: 'America/Mexico_City' });

module.exports = { revisarCaidaAlArrancar, latir, UMBRAL_MINUTOS };
