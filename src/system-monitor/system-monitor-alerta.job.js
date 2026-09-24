'use strict';

/**
 * system-monitor-alerta.job.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Avisa por correo cuando el Panel de Tráfico del Sistema detecta una
 * transición de estado — mismo estilo de correo que visor/jobs/credencialesAlertJob.js.
 * NO tiene su propio cron.schedule(): lo llama system-monitor.cron.js con el
 * snapshot YA calculado (evita pedirle a Mongo/Postgres el estado de salud 2
 * veces por tick — una para esto, otra para el guardado del histórico).
 *
 * Reglas de envío (estado en memoria — un solo proceso, mismo criterio "sin
 * cluster" que el resto de esta feature):
 *   normal → degradado/caido:  correo inmediato.
 *   degradado/caido sostenido: recordatorio cada 15 minutos (no cada tick del
 *                              cron — evita fatiga de alertas).
 *   degradado/caido → normal:  correo de "recuperado".
 */

const { logger } = require('../shared/utils/logger');
const emailSvc = require('../shared/services/email.service');
const globalConfigSvc = require('../shared/services/global-config.service');

const RECORDATORIO_MS = 15 * 60 * 1000;

let _ultimoEstado = 'normal';
let _ultimoEnvioMs = 0;

// Mismo texto que bannerTexto() en numo-frontend/.../system-monitor.component.ts —
// se duplica a propósito (no hay forma de compartir código entre Angular y Node):
// si se cambia la redacción de un lado, actualizar el otro.
function _descripcionEstado(estadoGeneral) {
  if (estadoGeneral === 'caido') {
    return 'El sistema dejó de recibir tráfico o perdió la conexión a la base de datos — revisa de inmediato.';
  }
  if (estadoGeneral === 'degradado') {
    return 'El sistema está funcionando, pero con señales de degradación (errores, latencia, CPU o una base de datos con problemas).';
  }
  return 'El sistema volvió a funcionar con normalidad.';
}

function _detalleFallas(snapshot) {
  const fallas = [];
  if (snapshot.salud.mongo === 'desconectado') fallas.push('MongoDB desconectado');
  if (snapshot.salud.postgres === 'desconectado') fallas.push('PostgreSQL desconectado');
  if (snapshot.tasaErrorPct > 10) fallas.push(`tasa de error ${snapshot.tasaErrorPct}%`);
  if (snapshot.eventLoopLagMs > 200) fallas.push(`event loop lag ${snapshot.eventLoopLagMs}ms`);
  if (snapshot.cpu.cores > 0 && snapshot.cpu.load1 > snapshot.cpu.cores) {
    fallas.push(`carga de CPU ${snapshot.cpu.load1} (${snapshot.cpu.cores} cores)`);
  }
  return fallas;
}

const TITULO_POR_TIPO = {
  'transicion-mal': (estado) => `⚠ Numo — sistema ${estado.toUpperCase()}`,
  recordatorio: (estado) => `⚠ Numo sigue ${estado.toUpperCase()} — recordatorio`,
  recuperado: () => '✔ Numo — sistema recuperado',
};
const COLOR_POR_TIPO = { 'transicion-mal': '#dc2626', recordatorio: '#b5790a', recuperado: '#0e9c6f' };

function _construirCorreo(tipo, snapshot) {
  const hora = new Date(snapshot.generadoEn).toLocaleString('es-MX', { timeZone: 'America/Mexico_City' });
  const fallas = _detalleFallas(snapshot);
  const subject = TITULO_POR_TIPO[tipo](snapshot.estadoGeneral);

  const html = `
    <div style="font-family:Arial,sans-serif;max-width:520px;margin:0 auto">
      <h2 style="color:${COLOR_POR_TIPO[tipo]}">${subject}</h2>
      <p>${_descripcionEstado(snapshot.estadoGeneral)}</p>
      <p><strong>Hora:</strong> ${hora} (hora de México)</p>
      ${fallas.length > 0 ? `<p><strong>Qué está fallando:</strong> ${fallas.join(', ')}</p>` : ''}
      <p style="color:#6b7280;font-size:.85rem;margin-top:1.5rem">
        Este es un aviso automático de Numo (Panel de Tráfico del Sistema) — no responder a este correo.
      </p>
    </div>
  `;
  return { subject, html };
}

/**
 * Procesa UN snapshot ya calculado, detecta la transición de estado y manda el
 * correo que corresponda (o ninguno). Llamado por system-monitor.cron.js.
 */
async function procesarAlerta(snapshot) {
  const estadoAnterior = _ultimoEstado;
  const estadoActual = snapshot.estadoGeneral;
  const ahoraMs = Date.now();

  let tipo = null;
  if (estadoAnterior === 'normal' && estadoActual !== 'normal') {
    tipo = 'transicion-mal';
  } else if (estadoAnterior !== 'normal' && estadoActual !== 'normal') {
    if (ahoraMs - _ultimoEnvioMs >= RECORDATORIO_MS) tipo = 'recordatorio';
  } else if (estadoAnterior !== 'normal' && estadoActual === 'normal') {
    tipo = 'recuperado';
  }

  _ultimoEstado = estadoActual;
  if (!tipo) return;

  let emailsCsv;
  try {
    emailsCsv = await globalConfigSvc.getValue('system-monitor', 'emails-alerta');
  } catch {
    // Sección/clave todavía no configurada (getValue tira si la fila no existe) —
    // ver seed-global-config-system-monitor.js. Nunca debe tirar la app por esto.
    emailsCsv = '';
  }
  const emails = (emailsCsv || '').split(',').map((s) => s.trim()).filter(Boolean);
  if (emails.length === 0) {
    logger.warn(`[system-monitor-alerta] estado=${estadoActual} (${tipo}) pero no hay emails-alerta configurado en Configuraciones Globales (sección system-monitor) — se omite el envío`);
    return;
  }

  const { subject, html } = _construirCorreo(tipo, snapshot);
  // Si enviarCorreo falla (returns false), _ultimoEnvioMs NO se actualiza a
  // propósito — el próximo tick (5 min) vuelve a intentar en vez de esperar los
  // 15 min completos del recordatorio, mismo criterio de "no relanzar pero
  // reintentar" que credencialesAlertJob.js.
  const enviado = await emailSvc.enviarCorreo({ to: emails, subject, html });
  if (enviado) _ultimoEnvioMs = ahoraMs;
}

// Solo para tests — resetea el estado en memoria (singleton de módulo).
function _resetParaTests() {
  _ultimoEstado = 'normal';
  _ultimoEnvioMs = 0;
}

module.exports = { procesarAlerta, _resetParaTests };
