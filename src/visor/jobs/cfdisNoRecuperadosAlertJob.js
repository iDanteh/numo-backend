'use strict';

/**
 * visor/jobs/cfdisNoRecuperadosAlertJob.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Avisa por correo cuando la descarga automática nocturna del SAT (arranca
 * ~1:00 am, ver historial-sat.component.html) deja algún checkpoint sin
 * 'completado' — corre a las 3:00 am, con margen de sobra para que el ciclo
 * nocturno (incluidos sus reintentos) ya haya terminado.
 *
 * A diferencia de credencialesAlertJob.js (que manda el aviso a
 * entity.emailsAlerta, la lista de correos de NEGOCIO de cada entidad), este
 * es un aviso TÉCNICO para el administrador del sistema — 2026-09-22, pedido
 * explícito del usuario: correo fijo a programador6, no la lista de la
 * entidad.
 *
 * Puramente informativo — NO intenta recuperar nada automáticamente (esa
 * decisión quedó explícitamente para el botón manual "Recuperar de Kore ERP"
 * en Historial > Descarga SAT, ver sat.controller.js#recuperarErp).
 */

const cron = require('node-cron');
const { logger } = require('../../shared/utils/logger');
const SatJobCheckpoint = require('../models/SatJobCheckpoint');
const emailSvc = require('../../shared/services/email.service');

// 2026-09-23, pedido explícito del usuario: agregar sistemas4 además de
// programador6.
const DESTINATARIOS = ['programador6@tubosyconexiones.mx', 'sistemas4@tubosyconexiones.mx'];
// 2026-09-23, pedido explícito del usuario: solo los "más actuales" — el
// checkpoint (`fecha`, el DÍA que describe, no cuándo se tocó por última
// vez) debe caer en los últimos 2 días. Antes se filtraba por `updatedAt`
// (cuándo se tocó), lo que hacía que un checkpoint viejo sin resolver
// reapareciera cada noche indefinidamente si algo lo seguía re-tocando.
const DIAS_ATRAS = 2;

function construirCorreo(pendientes) {
  const filas = pendientes.map(p => `
    <tr>
      <td style="padding:.4rem .6rem;border-bottom:1px solid #e5e7eb">${p.rfc}</td>
      <td style="padding:.4rem .6rem;border-bottom:1px solid #e5e7eb">${p.fecha}</td>
      <td style="padding:.4rem .6rem;border-bottom:1px solid #e5e7eb">${p.tipoComprobante}</td>
      <td style="padding:.4rem .6rem;border-bottom:1px solid #e5e7eb">${p.status}</td>
      <td style="padding:.4rem .6rem;border-bottom:1px solid #e5e7eb;font-size:.8rem;color:#6b7280">${(p.error || '').slice(0, 160)}</td>
    </tr>
  `).join('');

  return {
    subject: `⚠ ${pendientes.length} caso(s) de CFDIs sin recuperar del SAT anoche`,
    html: `
      <div style="font-family:Arial,sans-serif;max-width:680px;margin:0 auto">
        <h2 style="color:#dc2626">⚠ CFDIs sin recuperar del SAT</h2>
        <p>La descarga automática nocturna no logró completar los siguientes casos:</p>
        <table style="border-collapse:collapse;width:100%;font-size:.85rem">
          <thead>
            <tr style="background:#f9fafb">
              <th style="text-align:left;padding:.4rem .6rem">RFC</th>
              <th style="text-align:left;padding:.4rem .6rem">Fecha</th>
              <th style="text-align:left;padding:.4rem .6rem">Tipo</th>
              <th style="text-align:left;padding:.4rem .6rem">Estado</th>
              <th style="text-align:left;padding:.4rem .6rem">Error</th>
            </tr>
          </thead>
          <tbody>${filas}</tbody>
        </table>
        <p style="margin-top:1rem">
          Se pueden intentar recuperar manualmente desde Kore ERP con el botón
          "Recuperar de Kore ERP" en Historial &gt; Descarga SAT.
        </p>
        <p style="color:#6b7280;font-size:.85rem;margin-top:1.5rem">
          Este es un aviso automático de Numo — no responder a este correo.
        </p>
      </div>
    `,
  };
}

async function verificarCfdisNoRecuperados() {
  // `fecha` es un string 'YYYY-MM-DD' en día calendario MX (mismo criterio
  // que el resto del dominio SAT) — comparación lexicográfica de strings
  // funciona igual que comparación de fechas en este formato.
  const fmtMX = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Mexico_City', year: 'numeric', month: '2-digit', day: '2-digit' });
  const hoyMX = new Date();
  hoyMX.setDate(hoyMX.getDate() - DIAS_ATRAS);
  const desdeFecha = fmtMX.format(hoyMX);

  const pendientes = await SatJobCheckpoint.find({
    status: { $in: ['error', 'incompleto'] },
    fecha:  { $gte: desdeFecha },
  }).select('rfc fecha tipoComprobante status error updatedAt').sort({ rfc: 1, fecha: 1 }).lean();

  if (pendientes.length === 0) {
    logger.info('[CfdisNoRecuperadosAlert] Sin casos pendientes en la ventana revisada.');
    return;
  }

  const { subject, html } = construirCorreo(pendientes);
  const enviado = await emailSvc.enviarCorreo({ to: DESTINATARIOS, subject, html });
  if (enviado) {
    logger.info(`[CfdisNoRecuperadosAlert] Aviso enviado a ${DESTINATARIOS.join(', ')} — ${pendientes.length} caso(s).`);
  }
}

cron.schedule('0 3 * * *', async () => {
  try { await verificarCfdisNoRecuperados(); }
  catch (err) { logger.error(`[CfdisNoRecuperadosAlert] Error fatal: ${err.message}`); }
}, { timezone: 'America/Mexico_City' });

module.exports = { verificarCfdisNoRecuperados };
