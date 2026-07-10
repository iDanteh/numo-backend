'use strict';

/**
 * visor/jobs/credencialesAlertJob.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Avisa por correo cuando las credenciales SAT (e.firma) de una entidad están
 * por vencer (TTL de 5 días desde que se subieron, ver visor/models/SATCredencial.js).
 *
 * Avisos automáticos (uno por umbral, no se repiten dentro de la misma
 * "generación" de credenciales — ver `alertasEnviadas` en el modelo):
 *   - 2 días antes de vencer
 *   - 1 día antes de vencer
 *   - N horas antes de vencer (config.alertasSat.horasAntes, default 4)
 *
 * También expone `enviarAlertaManual` para el botón "Enviar alerta ahora"
 * en la pantalla de Entidades.
 */

const cron   = require('node-cron');
const config = require('../../config/env');
const { logger } = require('../../shared/utils/logger');
const credencialesSvc = require('../sat/credenciales');
const entityRepo      = require('../repositories/entity.repository');
const emailSvc         = require('../../shared/services/email.service');

const TTL_MS      = 5 * 24 * 60 * 60 * 1000;
const UMBRAL_D2_H = 48;
const UMBRAL_D1_H = 24;

function horasRestantes(createdAt) {
  return (new Date(createdAt).getTime() + TTL_MS - Date.now()) / (1000 * 60 * 60);
}

function formatearRestante(horas) {
  if (horas >= 24) return `${Math.round(horas / 24)} día(s)`;
  if (horas >= 1)  return `${Math.round(horas)} hora(s)`;
  return `${Math.max(0, Math.round(horas * 60))} minuto(s)`;
}

/**
 * @param {{ nombre?: string, rfc: string, horas: number, urgente: boolean, faltan?: boolean }} opts
 *   `faltan: true` → no hay credenciales cargadas en absoluto (no aplica cuenta regresiva).
 *   `faltan: false` (default) → las credenciales existen pero están por vencer en `horas`.
 */
function construirCorreo({ nombre, rfc, horas, urgente, faltan = false }) {
  const empresa = nombre ? `${nombre} (RFC: ${rfc})` : `RFC: ${rfc}`;

  if (faltan) {
    return {
      subject: `⚠ Faltan las credenciales del SAT para Numo — ${rfc}`,
      html: `
        <div style="font-family:Arial,sans-serif;max-width:520px;margin:0 auto">
          <h2 style="color:#dc2626">⚠ Faltan credenciales del SAT</h2>
          <p>
            Le informamos que actualmente no se cuenta con las credenciales del
            SAT registradas en Numo para la entidad <strong>${empresa}</strong>.
          </p>
          <p>
            Mientras dichas credenciales no sean cargadas, el sistema no podrá
            realizar la descarga automática de los CFDIs correspondientes a
            esta entidad.
          </p>
          <p>
            Le solicitamos, por favor, cargar las credenciales del SAT a la
            brevedad posible para evitar retrasos en el proceso.
          </p>
          <p>Quedamos atentos a cualquier duda o comentario.</p>
          <p>Saludos cordiales.</p>
          <p style="color:#6b7280;font-size:.85rem;margin-top:1.5rem">
            Este es un aviso automático de Numo — no responder a este correo.
          </p>
        </div>
      `,
    };
  }

  const restante = formatearRestante(horas);
  const subject  = urgente
    ? `⚠ URGENTE: las credenciales del SAT para Numo vencen en ${restante} — ${rfc}`
    : `Aviso: las credenciales del SAT para Numo vencen en ${restante} — ${rfc}`;

  const html = `
    <div style="font-family:Arial,sans-serif;max-width:520px;margin:0 auto">
      <h2 style="color:${urgente ? '#dc2626' : '#1e3a5f'}">
        ${urgente ? '⚠ Última llamada' : 'Aviso de vencimiento'} — Credenciales SAT
      </h2>
      <p>Hola,</p>
      <p>
        Las credenciales del SAT (e.firma) para <strong>${empresa}</strong>
        cargadas en Numo <strong>vencen en aproximadamente ${restante}</strong>.
      </p>
      <p>
        Para que la descarga automática de CFDIs no se interrumpa, por favor
        vuelve a subir las credenciales en Numo antes de que expiren.
      </p>
      <p style="color:#6b7280;font-size:.85rem;margin-top:1.5rem">
        Este es un aviso automático de Numo — no responder a este correo.
      </p>
    </div>
  `;
  return { subject, html };
}

/**
 * Revisa todas las credenciales SAT guardadas y manda los avisos que
 * correspondan según cuánto falta para que venzan. Pensado para correr por
 * cron cada hora.
 */
async function verificarVencimientoCredenciales() {
  const credenciales = await credencialesSvc.listarParaAlertas();

  for (const cred of credenciales) {
    const horas = horasRestantes(cred.createdAt);
    if (horas <= 0) continue; // ya venció (o está por eliminarse), no hay nada que avisar

    const umbrales = [
      { tipo: 'd2',    umbral: UMBRAL_D2_H,                    urgente: false },
      { tipo: 'd1',    umbral: UMBRAL_D1_H,                    urgente: false },
      { tipo: 'horas', umbral: config.alertasSat.horasAntes,   urgente: true  },
    ];

    for (const { tipo, umbral, urgente } of umbrales) {
      if (cred.alertasEnviadas?.[tipo]) continue;
      if (horas > umbral) continue;

      const entity = await entityRepo.findByRfc(cred.rfc);
      if (!entity || !entity.isActive) continue;

      const emails = (entity.emailsAlerta || []).filter(Boolean);
      if (emails.length === 0) {
        logger.warn(`[credencialesAlertJob] ${cred.rfc} sin credenciales por vencer sin emailsAlerta configurado — se omite aviso`);
        continue;
      }

      const { subject, html } = construirCorreo({ nombre: entity.nombre, rfc: cred.rfc, horas, urgente });
      const enviado = await emailSvc.enviarCorreo({ to: emails, subject, html });
      if (enviado) await credencialesSvc.marcarAlertaEnviada(cred.rfc, tipo);
    }
  }
}

/**
 * Revisa las entidades con descarga nocturna automática habilitada y avisa
 * por correo a las que NO tienen ninguna credencial SAT cargada — a
 * diferencia de `verificarVencimientoCredenciales` (que solo mira
 * credenciales YA existentes), esto cubre el caso de una entidad que nunca
 * llegó a subir sus credenciales. Pensado para correr una vez al día
 * (8:00 am hora de México) — se repite cada día mientras falten, hasta que
 * se carguen.
 */
async function verificarCredencialesFaltantes() {
  const entidades = await entityRepo.findWithAutoSync();

  for (const entity of entidades) {
    const estado = await credencialesSvc.tieneCredenciales(entity.rfc);
    if (estado.tiene) continue;

    const emails = (entity.emailsAlerta || []).filter(Boolean);
    if (emails.length === 0) {
      logger.warn(`[credencialesAlertJob] ${entity.rfc} sin credenciales SAT y sin emailsAlerta configurado — se omite aviso`);
      continue;
    }

    const { subject, html } = construirCorreo({ nombre: entity.nombre, rfc: entity.rfc, horas: 0, urgente: true, faltan: true });
    await emailSvc.enviarCorreo({ to: emails, subject, html });
  }
}

/**
 * Envío manual e inmediato (botón "Enviar alerta ahora"), sin tocar las
 * banderas de `alertasEnviadas` — es independiente del ciclo automático.
 *
 * @param {import('../../shared/models/postgres').Entity} entity
 * @returns {Promise<{ ok: boolean, motivo?: string }>}
 */
async function enviarAlertaManual(entity) {
  const emails = (entity.emailsAlerta || []).filter(Boolean);
  if (emails.length === 0) {
    return { ok: false, motivo: 'Esta entidad no tiene correos de alerta configurados.' };
  }

  const estado = await credencialesSvc.tieneCredenciales(entity.rfc);
  if (!estado.tiene) {
    const { subject, html } = construirCorreo({ nombre: entity.nombre, rfc: entity.rfc, horas: 0, urgente: true, faltan: true });
    const enviado = await emailSvc.enviarCorreo({ to: emails, subject, html });
    return enviado ? { ok: true } : { ok: false, motivo: 'Error al enviar el correo (revisa la configuración SMTP).' };
  }

  const horas = estado.ttlSegundos / 3600;
  const { subject, html } = construirCorreo({ nombre: entity.nombre, rfc: entity.rfc, horas, urgente: horas <= config.alertasSat.horasAntes });
  const enviado = await emailSvc.enviarCorreo({ to: emails, subject, html });
  return enviado ? { ok: true } : { ok: false, motivo: 'Error al enviar el correo (revisa la configuración SMTP).' };
}

// ── Cron: revisa vencimientos cada hora ───────────────────────────────────────
cron.schedule('0 * * * *', async () => {
  try { await verificarVencimientoCredenciales(); }
  catch (err) { logger.error(`[credencialesAlertJob] Error fatal: ${err.message}`); }
}, { timezone: 'America/Mexico_City' });

// ── Cron: revisa credenciales faltantes a las 8:00 am ─────────────────────────
cron.schedule('0 8 * * *', async () => {
  try { await verificarCredencialesFaltantes(); }
  catch (err) { logger.error(`[credencialesAlertJob] Error fatal (faltantes): ${err.message}`); }
}, { timezone: 'America/Mexico_City' });

module.exports = { verificarVencimientoCredenciales, verificarCredencialesFaltantes, enviarAlertaManual };
