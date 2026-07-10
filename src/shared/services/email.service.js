'use strict';

/**
 * shared/services/email.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Envío de correo saliente vía SMTP (nodemailer). Único punto de contacto con
 * el proveedor de correo — si mañana cambia (SendGrid, SES, etc.) solo se
 * toca este archivo.
 */

const nodemailer = require('nodemailer');
const config     = require('../../config/env');
const { logger } = require('../utils/logger');

let _transporter = null;

function getTransporter() {
  if (_transporter) return _transporter;

  if (!config.smtp.host || !config.smtp.user) {
    throw new Error(
      'SMTP no está configurado (faltan SMTP_HOST/SMTP_USER en las variables de entorno).',
    );
  }

  _transporter = nodemailer.createTransport({
    host:   config.smtp.host,
    port:   config.smtp.port,
    secure: config.smtp.secure,
    auth:   { user: config.smtp.user, pass: config.smtp.pass },
  });

  return _transporter;
}

/**
 * Envía un correo. No lanza si falla — registra el error y retorna `false`,
 * para que un job en background (ej. cron de alertas) no se caiga por un
 * problema puntual de SMTP.
 *
 * @param {{ to: string|string[], subject: string, html: string }} opts
 * @returns {Promise<boolean>} true si se envió correctamente
 */
async function enviarCorreo({ to, subject, html }) {
  const destinatarios = Array.isArray(to) ? to.filter(Boolean) : [to].filter(Boolean);
  if (destinatarios.length === 0) {
    logger.warn('[email.service] enviarCorreo: no hay destinatarios, se omite el envío');
    return false;
  }

  try {
    const transporter = getTransporter();
    await transporter.sendMail({
      from:    config.smtp.from,
      to:      destinatarios.join(', '),
      subject,
      html,
    });
    logger.info(`[email.service] Correo enviado a ${destinatarios.join(', ')} — "${subject}"`);
    return true;
  } catch (err) {
    logger.error(`[email.service] Error al enviar correo a ${destinatarios.join(', ')}: ${err.message}`);
    return false;
  }
}

module.exports = { enviarCorreo };
