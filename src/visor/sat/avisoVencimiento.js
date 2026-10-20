'use strict';

/**
 * visor/sat/avisoVencimiento.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Banner de credenciales SAT en todas las vistas del frontend (2026-10-19,
 * pedido del usuario): aparece en cuanto empiezan los avisos de vencimiento
 * — 48 h antes, el mismo momento que el correo "vencen en 2 días" de
 * credencialesAlertJob.js — y no se quita hasta que se vuelvan a subir; si
 * ya vencieron (o nunca se subieron) sigue apareciendo como "faltan".
 * Solo entidades activas con descarga automática, igual que el aviso por
 * correo de credenciales faltantes (credencialesAlertJob.js).
 */

const entityRepo = require('../repositories/entity.repository');
const { tieneCredenciales } = require('./credenciales');

// Roles que ven el banner — mismo arreglo que ROLES_CON_AVISO en
// numo-frontend core/services/aviso-credenciales.service.ts.
const ROLES_CON_AVISO = ['admin', 'contabilidad'];

// Mismo umbral que el primer correo de vencimiento (UMBRAL_D2_H en
// credencialesAlertJob.js) — si cambia uno, cambiar el otro.
const HORAS_ANTES = 48;

/**
 * @param {Date} expiraEn
 * @returns {Date} desde cuándo se muestra el aviso
 */
function inicioAviso(expiraEn) {
  return new Date(expiraEn.getTime() - HORAS_ANTES * 60 * 60 * 1000);
}

/**
 * @param {Date} [ahora]
 * @returns {Promise<Array<{ rfc: string, nombre: string|null, estado: 'faltan'|'por_vencer', expiraEn: string|null }>>}
 */
async function avisosVigentes(ahora = new Date()) {
  const entidades = await entityRepo.findWithAutoSync();
  const avisos = [];

  for (const entity of entidades) {
    const { tiene, ttlSegundos } = await tieneCredenciales(entity.rfc);
    if (!tiene) {
      avisos.push({ rfc: entity.rfc, nombre: entity.nombre ?? null, estado: 'faltan', expiraEn: null });
      continue;
    }
    const expiraEn = new Date(ahora.getTime() + ttlSegundos * 1000);
    if (ahora >= inicioAviso(expiraEn)) {
      avisos.push({ rfc: entity.rfc, nombre: entity.nombre ?? null, estado: 'por_vencer', expiraEn: expiraEn.toISOString() });
    }
  }
  return avisos;
}

module.exports = { avisosVigentes, inicioAviso, ROLES_CON_AVISO };
