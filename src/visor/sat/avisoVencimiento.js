'use strict';

/**
 * visor/sat/avisoVencimiento.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Banner de credenciales SAT en todas las vistas del frontend (2026-10-19,
 * pedido del usuario): aparece a partir de las 12:00 del día ANTERIOR al
 * vencimiento (hora de México) y no se quita hasta que se vuelvan a subir —
 * si ya vencieron (o nunca se subieron) sigue apareciendo como "faltan".
 * Solo entidades activas con descarga automática, igual que el aviso por
 * correo de credenciales faltantes (credencialesAlertJob.js).
 */

const entityRepo = require('../repositories/entity.repository');
const { tieneCredenciales } = require('./credenciales');

const fmtDiaMX = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'America/Mexico_City', year: 'numeric', month: '2-digit', day: '2-digit',
});

/**
 * 12:00 hora de México del día anterior al día (MX) en que vencen.
 * México no tiene horario de verano desde oct-2022 → 12:00 MX = 18:00 UTC.
 *
 * @param {Date} expiraEn
 * @returns {Date}
 */
function inicioAviso(expiraEn) {
  const [y, m, d] = fmtDiaMX.format(expiraEn).split('-').map(Number);
  return new Date(Date.UTC(y, m - 1, d - 1, 18, 0, 0));
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

module.exports = { avisosVigentes, inicioAviso };
