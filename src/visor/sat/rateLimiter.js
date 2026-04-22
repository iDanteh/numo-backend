'use strict';

const SatRateLimit = require('../models/SatRateLimit');

/**
 * Control de límites de Descarga Masiva SAT por RFC.
 *
 * Límites del SAT Web Service (según documentación oficial):
 *  - NO existe un límite diario documentado para solicitudes del WS.
 *    Los errores relevantes son:
 *      5002 — "Se agotó las solicitudes de por vida" (límite vitalicio por RFC+fechas+tipo)
 *      5005 — Solicitud duplicada (ya existe una activa con los mismos parámetros)
 *      5011 — Límite de descargas por folio por día (en la etapa de descarga de paquetes)
 *    Empíricamente se reportan ~10 solicitudes/día antes de recibir 5002/throttling.
 *  - Máximo 2 descargas por paquete (error 5008 al exceder).
 *  - Los paquetes vencen a las 72 horas (error 5007/5006).
 *  - Hasta 200,000 CFDIs por solicitud XML; hasta 1,000,000 registros por solicitud Metadata.
 *
 * Estrategia híbrida:
 *  - El Map en memoria es la fuente primaria (sin latencia).
 *  - MongoDB es el respaldo: si el proceso se reinicia, se recupera el
 *    contador de solicitudes del día para no exceder el límite SAT.
 *  - Las "activas" no se persisten: al reiniciar el proceso, todas
 *    las descargas anteriores ya terminaron.
 */

const MAX_DIARIO  = 10; // Límite empírico conservador; la documentación oficial no especifica límite diario para WS
const MAX_ACTIVOS = 3;

// Map<rfc, { fecha: 'YYYY-MM-DD', solicitudes: number, activas: number }>
const _estado = new Map();

// Fecha en zona horaria de México — el día del SAT se reinicia a medianoche CDMX
const _hoy = () => new Date().toLocaleDateString('en-CA', { timeZone: 'America/Mexico_City' });

/**
 * Obtiene (o crea/reinicia) la entrada del día para un RFC.
 * Si no está en memoria, intenta cargar desde MongoDB.
 */
const _get = async (rfc) => {
  const hoy  = _hoy();
  let entry  = _estado.get(rfc);

  if (!entry || entry.fecha !== hoy) {
    // Intentar recuperar conteo persistido de hoy
    try {
      const stored = await SatRateLimit.findOne({ rfc: rfc.toUpperCase() }).lean();
      if (stored && stored.fecha === hoy) {
        // Hoy ya hubo solicitudes antes del reinicio — respetarlas
        entry = { fecha: hoy, solicitudes: stored.solicitudes, activas: 0 };
      } else {
        entry = { fecha: hoy, solicitudes: 0, activas: 0 };
      }
    } catch {
      // Si MongoDB no responde, arrancar desde 0 (conservador pero no bloqueante)
      entry = { fecha: hoy, solicitudes: 0, activas: 0 };
    }
    _estado.set(rfc, entry);
  }

  return entry;
};

/**
 * Persiste el contador de solicitudes en MongoDB (fire-and-forget).
 */
const _persistir = (rfc, solicitudes) => {
  SatRateLimit.findOneAndUpdate(
    { rfc: rfc.toUpperCase() },
    { $set: { fecha: _hoy(), solicitudes, updatedAt: new Date() } },
    { upsert: true },
  ).catch(() => {}); // No bloquear el flujo principal si MongoDB falla
};

/**
 * Verifica si el RFC puede iniciar `count` solicitudes SAT nuevas.
 * @param {string} rfc
 * @param {number} [count=1]  — Cuántas solicitudes SAT se harán (1 o 5 para splits por subtipo).
 * @returns {Promise<{ puede: boolean, razon?: string, codigo?: string, disponibles?: number, necesarias?: number }>}
 */
const puedeIniciar = async (rfc, count = 1) => {
  const entry = await _get(rfc);

  if (entry.solicitudes + count > MAX_DIARIO) {
    const disponibles = Math.max(0, MAX_DIARIO - entry.solicitudes);
    return {
      puede:      false,
      razon:      disponibles === 0
        ? `Límite diario alcanzado: ${entry.solicitudes}/${MAX_DIARIO} solicitudes hoy para RFC ${rfc}. El contador se reinicia a medianoche.`
        : `Solicitudes insuficientes: esta descarga necesita ${count} pero solo quedan ${disponibles} de ${MAX_DIARIO} para RFC ${rfc} hoy.`,
      codigo:     'LIMITE_DIARIO',
      disponibles,
      necesarias: count,
    };
  }

  if (entry.activas >= MAX_ACTIVOS) {
    return {
      puede:  false,
      razon:  `Límite de solicitudes activas alcanzado: ${entry.activas}/${MAX_ACTIVOS} activas para RFC ${rfc}. Espera a que terminen antes de iniciar otra.`,
      codigo: 'LIMITE_ACTIVAS',
    };
  }

  return { puede: true };
};

/**
 * Registra el inicio de un job (incrementa `activas` en 1 y `solicitudes` en `count`).
 * @param {string} rfc
 * @param {number} [count=1]  — Cuántas solicitudes SAT consume este job.
 */
const registrarInicio = async (rfc, count = 1) => {
  const entry = await _get(rfc);
  entry.solicitudes += count;
  entry.activas++;
  _persistir(rfc, entry.solicitudes);
};

/**
 * Registra la finalización de una solicitud (decrementa activas).
 * Debe llamarse siempre en el bloque finally del job.
 */
const registrarFin = (rfc) => {
  const entry = _estado.get(rfc);
  if (entry && entry.activas > 0) entry.activas--;
};

/** Retorna el estado actual de límites para un RFC (para el endpoint informativo). */
const getEstado = async (rfc) => {
  const entry = await _get(rfc);
  return {
    solicitudesHoy:  entry.solicitudes,
    activas:         entry.activas,
    limiteDiario:    MAX_DIARIO,
    limiteActivas:   MAX_ACTIVOS,
    disponiblesHoy:  Math.max(0, MAX_DIARIO - entry.solicitudes),
  };
};

module.exports = { puedeIniciar, registrarInicio, registrarFin, getEstado, MAX_DIARIO, MAX_ACTIVOS };
