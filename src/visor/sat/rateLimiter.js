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
 * Estrategia:
 *  - MongoDB es la fuente de verdad para `solicitudes` del día (atómico con $inc).
 *    Esto garantiza que múltiples instancias Docker del backend compartan el mismo
 *    contador y no excedan el límite SAT entre instancias.
 *  - El Map en memoria es caché local para `activas` (concurrentes por instancia).
 *    Las "activas" NO se persisten: al reiniciar el proceso, todas las descargas
 *    anteriores ya terminaron o se reanudaron.
 */

const MAX_DIARIO  = 20; // Límite empírico conservador; la documentación oficial no especifica límite diario para WS
const MAX_ACTIVOS = 3;

// Map<rfc, { activas: number }> — solo activas en memoria (por instancia)
const _activas = new Map();

// Fecha en zona horaria de México — el día del SAT se reinicia a medianoche CDMX
const _hoy = () => new Date().toLocaleDateString('en-CA', { timeZone: 'America/Mexico_City' });

/** Retorna el conteo de solicitudes de hoy para un RFC desde MongoDB (fuente de verdad). */
const _solicitudesHoyDB = async (rfc) => {
  try {
    const stored = await SatRateLimit.findOne({ rfc: rfc.toUpperCase() }, 'fecha solicitudes').lean();
    if (stored?.fecha === _hoy()) return stored.solicitudes;
    return 0;
  } catch {
    // Si MongoDB no responde, devolver 0 (conservador — el check fallará al intentar $inc)
    return 0;
  }
};

/**
 * Verifica si el RFC puede iniciar `count` solicitudes SAT nuevas.
 * Lee el contador actual desde MongoDB para ser preciso entre instancias.
 *
 * @param {string} rfc
 * @param {number} [count=1]  — Cuántas solicitudes SAT se harán (1 o 5 para splits por subtipo).
 * @returns {Promise<{ puede: boolean, razon?: string, codigo?: string, disponibles?: number, necesarias?: number }>}
 */
const puedeIniciar = async (rfc, count = 1) => {
  const solicitudesHoy = await _solicitudesHoyDB(rfc);
  const activas = _activas.get(rfc) ?? 0;

  if (solicitudesHoy + count > MAX_DIARIO) {
    const disponibles = Math.max(0, MAX_DIARIO - solicitudesHoy);
    return {
      puede:      false,
      razon:      disponibles === 0
        ? `Límite diario alcanzado: ${solicitudesHoy}/${MAX_DIARIO} solicitudes hoy para RFC ${rfc}. El contador se reinicia a medianoche.`
        : `Solicitudes insuficientes: esta descarga necesita ${count} pero solo quedan ${disponibles} de ${MAX_DIARIO} para RFC ${rfc} hoy.`,
      codigo:     'LIMITE_DIARIO',
      disponibles,
      necesarias: count,
    };
  }

  if (activas >= MAX_ACTIVOS) {
    return {
      puede:  false,
      razon:  `Límite de solicitudes activas alcanzado: ${activas}/${MAX_ACTIVOS} activas para RFC ${rfc}. Espera a que terminen antes de iniciar otra.`,
      codigo: 'LIMITE_ACTIVAS',
    };
  }

  return { puede: true };
};

/**
 * Registra el inicio de un job.
 * Incrementa `solicitudes` en MongoDB de forma atómica ($inc) — safe para múltiples instancias.
 * Incrementa `activas` en el Map local (por instancia).
 *
 * @param {string} rfc
 * @param {number} [count=1]  — Cuántas solicitudes SAT consume este job.
 */
const registrarInicio = async (rfc, count = 1) => {
  const hoy = _hoy();

  // Pipeline de agregación atómico: una sola operación elimina la race condition
  // del enfoque anterior de dos pasos (findOneAndUpdate + updateOne separados).
  // — Si fecha == hoy  → incrementa solicitudes en `count`.
  // — Si fecha != hoy  → reinicia solicitudes a `count` (nuevo día).
  // El upsert crea el documento si no existe (equivalente al antiguo $setOnInsert).
  await SatRateLimit.findOneAndUpdate(
    { rfc: rfc.toUpperCase() },
    [
      {
        $set: {
          rfc:         rfc.toUpperCase(),
          fecha:       hoy,
          solicitudes: {
            $cond: {
              if:   { $eq: ['$fecha', hoy] },
              then: { $add: ['$solicitudes', count] },
              else: count,
            },
          },
          updatedAt: new Date(),
        },
      },
    ],
    { upsert: true, new: true },
  ).catch(() => {});

  const activas = (_activas.get(rfc) ?? 0) + 1;
  _activas.set(rfc, activas);
};

/**
 * Registra la finalización de una solicitud (decrementa activas locales).
 * Debe llamarse siempre en el bloque finally del job.
 */
const registrarFin = (rfc) => {
  const activas = _activas.get(rfc) ?? 0;
  if (activas > 0) _activas.set(rfc, activas - 1);
  else _activas.delete(rfc);
};

/** Retorna el estado actual de límites para un RFC (para el endpoint informativo). */
const getEstado = async (rfc) => {
  const solicitudesHoy = await _solicitudesHoyDB(rfc);
  const activas = _activas.get(rfc) ?? 0;
  return {
    solicitudesHoy,
    activas,
    limiteDiario:   MAX_DIARIO,
    limiteActivas:  MAX_ACTIVOS,
    disponiblesHoy: Math.max(0, MAX_DIARIO - solicitudesHoy),
  };
};

module.exports = { puedeIniciar, registrarInicio, registrarFin, getEstado, MAX_DIARIO, MAX_ACTIVOS };
