'use strict';

const SatRateLimit = require('../models/SatRateLimit');

/**
 * Control de límites de Descarga Masiva SAT por RFC.
 *
 * Límites oficiales del SAT:
 *  - 10 solicitudes por RFC por día calendario (reinicio a medianoche CDMX)
 *  - 3 solicitudes activas simultáneas por RFC
 *
 * Estrategia híbrida:
 *  - El Map en memoria es la fuente primaria (sin latencia).
 *  - MongoDB es el respaldo: si el proceso se reinicia, se recupera el
 *    contador de solicitudes del día para no exceder el límite SAT.
 *  - Las "activas" no se persisten: al reiniciar el proceso, todas
 *    las descargas anteriores ya terminaron.
 */

const MAX_DIARIO  = 10;
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
 * Verifica si el RFC puede iniciar una nueva solicitud.
 * @returns {Promise<{ puede: boolean, razon?: string, codigo?: string }>}
 */
const puedeIniciar = async (rfc) => {
  const entry = await _get(rfc);

  if (entry.solicitudes >= MAX_DIARIO) {
    return {
      puede:  false,
      razon:  `Límite diario alcanzado: ${entry.solicitudes}/${MAX_DIARIO} solicitudes hoy para RFC ${rfc}. El contador se reinicia a medianoche.`,
      codigo: 'LIMITE_DIARIO',
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

/** Registra el inicio de una solicitud (incrementa ambos contadores y persiste). */
const registrarInicio = async (rfc) => {
  const entry = await _get(rfc);
  entry.solicitudes++;
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
