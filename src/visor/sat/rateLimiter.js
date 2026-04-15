'use strict';

/**
 * Control de límites de Descarga Masiva SAT por RFC.
 *
 * Límites oficiales del SAT:
 *  - 10 solicitudes por RFC por día calendario (reinicio a medianoche)
 *  - 3 solicitudes activas simultáneas por RFC
 *
 * Implementado en memoria (Map). Al reiniciar el proceso el contador
 * arranca en 0, lo cual es correcto: si el servidor se reinicia en el
 * mismo día, el SAT ya cuenta las solicitudes anteriores, pero es
 * preferible perder el conteo a bloquear descargas legítimas. La
 * protección real es que el SAT mismo rechaza la solicitud #11.
 */

const MAX_DIARIO  = 10;
const MAX_ACTIVOS = 3;

// Map<rfc, { fecha: 'YYYY-MM-DD', solicitudes: number, activas: number }>
const _estado = new Map();

// Fecha en zona horaria de México — el día del SAT se reinicia a medianoche CDMX
const _hoy = () => new Date().toLocaleDateString('en-CA', { timeZone: 'America/Mexico_City' });

/** Obtiene (o crea/reinicia) la entrada del día para un RFC. */
const _get = (rfc) => {
  const hoy   = _hoy();
  let entry   = _estado.get(rfc);
  if (!entry || entry.fecha !== hoy) {
    entry = { fecha: hoy, solicitudes: 0, activas: 0 };
    _estado.set(rfc, entry);
  }
  return entry;
};

/**
 * Verifica si el RFC puede iniciar una nueva solicitud.
 * @returns {{ puede: boolean, razon?: string, codigo?: string }}
 */
const puedeIniciar = (rfc) => {
  const entry = _get(rfc);

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

/** Registra el inicio de una solicitud (incrementa ambos contadores). */
const registrarInicio = (rfc) => {
  const entry = _get(rfc);
  entry.solicitudes++;
  entry.activas++;
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
const getEstado = (rfc) => {
  const entry = _get(rfc);
  return {
    solicitudesHoy:  entry.solicitudes,
    activas:         entry.activas,
    limiteDiario:    MAX_DIARIO,
    limiteActivas:   MAX_ACTIVOS,
    disponiblesHoy:  Math.max(0, MAX_DIARIO - entry.solicitudes),
  };
};

module.exports = { puedeIniciar, registrarInicio, registrarFin, getEstado, MAX_DIARIO, MAX_ACTIVOS };
