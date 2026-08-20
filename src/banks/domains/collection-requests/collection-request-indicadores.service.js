'use strict';

// collection-request-indicadores.service.js — indicador de tiempo de identificación
// ACOTADO a Solicitudes de Cobro. A diferencia de
// bank-indicadores.service.js#getIndicadoresIdentificacion (que mide TODOS los
// BankMovement identificados, sin importar la vía: fichas, aplicación directa de cobro,
// motores automáticos, traspasos internos, etc.), este indicador solo cuenta
// CollectionRequest — por construcción, nunca se mezcla con otras vías. Pedido explícito
// del usuario (2026-08-20): el dashboard general "ensucia" el dato que necesita reportar
// (tiempo de respuesta entre que una tienda crea una solicitud y el contador la
// identifica), porque hoy mezcla movimientos identificados por vías completamente
// distintas a una solicitud de cobro.
//
// Se investigó primero si convenía filtrar el dashboard existente (BankMovement) en vez de
// crear este archivo nuevo — se descartó: `BankMovement.primeraIdentificacionAt` es
// INMUTABLE (se setea la PRIMERA vez que el status pasa a 'identificado', ver
// identificacion-timestamp.util.js) y no necesariamente corresponde a la identificación vía
// esta solicitud si el movimiento ya había sido identificado antes por otra vía. Calcular
// esto directo sobre CollectionRequest (createdAt/resueltoAt, ambos ya existen y se escriben
// en el mismo identificar()) evita ese problema de raíz.

const CollectionRequest = require('./CollectionRequest.model');
const { movimientosDe } = require('./collection-request-asignaciones');
const { horasHabilesEntre, promedio, mediana } = require('../banks/bank-indicadores.service');

const MS_PER_HOUR = 3600000;

// Fecha de corte del indicador — decisión explícita del usuario (2026-08-20, mismo
// criterio que INDICADORES_DESDE en bank-indicadores.service.js): solo se miden
// solicitudes creadas desde esta fecha en adelante, para no ensuciar el promedio con
// historial viejo. Las solicitudes MÁS VIEJAS que este corte simplemente no entran al
// cálculo — no se borran ni se ocultan en ningún otro lado, sigue existiendo el conteo
// `sinMovimientoVinculado` para lo que SÍ entra al corte. Si el deploy real ocurre en
// otra fecha, ACTUALIZAR este valor a mano antes de desplegar.
const INDICADORES_CR_DESDE = new Date(2026, 7, 20);

// Fase 1 (solicitud creada -> depósito cargado en Numo) y el TOTAL (solicitud creada ->
// identificada) se miden en RELOJ real, 24/7 — decisión explícita del usuario (2026-08-20):
// esa demora depende del banco/Kore (puede tardar hasta 72h en verse el depósito), no de un
// contador trabajando en horario laboral, así que horas hábiles subestimaría cuánto tardó en
// realidad si cae de noche o fin de semana. Solo la Fase 2 (depósito cargado -> identificada
// por el contador) usa horasHabilesEntre() (mismo criterio que el dashboard general de
// Bancos, bank-indicadores.service.js), porque ESA sí es trabajo activo de un humano.
function horasReloj(inicio, fin) {
  if (!(inicio instanceof Date) || !(fin instanceof Date) || !(fin > inicio)) return 0;
  return (fin.getTime() - inicio.getTime()) / MS_PER_HOUR;
}

// 2026-08-20 (fix real, reportado por el usuario con datos de producción): el depósito
// bancario casi siempre YA EXISTE en Numo ANTES de que la tienda cree la solicitud (por
// eso fase1Banco sale ~0 la mayoría de las veces) — pero la fase contador seguía
// arrancando el reloj en `movCreatedAt` (la fecha en que ese BankMovement se importó,
// que puede ser mucho más vieja que la solicitud misma), inflando "Fase Contador" con
// horas hábiles que transcurrieron ANTES de que hubiera nada que identificar. El reloj
// del contador nunca puede arrancar antes de que la solicitud exista — se usa el punto
// MÁS TARDÍO entre ambas fechas.
function inicioFaseContador(crCreatedAt, movCreatedAt) {
  return movCreatedAt > crCreatedAt ? movCreatedAt : crCreatedAt;
}

/**
 * @param {object} [filtros]
 * @param {string|number} [filtros.year]
 * @param {string|number} [filtros.month] (1-12, requiere year)
 */
async function getIndicadoresSolicitudesCobro({ year, month } = {}) {
  const match = { status: 'identificada', resueltoAt: { $ne: null } };
  if (year) {
    const y = parseInt(year, 10);
    const m = month != null ? parseInt(month, 10) - 1 : null;
    const desde = m != null ? new Date(y, m, 1)     : new Date(y, 0, 1);
    const hasta = m != null ? new Date(y, m + 1, 1) : new Date(y + 1, 0, 1);
    // El corte de frescura (INDICADORES_CR_DESDE) sigue aplicando aunque el usuario pida
    // un year/month explícito más viejo — un rango year=2025 nunca debería "esquivar" el
    // corte y volver a mezclar datos rancios. `desde` gana si es más reciente que el corte.
    match.createdAt = { $gte: desde > INDICADORES_CR_DESDE ? desde : INDICADORES_CR_DESDE, $lt: hasta };
  } else {
    match.createdAt = { $gte: INDICADORES_CR_DESDE };
  }

  const solicitudes = await CollectionRequest.find(match)
    .select('createdAt resueltoAt resueltoPorUserId resueltoPorNombre formasPago.bankMovementId bankMovementId')
    .populate('formasPago.bankMovementId', 'createdAt')
    .populate('bankMovementId', 'createdAt')
    .lean();

  const totalHorasArr = [];
  const fase1HorasArr = [];
  const fase2HorasArr = [];
  const porUsuarioMap = new Map();
  let sinMovimientoVinculado = 0;

  for (const cr of solicitudes) {
    const totalHoras = horasReloj(cr.createdAt, cr.resueltoAt);
    totalHorasArr.push(totalHoras);

    // 2026-08-20 (fix real, reportado por el usuario): "Por contador" mostraba el
    // promedio del TOTAL (creada->resuelta, reloj real) partido por resueltoPorUserId —
    // eso mezclaba la demora de banco/Kore (fuera del control del contador) en la métrica
    // "por contador", y explicaba el desfase real que el usuario vio (cada contador
    // promediando ~4min de TOTAL, mientras el bucket "Fase Contador" de arriba —
    // horasHabilesEntre del PRIMER movimiento a resueltoAt, para TODA la población —
    // mostraba 3h33m). Ahora "Por contador" acumula fase2Horas (la misma fase-contador
    // del bucket de arriba), NO totalHoras — así el desglose por persona es
    // consistente con el agregado que está justo arriba, y aísla lo que cada contador
    // realmente controla (nunca la demora del banco/Kore).
    const [primerMov] = movimientosDe(cr);
    const key = cr.resueltoPorUserId ?? '__sin_usuario__';
    if (!porUsuarioMap.has(key)) {
      porUsuarioMap.set(key, {
        userId: cr.resueltoPorUserId ?? null,
        nombre: cr.resueltoPorNombre ?? null,
        fase2Horas: [],
      });
    }

    if (primerMov?.createdAt) {
      fase1HorasArr.push(horasReloj(cr.createdAt, primerMov.createdAt));
      const fase2 = horasHabilesEntre(inicioFaseContador(cr.createdAt, primerMov.createdAt), cr.resueltoAt);
      fase2HorasArr.push(fase2);
      porUsuarioMap.get(key).fase2Horas.push(fase2);
    } else {
      // No debería pasar para status:'identificada' (identificar() siempre asigna al
      // menos 1 movimiento) — se cubre por si acaso un dato histórico quedó inconsistente
      // (ej. documentos de antes del backfill de formasPago[].bankMovementId). Cuenta
      // para el total, pero no aporta a fase1/fase2/por-contador (no hay con qué partir
      // el rango).
      sinMovimientoVinculado++;
    }
  }

  // Solo contadores con al menos 1 solicitud CON movimiento vinculado — sin eso no hay
  // fase2 que promediar para esa persona (evita mostrar un promedio null/NaN en la tabla).
  const porUsuario = [...porUsuarioMap.values()]
    .filter(u => u.fase2Horas.length > 0)
    .map(u => ({
      userId: u.userId,
      nombre: u.nombre,
      promedioHoras: promedio(u.fase2Horas),
      count: u.fase2Horas.length,
    }))
    .sort((a, b) => b.count - a.count);

  return {
    totalSolicitudesResueltas: solicitudes.length,
    sinMovimientoVinculado,
    total:         { promedioHoras: promedio(totalHorasArr), medianaHoras: mediana(totalHorasArr), count: totalHorasArr.length },
    fase1Banco:    { promedioHoras: promedio(fase1HorasArr), medianaHoras: mediana(fase1HorasArr), count: fase1HorasArr.length },
    fase2Contador: { promedioHoras: promedio(fase2HorasArr), medianaHoras: mediana(fase2HorasArr), count: fase2HorasArr.length },
    porUsuario,
  };
}

module.exports = { getIndicadoresSolicitudesCobro, horasReloj, inicioFaseContador, INDICADORES_CR_DESDE };
