'use strict';

// anticipo-generado.service.js — recepción y trazabilidad de anticipos que Kore
// genera automáticamente por sobrepago de una CxC cobrada vía Solicitudes de
// Cobro (ver AnticipoGenerado.model.js para el porqué del diseño completo).

const AnticipoGenerado  = require('./AnticipoGenerado.model');
const CollectionRequest = require('./CollectionRequest.model');
const BankMovement      = require('../banks/BankMovement.model');
const bankService       = require('../banks/bank.service'); // setErpIds — mismo mecanismo que usa el panel de cobros
const { movimientosDe } = require('./collection-request-asignaciones');
const { BadRequestError } = require('../../shared/errors/AppError');
const { emitToAll } = require('../../shared/socket');
const { logger } = require('../../shared/utils/logger');

// Usuario sintético para las escrituras automáticas de _vincularAnticipoAlDeposito
// — no hay un humano detrás de este flujo (webhook de Kore / job de
// reconciliación). setErpIds() SÍ exige un `user` real con permiso
// banks:erp:link/banks:cobro (a diferencia del motor de autorizaciones
// automáticas, bank-autorizaciones.service.js, que escribe el movimiento directo
// sin pasar por setErpIds y por eso no lo necesita). role:'admin' resuelve el
// permiso vía el wildcard '*' ya sembrado en Postgres, sin tener que replicar
// esa lógica acá.
const USUARIO_MOTOR_ANTICIPO = Object.freeze({ _id: 'motor-anticipo', role: 'admin', nombre: 'Motor de Anticipos (automático)' });

// Fase 2 (2026-09-24) — ver comentario original en _resolverCorrelacion sobre por
// qué no se anticipó un job de reconciliación desde el día 1: se confirmó con
// datos reales de Test que el webhook de Kore (POST /erp/anticipos-generados)
// puede llegar ANTES de que Numo termine su propio identificar() — el caso real
// tardó ~7.5s en resolverse solo. 24h da margen de sobra para esa carrera sin
// reprocesar para siempre algo genuinamente huérfano/ambiguo (eso es trabajo de
// revisión manual desde el historial, no de este job).
const VENTANA_RECONCILIACION_MS = 24 * 60 * 60 * 1000;

function _toDate(v) {
  if (!v) return null;
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? null : d;
}

// Resuelve a qué CollectionRequest corresponde este anticipo — nunca adivina:
// 0 candidatas o ambigüedad real (2+ candidatas sin poder desempatar por fecha)
// se guardan explícitamente como "sin correlación automática" para revisión
// manual desde el historial, en vez de asumir la primera que aparezca.
async function _resolverCorrelacion(origenCuentaId, fechaCreacionAnticipo) {
  const candidatas = await CollectionRequest.find({
    'cxcs.erpId': origenCuentaId,
    status:       'identificada',
  }).sort({ resueltoAt: -1 }).lean();

  if (candidatas.length === 0) {
    return {
      cr: null,
      motivoSinCorrelacion: `No se encontró ninguna solicitud de cobro identificada con cxcs.erpId=${origenCuentaId}.`,
    };
  }

  if (candidatas.length === 1) {
    return { cr: candidatas[0], motivoSinCorrelacion: null };
  }

  // 2+ solicitudes contra la misma CxC (ej. abonos parciales PPD) — el excedente
  // solo pudo salir de la que se resolvió justo antes (o al momento) de que Kore
  // generara el anticipo. Si ninguna cumple esa condición temporal, no hay forma
  // confiable de elegir una sola — queda para revisión manual.
  const antesDelAnticipo = fechaCreacionAnticipo
    ? candidatas.filter(c => c.resueltoAt && new Date(c.resueltoAt).getTime() <= fechaCreacionAnticipo.getTime())
    : [];

  if (antesDelAnticipo.length === 1) {
    return { cr: antesDelAnticipo[0], motivoSinCorrelacion: null };
  }

  return {
    cr: null,
    motivoSinCorrelacion: `Ambiguo: ${candidatas.length} solicitudes identificadas encontradas con cxcs.erpId=${origenCuentaId}, ninguna se pudo desempatar de forma confiable por fecha.`,
  };
}

/**
 * Vincula el anticipo a CADA movimiento bancario de la solicitud correlacionada
 * — agrega un erpLink nuevo, ADITIVO (preserva todos los demás que el movimiento
 * ya tenía; setErpIds() reemplaza el arreglo completo, así que el resultado final
 * se arma acá, no ahí). Mismo shape que confirmErp() (erp-modal.component.ts) — no
 * se inventan campos nuevos. Reusa aplicarLogicaErp()/setErpIds() ya existentes,
 * cero cálculo nuevo: el mismo camino que ya usa un humano vinculando a mano
 * desde el modal ERP.
 *
 * Aislado por movimiento (una solicitud puede tener 2+ vía multi-bank-movement)
 * Y no relanza — un fallo acá (ej. Mongo con un hipo) nunca debe hacer perder la
 * trazabilidad del propio AnticipoGenerado, que el caller ya guardó/actualizó con
 * correlacionAutomatica:true antes de llamar a esto.
 */
async function _vincularAnticipoAlDeposito(anticipo, cr) {
  const erpLinkAnticipo = {
    erpId:               anticipo.anticipoIdErp,
    saldoActual:         anticipo.monto,
    saldoPagado:         null,
    // Fijo al monto que ESTE depósito aportó — no el saldoActual "vivo" que Kore
    // reportaría después si el anticipo se consume en una compra futura; ese
    // consumo es una operación DISTINTA que no debe alterar retroactivamente lo
    // que ya se identificó acá.
    saldoPagadoTotal:    anticipo.monto,
    folioFiscal:         null,
    total:               anticipo.monto,
    serie:               anticipo.anticipoSerieExterna,
    folioExterno:        anticipo.anticipoFolioExterno,
    tipoPago:            null,
    desglosePorFormaPago: [],
    origen:              'anticipo',
  };

  for (const movId of movimientosDe(cr)) {
    try {
      // eslint-disable-next-line no-await-in-loop
      const mov = await BankMovement.findById(movId).select('erpLinks').lean();
      if (!mov) continue; // no debería pasar, pero un movimiento borrado no debe tumbar el resto

      // Upsert por erpId — reemplaza la entrada si ya existía (reintento desde
      // la reconciliación), preserva TODAS las demás tal cual.
      const existentes = (mov.erpLinks || []).filter(l => l.erpId !== erpLinkAnticipo.erpId);
      const erpLinksFinales = [...existentes, erpLinkAnticipo];

      // eslint-disable-next-line no-await-in-loop
      await bankService.setErpIds(movId, erpLinksFinales, USUARIO_MOTOR_ANTICIPO);
    } catch (err) {
      logger.error(`[anticipo-generado] Error vinculando erpLink del anticipo ${anticipo.anticipoIdErp} al movimiento ${movId}: ${err.message}`);
    }
  }
}

async function registrarAnticipoGenerado(payload) {
  const anticipoIdErp = payload?.id != null ? String(payload.id).trim() : '';
  if (!anticipoIdErp) throw new BadRequestError('id (Cuenta.id del anticipo en Kore) es requerido');

  const total = Number(payload?.total);
  if (!(total > 0)) throw new BadRequestError('total (monto del anticipo) es requerido y debe ser > 0');

  const origenCuentaId = payload?.origenCuentaId != null ? String(payload.origenCuentaId).trim() : '';
  if (!origenCuentaId) throw new BadRequestError('origenCuentaId es requerido');

  // Idempotencia — mismo criterio que create() en collection-request.service.js:
  // un reintento de Kore por timeout no debe volver a correlacionar ni duplicar.
  const existente = await AnticipoGenerado.findOne({ anticipoIdErp });
  if (existente) return existente.toObject();

  const fechaCreacionKore = _toDate(payload?.fechaCreacion);
  const { cr, motivoSinCorrelacion } = await _resolverCorrelacion(origenCuentaId, fechaCreacionKore);

  const bankMovementIds = cr ? movimientosDe(cr) : [];

  let doc;
  try {
    doc = await AnticipoGenerado.create({
      anticipoIdErp,
      anticipoSerie:         payload?.serie ?? null,
      anticipoFolio:         payload?.folio ?? null,
      anticipoSerieExterna: payload?.serieExterna ?? null,
      anticipoFolioExterno: payload?.folioExterno ?? null,
      monto:             total,
      fechaCreacionKore,
      personaId:     payload?.personaId ?? null,
      nombrePersona: payload?.nombrePersona ?? null,
      anotacion:     payload?.anotacion ?? null,
      origenCuentaIdErp: origenCuentaId,
      solicitudCobroId:  cr?._id ?? null,
      bankMovementIds,
      correlacionAutomatica: !!cr,
      motivoSinCorrelacion,
    });
  } catch (err) {
    // Race condition: 2 avisos casi simultáneos con el mismo anticipoIdErp
    // (mismo criterio que el índice único de solicitudIdErp en create()).
    if (err.code === 11000) {
      const ganador = await AnticipoGenerado.findOne({ anticipoIdErp });
      if (ganador) return ganador.toObject();
    }
    throw err;
  }

  // No relanza (ver comentario del helper) — un fallo acá no debe impedir que
  // este mismo registro ya se haya guardado con correlacionAutomatica:true.
  if (cr) await _vincularAnticipoAlDeposito(doc, cr);

  const safe = doc.toObject();
  emitToAll('collection-request:anticipo-generado', { anticipoId: String(doc._id) });
  return safe;
}

async function listAnticiposGenerados(filters = {}) {
  const { page = 1, limit = 50, fechaInicio, fechaFin, nombrePersona, personaId, correlacionAutomatica } = filters;

  const filter = {};
  if (personaId)     filter.personaId = String(personaId).trim();
  if (nombrePersona) filter.nombrePersona = new RegExp(String(nombrePersona).trim().replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
  if (correlacionAutomatica === 'true' || correlacionAutomatica === true)   filter.correlacionAutomatica = true;
  if (correlacionAutomatica === 'false' || correlacionAutomatica === false) filter.correlacionAutomatica = false;
  if (fechaInicio || fechaFin) {
    filter.createdAt = {};
    if (fechaInicio) filter.createdAt.$gte = new Date(fechaInicio);
    if (fechaFin)    filter.createdAt.$lt  = new Date(new Date(fechaFin).getTime() + 24 * 60 * 60 * 1000);
  }

  const skip = (parseInt(page) - 1) * parseInt(limit);
  const [data, total] = await Promise.all([
    AnticipoGenerado.find(filter)
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(parseInt(limit))
      .populate('solicitudCobroId', 'solicitudIdErp monto status')
      .populate('bankMovementIds', 'banco fecha concepto deposito')
      .lean(),
    AnticipoGenerado.countDocuments(filter),
  ]);

  return {
    data,
    pagination: { total, page: parseInt(page), limit: parseInt(limit), pages: Math.ceil(total / parseInt(limit)) },
  };
}

/** Reconcilia UN documento pendiente — separado de reconciliarAnticiposPendientes()
 *  para que el try/catch por documento (allá abajo) quede claro y testeable. */
async function _reconciliarUno(anticipo) {
  const { cr, motivoSinCorrelacion } = await _resolverCorrelacion(anticipo.origenCuentaIdErp, anticipo.fechaCreacionKore);

  if (cr) {
    await AnticipoGenerado.updateOne(
      { _id: anticipo._id },
      {
        $set: {
          solicitudCobroId: cr._id,
          bankMovementIds: movimientosDe(cr),
          correlacionAutomatica: true,
          motivoSinCorrelacion: null,
        },
      },
    );
    // No relanza (ver comentario del helper) — un fallo acá no debe impedir que
    // este AnticipoGenerado ya haya quedado correlacionado.
    await _vincularAnticipoAlDeposito(anticipo, cr);
    emitToAll('collection-request:anticipo-generado', { anticipoId: String(anticipo._id) });
    return;
  }

  // Sigue sin match, pero el motivo pudo haber cambiado de texto (ej. pasó de "no
  // encontrada" a "ambiguo" porque ahora sí apareció alguna candidata) — se
  // actualiza para que el historial quede preciso, pero SIN emitir el socket:
  // nada cambió para quien está mirando la bandeja, sigue sin correlación.
  if (motivoSinCorrelacion !== anticipo.motivoSinCorrelacion) {
    await AnticipoGenerado.updateOne({ _id: anticipo._id }, { $set: { motivoSinCorrelacion } });
  }
}

/**
 * Job de respaldo (Fase 2) — reintenta la correlación de los anticipos que
 * quedaron sin resolver la primera vez, dentro de la ventana de reconciliación.
 * Corrido por cron cada 5 minutos (ver banks/jobs/anticipoGeneradoReconciliacionCron.js).
 * Cada documento se procesa AISLADO: un error puntual en uno (ej. Mongo con un
 * hipo) no debe frenar la reconciliación de los demás del mismo batch.
 */
async function reconciliarAnticiposPendientes() {
  const cutoff = new Date(Date.now() - VENTANA_RECONCILIACION_MS);
  const pendientes = await AnticipoGenerado.find({
    correlacionAutomatica: false,
    solicitudCobroId: null,
    createdAt: { $gte: cutoff },
  }).lean();

  for (const anticipo of pendientes) {
    try {
      // eslint-disable-next-line no-await-in-loop
      await _reconciliarUno(anticipo);
    } catch (err) {
      logger.error(`[anticipo-generado] Error reconciliando anticipo ${anticipo._id}: ${err.message}`);
    }
  }
}

module.exports = {
  registrarAnticipoGenerado, listAnticiposGenerados, reconciliarAnticiposPendientes,
  _resolverCorrelacion, _vincularAnticipoAlDeposito,
};
