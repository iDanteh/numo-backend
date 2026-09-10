'use strict';

// anticipo-generado.service.js — recepción y trazabilidad de anticipos que Kore
// genera automáticamente por sobrepago de una CxC cobrada vía Solicitudes de
// Cobro (ver AnticipoGenerado.model.js para el porqué del diseño completo).

const AnticipoGenerado  = require('./AnticipoGenerado.model');
const CollectionRequest = require('./CollectionRequest.model');
const { movimientosDe } = require('./collection-request-asignaciones');
const { BadRequestError } = require('../../shared/errors/AppError');
const { emitToAll } = require('../../shared/socket');

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

module.exports = { registrarAnticipoGenerado, listAnticiposGenerados, _resolverCorrelacion };
