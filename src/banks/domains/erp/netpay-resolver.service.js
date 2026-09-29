'use strict';

// netpay-resolver.service.js — netpay-matching-v2 (design.md "Approach": "Candidate picker
// removed for Netpay"): reemplaza confirmarMatchNetpay/descartarMatchNetpay (removidos, ver
// netpay-match-confirm.service.js) para un bucket NetpayMatch. `resolver` cierra un bucket
// 'discrepancia' con una justificación humana OBLIGATORIA, opcionalmente vinculando 1-2
// BankMovement — preserva la capacidad de split manual que ya tenía v1 (design.md "Resolve
// movement cardinality": nunca más de 2). `rechazar` descarta un bucket desde cualquier
// estado activo (spec.md "any active state -> rechazado").
//
// GUARD DE DISEÑO NO NEGOCIABLE (spec.md "Manual action cannot force auto-confirmed"): este
// archivo JAMÁS escribe estatusMatch:'confirmado_automatico' — el único resultado posible de
// `resolver` es 'resuelto_manual'.

const BankMovement = require('../banks/BankMovement.model');
const NetpayMatch = require('./NetpayMatch.model');
const { setErpIds } = require('../banks/bank.service');
const { NotFoundError, BadRequestError, ConflictError } = require('../../shared/errors/AppError');
const { emitToBanco, emitToAll } = require('../../shared/socket');

// Estados desde los que `rechazar` YA NO puede actuar — el resto ("any active state",
// spec.md) sí admite rechazo, incluido pendiente_por_marca/discrepancia/
// confirmado_automatico/resuelto_por_reporte.
const ESTATUS_TERMINALES = new Set(['rechazado', 'resuelto_manual']);

// erpId sintético de una resolución manual — sufijo -MANUAL para que nunca colisione con
// el erpId automático (NETPAY-<terminalID>-<día>[-<bucket>]) del mismo terminal+día+bucket,
// ni con uno previamente revertido.
function _erpIdManual(bucket) {
  const fecha = new Date(bucket.dia).toISOString().slice(0, 10);
  const sufijo = bucket.bucket === 'general' ? '' : `-${bucket.bucket}`;
  return `NETPAY-${bucket.terminalID}-${fecha}${sufijo}-MANUAL`;
}

async function resolver(id, { justificacion, movementIds } = {}, user) {
  const justificacionLimpia = justificacion ? String(justificacion).trim() : '';
  if (!justificacionLimpia) throw new BadRequestError('Se requiere una justificación.');

  const ids = [...new Set((movementIds ?? []).map(String))];
  if (ids.length > 2) throw new BadRequestError('Se permiten a lo sumo 2 movementIds.');

  const bucket = await NetpayMatch.findById(id);
  if (!bucket) throw new NotFoundError('Bucket Netpay');
  if (bucket.estatusMatch !== 'discrepancia') {
    throw new ConflictError(`Este bucket no está en discrepancia (estatusMatch=${bucket.estatusMatch}).`);
  }

  const actualizados = [];
  if (ids.length > 0) {
    const movimientos = await BankMovement.find({ _id: { $in: ids } });
    if (movimientos.length !== ids.length) throw new NotFoundError('Uno o más movimientos bancarios');
    for (const mov of movimientos) {
      if (mov.banco !== 'BBVA') throw new ConflictError(`El movimiento ${mov._id} no es de BBVA.`);
      if ((mov.erpLinks ?? []).length > 0) {
        throw new ConflictError(`El movimiento ${mov._id} ya tiene un ID ERP vinculado — puede que otro usuario ya lo haya usado.`);
      }
    }
    const erpId = _erpIdManual(bucket);
    for (const mov of movimientos) {
      // eslint-disable-next-line no-await-in-loop
      const actualizado = await setErpIds(mov._id, [{
        erpId, origen: 'netpay-matching-manual',
        saldoPagadoTotal: mov.deposito, saldoPagado: mov.deposito, total: mov.deposito,
      }], user);
      actualizados.push(actualizado);
    }
  }

  bucket.estatusMatch = 'resuelto_manual';
  bucket.motivoDiscrepancia = null;
  bucket.justificacion = justificacionLimpia;
  bucket.resueltoManualPor = { userId: user?._id ?? null, nombre: user?.nombre || user?.email || null };
  bucket.resueltoManualEn = new Date();
  if (ids.length > 0) bucket.movementIdsConfirmados = ids;
  await bucket.save();

  for (const actualizado of actualizados) {
    emitToBanco(actualizado.banco, 'bank:movement:updated', actualizado);
    emitToAll('bank:ficha-pendiente:changed', { movementId: actualizado._id });
  }

  return { bucket: bucket.toObject(), movimientos: actualizados };
}

async function rechazar(id, { motivo } = {}, user) {
  const bucket = await NetpayMatch.findById(id);
  if (!bucket) throw new NotFoundError('Bucket Netpay');
  if (ESTATUS_TERMINALES.has(bucket.estatusMatch)) {
    throw new ConflictError(`Este bucket ya está en un estado terminal (estatusMatch=${bucket.estatusMatch}).`);
  }

  bucket.estatusMatch = 'rechazado';
  bucket.rechazoMotivo = motivo ? String(motivo).trim() || null : null;
  bucket.descartadoManualmentePor = { userId: user?._id ?? null, nombre: user?.nombre || user?.email || null };
  bucket.descartadoManualmenteEn = new Date();
  await bucket.save();

  return { bucket: bucket.toObject() };
}

module.exports = { resolver, rechazar };
