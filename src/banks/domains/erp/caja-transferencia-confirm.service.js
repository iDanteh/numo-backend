'use strict';

// caja-transferencia-confirm.service.js — Fase D del proceso de matching de
// transferencias entre cajas (ver plan acordado con el usuario 2026-09-01).
// Confirma un match sugerido por Fase C (caja-transferencia-match.service.js)
// contra 1 o 2 BankMovement, dejando rastro de quién lo autorizó.
//
// Reusa el mecanismo EXISTENTE de "IDs ERP" (bank.service.js#setErpIds) en vez
// de inventar un campo/estado nuevo — pedido explícito del usuario ("aprovechar
// esta columna existente"). El erpId es sintético (CAJA-<koreId>, nunca choca
// con un id real de Kore) y se distingue por origen:'transferencia-caja'. Es
// inofensivo para el resto del sistema: _syncErpKoreJob (erp.routes.js) y
// /sync-erp-kore/desvincular-cancelaciones (mismo archivo) solo tocan links con
// serie/folioExterno/conciliacionFinalizadaAt seteados — este link los deja
// todos en null/default, así que ningún job automático lo vuelve a tocar.
//
// saldoPagadoTotal/saldoPagado/total se setean al monto de CADA movimiento (no
// al monto total de la transferencia) para que aplicarLogicaErp() (bank.service.js)
// compute saldoErp === bankAmount de ESE movimiento y lo marque 'identificado' —
// necesario incluso en un split 1:2, donde cada movimiento cubre solo una parte.

const BankMovement      = require('../banks/BankMovement.model');
const CajaTransferencia = require('./CajaTransferencia.model');
const { setErpIds, ERP_TOLERANCE } = require('../banks/bank.service');
const { esCategoriaDepositoEfectivo } = require('./caja-transferencia-match.service');
const { NotFoundError, BadRequestError, ConflictError } = require('../../shared/errors/AppError');
const { emitToBanco } = require('../../shared/socket');
const mongoose = require('mongoose');

function _erpIdSintetico(transferencia) {
  return `CAJA-${transferencia.koreId}`;
}

async function _confirmarConSesion(transferencia, movimientos, user, session) {
  const erpId = _erpIdSintetico(transferencia);
  const actualizados = [];

  for (const mov of movimientos) {
    // eslint-disable-next-line no-await-in-loop
    const updated = await setErpIds(mov._id, [{
      erpId,
      origen:           'transferencia-caja',
      saldoPagadoTotal: mov.deposito,
      saldoPagado:      mov.deposito,
      total:            mov.deposito,
    }], user, { session });
    actualizados.push(updated);
  }

  await CajaTransferencia.updateOne(
    { _id: transferencia._id },
    {
      $set: {
        estatusMatch: 'matcheada',
        confirmadoPor: { userId: user?._id ?? null, nombre: user?.nombre || user?.email || null },
        confirmadoEn: new Date(),
        movementIdsConfirmados: movimientos.map(m => m._id),
      },
    },
    { session },
  );

  return actualizados;
}

// Confirma el match: transferencia debe estar 'pendiente', movementIds son 1 o 2
// BankMovement que el usuario eligió desde la bandeja (Fase D, UI todavía no
// implementada) — se RE-VALIDAN server-side contra la transferencia real, nunca se
// confía en que el cliente mandó un candidato válido (elegibilidad + suma de monto).
async function confirmarMatch(transferenciaId, movementIds, user) {
  const ids = [...new Set((movementIds ?? []).map(String))];
  if (ids.length < 1 || ids.length > 2) {
    throw new BadRequestError('Se requiere 1 o 2 movementIds.');
  }

  const transferencia = await CajaTransferencia.findById(transferenciaId);
  if (!transferencia) throw new NotFoundError('Transferencia de caja');
  if (transferencia.estatusMatch !== 'pendiente') {
    throw new ConflictError(`Esta transferencia ya no está pendiente (estatusMatch=${transferencia.estatusMatch}).`);
  }

  const movimientos = await BankMovement.find({ _id: { $in: ids } });
  if (movimientos.length !== ids.length) {
    throw new NotFoundError('Uno o más movimientos bancarios');
  }
  for (const mov of movimientos) {
    if (!esCategoriaDepositoEfectivo(mov.categoria)) {
      throw new ConflictError(`El movimiento ${mov._id} no es Depósito en efectivo.`);
    }
    if ((mov.erpLinks ?? []).length > 0) {
      throw new ConflictError(`El movimiento ${mov._id} ya tiene un ID ERP vinculado — puede que otro usuario ya lo haya usado.`);
    }
  }

  const suma = movimientos.reduce((acc, m) => acc + (m.deposito ?? 0), 0);
  if (Math.abs(suma - transferencia.monto) > ERP_TOLERANCE) {
    throw new ConflictError(
      `La suma de los movimientos elegidos (${suma}) no coincide con el monto de la transferencia (${transferencia.monto}).`,
    );
  }

  let session = null;
  let actualizados;
  try {
    session = await mongoose.connection.startSession();
    session.startTransaction();
    actualizados = await _confirmarConSesion(transferencia, movimientos, user, session);
    await session.commitTransaction();
  } catch (err) {
    if (session?.inTransaction?.()) {
      try { await session.abortTransaction(); } catch (_) { /* ignorar */ }
    }
    const sinSoporteTransacciones = err.code === 20
      || /transaction numbers are only allowed/i.test(err.message);
    if (!sinSoporteTransacciones) throw err;
    // Mongo standalone (sin replica set) — mismo fallback que mostrador-cyc.service.js.
    actualizados = await _confirmarConSesion(transferencia, movimientos, user, null);
  } finally {
    if (session) {
      try { await session.endSession(); } catch (_) { /* ignorar */ }
    }
  }

  for (const updated of actualizados) emitToBanco(updated.banco, 'bank:movement:updated', updated);

  return { transferencia: { ...transferencia.toObject(), estatusMatch: 'matcheada' }, movimientos: actualizados };
}

module.exports = { confirmarMatch };
