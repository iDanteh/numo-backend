'use strict';

// netpay-match-confirm.service.js — Fase D del matching Netpay↔BBVA (ver
// netpay-match.service.js y project_netpay_transacciones.md). Confirma/descarta un grupo
// (terminalID+día) sugerido por la bandeja, dejando rastro en NetpayMatch. Mismo mecanismo
// que caja-transferencia-confirm.service.js: reusa setErpIds() (bank.service.js) con un
// erpId sintético — nunca choca con un id real de Kore, e inofensivo para el resto del
// sistema (_syncErpKoreJob solo toca links con serie/folioExterno seteados, este link los
// deja en null/default).
//
// A diferencia de la bandeja (que confía en el neto ya calculado al listar), acá se
// RECALCULA el neto EN VIVO contra Kore para el día exacto antes de validar — puede haber
// llegado una transacción tardía desde que se cargó la bandeja, y confirmar contra un total
// desactualizado vincularía un depósito que en realidad no cuadra.

const BankMovement = require('../banks/BankMovement.model');
const NetpayMatch = require('./NetpayMatch.model');
const { setErpIds, ERP_TOLERANCE } = require('../banks/bank.service');
const { consultarTransaccionesNetpay } = require('./netpay-transacciones.service');
const { _diaUTC, _buscarCandidatosParaGrupo, _montosIguales } = require('./netpay-match.service');
const { NotFoundError, BadRequestError, ConflictError } = require('../../shared/errors/AppError');
const { emitToBanco, emitToAll } = require('../../shared/socket');
const mongoose = require('mongoose');

function _erpIdSintetico(terminalID, diaUTC) {
  return `NETPAY-${terminalID}-${diaUTC.toISOString().slice(0, 10)}`;
}

// Recalcula el neto EN VIVO para un terminalID+día exacto — acotado a ese único día
// (dateFrom/dateTo del mismo día), y se vuelve a filtrar por _diaUTC en memoria por si
// Kore devolviera algo fuera de rango por algún desfase de huso horario (mismo criterio
// defensivo que el resto del dominio ERP con fechas de Kore).
async function _recalcularNetoEnVivo(terminalID, diaUTC) {
  const dateFrom = diaUTC.toISOString();
  const dateTo = new Date(diaUTC.getTime() + 24 * 60 * 60 * 1000 - 1).toISOString();
  const { transacciones } = await consultarTransaccionesNetpay({ dateFrom, dateTo, terminalID });

  const delDia = transacciones.filter(t => _diaUTC(t.transactionDate).getTime() === diaUTC.getTime());
  const montoBruto = delDia.reduce((acc, t) => acc + (t.amount ?? 0), 0);
  const comision = delDia.reduce((acc, t) => acc + (t.commission ?? 0), 0);
  return { netoEsperado: montoBruto - comision, cantidadTransacciones: delDia.length };
}

async function _confirmarConSesion(terminalID, almacen, diaUTC, netoEsperado, movimientos, user, session) {
  const erpId = _erpIdSintetico(terminalID, diaUTC);
  const actualizados = [];

  for (const mov of movimientos) {
    // eslint-disable-next-line no-await-in-loop
    const updated = await setErpIds(mov._id, [{
      erpId,
      origen: 'netpay-matching',
      saldoPagadoTotal: mov.deposito,
      saldoPagado: mov.deposito,
      total: mov.deposito,
    }], user, { session });
    actualizados.push(updated);
  }

  await NetpayMatch.create([{
    terminalID, almacen: almacen ?? null, dia: diaUTC, netoEsperado, estatusMatch: 'matcheada',
    movementIdsConfirmados: movimientos.map(m => m._id),
    confirmadoPor: { userId: user?._id ?? null, nombre: user?.nombre || user?.email || null },
    confirmadoEn: new Date(),
  }], { session });

  return actualizados;
}

// Confirma el match de un grupo (terminalID+día): movementIds son 1 o 2 BankMovement que
// el usuario eligió desde la bandeja — se RE-VALIDAN server-side (elegibilidad + suma
// contra el neto recalculado EN VIVO), nunca se confía en lo que trae el cliente.
async function confirmarMatchNetpay({ terminalID, almacen, dia, movementIds, user }) {
  if (!terminalID || !dia) throw new BadRequestError('Se requieren terminalID y dia.');
  const ids = [...new Set((movementIds ?? []).map(String))];
  if (ids.length < 1 || ids.length > 2) {
    throw new BadRequestError('Se requiere 1 o 2 movementIds.');
  }

  const diaUTC = _diaUTC(dia);
  const yaResuelto = await NetpayMatch.findOne({ terminalID, dia: diaUTC });
  if (yaResuelto) {
    throw new ConflictError(`Este grupo ya no está pendiente (estatusMatch=${yaResuelto.estatusMatch}).`);
  }

  const { netoEsperado } = await _recalcularNetoEnVivo(terminalID, diaUTC);

  const movimientos = await BankMovement.find({ _id: { $in: ids } });
  if (movimientos.length !== ids.length) {
    throw new NotFoundError('Uno o más movimientos bancarios');
  }
  for (const mov of movimientos) {
    if (mov.banco !== 'BBVA') {
      throw new ConflictError(`El movimiento ${mov._id} no es de BBVA.`);
    }
    if ((mov.erpLinks ?? []).length > 0) {
      throw new ConflictError(`El movimiento ${mov._id} ya tiene un ID ERP vinculado — puede que otro usuario ya lo haya usado.`);
    }
  }

  const suma = movimientos.reduce((acc, m) => acc + (m.deposito ?? 0), 0);
  if (!_montosIguales(suma, netoEsperado)) {
    throw new ConflictError(
      `La suma de los movimientos elegidos (${suma}) no coincide con el neto recalculado en vivo (${netoEsperado}) — puede haber llegado una transacción nueva, recargá la bandeja.`,
    );
  }

  let session = null;
  let actualizados;
  try {
    session = await mongoose.connection.startSession();
    session.startTransaction();
    actualizados = await _confirmarConSesion(terminalID, almacen, diaUTC, netoEsperado, movimientos, user, session);
    await session.commitTransaction();
  } catch (err) {
    if (session?.inTransaction?.()) {
      try { await session.abortTransaction(); } catch (_) { /* ignorar */ }
    }
    const sinSoporteTransacciones = err.code === 20
      || /transaction numbers are only allowed/i.test(err.message);
    if (!sinSoporteTransacciones) throw err;
    // Mongo standalone (sin replica set) — mismo fallback que caja-transferencia-confirm.service.js.
    actualizados = await _confirmarConSesion(terminalID, almacen, diaUTC, netoEsperado, movimientos, user, null);
  } finally {
    if (session) {
      try { await session.endSession(); } catch (_) { /* ignorar */ }
    }
  }

  for (const updated of actualizados) {
    emitToBanco(updated.banco, 'bank:movement:updated', updated);
    emitToAll('bank:ficha-pendiente:changed', { movementId: updated._id });
  }

  return { movimientos: actualizados };
}

// Descarte MANUAL de un grupo 'pendiente' SIN candidatos — mismo criterio que
// caja-transferencia-descartar-manual.service.js: re-valida en vivo (recalcula neto +
// busca candidatos) que el grupo sigue sin nada que revisar antes de permitirlo. NUNCA
// toca BankMovement/erpLinks.
async function descartarMatchNetpay({ terminalID, almacen, dia, user }) {
  if (!terminalID || !dia) throw new BadRequestError('Se requieren terminalID y dia.');

  const diaUTC = _diaUTC(dia);
  const yaResuelto = await NetpayMatch.findOne({ terminalID, dia: diaUTC });
  if (yaResuelto) {
    throw new ConflictError(`Este grupo ya no está pendiente (estatusMatch=${yaResuelto.estatusMatch}).`);
  }

  const { netoEsperado } = await _recalcularNetoEnVivo(terminalID, diaUTC);
  const candidatos = await _buscarCandidatosParaGrupo({ terminalID, dia: diaUTC, netoEsperado });
  if (candidatos.length > 0) {
    throw new ConflictError('Este grupo ya tiene candidato(s) para revisar, no se puede descartar manualmente sin revisarlos primero.');
  }

  await NetpayMatch.create({
    terminalID, almacen: almacen ?? null, dia: diaUTC, netoEsperado, estatusMatch: 'descartada-manual',
    descartadoManualmentePor: { userId: user?._id ?? null, nombre: user?.nombre || user?.email || null },
    descartadoManualmenteEn: new Date(),
  });

  return { terminalID, dia: diaUTC, estatusMatch: 'descartada-manual' };
}

module.exports = { confirmarMatchNetpay, descartarMatchNetpay };
