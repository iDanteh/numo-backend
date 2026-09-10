'use strict';

// caja-transferencia-descartar-manual.service.js — descarte MANUAL de una transferencia
// 'pendiente' sin candidatos ("Sin candidatos" en la bandeja), pedido explícito del
// usuario 2026-09-10: un contador puede saber, por fuera de este panel, que el depósito
// correspondiente ya fue identificado — este descarte NO vincula nada contra Kore/CxC, es
// puramente "sacar de la bandeja, no hay nada más que revisar acá".
//
// DISTINTO de 'descartada' (caja-transferencia-match.service.js#reclasificarHistoricasDescartadas):
// ese estatus es EXCLUSIVO del job automático que limpia ruido histórico (transferencias
// anteriores a FECHA_CORTE_LOGICA_HISTORICA). Este es un nuevo estatus 'descartada-manual',
// disparado por un humano, con su propia trazabilidad (descartadoManualmentePor/En) — nunca
// se mezclan para no perder de dónde vino cada descarte.
//
// A diferencia de confirmarMatch() (Fase D), este NUNCA toca BankMovement/erpLinks.

const CajaTransferencia = require('./CajaTransferencia.model');
const { buscarCandidatos } = require('./caja-transferencia-match.service');
const { NotFoundError, ConflictError } = require('../../shared/errors/AppError');

// Re-valida SERVER-SIDE (nunca confiar en lo que el navegador ya cargó, mismo criterio que
// confirmarMatch()) que la transferencia sigue sin candidatos ANTES de descartarla — si
// apareció alguno desde que se cargó la bandeja, hay algo real que revisar, no se permite
// descartar a ciegas.
async function descartarManual(transferenciaId, user) {
  const transferencia = await CajaTransferencia.findById(transferenciaId);
  if (!transferencia) throw new NotFoundError('Transferencia de caja');
  if (transferencia.estatusMatch !== 'pendiente') {
    throw new ConflictError(`Esta transferencia ya no está pendiente (estatusMatch=${transferencia.estatusMatch}).`);
  }

  const candidatos = await buscarCandidatos(transferencia);
  if (candidatos.length > 0) {
    throw new ConflictError('Esta transferencia ya tiene candidato(s) para revisar, no se puede descartar manualmente sin revisarlos primero.');
  }

  await CajaTransferencia.updateOne(
    { _id: transferencia._id },
    {
      $set: {
        estatusMatch: 'descartada-manual',
        descartadoManualmentePor: { userId: user?._id ?? null, nombre: user?.nombre || user?.email || null },
        descartadoManualmenteEn: new Date(),
      },
    },
  );

  return {
    transferencia: {
      ...(transferencia.toObject ? transferencia.toObject() : transferencia),
      estatusMatch: 'descartada-manual',
    },
  };
}

module.exports = { descartarManual };
