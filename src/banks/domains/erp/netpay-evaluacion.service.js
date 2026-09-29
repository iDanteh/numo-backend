'use strict';

// netpay-evaluacion.service.js — netpay-matching-v2 (design.md "Technical Approach"): la
// unidad de decisión es un BUCKET (terminalID, dia, bucket). evaluarRango() reemplaza el
// candidate picker manual de v1 (netpay-match-confirm.service.js#confirmarMatchNetpay,
// removido) — decide automáticamente CADA bucket y persiste SIEMPRE la decisión en
// NetpayMatch, incluida discrepancia (a diferencia de v1, donde "pendiente" nunca tenía
// documento propio: acá auditar POR QUÉ un bucket quedó sin resolver, y habilitar el
// resolve manual — netpay-resolver.service.js — depende de que exista ese documento).
//
// Zero commission-gap tolerance (proposal: comisiones fijas de Kore no reflejan la tasa
// real negociada por sucursal): solo ERP_TOLERANCE ($1 MXN, bank.service.js) separa
// confirmado_automatico de discrepancia para un bucket 'general' — CUALQUIER otro
// resultado (0, 2+, o fuera de tolerancia) va a discrepancia, nunca se aproxima.

const BankMovement = require('../banks/BankMovement.model');
const NetpayMatch = require('./NetpayMatch.model');
const { setErpIds } = require('../banks/bank.service');
const { consultarTransaccionesNetpay } = require('./netpay-transacciones.service');
const {
  _agruparPorTerminalDiaYMarca, _marcasDiferidas, _ventanaDiasNetpay, _montosIguales,
} = require('./netpay-match.service');
const { ConflictError } = require('../../shared/errors/AppError');
const { emitToBanco, emitToAll } = require('../../shared/socket');

const PREFIJO_ERP_ID_AUTOMATICO = 'NETPAY-';

// Estados que una evaluación automática NUNCA debe tocar de nuevo — el reporte manda
// (resuelto_por_reporte), un rechazo o resolve manual son terminales, y un revert nunca se
// vuelve a auto-subir (ver netpay-match-revert.service.js).
const ESTATUS_NO_REEVALUABLES = new Set(['resuelto_por_reporte', 'rechazado', 'resuelto_manual']);
const MOTIVOS_NO_REEVALUABLES = new Set(['revertido', 'reporte_revertido']);

// Usuario sintético para las escrituras automáticas de evaluarRango — no hay un humano
// detrás de la decisión de confirmar un bucket 'general' (POST .../evaluar la dispara, pero
// decide el motor). Mismo patrón EXACTO que
// collection-requests/anticipo-generado.service.js#USUARIO_MOTOR_ANTICIPO: setErpIds()
// exige un `user` real con permiso banks:erp:link/banks:cobro; role:'admin' resuelve el
// permiso vía el wildcard '*' ya sembrado en Postgres, sin replicar esa lógica acá.
const USUARIO_MOTOR_EVALUACION = Object.freeze({
  _id: 'motor-netpay-evaluacion', role: 'admin', nombre: 'Motor de Evaluación Netpay (automático)',
});

// erpId sintético — bucket 'general' mantiene EXACTAMENTE el formato de v1
// (NETPAY-<terminalID>-<YYYY-MM-DD>, ver netpay-match-confirm.service.js#_erpIdSintetico)
// para que terminal-días sin marca diferida se comporten IGUAL que antes (spec.md
// "Non-Regression for Report-Free, Brand-Clean Days"). Un bucket de marca diferida agrega
// un sufijo — nunca colisiona con el general del mismo terminal+día.
function _erpIdAutomatico(terminalID, diaMx, bucket) {
  const fecha = diaMx.toISOString().slice(0, 10);
  return bucket === 'general'
    ? `${PREFIJO_ERP_ID_AUTOMATICO}${terminalID}-${fecha}`
    : `${PREFIJO_ERP_ID_AUTOMATICO}${terminalID}-${fecha}-${bucket}`;
}

// _debeReevaluarse — decisión PURA: si un bucket YA tiene un NetpayMatch existente en un
// estado terminal/cerrado, la evaluación automática nunca debe volver a tocarlo.
function _debeReevaluarse(docExistente) {
  if (!docExistente) return true;
  if (ESTATUS_NO_REEVALUABLES.has(docExistente.estatusMatch)) return false;
  if (docExistente.estatusMatch === 'discrepancia' && MOTIVOS_NO_REEVALUABLES.has(docExistente.motivoDiscrepancia)) {
    return false;
  }
  return true; // pendiente_por_marca, o discrepancia por otro motivo (sin_candidato/multiples_candidatos/candidato_en_conflicto)
}

// _evaluarBucket — decisión PURA de estatus/motivo para UN bucket, dados sus candidatos YA
// filtrados por monto/ventana (la ambigüedad ENTRE buckets de la misma corrida se resuelve
// afuera, ver _resolverAmbiguedadEntreGrupos — acá `conflicto` ya viene resuelto).
function _evaluarBucket(grupo, candidatos, conflicto = false) {
  if (grupo.bucket !== 'general') {
    // Una marca diferida SIEMPRE queda pendiente_por_marca — nunca se busca candidato BBVA
    // para ella (Amex liquida días después, ver design.md "No pool lookup").
    return { estatusMatch: 'pendiente_por_marca', motivoDiscrepancia: null };
  }
  if (conflicto) {
    return { estatusMatch: 'discrepancia', motivoDiscrepancia: 'candidato_en_conflicto' };
  }
  if (candidatos.length === 0) {
    return { estatusMatch: 'discrepancia', motivoDiscrepancia: 'sin_candidato' };
  }
  if (candidatos.length > 1) {
    return { estatusMatch: 'discrepancia', motivoDiscrepancia: 'multiples_candidatos' };
  }
  return { estatusMatch: 'confirmado_automatico', motivoDiscrepancia: null };
}

// _resolverAmbiguedadEntreGrupos — un MISMO BankMovement candidato de más de un bucket
// 'general' en la MISMA corrida (ej. dos terminales cuyo neto esperado coincide por
// casualidad) → candidato_en_conflicto para TODOS los buckets que lo reclaman. Nunca se
// auto-vincula un movimiento ambiguo (design.md "Ambiguity": "one movement claimed by 2+
// buckets or reports → discrepancia").
function _resolverAmbiguedadEntreGrupos(gruposConCandidatos) {
  const conteo = new Map();
  for (const { candidatos } of gruposConCandidatos) {
    if (candidatos.length !== 1) continue;
    const id = String(candidatos[0]._id);
    conteo.set(id, (conteo.get(id) ?? 0) + 1);
  }
  return gruposConCandidatos.map(({ grupo, candidatos }) => {
    const conflicto = candidatos.length === 1 && conteo.get(String(candidatos[0]._id)) > 1;
    return { grupo, candidatos: conflicto ? [] : candidatos, conflicto };
  });
}

function _snapshotDeGrupo(grupo) {
  return {
    terminalID: grupo.terminalID, dia: grupo.dia, montoBruto: grupo.montoBruto,
    comision: grupo.comision, netoEsperado: grupo.netoEsperado, folios: grupo.folios ?? [],
    reporteIdOrigen: null, claveRastreoOrigen: null, montoDepositoReporte: null,
  };
}

// Persiste la decisión — upsert por (terminalID, dia, bucket), el índice único del modelo
// es la última línea de defensa a nivel de datos ante 2 corridas concurrentes.
async function _guardarDecision(grupo, decision, movementIdsConfirmados = []) {
  const set = {
    netoEsperado: grupo.netoEsperado,
    estatusMatch: decision.estatusMatch,
    motivoDiscrepancia: decision.motivoDiscrepancia ?? null,
    snapshot: _snapshotDeGrupo(grupo),
    movementIdsConfirmados,
  };
  if (decision.estatusMatch === 'confirmado_automatico') {
    set.confirmadoPor = { userId: null, nombre: USUARIO_MOTOR_EVALUACION.nombre };
    set.confirmadoEn = new Date();
  }
  return NetpayMatch.findOneAndUpdate(
    { terminalID: grupo.terminalID, dia: grupo.dia, bucket: grupo.bucket },
    { $set: set },
    { upsert: true, new: true },
  );
}

// _confirmarYGuardar — vincula el ÚNICO candidato vía setErpIds (guardSinVinculos: true,
// ver bank.service.js "Concurrent link guard") y persiste confirmado_automatico. Si otro
// proceso reclamó el movimiento entre la lectura del pool y este intento (ConflictError),
// degrada a discrepancia/candidato_en_conflicto en vez de reventar — no hay otro candidato
// posible para reintentar (zero-tolerance ya validó que era el único).
async function _confirmarYGuardar(grupo, mov) {
  const erpId = _erpIdAutomatico(grupo.terminalID, grupo.dia, grupo.bucket);
  let movActualizado;
  try {
    movActualizado = await setErpIds(mov._id, [{
      erpId, origen: 'netpay-matching',
      saldoPagadoTotal: mov.deposito, saldoPagado: mov.deposito, total: mov.deposito,
    }], USUARIO_MOTOR_EVALUACION, { guardSinVinculos: true });
  } catch (err) {
    if (err instanceof ConflictError) {
      return _guardarDecision(grupo, { estatusMatch: 'discrepancia', motivoDiscrepancia: 'candidato_en_conflicto' });
    }
    throw err;
  }

  const guardado = await _guardarDecision(
    grupo, { estatusMatch: 'confirmado_automatico', motivoDiscrepancia: null }, [mov._id],
  );
  emitToBanco(movActualizado.banco, 'bank:movement:updated', movActualizado);
  emitToAll('bank:ficha-pendiente:changed', { movementId: mov._id });
  return guardado;
}

// evaluarRango — orquesta la evaluación automática para un rango/terminal: trae
// transacciones en vivo, agrupa por (terminalID, dia, bucket), salta lo que ya está en un
// estado no-reevaluable, busca candidatos BBVA UNA sola vez para todos los buckets
// pendientes (mismo criterio de optimización que obtenerBandejaNetpay), resuelve
// ambigüedad entre buckets de esta misma corrida, y persiste cada decisión.
async function evaluarRango({ dateFrom, dateTo, terminalID } = {}) {
  const [{ transacciones }, marcasDiferidas, ventanaDias] = await Promise.all([
    consultarTransaccionesNetpay({ dateFrom, dateTo, terminalID }),
    _marcasDiferidas(),
    _ventanaDiasNetpay(),
  ]);

  const grupos = _agruparPorTerminalDiaYMarca(transacciones, marcasDiferidas);
  if (grupos.length === 0) return { evaluados: [] };

  const existentes = await NetpayMatch.find({
    $or: grupos.map(g => ({ terminalID: g.terminalID, dia: g.dia, bucket: g.bucket })),
  }).lean();
  const existentesPorClave = new Map(
    existentes.map(e => [`${e.terminalID}|${new Date(e.dia).toISOString()}|${e.bucket}`, e]),
  );

  const aReevaluar = grupos.filter(g => _debeReevaluarse(
    existentesPorClave.get(`${g.terminalID}|${g.dia.toISOString()}|${g.bucket}`),
  ));
  if (aReevaluar.length === 0) return { evaluados: [] };

  const msVentana = ventanaDias * 24 * 60 * 60 * 1000;
  const diasMs = aReevaluar.map(g => g.dia.getTime());
  const desde = new Date(Math.min(...diasMs) - msVentana);
  const hasta = new Date(Math.max(...diasMs) + msVentana);
  const pool = await BankMovement.find({
    banco: 'BBVA', erpLinks: { $size: 0 }, status: { $ne: 'identificado' },
    fecha: { $gte: desde, $lte: hasta },
  }).lean();

  const gruposConCandidatos = aReevaluar.map(grupo => ({
    grupo,
    candidatos: grupo.bucket === 'general' ? pool.filter(m => _montosIguales(m.deposito, grupo.netoEsperado)) : [],
  }));
  const resueltos = _resolverAmbiguedadEntreGrupos(gruposConCandidatos);

  const evaluados = [];
  for (const { grupo, candidatos, conflicto } of resueltos) {
    const decision = _evaluarBucket(grupo, candidatos, conflicto);
    // eslint-disable-next-line no-await-in-loop
    const guardado = decision.estatusMatch === 'confirmado_automatico'
      ? await _confirmarYGuardar(grupo, candidatos[0])
      : await _guardarDecision(grupo, decision);
    evaluados.push(guardado);
  }

  return { evaluados };
}

module.exports = {
  evaluarRango,
  _debeReevaluarse,
  _evaluarBucket,
  _resolverAmbiguedadEntreGrupos,
  _erpIdAutomatico,
  USUARIO_MOTOR_EVALUACION,
};
