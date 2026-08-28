'use strict';

// collection-request-asignaciones.js — resolución de la asignación
// {formaPago → BankMovement} y el cálculo de reconciliación de montos.
// Funciones puras, sin llamadas a Kore ni a la base de datos (mismo patrón
// que collection-request-erp-links.js) — reciben datos ya consultados y
// devuelven estructuras planas, nunca mutan sus argumentos.

const { BadRequestError } = require('../../shared/errors/AppError');

// resolverAsignaciones — expande el body de identificar() (atajo escalar D3
// o arreglo explícito) a { porMovId, movIdsPorFormaPago }. Clave de asignación
// = formaPagoDocId (el _id del subdocumento Mongoose, D2) — formaPagoId NO es
// único dentro de formasPago[] (dos entradas "transferencia" son legales en
// Modo 1).
//
// Depósitos múltiples para UNA sola forma de pago (2026-08-27, caso real
// confirmado contra Kore: 1 formaPago + 2 comprobantes porque el cliente pagó
// ese único monto con 2 depósitos separados): el MISMO formaPagoDocId puede
// aparecer en 2+ entradas de `asignaciones` — a diferencia de antes, ya NO se
// sobreescribe, se ACUMULAN todos los movIds asignados a esa forma de pago
// (`movIdsPorFormaPago`). `porMovId` sigue siendo Map<movId, formaPago[]> —
// el MISMO objeto formaPago puede terminar en 2+ grupos (uno por cada
// depósito que le toca), identificar() calcula cuánto aporta cada uno.
//
// Guard todo-o-nada (spec: "Todo-o-nada completeness gate"): rechaza ANTES
// de tocar Kore/Mongo si CUALQUIER formaPago del request queda sin AL MENOS
// un bankMovementId asignado — no existe identificación parcial.
function resolverAsignaciones(cr, body) {
  const formasPago = cr.formasPago ?? [];

  // Atajo escalar (D3): un solo bankMovementId para TODAS las formasPago —
  // mantiene el camino feliz (1 forma de pago, o todas al mismo depósito)
  // byte-idéntico al de antes de este cambio.
  let asignaciones;
  if (body?.bankMovementId != null) {
    asignaciones = formasPago.map(f => ({
      formaPagoDocId: String(f._id),
      bankMovementId: String(body.bankMovementId),
    }));
  } else {
    asignaciones = Array.isArray(body?.asignaciones) ? body.asignaciones : [];
  }

  const movIdsPorFormaPagoDocId = new Map(); // formaPagoDocId -> string[] (sin duplicados)
  for (const a of asignaciones) {
    if (a?.formaPagoDocId == null || a?.bankMovementId == null) continue;
    const docId = String(a.formaPagoDocId);
    const movId = String(a.bankMovementId);
    if (!movIdsPorFormaPagoDocId.has(docId)) movIdsPorFormaPagoDocId.set(docId, []);
    const movIds = movIdsPorFormaPagoDocId.get(docId);
    if (!movIds.includes(movId)) movIds.push(movId);
  }

  // formaPagoDocId desconocido ANTES del guard de completitud — es un error
  // de payload distinto de "falta asignar" (ej. id de otra solicitud).
  const idsValidos = new Set(formasPago.map(f => String(f._id)));
  for (const docId of movIdsPorFormaPagoDocId.keys()) {
    if (!idsValidos.has(docId)) {
      throw new BadRequestError(`formaPagoDocId desconocido: ${docId}`);
    }
  }

  const sinAsignar = formasPago.filter(f => !movIdsPorFormaPagoDocId.has(String(f._id)));
  if (sinAsignar.length > 0) {
    const descripciones = sinAsignar.map(f => f.formaPagoDescripcion).join(', ');
    throw new BadRequestError(
      `Faltan ${sinAsignar.length} de ${formasPago.length} formas de pago sin movimiento ` +
      `bancario asignado (${descripciones}). Asigna todas antes de autorizar — no existe ` +
      `identificación parcial.`,
    );
  }

  const porMovId = new Map();
  for (const f of formasPago) {
    const movIds = movIdsPorFormaPagoDocId.get(String(f._id));
    for (const movId of movIds) {
      if (!porMovId.has(movId)) porMovId.set(movId, []);
      porMovId.get(movId).push(f);
    }
  }
  return { porMovId, movIdsPorFormaPago: movIdsPorFormaPagoDocId };
}

function formatearMonto(n) {
  return `$${n.toLocaleString('es-MX', { maximumFractionDigits: 2 })}`;
}

// calcularReconciliacion — comparación advisory (D6): NUNCA lanza, solo
// informa. montoDepositado suma `deposito` sobre movimientos DISTINTOS (un
// mismo movimiento referenciado por 2+ formasPago cuenta una sola vez).
// `mensaje` solo cuando hay faltante real (abono válido, spec); el exceso
// queda silencioso porque un depósito puede legítimamente cubrir otras
// solicitudes además de esta (fuera de alcance de este cambio).
function calcularReconciliacion(cr, movs) {
  const round2 = (n) => Math.round(n * 100) / 100;
  const montoSolicitado = round2(cr?.monto ?? 0);

  const vistos = new Set();
  let montoDepositado = 0;
  for (const m of (movs ?? [])) {
    const id = String(m?._id ?? m);
    if (vistos.has(id)) continue;
    vistos.add(id);
    montoDepositado += m?.deposito ?? 0;
  }
  montoDepositado = round2(montoDepositado);

  const diferencia   = round2(montoSolicitado - montoDepositado);
  const cubreParcial = diferencia > 0.01;
  const mensaje = cubreParcial
    ? `cubre ${formatearMonto(montoDepositado)} de ${formatearMonto(montoSolicitado)} — ` +
      `quedan ${formatearMonto(diferencia)} pendientes`
    : null;

  return { montoSolicitado, montoDepositado, diferencia, cubreParcial, mensaje };
}

// movimientosDe — movimientos bancarios DISTINTOS vinculados al request, en
// el orden en que aparecen en formasPago[] (ya populados). ÚNICO punto de
// fallback al campo raíz deprecado bankMovementId (D1): documentos previos
// al backfill (banks/scripts/backfill-formaspago-bankmovementid.js) no
// tienen formasPago[].bankMovementId, así que se asume que el campo raíz
// aplica a todas las formas — mismo comportamiento que existía antes de
// este cambio.
function movimientosDe(cr) {
  const formasPago = cr.formasPago ?? [];
  const vistos   = new Set();
  const resultado = [];
  const agregar = (mov) => {
    if (mov == null) return;
    const id = String(mov?._id ?? mov);
    if (vistos.has(id)) return;
    vistos.add(id);
    resultado.push(mov);
  };
  for (const f of formasPago) {
    agregar(f.bankMovementId);
    // Depósitos extra de UNA MISMA forma de pago (2026-08-27) — ver
    // CollectionRequest.model.js#depositosAdicionales.
    for (const d of (f.depositosAdicionales ?? [])) agregar(d.bankMovementId);
  }
  if (resultado.length === 0 && cr.bankMovementId != null) {
    return [cr.bankMovementId];
  }
  return resultado;
}

module.exports = { resolverAsignaciones, calcularReconciliacion, movimientosDe };
