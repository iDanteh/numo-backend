'use strict';

// netpay-migracion.service.js — netpay-matching-v2 (design.md "Migration Classification
// Rule"): clasificador PURO (sin I/O) que decide, para cada registro legacy de
// NetpayMatch/NetpayReporte, su nuevo estado v2. Reusado por
// scripts/migrate-netpay-v2.js (Phase 4, --dry-run/--apply/--revert) — este archivo no
// abre ninguna conexión ni corre queries, solo decide.
//
// El discriminador es la FUENTE del monto esperado (design.md): puede leerse de la propia
// colección MÁS el erpLink en el BankMovement asociado. Hay 3 fuentes posibles:
// - Live Kore neto — solo confirmarMatchNetpay lo escribía (prefijo NETPAY-).
// - Report montoDepositoTotal — solo confirmarReporte lo escribía (prefijo NETPAYRPT-).
// - Nada — un legacy 'pendiente' nunca tuvo link.
//
// Cada doc migrado recibe estatusLegacy (el valor original) y bucket:'general' — ningún
// registro legacy conocía el concepto de bucket, así que todos migran al bucket por
// defecto (ver NetpayMatch.model.js#bucket).

const PREFIJO_ERP_ID_AUTOMATICO = 'NETPAY-';

function _tieneErpLinkConPrefijo(mov, prefijo) {
  return (mov?.erpLinks ?? []).some(l => String(l?.erpId || '').startsWith(prefijo));
}

// clasificarNetpayMatch — legacy NetpayMatch.estatusMatch ('matcheada' |
// 'descartada-manual') -> nuevo estado v2. `movimientos` es un Map<String(movementId),
// BankMovement lean> — ya cargado por el caller (scripts/migrate-netpay-v2.js), esta
// función nunca hace I/O.
function clasificarNetpayMatch(doc, movimientos) {
  const legacy = doc.estatusMatch;

  if (legacy === 'matcheada') {
    const ids = doc.movementIdsConfirmados ?? [];
    const todosConLink = ids.length > 0
      && ids.every(id => _tieneErpLinkConPrefijo(movimientos.get(String(id)), PREFIJO_ERP_ID_AUTOMATICO));
    return todosConLink
      ? { estatusMatch: 'confirmado_automatico', motivoDiscrepancia: null, estatusLegacy: legacy, bucket: 'general' }
      : { estatusMatch: 'discrepancia', motivoDiscrepancia: 'vinculo_huerfano', estatusLegacy: legacy, bucket: 'general' };
  }

  if (legacy === 'descartada-manual') {
    return { estatusMatch: 'rechazado', motivoDiscrepancia: null, estatusLegacy: legacy, bucket: 'general' };
  }

  throw new Error(`netpay-migracion: estado NetpayMatch.estatusMatch no reconocido: ${legacy}`);
}

// clasificarNetpayReporte — legacy NetpayReporte.estatus ('confirmado' | 'pendiente' |
// 'descartado') -> nuevo estado v2. `movimiento` es el BankMovement lean asociado a
// doc.movementIdConfirmado (o null si no aplica/no existe) — ya cargado por el caller.
function clasificarNetpayReporte(doc, movimiento) {
  const legacy = doc.estatus;

  if (legacy === 'confirmado') {
    const erpIdEsperado = `NETPAYRPT-${doc.claveRastreo}`;
    const tieneLinkEsperado = (movimiento?.erpLinks ?? []).some(l => l.erpId === erpIdEsperado);
    return tieneLinkEsperado
      ? { estatus: 'resuelto_por_reporte', vinculo: 'erp-link', motivoDiscrepancia: null, estatusLegacy: legacy, bucket: 'general' }
      : { estatus: 'discrepancia', vinculo: null, motivoDiscrepancia: 'vinculo_huerfano', estatusLegacy: legacy, bucket: 'general' };
  }

  // Ratificado en design.md "Decisions Resolved": "a report always represents a complete
  // deposit, so a legacy pendiente report MUST NOT become pendiente_por_marca" — SIEMPRE
  // discrepancia/sin_candidato, nunca pendiente_por_marca.
  if (legacy === 'pendiente') {
    return { estatus: 'discrepancia', vinculo: null, motivoDiscrepancia: 'sin_candidato', estatusLegacy: legacy, bucket: 'general' };
  }

  if (legacy === 'descartado') {
    return { estatus: 'rechazado', vinculo: null, motivoDiscrepancia: null, estatusLegacy: legacy, bucket: 'general' };
  }

  throw new Error(`netpay-migracion: estado NetpayReporte.estatus no reconocido: ${legacy}`);
}

module.exports = { clasificarNetpayMatch, clasificarNetpayReporte };
