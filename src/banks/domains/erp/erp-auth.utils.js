'use strict';

// ── Series del ERP que contienen autorizaciones de pago ───────────────────────
// Usado tanto por el motor Match ERP como por el sync de CxC para pre-computar
// el campo _autsNorm indexado en ErpCuentaPendiente.
const SERIES_CON_AUTH = ['CBT', 'ABO', 'CPF', 'CFC'];

// Extrae el PRIMER bloque numérico y elimina ceros iniciales.
// Usar el primer bloque (no concatenar todos) evita falsos positivos cuando el
// banco guarda tokens multi-número como "04711358/7607235" (BBVA).
//   "AUT 04711358"     → "4711358"
//   "REF 0118169248"   → "118169248"
//   "D INT 7607235"    → "7607235"
//   "04711358/7607235" → "4711358"
function normalizarAuth(val) {
  if (val == null || val === '') return null;
  const match = String(val).trim().match(/(\d+)/);
  if (!match) return null;
  const n = parseInt(match[1], 10);
  return isNaN(n) ? null : String(n);
}

// Extrae todos los bloques numéricos normalizados a partir del SEGUNDO bloque.
// Cubre el caso BBVA donde numeroAutorizacion = "04711358/7607235" pero el ERP
// registró "7607235" (segundo bloque) como autorizacion en formasPago.
function normalizarAuthBloques(val) {
  if (val == null || val === '') return [];
  const bloques = String(val).trim().match(/\d+/g);
  if (!bloques || bloques.length < 2) return [];
  const primero = normalizarAuth(val);
  return [...new Set(
    bloques.slice(1).map(b => {
      const n = parseInt(b, 10);
      return isNaN(n) ? null : String(n);
    }).filter(b => b !== null && b !== primero),
  )];
}

// Extrae el conjunto de autorizaciones normalizadas de un array de movimientos ERP.
// Solo procesa movimientos de series SERIES_CON_AUTH; ignorar el resto.
// Se usa durante el sync para pre-computar _autsNorm en ErpCuentaPendiente,
// permitiendo la query inversa: movements → authNormSet → CxC por índice.
function extraerAutsNorm(movimientos) {
  const autsSet = new Set();
  for (const mov of (movimientos || [])) {
    if (!SERIES_CON_AUTH.includes(mov.serie)) continue;
    for (const fp of (mov.formasPago || [])) {
      const norm = normalizarAuth(fp.autorizacion);
      if (norm) autsSet.add(norm);
    }
  }
  return [...autsSet];
}

module.exports = { SERIES_CON_AUTH, normalizarAuth, normalizarAuthBloques, extraerAutsNorm };
