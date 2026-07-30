'use strict';

/**
 * receiptService.js — Extracción de datos de comprobantes de transferencia
 *
 * Desde 2026-07-28, el motor de extracción pesado (preprocesamiento con sharp,
 * deskew, binarización adaptativa, PaddleOCR/Tesseract, manejo de PDF) vive en
 * ocr-engine.js y corre DENTRO de un pool de worker_threads (ver
 * ocr-worker-pool.js/ocr-worker-entry.js) — no en este hilo. Esto se hizo
 * porque todo el backend (Bancos, ERP-Kore, Solicitudes de Cobro) corre en un
 * solo proceso Node sin cluster/PM2: sin esto, varios usuarios analizando
 * comprobantes a la vez competían por la misma CPU que atiende TODO lo demás.
 *
 * Este archivo ahora solo tiene dos responsabilidades:
 *   1. extractReceiptData() — despacha el trabajo al pool y espera el resultado
 *      (mismo contrato exacto de siempre: mismos parámetros, mismo retorno).
 *   2. findMatchingMovements() — busca/puntúa movimientos bancarios candidatos
 *      contra los datos ya extraídos (I/O-bound sobre Mongo, liviano — nunca
 *      necesitó moverse a un worker).
 */

const BankMovement = require('../banks/BankMovement.model');
const { runInPool } = require('./ocr-worker-pool');

const DATE_WINDOW_DAYS = 30;
const FALLBACK_WINDOW  = 90;

// Punto de entrada público — despacha al pool de workers (ocr-worker-pool.js).
// Mismo contrato exacto que antes de mover el motor a workers: mismos
// parámetros, mismo valor de retorno, mismos tipos de error (statusCode/name
// preservados a través del worker, ver ocr-worker-entry.js).
async function extractReceiptData(imageBuffer, mimeType, label = null) {
  return runInPool(imageBuffer, mimeType, label);
}

// ════════════════════════════════════════════════════════════════════════════
// SCORING Y BÚSQUEDA DE CANDIDATOS
// ════════════════════════════════════════════════════════════════════════════

/**
 * Puntúa un movimiento bancario contra los datos extraídos.
 * Retorna null si el monto difiere más de la tolerancia.
 *
 * Puntuación máxima: 100
 *   monto exacto / ±0.5%    40 pts  (obligatorio)
 *   fecha                    25 pts
 *   clave rastreo / ref      20 pts
 *   banco (origen o destino) 15 pts
 *   cuenta últimos 4 dígitos  5 pts  (—> suma sin superar 100)
 */
const BANCO_ALIASES = {
  'banamex': ['banamex','citibanamex','citi'],
  'bbva':    ['bbva','bancomer','bbva bancomer'],
  'santander':['santander'],
  'banorte': ['banorte','ixe'],
  'hsbc':    ['hsbc'],
  'azteca':  ['azteca','banco azteca'],
  'inbursa': ['inbursa'],
  'scotiabank':['scotiabank','scotiabank mexico'],
  'banbajio':['banbajío','bajío','banbajio'],
  'nu':      ['nu','nubank','nu bank'],
  'spin':    ['spin','spin by oxxo'],
  'hey':     ['hey banco','hey'],
  'albo':    ['albo'],
  'afirme':  ['afirme'],
};

function normalizarBanco(nombre) {
  if (!nombre) return null;
  const n = nombre.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  for (const [canonical, aliases] of Object.entries(BANCO_ALIASES)) {
    if (aliases.some(a => n.includes(a))) return canonical;
  }
  return n.trim();
}

function scoreMovement(mov, ext) {
  const movMonto = mov.deposito || mov.retiro || 0;
  let score = 0;
  const reasons = [];

  // ── Monto (40 pts) — tolerancias escalonadas ──────────────────────────────
  const diff = Math.abs(movMonto - ext.monto);
  const pct  = diff / ext.monto;

  if      (diff < 0.01)  { score += 40; reasons.push('Monto exacto'); }
  else if (diff <= 0.05) { score += 38; reasons.push('Monto ±$0.05 (redondeo banco)'); }
  else if (pct  <= 0.005){ score += 35; reasons.push('Monto ±0.5%'); }
  else if (diff <= 1.0)  { score += 30; reasons.push('Monto ±$1'); }
  else                   { return null; }  // descartado

  // ── Fecha (25 pts) ────────────────────────────────────────────────────────
  // Comparación por día calendario en UTC, NO con .toDateString() (usa la zona
  // horaria LOCAL del servidor) — ext.fecha siempre viene sin hora ("YYYY-MM-DD",
  // así lo pide el prompt de extracción), y new Date("YYYY-MM-DD") se parsea
  // como medianoche UTC. En un servidor en America/Mexico_City (UTC-6),
  // .toDateString() la regresaba un día atrás, restando puntos a comprobantes
  // del MISMO día (verificado: "2026-06-30" → "Mon Jun 29 2026" en local).
  if (ext.fecha) {
    const diaUTC = (d) => {
      const x = new Date(d);
      return Date.UTC(x.getUTCFullYear(), x.getUTCMonth(), x.getUTCDate());
    };
    const days = Math.abs((diaUTC(mov.fecha) - diaUTC(ext.fecha)) / 86_400_000);
    if      (days === 0) { score += 25; reasons.push('Misma fecha'); }
    else if (days <= 1)  { score += 20; reasons.push('±1 día'); }
    else if (days <= 3)  { score += 15; reasons.push('±3 días'); }
    else if (days <= 7)  { score +=  8; reasons.push('±7 días'); }
    else if (days <= 14) { score +=  4; reasons.push('±14 días'); }
  }

  // ── Clave rastreo / referencia (20 pts) ───────────────────────────────────
  const mAuth  = (mov.numeroAutorizacion || '').replace(/\s/g,'').toLowerCase();
  const mRefN  = (mov.referenciaNumerica || '').replace(/\s/g,'').toLowerCase();
  const eClave = (ext.claveRastreo       || '').replace(/\s/g,'').toLowerCase();
  const eRef   = (ext.referencia || ext.numeroAutorizacion || '').replace(/\s/g,'').toLowerCase();

  if (eClave && mAuth && (mAuth === eClave || mAuth.includes(eClave) || eClave.includes(mAuth)))
    { score += 20; reasons.push('Clave rastreo exacta'); }
  else if (eRef && mRefN && (mRefN === eRef || mRefN.includes(eRef) || eRef.includes(mRefN)))
    { score += 15; reasons.push('Referencia numérica'); }
  else if (eClave && mRefN && eClave.length >= 12 && mRefN.includes(eClave.slice(-12)))
    { score +=  8; reasons.push('Clave rastreo parcial'); }

  // ── Banco (15 pts) — comparación por alias normalizado ────────────────────
  if (mov.banco) {
    const movBancoNorm = normalizarBanco(mov.banco);
    const extBancos    = [ext.bancoOrigen, ext.bancoDestino]
      .filter(Boolean).map(normalizarBanco);

    if (extBancos.includes(movBancoNorm)) {
      score += 15; reasons.push(`Banco: ${mov.banco}`);
    }
  }

  // ── Cuenta últimos 4 (5 pts) ──────────────────────────────────────────────
  const last4 = ext.cuentaDestinoUltimos4 || ext.cuentaOrigenUltimos4;
  if (last4 && mov.concepto && mov.concepto.includes(last4)) {
    score += 5; reasons.push(`Cta ****${last4}`);
  }

  // ── Titular del comprobante en el concepto del movimiento (10 pts) ─────────
  // Los movimientos SPEI suelen incluir el nombre del remitente en el concepto,
  // ej: "SPEI DE EDGAR CORTES GONZALEZ". Comparar con titularOrigen/titularDestino.
  const movConceptoNorm = (mov.concepto || '')
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '').toUpperCase();

  const titular = ext.titularOrigen || ext.titularDestino || '';
  if (titular && movConceptoNorm) {
    const titNorm  = titular.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toUpperCase();
    // Filtrar tokens cortos (artículos, preposiciones) para evitar falsos positivos
    const tokens   = titNorm.split(/\s+/).filter(t => t.length > 2);
    if (tokens.length > 0) {
      const matched = tokens.filter(t => movConceptoNorm.includes(t));
      const ratio   = matched.length / tokens.length;
      if      (ratio >= 0.6) { score += 10; reasons.push(`Titular: ${titular.slice(0, 25)}…`); }
      else if (ratio >= 0.3) { score +=  5; reasons.push('Titular parcial'); }
    }
  }

  // ── Concepto extraído vs concepto del movimiento (5 pts) ──────────────────
  // El concepto del comprobante ("pago renta feb", "factura 234") puede coincidir
  // con palabras clave del concepto del banco.
  const extConcepto = (ext.concepto || '');
  if (extConcepto && movConceptoNorm) {
    const extNorm  = extConcepto.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toUpperCase();
    const extTokens = extNorm.split(/\s+/).filter(t => t.length > 3);
    if (extTokens.length > 0) {
      const matched = extTokens.filter(t => movConceptoNorm.includes(t));
      if (matched.length / extTokens.length >= 0.5) {
        score += 5; reasons.push('Concepto coincide');
      }
    }
  }

  // ── CLABE — últimos 8 dígitos como señal de cuenta (5 pts) ────────────────
  // La CLABE completa raramente aparece en el concepto, pero los últimos 8 dígitos
  // (que identifican al beneficiario + dígito de control) sí pueden estar presentes
  // en referenciaNumerica o en el concepto del banco.
  // Nota: los últimos 4 ya están cubiertos por la regla de cuenta arriba;
  //       aquí se buscan los 8 para sumar puntos adicionales sin duplicar.
  if (ext.clabe && ext.clabe.length === 18) {
    const last8   = ext.clabe.slice(-8);
    const haystack = [mov.concepto, mov.referenciaNumerica, mov.numeroAutorizacion]
      .filter(Boolean).join(' ');
    if (haystack.includes(last8) && !(last4 && last8.endsWith(last4) && haystack.includes(last4))) {
      score += 5; reasons.push(`CLABE ****${last8}`);
    }
  }

  return { score: Math.min(score, 100), reasons };
}

// Puntaje MÁXIMO alcanzable para ESTE comprobante específico — solo cuenta las
// categorías donde el OCR sí logró extraer un dato comparable (si el comprobante
// nunca trae "últimos 4 dígitos", esa categoría no cuenta ni a favor ni en contra).
// Es el mismo para todos los movimientos candidatos de una misma solicitud —
// permite expresar el score como un % real (score/maxPosible) en vez de puntos
// crudos sobre un máximo teórico (~125) que casi ningún comprobante alcanza.
function _maxPosibleScore(ext) {
  let max = 40; // monto — siempre aplica (ext.monto ya se garantiza antes de llamar a esto)
  if (ext.fecha) max += 25;
  if (ext.claveRastreo || ext.referencia || ext.numeroAutorizacion) max += 20;
  if (ext.bancoOrigen || ext.bancoDestino) max += 15;
  if (ext.cuentaOrigenUltimos4 || ext.cuentaDestinoUltimos4) max += 5;
  if (ext.titularOrigen || ext.titularDestino) max += 10;
  if (ext.concepto) max += 5;
  if (ext.clabe && ext.clabe.length === 18) max += 5;
  return Math.min(max, 100); // scoreMovement ya topa en 100, mantener consistente
}

/**
 * Busca movimientos bancarios candidatos para el comprobante analizado.
 * Si no hay monto, devuelve los 15 más recientes para selección manual.
 */
async function findMatchingMovements(ext) {
  if (!ext.monto) {
    const recent = await BankMovement.find({
      isActive: true,
      // Decisión explícita del usuario (2026-07-30): esta búsqueda NO filtra por
      // status/rol/regla, salvo 'identificado' — ver comentario completo más abajo,
      // antes del filtro principal.
      status: { $ne: 'identificado' },
      fecha:  { $gte: new Date(Date.now() - FALLBACK_WINDOW * 86_400_000) },
    }).sort({ fecha: -1 }).limit(15).lean();

    return recent.map(mov => ({
      movement: mov,
      score:    0,
      porcentaje: 0,
      reasons:  ['Sin monto extraído — selección manual'],
      nivel:    'bajo',
    }));
  }

  const tol = Math.max(0.50, ext.monto * 0.005);
  const filter = {
    isActive: true,
    // Decisión explícita del usuario (2026-07-30), con caso real: un depósito de $10,000
    // puede cubrir 2 solicitudes de $5,000 cada una — mientras el depósito no esté
    // TOTALMENTE cubierto (status !== 'identificado', ver aplicarLogicaErp: solo pasa a
    // 'identificado' cuando saldoErp cubre el depósito completo), debe seguir apareciendo
    // como candidato para CUALQUIER solicitud, sin importar si otra solicitud ya le
    // enganchó una CxC parcial — por eso se quitó `_sinCxcAjena`/`ownErpIds` (protegía
    // justo el escenario que el usuario confirmó que SÍ debe permitirse). Tampoco filtra
    // por rol/regla de ocultamiento (`oculto`/`ocultoRoles` — nunca se filtraron acá, y a
    // propósito se dejan sin tocar): esas son reglas de VISUALIZACIÓN en Bancos, no deben
    // afectar la conciliación real de una solicitud de cobro. NO agregar de vuelta un
    // filtro de rol, ni `_sinCxcAjena`, ni excluir 'reclasificado'/'otros' acá sin
    // confirmar con el usuario — es intencional, no un descuido.
    status: { $ne: 'identificado' },
    $or: [
      { deposito: { $gte: ext.monto - tol, $lte: ext.monto + tol } },
      { retiro:   { $gte: ext.monto - tol, $lte: ext.monto + tol } },
    ],
  };

  if (ext.fecha) {
    const base = new Date(ext.fecha);
    filter.fecha = {
      $gte: new Date(base.getTime() - DATE_WINDOW_DAYS * 86_400_000),
      $lte: new Date(base.getTime() + DATE_WINDOW_DAYS * 86_400_000),
    };
  } else {
    filter.fecha = { $gte: new Date(Date.now() - FALLBACK_WINDOW * 86_400_000) };
  }

  const candidates = await BankMovement.find(filter)
    .sort({ fecha: -1 }).limit(150).lean();

  const maxPosible = _maxPosibleScore(ext);

  return candidates
    .map(mov => {
      const r = scoreMovement(mov, ext);
      if (!r) return null;
      const porcentaje = Math.round(Math.min(100, (r.score / maxPosible) * 100));
      return { movement: mov, score: r.score, porcentaje, reasons: r.reasons,
               nivel: porcentaje >= 80 ? 'alto' : porcentaje >= 50 ? 'medio' : 'bajo' };
    })
    .filter(Boolean)
    .sort((a, b) => b.score - a.score)
    .slice(0, 10);
}

module.exports = { extractReceiptData, findMatchingMovements };
