'use strict';

// bank.service.isBBVAPseudoAuth.test.js — regresión del bug real 2026-09-11:
// bank.parser.js#parseBBVA extrae "COMP" (primer token tras "/", no numérico) como
// numeroAutorizacion para CUALQUIER concepto "COMP SPEI / COMP SPEI <ref>" o
// "COMPENSACION POR RETRASO / COMP SPEI" — token que NO estaba en la lista de
// palabras clave de BBVA_PSEUDO_AUTH_RE. Eso hacía que la Capa A del dedup
// intra-lote (banco+auth+monto) tratara como "duplicado" a compensaciones BBVA
// DISTINTAS del mismo día que compartían monto exacto, aunque tuvieran saldo y
// referencia SPEI distintos — confirmado con datos reales de
// "BANCOMER JUL-AGOS 2026.xlsx" (31-jul-2026): 5 movimientos genuinos descartados.
//
// Fix: isBBVAPseudoAuth() ahora agrega una validación positiva — cualquier token
// BBVA sin NINGÚN dígito se trata como pseudo-auth (además de la lista explícita
// ya existente), sin tocar bank.parser.js. Este archivo no mockea BankMovement:
// prueba (1) la función real isBBVAPseudoAuth exportada de bank.service.js, y
// (2) una réplica fiel del algoritmo de Capa A/B de bank.service.js (líneas
// ~1280-1321 al momento de este fix) alimentada con datos reales, usando la
// función REAL (ya corregida) — no una reimplementación del fix en sí.
jest.mock('./BankMovement.model');
jest.mock('../../shared/socket', () => ({ emitToUser: jest.fn(), emitToBanco: jest.fn(), emitToAll: jest.fn() }));

const { isBBVAPseudoAuth } = require('./bank.service');

describe('isBBVAPseudoAuth', () => {
  test('no aplica a otros bancos aunque el auth sea una palabra genérica', () => {
    expect(isBBVAPseudoAuth('Banamex', 'COMP')).toBe(false);
    expect(isBBVAPseudoAuth('Santander', 'BNET')).toBe(false);
  });

  test('null/undefined/"" nunca es pseudo-auth', () => {
    expect(isBBVAPseudoAuth('BBVA', null)).toBe(false);
    expect(isBBVAPseudoAuth('BBVA', undefined)).toBe(false);
    expect(isBBVAPseudoAuth('BBVA', '')).toBe(false);
  });

  test('regresión: casos ya cubiertos por la lista explícita siguen funcionando', () => {
    expect(isBBVAPseudoAuth('BBVA', 'BNET')).toBe(true);
    expect(isBBVAPseudoAuth('BBVA', 'REFBNTC')).toBe(true);
    expect(isBBVAPseudoAuth('BBVA', 'COMPENSACION')).toBe(true);
    expect(isBBVAPseudoAuth('BBVA', '******1014')).toBe(true); // cuenta enmascarada, SÍ tiene dígitos
  });

  test('FIX: "COMP" (token real extraído de "COMP SPEI / COMP SPEI ...") ahora es pseudo-auth', () => {
    expect(isBBVAPseudoAuth('BBVA', 'COMP')).toBe(true);
    expect(isBBVAPseudoAuth('BBVA', 'comp')).toBe(true); // case-insensitive, mismo criterio que el resto
  });

  test('un numeroAutorizacion real (con al menos un dígito) NUNCA es pseudo-auth, aunque sea alfanumérico', () => {
    expect(isBBVAPseudoAuth('BBVA', '1234567')).toBe(false);
    // Referencia SPEI real observada en BANCOMER JUL-AGOS 2026.xlsx (31-jul-2026)
    expect(isBBVAPseudoAuth('BBVA', '8846APR2202607295588657851')).toBe(false);
    expect(isBBVAPseudoAuth('BBVA', 'HSB5063523')).toBe(false);
  });
});

// ── Réplica del algoritmo real de bank.service.js (Capa A/B intra-lote, líneas
// ~1280-1321) — usa la función REAL isBBVAPseudoAuth (ya corregida), solo se
// reimplementa la iteración porque esa lógica vive inline dentro de importFile()
// y no está extraída a una función propia (fuera de alcance de este fix).
function toCents(v) { return v != null ? Math.round(v * 100) : ''; }

function simularDedupIntraLote(movimientos) {
  const intraAuthSeen = new Map();
  const intraSaldoSeen = new Map();
  const intraDupHashes = new Set();

  for (const m of movimientos) {
    const auth = m.numeroAutorizacion;
    let isDup = false;

    if (auth && auth !== '0' && !isBBVAPseudoAuth(m.banco, auth)) {
      const normAuth = /^\d+$/.test(auth) ? String(parseInt(auth, 10)) : auth;
      const k = `${m.banco}|${normAuth}|${toCents(m.deposito)}|${toCents(m.retiro)}`;
      if (intraAuthSeen.has(k)) isDup = true;
      else intraAuthSeen.set(k, true);
    }

    if (!isDup && m.saldo != null && (m.deposito != null || m.retiro != null)) {
      const k = `${m.banco}|${toCents(m.deposito)}|${toCents(m.retiro)}|${toCents(m.saldo)}`;
      if (intraSaldoSeen.has(k)) isDup = true;
      else intraSaldoSeen.set(k, true);
    }

    if (isDup) intraDupHashes.add(m.hash);
  }

  return movimientos.filter(m => !intraDupHashes.has(m.hash));
}

describe('Capa A/B intra-lote — regresión con datos reales BANCOMER JUL-AGOS 2026.xlsx (31-jul-2026)', () => {
  test('3 compensaciones BBVA distintas con monto=$0.02 y saldo distinto YA NO colisionan (antes: 2 de 3 se perdían)', () => {
    const movs = [
      { hash: 'h1', banco: 'BBVA', deposito: 0.02, retiro: null, saldo: 311026.88, numeroAutorizacion: 'COMP', concepto: 'COMP SPEI / COMP SPEI 7875APR2202607315597928966' },
      { hash: 'h2', banco: 'BBVA', deposito: 0.02, retiro: null, saldo: 311026.86, numeroAutorizacion: 'COMP', concepto: 'COMP SPEI / COMP SPEI 260731010485956085I' },
      { hash: 'h3', banco: 'BBVA', deposito: 0.02, retiro: null, saldo: 311026.64, numeroAutorizacion: 'COMP', concepto: 'COMP SPEI / COMP SPEI 132429461' },
    ];
    const sobrevivientes = simularDedupIntraLote(movs);
    expect(sobrevivientes.map(m => m.hash)).toEqual(['h1', 'h2', 'h3']);
  });

  test('2 compensaciones BBVA con monto=$0.01 y saldo distinto YA NO colisionan', () => {
    const movs = [
      { hash: 'h4', banco: 'BBVA', deposito: 0.01, retiro: null, saldo: 311019.31, numeroAutorizacion: 'COMP', concepto: 'COMP SPEI / COMP SPEI 085904418750321260' },
      { hash: 'h5', banco: 'BBVA', deposito: 0.01, retiro: null, saldo: 311019.30, numeroAutorizacion: 'COMP', concepto: 'COMP SPEI / COMP SPEI HSBC586047' },
    ];
    const sobrevivientes = simularDedupIntraLote(movs);
    expect(sobrevivientes.map(m => m.hash)).toEqual(['h4', 'h5']);
  });

  test('"COMPENSACION POR RETRASO / COMP SPEI" (sin sufijo, mismo bug) tampoco colisiona entre movimientos distintos', () => {
    const movs = [
      { hash: 'h6', banco: 'BBVA', deposito: 0.01, retiro: null, saldo: 500000.10, numeroAutorizacion: 'COMP', concepto: 'COMPENSACION POR RETRASO / COMP SPEI' },
      { hash: 'h7', banco: 'BBVA', deposito: 0.01, retiro: null, saldo: 500000.20, numeroAutorizacion: 'COMP', concepto: 'COMPENSACION POR RETRASO / COMP SPEI' },
    ];
    const sobrevivientes = simularDedupIntraLote(movs);
    expect(sobrevivientes.map(m => m.hash)).toEqual(['h6', 'h7']);
  });

  test('control: un duplicado REAL (mismo auth numérico + mismo monto + mismo saldo) SIGUE deduplicándose', () => {
    const movs = [
      { hash: 'd1', banco: 'BBVA', deposito: 1500.00, retiro: null, saldo: 500000.00, numeroAutorizacion: '1234567', concepto: 'SPEI RECIBIDOBANAMEX / 1234567 002 xyz' },
      { hash: 'd2', banco: 'BBVA', deposito: 1500.00, retiro: null, saldo: 500000.00, numeroAutorizacion: '1234567', concepto: 'SPEI RECIBIDOBANAMEX / 1234567 002 xyz (reimport)' },
    ];
    const sobrevivientes = simularDedupIntraLote(movs);
    expect(sobrevivientes).toHaveLength(1);
    expect(sobrevivientes[0].hash).toBe('d1');
  });

  test('control: un duplicado real por monto+saldo (sin auth útil, ej. DEPOSITO EN EFECTIVO) SIGUE deduplicándose', () => {
    const movs = [
      { hash: 'e1', banco: 'BBVA', deposito: 5000.00, retiro: null, saldo: 200000.00, numeroAutorizacion: null, concepto: 'DEPOSITO EN EFECTIVO / 0290109' },
      { hash: 'e2', banco: 'BBVA', deposito: 5000.00, retiro: null, saldo: 200000.00, numeroAutorizacion: null, concepto: 'DEPOSITO EN EFECTIVO / 0290109' },
    ];
    const sobrevivientes = simularDedupIntraLote(movs);
    expect(sobrevivientes).toHaveLength(1);
  });
});
