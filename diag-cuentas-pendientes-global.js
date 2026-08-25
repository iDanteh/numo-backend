'use strict';
require('dotenv').config();
const { sincronizarCuentasPendientes } = require('./src/banks/domains/erp/erp-sync.service');

const FECHA = process.env.DIAG_FECHA || '2026-08-11';
const SERIE = process.env.DIAG_SERIE || 'B0';
const FOLIO_GLOBAL = process.env.DIAG_FOLIO_GLOBAL || '260801256';

// Busca cuentas PENDIENTES (con saldoActual > 0) relacionadas a la fecha de
// la Global -- esto es un endpoint DISTINTO al de cobros (no depende de
// Kore), lista cuentas con su total y su saldo actual real. Si algun ticket
// de los 217+ de la Global quedo con saldo pendiente, deberia aparecer aqui.
async function main() {
  const fechaDesdeIso = new Date(`${FECHA}T00:00:00-06:00`).toISOString();
  const fechaHastaIso = new Date(`${FECHA}T23:59:59.999-06:00`).toISOString();

  console.log('Consultando /cuentas-pendientes sin filtro de estadoCobro (todas)...');
  const { raw } = await sincronizarCuentasPendientes({
    fechaDesde: fechaDesdeIso, fechaHasta: fechaHastaIso,
  });
  console.log('Total cuentas devueltas:', raw.length);

  // Mostrar TODAS las que tengan saldoActual > 0 (pendientes reales)
  const pendientes = raw.filter(c => Number(c.saldoActual) > 0.01);
  console.log('\nCuentas con saldoActual > 0 (pendientes reales):', pendientes.length);
  for (const c of pendientes) {
    console.log(JSON.stringify({
      serie: c.serie, folio: c.folio, serieExterna: c.serieExterna, folioExterno: c.folioExterno,
      folioFiscal: c.folioFiscal, total: c.total, saldoActual: c.saldoActual,
      nombrePersona: c.nombrePersona, origen: c.origen, esAnticipo: c.esAnticipo,
      nombreTipoMovimiento: c.nombreTipoMovimiento, tipoPago: c.tipoPago,
    }));
  }

  const sumaSaldoPendiente = pendientes.reduce((s, c) => s + (Number(c.saldoActual) || 0), 0);
  console.log('\nSuma total de saldoActual pendiente:', sumaSaldoPendiente.toFixed(2));

  // Buscar especificamente algo ligado a la Global (por serieExterna/folioExterno o folioFiscal)
  const ligadasGlobal = raw.filter(c =>
    (c.serieExterna === SERIE && String(c.folioExterno) === FOLIO_GLOBAL) ||
    String(c.folioFiscal || '').includes(FOLIO_GLOBAL),
  );
  console.log(`\nCuentas ligadas directamente a ${SERIE}-${FOLIO_GLOBAL}:`, ligadasGlobal.length);
  for (const c of ligadasGlobal) console.log(JSON.stringify(c));

  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
