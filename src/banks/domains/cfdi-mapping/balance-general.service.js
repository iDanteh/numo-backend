'use strict';

const balanzaSvc = require('./balanza-preliminar.service');

/**
 * Genera el Balance General y Estado de Resultados a partir de los CFDIs
 * vigentes del periodo, usando las reglas de mapeo activas.
 * Internamente llama a generarBalanzaPreliminar y agrupa por tipo de cuenta.
 *
 * @returns {Promise<{
 *   activo:      { cuentas, total },
 *   pasivo:      { cuentas, total },
 *   capital:     { cuentas, total },
 *   resultados:  { ingresos: { cuentas, total }, gastos: { cuentas, total }, utilidad },
 *   totales:     { activo, pasivoCapital, cuadra },
 *   meta:        { totalCfdis, sinRegla, periodo, ejercicio, tipos }
 * }>}
 */
async function generarBalanceGeneral({ rfc, ejercicio, periodo }) {
  // Obtener la balanza de comprobación con todos los tipos de CFDI
  const balanza = await balanzaSvc.generarBalanzaPreliminar({
    rfc, ejercicio, periodo, tipoCfdi: null,
  });

  // Clasificar cuentas por tipo
  const grupos = { ACTIVO: [], PASIVO: [], CAPITAL: [], INGRESO: [], GASTO: [] };
  for (const c of balanza.cuentas) {
    const tipo = (c.tipo || '').toUpperCase();
    if (grupos[tipo]) grupos[tipo].push(c);
  }

  // saldo = debe - haber (de la balanza)
  // ACTIVO/GASTO → naturaleza deudora  → saldo > 0 es normal → usar saldo tal cual
  // PASIVO/CAPITAL/INGRESO → naturaleza acreedora → saldo < 0 es normal → usar |saldo|
  const sum     = (arr, fn) => Math.round(arr.reduce((s, c) => s + fn(c), 0) * 100) / 100;

  const totalActivo   = sum(grupos.ACTIVO,   c => c.saldo);
  const totalPasivo   = sum(grupos.PASIVO,   c => Math.abs(c.saldo));
  const totalCapital  = sum(grupos.CAPITAL,  c => Math.abs(c.saldo));
  const totalIngresos = sum(grupos.INGRESO,  c => Math.abs(c.saldo));
  const totalGastos   = sum(grupos.GASTO,    c => c.saldo);
  const utilidad      = Math.round((totalIngresos - totalGastos) * 100) / 100;

  const totalPasivoCapital = Math.round((totalPasivo + totalCapital + utilidad) * 100) / 100;
  const cuadra = Math.abs(totalActivo - totalPasivoCapital) < 0.10;

  return {
    activo:  { cuentas: grupos.ACTIVO,  total: totalActivo  },
    pasivo:  { cuentas: grupos.PASIVO,  total: totalPasivo  },
    capital: { cuentas: grupos.CAPITAL, total: totalCapital },
    resultados: {
      ingresos: { cuentas: grupos.INGRESO, total: totalIngresos },
      gastos:   { cuentas: grupos.GASTO,   total: totalGastos   },
      utilidad,
    },
    totales: {
      activo:        totalActivo,
      pasivoCapital: totalPasivoCapital,
      cuadra,
    },
    meta: balanza.meta,
  };
}

module.exports = { generarBalanceGeneral };
