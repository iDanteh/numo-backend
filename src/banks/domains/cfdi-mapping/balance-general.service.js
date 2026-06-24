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
  // COSTO se trata igual que GASTO (naturaleza deudora, afecta Estado de Resultados)
  const grupos = { ACTIVO: [], PASIVO: [], CAPITAL: [], INGRESO: [], GASTO: [], COSTO: [] };
  for (const c of balanza.cuentas) {
    if (c.esAgrupadora) continue; // solo cuentas hoja — las agrupadoras ya suman sus hijos
    const tipo = (c.tipo || '').toUpperCase();
    if (grupos[tipo]) grupos[tipo].push(c);
    // cuentas con tipo desconocido se omiten (no rompen el cálculo)
  }

  // saldo = debe - haber + saldoInicial
  // Deudoras (ACTIVO, GASTO, COSTO): saldo > 0 es normal → usar saldo tal cual
  // Acreedoras (PASIVO, CAPITAL): saldo < 0 es normal → usar Math.abs(saldo)
  // INGRESO: normalmente acreedor (saldo < 0), pero las cuentas contra-ingreso
  //   (Descuentos s/Ventas) tienen saldo deudor (positivo) y deben RESTAR a ingresos.
  //   Se usa el saldo con signo negado: -saldo → ingresos netos correctos.
  const sum = (arr, fn) => Math.round(arr.reduce((s, c) => s + fn(c), 0) * 100) / 100;

  // Para grupos ACREEDORES (PASIVO, CAPITAL, INGRESO): se usa -c.saldo en lugar de Math.abs.
  // Math.abs falla cuando una cuenta acredora tiene saldo POSITIVO (anormal):
  //   Math.abs(+X) = X → suma al pasivo (incorrecto, debería restar)
  //   -c.saldo = -(+X) = -X → resta del pasivo (correcto)
  // Ejemplo: Anticipos de Clientes con saldo deudor (ya aplicados) deben reducir el pasivo.
  const totalActivo   = sum(grupos.ACTIVO,   c =>  c.saldo);
  const totalPasivo   = sum(grupos.PASIVO,   c => -c.saldo);
  const totalCapital  = sum(grupos.CAPITAL,  c => -c.saldo);
  const totalIngresos = sum(grupos.INGRESO,  c => -c.saldo);
  const totalGastos   = sum([...grupos.GASTO, ...grupos.COSTO], c => c.saldo);
  const utilidad      = Math.round((totalIngresos - totalGastos) * 100) / 100;

  const totalPasivoCapital = Math.round((totalPasivo + totalCapital + utilidad) * 100) / 100;
  const cuadra = Math.abs(totalActivo - totalPasivoCapital) < 0.10;

  return {
    activo:  { cuentas: grupos.ACTIVO,  total: totalActivo  },
    pasivo:  { cuentas: grupos.PASIVO,  total: totalPasivo  },
    capital: { cuentas: grupos.CAPITAL, total: totalCapital },
    resultados: {
      ingresos: { cuentas: grupos.INGRESO,                          total: totalIngresos },
      gastos:   { cuentas: [...grupos.GASTO, ...grupos.COSTO],      total: totalGastos   },
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
