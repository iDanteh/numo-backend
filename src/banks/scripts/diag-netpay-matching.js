'use strict';

/**
 * Diagnóstico de solo lectura para probar la hipótesis de matching Netpay↔BBVA
 * que describieron los contadores (ver project_netpay_transacciones.md):
 *   1. Cada sucursal tiene sus propias terminales (ya viene reflejado en el campo
 *      `almacen` de cada transacción Netpay — no hace falta catálogo aparte).
 *   2. Cada operación trae la comisión incluida, pero Kore no calcula el neto por
 *      venta — neto = amount - commission, por transacción.
 *   3. El depósito a BBVA es GENERAL por terminal (probablemente diario, un solo
 *      depósito que agrupa TODAS las ventas del día de esa terminal) — a diferencia
 *      del matching 1:1 ya usado en Transferencias entre cajas
 *      (caja-transferencia-match.service.js), acá habría que agrupar N transacciones
 *      contra 1 solo BankMovement.
 *
 * Qué hace: agrupa las transacciones Netpay por (almacen, día de transactionDate),
 * suma neto esperado del grupo, y busca en BankMovement (banco=BBVA, ventana de
 * fecha configurable) depósitos cuyo monto se acerque a ese neto — sin importar
 * tolerancia estricta, reporta la diferencia real para poder juzgar a ojo si el
 * patrón se sostiene o no. NO escribe nada, NO marca nada como identificado.
 *
 * Uso:
 *   node src/banks/scripts/diag-netpay-matching.js <dateFrom> <dateTo> [terminalID] [ventanaDias]
 *   node src/banks/scripts/diag-netpay-matching.js 2026-09-01T00:00:00Z 2026-09-15T23:59:59Z
 *   node src/banks/scripts/diag-netpay-matching.js 2026-09-01T00:00:00Z 2026-09-15T23:59:59Z 2840403056 3
 */

require('dotenv').config();

const mongoose = require('mongoose');
const BankMovement = require('../domains/banks/BankMovement.model');
const { consultarTransaccionesNetpay } = require('../domains/erp/netpay-transacciones.service');

const [, , dateFrom, dateTo, terminalID, ventanaDiasArg] = process.argv;
const VENTANA_DIAS = ventanaDiasArg ? parseInt(ventanaDiasArg, 10) : 2;

function fmt(n) {
  return (n ?? 0).toLocaleString('es-MX', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function soloFecha(iso) {
  return String(iso ?? '').slice(0, 10); // YYYY-MM-DD, sin asumir huso horario
}

async function run() {
  if (!dateFrom || !dateTo) {
    console.error('Uso: node diag-netpay-matching.js <dateFrom ISO> <dateTo ISO> [terminalID] [ventanaDias]');
    process.exit(1);
  }

  await mongoose.connect(process.env.MONGODB_URI);
  console.log(`Conectado a MongoDB. Ventana de búsqueda BBVA: ±${VENTANA_DIAS} día(s).`);

  console.log(`\nConsultando Netpay: ${dateFrom} .. ${dateTo}${terminalID ? ` terminalID=${terminalID}` : ' (todas las terminales)'}`);
  const { transacciones } = await consultarTransaccionesNetpay({ dateFrom, dateTo, terminalID });
  console.log(`Transacciones traídas: ${transacciones.length}`);

  // Agrupa por (almacen, día) — la unidad que se espera que corresponda a UN depósito.
  const grupos = new Map();
  for (const t of transacciones) {
    const dia = soloFecha(t.transactionDate);
    const clave = `${t.almacen ?? '(sin almacén)'} | terminal=${t.terminalID ?? '(sin terminal)'} | ${dia}`;
    const g = grupos.get(clave) ?? {
      almacen: t.almacen, terminalID: t.terminalID, dia, count: 0, monto: 0, comision: 0,
    };
    g.count += 1;
    g.monto += t.amount ?? 0;
    g.comision += t.commission ?? 0;
    grupos.set(clave, g);
  }

  console.log(`\nGrupos (almacen+terminal+día): ${grupos.size}`);

  // Trae de una sola vez TODOS los BankMovement de BBVA en el rango ampliado —
  // mismo criterio de "una sola consulta, filtrar en memoria" que buscarCandidatosBatch.
  const desde = new Date(dateFrom); desde.setDate(desde.getDate() - VENTANA_DIAS);
  const hasta = new Date(dateTo);   hasta.setDate(hasta.getDate() + VENTANA_DIAS);
  const movimientosBBVA = await BankMovement.find({
    banco: 'BBVA', fecha: { $gte: desde, $lte: hasta },
  }).select('_id folio fecha deposito status erpIds').lean();
  console.log(`Movimientos BBVA en el rango ampliado: ${movimientosBBVA.length}`);

  for (const g of [...grupos.values()].sort((a, b) => a.dia.localeCompare(b.dia))) {
    const neto = g.monto - g.comision;
    console.log(`\n=== ${g.dia} | almacen=${g.almacen ?? '?'} | terminal=${g.terminalID ?? '?'} | ${g.count} transacción(es) ===`);
    console.log(`  monto bruto=${fmt(g.monto)}  comisión=${fmt(g.comision)}  neto esperado=${fmt(neto)}`);

    const diaBase = new Date(`${g.dia}T00:00:00Z`);
    const desdeG = new Date(diaBase); desdeG.setDate(desdeG.getDate() - VENTANA_DIAS);
    const hastaG = new Date(diaBase); hastaG.setDate(hastaG.getDate() + VENTANA_DIAS + 1);

    const candidatos = movimientosBBVA
      .filter(m => m.fecha >= desdeG && m.fecha <= hastaG)
      .map(m => ({ ...m, diferencia: (m.deposito ?? 0) - neto }))
      .sort((a, b) => Math.abs(a.diferencia) - Math.abs(b.diferencia));

    if (candidatos.length === 0) {
      console.log('  Sin movimientos BBVA en la ventana — nada que comparar.');
      continue;
    }
    for (const c of candidatos.slice(0, 5)) {
      const marca = Math.abs(c.diferencia) <= 1 ? '*** MATCH EXACTO ***' : '';
      console.log(`  · folio=${c.folio} fecha=${c.fecha.toISOString().slice(0, 10)} deposito=${fmt(c.deposito)} status=${c.status} erpIds=${(c.erpIds ?? []).length} diferencia=${fmt(c.diferencia)} ${marca}`);
    }
  }

  await mongoose.connection.close();
}

run().catch(err => { console.error(err); process.exit(1); });
