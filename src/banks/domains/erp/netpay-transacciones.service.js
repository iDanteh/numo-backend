'use strict';

// netpay-transacciones.service.js — Fase 1 de la sección Netpay: consulta en vivo,
// sin persistencia. Envuelve buscarTransaccionesNetpay() (kore-caja.service.js, ya
// implementada — pega a GET /transactions/search con CAJA_BASE_URL) agregando SOLO
// totales/trazabilidad (monto, comisión, neto por almacén), sin remapear los campos
// crudos de Kore ni tocar nada de caja-transferencia-*.service.js (dominio distinto,
// no relacionado).
//
// Filtros adicionales (más allá de responseCode/almacenes/dateFrom/dateTo) y cualquier
// matching contra BankMovement quedan para una siguiente iteración — no adelantar
// diseño acá.
//
// CORRECCIÓN 2026-09-08 (hallazgo real del usuario contra Kore): el endpoint pagina
// (default pageSize=20) — agregar totales solo sobre `Data.transactions` de UNA
// respuesta daba totales silenciosamente incompletos (caso real: 20 de 59
// transacciones). Se pide siempre con PAGE_SIZE_MAX (100, el máximo real que acepta
// Kore, confirmado por el usuario) y se recorren todas las páginas (`Data.totalPages`)
// ANTES de calcular ningún total — nunca agregar sobre una sola página.

const { buscarTransaccionesNetpay } = require('./kore-caja.service');

const PAGE_SIZE_MAX = 100;
// Tope defensivo de páginas a recorrer por consulta — evita un loop descontrolado si
// Kore alguna vez devuelve un `totalPages` inconsistente. 50 páginas x 100 = 5000
// transacciones, muy por encima de cualquier volumen real esperado en esta vista.
const MAX_PAGINAS = 50;

async function _traerTodasLasPaginas({ responseCode, almacenes, dateFrom, dateTo }) {
  const transacciones = [];
  let page = 1;
  let totalPages = 1;

  do {
    const { raw } = await buscarTransaccionesNetpay({
      responseCode, almacenes, dateFrom, dateTo, page, pageSize: PAGE_SIZE_MAX,
    });
    const data = raw?.Data ?? {};
    transacciones.push(...(data.transactions ?? []));
    totalPages = data.totalPages || 1;
    page += 1;
  } while (page <= totalPages && page <= MAX_PAGINAS);

  if (page <= totalPages) {
    console.warn(`[consultarTransaccionesNetpay] se alcanzó el tope de ${MAX_PAGINAS} páginas (Kore reporta totalPages=${totalPages}) — totales calculados sobre un subconjunto, acotar filtros`);
  }

  return transacciones;
}

async function consultarTransaccionesNetpay(params = {}) {
  const { responseCode, almacenes, dateFrom, dateTo } = params;
  const transacciones = await _traerTodasLasPaginas({ responseCode, almacenes, dateFrom, dateTo });

  let totalMonto = 0;
  let totalComision = 0;
  const gruposPorAlmacen = new Map();

  for (const t of transacciones) {
    const monto = t.amount ?? 0;
    const comision = t.commission ?? 0;
    totalMonto += monto;
    totalComision += comision;

    const almacen = t.almacen ?? '(sin almacén)';
    const grupo = gruposPorAlmacen.get(almacen) ?? { almacen, totalMonto: 0, totalComision: 0 };
    grupo.totalMonto += monto;
    grupo.totalComision += comision;
    gruposPorAlmacen.set(almacen, grupo);
  }

  const porAlmacen = Array.from(gruposPorAlmacen.values())
    .map(g => ({ ...g, neto: g.totalMonto - g.totalComision }))
    .sort((a, b) => a.almacen.localeCompare(b.almacen));

  return {
    transacciones,
    totales: { monto: totalMonto, comision: totalComision, neto: totalMonto - totalComision },
    porAlmacen,
  };
}

module.exports = { consultarTransaccionesNetpay };
