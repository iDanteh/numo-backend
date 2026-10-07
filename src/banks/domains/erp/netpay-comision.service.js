'use strict';

// netpay-comision.service.js — detecta variación/comisiones nuevas en la comisión base que
// Netpay aplica (pedido explícito del usuario, 2026-10-07). NO es una colección nueva: el dato
// granular (folios[].comisionBasePct, storeId, sucursal, fechaTrx) ya se persiste desde el
// parseo del Excel (ver netpay-reporte-parser.service.js) — esto es una agregación de lectura
// sobre NetpayReporte, decidido así en vez de un snapshot/historial propio porque el volumen es
// bajo (evitar una segunda fuente de verdad que se pueda desincronizar, mismo riesgo que ya se
// vio con koreCache).
//
// Agrupado por storeId+sucursal, NO por terminalID (ver NetpayReporte.model.js, comentario de
// cabecera): la tasa de comisión se negocia por ALMACÉN, no por terminal — ya hay evidencia
// real de sobrecobro de hasta 2.36x en una sucursal con la tasa fija que usa el matching
// automático de Kore. Lee TODOS los NetpayReporte sin filtrar por estatus/eliminado — la
// comisión que Netpay aplicó es un hecho factual, independiente del estado de matching de ese
// depósito.

const NetpayReporte = require('./NetpayReporte.model');

function _redondear(pct) {
  return Math.round(pct * 100) / 100;
}

async function obtenerVariacionComisiones() {
  const filas = await NetpayReporte.aggregate([
    { $unwind: '$folios' },
    // CRITICAL de revisión de confiabilidad (2026-10-07): antes este $match solo excluía
    // comisionBasePct:null — folios SIN storeId/sucursal (campo opcional, Kore no siempre
    // expone cardTypeName/storeId por folio) caían todos bajo una clave fantasma compartida
    // "|" más abajo, mezclando sucursales DISTINTAS que no traen esos campos entre sí. Sin
    // saber de qué almacén es un folio, no se puede detectar variación por almacén — se
    // excluyen acá y se cuentan aparte (ver sinAlmacen más abajo) en vez de agruparlos.
    {
      $match: {
        'folios.comisionBasePct': { $ne: null },
        'folios.storeId': { $ne: null },
        'folios.sucursal': { $ne: null },
      },
    },
    {
      $group: {
        _id:      { storeId: '$folios.storeId', sucursal: '$folios.sucursal', pct: '$folios.comisionBasePct' },
        primera:  { $min: '$folios.fechaTrx' },
        ultima:   { $max: '$folios.fechaTrx' },
        cantidad: { $sum: 1 },
      },
    },
    { $sort: { '_id.storeId': 1, '_id.sucursal': 1, primera: 1 } },
  ]);

  const sinAlmacen = await NetpayReporte.aggregate([
    { $unwind: '$folios' },
    {
      $match: {
        'folios.comisionBasePct': { $ne: null },
        $or: [{ 'folios.storeId': null }, { 'folios.sucursal': null }],
      },
    },
    { $count: 'cantidad' },
  ]);

  // La agregación de arriba ya separó por cada % EXACTO visto — acá se reagrupa por
  // storeId+sucursal (1 fila de salida por almacén) Y se redondea comisionBasePct a 2
  // decimales ANTES de distinguir tasas. SUGGESTION de revisión de confiabilidad (2026-10-07):
  // Mongo agrupó por el float sin redondear, así que el drift de precisión de parseFloat al
  // leer el Excel (ej. 2.36 vs 2.3600000000000003) llegaría acá como 2 grupos separados —
  // reportando una "variación" falsa cuando Netpay nunca cambió nada. Se fusionan sumando
  // cantidad y uniendo el rango de fechas (primera/última) de los grupos que colapsan al
  // mismo valor redondeado.
  const porAlmacen = new Map();
  for (const f of filas) {
    const claveAlmacen = `${f._id.storeId}|${f._id.sucursal}`;
    if (!porAlmacen.has(claveAlmacen)) {
      porAlmacen.set(claveAlmacen, { storeId: f._id.storeId, sucursal: f._id.sucursal, tasasPorValor: new Map() });
    }
    const tasasPorValor = porAlmacen.get(claveAlmacen).tasasPorValor;
    const pctRedondeado = _redondear(f._id.pct);
    if (!tasasPorValor.has(pctRedondeado)) {
      tasasPorValor.set(pctRedondeado, { comisionBasePct: pctRedondeado, primera: f.primera, ultima: f.ultima, cantidad: 0 });
    }
    const tasa = tasasPorValor.get(pctRedondeado);
    tasa.cantidad += f.cantidad;
    if (f.primera < tasa.primera) tasa.primera = f.primera;
    if (f.ultima > tasa.ultima) tasa.ultima = f.ultima;
  }

  return {
    almacenes: [...porAlmacen.values()].map(a => ({
      storeId: a.storeId,
      sucursal: a.sucursal,
      tasas: [...a.tasasPorValor.values()],
      variacion: a.tasasPorValor.size > 1,
    })),
    // Transparencia (no se mezclan con ningún almacén real, ver $match de arriba) — cuántos
    // folios con comisión no se pudieron atribuir a un almacén por faltarles storeId/sucursal.
    foliosSinAlmacenIdentificado: sinAlmacen[0]?.cantidad ?? 0,
  };
}

module.exports = { obtenerVariacionComisiones };
