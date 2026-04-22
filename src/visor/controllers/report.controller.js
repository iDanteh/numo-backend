const ExcelJS = require('exceljs');
const CFDI = require('../models/CFDI');
const Comparison = require('../models/Comparison');
const Discrepancy = require('../models/Discrepancy');
const { asyncHandler } = require('../../shared/middleware/error-handler');

/**
 * MONTO_EFECTIVO_EXPR — expresión MongoDB para obtener el monto real de un CFDI.
 * Para tipo P (Complemento de Pago) el campo `total` es 0 por spec del SAT;
 * el monto real está en complementoPago.totales.montoTotalPagos o en el primer pago.
 */
const MONTO_EFECTIVO_EXPR = {
  $round: [{
    $cond: {
      // Si total > 0, usarlo directamente (ERP tipo P guardan el importe real en total).
      // Solo leer complementoPago cuando total === 0 (caso SAT tipo P por spec del SAT).
      if:   { $and: [{ $eq: ['$tipoDeComprobante', 'P'] }, { $eq: ['$total', 0] }] },
      then: { $ifNull: [
        '$complementoPago.totales.montoTotalPagos',
        { $ifNull: ['$complementoPago.pagos.0.monto', 0] },
      ]},
      else: '$total',
    },
  }, 2],
};

/**
 * GET /api/reports/dashboard
 */
const dashboard = asyncHandler(async (req, res) => {
  const { rfcEmisor, fechaInicio, fechaFin, ejercicio, periodo, tipoDeComprobante } = req.query;

  const dateFilter = {};
  if (fechaInicio) {
    const d = fechaInicio.split('T')[0];
    dateFilter.$gte = new Date(`${d}T06:00:00Z`);
  }
  if (fechaFin) {
    const d   = fechaFin.split('T')[0];
    const fin = new Date(`${d}T06:00:00Z`);
    fin.setUTCDate(fin.getUTCDate() + 1);
    dateFilter.$lt = fin;
  }

  const periodoFilter = {};
  if (ejercicio)         periodoFilter.ejercicio         = parseInt(ejercicio);
  if (periodo)           periodoFilter.periodo           = parseInt(periodo);
  if (tipoDeComprobante) periodoFilter.tipoDeComprobante = tipoDeComprobante;

  // Filtro base para KPIs de conciliación (solo ERP activos, sin cancelados ni deshabilitados)
  // Debe coincidir con los mismos criterios que countERP del aggregate de montos.
  // ERP: se filtra por erpStatus (estado en el origen), no satStatus
  const cfdiFilter = { isActive: true, source: 'ERP', erpStatus: { $nin: ['Cancelado', 'Deshabilitado', 'Cancelacion Pendiente'] }, uuid: { $not: /^SINUUID/ }, ...periodoFilter };
  if (rfcEmisor) cfdiFilter['emisor.rfc'] = rfcEmisor.toUpperCase();
  if (Object.keys(dateFilter).length) cfdiFilter.fecha = dateFilter;

  // Filtro para IVA y tipos: todos los CFDIs activos (ERP + SAT + MANUAL)
  const baseFilter = { isActive: true, ...periodoFilter };
  if (rfcEmisor) baseFilter['emisor.rfc'] = rfcEmisor.toUpperCase();
  if (Object.keys(dateFilter).length) baseFilter.fecha = dateFilter;

  // Filtro para montos: ERP, SAT y MANUAL (MANUAL = XMLs del portal SAT subidos manualmente).
  // Se usa { isActive: { $ne: false } } en vez de { isActive: true } porque
  // aggregate() no aplica Mongoose type-casting y documentos con isActive=null
  // o isActive=1 no serían encontrados con la comparación estricta boolean.
  // MANUAL se agrupa junto con SAT (igual que en comparisonEngine) para que el
  // total SAT del dashboard refleje todos los documentos del lado SAT.
  // Sin filtro SINUUID: registros ERP sin UUID son válidos y tienen montos reales.
  // El filtro SINUUID se mantiene solo en cfdiFilter (conciliación) donde se requiere UUID real.
  const montosFilter = { isActive: { $ne: false }, source: { $in: ['ERP', 'SAT', 'MANUAL'] }, ...periodoFilter };
  if (rfcEmisor) montosFilter['emisor.rfc'] = rfcEmisor.toUpperCase();
  if (Object.keys(dateFilter).length) montosFilter.fecha = dateFilter;

  // Filtro para CFDIs SAT/MANUAL que no tienen contraparte ERP
  const satSoloFilter = { isActive: { $ne: false }, source: { $in: ['SAT', 'MANUAL'] }, lastComparisonStatus: 'not_in_erp', ...periodoFilter };
  if (rfcEmisor) satSoloFilter['emisor.rfc'] = rfcEmisor.toUpperCase();
  if (Object.keys(dateFilter).length) satSoloFilter.fecha = dateFilter;

  // Cancelados ERP: solo los que tienen erpStatus = 'Cancelado'
  const canceladosFilter = { source: 'ERP', erpStatus: 'Cancelado', ...periodoFilter };
  if (rfcEmisor) canceladosFilter['emisor.rfc'] = rfcEmisor.toUpperCase();
  if (Object.keys(dateFilter).length) canceladosFilter.fecha = dateFilter;

  // Vigentes en SAT que también están en ERP
  const vigenteErpSatFilter = { isActive: true, source: 'ERP', satStatus: 'Vigente', uuid: { $not: /^SINUUID/ }, ...periodoFilter };
  if (rfcEmisor) vigenteErpSatFilter['emisor.rfc'] = rfcEmisor.toUpperCase();
  if (Object.keys(dateFilter).length) vigenteErpSatFilter.fecha = dateFilter;

  const [
    totalCFDIs, conciliados, conDiscrepancia, sinConciliar, notInErp, erpCanceladosCount,
    vigenteErpSatCount,
    montosAggregate, cfdisBySatStatus, comparisonStats,
    discrepancyStats, topDiscrepancyTypes, recentDiscrepancies,
    ivaAggregate, ivaByTipoAggregate,
  ] = await Promise.all([
    // Total: solo ERP activos válidos + SAT/MANUAL activos válidos (sin deshabilitados ni SINUUID)
    CFDI.countDocuments({ isActive: { $ne: false }, source: { $in: ['ERP', 'SAT', 'MANUAL'] }, uuid: { $not: /^SINUUID/ }, satStatus: { $nin: ['Deshabilitado'] }, erpStatus: { $nin: ['Deshabilitado'] }, ...periodoFilter, ...(rfcEmisor && { 'emisor.rfc': rfcEmisor.toUpperCase() }), ...(Object.keys(dateFilter).length && { fecha: dateFilter }) }),
    CFDI.countDocuments({ ...cfdiFilter, lastComparisonStatus: 'match' }),
    CFDI.countDocuments({ ...cfdiFilter, lastComparisonStatus: { $in: ['discrepancy', 'warning', 'not_in_sat', 'cancelled'] } }),
    CFDI.countDocuments({ ...cfdiFilter, lastComparisonStatus: { $in: [null, 'error', 'pending'] } }),
    CFDI.countDocuments(satSoloFilter),
    CFDI.countDocuments(canceladosFilter),
    CFDI.aggregate([
      { $match: vigenteErpSatFilter },
      { $group: { _id: null, count: { $sum: 1 }, total: { $sum: '$total' } } },
    ]),

    // Montos Y conteos por source. MANUAL se consolida con SAT (mismo lado en conciliación).
    // CFDIs cancelados (satStatus='Cancelado') se cuentan por separado y NO suman al total activo.
    // CFDIs deshabilitados (isActive=false) ya están excluidos por montosFilter.
    CFDI.aggregate([
      { $match: montosFilter },
      { $addFields: {
        sourceGroup: { $cond: { if: { $in: ['$source', ['SAT', 'MANUAL']] }, then: 'SAT', else: '$source' } },
        // ERP: solo suma Timbrado o Habilitado; SAT/MANUAL: excluir Cancelado/Deshabilitado
        excluir: { $cond: {
          if:   { $eq: ['$source', 'ERP'] },
          then: { $not: [{ $in: ['$erpStatus', ['Timbrado', 'Habilitado']] }] },
          else: { $in: ['$satStatus', ['Cancelado', 'Deshabilitado']] },
        }},
      }},
      { $group: {
        _id:             '$sourceGroup',
        total:           { $sum: { $cond: ['$excluir', 0, MONTO_EFECTIVO_EXPR] } },
        count:           { $sum: { $cond: ['$excluir', 0, 1] } },
        totalCancelados: { $sum: { $cond: ['$excluir', MONTO_EFECTIVO_EXPR, 0] } },
        countCancelados: { $sum: { $cond: ['$excluir', 1, 0] } },
      }},
    ]),

    CFDI.aggregate([
      { $match: cfdiFilter },
      { $group: { _id: '$satStatus', count: { $sum: 1 }, totalAmount: { $sum: '$total' } } },
    ]),
    Comparison.aggregate([
      { $group: { _id: '$status', count: { $sum: 1 } } },
    ]),
    Discrepancy.aggregate([
      { $group: { _id: '$severity', count: { $sum: 1 }, fiscalImpact: { $sum: '$fiscalImpact.amount' } } },
    ]),
    Discrepancy.aggregate([
      { $match: { status: 'open' } },
      { $group: { _id: '$type', count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 5 },
    ]),
    Discrepancy.find({ status: 'open' }).sort({ createdAt: -1 }).limit(10).lean(),

    // IVA por fuente:
    //   ERP      → erpStatus no Cancelado/Deshabilitado, excluye SINUUID
    //   SAT/MANUAL → satStatus Vigente
    //   isActive: { $ne: false } en lugar de true para evitar problemas de type-casting en aggregate
    CFDI.aggregate([
      { $match: { isActive: { $ne: false }, source: { $in: ['ERP', 'SAT', 'MANUAL'] }, ...periodoFilter,
        ...(rfcEmisor && { 'emisor.rfc': rfcEmisor.toUpperCase() }),
        ...(Object.keys(dateFilter).length && { fecha: dateFilter }),
      }},
      { $match: { $or: [
        { source: 'ERP',                      uuid: { $not: /^SINUUID/ }, erpStatus: { $nin: ['Cancelado', 'Deshabilitado', 'Cancelacion Pendiente'] } },
        { source: { $in: ['SAT', 'MANUAL'] }, satStatus: 'Vigente' },
      ]}},
      {
        $group: {
          _id:                '$source',
          ivaTrasladadoTotal: { $sum: { $ifNull: ['$impuestos.totalImpuestosTrasladados', 0] } },
          ivaRetenidoTotal:   { $sum: { $ifNull: ['$impuestos.totalImpuestosRetenidos',   0] } },
        },
      },
    ]),

    // IVA desglosado por tipo de comprobante (modal de detalle), mismos criterios
    CFDI.aggregate([
      { $match: { isActive: { $ne: false }, source: { $in: ['ERP', 'SAT', 'MANUAL'] }, ...periodoFilter,
        ...(rfcEmisor && { 'emisor.rfc': rfcEmisor.toUpperCase() }),
        ...(Object.keys(dateFilter).length && { fecha: dateFilter }),
      }},
      { $match: { $or: [
        { source: 'ERP',                      uuid: { $not: /^SINUUID/ }, erpStatus: { $nin: ['Cancelado', 'Deshabilitado', 'Cancelacion Pendiente'] } },
        { source: { $in: ['SAT', 'MANUAL'] }, satStatus: 'Vigente' },
      ]}},
      {
        $group: {
          _id:                { source: { $cond: { if: { $in: ['$source', ['SAT', 'MANUAL']] }, then: 'SAT', else: '$source' } }, tipo: '$tipoDeComprobante' },
          ivaTrasladadoTotal: { $sum: { $ifNull: ['$impuestos.totalImpuestosTrasladados', 0] } },
          ivaRetenidoTotal:   { $sum: { $ifNull: ['$impuestos.totalImpuestosRetenidos',   0] } },
          count:              { $sum: 1 },
        },
      },
    ]),
  ]);

  const vigenteErpSatRow = vigenteErpSatCount[0] ?? { count: 0, total: 0 };
  const erpRow = montosAggregate.find(m => m._id === 'ERP') ?? { total: 0, count: 0, totalCancelados: 0, countCancelados: 0 };
  const satRow = montosAggregate.find(m => m._id === 'SAT') ?? { total: 0, count: 0, totalCancelados: 0, countCancelados: 0 };
  const totalERP = Math.round((erpRow.total ?? 0) * 100) / 100;  // 2 decimales
  const totalSAT = Math.round((satRow.total ?? 0) * 100) / 100;  // 2 decimales
  const countERP = erpRow.count;           // solo activos
  const countSAT = satRow.count;           // solo activos

  const ivaRowERP = ivaAggregate.find(r => r._id === 'ERP')     ?? { ivaTrasladadoTotal: 0, ivaRetenidoTotal: 0 };
  const ivaRowSAT = ivaAggregate.find(r => r._id === 'SAT')     ?? { ivaTrasladadoTotal: 0, ivaRetenidoTotal: 0 };
  const ivaRowMAN = ivaAggregate.find(r => r._id === 'MANUAL')  ?? { ivaTrasladadoTotal: 0, ivaRetenidoTotal: 0 };

  const buildIva = (row) => ({
    ivaTrasladadoTotal: row.ivaTrasladadoTotal,
    ivaRetenidoTotal:   row.ivaRetenidoTotal,
    ivaNeto:            row.ivaTrasladadoTotal - row.ivaRetenidoTotal,
  });

  // Construir mapa byTipo: { 'I': { erp: {...}, sat: {...} }, 'E': {...}, ... }
  const byTipo = {};
  for (const row of ivaByTipoAggregate) {
    if (!row._id) continue;                        // _id null cuando tipoDeComprobante es null
    const tipo   = row._id.tipo  ?? 'Sin tipo';
    const fuente = row._id.source;
    if (!byTipo[tipo]) byTipo[tipo] = {};
    byTipo[tipo][fuente.toLowerCase()] = {
      ivaTrasladadoTotal: row.ivaTrasladadoTotal,
      ivaRetenidoTotal:   row.ivaRetenidoTotal,
      ivaNeto:            row.ivaTrasladadoTotal - row.ivaRetenidoTotal,
      count:              row.count,
    };
  }

  // SAT consolidado: SAT + MANUAL (igual que en byTipo)
  const ivaRowSATConsolidado = {
    ivaTrasladadoTotal: ivaRowSAT.ivaTrasladadoTotal + ivaRowMAN.ivaTrasladadoTotal,
    ivaRetenidoTotal:   ivaRowSAT.ivaRetenidoTotal   + ivaRowMAN.ivaRetenidoTotal,
  };

  const ivaStats = {
    // diferencia neta ERP − SAT (para backward-compat)
    ivaTrasladadoTotal: ivaRowERP.ivaTrasladadoTotal - ivaRowSATConsolidado.ivaTrasladadoTotal,
    ivaRetenidoTotal:   ivaRowERP.ivaRetenidoTotal   - ivaRowSATConsolidado.ivaRetenidoTotal,
    ivaNeto:            (ivaRowERP.ivaTrasladadoTotal - ivaRowSATConsolidado.ivaTrasladadoTotal) -
                        (ivaRowERP.ivaRetenidoTotal   - ivaRowSATConsolidado.ivaRetenidoTotal),
    // por fuente
    erp: buildIva(ivaRowERP),
    sat: buildIva(ivaRowSATConsolidado),
    // desglose por tipo de comprobante
    byTipo,
  };

  res.json({
    kpis: {
      totalCFDIs, conciliados, conDiscrepancia, sinConciliar, notInErp, erpCanceladosCount,
      vigenteErpSat: { count: vigenteErpSatRow.count, total: vigenteErpSatRow.total },
      totalERP, totalSAT, diferencia: totalERP - totalSAT,
      countERP, countSAT,
      // Cancelados y deshabilitados separados del total principal
      erpCancelados: { total: erpRow.totalCancelados, count: erpRow.countCancelados },
      satCancelados: { total: satRow.totalCancelados, count: satRow.countCancelados },
      cfdisBySatStatus, comparisonStats, discrepancyStats,
      ivaStats,
    },
    topDiscrepancyTypes,
    recentDiscrepancies,
  });
});

/**
 * GET /api/reports/discrepancias-montos
 * Retorna comparaciones con diferencias en montos/impuestos para el modal del dashboard.
 */
const CAMPOS_MONTO = ['total', 'subTotal', 'impuestos.totalImpuestosTrasladados', 'impuestos.totalImpuestosRetenidos', 'complementoPago.montoTotalPagos'];

const discrepanciasMontos = asyncHandler(async (req, res) => {
  const { ejercicio, periodo, tipoDeComprobante, page = 1, limit = 100, campos } = req.query;
  const pg = Math.max(1, parseInt(page));
  const lm = Math.min(1000, Math.max(1, parseInt(limit)));

  // Si se pasa `campos` (csv), filtrar solo esos; si no, todos los de monto
  const camposFiltro = campos
    ? campos.split(',').map(c => c.trim()).filter(c => CAMPOS_MONTO.includes(c))
    : CAMPOS_MONTO;

  const periodoFiltro = {};
  if (ejercicio)         periodoFiltro.ejercicio         = parseInt(ejercicio);
  if (periodo)           periodoFiltro.periodo           = parseInt(periodo);
  if (tipoDeComprobante) periodoFiltro.tipoDeComprobante = tipoDeComprobante;

  // Solo incluir CFDIs ERP que aún tienen discrepancia en su ÚLTIMA comparación.
  // lastComparisonStatus es actualizado cada vez que se corre la comparación,
  // por lo que garantiza que solo aparecen registros actuales, no históricos.
  const erpConDiscrepanciaIds = await CFDI.find({
    source: 'ERP',
    erpStatus: { $nin: ['Cancelado', 'Deshabilitado', 'Cancelacion Pendiente'] },
    lastComparisonStatus: { $in: ['discrepancy', 'warning'] },
    ...periodoFiltro,
  }).select('_id').lean().then(docs => docs.map(d => d._id));

  const filter = {
    'differences.field': { $in: camposFiltro },
    status: { $ne: 'cancelled' },
    erpCfdiId: { $in: erpConDiscrepanciaIds },
    ...periodoFiltro,
  };

  const [comparaciones, total] = await Promise.all([
    Comparison.find(filter)
      .select('uuid status differences criticalCount warningCount tipoDeComprobante ejercicio periodo comparedAt erpCfdiId satCfdiId')
      .populate({ path: 'erpCfdiId', model: 'CFDI', select: 'uuid serie folio fecha total subTotal impuestos tipoDeComprobante emisor receptor erpStatus satStatus moneda' })
      .populate({ path: 'satCfdiId', model: 'CFDI', select: 'uuid serie folio fecha total subTotal impuestos tipoDeComprobante emisor receptor satStatus moneda' })
      .sort({ comparedAt: -1, criticalCount: -1 })
      .skip((pg - 1) * lm)
      .limit(lm)
      .lean(),
    Comparison.countDocuments(filter),
  ]);

  // Deduplicar por UUID — puede haber múltiples Comparison para el mismo CFDI
  // si la comparación se ha corrido varias veces. Se conserva el más reciente
  // (el primero tras ordenar por criticalCount desc, comparedAt desc).
  const seen = new Set();
  const items = comparaciones
    .filter(c => {
      if (seen.has(c.uuid)) return false;
      seen.add(c.uuid);
      return true;
    })
    .map(c => ({
      ...c,
      differences: (c.differences ?? []).filter(d => camposFiltro.includes(d.field)),
    }));

  res.json({ items, total, page: pg, limit: lm, pages: Math.ceil(total / lm) });
});

/**
 * GET /api/reports/debug-discrepancias-montos — temporal, solo para diagnóstico
 */
const debugDiscrepanciasMontos = asyncHandler(async (req, res) => {
  const [porStatus, total, muestra] = await Promise.all([
    Comparison.aggregate([
      { $match: { 'differences.field': { $in: CAMPOS_MONTO } } },
      { $group: { _id: '$status', count: { $sum: 1 } } },
    ]),
    Comparison.countDocuments({ 'differences.field': { $in: CAMPOS_MONTO } }),
    Comparison.find({ 'differences.field': { $in: CAMPOS_MONTO } })
      .select('uuid status differences criticalCount')
      .limit(3).lean()
      .then(docs => docs.map(d => ({
        uuid: d.uuid,
        status: d.status,
        criticalCount: d.criticalCount,
        camposMontoEncontrados: (d.differences ?? []).filter(x => CAMPOS_MONTO.includes(x.field)).map(x => x.field),
      }))),
  ]);
  res.json({ total, porStatus, muestra });
});

/**
 * GET /api/reports/export/excel
 */
const exportExcel = asyncHandler(async (req, res) => {
  const { dateFrom, dateTo, status } = req.query;
  const filter = {};
  if (status) filter.status = status;
  if (dateFrom || dateTo) {
    filter.comparedAt = {};
    if (dateFrom) filter.comparedAt.$gte = new Date(dateFrom);
    if (dateTo)   filter.comparedAt.$lte = new Date(dateTo);
  }

  const comparisons = await Comparison.find(filter, { satRawResponse: 0 })
    .populate('erpCfdiId', 'uuid emisor receptor total fecha tipoDeComprobante')
    .lean();

  const workbook = new ExcelJS.Workbook();
  const sheet = workbook.addWorksheet('Comparaciones CFDI');

  sheet.columns = [
    { header: 'UUID',              key: 'uuid',             width: 40 },
    { header: 'Estado',            key: 'status',           width: 15 },
    { header: 'RFC Emisor',        key: 'rfcEmisor',        width: 15 },
    { header: 'RFC Receptor',      key: 'rfcReceptor',      width: 15 },
    { header: 'Total',             key: 'total',            width: 12 },
    { header: 'Fecha',             key: 'fecha',            width: 15 },
    { header: 'Diferencias',       key: 'totalDifferences', width: 12 },
    { header: 'Críticas',          key: 'criticalCount',    width: 10 },
    { header: 'Fecha Comparación', key: 'comparedAt',       width: 20 },
    { header: 'Resuelta',          key: 'resolved',         width: 10 },
  ];

  sheet.getRow(1).font = { bold: true, color: { argb: 'FFFFFFFF' } };
  sheet.getRow(1).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1F3A5F' } };

  for (const comp of comparisons) {
    const cfdi = comp.erpCfdiId;
    const row = sheet.addRow({
      uuid:             comp.uuid,
      status:           comp.status,
      rfcEmisor:        cfdi?.emisor?.rfc   || '',
      rfcReceptor:      cfdi?.receptor?.rfc || '',
      total:            cfdi?.total         || '',
      fecha:            cfdi?.fecha ? new Date(cfdi.fecha).toLocaleDateString('es-MX') : '',
      totalDifferences: comp.totalDifferences,
      criticalCount:    comp.criticalCount,
      comparedAt:       new Date(comp.comparedAt).toLocaleDateString('es-MX'),
      resolved:         comp.resolved ? 'Sí' : 'No',
    });
    if (comp.status === 'discrepancy')
      row.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFF3CD' } };
    if (comp.status === 'not_in_sat' || comp.status === 'cancelled')
      row.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF8D7DA' } };
  }

  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', `attachment; filename="reporte_cfdis_${Date.now()}.xlsx"`);
  await workbook.xlsx.write(res);
  res.end();
});

/**
 * GET /api/reports/debug-montos  — temporal, solo para diagnóstico
 */
const debugMontos = asyncHandler(async (req, res) => {
  const [bySource, sample] = await Promise.all([
    // Conteo y suma por source, sin ningún filtro extra
    CFDI.aggregate([
      { $group: {
        _id: '$source',
        count: { $sum: 1 },
        totalSum: { $sum: '$total' },
        totalSumConverted: { $sum: { $convert: { input: '$total', to: 'double', onError: 0, onNull: 0 } } },
        activeCount:   { $sum: { $cond: [{ $eq: ['$isActive', true]  }, 1, 0] } },
        inactiveCount: { $sum: { $cond: [{ $eq: ['$isActive', false] }, 1, 0] } },
        nullCount:     { $sum: { $cond: [{ $eq: ['$isActive', null]  }, 1, 0] } },
        totalZeroCount: { $sum: { $cond: [{ $lte: ['$total', 0] }, 1, 0] } },
      }},
    ]),
    // Muestra los primeros 5 documentos no-ERP con sus campos relevantes
    CFDI.find(
      { source: { $ne: 'ERP' } },
      { uuid: 1, source: 1, total: 1, isActive: 1, tipoDeComprobante: 1, fecha: 1 }
    ).limit(5).lean(),
  ]);
  res.json({ bySource, sample });
});

/**
 * GET /api/reports/sat-vigente-erp-inactivo
 * CFDIs vigentes en SAT pero cancelados, deshabilitados o con cancelación pendiente en ERP.
 */
const satVigenteErpInactivo = asyncHandler(async (req, res) => {
  const { ejercicio, periodo, tipoDeComprobante } = req.query;
  const periodoFiltro = {};
  if (ejercicio)         periodoFiltro.ejercicio         = parseInt(ejercicio);
  if (periodo)           periodoFiltro.periodo           = parseInt(periodo);
  if (tipoDeComprobante) periodoFiltro.tipoDeComprobante = tipoDeComprobante;

  const items = await CFDI.find({
    source: 'ERP',
    isActive: { $ne: false },
    satStatus: 'Vigente',
    erpStatus: { $in: ['Cancelado', 'Deshabilitado', 'Cancelacion Pendiente'] },
    uuid: { $not: /^SINUUID/ },
    ...periodoFiltro,
  })
    .select('uuid serie folio fecha total tipoDeComprobante emisor receptor satStatus erpStatus')
    .sort({ total: -1 })
    .limit(500)
    .lean();

  res.json({ items, total: items.length });
});

/**
 * GET /api/reports/discrepancias-criticas
 * Retorna TODAS las comparaciones con criticalCount > 0 para el periodo dado,
 * incluyendo not_in_erp, not_in_sat, discrepancias de monto, RFC, etc.
 */
const discrepanciasCriticas = asyncHandler(async (req, res) => {
  const { ejercicio, periodo, tipoDeComprobante, limit = 500 } = req.query;
  const lm = Math.min(2000, Math.max(1, parseInt(limit)));

  const periodoFiltro = {};
  if (ejercicio)         periodoFiltro.ejercicio         = parseInt(ejercicio);
  if (periodo)           periodoFiltro.periodo           = parseInt(periodo);
  if (tipoDeComprobante) periodoFiltro.tipoDeComprobante = tipoDeComprobante;

  const filter = {
    $or: [
      { criticalCount: { $gt: 0 } },
      { status: { $in: ['not_in_erp', 'not_in_sat', 'cancelled'] } },
    ],
    ...periodoFiltro,
  };

  const [comparaciones, total] = await Promise.all([
    Comparison.find(filter)
      .select('uuid status differences criticalCount warningCount tipoDeComprobante ejercicio periodo comparedAt erpCfdiId satCfdiId')
      .populate({ path: 'erpCfdiId', model: 'CFDI', select: 'uuid serie folio fecha total tipoDeComprobante emisor receptor erpStatus satStatus' })
      .populate({ path: 'satCfdiId', model: 'CFDI', select: 'uuid serie folio fecha total tipoDeComprobante emisor receptor satStatus' })
      .sort({ criticalCount: -1, comparedAt: -1 })
      .limit(lm)
      .lean(),
    Comparison.countDocuments(filter),
  ]);

  // Deduplicar por UUID — conservar la más reciente (primero tras el sort)
  const seen = new Set();
  const items = comparaciones.filter(c => {
    if (seen.has(c.uuid)) return false;
    seen.add(c.uuid);
    return true;
  });

  // Conteo por status para el resumen del dashboard
  const porStatus = items.reduce((acc, c) => {
    acc[c.status] = (acc[c.status] || 0) + 1;
    return acc;
  }, {});

  res.json({ items, total, porStatus });
});

module.exports = { dashboard, exportExcel, debugMontos, discrepanciasMontos, debugDiscrepanciasMontos, satVigenteErpInactivo, discrepanciasCriticas };
