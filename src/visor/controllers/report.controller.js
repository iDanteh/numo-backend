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
        // ERP: solo Timbrado o Habilitado con UUID real; SAT/MANUAL: solo Vigente
        excluir: { $cond: {
          if:   { $eq: ['$source', 'ERP'] },
          then: { $or: [
            { $not: [{ $in: ['$erpStatus', ['Timbrado', 'Habilitado']] }] },
            { $regexMatch: { input: { $ifNull: ['$uuid', ''] }, regex: '^SINUUID', options: 'i' } },
          ] },
          else: { $ne: ['$satStatus', 'Vigente'] },
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
    // Leer de CFDI.lastComparisonStatus (siempre refleja el resultado más reciente).
    // La colección Comparison acumula registros históricos de sesiones batch y no
    // es confiable para contar el estado actual — un CFDI puede tener múltiples
    // registros Comparison de distintas sesiones con estados contradictorios.
    CFDI.aggregate([
      { $match: { isActive: { $ne: false }, uuid: { $not: /^SINUUID/ }, ...(periodoFilter.ejercicio && { ejercicio: periodoFilter.ejercicio }), ...(periodoFilter.periodo && { periodo: periodoFilter.periodo }), ...(periodoFilter.tipoDeComprobante && { tipoDeComprobante: periodoFilter.tipoDeComprobante }) } },
      { $group: { _id: '$lastComparisonStatus', count: { $sum: 1 } } },
    ]),
    Discrepancy.aggregate([
      { $match: { ...(periodoFilter.ejercicio && { ejercicio: periodoFilter.ejercicio }), ...(periodoFilter.periodo && { periodo: periodoFilter.periodo }) } },
      { $group: { _id: '$severity', count: { $sum: 1 }, fiscalImpact: { $sum: '$fiscalImpact.amount' } } },
    ]),
    Discrepancy.aggregate([
      { $match: { status: 'open', ...(periodoFilter.ejercicio && { ejercicio: periodoFilter.ejercicio }), ...(periodoFilter.periodo && { periodo: periodoFilter.periodo }) } },
      { $group: { _id: '$type', count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 5 },
    ]),
    Discrepancy.find({ status: 'open', ...(periodoFilter.ejercicio && { ejercicio: periodoFilter.ejercicio }), ...(periodoFilter.periodo && { periodo: periodoFilter.periodo }) }).sort({ createdAt: -1 }).limit(10).lean(),

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
        { source: 'ERP',                      uuid: { $not: /^SINUUID/ }, erpStatus: { $in: ['Timbrado', 'Habilitado'] } },
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
        { source: 'ERP',                      uuid: { $not: /^SINUUID/ }, erpStatus: { $in: ['Timbrado', 'Habilitado'] } },
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

  // No se propaga tipoDeComprobante al filtro de Comparison porque los registros
  // de Comparison pueden tenerlo null (comparaciones anteriores a ese campo).
  // El tipo ya está implícito via erpCfdiId que solo contiene IDs del tipo seleccionado.
  const comparisonFiltro = {};
  if (ejercicio) comparisonFiltro.ejercicio = parseInt(ejercicio);
  if (periodo)   comparisonFiltro.periodo   = parseInt(periodo);

  const filter = {
    'differences.field': { $in: camposFiltro },
    status: { $ne: 'cancelled' },
    erpCfdiId: { $in: erpConDiscrepanciaIds },
    ...comparisonFiltro,
  };

  const cfdiPeriodoFiltro = {};
  if (ejercicio)         cfdiPeriodoFiltro.ejercicio         = parseInt(ejercicio);
  if (periodo)           cfdiPeriodoFiltro.periodo           = parseInt(periodo);
  if (tipoDeComprobante) cfdiPeriodoFiltro.tipoDeComprobante = tipoDeComprobante;

  const cfdiSelect = 'uuid serie folio fecha total subTotal impuestos tipoDeComprobante emisor receptor erpStatus satStatus moneda';

  // Filtro para CFDIs con RFC & — cubre documentos con y sin campo ejercicio/periodo explícito
  const pendienteFiltro = { source: 'ERP', isActive: { $ne: false }, satStatus: 'Pendiente', erpStatus: { $nin: ['Cancelado', 'Deshabilitado', 'Cancelacion Pendiente'] } };
  if (tipoDeComprobante) pendienteFiltro.tipoDeComprobante = tipoDeComprobante;
  if (ejercicio && periodo) {
    const ej = parseInt(ejercicio), pe = parseInt(periodo);
    const fechaIni = new Date(ej, pe - 1, 1);
    const fechaFin = new Date(ej, pe, 1);
    pendienteFiltro.$or = [
      { ejercicio: ej, periodo: pe },
      { ejercicio: { $exists: false }, fecha: { $gte: fechaIni, $lt: fechaFin } },
      { ejercicio: null,              fecha: { $gte: fechaIni, $lt: fechaFin } },
    ];
  }

  const [comparaciones, total, notInSatCfdis, notInErpCfdis, satCanceladoCfdis, pendientesCfdis] = await Promise.all([
    Comparison.find(filter)
      .select('uuid status differences criticalCount warningCount tipoDeComprobante ejercicio periodo comparedAt erpCfdiId satCfdiId')
      .populate({ path: 'erpCfdiId', model: 'CFDI', select: cfdiSelect })
      .populate({ path: 'satCfdiId', model: 'CFDI', select: cfdiSelect })
      .sort({ comparedAt: -1, criticalCount: -1 })
      .skip((pg - 1) * lm)
      .limit(lm)
      .lean(),
    Comparison.countDocuments(filter),

    // ERP en el periodo que no tienen contraparte en SAT
    CFDI.find({ source: 'ERP', isActive: { $ne: false }, lastComparisonStatus: 'not_in_sat', ...cfdiPeriodoFiltro })
      .select(cfdiSelect).sort({ total: -1 }).limit(500).lean(),

    // SAT/MANUAL en el periodo que no tienen contraparte en ERP
    CFDI.find({ source: { $in: ['SAT', 'MANUAL'] }, isActive: { $ne: false }, lastComparisonStatus: 'not_in_erp', ...cfdiPeriodoFiltro })
      .select(cfdiSelect).sort({ total: -1 }).limit(500).lean(),

    // ERP activo pero SAT cancelado (cruce de estatus fiscal)
    CFDI.find({ source: 'ERP', isActive: { $ne: false }, satStatus: 'Cancelado', erpStatus: { $nin: ['Cancelado', 'Deshabilitado', 'Cancelacion Pendiente'] }, ...cfdiPeriodoFiltro })
      .select(cfdiSelect).sort({ total: -1 }).limit(500).lean(),

    // ERP con RFC con & — verificación SAT pendiente (SOAP no soporta %26)
    CFDI.find(pendienteFiltro).select(cfdiSelect).sort({ total: -1 }).limit(500).lean(),
  ]);

  // Deduplicar por UUID — puede haber múltiples Comparison para el mismo CFDI
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

  res.json({
    items, total, page: pg, limit: lm, pages: Math.ceil(total / lm),
    notInSat:      notInSatCfdis,
    notInErp:      notInErpCfdis,
    satCancelados: satCanceladoCfdis,
    pendientes:    pendientesCfdis,
  });
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

  // matchPeriodo sobre Comparison — solo ejercicio/periodo, NO tipoDeComprobante
  // (tipoDeComprobante puede ser null en Comparisons antiguos, lo filtramos post-lookup)
  const matchPeriodo = {};
  if (ejercicio) matchPeriodo.ejercicio = parseInt(ejercicio);
  if (periodo)   matchPeriodo.periodo   = parseInt(periodo);

  // Filtro CFDI para los casos que se leen directamente de la colección CFDI
  const cfdiErp = { source: 'ERP', isActive: { $ne: false } };
  const cfdiSat = { source: { $in: ['SAT', 'MANUAL'] }, isActive: { $ne: false } };
  if (ejercicio)         { cfdiErp.ejercicio = parseInt(ejercicio); cfdiSat.ejercicio = parseInt(ejercicio); }
  if (periodo)           { cfdiErp.periodo   = parseInt(periodo);   cfdiSat.periodo   = parseInt(periodo);   }
  if (tipoDeComprobante) { cfdiErp.tipoDeComprobante = tipoDeComprobante; cfdiSat.tipoDeComprobante = tipoDeComprobante; }

  const cfdiSel    = 'uuid serie folio fecha total tipoDeComprobante emisor receptor erpStatus satStatus';
  const cfdiSelSat = 'uuid serie folio fecha total tipoDeComprobante emisor receptor satStatus';

  const erpProjection = { uuid: 1, serie: 1, folio: 1, fecha: 1, total: 1, tipoDeComprobante: 1, emisor: 1, receptor: 1, erpStatus: 1, satStatus: 1 };
  const satProjection = { uuid: 1, serie: 1, folio: 1, fecha: 1, total: 1, tipoDeComprobante: 1, emisor: 1, receptor: 1, satStatus: 1 };

  // Pipeline ORIGINAL que funcionaba — filtra Comparison por matchPeriodo
  const pipeline = [
    ...(Object.keys(matchPeriodo).length ? [{ $match: matchPeriodo }] : []),
    { $sort: { comparedAt: -1 } },
    { $group: { _id: '$uuid', doc: { $first: '$$ROOT' } } },
    { $replaceRoot: { newRoot: '$doc' } },
    { $match: { $or: [
      { criticalCount: { $gt: 0 } },
      { status: { $in: ['discrepancy', 'not_in_sat', 'cancelled'] } },
    ]}},
    { $sort:  { criticalCount: -1, comparedAt: -1 } },
    { $lookup: { from: 'cfdis', localField: 'erpCfdiId', foreignField: '_id', as: 'erpCfdiId', pipeline: [{ $project: erpProjection }] } },
    { $unwind: { path: '$erpCfdiId', preserveNullAndEmptyArrays: true } },
    // Filtrar por tipo DESPUÉS del lookup (Comparison.tipoDeComprobante puede ser null)
    ...(tipoDeComprobante ? [{ $match: { 'erpCfdiId.tipoDeComprobante': tipoDeComprobante } }] : []),
    { $lookup: { from: 'cfdis', localField: 'satCfdiId', foreignField: '_id', as: 'satCfdiId', pipeline: [{ $project: satProjection }] } },
    { $unwind: { path: '$satCfdiId', preserveNullAndEmptyArrays: true } },
    { $limit: lm },
  ];

  // Casos adicionales leídos directo de CFDI (no dependen de Comparison.ejercicio/periodo)
  const [compItems, notInErpCfdis, satCanceladoErpActivo, erpNotInSat, erpDeshabilitadosCfdis, erpCanceladosCfdis] = await Promise.all([
    Comparison.aggregate(pipeline),
    CFDI.find({ ...cfdiSat, lastComparisonStatus: 'not_in_erp' }).select(cfdiSelSat).sort({ total: -1 }).limit(lm).lean(),
    CFDI.find({ ...cfdiErp, satStatus: 'Cancelado', erpStatus: { $nin: ['Cancelado', 'Deshabilitado', 'Cancelacion Pendiente'] } }).select(cfdiSel).sort({ total: -1 }).limit(lm).lean(),
    CFDI.find({ ...cfdiErp, erpStatus: { $nin: ['Cancelado', 'Cancelacion Pendiente', 'Deshabilitado'] }, lastComparisonStatus: 'not_in_sat' }).select(cfdiSel).sort({ total: -1 }).limit(lm).lean(),
    CFDI.find({ ...cfdiErp, erpStatus: 'Deshabilitado' }).select(cfdiSel).sort({ tipoDeComprobante: 1, total: -1 }).limit(lm).lean(),
    CFDI.find({ ...cfdiErp, erpStatus: { $in: ['Cancelado', 'Cancelacion Pendiente'] } }).select(cfdiSel).sort({ tipoDeComprobante: 1, total: -1 }).limit(lm).lean(),
  ]);

  // UUIDs ya cubiertos por el pipeline para no duplicar
  const compUuids = new Set(compItems.map(c => (c.uuid || '').toUpperCase()));

  const notInErpItems = notInErpCfdis
    .filter(c => !compUuids.has((c.uuid || '').toUpperCase()))
    .map(c => ({
    uuid: c.uuid, status: 'not_in_erp', tipoDeComprobante: c.tipoDeComprobante,
    criticalCount: 0, differences: [], erpCfdiId: null,
    satCfdiId: { uuid: c.uuid, serie: c.serie, folio: c.folio, fecha: c.fecha, total: c.total, tipoDeComprobante: c.tipoDeComprobante, emisor: c.emisor, receptor: c.receptor, satStatus: c.satStatus },
  }));

  const satCanceladoItems = satCanceladoErpActivo
    .filter(c => !compUuids.has((c.uuid || '').toUpperCase()))
    .map(c => ({
      uuid: c.uuid, status: 'sat_cancelado', tipoDeComprobante: c.tipoDeComprobante,
      criticalCount: 1, differences: [], satCfdiId: null,
      erpCfdiId: { uuid: c.uuid, serie: c.serie, folio: c.folio, fecha: c.fecha, total: c.total, tipoDeComprobante: c.tipoDeComprobante, emisor: c.emisor, receptor: c.receptor, erpStatus: c.erpStatus, satStatus: c.satStatus },
    }));

  const notInSatItems = erpNotInSat
    .filter(c => !compUuids.has((c.uuid || '').toUpperCase()))
    .map(c => ({
      uuid: c.uuid, status: 'not_in_sat', tipoDeComprobante: c.tipoDeComprobante,
      criticalCount: 0, differences: [], satCfdiId: null,
      erpCfdiId: { uuid: c.uuid, serie: c.serie, folio: c.folio, fecha: c.fecha, total: c.total, tipoDeComprobante: c.tipoDeComprobante, emisor: c.emisor, receptor: c.receptor, erpStatus: c.erpStatus, satStatus: c.satStatus },
    }));

  const deshabilitadosItems = erpDeshabilitadosCfdis
    .filter(c => !compUuids.has((c.uuid || '').toUpperCase()))
    .map(c => ({
      uuid: c.uuid, status: 'deshabilitado', tipoDeComprobante: c.tipoDeComprobante,
      criticalCount: 0, differences: [], satCfdiId: null,
      erpCfdiId: { uuid: c.uuid, serie: c.serie, folio: c.folio, fecha: c.fecha, total: c.total, tipoDeComprobante: c.tipoDeComprobante, emisor: c.emisor, receptor: c.receptor, erpStatus: c.erpStatus, satStatus: c.satStatus },
    }));

  const allItems = [...compItems, ...notInErpItems, ...satCanceladoItems, ...notInSatItems];

  // CFDIs ERP cancelados (Cancelado + Cancelacion Pendiente) para el tab "Cancelados" del modal
  const canceladosItems = erpCanceladosCfdis
    .filter(c => !compUuids.has((c.uuid || '').toUpperCase()))
    .map(c => ({
      uuid: c.uuid, status: 'cancelado_erp', tipoDeComprobante: c.tipoDeComprobante,
      criticalCount: 0, differences: [], satCfdiId: null,
      erpCfdiId: { uuid: c.uuid, serie: c.serie, folio: c.folio, fecha: c.fecha, total: c.total, tipoDeComprobante: c.tipoDeComprobante, emisor: c.emisor, receptor: c.receptor, erpStatus: c.erpStatus, satStatus: c.satStatus },
    }));

  // Separar cancelados y deshabilitados del flujo principal de vigentes con discrepancia
  const cancelados      = canceladosItems;
  const items           = allItems.filter(i => i.status !== 'cancelled');
  const deshabilitados  = deshabilitadosItems;

  const porStatus = [...allItems, ...deshabilitados].reduce((acc, c) => {
    acc[c.status] = (acc[c.status] || 0) + 1;
    return acc;
  }, {});

  res.json({ items, cancelados, deshabilitados, total: allItems.length + deshabilitados.length, porStatus });
});

/**
 * GET /api/reports/not-in-erp
 * CFDIs que están en SAT/MANUAL pero NO en ERP para el periodo dado.
 * Consulta directo la colección CFDI por lastComparisonStatus para evitar
 * registros históricos de la colección Comparison.
 */
const notInErp = asyncHandler(async (req, res) => {
  const { ejercicio, periodo, tipoDeComprobante, limit = 500 } = req.query;
  const lm = Math.min(2000, Math.max(1, parseInt(limit)));

  const periodoBase = {};
  if (ejercicio)         periodoBase.ejercicio         = parseInt(ejercicio);
  if (periodo)           periodoBase.periodo           = parseInt(periodo);
  if (tipoDeComprobante) periodoBase.tipoDeComprobante = tipoDeComprobante;

  const selectFields = 'uuid serie folio fecha tipoDeComprobante total moneda emisor receptor satStatus lastComparisonStatus ejercicio periodo source';

  // Paso 1: ERP del periodo seleccionado
  const erpDelPeriodo = await CFDI.find({ isActive: { $ne: false }, source: 'ERP', ...periodoBase }, 'uuid ejercicio periodo').lean();
  const erpUuidsPeriodo = new Set(erpDelPeriodo.map(d => d.uuid?.toUpperCase()).filter(Boolean));

  // Paso 2: SAT/MANUAL del periodo seleccionado (sin filtro de tipo)
  const satFiltroBase = { isActive: { $ne: false }, source: { $in: ['SAT', 'MANUAL'] } };
  if (periodoBase.ejercicio) satFiltroBase.ejercicio = periodoBase.ejercicio;
  if (periodoBase.periodo)   satFiltroBase.periodo   = periodoBase.periodo;

  const satDocs = await CFDI.find(satFiltroBase)
    .select(selectFields)
    .sort({ fecha: -1 })
    .lean();

  // Paso 3a: SAT sin contraparte ERP en este mismo periodo
  const sinContraparteErp = satDocs.filter(d => !erpUuidsPeriodo.has(d.uuid?.toUpperCase()));

  // Paso 3b: duplicados SAT — mismo UUID más de una vez en SAT para el periodo
  const uuidCount = {};
  for (const d of satDocs) {
    const u = d.uuid?.toUpperCase();
    if (u) uuidCount[u] = (uuidCount[u] || 0) + 1;
  }
  const duplicadosSAT = satDocs.filter(d => uuidCount[d.uuid?.toUpperCase()] > 1);

  // Paso 3c: SAT con match pero ERP en otro periodo
  // → SAT está en el periodo seleccionado, tiene UUID en ERP pero ese ERP está en distinto periodo
  let matchOtroPeriodo = [];
  if (sinContraparteErp.length > 0 || (periodoBase.ejercicio || periodoBase.periodo)) {
    // UUIDs SAT de este periodo que NO están en ERP de este periodo
    const uuidsSinPeriodo = sinContraparteErp.map(d => d.uuid?.toUpperCase()).filter(Boolean);
    if (uuidsSinPeriodo.length > 0) {
      // Buscar si esos UUIDs sí existen en ERP pero en OTRO periodo
      const erpOtroPeriodo = await CFDI.find({
        isActive: { $ne: false },
        source: 'ERP',
        uuid: { $in: uuidsSinPeriodo },
      }, 'uuid ejercicio periodo').lean();

      const erpOtroMap = {};
      for (const e of erpOtroPeriodo) erpOtroMap[e.uuid?.toUpperCase()] = e;

      matchOtroPeriodo = sinContraparteErp
        .filter(d => erpOtroMap[d.uuid?.toUpperCase()])
        .map(d => ({
          ...d,
          erpEjercicio: erpOtroMap[d.uuid?.toUpperCase()]?.ejercicio,
          erpPeriodo:   erpOtroMap[d.uuid?.toUpperCase()]?.periodo,
        }));
    }
  }

  // Los que realmente no existen en ERP en ningún periodo
  const matchOtroPeriodoUuids = new Set(matchOtroPeriodo.map(d => d.uuid?.toUpperCase()));
  const realmenterNotInErp = sinContraparteErp.filter(d => !matchOtroPeriodoUuids.has(d.uuid?.toUpperCase()));

  res.json({
    sinContraparteErp: realmenterNotInErp,
    totalSinContraparte: realmenterNotInErp.length,
    duplicadosSAT,
    totalDuplicados: duplicadosSAT.length,
    matchOtroPeriodo,
    totalMatchOtroPeriodo: matchOtroPeriodo.length,
    items: realmenterNotInErp,
    total: realmenterNotInErp.length,
  });
});

/**
 * GET /api/reports/pagos-relacionados
 * Para CFDIs tipo P del periodo, cuenta los documentos relacionados
 * (doctoRelacionado.idDocumento) y cruza cuántos UUID existen en el sistema.
 * Solo tiene sentido cuando se filtra por tipoDeComprobante=P.
 */
const pagosRelacionados = asyncHandler(async (req, res) => {
  const { ejercicio, periodo } = req.query;

  const matchFilter = { tipoDeComprobante: 'P', isActive: { $ne: false } };
  if (ejercicio) matchFilter.ejercicio = parseInt(ejercicio);
  if (periodo)   matchFilter.periodo   = parseInt(periodo);

  const [agg, totalPagos] = await Promise.all([
    CFDI.aggregate([
      { $match: matchFilter },
      { $unwind: { path: '$complementoPago.pagos', preserveNullAndEmptyArrays: false } },
      { $unwind: { path: '$complementoPago.pagos.doctosRelacionados', preserveNullAndEmptyArrays: false } },
      {
        $group: {
          _id:         null,
          cfdiIds:     { $addToSet: '$_id' },
          doctosIds:   { $addToSet: { $toUpper: { $ifNull: ['$complementoPago.pagos.doctosRelacionados.idDocumento', ''] } } },
        },
      },
    ]),
    CFDI.countDocuments(matchFilter),
  ]);

  if (!agg.length || !agg[0].doctosIds?.length) {
    return res.json({ totalPagos, totalDoctos: 0, existenEnSistema: 0, noExistenEnSistema: 0, porcentajeCobertura: 100 });
  }

  const doctosIds = agg[0].doctosIds.filter(id => id && id.length > 0);
  const totalDoctos = doctosIds.length;

  const existenEnSistema = await CFDI.countDocuments({
    uuid: { $in: doctosIds },
    isActive: { $ne: false },
  });

  res.json({
    totalPagos,
    totalDoctos,
    existenEnSistema,
    noExistenEnSistema: totalDoctos - existenEnSistema,
    porcentajeCobertura: totalDoctos > 0 ? Math.round((existenEnSistema / totalDoctos) * 100) : 100,
  });
});

/**
 * GET /api/reports/conciliacion-excel
 * Reporte completo de conciliación CFDI.
 * Genera una hoja de Resumen General + una hoja por cada tipo de comprobante
 * que exista en el periodo (I=Ingreso, E=Egreso, P=Pago…) + una hoja "Solo en SAT".
 *
 * Cada hoja de tipo incluye:
 *   - KPI: Total ERP vs Total SAT vs Diferencia
 *   - Tabla: todos los CFDIs ERP del tipo con sus contrapartes SAT,
 *     IVA, diferencia de monto y detalle campo a campo de por qué difieren.
 */
const conciliacionExcel = asyncHandler(async (req, res) => {
  const { ejercicio, periodo } = req.query;
  const periodoFilter = {};
  if (ejercicio) periodoFilter.ejercicio = parseInt(ejercicio);
  if (periodo)   periodoFilter.periodo   = parseInt(periodo);

  const TIPO_LABEL    = { I: 'Ingreso', E: 'Egreso', P: 'Pago', T: 'Traslado', N: 'Nómina' };
  const SEV_LABEL     = { critical: 'Crítica', high: 'Alta', warning: 'Advertencia', medium: 'Media', low: 'Baja' };
  const COMP_LABEL    = { match: 'Conciliado', discrepancy: 'Discrepancia', warning: 'Advertencia', not_in_sat: 'No en SAT', not_in_erp: 'No en ERP', cancelled: 'Cancelado', pending: 'Pendiente', error: 'Error' };
  const CAMPO_LABEL   = { 'total': 'Total', 'subTotal': 'Subtotal', 'impuestos.totalImpuestosTrasladados': 'IVA Trasladado', 'impuestos.totalImpuestosRetenidos': 'IVA Retenido', 'emisor.rfc': 'RFC Emisor', 'receptor.rfc': 'RFC Receptor', 'fecha': 'Fecha', 'tipoDeComprobante': 'Tipo', 'moneda': 'Moneda', 'tipoCambio': 'Tipo Cambio' };
  const MESES         = ['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'];
  const periodoLabel  = ejercicio ? (periodo ? `${MESES[parseInt(periodo)-1]} ${ejercicio}` : `Año ${ejercicio}`) : 'Todos los periodos';

  // ── FASE 1: Datos que no dependen de los UUIDs ERP ───────────────────────
  const [allErpCfdis, erpCancelados, erpDeshabilitados, erpInactivoSatVigente, allSatForPeriod, resumenTipos, resumenSAT, cfdisMigrados] = await Promise.all([

    // CFDIs ERP Timbrados/Habilitados — igual que montosAggregate del dashboard
    CFDI.find({ source: 'ERP', isActive: { $ne: false }, erpStatus: { $in: ['Timbrado', 'Habilitado'] }, ...periodoFilter })
      .select('uuid serie folio tipoDeComprobante fecha emisor receptor subTotal descuento impuestos total moneda erpStatus satStatus lastComparisonStatus ejercicio periodo')
      .sort({ tipoDeComprobante: 1, lastComparisonStatus: 1, fecha: -1 })
      .lean(),

    // CFDIs ERP cancelados del periodo (hoja separada)
    CFDI.find({ source: 'ERP', isActive: { $ne: false }, erpStatus: { $in: ['Cancelado', 'Cancelacion Pendiente'] }, ...periodoFilter })
      .select('uuid serie folio tipoDeComprobante fecha emisor receptor subTotal descuento impuestos total moneda erpStatus satStatus lastComparisonStatus ejercicio periodo')
      .sort({ tipoDeComprobante: 1, fecha: -1 })
      .lean(),

    // CFDIs ERP deshabilitados del periodo (hoja separada)
    CFDI.find({ source: 'ERP', isActive: { $ne: false }, erpStatus: 'Deshabilitado', ...periodoFilter })
      .select('uuid serie folio tipoDeComprobante fecha emisor receptor subTotal descuento impuestos total moneda erpStatus satStatus lastComparisonStatus ejercicio periodo')
      .sort({ tipoDeComprobante: 1, fecha: -1 })
      .lean(),

    // ERP inactivo (Cancelado/Cancelacion Pendiente/Deshabilitado) pero SAT aún Vigente — hacen diferencia
    CFDI.find({ source: 'ERP', isActive: { $ne: false }, satStatus: 'Vigente', erpStatus: { $in: ['Cancelado', 'Cancelacion Pendiente', 'Deshabilitado'] }, ...periodoFilter })
      .select('uuid serie folio tipoDeComprobante fecha emisor receptor subTotal descuento impuestos total moneda erpStatus satStatus lastComparisonStatus ejercicio periodo')
      .sort({ tipoDeComprobante: 1, fecha: -1 })
      .lean(),

    // CFDIs SAT/MANUAL sin contraparte ERP (por lastComparisonStatus)
    CFDI.find({ source: { $in: ['SAT', 'MANUAL'] }, isActive: { $ne: false }, lastComparisonStatus: 'not_in_erp', ...periodoFilter })
      .select('uuid serie folio tipoDeComprobante fecha emisor receptor subTotal impuestos total moneda satStatus ejercicio periodo source')
      .sort({ tipoDeComprobante: 1, total: -1 })
      .lean(),

    // Resumen KPI ERP por tipo
    CFDI.aggregate([
      { $match: { source: 'ERP', isActive: { $ne: false }, erpStatus: { $in: ['Timbrado', 'Habilitado'] }, uuid: { $not: /^SINUUID/ }, ...periodoFilter } },
      { $group: {
        _id:             '$tipoDeComprobante',
        count:           { $sum: 1 },
        totalMonto:      { $sum: MONTO_EFECTIVO_EXPR },
        ivaTrasladadoTotal: { $sum: { $ifNull: ['$impuestos.totalImpuestosTrasladados', 0] } },
        ivaRetenidoTotal:   { $sum: { $ifNull: ['$impuestos.totalImpuestosRetenidos',   0] } },
        conciliados:     { $sum: { $cond: [{ $eq: ['$lastComparisonStatus', 'match'] }, 1, 0] } },
        conDiscrepancia: { $sum: { $cond: [{ $in: ['$lastComparisonStatus', ['discrepancy','warning']] }, 1, 0] } },
        notInSat:        { $sum: { $cond: [{ $eq: ['$lastComparisonStatus', 'not_in_sat'] }, 1, 0] } },
        sinConciliar:    { $sum: { $cond: [{ $in: ['$lastComparisonStatus', [null,'error','pending']] }, 1, 0] } },
      }},
      { $sort: { _id: 1 } },
    ]),

    // Totales SAT — solo Vigente
    CFDI.aggregate([
      { $match: { source: { $in: ['SAT', 'MANUAL'] }, isActive: { $ne: false }, satStatus: 'Vigente', ...periodoFilter } },
      { $group: {
        _id:                '$tipoDeComprobante',
        totalMonto:         { $sum: MONTO_EFECTIVO_EXPR },
        ivaTrasladadoTotal: { $sum: { $ifNull: ['$impuestos.totalImpuestosTrasladados', 0] } },
        ivaRetenidoTotal:   { $sum: { $ifNull: ['$impuestos.totalImpuestosRetenidos',   0] } },
        count:              { $sum: 1 },
      }},
    ]),

    // CFDIs migrados: CFDIs globales cuyo InformacionGlobal apunta a un periodo distinto
    // al que tiene registrado (fueron movidos manualmente a este periodo).
    // Se incluyen SAT/MANUAL/ERP con informacionGlobal.mes para cubrir ambos lados del match.
    CFDI.find({
      isActive: { $ne: false },
      'informacionGlobal.mes': { $exists: true, $nin: [null, ''] },
      ...periodoFilter,
      ...(periodoFilter.ejercicio || periodoFilter.periodo ? {
        $or: [
          ...(periodoFilter.periodo   ? [{ $expr: { $ne: [ { $toInt: '$informacionGlobal.mes'  }, periodoFilter.periodo   ] } }] : []),
          ...(periodoFilter.ejercicio ? [{ $expr: { $ne: [ { $toInt: '$informacionGlobal.anio' }, periodoFilter.ejercicio ] } }] : []),
        ],
      } : {}),
    })
      .select('uuid serie folio tipoDeComprobante fecha emisor receptor subTotal impuestos total moneda satStatus erpStatus lastComparisonStatus ejercicio periodo informacionGlobal source')
      .sort({ tipoDeComprobante: 1, fecha: -1 })
      .lean(),
  ]);

  // ── FASE 2: Queries que usan los UUIDs de los CFDIs ERP del periodo ────────
  const erpUuids = allErpCfdis.map(c => c.uuid).filter(Boolean);

  // soloSat ya viene filtrado por lastComparisonStatus desde la query
  const soloSat = allSatForPeriod;

  // Status mismatches: ERP activo pero SAT Cancelado / ERP cancelado pero SAT Vigente
  const satCanceladoErpActivo = allErpCfdis.filter(c => c.satStatus === 'Cancelado');
  const erpCanceladoSatVigente = erpInactivoSatVigente; // alias semántico

  const [comparisonsRaw, allDiscrepancias] = await Promise.all([
    // Traer comparaciones más recientes por UUID con differences completo
    // Se usa find+sort+dedup en lugar de aggregate para que $first no pierda el array differences
    Comparison.find({ uuid: { $in: erpUuids } })
      .select('uuid satCfdiId differences comparedAt')
      .sort({ comparedAt: -1 })
      .lean(),

    // Discrepancias activas filtradas por UUID del ERP del periodo
    Discrepancy.find({ uuid: { $in: erpUuids }, status: { $nin: ['resolved', 'ignored'] } })
      .select('uuid type severity description erpValue satValue')
      .lean(),
  ]);

  // ── Construir mapa de contrapartes SAT (más reciente por UUID) ─────────────
  const compByUuid = {};
  for (const c of comparisonsRaw) {
    if (!compByUuid[c.uuid]) compByUuid[c.uuid] = c; // ya ordenado desc por comparedAt
  }

  // Buscar contrapartes SAT directamente por UUID (más confiable que satCfdiId de Comparison)
  const satCfdiDocs = erpUuids.length
    ? await CFDI.find({ source: { $in: ['SAT', 'MANUAL'] }, uuid: { $in: erpUuids }, isActive: { $ne: false }, satStatus: 'Vigente' })
        .select('uuid total subTotal descuento impuestos satStatus').lean()
    : [];
  const satByUuid = {};
  for (const s of satCfdiDocs) satByUuid[(s.uuid || '').toUpperCase()] = s;

  // Mapa de totales SAT por tipo
  const satTotalByTipo = {};
  for (const r of resumenSAT) {
    satTotalByTipo[r._id] = { totalMonto: r.totalMonto || 0, ivaTrasladadoTotal: r.ivaTrasladadoTotal || 0, ivaRetenidoTotal: r.ivaRetenidoTotal || 0, count: r.count || 0 };
  }

  // ── Mapas auxiliares ───────────────────────────────────────────────────────
  const discByUuid = {};
  for (const d of allDiscrepancias) {
    if (!discByUuid[d.uuid]) discByUuid[d.uuid] = [];
    discByUuid[d.uuid].push(d);
  }

  const cfdisByTipo = {};
  for (const c of allErpCfdis) {
    const t = c.tipoDeComprobante || 'Otro';
    if (!cfdisByTipo[t]) cfdisByTipo[t] = [];
    cfdisByTipo[t].push(c);
  }

  // Agrupar ERP-inactivo/SAT-vigente por tipo para la sección de diferencias
  const erpInactivoSatVigentePorTipo = {};
  for (const c of erpInactivoSatVigente) {
    const t = c.tipoDeComprobante || 'Otro';
    if (!erpInactivoSatVigentePorTipo[t]) erpInactivoSatVigentePorTipo[t] = [];
    erpInactivoSatVigentePorTipo[t].push(c);
  }

  // ── Estilos ────────────────────────────────────────────────────────────────
  const FG_HDR    = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1F3A5F' } };
  const FG_TOTAL  = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFE8F0FE' } };
  const FG_KPI    = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF0F4FF' } };
  const FG_WARN   = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFF3CD' } };
  const FG_DANGER = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF8D7DA' } };
  const FG_OK     = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFD1FAE5' } };
  const FG_SAT    = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFF8F0' } };
  const FONT_HDR  = { bold: true, color: { argb: 'FFFFFFFF' }, size: 10 };
  const FONT_BOLD = { bold: true, size: 10 };
  const MXN       = '"$"#,##0.00';
  const colLetter = (n) => n <= 26 ? String.fromCharCode(64 + n) : 'Z';

  const workbook = new ExcelJS.Workbook();
  workbook.creator = 'NUMO'; workbook.created = new Date();

  const addTitle = (sheet, title, ncols) => {
    const lc = colLetter(ncols);
    sheet.mergeCells(`A1:${lc}1`);
    const t = sheet.getCell('A1');
    t.value = title; t.font = { bold: true, size: 13, color: { argb: 'FF1F3A5F' } }; t.alignment = { horizontal: 'center', vertical: 'middle' };
    sheet.getRow(1).height = 26;
    sheet.mergeCells(`A2:${lc}2`);
    const s = sheet.getCell('A2');
    s.value = `Generado el ${new Date().toLocaleString('es-MX')} — ${periodoLabel}`; s.font = { italic: true, size: 9, color: { argb: 'FF64748B' } }; s.alignment = { horizontal: 'center' };
    sheet.getRow(2).height = 15;
  };

  const fmtNum = (v) => v != null ? Math.round(v * 100) / 100 : null;

  // ══════════════════════════════════════════════════════════════════════════
  // HOJA 1 — Resumen General
  // ══════════════════════════════════════════════════════════════════════════
  const s1 = workbook.addWorksheet('1. Resumen General');
  s1.views = [{ state: 'frozen', ySplit: 3 }];
  addTitle(s1, `Reporte de Conciliación CFDI — ${periodoLabel}`, 11);

  s1.columns = [
    { key: 'tipo',        width: 8  },
    { key: 'desc',        width: 12 },
    { key: 'count',       width: 10 },
    { key: 'totalERP',    width: 20 },
    { key: 'ivaTraERP',   width: 20 },
    { key: 'ivaRetERP',   width: 20 },
    { key: 'totalSAT',    width: 20 },
    { key: 'diferencia',  width: 20 },
    { key: 'conciliados', width: 13 },
    { key: 'discrepancia',width: 18 },
    { key: 'notInSat',    width: 13 },
  ];
  const h1 = s1.getRow(3);
  h1.values = ['Tipo','Descripción','CFDIs ERP','Total ERP','IVA Trasladado ERP','IVA Retenido ERP','Total SAT','Diferencia','Conciliados','Con Discrepancia','No en SAT'];
  h1.eachCell(c => { c.font = FONT_HDR; c.fill = FG_HDR; c.alignment = { horizontal: 'center', vertical: 'middle' }; }); h1.height = 22;

  const gt = { count: 0, totalERP: 0, ivaTraERP: 0, ivaRetERP: 0, totalSAT: 0, conciliados: 0, discrepancia: 0, notInSat: 0 };
  for (const t of resumenTipos) {
    const satT = satTotalByTipo[t._id]?.totalMonto || 0;
    const dif  = fmtNum((t.totalMonto || 0) - satT);
    const row  = s1.addRow({ tipo: t._id || '?', desc: TIPO_LABEL[t._id] || t._id || 'Otro', count: t.count, totalERP: fmtNum(t.totalMonto), ivaTraERP: fmtNum(t.ivaTrasladadoTotal), ivaRetERP: fmtNum(t.ivaRetenidoTotal), totalSAT: fmtNum(satT), diferencia: dif, conciliados: t.conciliados, discrepancia: t.conDiscrepancia, notInSat: t.notInSat });
    ['totalERP','ivaTraERP','ivaRetERP','totalSAT','diferencia'].forEach(k => { row.getCell(k).numFmt = MXN; });
    if (Math.abs(dif) > 0.01) row.getCell('diferencia').fill = FG_DANGER;
    else                      row.getCell('diferencia').fill = FG_OK;
    if (t.conDiscrepancia > 0) row.getCell('discrepancia').fill = FG_WARN;
    if (t.notInSat > 0)        row.getCell('notInSat').fill     = FG_DANGER;
    if (t.conciliados === t.count && t.count > 0) row.getCell('conciliados').fill = FG_OK;
    gt.count += t.count; gt.totalERP += t.totalMonto || 0; gt.ivaTraERP += t.ivaTrasladadoTotal || 0; gt.ivaRetERP += t.ivaRetenidoTotal || 0; gt.totalSAT += satT; gt.conciliados += t.conciliados; gt.discrepancia += t.conDiscrepancia; gt.notInSat += t.notInSat;
  }
  const tr1 = s1.addRow({ tipo: 'TOTAL', desc: '', count: gt.count, totalERP: fmtNum(gt.totalERP), ivaTraERP: fmtNum(gt.ivaTraERP), ivaRetERP: fmtNum(gt.ivaRetERP), totalSAT: fmtNum(gt.totalSAT), diferencia: fmtNum(gt.totalERP - gt.totalSAT), conciliados: gt.conciliados, discrepancia: gt.discrepancia, notInSat: gt.notInSat });
  tr1.eachCell(c => { c.font = FONT_BOLD; c.fill = FG_TOTAL; });
  ['totalERP','ivaTraERP','ivaRetERP','totalSAT','diferencia'].forEach(k => { tr1.getCell(k).numFmt = MXN; });

  // ══════════════════════════════════════════════════════════════════════════
  // HOJAS POR TIPO — una hoja por cada tipo de comprobante con CFDIs
  // ══════════════════════════════════════════════════════════════════════════
  const DETAIL_COLS = [
    { key: 'uuid',         header: 'UUID',                width: 38 },
    { key: 'serie',        header: 'Serie',               width: 8  },
    { key: 'folio',        header: 'Folio',               width: 10 },
    { key: 'fecha',        header: 'Fecha',               width: 12 },
    { key: 'rfcEmisor',    header: 'RFC Emisor',          width: 15 },
    { key: 'nomEmisor',    header: 'Nombre Emisor',       width: 30 },
    { key: 'rfcReceptor',  header: 'RFC Receptor',        width: 15 },
    { key: 'nomReceptor',  header: 'Nombre Receptor',     width: 30 },
    { key: 'descuento',    header: 'Descuento ERP',       width: 16 },
    { key: 'subERP',       header: 'Subtotal ERP',        width: 16 },
    { key: 'ivaTraERP',    header: 'IVA Trasladado ERP',  width: 18 },
    { key: 'ivaRetERP',    header: 'IVA Retenido ERP',    width: 18 },
    { key: 'totalERP',     header: 'Total ERP',           width: 16 },
    { key: 'descuentoSAT', header: 'Descuento SAT',       width: 16 },
    { key: 'subSAT',       header: 'Subtotal SAT',        width: 16 },
    { key: 'ivaTraSAT',    header: 'IVA Trasladado SAT',  width: 18 },
    { key: 'totalSAT',     header: 'Total SAT',           width: 16 },
    { key: 'diferencia',   header: 'Diferencia',          width: 16 },
    { key: 'estadoERP',    header: 'Estado ERP',          width: 18 },
    { key: 'estadoSAT',    header: 'Estado SAT',          width: 14 },
    { key: 'conciliacion', header: 'Conciliación',        width: 18 },
    { key: 'tiposDisc',    header: 'Tipos Discrepancia',  width: 35 },
    { key: 'detalleDisc',  header: 'Detalle Diferencias', width: 80 },
  ];
  const MONEY_KEYS = ['descuento','subERP','ivaTraERP','ivaRetERP','totalERP','descuentoSAT','subSAT','ivaTraSAT','totalSAT','diferencia'];

  const tiposEnUso = [...new Set(allErpCfdis.map(c => c.tipoDeComprobante).filter(Boolean))].sort();
  const tiposEnUsoSet = new Set(tiposEnUso);

  // Agrupar soloSat por tipo para insertarlos al final de cada hoja de tipo
  const soloSatByTipo = {};
  for (const c of soloSat) {
    const t = c.tipoDeComprobante || 'Otro';
    if (!soloSatByTipo[t]) soloSatByTipo[t] = [];
    soloSatByTipo[t].push(c);
  }

  for (const tipo of tiposEnUso) {
    const cfdis = cfdisByTipo[tipo] || [];
    const label  = TIPO_LABEL[tipo] || tipo;
    const sheetN = workbook.worksheets.length + 1;
    const sheet  = workbook.addWorksheet(`${sheetN}. ${label} (${tipo})`);
    sheet.views  = [{ state: 'frozen', ySplit: 5 }];
    addTitle(sheet, `${label} (Tipo ${tipo}) — ${periodoLabel}`, DETAIL_COLS.length);

    // ── KPI row (fila 3) ──
    const resT   = resumenTipos.find(r => r._id === tipo) || {};
    const satTot = satTotalByTipo[tipo]?.totalMonto || 0;
    const difTot = fmtNum((resT.totalMonto || 0) - satTot);
    const NC     = DETAIL_COLS.length;
    const kpiTxt = [
      `CFDIs ERP: ${cfdis.length}`,
      `Total ERP: $${(resT.totalMonto || 0).toLocaleString('es-MX', { minimumFractionDigits: 2 })}`,
      `Total SAT: $${satTot.toLocaleString('es-MX', { minimumFractionDigits: 2 })}`,
      `Diferencia: $${difTot.toLocaleString('es-MX', { minimumFractionDigits: 2 })}`,
      `Conciliados: ${resT.conciliados || 0}  |  Con discrepancia: ${resT.conDiscrepancia || 0}  |  No en SAT: ${resT.notInSat || 0}`,
    ].join('     ');

    sheet.mergeCells(`A3:${colLetter(NC)}3`);
    const kpiCell = sheet.getCell('A3');
    kpiCell.value = kpiTxt;
    kpiCell.font  = FONT_BOLD;
    kpiCell.fill  = FG_KPI;
    kpiCell.alignment = { horizontal: 'left', vertical: 'middle' };
    sheet.getRow(3).height = 20;

    sheet.mergeCells(`A4:${colLetter(NC)}4`);
    sheet.getRow(4).height = 6;

    // ── Cabecera de tabla (fila 5) ──
    sheet.columns = DETAIL_COLS;
    const hdrRow  = sheet.getRow(5);
    hdrRow.values = DETAIL_COLS.map(c => c.header);
    hdrRow.eachCell(c => { c.font = FONT_HDR; c.fill = FG_HDR; c.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true }; });
    hdrRow.height = 30;

    let sumDescuento = 0, sumSubERP = 0, sumIvaTraERP = 0, sumIvaRetERP = 0, sumTotERP = 0;
    let sumDescuentoSAT = 0, sumSubSAT = 0, sumIvaTraSAT = 0, sumTotSAT = 0, sumDif = 0;
    const cfdisDiferencia = []; // CFDIs que hacen la diferencia

    for (const cfdi of cfdis) {
      const comp    = compByUuid[cfdi.uuid];
      const satCfdi = satByUuid[(cfdi.uuid || '').toUpperCase()] || null;
      const discs   = discByUuid[cfdi.uuid] || [];

      const descuentoERP = cfdi.descuento || 0;
      const subERP    = (cfdi.subTotal || 0) - descuentoERP;
      const ivaTraERP = cfdi.impuestos?.totalImpuestosTrasladados || 0;
      const ivaRetERP = cfdi.impuestos?.totalImpuestosRetenidos   || 0;
      const totERP    = cfdi.total || 0;
      const descuentoSAT = satCfdi ? (satCfdi.descuento || 0) : null;
      const subSAT    = satCfdi ? (satCfdi.subTotal || 0) - (satCfdi.descuento || 0) : null;
      const ivaTraSAT = satCfdi ? (satCfdi.impuestos?.totalImpuestosTrasladados || 0) : null;
      const totSAT    = satCfdi ? (satCfdi.total || 0) : null;
      const dif       = totSAT !== null ? fmtNum(totERP - totSAT) : null;

      // Tipos de discrepancia
      const tiposDisc = [...new Set(discs.map(d => d.type))].join(', ');

      // Detalle campo a campo: primero desde Comparison.differences, sino desde Discrepancy
      let detalleDisc = '';
      if (comp?.differences?.length) {
        detalleDisc = comp.differences.map(d => {
          const campo = CAMPO_LABEL[d.field] || d.field;
          const sev   = SEV_LABEL[d.severity] || '';
          const erp   = d.erpValue != null ? d.erpValue : '—';
          const sat   = d.satValue != null ? d.satValue : '—';
          return `${campo} [${sev}]: ERP=${erp} → SAT=${sat}`;
        }).join(' | ');
      } else if (discs.length) {
        detalleDisc = discs.map(d => `${d.type} (${SEV_LABEL[d.severity] || d.severity}): ${d.description}`).join(' | ');
      }

      const row = sheet.addRow({
        uuid: cfdi.uuid, serie: cfdi.serie || '', folio: cfdi.folio || '',
        fecha: cfdi.fecha ? new Date(cfdi.fecha).toLocaleDateString('es-MX') : '',
        rfcEmisor: cfdi.emisor?.rfc || '', nomEmisor: cfdi.emisor?.nombre || '',
        rfcReceptor: cfdi.receptor?.rfc || '', nomReceptor: cfdi.receptor?.nombre || '',
        descuento: descuentoERP, subERP, ivaTraERP, ivaRetERP, totalERP: totERP,
        descuentoSAT, subSAT, ivaTraSAT, totalSAT: totSAT,
        diferencia: dif,
        estadoERP:    cfdi.erpStatus || '—',
        estadoSAT:    cfdi.satStatus || '—',
        conciliacion: COMP_LABEL[cfdi.lastComparisonStatus] || cfdi.lastComparisonStatus || 'Sin comparar',
        tiposDisc,
        detalleDisc,
      });

      MONEY_KEYS.forEach(k => { if (row.getCell(k).value != null) row.getCell(k).numFmt = MXN; });

      const cs          = cfdi.lastComparisonStatus;
      const hasDif      = dif !== null && Math.abs(dif) > 0.01;
      const hasCritical = discs.some(d => d.severity === 'critical' || d.severity === 'high');
      const sinSatVigente = !satCfdi; // no hay contraparte SAT Vigente
      const FG_REVIEW   = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFD6CC' } }; // naranja claro — revisar

      // Color base de la fila completa
      if (cfdi.satStatus === 'Cancelado') {
        // Timbrado en ERP pero cancelado en SAT → rojo
        row.eachCell({ includeEmpty: true }, cell => { cell.fill = FG_DANGER; });
      } else if (hasDif || hasCritical) {
        // Diferencia de monto o discrepancia crítica → rojo
        row.eachCell({ includeEmpty: true }, cell => { cell.fill = FG_DANGER; });
      } else if (sinSatVigente) {
        // ERP Timbrado sin contraparte SAT Vigente → naranja para revisar
        row.eachCell({ includeEmpty: true }, cell => { cell.fill = FG_REVIEW; });
      } else if (cs === 'discrepancy' || cs === 'warning' || discs.length > 0) {
        // Discrepancia de campo sin diferencia de monto → amarillo
        row.eachCell({ includeEmpty: true }, cell => { cell.fill = FG_WARN; });
      } else if (cs === 'match') {
        // Conciliado → solo celda de estado en verde
        row.getCell('conciliacion').fill = FG_OK;
      }

      // Resaltar celdas individuales donde hay diferencia de montos (encima del color de fila)
      const FG_DIFF = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFDC2626' } }; // rojo intenso
      if (subSAT !== null && Math.abs(subERP - subSAT) > 0.01) {
        row.getCell('subERP').fill = FG_DIFF; row.getCell('subSAT').fill = FG_DIFF;
      }
      if (ivaTraSAT !== null && Math.abs(ivaTraERP - ivaTraSAT) > 0.01) {
        row.getCell('ivaTraERP').fill = FG_DIFF; row.getCell('ivaTraSAT').fill = FG_DIFF;
      }
      if (totSAT !== null && Math.abs(totERP - totSAT) > 0.01) {
        row.getCell('totalERP').fill = FG_DIFF; row.getCell('totalSAT').fill = FG_DIFF;
        row.getCell('diferencia').fill = FG_DIFF;
      }

      sumDescuento += descuentoERP; sumSubERP += subERP; sumIvaTraERP += ivaTraERP; sumIvaRetERP += ivaRetERP; sumTotERP += totERP;
      sumDescuentoSAT += descuentoSAT || 0;
      sumSubSAT    += subSAT    || 0;
      sumIvaTraSAT += ivaTraSAT || 0;
      sumTotSAT    += totSAT    || 0;
      sumDif       += dif       || 0;

      // Registrar CFDIs que hacen diferencia
      if (sinSatVigente || hasDif || hasCritical || cfdi.satStatus === 'Cancelado') {
        let motivo = '';
        if (cfdi.satStatus === 'Cancelado')  motivo = 'Cancelado en SAT';
        else if (sinSatVigente)              motivo = 'Sin Vigente en SAT';
        else if (hasDif)                     motivo = `Diferencia $${fmtNum(dif).toLocaleString('es-MX', { minimumFractionDigits: 2 })}`;
        else if (hasCritical)                motivo = 'Discrepancia crítica';
        cfdisDiferencia.push({ cfdi, totERP, totSAT, dif, motivo, detalleDisc });
      }
    }

    // Fila totales
    const tr = sheet.addRow({ uuid: `TOTAL (${cfdis.length} CFDIs)`, descuento: fmtNum(sumDescuento), subERP: fmtNum(sumSubERP), ivaTraERP: fmtNum(sumIvaTraERP), ivaRetERP: fmtNum(sumIvaRetERP), totalERP: fmtNum(sumTotERP), descuentoSAT: fmtNum(sumDescuentoSAT), subSAT: fmtNum(sumSubSAT), ivaTraSAT: fmtNum(sumIvaTraSAT), totalSAT: fmtNum(sumTotSAT), diferencia: fmtNum(sumDif) });
    tr.eachCell(c => { c.font = FONT_BOLD; c.fill = FG_TOTAL; });
    MONEY_KEYS.forEach(k => { tr.getCell(k).numFmt = MXN; });
    if (Math.abs(sumDif) > 0.01) tr.getCell('diferencia').fill = FG_DANGER;

    // Agregar ERP inactivo pero SAT Vigente al listado de diferencias
    for (const cfdi of (erpInactivoSatVigentePorTipo[tipo] || [])) {
      const motivo = cfdi.erpStatus === 'Cancelacion Pendiente'
        ? 'Cancelación Pendiente ERP — Vigente SAT'
        : cfdi.erpStatus === 'Cancelado'
          ? 'Cancelado ERP — Vigente SAT'
          : 'Deshabilitado ERP — Vigente SAT';
      cfdisDiferencia.push({ cfdi, totERP: cfdi.total || 0, totSAT: null, dif: null, motivo, detalleDisc: '' });
    }

    // ── Sección inferior: CFDIs que hacen la diferencia ──────────────────────
    if (cfdisDiferencia.length > 0) {
      // Fila separadora
      sheet.addRow({});
      const sepR = sheet.addRow({ uuid: `⚠ CFDIs que hacen la diferencia (${cfdisDiferencia.length})` });
      sheet.mergeCells(`A${sepR.number}:${colLetter(NC)}${sepR.number}`);
      sepR.getCell('uuid').font = { bold: true, size: 10, color: { argb: 'FF991B1B' } };
      sepR.getCell('uuid').fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFEE2E2' } };
      sepR.getCell('uuid').alignment = { horizontal: 'left', vertical: 'middle' };
      sepR.height = 20;

      // Cabecera de la sección
      const DIFF_COLS = ['UUID', 'Serie', 'Folio', 'Fecha', 'RFC Emisor', 'RFC Receptor', 'Total ERP', 'Total SAT', 'Diferencia', 'Estado ERP', 'Estado SAT', 'Motivo', 'Detalle Diferencias'];
      const DIFF_KEYS = ['uuid', 'serie', 'folio', 'fecha', 'rfcEmisor', 'rfcReceptor', 'totalERP', 'totalSAT', 'diferencia', 'estadoERP', 'estadoSAT', 'motivo', 'detalle'];
      const hdrDiff = sheet.addRow({});
      DIFF_COLS.forEach((h, i) => {
        const cell = hdrDiff.getCell(i + 1);
        cell.value = h;
        cell.font  = FONT_HDR;
        cell.fill  = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF7F1D1D' } };
        cell.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true };
      });
      hdrDiff.height = 22;

      const DIFF_MONEY = [7, 8, 9]; // columnas con montos (1-based)
      for (const { cfdi, totERP, totSAT, dif, motivo, detalleDisc } of cfdisDiferencia) {
        const dr = sheet.addRow([
          cfdi.uuid, cfdi.serie || '', cfdi.folio || '',
          cfdi.fecha ? new Date(cfdi.fecha).toLocaleDateString('es-MX') : '',
          cfdi.emisor?.rfc || '', cfdi.receptor?.rfc || '',
          totERP, totSAT, dif,
          cfdi.erpStatus || '—', cfdi.satStatus || '—',
          motivo, detalleDisc,
        ]);
        DIFF_MONEY.forEach(col => { if (dr.getCell(col).value != null) dr.getCell(col).numFmt = MXN; });
        const fg = motivo === 'Sin Vigente en SAT'
          ? { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFD6CC' } }
          : FG_DANGER;
        dr.eachCell({ includeEmpty: true }, cell => { cell.fill = fg; });
        if (dif !== null && Math.abs(dif) > 0.01) {
          dr.getCell(7).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFDC2626' } };
          dr.getCell(8).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFDC2626' } };
          dr.getCell(9).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFDC2626' } };
        }
      }
    }

    // ── Solo en SAT para este tipo ───────────────────────────────────────────
    const satOnlyTipo = soloSatByTipo[tipo] || [];
    if (satOnlyTipo.length > 0) {
      sheet.addRow({});
      const sepSAT = sheet.addRow({ uuid: `⛔ Solo en SAT — No encontrados en ERP (${satOnlyTipo.length})` });
      sheet.mergeCells(`A${sepSAT.number}:${colLetter(NC)}${sepSAT.number}`);
      sepSAT.getCell('uuid').font = { bold: true, size: 10, color: { argb: 'FFFFFFFF' } };
      sepSAT.getCell('uuid').fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFDC2626' } };
      sepSAT.getCell('uuid').alignment = { horizontal: 'left', vertical: 'middle' };
      sepSAT.height = 20;

      for (const c of satOnlyTipo) {
        const dr = sheet.addRow({
          uuid:        c.uuid,
          serie:       c.serie    || '',
          folio:       c.folio    || '',
          fecha:       c.fecha ? new Date(c.fecha).toLocaleDateString('es-MX') : '',
          rfcEmisor:   c.emisor?.rfc    || '',
          nomEmisor:   c.emisor?.nombre || '',
          rfcReceptor: c.receptor?.rfc    || '',
          nomReceptor: c.receptor?.nombre || '',
          descuento: null, subERP: null, ivaTraERP: null, ivaRetERP: null, totalERP: null,
          descuentoSAT: c.descuento || 0, subSAT: (c.subTotal || 0) - (c.descuento || 0),
          ivaTraSAT: c.impuestos?.totalImpuestosTrasladados || 0,
          totalSAT:  c.total    || 0,
          diferencia:   null,
          estadoERP:    '— No en ERP —',
          estadoSAT:    c.satStatus || '—',
          conciliacion: 'Solo en SAT',
          tiposDisc:    'MISSING_IN_ERP',
          detalleDisc:  '',
        });
        ['subSAT', 'ivaTraSAT', 'totalSAT'].forEach(k => { if (dr.getCell(k).value != null) dr.getCell(k).numFmt = MXN; });
        dr.eachCell({ includeEmpty: true }, cell => { cell.fill = FG_DANGER; });
      }
    }
  }

  // ══════════════════════════════════════════════════════════════════════════
  // HOJA — Facturas Migradas
  // CFDIs cuya InformacionGlobal apunta a un periodo distinto al actual
  // (fueron reclasificados manualmente a este periodo)
  // ══════════════════════════════════════════════════════════════════════════
  if (cfdisMigrados.length > 0) {
    const sMig  = workbook.addWorksheet(`${workbook.worksheets.length + 1}. Facturas Migradas`);
    sMig.views  = [{ state: 'frozen', ySplit: 3 }];
    const MIG_COLS = [
      { key: 'tipo',       header: 'Tipo',             width: 7  },
      { key: 'desc',       header: 'Descripción',      width: 11 },
      { key: 'source',     header: 'Origen',           width: 9  },
      { key: 'uuid',       header: 'UUID',             width: 38 },
      { key: 'serie',      header: 'Serie',            width: 8  },
      { key: 'folio',      header: 'Folio',            width: 10 },
      { key: 'fecha',      header: 'Fecha',            width: 12 },
      { key: 'rfcEmisor',  header: 'RFC Emisor',       width: 15 },
      { key: 'nomEmisor',  header: 'Nombre Emisor',    width: 30 },
      { key: 'rfcRec',     header: 'RFC Receptor',     width: 15 },
      { key: 'nomRec',     header: 'Nombre Receptor',  width: 30 },
      { key: 'subTotal',   header: 'Subtotal',         width: 16 },
      { key: 'ivaTrasl',   header: 'IVA Trasladado',   width: 18 },
      { key: 'total',      header: 'Total',            width: 16 },
      { key: 'periodoCurrent', header: 'Periodo Actual (ERP)', width: 20 },
      { key: 'periodoIG',  header: 'Periodo InfGlobal',width: 20 },
      { key: 'estadoSAT',  header: 'Estado SAT',       width: 13 },
      { key: 'concil',     header: 'Conciliación',     width: 18 },
    ];
    addTitle(sMig, `Facturas Migradas al Periodo — ${periodoLabel}`, MIG_COLS.length);
    sMig.columns = MIG_COLS;
    const hdrMig = sMig.getRow(3);
    hdrMig.values = MIG_COLS.map(c => c.header);
    hdrMig.eachCell(c => { c.font = FONT_HDR; c.fill = FG_HDR; c.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true }; });
    hdrMig.height = 28;

    const FG_MIG = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFEDE9FE' } };
    for (const c of cfdisMigrados) {
      const periodoActual = `${c.ejercicio || '?'}/${String(c.periodo || '?').padStart(2,'0')}`;
      const periodoIG     = `${c.informacionGlobal?.anio || '?'}/${String(c.informacionGlobal?.mes || '?').padStart(2,'0')}`;
      const row = sMig.addRow({
        tipo:         c.tipoDeComprobante || '',
        desc:         TIPO_LABEL[c.tipoDeComprobante] || '',
        source:       c.source,
        uuid:         c.uuid,
        serie:        c.serie  || '',
        folio:        c.folio  || '',
        fecha:        c.fecha  ? new Date(c.fecha).toLocaleDateString('es-MX') : '',
        rfcEmisor:    c.emisor?.rfc    || '',
        nomEmisor:    c.emisor?.nombre || '',
        rfcRec:       c.receptor?.rfc    || '',
        nomRec:       c.receptor?.nombre || '',
        subTotal:     c.subTotal || 0,
        ivaTrasl:     c.impuestos?.totalImpuestosTrasladados || 0,
        total:        c.total || 0,
        periodoCurrent: periodoActual,
        periodoIG,
        estadoSAT:    c.satStatus || '—',
        concil:       COMP_LABEL[c.lastComparisonStatus] || c.lastComparisonStatus || 'Sin comparar',
      });
      ['subTotal','ivaTrasl','total'].forEach(k => { row.getCell(k).numFmt = MXN; });
      row.eachCell(cell => { if (!cell.fill || cell.fill.type === 'none') cell.fill = FG_MIG; });
      if (periodoActual !== periodoIG) {
        row.getCell('periodoCurrent').fill = FG_WARN;
        row.getCell('periodoIG').fill      = FG_WARN;
      }
    }
  }

  // Helper: escribe hoja de CFDIs inactivos agrupados por tipo
  const addInactiveSheet = async (cfdis, sheetLabel, title, fgColor, satByUuidMap) => {
    const sheet = workbook.addWorksheet(`${workbook.worksheets.length + 1}. ${sheetLabel}`);
    sheet.views = [{ state: 'frozen', ySplit: 5 }];
    addTitle(sheet, `${title} — ${periodoLabel}`, DETAIL_COLS.length);

    sheet.mergeCells(`A3:${colLetter(DETAIL_COLS.length)}3`);
    const kpi = sheet.getCell('A3');
    kpi.value = `Total: ${cfdis.length} CFDIs`;
    kpi.font  = FONT_BOLD; kpi.fill = fgColor; kpi.alignment = { horizontal: 'left', vertical: 'middle' };
    sheet.getRow(3).height = 20;
    sheet.mergeCells(`A4:${colLetter(DETAIL_COLS.length)}4`);
    sheet.getRow(4).height = 6;

    sheet.columns = DETAIL_COLS;
    const hdr = sheet.getRow(5);
    hdr.values = DETAIL_COLS.map(c => c.header);
    hdr.eachCell(c => { c.font = FONT_HDR; c.fill = FG_HDR; c.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true }; });
    hdr.height = 30;

    // Agrupar por tipo
    const porTipo = {};
    for (const c of cfdis) {
      const t = c.tipoDeComprobante || 'Sin tipo';
      if (!porTipo[t]) porTipo[t] = [];
      porTipo[t].push(c);
    }

    for (const [tipo, lista] of Object.entries(porTipo).sort()) {
      // Fila separadora de tipo
      const sepRow = sheet.addRow({ uuid: `— ${TIPO_LABEL[tipo] || tipo} (${tipo}) — ${lista.length} CFDIs —` });
      sheet.mergeCells(`A${sepRow.number}:${colLetter(DETAIL_COLS.length)}${sepRow.number}`);
      sepRow.getCell('uuid').font = { bold: true, color: { argb: 'FF1F3A5F' }, size: 9 };
      sepRow.getCell('uuid').fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFE8F0FE' } };
      sepRow.getCell('uuid').alignment = { horizontal: 'left', vertical: 'middle' };
      sepRow.height = 18;

      for (const cfdi of lista) {
        const satCfdi = satByUuidMap[(cfdi.uuid || '').toUpperCase()] || null;
        const totERP  = cfdi.total || 0;
        const totSAT  = satCfdi ? (satCfdi.total || 0) : null;
        const dif     = totSAT !== null ? fmtNum(totERP - totSAT) : null;
        const row = sheet.addRow({
          uuid: cfdi.uuid, serie: cfdi.serie || '', folio: cfdi.folio || '',
          fecha: cfdi.fecha ? new Date(cfdi.fecha).toLocaleDateString('es-MX') : '',
          rfcEmisor: cfdi.emisor?.rfc || '', nomEmisor: cfdi.emisor?.nombre || '',
          rfcReceptor: cfdi.receptor?.rfc || '', nomReceptor: cfdi.receptor?.nombre || '',
          descuento: cfdi.descuento || 0,
          subERP: (cfdi.subTotal || 0) - (cfdi.descuento || 0),
          ivaTraERP: cfdi.impuestos?.totalImpuestosTrasladados || 0,
          ivaRetERP: cfdi.impuestos?.totalImpuestosRetenidos   || 0,
          totalERP: totERP,
          descuentoSAT: satCfdi ? (satCfdi.descuento || 0) : null,
          subSAT: satCfdi ? (satCfdi.subTotal || 0) - (satCfdi.descuento || 0) : null,
          ivaTraSAT: satCfdi ? (satCfdi.impuestos?.totalImpuestosTrasladados || 0) : null,
          totalSAT: totSAT,
          diferencia: dif,
          estadoERP: cfdi.erpStatus || '—',
          estadoSAT: cfdi.satStatus || '—',
          conciliacion: COMP_LABEL[cfdi.lastComparisonStatus] || cfdi.lastComparisonStatus || 'Sin comparar',
          tiposDisc: '', detalleDisc: '',
        });
        MONEY_KEYS.forEach(k => { if (row.getCell(k).value != null) row.getCell(k).numFmt = MXN; });
        row.eachCell({ includeEmpty: true }, cell => { cell.fill = fgColor; });
      }
    }
  };

  // ══════════════════════════════════════════════════════════════════════════
  // HOJA — Cancelados ERP (agrupados por tipo)
  // ══════════════════════════════════════════════════════════════════════════
  if (erpCancelados.length > 0) {
    const satCancelUuids = erpCancelados.map(c => c.uuid).filter(Boolean);
    const satCancelDocs  = satCancelUuids.length
      ? await CFDI.find({ source: { $in: ['SAT', 'MANUAL'] }, uuid: { $in: satCancelUuids }, isActive: { $ne: false } })
          .select('uuid total subTotal descuento impuestos satStatus').lean()
      : [];
    const satByUuidCan = {};
    for (const s of satCancelDocs) satByUuidCan[(s.uuid || '').toUpperCase()] = s;
    await addInactiveSheet(erpCancelados, 'Cancelados', 'CFDIs Cancelados en ERP', FG_DANGER, satByUuidCan);
  }

  // ══════════════════════════════════════════════════════════════════════════
  // HOJA — Deshabilitados ERP (agrupados por tipo)
  // ══════════════════════════════════════════════════════════════════════════
  if (erpDeshabilitados.length > 0) {
    const FG_DESH = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF3F4F6' } };
    const satDeshUuids = erpDeshabilitados.map(c => c.uuid).filter(Boolean);
    const satDeshDocs  = satDeshUuids.length
      ? await CFDI.find({ source: { $in: ['SAT', 'MANUAL'] }, uuid: { $in: satDeshUuids }, isActive: { $ne: false } })
          .select('uuid total subTotal descuento impuestos satStatus').lean()
      : [];
    const satByUuidDesh = {};
    for (const s of satDeshDocs) satByUuidDesh[(s.uuid || '').toUpperCase()] = s;
    await addInactiveSheet(erpDeshabilitados, 'Deshabilitados', 'CFDIs Deshabilitados en ERP', FG_DESH, satByUuidDesh);
  }

  // ══════════════════════════════════════════════════════════════════════════
  // HOJA — Discrepancias de Estado (SAT Cancelado/ERP Activo y viceversa)
  // ══════════════════════════════════════════════════════════════════════════
  const totalMismatch = satCanceladoErpActivo.length + erpCanceladoSatVigente.length;
  if (totalMismatch > 0) {
    const sMis = workbook.addWorksheet(`${workbook.worksheets.length + 1}. Mismatch Estado`);
    sMis.views = [{ state: 'frozen', ySplit: 3 }];
    const MIS_COLS = [
      { key: 'tipo',       header: 'Tipo',           width: 7  },
      { key: 'uuid',       header: 'UUID',           width: 38 },
      { key: 'serie',      header: 'Serie',          width: 8  },
      { key: 'folio',      header: 'Folio',          width: 10 },
      { key: 'fecha',      header: 'Fecha',          width: 12 },
      { key: 'rfcEmisor',  header: 'RFC Emisor',     width: 15 },
      { key: 'rfcRec',     header: 'RFC Receptor',   width: 15 },
      { key: 'total',      header: 'Total',          width: 16 },
      { key: 'estadoERP',  header: 'Estado ERP',     width: 18 },
      { key: 'estadoSAT',  header: 'Estado SAT',     width: 14 },
      { key: 'discrepancia', header: 'Discrepancia', width: 30 },
    ];
    addTitle(sMis, `Discrepancias de Estado — ${periodoLabel}`, MIS_COLS.length);
    sMis.columns = MIS_COLS;
    const hdrMis = sMis.getRow(3);
    hdrMis.values = MIS_COLS.map(c => c.header);
    hdrMis.eachCell(c => { c.font = FONT_HDR; c.fill = FG_HDR; c.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true }; });
    hdrMis.height = 28;

    const FG_MIS_A = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFDE8E8' } }; // rojo claro: SAT cancelado / ERP activo
    const FG_MIS_B = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFF3CD' } }; // amarillo: ERP cancelado / SAT vigente

    for (const c of satCanceladoErpActivo) {
      const row = sMis.addRow({
        tipo: c.tipoDeComprobante || '', uuid: c.uuid, serie: c.serie || '', folio: c.folio || '',
        fecha: c.fecha ? new Date(c.fecha).toLocaleDateString('es-MX') : '',
        rfcEmisor: c.emisor?.rfc || '', rfcRec: c.receptor?.rfc || '',
        total: c.total || 0, estadoERP: c.erpStatus || '—', estadoSAT: c.satStatus || '—',
        discrepancia: 'SAT Cancelado — ERP Activo',
      });
      row.getCell('total').numFmt = MXN;
      row.eachCell({ includeEmpty: true }, cell => { cell.fill = FG_MIS_A; });
    }

    for (const c of erpCanceladoSatVigente) {
      const row = sMis.addRow({
        tipo: c.tipoDeComprobante || '', uuid: c.uuid, serie: c.serie || '', folio: c.folio || '',
        fecha: c.fecha ? new Date(c.fecha).toLocaleDateString('es-MX') : '',
        rfcEmisor: c.emisor?.rfc || '', rfcRec: c.receptor?.rfc || '',
        total: c.total || 0, estadoERP: c.erpStatus || '—', estadoSAT: c.satStatus || '—',
        discrepancia: `ERP ${c.erpStatus || 'Inactivo'} — SAT Vigente`,
      });
      row.getCell('total').numFmt = MXN;
      row.eachCell({ includeEmpty: true }, cell => { cell.fill = FG_MIS_B; });
    }
  }

  // ══════════════════════════════════════════════════════════════════════════
  // HOJA FINAL — Solo en SAT con tipo no representado en ERP
  // ══════════════════════════════════════════════════════════════════════════
  // Los que SÍ tienen tipo en ERP ya se agregaron al final de su hoja de tipo
  const soloSatSinTipo = soloSat.filter(c => !tiposEnUsoSet.has(c.tipoDeComprobante));
  const sN   = workbook.worksheets.length + 1;
  const sLast = workbook.addWorksheet(`${sN}. Solo en SAT`);
  sLast.views = [{ state: 'frozen', ySplit: 3 }];
  const SAT_COLS = [
    { key: 'tipo',       header: 'Tipo',            width: 7  },
    { key: 'desc',       header: 'Descripción',     width: 11 },
    { key: 'source',     header: 'Origen',          width: 9  },
    { key: 'uuid',       header: 'UUID',            width: 38 },
    { key: 'serie',      header: 'Serie',           width: 8  },
    { key: 'folio',      header: 'Folio',           width: 10 },
    { key: 'fecha',      header: 'Fecha',           width: 12 },
    { key: 'rfcEmisor',  header: 'RFC Emisor',      width: 15 },
    { key: 'nomEmisor',  header: 'Nombre Emisor',   width: 30 },
    { key: 'rfcRec',     header: 'RFC Receptor',    width: 15 },
    { key: 'nomRec',     header: 'Nombre Receptor', width: 30 },
    { key: 'descuento',  header: 'Descuento',       width: 16 },
    { key: 'subTotal',   header: 'Subtotal',        width: 16 },
    { key: 'ivaTrasl',   header: 'IVA Trasladado',  width: 18 },
    { key: 'ivaRet',     header: 'IVA Retenido',    width: 18 },
    { key: 'total',      header: 'Total',           width: 16 },
    { key: 'estadoSAT',  header: 'Estado SAT',      width: 13 },
  ];
  addTitle(sLast, `CFDIs en SAT sin contraparte en ERP — ${periodoLabel}`, SAT_COLS.length);
  sLast.columns = SAT_COLS;
  const hdrLast = sLast.getRow(3);
  hdrLast.values = SAT_COLS.map(c => c.header);
  hdrLast.eachCell(c => { c.font = FONT_HDR; c.fill = FG_HDR; c.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true }; });
  hdrLast.height = 28;

  for (const c of soloSatSinTipo) {
    const row = sLast.addRow({ tipo: c.tipoDeComprobante || '', desc: TIPO_LABEL[c.tipoDeComprobante] || '', source: c.source, uuid: c.uuid, serie: c.serie || '', folio: c.folio || '', fecha: c.fecha ? new Date(c.fecha).toLocaleDateString('es-MX') : '', rfcEmisor: c.emisor?.rfc || '', nomEmisor: c.emisor?.nombre || '', rfcRec: c.receptor?.rfc || '', nomRec: c.receptor?.nombre || '', descuento: c.descuento || 0, subTotal: (c.subTotal || 0) - (c.descuento || 0), ivaTrasl: c.impuestos?.totalImpuestosTrasladados || 0, ivaRet: c.impuestos?.totalImpuestosRetenidos || 0, total: c.total || 0, estadoSAT: c.satStatus || '—' });
    ['descuento','subTotal','ivaTrasl','ivaRet','total'].forEach(k => { row.getCell(k).numFmt = MXN; });
    row.eachCell(cell => { if (!cell.fill || cell.fill.type === 'none') cell.fill = FG_DANGER; });
  }

  // ── Respuesta ──────────────────────────────────────────────────────────────
  const filename = `conciliacion_${ejercicio || 'all'}_${periodo || 'all'}_${Date.now()}.xlsx`;
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  await workbook.xlsx.write(res);
  res.end();
});

module.exports = { dashboard, exportExcel, debugMontos, discrepanciasMontos, debugDiscrepanciasMontos, satVigenteErpInactivo, discrepanciasCriticas, notInErp, pagosRelacionados, conciliacionExcel };
