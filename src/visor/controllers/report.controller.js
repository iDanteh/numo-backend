const ExcelJS = require('exceljs');
const mongoose = require('mongoose');
const CFDI = require('../models/CFDI');
const Comparison = require('../models/Comparison');
const Discrepancy = require('../models/Discrepancy');
const BankMovement = require('../../banks/domains/banks/BankMovement.model');
const { asyncHandler } = require('../../shared/middleware/error-handler');
const entityRepo = require('../repositories/entity.repository');
const { SERIES_CON_AUTH } = require('../../banks/domains/erp/erp-auth.utils');

// Cache in-memory para el dashboard — evita lanzar 15 queries MongoDB
// en cada carga de pantalla cuando múltiples usuarios consultan al mismo tiempo.
// TTL de 30 s: suficiente para agrupar clicks rápidos, sin ocultar cambios recientes.
const DASHBOARD_CACHE_TTL_MS = 30_000;
const dashboardCache = new Map(); // key → { data, expiresAt }

// El ERP sí manda `fecha` por movimiento del kardex — antes se perdía porque
// `movimientoSchema` (ErpCuentaPendiente.model.js) no la declaraba y Mongoose
// la recortaba silenciosamente al guardar. Ya está corregida ahí; de aquí en
// adelante los kardex sincronizados de nuevo van a traer `fecha` real.
//
// Para el kardex ya guardado ANTES de ese fix (sin `fecha`), se usa como
// respaldo la cadena de saldos (saldoAnterior de uno == saldoActual del
// anterior). Si esa cadena tampoco es inequívoca (empatada o rota), el
// último recurso es el folio: el ERP lo arma con prefijo AAMMDD (ej.
// 260512029 = 2026-05-12), por lo que es comparable numéricamente aun
// entre series distintas (RET/CPF/A0/BON/...) y sirve de proxy cronológico
// — mejor que dejar el orden nativo de Mongo, que puede venir invertido.
const centavos = (n) => Math.round((n ?? 0) * 100);

function ordenarPorFolio(movimientos) {
  const folios = movimientos.map(m => Number(m.folio));
  if (folios.some(f => !Number.isFinite(f))) return movimientos; // folio no numérico — no arriesgar
  return movimientos
    .map((m, i) => [m, folios[i]])
    .sort((a, b) => a[1] - b[1])
    .map(([m]) => m);
}

function ordenarPorCadenaDeSaldos(movimientos) {
  if (movimientos.length <= 1) return movimientos;

  const encadenarDesde = (raiz) => {
    const usados   = new Set([raiz]);
    const ordenado = [raiz];
    let actual = raiz;
    while (ordenado.length < movimientos.length) {
      const buscado    = centavos(actual.saldoActual);
      const candidatos = movimientos.filter(m => !usados.has(m) && centavos(m.saldoAnterior) === buscado);
      if (candidatos.length !== 1) return null; // ambiguo o roto
      actual = candidatos[0];
      usados.add(actual);
      ordenado.push(actual);
    }
    return ordenado;
  };

  // La raíz real (factura que origina la CxC) tiene saldoAnterior=0 y monto
  // POSITIVO. Un movimiento que en cambio LIQUIDA la CxC (queda en $0) tiene
  // saldoActual=0 — mismo valor que la raíz, pero en el extremo opuesto de la
  // cadena. En una CxC totalmente pagada, esa coincidencia numérica (0 == 0)
  // hacía que el detector de raíz anterior (basado solo en saldoAnterior vs.
  // saldosActuales) no reconociera a la factura como única raíz y cayera al
  // orden por folio, que no siempre es exactamente cronológico (visto en
  // producción: una Nota de Crédito quedaba entre dos abonos que en realidad
  // la precedían a ambos). El monto positivo distingue de forma confiable la
  // factura (raíz) de cualquier movimiento que también cierre en 0.
  const raicesPorMonto = movimientos.filter(m => centavos(m.saldoAnterior) === 0 && (m.total ?? 0) > 0);

  const saldosActuales  = movimientos.map(m => centavos(m.saldoActual));
  const raicesPorSaldo  = movimientos.filter(m => !saldosActuales.includes(centavos(m.saldoAnterior)));

  const candidatasRaiz = raicesPorMonto.length === 1 ? raicesPorMonto : raicesPorSaldo;
  if (candidatasRaiz.length !== 1) return ordenarPorFolio(movimientos);

  return encadenarDesde(candidatasRaiz[0]) ?? ordenarPorFolio(movimientos);
}

/**
 * Ordena el kardex de una CxC cronológicamente. Usa `fecha` cuando TODOS los
 * movimientos la tienen (dato real del ERP, 100% confiable); si falta en
 * alguno (kardex sincronizado antes del fix), cae a la cadena de saldos.
 */
function ordenarKardex(movimientos) {
  if (movimientos.length <= 1) return movimientos;
  if (movimientos.every(m => m.fecha)) {
    return [...movimientos].sort((a, b) => new Date(a.fecha) - new Date(b.fecha));
  }
  return ordenarPorCadenaDeSaldos(movimientos);
}

// Trae las CxC del ERP por erpId y marca cada movimiento de su kardex como
// pago bancario real (series con autorización: CBT/ABO/CPF/CFC) o ajuste sin
// depósito (bonificación, descuento, devolución, retención, etc.) — para que
// la UI pueda explicar por qué el saldo bajó sin que exista un depósito.
async function buscarCuentasPorCobrarConMovimientos(erpIds) {
  if (!erpIds.length) return [];
  const docs = await CFDI.db.collection('erp_cuentas_pendientes').find(
    { erpId: { $in: erpIds } },
    { projection: {
      erpId: 1, serie: 1, folio: 1, serieExterna: 1, folioExterno: 1,
      total: 1, saldoActual: 1, concepto: 1, tipoPago: 1, tipoMovimiento: 1,
      fechaCreacion: 1, fechaAfectacion: 1, fechaRealPago: 1, movimientos: 1,
    } },
  ).toArray();

  return docs.map(doc => ({
    ...doc,
    movimientos: ordenarKardex(doc.movimientos ?? []).map(m => ({
      ...m,
      esPagoBancario: SERIES_CON_AUTH.includes(m.serie),
    })),
  }));
}

// Series que en documentosRelacionados marcan el TIPO de Nota de Crédito
// (Bonificación/Devolución/Cargo a Cliente), no una referencia a documento.
const TIPO_MARCADORES = ['BON', 'BCT', 'DEV', 'CAC'];

// Clasifica una Nota de Crédito (Egreso) relacionada usando la misma convención
// de Serie en documentosRelacionados que usa el motor de balanza (BCT/BON/DEV/CAC).
function clasificarEgreso(doc) {
  const marcador = (doc.documentosRelacionados ?? []).find(d => TIPO_MARCADORES.includes((d.Serie ?? '').toUpperCase()));
  const serieRel = (marcador?.Serie ?? '').toUpperCase();
  if (serieRel === 'BCT') return 'Bonificación Club Tuberos';
  if (serieRel === 'BON') return 'Bonificación';
  if (serieRel === 'DEV') return 'Devolución';
  if (serieRel === 'CAC') return 'Cargo a Cliente';
  return 'Nota de Crédito';
}

// documentosRelacionados suele traer DOS entradas: una marca el TIPO (BON/BCT/
// DEV/CAC) y la otra es la referencia real a la CxC específica que afectó, vía
// su serieExterna/folioExterno — verificado contra datos reales: en los casos
// probados coincide exacto con el erpId correcto, incluso cuando la factura
// tiene decenas de registros de CxC (Facturas Globales). No siempre está
// presente (ej. algunas Bonificación Club Tuberos no la traen).
function extraerReferenciaCxc(doc) {
  const ref = (doc.documentosRelacionados ?? []).find(d =>
    !TIPO_MARCADORES.includes((d.Serie ?? '').toUpperCase()) && d.Folio,
  );
  return ref ? { serie: ref.Serie ?? null, folio: ref.Folio ?? null } : null;
}

// Clasifica una Nota de Crédito (Egreso) solo en Bonificación o Devolución
// (para la columna "Tipo NC" del reporte Pagos Asociados). Bonificación Club
// Tuberos se pliega dentro de "Bonificación" — el reporte solo distingue esas
// dos categorías. Cargo a Cliente y NCs sin marcador reconocido se ignoran
// (no cuentan para esta columna, a diferencia de clasificarEgreso()).
function clasificarBonificacionODevolucion(doc) {
  const marcador = (doc.documentosRelacionados ?? []).find(d => TIPO_MARCADORES.includes((d.Serie ?? '').toUpperCase()));
  const serieRel = (marcador?.Serie ?? '').toUpperCase();
  if (serieRel === 'BON' || serieRel === 'BCT') return 'Bonificación';
  if (serieRel === 'DEV') return 'Devolución';
  return null;
}

// Busca, en UNA sola consulta, las Notas de Crédito vigentes (Bonificación o
// Devolución) relacionadas a un lote de facturas — usado en pagosBanco /
// pagosBancoExport para no hacer N+1 queries (hasta 50,000 filas). Devuelve
// un Map facturaUuid -> { tipos: Set<string>, monto: number (suma, positiva) }.
async function buscarNotasCreditoPorFacturasBatch(facturaUuids) {
  const uuidsUnicos = [...new Set(facturaUuids.filter(Boolean))];
  if (!uuidsUnicos.length) return new Map();

  const docs = await CFDI.find(
    { tipoDeComprobante: 'E', isActive: true, satStatus: 'Vigente', 'cfdiRelacionados.uuids': { $in: uuidsUnicos } },
    'uuid source cfdiRelacionados documentosRelacionados total',
  ).lean();

  // Preferir la copia ERP si hay duplicado del mismo UUID (mismo criterio que
  // buscarEgresosRelacionados: la copia ERP trae documentosRelacionados).
  const porUuid = new Map();
  for (const doc of docs) {
    const actual = porUuid.get(doc.uuid);
    if (!actual || (doc.source === 'ERP' && actual.source !== 'ERP')) porUuid.set(doc.uuid, doc);
  }

  const uuidsSet = new Set(uuidsUnicos);
  const porFactura = new Map();

  for (const nc of porUuid.values()) {
    const tipo = clasificarBonificacionODevolucion(nc);
    if (!tipo) continue;

    const facturasRelacionadas = (nc.cfdiRelacionados ?? [])
      .flatMap(r => r.uuids ?? [])
      .filter(u => uuidsSet.has(u));

    for (const facturaUuid of facturasRelacionadas) {
      const acc = porFactura.get(facturaUuid) ?? { tipos: new Set(), monto: 0 };
      acc.tipos.add(tipo);
      acc.monto += Math.abs(nc.total ?? 0);
      porFactura.set(facturaUuid, acc);
    }
  }

  return porFactura;
}

// Notas de Crédito (CFDI tipo E) relacionadas a una factura por UUID (relación
// fiscal estándar cfdiRelacionados, tipoRelacion='01'). Es la fuente confiable
// para saber si a una factura se le aplicó una bonificación/descuento/devolución
// documentada — a diferencia del kardex interno de la CxC, que no siempre tiene
// una Nota de Crédito 1 a 1. Solo lectura sobre `cfdis`, no toca el módulo de bancos.
async function buscarEgresosRelacionados(facturaUuid) {
  if (!facturaUuid) return [];
  const docs = await CFDI.find(
    { tipoDeComprobante: 'E', isActive: true, 'cfdiRelacionados.uuids': facturaUuid },
    'uuid satStatus source serie folio fecha total documentosRelacionados',
  ).lean();

  // Puede haber copia SAT y ERP del mismo UUID; se prefiere la de ERP porque
  // trae documentosRelacionados (necesario para clasificar y para cxcRef).
  const porUuid = new Map();
  for (const doc of docs) {
    const actual = porUuid.get(doc.uuid);
    if (!actual || (doc.source === 'ERP' && actual.source !== 'ERP')) porUuid.set(doc.uuid, doc);
  }

  return [...porUuid.values()].map(doc => ({
    uuid:       doc.uuid,
    satStatus:  doc.satStatus ?? null,
    serie:      doc.serie ?? null,
    folio:      doc.folio ?? null,
    fecha:      doc.fecha ?? null,
    total:      doc.total ?? null,
    tipo:       clasificarEgreso(doc),
    cxcRef:     extraerReferenciaCxc(doc),
  }));
}

// Tolerancia de redondeo al comparar el total de una Nota de Crédito contra
// un monto del kardex de la CxC (se han visto diferencias de 1 centavo).
const TOLERANCIA_NC_MXN = 1.00;

// RFC genérico del SAT para "Público en General" — casi siempre acompaña a una
// Factura Global, pero el indicador OFICIAL es el nodo InformacionGlobal del
// propio CFDI (obligatorio en CFDI 4.0 cuando aplica). Se checan los dos: el
// RFC cubre CFDIs 3.3 o con datos incompletos donde InformacionGlobal no llegó
// a guardarse; informacionGlobal cubre el caso (raro) de un RFC distinto.
const RFC_PUBLICO_GENERAL = 'XAXX010101000';

function esFacturaGlobal(factura) {
  return factura?.receptor?.rfc === RFC_PUBLICO_GENERAL || !!factura?.informacionGlobal?.mes;
}

// Cruza el kardex de cada CxC con las Notas de Crédito (Egresos) relacionadas
// a la factura, SOLO si están Vigentes ante el SAT. Dos niveles de confianza:
//
//  'exacta'   — la NC trae en documentosRelacionados una referencia real a la
//               serieExterna/folioExterno de ESTA CxC específica (cxcRef). No es
//               una suposición: es una llave que coincidió en todos los casos
//               probados contra datos reales, incluso en Facturas Globales con
//               decenas de registros de CxC. Se usa siempre que esté disponible.
//  'inferida' — la NC no trae esa referencia (ej. algunas Bonificación Club
//               Tuberos), así que solo se puede intentar por coincidencia de
//               monto (±$1). Esto SÍ es una suposición, y por eso se desactiva
//               en Facturas Globales: ahí el mismo monto podría pertenecerle a
//               cualquiera de los otros clientes que comparten folioFiscal.
//
// En ambos casos, si el kardex ya tiene un movimiento "sin depósito" del monto
// correspondiente se anota sobre él; si no existe (el ERP aún no lo registró),
// se agrega un movimiento "virtual" marcado como tal. Nunca se modifica el
// saldoActual real del ERP — solo se anota/explica.
function enriquecerConNotasDeCredito(cuentasPorCobrar, egresosRelacionados, factura, movimientosBanco = []) {
  const vigentes = (egresosRelacionados ?? []).filter(e => e.satStatus === 'Vigente');

  const conReferencia = vigentes.filter(nc => nc.cxcRef);
  const sinReferencia = esFacturaGlobal(factura) ? [] : vigentes.filter(nc => !nc.cxcRef);

  return cuentasPorCobrar.map(cxc => {
    const usadas = new Set();
    const exactasDeEstaCxc = conReferencia.filter(nc =>
      nc.cxcRef.serie === cxc.serieExterna && nc.cxcRef.folio === cxc.folioExterno,
    );

    const anotar = (nc, confianza) => {
      usadas.add(nc.uuid);
      return { uuid: nc.uuid, serie: nc.serie, folio: nc.folio, tipo: nc.tipo, confianza };
    };

    let movimientos = (cxc.movimientos ?? []).map(m => {
      if (m.esPagoBancario) return m;
      const montoCoincide = (nc) => !usadas.has(nc.uuid) &&
        Math.abs(Math.abs(m.total ?? 0) - Math.abs(nc.total ?? 0)) <= TOLERANCIA_NC_MXN;

      const exacta = exactasDeEstaCxc.find(montoCoincide);
      if (exacta) return { ...m, notaCredito: anotar(exacta, 'exacta') };

      const inferida = sinReferencia.find(montoCoincide);
      if (inferida) return { ...m, notaCredito: anotar(inferida, 'inferida') };

      return m;
    });

    // Pagos bancarios (ABO/CBT/CPF/CFC) que Kore ya confirmó al conciliar el
    // depósito (bank_movements.erpLinks.movimientosKore) pero que el kardex
    // del ERP todavía no registra — mismo patrón que las NC virtuales de abajo:
    // se agregan como movimiento "virtual" sin tocar el saldoActual real.
    // Solo series con autorización bancaria real; las de Nota de Crédito
    // (RET/BON/etc.) ya se cubren mediante egresosRelacionados arriba.
    const yaEnKardex = new Set(movimientos.map(m => `${m.serie}-${m.folio}`));
    const koreDeEstaCxc = movimientosBanco
      .flatMap(bm => bm.erpLinks ?? [])
      .find(l => l.erpId === cxc.erpId)
      ?.movimientosKore ?? [];
    for (const mk of koreDeEstaCxc) {
      if (!SERIES_CON_AUTH.includes(mk.serie)) continue;
      if (yaEnKardex.has(`${mk.serie}-${mk.folio}`)) continue;
      movimientos = [...movimientos, {
        serie: mk.serie, folio: mk.folio,
        serieOrigen: mk.serieOrigen ?? null, folioOrigen: mk.folioOrigen ?? null,
        saldoAnterior: mk.saldoAnterior ?? null, saldoActual: mk.saldoActual ?? null,
        subtotal: mk.subtotal ?? null, impuesto: mk.impuesto ?? null, total: mk.total ?? 0,
        esPagoBancario: true, esVirtual: true,
      }];
    }

    // NC con referencia exacta a esta CxC que no calzó con ningún movimiento
    // existente del kardex — se agrega como movimiento virtual igual, porque
    // SÍ sabemos con certeza que pertenece aquí (solo falta que el ERP la registre).
    for (const nc of exactasDeEstaCxc) {
      if (usadas.has(nc.uuid)) continue;
      movimientos = [...movimientos, {
        serie: nc.serie, folio: nc.folio, serieOrigen: null, folioOrigen: null,
        saldoAnterior: null, saldoActual: null, subtotal: null, impuesto: null,
        total: -Math.abs(nc.total ?? 0),
        esPagoBancario: false, esVirtual: true,
        notaCredito: anotar(nc, 'exacta'),
      }];
    }

    // Fallback por monto (solo no-Global): si queda saldo pendiente sin explicar
    // y una NC sin referencia coincide con ese monto exacto.
    const saldoPendiente = cxc.saldoActual ?? 0;
    const ncRestanteInferida = saldoPendiente > 0
      ? sinReferencia.find(nc => !usadas.has(nc.uuid) && Math.abs(saldoPendiente - Math.abs(nc.total ?? 0)) <= TOLERANCIA_NC_MXN)
      : null;
    if (ncRestanteInferida) {
      movimientos = [...movimientos, {
        serie: ncRestanteInferida.serie, folio: ncRestanteInferida.folio,
        serieOrigen: null, folioOrigen: null,
        saldoAnterior: saldoPendiente, saldoActual: 0, subtotal: null, impuesto: null,
        total: -Math.abs(ncRestanteInferida.total ?? 0),
        esPagoBancario: false, esVirtual: true,
        notaCredito: anotar(ncRestanteInferida, 'inferida'),
      }];
    }

    return { ...cxc, movimientos };
  });
}

const getCacheKey = (query) => JSON.stringify(query);

const getFromCache = (key) => {
  const entry = dashboardCache.get(key);
  if (!entry) return null;
  if (Date.now() > entry.expiresAt) { dashboardCache.delete(key); return null; }
  return entry.data;
};

const setCache = (key, data) => {
  dashboardCache.set(key, { data, expiresAt: Date.now() + DASHBOARD_CACHE_TTL_MS });
};

/** Limpia todo el caché del dashboard — llamar tras una comparación batch. */
const clearDashboardCache = () => dashboardCache.clear();

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
  const cacheKey = getCacheKey(req.query);
  const cached = getFromCache(cacheKey);
  if (cached) return res.json(cached);

  const { rfcEmisor, fechaInicio, fechaFin, ejercicio, periodo, tipoDeComprobante } = req.query;

  // El dashboard solo debe contar Emitidos — nunca Recibidos (aunque compartan
  // source SAT/MANUAL/ERP). Si el frontend no manda rfcEmisor (ej. mientras
  // carga la entidad activa), se restringe a las RFC de las entidades propias
  // en vez de dejar el filtro abierto a cualquier emisor.
  let emisorConstraint = rfcEmisor ? rfcEmisor.toUpperCase() : null;
  if (!emisorConstraint) {
    const entidadesRfcs = (await entityRepo.findAll()).map(e => e.rfc?.toUpperCase()).filter(Boolean);
    emisorConstraint = { $in: entidadesRfcs };
  }

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
  else                   periodoFilter.tipoDeComprobante = { $ne: 'N' }; // nómina no suma en "todos los tipos"

  // Filtro base para KPIs de conciliación (solo ERP activos, sin cancelados ni deshabilitados)
  // Debe coincidir con los mismos criterios que countERP del aggregate de montos.
  // ERP: se filtra por erpStatus (estado en el origen), no satStatus
  const cfdiFilter = { isActive: true, source: 'ERP', erpStatus: { $nin: ['Cancelado', 'Deshabilitado', 'Cancelacion Pendiente'] }, uuid: { $not: /^SINUUID/ }, 'emisor.rfc': emisorConstraint, ...periodoFilter };
  if (Object.keys(dateFilter).length) cfdiFilter.fecha = dateFilter;

  // Filtro para IVA y tipos: todos los CFDIs activos (ERP + SAT + MANUAL)
  const baseFilter = { isActive: true, 'emisor.rfc': emisorConstraint, ...periodoFilter };
  if (Object.keys(dateFilter).length) baseFilter.fecha = dateFilter;

  // Filtro para montos: ERP, SAT y MANUAL (MANUAL = XMLs del portal SAT subidos manualmente).
  // Se usa { isActive: { $ne: false } } en vez de { isActive: true } porque
  // aggregate() no aplica Mongoose type-casting y documentos con isActive=null
  // o isActive=1 no serían encontrados con la comparación estricta boolean.
  // MANUAL se agrupa junto con SAT (igual que en comparisonEngine) para que el
  // total SAT del dashboard refleje todos los documentos del lado SAT.
  const montosFilter = { isActive: { $ne: false }, source: { $in: ['ERP', 'SAT', 'MANUAL'] }, 'emisor.rfc': emisorConstraint, ...periodoFilter };
  if (Object.keys(dateFilter).length) montosFilter.fecha = dateFilter;

  // Filtro para CFDIs SAT/MANUAL que no tienen contraparte ERP
  const satSoloFilter = { isActive: { $ne: false }, source: { $in: ['SAT', 'MANUAL'] }, lastComparisonStatus: 'not_in_erp', 'emisor.rfc': emisorConstraint, ...periodoFilter };
  if (Object.keys(dateFilter).length) satSoloFilter.fecha = dateFilter;

  // Cancelados ERP: solo los que tienen erpStatus = 'Cancelado'
  const canceladosFilter = { source: 'ERP', erpStatus: 'Cancelado', 'emisor.rfc': emisorConstraint, ...periodoFilter };
  if (Object.keys(dateFilter).length) canceladosFilter.fecha = dateFilter;

  // Vigentes en SAT que también están en ERP
  const vigenteErpSatFilter = { isActive: true, source: 'ERP', satStatus: 'Vigente', uuid: { $not: /^SINUUID/ }, 'emisor.rfc': emisorConstraint, ...periodoFilter };
  if (Object.keys(dateFilter).length) vigenteErpSatFilter.fecha = dateFilter;

  const [
    totalCFDIs, conciliados, conDiscrepancia, sinConciliar, notInErp, erpCanceladosCount,
    notInSat, cancelledMatch,
    vigenteErpSatCount,
    montosAggregate, cfdisBySatStatus, comparisonStats,
    discrepancyStats, topDiscrepancyTypes, recentDiscrepancies,
    ivaAggregate, ivaByTipoAggregate,
  ] = await Promise.all([
    // Total: solo ERP activos válidos + SAT/MANUAL activos válidos (sin deshabilitados ni SINUUID)
    CFDI.countDocuments({ isActive: { $ne: false }, source: { $in: ['ERP', 'SAT', 'MANUAL'] }, uuid: { $not: /^SINUUID/ }, satStatus: { $nin: ['Deshabilitado'] }, erpStatus: { $nin: ['Deshabilitado'] }, 'emisor.rfc': emisorConstraint, ...periodoFilter, ...(Object.keys(dateFilter).length && { fecha: dateFilter }) }),
    CFDI.countDocuments({ ...cfdiFilter, lastComparisonStatus: { $in: ['match', 'conciliado'] } }),
    CFDI.countDocuments({ ...cfdiFilter, lastComparisonStatus: { $in: ['discrepancy', 'warning'] } }),
    CFDI.countDocuments({ ...cfdiFilter, lastComparisonStatus: { $in: [null, 'error', 'pending'] } }),
    CFDI.countDocuments(satSoloFilter),
    CFDI.countDocuments(canceladosFilter),
    // ERP CFDIs not found in SAT (distinct from SAT-only)
    CFDI.countDocuments({ ...cfdiFilter, lastComparisonStatus: 'not_in_sat' }),
    // ERP CFDIs marked cancelled/coincide (cancelled in ERP but found in SAT)
    CFDI.countDocuments({ ...cfdiFilter, lastComparisonStatus: 'cancelled' }),
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
        esSinUuid: { $regexMatch: { input: { $ifNull: ['$uuid', ''] }, regex: '^SINUUID', options: 'i' } },
        // ERP: (Timbrado o Habilitado) → activo; SAT/MANUAL: solo Vigente
        excluir: { $cond: {
          if:   { $eq: ['$source', 'ERP'] },
          then: { $not: [{ $in: ['$erpStatus', ['Timbrado', 'Habilitado']] }] },
          else: { $ne: ['$satStatus', 'Vigente'] },
        }},
      }},
      { $group: {
        _id:             '$sourceGroup',
        // Activos: Timbrado/Habilitado y con UUID real
        total:           { $sum: { $cond: [{ $or: ['$excluir', '$esSinUuid'] }, 0, MONTO_EFECTIVO_EXPR] } },
        count:           { $sum: { $cond: [{ $or: ['$excluir', '$esSinUuid'] }, 0, 1] } },
        // Cancelados/no-vigentes: excluidos pero con UUID real
        totalCancelados: { $sum: { $cond: [{ $and: ['$excluir', { $not: ['$esSinUuid'] }] }, MONTO_EFECTIVO_EXPR, 0] } },
        countCancelados: { $sum: { $cond: [{ $and: ['$excluir', { $not: ['$esSinUuid'] }] }, 1, 0] } },
        // Sin UUID real (SINUUID): bucket propio, no afecta totales de conciliación
        totalSinUuid:    { $sum: { $cond: ['$esSinUuid', MONTO_EFECTIVO_EXPR, 0] } },
        countSinUuid:    { $sum: { $cond: ['$esSinUuid', 1, 0] } },
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
    // IMPORTANTE: filtrar source='ERP' para evitar contar doble — el engine escribe
    // lastComparisonStatus en AMBOS documentos (ERP y SAT) del mismo UUID.
    CFDI.aggregate([
      { $match: { source: 'ERP', isActive: { $ne: false }, uuid: { $not: /^SINUUID/ }, ...(periodoFilter.ejercicio && { ejercicio: periodoFilter.ejercicio }), ...(periodoFilter.periodo && { periodo: periodoFilter.periodo }), ...(periodoFilter.tipoDeComprobante && { tipoDeComprobante: periodoFilter.tipoDeComprobante }) } },
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
      { $match: { isActive: { $ne: false }, source: { $in: ['ERP', 'SAT', 'MANUAL'] }, 'emisor.rfc': emisorConstraint, ...periodoFilter,
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
      { $match: { isActive: { $ne: false }, source: { $in: ['ERP', 'SAT', 'MANUAL'] }, 'emisor.rfc': emisorConstraint, ...periodoFilter,
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

  const responseData = {
    kpis: {
      totalCFDIs, conciliados, conDiscrepancia, sinConciliar, notInErp, erpCanceladosCount,
      notInSat, cancelledMatch,
      vigenteErpSat: { count: vigenteErpSatRow.count, total: vigenteErpSatRow.total },
      totalERP, totalSAT, diferencia: totalERP - totalSAT,
      countERP, countSAT,
      // Cancelados y deshabilitados separados del total principal
      erpCancelados: { total: erpRow.totalCancelados, count: erpRow.countCancelados },
      satCancelados: { total: satRow.totalCancelados, count: satRow.countCancelados },
      // CFDIs ERP sin UUID real (SINUUID-…): excluidos de conciliación y de cancelados
      erpSinUuid: { total: erpRow.totalSinUuid ?? 0, count: erpRow.countSinUuid ?? 0 },
      cfdisBySatStatus, comparisonStats, discrepancyStats,
      ivaStats,
    },
    topDiscrepancyTypes,
    recentDiscrepancies,
  };

  setCache(cacheKey, responseData);
  res.json(responseData);
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

  const erpProjection = { uuid: 1, serie: 1, folio: 1, fecha: 1, total: 1, tipoDeComprobante: 1, emisor: 1, receptor: 1, erpStatus: 1, satStatus: 1, periodo: 1 };
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
    // Excluir CFDIs cuyo periodo real en MongoDB no coincida con el periodo solicitado.
    // Caso típico: ERP marca CFDI como "global" (pertenece al siguiente periodo contable)
    // y la comparación se registró con ese periodo nuevo, pero el documento ERP en MongoDB
    // tiene el periodo original → aparecería en el periodo incorrecto.
    // Si el CFDI tiene periodo incorrecto en MongoDB, seguirá visible en su propio mes.
    ...(periodo ? [{ $match: { $or: [{ erpCfdiId: null }, { 'erpCfdiId.periodo': parseInt(periodo) }] } }] : []),
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

  const [detalleAgg, totalPagos] = await Promise.all([
    CFDI.aggregate([
      { $match: matchFilter },
      { $unwind: { path: '$complementoPago.pagos', preserveNullAndEmptyArrays: false } },
      { $unwind: { path: '$complementoPago.pagos.doctosRelacionados', preserveNullAndEmptyArrays: false } },
      {
        $project: {
          _id:           0,
          uuidPago:      { $toUpper: { $ifNull: ['$uuid', ''] } },
          seriePago:     '$serie',
          folioPago:     '$folio',
          fechaPago:     '$complementoPago.pagos.fechaPago',
          metodoPago:    '$complementoPago.pagos.formaDePagoP',
          numOperacion:  '$complementoPago.pagos.numOperacion',
          montoPago:     '$complementoPago.pagos.monto',
          idDocumento:   { $toUpper: { $ifNull: ['$complementoPago.pagos.doctosRelacionados.idDocumento', ''] } },
          serieOrigen:   '$complementoPago.pagos.doctosRelacionados.serie',
          folioOrigen:   '$complementoPago.pagos.doctosRelacionados.folio',
          importePagado: '$complementoPago.pagos.doctosRelacionados.impSaldoInsoluto',
        },
      },
    ]),
    CFDI.countDocuments(matchFilter),
  ]);

  if (!detalleAgg.length) {
    return res.json({ totalPagos, totalDoctos: 0, existenEnSistema: 0, noExistenEnSistema: 0, porcentajeCobertura: 100, pagos: [] });
  }

  // Verificar cuáles documentos origen existen en el sistema
  const doctosIds = [...new Set(detalleAgg.map(r => r.idDocumento).filter(id => id && id.length > 0))];
  const totalDoctos = doctosIds.length;

  const cfdiOrigen = await CFDI.find(
    { uuid: { $in: doctosIds }, isActive: { $ne: false } },
    { uuid: 1, serie: 1, folio: 1, total: 1, satStatus: 1, _id: 0 },
  ).lean();

  const cfdiOrigenMap = Object.fromEntries(cfdiOrigen.map(c => [c.uuid?.toUpperCase(), c]));
  const existenEnSistema = cfdiOrigen.length;

  // Enriquecer cada fila con datos del CFDI origen si existe
  const pagos = detalleAgg.map(r => {
    const origen = cfdiOrigenMap[r.idDocumento] ?? null;
    return {
      uuidPago:       r.uuidPago      || null,
      seriePago:      r.seriePago     || null,
      folioPago:      r.folioPago     || null,
      fechaPago:      r.fechaPago     || null,
      metodoPago:     r.metodoPago    || null,
      numOperacion:   r.numOperacion  || null,
      montoPago:      r.montoPago     != null ? Number(r.montoPago)     : null,
      idDocumento:    r.idDocumento   || null,
      serieOrigen:    r.serieOrigen   || origen?.serie  || null,
      folioOrigen:    r.folioOrigen   || origen?.folio  || null,
      importePagado:  r.importePagado != null ? Number(r.importePagado) : null,
      enSistema:      origen != null,
      satStatusOrigen: origen?.satStatus ?? null,
    };
  });

  res.json({
    totalPagos,
    totalDoctos,
    existenEnSistema,
    noExistenEnSistema: totalDoctos - existenEnSistema,
    porcentajeCobertura: totalDoctos > 0 ? Math.round((existenEnSistema / totalDoctos) * 100) : 100,
    pagos,
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
  const COMP_LABEL    = { match: 'Conciliado', conciliado: 'Conciliado manualmente', discrepancy: 'Discrepancia', warning: 'Advertencia', not_in_sat: 'No en SAT', not_in_erp: 'No en ERP', cancelled: 'Cancelado', pending: 'Pendiente', error: 'Error' };
  const CAMPO_LABEL   = { 'total': 'Total', 'subTotal': 'Subtotal', 'impuestos.totalImpuestosTrasladados': 'IVA Trasladado', 'impuestos.totalImpuestosRetenidos': 'IVA Retenido', 'emisor.rfc': 'RFC Emisor', 'receptor.rfc': 'RFC Receptor', 'fecha': 'Fecha', 'tipoDeComprobante': 'Tipo', 'moneda': 'Moneda', 'tipoCambio': 'Tipo Cambio' };
  const MESES         = ['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'];
  const periodoLabel  = ejercicio ? (periodo ? `${MESES[parseInt(periodo)-1]} ${ejercicio}` : `Año ${ejercicio}`) : 'Todos los periodos';

  // ── FASE 1: Datos que no dependen de los UUIDs ERP ───────────────────────
  const [allErpCfdis, erpCancelados, erpDeshabilitados, erpInactivoSatVigente, allSatForPeriod, resumenTipos, resumenSAT, cfdisMigrados, sinUuidCfdis] = await Promise.all([

    // CFDIs ERP Timbrados/Habilitados — igual que montosAggregate del dashboard
    CFDI.find({ source: 'ERP', isActive: { $ne: false }, erpStatus: { $in: ['Timbrado', 'Habilitado'] }, uuid: { $not: /^SINUUID/ }, ...periodoFilter })
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
        conciliados:     { $sum: { $cond: [{ $in: ['$lastComparisonStatus', ['match', 'conciliado']] }, 1, 0] } },
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

    // CFDIs ERP sin timbrar (uuid SINUUID-*): existen en ERP pero sin UUID fiscal real
    CFDI.find({ source: 'ERP', isActive: { $ne: false }, erpStatus: { $in: ['Timbrado', 'Habilitado'] }, uuid: /^SINUUID/, ...periodoFilter })
      .select('uuid serie folio tipoDeComprobante fecha emisor receptor subTotal descuento impuestos total moneda erpStatus satStatus lastComparisonStatus ejercicio periodo')
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
        .select('uuid total subTotal descuento impuestos satStatus tipoDeComprobante complementoPago.totales.montoTotalPagos complementoPago.totales.totalTrasladosImpuestoIVA16 complementoPago.totales.totalTrasladosImpuestoIVA8 complementoPago.totales.totalRetencionesImpuestoIVA complementoPago.pagos.monto').lean()
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

      const esP = cfdi.tipoDeComprobante === 'P';

      // Monto efectivo de un CFDI tipo P: SAT guarda total=0 por spec, el real está en complementoPago
      const montoEfectivoCP = (c) =>
        c?.complementoPago?.totales?.montoTotalPagos
        ?? (c?.complementoPago?.pagos ?? []).reduce((s, p) => s + (Number(p.monto) || 0), 0)
        ?? 0;

      const descuentoERP = cfdi.descuento || 0;
      const totERP    = esP
        ? (cfdi.total > 0 ? cfdi.total : montoEfectivoCP(cfdi))
        : cfdi.total || 0;
      const subERP    = esP ? totERP : (cfdi.subTotal || 0) - descuentoERP;
      // IVA ERP: para todos los tipos (incluyendo P) está en impuestos.totalImpuestosTrasladados
      const ivaTraERP = cfdi.impuestos?.totalImpuestosTrasladados || 0;
      const ivaRetERP = cfdi.impuestos?.totalImpuestosRetenidos || 0;

      const descuentoSAT = satCfdi ? (satCfdi.descuento || 0) : null;
      const satTotRaw    = satCfdi ? (satCfdi.total || 0) : null;
      const satMontoP    = satCfdi ? montoEfectivoCP(satCfdi) : 0;
      const totSAT       = satCfdi
        ? (esP && satTotRaw === 0 ? satMontoP : satTotRaw)
        : null;
      const subSAT       = satCfdi
        ? (esP ? totSAT : (satCfdi.subTotal || 0) - (satCfdi.descuento || 0))
        : null;
      // Para tipo P SAT: el IVA se guarda en impuestos.totalImpuestosTrasladados (mismo que otros tipos)
      // y también en complementoPago.totales.totalTrasladosImpuestoIVA16 — usamos lo que tenga valor.
      const ivaTraSAT = satCfdi
        ? (
            (satCfdi.impuestos?.totalImpuestosTrasladados ?? 0)
            || (satCfdi.complementoPago?.totales?.totalTrasladosImpuestoIVA16 ?? 0)
            + (satCfdi.complementoPago?.totales?.totalTrasladosImpuestoIVA8 ?? 0)
          )
        : null;
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

      const cs            = cfdi.lastComparisonStatus;
      const absDif        = dif !== null ? Math.abs(dif) : 0;
      const hasDif        = dif !== null && absDif > 0.01;
      const isCentavos    = hasDif && absDif < 1.00;   // diferencia de centavos → amarillo
      const hasDifSignif  = hasDif && !isCentavos;     // diferencia significativa (≥ $1) → rojo
      const hasCritical   = discs.some(d => d.severity === 'critical' || d.severity === 'high');
      const sinSatVigente = !satCfdi;
      const FG_REVIEW     = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFD6CC' } };

      // Color base de la fila completa
      if (cfdi.satStatus === 'Cancelado') {
        row.eachCell({ includeEmpty: true }, cell => { cell.fill = FG_DANGER; });
      } else if (hasDifSignif || hasCritical) {
        // Diferencia ≥ $1 o discrepancia crítica → rojo
        row.eachCell({ includeEmpty: true }, cell => { cell.fill = FG_DANGER; });
      } else if (isCentavos) {
        // Diferencia de centavos (< $1.00) → amarillo
        row.eachCell({ includeEmpty: true }, cell => { cell.fill = FG_WARN; });
      } else if (sinSatVigente) {
        row.eachCell({ includeEmpty: true }, cell => { cell.fill = FG_REVIEW; });
      } else if (cs === 'discrepancy' || cs === 'warning' || discs.length > 0) {
        // Discrepancia de campo sin diferencia de monto → amarillo
        row.eachCell({ includeEmpty: true }, cell => { cell.fill = FG_WARN; });
      } else if (cs === 'match' || cs === 'conciliado') {
        row.getCell('conciliacion').fill = FG_OK;
      }

      // Resaltar celdas individuales con diferencia (encima del color de fila)
      // Centavos → naranja suave en celda; diferencia significativa → rojo intenso
      const FG_DIFF      = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFDC2626' } };
      const FG_DIFF_CENT = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFC107' } }; // ámbar para centavos
      if (subSAT !== null && Math.abs(subERP - subSAT) > 0.01) {
        const fg = Math.abs(subERP - subSAT) < 1.00 ? FG_DIFF_CENT : FG_DIFF;
        row.getCell('subERP').fill = fg; row.getCell('subSAT').fill = fg;
      }
      if (ivaTraSAT !== null && Math.abs(ivaTraERP - ivaTraSAT) > 0.01) {
        const fg = Math.abs(ivaTraERP - ivaTraSAT) < 1.00 ? FG_DIFF_CENT : FG_DIFF;
        row.getCell('ivaTraERP').fill = fg; row.getCell('ivaTraSAT').fill = fg;
      }
      if (totSAT !== null && Math.abs(totERP - totSAT) > 0.01) {
        const fg = Math.abs(totERP - totSAT) < 1.00 ? FG_DIFF_CENT : FG_DIFF;
        row.getCell('totalERP').fill = fg; row.getCell('totalSAT').fill = fg;
        row.getCell('diferencia').fill = fg;
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
        const satCfdi   = satByUuidMap[(cfdi.uuid || '').toUpperCase()] || null;
        const esPInact  = cfdi.tipoDeComprobante === 'P';
        const montoCP   = (c) =>
          c?.complementoPago?.totales?.montoTotalPagos
          ?? (c?.complementoPago?.pagos ?? []).reduce((s, p) => s + (Number(p.monto) || 0), 0)
          ?? 0;
        const totERPI   = esPInact
          ? (cfdi.total > 0 ? cfdi.total : montoCP(cfdi))
          : cfdi.total || 0;
        const subERPI   = esPInact ? totERPI : (cfdi.subTotal || 0) - (cfdi.descuento || 0);
        const ivaTraERPI = cfdi.impuestos?.totalImpuestosTrasladados || 0;
        const satTotRawI = satCfdi ? (satCfdi.total || 0) : null;
        const totSATI   = satCfdi
          ? (esPInact && satTotRawI === 0 ? montoCP(satCfdi) : satTotRawI)
          : null;
        const subSATI   = satCfdi
          ? (esPInact ? totSATI : (satCfdi.subTotal || 0) - (satCfdi.descuento || 0))
          : null;
        const ivaTraSATI = satCfdi
          ? (
              (satCfdi.impuestos?.totalImpuestosTrasladados ?? 0)
              || (satCfdi.complementoPago?.totales?.totalTrasladosImpuestoIVA16 ?? 0)
              + (satCfdi.complementoPago?.totales?.totalTrasladosImpuestoIVA8 ?? 0)
            )
          : null;
        const dif = totSATI !== null ? fmtNum(totERPI - totSATI) : null;
        const row = sheet.addRow({
          uuid: cfdi.uuid, serie: cfdi.serie || '', folio: cfdi.folio || '',
          fecha: cfdi.fecha ? new Date(cfdi.fecha).toLocaleDateString('es-MX') : '',
          rfcEmisor: cfdi.emisor?.rfc || '', nomEmisor: cfdi.emisor?.nombre || '',
          rfcReceptor: cfdi.receptor?.rfc || '', nomReceptor: cfdi.receptor?.nombre || '',
          descuento: cfdi.descuento || 0,
          subERP: subERPI,
          ivaTraERP: ivaTraERPI,
          ivaRetERP: cfdi.impuestos?.totalImpuestosRetenidos   || 0,
          totalERP: totERPI,
          descuentoSAT: satCfdi ? (satCfdi.descuento || 0) : null,
          subSAT: subSATI,
          ivaTraSAT: ivaTraSATI,
          totalSAT: totSATI,
          diferencia: dif,
          estadoERP: cfdi.erpStatus || '—',
          estadoSAT: cfdi.satStatus || '—',
          conciliacion: COMP_LABEL[cfdi.lastComparisonStatus] || cfdi.lastComparisonStatus || 'Sin comparar',
          tiposDisc: '', detalleDisc: '',
        });
        MONEY_KEYS.forEach(k => { if (row.getCell(k).value != null) row.getCell(k).numFmt = MXN; });
        // Centavos → color base heredado (fgColor) + ámbar en celda diferencia; diff grande → rojo
        if (dif !== null && Math.abs(dif) > 0.01) {
          const fg = Math.abs(dif) < 1.00
            ? { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFF3CD' } }
            : { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFDC2626' } };
          row.getCell('diferencia').fill = fg;
        }
        row.eachCell({ includeEmpty: true }, cell => { if (!cell.fill?.fgColor) cell.fill = fgColor; });
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
          .select('uuid total subTotal descuento impuestos satStatus tipoDeComprobante complementoPago.totales.montoTotalPagos complementoPago.totales.totalTrasladosImpuestoIVA16 complementoPago.totales.totalTrasladosImpuestoIVA8 complementoPago.totales.totalRetencionesImpuestoIVA complementoPago.pagos.monto').lean()
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
          .select('uuid total subTotal descuento impuestos satStatus tipoDeComprobante complementoPago.totales.montoTotalPagos complementoPago.totales.totalTrasladosImpuestoIVA16 complementoPago.totales.totalTrasladosImpuestoIVA8 complementoPago.totales.totalRetencionesImpuestoIVA complementoPago.pagos.monto').lean()
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

  // ══════════════════════════════════════════════════════════════════════════
  // HOJA — Pendientes de Timbrado (CFDIs sin UUID fiscal — SINUUID-*)
  // Excluidos de totales y conciliación; se muestran aquí para revisión
  // ══════════════════════════════════════════════════════════════════════════
  if (sinUuidCfdis.length > 0) {
    const FG_SIN = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFF8E1' } }; // amarillo suave
    const sSin  = workbook.addWorksheet(`${workbook.worksheets.length + 1}. Pendientes Timbrado`);
    sSin.views  = [{ state: 'frozen', ySplit: 4 }];
    const SIN_COLS = [
      { key: 'tipo',       header: 'Tipo',            width: 7  },
      { key: 'desc',       header: 'Descripción',     width: 11 },
      { key: 'serie',      header: 'Serie',           width: 8  },
      { key: 'folio',      header: 'Folio',           width: 10 },
      { key: 'fecha',      header: 'Fecha',           width: 12 },
      { key: 'rfcEmisor',  header: 'RFC Emisor',      width: 15 },
      { key: 'nomEmisor',  header: 'Nombre Emisor',   width: 30 },
      { key: 'rfcRec',     header: 'RFC Receptor',    width: 15 },
      { key: 'nomRec',     header: 'Nombre Receptor', width: 30 },
      { key: 'subTotal',   header: 'Subtotal',        width: 16 },
      { key: 'ivaTrasl',   header: 'IVA Trasladado',  width: 18 },
      { key: 'ivaRet',     header: 'IVA Retenido',    width: 18 },
      { key: 'total',      header: 'Total',           width: 16 },
      { key: 'estadoERP',  header: 'Estado ERP',      width: 16 },
      { key: 'nota',       header: 'Nota',            width: 35 },
    ];
    addTitle(sSin, `CFDIs Pendientes de Timbrado (Sin UUID Fiscal) — ${periodoLabel}`, SIN_COLS.length);

    sSin.mergeCells(`A3:${colLetter(SIN_COLS.length)}3`);
    const kpiSin = sSin.getCell('A3');
    kpiSin.value = `Total: ${sinUuidCfdis.length} CFDIs sin timbrar — NO incluidos en totales de conciliación`;
    kpiSin.font  = FONT_BOLD; kpiSin.fill = FG_SIN;
    kpiSin.alignment = { horizontal: 'left', vertical: 'middle' };
    sSin.getRow(3).height = 20;

    sSin.columns = SIN_COLS;
    const hdrSin = sSin.getRow(4);
    hdrSin.values = SIN_COLS.map(c => c.header);
    hdrSin.eachCell(c => { c.font = FONT_HDR; c.fill = FG_HDR; c.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true }; });
    hdrSin.height = 28;

    let sumSinTotal = 0;
    for (const c of sinUuidCfdis) {
      const row = sSin.addRow({
        tipo:      c.tipoDeComprobante || '',
        desc:      TIPO_LABEL[c.tipoDeComprobante] || '',
        serie:     c.serie  || '',
        folio:     c.folio  || '',
        fecha:     c.fecha  ? new Date(c.fecha).toLocaleDateString('es-MX') : '',
        rfcEmisor: c.emisor?.rfc    || '',
        nomEmisor: c.emisor?.nombre || '',
        rfcRec:    c.receptor?.rfc    || '',
        nomRec:    c.receptor?.nombre || '',
        subTotal:  (c.subTotal || 0) - (c.descuento || 0),
        ivaTrasl:  c.impuestos?.totalImpuestosTrasladados || 0,
        ivaRet:    c.impuestos?.totalImpuestosRetenidos   || 0,
        total:     c.total || 0,
        estadoERP: c.erpStatus || '—',
        nota:      'Pendiente de timbrado — sin UUID fiscal asignado',
      });
      ['subTotal','ivaTrasl','ivaRet','total'].forEach(k => { row.getCell(k).numFmt = MXN; });
      row.eachCell({ includeEmpty: true }, cell => { cell.fill = FG_SIN; });
      sumSinTotal += c.total || 0;
    }

    // Fila totales
    const trSin = sSin.addRow({ tipo: `TOTAL (${sinUuidCfdis.length})`, total: fmtNum(sumSinTotal) });
    trSin.eachCell(c => { c.font = FONT_BOLD; c.fill = FG_TOTAL; });
    trSin.getCell('total').numFmt = MXN;
  }

  // ── Respuesta ──────────────────────────────────────────────────────────────
  const filename = `conciliacion_${ejercicio || 'all'}_${periodo || 'all'}_${Date.now()}.xlsx`;
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  await workbook.xlsx.write(res);
  res.end();
});

// Un mismo CFDI de pago puede existir dos veces en Mongo: una vez importado
// de SAT (XML real, complementoPago correcto) y otra de ERP (fallback de
// `enrichComplementoPago`, que puede usar el total de la factura como
// impPagado cuando no conoce el desglose real de un pago que cubre varias
// facturas — ver esa función). Sin deduplicar, el reporte muestra AMBAS
// versiones como filas distintas con montos distintos para el mismo pago
// real. Se agrupa por `uuid` y se prefiere la versión SAT cuando existe.
const DEDUP_PAGO_PREFIERE_SAT = [
  { $addFields: { _srcOrden: { $cond: [{ $eq: ['$source', 'SAT'] }, 0, 1] } } },
  { $sort: { uuid: 1, _srcOrden: 1 } },
  { $group: { _id: '$uuid', doc: { $first: '$$ROOT' } } },
  { $replaceRoot: { newRoot: '$doc' } },
];

/**
 * "Saldo Banco": el saldo del depósito real de un movimiento bancario
 * conforme se va APLICANDO a las facturas que ese mismo depósito paga (vía
 * erpLinks.folioFiscal) — no un total fijo repetido en cada fila, sino un
 * saldo CORRIENTE que baja cronológicamente: cada aplicación (un pago-CFDI
 * + una factura) resta su `impPagado` del saldo que quedaba tras la
 * aplicación anterior. Reemplaza a "Saldo Movimiento" (que solo mostraba
 * `erpLinks.saldoActual`, el saldo de UNA CxC en el ERP, sin relación con
 * el depósito compartido). Confirmado con el usuario contra un caso real
 * donde un depósito de $7,365.02 se comparaba contra una sola factura de
 * $16,094.85 (marcando una "diferencia" que en realidad correspondía a
 * otras facturas pagadas por el mismo depósito), y que el saldo debe
 * "ir bajando" según se va ocupando en cada factura, no quedar fijo.
 *
 * No se puede calcular dentro del pipeline paginado de `pagosBanco` porque
 * las demás aplicaciones de un mismo depósito pueden caer en otra página —
 * se calcula aparte, sobre TODAS las aplicaciones de cada movimiento,
 * independiente de la paginación/filtros del reporte.
 *
 * @param {Array<string|null>} movimientoIds — _id (string) de bank_movements
 * @returns {Promise<Map<string, number>>} clave `${movimientoId}|${cfdiUuid}|${facturaUuid}`
 *   (mayúsculas) → saldo del depósito INMEDIATAMENTE DESPUÉS de esa aplicación.
 */
async function calcularSaldosBanco(movimientoIds) {
  const mapa = new Map();
  const ids = [...new Set(movimientoIds.filter(Boolean).map(String))];
  if (ids.length === 0) return mapa;

  const movimientos = await BankMovement.find(
    { _id: { $in: ids.map((id) => new mongoose.Types.ObjectId(id)) } },
    { deposito: 1, 'erpLinks.folioFiscal': 1, 'erpLinks.saldoActual': 1 },
  ).lean();

  // folioFiscal (mayúsculas) → [{movId, saldoActual}, ...] — una misma factura
  // puede haberse pagado con VARIOS depósitos distintos en fechas distintas
  // (ej. una parcialidad chica hoy, otra grande el mes que entra), cada uno
  // con su propio movimiento. saldoActual aquí es el monto de ESE depósito
  // específico aplicado a esa factura (ver comentario en pagosBanco) — se usa
  // más abajo para saber a CUÁL de los movimientos candidatos pertenece cada
  // aplicación real, en vez de metérsela a todos por compartir la factura.
  const folioAMovimientos = new Map();
  for (const m of movimientos) {
    for (const link of (m.erpLinks ?? [])) {
      const folio = (link.folioFiscal || '').toUpperCase();
      if (!folio) continue;
      if (!folioAMovimientos.has(folio)) folioAMovimientos.set(folio, []);
      folioAMovimientos.get(folio).push({ movId: String(m._id), saldoActual: Number(link.saldoActual) || 0 });
    }
  }

  const foliosUnicos = [...folioAMovimientos.keys()];
  if (foliosUnicos.length === 0) return mapa;

  // Traer TODAS las aplicaciones (un pago-CFDI + una factura) que tocan
  // cualquiera de esos folios, deduplicadas SAT/ERP, con su fecha real de
  // pago para poder ordenarlas cronológicamente y calcular el acumulado.
  const aplicaciones = await CFDI.aggregate([
    {
      $match: {
        tipoDeComprobante: 'P',
        isActive: true,
        'complementoPago.pagos.doctosRelacionados.idDocumento': {
          $in: foliosUnicos.map((f) => new RegExp(`^${f}$`, 'i')),
        },
      },
    },
    ...DEDUP_PAGO_PREFIERE_SAT,
    { $unwind: '$complementoPago.pagos' },
    { $unwind: '$complementoPago.pagos.doctosRelacionados' },
    {
      $project: {
        _id:         0,
        cfdiUuid:    { $toUpper: '$uuid' },
        facturaUuid: { $toUpper: '$complementoPago.pagos.doctosRelacionados.idDocumento' },
        fechaPago:   '$complementoPago.pagos.fechaPago',
        impPagado:   '$complementoPago.pagos.doctosRelacionados.impPagado',
      },
    },
  ]);

  // Agrupar aplicaciones por movimiento: cada aplicación se atribuye SOLO al
  // movimiento candidato cuyo erpLinks.saldoActual (monto de ESE depósito
  // aplicado a esa factura) está más cerca de su propio impPagado — no a
  // todos los movimientos que alguna vez tocaron esa factura. Sin esto, una
  // factura pagada por dos depósitos distintos en fechas distintas mezclaba
  // la aplicación de un depósito dentro del saldo corriente del otro.
  const aplicacionesPorMovimiento = new Map();
  for (const a of aplicaciones) {
    const candidatos = folioAMovimientos.get(a.facturaUuid) ?? [];
    if (candidatos.length === 0) continue;
    const impPagadoNum = Number(a.impPagado) || 0;
    const mejor = candidatos.reduce((best, c) =>
      Math.abs(c.saldoActual - impPagadoNum) < Math.abs(best.saldoActual - impPagadoNum) ? c : best,
    );
    if (!aplicacionesPorMovimiento.has(mejor.movId)) aplicacionesPorMovimiento.set(mejor.movId, []);
    aplicacionesPorMovimiento.get(mejor.movId).push(a);
  }

  for (const m of movimientos) {
    const idStr = String(m._id);
    const lista = (aplicacionesPorMovimiento.get(idStr) ?? [])
      .slice()
      .sort((a, b) => new Date(a.fechaPago) - new Date(b.fechaPago));

    let acumulado = 0;
    for (const a of lista) {
      acumulado += Number(a.impPagado) || 0;
      const saldoTrasEsta = Math.round(((m.deposito ?? 0) - acumulado) * 100) / 100;
      mapa.set(`${idStr}|${a.cfdiUuid}|${a.facturaUuid}`, saldoTrasEsta);
    }
  }

  return mapa;
}

/**
 * GET /api/reports/pagos-banco
 * Cruza CFDIs de pago (tipo P) con movimientos bancarios vía folioFiscal.
 * Params: uuid, serie, folio, fechaInicio, fechaFin, estado (todos|con_pago|sin_pago), page, limit
 */
const pagosBanco = asyncHandler(async (req, res) => {
  const {
    uuid, serie, folio, banco,
    numAutorizacion, idNumo,
    serieCxc, folioCxc,
    fechaInicio, fechaFin,
    ejercicio, periodo,
    estado = 'todos',
    page  = 1,
    limit = 20,
  } = req.query;

  const pg = parseInt(page);
  const lm = Math.min(parseInt(limit), 100);

  const baseMatch = {
    tipoDeComprobante: 'P',
    isActive: true,
    'complementoPago.pagos.doctosRelacionados.0': { $exists: true },
  };
  if (ejercicio) baseMatch.ejercicio = parseInt(ejercicio);
  if (periodo)   baseMatch.periodo   = parseInt(periodo);
  if (uuid) {
    const u = uuid.trim().toUpperCase();
    baseMatch.$or = [
      { uuid: { $regex: u, $options: 'i' } },
      { 'complementoPago.pagos.doctosRelacionados.idDocumento': { $regex: u, $options: 'i' } },
    ];
  }

  const drMatch = {};
  if (serie) drMatch['complementoPago.pagos.doctosRelacionados.serie'] = { $regex: serie.trim(), $options: 'i' };
  if (folio) drMatch['complementoPago.pagos.doctosRelacionados.folio'] = { $regex: folio.trim(), $options: 'i' };
  if (fechaInicio || fechaFin) {
    drMatch['complementoPago.pagos.fechaPago'] = {};
    if (fechaInicio) drMatch['complementoPago.pagos.fechaPago'].$gte = new Date(fechaInicio);
    if (fechaFin) {
      const fin = new Date(fechaFin);
      fin.setUTCDate(fin.getUTCDate() + 1);
      drMatch['complementoPago.pagos.fechaPago'].$lt = fin;
    }
  }

  const estadoMatch = {};
  if (estado === 'con_pago') estadoMatch['movimientos.0'] = { $exists: true };
  if (estado === 'sin_pago') estadoMatch.movimientos = { $size: 0 };

  // La CxC tiene dos numeraciones (interna serie/folio y serieExterna/folioExterno)
  // y erpLinks solo guarda serie + folioExterno, así que resolvemos el erpId
  // primero aceptando cualquiera de las dos numeraciones.
  const erpIdsCxc = (serieCxc || folioCxc)
    ? (await CFDI.db.collection('erp_cuentas_pendientes').find({
        $or: [
          {
            ...(serieCxc ? { serie: { $regex: serieCxc.trim(), $options: 'i' } } : {}),
            ...(folioCxc ? { folio: { $regex: folioCxc.trim(), $options: 'i' } } : {}),
          },
          {
            ...(serieCxc ? { serieExterna: { $regex: serieCxc.trim(), $options: 'i' } } : {}),
            ...(folioCxc ? { folioExterno: { $regex: folioCxc.trim(), $options: 'i' } } : {}),
          },
        ],
      }, { projection: { erpId: 1 } }).toArray()).map(c => c.erpId)
    : null;

  const pipeline = [
    { $match: baseMatch },
    ...DEDUP_PAGO_PREFIERE_SAT,
    { $unwind: '$complementoPago.pagos' },
    { $unwind: '$complementoPago.pagos.doctosRelacionados' },
    ...(Object.keys(drMatch).length ? [{ $match: drMatch }] : []),
    // erpLinks.folioFiscal se guarda con case inconsistente (algunos motores de
    // match lo escriben en minúsculas) — se generan las 3 variantes para que el
    // $lookup, que sigue usando localField/foreignField (índice multikey), no
    // pierda el match por diferencia de mayúsculas/minúsculas.
    {
      $addFields: {
        _idDocVariants: {
          $let: {
            vars: { d: { $ifNull: ['$complementoPago.pagos.doctosRelacionados.idDocumento', ''] } },
            in: ['$$d', { $toUpper: '$$d' }, { $toLower: '$$d' }],
          },
        },
      },
    },
    {
      $lookup: {
        from:         'bank_movements',
        localField:   '_idDocVariants',
        foreignField: 'erpLinks.folioFiscal',
        as:           'movimientos',
      },
    },
    // Filtrar inactivos + filtrar por banco si se especificó (antes de tomar [0])
    {
      $addFields: {
        movimientos: {
          $map: {
            input: {
              $filter: {
                input: '$movimientos',
                as: 'm',
                cond: {
                  $and: [
                    { $eq: ['$$m.isActive', true] },
                    ...(banco ? [{ $regexMatch: { input: { $ifNull: ['$$m.banco', ''] }, regex: banco.trim(), options: 'i' } }] : []),
                    ...(numAutorizacion ? [{ $or: [
                      { $regexMatch: { input: { $ifNull: [{ $toString: '$$m.numeroAutorizacion' }, ''] }, regex: numAutorizacion.trim(), options: 'i' } },
                      { $regexMatch: { input: { $ifNull: [{ $toString: '$$m.referenciaNumerica' }, ''] }, regex: numAutorizacion.trim(), options: 'i' } },
                    ]}] : []),
                    ...(idNumo ? [{ $regexMatch: { input: { $ifNull: [{ $toString: '$$m.folio' }, ''] }, regex: idNumo.trim(), options: 'i' } }] : []),
                    ...(erpIdsCxc ? [{
                      $gt: [{
                        $size: {
                          $filter: {
                            input: { $ifNull: ['$$m.erpLinks', []] },
                            as: 'l',
                            cond: { $in: ['$$l.erpId', erpIdsCxc] },
                          },
                        },
                      }, 0],
                    }] : []),
                  ],
                },
              },
            },
            as: 'm',
            in: {
              _id:          '$$m._id',
              banco:        '$$m.banco',
              fecha:        '$$m.fecha',
              deposito:     '$$m.deposito',
              folio:        '$$m.folio',
              concepto:     '$$m.concepto',
              numOperacion: { $ifNull: ['$$m.numeroAutorizacion', '$$m.referenciaNumerica'] },
              // Monto de ESTE depósito realmente aplicado a esta factura específica —
              // misma jerarquía que aplicarLogicaErp() en bank.service.js:
              // saldoPagadoTotal (cobro-panel, cualquier forma de pago) → saldoPagado
              // (solo bancario) → comportamiento legado (saldoActual si >0, si no total).
              // NO usar saldoActual a secas: en links legado es el SALDO RESTANTE de la
              // CxC (no lo aplicado), y comparar impPagado contra eso hacía que
              // "Diferencia" saliera en blanco para la mayoría de los pagos.
              saldoMovimiento: {
                $let: {
                  vars: {
                    link: {
                      $arrayElemAt: [{
                        $filter: {
                          input: { $ifNull: ['$$m.erpLinks', []] },
                          as: 'l',
                          cond: {
                            $eq: [
                              { $toLower: { $ifNull: ['$$l.folioFiscal', ''] } },
                              { $toLower: { $ifNull: ['$complementoPago.pagos.doctosRelacionados.idDocumento', ''] } },
                            ],
                          },
                        },
                      }, 0],
                    },
                  },
                  in: {
                    $cond: [
                      { $ne: ['$$link.saldoPagadoTotal', null] },
                      '$$link.saldoPagadoTotal',
                      {
                        $cond: [
                          { $ne: ['$$link.saldoPagado', null] },
                          '$$link.saldoPagado',
                          {
                            $cond: [
                              { $and: [{ $ne: ['$$link.saldoActual', null] }, { $gt: ['$$link.saldoActual', 0] }] },
                              '$$link.saldoActual',
                              { $ifNull: ['$$link.total', 0] },
                            ],
                          },
                        ],
                      },
                    ],
                  },
                },
              },
              // Serie/Folio de la factura origen — fallback cuando el CFDI de pago no
              // trae su propio doctoRelacionado con esos campos (mismo dato ya
              // capturado en el erpLink de este movimiento al vincularlo).
              serieOrigen: {
                $let: {
                  vars: {
                    link: {
                      $ifNull: [{
                        $arrayElemAt: [{
                          $filter: {
                            input: { $ifNull: ['$$m.erpLinks', []] },
                            as: 'l',
                            cond: {
                              $eq: [
                                { $toLower: { $ifNull: ['$$l.folioFiscal', ''] } },
                                { $toLower: { $ifNull: ['$complementoPago.pagos.doctosRelacionados.idDocumento', ''] } },
                              ],
                            },
                          },
                        }, 0],
                      }, null],
                    },
                  },
                  in: '$$link.serie',
                },
              },
              folioOrigen: {
                $let: {
                  vars: {
                    link: {
                      $ifNull: [{
                        $arrayElemAt: [{
                          $filter: {
                            input: { $ifNull: ['$$m.erpLinks', []] },
                            as: 'l',
                            cond: {
                              $eq: [
                                { $toLower: { $ifNull: ['$$l.folioFiscal', ''] } },
                                { $toLower: { $ifNull: ['$complementoPago.pagos.doctosRelacionados.idDocumento', ''] } },
                              ],
                            },
                          },
                        }, 0],
                      }, null],
                    },
                  },
                  in: '$$link.folioExterno',
                },
              },
              // erpId de la CxC vinculada — se usa después para cruzar con el
              // kardex de erp_cuentas_pendientes (Parcialidad / Saldo Anterior).
              erpIdOrigen: {
                $let: {
                  vars: {
                    link: {
                      $ifNull: [{
                        $arrayElemAt: [{
                          $filter: {
                            input: { $ifNull: ['$$m.erpLinks', []] },
                            as: 'l',
                            cond: {
                              $eq: [
                                { $toLower: { $ifNull: ['$$l.folioFiscal', ''] } },
                                { $toLower: { $ifNull: ['$complementoPago.pagos.doctosRelacionados.idDocumento', ''] } },
                              ],
                            },
                          },
                        }, 0],
                      }, null],
                    },
                  },
                  in: '$$link.erpId',
                },
              },
              // Usuario que identificó/vinculó ESTA CxC específica al movimiento
              // (identificadoPor.erpId se cruza contra el erpId del erpLink que
              // corresponde a este idDocumento — un mismo movimiento puede tener
              // varias CxC vinculadas por distintos usuarios).
              identificadoPorNombre: {
                $let: {
                  vars: {
                    erpIdLink: {
                      $let: {
                        vars: {
                          link: {
                            $arrayElemAt: [{
                              $filter: {
                                input: { $ifNull: ['$$m.erpLinks', []] },
                                as: 'l',
                                cond: {
                                  $eq: [
                                    { $toLower: { $ifNull: ['$$l.folioFiscal', ''] } },
                                    { $toLower: { $ifNull: ['$complementoPago.pagos.doctosRelacionados.idDocumento', ''] } },
                                  ],
                                },
                              },
                            }, 0],
                          },
                        },
                        // Mismo cuidado que en idEntry: forzar a null real, no "missing".
                        in: { $ifNull: ['$$link.erpId', null] },
                      },
                    },
                  },
                  in: {
                    $let: {
                      vars: {
                        // $arrayElemAt sobre un arreglo vacío regresa "missing" (no null) —
                        // usar ese valor directo como variable de $let envenena toda la
                        // expresión contenedora (el campo entero desaparece). Se envuelve
                        // en $ifNull para forzarlo a null real.
                        idEntry: {
                          $ifNull: [{
                            $arrayElemAt: [{
                              $filter: {
                                input: { $ifNull: ['$$m.identificadoPor', []] },
                                as: 'ip',
                                cond: { $eq: ['$$ip.erpId', '$$erpIdLink'] },
                              },
                            }, 0],
                          }, null],
                        },
                        // Fallback: varios motores de match (pagos-cyc, mostrador-cyc,
                        // refacturaciones-cyc) guardan una sola entrada resumen para todo
                        // el movimiento sin erpId por CxC. Si no hay coincidencia exacta,
                        // se listan los nombres disponibles — igual que ya hace Bancos
                        // (identificadoPorLabel en banks.component.ts).
                        todosNombres: {
                          $reduce: {
                            input: { $ifNull: ['$$m.identificadoPor', []] },
                            initialValue: [],
                            in: {
                              $let: {
                                vars: { n: { $ifNull: ['$$this.nombre', '$$this.userId'] } },
                                in: {
                                  $cond: [
                                    { $or: [{ $eq: ['$$n', null] }, { $in: ['$$n', '$$value'] }] },
                                    '$$value',
                                    { $concatArrays: ['$$value', ['$$n']] },
                                  ],
                                },
                              },
                            },
                          },
                        },
                      },
                      in: {
                        $cond: [
                          { $ne: ['$$idEntry', null] },
                          '$$idEntry.nombre',
                          {
                            $cond: [
                              { $gt: [{ $size: '$$todosNombres' }, 0] },
                              {
                                $reduce: {
                                  input: '$$todosNombres',
                                  initialValue: '',
                                  in: { $cond: [{ $eq: ['$$value', ''] }, '$$this', { $concat: ['$$value', ', ', '$$this'] }] },
                                },
                              },
                              null,
                            ],
                          },
                        ],
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
    },
    // Cuando la factura tiene varios movimientos bancarios vinculados (ej.
    // varias parcialidades pagadas por depósitos distintos), el índice 0 de
    // `movimientos` podía quedar en cualquiera de ellos — no necesariamente
    // el que corresponde a ESTE complemento de pago. Se reordena por
    // cercanía de monto al impPagado de este pago, así los $arrayElemAt(
    // movimientos.X, 0) de más abajo (Banco, Depósito, Diferencia, etc.)
    // toman el movimiento correcto en vez del primero que haya quedado ahí.
    {
      $addFields: {
        movimientos: {
          $sortArray: {
            input: {
              $map: {
                input: '$movimientos',
                as: 'mv',
                in: {
                  $mergeObjects: ['$$mv', {
                    _diffAbs: {
                      $abs: {
                        $subtract: [
                          { $ifNull: ['$$mv.deposito', 0] },
                          { $ifNull: ['$complementoPago.pagos.doctosRelacionados.impPagado', 0] },
                        ],
                      },
                    },
                  }],
                },
              },
            },
            sortBy: { _diffAbs: 1 },
          },
        },
      },
    },
    // Cruce con el kardex de CxC del ERP (erp_cuentas_pendientes) para recuperar
    // Parcialidad y Saldo Anterior — datos que no vienen ni en el CFDI ni en el
    // erpLink del movimiento bancario, solo en el historial de abonos de la CxC.
    {
      $lookup: {
        from:         'erp_cuentas_pendientes',
        localField:   'movimientos.erpIdOrigen',
        foreignField: 'erpId',
        as:           'cuentasPendientes',
      },
    },
    {
      $addFields: {
        parcialidadInfo: {
          $let: {
            vars: {
              erpIdActual: { $ifNull: [{ $arrayElemAt: ['$movimientos.erpIdOrigen', 0] }, null] },
            },
            in: {
              $let: {
                vars: {
                  cxc: {
                    $ifNull: [{
                      $arrayElemAt: [{
                        $filter: {
                          input: { $ifNull: ['$cuentasPendientes', []] },
                          as: 'c',
                          cond: { $eq: ['$$c.erpId', '$$erpIdActual'] },
                        },
                      }, 0],
                    }, null],
                  },
                },
                in: {
                  $let: {
                    vars: {
                      // Abono de esa CxC (trae formasPago) cuyo monto coincide con este
                      // pago (±$1 de tolerancia por redondeo).
                      abonoCoincidente: {
                        $ifNull: [{
                          $arrayElemAt: [{
                            $filter: {
                              input: { $ifNull: ['$$cxc.movimientos', []] },
                              as: 'mv',
                              cond: {
                                $and: [
                                  { $gt: [{ $size: { $ifNull: ['$$mv.formasPago', []] } }, 0] },
                                  {
                                    $lte: [
                                      { $abs: { $subtract: [
                                        { $sum: { $map: { input: { $ifNull: ['$$mv.formasPago', []] }, as: 'fp', in: { $ifNull: ['$$fp.monto', 0] } } } },
                                        { $ifNull: ['$complementoPago.pagos.monto', 0] },
                                      ] } },
                                      1,
                                    ],
                                  },
                                ],
                              },
                            },
                          }, 0],
                        }, null],
                      },
                      // Todos los abonos de esa CxC (para calcular la posición = parcialidad)
                      todosAbonos: {
                        $filter: {
                          input: { $ifNull: ['$$cxc.movimientos', []] },
                          as: 'mv',
                          cond: { $gt: [{ $size: { $ifNull: ['$$mv.formasPago', []] } }, 0] },
                        },
                      },
                    },
                    in: {
                      $cond: [
                        { $ne: ['$$abonoCoincidente', null] },
                        {
                          saldoAnterior: '$$abonoCoincidente.saldoAnterior',
                          saldoActual: '$$abonoCoincidente.saldoActual',
                          numParcialidad: {
                            $add: [
                              1,
                              {
                                $size: {
                                  $filter: {
                                    input: '$$todosAbonos',
                                    as: 'a',
                                    cond: { $lt: ['$$a.fecha', '$$abonoCoincidente.fecha'] },
                                  },
                                },
                              },
                            ],
                          },
                        },
                        null,
                      ],
                    },
                  },
                },
              },
            },
          },
        },
      },
    },
    // Si filtramos por banco/autorización/idNumo/CxC, solo conservar documentos con al menos un movimiento válido
    ...((banco || numAutorizacion || idNumo || serieCxc || folioCxc) ? [{ $match: { 'movimientos.0': { $exists: true } } }] : []),
    {
      $facet: {
        data: [
          ...(Object.keys(estadoMatch).length ? [{ $match: estadoMatch }] : []),
          { $sort: { 'complementoPago.pagos.fechaPago': -1 } },
          { $skip: (pg - 1) * lm },
          { $limit: lm },
          {
            $project: {
              _id: 0,
              cfdiUuid:        '$uuid',
              satStatus:       '$satStatus',
              source:          '$source',
              fechaPago:       '$complementoPago.pagos.fechaPago',
              montoPago:       '$complementoPago.pagos.monto',
              facturaUuid:     '$complementoPago.pagos.doctosRelacionados.idDocumento',
              // Serie/Folio: si el CFDI no los trae, usar los del erpLink del movimiento
              // vinculado (mismo dato, capturado al momento de identificar el pago).
              serie:           { $ifNull: ['$complementoPago.pagos.doctosRelacionados.serie', { $ifNull: [{ $arrayElemAt: ['$movimientos.serieOrigen', 0] }, null] }] },
              folio:           { $ifNull: ['$complementoPago.pagos.doctosRelacionados.folio', { $ifNull: [{ $arrayElemAt: ['$movimientos.folioOrigen', 0] }, null] }] },
              // Parcialidad/Saldo Anterior: si el CFDI no los trae, usar el kardex
              // de erp_cuentas_pendientes (parcialidadInfo, calculado arriba).
              numParcialidad:  { $ifNull: ['$complementoPago.pagos.doctosRelacionados.numParcialidad', '$parcialidadInfo.numParcialidad'] },
              // Imp. Pagado: si el CFDI no lo trae, usar saldoMovimiento (monto de
              // ESTE depósito específico aplicado a ESTA factura, vía el erpLink
              // cuyo folioFiscal coincide con el idDocumento) — no el saldoActual
              // del kardex ERP, que es un saldo remanente, no un monto pagado.
              impPagado: { $ifNull: ['$complementoPago.pagos.doctosRelacionados.impPagado', { $arrayElemAt: ['$movimientos.saldoMovimiento', 0] }] },
              impSaldoAnt: { $ifNull: ['$complementoPago.pagos.doctosRelacionados.impSaldoAnt', '$parcialidadInfo.saldoAnterior'] },
              // Saldo Insoluto: si el CFDI no lo trae, usar el saldoActual de la CxC
              // en el erpLink del movimiento (mismo concepto: saldo pendiente actual).
              // OJO: NO usar movimientos.saldoMovimiento aquí — ese campo es el monto
              // APLICADO de este depósito (equivale a Imp. Pagado, no a un saldo
              // restante); usarlo como saldo insoluto mostraba el pago aplicado en la
              // columna equivocada.
              impSaldoInsoluto: { $ifNull: ['$complementoPago.pagos.doctosRelacionados.impSaldoInsoluto', '$parcialidadInfo.saldoActual'] },
              tienePago:       { $gt: [{ $size: '$movimientos' }, 0] },
              banco:           { $arrayElemAt: ['$movimientos.banco',        0] },
              movFecha:        { $arrayElemAt: ['$movimientos.fecha',        0] },
              movFolio:        { $arrayElemAt: ['$movimientos.folio',        0] },
              deposito:        { $arrayElemAt: ['$movimientos.deposito',     0] },
              movConcepto:     { $arrayElemAt: ['$movimientos.concepto',     0] },
              numOperacion:    { $arrayElemAt: ['$movimientos.numOperacion', 0] },
              // Diferencia: comparar impPagado contra la porción de ESE depósito
              // aplicada a ESTA factura (saldoMovimiento), no contra el depósito
              // completo — un depósito de lote que cubre varias facturas mostraba
              // una "diferencia" enorme y sin sentido contra cada factura individual.
              //
              // Cuando UN depósito paga VARIAS facturas distintas, erpLinks solo
              // guarda un acumulado por CxC y no siempre distingue cuál porción es
              // de cuál factura. Pero cada CxC tiene su PROPIO kardex en el ERP
              // (erp_cuentas_pendientes) con su propio abono específico — ya
              // matcheado arriba en parcialidadInfo por monto de formasPago. Cuando
              // existe, saldoAnterior−saldoActual de ESE abono es el monto exacto
              // aplicado a ESTA factura, más preciso que erpLinks. Solo si tampoco
              // hay parcialidadInfo se cae a erpLinks/depósito completo, y si ni así
              // coincide (±$1) se muestra null en vez de un número engañoso.
              diferencia: {
                $let: {
                  vars: {
                    diff: { $round: [{ $subtract: [
                      '$complementoPago.pagos.doctosRelacionados.impPagado',
                      { $ifNull: [
                        { $cond: [
                          { $ne: ['$parcialidadInfo', null] },
                          { $subtract: ['$parcialidadInfo.saldoAnterior', '$parcialidadInfo.saldoActual'] },
                          null,
                        ] },
                        { $ifNull: [{ $arrayElemAt: ['$movimientos.saldoMovimiento', 0] }, { $ifNull: [{ $arrayElemAt: ['$movimientos.deposito', 0] }, 0] }] },
                      ] },
                    ]}, 2] },
                  },
                  in: { $cond: [{ $lte: [{ $abs: '$$diff' }, 1] }, '$$diff', null] },
                },
              },
              movimientoId:     { $arrayElemAt: ['$movimientos._id', 0] },
              identificadoPor:  { $arrayElemAt: ['$movimientos.identificadoPorNombre', 0] },
            },
          },
        ],
        resumenAgg: [
          {
            $group: {
              _id:           { tienePago: { $gt: [{ $size: '$movimientos' }, 0] } },
              cantidad:      { $sum: 1 },
              sumaImpPagado: { $sum: '$complementoPago.pagos.doctosRelacionados.impPagado' },
            },
          },
        ],
        totalAgg: [
          ...(Object.keys(estadoMatch).length ? [{ $match: estadoMatch }] : []),
          { $count: 'count' },
        ],
      },
    },
  ];

  const [result] = await CFDI.aggregate(pipeline).allowDiskUse(true);
  const total = result.totalAgg[0]?.count ?? 0;

  const resumen = { conPago: { cantidad: 0, monto: 0 }, sinPago: { cantidad: 0, monto: 0 } };
  for (const t of result.resumenAgg) {
    const key = t._id.tienePago ? 'conPago' : 'sinPago';
    resumen[key] = { cantidad: t.cantidad, monto: Math.round(t.sumaImpPagado * 100) / 100 };
  }

  const notasPorFactura = await buscarNotasCreditoPorFacturasBatch(result.data.map(r => r.facturaUuid));
  const saldosBanco = await calcularSaldosBanco(result.data.map(r => r.movimientoId));
  const data = result.data.map(r => {
    const nc = notasPorFactura.get(r.facturaUuid);
    const claveSaldoBanco = `${r.movimientoId}|${(r.cfdiUuid || '').toUpperCase()}|${(r.facturaUuid || '').toUpperCase()}`;
    return {
      ...r,
      tipoNC:     nc ? [...nc.tipos].sort().join(', ') : null,
      montoNC:    nc ? Math.round(nc.monto * 100) / 100 : null,
      saldoBanco: r.movimientoId ? saldosBanco.get(claveSaldoBanco) ?? null : null,
    };
  });

  res.json({ data, total, page: pg, limit: lm, pages: Math.ceil(total / lm), resumen });
});

/**
 * GET /api/reports/pagos-banco/export
 * Descarga Excel con los mismos filtros que pagosBanco (sin paginación).
 */
const pagosBancoExport = asyncHandler(async (req, res) => {
  const { uuid, serie, folio, banco, numAutorizacion, idNumo, serieCxc, folioCxc, fechaInicio, fechaFin, ejercicio, periodo, estado = 'todos' } = req.query;

  const baseMatch = {
    tipoDeComprobante: 'P',
    isActive: true,
    'complementoPago.pagos.doctosRelacionados.0': { $exists: true },
  };
  if (ejercicio) baseMatch.ejercicio = parseInt(ejercicio);
  if (periodo)   baseMatch.periodo   = parseInt(periodo);
  if (uuid) {
    const u = uuid.trim().toUpperCase();
    baseMatch.$or = [
      { uuid: { $regex: u, $options: 'i' } },
      { 'complementoPago.pagos.doctosRelacionados.idDocumento': { $regex: u, $options: 'i' } },
    ];
  }

  const drMatch = {};
  if (serie) drMatch['complementoPago.pagos.doctosRelacionados.serie'] = { $regex: serie.trim(), $options: 'i' };
  if (folio) drMatch['complementoPago.pagos.doctosRelacionados.folio'] = { $regex: folio.trim(), $options: 'i' };
  if (fechaInicio || fechaFin) {
    drMatch['complementoPago.pagos.fechaPago'] = {};
    if (fechaInicio) drMatch['complementoPago.pagos.fechaPago'].$gte = new Date(fechaInicio);
    if (fechaFin) {
      const fin = new Date(fechaFin);
      fin.setUTCDate(fin.getUTCDate() + 1);
      drMatch['complementoPago.pagos.fechaPago'].$lt = fin;
    }
  }

  const estadoMatch = {};
  if (estado === 'con_pago') estadoMatch['movimientos.0'] = { $exists: true };
  if (estado === 'sin_pago') estadoMatch.movimientos = { $size: 0 };

  const erpIdsCxc = (serieCxc || folioCxc)
    ? (await CFDI.db.collection('erp_cuentas_pendientes').find({
        $or: [
          {
            ...(serieCxc ? { serie: { $regex: serieCxc.trim(), $options: 'i' } } : {}),
            ...(folioCxc ? { folio: { $regex: folioCxc.trim(), $options: 'i' } } : {}),
          },
          {
            ...(serieCxc ? { serieExterna: { $regex: serieCxc.trim(), $options: 'i' } } : {}),
            ...(folioCxc ? { folioExterno: { $regex: folioCxc.trim(), $options: 'i' } } : {}),
          },
        ],
      }, { projection: { erpId: 1 } }).toArray()).map(c => c.erpId)
    : null;

  const pipeline = [
    { $match: baseMatch },
    ...DEDUP_PAGO_PREFIERE_SAT,
    { $unwind: '$complementoPago.pagos' },
    { $unwind: '$complementoPago.pagos.doctosRelacionados' },
    ...(Object.keys(drMatch).length ? [{ $match: drMatch }] : []),
    // Ver comentario equivalente en pagosBanco: erpLinks.folioFiscal tiene case
    // inconsistente en los datos, se generan variantes para no perder el match.
    {
      $addFields: {
        _idDocVariants: {
          $let: {
            vars: { d: { $ifNull: ['$complementoPago.pagos.doctosRelacionados.idDocumento', ''] } },
            in: ['$$d', { $toUpper: '$$d' }, { $toLower: '$$d' }],
          },
        },
      },
    },
    { $lookup: { from: 'bank_movements', localField: '_idDocVariants', foreignField: 'erpLinks.folioFiscal', as: 'movimientos' } },
    {
      $addFields: {
        movimientos: {
          $map: {
            input: {
              $filter: {
                input: '$movimientos',
                as: 'm',
                cond: {
                  $and: [
                    { $eq: ['$$m.isActive', true] },
                    ...(banco ? [{ $regexMatch: { input: { $ifNull: ['$$m.banco', ''] }, regex: banco.trim(), options: 'i' } }] : []),
                    ...(numAutorizacion ? [{ $or: [
                      { $regexMatch: { input: { $ifNull: [{ $toString: '$$m.numeroAutorizacion' }, ''] }, regex: numAutorizacion.trim(), options: 'i' } },
                      { $regexMatch: { input: { $ifNull: [{ $toString: '$$m.referenciaNumerica' }, ''] }, regex: numAutorizacion.trim(), options: 'i' } },
                    ]}] : []),
                    ...(idNumo ? [{ $regexMatch: { input: { $ifNull: [{ $toString: '$$m.folio' }, ''] }, regex: idNumo.trim(), options: 'i' } }] : []),
                    ...(erpIdsCxc ? [{
                      $gt: [{
                        $size: {
                          $filter: {
                            input: { $ifNull: ['$$m.erpLinks', []] },
                            as: 'l',
                            cond: { $in: ['$$l.erpId', erpIdsCxc] },
                          },
                        },
                      }, 0],
                    }] : []),
                  ],
                },
              },
            },
            as: 'm',
            in: {
              _id: '$$m._id',
              banco: '$$m.banco',
              fecha: '$$m.fecha',
              deposito: '$$m.deposito',
              folio: '$$m.folio',
              numOperacion: { $ifNull: ['$$m.numeroAutorizacion', '$$m.referenciaNumerica'] },
              // Ver comentario completo en pagosBanco — misma jerarquía que
              // aplicarLogicaErp() en bank.service.js (saldoPagadoTotal → saldoPagado
              // → legado saldoActual/total). NO usar saldoActual a secas.
              saldoMovimiento: {
                $let: {
                  vars: {
                    link: {
                      $arrayElemAt: [{
                        $filter: {
                          input: { $ifNull: ['$$m.erpLinks', []] },
                          as: 'l',
                          cond: {
                            $eq: [
                              { $toLower: { $ifNull: ['$$l.folioFiscal', ''] } },
                              { $toLower: { $ifNull: ['$complementoPago.pagos.doctosRelacionados.idDocumento', ''] } },
                            ],
                          },
                        },
                      }, 0],
                    },
                  },
                  in: {
                    $cond: [
                      { $ne: ['$$link.saldoPagadoTotal', null] },
                      '$$link.saldoPagadoTotal',
                      {
                        $cond: [
                          { $ne: ['$$link.saldoPagado', null] },
                          '$$link.saldoPagado',
                          {
                            $cond: [
                              { $and: [{ $ne: ['$$link.saldoActual', null] }, { $gt: ['$$link.saldoActual', 0] }] },
                              '$$link.saldoActual',
                              { $ifNull: ['$$link.total', 0] },
                            ],
                          },
                        ],
                      },
                    ],
                  },
                },
              },
              serieOrigen: {
                $let: {
                  vars: {
                    link: {
                      $ifNull: [{
                        $arrayElemAt: [{
                          $filter: {
                            input: { $ifNull: ['$$m.erpLinks', []] },
                            as: 'l',
                            cond: {
                              $eq: [
                                { $toLower: { $ifNull: ['$$l.folioFiscal', ''] } },
                                { $toLower: { $ifNull: ['$complementoPago.pagos.doctosRelacionados.idDocumento', ''] } },
                              ],
                            },
                          },
                        }, 0],
                      }, null],
                    },
                  },
                  in: '$$link.serie',
                },
              },
              folioOrigen: {
                $let: {
                  vars: {
                    link: {
                      $ifNull: [{
                        $arrayElemAt: [{
                          $filter: {
                            input: { $ifNull: ['$$m.erpLinks', []] },
                            as: 'l',
                            cond: {
                              $eq: [
                                { $toLower: { $ifNull: ['$$l.folioFiscal', ''] } },
                                { $toLower: { $ifNull: ['$complementoPago.pagos.doctosRelacionados.idDocumento', ''] } },
                              ],
                            },
                          },
                        }, 0],
                      }, null],
                    },
                  },
                  in: '$$link.folioExterno',
                },
              },
              // erpId de la CxC vinculada — se usa después para cruzar con el
              // kardex de erp_cuentas_pendientes (Parcialidad / Saldo Anterior).
              // (faltaba en este pipeline — sí estaba en pagosBanco — por eso el
              // export nunca rellenaba Parcialidad/Saldo Anterior vía fallback ERP).
              erpIdOrigen: {
                $let: {
                  vars: {
                    link: {
                      $ifNull: [{
                        $arrayElemAt: [{
                          $filter: {
                            input: { $ifNull: ['$$m.erpLinks', []] },
                            as: 'l',
                            cond: {
                              $eq: [
                                { $toLower: { $ifNull: ['$$l.folioFiscal', ''] } },
                                { $toLower: { $ifNull: ['$complementoPago.pagos.doctosRelacionados.idDocumento', ''] } },
                              ],
                            },
                          },
                        }, 0],
                      }, null],
                    },
                  },
                  in: '$$link.erpId',
                },
              },
              identificadoPorNombre: {
                $let: {
                  vars: {
                    erpIdLink: {
                      $let: {
                        vars: {
                          link: {
                            $arrayElemAt: [{
                              $filter: {
                                input: { $ifNull: ['$$m.erpLinks', []] },
                                as: 'l',
                                cond: {
                                  $eq: [
                                    { $toLower: { $ifNull: ['$$l.folioFiscal', ''] } },
                                    { $toLower: { $ifNull: ['$complementoPago.pagos.doctosRelacionados.idDocumento', ''] } },
                                  ],
                                },
                              },
                            }, 0],
                          },
                        },
                        // Mismo cuidado que en idEntry: forzar a null real, no "missing".
                        in: { $ifNull: ['$$link.erpId', null] },
                      },
                    },
                  },
                  in: {
                    $let: {
                      vars: {
                        // $arrayElemAt sobre un arreglo vacío regresa "missing" (no null) —
                        // usar ese valor directo como variable de $let envenena toda la
                        // expresión contenedora (el campo entero desaparece). Se envuelve
                        // en $ifNull para forzarlo a null real.
                        idEntry: {
                          $ifNull: [{
                            $arrayElemAt: [{
                              $filter: {
                                input: { $ifNull: ['$$m.identificadoPor', []] },
                                as: 'ip',
                                cond: { $eq: ['$$ip.erpId', '$$erpIdLink'] },
                              },
                            }, 0],
                          }, null],
                        },
                        // Fallback: varios motores de match (pagos-cyc, mostrador-cyc,
                        // refacturaciones-cyc) guardan una sola entrada resumen para todo
                        // el movimiento sin erpId por CxC. Si no hay coincidencia exacta,
                        // se listan los nombres disponibles — igual que ya hace Bancos
                        // (identificadoPorLabel en banks.component.ts).
                        todosNombres: {
                          $reduce: {
                            input: { $ifNull: ['$$m.identificadoPor', []] },
                            initialValue: [],
                            in: {
                              $let: {
                                vars: { n: { $ifNull: ['$$this.nombre', '$$this.userId'] } },
                                in: {
                                  $cond: [
                                    { $or: [{ $eq: ['$$n', null] }, { $in: ['$$n', '$$value'] }] },
                                    '$$value',
                                    { $concatArrays: ['$$value', ['$$n']] },
                                  ],
                                },
                              },
                            },
                          },
                        },
                      },
                      in: {
                        $cond: [
                          { $ne: ['$$idEntry', null] },
                          '$$idEntry.nombre',
                          {
                            $cond: [
                              { $gt: [{ $size: '$$todosNombres' }, 0] },
                              {
                                $reduce: {
                                  input: '$$todosNombres',
                                  initialValue: '',
                                  in: { $cond: [{ $eq: ['$$value', ''] }, '$$this', { $concat: ['$$value', ', ', '$$this'] }] },
                                },
                              },
                              null,
                            ],
                          },
                        ],
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
    },
    // Cuando la factura tiene varios movimientos bancarios vinculados (ej.
    // varias parcialidades pagadas por depósitos distintos), el índice 0 de
    // `movimientos` podía quedar en cualquiera de ellos — no necesariamente
    // el que corresponde a ESTE complemento de pago. Se reordena por
    // cercanía de monto al impPagado de este pago, así los $arrayElemAt(
    // movimientos.X, 0) de más abajo (Banco, Depósito, Diferencia, etc.)
    // toman el movimiento correcto en vez del primero que haya quedado ahí.
    {
      $addFields: {
        movimientos: {
          $sortArray: {
            input: {
              $map: {
                input: '$movimientos',
                as: 'mv',
                in: {
                  $mergeObjects: ['$$mv', {
                    _diffAbs: {
                      $abs: {
                        $subtract: [
                          { $ifNull: ['$$mv.deposito', 0] },
                          { $ifNull: ['$complementoPago.pagos.doctosRelacionados.impPagado', 0] },
                        ],
                      },
                    },
                  }],
                },
              },
            },
            sortBy: { _diffAbs: 1 },
          },
        },
      },
    },
    // Cruce con el kardex de CxC del ERP (erp_cuentas_pendientes) para recuperar
    // Parcialidad y Saldo Anterior — datos que no vienen ni en el CFDI ni en el
    // erpLink del movimiento bancario, solo en el historial de abonos de la CxC.
    {
      $lookup: {
        from:         'erp_cuentas_pendientes',
        localField:   'movimientos.erpIdOrigen',
        foreignField: 'erpId',
        as:           'cuentasPendientes',
      },
    },
    {
      $addFields: {
        parcialidadInfo: {
          $let: {
            vars: {
              erpIdActual: { $ifNull: [{ $arrayElemAt: ['$movimientos.erpIdOrigen', 0] }, null] },
            },
            in: {
              $let: {
                vars: {
                  cxc: {
                    $ifNull: [{
                      $arrayElemAt: [{
                        $filter: {
                          input: { $ifNull: ['$cuentasPendientes', []] },
                          as: 'c',
                          cond: { $eq: ['$$c.erpId', '$$erpIdActual'] },
                        },
                      }, 0],
                    }, null],
                  },
                },
                in: {
                  $let: {
                    vars: {
                      abonoCoincidente: {
                        $ifNull: [{
                          $arrayElemAt: [{
                            $filter: {
                              input: { $ifNull: ['$$cxc.movimientos', []] },
                              as: 'mv',
                              cond: {
                                $and: [
                                  { $gt: [{ $size: { $ifNull: ['$$mv.formasPago', []] } }, 0] },
                                  {
                                    $lte: [
                                      { $abs: { $subtract: [
                                        { $sum: { $map: { input: { $ifNull: ['$$mv.formasPago', []] }, as: 'fp', in: { $ifNull: ['$$fp.monto', 0] } } } },
                                        { $ifNull: ['$complementoPago.pagos.monto', 0] },
                                      ] } },
                                      1,
                                    ],
                                  },
                                ],
                              },
                            },
                          }, 0],
                        }, null],
                      },
                      todosAbonos: {
                        $filter: {
                          input: { $ifNull: ['$$cxc.movimientos', []] },
                          as: 'mv',
                          cond: { $gt: [{ $size: { $ifNull: ['$$mv.formasPago', []] } }, 0] },
                        },
                      },
                    },
                    in: {
                      $cond: [
                        { $ne: ['$$abonoCoincidente', null] },
                        {
                          saldoAnterior: '$$abonoCoincidente.saldoAnterior',
                          saldoActual: '$$abonoCoincidente.saldoActual',
                          numParcialidad: {
                            $add: [
                              1,
                              {
                                $size: {
                                  $filter: {
                                    input: '$$todosAbonos',
                                    as: 'a',
                                    cond: { $lt: ['$$a.fecha', '$$abonoCoincidente.fecha'] },
                                  },
                                },
                              },
                            ],
                          },
                        },
                        null,
                      ],
                    },
                  },
                },
              },
            },
          },
        },
      },
    },
    ...((banco || numAutorizacion || idNumo || serieCxc || folioCxc) ? [{ $match: { 'movimientos.0': { $exists: true } } }] : []),
    ...(Object.keys(estadoMatch).length ? [{ $match: estadoMatch }] : []),
    { $sort: { 'complementoPago.pagos.fechaPago': -1 } },
    { $limit: 50000 },
    { $project: {
      _id: 0,
      cfdiUuid:         '$uuid',
      satStatus:        '$satStatus',
      fechaPago:        '$complementoPago.pagos.fechaPago',
      facturaUuid:      '$complementoPago.pagos.doctosRelacionados.idDocumento',
      serie:            { $ifNull: ['$complementoPago.pagos.doctosRelacionados.serie', { $ifNull: [{ $arrayElemAt: ['$movimientos.serieOrigen', 0] }, null] }] },
      folio:            { $ifNull: ['$complementoPago.pagos.doctosRelacionados.folio', { $ifNull: [{ $arrayElemAt: ['$movimientos.folioOrigen', 0] }, null] }] },
      numParcialidad:   { $ifNull: ['$complementoPago.pagos.doctosRelacionados.numParcialidad', '$parcialidadInfo.numParcialidad'] },
      // Ver comentario equivalente en pagosBanco: impPagado cae a saldoMovimiento
      // (monto de ESTE depósito aplicado a ESTA factura), impSaldoInsoluto ya NO
      // cae a saldoMovimiento (ese es un monto aplicado, no un saldo restante).
      impPagado: { $ifNull: ['$complementoPago.pagos.doctosRelacionados.impPagado', { $arrayElemAt: ['$movimientos.saldoMovimiento', 0] }] },
      impSaldoAnt: { $ifNull: ['$complementoPago.pagos.doctosRelacionados.impSaldoAnt', '$parcialidadInfo.saldoAnterior'] },
      impSaldoInsoluto: { $ifNull: ['$complementoPago.pagos.doctosRelacionados.impSaldoInsoluto', '$parcialidadInfo.saldoActual'] },
      tienePago:        { $gt: [{ $size: '$movimientos' }, 0] },
      banco:            { $arrayElemAt: ['$movimientos.banco',        0] },
      movFecha:         { $arrayElemAt: ['$movimientos.fecha',        0] },
      movFolio:         { $arrayElemAt: ['$movimientos.folio',        0] },
      deposito:         { $arrayElemAt: ['$movimientos.deposito',     0] },
      numOperacion:     { $arrayElemAt: ['$movimientos.numOperacion', 0] },
      // Ver comentario completo en pagosBanco: cuando un depósito paga varias
      // facturas, se prefiere el abono específico de ESTA CxC en su propio
      // kardex del ERP (parcialidadInfo.saldoAnterior − saldoActual) antes de
      // caer a erpLinks/depósito completo.
      diferencia: {
        $let: {
          vars: {
            diff: { $round: [{ $subtract: [
              '$complementoPago.pagos.doctosRelacionados.impPagado',
              { $ifNull: [
                { $cond: [
                  { $ne: ['$parcialidadInfo', null] },
                  { $subtract: ['$parcialidadInfo.saldoAnterior', '$parcialidadInfo.saldoActual'] },
                  null,
                ] },
                { $ifNull: [{ $arrayElemAt: ['$movimientos.saldoMovimiento', 0] }, { $ifNull: [{ $arrayElemAt: ['$movimientos.deposito', 0] }, 0] }] },
              ] },
            ]}, 2] },
          },
          in: { $cond: [{ $lte: [{ $abs: '$$diff' }, 1] }, '$$diff', null] },
        },
      },
      movimientoId:     { $arrayElemAt: ['$movimientos._id', 0] },
      identificadoPor:  { $arrayElemAt: ['$movimientos.identificadoPorNombre', 0] },
    }},
  ];

  const rows = await CFDI.aggregate(pipeline).allowDiskUse(true);

  const notasPorFactura = await buscarNotasCreditoPorFacturasBatch(rows.map(r => r.facturaUuid));
  const saldosBanco = await calcularSaldosBanco(rows.map(r => r.movimientoId));
  for (const r of rows) {
    const nc = notasPorFactura.get(r.facturaUuid);
    const claveSaldoBanco = `${r.movimientoId}|${(r.cfdiUuid || '').toUpperCase()}|${(r.facturaUuid || '').toUpperCase()}`;
    r.tipoNC     = nc ? [...nc.tipos].sort().join(', ') : null;
    r.montoNC    = nc ? Math.round(nc.monto * 100) / 100 : null;
    r.saldoBanco = r.movimientoId ? saldosBanco.get(claveSaldoBanco) ?? null : null;
  }

  const workbook  = new ExcelJS.Workbook();
  const sheet     = workbook.addWorksheet('Pagos Asociados');

  sheet.columns = [
    { header: 'UUID CFDI Pago',    key: 'cfdiUuid',         width: 38 },
    { header: 'Estado SAT',        key: 'satStatus',         width: 12 },
    { header: 'Fecha Pago',        key: 'fechaPago',         width: 14 },
    { header: 'UUID Factura',       key: 'facturaUuid',       width: 38 },
    { header: 'Serie',              key: 'serie',             width: 8  },
    { header: 'Folio',              key: 'folio',             width: 12 },
    { header: 'Parcialidad',        key: 'numParcialidad',    width: 12 },
    { header: 'Depósito',           key: 'deposito',          width: 16 },
    { header: 'Saldo Anterior',     key: 'impSaldoAnt',  width: 16 },
    { header: 'Imp. Pagado',        key: 'impPagado',         width: 16 },
    { header: 'Saldo Insoluto',     key: 'impSaldoInsoluto',  width: 16 },
    { header: 'Diferencia',         key: 'diferencia',        width: 16 },
    { header: 'Tipo NC',            key: 'tipoNC',            width: 20 },
    { header: 'Monto NC',           key: 'montoNC',           width: 16 },
    { header: 'Tiene Pago',         key: 'tienePago',         width: 12 },
    { header: 'Banco',              key: 'banco',             width: 20 },
    { header: 'Fecha Movimiento',   key: 'movFecha',          width: 16 },
    { header: 'ID NUMO',             key: 'movFolio',          width: 16 },
    { header: 'Núm. Autorización',  key: 'numOperacion',      width: 22 },
    { header: 'Saldo Banco',        key: 'saldoBanco',        width: 18 },
    { header: 'Identificado por',   key: 'identificadoPor',   width: 20 },
  ];

  // Estilo encabezado
  sheet.getRow(1).eachCell(cell => {
    cell.fill   = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1E3A5F' } };
    cell.font   = { bold: true, color: { argb: 'FFFFFFFF' }, size: 10 };
    cell.border = { bottom: { style: 'thin', color: { argb: 'FF3B82F6' } } };
    cell.alignment = { horizontal: 'center', vertical: 'middle' };
  });
  sheet.getRow(1).height = 22;

  const mxn = { numFmt: '"$"#,##0.00' };
  const fecha = { numFmt: 'dd/mm/yyyy' };

  rows.forEach((r, idx) => {
    const row = sheet.addRow({
      cfdiUuid:         r.cfdiUuid,
      satStatus:        r.satStatus || '—',
      fechaPago:        r.fechaPago ? new Date(r.fechaPago) : null,
      facturaUuid:      r.facturaUuid,
      serie:            r.serie || '',
      folio:            r.folio || '',
      numParcialidad:   r.numParcialidad,
      impPagado:        r.impPagado,
      impSaldoAnt: r.impSaldoAnt ?? null,
      impSaldoInsoluto: r.impSaldoInsoluto,
      tipoNC:           r.tipoNC || '—',
      montoNC:          r.montoNC ?? null,
      tienePago:        r.tienePago ? 'Sí' : 'No',
      banco:            r.banco || '—',
      movFecha:         r.movFecha ? new Date(r.movFecha) : null,
      movFolio:         r.movFolio || '—',
      deposito:         r.deposito ?? null,
      numOperacion:     r.numOperacion || '—',
      diferencia:       r.diferencia,
      saldoBanco:       r.saldoBanco ?? null,
      identificadoPor:  r.identificadoPor || '—',
    });

    // Formato moneda y fecha
    row.getCell('impPagado').numFmt        = mxn.numFmt;
    row.getCell('impSaldoAnt').numFmt = mxn.numFmt;
    row.getCell('impSaldoInsoluto').numFmt = mxn.numFmt;
    row.getCell('montoNC').numFmt          = mxn.numFmt;
    row.getCell('deposito').numFmt         = mxn.numFmt;
    row.getCell('diferencia').numFmt       = mxn.numFmt;
    row.getCell('saldoBanco').numFmt       = mxn.numFmt;
    row.getCell('fechaPago').numFmt        = fecha.numFmt;
    row.getCell('movFecha').numFmt         = fecha.numFmt;

    // Color fila sin pago
    if (!r.tienePago) {
      row.eachCell(cell => { cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFEF2F2' } }; });
    } else if (r.diferencia !== 0) {
      row.eachCell(cell => { cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFFBEB' } }; });
    }

    // Alternar fila par
    if (!r.tienePago || r.diferencia === 0) {
      if (idx % 2 === 0 && r.tienePago) {
        row.eachCell(cell => { cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF8FAFC' } }; });
      }
    }

    // Diferencia negativa en rojo
    const difCell = row.getCell('diferencia');
    if (r.diferencia > 0)       difCell.font = { color: { argb: 'FFB91C1C' }, bold: true };
    else if (r.diferencia < 0)  difCell.font = { color: { argb: 'FF15803D' }, bold: true };
  });

  // Fila de totales — saldoBanco es un saldo CORRIENTE (baja con cada
  // aplicación), no un total fijo por fila: se suma una sola vez por
  // depósito (movimientoId), tomando el valor de la aplicación más
  // reciente (fechaPago), que es el saldo final real de ese depósito.
  const saldoBancoPorMovimiento = new Map(); // movimientoId -> { fecha, saldo }
  for (const r of rows) {
    if (!r.movimientoId) continue;
    const key    = String(r.movimientoId);
    const fecha  = r.fechaPago ? new Date(r.fechaPago).getTime() : 0;
    const actual = saldoBancoPorMovimiento.get(key);
    if (!actual || fecha >= actual.fecha) {
      saldoBancoPorMovimiento.set(key, { fecha, saldo: r.saldoBanco ?? 0 });
    }
  }
  const totalRow = sheet.addRow({
    cfdiUuid:    'TOTALES',
    impPagado:   rows.reduce((s, r) => s + (r.impPagado || 0), 0),
    deposito:    rows.reduce((s, r) => s + (r.deposito  || 0), 0),
    diferencia:  rows.reduce((s, r) => s + (r.diferencia || 0), 0),
    saldoBanco:  [...saldoBancoPorMovimiento.values()].reduce((s, v) => s + v.saldo, 0),
  });
  totalRow.eachCell(cell => {
    cell.font   = { bold: true };
    cell.fill   = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFE2E8F0' } };
    cell.border = { top: { style: 'medium' } };
  });
  totalRow.getCell('impPagado').numFmt  = mxn.numFmt;
  totalRow.getCell('deposito').numFmt   = mxn.numFmt;
  totalRow.getCell('diferencia').numFmt = mxn.numFmt;
  totalRow.getCell('saldoBanco').numFmt = mxn.numFmt;

  sheet.autoFilter = { from: 'A1', to: 'Q1' };

  const label = estado !== 'todos' ? `_${estado}` : '';
  const per   = periodo ? `_${periodo}` : '';
  const ej    = ejercicio ? `_${ejercicio}` : '';
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', `attachment; filename="pagos_banco${ej}${per}${label}_${Date.now()}.xlsx"`);
  await workbook.xlsx.write(res);
  res.end();
});

/**
 * GET /api/reports/pagos-banco/detalle?facturaUuid=XXX
 * Devuelve el satStatus de la factura y los movimientos bancarios vinculados.
 * Se llama solo al abrir el panel de detalle de una fila.
 */
const pagosBancoDetalle = asyncHandler(async (req, res) => {
  const { facturaUuid } = req.query;
  if (!facturaUuid) return res.status(400).json({ error: 'facturaUuid requerido' });

  const uuid = facturaUuid.trim().toUpperCase();

  const [factura, movimientos, parcialidades] = await Promise.all([
    CFDI.findOne({ uuid }).select('uuid satStatus serie folio total fecha emisor receptor informacionGlobal').lean(),
    CFDI.db.collection('bank_movements').find(
      { isActive: true, 'erpLinks.folioFiscal': uuid },
      { projection: { banco: 1, fecha: 1, deposito: 1, retiro: 1, folio: 1, concepto: 1, status: 1, numeroAutorizacion: 1, referenciaNumerica: 1, erpLinks: 1 } },
    ).toArray(),
    // Todos los CFDI-P que referencian esta factura, ordenados por parcialidad
    CFDI.aggregate([
      {
        $match: {
          tipoDeComprobante: 'P',
          isActive: true,
          'complementoPago.pagos.doctosRelacionados.idDocumento': { $regex: uuid, $options: 'i' },
        },
      },
      ...DEDUP_PAGO_PREFIERE_SAT,
      { $unwind: '$complementoPago.pagos' },
      { $unwind: '$complementoPago.pagos.doctosRelacionados' },
      {
        $match: {
          $expr: { $eq: [{ $toUpper: '$complementoPago.pagos.doctosRelacionados.idDocumento' }, uuid] },
        },
      },
      {
        $project: {
          _id:              0,
          serie:            '$serie',
          folio:            '$folio',
          fecha:            '$complementoPago.pagos.fechaPago',
          numParcialidad:   '$complementoPago.pagos.doctosRelacionados.numParcialidad',
          impSaldoAnt: '$complementoPago.pagos.doctosRelacionados.impSaldoAnt',
          impPagado:        '$complementoPago.pagos.doctosRelacionados.impPagado',
          impSaldoInsoluto: '$complementoPago.pagos.doctosRelacionados.impSaldoInsoluto',
        },
      },
      { $sort: { numParcialidad: 1, fecha: 1 } },
    ]),
  ]);

  // Cuentas por cobrar (CxC del ERP) afectadas por estos movimientos.
  const erpIds = [...new Set(
    movimientos.flatMap(m => (m.erpLinks ?? [])
      .filter(l => l.folioFiscal?.toUpperCase() === uuid)
      .map(l => l.erpId)),
  )];

  const [cuentasPorCobrarRaw, egresosRelacionados] = await Promise.all([
    buscarCuentasPorCobrarConMovimientos(erpIds),
    buscarEgresosRelacionados(uuid),
  ]);
  const cuentasPorCobrar = enriquecerConNotasDeCredito(cuentasPorCobrarRaw, egresosRelacionados, factura, movimientos);

  res.json({ factura: factura || null, movimientos, parcialidades, cuentasPorCobrar, egresosRelacionados, facturaEsGlobal: esFacturaGlobal(factura) });
});

/**
 * GET /api/reports/pagos-banco/bancos
 * Devuelve lista de bancos distintos en bank_movements activos.
 */
const pagosBancosDistintos = asyncHandler(async (req, res) => {
  const bancos = await CFDI.db.collection('bank_movements')
    .distinct('banco', { isActive: true });
  res.json(bancos.filter(Boolean).sort());
});

/**
 * GET /api/reports/pagos-banco/contexto-banco?banco=BBVA&fecha=2026-05-10&folio=000123&limit=10
 * Devuelve los últimos N movimientos del banco dado hasta la fecha indicada,
 * en orden cronológico, marcando con esFoco:true la fila cuyo folio coincide.
 */
const pagosBancoContextoBanco = asyncHandler(async (req, res) => {
  const { banco, fecha, folio, limit = '10' } = req.query;
  if (!banco || !fecha) return res.status(400).json({ error: 'banco y fecha son requeridos' });

  const lm      = Math.min(Math.max(parseInt(limit) || 10, 1), 50);
  const fechaLte = new Date(fecha);
  // Incluir movimientos del mismo día aunque sean posteriores en hora (hasta fin del día)
  fechaLte.setUTCHours(23, 59, 59, 999);

  // Traer lm+1 para detectar si hay más anteriores fuera de la ventana
  const docs = await CFDI.db.collection('bank_movements')
    .find(
      { banco, isActive: true, fecha: { $lte: fechaLte } },
      { projection: { folio: 1, fecha: 1, concepto: 1, deposito: 1, retiro: 1, saldo: 1, status: 1 } },
    )
    .sort({ fecha: -1, _id: -1 })
    .limit(lm + 1)
    .toArray();

  const hayMasAnteriores = docs.length > lm;
  const slice = docs.slice(0, lm).reverse(); // cronológico ascendente

  const movimientos = slice.map(m => ({
    folio:    m.folio   ?? null,
    fecha:    m.fecha,
    concepto: m.concepto ?? null,
    deposito: m.deposito ?? null,
    retiro:   m.retiro   ?? null,
    saldo:    m.saldo    ?? null,
    status:   m.status,
    esFoco:   folio ? m.folio === folio : false,
  }));

  res.json({ movimientos, hayMasAnteriores });
});

/**
 * GET /api/reports/depositos-ingresos
 * Relaciona facturas de Ingreso con su depósito bancario real, vía
 * bank_movements.erpLinks.folioFiscal (el ERP liga el depósito directo al UUID
 * de la factura de Ingreso, sin pasar por un complemento de pago — a diferencia
 * de pagosBanco, que sí necesita el $unwind de doctosRelacionados).
 * Pestañas Contado/Crédito = filtro `tipoVenta`, mismo criterio que el export
 * a CONTPAQ: metodoPago!=='PPD' → Contado, ==='PPD' → Crédito.
 */
const depositosIngresos = asyncHandler(async (req, res) => {
  const {
    uuid, serie, folio, banco,
    numAutorizacion, idNumo,
    serieCxc, folioCxc,
    fechaInicio, fechaFin,
    ejercicio, periodo,
    tipoVenta      = 'todos',
    tieneDeposito  = 'todos', // 'todos' | 'con_deposito' | 'sin_deposito'
    page  = 1,
    limit = 20,
  } = req.query;

  const pg = parseInt(page);
  const lm = Math.min(parseInt(limit), 100);

  const baseMatch = { tipoDeComprobante: 'I', source: 'ERP', satStatus: 'Vigente', isActive: true };
  if (ejercicio) baseMatch.ejercicio = parseInt(ejercicio);
  if (periodo)   baseMatch.periodo   = parseInt(periodo);
  if (uuid)  baseMatch.uuid  = { $regex: uuid.trim().toUpperCase(), $options: 'i' };
  if (serie) baseMatch.serie = { $regex: serie.trim(), $options: 'i' };
  if (folio) baseMatch.folio = { $regex: folio.trim(), $options: 'i' };
  if (fechaInicio || fechaFin) {
    baseMatch.fecha = {};
    if (fechaInicio) baseMatch.fecha.$gte = new Date(fechaInicio);
    if (fechaFin) {
      const fin = new Date(fechaFin);
      fin.setUTCDate(fin.getUTCDate() + 1);
      baseMatch.fecha.$lt = fin;
    }
  }

  const tipoVentaMatch = {};
  if (tipoVenta === 'contado') tipoVentaMatch.tipoVenta = 'Contado';
  if (tipoVenta === 'credito') tipoVentaMatch.tipoVenta = 'Credito';

  const depositoMatch = {};
  if (tieneDeposito === 'con_deposito') depositoMatch['movimientos.0'] = { $exists: true };
  if (tieneDeposito === 'sin_deposito') depositoMatch.movimientos = { $size: 0 };

  // La CxC tiene DOS numeraciones distintas (serie/folio interno del ERP y
  // serieExterna/folioExterno) y erpLinks solo guarda serie + folioExterno.
  // Para no depender de cuál usó el usuario, resolvemos primero los erpId
  // que coinciden por CUALQUIERA de las dos numeraciones.
  const erpIdsCxc = (serieCxc || folioCxc)
    ? (await CFDI.db.collection('erp_cuentas_pendientes').find({
        $or: [
          {
            ...(serieCxc ? { serie: { $regex: serieCxc.trim(), $options: 'i' } } : {}),
            ...(folioCxc ? { folio: { $regex: folioCxc.trim(), $options: 'i' } } : {}),
          },
          {
            ...(serieCxc ? { serieExterna: { $regex: serieCxc.trim(), $options: 'i' } } : {}),
            ...(folioCxc ? { folioExterno: { $regex: folioCxc.trim(), $options: 'i' } } : {}),
          },
        ],
      }, { projection: { erpId: 1 } }).toArray()).map(c => c.erpId)
    : null;

  const movimientosFilterCond = {
    $and: [
      { $eq: ['$$m.isActive', true] },
      ...(banco ? [{ $regexMatch: { input: { $ifNull: ['$$m.banco', ''] }, regex: banco.trim(), options: 'i' } }] : []),
      ...(numAutorizacion ? [{ $or: [
        { $regexMatch: { input: { $ifNull: [{ $toString: '$$m.numeroAutorizacion' }, ''] }, regex: numAutorizacion.trim(), options: 'i' } },
        { $regexMatch: { input: { $ifNull: [{ $toString: '$$m.referenciaNumerica' }, ''] }, regex: numAutorizacion.trim(), options: 'i' } },
      ] }] : []),
      ...(idNumo ? [{ $regexMatch: { input: { $ifNull: [{ $toString: '$$m.folio' }, ''] }, regex: idNumo.trim(), options: 'i' } }] : []),
      ...(erpIdsCxc ? [{
        $gt: [{
          $size: {
            $filter: {
              input: { $ifNull: ['$$m.erpLinks', []] },
              as: 'l',
              cond: { $in: ['$$l.erpId', erpIdsCxc] },
            },
          },
        }, 0],
      }] : []),
    ],
  };

  const pipeline = [
    { $match: baseMatch },
    // erpLinks.folioFiscal tiene case inconsistente en los datos (ver mismo
    // comentario en pagosBanco); se generan variantes para no perder el match.
    { $addFields: { _uuidVariants: ['$uuid', { $toUpper: '$uuid' }, { $toLower: '$uuid' }] } },
    { $lookup: { from: 'bank_movements', localField: '_uuidVariants', foreignField: 'erpLinks.folioFiscal', as: 'movimientos' } },
    {
      $addFields: {
        tipoVenta: { $cond: [{ $eq: ['$metodoPago', 'PPD'] }, 'Credito', 'Contado'] },
        movimientos: {
          $map: {
            input: { $filter: { input: '$movimientos', as: 'm', cond: movimientosFilterCond } },
            as: 'm',
            in: {
              banco:        '$$m.banco',
              fecha:        '$$m.fecha',
              deposito:     '$$m.deposito',
              folio:        '$$m.folio',
              concepto:     '$$m.concepto',
              numOperacion: { $ifNull: ['$$m.numeroAutorizacion', '$$m.referenciaNumerica'] },
              // Ver comentario completo en pagosBanco — misma jerarquía que
              // aplicarLogicaErp() en bank.service.js (saldoPagadoTotal → saldoPagado
              // → legado saldoActual/total). NO usar saldoActual a secas.
              saldoMovimiento: {
                $let: {
                  vars: {
                    link: {
                      $arrayElemAt: [{
                        $filter: {
                          input: { $ifNull: ['$$m.erpLinks', []] },
                          as: 'l',
                          cond: { $eq: [{ $toLower: { $ifNull: ['$$l.folioFiscal', ''] } }, { $toLower: '$uuid' }] },
                        },
                      }, 0],
                    },
                  },
                  in: {
                    $cond: [
                      { $ne: ['$$link.saldoPagadoTotal', null] },
                      '$$link.saldoPagadoTotal',
                      {
                        $cond: [
                          { $ne: ['$$link.saldoPagado', null] },
                          '$$link.saldoPagado',
                          {
                            $cond: [
                              { $and: [{ $ne: ['$$link.saldoActual', null] }, { $gt: ['$$link.saldoActual', 0] }] },
                              '$$link.saldoActual',
                              { $ifNull: ['$$link.total', 0] },
                            ],
                          },
                        ],
                      },
                    ],
                  },
                },
              },
            },
          },
        },
      },
    },
    ...((banco || numAutorizacion || idNumo || serieCxc || folioCxc) ? [{ $match: { 'movimientos.0': { $exists: true } } }] : []),
    ...(Object.keys(depositoMatch).length ? [{ $match: depositoMatch }] : []),
    {
      $facet: {
        data: [
          ...(Object.keys(tipoVentaMatch).length ? [{ $match: tipoVentaMatch }] : []),
          { $sort: { fecha: -1 } },
          { $skip: (pg - 1) * lm },
          { $limit: lm },
          {
            $project: {
              _id:          0,
              cfdiUuid:     '$uuid',
              satStatus:    '$satStatus',
              tipoVenta:      1,
              metodoPago:     1,
              serie:          1,
              folio:          1,
              fecha:          1,
              total:          1,
              tienePago:      { $gt: [{ $size: '$movimientos' }, 0] },
              numMovimientos: { $size: '$movimientos' },
              // Con más de un depósito vinculado (anticipos acumulados, etc.), el
              // primero es solo referencia — el monto real es la SUMA de todos.
              banco:          { $arrayElemAt: ['$movimientos.banco',        0] },
              movFecha:       { $arrayElemAt: ['$movimientos.fecha',        0] },
              movFolio:       { $arrayElemAt: ['$movimientos.folio',        0] },
              movConcepto:    { $arrayElemAt: ['$movimientos.concepto',     0] },
              numOperacion:   { $arrayElemAt: ['$movimientos.numOperacion', 0] },
              deposito:       { $round: [{ $sum: '$movimientos.deposito' }, 2] },
              diferencia:     { $round: [{ $subtract: ['$total', { $sum: '$movimientos.deposito' }] }, 2] },
              saldoMovimiento: { $arrayElemAt: ['$movimientos.saldoMovimiento', 0] },
            },
          },
        ],
        resumenAgg: [
          { $group: { _id: '$tipoVenta', cantidad: { $sum: 1 }, monto: { $sum: '$total' } } },
        ],
        totalAgg: [
          ...(Object.keys(tipoVentaMatch).length ? [{ $match: tipoVentaMatch }] : []),
          { $count: 'count' },
        ],
      },
    },
  ];

  const [result] = await CFDI.aggregate(pipeline).allowDiskUse(true);
  const total = result.totalAgg[0]?.count ?? 0;

  const resumen = { contado: { cantidad: 0, monto: 0 }, credito: { cantidad: 0, monto: 0 } };
  for (const t of result.resumenAgg) {
    const key = t._id === 'Credito' ? 'credito' : 'contado';
    resumen[key] = { cantidad: t.cantidad, monto: Math.round(t.monto * 100) / 100 };
  }

  res.json({ data: result.data, total, page: pg, limit: lm, pages: Math.ceil(total / lm), resumen });
});

/**
 * GET /api/reports/depositos-ingresos/detalle?facturaUuid=XXX
 * Devuelve la factura y los movimientos bancarios vinculados (sin parcialidades
 * — eso es un concepto de Pagos, no aplica a una factura de Ingreso).
 */
const depositosIngresosDetalle = asyncHandler(async (req, res) => {
  const { facturaUuid } = req.query;
  if (!facturaUuid) return res.status(400).json({ error: 'facturaUuid requerido' });

  const uuid = facturaUuid.trim().toUpperCase();

  const [factura, movimientos] = await Promise.all([
    CFDI.findOne({ uuid, tipoDeComprobante: 'I', source: 'ERP' }).select('uuid satStatus serie folio total fecha metodoPago emisor receptor informacionGlobal').lean(),
    CFDI.db.collection('bank_movements').find(
      { isActive: true, 'erpLinks.folioFiscal': uuid },
      { projection: { banco: 1, fecha: 1, deposito: 1, retiro: 1, folio: 1, concepto: 1, status: 1, numeroAutorizacion: 1, referenciaNumerica: 1, erpLinks: 1 } },
    ).sort({ fecha: -1 }).toArray(),
  ]);

  // Cuentas por cobrar (CxC del ERP) afectadas por estos movimientos — para
  // mostrarlas en el detalle sin que el usuario tenga que salir a otra pantalla.
  const erpIds = [...new Set(
    movimientos.flatMap(m => (m.erpLinks ?? [])
      .filter(l => l.folioFiscal?.toUpperCase() === uuid)
      .map(l => l.erpId)),
  )];

  const [cuentasPorCobrarRaw, egresosRelacionados] = await Promise.all([
    buscarCuentasPorCobrarConMovimientos(erpIds),
    buscarEgresosRelacionados(uuid),
  ]);
  const cuentasPorCobrar = enriquecerConNotasDeCredito(cuentasPorCobrarRaw, egresosRelacionados, factura, movimientos);

  res.json({ factura: factura || null, movimientos, cuentasPorCobrar, egresosRelacionados, facturaEsGlobal: esFacturaGlobal(factura) });
});

/**
 * GET /api/reports/depositos-ingresos/export
 * Descarga Excel con los mismos filtros que depositosIngresos (sin paginación).
 */
const depositosIngresosExport = asyncHandler(async (req, res) => {
  const { uuid, serie, folio, banco, numAutorizacion, idNumo, serieCxc, folioCxc, fechaInicio, fechaFin, ejercicio, periodo, tipoVenta = 'todos', tieneDeposito = 'todos' } = req.query;

  const baseMatch = { tipoDeComprobante: 'I', source: 'ERP', satStatus: 'Vigente', isActive: true };
  if (ejercicio) baseMatch.ejercicio = parseInt(ejercicio);
  if (periodo)   baseMatch.periodo   = parseInt(periodo);
  if (uuid)  baseMatch.uuid  = { $regex: uuid.trim().toUpperCase(), $options: 'i' };
  if (serie) baseMatch.serie = { $regex: serie.trim(), $options: 'i' };
  if (folio) baseMatch.folio = { $regex: folio.trim(), $options: 'i' };
  if (fechaInicio || fechaFin) {
    baseMatch.fecha = {};
    if (fechaInicio) baseMatch.fecha.$gte = new Date(fechaInicio);
    if (fechaFin) {
      const fin = new Date(fechaFin);
      fin.setUTCDate(fin.getUTCDate() + 1);
      baseMatch.fecha.$lt = fin;
    }
  }

  const tipoVentaMatch = {};
  if (tipoVenta === 'contado') tipoVentaMatch.tipoVenta = 'Contado';
  if (tipoVenta === 'credito') tipoVentaMatch.tipoVenta = 'Credito';

  const depositoMatch = {};
  if (tieneDeposito === 'con_deposito') depositoMatch['movimientos.0'] = { $exists: true };
  if (tieneDeposito === 'sin_deposito') depositoMatch.movimientos = { $size: 0 };

  // Ver nota en depositosIngresos: la CxC tiene dos numeraciones (interna y
  // externa) y erpLinks solo guarda serie + folioExterno, así que resolvemos
  // el erpId primero para aceptar cualquiera de las dos numeraciones.
  const erpIdsCxc = (serieCxc || folioCxc)
    ? (await CFDI.db.collection('erp_cuentas_pendientes').find({
        $or: [
          {
            ...(serieCxc ? { serie: { $regex: serieCxc.trim(), $options: 'i' } } : {}),
            ...(folioCxc ? { folio: { $regex: folioCxc.trim(), $options: 'i' } } : {}),
          },
          {
            ...(serieCxc ? { serieExterna: { $regex: serieCxc.trim(), $options: 'i' } } : {}),
            ...(folioCxc ? { folioExterno: { $regex: folioCxc.trim(), $options: 'i' } } : {}),
          },
        ],
      }, { projection: { erpId: 1 } }).toArray()).map(c => c.erpId)
    : null;

  const movimientosFilterCond = {
    $and: [
      { $eq: ['$$m.isActive', true] },
      ...(banco ? [{ $regexMatch: { input: { $ifNull: ['$$m.banco', ''] }, regex: banco.trim(), options: 'i' } }] : []),
      ...(numAutorizacion ? [{ $or: [
        { $regexMatch: { input: { $ifNull: [{ $toString: '$$m.numeroAutorizacion' }, ''] }, regex: numAutorizacion.trim(), options: 'i' } },
        { $regexMatch: { input: { $ifNull: [{ $toString: '$$m.referenciaNumerica' }, ''] }, regex: numAutorizacion.trim(), options: 'i' } },
      ] }] : []),
      ...(idNumo ? [{ $regexMatch: { input: { $ifNull: [{ $toString: '$$m.folio' }, ''] }, regex: idNumo.trim(), options: 'i' } }] : []),
      ...(erpIdsCxc ? [{
        $gt: [{
          $size: {
            $filter: {
              input: { $ifNull: ['$$m.erpLinks', []] },
              as: 'l',
              cond: { $in: ['$$l.erpId', erpIdsCxc] },
            },
          },
        }, 0],
      }] : []),
    ],
  };

  const pipeline = [
    { $match: baseMatch },
    // erpLinks.folioFiscal tiene case inconsistente en los datos (ver mismo
    // comentario en pagosBanco); se generan variantes para no perder el match.
    { $addFields: { _uuidVariants: ['$uuid', { $toUpper: '$uuid' }, { $toLower: '$uuid' }] } },
    { $lookup: { from: 'bank_movements', localField: '_uuidVariants', foreignField: 'erpLinks.folioFiscal', as: 'movimientos' } },
    {
      $addFields: {
        tipoVenta: { $cond: [{ $eq: ['$metodoPago', 'PPD'] }, 'Credito', 'Contado'] },
        movimientos: {
          $map: {
            input: { $filter: { input: '$movimientos', as: 'm', cond: movimientosFilterCond } },
            as: 'm',
            in: {
              banco:        '$$m.banco',
              fecha:        '$$m.fecha',
              deposito:     '$$m.deposito',
              folio:        '$$m.folio',
              numOperacion: { $ifNull: ['$$m.numeroAutorizacion', '$$m.referenciaNumerica'] },
            },
          },
        },
      },
    },
    ...((banco || numAutorizacion || idNumo || serieCxc || folioCxc) ? [{ $match: { 'movimientos.0': { $exists: true } } }] : []),
    ...(Object.keys(depositoMatch).length ? [{ $match: depositoMatch }] : []),
    ...(Object.keys(tipoVentaMatch).length ? [{ $match: tipoVentaMatch }] : []),
    { $sort: { fecha: -1 } },
    { $limit: 50000 },
    {
      $project: {
        _id:          0,
        cfdiUuid:       '$uuid',
        satStatus:      '$satStatus',
        tipoVenta:      1,
        serie:          1,
        folio:          1,
        fecha:          1,
        total:          1,
        tienePago:      { $gt: [{ $size: '$movimientos' }, 0] },
        numMovimientos: { $size: '$movimientos' },
        banco:          { $arrayElemAt: ['$movimientos.banco',        0] },
        movFecha:       { $arrayElemAt: ['$movimientos.fecha',        0] },
        movFolio:       { $arrayElemAt: ['$movimientos.folio',        0] },
        numOperacion:   { $arrayElemAt: ['$movimientos.numOperacion', 0] },
        deposito:       { $round: [{ $sum: '$movimientos.deposito' }, 2] },
        diferencia:     { $round: [{ $subtract: ['$total', { $sum: '$movimientos.deposito' }] }, 2] },
      },
    },
  ];

  const rows = await CFDI.aggregate(pipeline).allowDiskUse(true);

  const workbook = new ExcelJS.Workbook();
  const sheet    = workbook.addWorksheet('Depósitos Ingresos');

  sheet.columns = [
    { header: 'UUID Factura',      key: 'cfdiUuid',     width: 38 },
    { header: 'Estado SAT',        key: 'satStatus',     width: 12 },
    { header: 'Tipo de Venta',     key: 'tipoVenta',     width: 12 },
    { header: 'Serie',             key: 'serie',         width: 8  },
    { header: 'Folio',             key: 'folio',         width: 12 },
    { header: 'Fecha',             key: 'fecha',         width: 14 },
    { header: 'Total',             key: 'total',         width: 16 },
    { header: 'Tiene Depósito',    key: 'tienePago',     width: 14 },
    { header: 'Movimientos',       key: 'numMovimientos', width: 12 },
    { header: 'Banco',             key: 'banco',         width: 20 },
    { header: 'Fecha Movimiento',  key: 'movFecha',       width: 16 },
    { header: 'ID NUMO',           key: 'movFolio',       width: 16 },
    { header: 'Depósito (suma)',   key: 'deposito',       width: 16 },
    { header: 'Núm. Autorización', key: 'numOperacion',   width: 22 },
    { header: 'Diferencia',        key: 'diferencia',     width: 16 },
  ];

  sheet.getRow(1).eachCell(cell => {
    cell.fill   = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1E3A5F' } };
    cell.font   = { bold: true, color: { argb: 'FFFFFFFF' }, size: 10 };
    cell.border = { bottom: { style: 'thin', color: { argb: 'FF3B82F6' } } };
    cell.alignment = { horizontal: 'center', vertical: 'middle' };
  });
  sheet.getRow(1).height = 22;

  const mxn   = { numFmt: '"$"#,##0.00' };
  const fecha = { numFmt: 'dd/mm/yyyy' };

  rows.forEach((r, idx) => {
    const row = sheet.addRow({
      cfdiUuid:     r.cfdiUuid,
      satStatus:    r.satStatus || '—',
      tipoVenta:    r.tipoVenta,
      serie:        r.serie || '',
      folio:        r.folio || '',
      fecha:        r.fecha ? new Date(r.fecha) : null,
      total:          r.total,
      tienePago:      r.tienePago ? 'Sí' : 'No',
      numMovimientos: r.numMovimientos || 0,
      banco:          r.banco || '—',
      movFecha:       r.movFecha ? new Date(r.movFecha) : null,
      movFolio:       r.movFolio || '—',
      deposito:       r.deposito ?? null,
      numOperacion:   r.numOperacion || '—',
      diferencia:     r.diferencia,
    });

    row.getCell('total').numFmt        = mxn.numFmt;
    row.getCell('deposito').numFmt     = mxn.numFmt;
    row.getCell('diferencia').numFmt   = mxn.numFmt;
    row.getCell('fecha').numFmt        = fecha.numFmt;
    row.getCell('movFecha').numFmt     = fecha.numFmt;

    if (!r.tienePago) {
      row.eachCell(cell => { cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFEF2F2' } }; });
    } else if (r.diferencia !== 0) {
      row.eachCell(cell => { cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFFBEB' } }; });
    } else if (idx % 2 === 0) {
      row.eachCell(cell => { cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF8FAFC' } }; });
    }

    const difCell = row.getCell('diferencia');
    if (r.diferencia > 0)      difCell.font = { color: { argb: 'FFB91C1C' }, bold: true };
    else if (r.diferencia < 0) difCell.font = { color: { argb: 'FF15803D' }, bold: true };
  });

  const totalRow = sheet.addRow({
    cfdiUuid:   'TOTALES',
    total:      rows.reduce((s, r) => s + (r.total || 0), 0),
    deposito:   rows.reduce((s, r) => s + (r.deposito || 0), 0),
    diferencia: rows.reduce((s, r) => s + (r.diferencia || 0), 0),
  });
  totalRow.eachCell(cell => {
    cell.font   = { bold: true };
    cell.fill   = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFE2E8F0' } };
    cell.border = { top: { style: 'medium' } };
  });
  totalRow.getCell('total').numFmt      = mxn.numFmt;
  totalRow.getCell('deposito').numFmt   = mxn.numFmt;
  totalRow.getCell('diferencia').numFmt = mxn.numFmt;

  sheet.autoFilter = { from: 'A1', to: 'O1' };

  const label = tipoVenta !== 'todos' ? `_${tipoVenta}` : '';
  const per   = periodo   ? `_${periodo}`   : '';
  const ej    = ejercicio ? `_${ejercicio}` : '';
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', `attachment; filename="depositos_ingresos${ej}${per}${label}_${Date.now()}.xlsx"`);
  await workbook.xlsx.write(res);
  res.end();
});

module.exports = { dashboard, exportExcel, discrepanciasMontos, satVigenteErpInactivo, discrepanciasCriticas, notInErp, pagosRelacionados, conciliacionExcel, clearDashboardCache, pagosBanco, pagosBancoDetalle, pagosBancoExport, pagosBancosDistintos, pagosBancoContextoBanco, depositosIngresos, depositosIngresosDetalle, depositosIngresosExport };
