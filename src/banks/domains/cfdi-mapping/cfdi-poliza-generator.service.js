'use strict';

const CFDI                 = require('../../../visor/models/CFDI');
const { PolizaMovimiento, AccountPlan, CfdiMappingRule, Poliza } = require('../../../shared/models/postgres');
const centrosSvc = require('../centros-costo/centros-costo.service');
const { Op, QueryTypes }   = require('sequelize');
const { sequelize }        = require('../../../config/database.postgres');
const mappingSvc           = require('./cfdi-mapping.service');
const { _getRulesActive, _enrichTasaIvaFromRelatedCfdis, _normalizarEgresoPue99, _normalizarEgresoCondonacion, _normalizarEgresoSegunFacturaRelacionada } = require('./balanza-preliminar.service');
const ErpCuentaPendiente   = require('../erp/ErpCuentaPendiente.model');
const { BadRequestError }          = require('../../shared/errors/AppError');
const { repararSubtotalDesdeXml }  = require('../../../visor/services/cfdiSubtotalRepair');

// Extrae los uuids de CFDIs relacionados de un CFDI — soporta tanto `uuids`
// (array, formato ERP) como `uuid` (singular, formato SAT), según el origen.
const _uuidsRelacionados = (cfdi) => (cfdi.cfdiRelacionados || []).flatMap(r => r.uuids ?? (r.uuid ? [r.uuid] : []));

/**
 * Enriquece en memoria el campo `tasaIvaInferida` de CFDIs tipo P Metadata
 * cuyos UUIDs relacionados se encuentran en erp_cuentas_pendientes.
 * Replica la misma lógica del bloque ERP en balanza-preliminar.service.js.
 * Muta los objetos del array — NO escribe a MongoDB.
 */
async function _enrichTasaIvaErp(cfdis) {
  const sinTasa = cfdis.filter(c =>
    c.tasaIvaInferida == null &&
    !c.complementoPago?.pagos?.length &&
    c.cfdiRelacionados?.length,
  );
  if (!sinTasa.length) return;

  const uuidToIdxs = new Map();
  for (let i = 0; i < sinTasa.length; i++) {
    const uuids = (sinTasa[i].cfdiRelacionados ?? [])
      .flatMap(r => r.uuids ?? [])
      .flatMap(u => u.split(/\s*\|\s*/))
      .map(u => u.trim().toUpperCase())
      .filter(u => u.length >= 32);
    for (const uuid of uuids) {
      if (!uuidToIdxs.has(uuid)) uuidToIdxs.set(uuid, []);
      uuidToIdxs.get(uuid).push(i);
    }
  }
  if (!uuidToIdxs.size) return;

  const erpDocs = await ErpCuentaPendiente.find(
    { folioFiscal: { $in: [...uuidToIdxs.keys()] } },
    { folioFiscal: 1, factorImpuesto: 1, impuesto: 1, subtotal: 1 },
  ).lean();
  if (!erpDocs.length) return;

  const tasasPorIdx = new Map();
  for (const erp of erpDocs) {
    const uuidNorm = (erp.folioFiscal || '').trim().toUpperCase();
    const tasa = erp.factorImpuesto != null
      ? (erp.factorImpuesto > 0 ? '16' : '0')
      : (erp.subtotal > 0 && erp.impuesto != null
          ? (erp.impuesto > 0 ? '16' : '0') : null);
    if (!tasa) continue;
    for (const idx of (uuidToIdxs.get(uuidNorm) ?? [])) {
      if (!tasasPorIdx.has(idx)) tasasPorIdx.set(idx, []);
      tasasPorIdx.get(idx).push(tasa);
    }
  }
  for (const [idx, tasas] of tasasPorIdx) {
    const tiene16 = tasas.some(t => t === '16' || t === 'mixto');
    const tiene0  = tasas.some(t => t === '0'  || t === 'mixto');
    sinTasa[idx].tasaIvaInferida =
      (tiene16 && tiene0) ? 'mixto' : tiene16 ? '16' : tiene0 ? '0' : null;
  }
}

/**
 * Busca las Notas de Crédito (tipo E, tipoRelacion 01/03 — devolución,
 * descuento, bonificación, Club Tuberos) relacionadas a las facturas de
 * Ingreso de este batch, para FUSIONARLAS en la misma póliza de Ingreso en
 * vez de generarse como póliza de Egreso aparte. Confirmado con el usuario:
 * las NC deben vivir dentro de la póliza de la venta que ajustan, no en una
 * póliza de Egreso independiente — así el bloque Contado/Crédito de
 * `poliza.service.js` las puede agrupar y colorear junto a esa venta.
 *
 * Excluye NC que ya tengan movimiento en una póliza activa (mismo criterio
 * que `uuidsYaUsados`, ya resuelto por el caller).
 *
 * fechaInicio/fechaFin (opcionales, generación por día): cuando se generan
 * pólizas por día, la NC debe vivir en la póliza de SU PROPIO día (fecha
 * efectiva ERP/SAT — confirmado con el usuario), no en la del día de la
 * factura que ajusta si cae en un día distinto. Sin fechaInicio/fechaFin
 * (generación de todo el periodo) no se filtra por fecha — comportamiento
 * previo sin cambios.
 *
 * centroCostoId (opcional, generación por sucursal): en modo por día, la NC
 * solo se fusiona si SU PROPIA serie pertenece a esta sucursal — si no, se
 * excluye aquí y se recoge cuando le toque generarse la póliza de su propia
 * sucursal. Requiere `ccBySerieMap` (mapa serie→centro ya resuelto por el
 * caller, se reutiliza para no pagar una consulta extra a Postgres).
 *
 * @param {Array} facturasI - CFDIs tipo I ya cargados/enriquecidos de este batch
 * @param {string} rfc
 * @param {Set<string>} uuidsYaUsados - uuids (mayúsculas) con póliza activa
 * @param {{ejercicio?: number, periodo?: number, fechaInicio?: string, fechaFin?: string, centroCostoId?: string|number, ccBySerieMap?: object}} [opts]
 */
async function _fetchNotasCreditoParaFusion(facturasI, rfc, uuidsYaUsados, opts = {}) {
  const { ejercicio, periodo, fechaInicio, fechaFin, centroCostoId, ccBySerieMap } = opts;
  const facturaUuids = facturasI.map(c => c.uuid).filter(Boolean);

  const relUuidsDe = (c) => (c.cfdiRelacionados ?? [])
    .filter(r => r.tipoRelacion === '01' || r.tipoRelacion === '03')
    .flatMap(r => r.uuids ?? (r.uuid ? [r.uuid] : []));

  const filtroBaseNc = {
    $or:               [{ 'emisor.rfc': rfc }, { 'receptor.rfc': rfc }],
    tipoDeComprobante: 'E',
    source:            'SAT',
    satStatus:         'Vigente',
    isActive:          true,
    'cfdiRelacionados.tipoRelacion': { $in: ['01', '03'] },
  };
  const selectNc = 'uuid tipoDeComprobante metodoPago formaPago fecha folio serie emisor receptor subTotal total descuento impuestos complementoPago conceptos cfdiRelacionados tasaIvaInferida';

  let ncs;
  if (fechaInicio && fechaFin) {
    // Generación por día: la NC debe vivir en la póliza de SU PROPIO día
    // (fecha efectiva ERP/SAT — confirmado con el usuario), sin importar si
    // la factura que ajusta cayó en un día distinto y por tanto no está en
    // `facturasI` de este batch. Por eso se busca DIRECTO por la fecha
    // efectiva de la NC, no partiendo de la relación con las facturas del
    // lote (que nunca encontraría una NC cuya factura ya se generó en otro
    // día).
    const uuidsNcDelDia = await _uuidsPorFechaEfectiva({ rfc, ejercicio, periodo, tipoCfdi: 'E', fechaInicio, fechaFin });
    if (!uuidsNcDelDia.size) return [];
    const ncsRaw = await CFDI.find({ ...filtroBaseNc, uuid: { $in: [...uuidsNcDelDia] } })
      .select(selectNc)
      .lean();
    ncs = ncsRaw.filter(nc => !uuidsYaUsados.has((nc.uuid || '').toUpperCase()));
    // Generación por sucursal (centroCostoId presente): la NC solo debe
    // fusionarse aquí si SU PROPIA serie pertenece a esta sucursal — si no,
    // se estaba colando en la póliza de una sucursal ajena (bug real
    // reportado: "seleccioné solo Atzompa y me manda de más sucursales").
    if (centroCostoId && ccBySerieMap) {
      ncs = ncs.filter(nc => String(ccBySerieMap[nc.serie]?.id ?? '') === String(centroCostoId));
    }
  } else {
    // Generación de todo el periodo: comportamiento original, por relación
    // con las facturas ya cargadas en este batch.
    if (!facturaUuids.length) return [];
    const ncsRaw = await CFDI.find(filtroBaseNc).select(selectNc).lean();
    const facturaSet = new Set(facturaUuids.map(u => u.toUpperCase()));
    ncs = ncsRaw.filter(nc =>
      !uuidsYaUsados.has((nc.uuid || '').toUpperCase()) &&
      relUuidsDe(nc).some(u => facturaSet.has((u || '').toUpperCase())),
    );
  }
  if (!ncs.length) return [];

  await repararSubtotalDesdeXml(ncs);

  // Enriquecer con ERP — mismo patrón que el resto del pipeline.
  const uuidsSinMeta = ncs
    .filter(c => !c.formaPago || !c.metodoPago || !c.conceptos?.length)
    .map(c => c.uuid);
  let erpMetaMap = {};
  if (uuidsSinMeta.length) {
    const erpCfdis = await CFDI.find({ uuid: { $in: uuidsSinMeta }, source: 'ERP' })
      .select('uuid formaPago metodoPago conceptos impuestos tipoOrigen cfdiRelacionados documentosRelacionados').lean();
    erpMetaMap = Object.fromEntries(erpCfdis.map(c => [c.uuid, c]));
  }
  const ncsEnriquecidas = ncs.map(cfdi => {
    const erp = erpMetaMap[cfdi.uuid];
    if (!erp) return cfdi;
    const satHasTraslados = cfdi.conceptos?.some(con => con.impuestos?.traslados?.length);
    const relSAT     = cfdi.cfdiRelacionados ?? [];
    const tiposEnSAT = new Set(relSAT.map(r => r.tipoRelacion));
    const relERP     = (erp.cfdiRelacionados ?? []).filter(r => !tiposEnSAT.has(r.tipoRelacion));
    const esBCT = erp.documentosRelacionados?.some(d => d.Serie === 'BCT');
    const esBON = !esBCT && erp.documentosRelacionados?.some(d => (d.Serie ?? '').startsWith('BON'));
    return {
      ...cfdi,
      formaPago:              cfdi.formaPago  || erp.formaPago,
      metodoPago:             cfdi.metodoPago || erp.metodoPago,
      conceptos:              satHasTraslados ? cfdi.conceptos : (erp.conceptos?.length ? erp.conceptos : cfdi.conceptos ?? []),
      impuestos:              erp.impuestos ?? cfdi.impuestos,
      tipoOrigen:             esBCT ? 'Bonificación Club Tuberos' : esBON ? 'Bonificación' : (cfdi.tipoOrigen ?? erp.tipoOrigen ?? null),
      documentosRelacionados: erp.documentosRelacionados ?? cfdi.documentosRelacionados ?? [],
      cfdiRelacionados:       relERP.length ? [...relSAT, ...relERP] : relSAT,
    };
  });

  _normalizarEgresoPue99(ncsEnriquecidas);

  // metodoPago/formaPago reales de la factura relacionada: primero se busca
  // entre las ya cargadas en este mismo batch (sin costo extra de query); lo
  // que falte (factura de otro periodo) se resuelve con una consulta puntual.
  // metodoPagoPorFactura (solo metodoPago) alimenta _normalizarEgresoCondonacion
  // (formaPago=15); facturaRelacionadaMeta (metodoPago+formaPago) alimenta
  // _normalizarEgresoSegunFacturaRelacionada (medios de pago reales).
  const metodoPagoPorFactura   = Object.fromEntries(facturasI.map(c => [(c.uuid || '').toUpperCase(), c.metodoPago]));
  const facturaRelacionadaMeta = Object.fromEntries(facturasI.map(c => [(c.uuid || '').toUpperCase(), { metodoPago: c.metodoPago, formaPago: c.formaPago }]));
  const faltantes = [...new Set(ncsEnriquecidas.flatMap(relUuidsDe))]
    .map(u => (u || '').toUpperCase())
    .filter(u => !(u in metodoPagoPorFactura));
  if (faltantes.length) {
    const extra = await CFDI.find({ uuid: { $in: faltantes } }).select('uuid metodoPago formaPago').lean();
    for (const f of extra) {
      const uuidUp = (f.uuid || '').toUpperCase();
      metodoPagoPorFactura[uuidUp]   = f.metodoPago;
      facturaRelacionadaMeta[uuidUp] = { metodoPago: f.metodoPago, formaPago: f.formaPago };
    }
  }
  _normalizarEgresoCondonacion(ncsEnriquecidas, metodoPagoPorFactura);
  _normalizarEgresoSegunFacturaRelacionada(ncsEnriquecidas, facturaRelacionadaMeta);

  return ncsEnriquecidas;
}

/**
 * Genera una PROPUESTA de póliza a partir de los CFDIs vigentes del periodo
 * que aún no tienen movimiento contable registrado.
 *
 * No guarda nada en base de datos — devuelve el objeto para que el
 * frontend lo muestre en el modal de revisión.
 */
const LIMITE_CFDIS = 500;
const CHUNK_SIZE   = 200;

async function generarPropuesta({ rfc, ejercicio, periodo, tipoPropuesta = 'D', tipoCfdi, centroCostoId, fechaInicio, fechaFin }) {
  if (!rfc)       throw new BadRequestError('RFC requerido');
  if (!ejercicio) throw new BadRequestError('Ejercicio requerido');
  if (!periodo)   throw new BadRequestError('Periodo requerido');
  if (!tipoCfdi)  throw new BadRequestError('Debes seleccionar el tipo de CFDI a procesar (I, E o P)');

  // 1. UUIDs ya contabilizados — solo los del RFC solicitado (JOIN con polizas)
  const yaContabilizados = await PolizaMovimiento.findAll({
    where:      { cfdiUuid: { [Op.ne]: null } },
    attributes: ['cfdiUuid'],
    include: [{
      model:      Poliza,
      as:         'poliza',
      attributes: [],
      where:      { rfc, estado: { [Op.ne]: 'cancelada' } },
      required:   true,
    }],
    raw: true,
  });
  const uuidsYaUsados = new Set(yaContabilizados.map(m => m.cfdiUuid));

  // 2. CFDIs vigentes del periodo filtrados por tipo
  // Proyección mínima: solo los campos que necesita cfdiToMovimientos
  // fechaInicio/fechaFin (opcionales): acotan el periodo a un rango de días
  // específico — usado por `generarYGuardarPorDia` y por el filtro manual de
  // fecha en la UI. Sin ellos, el comportamiento es el de siempre (mes completo).
  // El filtro usa la fecha EFECTIVA (ERP cuando existe, SAT si no — ver
  // `_uuidsPorFechaEfectiva`), no el `fecha` crudo de SAT.
  const uuidsPorFechaProp = (fechaInicio && fechaFin)
    ? await _uuidsPorFechaEfectiva({ rfc, ejercicio, periodo, tipoCfdi, fechaInicio, fechaFin })
    : null;
  const filtroBase = {
    $or:               [{ 'emisor.rfc': rfc }, { 'receptor.rfc': rfc }],
    ejercicio:         Number(ejercicio),
    periodo:           Number(periodo),
    tipoDeComprobante: tipoCfdi,
    source:            'SAT',
    satStatus:         'Vigente',
    ...(uuidsPorFechaProp ? { uuid: { $in: [...uuidsPorFechaProp] } } : {}),
    isActive:          true,
  };

  const totalEncontrados = await CFDI.countDocuments(filtroBase);
  if (totalEncontrados > LIMITE_CFDIS) {
    throw new BadRequestError(
      `Se encontraron ${totalEncontrados} CFDIs tipo ${tipoCfdi} en este periodo. ` +
      `El límite por operación es ${LIMITE_CFDIS}. Divide el proceso por fuente (ERP/SAT) ` +
      `o contacta a soporte para procesamiento por lotes.`,
    );
  }

  const cfdis = await CFDI.find(filtroBase)
    .select('uuid tipoDeComprobante metodoPago formaPago fecha folio serie emisor receptor subTotal total descuento impuestos complementoPago conceptos cfdiRelacionados lastComparisonStatus tasaIvaInferida')
    .lean();

  await repararSubtotalDesdeXml(cfdis);

  const cfdisSinPoliza = cfdis.filter(c => !uuidsYaUsados.has(c.uuid));

  if (cfdisSinPoliza.length === 0) {
    throw new BadRequestError('Todos los CFDIs vigentes del periodo ya tienen póliza registrada');
  }

  // 3. Cargar reglas activas (cacheadas 60s)
  const rules = await _getRulesActive();

  // 4. Pre-fetch tipoDeComprobante de CFDIs relacionados para discriminador relacionadoTipo
  // (r.uuid singular o r.uuids array — cfdiRelacionados usa ambas formas según el origen).
  const relTipoUuidsProp = [...new Set(
    cfdisSinPoliza
      .flatMap(c => (c.cfdiRelacionados || []).flatMap(r => r.uuids ?? (r.uuid ? [r.uuid] : []))),
  )];
  const relTipoCfdisArr = relTipoUuidsProp.length
    ? await CFDI.find({ uuid: { $in: relTipoUuidsProp } })
        .select('uuid tipoDeComprobante metodoPago formaPago').lean()
    : [];
  const relTipoMap = Object.fromEntries(relTipoCfdisArr.map(c => [c.uuid, c.tipoDeComprobante]));
  // uuid de factura → su metodoPago — usado por _normalizarEgresoCondonacion
  // para resolver el metodoPago real de NCs formaPago=15 (Condonación).
  const relMetodoPagoMap = Object.fromEntries(relTipoCfdisArr.map(c => [c.uuid, c.metodoPago]));
  // uuid de factura → metodoPago+formaPago — usado por
  // _normalizarEgresoSegunFacturaRelacionada (medios de pago reales).
  const relFacturaMetaMap = Object.fromEntries(relTipoCfdisArr.map(c => [c.uuid, { metodoPago: c.metodoPago, formaPago: c.formaPago }]));

  // Inyectar _relacionadoTipo en cada CFDI antes del matching
  const cfdisSinPolizaEnriquecidos = cfdisSinPoliza.map(cfdi => {
    const primerUuid = (cfdi.cfdiRelacionados || [])[0]?.uuid;
    return primerUuid && relTipoMap[primerUuid]
      ? { ...cfdi, _relacionadoTipo: relTipoMap[primerUuid] }
      : cfdi;
  });

  // Enriquecer CFDIs SAT con datos del homólogo ERP — misma lógica que balanza-preliminar
  // para que el matching de reglas produzca movimientos idénticos a la balanza.
  const uuidsSinMeta = new Set(
    cfdisSinPolizaEnriquecidos
      .filter(c => c.uuid && (
        !c.formaPago ||
        !c.metodoPago ||
        !c.conceptos?.length ||
        c.conceptos.every(con => !(con.impuestos?.traslados?.length)) ||
        (c.tipoDeComprobante === 'I' && c.metodoPago === 'PPD') ||
        // Enriquecer también sustitutos (tipoRelacion='04'): se conservan en
        // la póliza y necesitan formaPago/conceptos/tipoOrigen del ERP.
        (['E', 'P'].includes(c.tipoDeComprobante) && c.cfdiRelacionados?.length > 0)
      ))
      .map(c => c.uuid),
  );
  let erpMetaMap = {};
  if (uuidsSinMeta.size) {
    const erpCfdis = await CFDI.find({
      uuid:   { $in: [...uuidsSinMeta] },
      source: 'ERP',
    }).select('uuid formaPago metodoPago conceptos impuestos tipoOrigen cfdiRelacionados documentosRelacionados').lean();
    erpMetaMap = Object.fromEntries(erpCfdis.map(c => [c.uuid, c]));
  }
  const cfdisSinPolizaFinal = cfdisSinPolizaEnriquecidos.map(cfdi => {
    const erp = erpMetaMap[cfdi.uuid];
    if (!erp) return cfdi;
    const satHasTraslados     = cfdi.conceptos?.some(con => con.impuestos?.traslados?.length);
    const satHasBaseTraslados = (cfdi.impuestos?.traslados ?? []).some(t => (t.base ?? 0) > 0);
    const relSAT    = cfdi.cfdiRelacionados ?? [];
    const tiposEnSAT = new Set(relSAT.map(r => r.tipoRelacion));
    const relERP    = (erp.cfdiRelacionados ?? []).filter(r => !tiposEnSAT.has(r.tipoRelacion));
    const metodoPagoFinal = (cfdi.metodoPago === 'PPD' && erp.metodoPago === 'PUE')
      ? 'PUE' : (cfdi.metodoPago || erp.metodoPago);
    const esBCT = erp.documentosRelacionados?.some(d => d.Serie === 'BCT');
    const esBON = !esBCT && erp.documentosRelacionados?.some(d => (d.Serie ?? '').startsWith('BON'));
    return {
      ...cfdi,
      formaPago:              cfdi.formaPago  || erp.formaPago,
      metodoPago:             metodoPagoFinal,
      conceptos:              satHasTraslados     ? cfdi.conceptos : (erp.conceptos?.length ? erp.conceptos : cfdi.conceptos ?? []),
      impuestos:              satHasBaseTraslados  ? cfdi.impuestos : (erp.impuestos ?? cfdi.impuestos),
      tipoOrigen:             esBCT ? 'Bonificación Club Tuberos' : esBON ? 'Bonificación' : (cfdi.tipoOrigen ?? erp.tipoOrigen ?? null),
      documentosRelacionados: erp.documentosRelacionados ?? cfdi.documentosRelacionados ?? [],
      cfdiRelacionados:       relERP.length ? [...relSAT, ...relERP] : relSAT,
    };
  });

  // Enriquecer tasaIvaInferida en memoria para CFDIs P Metadata.
  // Paso 1: facturas relacionadas en MongoDB SAT. Paso 2: fallback ERP.
  if (tipoCfdi === 'P') {
    await _enrichTasaIvaFromRelatedCfdis(cfdisSinPolizaFinal);
    await _enrichTasaIvaErp(cfdisSinPolizaFinal);
  }

  // Normalización: E PUE formaPago=99 → PPD (en memoria, antes de matching)
  _normalizarEgresoPue99(cfdisSinPolizaFinal);
  // Normalización: E formaPago=15 (Condonación) → metodoPago real de la factura relacionada
  _normalizarEgresoCondonacion(cfdisSinPolizaFinal, relMetodoPagoMap);
  // Normalización: E con medio de pago real (Efectivo/Cheque/Transferencia/Tarjeta)
  // que ajusta una factura PPD nunca cobrada → formaPago+metodoPago de esa factura.
  _normalizarEgresoSegunFacturaRelacionada(cfdisSinPolizaFinal, relFacturaMetaMap);

  // Excluir el CFDI cancelado cuando existe un sustituto (tipoRelacion='04').
  // Genera póliza solo para el CFDI vigente final — espeja CONTPAQi.
  const _canceladosPorSustitutoProp = new Set(
    cfdisSinPolizaFinal
      .filter(c => ['P', 'E'].includes(c.tipoDeComprobante) &&
                   c.cfdiRelacionados?.some(r => r.tipoRelacion === '04'))
      .flatMap(c => (c.cfdiRelacionados || [])
        .filter(r => r.tipoRelacion === '04')
        .flatMap(r => r.uuids ?? (r.uuid ? [r.uuid] : []))
        .map(u => u.toUpperCase())
      )
  );
  const cfdisSinPolizaFinalFiltradoSustituto = _canceladosPorSustitutoProp.size
    ? cfdisSinPolizaFinal.filter(c =>
        !_canceladosPorSustitutoProp.has(c.uuid?.toUpperCase() ?? '')
      )
    : cfdisSinPolizaFinal;

  // Centro de costo por serie de facturación del CFDI (asignación automática).
  // Se resuelve aquí (antes del matching de reglas) para poder filtrar por
  // sucursal cuando se pide una sola, y se reutiliza más abajo para etiquetar
  // cada movimiento — evita una segunda consulta a Postgres.
  const ccBySerieMapProp = await centrosSvc.resolveBySerieMap();

  const cfdisSinPolizaFinalFiltrado = centroCostoId
    ? cfdisSinPolizaFinalFiltradoSustituto.filter(c =>
        String(ccBySerieMapProp[c.serie]?.id ?? '') === String(centroCostoId),
      )
    : cfdisSinPolizaFinalFiltradoSustituto;

  if (centroCostoId && cfdisSinPolizaFinalFiltrado.length === 0) {
    throw new BadRequestError('No hay CFDIs sin póliza para la sucursal seleccionada en este periodo');
  }

  // Fusionar NC (tipo E) relacionadas a estas facturas en la MISMA póliza de
  // Ingreso — ver _fetchNotasCreditoParaFusion. Se agregan al final del batch
  // 'I'; el resto del pipeline (matching de reglas, cfdiToMovimientos) ya
  // maneja tipos mixtos genéricamente.
  const cfdisConNCProp = tipoCfdi === 'I'
    ? [...cfdisSinPolizaFinalFiltrado, ...await _fetchNotasCreditoParaFusion(cfdisSinPolizaFinalFiltrado, rfc, uuidsYaUsados, { ejercicio, periodo, fechaInicio, fechaFin, centroCostoId, ccBySerieMap: ccBySerieMapProp })]
    : cfdisSinPolizaFinalFiltrado;

  // 5. Precalcular regla por CFDI y recolectar todos los códigos de cuenta necesarios
  const cfdiConRegla = cfdisConNCProp.map(cfdi => ({
    cfdi,
    rule: mappingSvc.findRuleInList(cfdi, rules),
  }));

  const codigosNecesarios = [...new Set(
    cfdiConRegla
      .filter(({ rule }) => rule)
      .flatMap(({ rule: r }) => [
        r.cuentaCargo, r.cuentaAbono, r.cuentaIva,
        r.cuentaIvaPPD, r.cuentaIvaRetenido, r.cuentaIsrRetenido,
        r.cuentaAbono2, r.cuentaDescuento, r.cuentaDescuento0,
        r.cuentaIvaAnticipo, r.cuentaDeltaAnticipo, r.cuentaCargo2,
        r.cuentaCargoMixto0, r.cuentaIvaAbono,
      ].filter(Boolean)),
  )];

  const cuentasRows = codigosNecesarios.length
    ? await AccountPlan.findAll({
        where:      { codigo: { [Op.in]: codigosNecesarios } },
        attributes: ['id', 'codigo'],
        raw:        true,
      })
    : [];
  const cuentaMap = Object.fromEntries(cuentasRows.map(c => [c.codigo, c.id]));

  // 6. Pre-fetch CFDIs relacionados (5° movimiento anticipo) y saldo a favor
  const relUuidsProp = [...new Set(
    cfdiConRegla
      .filter(({ rule, cfdi }) => rule?.cuentaDeltaAnticipo && cfdi.cfdiRelacionados?.length)
      .flatMap(({ cfdi }) => cfdi.cfdiRelacionados.map(r => r.uuid).filter(Boolean)),
  )];
  const relCfdiMapProp = relUuidsProp.length
    ? Object.fromEntries(
        (await CFDI.find({ uuid: { $in: relUuidsProp } }).select('uuid total impuestos.totalImpuestosTrasladados').lean())
          .map(c => [c.uuid, c]),
      )
    : {};

  let saldoRestanteProp = 0;
  if (cfdiConRegla.some(({ rule }) => rule?.esAplicacionSaldo)) {
    const rows = await sequelize.query(
      `SELECT COALESCE(SUM(pm.debe) - SUM(pm.haber), 0) AS saldo
       FROM poliza_movimientos pm
       JOIN polizas p ON pm.poliza_id = p.id
       JOIN account_plans ap ON pm.cuenta_id = ap.id
       WHERE p.rfc = :rfc AND ap.codigo = '2103090001' AND p.estado != 'cancelada'`,
      { replacements: { rfc }, type: QueryTypes.SELECT },
    );
    saldoRestanteProp = Number(rows[0]?.saldo || 0);
  }

  // 6. Generar movimientos usando cuentaMap pre-cargado
  // (ccBySerieMapProp ya se resolvió arriba, antes del filtro por sucursal)

  // ── Fix doble-contabilización anticipo PUE ────────────────────────────────
  // Solo aplica cuando la factura final (formaPago=30) usa el modelo 2 asientos
  // (cuentaCargo=2103010001 Anticipos). En el modelo 3 asientos (cuentaCargo=1103010001
  // Clientes) la NC sí debe procesarse — cancela Anticipos vs Clientes en asiento 3.
  const anticosCubiertosPorReg22C = new Set();
  for (const { cfdi: c, rule: r } of cfdiConRegla) {
    if (c.tipoDeComprobante !== 'I' || c.formaPago !== '30') continue;
    if (r?.cuentaCargo !== '2103010001') continue;
    if (c.uuid) anticosCubiertosPorReg22C.add(c.uuid.toUpperCase());
  }

  // Fix 5: verificar también en BD — la NC y la factura final pueden venir en batches distintos.
  // Si el UUID relacionado tipo 07 de alguna NC ya tiene movimiento en una regla con cuentaIvaAnticipo
  // en una póliza no cancelada, la NC está cubierta aunque no esté en el batch actual.
  {
    const uuids07 = new Set(
      cfdiConRegla
        .filter(({ cfdi: c }) =>
          c.tipoDeComprobante === 'E' &&
          c.cfdiRelacionados?.some(r => r.tipoRelacion === '07'))
        .flatMap(({ cfdi: c }) =>
          (c.cfdiRelacionados || [])
            .filter(r => r.tipoRelacion === '07')
            .flatMap(r => r.uuids ?? (r.uuid ? [r.uuid] : []))
            .map(u => u.toUpperCase()),
        ),
    );
    if (uuids07.size > 0) {
      const reglasAnticipo = await CfdiMappingRule.findAll({
        where: { cuentaIvaAnticipo: { [Op.ne]: null } },
        attributes: ['id'], raw: true,
      });
      const idsAnticipo = reglasAnticipo.map(r => r.id);
      if (idsAnticipo.length > 0) {
        const yaEnBD = await PolizaMovimiento.findAll({
          where: { cfdiUuid: { [Op.in]: [...uuids07] }, reglaId: { [Op.in]: idsAnticipo } },
          attributes: ['cfdiUuid'],
          include: [{ model: Poliza, as: 'poliza', attributes: [], where: { rfc, estado: { [Op.ne]: 'cancelada' } }, required: true }],
        });
        for (const m of yaEnBD) anticosCubiertosPorReg22C.add(m.cfdiUuid.toUpperCase());
      }
    }
  }

  const movimientosResult = [];
  let sinRegla = 0;

  for (const { cfdi, rule } of cfdiConRegla) {
    // Omitir NC tipo E (tipoRelacion=07) cuyo anticipo original ya fue procesado
    // por una factura PUE formaPago=30 (Reg 22C) en este mismo batch.
    if (cfdi.tipoDeComprobante === 'E' &&
        cfdi.cfdiRelacionados?.some(r => r.tipoRelacion === '07')) {
      const _rel07 = (cfdi.cfdiRelacionados || []).find(r => r.tipoRelacion === '07');
      const uuid07 = (_rel07?.uuids?.[0] ?? _rel07?.uuid ?? '').toUpperCase() || undefined;
      if (uuid07 && anticosCubiertosPorReg22C.has(uuid07)) continue;
    }
    const context = {};
    if (rule?.cuentaDeltaAnticipo && cfdi.cfdiRelacionados?.length) {
      const uuidsProp = cfdi.cfdiRelacionados.map(r => r.uuid).filter(Boolean);
      const foundProp  = uuidsProp.some(u => relCfdiMapProp[u]);
      if (foundProp) {
        context.totalRelacionado = uuidsProp
          .reduce((s, u) => s + Number(relCfdiMapProp[u]?.total || 0), 0);
        context.ivaRelacionado = uuidsProp
          .reduce((s, u) => s + Number(relCfdiMapProp[u]?.impuestos?.totalImpuestosTrasladados || 0), 0);
      }
      // Si no se encontró el CFDI relacionado en MongoDB, omitir delta (sin context.totalRelacionado)
    }
    if (rule?.esAplicacionSaldo && saldoRestanteProp > 0) {
      context.saldoDisponible = saldoRestanteProp;
    }
    // Las NC (tipo E) deben tratarse como la VENTA ORIGINAL que ajustan, no
    // según su propio metodoPago declarado (puede no coincidir — confirmado
    // con el usuario: una NC "Efectivo/PUE" puede estar ajustando una
    // factura PPD nunca cobrada). Con esto el motor usa cuentaIvaPPD en vez
    // de cuentaIva cuando la factura relacionada era a crédito.
    if (cfdi.tipoDeComprobante === 'E') {
      const metodoPagoRel = _uuidsRelacionados(cfdi).map(u => relMetodoPagoMap[u]).find(Boolean);
      if (metodoPagoRel) context.metodoPagoRelacionado = metodoPagoRel;
    }

    const movs = await mappingSvc.cfdiToMovimientos(cfdi, rule, cuentaMap, context);

    if (rule?.esAplicacionSaldo) {
      const usado = movs.find(m => m._saldoUsado != null)?._saldoUsado ?? 0;
      saldoRestanteProp = Math.max(0, saldoRestanteProp - usado);
    }

    const ccProp = cfdi.serie ? (ccBySerieMapProp[cfdi.serie] ?? null) : null;

    for (const m of movs) {
      movimientosResult.push({
        ...m,
        centroCosto:   ccProp?.clave   ?? m.centroCosto   ?? null,
        centroCostoId: ccProp?.id      ?? null,
        _cfdiInfo: {
          uuid:              cfdi.uuid,
          tipo:              cfdi.tipoDeComprobante,
          emisor:            cfdi.emisor?.rfc,
          total:             cfdi.total,
          fecha:             cfdi.fecha,
          sinRegla:          !!m._sinRegla,
          comparisonStatus:  cfdi.lastComparisonStatus ?? null,
        },
      });
    }
    if (!rule) sinRegla++;
  }

  // 4. Construir propuesta (no guardada)
  // Si se generó para un día específico (fechaInicio), el encabezado debe
  // mostrar ESE día, no la fecha en la que se corrió la generación — si no,
  // una póliza del 1 de mayo mostraría en el encabezado la fecha de hoy.
  const fecha = fechaInicio ? new Date(`${fechaInicio}T12:00:00.000Z`) : new Date();
  const mesStr = String(periodo).padStart(2, '0');

  // ── Obs 4: detectar facturas PPD con tipoRelacion='07' que deberían ser PUE ──
  // Cuando el anticipo cubre el 100%, la factura final debe emitirse como PUE.
  // Si llega como PPD, el asiento queda incompleto (IVA en Por Trasladar en lugar de Trasladado).
  const ppd07 = cfdisSinPolizaFinal.filter(c =>
    c.tipoDeComprobante === 'I' &&
    c.metodoPago === 'PPD' &&
    c.cfdiRelacionados?.some(r => r.tipoRelacion === '07')
  );

  // Recopilar diagnóstico de CFDIs sin regla para incluir en advertencias
  const _sinReglaInfo = cfdiConRegla
    .filter(({ rule }) => !rule)
    .slice(0, 5)
    .map(({ cfdi: c }) => {
      const tasaDetectada = c.tipoDeComprobante === 'P' ? mappingSvc.detectTasaIva(c) : undefined;
      return (
        `${c.uuid?.slice(0, 8)}… tipo=${c.tipoDeComprobante} método=${c.metodoPago || '—'} ` +
        `forma=${c.formaPago || '—'} emisor=${c.emisor?.rfc || '—'}` +
        (tasaDetectada !== undefined ? ` tasaIva=${tasaDetectada ?? 'null (sin datos de tasa — descarga XML)'}` : '')
      );
    });

  const advertencias = [];
  const _ncFusionadasProp = cfdisConNCProp.length - cfdisSinPolizaFinalFiltrado.length;
  if (_ncFusionadasProp > 0) {
    advertencias.push(`${_ncFusionadasProp} Nota(s) de Crédito fusionada(s) en esta póliza de Ingreso (devoluciones/descuentos/bonificaciones/anticipos relacionados)`);
  }
  if (sinRegla > 0) {
    advertencias.push(`${sinRegla} CFDI(s) sin regla de mapeo — las cuentas deben asignarse manualmente`);
    for (const info of _sinReglaInfo) advertencias.push(`  • ${info}`);
    if (sinRegla > 5) advertencias.push(`  … y ${sinRegla - 5} más`);
  }
  if (ppd07.length > 0) {
    advertencias.push(
      `⚠ ${ppd07.length} factura(s) PPD con tipoRelacion='07' (aplicación de anticipo): ` +
      `verificar si el anticipo cubre el 100% — en ese caso debió emitirse como PUE. ` +
      `Folios: ${ppd07.map(c => [c.serie, c.folio].filter(Boolean).join('-')).slice(0, 5).join(', ')}` +
      (ppd07.length > 5 ? ` y ${ppd07.length - 5} más` : ''),
    );
  }
  // Sustitutos cuyo CFDI original ya tiene póliza contabilizada → doble asiento potencial
  if (_canceladosPorSustitutoProp.size) {
    for (const c of cfdisSinPolizaFinal) {
      if (!['P', 'E'].includes(c.tipoDeComprobante)) continue;
      for (const rel of (c.cfdiRelacionados || []).filter(r => r.tipoRelacion === '04')) {
        for (const uA of (rel.uuids ?? (rel.uuid ? [rel.uuid] : []))) {
          if (uuidsYaUsados.has(uA.toUpperCase())) {
            advertencias.push(
              `⚠ Sustitución: ${c.uuid?.slice(0, 8)}… sustituye al CFDI ${uA.slice(0, 8)}… ` +
              `que ya tiene póliza contabilizada — reviértela y cancélala antes de procesar este sustituto`,
            );
          }
        }
      }
    }
  }

  return {
    tipo:       tipoPropuesta,
    fecha:      fecha.toISOString().slice(0, 10),
    // Mismo fix que totalCfdis: con centroCostoId, cfdisSinPoliza.length sigue
    // siendo el total del periodo completo (antes del filtro por sucursal).
    concepto:   `CFDIs ${mesStr}/${ejercicio} — ${(centroCostoId ? cfdisSinPolizaFinalFiltrado.length : cfdisSinPoliza.length)} comprobante(s)`,
    ejercicio:  Number(ejercicio),
    periodo:    Number(periodo),
    rfc,
    movimientos: movimientosResult,
    _meta: {
      totalCfdis:   cfdisSinPoliza.length,
      sinRegla,
      advertencias,
    },
  };
}

/**
 * Procesa los CFDIs vigentes del periodo y guarda la póliza directamente
 * como borrador en PostgreSQL. Útil cuando el volumen es demasiado grande
 * para devolver al frontend (>500 CFDIs).
 *
 * Devuelve: { polizaId, totalCfdis, sinRegla, advertencias }
 */
async function generarYGuardar({ rfc, ejercicio, periodo, tipoPropuesta = 'D', tipoCfdi, centroCostoId, fechaInicio, fechaFin }) {
  if (!rfc)       throw new BadRequestError('RFC requerido');
  if (!ejercicio) throw new BadRequestError('Ejercicio requerido');
  if (!periodo)   throw new BadRequestError('Periodo requerido');
  if (!tipoCfdi)  throw new BadRequestError('Debes seleccionar el tipo de CFDI a procesar (I, E o P)');

  // 1. UUIDs ya contabilizados (filtrado por RFC)
  const yaContabilizados = await PolizaMovimiento.findAll({
    where:      { cfdiUuid: { [Op.ne]: null } },
    attributes: ['cfdiUuid'],
    include: [{
      model:      Poliza,
      as:         'poliza',
      attributes: [],
      where:      { rfc, estado: { [Op.ne]: 'cancelada' } },
      required:   true,
    }],
    raw: true,
  });
  const uuidsYaUsados = new Set(yaContabilizados.map(m => m.cfdiUuid));

  // 2. CFDIs vigentes del periodo (sin límite)
  // fechaInicio/fechaFin (opcionales): ver misma nota en `generarPropuesta`
  // (usa la fecha EFECTIVA vía `_uuidsPorFechaEfectiva`, no el fecha crudo de SAT).
  const uuidsPorFechaGuard = (fechaInicio && fechaFin)
    ? await _uuidsPorFechaEfectiva({ rfc, ejercicio, periodo, tipoCfdi, fechaInicio, fechaFin })
    : null;
  const filtroBase = {
    $or:               [{ 'emisor.rfc': rfc }, { 'receptor.rfc': rfc }],
    ejercicio:         Number(ejercicio),
    periodo:           Number(periodo),
    tipoDeComprobante: tipoCfdi,
    source:            'SAT',
    satStatus:         'Vigente',
    ...(uuidsPorFechaGuard ? { uuid: { $in: [...uuidsPorFechaGuard] } } : {}),
    isActive:          true,
  };

  const cfdis = await CFDI.find(filtroBase)
    .select('uuid tipoDeComprobante metodoPago formaPago fecha folio serie emisor receptor subTotal total descuento impuestos complementoPago conceptos cfdiRelacionados tasaIvaInferida')
    .lean();

  await repararSubtotalDesdeXml(cfdis);

  const cfdisSinPoliza = cfdis.filter(c => !uuidsYaUsados.has(c.uuid));

  if (cfdisSinPoliza.length === 0) {
    throw new BadRequestError('Todos los CFDIs vigentes del periodo ya tienen póliza registrada');
  }

  // 3. Cargar reglas activas (cacheadas 60s)
  const rules = await _getRulesActive();

  // 4. Pre-fetch tipoDeComprobante de CFDIs relacionados para discriminador relacionadoTipo
  // (r.uuid singular o r.uuids array — cfdiRelacionados usa ambas formas según el origen).
  const relTipoUuidsGuard = [...new Set(
    cfdisSinPoliza
      .flatMap(c => (c.cfdiRelacionados || []).flatMap(r => r.uuids ?? (r.uuid ? [r.uuid] : []))),
  )];
  const relTipoCfdisGuard = relTipoUuidsGuard.length
    ? await CFDI.find({ uuid: { $in: relTipoUuidsGuard } })
        .select('uuid tipoDeComprobante metodoPago formaPago').lean()
    : [];
  const relTipoMapGuard = Object.fromEntries(
    relTipoCfdisGuard.map(c => [c.uuid, c.tipoDeComprobante]),
  );
  // uuid de factura → su metodoPago — usado por _normalizarEgresoCondonacion
  // para resolver el metodoPago real de NCs formaPago=15 (Condonación).
  const relMetodoPagoMapGuard = Object.fromEntries(relTipoCfdisGuard.map(c => [c.uuid, c.metodoPago]));
  // uuid de factura → metodoPago+formaPago — usado por
  // _normalizarEgresoSegunFacturaRelacionada (medios de pago reales).
  const relFacturaMetaMapGuard = Object.fromEntries(relTipoCfdisGuard.map(c => [c.uuid, { metodoPago: c.metodoPago, formaPago: c.formaPago }]));

  const cfdisSinPolizaEnriquecidosGuard = cfdisSinPoliza.map(cfdi => {
    const primerUuid = (cfdi.cfdiRelacionados || [])[0]?.uuid;
    return primerUuid && relTipoMapGuard[primerUuid]
      ? { ...cfdi, _relacionadoTipo: relTipoMapGuard[primerUuid] }
      : cfdi;
  });

  // Enriquecer CFDIs SAT con datos del homólogo ERP — misma lógica que balanza-preliminar.
  const uuidsSinMetaGuard = new Set(
    cfdisSinPolizaEnriquecidosGuard
      .filter(c => c.uuid && (
        !c.formaPago ||
        !c.metodoPago ||
        !c.conceptos?.length ||
        c.conceptos.every(con => !(con.impuestos?.traslados?.length)) ||
        (c.tipoDeComprobante === 'I' && c.metodoPago === 'PPD') ||
        // Enriquecer también sustitutos (tipoRelacion='04'): se conservan en
        // la póliza y necesitan formaPago/conceptos/tipoOrigen del ERP.
        (['E', 'P'].includes(c.tipoDeComprobante) && c.cfdiRelacionados?.length > 0)
      ))
      .map(c => c.uuid),
  );
  let erpMetaMapGuard = {};
  if (uuidsSinMetaGuard.size) {
    const erpCfdisGuard = await CFDI.find({
      uuid:   { $in: [...uuidsSinMetaGuard] },
      source: 'ERP',
    }).select('uuid formaPago metodoPago conceptos impuestos tipoOrigen cfdiRelacionados documentosRelacionados').lean();
    erpMetaMapGuard = Object.fromEntries(erpCfdisGuard.map(c => [c.uuid, c]));
  }
  const cfdisSinPolizaFinalGuard = cfdisSinPolizaEnriquecidosGuard.map(cfdi => {
    const erp = erpMetaMapGuard[cfdi.uuid];
    if (!erp) return cfdi;
    const satHasTraslados     = cfdi.conceptos?.some(con => con.impuestos?.traslados?.length);
    const satHasBaseTraslados = (cfdi.impuestos?.traslados ?? []).some(t => (t.base ?? 0) > 0);
    const relSAT    = cfdi.cfdiRelacionados ?? [];
    const tiposEnSAT = new Set(relSAT.map(r => r.tipoRelacion));
    const relERP    = (erp.cfdiRelacionados ?? []).filter(r => !tiposEnSAT.has(r.tipoRelacion));
    const metodoPagoFinal = (cfdi.metodoPago === 'PPD' && erp.metodoPago === 'PUE')
      ? 'PUE' : (cfdi.metodoPago || erp.metodoPago);
    const esBCT = erp.documentosRelacionados?.some(d => d.Serie === 'BCT');
    const esBON = !esBCT && erp.documentosRelacionados?.some(d => (d.Serie ?? '').startsWith('BON'));
    return {
      ...cfdi,
      formaPago:              cfdi.formaPago  || erp.formaPago,
      metodoPago:             metodoPagoFinal,
      conceptos:              satHasTraslados     ? cfdi.conceptos : (erp.conceptos?.length ? erp.conceptos : cfdi.conceptos ?? []),
      impuestos:              satHasBaseTraslados  ? cfdi.impuestos : (erp.impuestos ?? cfdi.impuestos),
      tipoOrigen:             esBCT ? 'Bonificación Club Tuberos' : esBON ? 'Bonificación' : (cfdi.tipoOrigen ?? erp.tipoOrigen ?? null),
      documentosRelacionados: erp.documentosRelacionados ?? cfdi.documentosRelacionados ?? [],
      cfdiRelacionados:       relERP.length ? [...relSAT, ...relERP] : relSAT,
    };
  });

  // Enriquecer tasaIvaInferida en memoria para CFDIs P Metadata.
  // Paso 1: facturas relacionadas en MongoDB SAT. Paso 2: fallback ERP.
  if (tipoCfdi === 'P') {
    await _enrichTasaIvaFromRelatedCfdis(cfdisSinPolizaFinalGuard);
    await _enrichTasaIvaErp(cfdisSinPolizaFinalGuard);
  }

  // Normalización: E PUE formaPago=99 → PPD (en memoria, antes de matching)
  _normalizarEgresoPue99(cfdisSinPolizaFinalGuard);
  // Normalización: E formaPago=15 (Condonación) → metodoPago real de la factura relacionada
  _normalizarEgresoCondonacion(cfdisSinPolizaFinalGuard, relMetodoPagoMapGuard);
  // Normalización: E con medio de pago real (Efectivo/Cheque/Transferencia/Tarjeta)
  // que ajusta una factura PPD nunca cobrada → formaPago+metodoPago de esa factura.
  _normalizarEgresoSegunFacturaRelacionada(cfdisSinPolizaFinalGuard, relFacturaMetaMapGuard);

  // Excluir el CFDI cancelado cuando existe un sustituto (tipoRelacion='04').
  // Genera póliza solo para el CFDI vigente final — espeja CONTPAQi.
  const _canceladosPorSustitutoGuard = new Set(
    cfdisSinPolizaFinalGuard
      .filter(c => ['P', 'E'].includes(c.tipoDeComprobante) &&
                   c.cfdiRelacionados?.some(r => r.tipoRelacion === '04'))
      .flatMap(c => (c.cfdiRelacionados || [])
        .filter(r => r.tipoRelacion === '04')
        .flatMap(r => r.uuids ?? (r.uuid ? [r.uuid] : []))
        .map(u => u.toUpperCase())
      )
  );
  const cfdisSinPolizaFinalGuardFiltradoSustituto = _canceladosPorSustitutoGuard.size
    ? cfdisSinPolizaFinalGuard.filter(c =>
        !_canceladosPorSustitutoGuard.has(c.uuid?.toUpperCase() ?? '')
      )
    : cfdisSinPolizaFinalGuard;

  // Centro de costo por serie de facturación (ver comentario en generarPropuesta).
  const ccBySerieMap = await centrosSvc.resolveBySerieMap();

  const cfdisSinPolizaFinalGuardFiltrado = centroCostoId
    ? cfdisSinPolizaFinalGuardFiltradoSustituto.filter(c =>
        String(ccBySerieMap[c.serie]?.id ?? '') === String(centroCostoId),
      )
    : cfdisSinPolizaFinalGuardFiltradoSustituto;

  if (centroCostoId && cfdisSinPolizaFinalGuardFiltrado.length === 0) {
    throw new BadRequestError('No hay CFDIs sin póliza para la sucursal seleccionada en este periodo');
  }

  // Fusionar NC (tipo E) relacionadas a estas facturas en la MISMA póliza de
  // Ingreso — ver _fetchNotasCreditoParaFusion.
  const cfdisConNCGuard = tipoCfdi === 'I'
    ? [...cfdisSinPolizaFinalGuardFiltrado, ...await _fetchNotasCreditoParaFusion(cfdisSinPolizaFinalGuardFiltrado, rfc, uuidsYaUsados, { ejercicio, periodo, fechaInicio, fechaFin, centroCostoId, ccBySerieMap })]
    : cfdisSinPolizaFinalGuardFiltrado;

  // 5. Precalcular regla por CFDI y resolver cuentaMap en un solo query
  const cfdiConRegla = cfdisConNCGuard.map(cfdi => ({
    cfdi,
    rule: mappingSvc.findRuleInList(cfdi, rules),
  }));

  const codigosNecesarios = [...new Set(
    cfdiConRegla
      .filter(({ rule }) => rule)
      .flatMap(({ rule: r }) => [
        r.cuentaCargo, r.cuentaAbono, r.cuentaIva,
        r.cuentaIvaPPD, r.cuentaIvaRetenido, r.cuentaIsrRetenido,
        r.cuentaAbono2, r.cuentaDescuento, r.cuentaDescuento0,
        r.cuentaIvaAnticipo, r.cuentaDeltaAnticipo, r.cuentaCargo2,
        r.cuentaCargoMixto0, r.cuentaIvaAbono,
      ].filter(Boolean)),
  )];

  const cuentasRows = codigosNecesarios.length
    ? await AccountPlan.findAll({
        where:      { codigo: { [Op.in]: codigosNecesarios } },
        attributes: ['id', 'codigo'],
        raw:        true,
      })
    : [];
  const cuentaMap = Object.fromEntries(cuentasRows.map(c => [c.codigo, c.id]));

  // 6. Pre-fetch CFDIs relacionados (5° movimiento anticipo) y saldo a favor
  const relUuidsGuard = [...new Set(
    cfdiConRegla
      .filter(({ rule, cfdi }) => rule?.cuentaDeltaAnticipo && cfdi.cfdiRelacionados?.length)
      .flatMap(({ cfdi }) => cfdi.cfdiRelacionados.map(r => r.uuid).filter(Boolean)),
  )];
  const relCfdiMapGuard = relUuidsGuard.length
    ? Object.fromEntries(
        (await CFDI.find({ uuid: { $in: relUuidsGuard } }).select('uuid total impuestos.totalImpuestosTrasladados').lean())
          .map(c => [c.uuid, c]),
      )
    : {};

  let saldoRestanteGuard = 0;
  if (cfdiConRegla.some(({ rule }) => rule?.esAplicacionSaldo)) {
    const rows = await sequelize.query(
      `SELECT COALESCE(SUM(pm.debe) - SUM(pm.haber), 0) AS saldo
       FROM poliza_movimientos pm
       JOIN polizas p ON pm.poliza_id = p.id
       JOIN account_plans ap ON pm.cuenta_id = ap.id
       WHERE p.rfc = :rfc AND ap.codigo = '2103090001' AND p.estado != 'cancelada'`,
      { replacements: { rfc }, type: QueryTypes.SELECT },
    );
    saldoRestanteGuard = Number(rows[0]?.saldo || 0);
  }

  // 6. Generar movimientos en memoria
  // (ccBySerieMap ya se resolvió arriba, antes del filtro por sucursal)

  // ── Fix doble-contabilización anticipo PUE ────────────────────────────────
  // Misma lógica que en generarPropuesta: si hay una factura PUE formaPago=30
  // con tipoRelacion=07 en el batch, la NC tipo E del mismo anticipo se omite.
  const anticosCubiertosPorReg22CGuard = new Set();
  for (const { cfdi: c } of cfdiConRegla) {
    if (c.tipoDeComprobante !== 'I' || c.formaPago !== '30') continue;
    if (c.uuid) anticosCubiertosPorReg22CGuard.add(c.uuid.toUpperCase());
  }

  // Fix 5: verificar también en BD — la NC y la factura final pueden venir en batches distintos.
  {
    const uuids07g = new Set(
      cfdiConRegla
        .filter(({ cfdi: c }) =>
          c.tipoDeComprobante === 'E' &&
          c.cfdiRelacionados?.some(r => r.tipoRelacion === '07'))
        .flatMap(({ cfdi: c }) =>
          (c.cfdiRelacionados || [])
            .filter(r => r.tipoRelacion === '07')
            .flatMap(r => r.uuids ?? (r.uuid ? [r.uuid] : []))
            .map(u => u.toUpperCase()),
        ),
    );
    if (uuids07g.size > 0) {
      const reglasAnticipoG = await CfdiMappingRule.findAll({
        where: { cuentaIvaAnticipo: { [Op.ne]: null } },
        attributes: ['id'], raw: true,
      });
      const idsAnticipoG = reglasAnticipoG.map(r => r.id);
      if (idsAnticipoG.length > 0) {
        const yaEnBDG = await PolizaMovimiento.findAll({
          where: { cfdiUuid: { [Op.in]: [...uuids07g] }, reglaId: { [Op.in]: idsAnticipoG } },
          attributes: ['cfdiUuid'],
          include: [{ model: Poliza, as: 'poliza', attributes: [], where: { rfc, estado: { [Op.ne]: 'cancelada' } }, required: true }],
        });
        for (const m of yaEnBDG) anticosCubiertosPorReg22CGuard.add(m.cfdiUuid.toUpperCase());
      }
    }
  }

  const todosLosMovimientos = [];
  let sinRegla = 0;
  const advertencias = [];
  const ruleUsageCount = new Map();
  // Diagnóstico: acumular los primeros 5 CFDIs sin regla para dar info útil
  const muestrasSinRegla = [];

  for (const { cfdi, rule } of cfdiConRegla) {
    // Omitir NC tipo E (tipoRelacion=07) cuyo anticipo ya fue procesado por Reg 22C
    if (cfdi.tipoDeComprobante === 'E' &&
        cfdi.cfdiRelacionados?.some(r => r.tipoRelacion === '07')) {
      const _rel07g = (cfdi.cfdiRelacionados || []).find(r => r.tipoRelacion === '07');
      const uuid07 = (_rel07g?.uuids?.[0] ?? _rel07g?.uuid ?? '').toUpperCase() || undefined;
      if (uuid07 && anticosCubiertosPorReg22CGuard.has(uuid07)) continue;
    }

    if (!rule) {
      sinRegla++;
      if (muestrasSinRegla.length < 5) {
        const _tasaDet = cfdi.tipoDeComprobante === 'P' ? mappingSvc.detectTasaIva(cfdi) : undefined;
        muestrasSinRegla.push({
          uuid:    cfdi.uuid?.slice(0, 8),
          tipo:    cfdi.tipoDeComprobante,
          metodo:  cfdi.metodoPago,
          forma:   cfdi.formaPago,
          emisor:  cfdi.emisor?.rfc,
          ...(_tasaDet !== undefined ? { tasaIva: _tasaDet ?? 'null (sin datos — descarga XML)' } : {}),
        });
      }
      continue;
    }

    const context = {};
    if (rule?.cuentaDeltaAnticipo && cfdi.cfdiRelacionados?.length) {
      const uuidsGuard = cfdi.cfdiRelacionados.map(r => r.uuid).filter(Boolean);
      const foundGuard = uuidsGuard.some(u => relCfdiMapGuard[u]);
      if (foundGuard) {
        context.totalRelacionado = uuidsGuard
          .reduce((s, u) => s + Number(relCfdiMapGuard[u]?.total || 0), 0);
        context.ivaRelacionado = uuidsGuard
          .reduce((s, u) => s + Number(relCfdiMapGuard[u]?.impuestos?.totalImpuestosTrasladados || 0), 0);
      }
      // Si no se encontró el CFDI relacionado en MongoDB, omitir delta (sin context.totalRelacionado)
    }
    if (rule?.esAplicacionSaldo && saldoRestanteGuard > 0) {
      context.saldoDisponible = saldoRestanteGuard;
    }
    // Las NC (tipo E) deben tratarse como la VENTA ORIGINAL que ajustan, no
    // según su propio metodoPago declarado — ver comentario equivalente en
    // generarPropuesta.
    if (cfdi.tipoDeComprobante === 'E') {
      const metodoPagoRel = _uuidsRelacionados(cfdi).map(u => relMetodoPagoMapGuard[u]).find(Boolean);
      if (metodoPagoRel) context.metodoPagoRelacionado = metodoPagoRel;
    }

    const movs = await mappingSvc.cfdiToMovimientos(cfdi, rule, cuentaMap, context);
    ruleUsageCount.set(rule.id, (ruleUsageCount.get(rule.id) || 0) + 1);

    if (rule?.esAplicacionSaldo) {
      const usado = movs.find(m => m._saldoUsado != null)?._saldoUsado ?? 0;
      saldoRestanteGuard = Math.max(0, saldoRestanteGuard - usado);
    }

    // Marcar movimientos cuya cuenta no existe en el catálogo (cuentaId queda null).
    // Se guardan igualmente para que el usuario los identifique y corrija manualmente.
    const tieneFaltante = movs.some(m => m.cuentaId == null);
    if (tieneFaltante) {
      advertencias.push(`CFDI ${cfdi.uuid?.slice(0, 8)} — una o más cuentas no encontradas en catálogo (regla: ${rule.nombre})`);
    }
    const cc = cfdi.serie ? (ccBySerieMap[cfdi.serie] ?? null) : null;

    for (const m of movs) {
      // eslint-disable-next-line no-unused-vars
      const { _saldoUsado, ...cleanM } = m;
      todosLosMovimientos.push({
        ...cleanM,
        cuentaFaltante: cleanM.cuentaId == null,
        centroCosto:    cc?.clave ?? cleanM.centroCosto ?? null,
        centroCostoId:  cc?.id    ?? null,
      });
    }
  }

  // Sustitutos cuyo CFDI original ya tiene póliza contabilizada → doble asiento potencial
  if (_canceladosPorSustitutoGuard.size) {
    for (const c of cfdisSinPolizaFinalGuard) {
      if (!['P', 'E'].includes(c.tipoDeComprobante)) continue;
      for (const rel of (c.cfdiRelacionados || []).filter(r => r.tipoRelacion === '04')) {
        for (const uA of (rel.uuids ?? (rel.uuid ? [rel.uuid] : []))) {
          if (uuidsYaUsados.has(uA.toUpperCase())) {
            advertencias.push(
              `⚠ Sustitución: ${c.uuid?.slice(0, 8)}… sustituye al CFDI ${uA.slice(0, 8)}… ` +
              `que ya tiene póliza contabilizada — reviértela y cancélala antes de contabilizar este sustituto`,
            );
          }
        }
      }
    }
  }

  // 7. Guardar póliza + movimientos en una transacción con advisory lock
  // Si se generó para un día específico (fechaInicio), el encabezado debe
  // mostrar ESE día, no la fecha en la que se corrió la generación.
  const fecha    = fechaInicio ? new Date(`${fechaInicio}T12:00:00.000Z`) : new Date();
  const mesStr   = String(periodo).padStart(2, '0');
  // Mismo fix que totalCfdis: con centroCostoId, cfdisSinPoliza.length sigue
  // siendo el total del periodo completo (antes del filtro por sucursal) —
  // se guardaba un concepto con el conteo de TODAS las sucursales aunque la
  // póliza solo tuviera los CFDIs correctos de esta.
  const concepto = `CFDIs ${mesStr}/${ejercicio} — ${(centroCostoId ? cfdisSinPolizaFinalGuardFiltrado.length : cfdisSinPoliza.length)} comprobante(s)`;

  const poliza = await sequelize.transaction(async (t) => {
    await sequelize.query(
      'SELECT pg_advisory_xact_lock(hashtext(:key))',
      { replacements: { key: `poliza-${tipoPropuesta}-${rfc}-${ejercicio}-${periodo}` }, transaction: t },
    );

    const max = await Poliza.max('numero', {
      where: { tipo: tipoPropuesta, rfc, ejercicio: Number(ejercicio), periodo: Number(periodo) },
      transaction: t,
    });
    const numero = (max || 0) + 1;

    const polizaHeader = await Poliza.create({
      tipo:      tipoPropuesta,
      numero,
      fecha:     fecha.toISOString().slice(0, 10),
      concepto,
      ejercicio: Number(ejercicio),
      periodo:   Number(periodo),
      rfc,
      estado:    'borrador',
    }, { transaction: t });

    for (let i = 0; i < todosLosMovimientos.length; i += CHUNK_SIZE) {
      const chunk = todosLosMovimientos.slice(i, i + CHUNK_SIZE);
      const rows  = chunk.map((m, j) => ({
        ...m,
        polizaId: polizaHeader.id,
        orden:    i + j + 1,
      }));
      await PolizaMovimiento.bulkCreate(rows, { transaction: t });
    }

    return polizaHeader;
  });

  // Incrementar contador de uso por regla (fuera de la transacción para no bloquearla)
  if (ruleUsageCount.size > 0) {
    await Promise.all(
      [...ruleUsageCount.entries()].map(([id, count]) =>
        CfdiMappingRule.increment('vecesUsada', { by: count, where: { id } }),
      ),
    );
  }

  const advertenciasFinal = [];
  const _ncFusionadasGuard = cfdisConNCGuard.length - cfdisSinPolizaFinalGuardFiltrado.length;
  if (_ncFusionadasGuard > 0) {
    advertenciasFinal.push(`${_ncFusionadasGuard} Nota(s) de Crédito fusionada(s) en esta póliza de Ingreso (devoluciones/descuentos/bonificaciones/anticipos relacionados)`);
  }
  if (sinRegla > 0) {
    advertenciasFinal.push(`${sinRegla} CFDI(s) omitidos por no tener regla de mapeo`);
    // Muestra diagnóstico de los primeros 5 ignorados
    for (const m of muestrasSinRegla) {
      const tasaStr = m.tasaIva !== undefined ? ` tasaIva=${m.tasaIva}` : '';
      advertenciasFinal.push(
        `  Ej. ${m.uuid}… → tipo=${m.tipo} método=${m.metodo || '—'} forma=${m.forma || '—'} emisor=${m.emisor || '—'}${tasaStr}`,
      );
    }
    // Resumen de reglas activas para comparar
    if (rules.length === 0) {
      advertenciasFinal.push('  ⚠ No hay reglas activas en la base de datos');
    } else {
      advertenciasFinal.push(
        `  Reglas activas: ${rules.map(r => `"${r.nombre}" (tipo=${r.tipoComprobante || '*'} método=${r.metodoPago || '*'} forma=${r.formaPago || '*'} RFC=${r.rfcEmisor || '*'}) isActive=${r.isActive}`).join(', ')}`,
      );
    }
  }
  advertenciasFinal.push(...advertencias);

  return {
    polizaId:   poliza.id,
    // Bug corregido: con centroCostoId, cfdisSinPoliza.length sigue siendo el
    // total del día/periodo completo (antes del filtro por sucursal) — se
    // reportaba el conteo de TODAS las sucursales aunque la póliza guardada
    // sí contenía solo los CFDIs correctos de esa sucursal.
    totalCfdis:   centroCostoId ? cfdisSinPolizaFinalGuardFiltrado.length : cfdisSinPoliza.length,
    sinRegla,
    advertencias: advertenciasFinal,
  };
}

/**
 * Genera una póliza POR CADA sucursal (centro de costo) que tenga CFDIs sin
 * póliza en el periodo, en vez de una sola póliza con todo mezclado.
 * Reutiliza generarYGuardar por sucursal — no duplica lógica de mapeo.
 *
 * Devuelve: { resultados: [{ centroCosto, centroCostoId, polizaId?, totalCfdis?, sinRegla?, error? }] }
 */
async function generarYGuardarPorSucursal({ rfc, ejercicio, periodo, tipoPropuesta = 'D', tipoCfdi }) {
  const centros = await centrosSvc.list();
  const centrosConSerie = centros.filter(c => c.serieFacturacion);

  if (!centrosConSerie.length) {
    throw new BadRequestError('No hay centros de costo con serie de facturación configurada');
  }

  const resultados = await _conLimite(centrosConSerie, CONCURRENCIA_GENERACION, async (cc) => {
    try {
      const r = await generarYGuardar({ rfc, ejercicio, periodo, tipoPropuesta, tipoCfdi, centroCostoId: cc.id });
      return { centroCosto: cc.sucursal, centroCostoId: cc.id, ...r };
    } catch (err) {
      // "No hay CFDIs para esta sucursal" es esperado (no toda sucursal tiene
      // movimientos en cada periodo) — se reporta sin detener a las demás.
      return { centroCosto: cc.sucursal, centroCostoId: cc.id, error: err.message };
    }
  });

  return { resultados };
}

function _fmtDia(d) {
  const y  = d.getFullYear();
  const m  = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${dd}`;
}

/**
 * Lista de días (strings 'YYYY-MM-DD') entre fechaInicio/fechaFin, o del mes
 * calendario completo de ejercicio/periodo si no se especifica rango (mismo
 * supuesto "periodo fiscal = mes calendario" que usa el resto del sistema).
 * Construido con componentes y/m/d en vez de toISOString() para no depender
 * de la zona horaria del proceso Node.
 */
function _diasDelRango({ ejercicio, periodo, fechaInicio, fechaFin }) {
  const inicio = fechaInicio ? new Date(`${fechaInicio}T00:00:00`) : new Date(Number(ejercicio), Number(periodo) - 1, 1);
  const fin    = fechaFin    ? new Date(`${fechaFin}T00:00:00`)    : new Date(Number(ejercicio), Number(periodo), 0);

  const dias = [];
  for (const d = new Date(inicio); d <= fin; d.setDate(d.getDate() + 1)) {
    dias.push(_fmtDia(d));
  }
  return dias;
}

// Medianoche de `fechaYMD` en America/Mexico_City, como instante UTC real.
// México abolió el horario de verano (DST) desde 2022 — el offset es fijo
// UTC-6 todo el año, así que sumar 6 horas basta (no hace falta librería de
// zonas horarias).
function _medianocheMx(fechaYMD) {
  return new Date(`${fechaYMD}T06:00:00.000Z`);
}

function _diaSiguiente(fechaYMD) {
  // OJO: no reutilizar _fmtDia aquí — usa getters LOCALES, pero `d` se
  // construye y manipula en términos UTC (mismo tipo de bug de zona horaria
  // ya encontrado antes). Formatear con getters UTC para que sea consistente.
  const d = new Date(`${fechaYMD}T00:00:00.000Z`);
  d.setUTCDate(d.getUTCDate() + 1);
  const y  = d.getUTCFullYear();
  const m  = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${dd}`;
}

/**
 * Resuelve qué UUIDs de CFDI (tipo `tipoCfdi`, del RFC dado) tienen su fecha
 * EFECTIVA dentro de [fechaInicio, fechaFin] — usado para separar pólizas por
 * día. La fecha "efectiva" es la del documento ERP homólogo (mismo uuid,
 * source='ERP') cuando existe: el ERP entrega su fecha con hora real ya
 * resuelta a UTC (`FechaGeneracion`), mientras que la fecha del CFDI/SAT NO
 * trae zona horaria fiable — la mayoría son solo "fecha sin hora" (medianoche
 * UTC ingenua) y el resto es la hora de CDMX mal etiquetada como UTC. Por eso
 * NO se puede simplemente comparar `fecha` de SAT contra límites de huso
 * horario reales: quedaría corrida para las facturas emitidas por la tarde/
 * noche (~16% de los casos verificados). Cuando el CFDI no tiene homólogo ERP
 * (~34% de los casos), se usa su propio fecha de SAT con los mismos límites
 * "ingenuos" (UTC sin ajuste) que ya usaba el sistema — ese fecha, aunque no
 * es UTC real, ya está alineado por casualidad al día calendario de CDMX.
 */
async function _uuidsPorFechaEfectiva({ rfc, ejercicio, periodo, tipoCfdi, fechaInicio, fechaFin }) {
  const naiveInicio = new Date(`${fechaInicio}T00:00:00.000Z`);
  const naiveFin     = new Date(`${fechaFin}T23:59:59.999Z`);
  const mxInicio     = _medianocheMx(fechaInicio);
  const mxFin        = new Date(_medianocheMx(_diaSiguiente(fechaFin)).getTime() - 1);

  const filtroComun = {
    tipoDeComprobante: tipoCfdi,
    $or: [{ 'emisor.rfc': rfc }, { 'receptor.rfc': rfc }],
  };

  // 1. SAT cuyo fecha "ingenuo" cae en el rango — mismo universo que el
  //    filtro viejo, acotado por periodo (rápido, es el caso de siempre).
  const satNaive = await CFDI.find({
    ...filtroComun, source: 'SAT', ejercicio: Number(ejercicio), periodo: Number(periodo),
    fecha: { $gte: naiveInicio, $lte: naiveFin },
  }).select('uuid').lean();
  const uuidsSatNaive = satNaive.map(c => c.uuid.toUpperCase());

  // 2. De esos (no de TODO el histórico ERP del rfc), cuáles tienen homólogo
  //    ERP — para saber a cuáles no aplicarles el fallback de su fecha SAT.
  const erpDeEsosSat = uuidsSatNaive.length
    ? await CFDI.find({ uuid: { $in: uuidsSatNaive }, source: 'ERP' }).select('uuid').lean()
    : [];
  const uuidsConErp = new Set(erpDeEsosSat.map(c => c.uuid.toUpperCase()));

  // 3. UUIDs cuyo homólogo ERP cae en el rango (huso horario real de México)
  //    — acotado al rango de días, no a todo el histórico. Esto también
  //    reclasifica hacia este día CFDIs cuyo fecha SAT ingenuo cayó en OTRO
  //    día pero cuya fecha ERP real sí es este.
  const erpEnRango = await CFDI.find({ ...filtroComun, source: 'ERP', fecha: { $gte: mxInicio, $lte: mxFin } })
    .select('uuid').lean();
  const resultado = new Set(erpEnRango.map(c => c.uuid.toUpperCase()));

  // 4. SAT sin homólogo ERP → fallback a su propio fecha (ya está en rango,
  //    viene del paso 1). Los que SÍ tienen homólogo se descartan aquí: su
  //    inclusión/exclusión ya la decidió el paso 3 según su fecha ERP real.
  for (const uuid of uuidsSatNaive) {
    if (!uuidsConErp.has(uuid)) resultado.add(uuid);
  }

  return resultado;
}

// Corre `fn` sobre `items` con como máximo `limite` llamadas en vuelo a la
// vez, preservando el orden de `items` en el arreglo devuelto. Usado para que
// generar N pólizas (por día/sucursal) no espere una por una en serie —
// generarYGuardar ya serializa la parte crítica (asignar `numero`) con
// advisory lock por rfc/ejercicio/periodo, así que correr el resto en
// paralelo (fetch/enriquecimiento de CFDIs) es seguro.
async function _conLimite(items, limite, fn) {
  const resultado = new Array(items.length);
  let siguiente = 0;
  async function trabajador() {
    while (siguiente < items.length) {
      const idx = siguiente++;
      resultado[idx] = await fn(items[idx], idx);
    }
  }
  await Promise.all(Array.from({ length: Math.min(limite, items.length) }, trabajador));
  return resultado;
}

const CONCURRENCIA_GENERACION = 4;

/**
 * Genera una póliza POR CADA DÍA del rango indicado (o del mes calendario
 * completo de ejercicio/periodo si no se especifica fechaInicio/fechaFin)
 * que tenga CFDIs sin póliza. Mismo patrón que `generarYGuardarPorSucursal`:
 * reutiliza generarYGuardar por día, no duplica lógica de mapeo.
 *
 * Devuelve: { resultados: [{ fecha, polizaId?, totalCfdis?, sinRegla?, error? }] }
 */
async function generarYGuardarPorDia({ rfc, ejercicio, periodo, tipoPropuesta = 'D', tipoCfdi, centroCostoId, fechaInicio, fechaFin }) {
  if (!ejercicio) throw new BadRequestError('Ejercicio requerido');
  if (!periodo)   throw new BadRequestError('Periodo requerido');

  const dias = _diasDelRango({ ejercicio, periodo, fechaInicio, fechaFin });
  if (!dias.length) throw new BadRequestError('Rango de fechas inválido');

  const resultados = await _conLimite(dias, CONCURRENCIA_GENERACION, async (dia) => {
    try {
      const r = await generarYGuardar({ rfc, ejercicio, periodo, tipoPropuesta, tipoCfdi, centroCostoId, fechaInicio: dia, fechaFin: dia });
      return { fecha: dia, ...r };
    } catch (err) {
      // "No hay CFDIs para este día" es esperado (no todos los días tienen
      // movimientos) — se reporta sin detener a los demás.
      return { fecha: dia, error: err.message };
    }
  });

  return { resultados };
}

/**
 * Genera una póliza POR CADA COMBINACIÓN sucursal × día — el cruce de
 * `generarYGuardarPorSucursal` y `generarYGuardarPorDia`. Útil para el
 * export a CONTPAQ en ZIP con una carpeta por sucursal y un archivo por día
 * dentro de cada una.
 *
 * Devuelve: { resultados: [{ centroCosto, centroCostoId, fecha, polizaId?, totalCfdis?, sinRegla?, error? }] }
 */
async function generarYGuardarPorSucursalYDia({ rfc, ejercicio, periodo, tipoPropuesta = 'D', tipoCfdi, fechaInicio, fechaFin }) {
  if (!ejercicio) throw new BadRequestError('Ejercicio requerido');
  if (!periodo)   throw new BadRequestError('Periodo requerido');

  const centros = await centrosSvc.list();
  const centrosConSerie = centros.filter(c => c.serieFacturacion);
  if (!centrosConSerie.length) {
    throw new BadRequestError('No hay centros de costo con serie de facturación configurada');
  }

  const dias = _diasDelRango({ ejercicio, periodo, fechaInicio, fechaFin });
  if (!dias.length) throw new BadRequestError('Rango de fechas inválido');

  // Aplanar sucursal × día en una sola lista de combinaciones para que el
  // límite de concurrencia aplique sobre el total, no por sucursal.
  const combinaciones = centrosConSerie.flatMap(cc => dias.map(dia => ({ cc, dia })));

  const resultados = await _conLimite(combinaciones, CONCURRENCIA_GENERACION, async ({ cc, dia }) => {
    try {
      const r = await generarYGuardar({ rfc, ejercicio, periodo, tipoPropuesta, tipoCfdi, centroCostoId: cc.id, fechaInicio: dia, fechaFin: dia });
      return { centroCosto: cc.sucursal, centroCostoId: cc.id, fecha: dia, ...r };
    } catch (err) {
      // "No hay CFDIs para esta sucursal/día" es esperado — se reporta sin
      // detener las demás combinaciones.
      return { centroCosto: cc.sucursal, centroCostoId: cc.id, fecha: dia, error: err.message };
    }
  });

  return { resultados };
}

module.exports = {
  generarPropuesta, generarYGuardar, generarYGuardarPorSucursal,
  generarYGuardarPorDia, generarYGuardarPorSucursalYDia,
  _uuidsPorFechaEfectiva,
};
