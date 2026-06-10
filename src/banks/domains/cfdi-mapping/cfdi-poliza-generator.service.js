'use strict';

const CFDI                 = require('../../../visor/models/CFDI');
const { PolizaMovimiento, AccountPlan, CfdiMappingRule, Poliza } = require('../../../shared/models/postgres');
const centrosSvc = require('../centros-costo/centros-costo.service');
const { Op, QueryTypes }   = require('sequelize');
const { sequelize }        = require('../../../config/database.postgres');
const mappingSvc           = require('./cfdi-mapping.service');
const { _getRulesActive, _enrichTasaIvaFromRelatedCfdis, _normalizarEgresoPue99 } = require('./balanza-preliminar.service');
const ErpCuentaPendiente   = require('../erp/ErpCuentaPendiente.model');
const { BadRequestError }  = require('../../shared/errors/AppError');

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
 * Genera una PROPUESTA de póliza a partir de los CFDIs vigentes del periodo
 * que aún no tienen movimiento contable registrado.
 *
 * No guarda nada en base de datos — devuelve el objeto para que el
 * frontend lo muestre en el modal de revisión.
 */
const LIMITE_CFDIS = 500;
const CHUNK_SIZE   = 200;

async function generarPropuesta({ rfc, ejercicio, periodo, tipoPropuesta = 'D', tipoCfdi }) {
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
  const filtroBase = {
    $or:               [{ 'emisor.rfc': rfc }, { 'receptor.rfc': rfc }],
    ejercicio:         Number(ejercicio),
    periodo:           Number(periodo),
    tipoDeComprobante: tipoCfdi,
    source:            'SAT',
    satStatus:         'Vigente',
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

  const cfdisSinPoliza = cfdis.filter(c => !uuidsYaUsados.has(c.uuid));

  if (cfdisSinPoliza.length === 0) {
    throw new BadRequestError('Todos los CFDIs vigentes del periodo ya tienen póliza registrada');
  }

  // 3. Cargar reglas activas (cacheadas 60s)
  const rules = await _getRulesActive();

  // 4. Pre-fetch tipoDeComprobante de CFDIs relacionados para discriminador relacionadoTipo
  const relTipoUuidsProp = [...new Set(
    cfdisSinPoliza
      .flatMap(c => (c.cfdiRelacionados || []).map(r => r.uuid).filter(Boolean)),
  )];
  const relTipoCfdisArr = relTipoUuidsProp.length
    ? await CFDI.find({ uuid: { $in: relTipoUuidsProp } })
        .select('uuid tipoDeComprobante').lean()
    : [];
  const relTipoMap = Object.fromEntries(relTipoCfdisArr.map(c => [c.uuid, c.tipoDeComprobante]));

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
        (['E', 'P'].includes(c.tipoDeComprobante) && c.cfdiRelacionados?.length > 0 &&
         !c.cfdiRelacionados?.some(r => r.tipoRelacion === '04'))
      ))
      .map(c => c.uuid),
  );
  let erpMetaMap = {};
  if (uuidsSinMeta.size) {
    const erpCfdis = await CFDI.find({
      uuid:   { $in: [...uuidsSinMeta] },
      source: 'ERP',
    }).select('uuid formaPago metodoPago conceptos impuestos tipoOrigen cfdiRelacionados').lean();
    erpMetaMap = Object.fromEntries(erpCfdis.map(c => [c.uuid, c]));
  }
  const cfdisSinPolizaFinal = cfdisSinPolizaEnriquecidos.map(cfdi => {
    const erp = erpMetaMap[cfdi.uuid];
    if (!erp) return cfdi;
    const satHasTraslados = cfdi.conceptos?.some(con => con.impuestos?.traslados?.length);
    const relSAT    = cfdi.cfdiRelacionados ?? [];
    const tiposEnSAT = new Set(relSAT.map(r => r.tipoRelacion));
    const relERP    = (erp.cfdiRelacionados ?? []).filter(r => !tiposEnSAT.has(r.tipoRelacion));
    const metodoPagoFinal = (cfdi.metodoPago === 'PPD' && erp.metodoPago === 'PUE')
      ? 'PUE' : (cfdi.metodoPago || erp.metodoPago);
    return {
      ...cfdi,
      formaPago:        cfdi.formaPago  || erp.formaPago,
      metodoPago:       metodoPagoFinal,
      conceptos:        satHasTraslados ? cfdi.conceptos : (erp.conceptos?.length ? erp.conceptos : cfdi.conceptos ?? []),
      impuestos:        satHasTraslados ? cfdi.impuestos : (erp.impuestos  ?? cfdi.impuestos),
      tipoOrigen:       cfdi.tipoOrigen ?? erp.tipoOrigen ?? null,
      cfdiRelacionados: relERP.length ? [...relSAT, ...relERP] : relSAT,
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

  // 5. Precalcular regla por CFDI y recolectar todos los códigos de cuenta necesarios
  const cfdiConRegla = cfdisSinPolizaFinal.map(cfdi => ({
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
  // Centro de costo por serie de facturación del CFDI (asignación automática)
  const ccBySerieMapProp = await centrosSvc.resolveBySerieMap();

  const movimientosResult = [];
  let sinRegla = 0;

  for (const { cfdi, rule } of cfdiConRegla) {
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
  const fecha = new Date();
  const mesStr = String(periodo).padStart(2, '0');

  // ── Obs 4: detectar facturas PPD con tipoRelacion='07' que deberían ser PUE ──
  // Cuando el anticipo cubre el 100%, la factura final debe emitirse como PUE.
  // Si llega como PPD, el asiento queda incompleto (IVA en Por Trasladar en lugar de Trasladado).
  const ppd07 = cfdisSinPolizaFinal.filter(c =>
    c.tipoDeComprobante === 'I' &&
    c.metodoPago === 'PPD' &&
    c.cfdiRelacionados?.some(r => r.tipoRelacion === '07')
  );

  const advertencias = [];
  if (sinRegla > 0) {
    advertencias.push(`${sinRegla} CFDI(s) sin regla de mapeo — las cuentas deben asignarse manualmente`);
  }
  if (ppd07.length > 0) {
    advertencias.push(
      `⚠ ${ppd07.length} factura(s) PPD con tipoRelacion='07' (aplicación de anticipo): ` +
      `verificar si el anticipo cubre el 100% — en ese caso debió emitirse como PUE. ` +
      `Folios: ${ppd07.map(c => [c.serie, c.folio].filter(Boolean).join('-')).slice(0, 5).join(', ')}` +
      (ppd07.length > 5 ? ` y ${ppd07.length - 5} más` : ''),
    );
  }

  return {
    tipo:       tipoPropuesta,
    fecha:      fecha.toISOString().slice(0, 10),
    concepto:   `CFDIs ${mesStr}/${ejercicio} — ${cfdisSinPoliza.length} comprobante(s)`,
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
async function generarYGuardar({ rfc, ejercicio, periodo, tipoPropuesta = 'D', tipoCfdi }) {
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
  const filtroBase = {
    $or:               [{ 'emisor.rfc': rfc }, { 'receptor.rfc': rfc }],
    ejercicio:         Number(ejercicio),
    periodo:           Number(periodo),
    tipoDeComprobante: tipoCfdi,
    source:            'SAT',
    satStatus:         'Vigente',
    isActive:          true,
  };

  const cfdis = await CFDI.find(filtroBase)
    .select('uuid tipoDeComprobante metodoPago formaPago fecha folio serie emisor receptor subTotal total descuento impuestos complementoPago conceptos cfdiRelacionados tasaIvaInferida')
    .lean();

  const cfdisSinPoliza = cfdis.filter(c => !uuidsYaUsados.has(c.uuid));

  if (cfdisSinPoliza.length === 0) {
    throw new BadRequestError('Todos los CFDIs vigentes del periodo ya tienen póliza registrada');
  }

  // 3. Cargar reglas activas (cacheadas 60s)
  const rules = await _getRulesActive();

  // 4. Pre-fetch tipoDeComprobante de CFDIs relacionados para discriminador relacionadoTipo
  const relTipoUuidsGuard = [...new Set(
    cfdisSinPoliza
      .flatMap(c => (c.cfdiRelacionados || []).map(r => r.uuid).filter(Boolean)),
  )];
  const relTipoCfdisGuard = relTipoUuidsGuard.length
    ? await CFDI.find({ uuid: { $in: relTipoUuidsGuard } })
        .select('uuid tipoDeComprobante').lean()
    : [];
  const relTipoMapGuard = Object.fromEntries(
    relTipoCfdisGuard.map(c => [c.uuid, c.tipoDeComprobante]),
  );

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
        (['E', 'P'].includes(c.tipoDeComprobante) && c.cfdiRelacionados?.length > 0 &&
         !c.cfdiRelacionados?.some(r => r.tipoRelacion === '04'))
      ))
      .map(c => c.uuid),
  );
  let erpMetaMapGuard = {};
  if (uuidsSinMetaGuard.size) {
    const erpCfdisGuard = await CFDI.find({
      uuid:   { $in: [...uuidsSinMetaGuard] },
      source: 'ERP',
    }).select('uuid formaPago metodoPago conceptos impuestos tipoOrigen cfdiRelacionados').lean();
    erpMetaMapGuard = Object.fromEntries(erpCfdisGuard.map(c => [c.uuid, c]));
  }
  const cfdisSinPolizaFinalGuard = cfdisSinPolizaEnriquecidosGuard.map(cfdi => {
    const erp = erpMetaMapGuard[cfdi.uuid];
    if (!erp) return cfdi;
    const satHasTraslados = cfdi.conceptos?.some(con => con.impuestos?.traslados?.length);
    const relSAT    = cfdi.cfdiRelacionados ?? [];
    const tiposEnSAT = new Set(relSAT.map(r => r.tipoRelacion));
    const relERP    = (erp.cfdiRelacionados ?? []).filter(r => !tiposEnSAT.has(r.tipoRelacion));
    const metodoPagoFinal = (cfdi.metodoPago === 'PPD' && erp.metodoPago === 'PUE')
      ? 'PUE' : (cfdi.metodoPago || erp.metodoPago);
    return {
      ...cfdi,
      formaPago:        cfdi.formaPago  || erp.formaPago,
      metodoPago:       metodoPagoFinal,
      conceptos:        satHasTraslados ? cfdi.conceptos : (erp.conceptos?.length ? erp.conceptos : cfdi.conceptos ?? []),
      impuestos:        satHasTraslados ? cfdi.impuestos : (erp.impuestos  ?? cfdi.impuestos),
      tipoOrigen:       cfdi.tipoOrigen ?? erp.tipoOrigen ?? null,
      cfdiRelacionados: relERP.length ? [...relSAT, ...relERP] : relSAT,
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

  // 5. Precalcular regla por CFDI y resolver cuentaMap en un solo query
  const cfdiConRegla = cfdisSinPolizaFinalGuard.map(cfdi => ({
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
  // Centro de costo por serie de facturación del CFDI (asignación automática)
  const ccBySerieMap = await centrosSvc.resolveBySerieMap();

  const todosLosMovimientos = [];
  let sinRegla = 0;
  const advertencias = [];
  // Diagnóstico: acumular los primeros 5 CFDIs sin regla para dar info útil
  const muestrasSinRegla = [];

  for (const { cfdi, rule } of cfdiConRegla) {
    if (!rule) {
      sinRegla++;
      if (muestrasSinRegla.length < 5) {
        muestrasSinRegla.push({
          uuid:    cfdi.uuid?.slice(0, 8),
          tipo:    cfdi.tipoDeComprobante,
          metodo:  cfdi.metodoPago,
          forma:   cfdi.formaPago,
          emisor:  cfdi.emisor?.rfc,
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

    const movs = await mappingSvc.cfdiToMovimientos(cfdi, rule, cuentaMap, context);

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

  // 7. Guardar póliza + movimientos en una transacción con advisory lock
  const fecha    = new Date();
  const mesStr   = String(periodo).padStart(2, '0');
  const concepto = `CFDIs ${mesStr}/${ejercicio} — ${cfdisSinPoliza.length} comprobante(s)`;

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

  const advertenciasFinal = [];
  if (sinRegla > 0) {
    advertenciasFinal.push(`${sinRegla} CFDI(s) omitidos por no tener regla de mapeo`);
    // Muestra diagnóstico de los primeros 5 ignorados
    for (const m of muestrasSinRegla) {
      advertenciasFinal.push(
        `  Ej. ${m.uuid}… → tipo=${m.tipo} método=${m.metodo || '—'} forma=${m.forma || '—'} emisor=${m.emisor || '—'}`,
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
    polizaId:     poliza.id,
    totalCfdis:   cfdisSinPoliza.length,
    sinRegla,
    advertencias: advertenciasFinal,
  };
}

module.exports = { generarPropuesta, generarYGuardar };
