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
  const cfdisSinPolizaFinalFiltrado = _canceladosPorSustitutoProp.size
    ? cfdisSinPolizaFinal.filter(c =>
        !_canceladosPorSustitutoProp.has(c.uuid?.toUpperCase() ?? '')
      )
    : cfdisSinPolizaFinal;

  // 5. Precalcular regla por CFDI y recolectar todos los códigos de cuenta necesarios
  const cfdiConRegla = cfdisSinPolizaFinalFiltrado.map(cfdi => ({
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
  const cfdisSinPolizaFinalGuardFiltrado = _canceladosPorSustitutoGuard.size
    ? cfdisSinPolizaFinalGuard.filter(c =>
        !_canceladosPorSustitutoGuard.has(c.uuid?.toUpperCase() ?? '')
      )
    : cfdisSinPolizaFinalGuard;

  // 5. Precalcular regla por CFDI y resolver cuentaMap en un solo query
  const cfdiConRegla = cfdisSinPolizaFinalGuardFiltrado.map(cfdi => ({
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

  // Incrementar contador de uso por regla (fuera de la transacción para no bloquearla)
  if (ruleUsageCount.size > 0) {
    await Promise.all(
      [...ruleUsageCount.entries()].map(([id, count]) =>
        CfdiMappingRule.increment('vecesUsada', { by: count, where: { id } }),
      ),
    );
  }

  const advertenciasFinal = [];
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
    polizaId:     poliza.id,
    totalCfdis:   cfdisSinPoliza.length,
    sinRegla,
    advertencias: advertenciasFinal,
  };
}

module.exports = { generarPropuesta, generarYGuardar };
