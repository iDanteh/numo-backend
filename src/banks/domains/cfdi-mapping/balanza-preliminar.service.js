'use strict';

const CFDI                = require('../../../visor/models/CFDI');
const ErpCuentaPendiente  = require('../erp/ErpCuentaPendiente.model');
const { AccountPlan, CfdiMappingRule, Poliza, PolizaMovimiento } = require('../../../shared/models/postgres');
const { Op }              = require('sequelize');
const mappingSvc          = require('./cfdi-mapping.service');
const { BadRequestError }          = require('../../shared/errors/AppError');
const { repararSubtotalDesdeXml }  = require('../../../visor/services/cfdiSubtotalRepair');

// ── Helpers de enriquecimiento en memoria para tasaIvaInferida ────────────────

function _tasaDesdeConceptos(cfdi) {
  let tiene16 = false;
  let tiene0  = false;
  for (const c of (cfdi.conceptos || [])) {
    for (const t of (c.impuestos?.traslados || [])) {
      if ((t.impuesto || t.Impuesto || '') !== '002') continue;
      if (Number(t.tasaOCuota ?? t.TasaOCuota ?? 0) > 0) tiene16 = true;
      else tiene0 = true;
    }
  }
  // Fallback 1: IVA a nivel header (cfdi.impuestos.traslados) — igual que _detectTasaIva
  if (!tiene16 && !tiene0) {
    for (const t of (cfdi.impuestos?.traslados || [])) {
      if ((t.impuesto || t.Impuesto || '') !== '002') continue;
      if (Number(t.tasaOCuota ?? t.TasaOCuota ?? 0) > 0) tiene16 = true;
      else tiene0 = true;
    }
  }
  // Fallback 2: totalImpuestosTrasladados — mismo criterio mixto que _detectTasaIva ($0.50)
  if (!tiene16 && !tiene0) {
    const rawTot = cfdi.impuestos?.totalImpuestosTrasladados;
    if (rawTot != null) {
      const totalImptos = Number(rawTot);
      if (totalImptos <= 0) {
        tiene0 = true;
      } else {
        const base = Number(cfdi.subTotal || 0) - Number(cfdi.descuento || 0);
        if (base > 0 && Math.abs(totalImptos - base * 0.16) > 0.50) {
          tiene16 = true;
          tiene0  = true; // mixto
        } else {
          tiene16 = true;
        }
      }
    }
  }
  if (tiene16 && tiene0) return 'mixto';
  if (tiene16) return '16';
  if (tiene0)  return '0';
  return null;
}

/**
 * Enriquece tasaIvaInferida en memoria para CFDIs P Metadata buscando las
 * facturas relacionadas (tipo I/E) en MongoDB SAT por UUID.
 * Paso 1 del enriquecimiento — NO escribe a MongoDB.
 */
async function _enrichTasaIvaFromRelatedCfdis(cfdis) {
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

  const facturas = await CFDI.find(
    { uuid: { $in: [...uuidToIdxs.keys()] }, tipoDeComprobante: { $in: ['I', 'E'] } },
    { uuid: 1, conceptos: 1, impuestos: 1 },
  ).lean();
  if (!facturas.length) return;

  const tasasPorIdx = new Map();
  for (const factura of facturas) {
    const uuidNorm = (factura.uuid || '').trim().toUpperCase();
    const tasa = _tasaDesdeConceptos(factura);
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

// Cache de reglas activas — TTL 60 segundos para evitar queries repetidas por request
let _rulesCache = null;
let _rulesCacheAt = 0;
async function _getRulesActive() {
  if (_rulesCache && Date.now() - _rulesCacheAt < 60_000) return _rulesCache;
  _rulesCache = await CfdiMappingRule.findAll({ where: { isActive: true }, order: [['prioridad', 'ASC']] });
  _rulesCacheAt = Date.now();
  return _rulesCache;
}

/**
 * Pipeline compartido de enriquecimiento y filtrado de CFDIs.
 * Usado por generarBalanzaPreliminar, generarDetalleCuenta y generarDetalleExport.
 * Recibe CFDIs ya cargados desde MongoDB y devuelve el array procesado y filtrado.
 *
 * @param {Array}  cfdis                       - CFDIs cargados desde MongoDB (SAT Vigentes)
 * @param {string} tipo                         - Tipo de comprobante: 'I', 'E' o 'P'
 * @param {object} opts
 * @param {boolean} opts.excluirPagosSustitutos - Excluir CFDI cancelado cuando existe sustituto ('04')
 * @param {Set}    opts.uuidsFacturasPueAnticipo - UUIDs de facturas PUE modelo-2-asientos (anticipo)
 */
async function _enrichAndFilterCfdis(cfdis, tipo, { excluirPagosSustitutos, uuidsFacturasPueAnticipo }) {
  // 1. Detectar UUIDs que requieren enriquecimiento desde ERP
  const uuidsParaEnriquecer = new Set(
    cfdis
      .filter(c => c.uuid && (
        !c.formaPago ||
        !c.metodoPago ||
        !c.conceptos?.length ||
        c.conceptos.every(con => !(con.impuestos?.traslados?.length)) ||
        // Tipo I PPD: cargar ERP para detectar si fue cobrado de contado (ERP=PUE)
        (c.tipoDeComprobante === 'I' && c.metodoPago === 'PPD') ||
        // Enriquecer sustitutos (tipoRelacion='04'): necesitan formaPago/conceptos/tipoOrigen del ERP
        ['E', 'P'].includes(c.tipoDeComprobante) && c.cfdiRelacionados?.length > 0
      ))
      .map(c => c.uuid)
  );

  // 2. Cargar metadata ERP
  let erpMetaMap = {};
  if (uuidsParaEnriquecer.size) {
    const erpCfdis = await CFDI.find({
      uuid:   { $in: [...uuidsParaEnriquecer] },
      source: 'ERP',
    }).select('uuid formaPago metodoPago conceptos impuestos tipoOrigen cfdiRelacionados documentosRelacionados').lean();
    erpMetaMap = Object.fromEntries(erpCfdis.map(c => [c.uuid, c]));
  }

  // 3. Aplicar enriquecimiento en memoria
  const cfdisEnriquecidos = cfdis.map(cfdi => {
    const erp = erpMetaMap[cfdi.uuid];
    if (!erp) return cfdi;
    const satHasTraslados     = cfdi.conceptos?.some(con => con.impuestos?.traslados?.length);
    const satHasBaseTraslados = (cfdi.impuestos?.traslados ?? []).some(t => (t.base ?? 0) > 0);

    const relSAT     = cfdi.cfdiRelacionados ?? [];
    const tiposEnSAT = new Set(relSAT.map(r => r.tipoRelacion));
    const relERP     = (erp.cfdiRelacionados ?? []).filter(r => {
      if (tiposEnSAT.has(r.tipoRelacion)) return false;
      // Para tipo E: no inyectar '07' del ERP si el SAT ya tiene cfdiRelacionados.
      // El SAT marca la NC como '01'; el ERP a veces tiene '07' (anticipo).
      // Priorizar SAT para evitar que NCs normales caigan a Reg 23 (anticipo).
      if (cfdi.tipoDeComprobante === 'E' && r.tipoRelacion === '07' && relSAT.length > 0) return false;
      return true;
    });

    // Si SAT dice PPD pero ERP dice PUE → cobro inmediato, usar PUE.
    const metodoPagoFinal = (cfdi.metodoPago === 'PPD' && erp.metodoPago === 'PUE')
      ? 'PUE'
      : (cfdi.metodoPago || erp.metodoPago);

    const esBCT = erp.documentosRelacionados?.some(d => d.Serie === 'BCT');
    const esBON = !esBCT && erp.documentosRelacionados?.some(d => (d.Serie ?? '').startsWith('BON'));
    return {
      ...cfdi,
      formaPago:              cfdi.formaPago  || erp.formaPago,
      metodoPago:             metodoPagoFinal,
      conceptos:              satHasTraslados     ? cfdi.conceptos : (erp.conceptos?.length ? erp.conceptos : cfdi.conceptos ?? []),
      impuestos:              satHasBaseTraslados ? cfdi.impuestos : (erp.impuestos ?? cfdi.impuestos),
      tipoOrigen:             esBCT ? 'Bonificación Club Tuberos' : esBON ? 'Bonificación' : (cfdi.tipoOrigen ?? erp.tipoOrigen ?? null),
      documentosRelacionados: erp.documentosRelacionados ?? cfdi.documentosRelacionados ?? [],
      cfdiRelacionados:       relERP.length ? [...relSAT, ...relERP] : relSAT,
    };
  });

  // 4. Enriquecer tasaIvaInferida para tipo P (2 pasos: MongoDB SAT + ErpCuentaPendiente)
  if (tipo === 'P') {
    await _enrichTasaIvaFromRelatedCfdis(cfdisEnriquecidos);

    const sinTasaErp = cfdisEnriquecidos.filter(c =>
      c.tasaIvaInferida == null &&
      !c.complementoPago?.pagos?.length &&
      c.cfdiRelacionados?.length,
    );

    if (sinTasaErp.length) {
      const uuidToIdxs = new Map();
      for (let i = 0; i < sinTasaErp.length; i++) {
        const uuids = (sinTasaErp[i].cfdiRelacionados ?? [])
          .flatMap(r => r.uuids ?? [])
          .flatMap(u => u.split(/\s*\|\s*/))
          .map(u => u.trim().toUpperCase())
          .filter(u => u.length >= 32);
        for (const uuid of uuids) {
          if (!uuidToIdxs.has(uuid)) uuidToIdxs.set(uuid, []);
          uuidToIdxs.get(uuid).push(i);
        }
      }
      if (uuidToIdxs.size) {
        const erpDocs = await ErpCuentaPendiente.find(
          { folioFiscal: { $in: [...uuidToIdxs.keys()] } },
          { folioFiscal: 1, factorImpuesto: 1, impuesto: 1, subtotal: 1 },
        ).lean();
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
          sinTasaErp[idx].tasaIvaInferida =
            (tiene16 && tiene0) ? 'mixto' : tiene16 ? '16' : tiene0 ? '0' : null;
        }
      }
    }
  }

  // 5. Normalización: E PUE formaPago=99 → PPD (antes de matching)
  _normalizarEgresoPue99(cfdisEnriquecidos);

  // 5B. Normalización: E formaPago=15 (Condonación) → metodoPago real de la
  // factura relacionada (ver _normalizarEgresoCondonacion). Solo se consulta
  // la BD si hay al menos una NC con formaPago=15 en este lote.
  const relCondonacionUuids = [...new Set(
    cfdisEnriquecidos
      .filter(c => c.tipoDeComprobante === 'E' && c.formaPago === '15')
      .flatMap(c => (c.cfdiRelacionados ?? [])
        .filter(r => r.tipoRelacion === '01' || r.tipoRelacion === '03')
        .flatMap(r => r.uuids ?? (r.uuid ? [r.uuid] : []))),
  )];
  if (relCondonacionUuids.length) {
    const facturasRelacionadas = await CFDI.find({ uuid: { $in: relCondonacionUuids } })
      .select('uuid metodoPago').lean();
    const metodoPagoRelacionado = Object.fromEntries(facturasRelacionadas.map(f => [f.uuid, f.metodoPago]));
    _normalizarEgresoCondonacion(cfdisEnriquecidos, metodoPagoRelacionado);
  }

  // 5C. Normalización: E con medio de pago real (Efectivo/Cheque/Transferencia/
  // Tarjeta) que ajusta una factura PPD nunca cobrada → formaPago+metodoPago
  // de esa factura (ver _normalizarEgresoSegunFacturaRelacionada) — misma
  // regla que ya aplica al generar la póliza real; sin esto la balanza
  // preliminar mostraría una cuenta distinta a la que la póliza usa.
  const relPagoRealUuids = [...new Set(
    cfdisEnriquecidos
      .filter(c => c.tipoDeComprobante === 'E' && FORMAS_PAGO_REALES.includes(c.formaPago))
      .flatMap(c => (c.cfdiRelacionados ?? [])
        .filter(r => r.tipoRelacion === '01' || r.tipoRelacion === '03')
        .flatMap(r => r.uuids ?? (r.uuid ? [r.uuid] : []))),
  )];
  if (relPagoRealUuids.length) {
    const facturasPagoReal = await CFDI.find({ uuid: { $in: relPagoRealUuids } })
      .select('uuid metodoPago formaPago').lean();
    const facturaRelacionada = Object.fromEntries(facturasPagoReal.map(f => [f.uuid, { metodoPago: f.metodoPago, formaPago: f.formaPago }]));
    _normalizarEgresoSegunFacturaRelacionada(cfdisEnriquecidos, facturaRelacionada);
  }

  // 6. Marcar sustitutos — siempre, independiente de excluirPagosSustitutos.
  //    El marcado es información factual del CFDI (tipoRelacion='04').
  //    La EXCLUSIÓN del saldo de CFDI-A es condicional (ver generarBalanzaPreliminar).
  //    CFDI-B (sustituto): _meta.esSustituto=true + sustituyeA=[UUID-A]
  //    CFDI-A (original, si aparece como Vigente): _meta.fueReemplazado=true + reemplazadoPor=UUID-B
  {
    const originalASustituto = new Map(); // uuid-A (upper) → uuid-B (upper)
    for (const c of cfdisEnriquecidos) {
      if (!['P', 'E'].includes(c.tipoDeComprobante)) continue;
      const rels04 = (c.cfdiRelacionados || []).filter(r => r.tipoRelacion === '04');
      if (!rels04.length) continue;
      const uuidB      = (c.uuid ?? '').toUpperCase();
      const sustituyeA = rels04.flatMap(r => (r.uuids ?? (r.uuid ? [r.uuid] : [])).map(u => u.toUpperCase()));
      for (const uuidA of sustituyeA) originalASustituto.set(uuidA, uuidB);
      c._meta = { esSustituto: true, sustituyeA };
    }
    // Marcar CFDI-A si aparece en el array (edge case: aún figura como Vigente)
    for (const c of cfdisEnriquecidos) {
      if (c._meta) continue;
      const uuidUpper = (c.uuid ?? '').toUpperCase();
      if (originalASustituto.has(uuidUpper)) {
        c._meta = { fueReemplazado: true, reemplazadoPor: originalASustituto.get(uuidUpper) };
      }
    }
  }
  const cfdisBase = cfdisEnriquecidos;

  // 7. Fix doble-contabilización anticipo PUE (modelo 2 asientos: cuentaCargo=2103010001)
  if (!uuidsFacturasPueAnticipo.size) return cfdisBase;
  return cfdisBase.filter(c => {
    if (c.tipoDeComprobante !== 'E') return true;
    if (!c.cfdiRelacionados?.some(r => r.tipoRelacion === '07')) return true;
    const rel07  = (c.cfdiRelacionados || []).find(r => r.tipoRelacion === '07');
    const uuid07 = (rel07?.uuids?.[0] ?? rel07?.uuid ?? '').toUpperCase() || undefined;
    return !(uuid07 && uuidsFacturasPueAnticipo.has(uuid07));
  });
}

/**
 * Genera una balanza de comprobación preliminar a partir de los CFDIs vigentes
 * del periodo, aplicando las reglas de mapeo activas.
 * No crea ni modifica pólizas — es solo lectura/cálculo.
 *
 * @returns {Promise<{
 *   cuentas: Array,
 *   totales: { debe: number, haber: number },
 *   meta:    { totalCfdis, sinRegla, periodo, ejercicio, tipos }
 * }>}
 */
async function generarBalanzaPreliminar({ rfc, ejercicio, periodo, tipoCfdi, excluirPagosSustitutos = false, excluirAplicacionesAnticipos = false, excluirReclasificaciones = false, incluirFechaCruzada = false, excluirMesesPosteriores = false }) {
  if (!rfc)       throw new BadRequestError('RFC requerido');
  if (!ejercicio) throw new BadRequestError('Ejercicio requerido');
  if (!periodo)   throw new BadRequestError('Periodo requerido');

  const tipos = tipoCfdi ? [tipoCfdi] : ['I', 'E', 'P'];
  // Filtro sustitutos: cuando excluirPagosSustitutos=true, se resuelve en memoria
  // post-enriquecimiento (no en MongoDB). El CFDI cancelado (original) se excluye;
  // el sustituto (tipoRelacion='04') se mantiene. Así NUMO espeja el comportamiento
  // de CONTPAQi, que conserva solo el CFDI final vigente.
  const filtroPagosSustitutos = {};  // manejado en memoria — ver _enrichAndFilterCfdis

  // Filtro 3 + 4: control de qué CFDIs incluir según periodo/fecha
  //
  // filtroPeriodo:
  //   Normal:              periodo = N  (comportamiento por defecto)
  //   incluirFechaCruzada: $or[ periodo=N, month(fecha)=N ]
  //                        → agrega CFDIs de otros periodos cuya fecha es del mes N
  //
  // filtroReclasificaciones (toggle 3):
  //   Cuando activo: exige month(fecha) = N dentro del resultado ya filtrado
  //   → quita los reclasificados con fecha fuera del mes
  const filtroPeriodo = incluirFechaCruzada
    ? { $or: [{ periodo: Number(periodo) }, { $expr: { $eq: [{ $month: '$fecha' }, Number(periodo)] } }] }
    : { periodo: Number(periodo) };

  const filtroReclasificaciones = excluirReclasificaciones
    ? { $expr: { $eq: [{ $month: '$fecha' }, Number(periodo)] } }
    : {};

  // Filtro 5: excluir solo reclasificaciones de meses POSTERIORES al periodo.
  const filtroMesesPosteriores = excluirMesesPosteriores
    ? { $expr: { $lte: [{ $month: '$fecha' }, Number(periodo)] } }
    : {};

  // Filtro 2: excluir aplicaciones de anticipos — NO USAR (excluirAplicacionesAnticipos=false).
  const filtroAnticipos = excluirAplicacionesAnticipos
    ? { $nor: [
        { tipoDeComprobante: 'I', 'cfdiRelacionados.tipoRelacion': '07' },
        { tipoDeComprobante: 'E', 'cfdiRelacionados.tipoRelacion': '07' },
      ]}
    : {};

  // 1. Cargar reglas activas (cacheadas 60s)
  const rules = await _getRulesActive();

  // 2. Precalcular cuentaMap para todos los códigos de todas las reglas activas
  const codigosTodos = [...new Set(
    rules.flatMap(r => [
      r.cuentaCargo,        r.cuentaAbono,        r.cuentaAbono2,
      r.cuentaIva,          r.cuentaIvaPPD,       r.cuentaIvaRetenido,
      r.cuentaIsrRetenido,  r.cuentaIvaAnticipo,  r.cuentaDeltaAnticipo,
      r.cuentaCargo2,       r.cuentaDescuento,    r.cuentaDescuento0,
      r.cuentaCargoMixto0,  r.cuentaIvaAbono,
    ].filter(Boolean)),
  )];

  const cuentasRows = codigosTodos.length
    ? await AccountPlan.findAll({
        where:      { codigo: { [Op.in]: codigosTodos } },
        attributes: ['id', 'codigo', 'nombre', 'tipo'],
        raw:        true,
      })
    : [];

  const cuentaMapById   = Object.fromEntries(cuentasRows.map(c => [c.id,     c]));
  const cuentaMapByCod  = Object.fromEntries(cuentasRows.map(c => [c.codigo, c.id]));

  // 3a. Pre-query: UUIDs de facturas PUE (tipo I, formaPago=30) del periodo.
  const uuidsFacturasPueAnticipo = new Set();
  {
    const _facturasPue = await CFDI.find({
      $or:               [{ 'emisor.rfc': rfc }, { 'receptor.rfc': rfc }],
      ejercicio:         Number(ejercicio),
      ...filtroPeriodo,
      tipoDeComprobante: 'I',
      formaPago:         '30',
      source:            'SAT',
      satStatus:         'Vigente',
      isActive:          true,
      ...filtroMesesPosteriores,
    }).select('uuid tipoDeComprobante emisor receptor metodoPago formaPago conceptos cfdiRelacionados tipoOrigen').lean();
    for (const c of _facturasPue) {
      if (!c.uuid) continue;
      const _r = mappingSvc.findRuleInList(c, rules);
      if (_r?.cuentaCargo === '2103010001') uuidsFacturasPueAnticipo.add(c.uuid.toUpperCase());
    }
  }

  // 3. Procesar CFDIs por tipo
  const movimientosTodos = [];
  let totalCfdis = 0;
  let sinRegla   = 0;

  for (const tipo of tipos) {
    const cfdis = await CFDI.find({
      $or:               [{ 'emisor.rfc': rfc }, { 'receptor.rfc': rfc }],
      ejercicio:         Number(ejercicio),
      ...filtroPeriodo,
      tipoDeComprobante: tipo,
      source:            'SAT',
      satStatus:         'Vigente',
      isActive:          true,
      ...filtroPagosSustitutos,
      ...filtroAnticipos,
      ...filtroReclasificaciones,
      ...filtroMesesPosteriores,
    })
      .select('uuid tipoDeComprobante metodoPago formaPago emisor.rfc receptor.rfc subTotal total descuento impuestos conceptos.importe conceptos.Importe conceptos.descuento conceptos.Descuento conceptos.impuestos conceptos.descripcion conceptos.Descripcion complementoPago.totales complementoPago.pagos.monto complementoPago.pagos.formaDePagoP complementoPago.pagos.doctosRelacionados.trasladosDR cfdiRelacionados tasaIvaInferida')
      .maxTimeMS(60_000)
      .lean();

    await repararSubtotalDesdeXml(cfdis);

    totalCfdis += cfdis.length;

    const cfdisParaBalanza = await _enrichAndFilterCfdis(cfdis, tipo, {
      excluirPagosSustitutos,
      uuidsFacturasPueAnticipo,
    });

    const resultados = await Promise.all(
      cfdisParaBalanza.map(async (cfdi) => {
        // Opción C: el original marcado como fueReemplazado no aporta al saldo
        if (excluirPagosSustitutos && cfdi._meta?.fueReemplazado) return { sinRegla: 0, movs: [] };
        const rule = mappingSvc.findRuleInList(cfdi, rules);
        if (!rule) return { sinRegla: 1, movs: [] };
        const movs = await mappingSvc.cfdiToMovimientos(cfdi, rule, cuentaMapByCod);
        return { sinRegla: 0, movs };
      }),
    );

    for (const { sinRegla: sr, movs } of resultados) {
      sinRegla += sr;
      for (const m of movs) movimientosTodos.push({ ...m, tipoCfdi: tipo });
    }
  }

  // 4. Agrupar por cuenta
  const byAccount = {};

  for (const m of movimientosTodos) {
    if (m.cuentaId == null) continue; // sin cuenta asignada — omitir

    const info = cuentaMapById[m.cuentaId] ?? { codigo: String(m.cuentaId), nombre: 'Cuenta no encontrada', tipo: '?' };
    const key  = info.codigo;

    if (!byAccount[key]) {
      byAccount[key] = {
        codigo:   info.codigo,
        nombre:   info.nombre,
        tipo:     info.tipo,
        debe:     0,
        haber:    0,
        movCount: 0,
      };
    }

    byAccount[key].debe     = Math.round((byAccount[key].debe  + (Number(m.debe)  || 0)) * 100) / 100;
    byAccount[key].haber    = Math.round((byAccount[key].haber + (Number(m.haber) || 0)) * 100) / 100;
    byAccount[key].movCount += 1;
  }

  // 5. Saldo inicial = acumulado de movimientos de pólizas contabilizadas de periodos anteriores.
  const polizasAnt = await Poliza.findAll({
    where: {
      rfc:    rfc,
      estado: 'contabilizada',
      [Op.or]: [
        { ejercicio: { [Op.lt]: Number(ejercicio) } },
        { ejercicio: Number(ejercicio), periodo: { [Op.lt]: Number(periodo) } },
      ],
    },
    attributes: ['id'],
    raw: true,
  });

  const saldoInicialMap = {};  // codigo → saldo acumulado previo

  if (polizasAnt.length > 0) {
    const polizaIds = polizasAnt.map(p => p.id);
    const movAnt = await PolizaMovimiento.findAll({
      where:      { polizaId: { [Op.in]: polizaIds } },
      include: [{ model: AccountPlan, as: 'cuenta', attributes: ['codigo'], required: false }],
      raw:    true,
      nest:   true,
    });

    for (const m of movAnt) {
      const cod = m.cuenta?.codigo;
      if (!cod) continue;
      if (!saldoInicialMap[cod]) saldoInicialMap[cod] = 0;
      saldoInicialMap[cod] += Number(m.debe || 0) - Number(m.haber || 0);
    }
  }

  // 5.5. Rollup jerárquico: propagar saldos de cuentas hoja a cuentas padre.
  const codigosConMovimiento = new Set(Object.keys(byAccount));
  const todasCuentas = await AccountPlan.findAll({
    where:      { isActive: true },
    attributes: ['id', 'codigo', 'nombre', 'tipo', 'nivel', 'parentId'],
    raw:        true,
  });
  const cuentaById  = Object.fromEntries(todasCuentas.map(c => [c.id,     c]));
  const cuentaByCod = Object.fromEntries(todasCuentas.map(c => [c.codigo, c]));

  for (const [codigo, acc] of Object.entries(byAccount)) {
    const hoja = cuentaByCod[codigo];
    if (!hoja?.parentId) continue;

    // Pre-redondear para que la suma de hijos coincida exactamente con el padre
    const dR = Math.round(acc.debe  * 100) / 100;
    const hR = Math.round(acc.haber * 100) / 100;
    const sR = Math.round((saldoInicialMap[codigo] ?? 0) * 100) / 100;

    let parentId = hoja.parentId;
    while (parentId != null) {
      const padre = cuentaById[parentId];
      if (!padre) break;

      if (!byAccount[padre.codigo]) {
        byAccount[padre.codigo] = {
          codigo:       padre.codigo,
          nombre:       padre.nombre,
          tipo:         padre.tipo,
          nivel:        padre.nivel,
          esAgrupadora: true,   // excluir de sumas iguales — ya agrupa sus hijos
          debe:         0,
          haber:        0,
          movCount:     0,
        };
        saldoInicialMap[padre.codigo] = 0;
      }

      byAccount[padre.codigo].debe     = Math.round((byAccount[padre.codigo].debe  + dR) * 100) / 100;
      byAccount[padre.codigo].haber    = Math.round((byAccount[padre.codigo].haber + hR) * 100) / 100;
      byAccount[padre.codigo].movCount += acc.movCount;
      saldoInicialMap[padre.codigo]    = Math.round((saldoInicialMap[padre.codigo] + sR) * 100) / 100;

      parentId = padre.parentId;
    }
  }

  // Agregar nivel a las cuentas hoja que ya estaban en byAccount
  for (const [codigo, acc] of Object.entries(byAccount)) {
    if (acc.nivel == null) {
      const c = cuentaByCod[codigo];
      if (c?.nivel != null) acc.nivel = c.nivel;
    }
  }

  // 6. Calcular saldo y ordenar por código
  const cuentas = Object.values(byAccount)
    .map(c => ({
      ...c,
      debe:          Math.round(c.debe  * 100) / 100,
      haber:         Math.round(c.haber * 100) / 100,
      saldoInicial:  Math.round((saldoInicialMap[c.codigo] ?? 0) * 100) / 100,
      saldo:         Math.round((c.debe - c.haber + (saldoInicialMap[c.codigo] ?? 0)) * 100) / 100,
    }))
    .sort((a, b) => a.codigo.localeCompare(b.codigo, undefined, { numeric: true }));

  // Las sumas iguales usan solo cuentas hoja (no agrupadora) para evitar
  // doble conteo: cada cuenta agrupadora ya incluye el saldo de sus hijos.
  const hoja = cuentas.filter(c => !c.esAgrupadora);
  const totales = {
    debe:         hoja.reduce((s, c) => Math.round((s + c.debe)         * 100) / 100, 0),
    haber:        hoja.reduce((s, c) => Math.round((s + c.haber)        * 100) / 100, 0),
    saldoInicial: hoja.reduce((s, c) => Math.round((s + c.saldoInicial) * 100) / 100, 0),
    saldoFinal:   hoja.reduce((s, c) => Math.round((s + c.saldo)        * 100) / 100, 0),
  };
  // Normalizar diferencia de redondeo: en partida doble las sumas DEBEN ser iguales;
  // una diferencia ≤ 0.01 es artefacto de redondeo por cuenta, no un descuadre real.
  if (Math.abs(totales.debe - totales.haber) <= 0.01) {
    const max = Math.max(totales.debe, totales.haber);
    totales.debe  = max;
    totales.haber = max;
  }

  return {
    cuentas,
    totales,
    meta: {
      totalCfdis,
      sinRegla,
      periodo:   Number(periodo),
      ejercicio: Number(ejercicio),
      tipos,
    },
  };
}

/**
 * Normalización en memoria: Egreso PUE + formaPago=99 (Por definir) → trata como PPD.
 * Aplica ANTES del matching de reglas. No escribe a MongoDB.
 * Razón: los egresos capturados con formaPago 99 son crédito real, no contado.
 */
function _normalizarEgresoPue99(cfdis) {
  for (const cfdi of cfdis) {
    if (cfdi.tipoDeComprobante === 'E' &&
        cfdi.metodoPago === 'PUE' &&
        cfdi.formaPago  === '99') {
      cfdi.metodoPago = 'PPD';
    }
  }
}

/**
 * Normalización en memoria: Egreso con formaPago=15 (Condonación) → usa el
 * metodoPago REAL de la factura que ajusta (vía cfdiRelacionados 01/03) en
 * vez del propio. Aplica ANTES del matching de reglas. No escribe a MongoDB.
 *
 * Razón: a diferencia de formaPago=99 (inválido en PUE por regla del SAT),
 * 15 SÍ es válido en un comprobante PUE, así que no hay ninguna señal de
 * invalidez que delate el dato incorrecto. En la práctica, el ERP declara
 * casi toda Nota de Crédito de bonificación/condonación como PUE sin importar
 * si la factura que ajusta es de contado o de crédito — porque "Condonación"
 * nunca implica un movimiento real de efectivo. Confirmado con datos reales:
 * 100% de las NC formaPago=15 de una muestra (1,936 de 1,936) declaraban PUE
 * mientras su factura relacionada era PPD (ver diag-nc-metodopago-cruzado.js).
 * Sin este fix, esas NC caen en el bloque/folio de CONTPAQi equivocado,
 * abonan a Bancos/Caja en vez de a Clientes, y cancelan IVA Trasladado en vez
 * de IVA Por Trasladar PPD — dejando ese pasivo puente sin cancelar.
 *
 * @param {Array} cfdis
 * @param {Object<string,string>} metodoPagoRelacionado - uuid de factura → su metodoPago
 */
function _normalizarEgresoCondonacion(cfdis, metodoPagoRelacionado) {
  if (!metodoPagoRelacionado) return;
  for (const cfdi of cfdis) {
    if (cfdi.tipoDeComprobante !== 'E' || cfdi.formaPago !== '15') continue;
    const relUuid = (cfdi.cfdiRelacionados ?? [])
      .filter(r => r.tipoRelacion === '01' || r.tipoRelacion === '03')
      .flatMap(r => r.uuids ?? (r.uuid ? [r.uuid] : []))
      .find(u => metodoPagoRelacionado[u] != null);
    if (relUuid) cfdi.metodoPago = metodoPagoRelacionado[relUuid];
  }
}

// c_FormaPago SAT que representan un medio de pago REAL (efectivo, cheque,
// transferencia, tarjeta) — a diferencia de marcadores de negocio como '15'
// (Condonación, ya cubierto por `_normalizarEgresoCondonacion`) o '99' (Por
// definir, que ya cae en la familia de reglas de Crédito por diseño). Para
// estos códigos SÍ hace falta corregir tanto formaPago como metodoPago,
// porque el catálogo de reglas usa formaPago para elegir cuenta de abono
// (Caja/Bancos vs Clientes) — no solo metodoPago.
const FORMAS_PAGO_REALES = ['01', '02', '03', '04', '28', '29'];

/**
 * Normalización en memoria: Egreso (NC) que declara un medio de pago real
 * (Efectivo/Cheque/Transferencia/Tarjeta) pero cuya factura relacionada
 * sigue PPD (nunca cobrada) → se reclasifica con el formaPago Y metodoPago
 * de ESA factura, no los propios. Aplica ANTES del matching de reglas. No
 * escribe a MongoDB.
 *
 * Razón: confirmado con el usuario y con datos reales (ver
 * diag-nc-metodopago-cruzado.js) — una NC de devolución/bonificación puede
 * declarar "Efectivo/PUE" aunque la venta que ajusta nunca se cobró (era a
 * crédito). No tiene sentido un "reembolso en efectivo" de algo que nunca se
 * pagó: en realidad la NC debe reducir la Cuenta por Cobrar (Clientes), no
 * Caja/Bancos, y su IVA debe cancelar "IVA Por Trasladar" (PPD), no "IVA
 * Trasladado" (PUE). Sin este fix, la NC cae en el bloque/cuenta de Contado
 * equivocado.
 *
 * No aplica a formaPago='15' (Condonación) — ese es un marcador de negocio,
 * no un medio de pago; ya lo maneja `_normalizarEgresoCondonacion` sin tocar
 * formaPago (cambiarlo ahí rompería la clasificación de Condonación).
 *
 * @param {Array} cfdis
 * @param {Object<string,{metodoPago:string,formaPago:string}>} facturaRelacionada - uuid de factura → sus datos
 */
function _normalizarEgresoSegunFacturaRelacionada(cfdis, facturaRelacionada) {
  if (!facturaRelacionada) return;
  for (const cfdi of cfdis) {
    if (cfdi.tipoDeComprobante !== 'E') continue;
    if (!FORMAS_PAGO_REALES.includes(cfdi.formaPago)) continue;
    const relUuid = (cfdi.cfdiRelacionados ?? [])
      .filter(r => r.tipoRelacion === '01' || r.tipoRelacion === '03')
      .flatMap(r => r.uuids ?? (r.uuid ? [r.uuid] : []))
      .find(u => facturaRelacionada[u]?.metodoPago === 'PPD');
    if (relUuid) {
      cfdi.formaPago  = facturaRelacionada[relUuid].formaPago ?? cfdi.formaPago;
      cfdi.metodoPago = 'PPD';
    }
  }
}

/**
 * Genera un array de razones (strings) que explican por qué una regla
 * fue seleccionada para un CFDI específico.
 */
function _porQueAplicoRegla(cfdi, rule) {
  const TIPO_LABEL    = { I: 'Ingreso', E: 'Egreso', P: 'Pago' };
  const REL_LABEL     = { '04': 'Sustitución', '07': 'Aplicación de anticipo', '01': 'Nota de crédito', '03': 'Devolución' };
  const razones       = [];

  if (rule.tipoComprobante) {
    const v = cfdi.tipoDeComprobante;
    razones.push(`Tipo de comprobante: ${rule.tipoComprobante} (${TIPO_LABEL[rule.tipoComprobante] || rule.tipoComprobante}) — CFDI: ${v}`);
  }
  if (rule.metodoPago) {
    razones.push(`Método de pago: ${rule.metodoPago} — CFDI: ${cfdi.metodoPago || '—'}`);
  }
  if (rule.formaPago) {
    razones.push(`Forma de pago: ${rule.formaPago} — CFDI: ${cfdi.formaPago || '—'}`);
  }
  if (rule.tasaIva != null) {
    const tasaDetectada = mappingSvc._detectTasaIvaPublic(cfdi);
    const label = rule.tasaIva === 'mixto' ? 'Mixto (0%+16%)' : `${rule.tasaIva}%`;
    razones.push(`Tasa IVA: ${label} — detectada en CFDI: ${tasaDetectada ?? 'ninguna'}`);
  }
  if (rule.rfcEmisor) {
    razones.push(`RFC Emisor específico: ${rule.rfcEmisor}`);
  }
  if (rule.rfcReceptor) {
    razones.push(`RFC Receptor específico: ${rule.rfcReceptor}`);
  }
  if (rule.tipoRelacion) {
    const tipoRel = cfdi.cfdiRelacionados?.find(r => ['04', '07'].includes(r.tipoRelacion))?.tipoRelacion
      ?? cfdi.cfdiRelacionados?.[0]?.tipoRelacion ?? '—';
    const desc = REL_LABEL[rule.tipoRelacion] || rule.tipoRelacion;
    razones.push(`Tipo relación: ${rule.tipoRelacion} (${desc}) — CFDI: ${tipoRel}`);
  }
  if (rule.relacionadoTipo) {
    const label = TIPO_LABEL[rule.relacionadoTipo] || rule.relacionadoTipo;
    razones.push(`Tipo CFDI relacionado: ${rule.relacionadoTipo} (${label})`);
  }
  if (rule.tipoOrigen) {
    razones.push(`Tipo de origen: ${rule.tipoOrigen}`);
  }
  if (rule.tieneDescuento != null) {
    razones.push(`Con descuento: ${rule.tieneDescuento ? 'Sí' : 'No'}`);
  }
  if (rule.conceptoContiene) {
    razones.push(`Concepto contiene: "${rule.conceptoContiene}"`);
  }
  if (rule.claveProdServ) {
    razones.push(`Clave prod/serv: ${rule.claveProdServ}`);
  }

  if (!razones.length) {
    return ['Regla comodín — ningún filtro activo; aplica a cualquier CFDI que no coincida con reglas más específicas'];
  }
  return razones;
}

/**
 * Devuelve los CFDIs del periodo que generan movimientos en una cuenta específica.
 * Misma lógica de enriquecimiento y matching que generarBalanzaPreliminar.
 * Útil para el drill-down: click en cuenta → ver CFDIs que la componen.
 */
async function generarDetalleCuenta({ rfc, ejercicio, periodo, tipoCfdi, cuentaCodigo,
  excluirPagosSustitutos = false, excluirAplicacionesAnticipos = false,
  excluirReclasificaciones = false, incluirFechaCruzada = false, excluirMesesPosteriores = false }) {

  if (!rfc)          throw new BadRequestError('RFC requerido');
  if (!ejercicio)    throw new BadRequestError('Ejercicio requerido');
  if (!periodo)      throw new BadRequestError('Periodo requerido');
  if (!cuentaCodigo) throw new BadRequestError('Código de cuenta requerido');

  const tipos = tipoCfdi ? [tipoCfdi] : ['I', 'E', 'P'];

  const filtroPagosSustitutos = {};  // manejado en memoria — ver _enrichAndFilterCfdis

  const filtroPeriodo = incluirFechaCruzada
    ? { $or: [{ periodo: Number(periodo) }, { $expr: { $eq: [{ $month: '$fecha' }, Number(periodo)] } }] }
    : { periodo: Number(periodo) };

  const filtroReclasificaciones = excluirReclasificaciones
    ? { $expr: { $eq: [{ $month: '$fecha' }, Number(periodo)] } }
    : {};

  const filtroMesesPosteriores = excluirMesesPosteriores
    ? { $expr: { $lte: [{ $month: '$fecha' }, Number(periodo)] } }
    : {};

  const filtroAnticipos = excluirAplicacionesAnticipos
    ? { $nor: [
        { tipoDeComprobante: 'I', 'cfdiRelacionados.tipoRelacion': '07' },
        { tipoDeComprobante: 'E', 'cfdiRelacionados.tipoRelacion': '07' },
      ]}
    : {};

  const rules = await _getRulesActive();

  const cuentaObj = await AccountPlan.findOne({ where: { codigo: cuentaCodigo }, raw: true });

  const codigosTodos = [...new Set(
    rules.flatMap(r => [
      r.cuentaCargo, r.cuentaAbono, r.cuentaAbono2, r.cuentaIva, r.cuentaIvaPPD,
      r.cuentaIvaRetenido, r.cuentaIsrRetenido, r.cuentaIvaAnticipo, r.cuentaDeltaAnticipo,
      r.cuentaCargo2, r.cuentaDescuento, r.cuentaDescuento0, r.cuentaCargoMixto0, r.cuentaIvaAbono,
    ].filter(Boolean)),
  )];

  const cuentasRows = codigosTodos.length
    ? await AccountPlan.findAll({ where: { codigo: { [Op.in]: codigosTodos } }, attributes: ['id', 'codigo'], raw: true })
    : [];
  const cuentaMapByCod = Object.fromEntries(cuentasRows.map(c => [c.codigo, c.id]));
  const targetId = cuentaMapByCod[cuentaCodigo] ?? null;

  if (!targetId) {
    return {
      cuenta: { codigo: cuentaCodigo, nombre: cuentaObj?.nombre ?? cuentaCodigo, tipo: cuentaObj?.tipo ?? '?' },
      cfdis: [], totales: { debe: 0, haber: 0 },
    };
  }

  // Pre-query: UUIDs de facturas PUE (tipo I, formaPago=30) del periodo para el drill-down.
  const uuidsFacturasPueAnticipo = new Set();
  {
    const _fp = await CFDI.find({
      $or: [{ 'emisor.rfc': rfc }, { 'receptor.rfc': rfc }],
      ejercicio: Number(ejercicio), ...filtroPeriodo,
      tipoDeComprobante: 'I', formaPago: '30',
      source: 'SAT', satStatus: 'Vigente', isActive: true,
      ...filtroMesesPosteriores,
    }).select('uuid tipoDeComprobante emisor receptor metodoPago formaPago conceptos cfdiRelacionados tipoOrigen').lean();
    for (const c of _fp) {
      if (!c.uuid) continue;
      const _r = mappingSvc.findRuleInList(c, rules);
      if (_r?.cuentaCargo === '2103010001') uuidsFacturasPueAnticipo.add(c.uuid.toUpperCase());
    }
  }

  const resultado = [];

  for (const tipo of tipos) {
    const cfdis = await CFDI.find({
      $or: [{ 'emisor.rfc': rfc }, { 'receptor.rfc': rfc }],
      ejercicio: Number(ejercicio), ...filtroPeriodo,
      tipoDeComprobante: tipo, source: 'SAT', satStatus: 'Vigente', isActive: true,
      ...filtroPagosSustitutos, ...filtroAnticipos, ...filtroReclasificaciones, ...filtroMesesPosteriores,
    })
      .select('uuid tipoDeComprobante metodoPago formaPago fecha folio serie emisor.rfc emisor.nombre receptor.rfc receptor.nombre subTotal total descuento impuestos conceptos.importe conceptos.Importe conceptos.descuento conceptos.Descuento conceptos.impuestos conceptos.descripcion conceptos.Descripcion complementoPago.totales complementoPago.pagos.monto complementoPago.pagos.formaDePagoP complementoPago.pagos.doctosRelacionados.trasladosDR cfdiRelacionados tasaIvaInferida')
      .maxTimeMS(60_000).lean();

    const cfdisFinales = await _enrichAndFilterCfdis(cfdis, tipo, {
      excluirPagosSustitutos,
      uuidsFacturasPueAnticipo,
    });

    for (const cfdi of cfdisFinales) {
      const rule = mappingSvc.findRuleInList(cfdi, rules);
      if (!rule) continue;
      const movs = await mappingSvc.cfdiToMovimientos(cfdi, rule, cuentaMapByCod);
      const movsEnCuenta = movs.filter(m => m.cuentaId === targetId);
      if (!movsEnCuenta.length) continue;
      const _tipoRelCfdi = cfdi.cfdiRelacionados?.find(r => ['04', '07'].includes(r.tipoRelacion))?.tipoRelacion
        ?? cfdi.cfdiRelacionados?.[0]?.tipoRelacion ?? null;
      const _montos = cfdi.tipoDeComprobante !== 'P' ? mappingSvc._calcCfdiMontosPublic(cfdi) : null;

      resultado.push({
        uuid:              cfdi.uuid,
        tipoDeComprobante: cfdi.tipoDeComprobante,
        fecha:             cfdi.fecha,
        folio:             cfdi.folio ?? null,
        serie:             cfdi.serie ?? null,
        rfcEmisor:         cfdi.emisor?.rfc    ?? null,
        rfcReceptor:       cfdi.receptor?.rfc  ?? null,
        emisorNombre:      cfdi.emisor?.nombre  ?? null,
        receptorNombre:    cfdi.receptor?.nombre ?? null,
        subTotal:          Number(cfdi.subTotal || 0),
        descuento:         Number(cfdi.descuento || 0),
        total:             Number(cfdi.total    || 0),
        baseIva16: cfdi.tipoDeComprobante === 'P'
          ? Math.round(Number(cfdi.complementoPago?.totales?.totalTrasladosBaseIVA16 || 0) * 100) / 100
          : Math.round((_montos?.subTotal16 ?? 0) * 100) / 100,
        baseIva0: cfdi.tipoDeComprobante === 'P'
          ? 0
          : Math.round((_montos?.subTotal0 ?? 0) * 100) / 100,
        debe:  Math.round(movsEnCuenta.reduce((s, m) => s + (Number(m.debe)  || 0), 0) * 100) / 100,
        haber: Math.round(movsEnCuenta.reduce((s, m) => s + (Number(m.haber) || 0), 0) * 100) / 100,
        reglaNombre:       rule.nombre,
        formaPago:         cfdi.formaPago  ?? null,
        metodoPago:        cfdi.metodoPago ?? null,
        concepto:          movsEnCuenta[0]?.concepto ?? null,
        tasaIvaDetectada:  mappingSvc._detectTasaIvaPublic(cfdi),
        tipoRelacion:      _tipoRelCfdi,
        conceptos: (cfdi.conceptos ?? []).map(c => ({
          descripcion: c.descripcion || c.Descripcion || '',
          importe:     Number(c.importe || c.Importe || 0),
        })),
        cfdiRelacionados: (cfdi.cfdiRelacionados ?? []).map(r => ({
          tipoRelacion: r.tipoRelacion,
          uuids:        r.uuids ?? [],
        })),
        regla: {
          nombre:            rule.nombre,
          prioridad:         rule.prioridad,
          isActive:          rule.isActive,
          tipoComprobante:   rule.tipoComprobante   ?? null,
          metodoPago:        rule.metodoPago         ?? null,
          formaPago:         rule.formaPago          ?? null,
          tasaIva:           rule.tasaIva            ?? null,
          rfcEmisor:         rule.rfcEmisor          ?? null,
          rfcReceptor:       rule.rfcReceptor        ?? null,
          tipoRelacion:      rule.tipoRelacion       ?? null,
          relacionadoTipo:   rule.relacionadoTipo    ?? null,
          tipoOrigen:        rule.tipoOrigen         ?? null,
          tieneDescuento:    rule.tieneDescuento     ?? null,
          conceptoContiene:  rule.conceptoContiene   ?? null,
          claveProdServ:     rule.claveProdServ      ?? null,
          cuentaCargo:       rule.cuentaCargo,
          cuentaAbono:       rule.cuentaAbono,
          cuentaAbono2:      rule.cuentaAbono2       ?? null,
          cuentaIva:         rule.cuentaIva          ?? null,
          cuentaIvaPPD:      rule.cuentaIvaPPD       ?? null,
          cuentaIvaRetenido: rule.cuentaIvaRetenido  ?? null,
          cuentaIsrRetenido: rule.cuentaIsrRetenido  ?? null,
          cuentaIvaAnticipo: rule.cuentaIvaAnticipo  ?? null,
          cuentaDeltaAnticipo: rule.cuentaDeltaAnticipo ?? null,
          cuentaCargo2:      rule.cuentaCargo2       ?? null,
          cuentaDescuento:   rule.cuentaDescuento    ?? null,
          centroCosto:       rule.centroCosto        ?? null,
          ivaHaber:          rule.ivaHaber           ?? null,
          esAplicacionSaldo: rule.esAplicacionSaldo  ?? null,
        },
        porQue: _porQueAplicoRegla(cfdi, rule),
        // Opción C: flags de sustitución para que el frontend pinte el renglón diferente
        fueReemplazado: cfdi._meta?.fueReemplazado ?? false,
        reemplazadoPor: cfdi._meta?.reemplazadoPor ?? null,
        esSustituto:    cfdi._meta?.esSustituto    ?? false,
        sustituyeA:     cfdi._meta?.sustituyeA     ?? null,
      });
    }
  }

  // Cargar los CFDIs originales (CFDI-A, normalmente Cancelados en SAT) siempre que
  // haya sustitutos en el resultado — así el contador ve la trazabilidad completa
  // independientemente del toggle excluirPagosSustitutos.
  // Sin esta consulta extra, CFDI-A nunca aparece porque la query principal
  // filtra satStatus='Vigente' y el original ya fue cancelado por el SAT.
  {
    const uuidsOriginales = [
      ...new Set(
        resultado
          .filter(c => c.esSustituto && c.sustituyeA?.length)
          .flatMap(c => c.sustituyeA),
      ),
    ];
    if (uuidsOriginales.length) {
      const cfdisA = await CFDI.find({ uuid: { $in: uuidsOriginales } })
        .select('uuid tipoDeComprobante metodoPago formaPago fecha folio serie emisor.rfc emisor.nombre receptor.rfc receptor.nombre subTotal total descuento impuestos conceptos.importe conceptos.Importe conceptos.descuento conceptos.Descuento conceptos.impuestos conceptos.descripcion conceptos.Descripcion complementoPago.totales complementoPago.pagos.monto complementoPago.pagos.formaDePagoP complementoPago.pagos.doctosRelacionados.trasladosDR cfdiRelacionados tasaIvaInferida')
        .lean();

      // uuid-A (upper) → uuid-B: para poblar el campo reemplazadoPor
      const reemplazadoPorMap = new Map(
        resultado
          .filter(c => c.esSustituto && c.sustituyeA?.length)
          .flatMap(c => c.sustituyeA.map(uA => [uA, c.uuid])),
      );

      for (const cfdiA of cfdisA) {
        const rule = mappingSvc.findRuleInList(cfdiA, rules);
        if (!rule) continue;
        const movs = await mappingSvc.cfdiToMovimientos(cfdiA, rule, cuentaMapByCod);
        const movsEnCuenta = movs.filter(m => m.cuentaId === targetId);
        if (!movsEnCuenta.length) continue; // este CFDI-A no toca la cuenta del drill-down

        const _montosA = cfdiA.tipoDeComprobante !== 'P' ? mappingSvc._calcCfdiMontosPublic(cfdiA) : null;
        const _tipoRelA = cfdiA.cfdiRelacionados?.find(r => ['04', '07'].includes(r.tipoRelacion))?.tipoRelacion
          ?? cfdiA.cfdiRelacionados?.[0]?.tipoRelacion ?? null;

        resultado.push({
          uuid:              cfdiA.uuid,
          tipoDeComprobante: cfdiA.tipoDeComprobante,
          fecha:             cfdiA.fecha,
          folio:             cfdiA.folio ?? null,
          serie:             cfdiA.serie ?? null,
          rfcEmisor:         cfdiA.emisor?.rfc     ?? null,
          rfcReceptor:       cfdiA.receptor?.rfc   ?? null,
          emisorNombre:      cfdiA.emisor?.nombre  ?? null,
          receptorNombre:    cfdiA.receptor?.nombre ?? null,
          subTotal:          Number(cfdiA.subTotal || 0),
          descuento:         Number(cfdiA.descuento || 0),
          total:             Number(cfdiA.total    || 0),
          baseIva16: cfdiA.tipoDeComprobante === 'P'
            ? Math.round(Number(cfdiA.complementoPago?.totales?.totalTrasladosBaseIVA16 || 0) * 100) / 100
            : Math.round((_montosA?.subTotal16 ?? 0) * 100) / 100,
          baseIva0: cfdiA.tipoDeComprobante === 'P'
            ? 0
            : Math.round((_montosA?.subTotal0 ?? 0) * 100) / 100,
          debe:  Math.round(movsEnCuenta.reduce((s, m) => s + (Number(m.debe)  || 0), 0) * 100) / 100,
          haber: Math.round(movsEnCuenta.reduce((s, m) => s + (Number(m.haber) || 0), 0) * 100) / 100,
          reglaNombre:      rule.nombre,
          formaPago:        cfdiA.formaPago  ?? null,
          metodoPago:       cfdiA.metodoPago ?? null,
          concepto:         movsEnCuenta[0]?.concepto ?? null,
          tasaIvaDetectada: mappingSvc._detectTasaIvaPublic(cfdiA),
          tipoRelacion:     _tipoRelA,
          conceptos: (cfdiA.conceptos ?? []).map(c => ({
            descripcion: c.descripcion || c.Descripcion || '',
            importe:     Number(c.importe || c.Importe || 0),
          })),
          cfdiRelacionados: (cfdiA.cfdiRelacionados ?? []).map(r => ({
            tipoRelacion: r.tipoRelacion,
            uuids:        r.uuids ?? [],
          })),
          regla: {
            nombre:             rule.nombre,
            prioridad:          rule.prioridad,
            isActive:           rule.isActive,
            tipoComprobante:    rule.tipoComprobante    ?? null,
            metodoPago:         rule.metodoPago          ?? null,
            formaPago:          rule.formaPago           ?? null,
            tasaIva:            rule.tasaIva             ?? null,
            rfcEmisor:          rule.rfcEmisor           ?? null,
            rfcReceptor:        rule.rfcReceptor         ?? null,
            tipoRelacion:       rule.tipoRelacion        ?? null,
            relacionadoTipo:    rule.relacionadoTipo     ?? null,
            tipoOrigen:         rule.tipoOrigen          ?? null,
            tieneDescuento:     rule.tieneDescuento      ?? null,
            conceptoContiene:   rule.conceptoContiene    ?? null,
            claveProdServ:      rule.claveProdServ       ?? null,
            cuentaCargo:        rule.cuentaCargo,
            cuentaAbono:        rule.cuentaAbono,
            cuentaAbono2:       rule.cuentaAbono2        ?? null,
            cuentaIva:          rule.cuentaIva           ?? null,
            cuentaIvaPPD:       rule.cuentaIvaPPD        ?? null,
            cuentaIvaRetenido:  rule.cuentaIvaRetenido   ?? null,
            cuentaIsrRetenido:  rule.cuentaIsrRetenido   ?? null,
            cuentaIvaAnticipo:  rule.cuentaIvaAnticipo   ?? null,
            cuentaDeltaAnticipo: rule.cuentaDeltaAnticipo ?? null,
            cuentaCargo2:       rule.cuentaCargo2        ?? null,
            cuentaDescuento:    rule.cuentaDescuento     ?? null,
            centroCosto:        rule.centroCosto         ?? null,
            ivaHaber:           rule.ivaHaber            ?? null,
            esAplicacionSaldo:  rule.esAplicacionSaldo   ?? null,
          },
          porQue:         _porQueAplicoRegla(cfdiA, rule),
          fueReemplazado: true,
          reemplazadoPor: reemplazadoPorMap.get((cfdiA.uuid ?? '').toUpperCase()) ?? null,
          esSustituto:    false,
          sustituyeA:     null,
        });
      }
    }
  }

  resultado.sort((a, b) => new Date(b.fecha) - new Date(a.fecha));

  // Totales excluyen CFDIs marcados como fueReemplazado — solo el sustituto aporta al saldo
  // Totales: excluir CFDI-A del saldo solo cuando el toggle está activo.
  // Cuando excluirPagosSustitutos=false el original sí suma (vista informativa).
  const resultadoParaTotales = excluirPagosSustitutos
    ? resultado.filter(c => !c.fueReemplazado)
    : resultado;
  return {
    cuenta: { codigo: cuentaCodigo, nombre: cuentaObj?.nombre ?? cuentaCodigo, tipo: cuentaObj?.tipo ?? '?' },
    cfdis:   resultado,
    totales: {
      debe:  Math.round(resultadoParaTotales.reduce((s, c) => s + c.debe,  0) * 100) / 100,
      haber: Math.round(resultadoParaTotales.reduce((s, c) => s + c.haber, 0) * 100) / 100,
    },
  };
}

/**
 * Devuelve todos los movimientos CFDI→cuenta del periodo en una lista plana,
 * con cuenta, regla y datos del comprobante. Usado para el export Excel completo.
 */
async function generarDetalleExport({ rfc, ejercicio, periodo, tipoCfdi,
  excluirPagosSustitutos = false, excluirAplicacionesAnticipos = false,
  excluirReclasificaciones = false, incluirFechaCruzada = false, excluirMesesPosteriores = false }) {

  if (!rfc)       throw new BadRequestError('RFC requerido');
  if (!ejercicio) throw new BadRequestError('Ejercicio requerido');
  if (!periodo)   throw new BadRequestError('Periodo requerido');

  const tipos = tipoCfdi ? [tipoCfdi] : ['I', 'E', 'P'];

  const filtroPagosSustitutos = {};  // manejado en memoria — ver _enrichAndFilterCfdis
  const filtroPeriodo = incluirFechaCruzada
    ? { $or: [{ periodo: Number(periodo) }, { $expr: { $eq: [{ $month: '$fecha' }, Number(periodo)] } }] }
    : { periodo: Number(periodo) };
  const filtroReclasificaciones = excluirReclasificaciones ? { $expr: { $eq: [{ $month: '$fecha' }, Number(periodo)] } } : {};
  const filtroMesesPosteriores  = excluirMesesPosteriores  ? { $expr: { $lte: [{ $month: '$fecha' }, Number(periodo)] } } : {};
  const filtroAnticipos = excluirAplicacionesAnticipos
    ? { $nor: [{ tipoDeComprobante: 'I', 'cfdiRelacionados.tipoRelacion': '07' }, { tipoDeComprobante: 'E', 'cfdiRelacionados.tipoRelacion': '07' }] }
    : {};

  const rules = await _getRulesActive();

  const codigosTodos = [...new Set(
    rules.flatMap(r => [
      r.cuentaCargo, r.cuentaAbono, r.cuentaAbono2, r.cuentaIva, r.cuentaIvaPPD,
      r.cuentaIvaRetenido, r.cuentaIsrRetenido, r.cuentaIvaAnticipo, r.cuentaDeltaAnticipo,
      r.cuentaCargo2, r.cuentaDescuento, r.cuentaDescuento0, r.cuentaCargoMixto0, r.cuentaIvaAbono,
    ].filter(Boolean)),
  )];

  const cuentasRows = codigosTodos.length
    ? await AccountPlan.findAll({ where: { codigo: { [Op.in]: codigosTodos } }, attributes: ['id', 'codigo', 'nombre', 'tipo'], raw: true })
    : [];
  const cuentaMapByCod = Object.fromEntries(cuentasRows.map(c => [c.codigo, c.id]));
  const cuentaInfoById = Object.fromEntries(cuentasRows.map(c => [c.id, { codigo: c.codigo, nombre: c.nombre, tipo: c.tipo }]));

  // Pre-query facturas PUE formaPago=30 para fix doble-contabilización anticipo.
  const uuidsFacturasPueAnticipo = new Set();
  {
    const _fp = await CFDI.find({
      $or: [{ 'emisor.rfc': rfc }, { 'receptor.rfc': rfc }],
      ejercicio: Number(ejercicio), ...filtroPeriodo,
      tipoDeComprobante: 'I', formaPago: '30',
      source: 'SAT', satStatus: 'Vigente', isActive: true, ...filtroMesesPosteriores,
    }).select('uuid tipoDeComprobante emisor receptor metodoPago formaPago conceptos cfdiRelacionados tipoOrigen').lean();
    for (const c of _fp) {
      if (!c.uuid) continue;
      const _r = mappingSvc.findRuleInList(c, rules);
      if (_r?.cuentaCargo === '2103010001') uuidsFacturasPueAnticipo.add(c.uuid.toUpperCase());
    }
  }

  const entradas = [];
  let sinRegla = 0;

  for (const tipo of tipos) {
    const cfdis = await CFDI.find({
      $or: [{ 'emisor.rfc': rfc }, { 'receptor.rfc': rfc }],
      ejercicio: Number(ejercicio), ...filtroPeriodo,
      tipoDeComprobante: tipo, source: 'SAT', satStatus: 'Vigente', isActive: true,
      ...filtroPagosSustitutos, ...filtroAnticipos, ...filtroReclasificaciones, ...filtroMesesPosteriores,
    })
      .select('uuid tipoDeComprobante metodoPago formaPago fecha folio serie emisor.rfc emisor.nombre receptor.rfc receptor.nombre subTotal total descuento impuestos conceptos.importe conceptos.Importe conceptos.descuento conceptos.Descuento conceptos.impuestos conceptos.descripcion conceptos.Descripcion complementoPago.totales complementoPago.pagos.monto complementoPago.pagos.formaDePagoP complementoPago.pagos.doctosRelacionados.trasladosDR cfdiRelacionados tasaIvaInferida')
      .maxTimeMS(60_000).lean();

    const cfdisFinales = await _enrichAndFilterCfdis(cfdis, tipo, {
      excluirPagosSustitutos,
      uuidsFacturasPueAnticipo,
    });

    for (const cfdi of cfdisFinales) {
      const rule = mappingSvc.findRuleInList(cfdi, rules);
      if (!rule) { sinRegla++; continue; }
      const movs    = await mappingSvc.cfdiToMovimientos(cfdi, rule, cuentaMapByCod);
      const tasaIva = mappingSvc._detectTasaIvaPublic(cfdi);

      for (const mov of movs) {
        if (!mov.cuentaId) continue;
        const info = cuentaInfoById[mov.cuentaId];
        if (!info) continue;
        entradas.push({
          cuentaCodigo:      info.codigo,
          cuentaNombre:      info.nombre,
          cuentaTipo:        info.tipo,
          uuid:              cfdi.uuid,
          tipoDeComprobante: cfdi.tipoDeComprobante,
          fecha:             cfdi.fecha,
          folio:             cfdi.folio ?? null,
          serie:             cfdi.serie ?? null,
          rfcEmisor:         cfdi.emisor?.rfc     ?? null,
          emisorNombre:      cfdi.emisor?.nombre   ?? null,
          rfcReceptor:       cfdi.receptor?.rfc   ?? null,
          receptorNombre:    cfdi.receptor?.nombre ?? null,
          subTotal:          Number(cfdi.subTotal || 0),
          descuento:         Number(cfdi.descuento || 0),
          total:             Number(cfdi.total    || 0),
          formaPago:         cfdi.formaPago  ?? null,
          metodoPago:        cfdi.metodoPago ?? null,
          tasaIvaDetectada:  tasaIva,
          debe:              Math.round(Number(mov.debe  || 0) * 100) / 100,
          haber:             Math.round(Number(mov.haber || 0) * 100) / 100,
          concepto:          mov.concepto ?? null,
          reglaNombre:       rule.nombre,
          porQue:            _porQueAplicoRegla(cfdi, rule),
          fueReemplazado:    cfdi._meta?.fueReemplazado ?? false,
          reemplazadoPor:    cfdi._meta?.reemplazadoPor ?? null,
          esSustituto:       cfdi._meta?.esSustituto    ?? false,
          sustituyeA:        cfdi._meta?.sustituyeA     ?? null,
        });
      }
    }
  }

  entradas.sort((a, b) => {
    const cc = (a.cuentaCodigo ?? '').localeCompare(b.cuentaCodigo ?? '');
    if (cc !== 0) return cc;
    return new Date(a.fecha) - new Date(b.fecha);
  });
  return { entradas, sinRegla };
}

module.exports = { generarBalanzaPreliminar, generarDetalleCuenta, generarDetalleExport, _getRulesActive, _enrichTasaIvaFromRelatedCfdis, _normalizarEgresoPue99, _normalizarEgresoCondonacion, _normalizarEgresoSegunFacturaRelacionada };
