'use strict';

const { CfdiMappingRule, AccountPlan, PolizaMovimiento } = require('../../../shared/models/postgres');
const { Op }            = require('sequelize');
const { NotFoundError, BadRequestError } = require('../../shared/errors/AppError');

// ── CRUD de reglas ────────────────────────────────────────────────────────────

async function list() {
  return CfdiMappingRule.findAll({
    order: [['prioridad', 'ASC'], ['nombre', 'ASC']],
  });
}

async function getById(id) {
  const rule = await CfdiMappingRule.findByPk(id);
  if (!rule) throw new NotFoundError('Regla de mapeo');
  return rule;
}

async function create(data) {
  _validate(data);
  return CfdiMappingRule.create(data);
}

async function update(id, data) {
  const rule = await CfdiMappingRule.findByPk(id);
  if (!rule) throw new NotFoundError('Regla de mapeo');
  _validate({ ...rule.toJSON(), ...data });

  const usada = await PolizaMovimiento.count({ where: { reglaId: id } });
  const snapshotAntes = usada > 0 ? rule.toJSON() : null;

  await rule.update(data);

  const result = rule.toJSON();
  if (snapshotAntes) {
    result._advertencia = `Esta regla ya fue usada en ${usada} movimiento(s). Las pólizas anteriores no se modifican — cada movimiento guarda el nombre de la regla que se usó al generarlo.`;
    result._reglaAlMomentoDeUso = snapshotAntes;
  }
  return result;
}

async function remove(id) {
  const rule = await CfdiMappingRule.findByPk(id);
  if (!rule) throw new NotFoundError('Regla de mapeo');

  const usada = await PolizaMovimiento.count({ where: { reglaId: id } });
  if (usada > 0) {
    throw new BadRequestError(
      `No se puede eliminar la regla "${rule.nombre}" porque ya fue usada en ${usada} movimiento(s). ` +
      `Puedes deshabilitarla (isActive=false) o actualizarla sin afectar las pólizas anteriores.`,
    );
  }

  await rule.destroy();
}

// ── Matching ──────────────────────────────────────────────────────────────────

/**
 * Busca la regla más específica que aplica a un CFDI.
 * Orden de prioridad: menor número primero, y dentro del mismo número
 * gana la que tiene más restricciones (rfcEmisor específico antes que vacío).
 */
async function findRuleForCfdi(cfdi) {
  const rules = await CfdiMappingRule.findAll({
    where: { isActive: true },
    order: [['prioridad', 'ASC']],
  });
  return findRuleInList(cfdi, rules);
}

/**
 * Versión síncrona de findRuleForCfdi que opera sobre una lista ya cargada.
 * Usar cuando se procesan múltiples CFDIs para evitar una query por iteración.
 *
 * Una regla aplica si todos sus filtros no-nulos coinciden con el CFDI.
 * Entre las reglas que aplican, gana la de menor prioridad numérica;
 * en empate, la más específica (más filtros con valor).
 */
function findRuleInList(cfdi, rules) {
  const cfdiClaveProdServ  = cfdi.conceptos?.[0]?.claveProdServ ?? null;
  // Prioridad de tipoRelacion: '04' y '07' son más específicos que '01'.
  // Si el CFDI tiene múltiples relaciones (ej. SAT concatenó '01' y ERP tiene '04'),
  // se prefiere '04' (sustitución) > '07' (anticipo) > cualquier otro > primer elemento.
  const _RELACION_PRIORIDAD = ['04', '07'];
  const cfdiTipoRelacion = cfdi.cfdiRelacionados?.find(r => _RELACION_PRIORIDAD.includes(r.tipoRelacion))?.tipoRelacion
    ?? cfdi.cfdiRelacionados?.[0]?.tipoRelacion
    ?? null;
  const cfdiTasaIva        = _detectTasaIva(cfdi);
  const cfdiTieneDescuento = _detectTieneDescuento(cfdi);

  // _relacionadoTipo es un campo virtual inyectado por el generator antes del matching.
  // Contiene el tipoDeComprobante del primer CFDI relacionado (pre-fetched de MongoDB).
  const cfdiRelacionadoTipo = cfdi._relacionadoTipo ?? null;

  const cfdiDescripcion = (cfdi.conceptos?.[0]?.descripcion ?? cfdi.conceptos?.[0]?.Descripcion ?? '').toLowerCase();
  const cfdiTipoOrigen  = cfdi.tipoOrigen ?? _derivarTipoOrigen(cfdi) ?? null;

  const matching = rules.filter(r =>
    (!r.tipoComprobante    || r.tipoComprobante  === cfdi.tipoDeComprobante) &&
    (!r.rfcEmisor          || r.rfcEmisor        === cfdi.emisor?.rfc) &&
    (!r.rfcReceptor        || r.rfcReceptor      === cfdi.receptor?.rfc) &&
    (!r.metodoPago         || r.metodoPago       === cfdi.metodoPago) &&
    (!r.formaPago          || r.formaPago         === cfdi.formaPago) &&
    (!r.claveProdServ      || r.claveProdServ     === cfdiClaveProdServ) &&
    (!r.tipoRelacion       || r.tipoRelacion      === cfdiTipoRelacion) &&
    (!r.relacionadoTipo    || r.relacionadoTipo   === cfdiRelacionadoTipo) &&
    (!r.conceptoContiene   || cfdiDescripcion.includes(r.conceptoContiene.toLowerCase())) &&
    (!r.tipoOrigen         || r.tipoOrigen        === cfdiTipoOrigen) &&
    (r.tasaIva        == null || r.tasaIva        === cfdiTasaIva) &&
    (r.tieneDescuento == null || r.tieneDescuento === cfdiTieneDescuento),
  );
  if (!matching.length) return null;

  const spec = r => [
    r.tipoComprobante, r.rfcEmisor, r.rfcReceptor, r.metodoPago, r.formaPago,
    r.claveProdServ, r.tipoRelacion, r.relacionadoTipo, r.tasaIva,
    r.tieneDescuento  != null ? String(r.tieneDescuento)  : null,
    r.conceptoContiene != null ? r.conceptoContiene       : null,
    r.tipoOrigen      != null ? r.tipoOrigen              : null,
  ].filter(v => v != null).length;

  return matching.sort((a, b) => {
    if (a.prioridad !== b.prioridad) return a.prioridad - b.prioridad;
    return spec(b) - spec(a);
  })[0];
}

// ── Helpers de detección de propiedades del CFDI ──────────────────────────────

/** Detecta la tasa IVA dominante en los conceptos del CFDI.
 *  Tipo P (complemento de pago) siempre devuelve null — no tiene conceptos con tasa.
 *  Fallback: si los conceptos no tienen desglose de tasa, lee del header (cfdi.impuestos). */
function _detectTasaIva(cfdi) {
  if (cfdi.tipoDeComprobante === 'P') {
    // El complemento de pago no tiene conceptos propios; detectar tasa desde los
    // totales SAT 4.0 del complemento. Si hay IVA16 o IVA8 (fronterizo) → '16'.
    // Si hay monto pagado pero cero IVA → '0' (exento o tasa cero).
    // Si no hay totales (CFDI incompleto) → null (fallback a reglas sin tasaIva).
    const totales = cfdi.complementoPago?.totales;
    if (totales) {
      const iva16 = Number(totales.totalTrasladosImpuestoIVA16 || 0);
      const iva8  = Number(totales.totalTrasladosImpuestoIVA8  || 0);
      if (iva16 > 0 || iva8 > 0) return '16';
      const montoTotal = Number(totales.montoTotalPagos || 0);
      if (montoTotal > 0) return '0';
    } else {
      // CP 1.0 (CFDI 3.3): no hay <Totales>, pero cada DoctoRelacionado puede
      // tener <ImpuestosDR><TrasladosDR> con la tasa por documento.
      let drTiene16 = false;
      let drTiene0  = false;
      for (const pago of (cfdi.complementoPago?.pagos ?? [])) {
        for (const dr of (pago.doctosRelacionados ?? [])) {
          for (const t of (dr.trasladosDR ?? [])) {
            if ((t.impuesto || '') !== '002') continue;
            if ((t.tasaOCuota || 0) > 0) drTiene16 = true;
            else drTiene0 = true;
          }
        }
      }
      if (drTiene16 && drTiene0) return 'mixto';
      if (drTiene16) return '16';
      if (drTiene0)  return '0';
      // Último fallback: tasa pre-computada por migración (CP 1.0 sin xmlContent)
      if (cfdi.tasaIvaInferida != null) return cfdi.tasaIvaInferida;
    }
    return null;
  }
  let tiene16 = false;
  let tiene0  = false;
  for (const c of (cfdi.conceptos || [])) {
    for (const t of (c.impuestos?.traslados || [])) {
      // Solo IVA (código SAT '002') — excluye IEPS y otros impuestos
      const impuesto = t.impuesto || t.Impuesto || '';
      if (impuesto !== '002') continue;
      const tasa = Number(t.tasaOCuota ?? t.TasaOCuota ?? 0);
      if (tasa > 0)  tiene16 = true;
      else           tiene0  = true;
    }
  }
  // Fallback 1: IVA a nivel header (cfdi.impuestos.traslados)
  if (!tiene16 && !tiene0) {
    for (const t of (cfdi.impuestos?.traslados || [])) {
      const impuesto = t.impuesto || t.Impuesto || '';
      if (impuesto !== '002') continue;
      const tasa = Number(t.tasaOCuota ?? t.TasaOCuota ?? 0);
      if (tasa > 0)  tiene16 = true;
      else           tiene0  = true;
    }
  }
  // Fallback 2: traslados vacío pero totalImpuestosTrasladados disponible
  // (CFDIs Metadata — sin XML completo pero con totales SAT en el header)
  if (!tiene16 && !tiene0) {
    const rawTotal = cfdi.impuestos?.totalImpuestosTrasladados;
    // Solo actuar si el campo está EXPLÍCITAMENTE presente en el documento.
    // Si es undefined/null no sabemos la tasa → devolvemos null (no inferir 0%).
    if (rawTotal != null) {
      const totalImptos = Number(rawTotal);
      if (totalImptos <= 0) {
        tiene0 = true;
      } else {
        // Determinar si es puro 16% o mixto (parte 16% + parte exenta/0%).
        // Si el IVA real difiere del esperado a 16% en más de $0.50, FORZOSAMENTE
        // hay productos con tasa diferente (0% o exento) → MIXTO.
        // Tolerancia de $0.50 absorbe redondeos por concepto sin falsos positivos.
        const base = Number(cfdi.subTotal || 0) - Number(cfdi.descuento || 0);
        if (base > 0 && Math.abs(totalImptos - base * 0.16) > 0.50) {
          // Antes de marcar mixto: descartar artefacto SAT metadata donde
          // totalImpuestosRetenidos ≈ ivaTras y retenciones:[] — en ese caso la
          // porción aparente 0% es un descuento implícito, no producto a tasa 0%.
          const ivaRetChk = Number(cfdi.impuestos?.totalImpuestosRetenidos || 0);
          const esArtRet  = (cfdi.impuestos?.retenciones || []).length === 0 &&
                            ivaRetChk > 0 && Math.abs(ivaRetChk - totalImptos) < 1.0;
          tiene16 = true;
          if (!esArtRet) tiene0 = true; // mixto real; artefacto → solo 16%
        } else {
          tiene16 = true;   // puro 16% (diferencia ≤ $0.50 → solo redondeo)
        }
      }
    }
  }
  if (tiene16 && tiene0) return 'mixto';
  if (tiene16)           return '16';
  if (tiene0)            return '0';
  return null; // sin información de tasa
}

/**
 * Deriva el TipoOrigen de negocio a partir de los campos del CFDI cuando el ERP
 * no lo proporcionó. Se usa como fallback para CFDIs SAT sin homólogo ERP.
 */
function _derivarTipoOrigen(cfdi) {
  const tipo = cfdi.tipoDeComprobante;
  if (tipo === 'I') return 'Venta';
  if (tipo === 'E') {
    const rel = cfdi.cfdiRelacionados?.[0]?.tipoRelacion;
    if (rel === '01') return 'Nota de Crédito';
    if (rel === '03') return 'Devolución';
    return 'Bonificación';
  }
  if (tipo === 'P') return 'Pago';
  if (tipo === 'N') return 'Nómina';
  if (tipo === 'T') return 'Traslado';
  return null;
}

/** Detecta si el CFDI tiene descuento > 0 (header o en algún concepto). */
function _detectTieneDescuento(cfdi) {
  if (Number(cfdi.descuento || 0) > 0) return true;
  if ((cfdi.conceptos || []).some(c => Number(c.descuento || c.Descuento || 0) > 0)) return true;
  // Metadata SAT: ivaRet ≈ ivaTras y retenciones:[] → hay descuento implícito en conceptos.
  const iva0    = Number(cfdi.impuestos?.totalImpuestosTrasladados || 0);
  const ivaRet0 = Number(cfdi.impuestos?.totalImpuestosRetenidos  || 0);
  return (cfdi.conceptos || []).length === 0 &&
    (cfdi.impuestos?.retenciones || []).length === 0 &&
    ivaRet0 > 0 && Math.abs(ivaRet0 - iva0) < 1.0 &&
    (Number(cfdi.subTotal || 0) + iva0 - Number(cfdi.total || 0)) > 0.5;
}

/** Desglosa importes por tasa (para reglas mixtas y con descuento). */
function _calcCfdiMontos(cfdi) {
  let subTotal16 = 0, subTotal0 = 0, desc16 = 0, desc0 = 0;
  for (const c of (cfdi.conceptos || [])) {
    const importe   = Number(c.importe   || c.Importe   || 0);
    const descuento = Number(c.descuento || c.Descuento || 0);
    const traslados = c.impuestos?.traslados || [];
    const esTasa16  = traslados.some(t => {
      const imp = t.impuesto || t.Impuesto || '';
      return imp === '002' && Number(t.tasaOCuota ?? t.TasaOCuota ?? 0) > 0;
    });
    if (esTasa16) { subTotal16 += importe; desc16 += descuento; }
    else          { subTotal0  += importe; desc0  += descuento; }
  }
  // Fallback Metadata: no hay traslados en conceptos → estimar split desde el encabezado.
  // Se activa en dos casos:
  //   a) conceptos vacíos (subTotal16 + subTotal0 === 0)
  //   b) conceptos presentes pero SIN traslados → todos los importes cayeron en subTotal0
  //      aunque el header indique IVA > 0 (SAT Metadata o CFDIs sin desglose por concepto).
  const ivaHeader0 = Number(cfdi.impuestos?.totalImpuestosTrasladados || 0);
  if (subTotal16 === 0 && ivaHeader0 > 0) {
    const base = Number(cfdi.subTotal || 0) - Number(cfdi.descuento || 0);
    if (base > 0) {
      subTotal16 = parseFloat((ivaHeader0 / 0.16).toFixed(6));
      const rawSub0 = parseFloat(Math.max(0, base - subTotal16).toFixed(6));
      // Artefacto metadata: retenciones:[] con ivaRet ≈ ivaTras → rawSub0 es descuento implícito
      const ivaRetC = Number(cfdi.impuestos?.totalImpuestosRetenidos || 0);
      const esArtC  = (cfdi.conceptos || []).length === 0 &&
                      (cfdi.impuestos?.retenciones || []).length === 0 &&
                      ivaRetC > 0 && Math.abs(ivaRetC - ivaHeader0) < 1.0 &&
                      rawSub0 > 0.5;
      if (esArtC) {
        desc16    = rawSub0;  // descuento implícito inferido de los importes del CFDI
        subTotal0 = 0;
      } else {
        subTotal0 = rawSub0;
        const totalDesc = Number(cfdi.descuento || 0);
        if (totalDesc > 0 && (subTotal16 + subTotal0) > 0) {
          desc16 = parseFloat((totalDesc * subTotal16 / (subTotal16 + subTotal0)).toFixed(6));
          desc0  = parseFloat(Math.max(0, totalDesc - desc16).toFixed(6));
        }
      }
    }
  }
  return { subTotal16, subTotal0, desc16, desc0 };
}

/**
 * Convierte un CFDI en movimientos contables usando la regla encontrada.
 * Si no hay regla, devuelve movimientos con cuentaId null (requieren revisión manual).
 */
async function cfdiToMovimientos(cfdi, rule, cuentaMapExterno = null, context = {}) {
  const tipo      = cfdi.tipoDeComprobante;
  const esIngreso = tipo === 'I';
  const esPago    = tipo === 'P';
  const esPPD     = cfdi.metodoPago === 'PPD';

  // Para CFDI tipo P, total y subTotal son siempre 0 por diseño SAT.
  // Los montos reales viven en el complemento de pago.
  //
  // Complemento de Pago 2.0 (CFDI 4.0): usa el nodo <Totales> con MontoTotalPagos.
  // Complemento de Pago 1.0 (CFDI 3.3): no tiene <Totales> → suma pagos[].monto.
  // Descarga Metadata (>5 días): no tiene complementoPago → usar cfdi.total (viene del SAT metadata).
  const cpTotales = cfdi.complementoPago?.totales;
  const cpPagos   = cfdi.complementoPago?.pagos ?? [];

  const _cpTotal = esPago
    ? (cpTotales
        ? Number(cpTotales.montoTotalPagos || 0)
        : cpPagos.length > 0
          ? cpPagos.reduce((s, p) => s + Number(p.monto || 0), 0)
          : Number(cfdi.total || 0))   // fallback: total del SAT Metadata
    : Number(cfdi.total || 0);

  const subtotal = esPago
    ? (cpTotales
        ? Number(cpTotales.totalTrasladosBaseIVA16 || 0) +
          Number(cpTotales.totalTrasladosBaseIVA8  || 0)
        : _cpTotal)   // sin desglose de base: usar monto completo
    : (() => {
        const raw = Number(cfdi.subTotal || 0);
        if (raw > 0) return raw;
        // Metadata SAT: subTotal no viene en la descarga; derivar de total menos IVA conocido.
        const ivaKnown = Number(cfdi.impuestos?.totalImpuestosTrasladados || 0);
        return parseFloat((Number(cfdi.total || 0) - ivaKnown).toFixed(2));
      })();

  const total = _cpTotal;

  // Para tipo P el SAT exige Total=0 y SubTotal=0 en el header; los importes reales
  // están en complementoPago.totales (CP 2.0), pagos[].monto (CP 1.0), o cfdi.total (Metadata).
  const iva = esPago
    ? (cpTotales
        ? Number(cpTotales.totalTrasladosImpuestoIVA16 || 0) +
          Number(cpTotales.totalTrasladosImpuestoIVA8  || 0)
        : cpPagos.reduce((sum, pago) =>              // CP 1.0: IVA por DoctoRelacionado en trasladosDR
            sum + (pago.doctosRelacionados ?? []).reduce((s2, dr) =>
              s2 + (dr.trasladosDR ?? [])
                .filter(t => (t.impuesto || t.Impuesto || '') === '002' &&
                             Number(t.tasaOCuota ?? t.TasaOCuota ?? 0) > 0)
                .reduce((s3, t) => s3 + Number(t.importe || t.importeDR || t.ImporteDR || 0), 0)
            , 0)
          , 0))
    : Number(cfdi.impuestos?.totalImpuestosTrasladados || 0);

  const ivaRet = esPago
    ? Number(cpTotales?.totalRetencionesImpuestoIVA || 0)
    : Number(cfdi.impuestos?.totalImpuestosRetenidos || 0);

  // Artefacto metadata SAT: ivaRet ≈ ivaTras pero retenciones:[] → no hay retención real.
  // El gap (subTotal + iva − total) es un descuento implícito de conceptos que NUMO
  // infiere sin necesidad del XML. Permite generar el asiento bruto igual que el ERP.
  const esMetadataConDescuento = !esPago &&
    (cfdi.conceptos || []).length === 0 &&
    (cfdi.impuestos?.retenciones || []).length === 0 &&
    ivaRet > 0 && Math.abs(ivaRet - iva) < 1.0 &&
    (subtotal + iva - total) > 0.5;

  // ISR retenido: tipo P lo reporta en complementoPago.totales; tipo I/E en retenciones del header.
  // cfdi.impuestos está vacío para tipo P por especificación SAT CFDI 4.0.
  const isrRet = esPago
    ? Number(cpTotales?.totalRetencionesImpuestoISR || 0)
    : Number(
        (cfdi.impuestos?.retenciones ?? [])
          .filter(r => r.impuesto === '001')
          .reduce((s, r) => s + Number(r.importe || 0), 0),
      );

  // Para CFDIs emitidos (I=ingreso, E=nota crédito, P=cobro) la empresa es siempre
  // el emisor → el tercero para DIOT es el receptor (cliente).
  // Para CFDIs recibidos (gastos) la empresa es el receptor → el tercero es el emisor.
  const esEmitido = ['I', 'E', 'P'].includes(tipo);
  const rfcTercero = cfdi.emisor?.rfc === cfdi.receptor?.rfc
    ? null
    : esEmitido
      ? cfdi.receptor?.rfc
      : cfdi.emisor?.rfc;

  // Descripcion: algunos documentos tienen el campo en minúsculas (schema Mongoose)
  // y otros con mayúscula inicial (como viene del XML del SAT)
  const descRaw     = cfdi.conceptos?.[0]?.descripcion || cfdi.conceptos?.[0]?.Descripcion || '';
  const concepto    = descRaw.trim()
    ? descRaw.trim().slice(0, 200)
    : `CFDI ${tipo} ${cfdi.uuid?.slice(0, 8)}`;
  const centroCosto = rule?.centroCosto ?? '';
  // Fecha del CFDI como fecha de venta en formato YYYY-MM-DD
  const ventaFecha  = cfdi.fecha ? new Date(cfdi.fecha).toISOString().slice(0, 10) : null;
  // Serie del CFDI como referencia (serie+folio si existen)
  const serieCfdi   = [cfdi.serie, cfdi.folio].filter(Boolean).join('-').slice(0, 25) || null;

  if (!rule) {
    return [
      { cuentaId: null, concepto, centroCosto: '', debe: total,  haber: 0,    cfdiUuid: cfdi.uuid, rfcTercero, _sinRegla: true },
      { cuentaId: null, concepto, centroCosto: '', debe: 0,      haber: total, cfdiUuid: cfdi.uuid, rfcTercero, _sinRegla: true },
    ];
  }

  // Resolver cuentaId a partir del código
  const codigos = [
    rule.cuentaCargo,
    rule.cuentaAbono,
    rule.cuentaAbono2,
    rule.cuentaIva,
    rule.cuentaIvaPPD,
    rule.cuentaIvaRetenido,
    rule.cuentaIsrRetenido,
    rule.cuentaIvaAnticipo,
    rule.cuentaDeltaAnticipo,
    rule.cuentaDescuento,
    rule.cuentaDescuento0,
    rule.cuentaCargo2,
  ].filter(Boolean);

  let cuentaMap = cuentaMapExterno;
  if (!cuentaMap) {
    const cuentas = await AccountPlan.findAll({
      where: { codigo: { [Op.in]: codigos } },
      attributes: ['id', 'codigo'],
    });
    cuentaMap = Object.fromEntries(cuentas.map(c => [c.codigo, c.id]));
  }

  const movs = [];

  // ── Flags para lógica especializada ───────────────────────────────────────
  // esAnticipo (Reg 22C, Reg 23): el cargo cancela un pasivo → usa subtotal;
  //   el IVA hace swap: DEBE cuentaIvaAnticipo + HABER cuentaIva.
  // esIvaHaber (Reg 19): NC correctiva que actúa como ingreso →
  //   montoCargo=total, IVA va al HABER, montoAbono=subtotal.
  // esAplicacionSaldo (Reg 24B/25B): el cargo se divide entre saldo a favor
  //   (cuentaCargo) y efectivo/banco (cuentaCargo2).
  const esAnticipo        = !!(rule.cuentaIvaAnticipo && iva > 0);
  const esIvaHaber        = !!(rule.ivaHaber === true && tipo === 'E');
  const esAplicacionSaldo = !!(rule.esAplicacionSaldo && context.saldoDisponible != null);

  // H1: para Reg 22C cuando factura final > anticipo, el cargo debe cancelar SOLO el pasivo del
  // anticipo (no el subtotal completo de la factura). El IVA diferido a cancelar también es solo
  // el del anticipo. El exceso va a Bancos via el bloque delta.
  // context.ivaRelacionado y context.totalRelacionado son pre-fetched por el generator.
  // Si no están disponibles (balanza, o anticipo 1:1), cae al comportamiento original.
  const ivaCancelado     = (esAnticipo && context.ivaRelacionado     != null) ? context.ivaRelacionado                          : iva;
  const subtotalAnticipo = (esAnticipo && context.totalRelacionado   != null) ? context.totalRelacionado - ivaCancelado          : subtotal;

  // Línea principal cargo
  // esAnticipo + tipo I/E: cargo cancela el pasivo del anticipo → subtotalAnticipo
  // esAnticipo + tipo P: cargo aplica el saldo completo del monedero → total
  // esAplicacionSaldo: cargo = min(saldoDisponible, total) — el resto en cuentaCargo2
  let montoCargo;
  if (esAnticipo)        montoCargo = esPago ? total : subtotalAnticipo;
  else if (esIvaHaber)   montoCargo = total;
  else                   montoCargo = (esIngreso || esPago) ? total : subtotal;

  if (esAplicacionSaldo) {
    const saldoAplicado = parseFloat(Math.min(context.saldoDisponible, montoCargo).toFixed(2));
    const cashPago      = parseFloat((montoCargo - saldoAplicado).toFixed(2));
    montoCargo = saldoAplicado;
    // Cargo secundario: efectivo/banco por el monto no cubierto por el saldo
    if (cashPago > 0 && rule.cuentaCargo2) {
      movs.push({
        cuentaId:    cuentaMap[rule.cuentaCargo2] ?? null,
        concepto,
        centroCosto,
        ventaFecha,
        serie:       serieCfdi,
        debe:        cashPago,
        haber:       0,
        cfdiUuid:    cfdi.uuid,
        rfcTercero,
        _saldoUsado: saldoAplicado,   // metadata para que el generator actualice saldoRestante
      });
    }
  }

  movs.push({
    cuentaId:    cuentaMap[rule.cuentaCargo] ?? null,
    concepto,
    centroCosto,
    ventaFecha,
    serie:       serieCfdi,
    debe:        montoCargo,
    haber:       0,
    cfdiUuid:    cfdi.uuid,
    rfcTercero,
    ...(esAplicacionSaldo && !rule.cuentaCargo2 ? { _saldoUsado: montoCargo } : {}),
  });

  // IVA en facturas (tipo I y E)
  // PPD → cuenta "por cobrar/por pagar" (cuentaIvaPPD); PUE → cuenta final (cuentaIva)
  // Ingreso: HABER | Egreso: DEBE | esIvaHaber (Reg 19): HABER aunque sea E
  // esAnticipo: el IVA se maneja en el bloque dedicado de más abajo → omitir aquí
  if (!esPago && !esAnticipo && iva > 0) {
    const cuentaIvaAplicable = (esPPD && rule.cuentaIvaPPD) ? rule.cuentaIvaPPD : rule.cuentaIva;
    if (cuentaIvaAplicable) {
      const ivaEsHaber = esIngreso || esIvaHaber;
      // Derivar IVA de montos SAT (2 decimales) para evitar artefactos IEEE-754.
      // Formula SAT: total = subtotal − descuento + IVA → IVA = total − subtotal + descuento.
      // Se suma descuento solo cuando la regla genera movimiento separado (tieneDescuento=true);
      // si no hay movimiento de descuento, el término es 0 y la fórmula no cambia.
      const _descuentoEnIva = (rule.tieneDescuento && !esPago) ? Number(cfdi.descuento || 0) : 0;
      const ivaR = (ivaRet === 0 && isrRet === 0)
        ? parseFloat((total - subtotal + _descuentoEnIva).toFixed(2))
        : iva;
      movs.push({
        cuentaId:    cuentaMap[cuentaIvaAplicable] ?? null,
        concepto:    `IVA - ${concepto}`,
        centroCosto,
        ventaFecha,
        serie:       serieCfdi,
        debe:        ivaEsHaber ? 0    : ivaR,
        haber:       ivaEsHaber ? ivaR : 0,
        cfdiUuid:    cfdi.uuid,
        rfcTercero,
      });
    }
  }

  // Reconocimiento de IVA al cobro (solo tipo P con PPD configurado)
  // Cancela el saldo de cuentaIvaPPD y lo traslada a cuentaIva.
  // esAnticipo usa su propio bloque de swap IVA → se omite aquí.
  if (esPago && !esAnticipo && iva > 0 && rule.cuentaIvaPPD && rule.cuentaIva) {
    movs.push({
      cuentaId:    cuentaMap[rule.cuentaIvaPPD] ?? null,
      concepto:    `IVA cobrado - ${concepto}`,
      centroCosto,
      ventaFecha,
      serie:       serieCfdi,
      debe:        iva,
      haber:       0,
      cfdiUuid:    cfdi.uuid,
      rfcTercero,
    });
    movs.push({
      cuentaId:    cuentaMap[rule.cuentaIva] ?? null,
      concepto:    `IVA cobrado - ${concepto}`,
      centroCosto,
      ventaFecha,
      serie:       serieCfdi,
      debe:        0,
      haber:       iva,
      cfdiUuid:    cfdi.uuid,
      rfcTercero,
    });
  }

  // ── Motor anticipo: swap IVA-diferido → IVA-definitivo ───────────────────
  // Aplica a Reg 22C (I + formaPago=30) y Reg 23 (E + tipoRelacion=07).
  // DEBE cuentaIvaAnticipo (2104010002) cancela el IVA diferido del anticipo.
  // HABER cuentaIva (2104010001) reconoce el IVA causado definitivo.
  if (esAnticipo) {
    // DEBE: cancela el IVA diferido del anticipo (solo su monto, no el de la factura completa)
    movs.push({
      cuentaId:    cuentaMap[rule.cuentaIvaAnticipo] ?? null,
      concepto:    `IVA ant. - ${concepto}`,
      centroCosto, ventaFecha, serie: serieCfdi,
      debe:        ivaCancelado,
      haber:       0,
      cfdiUuid:    cfdi.uuid,
      rfcTercero,
    });
    if (rule.cuentaIva) {
      // HABER: reconoce el IVA definitivo completo de la factura final
      movs.push({
        cuentaId:    cuentaMap[rule.cuentaIva] ?? null,
        concepto:    `IVA ant. - ${concepto}`,
        centroCosto, ventaFecha, serie: serieCfdi,
        debe:        0,
        haber:       iva,
        cfdiUuid:    cfdi.uuid,
        rfcTercero,
      });
    }
  }

  // ── 5° movimiento: delta anticipo (total_factura - total_anticipo_relacionado) ─
  // Solo para Reg 22C (esAnticipo + tipo I/E) cuando la factura supera el anticipo.
  // context.totalRelacionado = suma de totales de los CFDIs relacionados (pre-fetch externo).
  if (esAnticipo && rule.cuentaDeltaAnticipo && context.totalRelacionado != null) {
    const delta = parseFloat((total - context.totalRelacionado).toFixed(2));
    if (delta > 0) {
      movs.push({
        cuentaId:    cuentaMap[rule.cuentaDeltaAnticipo] ?? null,
        concepto:    `Saldo - ${concepto}`,
        centroCosto, ventaFecha, serie: serieCfdi,
        debe:        delta,
        haber:       0,
        cfdiUuid:    cfdi.uuid,
        rfcTercero,
      });
    }
  }

  // IVA retenido (siempre HABER)
  if (rule.cuentaIvaRetenido && ivaRet > 0) {
    movs.push({
      cuentaId:    cuentaMap[rule.cuentaIvaRetenido] ?? null,
      concepto:    `IVA ret. - ${concepto}`,
      centroCosto,
      ventaFecha,
      serie:       serieCfdi,
      debe:        0,
      haber:       ivaRet,
      cfdiUuid:    cfdi.uuid,
      rfcTercero,
    });
  }

  // ISR retenido (siempre HABER)
  if (rule.cuentaIsrRetenido && isrRet > 0) {
    movs.push({
      cuentaId:    cuentaMap[rule.cuentaIsrRetenido] ?? null,
      concepto:    `ISR ret. - ${concepto}`,
      centroCosto,
      ventaFecha,
      serie:       serieCfdi,
      debe:        0,
      haber:       isrRet,
      cfdiUuid:    cfdi.uuid,
      rfcTercero,
    });
  }

  // Línea principal abono
  // esAnticipo (Reg 22C/23): abono = subtotal (monto sin IVA — el IVA ya está en bloque anticipo)
  // esIvaHaber (Reg 19):     abono = subtotal - descuento (IVA ya en HABER; descuento reduce base)
  // Ingreso normal:          HABER = subtotal
  // Egreso/Pago normal:      HABER = total neto (descontando retenciones)
  // cuentaIvaAbono (ej. Club Tuberos monedero): abono = subtotal; IVA va a cuenta separada
  // esAnticipo + tipo P: abono cierra la CxC por el total pagado
  // esAnticipo + tipo I/E / esIvaHaber: abono = subtotal - descuento (IVA ya manejado aparte)
  let montoAbono;
  const tieneIvaAbonoSplit = !esPago && !esAnticipo && !esIvaHaber && rule.cuentaIvaAbono && iva > 0;
  if (esAnticipo)      montoAbono = esPago ? (total - ivaRet - isrRet) : subtotal;
  else if (esIvaHaber) montoAbono = subtotal - Number(cfdi.descuento || 0);
  else if (tieneIvaAbonoSplit) {
    // Split IVA abono: cuentaAbono recibe solo subtotal, cuentaIvaAbono recibe IVA
    montoAbono = subtotal;
  } else if (esIngreso) {
    // IVA = total − subtotal + descuento (cuando tieneDescuento), por lo que el complemento
    // exacto de HABER es subtotal (importe bruto pre-descuento del CFDI). D=H garantizado.
    // Para CFDIs con retenciones se usa total−iva (rama else de ivaR arriba) — caso separado.
    // Excepción: no-mixto con tieneDescuento y tasa 0% — no hay línea de IVA que cierre la
    // brecha, y el subTotal del header SAT puede ser inconsistente con metadata (total+descuento
    // ≠ cfdi.subTotal). Forzar montoAbono = total + descuento para garantizar D=H.
    if (rule.tasaIva !== 'mixto' && rule.tieneDescuento && iva === 0) {
      montoAbono = parseFloat((total + Number(cfdi.descuento || 0)).toFixed(2));
    } else {
      montoAbono = (ivaRet === 0 && isrRet === 0) || esMetadataConDescuento
        ? subtotal
        : parseFloat((total - iva).toFixed(2));
    }
  } else {
    montoAbono = total - ivaRet - isrRet;
  }

  // ── Motor extendido: reglas MIXTAS (tasaIva=mixto, cuentaAbono2) ──────────
  // Divide el abono/cargo entre porción 16% y porción 0%.
  // Para tipo I: cuentaAbono=Ingresos16% (HABER subtotal16), cuentaAbono2=Ingresos0% (HABER subtotal0).
  // Para tipo E: cuentaAbono2=Devoluciones0% (DEBE subtotal0), ajusta cuentaCargo a subtotal16.
  if (!esPago && rule.tasaIva === 'mixto' && rule.cuentaAbono2) {
    const { subTotal16, subTotal0 } = _calcCfdiMontos(cfdi);
    if (subTotal16 + subTotal0 > 0) {
      if (esIngreso) {
        const subTotal16R = parseFloat(subTotal16.toFixed(2));
        const subTotal0R  = parseFloat(subTotal0.toFixed(2));
        // Si la regla tiene descuentos separados (tieneDescuento), el cargo por Dto ya se
        // agregará como DEBE extra → Ingresos16 debe ser el bruto (subTotal16_gross) para
        // que D=H. Sin descuentos separados, el neto garantiza el balance directamente.
        montoAbono = rule.tieneDescuento
          ? subTotal16R
          : parseFloat(((parseFloat((total - iva).toFixed(2))) - subTotal0R).toFixed(2));
        if (subTotal0R > 0) {
          movs.push({
            cuentaId:    cuentaMap[rule.cuentaAbono2] ?? null,
            concepto:    `${concepto} (0%)`,
            centroCosto, ventaFecha, serie: serieCfdi,
            debe: 0, haber: subTotal0R,
            cfdiUuid: cfdi.uuid, rfcTercero,
          });
          // Split cargo 0%: cuentaCargo recibe solo (total - subTotal0) = subtotal16 + IVA16
          // cuentaCargoMixto0 recibe subTotal0 (sin IVA, pues tasa 0% no genera IVA)
          if (rule.cuentaCargoMixto0) {
            const cargoLine = movs.find(m =>
              m.cuentaId === (cuentaMap[rule.cuentaCargo] ?? null) && m.debe > 0
            );
            if (cargoLine) {
              cargoLine.debe = parseFloat((cargoLine.debe - subTotal0R).toFixed(2));
            }
            movs.push({
              cuentaId:    cuentaMap[rule.cuentaCargoMixto0] ?? null,
              concepto:    `${concepto} (0%)`,
              centroCosto, ventaFecha, serie: serieCfdi,
              debe: subTotal0R, haber: 0,
              cfdiUuid: cfdi.uuid, rfcTercero,
            });
          }
        }
      } else {
        // Tipo E (NC mixta): cargo principal = subTotal16, cargo secundario = subTotal0.
        // Pre-redondear a 2 decimales antes de usar: el fallback (_calcCfdiMontos) puede
        // devolver valores con 4 decimales (iva/0.16). PostgreSQL los redondea al insertar
        // en DECIMAL(15,2), creando un descuadre de $0.01. Al redondear aquí primero y
        // calcular ivaR desde los valores ya redondeados se garantiza balance exacto.
        const s16R = parseFloat(subTotal16.toFixed(2));
        const s0R  = parseFloat(subTotal0.toFixed(2));
        const cargoLine = movs.find(m => m.cuentaId === (cuentaMap[rule.cuentaCargo] ?? null) && m.debe > 0);
        if (cargoLine) cargoLine.debe = s16R;
        const ivaLineMixto = movs.find(m => m.concepto?.startsWith('IVA - ') && m.debe > 0);
        if (ivaLineMixto) {
          ivaLineMixto.debe = parseFloat((total - s16R - s0R).toFixed(2));
        }
        if (s0R > 0) {
          movs.push({
            cuentaId:    cuentaMap[rule.cuentaAbono2] ?? null,
            concepto:    `${concepto} (0%)`,
            centroCosto, ventaFecha, serie: serieCfdi,
            debe: s0R, haber: 0,
            cfdiUuid: cfdi.uuid, rfcTercero,
          });
        }
      }
    }
  }

  // ── Motor extendido: reglas con DESCUENTO (tieneDescuento=true) ───────────
  // Agrega línea(s) de Descuentos s/Ventas como DEBE (reducen el ingreso bruto).
  // Solo para tipo I y E — los pagos no tienen descuento propio.
  if (!esPago && rule.tieneDescuento) {
    const { desc16, desc0 } = _calcCfdiMontos(cfdi);
    const descHeader = Number(cfdi.descuento || 0);
    // Para reglas mixtas usamos desc16 (concepto); para 16% o 0% usamos el header para
    // mantener consistencia con montoAbono = cfdi.subTotal (ambos de la misma fuente SAT).
    const descPrincipal = rule.tasaIva === 'mixto' ? desc16 : descHeader;

    if (rule.cuentaDescuento && descPrincipal > 0) {
      movs.push({
        cuentaId:    cuentaMap[rule.cuentaDescuento] ?? null,
        concepto:    `Dto. - ${concepto}`,
        centroCosto, ventaFecha, serie: serieCfdi,
        debe:  esIngreso ? descPrincipal : 0,
        haber: esIngreso ? 0 : descPrincipal,
        cfdiUuid: cfdi.uuid, rfcTercero,
      });
    }
    if (rule.cuentaDescuento0 && rule.tasaIva === 'mixto' && desc0 > 0) {
      movs.push({
        cuentaId:    cuentaMap[rule.cuentaDescuento0] ?? null,
        concepto:    `Dto.0% - ${concepto}`,
        centroCosto, ventaFecha, serie: serieCfdi,
        debe:  esIngreso ? desc0 : 0,
        haber: esIngreso ? 0 : desc0,
        cfdiUuid: cfdi.uuid, rfcTercero,
      });
    }
  }

  movs.push({
    cuentaId:    cuentaMap[rule.cuentaAbono] ?? null,
    concepto,
    centroCosto,
    ventaFecha,
    serie:       serieCfdi,
    debe:        0,
    haber:       montoAbono,
    cfdiUuid:    cfdi.uuid,
    rfcTercero,
  });

  // Split IVA abono: cuando cuentaIvaAbono está definida, el IVA va a cuenta separada
  // HABER cuentaIvaAbono = IVA (ej. 2104010002 IVA Trasladado Anticipos para Club Tuberos)
  // Esto espeja el patrón CONTPAQI: HABER Monedero=subtotal + HABER IVAAnticipo=IVA
  if (tieneIvaAbonoSplit && rule.cuentaIvaAbono) {
    const _desc2 = rule.tieneDescuento ? Number(cfdi.descuento || 0) : 0;
    const ivaR = parseFloat((total - subtotal + _desc2).toFixed(2));
    if (ivaR > 0) {
      movs.push({
        cuentaId:    cuentaMap[rule.cuentaIvaAbono] ?? null,
        concepto:    `IVA - ${concepto}`,
        centroCosto,
        ventaFecha,
        serie:       serieCfdi,
        debe:        0,
        haber:       ivaR,
        cfdiUuid:    cfdi.uuid,
        rfcTercero,
      });
    }
  }

  // Dentro del asiento de cada CFDI: cargos (debe > 0) primero, abonos después
  movs.sort((a, b) => {
    const ao = (a.debe || 0) > 0 ? 0 : 1;
    const bo = (b.debe || 0) > 0 ? 0 : 1;
    return ao - bo;
  });

  // Enriquecer cada movimiento con los campos SAT del CFDI origen y la regla usada
  const satMeta = {
    tipoComprobante: cfdi.tipoDeComprobante                      ?? null,
    metodoPago:      cfdi.metodoPago                            ?? null,
    formaPago:       cfdi.formaPago                             ?? null,
    folio:           cfdi.folio                                 ?? null,
    rfcEmisor:       cfdi.emisor?.rfc                           ?? null,
    rfcReceptor:     cfdi.receptor?.rfc                         ?? null,
    reglaId:         rule?.id                                   ?? null,
    reglaNombre:     rule?.nombre                               ?? null,
    tipoOrigen:      cfdi.tipoOrigen ?? _derivarTipoOrigen(cfdi) ?? null,
  };

  return movs.map(m => ({ ...m, ...satMeta }));
}

/**
 * Aplica la migración de descuentos PPD directamente en la BD:
 *   - Reg 6, 11, 13 → tieneDescuento=false
 *   - Inserta Reg 6B (PPD 16% con descuento) y Reg 6C (PPD 0% con descuento)
 * Es idempotente.
 */
async function migrarPpdDescuento() {
  const resultado = { actualizadas: [], insertadas: [], yaExistian: [] };

  // 1. Actualizar Reg 6, 11, 13
  const nombresActualizar = [
    'Reg 6 — Venta PPD 16% (Factura a Crédito)',
    'Reg 11 — Venta PPD Tasa 0%',
    'Reg 13 — Venta Mixta PPD (0%+16%)',
  ];
  for (const nombre of nombresActualizar) {
    const regla = await CfdiMappingRule.findOne({ where: { nombre } });
    if (!regla) continue;
    if (regla.tieneDescuento === false) { resultado.yaExistian.push(nombre); continue; }
    await regla.update({ tieneDescuento: false });
    resultado.actualizadas.push(nombre);
  }

  // 2. Insertar Reg 6B
  const nuevas = [
    {
      nombre:          'Reg 6B — Venta con Descuento PPD 16%',
      tipoComprobante: 'I',
      metodoPago:      'PPD',
      formaPago:       '99',
      tasaIva:         '16',
      tieneDescuento:  true,
      cuentaCargo:     '1103010001',
      cuentaAbono:     '4100020001',
      cuentaIvaPPD:    '2105010001',
      cuentaDescuento: '4200020001',
      prioridad:       59,
      isActive:        true,
    },
    {
      nombre:          'Reg 6C — Venta con Descuento PPD 0%',
      tipoComprobante: 'I',
      metodoPago:      'PPD',
      formaPago:       '99',
      tasaIva:         '0',
      tieneDescuento:  true,
      cuentaCargo:     '1103010002',
      cuentaAbono:     '4100010002',
      cuentaIvaPPD:    null,
      cuentaDescuento: '4200020002',
      prioridad:       64,
      isActive:        true,
    },
  ];
  for (const datos of nuevas) {
    const existe = await CfdiMappingRule.findOne({ where: { nombre: datos.nombre } });
    if (existe) { resultado.yaExistian.push(datos.nombre); continue; }
    await CfdiMappingRule.create(datos);
    resultado.insertadas.push(datos.nombre);
  }

  return resultado;
}

// ── Privado ───────────────────────────────────────────────────────────────────

function _validate(data) {
  if (!data.nombre?.trim())    throw new BadRequestError('El nombre es requerido');
  if (!data.cuentaCargo?.trim()) throw new BadRequestError('La cuenta de cargo es requerida');
  if (!data.cuentaAbono?.trim()) throw new BadRequestError('La cuenta de abono es requerida');
}

module.exports = { list, getById, create, update, remove, findRuleForCfdi, findRuleInList, cfdiToMovimientos, migrarPpdDescuento, _detectTasaIvaPublic: _detectTasaIva, _derivarTipoOrigenPublic: _derivarTipoOrigen };
