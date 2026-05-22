'use strict';

const { CfdiMappingRule, AccountPlan } = require('../../../shared/models/postgres');
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
  return rule.update(data);
}

async function remove(id) {
  const rule = await CfdiMappingRule.findByPk(id);
  if (!rule) throw new NotFoundError('Regla de mapeo');
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
  const cfdiTipoRelacion   = cfdi.cfdiRelacionados?.[0]?.tipoRelacion ?? null;
  const cfdiTasaIva        = _detectTasaIva(cfdi);
  const cfdiTieneDescuento = _detectTieneDescuento(cfdi);

  // _relacionadoTipo es un campo virtual inyectado por el generator antes del matching.
  // Contiene el tipoDeComprobante del primer CFDI relacionado (pre-fetched de MongoDB).
  const cfdiRelacionadoTipo = cfdi._relacionadoTipo ?? null;

  const matching = rules.filter(r =>
    (!r.tipoComprobante    || r.tipoComprobante  === cfdi.tipoDeComprobante) &&
    (!r.rfcEmisor          || r.rfcEmisor        === cfdi.emisor?.rfc) &&
    (!r.rfcReceptor        || r.rfcReceptor      === cfdi.receptor?.rfc) &&
    (!r.metodoPago         || r.metodoPago       === cfdi.metodoPago) &&
    (!r.formaPago          || r.formaPago         === cfdi.formaPago) &&
    (!r.claveProdServ      || r.claveProdServ     === cfdiClaveProdServ) &&
    (!r.tipoRelacion       || r.tipoRelacion      === cfdiTipoRelacion) &&
    (!r.relacionadoTipo    || r.relacionadoTipo   === cfdiRelacionadoTipo) &&
    (r.tasaIva        == null || r.tasaIva        === cfdiTasaIva) &&
    (r.tieneDescuento == null || r.tieneDescuento === cfdiTieneDescuento),
  );
  if (!matching.length) return null;

  const spec = r => [
    r.tipoComprobante, r.rfcEmisor, r.rfcReceptor, r.metodoPago, r.formaPago,
    r.claveProdServ, r.tipoRelacion, r.relacionadoTipo, r.tasaIva,
    r.tieneDescuento != null ? String(r.tieneDescuento) : null,
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
  if (cfdi.tipoDeComprobante === 'P') return null;
  let tiene16 = false;
  let tiene0  = false;
  for (const c of (cfdi.conceptos || [])) {
    for (const t of (c.impuestos?.traslados || [])) {
      // Solo IVA (código SAT '002') — excluye IEPS y otros impuestos
      const impuesto = t.impuesto || t.Impuesto || '';
      if (impuesto !== '002') continue;
      const tasa = Number(t.tasaOCuota ?? t.TasaOCuota ?? 0);
      if (tasa >= 0.1) tiene16 = true;
      else             tiene0  = true;
    }
  }
  // Fallback 1: IVA a nivel header (cfdi.impuestos.traslados)
  if (!tiene16 && !tiene0) {
    for (const t of (cfdi.impuestos?.traslados || [])) {
      const impuesto = t.impuesto || t.Impuesto || '';
      if (impuesto !== '002') continue;
      const tasa = Number(t.tasaOCuota ?? t.TasaOCuota ?? 0);
      if (tasa >= 0.1) tiene16 = true;
      else             tiene0  = true;
    }
  }
  // Fallback 2: traslados vacío pero totalImpuestosTrasladados disponible
  // (CFDIs importados sin desglose de tasa pero con importe de IVA en el header)
  if (!tiene16 && !tiene0) {
    const rawTotal = cfdi.impuestos?.totalImpuestosTrasladados;
    // Solo actuar si el campo está EXPLÍCITAMENTE presente en el documento.
    // Si es undefined/null no sabemos la tasa → devolvemos null (no inferir 0%).
    if (rawTotal != null) {
      const totalImptos = Number(rawTotal);
      // Cualquier IVA > 0 → tasa no-cero (captura 16%, 8% fronterizo, etc.)
      // Solo se marca tasa 0% cuando el campo está presente Y es exactamente 0.
      if (totalImptos > 0) tiene16 = true;
      else                 tiene0  = true;
    }
  }
  if (tiene16 && tiene0) return 'mixto';
  if (tiene16)           return '16';
  if (tiene0)            return '0';
  return null; // sin información de tasa
}

/** Detecta si el CFDI tiene descuento > 0 (header o en algún concepto). */
function _detectTieneDescuento(cfdi) {
  if (Number(cfdi.descuento || 0) > 0) return true;
  return (cfdi.conceptos || []).some(c => Number(c.descuento || c.Descuento || 0) > 0);
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
      return imp === '002' && Number(t.tasaOCuota ?? t.TasaOCuota ?? 0) >= 0.1;
    });
    if (esTasa16) { subTotal16 += importe; desc16 += descuento; }
    else          { subTotal0  += importe; desc0  += descuento; }
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
  const subtotal = esPago
    ? Number(cfdi.complementoPago?.totales?.totalTrasladosBaseIVA16 || 0) +
      Number(cfdi.complementoPago?.totales?.totalTrasladosBaseIVA8  || 0)
    : Number(cfdi.subTotal || 0);

  const total = esPago
    ? Number(cfdi.complementoPago?.totales?.montoTotalPagos || 0)
    : Number(cfdi.total || 0);

  // Para tipo P el SAT exige Total=0 y SubTotal=0 en el header; los importes reales
  // están en complementoPago.totales (Complemento de Pago 2.0).
  const iva = esPago
    ? Number(cfdi.complementoPago?.totales?.totalTrasladosImpuestoIVA16 || 0) +
      Number(cfdi.complementoPago?.totales?.totalTrasladosImpuestoIVA8  || 0)
    : Number(cfdi.impuestos?.totalImpuestosTrasladados || 0);

  const ivaRet = esPago
    ? Number(cfdi.complementoPago?.totales?.totalRetencionesIVA || 0)
    : Number(cfdi.impuestos?.totalImpuestosRetenidos || 0);

  // ISR retenido viene de retenciones individuales con impuesto='001'
  const isrRet = Number(
    (cfdi.impuestos?.retenciones ?? [])
      .filter(r => r.impuesto === '001')
      .reduce((s, r) => s + Number(r.importe || 0), 0),
  );

  const rfcTercero = cfdi.emisor?.rfc === cfdi.receptor?.rfc
    ? null
    : esIngreso
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

  // Línea principal cargo
  // esAnticipo + tipo I/E: cargo cancela el pasivo (sin IVA) → subtotal
  // esAnticipo + tipo P: cargo aplica el saldo completo del monedero → total
  // esAplicacionSaldo: cargo = min(saldoDisponible, total) — el resto en cuentaCargo2
  let montoCargo;
  if (esAnticipo)        montoCargo = esPago ? total : subtotal;
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
      movs.push({
        cuentaId:    cuentaMap[cuentaIvaAplicable] ?? null,
        concepto:    `IVA - ${concepto}`,
        centroCosto,
        ventaFecha,
        serie:       serieCfdi,
        debe:        ivaEsHaber ? 0   : iva,
        haber:       ivaEsHaber ? iva : 0,
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
    movs.push({
      cuentaId:    cuentaMap[rule.cuentaIvaAnticipo] ?? null,
      concepto:    `IVA ant. - ${concepto}`,
      centroCosto, ventaFecha, serie: serieCfdi,
      debe:        iva,
      haber:       0,
      cfdiUuid:    cfdi.uuid,
      rfcTercero,
    });
    if (rule.cuentaIva) {
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
  // esIvaHaber (Reg 19):     abono = subtotal (el IVA ya está en HABER por la línea de IVA)
  // Ingreso normal:          HABER = subtotal
  // Egreso/Pago normal:      HABER = total neto (descontando retenciones)
  // esAnticipo + tipo P: abono cierra la CxC por el total pagado
  // esAnticipo + tipo I/E / esIvaHaber: abono = subtotal (IVA ya manejado aparte)
  let montoAbono;
  if (esAnticipo)      montoAbono = esPago ? (total - ivaRet - isrRet) : subtotal;
  else if (esIvaHaber) montoAbono = subtotal;
  else                 montoAbono = esIngreso ? subtotal : total - ivaRet - isrRet;

  // ── Motor extendido: reglas MIXTAS (tasaIva=mixto, cuentaAbono2) ──────────
  // Divide el abono/cargo entre porción 16% y porción 0%.
  // Para tipo I: cuentaAbono=Ingresos16% (HABER subtotal16), cuentaAbono2=Ingresos0% (HABER subtotal0).
  // Para tipo E: cuentaAbono2=Devoluciones0% (DEBE subtotal0), ajusta cuentaCargo a subtotal16.
  if (!esPago && rule.tasaIva === 'mixto' && rule.cuentaAbono2) {
    const { subTotal16, subTotal0 } = _calcCfdiMontos(cfdi);
    if (subTotal16 + subTotal0 > 0) {
      if (esIngreso) {
        montoAbono = subTotal16;                    // cuentaAbono cubre solo 16%
        if (subTotal0 > 0) {
          movs.push({
            cuentaId:    cuentaMap[rule.cuentaAbono2] ?? null,
            concepto:    `${concepto} (0%)`,
            centroCosto, ventaFecha, serie: serieCfdi,
            debe: 0, haber: subTotal0,
            cfdiUuid: cfdi.uuid, rfcTercero,
          });
        }
      } else {
        // Tipo E (NC mixta): cargo principal = subTotal16, cargo secundario = subTotal0
        const cargoLine = movs.find(m => m.cuentaId === (cuentaMap[rule.cuentaCargo] ?? null) && m.debe > 0);
        if (cargoLine) cargoLine.debe = subTotal16;
        if (subTotal0 > 0) {
          movs.push({
            cuentaId:    cuentaMap[rule.cuentaAbono2] ?? null,
            concepto:    `${concepto} (0%)`,
            centroCosto, ventaFecha, serie: serieCfdi,
            debe: subTotal0, haber: 0,
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
    // Para reglas mixtas usamos desc16; para 16% o 0% usamos el total del header
    const descPrincipal = rule.tasaIva === 'mixto' ? desc16 : (desc16 + desc0 > 0 ? desc16 + desc0 : descHeader);

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

  // Dentro del asiento de cada CFDI: cargos (debe > 0) primero, abonos después
  movs.sort((a, b) => {
    const ao = (a.debe || 0) > 0 ? 0 : 1;
    const bo = (b.debe || 0) > 0 ? 0 : 1;
    return ao - bo;
  });

  return movs;
}

// ── Privado ───────────────────────────────────────────────────────────────────

function _validate(data) {
  if (!data.nombre?.trim())    throw new BadRequestError('El nombre es requerido');
  if (!data.cuentaCargo?.trim()) throw new BadRequestError('La cuenta de cargo es requerida');
  if (!data.cuentaAbono?.trim()) throw new BadRequestError('La cuenta de abono es requerida');
}

module.exports = { list, getById, create, update, remove, findRuleForCfdi, findRuleInList, cfdiToMovimientos };
