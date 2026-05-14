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
  const matching = rules.filter(r =>
    (!r.tipoComprobante || r.tipoComprobante === cfdi.tipoDeComprobante) &&
    (!r.rfcEmisor       || r.rfcEmisor       === cfdi.emisor?.rfc) &&
    (!r.metodoPago      || r.metodoPago      === cfdi.metodoPago) &&
    (!r.formaPago       || r.formaPago       === cfdi.formaPago),
  );
  if (!matching.length) return null;

  const spec = r => [r.tipoComprobante, r.rfcEmisor, r.metodoPago, r.formaPago].filter(Boolean).length;
  return matching.sort((a, b) => {
    if (a.prioridad !== b.prioridad) return a.prioridad - b.prioridad;
    return spec(b) - spec(a);
  })[0];
}

/**
 * Convierte un CFDI en movimientos contables usando la regla encontrada.
 * Si no hay regla, devuelve movimientos con cuentaId null (requieren revisión manual).
 */
async function cfdiToMovimientos(cfdi, rule, cuentaMapExterno = null) {
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

  const iva    = Number(cfdi.impuestos?.totalImpuestosTrasladados || 0);
  const ivaRet = Number(cfdi.impuestos?.totalImpuestosRetenidos   || 0);
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
    rule.cuentaIva,
    rule.cuentaIvaPPD,
    rule.cuentaIvaRetenido,
    rule.cuentaIsrRetenido,
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

  // Línea principal cargo
  // Ingreso: DEBE = total (CxC por el monto completo con IVA)
  // Egreso/Pago: DEBE = subtotal (gasto neto o base del pago)
  const montoCargo = (esIngreso || esPago) ? total : subtotal;
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
  });

  // IVA en facturas (tipo I y E)
  // PPD → cuenta "por cobrar/por pagar" (cuentaIvaPPD); PUE → cuenta final (cuentaIva)
  // Ingreso: HABER | Egreso: DEBE
  if (!esPago && iva > 0) {
    const cuentaIvaAplicable = (esPPD && rule.cuentaIvaPPD) ? rule.cuentaIvaPPD : rule.cuentaIva;
    if (cuentaIvaAplicable) {
      movs.push({
        cuentaId:    cuentaMap[cuentaIvaAplicable] ?? null,
        concepto:    `IVA - ${concepto}`,
        centroCosto,
        ventaFecha,
        serie:       serieCfdi,
        debe:        esIngreso ? 0   : iva,
        haber:       esIngreso ? iva : 0,
        cfdiUuid:    cfdi.uuid,
        rfcTercero,
      });
    }
  }

  // Reconocimiento de IVA al cobro (solo tipo P con PPD configurado)
  // Cancela el saldo de cuentaIvaPPD y lo traslada a cuentaIva
  if (esPago && iva > 0 && rule.cuentaIvaPPD && rule.cuentaIva) {
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
  // Ingreso: HABER = subtotal (ventas sin IVA)
  // Egreso:  HABER = total neto (proveedor/bancos, descontando retenciones)
  // Pago:    HABER = total (CxC a liquidar)
  const montoAbono = esIngreso ? subtotal : total - ivaRet - isrRet;
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
