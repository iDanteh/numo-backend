'use strict';

const CFDI           = require('../../../visor/models/CFDI');
const { AccountPlan, CfdiMappingRule, Poliza, PolizaMovimiento } = require('../../../shared/models/postgres');
const { Op }         = require('sequelize');
const mappingSvc     = require('./cfdi-mapping.service');
const { BadRequestError } = require('../../shared/errors/AppError');

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
async function generarBalanzaPreliminar({ rfc, ejercicio, periodo, tipoCfdi }) {
  if (!rfc)       throw new BadRequestError('RFC requerido');
  if (!ejercicio) throw new BadRequestError('Ejercicio requerido');
  if (!periodo)   throw new BadRequestError('Periodo requerido');

  const tipos = tipoCfdi ? [tipoCfdi] : ['I', 'E', 'P'];

  // 1. Cargar reglas activas una sola vez
  const rules = await CfdiMappingRule.findAll({
    where: { isActive: true },
    order: [['prioridad', 'ASC']],
  });

  // 2. Precalcular cuentaMap para todos los códigos de todas las reglas activas
  const codigosTodos = [...new Set(
    rules.flatMap(r => [
      r.cuentaCargo, r.cuentaAbono, r.cuentaIva,
      r.cuentaIvaPPD, r.cuentaIvaRetenido, r.cuentaIsrRetenido,
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

  // 3. Procesar CFDIs por tipo
  const movimientosTodos = [];
  let totalCfdis = 0;
  let sinRegla   = 0;

  for (const tipo of tipos) {
    const cfdis = await CFDI.find({
      $or:               [{ 'emisor.rfc': rfc }, { 'receptor.rfc': rfc }],
      ejercicio:         Number(ejercicio),
      periodo:           Number(periodo),
      tipoDeComprobante: tipo,
      source:            'SAT',
      satStatus:         'Vigente',
      isActive:          true,
    })
      .select('tipoDeComprobante metodoPago formaPago emisor.rfc receptor.rfc subTotal total impuestos complementoPago.totales')
      .maxTimeMS(60_000)
      .lean();

    totalCfdis += cfdis.length;

    const resultados = await Promise.all(
      cfdis.map(async (cfdi) => {
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

    byAccount[key].debe     += Number(m.debe)  || 0;
    byAccount[key].haber    += Number(m.haber) || 0;
    byAccount[key].movCount += 1;
  }

  // 5. Saldo inicial = acumulado de movimientos de pólizas contabilizadas de periodos anteriores.
  //    Cubre años anteriores completos + meses anteriores del año actual.
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

  const totales = {
    debe:         Math.round(cuentas.reduce((s, c) => s + c.debe,         0) * 100) / 100,
    haber:        Math.round(cuentas.reduce((s, c) => s + c.haber,        0) * 100) / 100,
    saldoInicial: Math.round(cuentas.reduce((s, c) => s + c.saldoInicial, 0) * 100) / 100,
    saldoFinal:   Math.round(cuentas.reduce((s, c) => s + c.saldo,        0) * 100) / 100,
  };

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

module.exports = { generarBalanzaPreliminar };
