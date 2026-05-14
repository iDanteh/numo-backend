'use strict';

const { Op, Transaction, QueryTypes } = require('sequelize');
const { sequelize }        = require('../../../../config/database.postgres');
const { Poliza, PolizaMovimiento, AccountPlan } = require('../../../../shared/models/postgres');
const CFDI = require('../../../../visor/models/CFDI');

// ── Inclusión estándar de movimientos con cuenta ──────────────────────────────
const MOVIMIENTOS_INCLUDE = {
  model:      PolizaMovimiento,
  as:         'movimientos',
  include: [{
    model:      AccountPlan,
    as:         'cuenta',
    attributes: ['id', 'codigo', 'nombre', 'tipo', 'naturaleza'],
  }],
  order: [['orden', 'ASC']],
};

async function findAll(filters = {}) {
  const where = {};
  if (filters.rfc)       where.rfc       = filters.rfc;
  if (filters.ejercicio) where.ejercicio  = Number(filters.ejercicio);
  if (filters.periodo)   where.periodo    = Number(filters.periodo);
  if (filters.tipo)      where.tipo       = filters.tipo.toUpperCase();
  if (filters.estado)    where.estado     = filters.estado;

  const page  = Math.max(1, Number(filters.page)  || 1);
  const limit = Math.min(100, Number(filters.limit) || 50);
  const offset = (page - 1) * limit;

  const { count, rows } = await Poliza.findAndCountAll({
    where,
    order:  [['fecha', 'DESC'], ['tipo', 'ASC'], ['numero', 'DESC']],
    limit,
    offset,
  });

  // ── Enriquecer con estado de CFDIs vinculados (cross PostgreSQL → MongoDB) ──
  if (rows.length > 0) {
    const polizaIds = rows.map(r => r.id);

    // DISTINCT en SQL — evita traer N filas por cada CFDI (antes: 28K filas para 9K CFDIs)
    const movCfdis = await sequelize.query(
      `SELECT DISTINCT poliza_id AS "polizaId", cfdi_uuid AS "cfdiUuid"
       FROM poliza_movimientos
       WHERE poliza_id IN (:polizaIds) AND cfdi_uuid IS NOT NULL`,
      { replacements: { polizaIds }, type: QueryTypes.SELECT },
    );

    if (movCfdis.length > 0) {
      // polizaId → Set<uuid>
      const polizaCfdiMap = {};
      for (const m of movCfdis) {
        if (!polizaCfdiMap[m.polizaId]) polizaCfdiMap[m.polizaId] = new Set();
        polizaCfdiMap[m.polizaId].add(m.cfdiUuid);
      }

      // Limitar $in a 400 UUIDs para no saturar MongoDB en cada carga de lista
      const MAX_CHECK = 400;
      const allUuids  = [...new Set(movCfdis.map(m => m.cfdiUuid))];
      const uuidsCheck = allUuids.length <= MAX_CHECK ? allUuids : allUuids.slice(0, MAX_CHECK);

      const cfdis = await CFDI.find(
        { uuid: { $in: uuidsCheck } },
        { uuid: 1, satStatus: 1, source: 1, _id: 0 },
      ).lean();

      // uuid → { satStatus, sources }
      const cfdiMap = {};
      for (const c of cfdis) {
        if (!cfdiMap[c.uuid]) cfdiMap[c.uuid] = { satStatus: c.satStatus, sources: new Set() };
        cfdiMap[c.uuid].sources.add(c.source);
        if (c.satStatus === 'Cancelado') cfdiMap[c.uuid].satStatus = 'Cancelado';
      }

      for (const poliza of rows) {
        const uuids = [...(polizaCfdiMap[poliza.id] || [])];
        if (uuids.length === 0) continue;

        let vigentes = 0, cancelados = 0, ambosLados = 0, soloSat = 0;
        for (const uuid of uuids) {
          const info = cfdiMap[uuid];
          if (!info) continue;
          if (info.satStatus === 'Cancelado') cancelados++;
          else vigentes++;
          const hasSat = info.sources.has('SAT');
          const hasErp = info.sources.has('ERP');
          if (hasSat && hasErp) ambosLados++;
          else if (hasSat)     soloSat++;
        }

        poliza.dataValues.cfdiSummary = { total: uuids.length, vigentes, cancelados, ambosLados, soloSat };
      }
    }
  }

  return { total: count, page, limit, pages: Math.ceil(count / limit), polizas: rows };
}

async function findById(id) {
  const poliza = await Poliza.findByPk(id, { include: [MOVIMIENTOS_INCLUDE] });
  if (!poliza) return null;

  const uuids = [...new Set(
    poliza.movimientos.map(m => m.cfdiUuid).filter(Boolean),
  )];

  if (uuids.length > 0) {
    const cfdis = await CFDI.find(
      { uuid: { $in: uuids } },
      { uuid: 1, satStatus: 1, erpStatus: 1, source: 1, _id: 0 },
    ).lean();

    // Consolidar por uuid — un UUID puede tener registro SAT y ERP por separado
    const byUuid = {};
    for (const c of cfdis) {
      if (!byUuid[c.uuid]) byUuid[c.uuid] = { satStatus: null, erpStatus: null, sources: new Set() };
      byUuid[c.uuid].sources.add(c.source);
      if (c.source === 'SAT' && c.satStatus) byUuid[c.uuid].satStatus = c.satStatus;
      if (c.source === 'ERP' && c.erpStatus) byUuid[c.uuid].erpStatus = c.erpStatus;
    }

    const cfdiAlertMap = {};
    for (const uuid of uuids) {
      const info = byUuid[uuid];
      if (!info) {
        cfdiAlertMap[uuid] = { alerts: ['no_encontrado'] };
        continue;
      }
      const alerts = [];
      const hasSat = info.sources.has('SAT');
      const hasErp = info.sources.has('ERP');

      if (hasSat && !hasErp)                                                  alerts.push('solo_sat');
      if (info.satStatus === 'Cancelado')                                     alerts.push('cancelado_sat');
      if (info.erpStatus === 'Cancelacion Pendiente')                         alerts.push('cancelacion_pendiente');
      if (info.erpStatus === 'Cancelado' && info.satStatus === 'Vigente')     alerts.push('cancelado_erp_vigente_sat');
      if (info.erpStatus === 'Deshabilitado' && info.satStatus === 'Vigente') alerts.push('deshabilitado_erp');

      if (alerts.length > 0) {
        cfdiAlertMap[uuid] = { satStatus: info.satStatus, erpStatus: info.erpStatus, alerts };
      }
    }

    if (Object.keys(cfdiAlertMap).length > 0) {
      poliza.dataValues.cfdiAlertMap = cfdiAlertMap;
    }
  }

  return poliza;
}

async function nextNumero(tipo, rfc, ejercicio, periodo, transaction) {
  const max = await Poliza.max('numero', {
    where: { tipo, rfc, ejercicio, periodo },
    transaction,
  });
  return (max || 0) + 1;
}

async function create(data) {
  const { movimientos = [], ...header } = data;

  return sequelize.transaction(async (t) => {
    // Lock para evitar race condition en numeración simultánea
    await sequelize.query(
      'SELECT pg_advisory_xact_lock(hashtext(:key))',
      { replacements: { key: `poliza-${header.tipo}-${header.rfc}-${header.ejercicio}-${header.periodo}` }, transaction: t }
    );

    header.numero = await nextNumero(header.tipo, header.rfc, header.ejercicio, header.periodo, t);

    const poliza = await Poliza.create(header, { transaction: t });

    if (movimientos.length > 0) {
      const rows = movimientos.map((m, i) => ({ ...m, polizaId: poliza.id, orden: i + 1 }));
      await PolizaMovimiento.bulkCreate(rows, { transaction: t });
    }

    return findById(poliza.id);
  });
}

async function update(id, data) {
  const { movimientos, ...header } = data;

  return sequelize.transaction(async (t) => {
    // Lock de fila para evitar race condition TOCTOU
    const poliza = await Poliza.findByPk(id, { transaction: t, lock: Transaction.LOCK.UPDATE });
    if (!poliza) return null;

    if (Object.keys(header).length > 0) {
      await poliza.update(header, { transaction: t });
    }

    if (movimientos !== undefined) {
      await PolizaMovimiento.destroy({ where: { polizaId: id }, transaction: t });
      if (movimientos.length > 0) {
        const rows = movimientos.map((m, i) => ({ ...m, polizaId: id, orden: i + 1 }));
        await PolizaMovimiento.bulkCreate(rows, { transaction: t });
      }
    }

    return findById(id);
  });
}

// Cambio de estado con lock — evita TOCTOU en contabilizar/cancelar/revertir
async function setEstado(id, estado, auditFields = {}) {
  return sequelize.transaction(async (t) => {
    const poliza = await Poliza.findByPk(id, { transaction: t, lock: Transaction.LOCK.UPDATE });
    if (!poliza) return null;
    await poliza.update({ estado, ...auditFields }, { transaction: t });
    return findById(id);
  });
}

async function cancel(id, auditFields = {}) {
  return setEstado(id, 'cancelada', auditFields);
}

async function destroy(id) {
  const count = await Poliza.destroy({ where: { id } });
  return count > 0;
}

/** Trae todas las pólizas contabilizadas de un periodo con sus movimientos y cuenta. */
async function findAllContabilizadas({ rfc, ejercicio, periodo }) {
  return Poliza.findAll({
    where:   { rfc, ejercicio, periodo, estado: 'contabilizada' },
    order:   [['tipo', 'ASC'], ['numero', 'ASC']],
    include: [MOVIMIENTOS_INCLUDE],
  });
}

module.exports = { findAll, findById, create, update, cancel, setEstado, destroy, findAllContabilizadas };
