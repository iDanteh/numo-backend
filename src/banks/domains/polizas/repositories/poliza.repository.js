'use strict';

const { Transaction, QueryTypes, Op } = require('sequelize');
const { sequelize }        = require('../../../../config/database.postgres');
const { Poliza, PolizaMovimiento, AccountPlan, CentroCosto, CfdiMappingRule } = require('../../../../shared/models/postgres');
const CFDI = require('../../../../visor/models/CFDI');
const { _extraerSustitutos } = require('../../cfdi-mapping/sustitutos-cfdi.util');

// ── Inclusión estándar de movimientos con cuenta y centro de costo ───────────
const MOVIMIENTOS_INCLUDE = {
  model:      PolizaMovimiento,
  as:         'movimientos',
  include: [
    {
      model:      AccountPlan,
      as:         'cuenta',
      attributes: ['id', 'codigo', 'nombre', 'tipo', 'naturaleza'],
    },
    {
      model:      CentroCosto,
      as:         'centroCostoObj',
      attributes: ['id', 'clave', 'sucursal', 'serieFacturacion'],
      required:   false,
    },
    {
      model:      CfdiMappingRule,
      as:         'regla',
      attributes: ['id', 'nombre', 'prioridad', 'tipoComprobante', 'metodoPago', 'formaPago',
                   'rfcEmisor', 'rfcReceptor', 'tipoRelacion', 'tasaIva', 'tieneDescuento',
                   'cuentaCargo', 'cuentaAbono', 'cuentaIva', 'isActive'],
      required:   false,
    },
  ],
  order: [['orden', 'ASC']],
};

async function findAll(filters = {}) {
  const where = {};
  if (filters.rfc)       where.rfc       = filters.rfc;
  if (filters.ejercicio) where.ejercicio  = Number(filters.ejercicio);
  if (filters.periodo)   where.periodo    = Number(filters.periodo);
  if (filters.tipo)      where.tipo       = filters.tipo.toUpperCase();
  if (filters.estado)    where.estado     = filters.estado;
  if (filters.q) {
    const q = `%${filters.q.trim()}%`;
    where[Op.or] = [
      { concepto: { [Op.iLike]: q } },
      { folio:    { [Op.iLike]: q } },
    ];
  }
  // Vista "Pólizas de Cobranza" vs. "Pólizas de Ingreso": separa estrictamente
  // por si la póliza tiene algún movimiento que viene de un CFDI de Pago
  // (complemento de pago / cobranza de cartera), mismo marcador que usa
  // poliza.service.js (esPagos) para el export CONTPAQ — true = solo esas
  // (Cobranza); false = solo las que NO tienen ninguna (Ingreso), para que no
  // se mezclen (confirmado con el usuario 2026-08-11 — antes solo se
  // filtraba el caso true, dejando las de Cobranza visibles también en
  // Ingreso). Mismo criterio que ya usa `listBorradorCandidatas`.
  const SUBQUERY_POLIZAS_PAGO = `(SELECT DISTINCT poliza_id FROM poliza_movimientos WHERE tipo_comprobante = 'P')`;
  if (filters.soloCobranza === true || filters.soloCobranza === 'true') {
    where.id = { [Op.in]: sequelize.literal(SUBQUERY_POLIZAS_PAGO) };
  } else if (filters.soloCobranza === false || filters.soloCobranza === 'false') {
    where.id = { [Op.notIn]: sequelize.literal(SUBQUERY_POLIZAS_PAGO) };
    // Pólizas de Traspaso (T) ni de Compensaciones/Intereses Ganados (B/G,
    // 2026-08-27) son Ingreso ni Cobranza — se excluyen de esta vista (mismo
    // criterio confirmado con el usuario 2026-08-26 para T, extendido a B/G).
    // Si ya se filtró por un tipo explícito, se respeta.
    if (!filters.tipo) where.tipo = { [Op.notIn]: ['T', 'B', 'G'] };
  }

  const page  = Math.max(1, Number(filters.page)  || 1);
  const limit = Math.min(100, Number(filters.limit) || 50);
  const offset = (page - 1) * limit;

  const [count, rows] = await Promise.all([
    Poliza.count({ where }),
    Poliza.findAll({
      where,
      order:  [['fecha', 'DESC'], ['tipo', 'ASC'], ['numero', 'DESC']],
      limit,
      offset,
    }),
  ]);

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
      const allUuids   = [...new Set(movCfdis.map(m => m.cfdiUuid))];
      const uuidsCheck = allUuids.length <= MAX_CHECK ? allUuids : allUuids.slice(0, MAX_CHECK);

      // Lanzar la query de Mongo tan pronto como tenemos los UUIDs
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

/** Trae la póliza con movimientos sin la consulta cruzada a MongoDB.
 *  Usar en operaciones de estado (contabilizar/cancelar/revertir) donde
 *  el cfdiAlertMap no es necesario y la consulta Mongo sería muy costosa. */
async function findByIdLight(id) {
  return Poliza.findByPk(id, { include: [MOVIMIENTOS_INCLUDE] });
}

// `transaction` opcional: create()/update() de abajo lo llaman DESDE DENTRO de su
// propia transacción, antes de que confirme — sin pasarla acá, Poliza.findByPk usa
// otra conexión del pool que bajo READ COMMITTED todavía no ve la fila recién creada
// (isolation normal de Postgres, no un bug de Sequelize) y devuelve null. Bug real
// encontrado 2026-08-25 al generar la primera póliza de Traspasos C.P. (create()
// nunca se había ejercitado antes en la práctica — generarYGuardar(), el camino real
// de generación desde CFDIs, no pasa por acá).
async function findById(id, transaction) {
  const poliza = await Poliza.findByPk(id, { include: [MOVIMIENTOS_INCLUDE], transaction });
  if (!poliza) return null;

  const uuids = [...new Set(
    poliza.movimientos.map(m => m.cfdiUuid).filter(Boolean),
  )];

  if (uuids.length > 0) {
    const cfdis = await CFDI.find(
      { uuid: { $in: uuids } },
      { uuid: 1, satStatus: 1, erpStatus: 1, source: 1, metodoPago: 1, formaPago: 1, 'emisor.rfc': 1, tipoDeComprobante: 1, _id: 0 },
    ).lean();

    // Consolidar por uuid — un UUID puede tener registro SAT y ERP por separado
    const byUuid = {};
    for (const c of cfdis) {
      if (!byUuid[c.uuid]) byUuid[c.uuid] = { satStatus: null, erpStatus: null, sources: new Set(), metodoPago: null, formaPago: null, rfc: c.emisor?.rfc ?? null, tipoDeComprobante: c.tipoDeComprobante ?? null };
      byUuid[c.uuid].sources.add(c.source);
      if (c.source === 'SAT' && c.satStatus)    byUuid[c.uuid].satStatus  = c.satStatus;
      if (c.source === 'ERP' && c.erpStatus)    byUuid[c.uuid].erpStatus  = c.erpStatus;
      if (c.metodoPago && !byUuid[c.uuid].metodoPago) byUuid[c.uuid].metodoPago = c.metodoPago;
      if (c.formaPago  && !byUuid[c.uuid].formaPago)  byUuid[c.uuid].formaPago  = c.formaPago;
    }

    const cfdiAlertMap = {};
    const cfdiMetaMap  = {};
    for (const uuid of uuids) {
      const info = byUuid[uuid];
      if (!info) {
        cfdiAlertMap[uuid] = { alerts: ['no_encontrado'] };
        continue;
      }

      // Meta (metodoPago / formaPago) — siempre disponible
      cfdiMetaMap[uuid] = { metodoPago: info.metodoPago, formaPago: info.formaPago };

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

    // Para los CFDIs cancelados de esta póliza: ¿ya tienen un sustituto
    // (tipoRelacion='04')? Botón "CFDIs cancelados" del detalle de póliza —
    // se reutiliza `_extraerSustitutos` (mismo parseo que usa el generador,
    // incluye el fix del bug real de dos UUIDs concatenados en un solo
    // string) en vez de una query Mongo ingenua por `uuids: uuid`.
    const uuidsCancelados = Object.keys(cfdiAlertMap).filter(u => cfdiAlertMap[u].alerts.includes('cancelado_sat'));
    if (uuidsCancelados.length > 0) {
      const rfcs  = [...new Set(uuidsCancelados.map(u => byUuid[u]?.rfc).filter(Boolean))];
      const tipos = [...new Set(uuidsCancelados.map(u => byUuid[u]?.tipoDeComprobante).filter(Boolean))];
      const candidatosSustituto = (rfcs.length && tipos.length)
        ? await CFDI.find({
            'emisor.rfc':        { $in: rfcs },
            tipoDeComprobante:   { $in: tipos },
            'cfdiRelacionados.tipoRelacion': '04',
          }, { uuid: 1, serie: 1, folio: 1, tipoDeComprobante: 1, cfdiRelacionados: 1, _id: 0 }).lean()
        : [];
      const sustitutoPorOriginal = new Map();
      for (const s of _extraerSustitutos(candidatosSustituto)) {
        for (const uOriginal of s.sustituyeA) {
          sustitutoPorOriginal.set(uOriginal, { uuid: s.uuid, serie: s.serie, folio: s.folio });
        }
      }
      for (const uuid of uuidsCancelados) {
        cfdiAlertMap[uuid].sustituto = sustitutoPorOriginal.get(uuid.toUpperCase()) ?? null;
      }
    }

    if (Object.keys(cfdiAlertMap).length > 0) {
      poliza.dataValues.cfdiAlertMap = cfdiAlertMap;
    }
    if (Object.keys(cfdiMetaMap).length > 0) {
      poliza.dataValues.cfdiMetaMap = cfdiMetaMap;
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

    return findById(poliza.id, t);
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

    return findById(id, t);
  });
}

// Antes de sobreescribir cuentaId, guarda la cuenta previa en
// cuentaAnteriorId — SOLO si todavía no había una guardada (COALESCE), para
// que tras varios reemplazos encadenados (automático + manual) siempre quede
// la cuenta ORIGINAL de antes del primer cruce, no la intermedia. Se usa
// tanto en el cruce automático como en el reemplazo manual, y se restaura al
// revertir la póliza a borrador (ver `restaurarCuentasAnteriores`).
const _GUARDAR_CUENTA_ANTERIOR = sequelize.literal('COALESCE("cuenta_anterior_id", "cuenta_id")');

// Actualiza cuentaId de movimientos puntuales de una póliza (usado por
// `_resolverCuentasBancoReal` al contabilizar) — un UPDATE por fila porque
// cada movimiento va a una cuenta distinta (no es un reemplazo uniforme).
async function actualizarCuentasMovimientos(polizaId, actualizaciones) {
  if (!actualizaciones?.length) return 0;
  return sequelize.transaction(async (t) => {
    for (const { movimientoId, cuentaId } of actualizaciones) {
      await PolizaMovimiento.update(
        { cuentaId, cuentaFaltante: false, cuentaAnteriorId: _GUARDAR_CUENTA_ANTERIOR },
        { where: { id: movimientoId, polizaId }, transaction: t },
      );
    }
    return actualizaciones.length;
  });
}

// Reemplazo manual uniforme: toda línea de la póliza que use `cuentaPuenteId`
// pasa a `cuentaDestinoId`. Usado para resolver a mano lo que el cruce
// automático de `contabilizar` no pudo cruzar.
async function reemplazarCuentaEnPoliza(polizaId, cuentaPuenteId, cuentaDestinoId) {
  const [afectados] = await PolizaMovimiento.update(
    { cuentaId: cuentaDestinoId, cuentaFaltante: false, cuentaAnteriorId: _GUARDAR_CUENTA_ANTERIOR },
    { where: { polizaId, cuentaId: cuentaPuenteId } },
  );
  return afectados;
}

// Deshace el/los cruce(s) de cuenta banco-real (automático o manual) al
// revertir la póliza a borrador — limpia cuentaAnteriorId para que el
// próximo `contabilizar`/`resolverCuentasBanco` vuelva a calcular desde cero.
async function restaurarCuentasAnteriores(polizaId) {
  const [afectados] = await PolizaMovimiento.update(
    { cuentaId: sequelize.literal('"cuenta_anterior_id"'), cuentaAnteriorId: null },
    { where: { polizaId, cuentaAnteriorId: { [Op.ne]: null } } },
  );
  return afectados;
}

// Cambio de estado con lock — evita TOCTOU en contabilizar/cancelar/revertir
async function setEstado(id, estado, auditFields = {}) {
  return sequelize.transaction(async (t) => {
    const poliza = await Poliza.findByPk(id, { transaction: t, lock: Transaction.LOCK.UPDATE });
    if (!poliza) return null;
    await poliza.update({ estado, ...auditFields }, { transaction: t });
    return findByIdLight(id);
  });
}

async function cancel(id, auditFields = {}) {
  return setEstado(id, 'cancelada', auditFields);
}

async function destroy(id) {
  const count = await Poliza.destroy({ where: { id } });
  return count > 0;
}

/** Devuelve los asientos (agrupados por cfdi_uuid + poliza) donde debe ≠ haber,
 *  enriquecidos con los datos del CFDI desde MongoDB. */
async function findDescuadradas({ rfc, ejercicio, periodo, estado, polizaId }) {
  const conditions   = ['pm.cfdi_uuid IS NOT NULL', 'p.rfc = :rfc'];
  const replacements = { rfc };
  if (polizaId)  { conditions.push('p.id        = :polizaId'); replacements.polizaId  = Number(polizaId); }
  if (ejercicio) { conditions.push('p.ejercicio = :ejercicio'); replacements.ejercicio = Number(ejercicio); }
  if (periodo)   { conditions.push('p.periodo   = :periodo');   replacements.periodo   = Number(periodo); }
  if (estado)    { conditions.push('p.estado    = :estado');    replacements.estado    = estado; }

  const rows = await sequelize.query(`
    SELECT
      pm.cfdi_uuid                                               AS "cfdiUuid",
      pm.poliza_id                                               AS "polizaId",
      p.tipo,
      p.numero,
      p.fecha::text                                              AS fecha,
      p.estado,
      ROUND(SUM(pm.debe)::numeric,  2)                          AS "totalDebe",
      ROUND(SUM(pm.haber)::numeric, 2)                          AS "totalHaber",
      ROUND(ABS(SUM(pm.debe) - SUM(pm.haber))::numeric, 2)      AS diferencia
    FROM poliza_movimientos pm
    JOIN polizas p ON p.id = pm.poliza_id
    WHERE ${conditions.join(' AND ')}
    GROUP BY pm.cfdi_uuid, pm.poliza_id, p.tipo, p.numero, p.fecha, p.estado
    HAVING ABS(SUM(pm.debe) - SUM(pm.haber)) > 0.01
    ORDER BY diferencia DESC
  `, { replacements, type: QueryTypes.SELECT });

  if (rows.length === 0) return [];

  const uuids = [...new Set(rows.map(r => r.cfdiUuid))];
  const cfdis = await CFDI.find(
    { uuid: { $in: uuids } },
    { uuid: 1, tipoDeComprobante: 1, metodoPago: 1, formaPago: 1,
      total: 1, subTotal: 1, fecha: 1, folio: 1, serie: 1,
      moneda: 1, exportacion: 1, lugarExpedicion: 1,
      'emisor.rfc': 1, 'emisor.nombre': 1, 'emisor.regimenFiscal': 1,
      'receptor.rfc': 1, 'receptor.nombre': 1, 'receptor.usoCfdi': 1,
      'impuestos.totalImpuestosTrasladados': 1,
      satStatus: 1, erpStatus: 1, source: 1, _id: 0 },
  ).lean();

  const cfdiMap = {};
  for (const c of cfdis) {
    if (!cfdiMap[c.uuid]) cfdiMap[c.uuid] = { ...c, sources: [] };
    if (!cfdiMap[c.uuid].sources.includes(c.source)) cfdiMap[c.uuid].sources.push(c.source);
    if (c.source === 'SAT') cfdiMap[c.uuid].satStatus = c.satStatus;
  }

  return rows.map(r => ({ ...r, cfdi: cfdiMap[r.cfdiUuid] ?? null }));
}

/** Trae todas las pólizas contabilizadas de un periodo con sus movimientos y cuenta. */
async function findAllContabilizadas({ rfc, ejercicio, periodo }) {
  return Poliza.findAll({
    where:   { rfc, ejercicio, periodo, estado: 'contabilizada' },
    order:   [['tipo', 'ASC'], ['numero', 'ASC']],
    include: [MOVIMIENTOS_INCLUDE],
  });
}

module.exports = {
  findAll, findById, findByIdLight, create, update, cancel, setEstado, destroy, findAllContabilizadas, findDescuadradas,
  actualizarCuentasMovimientos, reemplazarCuentaEnPoliza, restaurarCuentasAnteriores,
};
