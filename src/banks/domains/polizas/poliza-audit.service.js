'use strict';

/**
 * poliza-audit.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Bitácora de pólizas (2026-10-15) — escritura (`registrar`) y consultas del
 * monitor solo-admin (`resumenPorUsuario`, `actividadDiaria`, `bitacora`,
 * `cancelaciones`).
 *
 * `registrar` NUNCA lanza: una falla de la bitácora no debe tumbar la acción
 * real (contabilizar, cancelar, exportar...). Solo deja un warning en el log.
 *
 * Creación: se registra con un hook `afterCreate` definido en el modelo Poliza
 * (shared/models/postgres/Poliza.js) en vez de tocar cada generador.
 */

const { QueryTypes } = require('sequelize');
const { sequelize }  = require('../../../config/database.postgres');
const PolizaAuditLog = require('../../../shared/models/postgres/PolizaAuditLog');
const { logger }     = require('../../../shared/utils/logger');

const ACCIONES = Object.freeze([
  'creada', 'editada', 'contabilizada', 'cancelada', 'revertida',
  'exportada', 'exportada_xml_sat', 'folio_contpaq', 'cuenta_reemplazada',
]);

// Mismo criterio que userLabel() de poliza.service.js — así los nombres de la
// bitácora coinciden con los de las columnas *Por de `polizas`.
function _usuario(user) {
  if (!user) return { usuarioId: null, usuarioNombre: 'sistema', usuarioRol: null };
  if (typeof user === 'string') return { usuarioId: null, usuarioNombre: user, usuarioRol: null };
  return {
    usuarioId:     user.dbId != null ? String(user.dbId) : null,
    usuarioNombre: user.nombre || user.email || String(user.dbId ?? 'sistema'),
    usuarioRol:    user.role || null,
  };
}

function _datosPoliza(poliza) {
  if (!poliza) return {};
  return {
    polizaId:       poliza.id ?? null,
    rfc:            poliza.rfc ?? null,
    polizaTipo:     poliza.tipo ?? null,
    polizaNumero:   poliza.numero ?? null,
    polizaFecha:    poliza.fecha ?? null,
    polizaConcepto: poliza.concepto ? String(poliza.concepto).slice(0, 500) : null,
    ejercicio:      poliza.ejercicio ?? null,
    periodo:        poliza.periodo ?? null,
  };
}

/**
 * @param {string} accion — una de ACCIONES
 * @param {object|null} poliza — instancia o plain object (id, tipo, numero, fecha, concepto, rfc...)
 * @param {object|string|null} user — req.user, o la etiqueta ya resuelta
 * @param {{ estadoAnterior?, estadoNuevo?, motivo?, detalle? }} extra
 */
async function registrar(accion, poliza, user, extra = {}) {
  try {
    await PolizaAuditLog.create({
      accion,
      ..._datosPoliza(poliza),
      ..._usuario(user),
      estadoAnterior: extra.estadoAnterior ?? null,
      estadoNuevo:    extra.estadoNuevo ?? null,
      motivo:         extra.motivo ?? null,
      detalle:        extra.detalle ?? null,
      ...(extra.rfc       ? { rfc: extra.rfc } : {}),
      ...(extra.ejercicio ? { ejercicio: Number(extra.ejercicio) } : {}),
      ...(extra.periodo   ? { periodo: Number(extra.periodo) } : {}),
    });
  } catch (e) {
    logger.warn(`[PolizaAudit] No se pudo registrar "${accion}" (póliza ${poliza?.id ?? '-'}): ${e.message}`);
  }
}

// ── Consultas del monitor ──────────────────────────────────────────────────────

// desde/hasta son días de México ('YYYY-MM-DD'); México sin horario de verano → UTC-6.
function _filtros({ desde, hasta, rfc, usuario, accion, tipo, q }, alias = '') {
  const a = alias ? `${alias}.` : '';
  const where = [];
  const repl  = {};
  if (desde)  { where.push(`${a}fecha >= :desde`); repl.desde = new Date(`${desde}T00:00:00-06:00`); }
  if (hasta)  {
    const fin = new Date(`${hasta}T00:00:00-06:00`);
    fin.setUTCDate(fin.getUTCDate() + 1);
    where.push(`${a}fecha < :hasta`); repl.hasta = fin;
  }
  if (rfc)     { where.push(`${a}rfc = :rfc`); repl.rfc = rfc; }
  if (usuario) {
    if (usuario === '(sin registro)') where.push(`${a}usuario_nombre IS NULL`);
    else { where.push(`${a}usuario_nombre = :usuario`); repl.usuario = usuario; }
  }
  if (accion)  { where.push(`${a}accion = :accion`); repl.accion = accion; }
  if (tipo === 'COB') where.push(`${a}poliza_tipo = 'D' AND ${a}poliza_concepto LIKE 'Ingresos por Cobranza%'`);
  else if (tipo)      { where.push(`${a}poliza_tipo = :tipo`); repl.tipo = tipo; }
  if (q) {
    where.push(`(${a}poliza_concepto ILIKE :q OR ${a}motivo ILIKE :q OR CAST(${a}poliza_numero AS TEXT) = :qExacto)`);
    repl.q = `%${q}%`; repl.qExacto = String(q).trim();
  }
  return { sql: where.length ? `WHERE ${where.join(' AND ')}` : '', repl };
}

async function resumenPorUsuario(filtros) {
  const { sql, repl } = _filtros({ ...filtros, usuario: undefined, accion: undefined });
  const rows = await sequelize.query(`
    SELECT COALESCE(usuario_nombre, '(sin registro)') AS usuario,
           MAX(usuario_rol) AS rol,
           accion, COUNT(*)::int AS total, MAX(fecha) AS ultima
      FROM poliza_audit_logs ${sql}
     GROUP BY 1, accion
  `, { replacements: repl, type: QueryTypes.SELECT });

  const porUsuario = new Map();
  for (const r of rows) {
    if (!porUsuario.has(r.usuario)) {
      porUsuario.set(r.usuario, { usuario: r.usuario, rol: r.rol, total: 0, ultimaActividad: null, acciones: {} });
    }
    const u = porUsuario.get(r.usuario);
    u.acciones[r.accion] = r.total;
    u.total += r.total;
    if (r.rol && !u.rol) u.rol = r.rol;
    if (!u.ultimaActividad || new Date(r.ultima) > new Date(u.ultimaActividad)) u.ultimaActividad = r.ultima;
  }
  return [...porUsuario.values()].sort((x, y) => y.total - x.total);
}

async function actividadDiaria(filtros) {
  const { sql, repl } = _filtros(filtros);
  return sequelize.query(`
    SELECT to_char((fecha AT TIME ZONE 'America/Mexico_City')::date, 'YYYY-MM-DD') AS dia,
           accion, COUNT(*)::int AS total
      FROM poliza_audit_logs ${sql}
     GROUP BY 1, accion
     ORDER BY 1
  `, { replacements: repl, type: QueryTypes.SELECT });
}

async function bitacora({ page = 1, limit = 50, ...filtros }) {
  const lim = Math.min(Math.max(Number(limit) || 50, 1), 500);
  const off = (Math.max(Number(page) || 1, 1) - 1) * lim;
  const { sql, repl } = _filtros(filtros);
  const [{ total }] = await sequelize.query(
    `SELECT COUNT(*)::int AS total FROM poliza_audit_logs ${sql}`,
    { replacements: repl, type: QueryTypes.SELECT },
  );
  const rows = await sequelize.query(`
    SELECT id, poliza_id AS "polizaId", accion, usuario_nombre AS "usuarioNombre", usuario_rol AS "usuarioRol",
           rfc, poliza_tipo AS "polizaTipo", poliza_numero AS "polizaNumero", poliza_fecha AS "polizaFecha",
           poliza_concepto AS "polizaConcepto", ejercicio, periodo, estado_anterior AS "estadoAnterior",
           estado_nuevo AS "estadoNuevo", motivo, detalle, origen, fecha
      FROM poliza_audit_logs ${sql}
     ORDER BY fecha DESC, id DESC
     LIMIT :lim OFFSET :off
  `, { replacements: { ...repl, lim, off }, type: QueryTypes.SELECT });
  return { total, page: Number(page) || 1, limit: lim, rows };
}

/**
 * Cancelaciones con su "regeneración": la primera póliza del mismo rfc/tipo/
 * fecha/concepto creada después de la cancelación (así se regenera hoy: cancelar
 * y volver a generar el mismo día/sucursal). Se muestra aunque esa también se
 * haya cancelado después — su estado va en regeneradaEstado.
 */
async function cancelaciones(filtros) {
  const { sql, repl } = _filtros({ ...filtros, accion: 'cancelada' }, 'l');
  return sequelize.query(`
    SELECT l.id, l.poliza_id AS "polizaId", l.usuario_nombre AS "usuarioNombre", l.usuario_rol AS "usuarioRol",
           l.rfc, l.poliza_tipo AS "polizaTipo", l.poliza_numero AS "polizaNumero", l.poliza_fecha AS "polizaFecha",
           l.poliza_concepto AS "polizaConcepto", l.estado_anterior AS "estadoAnterior", l.motivo, l.origen, l.fecha,
           orig.creado_por AS "creadoPor", orig.created_at AS "creadaEn",
           r.id AS "regeneradaId", r.numero AS "regeneradaNumero", r.estado AS "regeneradaEstado",
           r.creado_por AS "regeneradaPor", r.created_at AS "regeneradaEn"
      FROM poliza_audit_logs l
      LEFT JOIN polizas orig ON orig.id = l.poliza_id
      LEFT JOIN LATERAL (
        SELECT p.id, p.numero, p.estado, p.creado_por, p.created_at
          FROM polizas p
         WHERE p.rfc = l.rfc AND p.tipo::text = l.poliza_tipo AND p.fecha = l.poliza_fecha
           AND p.concepto = orig.concepto AND p.id <> l.poliza_id
           AND p.created_at >= l.fecha
         ORDER BY p.created_at
         LIMIT 1
      ) r ON TRUE
      ${sql}
     ORDER BY l.fecha DESC
     LIMIT 1000
  `, { replacements: repl, type: QueryTypes.SELECT });
}

async function usuarios() {
  const rows = await sequelize.query(
    `SELECT DISTINCT COALESCE(usuario_nombre, '(sin registro)') AS u FROM poliza_audit_logs ORDER BY 1`,
    { type: QueryTypes.SELECT },
  );
  return rows.map(r => r.u);
}

module.exports = {
  ACCIONES, registrar,
  resumenPorUsuario, actividadDiaria, bitacora, cancelaciones, usuarios,
};
