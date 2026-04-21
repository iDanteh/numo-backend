'use strict';

/**
 * shared/services/rbac-store.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Cache en memoria de roles/permisos cargados desde PostgreSQL.
 *
 * Reemplaza las llamadas síncronas a rbac.js en permit(), de modo que los
 * roles se pueden gestionar en caliente desde la API sin reiniciar el servidor.
 *
 * TTL: 5 minutos.  Se invalida manualmente con invalidate() tras cada cambio.
 */

let _cache  = null;
let _expiry = 0;
const TTL   = 5 * 60 * 1000;

async function _load() {
  // Importación deferida: Role no está disponible en el momento que este
  // módulo se evalúa por primera vez (bootstrap circular con sequelize).
  const Role = require('../models/postgres/Role');
  const rows = await Role.findAll({ raw: true });
  return new Map(rows.map(r => [r.value, { label: r.label, permissions: r.permissions }]));
}

async function _get() {
  if (_cache && Date.now() < _expiry) return _cache;
  _cache  = await _load();
  _expiry = Date.now() + TTL;
  return _cache;
}

async function hasPermission(role, permission) {
  const map    = await _get();
  const config = map.get(role);
  if (!config) return false;
  return config.permissions.includes('*') || config.permissions.includes(permission);
}

async function hasAllPermissions(role, perms) {
  const checks = await Promise.all(perms.map(p => hasPermission(role, p)));
  return checks.every(Boolean);
}

/** Invalida el cache — llamar después de crear/editar/eliminar roles. */
function invalidate() {
  _cache  = null;
  _expiry = 0;
}

module.exports = { hasPermission, hasAllPermissions, invalidate };
