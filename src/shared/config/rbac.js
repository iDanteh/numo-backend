'use strict';

/**
 * shared/config/rbac.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de control de acceso basado en roles y permisos (RBAC).
 *
 * Cómo agregar un nuevo rol:
 *   1. Añade una entrada en ROLES con su label y array de permissions.
 *   2. Listo — sin tocar ningún archivo de rutas.
 *
 * Cómo agregar un nuevo permiso:
 *   1. Define la constante en PERMISSIONS.
 *   2. Asígnala a los roles correspondientes en ROLES.
 *   3. Usa permit('nuevo:permiso') en la ruta que quieras proteger.
 *
 * Wildcard '*' en permissions concede acceso total (solo admin).
 */

// ── Catálogo de permisos ──────────────────────────────────────────────────────

const PERMISSIONS = Object.freeze({
  // Banks — movimientos bancarios
  BANKS_READ:          'banks:read',
  BANKS_IMPORT:        'banks:import',
  BANKS_UPDATE:        'banks:update',
  BANKS_CONFIG:        'banks:config',
  BANKS_RULES:         'banks:rules',

  // Catálogo de cuentas contables
  ACCOUNT_PLAN_READ:   'account-plan:read',
  ACCOUNT_PLAN_WRITE:  'account-plan:write',

  // Solicitudes de cobranza
  COLLECTIONS_READ:    'collections:read',
  COLLECTIONS_WRITE:   'collections:write',

  // Integración ERP
  ERP_MANAGE:          'erp:manage',

  // Visor — CFDIs, comparaciones, discrepancias
  VISOR_READ:          'visor:read',
  VISOR_WRITE:         'visor:write',
  VISOR_SAT:           'visor:sat',
  VISOR_REPORTS:       'visor:reports',

  // Google Drive
  DRIVE_READ:          'drive:read',
  DRIVE_IMPORT:        'drive:import',

  // Entidades fiscales
  ENTITIES_READ:       'entities:read',
  ENTITIES_WRITE:      'entities:write',

  // Administración de usuarios
  USERS_MANAGE:        'users:manage',
});

// ── Roles y sus permisos ──────────────────────────────────────────────────────

const ROLES = Object.freeze({
  admin: {
    label:       'Administrador',
    permissions: ['*'],                // acceso total
  },

  contabilidad: {
    label: 'Contabilidad',
    permissions: [
      PERMISSIONS.BANKS_READ,
      PERMISSIONS.BANKS_IMPORT,
      PERMISSIONS.BANKS_UPDATE,
      PERMISSIONS.BANKS_CONFIG,
      PERMISSIONS.BANKS_RULES,
      PERMISSIONS.ACCOUNT_PLAN_READ,
      PERMISSIONS.ACCOUNT_PLAN_WRITE,
      PERMISSIONS.COLLECTIONS_READ,
      PERMISSIONS.COLLECTIONS_WRITE,
      PERMISSIONS.ERP_MANAGE,
      PERMISSIONS.VISOR_READ,
      PERMISSIONS.VISOR_WRITE,
      PERMISSIONS.VISOR_SAT,
      PERMISSIONS.VISOR_REPORTS,
      PERMISSIONS.DRIVE_READ,
      PERMISSIONS.DRIVE_IMPORT,
      PERMISSIONS.ENTITIES_READ,
    ],
  },

  cobranza: {
    label: 'Cobranza',
    permissions: [
      PERMISSIONS.BANKS_READ,
      PERMISSIONS.BANKS_UPDATE,        // puede cambiar estado de movimientos
      PERMISSIONS.COLLECTIONS_READ,
      PERMISSIONS.COLLECTIONS_WRITE,
    ],
  },

  tienda: {
    label: 'Tienda',
    permissions: [
      PERMISSIONS.ACCOUNT_PLAN_READ,
      PERMISSIONS.VISOR_READ,
      PERMISSIONS.COLLECTIONS_READ,
    ],
  },
});

// ── Helpers ───────────────────────────────────────────────────────────────────

/**
 * Devuelve true si el rol tiene el permiso indicado.
 * Roles con wildcard '*' siempre devuelven true.
 * @param {string} role
 * @param {string} permission
 * @returns {boolean}
 */
function hasPermission(role, permission) {
  const roleConfig = ROLES[role];
  if (!roleConfig) return false;
  const { permissions } = roleConfig;
  return permissions.includes('*') || permissions.includes(permission);
}

/**
 * Devuelve true si el rol tiene TODOS los permisos indicados.
 * @param {string} role
 * @param {string[]} permissions
 * @returns {boolean}
 */
function hasAllPermissions(role, permissions) {
  return permissions.every((p) => hasPermission(role, p));
}

/**
 * Lista todos los roles disponibles (sin exponer permisos internos).
 * @returns {{ value: string, label: string }[]}
 */
function listRoles() {
  return Object.entries(ROLES).map(([value, { label }]) => ({ value, label }));
}

/**
 * Lista todos los roles con sus permisos (para la interfaz de administración).
 * @returns {{ value: string, label: string, permissions: string[] }[]}
 */
function listRolesWithPermissions() {
  return Object.entries(ROLES).map(([value, { label, permissions }]) => ({
    value,
    label,
    permissions: [...permissions],
  }));
}

module.exports = { PERMISSIONS, ROLES, hasPermission, hasAllPermissions, listRoles, listRolesWithPermissions };
