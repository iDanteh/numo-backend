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

// ── Alcance de visibilidad de movimientos (para roles restringidos) ───────────

/**
 * Define cómo ve los movimientos identificados un rol sin banks:config.
 * OWN  → solo los que él mismo identificó (identificadoPorUsuario = userId)
 * ALL  → todos los identificados, sin importar quién los identificó
 *
 * Asignar en la definición del rol como `movementScope`. Si un rol no declara
 * este campo, el comportamiento por defecto es OWN (retrocompatibilidad).
 */
const MOVEMENT_SCOPE = Object.freeze({
  OWN: 'own',
  ALL: 'all',
});

// ── Catálogo de permisos ──────────────────────────────────────────────────────

const PERMISSIONS = Object.freeze({
  // Banks — movimientos bancarios
  BANKS_READ:          'banks:read',
  BANKS_IMPORT:        'banks:import',
  BANKS_UPDATE:        'banks:update',
  BANKS_MOVEMENT_EDIT: 'banks:movement:edit',
  BANKS_MOVEMENT_CATEGORIA: 'banks:movement:categoria',
  BANKS_CONFIG:        'banks:config',
  BANKS_RULES:         'banks:rules',

  // Catálogo de cuentas contables
  ACCOUNT_PLAN_READ:   'account-plan:read',
  ACCOUNT_PLAN_WRITE:  'account-plan:write',

  // Pólizas contables
  POLIZAS_READ:        'polizas:read',
  POLIZAS_WRITE:       'polizas:write',
  POLIZAS_ADMIN:       'polizas:admin',

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
  ENTITIES_WRITE:      'entities:write',   // acceso amplio heredado (también usado por schedule.js) — no confundir con los permisos finos de abajo
  ENTITIES_EDIT:       'entities:edit',    // crear/editar datos de la entidad (razón social, config de sync, activar/desactivar)
  ENTITIES_MESSAGE:    'entities:message', // enviar manualmente la alerta de credenciales SAT por correo

  // Administración de usuarios
  USERS_MANAGE:        'users:manage',

  // Operaciones exclusivas de administrador en bancos
  BANKS_ADMIN:         'banks:admin',

  // Registro y eliminación de fichas bancarias (solo contabilidad y admin)
  BANKS_FICHA:         'banks:ficha',

  // Exportar movimientos a Excel (reporte personalizado y vista detalle)
  BANKS_EXPORT:        'banks:export',

  // Exportar movimientos de CUALQUIER usuario (sin restricción de identificadoPor).
  // Roles con este permiso ven todos los depósitos identificados, no solo los propios.
  // Úsalo para roles tipo "jefe de cobranza" que supervisan a su equipo.
  BANKS_EXPORT_ALL:    'banks:export:all',

  // Vincular CxC del ERP directamente a un movimiento bancario (sin flujo de cobro).
  // Permite al área de contabilidad registrar la conciliación sin generar un cobro.
  BANKS_ERP_LINK:      'banks:erp:link',

  // Aplicar cobro bancario desde el modal ERP (genera el recibo de pago en Kore).
  // Exclusivo del área de cobranza; contabilidad usa banks:erp:link en su lugar.
  BANKS_COBRO:         'banks:cobro',

  // Desvincular una CxC ya asociada a un movimiento bancario.
  // Restringido a admin — ningún otro rol puede eliminar una vinculación existente.
  BANKS_ERP_UNLINK:    'banks:erp:unlink',

  // Ver/buscar CxC pendientes del ERP desde el modal "Vincular CxC del ERP"
  // (GET /erp/cuentas-pendientes). Sin este permiso, ningún dato del ERP
  // (montos, folios, clientes) debe ser visible.
  BANKS_ERP_READ:       'banks:erp:read',

  // Usar el switch "Solo anticipos" del modal ERP (filtra por origen=anticipo).
  BANKS_ERP_ANTICIPOS:  'banks:erp:anticipos',

  // Buscar CFDIs (colección cfdis, solo source='ERP') por serie-folio desde el
  // modal ERP. Permiso nuevo, sin asignar a ningún rol todavía — decisión
  // pendiente del usuario, admin lo tiene por el wildcard '*'.
  BANKS_CFDI_READ:      'banks:cfdi:read',

  // Ver la bandeja de auditoría (bitácora, solo lectura) de reversiones de CxC aplicadas por
  // el webhook de Kore. Permiso nuevo, sin asignar a ningún rol todavía — mismo criterio que
  // BANKS_CFDI_READ: decisión pendiente del usuario, admin lo tiene por el wildcard '*'.
  BANKS_ERP_REVERSIONES: 'banks:erp:reversiones',

  // Transferencias entre cajas — matching de "Depósito en efectivo" huérfanos contra
  // transferencias internas de Kore (ver caja-transferencia-*.service.js). Permiso propio
  // y exclusivo (2026-09-03, pedido explícito del usuario): la sección todavía no debe ser
  // visible para nadie más que admin — sin asignar a contabilidad/cobranza todavía, admin
  // lo tiene por el wildcard '*'. Gatea el botón/panel completo (listar, bandeja,
  // sincronizar manual, confirmar match).
  BANKS_TRANSFERENCIAS_CAJA: 'banks:transferencias-caja',

  // Configuraciones Globales (runtime, ver global-config.service.js) — dos niveles a propósito,
  // mismo patrón que banks:erp:read/link/unlink. CONFIG_MANAGE: ver/crear secciones, ver
  // valores (secretos enmascarados), editar cualquier valor. CONFIG_SECRETS_REVEAL: además,
  // desenmascarar el valor real de un secreto puntual. Permisos nuevos, sin asignar a ningún
  // rol todavía — admin los tiene por el wildcard '*'.
  CONFIG_MANAGE:         'config:manage',
  CONFIG_SECRETS_REVEAL: 'config:secrets:reveal',
});

// ── Roles y sus permisos ──────────────────────────────────────────────────────

const ROLES = Object.freeze({
  admin: {
    label:       'Administrador',
    permissions: ['*'],                // acceso total (incluye banks:admin y cualquier otro permiso)
  },

  contabilidad: {
    label: 'Contabilidad',
    // Nota: seed.js NO sincroniza permisos nuevos a roles que ya existen en Postgres —
    // tras desplegar BANKS_ERP_READ/BANKS_ERP_ANTICIPOS hay que asignarlos a mano vía
    // /users → Roles, o este rol queda con 403 en /erp/cuentas-pendientes.
    permissions: [
      PERMISSIONS.BANKS_READ,
      PERMISSIONS.BANKS_IMPORT,
      PERMISSIONS.BANKS_UPDATE,
      PERMISSIONS.BANKS_MOVEMENT_EDIT,
      PERMISSIONS.BANKS_MOVEMENT_CATEGORIA,
      PERMISSIONS.BANKS_CONFIG,
      PERMISSIONS.BANKS_RULES,
      PERMISSIONS.BANKS_FICHA,
      PERMISSIONS.BANKS_EXPORT,
      PERMISSIONS.BANKS_EXPORT_ALL,
      PERMISSIONS.BANKS_ERP_LINK,
      PERMISSIONS.BANKS_ERP_READ,
      PERMISSIONS.BANKS_ERP_ANTICIPOS,
      PERMISSIONS.ACCOUNT_PLAN_READ,
      PERMISSIONS.ACCOUNT_PLAN_WRITE,
      PERMISSIONS.POLIZAS_READ,
      PERMISSIONS.POLIZAS_WRITE,
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
    movementScope: MOVEMENT_SCOPE.OWN, // cambia a ALL para ver todos los identificados
    // Nota: seed.js NO sincroniza permisos nuevos a roles que ya existen en Postgres —
    // tras desplegar BANKS_ERP_READ/BANKS_ERP_ANTICIPOS hay que asignarlos a mano vía
    // /users → Roles, o este rol queda con 403 en /erp/cuentas-pendientes.
    permissions: [
      PERMISSIONS.BANKS_READ,
      PERMISSIONS.BANKS_UPDATE,        // puede cambiar estado de movimientos
      PERMISSIONS.BANKS_EXPORT,        // puede exportar Excel (con vista restringida)
      PERMISSIONS.BANKS_FICHA,         // puede identificar movimientos a partir de una ficha
      // PERMISSIONS.BANKS_COBRO,         // aplica cobros desde el modal ERP (genera recibo en Kore)
      PERMISSIONS.BANKS_ERP_LINK,
      PERMISSIONS.BANKS_ERP_READ,
      PERMISSIONS.BANKS_ERP_ANTICIPOS,
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

/**
 * Devuelve el alcance de visibilidad de movimientos para un rol restringido.
 * Roles sin `movementScope` definido usan OWN como comportamiento por defecto.
 * @param {string} role
 * @returns {'own' | 'all'}
 */
function getMovementScope(role) {
  return ROLES[role]?.movementScope ?? MOVEMENT_SCOPE.OWN;
}

module.exports = {
  PERMISSIONS,
  ROLES,
  MOVEMENT_SCOPE,
  hasPermission,
  hasAllPermissions,
  listRoles,
  listRolesWithPermissions,
  getMovementScope,
};
