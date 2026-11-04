'use strict';

/**
 * permisos-solo-admin.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Permisos que SOLO un administrador puede dar o quitar (2026-11-04, pedido
 * del usuario para "Fichas de efectivo"): quien tenga `users:manage` sin ser
 * admin no puede ponerlos ni quitarlos en un rol, en los permisos extra de una
 * persona, ni asignarle a alguien un rol que los traiga (o quitárselo), ni
 * borrar el permiso del catálogo. Admin = rol con '*'.
 */

const { PERMISSIONS } = require('../../../shared/config/rbac');
const rbacStore = require('../../../shared/services/rbac-store');
const { ForbiddenError } = require('../../../shared/errors/AppError');

const SOLO_ADMIN_ASIGNA = Object.freeze([PERMISSIONS.REPORTES_FICHAS_EFECTIVO]);

const _tiene = (permisos, p) => Array.isArray(permisos) && (permisos.includes('*') || permisos.includes(p));

async function esAdmin(user) {
  return _tiene(await rbacStore.getPermissions(user?.role), '*');
}

/** Permisos exclusivos que cambian entre `antes` y `despues` (listas de permisos). */
function cambiosSoloAdmin(antes, despues) {
  return SOLO_ADMIN_ASIGNA.filter(p => _tiene(antes, p) !== _tiene(despues, p));
}

/** Lanza 403 si `user` no es admin y el cambio pone o quita un permiso exclusivo. */
async function exigirAdminSiCambia(user, antes, despues) {
  const cambian = cambiosSoloAdmin(antes ?? [], despues ?? []);
  if (cambian.length && !(await esAdmin(user))) {
    throw new ForbiddenError(`Solo un administrador puede asignar o quitar este permiso: ${cambian.join(', ')}.`);
  }
}

module.exports = { SOLO_ADMIN_ASIGNA, esAdmin, cambiosSoloAdmin, exigirAdminSiCambia };
