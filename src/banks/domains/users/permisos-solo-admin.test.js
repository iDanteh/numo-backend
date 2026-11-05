'use strict';

// Permisos que solo un administrador puede dar o quitar (2026-11-04).
jest.mock('../../../shared/services/rbac-store', () => ({ getPermissions: jest.fn() }));
const rbacStore = require('../../../shared/services/rbac-store');
const { exigirAdminSiCambia, cambiosSoloAdmin } = require('./permisos-solo-admin');

const FICHAS = 'reportes:fichas-efectivo';

beforeEach(() => {
  rbacStore.getPermissions.mockImplementation(async role => ({ admin: ['*'], gestor: ['users:manage'] }[role] ?? []));
});

test('detecta cuando se pone o se quita (también por rol con *)', () => {
  expect(cambiosSoloAdmin([], [FICHAS])).toEqual([FICHAS]);
  expect(cambiosSoloAdmin([FICHAS], ['banks:read'])).toEqual([FICHAS]);
  expect(cambiosSoloAdmin(['*'], ['banks:read'])).toEqual([FICHAS, 'reportes:fichas-efectivo:anexo']);
  // Anexo de la ficha (2026-11-05): también solo lo asigna el administrador.
  expect(cambiosSoloAdmin([], ['reportes:fichas-efectivo:anexo'])).toEqual(['reportes:fichas-efectivo:anexo']);
  expect(cambiosSoloAdmin(['banks:read'], ['banks:read', 'visor:read'])).toEqual([]);
  expect(cambiosSoloAdmin([FICHAS], [FICHAS, 'visor:read'])).toEqual([]);
});

test('el administrador sí puede; quien solo gestiona usuarios no', async () => {
  await expect(exigirAdminSiCambia({ role: 'admin' }, [], [FICHAS])).resolves.toBeUndefined();
  await expect(exigirAdminSiCambia({ role: 'gestor' }, [], [FICHAS])).rejects.toThrow(/Solo un administrador/);
  await expect(exigirAdminSiCambia({ role: 'gestor' }, [FICHAS], [])).rejects.toThrow(/Solo un administrador/);
  // Otros permisos los sigue manejando igual.
  await expect(exigirAdminSiCambia({ role: 'gestor' }, [], ['banks:read'])).resolves.toBeUndefined();
});
