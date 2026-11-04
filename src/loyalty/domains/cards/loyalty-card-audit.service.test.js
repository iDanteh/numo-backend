'use strict';

/**
 * loyalty-card-audit.service.test.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Único punto de escritura de LoyaltyCardAuditLog — ver docstring del archivo
 * real sobre por qué `antes`/`despues` se reciben explícitos en vez de leer
 * una instancia de LoyaltyCard en vivo.
 */

jest.mock('../../../shared/models/postgres', () => ({
  LoyaltyCardAuditLog: { create: jest.fn(), findAll: jest.fn() },
}));

const { LoyaltyCardAuditLog } = require('../../../shared/models/postgres');
const { registrarCambioTarjeta, listarHistorialCambios } = require('./loyalty-card-audit.service');

beforeEach(() => {
  jest.clearAllMocks();
});

describe('registrarCambioTarjeta', () => {
  test('inserta una fila con solo los campos que realmente cambiaron', async () => {
    await registrarCambioTarjeta({
      cardId: 1,
      antes: { telefono: '5551234567', nombre: 'Juan', activa: true },
      despues: { telefono: '5559999999', nombre: 'Juan' },
      accion: 'edicion_staff',
      origen: 'numo',
      usuarioId: '7',
      usuarioNombre: 'Admin',
    });

    expect(LoyaltyCardAuditLog.create).toHaveBeenCalledWith({
      cardId: 1,
      accion: 'edicion_staff',
      origen: 'numo',
      usuarioId: '7',
      usuarioNombre: 'Admin',
      cambios: { telefono: { antes: '5551234567', despues: '5559999999' } },
    });
  });

  test('si ningún campo cambió de verdad, no inserta nada y devuelve null', async () => {
    const resultado = await registrarCambioTarjeta({
      cardId: 1,
      antes: { telefono: '5551234567', nombre: 'Juan' },
      despues: { telefono: '5551234567', nombre: 'Juan' },
      accion: 'edicion_staff',
      origen: 'numo',
    });

    expect(resultado).toBeNull();
    expect(LoyaltyCardAuditLog.create).not.toHaveBeenCalled();
  });

  test('usuarioId/usuarioNombre null por default (origen kore sin actor humano)', async () => {
    await registrarCambioTarjeta({
      cardId: 1,
      antes: { activa: true },
      despues: { activa: false },
      accion: 'webhook_kore_baja',
      origen: 'kore',
    });

    expect(LoyaltyCardAuditLog.create).toHaveBeenCalledWith(expect.objectContaining({
      usuarioId: null,
      usuarioNombre: null,
    }));
  });

  test('un campo ausente en "antes" se trata como null (ej. koreClienteId nunca seteado)', async () => {
    await registrarCambioTarjeta({
      cardId: 1,
      antes: {},
      despues: { koreClienteId: 'K1' },
      accion: 'match_kore',
      origen: 'kore',
    });

    expect(LoyaltyCardAuditLog.create).toHaveBeenCalledWith(expect.objectContaining({
      cambios: { koreClienteId: { antes: null, despues: 'K1' } },
    }));
  });

  test('pasa la transacción a LoyaltyCardAuditLog.create cuando se provee', async () => {
    const t = { fake: true };
    await registrarCambioTarjeta({
      cardId: 1,
      antes: { activa: true },
      despues: { activa: false },
      accion: 'webhook_kore_baja',
      origen: 'kore',
      transaction: t,
    });

    expect(LoyaltyCardAuditLog.create).toHaveBeenCalledWith(expect.anything(), { transaction: t });
  });

  test('varios campos cambiando a la vez quedan todos en la misma fila', async () => {
    await registrarCambioTarjeta({
      cardId: 1,
      antes: { telefono: '555', nombre: 'A', activa: true, tarjetaLegada: '1' },
      despues: { telefono: '556', nombre: 'B', activa: false, tarjetaLegada: '2' },
      accion: 'webhook_kore_actualizacion',
      origen: 'kore',
    });

    expect(LoyaltyCardAuditLog.create).toHaveBeenCalledTimes(1);
    expect(LoyaltyCardAuditLog.create).toHaveBeenCalledWith(expect.objectContaining({
      cambios: {
        telefono: { antes: '555', despues: '556' },
        nombre: { antes: 'A', despues: 'B' },
        activa: { antes: true, despues: false },
        tarjetaLegada: { antes: '1', despues: '2' },
      },
    }));
  });
});

describe('listarHistorialCambios', () => {
  test('consulta por cardId ordenado por fecha descendente', async () => {
    LoyaltyCardAuditLog.findAll.mockResolvedValue([{ id: 1 }]);

    const resultado = await listarHistorialCambios(42);

    expect(LoyaltyCardAuditLog.findAll).toHaveBeenCalledWith({
      where: { cardId: 42 },
      order: [['fecha', 'DESC']],
    });
    expect(resultado).toEqual([{ id: 1 }]);
  });
});
