'use strict';

// bank.service.ficha.test.js — setFicha()/deleteFicha() (BankMovement.model.js `ficha`,
// respaldo documental cargado a mano por el contador). Pedido explícito del usuario
// 2026-09-03: ambas funciones deben emitir 'bank:ficha-pendiente:changed' (emitToAll,
// cross-banco) SOLO cuando el movimiento tiene un erpLink origen:'transferencia-caja' (ver
// caja-transferencia-confirm.service.js) — cualquier otro movimiento con ficha (la mayoría)
// no le interesa a esa bandeja, y no debe disparar un refresco irrelevante.
//
// Mismo patrón que bank.service.setErpIds.test.js: solo se mockean las dependencias con
// I/O (BankMovement.model, shared/socket) — resolvePrimeraIdentificacion/aplicarLogicaErp
// son lógica interna pura, corren reales.
jest.mock('./BankMovement.model');
jest.mock('../../shared/socket');

const BankMovement = require('./BankMovement.model');
const { emitToBanco, emitToAll } = require('../../shared/socket');
const bankService = require('./bank.service');

function fakeMov(overrides = {}) {
  return {
    _id: 'mov-1', banco: 'BBVA', ficha: null, fichaBy: null, fichaNombre: null, fichaAt: null,
    status: 'no_identificado', erpLinks: [], primeraIdentificacionAt: null, primeraIdentificacionPor: null,
    save: jest.fn(function () { return Promise.resolve(this); }),
    ...overrides,
  };
}

const USER = { _id: 'user-1', nombre: 'Ana', role: 'contabilidad' };

beforeEach(() => {
  jest.clearAllMocks();
});

describe('setFicha', () => {
  test('movimiento SIN erpLink transferencia-caja (caso normal, la mayoría): NO emite bank:ficha-pendiente:changed', async () => {
    const mov = fakeMov({ erpLinks: [] });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);

    await bankService.setFicha('mov-1', '00123', USER);

    expect(emitToBanco).toHaveBeenCalledTimes(1);
    expect(emitToAll).not.toHaveBeenCalled();
  });

  test('movimiento CON erpLink origen:transferencia-caja: SÍ emite bank:ficha-pendiente:changed con el movementId', async () => {
    const mov = fakeMov({ erpLinks: [{ erpId: 'CAJA-abc123', origen: 'transferencia-caja' }] });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);

    await bankService.setFicha('mov-1', '00123', USER);

    expect(emitToAll).toHaveBeenCalledTimes(1);
    expect(emitToAll).toHaveBeenCalledWith('bank:ficha-pendiente:changed', { movementId: 'mov-1' });
  });

  test('movimiento con erpLink de OTRO origen (no transferencia-caja): NO emite', async () => {
    const mov = fakeMov({ erpLinks: [{ erpId: 'CXC-1', origen: null }] });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);

    await bankService.setFicha('mov-1', '00123', USER);

    expect(emitToAll).not.toHaveBeenCalled();
  });

  test('movimiento inexistente: NotFoundError, no emite nada', async () => {
    BankMovement.findById = jest.fn().mockResolvedValue(null);
    await expect(bankService.setFicha('mov-x', '00123', USER)).rejects.toThrow('Movimiento');
    expect(emitToBanco).not.toHaveBeenCalled();
    expect(emitToAll).not.toHaveBeenCalled();
  });

  test('movimiento que YA tiene ficha: ConflictError, no emite nada', async () => {
    const mov = fakeMov({ ficha: '00099', erpLinks: [{ erpId: 'CAJA-abc123', origen: 'transferencia-caja' }] });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);

    await expect(bankService.setFicha('mov-1', '00123', USER)).rejects.toThrow(/ya tiene una ficha/);
    expect(emitToAll).not.toHaveBeenCalled();
  });
});

describe('deleteFicha', () => {
  test('movimiento SIN erpLink transferencia-caja: NO emite bank:ficha-pendiente:changed', async () => {
    const mov = fakeMov({ ficha: '00123', fichaBy: 'user-1', erpLinks: [] });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);

    await bankService.deleteFicha('mov-1', USER);

    expect(emitToBanco).toHaveBeenCalledTimes(1);
    expect(emitToAll).not.toHaveBeenCalled();
  });

  test('movimiento CON erpLink origen:transferencia-caja: SÍ emite bank:ficha-pendiente:changed con el movementId', async () => {
    const mov = fakeMov({
      ficha: '00123', fichaBy: 'user-1',
      erpLinks: [{ erpId: 'CAJA-abc123', origen: 'transferencia-caja' }],
    });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);

    await bankService.deleteFicha('mov-1', USER);

    expect(emitToAll).toHaveBeenCalledTimes(1);
    expect(emitToAll).toHaveBeenCalledWith('bank:ficha-pendiente:changed', { movementId: 'mov-1' });
  });

  test('movimiento sin ficha registrada: BadRequestError, no emite nada', async () => {
    const mov = fakeMov({ ficha: null });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);

    await expect(bankService.deleteFicha('mov-1', USER)).rejects.toThrow(/no tiene ficha registrada/);
    expect(emitToAll).not.toHaveBeenCalled();
  });

  test('usuario sin permiso (no admin, no autor): ForbiddenError, no emite nada', async () => {
    const mov = fakeMov({ ficha: '00123', fichaBy: 'otro-user', erpLinks: [{ erpId: 'CAJA-abc123', origen: 'transferencia-caja' }] });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);

    await expect(bankService.deleteFicha('mov-1', USER)).rejects.toThrow(/Solo el usuario/);
    expect(emitToAll).not.toHaveBeenCalled();
  });
});
