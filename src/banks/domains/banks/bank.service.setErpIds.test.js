'use strict';

// bank.service.setErpIds.test.js — setErpIds(): función EXISTENTE, modificada
// para multi-bank-movement (D5: opts.session + emit diferido). Approval tests
// primero (comportamiento actual, SIN session, capturado ANTES de leer que ya
// estaba modificado) + triangulación del comportamiento nuevo (CON session).
//
// bank.service.js es un módulo grande con muchas dependencias transitivas —
// se mockean solo las 3 que setErpIds toca: BankMovement.model, rbac-store,
// shared/socket. aplicarLogicaErp es lógica interna PURA del propio archivo,
// no se mockea (corre real, sobre erpLinks vacíos -> resultado determinista).
jest.mock('./BankMovement.model');
jest.mock('../../../shared/services/rbac-store');
jest.mock('../../shared/socket');

const BankMovement = require('./BankMovement.model');
const rbacStore     = require('../../../shared/services/rbac-store');
const { emitToBanco } = require('../../shared/socket');
const bankService    = require('./bank.service');

// Mongoose Query real es thenable Y chainable (.session() devuelve el mismo
// query) — se replica ambas propiedades para que
// `session ? movQuery.session(session) : movQuery` funcione en cualquiera de
// los 2 casos, igual que el código de producción espera.
function fakeQuery(mov) {
  const q = { session: jest.fn(() => q), then: (resolve) => resolve(mov) };
  return q;
}

function fakeMov(overrides = {}) {
  return {
    _id: 'mov-1', banco: 'BBVA', erpIds: [], erpLinks: [], identificadoPor: [], status: 'no_identificado',
    save: jest.fn().mockResolvedValue(undefined),
    ...overrides,
  };
}

beforeEach(() => {
  jest.clearAllMocks();
  rbacStore.hasPermission = jest.fn().mockResolvedValue(true);
});

describe('setErpIds — approval (SIN session, comportamiento actual, sin cambios)', () => {
  test('sin opts: mov.save() sin argumentos de sesión, emitToBanco se llama de inmediato', async () => {
    const mov = fakeMov();
    BankMovement.findById.mockReturnValue(fakeQuery(mov));

    const updated = await bankService.setErpIds('mov-1', [{ erpId: 'CXC-1', saldoActual: 0 }], { _id: 'user-1', role: 'admin' });

    expect(mov.save).toHaveBeenCalledWith(undefined);
    expect(emitToBanco).toHaveBeenCalledTimes(1);
    expect(emitToBanco).toHaveBeenCalledWith('BBVA', 'bank:movement:updated', updated);
    expect(updated.erpIds).toEqual(['CXC-1']);
  });

  // Pedido 2026-08-10: origen ('cfdi_liquidado') debe sobrevivir a setErpIds igual que
  // tipoPago/serie — antes se descartaba en silencio (whitelist explícito de cleanLinks).
  test('erpLinks con origen: se persiste tal cual en el link guardado', async () => {
    const mov = fakeMov();
    BankMovement.findById.mockReturnValue(fakeQuery(mov));

    const updated = await bankService.setErpIds(
      'mov-1',
      [{ erpId: 'CXC-1', saldoActual: 0, origen: 'cfdi_liquidado' }],
      { _id: 'user-1', role: 'admin' },
    );

    expect(updated.erpLinks[0].origen).toBe('cfdi_liquidado');
  });

  test('erpLinks sin origen: se guarda como null (no undefined)', async () => {
    const mov = fakeMov();
    BankMovement.findById.mockReturnValue(fakeQuery(mov));

    const updated = await bankService.setErpIds(
      'mov-1',
      [{ erpId: 'CXC-1', saldoActual: 0 }],
      { _id: 'user-1', role: 'admin' },
    );

    expect(updated.erpLinks[0].origen).toBeNull();
  });
});

describe('setErpIds — primeraIdentificacionAt/primeraIdentificacionPor (indicador de tiempo de identificación)', () => {
  test('primera vez que el movimiento queda identificado: setea primeraIdentificacionAt/primeraIdentificacionPor', async () => {
    const mov = fakeMov({ primeraIdentificacionAt: null, primeraIdentificacionPor: null });
    BankMovement.findById.mockReturnValue(fakeQuery(mov));

    await bankService.setErpIds(
      'mov-1',
      [{ erpId: 'CXC-1', saldoActual: 0 }],
      { _id: 'user-1', role: 'admin', nombre: 'Usuario Uno' },
    );

    expect(mov.status).toBe('identificado');
    expect(mov.primeraIdentificacionAt).toBeInstanceOf(Date);
    expect(mov.primeraIdentificacionPor).toEqual({ userId: 'user-1', nombre: 'Usuario Uno' });
  });

  test('ya tenía primeraIdentificacionAt: no se sobreescribe (inmutable)', async () => {
    const fechaOriginal = new Date('2026-01-01T00:00:00.000Z');
    const porOriginal   = { userId: 'user-999', nombre: 'Otro Usuario' };
    const mov = fakeMov({ primeraIdentificacionAt: fechaOriginal, primeraIdentificacionPor: porOriginal });
    BankMovement.findById.mockReturnValue(fakeQuery(mov));

    await bankService.setErpIds(
      'mov-1',
      [{ erpId: 'CXC-1', saldoActual: 0 }],
      { _id: 'user-1', role: 'admin', nombre: 'Usuario Uno' },
    );

    expect(mov.status).toBe('identificado');
    expect(mov.primeraIdentificacionAt).toBe(fechaOriginal);
    expect(mov.primeraIdentificacionPor).toBe(porOriginal);
  });
});

describe('setErpIds — opts.session (multi-bank-movement, D5, comportamiento nuevo)', () => {
  test('con session: BankMovement.findById().session(session), mov.save({session}), emit DIFERIDO (no se llama)', async () => {
    const mov = fakeMov();
    const query = fakeQuery(mov);
    BankMovement.findById.mockReturnValue(query);
    const sesionFalsa = { id: 'sesion-falsa' };

    const updated = await bankService.setErpIds('mov-1', [{ erpId: 'CXC-1', saldoActual: 0 }], { _id: 'user-1', role: 'admin' }, { session: sesionFalsa });

    expect(query.session).toHaveBeenCalledWith(sesionFalsa);
    expect(mov.save).toHaveBeenCalledWith({ session: sesionFalsa });
    expect(emitToBanco).not.toHaveBeenCalled(); // el caller (identificar()) emite tras el commit
    expect(updated.erpIds).toEqual(['CXC-1']); // el payload SÍ se devuelve, para que el caller lo emita después
  });
});
