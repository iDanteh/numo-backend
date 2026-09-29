'use strict';

// netpay-evaluacion.service.test.js — netpay-matching-v2 (design.md "Technical Approach"):
// evaluarRango() reemplaza el candidate picker manual — decide automáticamente CADA bucket
// (terminalID, dia, bucket) y persiste SIEMPRE, incluida discrepancia (a diferencia de v1,
// donde "pendiente" nunca tenía documento propio). Zero commission-gap tolerance: solo
// ERP_TOLERANCE ($1 MXN) separa confirmado_automatico de discrepancia para un bucket
// 'general'. netpay-match.service.js NO se mockea completo — se usan sus funciones puras
// reales (_agruparPorTerminalDiaYMarca, _montosIguales), solo sus dependencias externas
// (global-config, netpay-transacciones) están mockeadas vía los otros jest.mock().
jest.mock('../banks/BankMovement.model');
jest.mock('./NetpayMatch.model');
jest.mock('./netpay-transacciones.service');
jest.mock('../../../shared/services/global-config.service');
jest.mock('../../shared/socket', () => ({ emitToBanco: jest.fn(), emitToAll: jest.fn() }));
// ERP_TOLERANCE se toma REAL (jest.requireActual) — netpay-match.service.js#_montosIguales
// la necesita para decidir "dentro de tolerancia"; solo setErpIds se mockea.
jest.mock('../banks/bank.service', () => {
  const real = jest.requireActual('../banks/bank.service');
  return { setErpIds: jest.fn(), ERP_TOLERANCE: real.ERP_TOLERANCE };
});

const BankMovement = require('../banks/BankMovement.model');
const NetpayMatch = require('./NetpayMatch.model');
const globalConfigService = require('../../../shared/services/global-config.service');
const { consultarTransaccionesNetpay } = require('./netpay-transacciones.service');
const { setErpIds } = require('../banks/bank.service');
const { emitToBanco, emitToAll } = require('../../shared/socket');
const { ConflictError } = require('../../shared/errors/AppError');
const {
  evaluarRango, _debeReevaluarse, _evaluarBucket, _resolverAmbiguedadEntreGrupos, _erpIdAutomatico,
} = require('./netpay-evaluacion.service');

const TERMINAL = '2840403056';

function fakeFind(result) {
  return { lean: jest.fn().mockResolvedValue(result) };
}

function t(overrides = {}) {
  return {
    amount: 100, commission: 10, almacen: 'A0', terminalID: TERMINAL,
    transactionDate: '2026-09-10T14:00:00Z', cardTypeName: 'VISA',
    ...overrides,
  };
}

beforeEach(() => {
  jest.clearAllMocks();
  globalConfigService.getValue.mockImplementation((section, key) => {
    if (key === 'NETPAY_DATE_WINDOW_DAYS') return Promise.resolve('2');
    if (key === 'NETPAY_MARCAS_DIFERIDAS') return Promise.resolve('AMEX');
    return Promise.reject(new Error(`No existe la configuración ${section}.${key}`));
  });
  NetpayMatch.find = jest.fn(() => fakeFind([]));
  NetpayMatch.findOneAndUpdate = jest.fn().mockResolvedValue({ estatusMatch: 'discrepancia' });
  BankMovement.find = jest.fn(() => fakeFind([]));
});

describe('_debeReevaluarse (pure)', () => {
  test('sin documento existente: sí se reevalúa', () => {
    expect(_debeReevaluarse(null)).toBe(true);
  });

  test.each(['resuelto_por_reporte', 'rechazado', 'resuelto_manual'])(
    'estatusMatch=%s: NUNCA se reevalúa',
    (estatusMatch) => {
      expect(_debeReevaluarse({ estatusMatch })).toBe(false);
    },
  );

  test.each(['revertido', 'reporte_revertido'])(
    'discrepancia con motivo=%s: NUNCA se reevalúa (nunca se re-sube automáticamente tras un revert)',
    (motivoDiscrepancia) => {
      expect(_debeReevaluarse({ estatusMatch: 'discrepancia', motivoDiscrepancia })).toBe(false);
    },
  );

  test.each(['sin_candidato', 'multiples_candidatos', 'candidato_en_conflicto'])(
    'discrepancia con motivo=%s (no revertido): SÍ se reevalúa',
    (motivoDiscrepancia) => {
      expect(_debeReevaluarse({ estatusMatch: 'discrepancia', motivoDiscrepancia })).toBe(true);
    },
  );

  test('pendiente_por_marca: sí se reevalúa (puede cerrar por reporte o seguir pendiente)', () => {
    expect(_debeReevaluarse({ estatusMatch: 'pendiente_por_marca' })).toBe(true);
  });
});

describe('_evaluarBucket (pure)', () => {
  const grupoGeneral = { bucket: 'general' };
  const grupoAmex = { bucket: 'AMEX' };

  test('bucket no-general (marca diferida): SIEMPRE pendiente_por_marca, sin importar candidatos', () => {
    expect(_evaluarBucket(grupoAmex, [])).toEqual({ estatusMatch: 'pendiente_por_marca', motivoDiscrepancia: null });
    expect(_evaluarBucket(grupoAmex, [{ _id: 'm1' }])).toEqual({ estatusMatch: 'pendiente_por_marca', motivoDiscrepancia: null });
  });

  test('bucket general, 0 candidatos: discrepancia/sin_candidato', () => {
    expect(_evaluarBucket(grupoGeneral, [])).toEqual({ estatusMatch: 'discrepancia', motivoDiscrepancia: 'sin_candidato' });
  });

  test('bucket general, 2+ candidatos: discrepancia/multiples_candidatos', () => {
    expect(_evaluarBucket(grupoGeneral, [{ _id: 'm1' }, { _id: 'm2' }]))
      .toEqual({ estatusMatch: 'discrepancia', motivoDiscrepancia: 'multiples_candidatos' });
  });

  test('bucket general, exactamente 1 candidato: confirmado_automatico', () => {
    expect(_evaluarBucket(grupoGeneral, [{ _id: 'm1' }])).toEqual({ estatusMatch: 'confirmado_automatico', motivoDiscrepancia: null });
  });

  test('bucket general, 1 candidato pero conflicto=true (ambigüedad entre grupos): discrepancia/candidato_en_conflicto', () => {
    expect(_evaluarBucket(grupoGeneral, [{ _id: 'm1' }], true))
      .toEqual({ estatusMatch: 'discrepancia', motivoDiscrepancia: 'candidato_en_conflicto' });
  });
});

describe('_resolverAmbiguedadEntreGrupos (pure)', () => {
  test('un mismo movimiento candidato de 2 grupos: ambos quedan en conflicto, candidatos vaciados', () => {
    const mov = { _id: 'mov-1' };
    const entrada = [
      { grupo: { terminalID: 'T1' }, candidatos: [mov] },
      { grupo: { terminalID: 'T2' }, candidatos: [mov] },
    ];
    const resultado = _resolverAmbiguedadEntreGrupos(entrada);
    expect(resultado.every(r => r.conflicto)).toBe(true);
    expect(resultado.every(r => r.candidatos.length === 0)).toBe(true);
  });

  test('candidatos exclusivos (sin solapar): sin conflicto, candidatos intactos', () => {
    const entrada = [
      { grupo: { terminalID: 'T1' }, candidatos: [{ _id: 'mov-1' }] },
      { grupo: { terminalID: 'T2' }, candidatos: [{ _id: 'mov-2' }] },
    ];
    const resultado = _resolverAmbiguedadEntreGrupos(entrada);
    expect(resultado.every(r => !r.conflicto)).toBe(true);
    expect(resultado[0].candidatos).toEqual([{ _id: 'mov-1' }]);
  });

  test('un grupo con 0 o 2+ candidatos nunca contamina el conteo de ambigüedad de otro grupo', () => {
    const entrada = [
      { grupo: { terminalID: 'T1' }, candidatos: [] },
      { grupo: { terminalID: 'T2' }, candidatos: [{ _id: 'mov-1' }, { _id: 'mov-2' }] },
      { grupo: { terminalID: 'T3' }, candidatos: [{ _id: 'mov-3' }] },
    ];
    const resultado = _resolverAmbiguedadEntreGrupos(entrada);
    expect(resultado[2].conflicto).toBe(false);
    expect(resultado[2].candidatos).toEqual([{ _id: 'mov-3' }]);
  });
});

describe('_erpIdAutomatico (pure)', () => {
  test('bucket general: NETPAY-<terminalID>-<YYYY-MM-DD>, sin sufijo (idéntico a v1)', () => {
    expect(_erpIdAutomatico(TERMINAL, new Date('2026-09-10T00:00:00.000Z'), 'general')).toBe(`NETPAY-${TERMINAL}-2026-09-10`);
  });

  test('bucket de marca diferida: sufijo -<bucket> (nunca colisiona con el general del mismo día)', () => {
    expect(_erpIdAutomatico(TERMINAL, new Date('2026-09-10T00:00:00.000Z'), 'AMEX')).toBe(`NETPAY-${TERMINAL}-2026-09-10-AMEX`);
  });
});

describe('evaluarRango (integración, dependencias mockeadas)', () => {
  // Caso real del proposal: Kore dice 60,758.23 (48,099.79 general + 12,658.44 AMEX). El
  // depósito real en BBVA es 47,681.40 — SOLO si la comisión de Kore en el bucket general
  // cuadra contra ESE monto exacto (± $1) hay confirmado_automatico; el AMEX SIEMPRE queda
  // pendiente_por_marca, independiente de si el general cuadra o no.
  test('fixture $60,758.23: bucket general (comisión Kore correcta) -> confirmado_automatico; bucket AMEX -> pendiente_por_marca', async () => {
    consultarTransaccionesNetpay.mockResolvedValue({
      transacciones: [
        t({ amount: 48099.79, commission: 418.39, cardTypeName: 'VISA' }),
        t({ amount: 12658.44, commission: 513.932664, cardTypeName: 'AMEX' }),
      ],
    });
    const movGeneral = { _id: 'mov-general', banco: 'BBVA', deposito: 47681.40 };
    BankMovement.find = jest.fn(() => fakeFind([movGeneral]));
    setErpIds.mockResolvedValue({ _id: 'mov-general', banco: 'BBVA', erpIds: [`NETPAY-${TERMINAL}-2026-09-10`] });
    NetpayMatch.findOneAndUpdate = jest.fn().mockImplementation((filtro, update) => Promise.resolve({ ...filtro, ...update.$set }));

    const { evaluados } = await evaluarRango({ dateFrom: '2026-09-10', dateTo: '2026-09-10', terminalID: TERMINAL });

    expect(evaluados).toHaveLength(2);
    const general = evaluados.find(e => e.bucket === 'general');
    const amex = evaluados.find(e => e.bucket === 'AMEX');
    expect(general.estatusMatch).toBe('confirmado_automatico');
    expect(amex.estatusMatch).toBe('pendiente_por_marca');

    expect(setErpIds).toHaveBeenCalledWith(
      'mov-general',
      [expect.objectContaining({ erpId: `NETPAY-${TERMINAL}-2026-09-10`, origen: 'netpay-matching' })],
      expect.objectContaining({ role: 'admin' }),
      { guardSinVinculos: true },
    );
    expect(emitToBanco).toHaveBeenCalled();
    expect(emitToAll).toHaveBeenCalledWith('bank:ficha-pendiente:changed', { movementId: 'mov-general' });
  });

  // design.md "Data Model": NetpayMatch.snapshot.folios[] — netpay-reporte.service.js
  // necesita el detalle por transacción (orderId/referencia/marca/monto/comision) para
  // saber si un reporte posterior cubre TODOS los folios de un bucket antes de cerrarlo.
  test('pendiente_por_marca (AMEX): snapshot.folios trae el detalle por transacción (orderId/referencia/marca/monto/comision)', async () => {
    consultarTransaccionesNetpay.mockResolvedValue({
      transacciones: [t({
        amount: 12658.44, commission: 513.932664, cardTypeName: 'AMEX',
        orderID: '260924181626-2840746396783601', folio: 'F20260924-00311',
      })],
    });
    let guardado = null;
    NetpayMatch.findOneAndUpdate = jest.fn().mockImplementation((filtro, update) => {
      guardado = { ...filtro, ...update.$set };
      return Promise.resolve(guardado);
    });

    await evaluarRango({ terminalID: TERMINAL });

    expect(guardado.snapshot.folios).toEqual([{
      orderId: '260924181626-2840746396783601', referencia: 'F20260924-00311', marca: 'AMEX',
      monto: 12658.44, comision: 513.932664,
    }]);
  });

  test('0 candidatos BBVA para el bucket general: discrepancia/sin_candidato, setErpIds NUNCA se llama', async () => {
    consultarTransaccionesNetpay.mockResolvedValue({ transacciones: [t({ amount: 300, commission: 20 })] });
    BankMovement.find = jest.fn(() => fakeFind([]));
    NetpayMatch.findOneAndUpdate = jest.fn().mockImplementation((filtro, update) => Promise.resolve({ ...filtro, ...update.$set }));

    const { evaluados } = await evaluarRango({ terminalID: TERMINAL });

    expect(evaluados[0].estatusMatch).toBe('discrepancia');
    expect(evaluados[0].motivoDiscrepancia).toBe('sin_candidato');
    expect(setErpIds).not.toHaveBeenCalled();
  });

  test('2 candidatos BBVA dentro de tolerancia para el mismo bucket general: discrepancia/multiples_candidatos', async () => {
    consultarTransaccionesNetpay.mockResolvedValue({ transacciones: [t({ amount: 300, commission: 20 })] });
    BankMovement.find = jest.fn(() => fakeFind([
      { _id: 'mov-1', banco: 'BBVA', deposito: 280 },
      { _id: 'mov-2', banco: 'BBVA', deposito: 280.5 },
    ]));
    NetpayMatch.findOneAndUpdate = jest.fn().mockImplementation((filtro, update) => Promise.resolve({ ...filtro, ...update.$set }));

    const { evaluados } = await evaluarRango({ terminalID: TERMINAL });

    expect(evaluados[0].estatusMatch).toBe('discrepancia');
    expect(evaluados[0].motivoDiscrepancia).toBe('multiples_candidatos');
    expect(setErpIds).not.toHaveBeenCalled();
  });

  // Zero-tolerance (proposal/design.md): el "commission gap" de Kore NUNCA se perdona más
  // allá de ERP_TOLERANCE ($1) — un neto que difiere por más de eso SIEMPRE es discrepancia.
  test('candidato fuera de ERP_TOLERANCE (comisión de Kore errónea): discrepancia/sin_candidato, NUNCA confirmado_automatico', async () => {
    consultarTransaccionesNetpay.mockResolvedValue({ transacciones: [t({ amount: 300, commission: 20 })] }); // neto=280
    BankMovement.find = jest.fn(() => fakeFind([{ _id: 'mov-1', banco: 'BBVA', deposito: 250 }])); // fuera de $1
    NetpayMatch.findOneAndUpdate = jest.fn().mockImplementation((filtro, update) => Promise.resolve({ ...filtro, ...update.$set }));

    const { evaluados } = await evaluarRango({ terminalID: TERMINAL });

    expect(evaluados[0].estatusMatch).toBe('discrepancia');
    expect(setErpIds).not.toHaveBeenCalled();
  });

  test('setErpIds lanza ConflictError (guardSinVinculos, race real): discrepancia/candidato_en_conflicto, no revienta', async () => {
    consultarTransaccionesNetpay.mockResolvedValue({ transacciones: [t({ amount: 300, commission: 20 })] });
    BankMovement.find = jest.fn(() => fakeFind([{ _id: 'mov-1', banco: 'BBVA', deposito: 280 }]));
    setErpIds.mockRejectedValue(new ConflictError('El movimiento mov-1 ya fue vinculado por otro proceso concurrente.'));
    NetpayMatch.findOneAndUpdate = jest.fn().mockImplementation((filtro, update) => Promise.resolve({ ...filtro, ...update.$set }));

    const { evaluados } = await evaluarRango({ terminalID: TERMINAL });

    expect(evaluados[0].estatusMatch).toBe('discrepancia');
    expect(evaluados[0].motivoDiscrepancia).toBe('candidato_en_conflicto');
  });

  // design.md "Revert (unlink)": "It is never auto-upgraded again" — un bucket YA
  // revertido (discrepancia/revertido) nunca vuelve a evaluarse, ni siquiera si ahora
  // habría un candidato exacto disponible.
  test('bucket ya revertido (discrepancia/revertido): NUNCA se re-evalúa, ni se consulta BankMovement para él', async () => {
    consultarTransaccionesNetpay.mockResolvedValue({ transacciones: [t({ amount: 300, commission: 20 })] });
    NetpayMatch.find = jest.fn(() => fakeFind([
      { terminalID: TERMINAL, dia: new Date('2026-09-10T00:00:00.000Z'), bucket: 'general', estatusMatch: 'discrepancia', motivoDiscrepancia: 'revertido' },
    ]));

    const { evaluados } = await evaluarRango({ terminalID: TERMINAL });

    expect(evaluados).toEqual([]);
    expect(BankMovement.find).not.toHaveBeenCalled();
    expect(NetpayMatch.findOneAndUpdate).not.toHaveBeenCalled();
  });

  // design.md "Report-Present Precedence": un bucket ya cerrado por un reporte
  // (resuelto_por_reporte) es autoridad del reporte, no de la evaluación en vivo — sigue
  // sin tocarse.
  test('bucket ya resuelto_por_reporte: NUNCA se re-evalúa (el reporte manda)', async () => {
    consultarTransaccionesNetpay.mockResolvedValue({ transacciones: [t({ amount: 300, commission: 20 })] });
    NetpayMatch.find = jest.fn(() => fakeFind([
      { terminalID: TERMINAL, dia: new Date('2026-09-10T00:00:00.000Z'), bucket: 'general', estatusMatch: 'resuelto_por_reporte' },
    ]));

    const { evaluados } = await evaluarRango({ terminalID: TERMINAL });

    expect(evaluados).toEqual([]);
    expect(BankMovement.find).not.toHaveBeenCalled();
  });

  test('sin transacciones: evaluados [], sin ninguna query a NetpayMatch/BankMovement', async () => {
    consultarTransaccionesNetpay.mockResolvedValue({ transacciones: [] });

    const { evaluados } = await evaluarRango({ terminalID: TERMINAL });

    expect(evaluados).toEqual([]);
    expect(NetpayMatch.find).not.toHaveBeenCalled();
    expect(BankMovement.find).not.toHaveBeenCalled();
  });

  // Fix (2026-09-29, pedido explícito del usuario): el tab "Matching" comparte
  // dateFrom/dateTo/terminalID con "Consulta", pero evaluarRango() ignoraba
  // responseCode/almacenes/status aunque se los pasaran — consultarTransaccionesNetpay()
  // (netpay-transacciones.service.js) SÍ los acepta, la misma función que usa Consulta.
  test('reenvía responseCode/almacenes/status a consultarTransaccionesNetpay cuando se pasan', async () => {
    consultarTransaccionesNetpay.mockResolvedValue({ transacciones: [] });

    await evaluarRango({
      dateFrom: '2026-09-10', dateTo: '2026-09-10', terminalID: TERMINAL,
      responseCode: '00', almacenes: 'A0,N0', status: 'completed',
    });

    expect(consultarTransaccionesNetpay).toHaveBeenCalledWith({
      dateFrom: '2026-09-10', dateTo: '2026-09-10', terminalID: TERMINAL,
      responseCode: '00', almacenes: 'A0,N0', status: 'completed',
    });
  });

  test('responseCode/almacenes/status ausentes: se reenvían como undefined, no rompe la llamada', async () => {
    consultarTransaccionesNetpay.mockResolvedValue({ transacciones: [] });

    await evaluarRango({ dateFrom: '2026-09-10', dateTo: '2026-09-10', terminalID: TERMINAL });

    expect(consultarTransaccionesNetpay).toHaveBeenCalledWith({
      dateFrom: '2026-09-10', dateTo: '2026-09-10', terminalID: TERMINAL,
      responseCode: undefined, almacenes: undefined, status: undefined,
    });
  });
});
