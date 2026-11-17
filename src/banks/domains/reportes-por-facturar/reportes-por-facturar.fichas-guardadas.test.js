'use strict';

// Fichas de efectivo guardadas (2026-11-05): folio, todos los bancos (2026-11-17, sin
// elegir banco ni denominación), una caja en una sola ficha vigente, cancelar solo admin.
jest.mock('./FichaEfectivo.model', () => ({ find: jest.fn(), create: jest.fn(), findOneAndUpdate: jest.fn(), findOne: jest.fn(), collection: { dropIndex: jest.fn(async () => ({})) } }));
jest.mock('../../shared/models/Counter', () => ({ nextSeq: jest.fn(async () => ({ seq: 1 })) }));
jest.mock('../users/permisos-solo-admin', () => ({ esAdmin: jest.fn() }));
jest.mock('./reportes-por-facturar.fichas', () => ({ calcularFicha: jest.fn(), listarBancos: jest.fn() }));

const FichaEfectivo = require('./FichaEfectivo.model');
const { esAdmin } = require('../users/permisos-solo-admin');
const fichas = require('./reportes-por-facturar.fichas');
const svc = require('./reportes-por-facturar.fichas-guardadas');

const lean = v => ({ select: () => ({ lean: async () => v }), lean: async () => v });
const fichaCalc = (extra = {}) => ({
  centroCostoId: 3, sucursal: 'CONSTRUCASA', series: ['C0'], fechaInicio: '2026-10-31', fechaFin: '2026-10-31',
  cortes: [{ serie: 'C0', dia: '2026-10-31', totalEntregar: 1520.39 }],
  totales: { totalEntregar: 1520.39 },
  detalle: [{ dia: '2026-10-31', serie: 'C0', concepto: 'efectivoPorFacturar', remision: 'C0-2', importe: 1520.39 }],
  yaEnFicha: [], incompleta: false, ...extra,
});
const base = { rfc: 'R', centroCostoId: 3, fechaInicio: '2026-10-31', fechaFin: '2026-10-31' };

beforeEach(() => {
  jest.clearAllMocks();
  fichas.listarBancos.mockResolvedValue([{ banco: 'Banorte', numeroCuenta: '0123' }, { banco: 'BBVA', numeroCuenta: null }]);
  fichas.calcularFicha.mockResolvedValue(fichaCalc());
  FichaEfectivo.create.mockImplementation(async d => ({ toObject: () => d }));
  jest.useFakeTimers({ now: new Date('2026-11-05T18:00:00Z'), doNotFake: ['nextTick', 'setImmediate', 'setTimeout'] });
});
afterEach(() => jest.useRealTimers());

test('folio: mes + consecutivo del mes (11001, 11002…), único por año; choque → siguiente; tope 999', async () => {
  const Counter = require('../../shared/models/Counter');
  const r1 = await svc.guardar(base, {});
  expect(Counter.nextSeq).toHaveBeenLastCalledWith('fichaEfectivo_202611');
  expect(r1).toMatchObject({ folio: 11001, anio: 2026 });
  Counter.nextSeq.mockResolvedValueOnce({ seq: 2 }).mockResolvedValueOnce({ seq: 3 });
  FichaEfectivo.create.mockRejectedValueOnce(Object.assign(new Error('dup'), { code: 11000 }));
  expect((await svc.guardar(base, {})).folio).toBe(11003);
  // Enero: 01001 (se muestra con el cero).
  jest.setSystemTime(new Date('2027-01-10T18:00:00Z'));
  const r3 = await svc.guardar(base, {});
  expect(Counter.nextSeq).toHaveBeenLastCalledWith('fichaEfectivo_202701');
  expect(svc.folioTexto(r3.folio)).toBe('01001');
  Counter.nextSeq.mockResolvedValueOnce({ seq: 1000 });
  await expect(svc.guardar(base, {})).rejects.toThrow(/999/);
});

test('guardar: recalcula y guarda con folio, TODOS los bancos (sin importe), sin denominaciones, cajas y detalle', async () => {
  // Lo que mande la pantalla de banco / denominación se ignora.
  const r = await svc.guardar({ ...base, bancos: [{ banco: 'Otro' }], sucursalBanco: '123', denominaciones: [{ tipo: 'Billete', valor: 300, cantidad: 1 }] }, { dbId: '5', nombre: 'Ana' });
  expect(r).toMatchObject({
    folio: 11001, sucursalBanco: null, cajas: ['C0|2026-10-31'],
    totalContado: null, diferencia: null, creadoPor: { id: '5', nombre: 'Ana' },
  });
  expect(r.bancos).toEqual([{ banco: 'Banorte', numeroCuenta: '0123', importe: null }, { banco: 'BBVA', numeroCuenta: null, importe: null }]);
  expect(r.denominaciones).toEqual([]);
  expect(r.detalle[0].remision).toBe('C0-2');
});

test('guardar: caja ya en otra ficha vigente, Kore incompleto → 409; sin efectivo → 400', async () => {
  fichas.calcularFicha.mockResolvedValueOnce(fichaCalc({ yaEnFicha: [{ caja: 'C0|2026-10-31', folio: 3 }] }));
  await expect(svc.guardar(base, {})).rejects.toThrow(/ficha 00003/);
  fichas.calcularFicha.mockResolvedValueOnce(fichaCalc({ incompleta: true }));
  await expect(svc.guardar(base, {})).rejects.toThrow(/Kore no respondió/);
  fichas.calcularFicha.mockResolvedValueOnce(fichaCalc({ cortes: [] }));
  await expect(svc.guardar(base, {})).rejects.toThrow(/No hay efectivo/);
  expect(FichaEfectivo.create).not.toHaveBeenCalled();
});

test('listar: busca por remisión / factura y devuelve los renglones que coinciden', async () => {
  FichaEfectivo.find.mockReturnValue({ sort: () => ({ limit: () => ({ select: () => ({ lean: async () => [
    { _id: 'x', folio: 7, detalle: [{ remision: 'C0-2', factura: 'C0-1990' }, { remision: 'C0-9', factura: null }] },
  ] }) }) }) });
  const r = await svc.listar({ rfc: 'R', q: ' c0-1990 ' });
  expect(FichaEfectivo.find.mock.calls[0][0].$or).toEqual([{ 'detalle.remision': 'C0-1990' }, { 'detalle.factura': 'C0-1990' }]);
  expect(r[0]).toMatchObject({ id: 'x', folio: 7, coincidencias: [{ remision: 'C0-2', factura: 'C0-1990' }] });
  expect(r[0].detalle).toBeUndefined();
});

test('listar: usuario con sucursales asignadas solo ve las suyas', async () => {
  FichaEfectivo.find.mockReturnValue({ sort: () => ({ limit: () => ({ select: () => ({ lean: async () => [] }) }) }) });
  await svc.listar({ rfc: 'R', centroCostoIds: [5, 9] });
  expect(FichaEfectivo.find.mock.calls[0][0]).toEqual({ rfc: 'R', centroCostoId: { $in: [5, 9] } });
  await svc.listar({ rfc: 'R', centroCostoId: '5', centroCostoIds: [5, 9] });
  expect(FichaEfectivo.find.mock.calls[1][0]).toEqual({ rfc: 'R', centroCostoId: 5 });
});

test('cancelar: solo admin y con motivo', async () => {
  esAdmin.mockResolvedValue(false);
  await expect(svc.cancelar({ rfc: 'R', id: 'x', motivo: 'error de captura' }, {})).rejects.toThrow(/administrador/);
  esAdmin.mockResolvedValue(true);
  await expect(svc.cancelar({ rfc: 'R', id: 'x', motivo: 'no' }, {})).rejects.toThrow(/motivo/);
  FichaEfectivo.findOneAndUpdate.mockReturnValue(lean({ _id: 'x', estado: 'cancelada' }));
  const r = await svc.cancelar({ rfc: 'R', id: 'x', motivo: 'error de captura' }, { dbId: '1', nombre: 'Admin' });
  expect(r.estado).toBe('cancelada');
  expect(FichaEfectivo.findOneAndUpdate.mock.calls[0][0]).toMatchObject({ estado: 'vigente' });
});
