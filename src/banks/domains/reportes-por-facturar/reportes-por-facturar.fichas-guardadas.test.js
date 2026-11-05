'use strict';

// Fichas de efectivo guardadas (2026-11-05): folio, denominaciones que cuadran,
// una caja en una sola ficha vigente, cancelar solo admin.
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
const base = { rfc: 'R', centroCostoId: 3, fechaInicio: '2026-10-31', fechaFin: '2026-10-31', bancos: [{ banco: 'Banorte' }] };
// $1,520.00 contado vs $1,520.39 → diferencia de centavos, aceptada.
const DEN = [{ tipo: 'Billete', valor: 500, cantidad: 3 }, { tipo: 'Moneda', valor: 10, cantidad: 2 }, { tipo: 'Moneda', valor: 1, cantidad: 0 }];

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
  const r1 = await svc.guardar({ ...base, denominaciones: DEN }, {});
  expect(Counter.nextSeq).toHaveBeenLastCalledWith('fichaEfectivo_202611');
  expect(r1).toMatchObject({ folio: 11001, anio: 2026 });
  Counter.nextSeq.mockResolvedValueOnce({ seq: 2 }).mockResolvedValueOnce({ seq: 3 });
  FichaEfectivo.create.mockRejectedValueOnce(Object.assign(new Error('dup'), { code: 11000 }));
  expect((await svc.guardar({ ...base, denominaciones: DEN }, {})).folio).toBe(11003);
  // Enero: 01001 (se muestra con el cero).
  jest.setSystemTime(new Date('2027-01-10T18:00:00Z'));
  const r3 = await svc.guardar({ ...base, denominaciones: DEN }, {});
  expect(Counter.nextSeq).toHaveBeenLastCalledWith('fichaEfectivo_202701');
  expect(svc.folioTexto(r3.folio)).toBe('01001');
  Counter.nextSeq.mockResolvedValueOnce({ seq: 1000 });
  await expect(svc.guardar({ ...base, denominaciones: DEN }, {})).rejects.toThrow(/999/);
});

test('guardar: recalcula, valida denominaciones y guarda con folio, banco, cajas y detalle', async () => {
  const r = await svc.guardar({ ...base, sucursalBanco: ' 123 ', denominaciones: DEN }, { dbId: '5', nombre: 'Ana' });
  expect(r).toMatchObject({
    folio: 11001, bancos: [{ banco: 'Banorte', numeroCuenta: '0123', importe: 1520 }], sucursalBanco: '123', cajas: ['C0|2026-10-31'],
    totalContado: 1520, diferencia: -0.39, creadoPor: { id: '5', nombre: 'Ana' },
  });
  expect(r.denominaciones).toEqual([{ tipo: 'Billete', valor: 500, cantidad: 3, importe: 1500 }, { tipo: 'Moneda', valor: 10, cantidad: 2, importe: 20 }]);
  expect(r.detalle[0].remision).toBe('C0-2');
});

test('guardar: denominaciones que no dan el total → 400', async () => {
  await expect(svc.guardar({ ...base, denominaciones: [{ tipo: 'Billete', valor: 500, cantidad: 2 }] }, {})).rejects.toThrow(/no dan el total/);
  expect(FichaEfectivo.create).not.toHaveBeenCalled();
});

test('guardar: denominación inventada o cantidad con decimales → 400', async () => {
  await expect(svc.guardar({ ...base, denominaciones: [{ tipo: 'Billete', valor: 300, cantidad: 1 }] }, {})).rejects.toThrow(/no válida/);
  await expect(svc.guardar({ ...base, denominaciones: [{ tipo: 'Billete', valor: 500, cantidad: 1.5 }] }, {})).rejects.toThrow(/no válida/);
});

test('guardar: caja ya en otra ficha vigente o Kore incompleto → 409; banco fuera de Bancos → 400', async () => {
  fichas.calcularFicha.mockResolvedValueOnce(fichaCalc({ yaEnFicha: [{ caja: 'C0|2026-10-31', folio: 3 }] }));
  await expect(svc.guardar({ ...base, denominaciones: DEN }, {})).rejects.toThrow(/ficha 00003/);
  fichas.calcularFicha.mockResolvedValueOnce(fichaCalc({ incompleta: true }));
  await expect(svc.guardar({ ...base, denominaciones: DEN }, {})).rejects.toThrow(/Kore no respondió/);
  await expect(svc.guardar({ ...base, bancos: [{ banco: 'Otro' }], denominaciones: DEN }, {})).rejects.toThrow(/no está en Bancos/);
  await expect(svc.guardar({ ...base, bancos: [], denominaciones: DEN }, {})).rejects.toThrow(/al menos un banco/);
  expect(FichaEfectivo.create).not.toHaveBeenCalled();
});

test('guardar: varios bancos — cada uno con su importe y que sumen lo contado', async () => {
  const dos = (a, b) => ({ ...base, bancos: [{ banco: 'Banorte', importe: a }, { banco: 'BBVA', importe: b }], denominaciones: DEN });
  await expect(svc.guardar(dos(1000, 0), {})).rejects.toThrow(/cada banco/);
  await expect(svc.guardar(dos(1000, 500), {})).rejects.toThrow(/no da lo contado/);
  await expect(svc.guardar({ ...base, bancos: [{ banco: 'BBVA' }, { banco: 'BBVA' }], denominaciones: DEN }, {})).rejects.toThrow(/repetido/);
  const r = await svc.guardar(dos(1000, 520), {});
  expect(r.bancos).toEqual([{ banco: 'Banorte', numeroCuenta: '0123', importe: 1000 }, { banco: 'BBVA', numeroCuenta: null, importe: 520 }]);
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
