'use strict';

// Fichas de efectivo (2026-11-04): globales del periodo y efectivo de la caja
// de cada sucursal / día que indican.
jest.mock('../../../visor/models/CFDI', () => ({ find: jest.fn() }));
jest.mock('../centros-costo/centros-costo.service', () => ({ resolveBySerieMap: jest.fn() }));
jest.mock('../erp/erp-sync.service', () => ({ obtenerDesglosesCobroAlmacenPorCentro: jest.fn(), obtenerDesglosesSalidasCajaPorAlmacen: jest.fn(), obtenerDesglosesCobroPorFactura: jest.fn(async () => []), obtenerCuentaTicket: jest.fn(async () => null) }));
jest.mock('../banks/BankMovement.model', () => ({ distinct: jest.fn() }));
jest.mock('../banks/repositories/bank-config.repository', () => ({ findAllAsMap: jest.fn() }));
jest.mock('../erp/KoreRemision.model', () => ({ find: jest.fn(() => ({ select: () => ({ lean: async () => [{ serie: 'C0', folio: '2', nombre: 'JUAN PEREZ' }] }) })) }));
jest.mock('../erp/kore-remisiones.service', () => ({ infoRemisiones: jest.fn() }));
jest.mock('./FichaEfectivo.model', () => ({ find: jest.fn(() => ({ select: () => ({ lean: async () => [] }) })) }));
jest.mock('./reportes-por-facturar.fichas-guardadas', () => ({ cajasYaGuardadas: jest.fn(async () => []) }));
jest.mock('./reportes-por-facturar.facturas-kore', () => ({ traerFacturasRecientes: jest.fn(async () => ({ dias: [], nuevas: 0, error: null })) }));

const CFDI = require('../../../visor/models/CFDI');
const centros = require('../centros-costo/centros-costo.service');
const erp = require('../erp/erp-sync.service');
const koreRem = require('../erp/kore-remisiones.service');
const fichas = require('./reportes-por-facturar.fichas');

const lean = v => ({ select: () => ({ lean: async () => v }) });
const EF = m => ({ claveSat: '01', nombre: 'EFECTIVO', monto: m });
// 31-oct 12:00 MX
const cobro = (serieOrigen, monto, formasPago, extra = {}) => ({ serieOrigen, folioOrigen: '1', claveCentro: 'C0', fecha: '2026-10-31T18:00:00Z', monto: -monto, formasPago, ...extra });
const salida = (abreviatura, montoRetirado, fecha = '2026-10-31T20:00:00Z') => ({ fecha, tipoMovimiento: { abreviatura }, montoRetirado });

beforeEach(() => {
  jest.clearAllMocks();
  centros.resolveBySerieMap.mockResolvedValue({ A0: { id: 1, sucursal: 'CEDIS' }, C0: { id: 3, sucursal: 'CONSTRUCASA' } });
  koreRem.infoRemisiones.mockResolvedValue({ info: new Map() });
});

test('listarGlobales: solo vigentes, una por UUID (manda la ERP), ordenadas', async () => {
  CFDI.find.mockReturnValue(lean([
    { uuid: 'b', serie: 'C0', folio: '20', fecha: '2026-11-01T18:00:00Z', total: 50, receptor: { rfc: 'XAXX010101000', nombre: 'PUBLICO EN GENERAL' }, global: true, source: 'SAT' },
    { uuid: 'B', serie: 'C0', folio: '20', fecha: '2026-11-01T18:00:00Z', total: 50, receptor: { rfc: 'XAXX010101000', nombre: 'PUBLICO EN GENERAL' }, global: true, source: 'ERP' },
    { uuid: 'a', serie: 'C0', folio: '10', fecha: '2026-10-31T18:00:00Z', total: 100, receptor: { rfc: 'XAXX010101000', nombre: 'PUBLICO EN GENERAL' }, global: true, source: 'ERP' },
    { uuid: 'c', serie: 'C0', folio: '30', fecha: '2026-10-31T18:00:00Z', total: 70, receptor: { rfc: 'XAXX010101000', nombre: 'PUBLICO EN GENERAL' }, global: true, source: 'ERP', satStatus: 'Cancelado' },
    // RFC genérico con nombre del cliente y sin InformacionGlobal: NO es global (2026-11-26).
    { uuid: 'd', serie: 'C0', folio: '40', fecha: '2026-10-31T18:00:00Z', total: 9, receptor: { rfc: 'XAXX010101000', nombre: 'ABRAHAM PACHECO' }, source: 'ERP' },
  ]));
  const r = await fichas.listarGlobales({ rfc: 'CCO011113663', centroCostoId: 3, fechaInicio: '2026-10-31', fechaFin: '2026-11-01' });
  expect(CFDI.find.mock.calls[0][0]).toMatchObject({ 'receptor.rfc': 'XAXX010101000', serie: { $in: ['C0'] }, tipoDeComprobante: 'I' });
  expect(r.sucursal).toBe('CONSTRUCASA');
  expect(r.globales.map(g => [g.folio, g.dia, g.total])).toEqual([['10', '2026-10-31', 100], ['20', '2026-11-01', 50]]);
});

test('listarGlobales valida el rango', async () => {
  await expect(fichas.listarGlobales({ rfc: 'X', fechaInicio: '2026-10-01', fechaFin: '2026-11-15' })).rejects.toThrow(/31 días/);
});

test('calcularFicha: efectivo de la caja del día por concepto, menos devoluciones y faltante', async () => {
  // Facturas del detalle: C0-1 es la global (público en general).
  CFDI.find.mockReturnValue(lean([{ uuid: 'G1', serie: 'C0', folio: '1', receptor: { rfc: 'XAXX010101000', nombre: 'PUBLICO EN GENERAL' }, global: true, source: 'ERP' }]));
  erp.obtenerDesglosesCobroAlmacenPorCentro.mockResolvedValue([
    { serieVenta: 'C0', folioVenta: '1', serieFactura: 'C0', folioFactura: '1', cobros: [
      cobro('CBT', 1000, [EF(1000)]),                                                      // facturado
      cobro('CBT', 100, [EF(100), { claveSat: '04', nombre: 'TARJETA DE CREDITO', monto: 100 }]), // mixto que excede → 50
      cobro('CBT', 500, [{ claveSat: '28', nombre: 'TARJETA DE DEBITO', monto: 500 }]),    // no va
      cobro('CBT', 50.78, [{ claveSat: '01', nombre: 'PUNTOS', monto: 50.78 }]),           // puntos: no es efectivo
      cobro('CFC', 300, [EF(300)]),                                                        // cobranza
      cobro('ABO', 150, [EF(150)]),                                                        // abono
      cobro('MIS', 40, [EF(40)]),                                                          // venta miscelánea
      cobro('APA', 80, [EF(5), { claveSat: '30', nombre: 'SALDO A FAVOR', monto: 80 }]),  // fuera
      cobro('CBT', 70, [{ claveSat: '30', nombre: 'SALDO A FAVOR', monto: 70 }]),          // informativo
      cobro('CPF', 400, [{ claveSat: '01', nombre: 'DEPOSITO EN EFECTIVO', monto: 400 }]), // informativo
      cobro('CBT', 999, [EF(999)], { claveCentro: 'A0' }),                                 // otra caja
      cobro('CBT', 888, [EF(888)], { fecha: '2026-11-01T07:00:00Z' }),                     // 1-nov 01:00 MX
    ] },
    { serieVenta: 'C0', folioVenta: '2', serieFactura: null, cobros: [cobro('CBT', 170.39, [EF(170.39)])] },  // por facturar
  ]);
  erp.obtenerDesglosesSalidasCajaPorAlmacen.mockResolvedValue([
    salida('RETD', 20.78), salida('RETEF', 100), salida('RPF', 0.55), salida('ST', 50000), salida('CIC', 100000),
  ]);
  // C0-1 es una venta PPD facturada: su cobranza y su abono van a "Cobranza facturado" (2026-11-25).
  koreRem.infoRemisiones.mockResolvedValue({ info: new Map([['C0|1', { tipoPago: 'PPD', nombreTipoMovimiento: 'FACTURADO' }]]) });

  const r = await fichas.calcularFicha({ rfc: 'CCO011113663', centroCostoId: 3, fechaInicio: '2026-10-31', fechaFin: '2026-10-31' });
  expect(erp.obtenerDesglosesCobroAlmacenPorCentro).toHaveBeenCalledTimes(1);
  expect(erp.obtenerDesglosesCobroAlmacenPorCentro.mock.calls[0][0]).toMatchObject({ centro: 'C0', fechaDesde: '2026-10-31T06:00:00.000Z', fechaHasta: '2026-11-01T05:59:59.999Z' });
  expect(r.cortes).toHaveLength(1);
  expect(r.cortes[0]).toMatchObject({
    serie: 'C0', dia: '2026-10-31', sucursal: 'CONSTRUCASA',
    efectivoFacturado: 1050, efectivoPorFacturar: 170.39, miscelaneaEfectivo: 40, cobranzaEfectivo: 450, abonosEfectivo: 0,
    devoluciones: 20.78, retiroEfectivo: 100, faltante: 0.55, depositoEfectivo: 400, saldoFavor: 70,
    // Sin factura (170.39) ya NO suma (2026-11-25).
    totalEntregar: 1418.67,
  });
  expect(r.totales.totalEntregar).toBe(1418.67);
  expect(r.sucursal).toBe('CONSTRUCASA');
  expect(r.advertencias).toBeNull();
  expect(r.incompleta).toBe(false);
  // Rastro: el detalle suma lo mismo que el total y cada cobro trae remisión / factura / global.
  expect(Math.round(r.detalle.filter(d => d.concepto !== 'efectivoPorFacturar').reduce((s, d) => s + d.importe, 0) * 100) / 100).toBe(1418.67);
  // Lo sin factura va al final del anexo.
  expect(r.detalle[r.detalle.length - 1]).toMatchObject({ concepto: 'efectivoPorFacturar', remision: 'C0-2' });
  expect(r.detalle.find(d => d.concepto === 'miscelaneaEfectivo')).toMatchObject({ cobro: 'MIS-1', importe: 40 });
  const porFacturar = r.detalle.find(d => d.concepto === 'efectivoPorFacturar');
  expect(porFacturar).toMatchObject({ remision: 'C0-2', factura: null, cliente: 'JUAN PEREZ', importe: 170.39 });
  const facturado = r.detalle.find(d => d.concepto === 'efectivoFacturado');
  expect(facturado).toMatchObject({ remision: 'C0-1', factura: 'C0-1', facturaUuid: 'G1', esGlobal: true });
  expect(r.detalle.find(d => d.concepto === 'devoluciones')).toMatchObject({ importe: -20.78 });
  // RETIRO DE EFECTIVO (RETEF, 2026-11-06) resta; ST y CIC no.
  expect(r.detalle.find(d => d.concepto === 'retiroEfectivo')).toMatchObject({ importe: -100 });
  expect(r.detalle.some(d => d.concepto === 'depositoEfectivo' || d.concepto === 'saldoFavor')).toBe(false);
});

test('calcularFicha: rango de varios días — una caja por día, días sin efectivo no salen', async () => {
  erp.obtenerDesglosesCobroAlmacenPorCentro.mockImplementation(async ({ fechaDesde }) =>
    fechaDesde.startsWith('2026-11-01') ? [{ serieFactura: null, cobros: [cobro('CBT', 100, [EF(100)], { fecha: '2026-11-01T18:00:00Z' })] }] : []);
  erp.obtenerDesglosesSalidasCajaPorAlmacen.mockResolvedValue([]);
  CFDI.find.mockReturnValue(lean([]));
  const r = await fichas.calcularFicha({ rfc: 'X', centroCostoId: 3, fechaInicio: '2026-10-31', fechaFin: '2026-11-02' });
  expect(erp.obtenerDesglosesCobroAlmacenPorCentro).toHaveBeenCalledTimes(3);
  expect(r.cortes.map(c => [c.dia, c.totalEntregar])).toEqual([['2026-11-01', 0]]);
  expect(r.totales.efectivoPorFacturar).toBe(100);
});

test('calcularFicha: Kore sin respuesta → se omite esa caja con advertencia', async () => {
  erp.obtenerDesglosesCobroAlmacenPorCentro.mockRejectedValue(new Error('429'));
  erp.obtenerDesglosesSalidasCajaPorAlmacen.mockResolvedValue([]);
  const r = await fichas.calcularFicha({ rfc: 'CCO011113663', centroCostoId: 3, fechaInicio: '2026-10-31', fechaFin: '2026-10-31' });
  expect(r.cortes).toEqual([]);
  expect(r.incompleta).toBe(true);
  expect(r.advertencias[0]).toMatch(/no respondió la caja C0 del 2026-10-31/);
});

test('calcularFicha sin sucursal o con rango largo → 400', async () => {
  await expect(fichas.calcularFicha({ rfc: 'X', fechaInicio: '2026-10-31' })).rejects.toThrow(/sucursal/);
  await expect(fichas.calcularFicha({ rfc: 'X', centroCostoId: 3, fechaInicio: '2026-10-01', fechaFin: '2026-11-15' })).rejects.toThrow(/10 días/);
  await expect(fichas.calcularFicha({ rfc: 'X', centroCostoId: 3, fechaInicio: '2026-10-01', fechaFin: '2026-10-11' })).rejects.toThrow(/10 días/);
});

test('listarBancos: los de Bancos (movimientos + configuración) con su cuenta', async () => {
  const BankMovement = require('../banks/BankMovement.model');
  const repo = require('../banks/repositories/bank-config.repository');
  BankMovement.distinct.mockResolvedValue(['BBVA', 'Banorte']);
  repo.findAllAsMap.mockResolvedValue(new Map([['Banorte', { numeroCuenta: '0123' }], ['Azteca', { numeroCuenta: null }]]));
  expect(await fichas.listarBancos()).toEqual([
    { banco: 'Azteca', numeroCuenta: null }, { banco: 'Banorte', numeroCuenta: '0123' }, { banco: 'BBVA', numeroCuenta: null },
  ]);
});

test('Anexo › Actualizar: pone la factura que ya tiene la remisión (sin caché), sin tocar importes ni conceptos', async () => {
  erp.obtenerDesglosesCobroAlmacenPorCentro.mockResolvedValue([
    { serieVenta: 'C0', folioVenta: '2', serieFactura: 'C0', folioFactura: '900' },
    { serieVenta: 'C0', folioVenta: '3' }, // sigue sin factura
  ]);
  CFDI.find.mockReturnValue(lean([{ uuid: 'U9', serie: 'C0', folio: '900', receptor: { rfc: 'XAXX010101000' }, global: true, source: 'ERP' }]));
  const detalle = [
    { dia: '2026-10-31', serie: 'C0', concepto: 'efectivoPorFacturar', remision: 'C0-2', factura: null, importe: 100 },
    { dia: '2026-10-31', serie: 'C0', concepto: 'efectivoPorFacturar', remision: 'C0-3', factura: null, importe: 50 },
    { dia: '2026-10-31', serie: 'C0', concepto: 'efectivoFacturado', remision: 'C0-1', factura: 'C0-800', importe: 10 },
  ];
  const r = await fichas.actualizarFacturasDetalle({ rfc: 'R', detalle });
  expect(erp.obtenerDesglosesCobroAlmacenPorCentro).toHaveBeenCalledTimes(1);
  expect(erp.obtenerDesglosesCobroAlmacenPorCentro.mock.calls[0][0]).toMatchObject({ centro: 'C0', sinCache: true });
  expect(r.nuevas).toBe(1);
  expect(r.siguenSinFactura).toBe(1);
  expect(detalle[0]).toMatchObject({ factura: 'C0-900', facturadaDespues: true, esGlobal: true, concepto: 'efectivoPorFacturar', importe: 100 });
  expect(detalle[1].factura).toBeNull();
});

test('Anexo › Actualizar: si Kore falla, avisa y no cambia nada', async () => {
  erp.obtenerDesglosesCobroAlmacenPorCentro.mockRejectedValue(new Error('429'));
  const detalle = [{ dia: '2026-10-31', serie: 'C0', concepto: 'efectivoPorFacturar', remision: 'C0-2', factura: null, importe: 100 }];
  const r = await fichas.actualizarFacturasDetalle({ rfc: 'R', detalle });
  expect(r.nuevas).toBe(0);
  expect(r.advertencias[0]).toMatch(/C0 del 2026-10-31/);
  expect(detalle[0].factura).toBeNull();
});

test('calcularFicha: cobros y abonos de remisiones de OTRA sucursal van en su propio renglón (2026-11-06)', async () => {
  erp.obtenerDesglosesCobroAlmacenPorCentro.mockResolvedValue([
    { serieVenta: 'A0', folioVenta: '5411', serieFactura: null, cobros: [cobro('CPF', 6737.09, [EF(6737.09)])] },
    { serieVenta: 'A0', folioVenta: '4991', serieFactura: 'A0', folioFactura: '14603', cobros: [cobro('ABO', 2725.73, [EF(2725.73)])] },
    { serieVenta: 'C0', folioVenta: '9', serieFactura: 'C0', folioFactura: '9', cobros: [cobro('CBT', 100, [EF(100)])] },
  ]);
  erp.obtenerDesglosesSalidasCajaPorAlmacen.mockResolvedValue([]);
  CFDI.find.mockReturnValue(lean([]));
  const r = await fichas.calcularFicha({ rfc: 'X', centroCostoId: 3, fechaInicio: '2026-10-31', fechaFin: '2026-10-31' });
  // 2026-11-26: la de otra sucursal SIN factura (A0-5411) ya no suma: va al anexo hasta que se facture.
  expect(r.totales).toMatchObject({ cobroOtraSucursal: 2725.73, cobroOtraSucursalSinFactura: 6737.09, efectivoPorFacturar: 0, abonosEfectivo: 0, efectivoFacturado: 100, totalEntregar: 2825.73 });
  expect(r.detalle.filter(d => d.concepto === 'cobroOtraSucursal').map(d => d.remision)).toEqual(['A0-4991']);
  expect(r.detalle.find(d => d.remision === 'A0-5411')).toMatchObject({ concepto: 'cobroOtraSucursalSinFactura', factura: null });
});

describe('2026-11-25: renglones por tipo de venta (consignación / PPD / contado / abono)', () => {
  const MOV = (movimiento, saldoAnterior, saldo, fecha, forma = 'EFECTIVO') => ({ movimiento, forma, saldoAnterior, saldo, fecha });
  beforeEach(() => {
    erp.obtenerDesglosesSalidasCajaPorAlmacen.mockResolvedValue([]);
    CFDI.find.mockReturnValue(lean([]));
  });

  test('consignación facturada / sin factura, PPD facturada, contado liquidado (con BON) y abono con saldo', async () => {
    const cta = (folio, factura, origen, monto) => ({
      serieVenta: 'C0', folioVenta: folio, serieFactura: factura ? 'C0' : null, folioFactura: factura, fechaCreacion: '2026-10-01T00:00:00Z',
      cobros: [cobro(origen, monto, [EF(monto)], { folioOrigen: folio })],
    });
    erp.obtenerDesglosesCobroAlmacenPorCentro.mockResolvedValue([
      cta('10', '900', 'CPF', 1000), cta('11', null, 'CPF', 200), cta('12', '901', 'ABO', 300), cta('13', '902', 'ABO', 96), cta('14', '903', 'ABO', 50),
      cta('15', null, 'ABO', 70),
    ]);
    const PUE = { tipoPago: 'PUE', nombreTipoMovimiento: 'FACTURADO' };
    koreRem.infoRemisiones.mockImplementation(async ({ necesitaSaldo }) => ({ info: new Map([
      ['C0|10', { tipoPago: 'PPD', nombreTipoMovimiento: 'CONSIGNACION' }],
      // Consignación sin factura: saldo fresco para separar el anexo (2026-11-26).
      ['C0|11', necesitaSaldo ? { tipoPago: 'PPD', nombreTipoMovimiento: 'CONSIGNACION', abonos: [MOV('CPF-11', 200, 0, '2026-10-31T18:00:00Z')] } : { tipoPago: 'PPD', nombreTipoMovimiento: 'CONSIGNACION' }],
      ['C0|15', necesitaSaldo ? { tipoPago: 'PPD', nombreTipoMovimiento: 'CONSIGNACION', abonos: [MOV('ABO-15', 100, 30, '2026-10-31T18:00:00Z')] } : { tipoPago: 'PPD', nombreTipoMovimiento: 'CONSIGNACION' }],
      ['C0|12', { tipoPago: 'PPD', nombreTipoMovimiento: 'FACTURADO' }],
      // Contado: el saldo solo se pide fresco (necesitaSaldo).
      ['C0|13', necesitaSaldo ? { ...PUE, abonos: [MOV('ABO-13', 100, 4, '2026-10-31T18:00:00Z'), MOV('RET-1', 4, 0, '2026-10-31T18:00:02Z', null)] } : PUE],
      ['C0|14', necesitaSaldo ? { ...PUE, abonos: [MOV('ABO-14', 80, 30, '2026-10-31T18:00:00Z')] } : PUE],
    ]) }));
    const r = await fichas.calcularFicha({ rfc: 'X', centroCostoId: 3, fechaInicio: '2026-10-31', fechaFin: '2026-10-31' });
    expect(r.totales).toMatchObject({
      consignacionFacturada: 1000, efectivoPorFacturar: 270, cobranzaEfectivo: 300, efectivoFacturado: 96, abonosEfectivo: 50,
      totalEntregar: 1446,
    });
    expect(r.incompleta).toBe(false);
    // Saldo fresco: contado y consignación sin factura.
    const fresca = koreRem.infoRemisiones.mock.calls.find(c => c[0].necesitaSaldo);
    expect(fresca[0].remisiones.map(x => x.folio).sort()).toEqual(['11', '13', '14', '15']);
    expect(r.detalle.find(d => d.remision === 'C0-14')).toMatchObject({ concepto: 'abonosEfectivo', saldoDespues: 30, tipoVenta: 'PUE FACTURADO' });
    expect(r.detalle.find(d => d.remision === 'C0-11')).toMatchObject({ concepto: 'efectivoPorFacturar', saldoDespues: 0 });
    expect(r.detalle.find(d => d.remision === 'C0-15')).toMatchObject({ concepto: 'efectivoPorFacturar', saldoDespues: 30 });
  });

  test('sin tipo de venta de Kore → como antes y la ficha queda incompleta (no se puede guardar)', async () => {
    erp.obtenerDesglosesCobroAlmacenPorCentro.mockResolvedValue([
      { serieVenta: 'C0', folioVenta: '20', serieFactura: 'C0', folioFactura: '9', cobros: [cobro('CPF', 10, [EF(10)])] },
    ]);
    const r = await fichas.calcularFicha({ rfc: 'X', centroCostoId: 3, fechaInicio: '2026-10-31', fechaFin: '2026-10-31' });
    expect(r.totales.cobranzaEfectivo).toBe(10);
    expect(r.incompleta).toBe(true);
    expect(r.advertencias[0]).toMatch(/tipo de venta/);
  });
});

// 2026-11-26 (pedido del usuario): cobrado sin factura el 31-oct y amparado por la global
// C0-G2 del 1-nov → el 31 va como sin factura (no suma) y el 1-nov suma como consignación facturada.
describe('cobro sin factura que ampara una global de un día posterior', () => {
  const GLOBAL = { uuid: 'G2', serie: 'C0', folio: 'G2', fecha: '2026-11-01T20:00:00Z', receptor: { rfc: 'XAXX010101000' }, informacionGlobal: { periodicidad: '01' }, global: true, source: 'ERP' };
  const cuenta = { serieVenta: 'C0', folioVenta: '30', serieFactura: 'C0', folioFactura: 'G2', fechaCreacion: '2026-10-01T00:00:00Z',
    cobros: [cobro('CPF', 500, [EF(500)], { folioOrigen: '30' })] };
  beforeEach(() => {
    erp.obtenerDesglosesSalidasCajaPorAlmacen.mockResolvedValue([]);
    CFDI.find.mockImplementation(q => lean(q.fecha ? (q.fecha.$gte <= new Date(GLOBAL.fecha) && new Date(GLOBAL.fecha) <= q.fecha.$lte ? [GLOBAL] : []) : [GLOBAL]));
    koreRem.infoRemisiones.mockImplementation(async ({ necesitaSaldo }) => ({ info: new Map([
      ['C0|30', { tipoPago: 'PPD', nombreTipoMovimiento: 'CONSIGNACION', ...(necesitaSaldo ? { abonos: [{ movimiento: 'CPF-30', forma: 'EFECTIVO', importe: 500, saldoAnterior: 500, saldo: 0, fecha: '2026-10-31T18:00:00Z' },
        // BN que Kore aplica al timbrar la global (20%): la factura sale por 400.
        { movimiento: 'RET-9', origen: 'BN-1', forma: null, importe: 100, saldoAnterior: 0, saldo: -100, fecha: '2026-11-01T20:00:00Z' }] } : {}) }],
    ]) }));
  });

  test('día del cobro: sin factura (no suma) con la global posterior anotada', async () => {
    erp.obtenerDesglosesCobroAlmacenPorCentro.mockResolvedValue([cuenta]);
    const r = await fichas.calcularFicha({ rfc: 'X', centroCostoId: 3, fechaInicio: '2026-10-31', fechaFin: '2026-10-31' });
    expect(r.totales).toMatchObject({ consignacionFacturada: 0, efectivoPorFacturar: 500, totalEntregar: 0 });
    expect(r.detalle[0]).toMatchObject({ concepto: 'efectivoPorFacturar', factura: null, facturaPosterior: 'C0-G2', facturaPosteriorDia: '2026-11-01', saldoDespues: 0 });
  });

  test('día de la global: suma como consignación facturada con el día del cobro', async () => {
    erp.obtenerDesglosesCobroAlmacenPorCentro.mockResolvedValue([]);
    erp.obtenerDesglosesCobroPorFactura.mockResolvedValue([cuenta]);
    const r = await fichas.calcularFicha({ rfc: 'X', centroCostoId: 3, fechaInicio: '2026-11-01', fechaFin: '2026-11-01' });
    expect(erp.obtenerDesglosesCobroPorFactura).toHaveBeenCalledWith(expect.objectContaining({ serieFactura: 'C0', folioFactura: 'G2' }));
    // Suma lo facturado (cobrado − BN de la global); la BN queda como informativo.
    expect(r.totales).toMatchObject({ consignacionFacturada: 400, bonificacionGlobal: 100, efectivoPorFacturar: 0, totalEntregar: 400 });
    expect(r.detalle[0]).toMatchObject({ dia: '2026-11-01', concepto: 'consignacionFacturada', factura: 'C0-G2', cobradoEl: '2026-10-31', importe: 500, facturado: 400 });
  });
});

// 2026-11-26: cobro de otra sucursal sin factura (ficha guardada del 31-oct) que se facturó el
// 1-nov con una global con BN → la ficha del 1-nov de ESTA caja lo suma por lo facturado.
test('cobro de otra sucursal sin factura suma el día de su factura en la caja donde se cobró', async () => {
  const FichaEfectivo = require('./FichaEfectivo.model');
  FichaEfectivo.find.mockReturnValueOnce({ select: () => ({ lean: async () => [{ detalle: [
    { dia: '2026-10-31', serie: 'C0', concepto: 'cobroOtraSucursalSinFactura', remision: 'E0-7', cobro: 'CCE-1', fecha: '2026-10-31T18:00:00Z', importe: 500, remisionCreada: '2026-10-30T18:00:00Z' },
  ] }] }) });
  erp.obtenerDesglosesCobroAlmacenPorCentro.mockResolvedValue([]);
  erp.obtenerDesglosesSalidasCajaPorAlmacen.mockResolvedValue([]);
  erp.obtenerCuentaTicket.mockResolvedValueOnce({ factura: 'E0-G9', abonos: [
    { movimiento: 'CCE-1', forma: 'EFECTIVO', importe: 500 }, { movimiento: 'RET-1', origen: 'BN-1', forma: null, importe: 100 } ] });
  CFDI.find.mockImplementation(q => lean(q.$or ? [{ uuid: 'U9', serie: 'E0', folio: 'G9', fecha: '2026-11-01T18:00:00Z', receptor: { rfc: 'XAXX010101000' }, global: true, source: 'ERP' }] : []));
  const r = await fichas.calcularFicha({ rfc: 'X', centroCostoId: 3, fechaInicio: '2026-11-01', fechaFin: '2026-11-01' });
  expect(r.totales).toMatchObject({ cobroOtraSucursal: 400, bonificacionGlobal: 100, totalEntregar: 400 });
  expect(r.detalle[0]).toMatchObject({ concepto: 'cobroOtraSucursal', remision: 'E0-7', factura: 'E0-G9', cobradoEl: '2026-10-31', importe: 500, facturado: 400 });
});

// 2026-11-26: contra entrega (CCE) de otra sucursal sin factura ligada en Kore: se liga a la
// factura de su serie con el mismo total timbrada junto al cobro (Atzompa E0-261102072).
test('contra entrega sin factura ligada toma la factura del mismo importe y momento', async () => {
  erp.obtenerDesglosesCobroAlmacenPorCentro.mockResolvedValue([
    { serieVenta: 'E0', folioVenta: '72', serieFactura: null, fechaCreacion: '2026-10-30T18:00:00Z', cobros: [cobro('CCE', 2033.99, [EF(2033.99)])] },
  ]);
  erp.obtenerDesglosesSalidasCajaPorAlmacen.mockResolvedValue([]);
  const FACT = { uuid: '4B006F47', serie: 'E0', folio: '257', fecha: '2026-10-31T12:00:00Z', total: 2033.99, receptor: { rfc: 'XAXX010101000', nombre: 'ABRAHAM  PACHECO  PACHECO' }, global: false, source: 'ERP' };
  CFDI.find.mockImplementation(q => lean(q.total || q.$or ? [FACT] : []));
  const r = await fichas.calcularFicha({ rfc: 'X', centroCostoId: 3, fechaInicio: '2026-10-31', fechaFin: '2026-10-31' });
  expect(CFDI.find.mock.calls.find(c => c[0].total)[0]).toMatchObject({ serie: 'E0', total: { $gte: 2033.98, $lte: 2034 } });
  expect(r.totales).toMatchObject({ cobroOtraSucursal: 2033.99, cobroOtraSucursalSinFactura: 0, totalEntregar: 2033.99 });
  // Factura individual al RFC genérico (no global).
  expect(r.detalle[0]).toMatchObject({ concepto: 'cobroOtraSucursal', remision: 'E0-72', factura: 'E0-257', esGlobal: false });
});
