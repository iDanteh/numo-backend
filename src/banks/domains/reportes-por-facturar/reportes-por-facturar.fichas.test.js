'use strict';

// Fichas de efectivo (2026-11-04): globales del periodo y efectivo de la caja
// de cada sucursal / día que indican.
jest.mock('../../../visor/models/CFDI', () => ({ find: jest.fn() }));
jest.mock('../centros-costo/centros-costo.service', () => ({ resolveBySerieMap: jest.fn() }));
jest.mock('../erp/erp-sync.service', () => ({ obtenerDesglosesCobroAlmacenPorCentro: jest.fn(), obtenerDesglosesSalidasCajaPorAlmacen: jest.fn() }));
jest.mock('./reportes-por-facturar.facturas-kore', () => ({ traerFacturasRecientes: jest.fn(async () => ({ dias: [], nuevas: 0, error: null })) }));

const CFDI = require('../../../visor/models/CFDI');
const centros = require('../centros-costo/centros-costo.service');
const erp = require('../erp/erp-sync.service');
const fichas = require('./reportes-por-facturar.fichas');

const lean = v => ({ select: () => ({ lean: async () => v }) });
const EF = m => ({ claveSat: '01', nombre: 'EFECTIVO', monto: m });
// 31-oct 12:00 MX
const cobro = (serieOrigen, monto, formasPago, extra = {}) => ({ serieOrigen, folioOrigen: '1', claveCentro: 'C0', fecha: '2026-10-31T18:00:00Z', monto: -monto, formasPago, ...extra });
const salida = (abreviatura, montoRetirado, fecha = '2026-10-31T20:00:00Z') => ({ fecha, tipoMovimiento: { abreviatura }, montoRetirado });

beforeEach(() => {
  jest.clearAllMocks();
  centros.resolveBySerieMap.mockResolvedValue({ A0: { id: 1, sucursal: 'CEDIS' }, C0: { id: 3, sucursal: 'CONSTRUCASA' } });
});

test('listarGlobales: solo vigentes, una por UUID (manda la ERP), ordenadas', async () => {
  CFDI.find.mockReturnValue(lean([
    { uuid: 'b', serie: 'C0', folio: '20', fecha: '2026-11-01T18:00:00Z', total: 50, source: 'SAT' },
    { uuid: 'B', serie: 'C0', folio: '20', fecha: '2026-11-01T18:00:00Z', total: 50, source: 'ERP' },
    { uuid: 'a', serie: 'C0', folio: '10', fecha: '2026-10-31T18:00:00Z', total: 100, source: 'ERP' },
    { uuid: 'c', serie: 'C0', folio: '30', fecha: '2026-10-31T18:00:00Z', total: 70, source: 'ERP', satStatus: 'Cancelado' },
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
  CFDI.find.mockReturnValue(lean([
    { uuid: 'G1', serie: 'C0', folio: '1990', fecha: '2026-10-31T23:00:00Z', total: 5000, receptor: { rfc: 'XAXX010101000' }, source: 'ERP' },
    { uuid: 'G2', serie: 'C0', folio: '1991', fecha: '2026-10-31T23:10:00Z', total: 10, receptor: { rfc: 'XAXX010101000' }, source: 'ERP' },  // misma caja/día
    { uuid: 'C1', serie: 'C0', folio: '1992', fecha: '2026-10-31T18:00:00Z', total: 10, receptor: { rfc: 'ABC010101AB1' }, source: 'ERP' },
  ]));
  erp.obtenerDesglosesCobroAlmacenPorCentro.mockResolvedValue([
    { serieVenta: 'C0', folioVenta: '1', serieFactura: 'C0', cobros: [
      cobro('CBT', 1000, [EF(1000)]),                                                      // facturado
      cobro('CBT', 100, [EF(100), { claveSat: '04', nombre: 'TARJETA DE CREDITO', monto: 100 }]), // mixto que excede → 50
      cobro('CBT', 500, [{ claveSat: '28', nombre: 'TARJETA DE DEBITO', monto: 500 }]),    // no va
      cobro('CBT', 50.78, [{ claveSat: '01', nombre: 'PUNTOS', monto: 50.78 }]),           // puntos: no es efectivo
      cobro('CFC', 300, [EF(300)]),                                                        // cobranza
      cobro('ABO', 150, [EF(150)]),                                                        // abono
      cobro('APA', 80, [EF(5), { claveSat: '30', nombre: 'SALDO A FAVOR', monto: 80 }]),  // fuera
      cobro('CBT', 70, [{ claveSat: '30', nombre: 'SALDO A FAVOR', monto: 70 }]),          // informativo
      cobro('CPF', 400, [{ claveSat: '01', nombre: 'DEPOSITO EN EFECTIVO', monto: 400 }]), // informativo
      cobro('CBT', 999, [EF(999)], { claveCentro: 'A0' }),                                 // otra caja
      cobro('CBT', 888, [EF(888)], { fecha: '2026-11-01T07:00:00Z' }),                     // 1-nov 01:00 MX
    ] },
    { serieVenta: 'C0', folioVenta: '2', serieFactura: null, cobros: [cobro('CBT', 170.39, [EF(170.39)])] },  // por facturar
  ]);
  erp.obtenerDesglosesSalidasCajaPorAlmacen.mockResolvedValue([
    salida('RETD', 20.78), salida('RPF', 0.55), salida('ST', 50000), salida('CIC', 100000),
  ]);

  const r = await fichas.calcularFicha({ rfc: 'CCO011113663', uuids: ['g1', 'G2', 'C1'] });
  // Una sola consulta por caja / día aunque haya dos globales.
  expect(erp.obtenerDesglosesCobroAlmacenPorCentro).toHaveBeenCalledTimes(1);
  expect(erp.obtenerDesglosesCobroAlmacenPorCentro.mock.calls[0][0]).toMatchObject({ centro: 'C0', fechaDesde: '2026-10-31T06:00:00.000Z', fechaHasta: '2026-11-01T05:59:59.999Z' });
  expect(r.cortes).toHaveLength(1);
  expect(r.cortes[0]).toMatchObject({
    serie: 'C0', dia: '2026-10-31', sucursal: 'CONSTRUCASA', globales: ['C0-1990', 'C0-1991'],
    efectivoFacturado: 1050, efectivoPorFacturar: 170.39, cobranzaEfectivo: 300, abonosEfectivo: 150,
    devoluciones: 20.78, faltante: 0.55, depositoEfectivo: 400, saldoFavor: 70,
    totalEntregar: 1649.06,
  });
  expect(r.totales.totalEntregar).toBe(1649.06);
  expect(r.dias).toEqual(['2026-10-31']);
  expect(r.advertencias).toEqual(['C0-1992 no es factura global; se omitió.']);
});

test('calcularFicha: Kore sin respuesta → se omite esa caja con advertencia', async () => {
  CFDI.find.mockReturnValue(lean([{ uuid: 'G1', serie: 'C0', folio: '1990', fecha: '2026-10-31T18:00:00Z', total: 1, receptor: { rfc: 'XAXX010101000' } }]));
  erp.obtenerDesglosesCobroAlmacenPorCentro.mockRejectedValue(new Error('429'));
  erp.obtenerDesglosesSalidasCajaPorAlmacen.mockResolvedValue([]);
  const r = await fichas.calcularFicha({ rfc: 'CCO011113663', uuids: ['G1'] });
  expect(r.cortes).toEqual([]);
  expect(r.advertencias[0]).toMatch(/no respondió la caja C0 del 2026-10-31/);
});

test('calcularFicha sin facturas → 400', async () => {
  await expect(fichas.calcularFicha({ rfc: 'X', uuids: [] })).rejects.toThrow(/al menos una/);
});
