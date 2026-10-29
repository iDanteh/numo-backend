'use strict';

// "Estadística por facturar" (2026-10-29): de los depósitos PPD por facturar
// que entraron en el periodo, cuántos había, cuántos ya se facturaron y
// cuántos faltan, por almacén y día a día.
jest.mock('../../../visor/models/CFDI', () => ({ find: jest.fn() }));
jest.mock('../banks/BankMovement.model', () => ({ find: jest.fn() }));
jest.mock('../centros-costo/centros-costo.service', () => ({ resolveBySerieMap: jest.fn() }));
jest.mock('../erp/erp-sync.service', () => ({ obtenerDesglosesCobroAlmacen: jest.fn(), obtenerCuentaTicket: jest.fn() }));

const CFDI = require('../../../visor/models/CFDI');
const BankMovement = require('../banks/BankMovement.model');
const centros = require('../centros-costo/centros-costo.service');
const erp = require('../erp/erp-sync.service');
const est = require('./reportes-por-facturar.estadistica');

const lean = v => ({ select: () => ({ lean: async () => v }) });
const dep = (folio, fecha, importe, links, extra = {}) => ({ folio, fecha: new Date(`${fecha}T00:00:00Z`), deposito: importe, concepto: `DEP ${folio}`, banco: 'BBVA', erpLinks: links, ...extra });
const link = (serie, folioExterno, tipoPago = 'PPD') => ({ serie, folioExterno, tipoPago, saldoPagado: 0 });

beforeEach(() => {
  jest.clearAllMocks();
  centros.resolveBySerieMap.mockResolvedValue({ A0: { id: 1, sucursal: 'CEDIS' }, I0: { id: 2, sucursal: 'ITURBIDE' } });
  BankMovement.find.mockReturnValue(lean([
    dep('100', '2026-10-20', 1000, [link('A0', '1')]),               // facturado en global el 22
    dep('101', '2026-10-20', 500, [link('A0', '2')]),                // falta, debe saldo
    dep('102', '2026-10-21', 300, [link('I0', '3')]),                // facturado a nombre del cliente el 21
    dep('103', '2026-10-21', 700, [link('A0', '4', 'PUE')]),         // PUE: no cuenta
    dep('104', '2026-10-21', 900, [link('A0', '5')]),                // PPD pero ya no "POR FACTURAR": no cuenta
    // Depósito de septiembre identificado el 21-oct (19:00 MX) → entra el 21
    dep('105', '2026-09-15', 200, [link('A0', '6')], { primeraIdentificacionAt: new Date('2026-10-22T01:00:00Z') }),
  ]));
  erp.obtenerDesglosesCobroAlmacen.mockImplementation(async ({ series, folios }) => series.map((s, i) => ({
    serieVenta: s, folioVenta: folios[i], fechaCreacion: '2026-10-01',
    ...({ 'A0|1': { serieFactura: 'A0', folioFactura: 'G1' }, 'I0|3': { serieFactura: 'I0', folioFactura: 'C1' } }[`${s}|${folios[i]}`] ?? {}),
  })));
  erp.obtenerCuentaTicket.mockImplementation(async ({ serie, folio }) => ({
    tipoPago: 'PPD', nombreTipoMovimiento: folio === '5' ? 'FACTURADO' : 'POR FACTURAR',
    saldoActual: folio === '2' ? 120.5 : 0, nombre: `CLIENTE ${serie}${folio}`,
  }));
  CFDI.find.mockReturnValue(lean([
    { serie: 'A0', folio: 'G1', fecha: '2026-10-22T18:00:00Z', receptor: { rfc: 'XAXX010101000' }, source: 'ERP' },
    { serie: 'I0', folio: 'C1', fecha: '2026-10-21T20:00:00Z', receptor: { rfc: 'ABC010101AB1' }, source: 'ERP' },
  ]));
});

test('había / facturados / faltan, por almacén y día a día', async () => {
  const r = await est.calcular({ rfc: 'CCO011113663', fechaInicio: '2026-10-20', fechaFin: '2026-10-22' });

  // Las PUE ni se consultan en Kore.
  expect(erp.obtenerDesglosesCobroAlmacen.mock.calls[0][0].folios).not.toContain('4');

  expect(r.depositos.map(d => [d.idNumo, d.entrada, d.estado])).toEqual([
    ['100', '2026-10-20', 'facturado'], ['101', '2026-10-20', 'falta'],
    ['102', '2026-10-21', 'facturado'], ['105', '2026-10-21', 'falta'],
  ]);
  expect(r.totales).toEqual({
    habia:      { depositos: 4, importe: 2000 },
    facturados: { depositos: 2, importe: 1300, global: 1, cliente: 1 },
    faltan:     { depositos: 2, importe: 700, conSaldo: 1, saldo: 120.5 },
    avance:     50,
    promedioDiasParaFacturar: 1,
  });
  expect(r.porAlmacen.map(a => [a.serie, a.sucursal, a.habia.depositos, a.facturados.depositos])).toEqual([['A0', 'CEDIS', 3, 1], ['I0', 'ITURBIDE', 1, 1]]);
  expect(r.diaADia.map(d => [d.dia, d.nuevos.depositos, d.facturados.depositos, d.pendientes.depositos])).toEqual([
    ['2026-10-20', 2, 0, 2], ['2026-10-21', 2, 1, 3], ['2026-10-22', 0, 1, 2],
  ]);
  const f = r.depositos.find(d => d.idNumo === '100');
  expect(f).toMatchObject({ facturas: ['A0-G1'], tipoFactura: 'global', fechaFactura: '2026-10-22', diasParaFacturar: 2, cliente: 'CLIENTE A01' });
  expect(r.advertencias).toBeNull();
});

test('sucursal elegida: solo sus series', async () => {
  const r = await est.calcular({ rfc: 'CCO011113663', centroCostoId: 2, fechaInicio: '2026-10-20', fechaFin: '2026-10-22' });
  expect(BankMovement.find.mock.calls[0][0]['erpLinks.serie']).toEqual({ $in: ['I0'] });
  expect(r.sucursal).toBe('ITURBIDE');
  expect(r.depositos.map(d => d.idNumo)).toEqual(['102']);
});

test('Kore sin respuesta → advertencia', async () => {
  erp.obtenerCuentaTicket.mockRejectedValueOnce(new Error('429'));
  const r = await est.calcular({ rfc: 'CCO011113663', fechaInicio: '2026-10-20', fechaFin: '2026-10-22' });
  expect(r.advertencias[0]).toMatch(/no respondió 1 remisión/);
});

test('valida rango', () => {
  expect(() => est.validar({ rfc: 'X', fechaInicio: '2026-10-01', fechaFin: '2026-11-15' })).toThrow(/31 días/);
  expect(() => est.validar({ rfc: 'X', fechaInicio: '2026-10-05', fechaFin: '2026-10-01' })).toThrow(/anterior/);
});

test('segundo plano + Excel con RESUMEN, DÍA A DÍA, FACTURADOS y FALTAN', async () => {
  const ExcelJS = require('exceljs');
  const { id } = est.iniciar({ rfc: 'CCO011113663', fechaInicio: '2026-10-20', fechaFin: '2026-10-22' }, { nombre: 'X' });
  for (let i = 0; i < 50 && est.estado(id).estado === 'calculando'; i++) await new Promise(r => setImmediate(r));
  expect(est.estado(id).estado).toBe('listo');

  const { buffer, filename } = await est.excel(id);
  expect(filename).toBe('Estadistica_por_facturar_Todas_2026-10-20_2026-10-22.xlsx');
  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load(buffer);
  expect(wb.worksheets.map(w => w.name)).toEqual(['RESUMEN', 'DÍA A DÍA', 'FACTURADOS', 'FALTAN']);
  expect(wb.getWorksheet('FACTURADOS').rowCount).toBe(3);
  expect(wb.getWorksheet('FALTAN').getRow(2).getCell(7).value).toBe('CLIENTE A02');
});
