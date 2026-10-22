'use strict';

// Reporte "Por facturar" (2026-10-22): facturas pagadas con depósitos de días
// anteriores (equivalente sin póliza de "Venta Sin Cobro") + depósitos
// pendientes de generar factura, con historial.
jest.mock('../../../shared/models/postgres', () => ({ ReportePorFacturar: { create: jest.fn(), findByPk: jest.fn(), findAndCountAll: jest.fn() }, User: {} }));
jest.mock('../../../visor/models/CFDI', () => ({ find: jest.fn() }));
jest.mock('../banks/BankMovement.model', () => ({ find: jest.fn() }));
jest.mock('../centros-costo/centros-costo.service', () => ({ resolveBySerieMap: jest.fn() }));
jest.mock('../cfdi-mapping/cfdi-poliza-generator.service', () => ({ _uuidsPorFechaEfectiva: jest.fn() }));
jest.mock('../cfdi-mapping/mostrador.service', () => ({ detectarMovimientosMostrador: jest.fn(), detectarDepositosPendientesFactura: jest.fn() }));

const { ReportePorFacturar } = require('../../../shared/models/postgres');
const CFDI = require('../../../visor/models/CFDI');
const BankMovement = require('../banks/BankMovement.model');
const centros = require('../centros-costo/centros-costo.service');
const gen = require('../cfdi-mapping/cfdi-poliza-generator.service');
const mostrador = require('../cfdi-mapping/mostrador.service');
const service = require('./reportes-por-facturar.service');

const lean = v => ({ select: () => ({ lean: async () => v }) });
const GLOBAL = '1C02D46C-FF4C-411E-BF11-7960AB39BA9C';   // A0-261009872, 20-oct
const MISMO_DIA = 'AAAAAAAA-0000-0000-0000-000000000001'; // cobrada el mismo día
const OTRA_SUC = 'BBBBBBBB-0000-0000-0000-000000000002';  // serie B0

beforeEach(() => {
  jest.clearAllMocks();
  centros.resolveBySerieMap.mockResolvedValue({ A0: { id: 1, sucursal: 'CEDIS' }, B0: { id: 2, sucursal: 'HIDALGO' } });
  gen._uuidsPorFechaEfectiva.mockImplementation(async ({ fechaInicio }) => new Set(fechaInicio === '2026-10-20' ? [GLOBAL, MISMO_DIA, OTRA_SUC] : []));
  CFDI.find.mockReturnValue(lean([
    { uuid: GLOBAL, serie: 'A0', folio: '261009872', fecha: '2026-10-20T17:22:35Z', total: 229702.95, receptor: { rfc: 'XAXX010101000' }, source: 'ERP' },
    { uuid: MISMO_DIA, serie: 'A0', folio: '261009900', fecha: '2026-10-20T18:00:00Z', total: 100, receptor: { rfc: 'XAXX010101000' }, source: 'ERP' },
    { uuid: OTRA_SUC, serie: 'B0', folio: '1', fecha: '2026-10-20T18:00:00Z', total: 100, source: 'ERP' },
  ]));
  BankMovement.find.mockReturnValue(lean([
    { fecha: new Date('2026-10-13T00:00:00Z'), erpLinks: [{ folioFiscal: GLOBAL }] },
    { fecha: new Date('2026-10-20T00:00:00Z'), erpLinks: [{ folioFiscal: MISMO_DIA.toLowerCase() }] },
  ]));
  mostrador.detectarMovimientosMostrador.mockResolvedValue([{ uuid: GLOBAL, esGlobal: true, depositos: [{ importe: 5000 }] }]);
  mostrador.detectarDepositosPendientesFactura.mockResolvedValue({ depositos: [{ importe: 146077.63, idNumo: '053966' }], remisionesSinRespuesta: 0 });
  ReportePorFacturar.create.mockResolvedValue({ id: 7 });
  ReportePorFacturar.findByPk.mockImplementation(async () => ({ toJSON: () => ({ id: 7, datos: ReportePorFacturar.create.mock.calls[0][0].datos }) }));
});

test('solo facturas de la sucursal con algún depósito de días anteriores; guarda quién lo creó', async () => {
  const r = await service.generar({ rfc: 'CCO011113663', centroCostoId: 1, fechaInicio: '2026-10-20' }, { dbId: '5' });

  const [{ cfdis, movimientos }] = mostrador.detectarMovimientosMostrador.mock.calls[0];
  expect(movimientos).toEqual([{ tipoOrigen: 'Venta Sin Cobro', debe: 1, cfdiUuid: GLOBAL }]);
  expect(cfdis.map(c => c.folio)).toEqual(['261009872']);
  expect(mostrador.detectarDepositosPendientesFactura).toHaveBeenCalledWith({ rfc: 'CCO011113663', series: ['A0'], fechaInicio: '2026-10-20', fechaFin: '2026-10-20' });

  const guardado = ReportePorFacturar.create.mock.calls[0][0];
  expect(guardado).toMatchObject({ rfc: 'CCO011113663', centroCostoId: 1, sucursal: 'CEDIS', fechaInicio: '2026-10-20', fechaFin: '2026-10-20', creadoPorId: 5, advertencias: null });
  expect(guardado.resumen).toEqual({
    facturasGlobales:    { facturas: 1, depositos: 1, importe: 5000 },
    ppdFacturadoCliente: { facturas: 0, depositos: 0, importe: 0 },
    depositosPendientes: { depositos: 1, importe: 146077.63 },
  });
  expect(r.tablas.facturasGlobales).toHaveLength(1);
  expect(r.tablas.depositosPendientes).toHaveLength(1);
});

test('rango: consulta cada día y valida fechas', async () => {
  await service.generar({ rfc: 'X', fechaInicio: '2026-10-19', fechaFin: '2026-10-21' }, null);
  expect(gen._uuidsPorFechaEfectiva.mock.calls.map(c => c[0].fechaInicio)).toEqual(['2026-10-19', '2026-10-20', '2026-10-21']);
  expect(ReportePorFacturar.create.mock.calls[0][0]).toMatchObject({ sucursal: 'Todas', centroCostoId: null, creadoPorId: null });

  await expect(service.generar({ rfc: 'X', fechaInicio: '2026-10-21', fechaFin: '2026-10-19' })).rejects.toThrow('anterior');
  await expect(service.generar({ rfc: 'X', fechaInicio: '2026-09-01', fechaFin: '2026-10-21' })).rejects.toThrow('31 días');
  await expect(service.generar({ rfc: 'X', fechaInicio: '21/10/2026' })).rejects.toThrow('Fechas inválidas');
});

test('si Kore no respondió remisiones, queda advertencia en el reporte', async () => {
  mostrador.detectarDepositosPendientesFactura.mockResolvedValue({ depositos: [], remisionesSinRespuesta: 3 });
  await service.generar({ rfc: 'X', centroCostoId: 1, fechaInicio: '2026-10-20' }, null);
  expect(ReportePorFacturar.create.mock.calls[0][0].advertencias[0]).toMatch('no respondió 3');
});

test('historial paginado: 5 por página, más recientes primero', async () => {
  ReportePorFacturar.findAndCountAll.mockResolvedValue({ rows: [{ id: 12 }], count: 12 });
  const r = await service.listar({ rfc: 'CCO011113663', page: '3' });
  expect(r).toEqual({ data: [{ id: 12 }], total: 12, page: 3, limit: 5 });
  expect(ReportePorFacturar.findAndCountAll.mock.calls[0][0]).toMatchObject({
    where: { rfc: 'CCO011113663' }, limit: 5, offset: 10, order: [['createdAt', 'DESC'], ['id', 'DESC']],
  });
});
