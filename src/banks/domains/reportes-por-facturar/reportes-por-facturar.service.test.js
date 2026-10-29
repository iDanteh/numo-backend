'use strict';

// Reporte "Por facturar" (2026-10-22): facturas pagadas con depósitos de días
// anteriores (equivalente sin póliza de "Venta Sin Cobro") + depósitos
// pendientes de generar factura, con historial.
jest.mock('../../../shared/models/postgres', () => ({ ReportePorFacturar: { create: jest.fn(), findByPk: jest.fn(), findAndCountAll: jest.fn() }, User: {} }));
jest.mock('../../../visor/models/CFDI', () => ({ find: jest.fn() }));
jest.mock('../banks/BankMovement.model', () => ({ find: jest.fn() }));
jest.mock('../centros-costo/centros-costo.service', () => ({ resolveBySerieMap: jest.fn() }));
jest.mock('../cfdi-mapping/cfdi-poliza-generator.service', () => ({ _uuidsPorFechaEfectiva: jest.fn() }));
jest.mock('../cfdi-mapping/mostrador.service', () => ({ detectarMovimientosMostrador: jest.fn(), detectarDepositosPendientesFactura: jest.fn(), detectarDepositosEfectivoSinAsociar: jest.fn(), FORMAS_PAGO: ['efectivo', 'depositoEfectivo', 'transferencia', 'tarjeta', 'cheque', 'abonos', 'otras'] }));

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
  mostrador.detectarMovimientosMostrador.mockResolvedValue([{ uuid: GLOBAL, esGlobal: true, depositos: [{ importe: 5000, ppdPorFacturar: true }, { importe: 900, ppdPorFacturar: false }] }]);
  mostrador.detectarDepositosPendientesFactura.mockResolvedValue({ depositos: [{ importe: 146077.63, idNumo: '053966' }], depositosConSaldo: [{ importe: 1147.04, saldo: 47.79, idNumo: '053960' }], remisionesSinRespuesta: 0 });
  mostrador.detectarDepositosEfectivoSinAsociar.mockResolvedValue([{ importe: 1618.95, idNumo: '051499' }]);
  ReportePorFacturar.create.mockResolvedValue({ id: 7 });
  ReportePorFacturar.findByPk.mockImplementation(async () => ({ toJSON: () => ({ id: 7, datos: ReportePorFacturar.create.mock.calls[0][0].datos }) }));
});

test('solo facturas de la sucursal con algún depósito de días anteriores; guarda quién lo creó', async () => {
  const r = await service.generar({ rfc: 'CCO011113663', centroCostoId: 1, fechaInicio: '2026-10-20' }, { dbId: '5' });

  const [{ cfdis, movimientos }] = mostrador.detectarMovimientosMostrador.mock.calls[0];
  expect(movimientos).toEqual([{ tipoOrigen: 'Venta Sin Cobro', debe: 1, cfdiUuid: GLOBAL }]);
  expect(cfdis.map(c => c.folio)).toEqual(['261009872']);
  expect(mostrador.detectarDepositosPendientesFactura).toHaveBeenCalledWith({ rfc: 'CCO011113663', series: ['A0'], fechaInicio: '2026-10-20', fechaFin: '2026-10-20', incluirIdentificados: true });

  const guardado = ReportePorFacturar.create.mock.calls[0][0];
  expect(guardado).toMatchObject({ rfc: 'CCO011113663', centroCostoId: 1, sucursal: 'CEDIS', fechaInicio: '2026-10-20', fechaFin: '2026-10-20', creadoPorId: 5, advertencias: null });
  expect(guardado.resumen).toEqual({
    facturasGlobales:    { facturas: 1, depositos: 1, importe: 5000 },
    ppdFacturadoCliente: { facturas: 0, depositos: 0, importe: 0 },
    depositosPendientes: { depositos: 1, importe: 146077.63 },
    depositosEfectivo:   { depositos: 1, importe: 1618.95 },
    pendientesPago:      { depositos: 1, importe: 1147.04 },
  });
  expect(r.tablas.pendientesPago).toEqual([{ importe: 1147.04, saldo: 47.79, idNumo: '053960' }]);
  expect(mostrador.detectarDepositosEfectivoSinAsociar).toHaveBeenCalledWith({ fechaInicio: '2026-10-20', fechaFin: '2026-10-20' });
  expect(r.tablas.depositosEfectivo).toEqual([{ importe: 1618.95, idNumo: '051499' }]);
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
    where: { rfc: 'CCO011113663', eliminadoEn: null }, limit: 5, offset: 10, order: [['createdAt', 'DESC'], ['id', 'DESC']],
  });
});

test('eliminar: borrado lógico con quién lo eliminó; ya no se puede abrir', async () => {
  const update = jest.fn();
  ReportePorFacturar.findByPk.mockResolvedValueOnce({ id: 7, eliminadoEn: null, update });
  expect(await service.eliminar(7, { dbId: '5' })).toEqual({ ok: true, id: 7 });
  expect(update.mock.calls[0][0]).toMatchObject({ eliminadoPorId: 5, eliminadoEn: expect.any(Date) });

  ReportePorFacturar.findByPk.mockResolvedValue({ id: 7, eliminadoEn: new Date(), update, toJSON: () => ({}) });
  await expect(service.obtener(7)).rejects.toThrow();
  await expect(service.eliminar(7, { dbId: '5' })).rejects.toThrow();
});

test('actualizar: recalcula con la misma sucursal y fechas y reemplaza los datos del mismo reporte', async () => {
  const update = jest.fn();
  ReportePorFacturar.findByPk.mockReset();
  ReportePorFacturar.findByPk
    .mockResolvedValueOnce({ id: 7, rfc: 'CCO011113663', centroCostoId: 1, fechaInicio: '2026-10-20', fechaFin: '2026-10-20', eliminadoEn: null, update })
    .mockImplementation(async () => ({ toJSON: () => ({ id: 7, datos: update.mock.calls[0][0].datos }) }));
  const r = await service.actualizar(7, { dbId: '9' });
  expect(ReportePorFacturar.create).not.toHaveBeenCalled();
  expect(mostrador.detectarDepositosPendientesFactura).toHaveBeenCalledWith({ rfc: 'CCO011113663', series: ['A0'], fechaInicio: '2026-10-20', fechaFin: '2026-10-20', incluirIdentificados: true });
  const cambios = update.mock.calls[0][0];
  expect(cambios).toMatchObject({ sucursal: 'CEDIS', actualizadoPorId: 9, advertencias: null });
  expect(cambios.actualizadoEn).toBeInstanceOf(Date);
  expect(cambios.datos.depositosPendientesFactura.map(d => d.idNumo)).toEqual(['053966']);
  expect(r.id).toBe(7);
});

test('actualizar un reporte eliminado → 404', async () => {
  ReportePorFacturar.findByPk.mockReset();
  ReportePorFacturar.findByPk.mockResolvedValue({ id: 7, eliminadoEn: new Date() });
  await expect(service.actualizar(7, {})).rejects.toThrow();
});

// 2026-10-28: 5 pestañas, cada una con su Excel — por almacén (serie de la
// remisión de cada depósito), agrupadas por cliente y con formas de pago.
describe('secciones y Excel por pestaña', () => {
  const ExcelJS = require('exceljs');
  const fp = (efectivo, transferencia) => ({ efectivo, tarjeta: 0, transferencia, cheque: 0, otras: 0, mixto: efectivo > 0 && transferencia > 0 });
  const dep = (cliente, importe, rem, formasPago) => ({ fecha: '2026-10-20T00:00:00Z', descripcion: 'SPEI', importe, aplicado: importe, cliente, remisiones: [rem], ppdPorFacturar: true, formasPago });
  const cargar = () => {
    ReportePorFacturar.findByPk.mockReset();
    ReportePorFacturar.findByPk.mockResolvedValue({ id: 3, toJSON: () => ({
      id: 3, sucursal: 'Todas', fechaInicio: '2026-10-20', fechaFin: '2026-10-20',
      datos: {
        // El depósito de GAMA es de una remisión I0 → va al almacén I0.
        movimientosMostrador: [{ serie: 'A0', folio: '1', esGlobal: true, totalFactura: 999, fechaFactura: '2026-10-21T18:00:00Z', depositos: [
          dep('ZETA', 10, 'A0-1', fp(4, 6)), dep('ALFA', 20, 'A0-2', fp(0, 20)), dep('ZETA', 5, 'A0-3', fp(5, 0)), dep('GAMA', 50, 'I0-7', fp(0, 50)),
        ] }],
        depositosPendientesFactura: [dep('', 7, 'I0-9', null), dep('BETA', 3, 'I0-8', fp(1, 2)), dep('ALFA', 1, 'A0-4', fp(1, 0))],
        depositosPendientesPago: [{ ...dep('OMEGA', 100, 'A0-5', fp(30, 70)), saldo: 12 }],
        depositosEfectivoSinAsociar: [{ fecha: '2026-10-20T00:00:00Z', descripcion: 'DEP EFECTIVO', importe: 800, banco: 'BBVA', ficha: '1', idNumo: '051499' }],
      },
    }) });
  };

  test('obtener: secciones por almacén, clientes ordenados y total de efectivo por global', async () => {
    cargar();
    const r = await service.obtener(3);
    const s = r.secciones;
    expect(s.global.map(a => [a.serie, a.sucursal])).toEqual([['A0', 'CEDIS'], ['I0', null]]);
    const g = s.global[0].bloques[0];
    expect(g).toMatchObject({ factura: 'A0-1', totalFactura: 999 });
    expect(g.grupos.map(x => x.cliente)).toEqual(['ALFA', 'ZETA']);
    expect(g.grupos[1].totales.formas).toMatchObject({ efectivo: 9, transferencia: 6 });
    expect(g.totales.formas.efectivo).toBe(9);
    expect(s.global[1].bloques[0].grupos[0].cliente).toBe('GAMA');
    // Pendiente: una sola tabla por almacén; sin cliente al final.
    expect(s.pendiente.map(a => a.serie)).toEqual(['A0', 'I0']);
    expect(s.pendiente[1].bloques).toHaveLength(1);
    expect(s.pendiente[1].bloques[0].grupos.map(x => x.cliente)).toEqual(['BETA', 'CLIENTE NO IDENTIFICADO']);
    expect(s.pendientesPago[0].bloques[0].totales).toMatchObject({ saldo: 12, formas: { efectivo: 30 } });
    expect(s.efectivo).toHaveLength(1);
  });

  test('excel de una pestaña: una hoja por almacén; sin sección, todas', async () => {
    cargar();
    const leer = async seccion => { const wb = new ExcelJS.Workbook(); await wb.xlsx.load((await service.excel(3, seccion)).buffer); return wb; };
    const global = await leer('global');
    expect(global.worksheets.map(w => w.name)).toEqual(['A0 - CEDIS', 'I0']);
    const textos = global.getWorksheet('A0 - CEDIS').getColumn(2).values.filter(Boolean);
    expect(textos).toEqual(expect.arrayContaining(['Subtotal ALFA', 'Subtotal ZETA', 'TOTAL', 'Total factura', 'EFECTIVO DE LA FACTURA A0-1']));
    expect((await leer('efectivo')).worksheets.map(w => w.name)).toEqual(['DEPÓSITOS EN EFECTIVO']);
    expect((await leer()).worksheets.map(w => w.name)).toEqual([
      'GLOBAL - A0 - CEDIS', 'GLOBAL - I0', 'PENDIENTE - A0 - CEDIS', 'PENDIENTE - I0', 'PEND PAGO - A0 - CEDIS', 'EFECTIVO',
    ]);
    const { filename } = await service.excel(3, 'pendientesPago');
    expect(filename).toBe('Por_facturar_PEND_PAGO_Todas_2026-10-20.xlsx');
    await expect(service.excel(3, 'otra')).rejects.toThrow();
  });
});
