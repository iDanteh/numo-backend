'use strict';

// Reporte "Por facturar" (2026-10-22): facturas pagadas con depósitos de días
// anteriores (equivalente sin póliza de "Venta Sin Cobro") + depósitos
// pendientes de generar factura, con historial.
jest.mock('../../../shared/models/postgres', () => ({ ReportePorFacturar: { create: jest.fn(), findByPk: jest.fn(), findAndCountAll: jest.fn(), update: jest.fn(async () => [1]) }, User: {} }));
jest.mock('../../../visor/models/CFDI', () => ({ find: jest.fn() }));
jest.mock('../banks/BankMovement.model', () => ({ find: jest.fn() }));
jest.mock('../centros-costo/centros-costo.service', () => ({ resolveBySerieMap: jest.fn() }));
jest.mock('../cfdi-mapping/cfdi-poliza-generator.service', () => ({ _uuidsPorFechaEfectiva: jest.fn() }));
jest.mock('./reportes-por-facturar.facturas-kore', () => ({ traerFacturasRecientes: jest.fn(async () => ({ dias: [], nuevas: 0, error: null })) }));
jest.mock('./reportes-por-facturar.cobros-caja', () => ({ cobrosCajaDeGlobales: jest.fn(async () => ({ porFactura: [], sinRespuesta: 0 })) }));
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
  ReportePorFacturar.findByPk.mockImplementation(async () => ({ toJSON: () => ({ id: 7, datos: terminado()?.datos ?? null }) }));
});

// 2026-10-29: generar / actualizar guardan el reporte "calculando" y lo
// completan en segundo plano — `terminado()` = lo que se guardó al terminar.
const terminado = () => ReportePorFacturar.update.mock.calls.map(c => c[0]).find(x => x.estado === 'listo' || x.estado === 'error');
const generarYEsperar = async (...args) => { const r = await service.generar(...args); await service._esperar(r.id ?? 7); return service.obtener(7); };

test('solo facturas de la sucursal con algún depósito de días anteriores; guarda quién lo creó', async () => {
  const inicial = await service.generar({ rfc: 'CCO011113663', centroCostoId: 1, fechaInicio: '2026-10-20' }, { dbId: '5' });
  // Se guarda de inmediato, sin datos, calculando.
  expect(ReportePorFacturar.create.mock.calls[0][0]).toMatchObject({ estado: 'calculando', datos: null, sucursal: 'CEDIS', fechaFin: '2026-10-20', creadoPorId: 5 });
  await service._esperar(7);
  const r = await service.obtener(7);
  expect(inicial.id).toBe(7);

  const [{ cfdis, movimientos }] = mostrador.detectarMovimientosMostrador.mock.calls[0];
  expect(movimientos).toEqual([{ tipoOrigen: 'Venta Sin Cobro', debe: 1, cfdiUuid: GLOBAL }]);
  expect(cfdis.map(c => c.folio)).toEqual(['261009872']);
  expect(mostrador.detectarDepositosPendientesFactura).toHaveBeenCalledWith({ rfc: 'CCO011113663', series: ['A0'], fechaInicio: '2026-10-20', fechaFin: '2026-10-20', incluirIdentificados: true });

  expect(ReportePorFacturar.create.mock.calls[0][0]).toMatchObject({ rfc: 'CCO011113663', centroCostoId: 1, fechaInicio: '2026-10-20' });
  const guardado = terminado();
  expect(guardado).toMatchObject({ estado: 'listo', sucursal: 'CEDIS', advertencias: null, error: null });
  expect(ReportePorFacturar.update.mock.calls.some(c => /Pendientes de generar factura/.test(c[0].avance ?? ''))).toBe(true);
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
  await generarYEsperar({ rfc: 'X', fechaInicio: '2026-10-19', fechaFin: '2026-10-21' }, null);
  expect(gen._uuidsPorFechaEfectiva.mock.calls.map(c => c[0].fechaInicio)).toEqual(['2026-10-19', '2026-10-20', '2026-10-21']);
  expect(ReportePorFacturar.create.mock.calls[0][0]).toMatchObject({ sucursal: 'Todas', centroCostoId: null, creadoPorId: null });

  await expect(service.generar({ rfc: 'X', fechaInicio: '2026-10-21', fechaFin: '2026-10-19' })).rejects.toThrow('anterior');
  await expect(service.generar({ rfc: 'X', fechaInicio: '2026-09-01', fechaFin: '2026-10-21' })).rejects.toThrow('31 días');
  await expect(service.generar({ rfc: 'X', fechaInicio: '21/10/2026' })).rejects.toThrow('Fechas inválidas');
});

test('si Kore no respondió remisiones, queda advertencia en el reporte', async () => {
  mostrador.detectarDepositosPendientesFactura.mockResolvedValue({ depositos: [], remisionesSinRespuesta: 3 });
  await generarYEsperar({ rfc: 'X', centroCostoId: 1, fechaInicio: '2026-10-20' }, null);
  expect(terminado().advertencias[0]).toMatch('no respondió 3');
});

test('si el cálculo truena, el reporte queda en error con el mensaje', async () => {
  mostrador.detectarDepositosPendientesFactura.mockRejectedValue(new Error('Kore caído'));
  await generarYEsperar({ rfc: 'X', centroCostoId: 1, fechaInicio: '2026-10-20' }, null);
  expect(terminado()).toMatchObject({ estado: 'error', error: 'Kore caído' });
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
    .mockResolvedValueOnce({ id: 7, rfc: 'CCO011113663', centroCostoId: 1, fechaInicio: '2026-10-20', fechaFin: '2026-10-20', eliminadoEn: null, estado: 'listo', update })
    .mockImplementation(async () => ({ toJSON: () => ({ id: 7, datos: terminado()?.datos ?? null }) }));
  const r = await service.actualizar(7, { dbId: '9' });
  // Primero se marca "calculando" (los datos anteriores se siguen viendo).
  expect(update.mock.calls[0][0]).toMatchObject({ estado: 'calculando', error: null });
  await service._esperar(7);
  expect(ReportePorFacturar.create).not.toHaveBeenCalled();
  expect(mostrador.detectarDepositosPendientesFactura).toHaveBeenCalledWith({ rfc: 'CCO011113663', series: ['A0'], fechaInicio: '2026-10-20', fechaFin: '2026-10-20', incluirIdentificados: true });
  const cambios = terminado();
  expect(cambios).toMatchObject({ estado: 'listo', sucursal: 'CEDIS', actualizadoPorId: 9, advertencias: null });
  expect(cambios.actualizadoEn).toBeInstanceOf(Date);
  expect(cambios.datos.depositosPendientesFactura.map(d => d.idNumo)).toEqual(['053966']);
  expect(r.id).toBe(7);
});

test('actualizar mientras ya se está calculando → 409', async () => {
  ReportePorFacturar.findByPk.mockReset();
  ReportePorFacturar.findByPk.mockResolvedValue({ id: 7, eliminadoEn: null, estado: 'calculando' });
  await expect(service.actualizar(7, {})).rejects.toThrow('ya se está calculando');
});

test('actualizar un reporte eliminado → 404', async () => {
  ReportePorFacturar.findByPk.mockReset();
  ReportePorFacturar.findByPk.mockResolvedValue({ id: 7, eliminadoEn: new Date() });
  await expect(service.actualizar(7, {})).rejects.toThrow();
});

// 2026-10-29: formato del Excel "MOSTRADOR 2026" del usuario — MOSTRADOR (un
// bloque por global + pendientes), REF. por almacén, FACT ESPECIALES, X FAC
// CON FICHA DE DEP y DEPÓSITOS EN EFECTIVO, agrupados por cliente.
describe('secciones y Excel por pestaña', () => {
  const ExcelJS = require('exceljs');
  const dep = (cliente, importe, rem, extra = {}) => ({ fecha: '2026-10-20T00:00:00Z', descripcion: 'SPEI', importe, aplicado: importe, banco: 'BBVA', cliente, remisiones: [rem], idNumo: '1', ppdPorFacturar: true, ...extra });
  const cargar = () => {
    ReportePorFacturar.findByPk.mockReset();
    ReportePorFacturar.findByPk.mockResolvedValue({ id: 3, toJSON: () => ({
      id: 3, sucursal: 'Todas', fechaInicio: '2026-10-20', fechaFin: '2026-10-22',
      datos: {
        movimientosMostrador: [
          { uuid: 'G2', serie: 'I0', folio: '9', esGlobal: true, totalFactura: 50, fechaFactura: '2026-10-22T18:00:00Z', depositos: [dep('GAMA', 50, 'I0-7')] },
          { uuid: 'G1', serie: 'A0', folio: '1', esGlobal: true, totalFactura: 999, fechaFactura: '2026-10-21T18:00:00Z', depositos: [
            dep('ZETA', 10, 'A0-1'), dep('ALFA', 20, 'A0-2'), dep('ZETA', 5, 'A0-3'), dep('NO PPD', 99, 'A0-9', { ppdPorFacturar: false }),
          ] },
          { uuid: 'C1', serie: 'B0', folio: '50', esGlobal: false, receptorNombre: 'GRUPO CHILI GUAJILI S.A. DE C.V.', fechaFactura: '2026-10-20T18:00:00Z', depositos: [dep('GRUPO CHILI GUAJILI', 30, 'B0-5')] },
          { uuid: 'C2', serie: 'B0', folio: '51', esGlobal: false, receptorNombre: 'CRISLA', fechaFactura: '2026-10-20T19:00:00Z', depositos: [dep('MARIO VASQUEZ', 40, 'B0-6')] },
        ],
        // Cobro de caja de Kore en la global A0-1 y una global solo con efectivo.
        cobrosCajaGlobales: [
          { uuid: 'G1', serie: 'A0', folio: '1', depositos: [dep('ALFA', 1500, 'A0-2', { banco: 'EFECTIVO', idNumo: 'S/R', deKore: true })] },
          { uuid: 'G3', serie: 'A0', folio: '3', fechaFactura: '2026-10-20T18:00:00Z', depositos: [dep('BETA', 242.06, 'A0-8', { banco: 'SALDO A FAVOR', idNumo: 'S/R' })] },
        ],
        depositosPendientesFactura: [dep('', 7, 'I0-9'), dep('BETA', 3, 'I0-8')],
        depositosPendientesPago: [{ ...dep('OLGA PEREZ', 100, 'A0-5'), saldo: 12 }],
        depositosEfectivoSinAsociar: [{ fecha: '2026-10-20T00:00:00Z', descripcion: 'DEP EFECTIVO', importe: 800, banco: 'BBVA', ficha: '1', idNumo: '051499' }],
      },
    }) });
  };

  test('obtener: MOSTRADOR por fecha de la global con cobros de caja, pendientes al final; REF. y especiales', async () => {
    cargar();
    const s = (await service.obtener(3)).secciones;
    expect(s.mostrador.map(b => [b.tipo, b.factura ?? null])).toEqual([['global', 'A0-3'], ['global', 'A0-1'], ['global', 'I0-9'], ['pendiente', null]]);
    const a01 = s.mostrador[1];
    expect(a01.grupos.map(g => [g.cliente, g.total])).toEqual([['ALFA', 1520], ['ZETA', 15]]);
    expect(a01.total).toBe(1535);
    expect(a01.grupos[0].depositos.map(d => d.banco)).toEqual(['BBVA', 'EFECTIVO']);
    expect(s.mostrador[3].grupos.map(g => g.cliente)).toEqual(['BETA', 'CLIENTE NO IDENTIFICADO']);
    expect(s.ref.map(a => [a.serie, a.bloques.length])).toEqual([['B0', 1]]);
    expect(s.ref[0].bloques[0].grupos[0].depositos[0]).toMatchObject({ factura: 'B0-50', clienteFacturado: 'GRUPO CHILI GUAJILI S.A. DE C.V.' });
    expect(s.especiales).toHaveLength(1);
    expect(s.especiales[0].grupos[0].depositos[0]).toMatchObject({ cliente: 'MARIO VASQUEZ', clienteFacturado: 'CRISLA' });
    expect(s.ficha.total).toBe(100);
    expect(s.efectivo).toHaveLength(1);
  });

  test('excel: hojas MOSTRADOR, REF. por almacén, FACT ESPECIALES, X FAC CON FICHA y efectivo', async () => {
    cargar();
    const leer = async seccion => { const wb = new ExcelJS.Workbook(); await wb.xlsx.load((await service.excel(3, seccion)).buffer); return wb; };
    const todo = await leer();
    expect(todo.worksheets.map(w => w.name)).toEqual(['MOSTRADOR', 'REF. B0 - HIDALGO', 'FACT ESPECIALES', 'X FAC CON FICHA DE DEP', 'DEPÓSITOS EN EFECTIVO']);
    const ws = todo.getWorksheet('MOSTRADOR');
    // Encabezado del bloque de la global A0-1 y sus columnas, como el Excel del usuario.
    const fila = n => ws.getRow(n).values.slice(1);
    const enc = ws.getColumn(6).values.indexOf('A0-1');
    expect(fila(enc)).toEqual([expect.any(Date), undefined, undefined, undefined, 'A0', 'A0-1', 'ID NUMO']);
    expect(fila(enc + 1)).toEqual(['FECHA', 'DESCRIPCIÓN', 'IMPORTE', 'BANCO', 'REM', 'CLIENTE']);
    expect(ws.getColumn(2).values).toEqual(expect.arrayContaining(['Subtotal ALFA', 'TOTAL', 'PENDIENTE GENERAR FACTURA']));
    const ref = todo.getWorksheet('REF. B0 - HIDALGO').getRow(2).values.slice(1);
    expect(ref).toEqual(['FECHA', 'DESCRIPCIÓN', 'IMPORTE', 'BANCO', 'B0', 'B0 ORIGEN', 'CTE', 'CTE FACTURADO', 'ID DE PAGO']);
    expect((await leer('ficha')).worksheets.map(w => w.name)).toEqual(['X FAC CON FICHA DE DEP']);
    const { filename } = await service.excel(3, 'especiales');
    expect(filename).toBe('Por_facturar_FACT_ESPECIALES_Todas_2026-10-20_2026-10-22.xlsx');
    await expect(service.excel(3, 'otra')).rejects.toThrow();
  });
});
