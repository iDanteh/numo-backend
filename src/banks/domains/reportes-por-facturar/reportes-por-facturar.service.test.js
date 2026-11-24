'use strict';

// Reporte "Por facturar" (2026-10-22): facturas pagadas con depósitos de días
// anteriores (equivalente sin póliza de "Venta Sin Cobro") + depósitos
// pendientes de generar factura, con historial.
// 2026-11-24: generar revisa en Postgres (candado + findOne) — sin uno igual calculando, por default.
jest.mock('../../../shared/models/postgres', () => ({ ReportePorFacturar: { create: jest.fn(), findByPk: jest.fn(), findOne: jest.fn(async () => null), findAndCountAll: jest.fn(), update: jest.fn(async () => [1]), sequelize: { transaction: jest.fn(async fn => fn({})), query: jest.fn(async () => []) } }, User: {} }));
jest.mock('../../../visor/models/CFDI', () => ({ find: jest.fn() }));
jest.mock('../banks/BankMovement.model', () => ({ find: jest.fn() }));
jest.mock('../centros-costo/centros-costo.service', () => ({ resolveBySerieMap: jest.fn() }));
jest.mock('../cfdi-mapping/cfdi-poliza-generator.service', () => ({ _uuidsPorFechaEfectiva: jest.fn() }));
jest.mock('./reportes-por-facturar.otra-caja', () => ({ cobrosEnOtraCaja: jest.fn(async () => ({ cobros: [], sinRespuesta: 0 })) }));
jest.mock('./reportes-por-facturar.abonos', () => ({ agregarHistorialAbonos: jest.fn(async () => ({ sinRespuesta: 0 })) }));
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
  expect(ReportePorFacturar.create.mock.calls[0][0].calculoInicio).toBeInstanceOf(Date);
  await service._esperar(7);
  // Hora de fin guardada aparte (2026-11-24): updated_at se pisa al eliminar.
  expect(terminado().calculoFin).toBeInstanceOf(Date);
  const r = await service.obtener(7);
  expect(inicial.id).toBe(7);

  const [{ cfdis, movimientos }] = mostrador.detectarMovimientosMostrador.mock.calls[0];
  expect(movimientos).toEqual([{ tipoOrigen: 'Venta Sin Cobro', debe: 1, cfdiUuid: GLOBAL }]);
  expect(cfdis.map(c => c.folio)).toEqual(['261009872']);
  expect(mostrador.detectarDepositosPendientesFactura).toHaveBeenCalledWith({ rfc: 'CCO011113663', series: ['A0'], fechaInicio: '2026-10-20', fechaFin: '2026-10-20', incluirIdentificados: true, acumuladoDesde: '2026-01-01' });

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

// 2026-11-24 (reporte 93): Kore 429 → se espera y se repite el paso, hasta 2 rondas.
describe('rondas de reintento cuando Kore no responde', () => {
  const cobrosCaja = require('./reportes-por-facturar.cobros-caja');
  const otraCaja = require('./reportes-por-facturar.otra-caja');
  const abonos = require('./reportes-por-facturar.abonos');
  const params = { rfc: 'X', centroCostoId: 1, fechaInicio: '2026-10-20' };
  beforeAll(() => service._setEsperaRonda(0));
  afterAll(() => service._setEsperaRonda(11000));

  test('sin consultas fallidas no repite ningún paso', async () => {
    await generarYEsperar(params, null);
    expect(cobrosCaja.cobrosCajaDeGlobales).toHaveBeenCalledTimes(1);
    expect(otraCaja.cobrosEnOtraCaja).toHaveBeenCalledTimes(1);
    expect(terminado().advertencias).toBeNull();
  });

  test('cobros de caja: falla 1 y a la 1ª ronda responde → sin advertencia', async () => {
    cobrosCaja.cobrosCajaDeGlobales
      .mockResolvedValueOnce({ porFactura: [], sinRespuesta: 1 })
      .mockResolvedValueOnce({ porFactura: [], sinRespuesta: 0 });
    await generarYEsperar(params, null);
    expect(cobrosCaja.cobrosCajaDeGlobales).toHaveBeenCalledTimes(2);
    expect(terminado().advertencias).toBeNull();
    // Se avisa en el avance que está reintentando.
    expect(ReportePorFacturar.update.mock.calls.map(c => c[0].avance).filter(Boolean).join(' | ')).toMatch('reintentando 1/2');
  });

  test('otra caja: sigue fallando tras 2 rondas → 3 intentos en total y queda la advertencia', async () => {
    otraCaja.cobrosEnOtraCaja.mockResolvedValue({ cobros: [], soloOtraCaja: [], sinRespuesta: 2 });
    await generarYEsperar(params, null);
    expect(otraCaja.cobrosEnOtraCaja).toHaveBeenCalledTimes(3);
    expect(terminado().advertencias.join(' ')).toMatch('no respondió 2 remisión(es) al buscar cobros en la caja de otro almacén');
    otraCaja.cobrosEnOtraCaja.mockResolvedValue({ cobros: [], sinRespuesta: 0 });
  });

  test('historial de abonos no se repite (volvería a preguntar todo a Kore)', async () => {
    abonos.agregarHistorialAbonos.mockResolvedValueOnce({ sinRespuesta: 4 });
    await generarYEsperar(params, null);
    expect(abonos.agregarHistorialAbonos).toHaveBeenCalledTimes(1);
    expect(terminado().advertencias.join(' ')).toMatch('historial de abonos de 4');
  });
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
  expect(update.mock.calls[0][0]).toMatchObject({ estado: 'calculando', error: null, calculoFin: null });
  expect(update.mock.calls[0][0].calculoInicio).toBeInstanceOf(Date);
  await service._esperar(7);
  expect(ReportePorFacturar.create).not.toHaveBeenCalled();
  expect(mostrador.detectarDepositosPendientesFactura).toHaveBeenCalledWith({ rfc: 'CCO011113663', series: ['A0'], fechaInicio: '2026-10-20', fechaFin: '2026-10-20', incluirIdentificados: true, acumuladoDesde: '2026-01-01' });
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

// 2026-11-24: reportes 87 y 88 creados con 0.2 s de diferencia (mismo día y sucursal).
test('generar dos veces el mismo reporte a la vez → un solo reporte; ya terminado → uno nuevo', async () => {
  const params = { rfc: 'CCO011113663', centroCostoId: 1, fechaInicio: '2026-10-20', fechaFin: '2026-10-20' };
  const [a, b] = await Promise.all([service.generar(params, { dbId: '8' }), service.generar({ ...params }, { dbId: '8' })]);
  expect(ReportePorFacturar.create).toHaveBeenCalledTimes(1);
  expect(a.id).toBe(7);
  expect(b.id).toBe(7);
  // Mientras se calcula, otro "Generar" igual devuelve el mismo.
  await service.generar(params, { dbId: '4' });
  expect(ReportePorFacturar.create).toHaveBeenCalledTimes(1);
  // Otra fecha sí crea otro.
  ReportePorFacturar.create.mockResolvedValueOnce({ id: 8 });
  await service.generar({ ...params, fechaInicio: '2026-10-21', fechaFin: '2026-10-21' }, { dbId: '8' });
  expect(ReportePorFacturar.create).toHaveBeenCalledTimes(2);
  await service._esperar(7);
  await service._esperar(8);
  // Ya terminado → "Generar" crea uno nuevo.
  await service.generar(params, { dbId: '8' });
  expect(ReportePorFacturar.create).toHaveBeenCalledTimes(3);
  await service._esperar(7);
});

// 2026-11-24 testnumo (cluster, 2 procesos): otro proceso ya lo está calculando → se devuelve ese, sin crear ni calcular.
test('cluster: si en la base ya hay uno igual calculando, generar devuelve ese', async () => {
  ReportePorFacturar.findOne.mockResolvedValueOnce({ id: 31, estado: 'calculando' });
  const params = { rfc: 'CCO011113663', centroCostoId: 1, fechaInicio: '2026-09-04', fechaFin: '2026-09-04' };
  const r = await service.generar(params, { dbId: '8' });
  expect(ReportePorFacturar.create).not.toHaveBeenCalled();
  expect(mostrador.detectarDepositosPendientesFactura).not.toHaveBeenCalled();
  expect(ReportePorFacturar.findByPk.mock.calls[0][0]).toBe(31);
  // Candado por empresa + sucursal + fechas, y búsqueda solo de los que siguen calculando.
  expect(ReportePorFacturar.sequelize.query.mock.calls[0][1].replacements.clave).toBe('reportes_por_facturar|CCO011113663|1|2026-09-04|2026-09-04');
  expect(ReportePorFacturar.findOne.mock.calls[0][0].where).toMatchObject({ rfc: 'CCO011113663', centroCostoId: 1, estado: 'calculando', eliminadoEn: null });
  expect(r.id).toBe(7); // obtener del mock
});

test('si el que se calculaba ya fue eliminado, generar crea uno nuevo', async () => {
  const params = { rfc: 'CCO011113663', centroCostoId: 1, fechaInicio: '2026-10-22', fechaFin: '2026-10-22' };
  await service.generar(params, {});
  ReportePorFacturar.findByPk.mockResolvedValueOnce({ eliminadoEn: new Date() });
  ReportePorFacturar.create.mockResolvedValueOnce({ id: 9 });
  await service.generar(params, {});
  expect(ReportePorFacturar.create).toHaveBeenCalledTimes(2);
  await service._esperar(7);
  await service._esperar(9);
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

  test('obtener: MOSTRADOR por almacén y fecha de la global con cobros de caja, pendientes al final; REF. y especiales', async () => {
    cargar();
    const s = (await service.obtener(3)).secciones;
    // 2026-10-30: una hoja MOSTRADOR por almacén; el pendiente va en el de su remisión.
    expect(s.mostrador.map(a => [a.serie, a.sucursal, a.bloques.map(b => [b.tipo, b.factura ?? null])])).toEqual([
      ['A0', 'CEDIS', [['global', 'A0-3'], ['global', 'A0-1']]],
      ['I0', null, [['global', 'I0-9'], ['pendiente', null]]],
    ]);
    const a01 = s.mostrador[0].bloques[1];
    expect(a01.grupos.map(g => [g.cliente, g.total])).toEqual([['ALFA', 1520], ['ZETA', 15]]);
    expect(a01.total).toBe(1535);
    expect(a01.grupos[0].depositos.map(d => d.banco)).toEqual(['BBVA', 'EFECTIVO']);
    expect(s.mostrador[1].bloques[1].grupos.map(g => g.cliente)).toEqual(['BETA', 'CLIENTE NO IDENTIFICADO']);
    expect(s.ref.map(a => [a.serie, a.bloques.length])).toEqual([['B0', 1]]);
    // 2026-11-20: la especial (CRISLA) va en el mismo bloque de REF. B0, marcada.
    const refB0 = s.ref[0].bloques[0].grupos.flatMap(g => g.depositos);
    expect(refB0.find(d => d.factura === 'B0-50')).toMatchObject({ clienteFacturado: 'GRUPO CHILI GUAJILI S.A. DE C.V.' });
    expect(refB0.find(d => d.factura === 'B0-50').especial).toBeUndefined();
    expect(refB0.find(d => d.factura === 'B0-51')).toMatchObject({ cliente: 'MARIO VASQUEZ', clienteFacturado: 'CRISLA', especial: true });
    expect(s.especiales).toEqual([]);
    expect(s.ficha.total).toBe(100);
    expect(s.efectivo).toHaveLength(1);
  });

  // 2026-11-20 (usuario: "las facturas especiales y las REF … juntas sin
  // separación", en todos los almacenes): la especial va en su REF., mismo
  // bloque del día, marcada `especial`.
  test('A0: las especiales van mezcladas en REF. A0, marcadas', async () => {
    cargar();
    const base = (await ReportePorFacturar.findByPk()).toJSON();
    base.datos.movimientosMostrador.push(
      { uuid: 'C3', serie: 'A0', folio: '70', esGlobal: false, receptorNombre: 'NOMBRE FACTURA', fechaFactura: '2026-10-20T18:00:00Z', depositos: [dep('OTRO NOMBRE KORE', 60, 'A0-60')] },
      { uuid: 'C4', serie: 'A0', folio: '71', esGlobal: false, receptorNombre: 'MISMO CLIENTE', fechaFactura: '2026-10-20T19:00:00Z', depositos: [dep('MISMO CLIENTE', 70, 'A0-61')] },
    );
    ReportePorFacturar.findByPk.mockResolvedValue({ id: 3, toJSON: () => base });
    const s = (await service.obtener(3)).secciones;
    const refA0 = s.ref.find(a => a.serie === 'A0');
    expect(refA0.bloques).toHaveLength(1);
    const filas = refA0.bloques[0].grupos.flatMap(g => g.depositos);
    expect(filas.map(d => [d.factura, !!d.especial]).sort()).toEqual([['A0-70', true], ['A0-71', false]]);
    expect(s.especiales).toEqual([]);
  });

  test('excel: hojas MOSTRADOR, REF. por almacén (especiales en color), X FAC CON FICHA y efectivo', async () => {
    cargar();
    const leer = async seccion => { const wb = new ExcelJS.Workbook(); await wb.xlsx.load((await service.excel(3, seccion)).buffer); return wb; };
    const todo = await leer();
    expect(todo.worksheets.map(w => w.name)).toEqual(['MOSTRADOR A0 - CEDIS', 'MOSTRADOR I0', 'REF. B0 - HIDALGO', 'X FAC CON FICHA DE DEP', 'DEPÓSITOS EN EFECTIVO', 'RESUMEN POR FACTURAR']);
    expect(todo.getWorksheet('MOSTRADOR I0').getColumn(2).values).toEqual(expect.arrayContaining(['PENDIENTE GENERAR FACTURA']));
    expect((await leer('mostrador')).worksheets.map(w => w.name)).toEqual(['MOSTRADOR A0 - CEDIS', 'MOSTRADOR I0']);
    const ws = todo.getWorksheet('MOSTRADOR A0 - CEDIS');
    // Encabezado del bloque de la global A0-1 y sus columnas, como el Excel del usuario.
    const fila = n => ws.getRow(n).values.slice(1);
    const enc = ws.getColumn(7).values.indexOf('A0-1');
    expect(fila(enc)).toEqual([expect.any(Date), undefined, undefined, undefined, undefined, 'A0', 'A0-1', 'ID NUMO']);
    // 2026-10-30: columna FORMA DE PAGO después de BANCO.
    // 2026-11-10: CLIENTE junto a la FECHA.
    expect(fila(enc + 1)).toEqual(['FECHA', 'CLIENTE', 'DESCRIPCIÓN', 'IMPORTE', 'BANCO', 'FORMA DE PAGO', 'REM', undefined, 'ESTATUS', 'SALDO PENDIENTE', 'SALDADA EL']); // 2026-11-12: ESTATUS en MOSTRADOR
    expect(ws.getColumn(2).values).toEqual(expect.arrayContaining(['Subtotal ALFA', 'TOTAL']));
    // Al final: TOTAL EFECTIVO (solo el cobro en caja de $1,500) y, aparte, DEPÓSITO EN EFECTIVO.
    const totalDe = etiqueta => { const n = ws.getColumn(2).values.indexOf(etiqueta); return n > 0 ? ws.getRow(n).getCell(4).value : null; };
    expect(totalDe('TOTAL EFECTIVO')).toBe(1500);
    expect(totalDe('TOTAL DEPÓSITO EN EFECTIVO (APARTE)')).toBe(0);
    const ref = todo.getWorksheet('REF. B0 - HIDALGO').getRow(2).values.slice(1);
    expect(ref).toEqual(['FECHA', 'CTE', 'DESCRIPCIÓN', 'IMPORTE', 'BANCO', 'FORMA DE PAGO', 'B0', 'B0 ORIGEN', 'CTE FACTURADO', 'ID DE PAGO']);
    expect((await leer('ficha')).worksheets.map(w => w.name)).toEqual(['X FAC CON FICHA DE DEP']);
    // Renglón de la especial (B0-51, CRISLA) en color; el normal (B0-50) no.
    const wsRef = todo.getWorksheet('REF. B0 - HIDALGO');
    const filaDe = folio => wsRef.getRow(wsRef.getColumn(7).values.indexOf(folio));
    expect(filaDe('B0-51').getCell(1).fill?.fgColor?.argb).toBe('FFFCE4D6');
    expect(filaDe('B0-51').getCell(9).fill?.fgColor?.argb).toBe('FFFCE4D6');
    expect(filaDe('B0-50').getCell(1).fill?.fgColor?.argb).toBeUndefined();
    await expect(service.excel(3, 'especiales')).rejects.toThrow();
    await expect(service.excel(3, 'otra')).rejects.toThrow();
  });
});

// 2026-10-30: remisión A0-260804937 repartida en 3 facturas el 30-oct → el
// encabezado de la global dice en qué otras facturas también quedó.
test('refacturada: encabezado con las otras facturas de sus mismas remisiones', async () => {
  const ExcelJS = require('exceljs');
  gen._uuidsPorFechaEfectiva.mockImplementation(async ({ fechaInicio }) => new Set(fechaInicio === '2026-10-20' ? [GLOBAL] : []));
  CFDI.find.mockImplementation(q => (q.documentosRelacionados
    ? lean([
      { serie: 'A0', folio: '261015291', documentosRelacionados: [{ Serie: 'A0', Folio: '260804937' }] },
      { serie: 'A0', folio: '261015312', documentosRelacionados: [{ Serie: 'A0', Folio: '260804937' }] },
      { serie: 'A0', folio: '261009872', documentosRelacionados: [{ Serie: 'A0', Folio: '260804937' }] },
    ])
    : lean([{ uuid: GLOBAL, serie: 'A0', folio: '261009872', fecha: '2026-10-20T17:22:35Z', total: 12896.62, receptor: { rfc: 'XAXX010101000' }, source: 'ERP' }])));
  mostrador.detectarMovimientosMostrador.mockResolvedValue([{ uuid: GLOBAL, serie: 'A0', folio: '261009872', esGlobal: true, fechaFactura: '2026-10-20T17:22:35Z',
    depositos: [{ fecha: '2026-09-03T00:00:00Z', importe: 59997.42, aplicado: 59997.42, remisiones: ['A0-260804937'], cliente: 'TIBURCIO GARCIA CRUZ', idNumo: '1', ppdPorFacturar: true }] }]);
  await generarYEsperar({ rfc: 'CCO011113663', centroCostoId: 1, fechaInicio: '2026-10-20' }, { dbId: '5' });

  const filtro = CFDI.find.mock.calls.map(c => c[0]).find(q => q.documentosRelacionados);
  expect(filtro.documentosRelacionados.$elemMatch.$or).toEqual([{ Serie: 'A0', Folio: '260804937' }]);
  expect(terminado().datos.movimientosMostrador[0].otrasFacturas).toEqual(['A0-261015291', 'A0-261015312']);

  ReportePorFacturar.findByPk.mockResolvedValue({ id: 7, toJSON: () => ({ id: 7, sucursal: 'CEDIS', fechaInicio: '2026-10-20', fechaFin: '2026-10-20', datos: terminado().datos }) });
  const bloque = (await service.obtener(7)).secciones.mostrador.flatMap(a => a.bloques).find(b => b.tipo === 'global');
  expect(bloque).toMatchObject({ factura: 'A0-261009872', otrasFacturas: ['A0-261015291', 'A0-261015312'] });
  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load((await service.excel(7, 'mostrador')).buffer);
  const ws = wb.worksheets.find(w => w.name.startsWith('MOSTRADOR A0'));
  expect(ws.getRow(1).getCell(2).value).toBe('TAMBIÉN FACTURADA EN: A0-261015291, A0-261015312');
});

// 2026-11-20 (pedido del usuario): remisión PPD (consignación) pagada y
// facturada el MISMO día sí entra; el vínculo sin UUID se liga por remisión.
// Un depósito PPD POSTERIOR a la factura (cobranza de crédito) no.
test('consignación PPD del mismo día entra, ligada por remisión; PPD posterior no', async () => {
  const FACT = '6C2D6D4C-DE8A-48A2-9AF1-DA9A7E3E5288';
  const CREDITO = 'CCCCCCCC-0000-0000-0000-000000000003';
  gen._uuidsPorFechaEfectiva.mockImplementation(async ({ fechaInicio }) => new Set(fechaInicio === '2026-11-20' ? [FACT, CREDITO] : []));
  CFDI.find.mockReturnValue(lean([
    { uuid: FACT, serie: 'A0', folio: '261110111', fecha: '2026-11-20T16:37:25Z', total: 9120.85, receptor: { rfc: 'XXXX010101XX1' }, source: 'ERP',
      documentosRelacionados: [{ Serie: 'A0', Folio: '261002627' }] },
    { uuid: CREDITO, serie: 'A0', folio: '261110200', fecha: '2026-11-20T18:00:00Z', total: 500, receptor: { rfc: 'XEXX010101000' }, source: 'ERP' },
  ]));
  BankMovement.find.mockReturnValue(lean([
    { fecha: new Date('2026-11-20T00:00:00Z'), erpLinks: [{ folioFiscal: null, serie: 'A0', folioExterno: '261002627', tipoPago: 'PPD' }] },
    { fecha: new Date('2026-11-21T00:00:00Z'), erpLinks: [{ folioFiscal: CREDITO, serie: 'A0', folioExterno: '261009999', tipoPago: 'PPD' }] },
  ]));
  mostrador.detectarMovimientosMostrador.mockResolvedValue([]);
  await generarYEsperar({ rfc: 'CCO011113663', centroCostoId: 1, fechaInicio: '2026-11-20' }, { dbId: '5' });

  const [arg] = mostrador.detectarMovimientosMostrador.mock.calls[0];
  expect(arg.movimientos).toEqual([{ tipoOrigen: 'Venta Sin Cobro', debe: 1, cfdiUuid: FACT }]);
  expect([...arg.uuidPorRemision]).toEqual([['A0|261002627', FACT]]);
});

// 2026-11-23: copia ERP sin timbrar ("SINUUID-…") de la misma global que la
// timbrada → antes salía el bloque dos veces; ahora solo la timbrada.
test('una copia SINUUID (sin timbrar) no entra como factura', async () => {
  const REAL = '29DCF225-861C-4639-97D0-94F50A2EE69C';
  const SIN = 'SINUUID-CCO011113663-A0-261110712-18598.4254';
  gen._uuidsPorFechaEfectiva.mockImplementation(async ({ fechaInicio }) => new Set(fechaInicio === '2026-11-21' ? [REAL, SIN] : []));
  CFDI.find.mockReturnValue(lean([
    { uuid: SIN, serie: 'A0', folio: '261110712', fecha: '2026-11-21T13:26:42Z', total: 18598.4254, receptor: { rfc: 'XAXX010101000' }, source: 'ERP' },
    { uuid: REAL, serie: 'A0', folio: '261110712', fecha: '2026-11-21T13:26:42Z', total: 18598.42, receptor: { rfc: 'XAXX010101000' }, source: 'ERP' },
  ]));
  BankMovement.find.mockReturnValue(lean([
    { fecha: new Date('2026-11-18T00:00:00Z'), erpLinks: [{ folioFiscal: REAL }] },
    { fecha: new Date('2026-11-18T00:00:00Z'), erpLinks: [{ folioFiscal: SIN }] },
  ]));
  mostrador.detectarMovimientosMostrador.mockResolvedValue([]);
  await generarYEsperar({ rfc: 'CCO011113663', centroCostoId: 1, fechaInicio: '2026-11-21' }, { dbId: '6' });

  const [arg] = mostrador.detectarMovimientosMostrador.mock.calls[0];
  expect(arg.cfdis.map(c => c.uuid)).toEqual([REAL]);
  expect(arg.movimientos.map(m => m.cfdiUuid)).toEqual([REAL]);
});
