'use strict';

// Hoja MOSTRADOR (2026-10-21): solo facturas con "Venta Sin Cobro", depósitos
// de Bancos ligados por UUID, cliente de Kore por remisión. Datos reales de la
// factura A0-261009872 (CEDIS 20-oct).
jest.mock('../banks/BankMovement.model', () => ({ find: jest.fn() }));
jest.mock('../erp/erp-sync.service', () => ({ obtenerDesglosesCobroAlmacen: jest.fn(), obtenerCuentaTicket: jest.fn() }));

const BankMovement = require('../banks/BankMovement.model');
const erp = require('../erp/erp-sync.service');
const { detectarMovimientosMostrador, detectarDepositosPendientesFactura, detectarDepositosEfectivoSinAsociar } = require('./mostrador.service');

const UUID = '1C02D46C-FF4C-411E-BF11-7960AB39BA9C';
const cfdis = [{ uuid: UUID, serie: 'A0', folio: '261009872', fecha: '2026-10-20T17:22:35Z', total: 229702.95, receptor: { rfc: 'XAXX010101000' } }];
const link = (folio, monto, uuid = UUID, tipoPago = null) => ({ folioFiscal: uuid, serie: 'A0', folioExterno: folio, saldoPagado: monto, tipoPago });

beforeEach(() => {
  jest.clearAllMocks();
  BankMovement.find.mockReturnValue({ lean: async () => [
    { folio: '052549', banco: 'Santander', fecha: '2026-10-13', concepto: 'DEPOSITO EN EFECTIVO ATM', deposito: 5000,
      erpLinks: [link('260701116', 1078.46), link('260603748', 3921.28), link('999', 50, 'OTRA-FACTURA')] },
    { folio: '039909', banco: 'BBVA', fecha: '2026-08-10', concepto: 'DEPOSITO EN EFECTIVO', deposito: 12481.57, erpLinks: [link('260704838', 12481.57)] },
  ] });
  erp.obtenerDesglosesCobroAlmacen.mockResolvedValue([
    { serieVenta: 'A0', folioVenta: '260701116', fechaCreacion: '2026-07-01' },
    { serieVenta: 'A0', folioVenta: '260603748', fechaCreacion: '2026-06-01' },
    { serieVenta: 'A0', folioVenta: '260704838', fechaCreacion: '2026-07-04' },
  ]);
  erp.obtenerCuentaTicket.mockImplementation(async ({ folio }) => (folio === '260704838'
    ? { nombre: 'FERNANDO CASTREJON CARDONA', tipoPago: 'PPD', nombreTipoMovimiento: 'POR FACTURAR' }
    : { nombre: 'ADRIANA DEL CARMEN VASQUEZ SIMON', tipoPago: 'PUE', nombreTipoMovimiento: 'FACTURADO' }));
});

test('arma la factura con sus depósitos ordenados por fecha', async () => {
  const movimientos = [{ tipoOrigen: 'Venta Sin Cobro', debe: 222202.67, cfdiUuid: UUID }, { tipoOrigen: 'Venta', debe: 0, haber: 31683.17, cfdiUuid: UUID }];
  const [f] = await detectarMovimientosMostrador({ rfc: 'X', cfdis, movimientos });
  expect(f).toMatchObject({ serie: 'A0', folio: '261009872', esGlobal: true, totalFactura: 229702.95, totalAplicado: 17481.31 });
  expect(f.depositos.map(d => [d.idNumo, d.importe, d.aplicado, d.remisiones, d.cliente, d.estatus, d.ppdPorFacturar])).toEqual([
    ['039909', 12481.57, 12481.57, ['A0-260704838'], 'FERNANDO CASTREJON CARDONA', 'POR FACTURAR', true],
    ['052549', 5000, 4999.74, ['A0-260701116', 'A0-260603748'], 'ADRIANA DEL CARMEN VASQUEZ SIMON', 'FACTURADO', false],
  ]);
});

test('sin "Venta Sin Cobro" no consulta nada', async () => {
  expect(await detectarMovimientosMostrador({ rfc: 'X', cfdis, movimientos: [{ tipoOrigen: 'Venta', debe: 100, cfdiUuid: UUID }] })).toEqual([]);
  expect(BankMovement.find).not.toHaveBeenCalled();
});

test('si Kore falla, sale sin cliente pero con los depósitos', async () => {
  erp.obtenerDesglosesCobroAlmacen.mockRejectedValue(new Error('timeout'));
  const [f] = await detectarMovimientosMostrador({ rfc: 'X', cfdis, movimientos: [{ tipoOrigen: 'Venta Sin Cobro', debe: 1, cfdiUuid: UUID }] });
  expect(f.depositos).toHaveLength(2);
  expect(f.depositos.every(d => d.cliente === '' && d.estatus === '' && d.ppdPorFacturar === false)).toBe(true);
});

test('factura a nombre de un cliente no es global', async () => {
  const cfdisCliente = [{ ...cfdis[0], receptor: { rfc: 'MILN800101ABC' } }];
  const [f] = await detectarMovimientosMostrador({ rfc: 'X', cfdis: cfdisCliente, movimientos: [{ tipoOrigen: 'Venta Sin Cobro', debe: 1, cfdiUuid: UUID }] });
  expect(f.esGlobal).toBe(false);
});

test('PPD del link cuando Kore no trae tipoPago; sin estatus "POR FACTURAR" no cuenta', async () => {
  BankMovement.find.mockReturnValue({ lean: async () => [
    { folio: '1', banco: 'BBVA', fecha: '2026-10-19', concepto: 'X', deposito: 10, erpLinks: [link('260704838', 10, UUID, 'PPD')] },
  ] });
  const movimientos = [{ tipoOrigen: 'Venta Sin Cobro', debe: 1, cfdiUuid: UUID }];
  erp.obtenerCuentaTicket.mockResolvedValue({ nombre: 'X', tipoPago: null, nombreTipoMovimiento: 'POR FACTURAR' });
  expect((await detectarMovimientosMostrador({ rfc: 'X', cfdis, movimientos }))[0].depositos[0].ppdPorFacturar).toBe(true);
  erp.obtenerCuentaTicket.mockResolvedValue(null);
  expect((await detectarMovimientosMostrador({ rfc: 'X', cfdis, movimientos }))[0].depositos[0].ppdPorFacturar).toBe(false);
});

// "PENDIENTE GENERAR FACTURA" — datos reales de la lista de CEDIS (2026-10-22).
describe('PENDIENTE GENERAR FACTURA', () => {
  const rem = (folio, extra = {}) => ({ serie: 'A0', folioExterno: folio, folioFiscal: null, tipoPago: 'PPD', saldoPagado: 1, ...extra });
  const kore = { nombre: 'MISAEL ABDIEL JIMENEZ SORIA', tipoPago: 'PPD', nombreTipoMovimiento: 'POR FACTURAR', saldoActual: 0, factura: null };
  const cuentasKore = {
    260804886: kore,                                                  // 053966 → sale
    260902675: { ...kore, nombre: 'ROMAN SOLANO PAZ' },               // 053487 → sale
    261002980: { ...kore, saldoActual: 47.79 },                       // 053960 → saldo pendiente, no sale
    261001844: { ...kore, tipoPago: 'PUE' },                          // 052144 → PUE, no sale
    260901859: kore,                                                  // 054089 → ya facturada (desgloses), no sale
    260900887: { ...kore, nombreTipoMovimiento: 'FACTURADO' },        // no "POR FACTURAR", no sale
  };
  beforeEach(() => {
    BankMovement.find.mockReturnValue({ lean: async () => [
      { folio: '053966', banco: 'BBVA', fecha: '2026-10-21T06:00:00Z', concepto: 'DEPOSITO EN EFECTIVO', deposito: 146077.63, erpLinks: [rem('260804886')] },
      { folio: '053487', banco: 'Banamex', fecha: '2026-10-19T06:00:00Z', concepto: 'Depósito de efectivo', deposito: 9968, erpLinks: [rem('260902675'), rem('999', { serie: 'B0' })] },
      { folio: '053960', banco: 'BBVA', fecha: '2026-10-21T06:00:00Z', concepto: 'PAGO', deposito: 1147.04, erpLinks: [rem('261002980')] },
      { folio: '052144', banco: 'BBVA', fecha: '2026-10-12T06:00:00Z', concepto: 'X', deposito: 2046.88, erpLinks: [rem('261001844')] },
      { folio: '054089', banco: 'Santander', fecha: '2026-10-21T06:00:00Z', concepto: 'X', deposito: 7067.4, erpLinks: [rem('260901859')] },
      { folio: '053673', banco: 'BBVA', fecha: '2026-10-19T06:00:00Z', concepto: 'X', deposito: 13590, erpLinks: [rem('260804886'), rem('260900887')] },
    ] });
    erp.obtenerDesglosesCobroAlmacen.mockImplementation(async ({ folios }) => folios.map(f => ({
      serieVenta: 'A0', folioVenta: f, fechaCreacion: '2026-09-01', ...(f === '260901859' ? { serieFactura: 'A0', folioFactura: '261010582' } : {}),
    })));
    erp.obtenerCuentaTicket.mockImplementation(async ({ folio }) => cuentasKore[folio] ?? null);
  });

  test('día de un solo día: fechaFin vacío usa fechaInicio', async () => {
    await detectarDepositosPendientesFactura({ rfc: 'X', series: ['A0'], fechaInicio: '2026-10-21' });
    const { fecha } = BankMovement.find.mock.calls[0][0];
    expect([fecha.$gte.toISOString(), fecha.$lte.toISOString()]).toEqual(['2026-10-21T00:00:00.000Z', '2026-10-21T23:59:59.999Z']);
  });

  test('solo depósitos cuyas remisiones son PPD, saldo 0, "POR FACTURAR" y sin factura', async () => {
    const { depositos, depositosConSaldo, remisionesSinRespuesta } = await detectarDepositosPendientesFactura({ rfc: 'X', series: ['A0'], fechaInicio: '2026-10-19', fechaFin: '2026-10-21' });
    expect(remisionesSinRespuesta).toBe(0);
    // 053960: POR FACTURAR pero debe $47.79 → CON PENDIENTES DE PAGO, con su saldo.
    expect(depositosConSaldo.map(d => [d.idNumo, d.saldo, d.remisiones])).toEqual([['053960', 47.79, ['A0-261002980']]]);
    expect(depositos).toEqual([
      { fecha: '2026-10-19T06:00:00Z', descripcion: 'Depósito de efectivo', importe: 9968, aplicado: 1, banco: 'Banamex', remisiones: ['A0-260902675'], cliente: 'ROMAN SOLANO PAZ', idNumo: '053487', estatus: 'POR FACTURAR' },
      { fecha: '2026-10-21T06:00:00Z', descripcion: 'DEPOSITO EN EFECTIVO', importe: 146077.63, aplicado: 1, banco: 'BBVA', remisiones: ['A0-260804886'], cliente: 'MISAEL ABDIEL JIMENEZ SORIA', idNumo: '053966', estatus: 'POR FACTURAR' },
    ]);
    const filtro = BankMovement.find.mock.calls[0][0];
    expect(filtro.fecha.$gte.toISOString()).toBe('2026-10-19T00:00:00.000Z');
    expect(filtro.fecha.$lte.toISOString()).toBe('2026-10-21T23:59:59.999Z');
    expect(filtro.erpLinks.$elemMatch.serie).toEqual({ $in: ['A0'] });
    // La remisión ya facturada no se consulta en /cuentas-pendientes.
    expect(erp.obtenerCuentaTicket.mock.calls.map(c => c[0].folio)).not.toContain('260901859');
  });

  test('si Kore no responde una remisión, su depósito no sale y se cuenta', async () => {
    erp.obtenerCuentaTicket.mockImplementation(async ({ folio }) => (folio === '260804886' ? null : cuentasKore[folio] ?? null));
    const { depositos, remisionesSinRespuesta } = await detectarDepositosPendientesFactura({ rfc: 'X', series: ['A0'], fechaInicio: '2026-10-19', fechaFin: '2026-10-21' });
    expect(depositos.map(d => d.idNumo)).toEqual(['053487']);
    expect(remisionesSinRespuesta).toBe(1);
  });

  test('sin series o sin fecha no consulta nada', async () => {
    expect(await detectarDepositosPendientesFactura({ rfc: 'X', series: [], fechaInicio: '2026-10-21' })).toEqual({ depositos: [], depositosConSaldo: [], remisionesSinRespuesta: 0 });
    expect(await detectarDepositosPendientesFactura({ rfc: 'X', series: ['A0'] })).toEqual({ depositos: [], depositosConSaldo: [], remisionesSinRespuesta: 0 });
    expect(BankMovement.find).not.toHaveBeenCalled();
  });
});

test('DEPÓSITOS EN EFECTIVO: sin ligas, concepto con efectivo, rango del periodo', async () => {
  const select = jest.fn().mockReturnValue({ lean: async () => [
    { folio: '051499', banco: 'BBVA', fecha: new Date('2026-10-08T00:00:00Z'), concepto: 'DEPOSITO EN EFECTIVO', deposito: 1618.95, ficha: null },
  ] });
  BankMovement.find.mockReturnValue({ select });
  const r = await detectarDepositosEfectivoSinAsociar({ fechaInicio: '2026-10-08' });
  expect(r).toEqual([{ fecha: new Date('2026-10-08T00:00:00Z'), descripcion: 'DEPOSITO EN EFECTIVO', importe: 1618.95, banco: 'BBVA', ficha: '', idNumo: '051499' }]);
  const filtro = BankMovement.find.mock.calls[0][0];
  expect(filtro.concepto).toEqual({ $regex: 'efectivo', $options: 'i' });
  expect(filtro.$or).toEqual([{ erpLinks: { $exists: false } }, { erpLinks: { $size: 0 } }]);
  expect([filtro.fecha.$gte.toISOString(), filtro.fecha.$lte.toISOString()]).toEqual(['2026-10-08T00:00:00.000Z', '2026-10-08T23:59:59.999Z']);
});
