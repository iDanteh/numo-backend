'use strict';

// Hoja MOSTRADOR (2026-10-21): solo facturas con "Venta Sin Cobro", depósitos
// de Bancos ligados por UUID, cliente de Kore por remisión. Datos reales de la
// factura A0-261009872 (CEDIS 20-oct).
jest.mock('../banks/BankMovement.model', () => ({ find: jest.fn() }));
jest.mock('../erp/erp-sync.service', () => ({ obtenerDesglosesCobroAlmacen: jest.fn(), obtenerCuentaTicket: jest.fn() }));

const BankMovement = require('../banks/BankMovement.model');
const erp = require('../erp/erp-sync.service');
const { detectarMovimientosMostrador } = require('./mostrador.service');

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
