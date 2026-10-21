'use strict';

// Hoja MOSTRADOR (2026-10-21): solo facturas con "Venta Sin Cobro", depósitos
// de Bancos ligados por UUID, cliente de Kore por remisión. Datos reales de la
// factura A0-261009872 (CEDIS 20-oct).
jest.mock('../banks/BankMovement.model', () => ({ find: jest.fn() }));
jest.mock('../erp/erp-sync.service', () => ({ obtenerDesglosesCobroAlmacen: jest.fn(), obtenerNombrePersonaTicket: jest.fn() }));

const BankMovement = require('../banks/BankMovement.model');
const erp = require('../erp/erp-sync.service');
const { detectarMovimientosMostrador } = require('./mostrador.service');

const UUID = '1C02D46C-FF4C-411E-BF11-7960AB39BA9C';
const cfdis = [{ uuid: UUID, serie: 'A0', folio: '261009872', fecha: '2026-10-20T17:22:35Z', total: 229702.95 }];
const link = (folio, monto, uuid = UUID) => ({ folioFiscal: uuid, serie: 'A0', folioExterno: folio, saldoPagado: monto });

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
  erp.obtenerNombrePersonaTicket.mockImplementation(async ({ folio }) => (folio === '260704838' ? 'FERNANDO CASTREJON CARDONA' : 'ADRIANA DEL CARMEN VASQUEZ SIMON'));
});

test('arma la factura con sus depósitos ordenados por fecha', async () => {
  const movimientos = [{ tipoOrigen: 'Venta Sin Cobro', debe: 222202.67, cfdiUuid: UUID }, { tipoOrigen: 'Venta', debe: 0, haber: 31683.17, cfdiUuid: UUID }];
  const [f] = await detectarMovimientosMostrador({ rfc: 'X', cfdis, movimientos });
  expect(f).toMatchObject({ serie: 'A0', folio: '261009872', totalFactura: 229702.95, totalAplicado: 17481.31 });
  expect(f.depositos.map(d => [d.idNumo, d.importe, d.aplicado, d.remisiones, d.cliente, d.estatus])).toEqual([
    ['039909', 12481.57, 12481.57, ['A0-260704838'], 'FERNANDO CASTREJON CARDONA', 'POR FACTURAR'],
    ['052549', 5000, 4999.74, ['A0-260701116', 'A0-260603748'], 'ADRIANA DEL CARMEN VASQUEZ SIMON', 'POR FACTURAR'],
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
  expect(f.depositos.every(d => d.cliente === '')).toBe(true);
});
