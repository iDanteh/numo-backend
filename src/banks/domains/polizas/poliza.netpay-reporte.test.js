'use strict';

// Comisión NetPay desde `netpay_reportes` (2026-10-09): la venta que está en un
// reporte cargado usa su comisión real; la que no, la de Kore y el asiento lleva
// la leyenda "COMISIONES PROVISIONALES" (también con reporte parcial). Datos
// reales de producción: Oaxaca D0 6-oct, terminal 2840401820.
jest.mock('../../../shared/models/postgres', () => ({
  AccountPlan: {
    findAll: jest.fn(async () => ['5201030001', '1108010001', '2102010001', '1107010001'].map((codigo, i) => ({ id: i + 1, codigo }))),
    findOne: jest.fn(async () => null),
  },
  Terminal: { findAll: jest.fn(async () => [{ numeroSerie: '2840401820', centroCostoId: 7 }]) },
  CfdiMappingRule: {}, PolizaMovimiento: {}, Poliza: {}, CentroCosto: {},
}));
jest.mock('../erp/netpay-transacciones.service', () => ({ consultarTransaccionesNetpay: jest.fn() }));
jest.mock('../erp/NetpayReporte.model', () => ({ aggregate: jest.fn() }));

const { consultarTransaccionesNetpay } = require('../erp/netpay-transacciones.service');
const NetpayReporte = require('../erp/NetpayReporte.model');
const { _construirNetpayInfo, _lineasNetpay } = require('./poliza.service');

const centroCostoObj = { id: 7, clave: '107', serieFacturacion: 'D0', sucursal: 'OAXACA' };
const venta = (id, folio, monto) => ({
  id, tipoOrigen: 'Venta', formaPago: '04', debe: monto, haber: 0, centroCostoObj,
  serieVentaTicket: 'D0', folioVentaTicket: folio,
});
const tx = (folio, ticket, amount, commission) => ({
  folio, terminalID: '2840401820', status: 'completed', amount, commission, cardTypeName: 'VISA',
  cuentas: [{ SerieExterna: 'D0', FolioExterno: ticket }],
});

const movimientos = [venta(1, '261000001', 5342.06), venta(2, '261000002', 2223.70)];
const transacciones = [
  tx('F20261006-00253', '261000001', 5342.06, 98.52895464),
  tx('F20261006-00092', '261000002', 2223.70, 502.7429908),
];
const fecha = new Date('2026-10-06T12:00:00.000Z');

beforeEach(() => {
  jest.clearAllMocks();
  consultarTransaccionesNetpay.mockResolvedValue({ transacciones });
});

test('todas las ventas en reporte: comisión real (meses sin intereses = total − IVA) y sin leyenda', async () => {
  NetpayReporte.aggregate.mockResolvedValue([
    { referencia: 'F20261006-00253', comisionMasIva: 46.47, ivaComision: 6.41 },
    { referencia: 'F20261006-00092', comisionMasIva: 496.28, ivaComision: 68.45 },
  ]);

  const info = await _construirNetpayInfo(movimientos, fecha);
  const c = info.porCentro.get(7);

  expect(c.comision).toBe(40.06 + 427.83);
  expect(c.ivaComision).toBe(6.41 + 68.45);
  expect(c.comisionesConReporte).toBe(2);
  expect(c.comisionesProvisionales).toBe(0);
  expect(c.detalle.every(d => !d.provisional)).toBe(true);

  const lineas = _lineasNetpay(c, { id: 99 }, info.cuentasComision);
  expect(lineas[0].debe).toBe(Math.round((7565.76 - 46.47 - 496.28) * 100) / 100);
  expect(lineas[1].concepto).toBe('NETPAY SAPI DE CV');
});

test('reporte parcial: la venta sin reporte usa Kore y el asiento lleva la leyenda', async () => {
  NetpayReporte.aggregate.mockResolvedValue([
    { referencia: 'F20261006-00253', comisionMasIva: 46.47, ivaComision: 6.41 },
  ]);

  const info = await _construirNetpayInfo(movimientos, fecha);
  const c = info.porCentro.get(7);

  // Kore 502.7429908 → comisión trunc(502.74/1.16) = 433.39, IVA 69.34.
  expect(c.comision).toBe(Math.round((40.06 + 433.39) * 100) / 100);
  expect(c.ivaComision).toBe(Math.round((6.41 + 69.34) * 100) / 100);
  expect(c.comisionesConReporte).toBe(1);
  expect(c.comisionesProvisionales).toBe(1);
  expect(c.detalle.find(d => d.fila.id === 2).provisional).toBe(true);

  const lineas = _lineasNetpay(c, { id: 99 }, info.cuentasComision);
  expect(lineas.slice(1).every(l => l.concepto === 'NETPAY SAPI DE CV - COMISIONES PROVISIONALES')).toBe(true);
  expect(lineas[0].concepto).toBe('VENTAS SUC.OAXACA');
});

test('sin reporte del día: todo con Kore y leyenda; si Mongo falla, igual', async () => {
  NetpayReporte.aggregate.mockResolvedValue([]);
  let c = (await _construirNetpayInfo(movimientos, fecha)).porCentro.get(7);
  expect(c.comisionesProvisionales).toBe(2);
  expect(_lineasNetpay(c, { id: 99 }, {})[1].concepto).toMatch(/COMISIONES PROVISIONALES$/);

  NetpayReporte.aggregate.mockRejectedValue(new Error('mongo caído'));
  c = (await _construirNetpayInfo(movimientos, fecha)).porCentro.get(7);
  expect(c.comisionesProvisionales).toBe(2);
});

test('busca solo depósitos desde el día de la venta y sin reportes eliminados', async () => {
  NetpayReporte.aggregate.mockResolvedValue([]);
  await _construirNetpayInfo(movimientos, fecha);
  const [pipeline] = NetpayReporte.aggregate.mock.calls[0];
  expect(pipeline[0].$match).toEqual({
    eliminado: { $ne: true },
    fechaMovimiento: { $gte: new Date('2026-10-06T00:00:00.000Z') },
    'folios.referencia': { $in: ['F20261006-00253', 'F20261006-00092'] },
  });
});
