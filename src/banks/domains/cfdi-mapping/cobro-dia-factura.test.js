'use strict';

// Cobro de una venta que se factura después (2026-11-11, caso real EUGENIO
// CORTES JIMENEZ A0-261004610: cobrada el 7-nov, en la Factura Global
// A0-261103732 del 10-nov). Desde los cobros del 1-nov el dinero va a la póliza
// del día de la factura; el día del cobro queda como "pendiente de facturar"
// (hoja informativa). Antes del corte, o con la factura timbrada antes del
// cobro, sigue como siempre (día del cobro).
jest.mock('../banks/BankMovement.model', () => ({ find: jest.fn() }));
jest.mock('../../../visor/models/CFDI', () => ({ find: jest.fn() }));
jest.mock('../../../shared/models/postgres', () => ({
  Poliza: {}, PolizaMovimiento: {}, AccountPlan: {}, CfdiMappingRule: {}, CobroSucursalPendienteCobranza: {}, CentroCosto: {},
}));
jest.mock('../erp/erp-sync.service', () => ({
  obtenerSaldosFavor: jest.fn(), obtenerDesglosesCobroAlmacen: jest.fn(), obtenerDesglosesCobroAlmacenPorCentro: jest.fn(),
  obtenerSaldosFavorPorCentro: jest.fn(), sincronizarCuentasPendientes: jest.fn(), obtenerNombrePersonaTicket: jest.fn(), fotoEstadisticasErp: jest.fn(),
}));

const CFDI = require('../../../visor/models/CFDI');
const erp = require('../erp/erp-sync.service');
const { _cobrosSinFacturaPorCentro, _cobroVaAlDiaDeFactura } = require('./cfdi-poliza-generator.service');

const GLOBAL_10NOV = { serie: 'A0', folio: '261103732', fecha: new Date('2026-11-10T16:51:02Z'), metodoPago: 'PUE' };
const cuenta = (folioVenta, fechaCobro, monto, factura) => ({
  serieVenta: 'A0', folioVenta,
  ...(factura ? { serieFactura: factura.serie, folioFactura: factura.folio } : {}),
  cobros: [{ claveCentro: 'A0', fecha: fechaCobro, serieOrigen: 'CBT', folioOrigen: '1', monto, formasPago: [{ nombre: 'TRANSFERENCIA', claveSat: '03', monto }] }],
});

function preparar(cuentas, cfdis) {
  erp.obtenerDesglosesCobroAlmacenPorCentro.mockResolvedValue(cuentas);
  CFDI.find.mockImplementation(() => ({ select: () => ({ lean: async () => cfdis }) }));
}

test('regla: desde el 1-nov va al día de la factura, salvo factura previa', () => {
  expect(_cobroVaAlDiaDeFactura('2026-11-07', '2026-11-10')).toBe(true);   // Eugenio
  expect(_cobroVaAlDiaDeFactura('2026-11-07', null)).toBe(true);           // aún sin factura
  expect(_cobroVaAlDiaDeFactura('2026-10-30', '2026-11-10')).toBe(false);  // antes del corte
  expect(_cobroVaAlDiaDeFactura('2026-11-07', '2026-11-03')).toBe(false);  // factura timbrada antes
});

test('cobro del 7-nov facturado en la Global del 10-nov: pendiente de facturar el día del cobro', async () => {
  preparar([cuenta('261004610', '2026-11-07T18:37:00Z', 4217.77, GLOBAL_10NOV)], [GLOBAL_10NOV]);
  const r = await _cobrosSinFacturaPorCentro({ rfc: 'X', centro: 'A0', fechaInicio: '2026-11-07', fechaFin: '2026-11-07' });
  expect(r).toEqual([{ ventaSerie: 'A0', ventaFolio: '261004610', clave: '03', monto: 4217.77, pendienteFactura: true, diaCobro: '2026-11-07' }]);
});

test('cobro de octubre: sigue como cobro sin factura en su día (no se duplica con pólizas ya hechas)', async () => {
  preparar([cuenta('261004594', '2026-10-30T18:00:00Z', 12166, GLOBAL_10NOV)], [GLOBAL_10NOV]);
  const r = await _cobrosSinFacturaPorCentro({ rfc: 'X', centro: 'A0', fechaInicio: '2026-10-30', fechaFin: '2026-10-30' });
  expect(r).toHaveLength(1);
  expect(r[0].pendienteFactura).toBe(false);
});

test('factura confirmada timbrada ANTES del cobro: sigue en el día del cobro', async () => {
  const previa = { serie: 'A0', folio: '261100500', fecha: new Date('2026-11-03T12:00:00Z'), metodoPago: 'PUE' };
  preparar([cuenta('261100400', '2026-11-07T18:00:00Z', 500, previa)], [previa]);
  const r = await _cobrosSinFacturaPorCentro({ rfc: 'X', centro: 'A0', fechaInicio: '2026-11-07', fechaFin: '2026-11-07' });
  expect(r[0].pendienteFactura).toBe(false);
});
