'use strict';

// Renglones EFECTIVO / SALDO A FAVOR de las globales (2026-10-29) — casos
// reales del Excel "MOSTRADOR 2026" del usuario (datos de Kore copiados).
jest.mock('../banks/BankMovement.model', () => ({ find: jest.fn() }));
jest.mock('../erp/erp-sync.service', () => ({ obtenerDesglosesCobroPorFactura: jest.fn(), obtenerCuentaTicket: jest.fn() }));

const BankMovement = require('../banks/BankMovement.model');
const erp = require('../erp/erp-sync.service');
const { cobrosCajaDeGlobales } = require('./reportes-por-facturar.cobros-caja');

const lean = v => ({ select: () => ({ lean: async () => v }) });
const cobro = (serieOrigen, monto, fecha, formas) => ({ serieOrigen, folioOrigen: '1', claveCentro: 'A0', monto: -monto, fecha, formasPago: formas });
const fp = (claveSat, nombre, monto) => ({ claveSat, nombre, monto });

beforeEach(() => {
  jest.clearAllMocks();
  erp.obtenerCuentaTicket.mockImplementation(async ({ folio }) => ({
    tipoPago: folio === '999' ? 'PUE' : 'PPD', nombreTipoMovimiento: folio === '999' ? 'TICKET SENCILLO' : 'POR FACTURAR', nombre: 'CARLOS VALENCIA MOLINA',
  }));
});

test('A0-261002123: efectivo en caja y abono de un pago que no está en Bancos → SALDO A FAVOR; lo que sí es depósito no se repite', async () => {
  erp.obtenerDesglosesCobroPorFactura.mockResolvedValue([
    { serieVenta: 'A0', folioVenta: '260501745', fechaCreacion: '2026-05-12T23:24:47Z', cobros: [
      cobro('ABO', 242.06, '2026-07-15T21:37:52Z', [fp('03', 'TRANSFERENCIA', 1500)]),
      cobro('CPF', 1500, '2026-07-31T19:41:40Z', [fp('01', 'EFECTIVO', 1500)]),
      cobro('CPF', 800, '2026-07-31T19:56:34Z', [fp('03', 'TRANSFERENCIA', 800)]),
      cobro('ABO', 318.74, '2026-08-13T16:16:30Z', [fp('03', 'TRANSFERENCIA', 2000)]),
    ] },
    // Ticket de contado (CBT) y PUE: no salen.
    { serieVenta: 'A0', folioVenta: '999', fechaCreacion: '2026-10-05T10:00:00Z', cobros: [cobro('CPF', 50, '2026-10-06T10:00:00Z', [fp('01', 'EFECTIVO', 50)])] },
    { serieVenta: 'A0', folioVenta: '777', fechaCreacion: '2026-10-05T10:00:00Z', cobros: [cobro('CBT', 70, '2026-10-05T10:00:00Z', [fp('01', 'EFECTIVO', 70)])] },
  ]);
  BankMovement.find.mockReturnValue(lean([{ deposito: 2000 }, { deposito: 800 }]));

  const { porFactura, sinRespuesta } = await cobrosCajaDeGlobales({ rfc: 'X', facturas: [{ uuid: 'abc', serie: 'A0', folio: '261002123', fecha: '2026-10-05T18:00:00Z', total: 1 }] });
  expect(sinRespuesta).toBe(0);
  expect(erp.obtenerCuentaTicket).toHaveBeenCalledTimes(2);
  expect(porFactura).toHaveLength(1);
  expect(porFactura[0].depositos.map(d => [d.banco, d.importe, d.remisiones, d.idNumo])).toEqual([
    ['SALDO A FAVOR', 242.06, ['A0-260501745'], 'S/R'],
    ['EFECTIVO', 1500, ['A0-260501745'], 'S/R'],
  ]);
  expect(BankMovement.find.mock.calls[0][0].$or[0]).toEqual({ 'erpLinks.folioFiscal': { $in: ['ABC', 'abc'] } });
});

test('I0-261000245: abonos del mismo pago se juntan en un renglón', async () => {
  erp.obtenerDesglosesCobroPorFactura.mockResolvedValue([
    { serieVenta: 'I0', folioVenta: '260800408', fechaCreacion: '2026-08-24T14:13:08Z', cobros: [cobro('ABO', 8039.1, '2026-09-15T15:45:48Z', [fp('03', 'TRANSFERENCIA', 52460.58)])] },
    { serieVenta: 'I0', folioVenta: '260800407', fechaCreacion: '2026-08-24T14:12:58Z', cobros: [cobro('ABO', 1050.67, '2026-09-15T15:45:48Z', [fp('03', 'TRANSFERENCIA', 52460.58)])] },
  ]);
  BankMovement.find.mockReturnValue(lean([]));
  const { porFactura } = await cobrosCajaDeGlobales({ rfc: 'X', facturas: [{ uuid: 'u', serie: 'I0', folio: '261000245' }] });
  expect(porFactura[0].depositos).toHaveLength(1);
  expect(porFactura[0].depositos[0]).toMatchObject({ banco: 'SALDO A FAVOR', importe: 9089.77, remisiones: ['I0-260800408', 'I0-260800407'] });
});

test('efectivo que ya se depositó (mismo importe en Bancos) no se repite; Kore caído → advertencia', async () => {
  erp.obtenerDesglosesCobroPorFactura.mockResolvedValueOnce([
    { serieVenta: 'A0', folioVenta: '261000949', fechaCreacion: '2026-10-06T10:00:00Z', cobros: [cobro('CPF', 3899.27, '2026-10-14T10:00:00Z', [fp('01', 'EFECTIVO', 3899.27)])] },
  ]).mockRejectedValueOnce(new Error('429'));
  BankMovement.find.mockReturnValue(lean([{ deposito: 3899.27 }]));
  const r = await cobrosCajaDeGlobales({ rfc: 'X', facturas: [{ uuid: 'a', serie: 'A0', folio: '261014203' }, { uuid: 'b', serie: 'A0', folio: '2' }] });
  expect(r.porFactura).toEqual([]);
  expect(r.sinRespuesta).toBe(1);
});

test('abonos anteriores: depósito ligado a la remisión sin el UUID de la global → sale en su bloque', async () => {
  erp.obtenerDesglosesCobroPorFactura.mockResolvedValue([
    { serieVenta: 'A0', folioVenta: '260502856', fechaCreacion: '2026-05-19T10:00:00Z', cobros: [cobro('ABO', 1681.26, '2026-08-13T16:16:30Z', [fp('03', 'TRANSFERENCIA', 2000)])] },
    { serieVenta: 'A0', folioVenta: '999', fechaCreacion: '2026-10-05T10:00:00Z', cobros: [] },
  ]);
  BankMovement.find.mockReturnValue(lean([
    // Ya trae el UUID → sale por detectarMovimientosMostrador, aquí no.
    { folio: '40711', fecha: '2026-08-13T00:00:00Z', concepto: 'ABONO TRANSFERENCIA ENLACE', deposito: 2000, banco: 'SANTANDER',
      erpLinks: [{ serie: 'A0', folioExterno: '260502856', folioFiscal: 'abc', saldoPagado: 1681.26 }] },
    // Identificado contra la remisión antes de la factura: sin UUID.
    { folio: '38103', fecha: '2026-07-30T00:00:00Z', concepto: 'ABONO TRANSFERENCIA ENLACE', deposito: 800, banco: 'Santander',
      erpLinks: [{ serie: 'A0', folioExterno: '260502856', folioFiscal: null, saldoPagado: 800 }] },
    // De un ticket PUE de la misma global: no sale.
    { folio: '1', fecha: '2026-10-05T00:00:00Z', concepto: 'SPEI', deposito: 50, banco: 'BBVA', erpLinks: [{ serie: 'A0', folioExterno: '999' }] },
  ]));
  const { porFactura } = await cobrosCajaDeGlobales({ rfc: 'X', facturas: [{ uuid: 'abc', serie: 'A0', folio: '261002123' }] });
  expect(BankMovement.find.mock.calls[0][0].$or[1]).toEqual({ 'erpLinks.folioExterno': { $in: ['260502856', '999'] } });
  expect(porFactura[0].depositos).toEqual([expect.objectContaining({
    idNumo: '38103', importe: 800, banco: 'Santander', remisiones: ['A0-260502856'], cliente: 'CARLOS VALENCIA MOLINA', ppdPorFacturar: true,
  })]);
  expect(porFactura[0].depositos[0].deKore).toBeUndefined();
});
