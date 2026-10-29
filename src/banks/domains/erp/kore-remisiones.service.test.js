'use strict';

// Memoria permanente de remisiones de Kore (2026-10-29): cliente, tipo de
// pago y estatus se piden a Kore solo la primera vez.
jest.mock('./KoreRemision.model', () => ({ find: jest.fn(), bulkWrite: jest.fn(async () => ({})) }));
jest.mock('./erp-sync.service', () => ({ obtenerCuentaTicket: jest.fn() }));

const KoreRemision = require('./KoreRemision.model');
const { obtenerCuentaTicket } = require('./erp-sync.service');
const { infoRemisiones } = require('./kore-remisiones.service');

const guardadas = docs => KoreRemision.find.mockReturnValue({ lean: async () => docs });
const rem = folio => ({ serie: 'A0', folio, fechaCreacion: '2026-07-01T00:00:00Z' });

beforeEach(() => {
  jest.clearAllMocks();
  obtenerCuentaTicket.mockImplementation(async ({ folio }) => ({ nombre: `CLIENTE ${folio}`, tipoPago: 'PPD', nombreTipoMovimiento: 'POR FACTURAR', saldoActual: 0, factura: null }));
});

test('las guardadas no van a Kore; las nuevas sí y se guardan', async () => {
  guardadas([{ serie: 'A0', folio: '1', nombre: 'GERARDO SOSA QUERO', tipoPago: 'PPD', nombreTipoMovimiento: 'POR FACTURAR' }]);
  const r = await infoRemisiones({ rfc: 'X', remisiones: [rem('1'), rem('2'), rem('2')] });
  expect(obtenerCuentaTicket).toHaveBeenCalledTimes(1);
  expect(obtenerCuentaTicket.mock.calls[0][0]).toMatchObject({ serie: 'A0', folio: '2' });
  expect(r.consultadas).toBe(1);
  expect(r.info.get('A0|1')).toEqual({ nombre: 'GERARDO SOSA QUERO', tipoPago: 'PPD', nombreTipoMovimiento: 'POR FACTURAR' });
  expect(r.info.get('A0|2')).toMatchObject({ nombre: 'CLIENTE 2', saldoActual: 0 });
  const [ops] = KoreRemision.bulkWrite.mock.calls[0];
  expect(ops).toEqual([{ updateOne: expect.objectContaining({
    filter: { rfc: 'X', serie: 'A0', folio: '2' },
    update: { $set: expect.objectContaining({ nombre: 'CLIENTE 2', tipoPago: 'PPD', nombreTipoMovimiento: 'POR FACTURAR' }) },
    upsert: true,
  }) }]);
});

test('necesitaSaldo: aunque esté guardada se pide a Kore (saldo / factura frescos)', async () => {
  guardadas([{ serie: 'A0', folio: '1', nombre: 'X', tipoPago: 'PPD', nombreTipoMovimiento: 'POR FACTURAR' }]);
  const r = await infoRemisiones({ rfc: 'X', remisiones: [rem('1')], necesitaSaldo: k => k === 'A0|1' });
  expect(obtenerCuentaTicket).toHaveBeenCalledTimes(1);
  expect(r.info.get('A0|1').saldoActual).toBe(0);
});

test('Kore no responde: cuenta como sin respuesta y no se guarda; sin memoria disponible se consulta todo', async () => {
  KoreRemision.find.mockReturnValue({ lean: async () => { throw new Error('mongo caído'); } });
  obtenerCuentaTicket.mockResolvedValueOnce(null);
  const r = await infoRemisiones({ rfc: 'X', remisiones: [rem('1'), rem('2')] });
  expect(r).toMatchObject({ sinRespuesta: 1, consultadas: 2 });
  expect(KoreRemision.bulkWrite.mock.calls[0][0]).toHaveLength(1);
});
