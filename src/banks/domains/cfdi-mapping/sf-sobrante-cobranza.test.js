'use strict';

// Saldo a favor del sobrante de un depósito de cobranza (2026-11-17, pedido del
// usuario): transferencia de $5,000 que solo abona $2,100 → $2,900 como saldo
// a favor (2103090001 $2,500 + 2104010002 $400) para que el depósito salga completo.
jest.mock('../banks/BankMovement.model', () => ({ find: jest.fn() }));
jest.mock('../../../visor/models/CFDI', () => ({ exists: jest.fn(), find: jest.fn() }));
jest.mock('../../../shared/models/postgres', () => ({
  Poliza: {}, PolizaMovimiento: { findAll: jest.fn(async () => []) }, AccountPlan: {}, CfdiMappingRule: {}, CobroSucursalPendienteCobranza: {},
}));

const BankMovement = require('../banks/BankMovement.model');
const { PolizaMovimiento } = require('../../../shared/models/postgres');
const { depositosConSobrantePorFacturas } = require('./opa-cobranza.service');
const { _movimientosSfSobranteDeCobranza } = require('./cobranza-poliza-generator.service');

const lean = v => ({ select: () => ({ lean: async () => v }) });
const FACT = 'AAAA1111-2222-3333-4444-555566667777';
const dep = (extra = {}) => ({
  _id: 'd1', folio: '059000', fecha: new Date('2026-11-14T12:00:00Z'), deposito: 5000, status: 'identificado',
  erpLinks: [{ serie: 'A0', folioExterno: '261100001', saldoPagado: 2100, tipoPago: 'PPD', folioFiscal: FACT }],
  ...extra,
});
const cuentaMap = { '2103090001': 20, '2104010002': 21, '2103030001': 22, '1102011005': 23 };
const pago = (fecha = '2026-11-14T10:00:00Z') => ({
  uuid: 'PAGO-1', serie: 'A0', folio: '261105900', fecha: new Date(fecha),
  receptor: { nombre: 'CLIENTE X', rfc: 'XAXX010101000' }, emisor: { rfc: 'CCO011113663' },
});
const lote = p => ({ cfdiConRegla: [{ cfdi: p, rule: { id: 1 } }], doctosPorUuid: new Map([[p.uuid, [{ idDocumento: FACT.toLowerCase() }]]]) });
const args = p => ({ ...lote(p), cuentaMap, ccBySerieMap: { A0: { id: 1, clave: '101' } }, rfc: 'CCO011113663' });
const lineas = r => r.movs.map(m => [m.cuentaId, m.debe, m.haber, m.reglaNombre]);

beforeEach(() => { BankMovement.find.mockReset(); PolizaMovimiento.findAll.mockResolvedValue([]); });

test('depósito $5,000 que aplica $2,100 → sobrante $2,900', async () => {
  BankMovement.find.mockReturnValue(lean([dep()]));
  const r = await depositosConSobrantePorFacturas([FACT]);
  expect(r).toHaveLength(1);
  expect(r[0]).toMatchObject({ folio: '059000', deposito: 5000, aplicado: 2100, sobrante: 2900 });
});

test('depósito aplicado solo en parte (no_identificado en Bancos) → también cuenta', async () => {
  BankMovement.find.mockReturnValue(lean([dep({ status: 'no_identificado' })]));
  expect((await depositosConSobrantePorFacturas([FACT]))[0]).toMatchObject({ sobrante: 2900 });
  expect(BankMovement.find.mock.calls[0][0].status).toEqual({ $in: ['identificado', 'no_identificado'] });
});

test('depósito con OPA o sobrante de $1 o menos (centavos) → no es saldo a favor', async () => {
  BankMovement.find.mockReturnValue(lean([
    dep({ erpLinks: [...dep().erpLinks, { serie: 'OPA', folioExterno: '01200', saldoPagado: 2900 }] }),
    dep({ _id: 'd2', folio: '059001', deposito: 2100.25 }),
    dep({ _id: 'd3', folio: '059002', deposito: 2101 }),
  ]));
  expect(await depositosConSobrantePorFacturas([FACT])).toEqual([]);
});

test('Cobranza: cargo al banco $2,900 + abono 2103090001 $2,500 + 2104010002 $400', async () => {
  BankMovement.find.mockReturnValue(lean([dep()]));
  const r = await _movimientosSfSobranteDeCobranza(args(pago()));
  expect(lineas(r)).toEqual([[23, 2900, 0, 'SF-COBRANZA'], [20, 0, 2500, 'SF-COBRANZA'], [21, 0, 400, 'SF-COBRANZA']]);
  expect(r.movs[0]).toMatchObject({ serie: '059000', facturaUuid: FACT, cfdiUuid: 'PAGO-1', formaPago: '03' });
});

test('depósito de un mes anterior al complemento → Depósitos No Identificados + IVA NI', async () => {
  BankMovement.find.mockReturnValue(lean([dep({ fecha: new Date('2026-10-30T12:00:00Z') })]));
  const r = await _movimientosSfSobranteDeCobranza(args(pago()));
  expect(lineas(r)).toEqual([[22, 2500, 0, 'NI'], [21, 400, 0, 'NI'], [20, 0, 2500, 'SF-COBRANZA'], [21, 0, 400, 'SF-COBRANZA']]);
});

test('complemento antes del 1-nov → no cambia nada', async () => {
  BankMovement.find.mockReturnValue(lean([dep()]));
  const r = await _movimientosSfSobranteDeCobranza(args(pago('2026-10-31T18:00:00Z')));
  expect(r.movs).toEqual([]);
  expect(BankMovement.find).not.toHaveBeenCalled();
});

test('sobrante ya registrado en otra póliza vigente → no se repite', async () => {
  BankMovement.find.mockReturnValue(lean([dep()]));
  PolizaMovimiento.findAll.mockResolvedValue([{ serie: '059000' }]);
  expect((await _movimientosSfSobranteDeCobranza(args(pago()))).movs).toEqual([]);
});
