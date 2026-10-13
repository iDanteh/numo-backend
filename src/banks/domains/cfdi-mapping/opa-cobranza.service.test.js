'use strict';

// OPA del sobrante de un depósito de cobranza (2026-10-13, regla del usuario:
// "si la OPA viene de un PPD o complemento de pago va a Cobranza y no a
// Ingresos"). Caso real MINI ABASTOS dep 046486 ($7,479.42, 11-sep).
jest.mock('../banks/BankMovement.model', () => ({ find: jest.fn() }));
jest.mock('../../../visor/models/CFDI', () => ({ exists: jest.fn(), find: jest.fn() }));
jest.mock('../../../shared/models/postgres', () => ({
  Poliza: {}, PolizaMovimiento: { findAll: jest.fn(async () => []) }, AccountPlan: {}, CfdiMappingRule: {}, CobroSucursalPendienteCobranza: {},
}));

const BankMovement = require('../banks/BankMovement.model');
const CFDI = require('../../../visor/models/CFDI');
const { PolizaMovimiento } = require('../../../shared/models/postgres');
const { depositosConOpaPorFacturas, referenciaOpaDeCobranza } = require('./opa-cobranza.service');
const { _movimientosOpaDeCobranza } = require('./cobranza-poliza-generator.service');

const lean = v => ({ select: () => ({ lean: async () => v }) });
const DEP_046486 = {
  _id: 'd46486', fecha: new Date('2026-09-11T12:00:00Z'),
  erpLinks: [
    { serie: 'OPA', folioExterno: '01106', total: 68.91, saldoPagado: 68.91, tipoPago: 'PUE', folioFiscal: null },
    { serie: 'B0', folioExterno: '260803238', saldoPagado: 573.64, tipoPago: 'PPD', folioFiscal: '84E8DD64-E2B8-45BB-AEAE-A141B110E500' },
    { serie: 'B0', folioExterno: '260803365', saldoPagado: 491.35, tipoPago: 'PPD', folioFiscal: '3C8BFEB4-8879-4B90-98D8-25C88AC66FB3' },
    { serie: 'B0', folioExterno: '260803492', saldoPagado: 6345.52, tipoPago: 'PPD', folioFiscal: 'B2A2B3E1-EC35-4F48-A014-011D050730DC' },
  ],
};

beforeEach(() => { BankMovement.find.mockReset(); CFDI.exists.mockReset(); });

test('depósito con cuentas PPD + OPA → se detecta la OPA', async () => {
  BankMovement.find.mockReturnValue(lean([DEP_046486]));
  const r = await depositosConOpaPorFacturas(['b2a2b3e1-ec35-4f48-a014-011d050730dc']);
  expect(r).toHaveLength(1);
  expect(r[0].opas).toEqual([{ folio: '01106', referencia: 'OPA-01106', monto: 68.91 }]);
  expect(r[0].facturasPpd.size).toBe(3);
});

test('anticipo con folio OPA conocido en depósito de cobranza → va a Cobranza', async () => {
  BankMovement.find.mockReturnValue(lean([DEP_046486]));
  expect(await referenciaOpaDeCobranza({ uuid: 'A1' }, 'OPA-01106')).toBe('OPA-01106');
});

test('anticipo cuyo depósito NO paga cuentas PPD → se queda en Ingreso', async () => {
  BankMovement.find.mockReturnValue(lean([{ erpLinks: [{ serie: 'OPA', folioExterno: '00900', total: 5000, tipoPago: 'PUE' }] }]));
  expect(await referenciaOpaDeCobranza({ uuid: 'A2' }, 'OPA-00900')).toBeNull();
});

test('sin folio OPA: por monto + mismo cliente', async () => {
  BankMovement.find.mockReturnValue(lean([DEP_046486]));
  CFDI.exists.mockResolvedValue(true);
  const anticipo = { uuid: '790BAE35', total: 68.91, fecha: new Date('2026-10-07T12:00:00Z'), receptor: { rfc: 'MAB000000AAA' } };
  expect(await referenciaOpaDeCobranza(anticipo, null)).toBe('OPA-01106');
});

describe('Cobranza: movimientos de la OPA', () => {
  const cuentaMap = { '2103010001': 10, '2104010002': 11, '2103030001': 12, '1102011005': 13 };
  const pago = {
    uuid: 'D57B1F20-36BE-4FD5-9E8F-EAA1E1285F07', serie: 'B0', folio: '261000927', fecha: new Date('2026-10-08T12:38:00Z'),
    receptor: { nombre: 'MINI ABASTOS', rfc: 'MAB000000AAA' }, emisor: { rfc: 'CCO011113663' },
  };
  const doctosPorUuid = new Map([[pago.uuid, [
    { idDocumento: 'B2A2B3E1-EC35-4F48-A014-011D050730DC' }, { idDocumento: '84E8DD64-E2B8-45BB-AEAE-A141B110E500' },
  ]]]);
  const args = { cfdiConRegla: [{ cfdi: pago, rule: { id: 1 } }], doctosPorUuid, cuentaMap, ccBySerieMap: { B0: { id: 3, clave: '111' } }, rfc: 'CCO011113663' };

  test('depósito de mes anterior → NI + Abono Anticipos/IVA; cuadra', async () => {
    BankMovement.find.mockReturnValue(lean([DEP_046486]));
    PolizaMovimiento.findAll.mockResolvedValueOnce([]);
    const { movs, agregadas } = await _movimientosOpaDeCobranza(args);
    expect(agregadas).toEqual(['OPA-01106 $68.91']);
    expect(movs.map(m => [m.cuentaId, m.debe, m.haber, m.reglaNombre])).toEqual([
      [12, 59.41, 0, 'NI'], [11, 9.5, 0, 'NI'], [10, 0, 59.41, 'OPA-COBRANZA'], [11, 0, 9.5, 'OPA-COBRANZA'],
    ]);
    expect(movs.every(m => m.serie === 'OPA-01106' && m.cfdiUuid === pago.uuid && m.facturaUuid === null)).toBe(true);
  });

  test('OPA ya registrada en otra póliza de Cobranza → no se repite', async () => {
    BankMovement.find.mockReturnValue(lean([DEP_046486]));
    PolizaMovimiento.findAll.mockResolvedValueOnce([{ serie: 'OPA-01106' }]);
    expect((await _movimientosOpaDeCobranza(args)).movs).toEqual([]);
  });

  test('depósito del mismo mes → Cargo al banco por la OPA', async () => {
    BankMovement.find.mockReturnValue(lean([{ ...DEP_046486, fecha: new Date('2026-10-05T12:00:00Z') }]));
    PolizaMovimiento.findAll.mockResolvedValueOnce([]);
    const { movs } = await _movimientosOpaDeCobranza(args);
    expect(movs[0]).toMatchObject({ cuentaId: 13, debe: 68.91, haber: 0 });
  });
});
