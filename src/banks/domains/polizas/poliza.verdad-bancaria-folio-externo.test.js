'use strict';

// 2026-10-27, Cobranza 26-oct: la factura A0-260900821 de BERLI JEANETH
// DESALES (efectivo en caja A0) salía en Azteca "47168". Ese depósito está
// ligado en Bancos a la CUENTA interna A0-260900821 de JESUS JAIME CRUZ
// MIJANGOS (folioFiscal de su propia factura) — el fallback serie+folioExterno
// lo pegaba a la factura con el mismo serie-folio. Solo Cobranza: Ingreso
// conserva el fallback.
jest.mock('../../../shared/models/postgres', () => ({
  AccountPlan: { findAll: jest.fn(async () => [{ id: 16, codigo: '1102016001', nombre: 'Azteca' }]), findOne: jest.fn(async () => null) },
  Terminal: { findAll: jest.fn(async () => []) },
  CfdiMappingRule: {}, PolizaMovimiento: {}, Poliza: {}, CentroCosto: {},
}));
jest.mock('../banks/BankMovement.model', () => ({ find: jest.fn() }));

const BankMovement = require('../banks/BankMovement.model');
const { _construirVerdadBancaria: construir } = require('./poliza.service');

const FACTURA_BERLI = 'AAAAAAAA-0000-0000-0000-000000000001';
const deposito047168 = folioFiscal => ({
  _id: 'az', banco: 'Azteca', folio: '047168', deposito: 22687, fecha: new Date('2026-09-16T12:00:00Z'), categoria: null,
  erpLinks: [{ folioFiscal, serie: 'A0', folioExterno: '260900821', saldoPagado: 12065.04 }],
});
const lean = rows => ({ lean: async () => rows });

beforeEach(() => BankMovement.find.mockReset());

const COBRANZA = { fallbackSoloSinFolioFiscal: true };

test('Cobranza: folioFiscal de otra factura → no se liga por serie+folioExterno', async () => {
  BankMovement.find.mockReturnValueOnce(lean([])).mockReturnValueOnce(lean([deposito047168('C0392C92-C652-4DDA-BBB6-AC96D89A4588')]));
  const mapa = await construir([{ cfdiUuid: FACTURA_BERLI, serie: 'A0-260900821' }], undefined, '2026-10-26', COBRANZA);
  expect(mapa.has(FACTURA_BERLI)).toBe(false);
});

test('Cobranza: folioFiscal null → sigue el fallback por serie+folioExterno', async () => {
  BankMovement.find.mockReturnValueOnce(lean([])).mockReturnValueOnce(lean([deposito047168(null)]));
  const mapa = await construir([{ cfdiUuid: FACTURA_BERLI, serie: 'A0-260900821' }], undefined, '2026-10-26', COBRANZA);
  expect(mapa.get(FACTURA_BERLI)?.referencia).toBe('047168');
});

test('Ingreso (sin opción): se conserva el fallback aunque traiga folioFiscal de otra factura', async () => {
  BankMovement.find.mockReturnValueOnce(lean([])).mockReturnValueOnce(lean([deposito047168('C0392C92-C652-4DDA-BBB6-AC96D89A4588')]));
  const mapa = await construir([{ cfdiUuid: FACTURA_BERLI, serie: 'A0-260900821' }], undefined, '2026-10-26');
  expect(mapa.get(FACTURA_BERLI)?.referencia).toBe('047168');
});
