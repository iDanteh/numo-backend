'use strict';

// Cobranza (2026-10-14, póliza 1247 del 13-oct): todo el efectivo va a BBVA
// 1102011001 — el de la caja A0 sin ficha, el de cada sucursal y el
// EFECTIVO-COS de cualquier caja. Tarjeta, PXA, NI y abonos no se tocan.
jest.mock('../banks/BankMovement.model', () => ({ find: jest.fn() }));
jest.mock('../../../visor/models/CFDI', () => ({ exists: jest.fn(), find: jest.fn() }));
jest.mock('../../../shared/models/postgres', () => ({
  Poliza: {}, PolizaMovimiento: { findAll: jest.fn(async () => []) }, AccountPlan: {}, CfdiMappingRule: {}, CobroSucursalPendienteCobranza: {},
}));

const { _efectivoABanco } = require('./cobranza-poliza-generator.service');

const CAJA = 3, BBVA = 11, CAJA_GENERAL = 1, CLIENTES = 5;
const cuentaMap = { '1101010003': CAJA, '1102011001': BBVA, '1101010001': CAJA_GENERAL, '1103010001': CLIENTES };

test('efectivo en Caja → BBVA; tarjeta, PXA y abonos igual', () => {
  const movs = [
    { cuentaId: CAJA, debe: 20049.99, haber: 0, _formaPagoReal: '01', serie: 'A0-1' },                         // EFECTIVO 300 sin ficha
    { cuentaId: CAJA, debe: 7033.58, haber: 0, _formaPagoReal: '01', serie: 'B0-1' },                          // EFECTIVO 111
    { cuentaId: CAJA, debe: 3679.51, haber: 0, _formaPagoReal: '01', serie: 'EFECTIVO-COS' },                  // otra caja (C0)
    { cuentaId: CAJA, debe: 500, haber: 0, serie: 'A0-2' },                                                    // sin desglose (Pago en efectivo)
    { cuentaId: CAJA, debe: 900, haber: 0, _formaPagoReal: '04', serie: 'A0-3' },                              // tarjeta en la cuenta de la regla
    { cuentaId: CAJA_GENERAL, debe: 120, haber: 0, _formaPagoReal: '04', serie: 'TARJETA DE CREDITO' },
    { cuentaId: CAJA_GENERAL, debe: 300, haber: 0, serie: 'PXA' },
    { cuentaId: CAJA, debe: 0, haber: 50, serie: 'X' },
    { cuentaId: CLIENTES, debe: 0, haber: 20049.99 },
  ];
  _efectivoABanco(movs, cuentaMap);
  expect(movs.map(m => m.cuentaId)).toEqual([BBVA, BBVA, BBVA, BBVA, CAJA, CAJA_GENERAL, CAJA_GENERAL, CAJA, CLIENTES]);
});

test('sin la cuenta BBVA en el catálogo → no toca nada', () => {
  const movs = [{ cuentaId: CAJA, debe: 100, haber: 0, _formaPagoReal: '01' }];
  _efectivoABanco(movs, { '1101010003': CAJA });
  expect(movs[0].cuentaId).toBe(CAJA);
});
