'use strict';

// Refacturación sin relación 04 (2026-11-20, caso real Puerto Escondido
// O0-261100474 del ticket O0-261100672): la factura nominativa que reemplaza
// al ticket sacado de la Factura Global ya no exige tipoRelacion='04'; basta
// con que NO sea Factura Global (el falso positivo de Atzompa era una FG).
jest.mock('../banks/BankMovement.model', () => ({ find: jest.fn() }));
jest.mock('../../../visor/models/CFDI', () => ({ find: jest.fn() }));
jest.mock('../../../shared/models/postgres', () => ({
  Poliza: {}, PolizaMovimiento: {}, AccountPlan: {}, CfdiMappingRule: {}, CobroSucursalPendienteCobranza: {}, CentroCosto: {},
}));
jest.mock('../erp/erp-sync.service', () => ({
  obtenerSaldosFavor: jest.fn(), obtenerDesglosesCobroAlmacen: jest.fn(), obtenerDesglosesCobroAlmacenPorCentro: jest.fn(),
  obtenerSaldosFavorPorCentro: jest.fn(), sincronizarCuentasPendientes: jest.fn(), obtenerNombrePersonaTicket: jest.fn(), fotoEstadisticasErp: jest.fn(),
}));

const { _esFacturaGlobalRefacturacion } = require('./cfdi-poliza-generator.service');

describe('_esFacturaGlobalRefacturacion', () => {
  test('factura nominativa sin 04 (O0-261100474, JOSE JORGE) NO es global → puede ser refacturación', () => {
    const cfdi = { serie: 'O0', folio: '261100474', receptor: { rfc: 'COAJ651020CU3' }, cfdiRelacionados: [] };
    const erp = { documentosRelacionados: [{ Serie: 'O0', Folio: '261100672' }] };
    expect(_esFacturaGlobalRefacturacion(cfdi, erp)).toBe(false);
  });

  test('Factura Global (InformacionGlobal en ERP) nunca es refacturación', () => {
    const cfdi = { serie: 'O0', folio: '261100433', receptor: { rfc: 'COAJ651020CU3' } };
    expect(_esFacturaGlobalRefacturacion(cfdi, { informacionGlobal: { periodicidad: '01', mes: '11', anio: '2026' } })).toBe(true);
  });

  test('receptor PUBLICO EN GENERAL (XAXX010101000) se trata como global', () => {
    expect(_esFacturaGlobalRefacturacion({ receptor: { rfc: 'xaxx010101000' } }, {})).toBe(true);
  });
});
