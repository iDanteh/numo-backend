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
jest.mock('../../../config/database.postgres', () => ({ sequelize: { query: jest.fn() } }));
jest.mock('../erp/erp-sync.service', () => ({
  obtenerSaldosFavor: jest.fn(), obtenerDesglosesCobroAlmacen: jest.fn(), obtenerDesglosesCobroAlmacenPorCentro: jest.fn(),
  obtenerSaldosFavorPorCentro: jest.fn(), sincronizarCuentasPendientes: jest.fn(), obtenerNombrePersonaTicket: jest.fn(), fotoEstadisticasErp: jest.fn(),
}));

const { sequelize } = require('../../../config/database.postgres');
const { _esFacturaGlobalRefacturacion, _ticketsConCobroYaContabilizado } = require('./cfdi-poliza-generator.service');

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

// Solo se oculta el cargo de la refactura si el cobro del ticket YA está en
// una póliza de Ingreso de un día anterior (póliza 1749, 18-nov, O0|261100672).
describe('_ticketsConCobroYaContabilizado', () => {
  beforeEach(() => sequelize.query.mockReset());

  test('devuelve solo los tickets que ya tienen cobro en una póliza anterior', async () => {
    sequelize.query.mockResolvedValue([{ serie: 'O0', folio: '261100672' }]);
    const r = await _ticketsConCobroYaContabilizado(new Set(['O0|261100672', 'B0|261100999']), { rfc: 'CCO011113663', fechaInicio: '2026-11-19' });
    expect([...r]).toEqual(['O0|261100672']);
    const [, opts] = sequelize.query.mock.calls[0];
    expect(opts.replacements).toEqual({ fechaInicio: '2026-11-19', rfc: 'CCO011113663', pares: [['O0', '261100672'], ['B0', '261100999']] });
  });

  test('si el día de la Factura Global ya no tiene el cobro (se regeneró), NO es refacturación', async () => {
    sequelize.query.mockResolvedValue([]);
    const r = await _ticketsConCobroYaContabilizado(new Set(['O0|261100672']), { rfc: 'CCO011113663', fechaInicio: '2026-11-19' });
    expect(r.size).toBe(0);
  });

  test('sin claves no consulta la base', async () => {
    const r = await _ticketsConCobroYaContabilizado(new Set(), { rfc: 'X', fechaInicio: '2026-11-19' });
    expect(r.size).toBe(0);
    expect(sequelize.query).not.toHaveBeenCalled();
  });
});
