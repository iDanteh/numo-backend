'use strict';

// "↻ Actualizar" también en Cobranza (2026-11-12, pedido del usuario): la póliza
// de Cobranza es una por día para todas las sucursales (tipo 'D'); se regenera
// con el mismo día, tipo de CFDI 'P' y sin sucursal.
jest.mock('../banks/BankMovement.model', () => ({ find: jest.fn() }));
jest.mock('../../../visor/models/CFDI', () => ({ find: jest.fn() }));
jest.mock('../../../shared/models/postgres', () => ({
  Poliza: {}, PolizaMovimiento: {}, AccountPlan: {}, CfdiMappingRule: {}, CobroSucursalPendienteCobranza: {}, CentroCosto: {},
}));
jest.mock('../erp/erp-sync.service', () => ({
  obtenerSaldosFavor: jest.fn(), obtenerDesglosesCobroAlmacen: jest.fn(), obtenerDesglosesCobroAlmacenPorCentro: jest.fn(),
  obtenerSaldosFavorPorCentro: jest.fn(), sincronizarCuentasPendientes: jest.fn(), obtenerNombrePersonaTicket: jest.fn(), fotoEstadisticasErp: jest.fn(),
}));

const { _parametrosRegeneracion } = require('./cfdi-poliza-generator.service');

test('Cobranza de un día: mismo día, CFDI P, todas las sucursales', async () => {
  const p = await _parametrosRegeneracion({ tipo: 'D', concepto: 'Ingresos por Cobranza del Día: 07/11/2026' });
  expect(p).toEqual({ tipoCfdi: 'P', tipoPropuesta: 'D', centroCostoId: null, sucursal: 'Todas las sucursales', fechaInicio: '2026-11-07', fechaFin: '2026-11-07' });
});

test('Cobranza de un rango de días', async () => {
  const p = await _parametrosRegeneracion({ tipo: 'D', concepto: 'Ingresos por Cobranza del Día: 01/11/2026 al 03/11/2026' });
  expect([p.fechaInicio, p.fechaFin]).toEqual(['2026-11-01', '2026-11-03']);
});

test('otras pólizas tipo D (no Cobranza) o Traspasos: no se pueden actualizar', async () => {
  await expect(_parametrosRegeneracion({ tipo: 'D', concepto: 'Póliza de diario manual' })).rejects.toThrow(/Ingreso o de Cobranza/);
  await expect(_parametrosRegeneracion({ tipo: 'T', concepto: 'Traspasos entre cuentas propias — 2026-10-15' })).rejects.toThrow(/Ingreso o de Cobranza/);
});
