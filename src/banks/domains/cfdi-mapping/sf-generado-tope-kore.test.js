'use strict';

// Saldo a favor de una NC = lo que Kore realmente dejó a favor, nunca más que eso
// (2026-11-12). Caso real JOSUE ISIDRO LOPEZ MARTINEZ, CEDIS 7-nov: NC de
// bonificación A0-261102119 (BON-325848) $329.86 sobre la venta A0-261001339.
// Kore la aplicó directo al saldo de la venta (RET-261103385 $329.85) y solo
// generó $0.01 de saldo a favor, pero la póliza registraba los $329.86 como SF
// (2103090001 $284.36 + 2104010002 $45.50). Las devoluciones con todo el monto
// a favor (DEV-059087 $360.66, Kore $360.66) siguen igual.
jest.mock('../banks/BankMovement.model', () => ({ find: jest.fn() }));
jest.mock('../../../visor/models/CFDI', () => ({ find: jest.fn(), findOne: jest.fn(() => ({ lean: async () => null })) }));
jest.mock('../../../shared/models/postgres', () => ({
  Poliza: {}, PolizaMovimiento: {}, AccountPlan: {}, CfdiMappingRule: {}, CobroSucursalPendienteCobranza: {}, CentroCosto: {},
  CobroSucursalPendiente: { destroy: jest.fn(async () => 0) },
}));
jest.mock('../erp/erp-sync.service', () => ({
  obtenerSaldosFavor: jest.fn(async () => []), obtenerDesglosesCobroAlmacen: jest.fn(), obtenerDesglosesCobroAlmacenPorCentro: jest.fn(),
  obtenerSaldosFavorPorCentro: jest.fn(), sincronizarCuentasPendientes: jest.fn(), obtenerNombrePersonaTicket: jest.fn(), fotoEstadisticasErp: jest.fn(),
}));

const { _inyectarSaldoFavorGenerado } = require('./cfdi-poliza-generator.service');

const SF = 50, IVA_SF = 51;
const cc = { id: 9, clave: '300' };
const inyectar = (cfdi, generado) => _inyectarSaldoFavorGenerado({
  cfdi, mapaGenerados: new Map([[`${cfdi.documentosRelacionados[0].Serie}|${cfdi.documentosRelacionados[0].Folio}`, generado]]),
  cuentaSaldoFavorId: SF, cuentaIvaSaldoFavorId: IVA_SF, cuentaCajaId: 1, cuentaBancosId: 2, cc, rfc: 'CCO011113663',
  origenesConvertidosAAnticipo: new Set(), anticiposClasificados: [],
});
const suma = lineas => Math.round(lineas.reduce((s, l) => s + (Number(l.haber) || 0), 0) * 100) / 100;

test('bonificación aplicada a la cuenta: solo el centavo que Kore dejó a favor (a Otros Ingresos)', async () => {
  const nc = { uuid: '2D665747-5D5C-448E-9365-F04C53EF59EC', tipoDeComprobante: 'E', serie: 'A0', folio: '261102119', total: 329.86,
    documentosRelacionados: [{ Serie: 'BON', Folio: '325848' }], receptor: { nombre: 'JOSUE ISIDRO LOPEZ MARTINEZ' } };
  const lineas = await inyectar(nc, { monto: 0.01, montoGenerado: 0.01, montoUsadoKore: 0, ventaSerie: 'A0', ventaFolio: '261001339', oculto: false });
  expect(suma(lineas)).toBe(0.01);
  expect(lineas.every(l => l.reglaNombre === 'SF-MENOR-SIN-USAR')).toBe(true);
});

test('devolución con todo el monto a favor: sin cambio', async () => {
  const dev = { uuid: 'DEV-1', tipoDeComprobante: 'E', serie: 'C0', folio: '261100034', total: 360.66,
    documentosRelacionados: [{ Serie: 'DEV', Folio: '059087' }], receptor: { nombre: 'X' } };
  const lineas = await inyectar(dev, { monto: 360.66, montoGenerado: 360.66, montoUsadoKore: 100, ventaSerie: 'C0', ventaFolio: '261006353', oculto: false });
  expect(suma(lineas)).toBe(360.66);
  expect(lineas.find(l => l.cuentaId === SF).haber).toBe(310.91);
  expect(lineas.find(l => l.cuentaId === IVA_SF).haber).toBe(49.75);
});

test('misma venta cancelada en 2 NC: cada una lleva su propio total (no el generado completo)', async () => {
  const nc = { uuid: 'NC-1', tipoDeComprobante: 'E', serie: 'B0', folio: '1', total: 100,
    documentosRelacionados: [{ Serie: 'CAC', Folio: '1' }], receptor: { nombre: 'X' } };
  const lineas = await inyectar(nc, { monto: 300, montoGenerado: 300, montoUsadoKore: 300, ventaSerie: 'B0', ventaFolio: '2', oculto: false });
  expect(suma(lineas)).toBe(100);
});
