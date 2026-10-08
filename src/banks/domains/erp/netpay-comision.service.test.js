'use strict';

// netpay-comision.service.test.js — cobertura para obtenerVariacionComisiones() (pedido
// explícito del usuario, 2026-10-07): agrupa folios de TODOS los NetpayReporte por
// storeId+sucursal para detectar si Netpay cambió la tasa de comisión negociada por almacén.
// Dos correcciones de revisión de confiabilidad (2026-10-07) cubiertas acá: (1) folios sin
// storeId/sucursal ya NO se mezclan en un almacén fantasma compartido, (2) el redondeo evita
// que el drift de precisión de parseFloat reporte una "variación" falsa entre valores
// semánticamente iguales (0.0159 vs 0.015900000000000001).
// comisionBasePct es una FRACCIÓN (0.0159 = 1.59%, no 1.59) — ver bug real corregido 2026-10-08
// contra un documento de producción (_redondear en netpay-comision.service.js); estos fixtures
// usan el valor real tal como lo persiste netpay-reporte-parser.service.js.
jest.mock('./NetpayReporte.model');

const NetpayReporte = require('./NetpayReporte.model');
const { obtenerVariacionComisiones } = require('./netpay-comision.service');

beforeEach(() => {
  jest.clearAllMocks();
});

describe('obtenerVariacionComisiones', () => {
  test('mismo storeId+sucursal con 2 tasas distintas a lo largo del tiempo: variacion:true, ambas tasas listadas', async () => {
    NetpayReporte.aggregate = jest.fn()
      .mockResolvedValueOnce([
        { _id: { storeId: 'S1', sucursal: 'Oaxaca 02', pct: 0.0159 }, primera: new Date('2026-01-01'), ultima: new Date('2026-03-01'), cantidad: 10 },
        { _id: { storeId: 'S1', sucursal: 'Oaxaca 02', pct: 0.0375 }, primera: new Date('2026-04-01'), ultima: new Date('2026-09-01'), cantidad: 5 },
      ])
      .mockResolvedValueOnce([]);

    const resultado = await obtenerVariacionComisiones();

    expect(resultado.almacenes).toHaveLength(1);
    expect(resultado.almacenes[0]).toMatchObject({ storeId: 'S1', sucursal: 'Oaxaca 02', variacion: true });
    expect(resultado.almacenes[0].tasas).toHaveLength(2);
    expect(resultado.almacenes[0].tasas.map(t => t.comisionBasePct).sort()).toEqual([0.0159, 0.0375]);
  });

  test('una sola tasa estable: variacion:false', async () => {
    NetpayReporte.aggregate = jest.fn()
      .mockResolvedValueOnce([
        { _id: { storeId: 'S2', sucursal: 'Ferrocarril', pct: 0.0159 }, primera: new Date('2026-01-01'), ultima: new Date('2026-09-01'), cantidad: 40 },
      ])
      .mockResolvedValueOnce([]);

    const resultado = await obtenerVariacionComisiones();

    expect(resultado.almacenes[0].variacion).toBe(false);
    expect(resultado.almacenes[0].tasas).toHaveLength(1);
  });

  test('fix 2026-10-07 (precisión de float): 0.0159 y 0.015900000000000001 son la MISMA tasa tras redondear, no generan variacion:true', async () => {
    NetpayReporte.aggregate = jest.fn()
      .mockResolvedValueOnce([
        { _id: { storeId: 'S3', sucursal: 'Centro', pct: 0.0159 }, primera: new Date('2026-01-01'), ultima: new Date('2026-02-01'), cantidad: 3 },
        { _id: { storeId: 'S3', sucursal: 'Centro', pct: 0.015900000000000001 }, primera: new Date('2026-03-01'), ultima: new Date('2026-04-01'), cantidad: 7 },
      ])
      .mockResolvedValueOnce([]);

    const resultado = await obtenerVariacionComisiones();

    expect(resultado.almacenes).toHaveLength(1);
    expect(resultado.almacenes[0].variacion).toBe(false);
    expect(resultado.almacenes[0].tasas).toHaveLength(1);
    expect(resultado.almacenes[0].tasas[0].comisionBasePct).toBe(0.0159);
    // Las 2 filas de Mongo se fusionan en 1 sola tasa: cantidad sumada, rango de fechas unido.
    expect(resultado.almacenes[0].tasas[0].cantidad).toBe(10);
    expect(resultado.almacenes[0].tasas[0].primera).toEqual(new Date('2026-01-01'));
    expect(resultado.almacenes[0].tasas[0].ultima).toEqual(new Date('2026-04-01'));
  });

  test('fix 2026-10-07 (agrupamiento fantasma): el $match excluye storeId/sucursal null de los almacenes reales y los cuenta aparte', async () => {
    NetpayReporte.aggregate = jest.fn()
      .mockResolvedValueOnce([
        { _id: { storeId: 'S4', sucursal: 'Reforma', pct: 0.021 }, primera: new Date('2026-01-01'), ultima: new Date('2026-01-01'), cantidad: 1 },
      ])
      .mockResolvedValueOnce([{ cantidad: 6 }]);

    const resultado = await obtenerVariacionComisiones();

    expect(resultado.almacenes.find(a => a.storeId == null)).toBeUndefined();
    expect(resultado.foliosSinAlmacenIdentificado).toBe(6);
    const pipelinePrincipal = NetpayReporte.aggregate.mock.calls[0][0];
    const matchStage = pipelinePrincipal.find(stage => stage.$match && 'folios.storeId' in stage.$match);
    expect(matchStage.$match['folios.storeId']).toEqual({ $ne: null });
    expect(matchStage.$match['folios.sucursal']).toEqual({ $ne: null });
  });

  test('bug real 2026-10-08: 0.75% y 1.25% son tasas DISTINTAS, el redondeo no debe fusionarlas en el mismo punto porcentual', async () => {
    NetpayReporte.aggregate = jest.fn()
      .mockResolvedValueOnce([
        { _id: { storeId: 'S5', sucursal: 'Santa Rosa', pct: 0.0075 }, primera: new Date('2026-09-01'), ultima: new Date('2026-09-15'), cantidad: 44 },
        { _id: { storeId: 'S5', sucursal: 'Santa Rosa', pct: 0.0125 }, primera: new Date('2026-09-16'), ultima: new Date('2026-10-07'), cantidad: 20 },
      ])
      .mockResolvedValueOnce([]);

    const resultado = await obtenerVariacionComisiones();

    expect(resultado.almacenes[0].variacion).toBe(true);
    expect(resultado.almacenes[0].tasas.map(t => t.comisionBasePct).sort()).toEqual([0.0075, 0.0125]);
  });

  test('sin folios con comisión: devuelve almacenes vacío y 0 sin identificar', async () => {
    NetpayReporte.aggregate = jest.fn().mockResolvedValueOnce([]).mockResolvedValueOnce([]);

    const resultado = await obtenerVariacionComisiones();

    expect(resultado.almacenes).toEqual([]);
    expect(resultado.foliosSinAlmacenIdentificado).toBe(0);
  });
});
