'use strict';

// Facturas de hoy / ayer directo de Kore al generar el reporte (2026-10-30).
jest.mock('../../../visor/models/CFDI', () => ({ exists: jest.fn() }));
jest.mock('../../../visor/services/erp.service', () => ({ fetchTodasLasFacturas: jest.fn() }));
jest.mock('../../../visor/services/erp-transformer.service', () => ({ transformarTolerante: jest.fn(f => ({ doc: f, errores: [] })) }));
jest.mock('../../../visor/repositories/cfdi.repository', () => ({ upsertFromERP: jest.fn(async () => ({ isNew: true })) }));
jest.mock('../../../visor/services/reclasificacionGlobal.service', () => ({ aplicarReclasificacion: jest.fn(async () => ({})) }));

const CFDI = require('../../../visor/models/CFDI');
const { fetchTodasLasFacturas } = require('../../../visor/services/erp.service');
const { upsertFromERP } = require('../../../visor/repositories/cfdi.repository');
const { aplicarReclasificacion } = require('../../../visor/services/reclasificacionGlobal.service');
const { traerFacturasRecientes, diasRecientes } = require('./reportes-por-facturar.facturas-kore');

const AHORA = new Date('2026-10-30T18:00:00Z'); // 30-oct 12:00 México

beforeEach(() => jest.clearAllMocks());

test('solo hoy y ayer (hora México) dentro del rango', () => {
  expect(diasRecientes('2026-10-27', '2026-10-30', AHORA)).toEqual(['2026-10-29', '2026-10-30']);
  expect(diasRecientes('2026-10-29', '2026-10-29', AHORA)).toEqual(['2026-10-29']);
  expect(diasRecientes('2026-10-20', '2026-10-28', AHORA)).toEqual([]);
  // 30-oct 23:30 México = 31-oct UTC → sigue siendo "hoy" el 30
  expect(diasRecientes('2026-10-30', '2026-10-30', new Date('2026-10-31T05:30:00Z'))).toEqual(['2026-10-30']);
});

test('rango sin hoy/ayer → no consulta Kore', async () => {
  const r = await traerFacturasRecientes({ series: ['A0'], fechaInicio: '2026-10-20', fechaFin: '2026-10-28', ahora: AHORA });
  expect(r).toEqual({ dias: [], nuevas: 0, error: null });
  expect(fetchTodasLasFacturas).not.toHaveBeenCalled();
});

test('guarda solo Ingreso de las series del reporte que no estén ya; reclasifica', async () => {
  fetchTodasLasFacturas.mockResolvedValue([
    { uuid: 'G1', serie: 'A0', tipoDeComprobante: 'I' },
    { uuid: 'YA', serie: 'A0', tipoDeComprobante: 'I' },
    { uuid: 'P1', serie: 'A0', tipoDeComprobante: 'P' },
    { uuid: 'B1', serie: 'B0', tipoDeComprobante: 'I' },
  ]);
  CFDI.exists.mockImplementation(async q => (q.uuid === 'YA' ? { _id: 1 } : null));
  const r = await traerFacturasRecientes({ series: ['A0'], fechaInicio: '2026-10-30', fechaFin: '2026-10-30', ahora: AHORA });
  expect(fetchTodasLasFacturas).toHaveBeenCalledWith({ fechaInicio: '2026-10-30T06:00:00Z', fechaFin: '2026-10-31T05:59:59Z' });
  expect(upsertFromERP.mock.calls.map(c => c[0].uuid)).toEqual(['G1']);
  expect(r).toEqual({ dias: ['2026-10-30'], nuevas: 1, error: null });
  expect(aplicarReclasificacion).toHaveBeenCalledWith({ ejercicio: 2026, periodo: 10, source: 'ERP' });
});

test('Kore no responde → regresa el error sin tronar', async () => {
  fetchTodasLasFacturas.mockRejectedValue(new Error('El ERP no respondió en 30 segundos (timeout)'));
  const r = await traerFacturasRecientes({ series: ['A0'], fechaInicio: '2026-10-30', fechaFin: '2026-10-30', ahora: AHORA });
  expect(r.error).toMatch(/timeout/);
  expect(aplicarReclasificacion).not.toHaveBeenCalled();
});
