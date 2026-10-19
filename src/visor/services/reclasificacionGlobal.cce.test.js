'use strict';

// Consulta CCE de la reclasificación de Globales: Kore exige UNA serie por
// folio (400 "series y folios de distinta longitud" si no — 2026-10-19).

jest.mock('../models/CFDI', () => ({}));
jest.mock('../models/Comparison', () => ({}));
jest.mock('../../shared/utils/logger', () => ({ logger: { info: jest.fn(), warn: jest.fn(), error: jest.fn() } }));
jest.mock('../../banks/domains/erp/erp-sync.service', () => ({ obtenerDesglosesCobroAlmacen: jest.fn() }));
jest.mock('./periodoFiscal.service', () => ({ derivarPeriodoDesdeFecha: jest.fn() }));

const { obtenerDesglosesCobroAlmacen } = require('../../banks/domains/erp/erp-sync.service');
const { logger } = require('../../shared/utils/logger');
const { _detectarUuidsConCCE } = require('./reclasificacionGlobal.service');

const globalCon = (uuid, serie, folios) => ({
  uuid, serie, emisor: { rfc: 'CCO011113663' },
  conceptos: folios.map(f => ({ noIdentificacion: String(f) })),
});

beforeEach(() => {
  jest.clearAllMocks();
  obtenerDesglosesCobroAlmacen.mockResolvedValue([]);
});

test('manda una serie por cada folio, en lotes de 100', async () => {
  const folios = Array.from({ length: 150 }, (_, i) => 260101600 + i);
  await _detectarUuidsConCCE([globalCon('u1', 'M0', folios)]);

  expect(obtenerDesglosesCobroAlmacen).toHaveBeenCalledTimes(2);
  for (const [{ rfc, series, folios: lote }] of obtenerDesglosesCobroAlmacen.mock.calls) {
    expect(rfc).toBe('CCO011113663');
    expect(series).toHaveLength(lote.length);
    expect(new Set(series)).toEqual(new Set(['M0']));
  }
  expect(obtenerDesglosesCobroAlmacen.mock.calls.map(([a]) => a.folios.length)).toEqual([100, 50]);
});

test('detecta la Global cuyo ticket se cobró por CCE', async () => {
  obtenerDesglosesCobroAlmacen.mockResolvedValue([
    { serieVenta: 'M0', folioVenta: '260101623', cobros: [{ serieOrigen: 'CCE' }] },
    { serieVenta: 'M0', folioVenta: '260101622', cobros: [{ serieOrigen: 'A0' }] },
  ]);
  const uuids = await _detectarUuidsConCCE([
    globalCon('aaa', 'M0', [260101622, 260101623]),
    globalCon('bbb', 'M0', [260101622]),
  ]);
  expect([...uuids]).toEqual(['AAA']);
});

test('una serie por grupo: dos sucursales hacen dos consultas separadas', async () => {
  await _detectarUuidsConCCE([globalCon('u1', 'M0', [1, 2]), globalCon('u2', 'I0', [3])]);
  const llamadas = obtenerDesglosesCobroAlmacen.mock.calls.map(([a]) => [a.series, a.folios]);
  expect(llamadas).toEqual([[['M0', 'M0'], ['1', '2']], [['I0'], ['3']]]);
});

test('si Kore falla, no lanza (solo avisa) y no marca nada', async () => {
  obtenerDesglosesCobroAlmacen.mockRejectedValue(new Error('Request failed with status code 400'));
  const uuids = await _detectarUuidsConCCE([globalCon('u1', 'M0', [1])]);
  expect(uuids.size).toBe(0);
  expect(logger.warn).toHaveBeenCalled();
});
