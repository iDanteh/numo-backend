'use strict';

// 2026-11-12, pedido del usuario: "un CFDI tipo 04 no se debe contabilizar, se va
// a la pestaña de CFDIs sustitutos". Antes (regla del 13-ago) el sustituto cuyo
// original era del MISMO mes se contabilizaba con el original + su reversión
// (caso real SANTA ROSA I-1018 28-oct: M0-261000874 sustituye a 83DDC222…, ya
// contabilizado el 21-oct en I-1025). Ahora todos quedan fuera de la póliza.
jest.mock('../../../visor/models/CFDI', () => ({ find: jest.fn() }));

const { _particionarSustitutosPorRiesgo } = require('./sustitutos-cfdi.util');

const sustituto = (originales) => ({ uuid: '81D9623A-7FAD-4CD9-85B8-A8E21DCC7F1D', serie: 'M0', folio: '261000874', tipoDeComprobante: 'I',
  sustituyeA: originales.map(o => o.uuid), originales });

test('original del mismo mes (ya contabilizado): el sustituto no se contabiliza', () => {
  const { excluidos } = _particionarSustitutosPorRiesgo(
    [sustituto([{ uuid: '83DDC222-2B73-430C-AF4B-2B5B787CB13C', ejercicio: 2026, periodo: 10, tipoDeComprobante: 'I' }])],
    { uuidsYaUsados: new Set(['83DDC222-2B73-430C-AF4B-2B5B787CB13C']), ejercicio: 2026, periodo: 10 },
  );
  expect(excluidos).toHaveLength(1);
  expect(excluidos[0].mismoPeriodo).toBe(false);
  expect(excluidos[0].motivo).toBe('ya_contabilizado_en_numo');
});

test('original del mismo mes sin póliza: tampoco se contabiliza (va a la pestaña)', () => {
  const { excluidos } = _particionarSustitutosPorRiesgo(
    [sustituto([{ uuid: 'ORIG', ejercicio: 2026, periodo: 10, tipoDeComprobante: 'I' }])],
    { uuidsYaUsados: new Set(), ejercicio: 2026, periodo: 10 },
  );
  expect(excluidos[0].mismoPeriodo).toBe(false);
});

test('original de un mes anterior: igual que antes, a la pestaña', () => {
  const { excluidos } = _particionarSustitutosPorRiesgo(
    [sustituto([{ uuid: 'ORIG', ejercicio: 2026, periodo: 9, tipoDeComprobante: 'I' }])],
    { uuidsYaUsados: new Set(), ejercicio: 2026, periodo: 10 },
  );
  expect(excluidos[0]).toMatchObject({ mismoPeriodo: false, motivo: 'periodo_anterior' });
});
