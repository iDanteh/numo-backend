'use strict';

// backfill-netpay-kore-cache.test.js (2026-10-06) — mismo criterio que migrate-netpay-v2.test.js:
// mockea el modelo y el servicio, NUNCA toca Mongo real ni pega a Kore. consultarFoliosPendientes
// (netpay-reporte.service.js) se mockea completo — ya tiene su propia suite en
// netpay-reporte.service.test.js, acá solo interesa que este script la invoque bien.
jest.mock('../src/banks/domains/erp/NetpayReporte.model');
jest.mock('../src/banks/domains/erp/netpay-reporte.service');

const NetpayReporte = require('../src/banks/domains/erp/NetpayReporte.model');
const { consultarFoliosPendientes } = require('../src/banks/domains/erp/netpay-reporte.service');

const {
  parseArgs,
  _folioPendientes,
  _clasificar,
  imprimirResumenDryRun,
  runDryRun,
  runApply,
} = require('./backfill-netpay-kore-cache');

function fakeFind(result) {
  return { select: jest.fn().mockReturnValue({ lean: jest.fn().mockResolvedValue(result) }) };
}

function folio(referencia, cuenta) {
  return { referencia, koreCache: cuenta ? { consultadoEn: new Date(), cuenta } : null };
}

function reporte(id, folios) {
  return { _id: id, movementIdConfirmado: `mov-${id}`, folios };
}

beforeEach(() => {
  jest.clearAllMocks();
  jest.spyOn(console, 'log').mockImplementation(() => {});
});

describe('parseArgs', () => {
  test('sin flags: modo dry-run por default', () => {
    expect(parseArgs([])).toEqual({ modo: 'dry-run' });
  });

  test('--dry-run explícito: modo dry-run', () => {
    expect(parseArgs(['--dry-run'])).toEqual({ modo: 'dry-run' });
  });

  test('--apply: modo apply', () => {
    expect(parseArgs(['--apply'])).toEqual({ modo: 'apply' });
  });

  test('flag desconocido: arroja', () => {
    expect(() => parseArgs(['--revert'])).toThrow(/no reconocido/);
  });

  test('2 modos a la vez: arroja', () => {
    expect(() => parseArgs(['--dry-run', '--apply'])).toThrow(/Solo se permite un modo/);
  });
});

describe('_folioPendientes', () => {
  test('folio sin koreCache.cuenta cuenta como pendiente', () => {
    const r = reporte('r1', [folio('F1', null)]);
    expect(_folioPendientes(r)).toEqual([folio('F1', null)]);
  });

  test('folio con koreCache.cuenta ya resuelto NO cuenta como pendiente', () => {
    const r = reporte('r1', [folio('F1', { Id: 1 })]);
    expect(_folioPendientes(r)).toEqual([]);
  });

  test('folio sin referencia se ignora aunque no tenga koreCache', () => {
    const r = reporte('r1', [{ referencia: null, koreCache: null }]);
    expect(_folioPendientes(r)).toEqual([]);
  });
});

describe('_clasificar', () => {
  test('separa completos (sin pendientes) de afectados (con pendientes)', () => {
    const completo  = reporte('r1', [folio('F1', { Id: 1 })]);
    const afectado   = reporte('r2', [folio('F2', null), folio('F3', { Id: 2 })]);
    const resultado = _clasificar([completo, afectado]);
    expect(resultado.completos).toEqual([completo]);
    expect(resultado.afectados).toEqual([{ reporte: afectado, pendientes: [folio('F2', null)] }]);
  });
});

describe('runDryRun', () => {
  test('consulta por movementIdConfirmado:{$ne:null} y reporta sin tocar Kore', async () => {
    const completo  = reporte('r1', [folio('F1', { Id: 1 })]);
    const afectado  = reporte('r2', [folio('F2', null)]);
    NetpayReporte.find.mockReturnValue(fakeFind([completo, afectado]));

    const resultado = await runDryRun();

    expect(NetpayReporte.find).toHaveBeenCalledWith({ movementIdConfirmado: { $ne: null } });
    expect(resultado).toEqual({ totalReportesAfectados: 1, totalFoliosPendientes: 1 });
    expect(consultarFoliosPendientes).not.toHaveBeenCalled();
  });

  test('sin reportes afectados: totalFoliosPendientes 0', async () => {
    NetpayReporte.find.mockReturnValue(fakeFind([reporte('r1', [folio('F1', { Id: 1 })])]));
    const resultado = await runDryRun();
    expect(resultado).toEqual({ totalReportesAfectados: 0, totalFoliosPendientes: 0 });
  });
});

describe('runApply', () => {
  test('sin reportes afectados: no llama a consultarFoliosPendientes', async () => {
    NetpayReporte.find.mockReturnValue(fakeFind([reporte('r1', [folio('F1', { Id: 1 })])]));
    const resultado = await runApply();
    expect(consultarFoliosPendientes).not.toHaveBeenCalled();
    expect(resultado).toEqual({ totalResueltos: 0, fallos: [] });
  });

  test('procesa cada reporte afectado secuencialmente y acumula resueltos/fallos', async () => {
    const r1 = reporte('r1', [folio('F1', null), folio('F2', null)]);
    const r2 = reporte('r2', [folio('F3', null)]);
    NetpayReporte.find.mockReturnValue(fakeFind([r1, r2]));
    consultarFoliosPendientes
      .mockResolvedValueOnce({ consultados: 1, fallos: [{ referencia: 'F2', error: 'red caída' }] })
      .mockResolvedValueOnce({ consultados: 1, fallos: [] });

    const resultado = await runApply();

    expect(consultarFoliosPendientes).toHaveBeenNthCalledWith(1, 'r1');
    expect(consultarFoliosPendientes).toHaveBeenNthCalledWith(2, 'r2');
    expect(resultado.totalResueltos).toBe(2);
    expect(resultado.fallos).toEqual([{ reporteId: 'r1', referencia: 'F2', error: 'red caída' }]);
  });

  test('clasifica en consola los fallos "ya no disponible" aparte de otros errores', async () => {
    const r1 = reporte('r1', [folio('F1', null), folio('F2', null)]);
    NetpayReporte.find.mockReturnValue(fakeFind([r1]));
    consultarFoliosPendientes.mockResolvedValueOnce({
      consultados: 0,
      fallos: [
        { referencia: 'F1', error: 'No se encontró la cuenta... puede que la transacción ya no esté disponible...' },
        { referencia: 'F2', error: '429 Too Many Requests, reintentos agotados' },
      ],
    });

    const resultado = await runApply();

    expect(resultado.fallos).toHaveLength(2);
    const logs = console.log.mock.calls.map(c => c.join(' '));
    expect(logs.some(l => l.includes('ya no disponibles en Kore (permanente, no reintentable): 1'))).toBe(true);
    expect(logs.some(l => l.includes('otro error (red/429 agotado — reintentable corriendo de nuevo): 1'))).toBe(true);
  });
});
