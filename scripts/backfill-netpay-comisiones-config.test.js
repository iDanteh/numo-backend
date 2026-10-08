'use strict';

// backfill-netpay-comisiones-config.test.js (2026-10-07) — mismo criterio que
// backfill-netpay-kore-cache.test.js: mockea el modelo Mongo y el servicio de sincronización,
// NUNCA toca Mongo ni Postgres real. sincronizarComisiones/_categoriasDeReporte
// (netpay-comision-sync.service.js) tienen su propia suite en
// netpay-comision-sync.service.test.js — acá solo interesa que este script las invoque bien,
// en el orden cronológico correcto.
jest.mock('../src/banks/domains/erp/NetpayReporte.model');
jest.mock('../src/banks/domains/erp/netpay-comision-sync.service');

const NetpayReporte = require('../src/banks/domains/erp/NetpayReporte.model');
const { sincronizarComisiones, _categoriasDeReporte } = require('../src/banks/domains/erp/netpay-comision-sync.service');

const {
  parseArgs,
  _contarCategorias,
  runDryRun,
  runApply,
} = require('./backfill-netpay-comisiones-config');

function fakeFind(result) {
  return { sort: jest.fn().mockReturnValue({ lean: jest.fn().mockResolvedValue(result) }) };
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

describe('_contarCategorias', () => {
  test('cuenta claves DISTINTAS entre varios reportes, sin duplicar las compartidas', () => {
    _categoriasDeReporte
      .mockReturnValueOnce(new Map([['tarjeta-bbva-credito', {}], ['sucursal-s1', {}]]))
      .mockReturnValueOnce(new Map([['tarjeta-bbva-credito', {}], ['sucursal-s2', {}]]));

    const total = _contarCategorias([{ _id: 'r1' }, { _id: 'r2' }]);

    expect(total).toBe(3); // tarjeta-bbva-credito (compartida entre ambos), sucursal-s1, sucursal-s2
  });

  test('sin reportes: 0', () => {
    expect(_contarCategorias([])).toBe(0);
  });
});

describe('runDryRun', () => {
  test('ordena por fechaMovimiento ASCENDENTE y no escribe nada', async () => {
    const r1 = { _id: 'r1', fechaMovimiento: new Date('2026-01-01') };
    NetpayReporte.find.mockReturnValue(fakeFind([r1]));
    _categoriasDeReporte.mockReturnValue(new Map([['tarjeta-bbva-credito', {}]]));

    const resultado = await runDryRun();

    expect(NetpayReporte.find).toHaveBeenCalledWith({});
    const sortMock = NetpayReporte.find.mock.results[0].value.sort;
    expect(sortMock).toHaveBeenCalledWith({ fechaMovimiento: 1 });
    expect(resultado).toEqual({ totalReportes: 1, totalCategorias: 1 });
    expect(sincronizarComisiones).not.toHaveBeenCalled();
  });

  test('sin reportes: totalCategorias 0', async () => {
    NetpayReporte.find.mockReturnValue(fakeFind([]));
    const resultado = await runDryRun();
    expect(resultado).toEqual({ totalReportes: 0, totalCategorias: 0 });
  });
});

describe('runApply', () => {
  test('procesa cada reporte SECUENCIALMENTE en el orden cronológico recibido (fechaMovimiento ASC)', async () => {
    const r1 = { _id: 'r1', fechaMovimiento: new Date('2026-01-01') };
    const r2 = { _id: 'r2', fechaMovimiento: new Date('2026-02-01') };
    NetpayReporte.find.mockReturnValue(fakeFind([r1, r2]));
    sincronizarComisiones.mockResolvedValue(undefined);

    const resultado = await runApply();

    expect(sincronizarComisiones).toHaveBeenNthCalledWith(1, r1);
    expect(sincronizarComisiones).toHaveBeenNthCalledWith(2, r2);
    expect(resultado).toEqual({ procesados: 2 });
  });

  test('sin reportes: no llama a sincronizarComisiones', async () => {
    NetpayReporte.find.mockReturnValue(fakeFind([]));
    const resultado = await runApply();
    expect(sincronizarComisiones).not.toHaveBeenCalled();
    expect(resultado).toEqual({ procesados: 0 });
  });
});
