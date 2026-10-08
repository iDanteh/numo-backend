'use strict';

// netpay-comision-sync.service.test.js (2026-10-07) — cobertura de _categoriasDeReporte
// (detección de categorías tarjeta/sucursal desde los folios de un reporte), _slug
// (normalización de claves), _seccionDeClave (ruteo tarjeta/sucursal a su ConfigSection) y
// sincronizarComisiones (guard de "valor sin cambio" contra ConfigAuditLog, aislamiento de
// fallos por categoría, best-effort completo). Mockea global-config.service.js entero — nunca
// toca Postgres real.
//
// Fix de agrupación por sucursal (2026-10-07): la categoría de sucursal agrupa por NOMBRE real
// de la sucursal (no por storeId — storeId identifica una terminal, no el local físico; ver
// netpay-comision-sync.service.js para el caso real de producción que motivó el fix).
jest.mock('../../../shared/services/global-config.service');

const globalConfigService = require('../../../shared/services/global-config.service');
const {
  sincronizarComisiones, _categoriasDeReporte, _slug, _seccionDeClave,
  SECCION_TARJETAS, SECCION_SUCURSALES,
} = require('./netpay-comision-sync.service');

function folio(overrides = {}) {
  return {
    banco: 'BBVA', tipoTarjeta: 'Crédito', storeId: 'S1', sucursal: 'Ferrocarril',
    comisionBasePct: 1.59,
    ...overrides,
  };
}

beforeEach(() => {
  jest.clearAllMocks();
});

describe('_slug', () => {
  test('minúsculas, sin acentos, espacios colapsados a un guión', () => {
    expect(_slug('BBVA Crédito')).toBe('bbva-credito');
  });

  test('múltiples caracteres no alfanuméricos seguidos colapsan a UN solo guión', () => {
    expect(_slug('Sucursal  Oaxaca -- 02')).toBe('sucursal-oaxaca-02');
  });

  test('sin guiones al principio/final', () => {
    expect(_slug('  ¡Hola!  ')).toBe('hola');
  });

  test('null/undefined no explota', () => {
    expect(_slug(null)).toBe('');
    expect(_slug(undefined)).toBe('');
  });
});

describe('_seccionDeClave', () => {
  test('clave tarjeta-* va a SECCION_TARJETAS', () => {
    expect(_seccionDeClave('tarjeta-bbva-credito')).toBe(SECCION_TARJETAS);
  });

  test('clave sucursal-* va a SECCION_SUCURSALES', () => {
    expect(_seccionDeClave('sucursal-ferrocarril')).toBe(SECCION_SUCURSALES);
  });
});

describe('_categoriasDeReporte', () => {
  test('folio completo: genera AMBAS categorías (tarjeta y sucursal)', () => {
    const reporte = { folios: [folio()] };
    const categorias = _categoriasDeReporte(reporte);

    expect(categorias.size).toBe(2);
    expect(categorias.get('tarjeta-bbva-credito')).toMatchObject({ pct: 1.59 });
    expect(categorias.get('sucursal-ferrocarril')).toMatchObject({ pct: 1.59 });
  });

  test('falta banco o tipoTarjeta: NO genera categoría de tarjeta (pero sí la de sucursal)', () => {
    const reporte = { folios: [folio({ banco: null })] };
    const categorias = _categoriasDeReporte(reporte);

    expect([...categorias.keys()].some(k => k.startsWith('tarjeta-'))).toBe(false);
    expect(categorias.has('sucursal-ferrocarril')).toBe(true);
  });

  test('falta storeId o sucursal: NO genera categoría de sucursal (pero sí la de tarjeta)', () => {
    const reporte = { folios: [folio({ sucursal: null })] };
    const categorias = _categoriasDeReporte(reporte);

    expect([...categorias.keys()].some(k => k.startsWith('sucursal-'))).toBe(false);
    expect(categorias.has('tarjeta-bbva-credito')).toBe(true);
  });

  test('comisionBasePct null: el folio no aporta a NINGUNA categoría', () => {
    const reporte = { folios: [folio({ comisionBasePct: null })] };
    expect(_categoriasDeReporte(reporte).size).toBe(0);
  });

  test('redondea a 2 decimales (fix de precisión de float del parseo del Excel)', () => {
    const reporte = { folios: [folio({ comisionBasePct: 1.5900000000000001 })] };
    const categorias = _categoriasDeReporte(reporte);
    expect(categorias.get('tarjeta-bbva-credito').pct).toBe(1.59);
  });

  test('2 folios con la MISMA clave y distinto valor dentro del mismo reporte: el ÚLTIMO gana', () => {
    const reporte = {
      folios: [
        folio({ comisionBasePct: 1.59 }),
        folio({ comisionBasePct: 3.75 }),
      ],
    };
    const categorias = _categoriasDeReporte(reporte);
    expect(categorias.get('tarjeta-bbva-credito').pct).toBe(3.75);
    expect(categorias.get('sucursal-ferrocarril').pct).toBe(3.75);
  });

  // Fix de agrupación (2026-10-07, caso real de producción): storeId=1196184 y storeId=1650292
  // son AMBOS "AV FERROCARRIL 802" — storeId identifica una terminal, no la sucursal física.
  // Antes del fix, cada storeId generaba su propia clave (sucursal-1196184, sucursal-1650292),
  // duplicando la sucursal con comisiones que deberían tratarse como la MISMA entidad.
  test('2 folios con MISMO nombre de sucursal pero DISTINTO storeId: colapsan en la MISMA clave (misma sucursal física, distinta terminal)', () => {
    const reporte = {
      folios: [
        folio({ storeId: '1196184', sucursal: 'AV FERROCARRIL 802', comisionBasePct: 0.02, banco: null }),
        folio({ storeId: '1650292', sucursal: 'AV FERROCARRIL 802', comisionBasePct: 0.01, banco: null }),
      ],
    };
    const categorias = _categoriasDeReporte(reporte);

    expect(categorias.size).toBe(1);
    expect(categorias.get('sucursal-av-ferrocarril-802')).toMatchObject({ pct: 0.01 }); // el último folio gana
  });

  test('sin folios: Map vacío', () => {
    expect(_categoriasDeReporte({ folios: [] }).size).toBe(0);
    expect(_categoriasDeReporte({}).size).toBe(0);
  });
});

describe('sincronizarComisiones', () => {
  test('valor sin cambio: NO llama a setValue (evita ensuciar ConfigAuditLog)', async () => {
    globalConfigService.getValue.mockResolvedValue('1.59');
    const reporte = { folios: [folio({ comisionBasePct: 1.59 })] };

    await sincronizarComisiones(reporte);

    expect(globalConfigService.setValue).not.toHaveBeenCalled();
  });

  test('valor distinto al actual: llama a setValue con el nuevo valor, en la sección de SUCURSALES', async () => {
    globalConfigService.getValue.mockResolvedValue('1.59');
    const reporte = { folios: [folio({ comisionBasePct: 3.75, banco: null })] }; // solo categoría sucursal

    await sincronizarComisiones(reporte);

    expect(globalConfigService.setValue).toHaveBeenCalledWith(
      SECCION_SUCURSALES, 'sucursal-ferrocarril', '3.75',
      expect.objectContaining({ tipo: 'numero' }),
    );
  });

  test('categoría de tarjeta se escribe en la sección de TARJETAS', async () => {
    globalConfigService.getValue.mockResolvedValue(null);
    const reporte = { folios: [folio({ sucursal: null, storeId: null })] }; // solo categoría tarjeta

    await sincronizarComisiones(reporte);

    expect(globalConfigService.setValue).toHaveBeenCalledWith(
      SECCION_TARJETAS, 'tarjeta-bbva-credito', '1.59',
      expect.objectContaining({ tipo: 'numero' }),
    );
  });

  test('clave sin valor previo (getValue tira): llama a setValue igual, como alta nueva', async () => {
    globalConfigService.getValue.mockRejectedValue(new Error('no existe'));
    const reporte = { folios: [folio({ banco: null })] }; // solo categoría sucursal

    await sincronizarComisiones(reporte);

    expect(globalConfigService.setValue).toHaveBeenCalledWith(
      SECCION_SUCURSALES, 'sucursal-ferrocarril', '1.59',
      expect.any(Object),
    );
  });

  test('una categoría que falla NO aborta las demás', async () => {
    globalConfigService.getValue.mockResolvedValue(null);
    globalConfigService.setValue
      .mockRejectedValueOnce(new Error('Postgres caído'))
      .mockResolvedValueOnce(1);
    const reporte = { folios: [folio()] }; // genera tarjeta-bbva-credito Y sucursal-ferrocarril

    await expect(sincronizarComisiones(reporte)).resolves.toBeUndefined();

    expect(globalConfigService.setValue).toHaveBeenCalledTimes(2);
  });

  test('nunca tira, incluso si TODO falla', async () => {
    globalConfigService.getValue.mockRejectedValue(new Error('falló'));
    globalConfigService.setValue.mockRejectedValue(new Error('falló también'));
    const reporte = { folios: [folio()] };

    await expect(sincronizarComisiones(reporte)).resolves.toBeUndefined();
  });

  test('reporte sin categorías detectables: no llama ni a getValue ni a setValue', async () => {
    const reporte = { folios: [folio({ comisionBasePct: null })] };

    await sincronizarComisiones(reporte);

    expect(globalConfigService.getValue).not.toHaveBeenCalled();
    expect(globalConfigService.setValue).not.toHaveBeenCalled();
  });
});
