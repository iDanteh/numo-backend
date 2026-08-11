'use strict';

// erp.routes.test.js — _aporteConRatchet(): función NUEVA (fix 2026-08-06, folio 032686).
// Un depósito bancario ya identificado por un humano NUNCA debe bajar su aporte en una
// corrida posterior de "Recalcular saldo ERP", sin importar la causa (RET/CAC/DEV,
// retención, cancelación, devolución) — solo sube si Kore trae un monto MAYOR atribuible a
// este movimiento. Requerir el router real no pega a Mongo/Kore — los modelos solo
// declaran esquemas y las llamadas HTTP viven dentro de handlers, nunca a nivel de módulo.
//
// Tests de la ruta GET /cuentas-pendientes — 2026-08-07: se mockean auth.real
// (authenticate/permit) para controlar el permiso del usuario de prueba vía headers,
// y rbac-store (chequeo inline de banks:erp:anticipos que NO pasa por permit()) —
// mismo criterio de "mockear los límites de I/O" que ya usa
// collection-request.identificar.test.js en este mismo repo.
jest.mock('../../shared/middleware/auth.real', () => ({
  authenticate: (req, _res, next) => {
    req.user = {
      _id:  'user-test',
      role: 'test-role',
      extraPermissions: [],
    };
    next();
  },
  permit: (...perms) => (req, res, next) => {
    const granted = JSON.parse(req.headers['x-test-permissions'] || '[]');
    const ok = perms.every(p => granted.includes(p));
    if (!ok) {
      return res.status(403).json({ error: 'Permisos insuficientes para esta acción.', required: perms });
    }
    next();
  },
}));

jest.mock('../../../shared/services/rbac-store', () => ({
  hasPermission:     jest.fn(),
  hasAllPermissions:  jest.fn(),
  invalidate:         jest.fn(),
  getPermissions:     jest.fn(),
  roleExists:         jest.fn(),
}));

jest.mock('./kore-caja.service', () => ({
  KoreCajaError:          class KoreCajaError extends Error {},
  koreTokenCache:         new Map(),
  KORE_CAJA_BASE_URL:     'http://kore.test',
  obtenerSesionCaja:      jest.fn(),
  obtenerCuentasKore:     jest.fn(),
  aplicarCobroOperacion:  jest.fn(),
  aplicarCobroOperacionMultiple: jest.fn(),
  listarBancos:           jest.fn(),
  listarFormasPago:       jest.fn(),
}));

// erp-sync.service (sincronizarCuentasPendientes) — límite de I/O real de
// GET /cuenta-por-serie-folio (2026-08-07, segunda parte del buscador de CFDI). Los tests
// preexistentes de este archivo nunca lo necesitaron porque solo cubrían el 403 de
// permisos, que corta ANTES de llegar a esta llamada.
jest.mock('./erp-sync.service', () => ({
  sincronizarCuentasPendientes: jest.fn(),
}));

// CFDI (dominio visor) — límite de I/O real del fallback nuevo de GET /cuenta-por-serie-folio
// (2026-08-10: factura liquidada al 100%, Kore la excluye de /cuentas-pendientes sin importar
// el rango de fecha). Mismo patrón de mock ya usado en bank.routes.test.js para /cfdis/buscar.
jest.mock('../../../visor/models/CFDI', () => ({
  findOne: jest.fn(() => ({
    lean: jest.fn(),
  })),
}));

const express      = require('express');
const request      = require('supertest');
const router       = require('./erp.routes');
const rbacStore    = require('../../../shared/services/rbac-store');
const koreCaja     = require('./kore-caja.service');
const { sincronizarCuentasPendientes } = require('./erp-sync.service');
const CFDI         = require('../../../visor/models/CFDI');
const { PERMISSIONS } = require('../../../shared/config/rbac');

describe('_aporteConRatchet', () => {
  test('caso real 032686: calculado null (sin tag Aut/Numo) + saldoErpAportado/saldoPagadoTotal ya en null + un único link -> usa el monto del depósito bancario como piso', () => {
    const link = { saldoErpAportado: null, saldoPagadoTotal: null };
    const mov  = { deposito: 3620.48, retiro: null };

    const resultado = router._aporteConRatchet(link, null, mov, 1);

    expect(resultado).toBe(3620.48);
  });

  test('nunca baja: calculado trae menos que el piso ya confirmado (ej. una devolución/retención posterior)', () => {
    const link = { saldoErpAportado: 3620.48, saldoPagadoTotal: null };
    const mov  = { deposito: 3620.48, retiro: null };

    const resultado = router._aporteConRatchet(link, 0, mov, 1);

    expect(resultado).toBe(3620.48);
  });

  test('sí sube: calculado trae MÁS que el piso ya confirmado (bonificación real atribuible a este movimiento)', () => {
    const link = { saldoErpAportado: 3620.48, saldoPagadoTotal: null };
    const mov  = { deposito: 3620.48, retiro: null };

    const resultado = router._aporteConRatchet(link, 4000, mov, 1);

    expect(resultado).toBe(4000);
  });

  test('piso usa saldoPagadoTotal cuando saldoErpAportado nunca se determinó', () => {
    const link = { saldoErpAportado: null, saldoPagadoTotal: 1500 };
    const mov  = { deposito: 1500, retiro: null };

    const resultado = router._aporteConRatchet(link, null, mov, 1);

    expect(resultado).toBe(1500);
  });

  test('sin ningún piso disponible y calculado null (link recién detectado, aún sin confirmar) -> null', () => {
    const link = { saldoErpAportado: null, saldoPagadoTotal: null };
    const mov  = { deposito: 500, retiro: null };

    const resultado = router._aporteConRatchet(link, null, mov, 2);

    expect(resultado).toBeNull();
  });

  test('el fallback al monto del depósito NUNCA aplica si el movimiento tiene más de un erpLink (ambiguo, no se puede atribuir todo el depósito a un solo link)', () => {
    const link = { saldoErpAportado: null, saldoPagadoTotal: null };
    const mov  = { deposito: 5000, retiro: null };

    const resultado = router._aporteConRatchet(link, null, mov, 2);

    expect(resultado).toBeNull();
  });

  test('retiro se usa como piso cuando el movimiento es un cargo (deposito null)', () => {
    const link = { saldoErpAportado: null, saldoPagadoTotal: null };
    const mov  = { deposito: null, retiro: 800 };

    const resultado = router._aporteConRatchet(link, null, mov, 1);

    expect(resultado).toBe(800);
  });
});

// _FILTRO_LINK_ATRAPADO — fix 2026-08-06: antes exigía conciliacionFinalizadaAt != null,
// invisible para los links de Solicitudes de Cobro/Aplicar cobro manual (ese campo NUNCA se
// llena en ese flujo). Ahora el patrón "atrapado" (checkpoint avanzó, folioFiscal sin
// resolver) no exige de qué flujo vino el link.
describe('_FILTRO_LINK_ATRAPADO', () => {
  test('ya no exige conciliacionFinalizadaAt', () => {
    expect(router._FILTRO_LINK_ATRAPADO).not.toHaveProperty('conciliacionFinalizadaAt');
  });

  test('solo exige checkpoint avanzado + folioFiscal sin resolver', () => {
    expect(router._FILTRO_LINK_ATRAPADO).toEqual({
      recomputedFormasPagoAt: { $ne: null },
      folioFiscal:            { $in: [null, ''] },
    });
  });
});

// GET /cuentas-pendientes — cobertura mínima de permisos (banks:erp:read,
// banks:erp:anticipos, banks:ficha) confirmada como faltante en la revisión 4R 2026-08-07.
describe('GET /cuentas-pendientes', () => {
  let app;

  beforeEach(() => {
    jest.clearAllMocks();
    app = express();
    app.use(router);
  });

  test('GET /cuentas-pendientes responde 403 sin banks:erp:read', async () => {
    const res = await request(app)
      .get('/cuentas-pendientes')
      .set('x-test-permissions', JSON.stringify([]));

    expect(res.status).toBe(403);
    expect(res.body.required).toEqual([PERMISSIONS.BANKS_ERP_READ]);
  });

  test('GET /cuentas-pendientes?origen=anticipo responde 403 con banks:erp:read pero sin banks:erp:anticipos', async () => {
    rbacStore.hasPermission.mockResolvedValue(false);

    const res = await request(app)
      .get('/cuentas-pendientes?origen=anticipo')
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_ERP_READ]));

    expect(res.status).toBe(403);
    expect(res.body.required).toEqual([PERMISSIONS.BANKS_ERP_ANTICIPOS]);
    expect(rbacStore.hasPermission).toHaveBeenCalledWith(
      'test-role', PERMISSIONS.BANKS_ERP_ANTICIPOS, [],
    );
  });
});

// GET /cuenta-por-serie-folio — segunda parte del buscador de CFDI (2026-08-07): resuelve
// la CxC real de Kore por serie-folio antes de vincularla, en vez de confiar en el `total`
// del CFDI (que no refleja pagos parciales).
describe('GET /cuenta-por-serie-folio', () => {
  let app;

  beforeEach(() => {
    jest.clearAllMocks();
    app = express();
    app.use(router);
  });

  test('responde 403 sin banks:cfdi:read', async () => {
    const res = await request(app)
      .get('/cuenta-por-serie-folio')
      .query({ serie: 'A0', folio: '260800216' })
      .set('x-test-permissions', JSON.stringify([]));

    expect(res.status).toBe(403);
    expect(res.body.required).toEqual([PERMISSIONS.BANKS_CFDI_READ]);
    expect(sincronizarCuentasPendientes).not.toHaveBeenCalled();
  });

  test('responde 400 sin serie o sin folio', async () => {
    const res = await request(app)
      .get('/cuenta-por-serie-folio')
      .query({ serie: 'A0' })
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_CFDI_READ]));

    expect(res.status).toBe(400);
    expect(sincronizarCuentasPendientes).not.toHaveBeenCalled();
  });

  test('responde 404 si Kore no devuelve ninguna cuenta para esa serie-folio', async () => {
    sincronizarCuentasPendientes.mockResolvedValue({ raw: [] });

    const res = await request(app)
      .get('/cuenta-por-serie-folio')
      .query({ serie: 'A0', folio: '260800216' })
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_CFDI_READ]));

    expect(res.status).toBe(404);
  });

  test('calcula el rango de fecha del folio (YYMM) y devuelve la cuenta con saldoActual en vivo', async () => {
    sincronizarCuentasPendientes.mockResolvedValue({
      raw: [{
        id: 'erp-1', serie: 'A0', folio: '1', serieExterna: 'A0', folioExterno: '260800216',
        folioFiscal: 'UUID-1', tipoPago: 'PPD', subtotal: 1000, impuesto: 160, total: 1160,
        saldoActual: 500, fechaVencimiento: null, nombrePersona: 'Cliente Test',
        nombreTipoMovimiento: 'Venta', personaId: 'p1', esAnticipo: false, origen: null,
      }],
    });

    const res = await request(app)
      .get('/cuenta-por-serie-folio')
      .query({ serie: 'A0', folio: '260800216' })
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_CFDI_READ]));

    expect(res.status).toBe(200);
    expect(sincronizarCuentasPendientes).toHaveBeenCalledWith({
      serieExterna: 'A0', folioExterno: '260800216',
      fechaDesde: '2026-08-01T00:00:00Z', fechaHasta: '2026-08-31T23:59:59Z',
    });
    // saldoActual viene de Kore EN VIVO (500), no del `total` que traería un CFDI (1160) —
    // es exactamente lo que este endpoint existe para garantizar.
    expect(res.body.saldoActual).toBe(500);
    expect(res.body.id).toBe('erp-1');
  });

  test('sin match exacto entre lo que devuelve Kore: responde 404, NUNCA vincula la primera cuenta que venga (sin fallback a raw[0])', async () => {
    // Bug real de revisión: la versión original caía a raw[0] si el .find() exacto
    // fallaba — esto simula que Kore devuelve una cuenta de OTRA serie-folio en la misma
    // ventana de fecha, y confirma que ya NO se vincula por error.
    sincronizarCuentasPendientes.mockResolvedValue({
      raw: [{ id: 'erp-otra', serie: 'B0', folio: '9', serieExterna: 'B0', folioExterno: '999999' }],
    });

    const res = await request(app)
      .get('/cuenta-por-serie-folio')
      .query({ serie: 'A0', folio: '260800216' })
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_CFDI_READ]));

    expect(res.status).toBe(404);
  });

  test('ventana normal vacía: reintenta con la ventana de spillover de fin de mes y encuentra la cuenta ahí', async () => {
    sincronizarCuentasPendientes
      .mockResolvedValueOnce({ raw: [] }) // ventana normal (agosto completo) — vacía
      .mockResolvedValueOnce({ // spillover (1 día de septiembre) — acá sí está
        raw: [{
          id: 'erp-spillover', serie: 'A0', folio: '1', serieExterna: 'A0', folioExterno: '260800216',
          total: 800, saldoActual: 800,
        }],
      });

    const res = await request(app)
      .get('/cuenta-por-serie-folio')
      .query({ serie: 'A0', folio: '260800216' })
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_CFDI_READ]));

    expect(res.status).toBe(200);
    expect(res.body.id).toBe('erp-spillover');
    expect(sincronizarCuentasPendientes).toHaveBeenCalledTimes(2);
    expect(sincronizarCuentasPendientes).toHaveBeenNthCalledWith(2, {
      serieExterna: 'A0', folioExterno: '260800216',
      fechaDesde: '2026-09-01T00:00:00.000Z', fechaHasta: '2026-09-01T23:59:59.000Z',
    });
  }, 10000);

  // Fallback nuevo (2026-08-10): factura YA LIQUIDADA (pagada al 100%) — Kore la excluye
  // por completo de /cuentas-pendientes sin importar el rango de fecha, así que raw0 nunca
  // aparece. Caso real: CFDI H0-260100639, pago 488.73 == total 488.73. Pedido explícito
  // del usuario: permitir vincular igual esa CxC ("es solo una relación simple"), sin
  // verificarla en vivo contra Kore.
  test('Kore sin match + CFDI local encontrado con erpId válido y sin cancelar -> 200 con saldoActual:0 y origen:cfdi_liquidado', async () => {
    sincronizarCuentasPendientes.mockResolvedValue({ raw: [] });
    CFDI.findOne.mockReturnValue({
      lean: jest.fn().mockResolvedValue({
        erpId:     'erp-liquidado-1',
        serie:     'H0',
        folio:     '260100639',
        uuid:      'UUID-LIQUIDADO-1',
        formaPago: '03',
        subTotal:  421.32,
        impuestos: { totalImpuestosTrasladados: 67.41 },
        total:     488.73,
        receptor:  { nombre: 'Cliente Test' },
        erpStatus: 'Timbrado',
        satStatus: 'Vigente',
      }),
    });

    const res = await request(app)
      .get('/cuenta-por-serie-folio')
      .query({ serie: 'H0', folio: '260100639' })
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_CFDI_READ]));

    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({
      id:                   'erp-liquidado-1',
      serie:                'H0',
      folio:                '260100639',
      serieExterna:         'H0',
      folioExterno:         '260100639',
      folioFiscal:          'UUID-LIQUIDADO-1',
      tipoPago:             '03',
      subtotal:             421.32,
      impuesto:             67.41,
      total:                488.73,
      saldoActual:          0,
      fechaVencimiento:     null,
      nombrePersona:        'Cliente Test',
      nombreTipoMovimiento: null,
      personaId:            null,
      esAnticipo:           false,
      origen:               'cfdi_liquidado',
    });
    expect(CFDI.findOne).toHaveBeenCalledWith({ source: 'ERP', serie: 'H0', folio: '260100639' });
  });

  test('Kore sin match + CFDI local SIN erpId -> 404 con mensaje específico (sin ID de Kore no se puede vincular)', async () => {
    sincronizarCuentasPendientes.mockResolvedValue({ raw: [] });
    CFDI.findOne.mockReturnValue({
      lean: jest.fn().mockResolvedValue({
        erpId: null, serie: 'H0', folio: '260100639', total: 488.73,
      }),
    });

    const res = await request(app)
      .get('/cuenta-por-serie-folio')
      .query({ serie: 'H0', folio: '260100639' })
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_CFDI_READ]));

    expect(res.status).toBe(404);
    expect(res.body.error).toBe('Esta factura no tiene un identificador de ERP asociado — no se puede vincular.');
  });

  test('Kore sin match + CFDI local con erpStatus:Cancelado -> 404 con mensaje específico', async () => {
    sincronizarCuentasPendientes.mockResolvedValue({ raw: [] });
    CFDI.findOne.mockReturnValue({
      lean: jest.fn().mockResolvedValue({
        erpId: 'erp-cancelado-1', serie: 'H0', folio: '260100639', total: 488.73,
        erpStatus: 'Cancelado',
      }),
    });

    const res = await request(app)
      .get('/cuenta-por-serie-folio')
      .query({ serie: 'H0', folio: '260100639' })
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_CFDI_READ]));

    expect(res.status).toBe(404);
    expect(res.body.error).toBe('Esta factura está cancelada — no se puede vincular.');
  });

  test('Kore sin match + CFDI no existe tampoco localmente -> 404 genérico (sin regresión)', async () => {
    sincronizarCuentasPendientes.mockResolvedValue({ raw: [] });
    CFDI.findOne.mockReturnValue({ lean: jest.fn().mockResolvedValue(null) });

    const res = await request(app)
      .get('/cuenta-por-serie-folio')
      .query({ serie: 'H0', folio: '260100639' })
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_CFDI_READ]));

    expect(res.status).toBe(404);
    expect(res.body.error).toBe('No se encontró esta CxC en Kore — puede que ya no esté disponible para vincular.');
  });
});

// _resolverCuentaDesdeCfdiLiquidado — función pura del fallback nuevo (2026-08-10),
// testeada aislada sin pasar por la ruta ni mockear Mongo.
describe('_resolverCuentaDesdeCfdiLiquidado', () => {
  test('sin erpId -> error específico', () => {
    const resultado = router._resolverCuentaDesdeCfdiLiquidado({ erpId: null, total: 100 });
    expect(resultado).toEqual({ error: 'Esta factura no tiene un identificador de ERP asociado — no se puede vincular.' });
  });

  test('erpStatus Cancelado -> error específico', () => {
    const resultado = router._resolverCuentaDesdeCfdiLiquidado({ erpId: 'x', erpStatus: 'Cancelado', total: 100 });
    expect(resultado).toEqual({ error: 'Esta factura está cancelada — no se puede vincular.' });
  });

  test('satStatus Cancelado -> error específico', () => {
    const resultado = router._resolverCuentaDesdeCfdiLiquidado({ erpId: 'x', satStatus: 'Cancelado', total: 100 });
    expect(resultado).toEqual({ error: 'Esta factura está cancelada — no se puede vincular.' });
  });

  test('CFDI válido -> cuenta con saldoActual:0 y origen cfdi_liquidado', () => {
    const resultado = router._resolverCuentaDesdeCfdiLiquidado({
      erpId: 'erp-1', serie: 'H0', folio: '260100639', uuid: 'UUID-1',
      formaPago: '03', subTotal: 421.32, impuestos: { totalImpuestosTrasladados: 67.41 },
      total: 488.73, receptor: { nombre: 'Cliente Test' },
      erpStatus: 'Timbrado', satStatus: 'Vigente',
    });

    expect(resultado).toEqual({
      cuenta: {
        id: 'erp-1', serie: 'H0', folio: '260100639',
        serieExterna: 'H0', folioExterno: '260100639',
        folioFiscal: 'UUID-1', tipoPago: '03',
        subtotal: 421.32, impuesto: 67.41, total: 488.73,
        saldoActual: 0, fechaVencimiento: null,
        nombrePersona: 'Cliente Test', nombreTipoMovimiento: null,
        personaId: null, esAnticipo: false, origen: 'cfdi_liquidado',
      },
    });
  });
});
