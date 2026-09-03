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
  // obtenerCajaBaseUrl reemplaza al viejo export KORE_CAJA_BASE_URL (ahora viene de
  // Configuraciones Globales, sección `kore`) — ningún test de este archivo ejercita
  // las rutas que la usan (/cobros/conceptos, /cobros/anticipos/*), se deja igual mockeada
  // por completitud del mock del módulo.
  obtenerCajaBaseUrl:     jest.fn().mockResolvedValue('http://kore.test'),
  obtenerSesionCaja:      jest.fn(),
  obtenerCuentasKore:     jest.fn(),
  aplicarCobroOperacion:  jest.fn(),
  aplicarCobroOperacionMultiple: jest.fn(),
  listarBancos:           jest.fn(),
  listarFormasPago:       jest.fn(),
  buscarTransferenciasCajas: jest.fn(),
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

// Fase D (bandeja + confirmar transferencias de caja) — límite de I/O real de las 2 rutas
// nuevas. caja-transferencia-match/confirm.service tienen sus propios tests unitarios
// (caja-transferencia-match.service.test.js / caja-transferencia-confirm.service.test.js);
// acá solo se cubre el cableado HTTP (params, permisos, códigos de respuesta).
jest.mock('./CajaTransferencia.model');
jest.mock('./caja-transferencia-match.service', () => ({ buscarCandidatos: jest.fn() }));
jest.mock('./caja-transferencia-confirm.service', () => ({ confirmarMatch: jest.fn() }));
jest.mock('./caja-transferencia-sync.service', () => ({ sincronizarTransferenciasCajasManual: jest.fn(), init: jest.fn() }));

const express      = require('express');
const request      = require('supertest');
const router       = require('./erp.routes');
const rbacStore    = require('../../../shared/services/rbac-store');
const koreCaja     = require('./kore-caja.service');
const { sincronizarCuentasPendientes } = require('./erp-sync.service');
const CFDI         = require('../../../visor/models/CFDI');
const CajaTransferencia = require('./CajaTransferencia.model');
const { buscarCandidatos } = require('./caja-transferencia-match.service');
const { confirmarMatch }   = require('./caja-transferencia-confirm.service');
const { sincronizarTransferenciasCajasManual } = require('./caja-transferencia-sync.service');
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

describe('_aportesPorErpIdCronologico (2026-08-21, bug real de atribución cruzada entre movimientos)', () => {
  // Caso real simple, folioExterno 260800166: 2 movimientos pagan $100 cada uno a la misma
  // CxC, se revierte 1 sin Aut/Numo — la reversión canceló el abono MÁS RECIENTE (American
  // Express), no el más viejo (BANCOMER). _montoSaldoLinkPorMovimiento (evaluado por
  // separado) daba 0 y 0 para este caso — Kore reportaba $100 pagados de verdad.
  test('caso real folioExterno 260800166: reversión sin tag cancela el abono MÁS RECIENTE, no el más viejo', () => {
    const raw0 = {
      total: 1591.72,
      saldoActual: 1491.72,
      movimientos: [
        { serie: 'A0', folio: '260800186', fecha: '2026-08-21T16:40:25.531697Z', total: 1591.72 },
        { serie: 'ABO', folio: '260800300', fecha: '2026-08-21T16:45:15.898396Z', total: -100,
          formasPago: [{ nombreFormaPago: 'TRANSFERENCIA', monto: 100, adicionales: [
            { nombre: 'Aut', valor: '039033' }, { nombre: 'Numo', valor: '18411758' }, { nombre: 'Banco', valor: 'BANCOMER' },
          ] }] },
        { serie: 'ABO', folio: '260800302', fecha: '2026-08-21T16:46:02.926969Z', total: -100,
          formasPago: [{ nombreFormaPago: 'TRANSFERENCIA', monto: 100, adicionales: [
            { nombre: 'Aut', valor: '040727' }, { nombre: 'Numo', valor: '477911' }, { nombre: 'Banco', valor: 'American Express' },
          ] }] },
        { serie: 'REV ABO', folio: '260800024', fecha: '2026-08-21T16:50:11.280541Z', total: 100,
          formasPago: [{ nombreFormaPago: 'TRANSFERENCIA', monto: 100 }] },
      ],
    };
    const movBancomer = { numeroAutorizacion: '18411758', folio: 'X' }; // se revierte NUNCA — debe quedar con $100
    const movAmex      = { numeroAutorizacion: '477911',   folio: 'Y' }; // se revierte — debe quedar sin aporte

    const resultado = router._aportesPorErpIdCronologico(raw0, [movBancomer, movAmex]);

    expect(resultado.get(0)).toBe(100); // BANCOMER, índice 0
    expect(resultado.has(1)).toBe(false); // American Express, índice 1 — completamente revertido
  });

  // Caso real complejo, folioExterno 260800164: 2 movimientos (BANCOMER/American Express)
  // con 3 ciclos de aplicar→revertir intercalados en ~5 minutos. Verificado a mano contra el
  // propio total de Kore (total-saldoActual=150): BANCOMER debía terminar en $100 (su primer
  // abono, nunca tocado de nuevo) y American Express en $50 (su última reaplicación, la única
  // que no se volvió a revertir). _montoSaldoLinkPorMovimiento (evaluado por separado) daba 0
  // y 0 para este caso.
  test('caso real folioExterno 260800164: 3 ciclos de aplicar/revertir intercalados entre 2 movimientos, cada reversión cancela lo más reciente', () => {
    const raw0 = {
      total: 346.62,
      saldoActual: 196.62,
      movimientos: [
        { serie: 'A0', folio: '260800183', fecha: '2026-08-21T14:59:51.206396Z', total: 346.62 },
        { serie: 'ABO', folio: '260800288', fecha: '2026-08-21T15:09:13.048446Z', total: -100,
          formasPago: [{ nombreFormaPago: 'TRANSFERENCIA', monto: 100, adicionales: [
            { nombre: 'Aut', valor: '039033' }, { nombre: 'Numo', valor: '18411758' }, { nombre: 'Banco', valor: 'BANCOMER' },
          ] }] },
        { serie: 'ABO', folio: '260800290', fecha: '2026-08-21T15:12:19.709737Z', total: -100,
          formasPago: [{ nombreFormaPago: 'TRANSFERENCIA', monto: 100, adicionales: [
            { nombre: 'Aut', valor: '040727' }, { nombre: 'Numo', valor: '477911' }, { nombre: 'Banco', valor: 'American Express' },
          ] }] },
        { serie: 'REV ABO', folio: '260800021', fecha: '2026-08-21T16:10:10.799155Z', total: 100,
          formasPago: [{ nombreFormaPago: 'TRANSFERENCIA', monto: 100 }] },
        { serie: 'ABO', folio: '260800292', fecha: '2026-08-21T16:11:31.330654Z', total: -246.62,
          formasPago: [{ nombreFormaPago: 'TRANSFERENCIA', monto: 246.62, adicionales: [
            { nombre: 'Aut', valor: '040727' }, { nombre: 'Numo', valor: '477911' }, { nombre: 'Banco', valor: 'American Express' },
          ] }] },
        { serie: 'REV ABO', folio: '260800022', fecha: '2026-08-21T16:12:04.677325Z', total: 246.62,
          formasPago: [{ nombreFormaPago: 'TRANSFERENCIA', monto: 246.62 }] },
        { serie: 'ABO', folio: '260800294', fecha: '2026-08-21T16:13:02.653023Z', total: -50,
          formasPago: [{ nombreFormaPago: 'TRANSFERENCIA', monto: 50, adicionales: [
            { nombre: 'Aut', valor: '040727' }, { nombre: 'Numo', valor: '477911' }, { nombre: 'Banco', valor: 'American Express' },
          ] }] },
        { serie: 'ABO', folio: '260800296', fecha: '2026-08-21T16:13:54.622154Z', total: -50,
          formasPago: [{ nombreFormaPago: 'TRANSFERENCIA', monto: 50, adicionales: [
            { nombre: 'Aut', valor: '039033' }, { nombre: 'Numo', valor: '18411758' }, { nombre: 'Banco', valor: 'BANCOMER' },
          ] }] },
        { serie: 'REV ABO', folio: '260800023', fecha: '2026-08-21T16:14:20.116981Z', total: 50,
          formasPago: [{ nombreFormaPago: 'TRANSFERENCIA', monto: 50 }] },
      ],
    };
    const movBancomer = { numeroAutorizacion: '18411758', folio: 'X' };
    const movAmex      = { numeroAutorizacion: '477911',   folio: 'Y' };

    const resultado = router._aportesPorErpIdCronologico(raw0, [movBancomer, movAmex]);

    expect(resultado.get(0)).toBe(100); // BANCOMER: solo su primer abono (288) sigue vigente
    expect(resultado.get(1)).toBe(50);  // American Express: solo su última reaplicación (294) sigue vigente
    // Reconcilia exacto con Kore: total-saldoActual = 346.62-196.62 = 150 = 100+50.
    expect(resultado.get(0) + resultado.get(1)).toBeCloseTo(raw0.total - raw0.saldoActual, 2);
  });

  test('reversa cuyo monto no coincide con NINGUNA entrada de la pila se ignora (no se inventa a qué abono pertenece)', () => {
    const raw0 = {
      total: 200, saldoActual: 100,
      movimientos: [
        { serie: 'ABO', folio: '1', fecha: '2026-01-01T00:00:00Z', total: -100,
          formasPago: [{ nombreFormaPago: 'TRANSFERENCIA', monto: 100, adicionales: [
            { nombre: 'Numo', valor: '111' },
          ] }] },
        { serie: 'REV ABO', folio: '2', fecha: '2026-01-01T00:05:00Z', total: 37, // no coincide con nada
          formasPago: [{ nombreFormaPago: 'TRANSFERENCIA', monto: 37 }] },
      ],
    };
    const mov = { numeroAutorizacion: '111', folio: 'X' };

    const resultado = router._aportesPorErpIdCronologico(raw0, [mov]);

    expect(resultado.get(0)).toBe(100); // la reversa sin match no toca nada
  });

  test('un abono tageado que pertenece a OTRO movimiento fuera del grupo no compite por la pila de este grupo', () => {
    const raw0 = {
      total: 200, saldoActual: 100,
      movimientos: [
        { serie: 'ABO', folio: '1', fecha: '2026-01-01T00:00:00Z', total: -100,
          formasPago: [{ nombreFormaPago: 'TRANSFERENCIA', monto: 100, adicionales: [{ nombre: 'Numo', valor: '111' }] }] },
        { serie: 'ABO', folio: '2', fecha: '2026-01-01T00:01:00Z', total: -100,
          formasPago: [{ nombreFormaPago: 'TRANSFERENCIA', monto: 100, adicionales: [{ nombre: 'Numo', valor: '999-AJENO' }] }] },
      ],
    };
    const mov = { numeroAutorizacion: '111', folio: 'X' };

    const resultado = router._aportesPorErpIdCronologico(raw0, [mov]);

    expect(resultado.get(0)).toBe(100); // solo el propio, el ajeno se ignora por completo
  });

  // Caso real 2026-08-24, folioExterno 260800204: "Depósito en efectivo" manda un tag
  // DISTINTO a Aut/Numo — { Nombre: 'Num Recibo', Valor: mov.folio } (collection-request.
  // service.js). Antes del fix, _perteneceAEsteMovimiento/_tieneTagIdentidadPropia no
  // reconocían 'Num Recibo': los 2 abonos ($500 y $100) caían como "sin tag propio", nunca se
  // empujaban a la pila, y la reversión de $100 (sin tag, como toda REV ABO) no encontraba
  // nada que cancelar — calculado quedaba en 0 (vacío) aunque Kore reportara $500 vigentes
  // (total-saldoActual=500), disparando la red de seguridad de atribución y bloqueando la
  // reversión entera ("NO se toca ningún link").
  test('caso real folioExterno 260800204: Depósito en efectivo (tag "Num Recibo") se reconoce como aporte propio, no como reversa anónima', () => {
    const raw0 = {
      total: 2203.36,
      saldoActual: 1703.36, // total-saldoActual = 500 (el abono de $500 sigue vigente)
      movimientos: [
        { serie: 'A0', folio: '260800204', fecha: '2026-08-01T00:00:00Z', total: 2203.36 },
        { serie: 'ABO', folio: '1', fecha: '2026-08-24T18:40:00Z', total: -500,
          formasPago: [{ nombreFormaPago: 'DEPOSITO EN EFECTIVO', monto: 500, adicionales: [
            { nombre: 'Num Recibo', valor: 'F-4361' },
          ] }] },
        { serie: 'ABO', folio: '2', fecha: '2026-08-24T18:52:00Z', total: -100,
          formasPago: [{ nombreFormaPago: 'DEPOSITO EN EFECTIVO', monto: 100, adicionales: [
            { nombre: 'Num Recibo', valor: 'F-4361' },
          ] }] },
        { serie: 'REV ABO', folio: '3', fecha: '2026-08-24T18:52:11Z', total: 100,
          formasPago: [{ nombreFormaPago: 'DEPOSITO EN EFECTIVO', monto: 100 }] },
      ],
    };
    const mov = { numeroAutorizacion: null, folio: 'F-4361' };

    const resultado = router._aportesPorErpIdCronologico(raw0, [mov]);

    expect(resultado.get(0)).toBe(500);
    expect(resultado.get(0)).toBeCloseTo(raw0.total - raw0.saldoActual, 2);
  });

  // Misma CxC pagada con 2 movimientos de tipos DISTINTOS: uno por Depósito en efectivo
  // (tag 'Num Recibo') y otro por Transferencia (tags 'Aut'/'Numo'). Antes del fix ambos
  // tags convivían sin problema entre sí (el bug era que 'Num Recibo' no se reconocía EN
  // ABSOLUTO, no que se confundiera con 'Aut'/'Numo') — este test fija que, con los 3 tags
  // ahora reconocidos, cada aporte sigue atribuyéndose EXCLUSIVAMENTE a su propio
  // movimiento, sin cruzarse entre el depósito en efectivo y la transferencia.
  test('depósito en efectivo (Num Recibo) y transferencia (Aut/Numo) como aportes de 2 movimientos distintos a la misma CxC, sin cruzarse', () => {
    const raw0 = {
      total: 1000, saldoActual: 400, // 600 pagados entre los dos
      movimientos: [
        { serie: 'A0', folio: '1', fecha: '2026-01-01T00:00:00Z', total: 1000 },
        { serie: 'ABO', folio: '2', fecha: '2026-01-01T00:01:00Z', total: -400,
          formasPago: [{ nombreFormaPago: 'DEPOSITO EN EFECTIVO', monto: 400, adicionales: [
            { nombre: 'Num Recibo', valor: 'F-EFECTIVO' },
          ] }] },
        { serie: 'ABO', folio: '3', fecha: '2026-01-01T00:02:00Z', total: -200,
          formasPago: [{ nombreFormaPago: 'TRANSFERENCIA', monto: 200, adicionales: [
            { nombre: 'Aut', valor: '039033' }, { nombre: 'Numo', valor: '18411758' },
          ] }] },
      ],
    };
    const movEfectivo      = { numeroAutorizacion: null,          folio: 'F-EFECTIVO' };
    const movTransferencia = { numeroAutorizacion: '18411758',    folio: 'F-TRANSFER' };

    const resultado = router._aportesPorErpIdCronologico(raw0, [movEfectivo, movTransferencia]);

    expect(resultado.get(0)).toBe(400); // efectivo: solo lo suyo
    expect(resultado.get(1)).toBe(200); // transferencia: solo lo suyo
    expect(resultado.get(0) + resultado.get(1)).toBeCloseTo(raw0.total - raw0.saldoActual, 2);
  });
});

// 2026-09-01 — bug real encontrado investigando una reversión con "atribución ambigua" que no
// debería haber sido ambigua (erpId 6a971dd5b6007400011db4de, folioExterno 260900009).
// _perteneceAEsteMovimiento solo reconocía la convención de Transferencia (Numo=autorización,
// Aut=folio) — desde el 2026-08-28, Depósito en efectivo y Cheque mandan DatosAdicionales con
// la convención INVERTIDA (Numo=folio, Aut=autorización; "Num Recibo" queda vacío, ver
// collection-request.service.js). Los tests de arriba ("Num Recibo") cubren el contrato VIEJO
// (anterior al 28/08) — estos cubren el contrato REAL vigente hoy para las 3 formas de pago.
describe('_aportesPorErpIdCronologico — convención invertida (Numo=folio, Aut=autorización) para Depósito en efectivo y Cheque', () => {
  test('Depósito en efectivo (contrato vigente: Numo=folio, Aut=autorización, Num Recibo vacío) se atribuye a su propio movimiento', () => {
    const raw0 = {
      total: 500, saldoActual: 0,
      movimientos: [
        { serie: 'A0', folio: '1', fecha: '2026-01-01T00:00:00Z', total: 500 },
        { serie: 'ABO', folio: '2', fecha: '2026-01-01T00:01:00Z', total: -500,
          formasPago: [{ nombreFormaPago: 'DEPOSITO EN EFECTIVO', monto: 500, adicionales: [
            { nombre: 'Num Recibo', valor: '' },
            { nombre: 'Numo', valor: 'F-EFECTIVO' },   // folio de Numo
            { nombre: 'Aut', valor: '291441' },         // autorización bancaria
          ] }] },
      ],
    };
    const mov = { numeroAutorizacion: '291441', folio: 'F-EFECTIVO' };

    const resultado = router._aportesPorErpIdCronologico(raw0, [mov]);

    expect(resultado.get(0)).toBe(500);
  });

  test('Cheque (mismo contrato invertido que Depósito en efectivo: Numo=folio, Aut=autorización) se atribuye a su propio movimiento', () => {
    const raw0 = {
      total: 728.12, saldoActual: 0,
      movimientos: [
        { serie: 'A0', folio: '1', fecha: '2026-01-01T00:00:00Z', total: 728.12 },
        { serie: 'ABO', folio: '2', fecha: '2026-01-01T00:01:00Z', total: -728.12,
          formasPago: [{ nombreFormaPago: 'CHEQUE', monto: 728.12, adicionales: [
            { nombre: 'Numo', valor: 'F-CHEQUE' },  // folio de Numo
            { nombre: 'Aut', valor: '13280' },       // autorización bancaria
          ] }] },
      ],
    };
    const mov = { numeroAutorizacion: '13280', folio: 'F-CHEQUE' };

    const resultado = router._aportesPorErpIdCronologico(raw0, [mov]);

    expect(resultado.get(0)).toBe(728.12);
  });

  // Reproduce el caso real (sin el 4to movimiento artificial que el usuario aplicó a mano
  // desde Kore solo para cerrar la CxC de prueba): UN SOLO BankMovement recibió 2 abonos —
  // Efectivo $500 y Transferencia $691.26 — y Kore revirtió la parte de Transferencia. Antes
  // del fix, el abono de efectivo nunca entraba a la pila (quedaba "de otro" / ignorado) y el
  // resultado daba 0 en vez de 500, disparando la red de seguridad de atribución ambigua.
  test('caso real: mismo movimiento con Efectivo + Transferencia, se revierte la Transferencia — queda el aporte de Efectivo', () => {
    const raw0 = {
      total: 1191.26, saldoActual: 691.26, // reversión ya aplicada, SIN el 3er abono de prueba
      movimientos: [
        { serie: 'A0', folio: '260900009', fecha: '2026-09-01T18:47:49Z', total: 1191.26 },
        { serie: 'ABO', folio: '260900022', fecha: '2026-09-01T18:49:23Z', total: -500,
          formasPago: [{ nombreFormaPago: 'DEPOSITO EN EFECTIVO', monto: 500, adicionales: [
            { nombre: 'Num Recibo', valor: '' },
            { nombre: 'Numo', valor: '040443' },
            { nombre: 'Aut', valor: '291441' },
          ] }] },
        { serie: 'ABO', folio: '260900024', fecha: '2026-09-01T18:50:33Z', total: -691.26,
          formasPago: [{ nombreFormaPago: 'TRANSFERENCIA', monto: 691.26, adicionales: [
            { nombre: 'Aut', valor: '040443' },
            { nombre: 'Numo', valor: '291441' },
          ] }] },
        { serie: 'REV ABO', folio: '260900003', fecha: '2026-09-01T18:51:35Z', total: 691.26,
          formasPago: [{ nombreFormaPago: 'TRANSFERENCIA', monto: 691.26 }] }, // sin adicionales, como toda REV ABO
      ],
    };
    const mov = { numeroAutorizacion: '291441', folio: '040443' };

    const resultado = router._aportesPorErpIdCronologico(raw0, [mov]);

    expect(resultado.get(0)).toBe(500);
    expect(resultado.get(0)).toBeCloseTo(raw0.total - raw0.saldoActual, 2);
  });

  // 2026-09-01 — caso real CONFIRMADO por el usuario contra Kore real (erpId
  // 6a9724fdb6007400011db6df, folioExterno 260900011): mismo movimiento con Transferencia
  // $500 + Depósito en efectivo $660, se revierte la parte de Efectivo. Resultado real en
  // producción: "Ajustado (siguió vinculado)", aporte 1160 -> 500. Fijado acá como regresión.
  test('caso real CONFIRMADO: mismo movimiento con Transferencia + Efectivo, se revierte el Efectivo — queda el aporte de Transferencia', () => {
    const raw0 = {
      total: 1160, saldoActual: 660,
      movimientos: [
        { serie: 'A0', folio: '260900011', fecha: '2026-09-01T19:18:21Z', total: 1160 },
        { serie: 'ABO', folio: '260900028', fecha: '2026-09-01T19:19:44Z', total: -500,
          formasPago: [{ nombreFormaPago: 'TRANSFERENCIA', monto: 500, adicionales: [
            { nombre: 'Aut', valor: '040443' },
            { nombre: 'Numo', valor: '291441' },
          ] }] },
        { serie: 'ABO', folio: '260900030', fecha: '2026-09-01T19:20:42Z', total: -660,
          formasPago: [{ nombreFormaPago: 'DEPOSITO EN EFECTIVO', monto: 660, adicionales: [
            { nombre: 'Num Recibo', valor: '' },
            { nombre: 'Numo', valor: '040443' },
            { nombre: 'Aut', valor: '291441' },
          ] }] },
        { serie: 'REV ABO', folio: '260900004', fecha: '2026-09-01T19:21:04Z', total: 660,
          formasPago: [{ nombreFormaPago: 'DEPOSITO EN EFECTIVO', monto: 660 }] },
      ],
    };
    const mov = { numeroAutorizacion: '291441', folio: '040443' };

    const resultado = router._aportesPorErpIdCronologico(raw0, [mov]);

    expect(resultado.get(0)).toBe(500);
    expect(resultado.get(0)).toBeCloseTo(raw0.total - raw0.saldoActual, 2);
  });

  test('mismo movimiento con Cheque + Transferencia, se revierte el Cheque — queda el aporte de Transferencia', () => {
    const raw0 = {
      total: 1000, saldoActual: 700, // se revirtieron los $700 del cheque, quedan los $300 de transferencia como deuda cubierta y $700 pendiente
      movimientos: [
        { serie: 'A0', folio: '1', fecha: '2026-01-01T00:00:00Z', total: 1000 },
        { serie: 'ABO', folio: '2', fecha: '2026-01-01T00:01:00Z', total: -300,
          formasPago: [{ nombreFormaPago: 'TRANSFERENCIA', monto: 300, adicionales: [
            { nombre: 'Aut', valor: 'F-1' }, { nombre: 'Numo', valor: 'AUT-1' },
          ] }] },
        { serie: 'ABO', folio: '3', fecha: '2026-01-01T00:02:00Z', total: -700,
          formasPago: [{ nombreFormaPago: 'CHEQUE', monto: 700, adicionales: [
            { nombre: 'Numo', valor: 'F-1' }, { nombre: 'Aut', valor: 'AUT-1' },
          ] }] },
        { serie: 'REV ABO', folio: '4', fecha: '2026-01-01T00:03:00Z', total: 700,
          formasPago: [{ nombreFormaPago: 'CHEQUE', monto: 700 }] },
      ],
    };
    const mov = { numeroAutorizacion: 'AUT-1', folio: 'F-1' };

    const resultado = router._aportesPorErpIdCronologico(raw0, [mov]);

    expect(resultado.get(0)).toBe(300);
    expect(resultado.get(0)).toBeCloseTo(raw0.total - raw0.saldoActual, 2);
  });

  test('mismo movimiento con Cheque + Efectivo (los 2 con la convención invertida), se revierte el Efectivo — queda el aporte del Cheque', () => {
    const raw0 = {
      total: 900, saldoActual: 400,
      movimientos: [
        { serie: 'A0', folio: '1', fecha: '2026-01-01T00:00:00Z', total: 900 },
        { serie: 'ABO', folio: '2', fecha: '2026-01-01T00:01:00Z', total: -500,
          formasPago: [{ nombreFormaPago: 'CHEQUE', monto: 500, adicionales: [
            { nombre: 'Numo', valor: 'F-1' }, { nombre: 'Aut', valor: 'AUT-1' },
          ] }] },
        { serie: 'ABO', folio: '3', fecha: '2026-01-01T00:02:00Z', total: -400,
          formasPago: [{ nombreFormaPago: 'DEPOSITO EN EFECTIVO', monto: 400, adicionales: [
            { nombre: 'Num Recibo', valor: '' }, { nombre: 'Numo', valor: 'F-1' }, { nombre: 'Aut', valor: 'AUT-1' },
          ] }] },
        { serie: 'REV ABO', folio: '4', fecha: '2026-01-01T00:03:00Z', total: 400,
          formasPago: [{ nombreFormaPago: 'DEPOSITO EN EFECTIVO', monto: 400 }] },
      ],
    };
    const mov = { numeroAutorizacion: 'AUT-1', folio: 'F-1' };

    const resultado = router._aportesPorErpIdCronologico(raw0, [mov]);

    expect(resultado.get(0)).toBe(500);
    expect(resultado.get(0)).toBeCloseTo(raw0.total - raw0.saldoActual, 2);
  });

  // Riesgo real introducido por el fix: al probar las 2 lecturas (Numo/Aut) para CUALQUIER
  // tag, ¿un abono de Efectivo/Cheque de UN movimiento podría "contaminar" a OTRO movimiento
  // de la misma CxC si sus folios/autorizaciones cruzan por coincidencia? Este test fija 2
  // movimientos SEPARADOS, cada uno pagado con una forma de pago de convención invertida
  // distinta, con folios/autorizaciones que NO se repiten entre sí — confirma que cada aporte
  // sigue cayendo exclusivamente en su propio movimiento.
  test('2 movimientos separados, cada uno con una forma de pago de convención invertida distinta — sin contaminación cruzada', () => {
    const raw0 = {
      total: 1200, saldoActual: 0,
      movimientos: [
        { serie: 'A0', folio: '1', fecha: '2026-01-01T00:00:00Z', total: 1200 },
        { serie: 'ABO', folio: '2', fecha: '2026-01-01T00:01:00Z', total: -500,
          formasPago: [{ nombreFormaPago: 'DEPOSITO EN EFECTIVO', monto: 500, adicionales: [
            { nombre: 'Num Recibo', valor: '' }, { nombre: 'Numo', valor: 'F-EFECTIVO' }, { nombre: 'Aut', valor: 'AUT-EFECTIVO' },
          ] }] },
        { serie: 'ABO', folio: '3', fecha: '2026-01-01T00:02:00Z', total: -700,
          formasPago: [{ nombreFormaPago: 'CHEQUE', monto: 700, adicionales: [
            { nombre: 'Numo', valor: 'F-CHEQUE' }, { nombre: 'Aut', valor: 'AUT-CHEQUE' },
          ] }] },
      ],
    };
    const movEfectivo = { numeroAutorizacion: 'AUT-EFECTIVO', folio: 'F-EFECTIVO' };
    const movCheque    = { numeroAutorizacion: 'AUT-CHEQUE',  folio: 'F-CHEQUE' };

    const resultado = router._aportesPorErpIdCronologico(raw0, [movEfectivo, movCheque]);

    expect(resultado.get(0)).toBe(500); // efectivo: solo lo suyo
    expect(resultado.get(1)).toBe(700); // cheque: solo lo suyo
  });

  // Ciclo aplicar -> revertir -> reaplicar con Cheque (convención invertida): el neteo con
  // signo debe seguir quedando en el último valor vigente, sin triplicar ni perder el aporte.
  test('ciclo aplicar -> revertir -> reaplicar con Cheque: neteo con signo, sin triplicar', () => {
    const raw0 = {
      total: 500, saldoActual: 0,
      movimientos: [
        { serie: 'A0', folio: '1', fecha: '2026-01-01T00:00:00Z', total: 500 },
        { serie: 'ABO', folio: '2', fecha: '2026-01-01T00:01:00Z', total: -500,
          formasPago: [{ nombreFormaPago: 'CHEQUE', monto: 500, adicionales: [
            { nombre: 'Numo', valor: 'F-1' }, { nombre: 'Aut', valor: 'AUT-1' },
          ] }] },
        { serie: 'REV ABO', folio: '3', fecha: '2026-01-01T00:02:00Z', total: 500,
          formasPago: [{ nombreFormaPago: 'CHEQUE', monto: 500 }] },
        { serie: 'ABO', folio: '4', fecha: '2026-01-01T00:03:00Z', total: -500,
          formasPago: [{ nombreFormaPago: 'CHEQUE', monto: 500, adicionales: [
            { nombre: 'Numo', valor: 'F-1' }, { nombre: 'Aut', valor: 'AUT-1' },
          ] }] },
      ],
    };
    const mov = { numeroAutorizacion: 'AUT-1', folio: 'F-1' };

    const resultado = router._aportesPorErpIdCronologico(raw0, [mov]);

    expect(resultado.get(0)).toBe(500); // no 1000 (triplicado) ni 0 (perdido)
  });

  // Reversa cuyo monto no coincide con NADA en la pila (ej. Kore manda un monto raro, o el
  // reversal pertenece a un abono fuera de este grupo) — con tags de convención invertida de
  // por medio, sigue sin inventarse a qué abono pertenece.
  test('reversa de Efectivo cuyo monto no coincide con ninguna entrada de la pila se ignora, no se resta de cualquier cosa', () => {
    const raw0 = {
      total: 500, saldoActual: 500,
      movimientos: [
        { serie: 'A0', folio: '1', fecha: '2026-01-01T00:00:00Z', total: 500 },
        { serie: 'ABO', folio: '2', fecha: '2026-01-01T00:01:00Z', total: -500,
          formasPago: [{ nombreFormaPago: 'DEPOSITO EN EFECTIVO', monto: 500, adicionales: [
            { nombre: 'Num Recibo', valor: '' }, { nombre: 'Numo', valor: 'F-1' }, { nombre: 'Aut', valor: 'AUT-1' },
          ] }] },
        { serie: 'REV ABO', folio: '3', fecha: '2026-01-01T00:02:00Z', total: 999, // monto que no cancela nada
          formasPago: [{ nombreFormaPago: 'DEPOSITO EN EFECTIVO', monto: 999 }] },
      ],
    };
    const mov = { numeroAutorizacion: 'AUT-1', folio: 'F-1' };

    const resultado = router._aportesPorErpIdCronologico(raw0, [mov]);

    expect(resultado.get(0)).toBe(500); // el abono de $500 sigue intacto, la reversa de $999 se ignoró
  });
});

describe('_montoSaldoLinkPorMovimiento — convención invertida (Numo=folio, Aut=autorización) para Depósito en efectivo y Cheque', () => {
  test('Depósito en efectivo (contrato vigente) se atribuye a "mío", no a "de otro"', () => {
    const raw0 = {
      movimientos: [
        { serie: 'ABO', folio: '1', total: -500,
          formasPago: [{ nombreFormaPago: 'DEPOSITO EN EFECTIVO', monto: 500, adicionales: [
            { nombre: 'Num Recibo', valor: '' },
            { nombre: 'Numo', valor: 'F-EFECTIVO' },
            { nombre: 'Aut', valor: '291441' },
          ] }] },
      ],
    };
    const mov = { numeroAutorizacion: '291441', folio: 'F-EFECTIVO' };

    const resultado = router._montoSaldoLinkPorMovimiento(raw0, mov);

    expect(resultado).toBe(500);
  });

  test('Cheque (contrato vigente) se atribuye a "mío", no a "de otro"', () => {
    const raw0 = {
      movimientos: [
        { serie: 'ABO', folio: '1', total: -728.12,
          formasPago: [{ nombreFormaPago: 'CHEQUE', monto: 728.12, adicionales: [
            { nombre: 'Numo', valor: 'F-CHEQUE' },
            { nombre: 'Aut', valor: '13280' },
          ] }] },
      ],
    };
    const mov = { numeroAutorizacion: '13280', folio: 'F-CHEQUE' };

    const resultado = router._montoSaldoLinkPorMovimiento(raw0, mov);

    expect(resultado).toBe(728.12);
  });
});

describe('_montoSaldoLinkPorMovimiento — reconoce "Num Recibo" (Depósito en efectivo, caso real 2026-08-24)', () => {
  test('abono propio tageado con Num Recibo se atribuye a este movimiento (no cae en el neteo de "reversa sin tag")', () => {
    const raw0 = {
      movimientos: [
        { serie: 'ABO', folio: '1', total: -500,
          formasPago: [{ nombreFormaPago: 'DEPOSITO EN EFECTIVO', monto: 500, adicionales: [
            { nombre: 'Num Recibo', valor: 'F-4361' },
          ] }] },
      ],
    };
    const mov = { numeroAutorizacion: null, folio: 'F-4361' };

    const resultado = router._montoSaldoLinkPorMovimiento(raw0, mov);

    expect(resultado).toBe(500);
  });

  test('abono con Num Recibo de OTRO movimiento se atribuye a "de otro", no se confunde con una reversa sin tag propia', () => {
    const raw0 = {
      movimientos: [
        { serie: 'ABO', folio: '1', total: -500,
          formasPago: [{ nombreFormaPago: 'DEPOSITO EN EFECTIVO', monto: 500, adicionales: [
            { nombre: 'Num Recibo', valor: 'F-OTRO' },
          ] }] },
      ],
    };
    const mov = { numeroAutorizacion: null, folio: 'F-4361' };

    const resultado = router._montoSaldoLinkPorMovimiento(raw0, mov);

    // Sin ninguna coincidencia PROPIA, huboCoincidenciaPropia queda false -> null (no 0).
    expect(resultado).toBeNull();
  });
});

describe('_backfillFormasPagoYFolioFiscal — aporteBancarioPrevio (2026-08-21, bug real: saldoPagado quedaba en $0 en el dropdown "CxC vinculadas")', () => {
  // Reproduce el caso real folioExterno 260800166: BANCOMER debía terminar en $150
  // (saldoErpAportado ya viene corregido por fuera vía _aportesPorErpIdCronologico), pero
  // saldoPagado (bancario-únicamente) se calculaba con _montoSaldoLinkPorMovimiento evaluado
  // AISLADO — el mismo bug de atribución cruzada, dando $0 en vez de $150. El dropdown "CxC
  // vinculadas" (banks.component.html) muestra `saldoPagado` con prioridad sobre saldoActual.
  const raw0 = {
    total: 1591.72, saldoActual: 1441.72,
    movimientos: [
      { serie: 'ABO', fecha: '2026-08-21T16:45:15Z', total: -100,
        formasPago: [{ nombreFormaPago: 'TRANSFERENCIA', monto: 100, adicionales: [{ nombre: 'Numo', valor: '18411758' }] }] },
      { serie: 'ABO', fecha: '2026-08-21T16:46:02Z', total: -100,
        formasPago: [{ nombreFormaPago: 'TRANSFERENCIA', monto: 100, adicionales: [{ nombre: 'Numo', valor: '477911' }] }] },
      { serie: 'REV ABO', fecha: '2026-08-21T16:50:11Z', total: 100,
        formasPago: [{ nombreFormaPago: 'TRANSFERENCIA', monto: 100 }] },
      { serie: 'ABO', fecha: '2026-08-21T17:11:55Z', total: -50,
        formasPago: [{ nombreFormaPago: 'TRANSFERENCIA', monto: 50, adicionales: [{ nombre: 'Numo', valor: '18411758' }] }] },
      { serie: 'ABO', fecha: '2026-08-21T17:12:59Z', total: -50,
        formasPago: [{ nombreFormaPago: 'TRANSFERENCIA', monto: 50, adicionales: [{ nombre: 'Numo', valor: '477911' }] }] },
      { serie: 'REV ABO', fecha: '2026-08-21T17:13:43Z', total: 50,
        formasPago: [{ nombreFormaPago: 'TRANSFERENCIA', monto: 50 }] },
    ],
  };
  const movBancomer = { numeroAutorizacion: '18411758', folio: 'X' };
  const link = { saldoErpAportado: null, saldoPagadoTotal: null, saldoPagado: null, folioFiscal: null };

  test('sin aporteBancarioPrevio (llamadores viejos, sync/recompute): sigue con el cálculo aislado de siempre (bug preexistente, sin cambios)', () => {
    const resultado = router._backfillFormasPagoYFolioFiscal(link, raw0, movBancomer, true, 150);

    expect(resultado.saldoPagadoTotal).toBe(150); // este SÍ viene del aporteNuevo pasado, no del cálculo aislado
    expect(resultado.saldoPagado).toBe(0); // bug preexistente: el cálculo aislado interno sigue dando 0
  });

  test('CON aporteBancarioPrevio (erp-reversion.service.js): usa el valor ya calculado por la pasada cronológica, corrigiendo el bug', () => {
    const resultado = router._backfillFormasPagoYFolioFiscal(link, raw0, movBancomer, true, 150, 150);

    expect(resultado.saldoPagadoTotal).toBe(150);
    expect(resultado.saldoPagado).toBe(150); // corregido — coincide con saldoErpAportado
  });

  test('aporteBancarioPrevio=0 (movimiento completamente revertido) se respeta tal cual, no cae al fallback', () => {
    const resultado = router._backfillFormasPagoYFolioFiscal(link, raw0, movBancomer, true, null, 0);

    expect(resultado.saldoPagado).toBe(0);
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

// GET /transferencias-cajas — solo trae los datos crudos de Kore (transferencias internas
// entre cajas), sin ninguna lógica de matching contra BankMovement (fase posterior).
describe('GET /transferencias-cajas', () => {
  let app;

  beforeEach(() => {
    jest.clearAllMocks();
    app = express();
    app.use(router);
  });

  test('responde 403 sin banks:erp:read', async () => {
    const res = await request(app)
      .get('/transferencias-cajas')
      .set('x-test-permissions', JSON.stringify([]));

    expect(res.status).toBe(403);
    expect(res.body.required).toEqual([PERMISSIONS.BANKS_ERP_READ]);
    expect(koreCaja.buscarTransferenciasCajas).not.toHaveBeenCalled();
  });

  test('pasa fechaDesde/fechaHasta a buscarTransferenciasCajas y devuelve los datos crudos mapeados', async () => {
    koreCaja.buscarTransferenciasCajas.mockResolvedValue({
      raw: [{
        id: '6a97291ab6007400011db828', monto: 1500, estatus: 'RECIBIDO',
        cajaOrigenId: '62cdb5782d75cf00018309da', nombreCajaOrigen: 'CAJA SILVA', almacenCajaOrigen: 'A0',
        cajaDestinoId: '6a7b7d115f9c490001f589a8', nombreCajaDestino: 'CAJA - HECTOR', almacenCajaDestino: 'A0',
        sessionOrigenId: '6a831ed77a16ce000158fba5', sessionDestinoId: '6a972934b6007400011db834',
        formaPago: '5f85d826acfcf300018a088a', nombreFormaPago: 'EFECTIVO',
        solicito: '602ec3ccb0aeec0001a58200', nombreSolicito: 'CARLOS MARTINEZ SILVA',
        recibio: '6a7a0e9cfe132d0001cfab40', nombreRecibio: 'ROBERTO HECTOR CORONA QUINTAS',
        autorizo: '', nombreAutorizo: '',
        fechaSolicitud: '2026-09-01T19:35:54.606037Z', fechaRecepcion: '2026-09-01T19:36:32.057614Z',
        observacion: 'FONDO INICIAL 1500, dotación por traslado',
        idTipoTransferencia: '6573be21a19c710001571324', nombreTipoTransferencia: 'INICIO DE SESIÓN',
      }],
    });

    const res = await request(app)
      .get('/transferencias-cajas')
      .query({ fechaDesde: '2026-09-01T00:00:00Z', fechaHasta: '2026-09-01T23:59:59Z' })
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_ERP_READ]));

    expect(res.status).toBe(200);
    expect(koreCaja.buscarTransferenciasCajas).toHaveBeenCalledWith({
      fechaDesde: '2026-09-01T00:00:00Z', fechaHasta: '2026-09-01T23:59:59Z',
    });
    expect(res.body.total).toBe(1);
    expect(res.body.data[0]).toMatchObject({
      id: '6a97291ab6007400011db828', monto: 1500, estatus: 'RECIBIDO',
      nombreCajaOrigen: 'CAJA SILVA', nombreCajaDestino: 'CAJA - HECTOR',
      nombreFormaPago: 'EFECTIVO', nombreTipoTransferencia: 'INICIO DE SESIÓN',
    });
  });

  test('responde 503 si el ERP no está configurado en este ambiente', async () => {
    koreCaja.buscarTransferenciasCajas.mockRejectedValue(new Error('No existe la configuración kore.CAJA_BASE_URL'));

    const res = await request(app)
      .get('/transferencias-cajas')
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_ERP_READ]));

    expect(res.status).toBe(503);
  });
});

// POST /transferencias-cajas/sincronizar-manual — pedido del usuario: sincronización con
// fechaDesde/fechaHasta a mano, mismo permiso que el sync manual ERP-Kore ('banks:admin').
describe('POST /transferencias-cajas/sincronizar-manual', () => {
  let app;

  beforeEach(() => {
    jest.clearAllMocks();
    app = express();
    app.use(express.json());
    app.use(router);
  });

  test('responde 403 sin banks:admin', async () => {
    const res = await request(app)
      .post('/transferencias-cajas/sincronizar-manual')
      .send({ fechaDesde: '2020-01-01T00:00:00Z', fechaHasta: '2020-01-31T23:59:59Z' })
      .set('x-test-permissions', JSON.stringify([]));

    expect(res.status).toBe(403);
    expect(sincronizarTransferenciasCajasManual).not.toHaveBeenCalled();
  });

  test('delega en sincronizarTransferenciasCajasManual con las fechas del body', async () => {
    sincronizarTransferenciasCajasManual.mockResolvedValue({ sincronizadas: 3, descartadas: 1, rango: {} });

    const res = await request(app)
      .post('/transferencias-cajas/sincronizar-manual')
      .send({ fechaDesde: '2020-01-01T00:00:00Z', fechaHasta: '2020-01-31T23:59:59Z' })
      .set('x-test-permissions', JSON.stringify(['banks:admin']));

    expect(res.status).toBe(200);
    expect(sincronizarTransferenciasCajasManual).toHaveBeenCalledWith({
      fechaDesde: '2020-01-01T00:00:00Z', fechaHasta: '2020-01-31T23:59:59Z',
    });
    expect(res.body.sincronizadas).toBe(3);
  });

  test('400 si faltan las fechas', async () => {
    sincronizarTransferenciasCajasManual.mockRejectedValue(new Error('Se requieren fechaDesde y fechaHasta.'));

    const res = await request(app)
      .post('/transferencias-cajas/sincronizar-manual')
      .send({})
      .set('x-test-permissions', JSON.stringify(['banks:admin']));

    expect(res.status).toBe(400);
  });

  test('409 si ya hay una sincronización manual en curso', async () => {
    sincronizarTransferenciasCajasManual.mockRejectedValue(new Error('Ya hay una sincronización manual de transferencias de caja en curso.'));

    const res = await request(app)
      .post('/transferencias-cajas/sincronizar-manual')
      .send({ fechaDesde: '2020-01-01T00:00:00Z', fechaHasta: '2020-01-31T23:59:59Z' })
      .set('x-test-permissions', JSON.stringify(['banks:admin']));

    expect(res.status).toBe(409);
  });
});

// GET /transferencias-cajas/bandeja — Fase D: pendientes con candidatos (Fase C, en vivo).
// (2026-09-02: se eliminó el apartado de huérfanas — pedido explícito del usuario.)
describe('GET /transferencias-cajas/bandeja', () => {
  let app;

  beforeEach(() => {
    jest.clearAllMocks();
    app = express();
    app.use(router);
  });

  test('responde 403 sin banks:erp:read', async () => {
    const res = await request(app)
      .get('/transferencias-cajas/bandeja')
      .set('x-test-permissions', JSON.stringify([]));

    expect(res.status).toBe(403);
    expect(CajaTransferencia.find).not.toHaveBeenCalled();
  });

  test('devuelve pendientes con sus candidatos calculados', async () => {
    const pendiente = { _id: 't-1', estatusMatch: 'pendiente', monto: 1500 };
    CajaTransferencia.find = jest.fn(() => ({
      sort: jest.fn(() => ({
        lean: jest.fn().mockResolvedValue([pendiente]),
      })),
    }));
    buscarCandidatos.mockResolvedValue([[{ _id: 'mov-1' }]]);

    const res = await request(app)
      .get('/transferencias-cajas/bandeja')
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_ERP_READ]));

    expect(res.status).toBe(200);
    expect(res.body.pendientes).toEqual([{ transferencia: pendiente, candidatos: [[{ _id: 'mov-1' }]] }]);
    expect(res.body.huerfanas).toBeUndefined();
  });
});

// POST /transferencias-cajas/:id/confirmar — Fase D: sin permit() propio a propósito (ver
// comentario en erp.routes.js) — setErpIds() exige el permiso internamente.
describe('POST /transferencias-cajas/:id/confirmar', () => {
  let app;

  beforeEach(() => {
    jest.clearAllMocks();
    app = express();
    app.use(express.json());
    app.use(router);
  });

  test('delega en confirmarMatch con los movementIds del body y el usuario autenticado', async () => {
    confirmarMatch.mockResolvedValue({ transferencia: { _id: 't-1', estatusMatch: 'matcheada' }, movimientos: [] });

    const res = await request(app)
      .post('/transferencias-cajas/t-1/confirmar')
      .send({ movementIds: ['mov-1', 'mov-2'] });

    expect(res.status).toBe(200);
    expect(confirmarMatch).toHaveBeenCalledWith('t-1', ['mov-1', 'mov-2'], expect.objectContaining({ _id: 'user-test' }));
    expect(res.body.transferencia.estatusMatch).toBe('matcheada');
  });

  test('propaga el status code de un error de negocio (ej. ConflictError) a la respuesta', async () => {
    const { ConflictError } = require('../../../shared/errors/AppError');
    confirmarMatch.mockRejectedValue(new ConflictError('El movimiento ya tiene un ID ERP vinculado'));

    const res = await request(app)
      .post('/transferencias-cajas/t-1/confirmar')
      .send({ movementIds: ['mov-1'] });

    expect(res.status).toBe(409);
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
