'use strict';

// erp.routes.test.js — _aporteConRatchet(): función NUEVA (fix 2026-08-06, folio 032686).
// Un depósito bancario ya identificado por un humano NUNCA debe bajar su aporte en una
// corrida posterior de "Recalcular saldo ERP", sin importar la causa (RET/CAC/DEV,
// retención, cancelación, devolución) — solo sube si Kore trae un monto MAYOR atribuible a
// este movimiento. Requerir el router real no pega a Mongo/Kore — los modelos solo
// declaran esquemas y las llamadas HTTP viven dentro de handlers, nunca a nivel de módulo.
const router = require('./erp.routes');

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
