'use strict';

// collection-request-erp-links.test.js — buildErpLinksParaCobro(): funciones
// puras, sin Kore ni Mongo real. Primero fija (approval tests) el
// comportamiento actual de las llamadas de 3 argumentos (Modo 1 y Modo 2,
// SIN cambios en este PR) y luego cubre el 4º parámetro `opts` nuevo
// (multi-bank-movement, D5/D7 del design).
const { buildErpLinksParaCobro } = require('./collection-request-erp-links');

function cxc(erpId, total) {
  return { erpId, total, serie: null, folioExterno: null, tipoPago: null, folioFiscal: null };
}

function forma(descripcion, importe, extra = {}) {
  return { formaPagoId: 'fp-x', formaPagoDescripcion: descripcion, importe, ...extra };
}

describe('buildErpLinksParaCobro — approval tests', () => {
  test('Modo 1 (3 args): 1 CxC + N formasPago, saldoActual descuenta el total de ESTE cobro', () => {
    const cr = {
      cxcs: [cxc('CXC-1', 100000)],
      formasPago: [forma('Transferencia', 60000), forma('Efectivo', 40000)],
    };
    const cuentasKore = [{ id: 'CXC-1', saldoActual: 100000 }];

    const [link] = buildErpLinksParaCobro(cr, cuentasKore, []);

    expect(link.erpId).toBe('CXC-1');
    // saldoActual SIEMPRE descuenta el total real pagado en Kore, sin importar la forma.
    expect(link.saldoActual).toBe(0);
    // CORRECCIÓN 2026-09-04 (pedido explícito del usuario): saldoPagadoTotal ahora es
    // EXACTAMENTE igual a saldoPagado (solo formas bancarias) — antes sumaba las 2 formas
    // (100000). El Efectivo sigue íntegro en desglosePorFormaPago, solo deja de alimentar
    // saldoErp.
    expect(link.saldoPagado).toBe(60000);
    expect(link.saldoPagadoTotal).toBe(60000);
    expect(link.desglosePorFormaPago).toHaveLength(2);
  });

  test('Modo 2 (3 args): N CxC + 1 forma de pago global, montoAsignado reparte por CxC', () => {
    const cr = {
      cxcs: [
        { ...cxc('CXC-1', 30000), montoAsignado: 30000 },
        { ...cxc('CXC-2', 20000), montoAsignado: 20000 },
      ],
      formasPago: [forma('Transferencia', 50000)],
    };
    const cuentasKore = [{ id: 'CXC-1', saldoActual: 30000 }, { id: 'CXC-2', saldoActual: 20000 }];

    const links = buildErpLinksParaCobro(cr, cuentasKore, []);

    expect(links).toHaveLength(2);
    expect(links.find(l => l.erpId === 'CXC-1').saldoActual).toBe(0);
    expect(links.find(l => l.erpId === 'CXC-2').saldoActual).toBe(0);
    expect(links.find(l => l.erpId === 'CXC-1').saldoPagado).toBe(30000);
    expect(links.find(l => l.erpId === 'CXC-1').saldoPagadoTotal).toBe(30000);
  });

  // CORRECCIÓN 2026-09-04 (pedido explícito del usuario): saldoErp (alimentado por
  // saldoPagadoTotal) debe reflejar solo Transferencia/Depósito en efectivo/Cheque —
  // cualquier otra forma (ej. Anticipo) cierra la CxC en Kore igual, pero no debe
  // sumar a saldoErp. Esa plata sigue viva en desglosePorFormaPago ("Otros" en el
  // dropdown de CxC vinculadas, del lado del frontend).
  test('Modo 1 con una forma NO bancaria (ej. Anticipo): saldoActual la descuenta igual, saldoPagadoTotal la ignora', () => {
    const cr = {
      cxcs: [cxc('CXC-1', 80000)],
      formasPago: [forma('Anticipo', 80000)],
    };
    const cuentasKore = [{ id: 'CXC-1', saldoActual: 80000 }];

    const [link] = buildErpLinksParaCobro(cr, cuentasKore, []);

    expect(link.saldoActual).toBe(0); // la CxC sí queda saldada en Kore
    expect(link.saldoPagado).toBe(0);
    expect(link.saldoPagadoTotal).toBe(0); // pero no aporta a saldoErp
    expect(link.desglosePorFormaPago).toEqual([
      expect.objectContaining({ formaPagoDescripcion: 'Anticipo', monto: 80000 }),
    ]);
  });

  test('Modo 2 con la forma global NO bancaria (ej. Anticipo): mismo criterio que Modo 1', () => {
    const cr = {
      cxcs: [{ ...cxc('CXC-1', 30000), montoAsignado: 30000 }],
      formasPago: [forma('Anticipo', 30000)],
    };
    const cuentasKore = [{ id: 'CXC-1', saldoActual: 30000 }];

    const [link] = buildErpLinksParaCobro(cr, cuentasKore, []);

    expect(link.saldoActual).toBe(0);
    expect(link.saldoPagado).toBe(0);
    expect(link.saldoPagadoTotal).toBe(0);
  });
});

describe('buildErpLinksParaCobro — opts (multi-bank-movement, PR1 inerte)', () => {
  test('opts.formasPago acota totalPaid/bancoPaid/desglose al GRUPO, no a cr.formasPago completo', () => {
    const cr = {
      cxcs: [cxc('CXC-1', 100000)],
      // cr.formasPago tiene AMBAS formas, pero el grupo de este movimiento solo cubre una.
      formasPago: [forma('Transferencia', 60000), forma('Efectivo', 40000)],
    };
    const cuentasKore = [{ id: 'CXC-1', saldoActual: 100000 }];
    const grupoTransferencia = [cr.formasPago[0]];

    const [link] = buildErpLinksParaCobro(cr, cuentasKore, [], { formasPago: grupoTransferencia });

    expect(link.saldoPagado).toBe(60000);
    expect(link.saldoPagadoTotal).toBe(60000); // solo el grupo, NO los 100000 totales
    expect(link.desglosePorFormaPago).toHaveLength(1);
  });

  test('opts.pagadoTotalCxc hace que 2 links (uno por grupo) compartan el MISMO saldoActual', () => {
    const cr = { cxcs: [cxc('CXC-1', 100000)], formasPago: [forma('Transferencia', 50000), forma('Efectivo', 50000)] };
    const cuentasKore = [{ id: 'CXC-1', saldoActual: 100000 }];
    const pagadoTotalCxc = 100000; // suma de AMBOS grupos combinados

    const [linkGrupoA] = buildErpLinksParaCobro(cr, cuentasKore, [], {
      formasPago: [cr.formasPago[0]],
      pagadoTotalCxc,
    });
    const [linkGrupoB] = buildErpLinksParaCobro(cr, cuentasKore, [], {
      formasPago: [cr.formasPago[1]],
      pagadoTotalCxc,
    });

    // saldoActual SIEMPRE usa el total real (ambas formas) — sin pagadoTotalCxc, cada
    // llamada vería solo su mitad (50000) y ambas quedarían en saldoActual=50000 en vez
    // de 0 — exactamente el bug que D5 corrige.
    expect(linkGrupoA.saldoActual).toBe(0);
    expect(linkGrupoB.saldoActual).toBe(0);
    // CORRECCIÓN 2026-09-04: saldoPagadoTotal ahora es bancario-only, por grupo. El grupo A
    // (Transferencia) sigue sumando su monto; el grupo B (Efectivo, NO bancario) ahora
    // acumula 0 — ese dinero sigue existiendo en desglosePorFormaPago, solo deja de
    // alimentar saldoErp.
    expect(linkGrupoA.saldoPagadoTotal).toBe(50000);
    expect(linkGrupoB.saldoPagadoTotal).toBe(0);
  });

  test('sin opts.pagadoTotalCxc (default null): saldoActual usa el totalPaid del propio grupo (comportamiento 3-arg)', () => {
    const cr = { cxcs: [cxc('CXC-1', 100000)], formasPago: [forma('Transferencia', 30000)] };
    const cuentasKore = [{ id: 'CXC-1', saldoActual: 100000 }];

    const [link] = buildErpLinksParaCobro(cr, cuentasKore, [], { formasPago: cr.formasPago });

    expect(link.saldoActual).toBe(70000);
  });

  test('Modo 2 con opts: el 4º parámetro se ignora, comportamiento idéntico al de 3 args', () => {
    const cr = {
      cxcs: [{ ...cxc('CXC-1', 30000), montoAsignado: 30000 }],
      formasPago: [forma('Transferencia', 30000)],
    };
    const cuentasKore = [{ id: 'CXC-1', saldoActual: 30000 }];

    // Congela `new Date()` — buildErpLinksParaCobro calcula fechaCobro internamente
    // en cada invocación, y comparar 2 llamadas reales podría diferir en 1ms sin
    // que eso sea un cambio de comportamiento real.
    jest.useFakeTimers().setSystemTime(new Date('2026-08-06T00:00:00Z'));
    const conOpts = buildErpLinksParaCobro(cr, cuentasKore, [], { pagadoTotalCxc: 999999 });
    const sinOpts = buildErpLinksParaCobro(cr, cuentasKore, []);
    jest.useRealTimers();

    expect(conOpts).toEqual(sinOpts);
  });
});
