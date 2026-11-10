'use strict';

// 2026-11-10 (pedido del usuario): en el export, la TARJETA cobrada en otra
// sucursal sin depósito real ligado va a Caja (1101010001); el Efectivo y lo
// demás siguen en Caja por identificar (1101010003); con banco real se queda
// en su banco.
const { _extraerCobrosSucursal } = require('./poliza.service');

const cta = codigo => ({ id: Number(codigo.slice(-4)), codigo, nombre: codigo });
const CAJA_X_IDENT = cta('1101010003');
const CAJA = cta('1101010001');

const cobro = (reglaNombre, cuenta, haber, extra = {}) => ({
  id: Math.random(), tipoOrigen: 'Cobro Sucursal', reglaNombre, cuenta, concepto: `Cobro ${reglaNombre} ${haber}`,
  debe: 0, haber, cfdiUuid: null, metodoPago: 'PUE', ...extra,
});

test('tarjeta COS → 1101010001; efectivo y transferencia → 1101010003; con banco real se queda', () => {
  const movs = [
    cobro('TARJETA DE DEBITO', cta('1102011005'), 100),
    cobro('TARJETA DE CREDITO', cta('1102011005'), 200),
    cobro('TARJETA', cta('2103040001'), 300),
    cobro('TARJETA DE CREDITO', CAJA_X_IDENT, 400),
    cobro('EFECTIVO', CAJA_X_IDENT, 500),
    cobro('TRANSFERENCIA', cta('1102011005'), 600),
    cobro('056337', cta('1102011001'), 700), // depósito real BBVA (folio Numo)
  ];
  const { filas } = _extraerCobrosSucursal(movs, CAJA_X_IDENT, null, CAJA);
  const cuentaDe = haber => filas.find(f => f.haber === haber)?.cuenta?.codigo;
  expect(cuentaDe(100)).toBe('1101010001');
  expect(cuentaDe(200)).toBe('1101010001');
  expect(cuentaDe(300)).toBe('1101010001');
  expect(cuentaDe(400)).toBe('1101010001');
  expect(cuentaDe(500)).toBe('1101010003');
  expect(cuentaDe(600)).toBe('1101010003');
  expect(cuentaDe(700)).toBe('1102011001');
});

test('sin la cuenta de tarjeta (catálogo sin 1101010001) la tarjeta sigue como antes', () => {
  const { filas } = _extraerCobrosSucursal([cobro('TARJETA DE DEBITO', cta('1102011005'), 100)], CAJA_X_IDENT, null, null);
  expect(filas[0].cuenta.codigo).toBe('1101010003');
});
