'use strict';

// Egreso de Aplicación de Anticipo (tipoRelacion 07) sin tipoOrigen del ERP:
// debe deducirse 'Egreso' (reglas TO-EGR), no 'Bonificación' — con eso tomaba
// TO-BON y abonaba a Bancos (caso real Puerto O0-261000173, 2026-10-12).

const { _derivarTipoOrigenPublic: derivar } = require('./cfdi-mapping.service');

const egreso = (tipoRelacion, descripcion = 'APLICACION DE ANTICIPO') => ({
  tipoDeComprobante: 'E',
  cfdiRelacionados: tipoRelacion ? [{ tipoRelacion, uuids: ['X'] }] : [],
  conceptos: [{ descripcion }],
});

test.each([
  ['07', 'Egreso'],
  ['03', 'Devolución'],
  ['01', 'Nota de Crédito'],
  [null, 'Bonificación'],
])('Egreso con tipoRelacion %s → %s', (rel, esperado) => {
  expect(derivar(egreso(rel))).toBe(esperado);
});

test('CANCELAC en el concepto sigue siendo Cancelación aunque sea 07', () => {
  expect(derivar(egreso('07', 'CANCELACION DE VENTA'))).toBe('Cancelación');
});
