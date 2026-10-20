'use strict';

// CentroCosto.test.js — nuevo, cubre únicamente las dos columnas aditivas de
// Sistema de Lealtad (sistema-lealtad, PR1/8, design.md "Data Model" →
// CentroCosto): `participaLealtad` y `permiteCanjeLealtad`. No existe un
// test previo de este modelo; las columnas preexistentes (clave, sucursal,
// serieFacturacion, isActive) no se tocan ni se re-verifican aquí.
const CentroCosto = require('./CentroCosto');

describe('CentroCosto — columnas aditivas de Lealtad', () => {
  test('participaLealtad: BOOLEAN NOT NULL default true', () => {
    const attr = CentroCosto.getAttributes().participaLealtad;
    expect(attr).toBeDefined();
    expect(attr.allowNull).toBe(false);
    expect(attr.defaultValue).toBe(true);
  });

  test('permiteCanjeLealtad: BOOLEAN NOT NULL default true (independiente de participaLealtad)', () => {
    const attr = CentroCosto.getAttributes().permiteCanjeLealtad;
    expect(attr).toBeDefined();
    expect(attr.allowNull).toBe(false);
    expect(attr.defaultValue).toBe(true);
  });
});
