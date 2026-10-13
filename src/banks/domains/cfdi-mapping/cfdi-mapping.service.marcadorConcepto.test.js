'use strict';

// NC SAT ya completa (sin documentosRelacionados): el marcador del concepto
// (columna H) sale de `documentosRelacionadosConcepto` (los del ERP) — caso
// real Tehuantepec 7-oct, H0-261000130 / CAC-079958.

const { cfdiToMovimientos } = require('./cfdi-mapping.service');

const rule = { cuentaCargo: '4200010001', cuentaAbono: '1101010003', cuentaIva: '2104010001', tasaIva: '16' };
const cuentaMap = { '4200010001': 1, '1101010003': 2, '2104010001': 3 };

const nc = (extra = {}) => ({
  uuid: 'E587E3C2-29BE-4A87-80AB-64A22213E5AD', tipoDeComprobante: 'E', serie: 'H0', folio: '261000130',
  fecha: new Date('2026-10-07T15:19:54Z'), formaPago: '01', metodoPago: 'PUE',
  subTotal: 1864.29, total: 2162.58,
  emisor: { rfc: 'CCO011113663' }, receptor: { rfc: 'XAXX010101000', nombre: 'PUBLICO EN GENERAL' },
  conceptos: [{ descripcion: 'CANCELACION DE CLIENTE *', importe: 1864.29, impuestos: { traslados: [{ impuesto: '002', tasaOCuota: 0.16, base: 1864.29, importe: 298.29 }] } }],
  impuestos: { totalImpuestosTrasladados: 298.29, traslados: [{ impuesto: '002', tasaOCuota: 0.16, base: 1864.29, importe: 298.29 }] },
  ...extra,
});

const conceptos = (movs) => [...new Set(movs.map(m => m.concepto.replace(/^IVA - /, '')))];

test('NC completa sin documentosRelacionados → concepto con el CAC del ERP', async () => {
  const movimientos = await cfdiToMovimientos(nc({
    documentosRelacionadosConcepto: [{ Serie: 'CAC', Folio: '079958' }, { Serie: 'H0', Folio: '261000510' }],
  }), rule, cuentaMap);
  expect(conceptos(movimientos)).toEqual(['CAC-079958']);
  expect(movimientos.every(m => m.serie === 'H0-261000130')).toBe(true);
});

test('documentosRelacionados propios siguen teniendo prioridad', async () => {
  const movimientos = await cfdiToMovimientos(nc({
    documentosRelacionados: [{ Serie: 'DEV', Folio: '055991' }],
    documentosRelacionadosConcepto: [{ Serie: 'CAC', Folio: '079958' }],
  }), rule, cuentaMap);
  expect(conceptos(movimientos)).toEqual(['DEV-055991']);
});

test('sin ninguno → descripción del producto (como antes)', async () => {
  const movimientos = await cfdiToMovimientos(nc(), rule, cuentaMap);
  expect(conceptos(movimientos)).toEqual(['CANCELACION DE CLIENTE *']);
});
