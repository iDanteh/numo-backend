'use strict';

// Hoja "RESUMEN POR FACTURAR" (2026-11-12): formato del Excel del usuario, BN 20 % al efectivo.
const ExcelJS = require('exceljs');
const { armarSecciones, agregarHojasSeccion, formaPagoDe } = require('./reportes-por-facturar.secciones');
const { filasResumen } = require('./reportes-por-facturar.resumen');

const datos = {
  movimientosMostrador: [{
    serie: 'A0', folio: '261014387', uuid: 'U1', esGlobal: true, fechaFactura: '2026-11-10T18:00:00Z', receptorNombre: 'PUBLICO EN GENERAL',
    depositos: [
      { remisiones: ['A0-261003322'], cliente: 'PEDRO', importe: 1000, idNumo: '054001', banco: 'BBVA', fecha: '2026-11-09T00:00:00Z', ppdPorFacturar: true, formasPago: { efectivo: 1000 } },
      // Mixto: se parte en dos renglones.
      { remisiones: ['A0-261003400', 'A0-261003401'], cliente: 'ANA', importe: 500, idNumo: '054003', banco: 'BBVA', fecha: '2026-11-09T00:00:00Z', ppdPorFacturar: true, formasPago: { transferencia: 300, depositoEfectivo: 200 } },
    ],
  }],
  cobrosCajaGlobales: [],
  depositosPendientesFactura: [],
  depositosPendientesPago: [
    { remisiones: ['I0-261000010'], cliente: 'JUANA', importe: 400, idNumo: '054010', banco: 'AZTECA', fecha: '2026-11-10T00:00:00Z', saldo: 50, formasPago: { efectivo: 400 },
      historialAbonos: [{ remision: 'I0-261000010', saldo: 50, abonos: [
        { fecha: '2026-11-05T17:00:00Z', movimiento: 'ABO-1', forma: 'EFECTIVO', importe: 250, saldo: 450 },
        { fecha: '2026-11-10T17:00:00Z', movimiento: 'ABO-2', forma: 'EFECTIVO', importe: 400, saldo: 50, idNumo: '054010' },
      ] }] },
  ],
};

test('un renglón por forma de pago, claves SAT y abonos anteriores', () => {
  const filas = filasResumen(armarSecciones(datos), formaPagoDe);
  expect(filas.map(f => [f.serie, f.forma, f.folio, f.factura, f.cliente, f.importe, f.abonosAnteriores])).toEqual([
    ['A0', '01 DEP', '261003400, 261003401', 'A0-261014387', 'ANA', 200, 0],
    ['A0', '03', '261003400, 261003401', 'A0-261014387', 'ANA', 300, 0],
    ['A0', '01', '261003322', 'A0-261014387', 'PEDRO', 1000, 0],
    ['I0', '01', '261000010', 'POR FACTURAR', 'JUANA', 400, 250],
  ]);
});

test('Excel: fórmulas BN 20 % solo en 01 y FACTURADO = cobrado + abonos − BN', async () => {
  const wb = new ExcelJS.Workbook();
  expect(agregarHojasSeccion(wb, armarSecciones(datos), 'resumen', { fechaInicio: '2026-11-10', fechaFin: '2026-11-10', sucursal: 'Todas' })).toBe(true);
  const ws = wb.getWorksheet('RESUMEN POR FACTURAR');
  expect(ws.getRow(2).getCell(6).value).toBe('POR FACTURAR DEL DÍA 10 NOVIEMBRE 2026');
  const fila = n => ws.getRow(n);
  // Encabezado en 4; renglones 5–7 (A0) y 8 (I0) seguidos, sin vacío (filtros).
  expect(fila(4).getCell(10).value).toBe('BN');
  expect(fila(5).getCell(10).value.formula).toBe('IF(B5="01",(G5+H5)*0.2,0)');
  expect(fila(5).getCell(10).value.result || 0).toBe(0); // 01 DEP: sin BN
  expect(fila(7).getCell(10).value.result).toBe(200);
  expect(fila(7).getCell(9).value).toEqual({ formula: 'G7+H7-J7', result: 800 });
  expect(fila(8).getCell(10).value.result).toBe(130); // (400 + 250) × 20 %
  expect(fila(8).getCell(9).value.result).toBe(520);
  const tot = fila(10);
  expect(tot.getCell(6).value).toBe('TOTALES');
  expect(tot.getCell(7).value).toEqual({ formula: 'SUBTOTAL(9,G5:G8)', result: 1900 });
  expect(tot.getCell(10).value.result).toBe(330);
  expect(ws.autoFilter).toEqual({ from: { row: 4, column: 1 }, to: { row: 8, column: 19 } });
  // Abonos anteriores: cuántos, último y la fecha de cada uno (solo el de días anteriores).
  expect(fila(8).getCell(17).value).toBe(1);
  expect(fila(8).getCell(18).value).toEqual(new Date('2026-11-05T12:00:00Z'));
  expect(fila(8).getCell(19).value).toBe('5/11/26 $250.00 · ABO-1 · EFECTIVO');
  expect(fila(7).getCell(17).value).toBeNull();
});

test('el Excel completo incluye la hoja RESUMEN al final', async () => {
  const wb = new ExcelJS.Workbook();
  agregarHojasSeccion(wb, armarSecciones(datos), undefined, {});
  expect(wb.worksheets.map(w => w.name).at(-1)).toBe('RESUMEN POR FACTURAR');
});
