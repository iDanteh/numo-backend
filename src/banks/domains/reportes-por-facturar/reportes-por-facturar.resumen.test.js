'use strict';

// Hoja "RESUMEN POR FACTURAR" (2026-11-12): formato del Excel del usuario, BN 20 % al efectivo.
const ExcelJS = require('exceljs');
const { armarSecciones, agregarHojasSeccion, formaPagoDe } = require('./reportes-por-facturar.secciones');
const AdmZip = require('adm-zip');
const { filasResumen, aplicarCombinadas, tipoMovimiento } = require('./reportes-por-facturar.resumen');

const datos = {
  movimientosMostrador: [{
    serie: 'A0', folio: '261014387', uuid: 'U1', esGlobal: true, fechaFactura: '2026-11-10T18:00:00Z', receptorNombre: 'PUBLICO EN GENERAL',
    // Ya facturado en la global: NO entra al resumen (2026-11-12, "cuando se facturen salga").
    depositos: [{ remisiones: ['A0-261003999'], cliente: 'LUIS', importe: 777, idNumo: '054099', banco: 'BBVA', fecha: '2026-11-09T00:00:00Z', ppdPorFacturar: true, formasPago: { efectivo: 777 } }],
  }],
  cobrosCajaGlobales: [],
  depositosPendientesFactura: [
      { remisiones: ['A0-261003322'], cliente: 'PEDRO', importe: 1000, idNumo: '054001', banco: 'BBVA', fecha: '2026-11-09T00:00:00Z', ppdPorFacturar: true, formasPago: { efectivo: 1000 },
        historialAbonos: [{ remision: 'A0-261003322', saldo: 0, saldadaEl: '2026-11-09T18:00:00Z', abonos: [
          { fecha: '2026-11-09T18:00:00Z', movimiento: 'CPF-9', importe: 1000, saldo: 0, idNumo: '054001' }] }] },
      // Mixto: se parte en dos renglones.
      { remisiones: ['A0-261003400', 'A0-261003401'], cliente: 'ANA', importe: 500, idNumo: '054003', banco: 'BBVA', fecha: '2026-11-09T00:00:00Z', ppdPorFacturar: true, formasPago: { transferencia: 300, depositoEfectivo: 200 } },
  ],
  depositosPendientesPago: [
    { remisiones: ['I0-261000010'], cliente: 'JUANA', importe: 400, idNumo: '054010', banco: 'AZTECA', fecha: '2026-11-10T00:00:00Z', saldo: 50, formasPago: { efectivo: 400 },
      // Historial completo: la devolución y la bonificación NO son abonos (caso INTI CESAR, 2026-11-12).
      historialAbonos: [{ remision: 'I0-261000010', saldo: 50, abonos: [
        { fecha: '2026-11-03T17:00:00Z', movimiento: 'ABO-0', forma: 'TRANSFERENCIA', importe: 100, saldo: 750 },
        { fecha: '2026-11-04T17:00:00Z', movimiento: 'RET-5', forma: 'DE DEV-1', importe: 30, saldo: 720 },
        { fecha: '2026-11-05T17:00:00Z', movimiento: 'ABO-1', forma: 'EFECTIVO', importe: 250, saldo: 470 },
        { fecha: '2026-11-12T17:00:00Z', movimiento: 'ABO-2', forma: 'EFECTIVO', importe: 400, saldo: 70, idNumo: '054010' },
        { fecha: '2026-11-12T18:00:00Z', movimiento: 'RET-6', forma: 'DE BON-2', importe: 20, saldo: 50 },
      ] }] },
  ],
};

test('un renglón por forma de pago, claves SAT y abonos anteriores', () => {
  const filas = filasResumen(armarSecciones(datos), formaPagoDe);
  expect(filas.map(f => [f.serie, f.forma, f.folio, f.factura, f.cliente, f.importe, f.abonosAnteriores])).toEqual([
    ['A0', '01 DEP', '261003400, 261003401', 'POR FACTURAR', 'ANA', 200, 0],
    ['A0', '03', '261003400, 261003401', 'POR FACTURAR', 'ANA', 300, 0],
    ['A0', '01', '261003322', 'POR FACTURAR', 'PEDRO', 1000, 0],
    ['I0', '01', '261000010', 'POR FACTURAR', 'JUANA', 400, 350],
  ]);
  // Estatus (2026-11-12): PEDRO saldada el 9-nov; JUANA todavía debe $50; ANA sin historial (pendiente → SALDADA).
  expect(filas.map(f => [f.cliente, f.estatus, f.saldo, f.pagadaEl])).toEqual([
    ['ANA', 'SALDADA', 0, null], ['ANA', 'SALDADA', null, null], // saldo solo en el 1er renglón del mixto
    ['PEDRO', 'SALDADA', 0, '2026-11-09'], ['JUANA', 'ABONADA', 50, null],
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
  // JUANA: 5 movimientos en Kore → 5 sub-renglones (8–12), A–U combinadas.
  // Solo los 2 pagos anteriores suman (350); la devolución no.
  expect(fila(8).getCell(10).value.result).toBe(150); // (400 + 350) × 20 %
  expect(fila(8).getCell(9).value.result).toBe(600);
  expect(fila(8).getCell(17).value).toBe(2);
  expect(fila(8).getCell(18).value).toEqual(new Date('2026-11-05T12:00:00Z'));
  // No. ABONOS: PEDRO solo tiene su pago inicial → 0 (no vacío); ANA sin historial de Kore → vacío.
  expect(fila(7).getCell(17).value).toBe(0);
  // Gris / blanco intercalado por venta: ANA (5) gris, ANA (6) blanco, PEDRO (7) gris, JUANA (8–12) blanco.
  const gris = n => fila(n).getCell(1).fill?.fgColor?.argb ?? null;
  expect([5, 6, 7, 8, 12].map(gris)).toEqual(['FFE5E7EB', null, 'FFE5E7EB', null, null]);
  expect(fila(7).getCell(28).fill.fgColor.argb).toBe('FFE5E7EB');
  expect(fila(5).getCell(17).value).toBeNull();
  // IMPORTE TOTAL VENTA (S) = lo que debía antes del primer movimiento: JUANA 750 + 100; PEDRO 0 + 1000; ANA sin historial.
  expect(fila(4).getCell(19).value).toBe('IMPORTE TOTAL VENTA');
  expect([19, 20, 21, 22].map(c => fila(8).getCell(c).value)).toEqual([850, 'ABONADA', 50, null]);
  expect([19, 20, 21, 22].map(c => fila(7).getCell(c).value)).toEqual([1000, 'SALDADA', 0, new Date('2026-11-09T12:00:00Z')]);
  expect(fila(5).getCell(19).value).toBeNull();
  expect(fila(9).getCell(19).value).toBeNull(); // sub-renglón: no suma doble
  expect(fila(4).getCell(28).value).toBe('SALDO DESPUÉS DE ESTE MOVIMIENTO');
  const mov = n => [23, 24, 25, 26, 27, 28].map(c => fila(n).getCell(c).value);
  expect(mov(7)).toEqual([new Date('2026-11-09T12:00:00Z'), 'PAGO INICIAL', 1000, 'CPF-9', '', 0]);
  expect([8, 9, 10, 11, 12].map(mov)).toEqual([
    // Pago inicial arriba (aunque Kore lo registró 2 días después); lo demás por fecha.
    [new Date('2026-11-12T12:00:00Z'), 'PAGO INICIAL', 400, 'ABO-2', 'EFECTIVO', 70],
    [new Date('2026-11-03T12:00:00Z'), 'ABONO', 100, 'ABO-0', 'TRANSFERENCIA', 750],
    [new Date('2026-11-04T12:00:00Z'), 'DEVOLUCIÓN', 30, 'RET-5', 'DE DEV-1', 720],
    [new Date('2026-11-05T12:00:00Z'), 'ABONO', 250, 'ABO-1', 'EFECTIVO', 470],
    [new Date('2026-11-12T12:00:00Z'), 'BONIFICACIÓN', 20, 'RET-6', 'DE BON-2', 50],
  ]);
  // Combinadas sin perder el texto de abajo (para los filtros); importes solo arriba.
  const rangos = wb._combinadasOcultas[0].rangos;
  expect(rangos).toHaveLength(22); // A–V de JUANA
  expect(rangos).toEqual(expect.arrayContaining(['C8:C12', 'S8:S12', 'V8:V12']));
  expect(rangos.some(r => r.startsWith('W'))).toBe(false);
  // En el .xlsx: <mergeCells> después de <autoFilter> y el texto de abajo se conserva.
  const xml = new AdmZip(aplicarCombinadas(await wb.xlsx.writeBuffer(), wb)).getEntry(`xl/worksheets/sheet${ws.id}.xml`).getData().toString('utf8');
  expect(xml).toMatch(/<autoFilter ref="A4:AB12"\/><mergeCells count="22"><mergeCell ref="A8:A12"\/>/);
  expect(xml).toMatch(/<c r="F12"[^>]*t="(s|str|inlineStr)"/);
  expect([fila(12).getCell(3).value, fila(12).getCell(6).value, fila(12).getCell(7).value, fila(12).getCell(9).value]).toEqual(['I0', 'JUANA', null, null]);
  const tot = fila(14);
  expect(tot.getCell(6).value).toBe('TOTALES');
  expect(tot.getCell(7).value).toEqual({ formula: 'SUBTOTAL(9,G5:G12)', result: 1900 });
  expect(tot.getCell(8).value.result).toBe(350);
  expect(tot.getCell(10).value.result).toBe(350);
  expect(tot.getCell(19).value).toEqual({ formula: 'SUBTOTAL(9,S5:S12)', result: 1850 });
  expect(tot.getCell(21).value).toEqual({ formula: 'SUBTOTAL(9,U5:U12)', result: 50 });
  expect(ws.autoFilter).toEqual({ from: { row: 4, column: 1 }, to: { row: 12, column: 28 } });
});

test('TIPO de movimiento: caso real INTI CESAR A0-261000641 (depósito 057403 del 9-nov)', () => {
  const d = { idNumo: '057403', importe: 1264 };
  const tipo = a => tipoMovimiento(a, d, '2026-11-09', new Set(['057403']));
  expect(tipo({ fecha: '2026-10-06T01:27:39Z', forma: 'DE DEV-058156', importe: 247.52 })).toBe('DEVOLUCIÓN');
  expect(tipo({ fecha: '2026-11-11T23:03:56Z', forma: 'DEPOSITO EN EFECTIVO', importe: 1264, idNumo: '057403' })).toBe('PAGO INICIAL');
  expect(tipo({ fecha: '2026-11-11T23:44:49Z', forma: 'DE BON-326705', importe: 74.77 })).toBe('BONIFICACIÓN');
  expect(tipo({ fecha: '2026-10-20T17:00:00Z', forma: 'EFECTIVO', importe: 100 })).toBe('ABONO ANTERIOR');
  expect(tipo({ fecha: '2026-11-15T17:00:00Z', forma: 'TRANSFERENCIA', importe: 100 })).toBe('ABONO POSTERIOR');
  expect(tipo({ fecha: '2026-10-20T17:00:00Z', forma: 'DE XYZ-1', importe: 5 })).toBe('AJUSTE');
  expect(tipo({ fecha: '2026-10-20T17:00:00Z', forma: 'X', importe: -5 })).toBe('CARGO');
});

test('el Excel completo incluye la hoja RESUMEN al final', async () => {
  const wb = new ExcelJS.Workbook();
  agregarHojasSeccion(wb, armarSecciones(datos), undefined, {});
  expect(wb.worksheets.map(w => w.name).at(-1)).toBe('RESUMEN POR FACTURAR');
});
