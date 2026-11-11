'use strict';

jest.mock('../erp/erp-sync.service', () => ({ obtenerDesglosesCobroAlmacen: jest.fn() }));
jest.mock('../erp/kore-remisiones.service', () => ({ infoRemisiones: jest.fn() }));
jest.mock('../erp/KoreRemision.model', () => ({ find: jest.fn() }));

const { obtenerDesglosesCobroAlmacen } = require('../erp/erp-sync.service');
const { infoRemisiones } = require('../erp/kore-remisiones.service');
const KoreRemision = require('../erp/KoreRemision.model');
const { agregarHistorialAbonos } = require('./reportes-por-facturar.abonos');

const ABONOS_2996 = [
  { fecha: '2026-10-19T16:40:42Z', movimiento: 'CPF-261003396', forma: 'DEPOSITO EN EFECTIVO', importe: 1663.32, saldo: 69.31, idNumo: '053049' },
  { fecha: '2026-10-30T17:24:14Z', movimiento: 'RET-261024429', forma: null, importe: 69.31, saldo: 0, idNumo: null },
];

const _lean = v => ({ select: () => ({ lean: async () => v }) });

beforeEach(() => jest.clearAllMocks());

// Caso real 053049 / A0-260902996 (2026-10-30): depósito en efectivo + RET → saldada el 30-oct.
test('agrega el historial de abonos a todas las filas con esa remisión; la liquidada guardada no se vuelve a pedir', async () => {
  KoreRemision.find.mockReturnValue(_lean([
    { serie: 'A0', folio: '260902996', liquidadaEl: new Date('2026-10-30T17:24:14Z'), abonos: ABONOS_2996 },
  ]));
  obtenerDesglosesCobroAlmacen.mockResolvedValue([{ serieVenta: 'A0', folioVenta: '7', fechaCreacion: '2026-10-01T10:00:00Z' }]);
  infoRemisiones.mockImplementation(async ({ necesitaSaldo }) => {
    // Solo la que debe y tiene fecha de creación va a Kore.
    expect(necesitaSaldo('A0|260902996', { liquidadaEl: new Date(), abonos: ABONOS_2996 })).toBe(false);
    expect(necesitaSaldo('A0|7', undefined)).toBe(true);
    return { info: new Map([
      ['A0|260902996', { saldoActual: 0, liquidadaEl: new Date('2026-10-30T17:24:14Z'), abonos: ABONOS_2996 }],
      ['A0|7', { saldoActual: 50, abonos: [{ fecha: '2026-10-05T10:00:00Z', movimiento: 'ABO-1', forma: 'EFECTIVO', importe: 50, saldo: 50 }] }],
    ]), sinRespuesta: 0 };
  });

  const pendiente = { idNumo: '053049', remisiones: ['A0-260902996'] };
  const ficha = { idNumo: '1', remisiones: ['A0-7'] };
  const sinAbonos = { idNumo: '2', remisiones: ['A0-9'] };
  const datos = {
    movimientosMostrador: [{ depositos: [{ idNumo: '053049', remisiones: ['A0-260902996'] }] }],
    depositosPendientesFactura: [pendiente, sinAbonos],
    depositosPendientesPago: [ficha],
  };
  expect(await agregarHistorialAbonos({ rfc: 'X', datos })).toEqual({ sinRespuesta: 0 });

  // A Kore (desgloses) solo las que faltan.
  expect(obtenerDesglosesCobroAlmacen.mock.calls[0][0].folios.sort()).toEqual(['7', '9']);
  expect(pendiente.historialAbonos).toEqual([{ remision: 'A0-260902996', abonos: ABONOS_2996, saldo: 0, saldadaEl: new Date('2026-10-30T17:24:14Z') }]);
  expect(datos.movimientosMostrador[0].depositos[0].historialAbonos[0].abonos).toHaveLength(2);
  expect(ficha.historialAbonos[0]).toMatchObject({ saldo: 50, saldadaEl: null });
  expect(sinAbonos.historialAbonos).toBeUndefined();
});

test('Excel: un renglón por abono debajo del depósito y SALDADA / PENDIENTE al final, sin tocar la columna IMPORTE', () => {
  const ExcelJS = require('exceljs');
  const { agregarHojasSeccion } = require('./reportes-por-facturar.secciones');
  const wb = new ExcelJS.Workbook();
  const d = {
    fecha: '2026-10-15T00:00:00Z', descripcion: 'DEP EFECTIVO', importe: 1663.32, banco: 'BBVA', remisiones: ['A0-260902996'], cliente: 'JUAN', idNumo: '053049',
    historialAbonos: [{ remision: 'A0-260902996', abonos: ABONOS_2996, saldo: 0, saldadaEl: '2026-10-30T17:24:14Z' }],
  };
  const secciones = { mostrador: [{ serie: 'A0', sucursal: 'CEDIS', bloques: [{ tipo: 'pendiente', fecha: '2026-10-30', grupos: [{ cliente: 'JUAN', depositos: [d], total: 1663.32 }], total: 1663.32 }] }] };
  expect(agregarHojasSeccion(wb, secciones, 'mostrador')).toBe(true);
  const filas = [];
  wb.worksheets[0].eachRow(r => filas.push(r.values.slice(1)));
  const i = filas.findIndex(f => f[2] === 'DEP EFECTIVO');
  // CLIENTE junto a la FECHA en el renglón del depósito.
  expect(filas[i][1]).toBe('JUAN');
  // Sin renglón de encabezado con el nombre del cliente (2026-11-10).
  expect(filas[i - 1][0]).toBe('FECHA');
  // FECHA | CLIENTE | movimiento (la columna IMPORTE no lleva número).
  expect(filas[i + 1][1]).toBe('JUAN');
  expect(filas[i + 1][2]).toBe('↳ CPF-261003396 · DEPOSITO EN EFECTIVO · ID NUMO 053049 — $1,663.32 · saldo pendiente a esa fecha $69.31');
  expect(filas[i + 2][2]).toBe('↳ RET-261024429 — $69.31 · saldo pendiente a esa fecha $0.00');
  expect(filas[i + 3][1]).toBe('JUAN');
  expect(filas[i + 3][2]).toBe('✔ SALDADA EL 30/10/26');
  // La agrupación sigue: subtotal del cliente.
  expect(filas[i + 4][1]).toBe('Subtotal JUAN');
});
