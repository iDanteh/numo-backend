'use strict';

// "A0 COBRADAS EN PUERTO" (2026-10-30): cobros de Kore en la caja de otro almacén.
jest.mock('../erp/erp-sync.service', () => ({ obtenerDesglosesCobroAlmacen: jest.fn() }));

const ExcelJS = require('exceljs');
const erp = require('../erp/erp-sync.service');
const { cobrosEnOtraCaja } = require('./reportes-por-facturar.otra-caja');
const { armarSecciones, agregarHojasSeccion } = require('./reportes-por-facturar.secciones');

const datos = {
  movimientosMostrador: [{ serie: 'A0', folio: '261014387', depositos: [{ remisiones: ['A0-261003322'], cliente: 'PEDRO', importe: 689.44, idNumo: '054001' }] }],
  cobrosCajaGlobales: [],
  depositosPendientesFactura: [{ remisiones: ['A0-261002139'], cliente: 'JUANA', importe: 100, idNumo: '054002' }],
  depositosPendientesPago: [],
};

beforeEach(() => {
  jest.clearAllMocks();
  erp.obtenerDesglosesCobroAlmacen.mockResolvedValue([
    { serieVenta: 'A0', folioVenta: '261003322', serieFactura: 'A0', folioFactura: '261014387', cobros: [
      { serieOrigen: 'CPF', folioOrigen: '261003893', claveCentro: 'O0', monto: -689.44, fecha: '2026-10-21T16:27:28Z', formasPago: [{ nombre: 'EFECTIVO', monto: 689.44 }] },
      { serieOrigen: 'CPF', folioOrigen: '1', claveCentro: 'A0', monto: -5, fecha: '2026-10-21T16:00:00Z', formasPago: [] }, // en su propia caja: no
    ] },
    { serieVenta: 'A0', folioVenta: '261002139', cobros: [
      { serieOrigen: 'CPF', folioOrigen: '261002835', claveCentro: 'O0', monto: -2510.71, fecha: '2026-10-15T17:01:27Z', formasPago: [{ nombre: 'EFECTIVO', monto: 2510.71 }] },
    ] },
  ]);
});

test('solo cobros en la caja de otro almacén, con factura, cliente e ID NUMO del depósito del mismo importe', async () => {
  const { cobros } = await cobrosEnOtraCaja({ rfc: 'X', datos });
  expect(erp.obtenerDesglosesCobroAlmacen).toHaveBeenCalledWith({ rfc: 'X', series: ['A0', 'A0'], folios: ['261003322', '261002139'] });
  expect(cobros.map(c => [c.remisiones[0], c.caja, c.importe, c.banco, c.factura, c.cliente, c.idNumo])).toEqual([
    ['A0-261002139', 'O0', 2510.71, 'EFECTIVO', 'POR FACTURAR', 'JUANA', 'S/R'],
    ['A0-261003322', 'O0', 689.44, 'EFECTIVO', 'A0-261014387', 'PEDRO', '054001'],
  ]);
});

test('pestaña y hoja "A0 COBRADAS EN PUERTO"', async () => {
  const { cobros } = await cobrosEnOtraCaja({ rfc: 'X', datos });
  const s = armarSecciones({ cobrosOtraCaja: cobros }, serie => ({ O0: 'PUERTO', A0: 'CEDIS' }[serie] ?? null));
  expect(s.otraCaja.map(e => [e.titulo, e.bloques[0].total])).toEqual([['A0 COBRADAS EN PUERTO', 3200.15]]);
  const wb = new ExcelJS.Workbook();
  agregarHojasSeccion(wb, s, 'otraCaja');
  expect(wb.worksheets.map(w => w.name)).toEqual(['A0 COBRADAS EN PUERTO']);
  expect(wb.worksheets[0].getRow(2).values.slice(1)).toEqual(['FECHA', 'DESCRIPCIÓN', 'IMPORTE', 'FORMA DE PAGO', 'FACTURA', 'REM', 'CLIENTE', 'CAJA', 'ID NUMO']);
});

test('Kore no responde → cuenta las remisiones sin respuesta', async () => {
  erp.obtenerDesglosesCobroAlmacen.mockRejectedValue(new Error('429'));
  expect(await cobrosEnOtraCaja({ rfc: 'X', datos })).toEqual({ cobros: [], sinRespuesta: 2 });
});
