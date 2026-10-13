'use strict';

// importExcel (reporte Excel del SAT): no pisa una copia que ya tiene su XML
// real y, sin columna TipoRelacion, no inventa relación '04' (2026-10-13).

jest.mock('../models/CFDI', () => ({
  findOneAndUpdate: jest.fn(),
  find: jest.fn(() => ({ lean: async () => [] })),
  updateMany: jest.fn(async () => ({})),
}));
jest.mock('../services/satVerification', () => ({ verifyCFDIWithSAT: jest.fn(() => new Promise(() => {})) }));

const ExcelJS = require('exceljs');
const CFDI = require('../models/CFDI');
const { importExcel } = require('./cfdi.controller');

const excel = async (filas) => {
  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet('CFDIs');
  ws.addRow(['UUID', 'RFC Emisor', 'RFC Receptor', 'Tipo', 'Fecha Emision', 'Total', 'SubTotal', 'Estatus', 'UUID Relacion', 'Conceptos']);
  filas.forEach(f => ws.addRow(f));
  return Buffer.from(await wb.xlsx.writeBuffer());
};

const fila = ['E587E3C2-29BE-4A87-80AB-64A22213E5AD', 'CCO011113663', 'XAXX010101000', 'Egreso', '07/10/2026',
  2162.58, 1864.29, 'Vigente', '8FD4E688-608A-46B9-87FD-DC70D1940883', 'CANCELACION DE CLIENTE'];

const correr = async () => {
  const req = {
    file: { buffer: await excel([fila]) },
    body: { ejercicio: '2026', periodo: '10', source: 'SAT' },
    user: { _id: 'auth0|x' },
  };
  // asyncHandler no regresa la promesa: se espera a que responda.
  await new Promise((resolve, reject) => {
    const res = { status: jest.fn(() => res), json: jest.fn(resolve) };
    importExcel(req, res, reject);
  });
};

beforeEach(() => CFDI.findOneAndUpdate.mockReset());

test('copia con XML: solo actualiza estatus/uploadedBy, no la pisa', async () => {
  CFDI.findOneAndUpdate.mockResolvedValueOnce({ lastComparisonStatus: 'match' });
  await correr();
  expect(CFDI.findOneAndUpdate).toHaveBeenCalledTimes(1);
  const [filtro, update] = CFDI.findOneAndUpdate.mock.calls[0];
  expect(filtro.xmlContent).toEqual({ $exists: true, $nin: [null, ''] });
  expect(Object.keys(update.$set).sort()).toEqual(['satStatus', 'uploadedBy']);
});

test('sin XML: upsert normal y relación sin tipo (no 04)', async () => {
  CFDI.findOneAndUpdate.mockResolvedValueOnce(null).mockResolvedValueOnce(null);
  await correr();
  expect(CFDI.findOneAndUpdate).toHaveBeenCalledTimes(2);
  const [, update, opts] = CFDI.findOneAndUpdate.mock.calls[1];
  expect(opts.upsert).toBe(true);
  expect(update.$set.cfdiRelacionados).toEqual([{ tipoRelacion: null, uuids: ['8FD4E688-608A-46B9-87FD-DC70D1940883'] }]);
});
