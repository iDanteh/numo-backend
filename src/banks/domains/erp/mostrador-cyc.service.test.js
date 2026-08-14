'use strict';

// mostrador-cyc.service.test.js — test de humo: procesarMostradorCyc() no tenía test hoy.
// Cubre únicamente el punto agregado en esta tarea: el $set del updateOne que
// vincula una CxC debe incluir primeraIdentificacionAt/primeraIdentificacionPor
// (vía resolvePrimeraIdentificacion) además de los campos preexistentes.
jest.mock('../banks/BankMovement.model');
jest.mock('./ErpCuentaPendiente.model');

const ExcelJS            = require('exceljs');
const BankMovement       = require('../banks/BankMovement.model');
const ErpCuentaPendiente = require('./ErpCuentaPendiente.model');
const { procesarMostradorCyc } = require('./mostrador-cyc.service');

async function buildExcelBuffer() {
  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet('Mostrador');
  ws.addRow(['FECHA', 'DESCRIPCIÓN', 'IMPORTE', 'BANCO', 'VENTAS', 'CLIENTE']);
  ws.addRow([null, 'Pago mostrador XYZ', 750, 'Santander', 'B1-456', 'Cliente Uno']);
  return wb.xlsx.writeBuffer();
}

beforeEach(() => {
  jest.clearAllMocks();
});

describe('procesarMostradorCyc — vinculación (smoke)', () => {
  test('vincula la CxC y el $set incluye primeraIdentificacionAt/Por junto con los campos de siempre', async () => {
    ErpCuentaPendiente.find.mockReturnValue({
      lean: jest.fn().mockResolvedValue([{
        erpId: 'CXC-2', serie: 'B1', folio: '456',
        serieExterna: null, folioExterno: null, folioFiscal: null,
        saldoActual: 750, total: 750, tipoPago: null,
      }]),
    });

    BankMovement.find.mockReturnValue({
      lean: jest.fn().mockResolvedValue([{
        _id: 'mov-2', concepto: 'Pago mostrador XYZ', deposito: 750, banco: 'Santander', fecha: null,
        numeroAutorizacion: null, referenciaNumerica: null,
        status: 'no_identificado', erpIds: [], erpLinks: [], identificadoPor: [], folio: '000002',
      }]),
    });

    BankMovement.bulkWrite.mockResolvedValue({ modifiedCount: 1 });

    const buffer = await buildExcelBuffer();
    const resultado = await procesarMostradorCyc(buffer, 'auth0|user-1', 'Ana');

    expect(resultado.relacionados).toBe(1);
    expect(BankMovement.bulkWrite).toHaveBeenCalledTimes(1);

    const ops = BankMovement.bulkWrite.mock.calls[0][0];
    const { update } = ops[0].updateOne;

    expect(update.$set.status).toBe('identificado');
    expect(update.$set.erpIds).toEqual(['CXC-2']);
    expect(update.$set.primeraIdentificacionAt).toBeInstanceOf(Date);
    expect(update.$set.primeraIdentificacionPor).toEqual({ userId: 'auth0|user-1', nombre: 'Ana' });
  });
});
