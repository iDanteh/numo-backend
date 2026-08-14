'use strict';

// pagos-cyc.service.test.js — test de humo: procesarPagosCyc() no tenía test hoy.
// Cubre únicamente el punto agregado en esta tarea: el $set del updateOne que
// vincula una CxC debe incluir primeraIdentificacionAt/primeraIdentificacionPor
// (vía resolvePrimeraIdentificacion) además de los campos preexistentes.
jest.mock('../banks/BankMovement.model');
jest.mock('./ErpCuentaPendiente.model');

const ExcelJS            = require('exceljs');
const BankMovement       = require('../banks/BankMovement.model');
const ErpCuentaPendiente = require('./ErpCuentaPendiente.model');
const { procesarPagosCyc } = require('./pagos-cyc.service');

async function buildExcelBuffer() {
  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet('Pagos');
  ws.addRow(['FECHA', 'DESCRIPCIÓN', 'MONTO', 'BANCO', 'VENTAS']);
  ws.addRow([null, 'Pago cliente ABC', 500, 'BBVA', 'A0-123']);
  return wb.xlsx.writeBuffer();
}

beforeEach(() => {
  jest.clearAllMocks();
});

describe('procesarPagosCyc — vinculación (smoke)', () => {
  test('vincula la CxC y el $set incluye primeraIdentificacionAt/Por junto con los campos de siempre', async () => {
    ErpCuentaPendiente.find.mockReturnValue({
      lean: jest.fn().mockResolvedValue([{
        erpId: 'CXC-1', serie: 'A0', folio: '123',
        serieExterna: null, folioExterno: null, folioFiscal: null,
        saldoActual: 500, total: 500, tipoPago: null,
      }]),
    });

    BankMovement.find.mockReturnValue({
      lean: jest.fn().mockResolvedValue([{
        _id: 'mov-1', concepto: 'Pago cliente ABC', deposito: 500, banco: 'BBVA', fecha: null,
        numeroAutorizacion: null, referenciaNumerica: null,
        status: 'no_identificado', erpIds: [], erpLinks: [], identificadoPor: [], folio: '000001',
      }]),
    });

    BankMovement.bulkWrite.mockResolvedValue({ modifiedCount: 1 });

    const buffer = await buildExcelBuffer();
    const resultado = await procesarPagosCyc(buffer, 'auth0|user-1', 'Ana');

    expect(resultado.relacionados).toBe(1);
    expect(BankMovement.bulkWrite).toHaveBeenCalledTimes(1);

    const ops = BankMovement.bulkWrite.mock.calls[0][0];
    const { update } = ops[0].updateOne;

    expect(update.$set.status).toBe('identificado');
    expect(update.$set.erpIds).toEqual(['CXC-1']);
    expect(update.$set.primeraIdentificacionAt).toBeInstanceOf(Date);
    expect(update.$set.primeraIdentificacionPor).toEqual({ userId: 'auth0|user-1', nombre: 'Ana' });
  });
});
