'use strict';

// refacturaciones-cyc.service.test.js — test de humo: procesarRefacturacionesCyc()
// no tenía test hoy. Cubre únicamente el punto agregado en esta tarea: el $set
// del updateOne del Tier 1 (AUTO, vía token numérico del concepto) debe incluir
// primeraIdentificacionAt/primeraIdentificacionPor cuando newStatus pasa a
// 'identificado', además de los campos preexistentes.
jest.mock('../banks/BankMovement.model');
jest.mock('./ErpCuentaPendiente.model');

const ExcelJS            = require('exceljs');
const BankMovement       = require('../banks/BankMovement.model');
const ErpCuentaPendiente = require('./ErpCuentaPendiente.model');
const { procesarRefacturacionesCyc } = require('./refacturaciones-cyc.service');

async function buildExcelBuffer() {
  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet('Refacturaciones');
  ws.addRow(['CONCEPTO', 'IMPORTE', 'BANCO', 'FOLIOS']);
  ws.addRow(['PAGO AUT12345 CLIENTE', 300, 'BBVA', 'A0-789']);
  return wb.xlsx.writeBuffer();
}

beforeEach(() => {
  jest.clearAllMocks();
});

describe('procesarRefacturacionesCyc — Tier 1 AUTO (smoke)', () => {
  test('vincula por token de concepto y el $set incluye primeraIdentificacionAt/Por cuando newStatus=identificado', async () => {
    ErpCuentaPendiente.find.mockReturnValue({
      lean: jest.fn().mockResolvedValue([{
        erpId: 'CXC-3', serieExterna: 'A0', folioExterno: '789',
        saldoActual: 300, total: 300, folioFiscal: null, serie: 'A0', tipoPago: null,
        movimientos: [],
      }]),
    });

    BankMovement.find.mockReturnValue({
      lean: jest.fn().mockResolvedValue([{
        _id: 'mov-3', numeroAutorizacion: '12345', referenciaNumerica: null,
        concepto: 'DEPOSITO', deposito: 300, banco: 'BBVA', status: 'no_identificado',
        erpIds: [], erpLinks: [], identificadoPor: [],
      }]),
    });

    BankMovement.bulkWrite.mockResolvedValue({ modifiedCount: 1 });

    const buffer = await buildExcelBuffer();
    const resultado = await procesarRefacturacionesCyc(buffer, 'auth0|user-1', 'Ana');

    expect(resultado.auto).toBe(1);
    expect(BankMovement.bulkWrite).toHaveBeenCalledTimes(1);

    const ops = BankMovement.bulkWrite.mock.calls[0][0];
    const { update } = ops[0].updateOne;

    expect(update.$set.status).toBe('identificado');
    expect(update.$set.erpIds).toEqual(['CXC-3']);
    expect(update.$set.primeraIdentificacionAt).toBeInstanceOf(Date);
    expect(update.$set.primeraIdentificacionPor).toEqual({ userId: 'auth0|user-1', nombre: 'Ana' });
  });
});
