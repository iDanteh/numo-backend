'use strict';

// bank-autorizaciones.service.matchDesdeErp.test.js — test de humo para
// matchAutorizacionesDesdeErp() (Fase A, auth explícita en formasPago). Cubre
// únicamente el punto agregado en esta tarea: el $set del updateOne (pushGroupOp)
// debe incluir primeraIdentificacionAt/primeraIdentificacionPor.
jest.mock('./BankMovement.model');
jest.mock('../erp/ErpCuentaPendiente.model');

const BankMovement       = require('./BankMovement.model');
const ErpCuentaPendiente = require('../erp/ErpCuentaPendiente.model');
const { matchAutorizacionesDesdeErp } = require('./bank-autorizaciones.service');

function fakeFindQuery(result) {
  const q = {};
  q.select = jest.fn(() => q);
  q.lean   = jest.fn().mockResolvedValue(result);
  return q;
}

beforeEach(() => {
  jest.clearAllMocks();
});

describe('matchAutorizacionesDesdeErp — Fase A (auth explícita en formasPago) — smoke', () => {
  test('vincula por autorización + importe y el $set incluye primeraIdentificacionAt/Por', async () => {
    const mov = {
      _id: 'mov-9', numeroAutorizacion: '55555', referenciaNumerica: null,
      concepto: 'DEPOSITO SPEI', deposito: 1000, banco: 'BBVA', fecha: null,
      erpIds: [], erpLinks: [],
    };
    BankMovement.find.mockReturnValue(fakeFindQuery([mov]));
    BankMovement.bulkWrite.mockResolvedValue({ modifiedCount: 1 });

    ErpCuentaPendiente.aggregate.mockResolvedValue([{
      erpId: 'CXC-9', total: 1000, folioFiscal: null, serie: 'A0', folioExterno: '999',
      tipoPago: null, fechaRealPago: null, fechaAfectacion: null, tieneRetencion: false,
      movimientos: [{
        serie: 'ABO', total: -1000,
        formasPago: [{ autorizacion: '55555', monto: 1000 }],
      }],
    }]);

    const resultado = await matchAutorizacionesDesdeErp({});

    expect(resultado.matcheados).toBe(1);
    expect(resultado.identificados).toBe(1);
    expect(BankMovement.bulkWrite).toHaveBeenCalledTimes(1);

    const ops = BankMovement.bulkWrite.mock.calls[0][0];
    expect(ops).toHaveLength(1);
    const { update } = ops[0].updateOne;

    expect(update.$set.status).toBe('identificado');
    expect(update.$set.erpIds).toEqual(['CXC-9']);
    expect(update.$set.primeraIdentificacionAt).toBeInstanceOf(Date);
    expect(update.$set.primeraIdentificacionPor).toEqual({ userId: null, nombre: null });
  });
});
