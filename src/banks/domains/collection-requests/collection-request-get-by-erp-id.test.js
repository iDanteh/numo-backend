'use strict';

// collection-request-get-by-erp-id.test.js — getByErpId(): multi-bank-movement
// (spec: "getByErpId() backward-compatible multi-movement contract"). Mockea
// CollectionRequest.model (única dependencia de I/O de esta función) — mismo
// patrón que collection-request.identificar.test.js.
jest.mock('./CollectionRequest.model');

const CollectionRequest = require('./CollectionRequest.model');
const { getByErpId } = require('./collection-request.service');

function mockQuery(resolvedValue) {
  const query = {
    select: jest.fn().mockReturnThis(),
    populate: jest.fn().mockReturnThis(),
    lean: jest.fn().mockResolvedValue(resolvedValue),
  };
  return query;
}

describe('getByErpId() — multi-bank-movement', () => {
  test('campos agregados EXISTENTES quedan byte-idénticos (compatibilidad Kore)', async () => {
    const doc = {
      solicitudIdErp: 'SOL-1', status: 'identificada', motivoRechazo: null, resueltoAt: new Date('2026-08-01'),
      monto: 100000, cobroAplicado: true, cobroAplicadoAt: new Date('2026-08-01'),
      cxcs: [{ erpId: 'CXC-1', serie: 'D0', folioExterno: '1', folioFiscal: null, montoAsignado: null }],
      bankMovementId: { folio: 'F-1', fecha: new Date('2026-08-01'), deposito: 100000 },
      formasPago: [{ formaPagoDescripcion: 'Transferencia', importe: 100000, bankMovementId: { _id: 'm1', folio: 'F-1', fecha: new Date('2026-08-01'), deposito: 100000 } }],
    };
    CollectionRequest.findOne.mockReturnValue(mockQuery(doc));

    const resultado = await getByErpId('SOL-1');

    expect(resultado.bankMovement).toEqual({ folio: 'F-1', fecha: doc.bankMovementId.fecha, deposito: 100000 });
    expect(resultado.status).toBe('identificada');
  });

  test('2 movimientos DISTINTOS: bankMovements[] trae una entrada por movimiento con SUS formasPago', async () => {
    const doc = {
      solicitudIdErp: 'SOL-2', status: 'identificada', motivoRechazo: null, resueltoAt: new Date('2026-08-01'),
      monto: 100000, cobroAplicado: true, cobroAplicadoAt: new Date('2026-08-01'),
      cxcs: [{ erpId: 'CXC-1' }],
      bankMovementId: { folio: 'F-A', fecha: new Date('2026-08-01'), deposito: 60000 },
      formasPago: [
        { formaPagoDescripcion: 'Transferencia', importe: 60000, bankMovementId: { _id: 'mov-A', folio: 'F-A', fecha: new Date('2026-08-01'), deposito: 60000 } },
        { formaPagoDescripcion: 'Efectivo', importe: 40000, bankMovementId: { _id: 'mov-B', folio: 'F-B', fecha: new Date('2026-08-02'), deposito: 40000 } },
      ],
    };
    CollectionRequest.findOne.mockReturnValue(mockQuery(doc));

    const resultado = await getByErpId('SOL-2');

    expect(resultado.bankMovements).toHaveLength(2);
    expect(resultado.bankMovements[0]).toEqual({
      folio: 'F-A', fecha: doc.formasPago[0].bankMovementId.fecha, deposito: 60000,
      formasPago: [{ formaPagoDescripcion: 'Transferencia', importe: 60000 }],
    });
    expect(resultado.bankMovements[1].folio).toBe('F-B');
    expect(resultado.bankMovements[1].formasPago).toEqual([{ formaPagoDescripcion: 'Efectivo', importe: 40000 }]);
  });

  test('mismo movimiento referenciado por 2 formasPago: UNA sola entrada en bankMovements[], con AMBAS formasPago', async () => {
    const movCompartido = { _id: 'mov-X', folio: 'F-X', fecha: new Date('2026-08-01'), deposito: 100000 };
    const doc = {
      solicitudIdErp: 'SOL-3', status: 'identificada', motivoRechazo: null, resueltoAt: new Date('2026-08-01'),
      monto: 100000, cobroAplicado: true, cobroAplicadoAt: new Date('2026-08-01'),
      cxcs: [{ erpId: 'CXC-1' }],
      bankMovementId: movCompartido,
      formasPago: [
        { formaPagoDescripcion: 'Transferencia', importe: 60000, bankMovementId: movCompartido },
        { formaPagoDescripcion: 'Efectivo', importe: 40000, bankMovementId: movCompartido },
      ],
    };
    CollectionRequest.findOne.mockReturnValue(mockQuery(doc));

    const resultado = await getByErpId('SOL-3');

    expect(resultado.bankMovements).toHaveLength(1);
    expect(resultado.bankMovements[0].formasPago).toHaveLength(2);
  });
});
