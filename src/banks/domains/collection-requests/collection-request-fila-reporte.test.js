'use strict';

// collection-request-fila-reporte.test.js — _filaReporte() (reporte Excel):
// función pura, sin I/O, mismo patrón que _buildBusquedaFilter (ya re-expuesta
// para pruebas). multi-bank-movement (D7): 1 movimiento -> celda numérica sin
// cambios; 2+ -> valores concatenados con "; ".
//
// Requerir todo el service.js implica cargar sus dependencias reales (Mongoose
// models, koreCaja, etc.) — jest usa require real de Node, así que basta con
// que esos módulos NO fallen al cargarse (no se llama nada de ellos en este
// test, _filaReporte es pura).
const { _filaReporte } = require('./collection-request.service');

function cr(overrides) {
  return {
    solicitudIdErp: 'SOL-1',
    createdAt: new Date('2026-08-01T00:00:00Z'),
    status: 'identificada',
    monto: 100000,
    cxcs: [{ erpId: 'CXC-1', serie: 'D0', folioExterno: '123' }],
    formasPago: [{ formaPagoDescripcion: 'Transferencia', bancoDescripcion: 'BBVA', referencia: 'F-1' }],
    resueltoAt: new Date('2026-08-02T00:00:00Z'),
    solicitanteNombre: 'Tienda 1',
    resueltoPorNombre: 'Ana',
    cobroAplicado: true,
    cobroAplicadoAt: new Date('2026-08-02T00:00:00Z'),
    ...overrides,
  };
}

function mov(id, extra = {}) {
  return { _id: id, banco: 'BBVA', fecha: new Date('2026-08-01T00:00:00Z'), concepto: 'Depósito', deposito: 1000, retiro: 0, numeroAutorizacion: `AUT-${id}`, ...extra };
}

describe('_filaReporte — reporte rico (rico=true)', () => {
  test('1 movimiento (vía formasPago[].bankMovementId post-backfill): celda numérica, sin cambios', () => {
    const doc = cr({ formasPago: [{ formaPagoDescripcion: 'Transferencia', bancoDescripcion: 'BBVA', referencia: 'F-1', bankMovementId: mov('m1', { deposito: 5000, retiro: 250 }) }] });

    const fila = _filaReporte(doc, true);

    expect(fila.banco).toBe('BBVA');
    expect(fila.deposito).toBe(5000); // NUMÉRICO, no string
    expect(fila.retiro).toBe(250);
    expect(fila.autorizacionBancaria).toBe('AUT-m1');
  });

  test('1 movimiento vía el campo raíz deprecado bankMovementId (pre-backfill): idéntico a antes', () => {
    const doc = cr({ bankMovementId: mov('legacy', { deposito: 8000 }), formasPago: [{ formaPagoDescripcion: 'Transferencia', bancoDescripcion: 'BBVA', referencia: 'F-1' }] });

    const fila = _filaReporte(doc, true);

    expect(fila.deposito).toBe(8000);
    expect(fila.banco).toBe('BBVA');
  });

  test('2 movimientos DISTINTOS: banco/fecha/concepto/depósito/autorización concatenan con "; ", 1 sola fila', () => {
    const doc = cr({
      formasPago: [
        { formaPagoDescripcion: 'Transferencia', bancoDescripcion: 'BBVA', referencia: 'F-1', bankMovementId: mov('m1', { banco: 'BBVA', deposito: 60000, numeroAutorizacion: 'AUT-1' }) },
        { formaPagoDescripcion: 'Efectivo', bancoDescripcion: null, referencia: 'F-2', bankMovementId: mov('m2', { banco: 'Santander', deposito: 40000, numeroAutorizacion: 'AUT-2' }) },
      ],
    });

    const fila = _filaReporte(doc, true);

    expect(fila.banco).toBe('BBVA; Santander');
    expect(fila.deposito).toBe('60000; 40000'); // 2+ -> STRING concatenado (D7), no numérico
    expect(fila.autorizacionBancaria).toBe('AUT-1; AUT-2');
    // La fila sigue siendo UNA sola (el llamador solo invoca _filaReporte una
    // vez por solicitud) — no hay una segunda fila para el segundo movimiento.
  });

  test('sin ningún movimiento vinculado (solicitud rechazada, nunca identificada): celdas vacías', () => {
    const doc = cr({ status: 'rechazada', formasPago: [{ formaPagoDescripcion: 'Transferencia', referencia: null }] });

    const fila = _filaReporte(doc, true);

    expect(fila.banco).toBe('');
    expect(fila.deposito).toBe('');
  });
});
