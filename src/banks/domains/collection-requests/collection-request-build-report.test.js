'use strict';

// collection-request-build-report.test.js — buildReport(): regresión CRÍTICA
// hallada en sdd-verify (2026-08-06, hallazgo independiente de apply-progress):
// buildReport() era el ÚNICO de los 5 sitios que popula movimientos bancarios
// al que le faltaba `.populate('formasPago.bankMovementId', ...)`. _filaReporte
// (cr, true) usa movimientosDe(cr) (collection-request-asignaciones.js), que
// lee formasPago[].bankMovementId PRIMERO y solo cae al campo raíz
// bankMovementId cuando ese arreglo viene TODO vacío — como identificar()
// (PR2) SIEMPRE llena formasPago[].bankMovementId para cada forma en toda
// identificación nueva, ese fallback nunca se activa para solicitudes
// identificadas de ahora en adelante. Sin el populate faltante, esos
// ObjectId quedan sin poblar -> celdas vacías en el Excel para banco/fecha/
// concepto/depósito/retiro/autorización, en TODA solicitud (N=1 y N=2+ por
// igual, no solo multi-movimiento).
//
// A diferencia de collection-request-fila-reporte.test.js (que construye
// formasPago[].bankMovementId YA poblado a mano en el fixture, sin pasar
// nunca por el query real de Mongoose), este test ejercita el query real:
// el mock de CollectionRequest.find() solo "puebla" (enriquece) un campo si
// `.populate(path, ...)` fue realmente invocado con ESE path exacto —
// exactamente como Mongoose se comporta. Si buildReport() no llama
// `.populate('formasPago.bankMovementId', ...)`, este mock deja
// formasPago[].bankMovementId como el ObjectId crudo (string, sin .banco/
// .fecha/etc.), reproduciendo el bug real — no una simulación optimista.
jest.mock('./CollectionRequest.model');

const ExcelJS           = require('exceljs');
const CollectionRequest = require('./CollectionRequest.model');
const { buildReport }   = require('./collection-request.service');

// Simula el comportamiento REAL de Mongoose .populate(): un campo solo pasa
// de ObjectId crudo a documento completo si el query llamó a .populate() con
// ESE path exacto — nunca por accidente ni por venir de otro path.
function mockPopulatingQuery(docs, movimientosPorId) {
  const populatedPaths = new Set();
  const query = {
    sort: jest.fn().mockReturnThis(),
    select: jest.fn().mockReturnThis(),
    populate: jest.fn((path) => { populatedPaths.add(path); return query; }),
    lean: jest.fn(() => Promise.resolve(docs.map(doc => _aplicarPopulate(doc, populatedPaths, movimientosPorId)))),
  };
  return query;
}

function _aplicarPopulate(doc, populatedPaths, movimientosPorId) {
  const clon = { ...doc, formasPago: (doc.formasPago || []).map(f => ({ ...f })) };
  if (populatedPaths.has('bankMovementId') && clon.bankMovementId != null) {
    clon.bankMovementId = movimientosPorId[clon.bankMovementId] ?? clon.bankMovementId;
  }
  if (populatedPaths.has('formasPago.bankMovementId')) {
    clon.formasPago = clon.formasPago.map(f => (
      f.bankMovementId != null ? { ...f, bankMovementId: movimientosPorId[f.bankMovementId] ?? f.bankMovementId } : f
    ));
  }
  return clon;
}

function mov(id, extra = {}) {
  return { _id: id, banco: 'BBVA', fecha: new Date('2026-08-01T00:00:00Z'), concepto: 'Depósito', deposito: 60000, retiro: 0, numeroAutorizacion: `AUT-${id}`, ...extra };
}

function docBase(overrides = {}) {
  return {
    _id: 'cr-1', solicitudIdErp: 'SOL-1', createdAt: new Date('2026-08-01T00:00:00Z'),
    status: 'identificada', monto: 100000,
    cxcs: [{ erpId: 'CXC-1', serie: 'D0', folioExterno: '1' }],
    // Campo raíz deprecado: identificar() (PR2) SIEMPRE llena
    // formasPago[].bankMovementId, así que este queda null para toda
    // solicitud identificada de ahora en adelante — movimientosDe() nunca
    // cae al fallback en este escenario.
    bankMovementId: null,
    resueltoAt: new Date('2026-08-02T00:00:00Z'), solicitanteNombre: 'Tienda 1', resueltoPorNombre: 'Ana',
    cobroAplicado: true, cobroAplicadoAt: new Date('2026-08-02T00:00:00Z'), motivoRechazo: null,
    ...overrides,
  };
}

async function leerCeldaReporte(buffer, header) {
  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load(buffer);
  const ws = wb.getWorksheet('Autorizadas');
  const headerRow = ws.getRow(1);
  let col = null;
  headerRow.eachCell((cell, colNumber) => { if (cell.value === header) col = colNumber; });
  if (col == null) throw new Error(`Columna "${header}" no encontrada en el reporte`);
  return ws.getRow(2).getCell(col).value;
}

describe('buildReport() — populate real de formasPago.bankMovementId (regresión CRÍTICA de sdd-verify)', () => {
  test('con 2 movimientos DISTINTOS vinculados vía formasPago[], el Excel trae banco/depósito/autorización REALES, no celdas vacías', async () => {
    const doc = docBase({
      formasPago: [
        { formaPagoDescripcion: 'Transferencia', bancoDescripcion: 'BBVA', referencia: 'F-1', importe: 60000, bankMovementId: 'mov-A' },
        { formaPagoDescripcion: 'Efectivo',      bancoDescripcion: null,   referencia: 'F-2', importe: 40000, bankMovementId: 'mov-B' },
      ],
    });
    const movimientosPorId = {
      'mov-A': mov('mov-A', { banco: 'BBVA', deposito: 60000, numeroAutorizacion: 'AUT-1' }),
      'mov-B': mov('mov-B', { banco: 'Santander', deposito: 40000, numeroAutorizacion: 'AUT-2' }),
    };
    CollectionRequest.find.mockReturnValue(mockPopulatingQuery([doc], movimientosPorId));

    const buffer = await buildReport({});

    expect(await leerCeldaReporte(buffer, 'Banco')).toBe('BBVA; Santander');
    expect(await leerCeldaReporte(buffer, 'Depósito')).toBe('60000; 40000');
    expect(await leerCeldaReporte(buffer, 'Autorización bancaria')).toBe('AUT-1; AUT-2');
  });

  test('con 1 solo movimiento vinculado vía formasPago[] (caso N=1, también afectado por el bug), el Excel trae el banco/depósito reales', async () => {
    const doc = docBase({
      formasPago: [{ formaPagoDescripcion: 'Transferencia', bancoDescripcion: 'BBVA', referencia: 'F-1', importe: 100000, bankMovementId: 'mov-solo' }],
    });
    const movimientosPorId = { 'mov-solo': mov('mov-solo', { banco: 'Banorte', deposito: 100000, numeroAutorizacion: 'AUT-X' }) };
    CollectionRequest.find.mockReturnValue(mockPopulatingQuery([doc], movimientosPorId));

    const buffer = await buildReport({});

    expect(await leerCeldaReporte(buffer, 'Banco')).toBe('Banorte');
    expect(await leerCeldaReporte(buffer, 'Depósito')).toBe(100000);
    expect(await leerCeldaReporte(buffer, 'Autorización bancaria')).toBe('AUT-X');
  });

  test('guardia explícita: el query de buildReport() llama .populate() con "formasPago.bankMovementId" (evita que esta regresión exacta vuelva a pasar inadvertida)', async () => {
    const doc = docBase({ formasPago: [{ formaPagoDescripcion: 'Transferencia', bancoDescripcion: 'BBVA', referencia: 'F-1', importe: 100000, bankMovementId: 'mov-solo' }] });
    CollectionRequest.find.mockReturnValue(mockPopulatingQuery([doc], {}));

    await buildReport({});

    const query = CollectionRequest.find.mock.results[0].value;
    const pathsPopulados = query.populate.mock.calls.map(call => call[0]);
    expect(pathsPopulados).toContain('formasPago.bankMovementId');
  });
});
