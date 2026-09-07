'use strict';

// bank.service.exportMovements.test.js — cobertura MÍNIMA para el filtro nuevo
// "Fecha de importación" (fechaImportacionInicio/fechaImportacionFin sobre
// BankMovement.createdAt) y su columna opcional 'fechaImportacion' (fecha+hora
// convertida a hora de México, America/Mexico_City), agregados a
// exportMovements(). exportMovements() en sí NO tenía
// cobertura previa en todo el proyecto — este archivo cubre solo lo nuevo, no
// la función completa (desproporcionado para el alcance pedido).
//
// Mismo patrón que bank.service.setErpIds.test.js / bank-autorizaciones.service.
// ejecutarMatch.test.js: se mockea solo BankMovement.model (automock vía
// jest.mock), se replica la cadena real .find(filter).sort(...).lean() con un
// query fake chainable. Para leer la columna nueva del Excel se usa exceljs
// REAL (no mockeado) sobre el buffer devuelto, igual que collection-request-
// build-report.test.js.
jest.mock('./BankMovement.model');

const ExcelJS      = require('exceljs');
const BankMovement = require('./BankMovement.model');
const { exportMovements } = require('./bank.service');

function fakeFindQuery(movimientos) {
  const q = {};
  q.sort = jest.fn(() => q);
  q.lean = jest.fn().mockResolvedValue(movimientos);
  return q;
}

function fakeMov(overrides = {}) {
  return {
    _id: 'mov-1', banco: 'BBVA', folio: '00123', concepto: 'Depósito',
    fecha: new Date('2026-09-01T00:00:00.000Z'),
    deposito: 1000, retiro: null, status: 'identificado', categoria: null,
    erpLinks: [], saldoErp: null, identificadoPor: [], fichaAt: null,
    fichaNombre: null, fichaBy: null, ficha: null, numeroAutorizacion: null,
    createdAt: new Date('2026-09-01T00:00:00.000Z'),
    ...overrides,
  };
}

beforeEach(() => {
  jest.clearAllMocks();
  BankMovement.find.mockReturnValue(fakeFindQuery([fakeMov()]));
});

describe('exportMovements — filtro "Fecha de importación" (fechaImportacionInicio/Fin -> createdAt)', () => {
  test('con inicio Y fin: filter.createdAt.$gte/$lte quedan seteados (inicio = medianoche UTC, fin = 23:59:59.999Z)', async () => {
    await exportMovements({ fechaImportacionInicio: '2026-09-01', fechaImportacionFin: '2026-09-05' });

    const filtroUsado = BankMovement.find.mock.calls[0][0];
    expect(filtroUsado.createdAt).toEqual({
      $gte: new Date('2026-09-01T00:00:00.000Z'),
      $lte: new Date('2026-09-05T23:59:59.999Z'),
    });
  });

  test('solo fechaImportacionInicio: únicamente $gte queda en el filtro (sin $lte)', async () => {
    await exportMovements({ fechaImportacionInicio: '2026-09-01' });

    const filtroUsado = BankMovement.find.mock.calls[0][0];
    expect(filtroUsado.createdAt).toEqual({ $gte: new Date('2026-09-01T00:00:00.000Z') });
  });

  test('solo fechaImportacionFin: únicamente $lte queda en el filtro (sin $gte)', async () => {
    await exportMovements({ fechaImportacionFin: '2026-09-05' });

    const filtroUsado = BankMovement.find.mock.calls[0][0];
    expect(filtroUsado.createdAt).toEqual({ $lte: new Date('2026-09-05T23:59:59.999Z') });
  });

  test('ambos como string vacío (\'\'): NO se agrega la llave createdAt al filtro (comportamiento real: \'\' es falsy, el bloque completo se salta)', async () => {
    await exportMovements({ fechaImportacionInicio: '', fechaImportacionFin: '' });

    const filtroUsado = BankMovement.find.mock.calls[0][0];
    expect(filtroUsado.createdAt).toBeUndefined();
  });
});

describe('exportMovements — columna opcional "fechaImportacion" (fecha+hora de México, America/Mexico_City, de createdAt)', () => {
  test('con columnas incluyendo "fechaImportacion": la columna existe, es la ÚLTIMA, y su celda trae fecha+hora de México (no UTC crudo)', async () => {
    BankMovement.find.mockReturnValue(fakeFindQuery([
      // 14:35 UTC = 08:35 hora de México (America/Mexico_City, UTC-6 fijo).
      fakeMov({ createdAt: new Date('2026-09-01T14:35:00.000Z') }),
    ]));

    const buffer = await exportMovements({ columnas: 'fechaImportacion' });

    const wb = new ExcelJS.Workbook();
    await wb.xlsx.load(buffer);
    const ws = wb.getWorksheet('Movimientos');

    const headerRow = ws.getRow(1);
    const totalCols = headerRow.actualCellCount;
    const ultimaCelda = headerRow.getCell(totalCols);
    expect(ultimaCelda.value).toBe('Fecha de importación');

    const valorCelda = ws.getRow(2).getCell(totalCols).value;
    expect(valorCelda).toBe('01/09/2026 08:35');
    expect(valorCelda).toMatch(/\d{2}\/\d{2}\/\d{4}/);   // parte de fecha
    expect(valorCelda).toMatch(/\d{2}:\d{2}/);            // parte de hora
  });
});
