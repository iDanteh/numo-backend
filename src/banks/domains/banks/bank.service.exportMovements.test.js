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
  // FIX 2026-09-11 (bug real reportado por el usuario): antes estos límites eran UTC
  // puro (`new Date('2026-09-01')` = medianoche UTC, `T23:59:59.999Z` = fin de día
  // UTC) — un movimiento importado entre las 18:00 y 23:59 hora de México (que ya
  // cae en las 00:00-05:59 UTC del día calendario SIGUIENTE) quedaba contado en el
  // día equivocado. Ahora usan _inicioDiaMx/_finDiaMx (medianoche/fin de día de
  // México, offset fijo UTC-6, mismo criterio que _medianocheMx en
  // collection-request-indicadores.service.js): inicio = "T06:00:00.000Z" del día
  // pedido, fin = "T05:59:59.999Z" del día SIGUIENTE.
  test('con inicio Y fin: filter.createdAt.$gte/$lte quedan en medianoche/fin de día de México (no UTC puro)', async () => {
    await exportMovements({ fechaImportacionInicio: '2026-09-01', fechaImportacionFin: '2026-09-05' });

    const filtroUsado = BankMovement.find.mock.calls[0][0];
    expect(filtroUsado.createdAt).toEqual({
      $gte: new Date('2026-09-01T06:00:00.000Z'),
      $lte: new Date('2026-09-06T05:59:59.999Z'),
    });
  });

  test('solo fechaImportacionInicio: únicamente $gte queda en el filtro (medianoche MX, sin $lte)', async () => {
    await exportMovements({ fechaImportacionInicio: '2026-09-01' });

    const filtroUsado = BankMovement.find.mock.calls[0][0];
    expect(filtroUsado.createdAt).toEqual({ $gte: new Date('2026-09-01T06:00:00.000Z') });
  });

  test('solo fechaImportacionFin: únicamente $lte queda en el filtro (fin de día MX, sin $gte)', async () => {
    await exportMovements({ fechaImportacionFin: '2026-09-05' });

    const filtroUsado = BankMovement.find.mock.calls[0][0];
    expect(filtroUsado.createdAt).toEqual({ $lte: new Date('2026-09-06T05:59:59.999Z') });
  });

  test('ambos como string vacío (\'\'): NO se agrega la llave createdAt al filtro (comportamiento real: \'\' es falsy, el bloque completo se salta)', async () => {
    await exportMovements({ fechaImportacionInicio: '', fechaImportacionFin: '' });

    const filtroUsado = BankMovement.find.mock.calls[0][0];
    expect(filtroUsado.createdAt).toBeUndefined();
  });

  test('REGRESIÓN bug real: movimiento importado a las 19:00 hora MX (2026-09-10) — createdAt UTC ya es 2026-09-11T01:00:00.000Z — cae en el día MX 10, NO en el 11', async () => {
    // Con el bug viejo (UTC puro), un filtro de "10-sep" ($lte=2026-09-10T23:59:59.999Z)
    // NO incluía este createdAt (2026-09-11T01:00:00.000Z queda fuera) — el movimiento
    // solo aparecía si el usuario filtraba "11-sep", día calendario equivocado en MX.
    const filtroDia10 = { fechaImportacionInicio: '2026-09-10', fechaImportacionFin: '2026-09-10' };
    await exportMovements(filtroDia10);
    const { $gte, $lte } = BankMovement.find.mock.calls[0][0].createdAt;

    const createdAtReal = new Date('2026-09-11T01:00:00.000Z'); // 19:00 hora MX del 10-sep
    expect(createdAtReal.getTime()).toBeGreaterThanOrEqual($gte.getTime());
    expect(createdAtReal.getTime()).toBeLessThanOrEqual($lte.getTime());

    // Y el día MX 11 (filtro siguiente) YA NO lo incluye — el rango no se solapa.
    await exportMovements({ fechaImportacionInicio: '2026-09-11', fechaImportacionFin: '2026-09-11' });
    const { $gte: gte11 } = BankMovement.find.mock.calls[1][0].createdAt;
    expect(createdAtReal.getTime()).toBeLessThan(gte11.getTime());
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
