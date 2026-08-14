'use strict';

// bank.service.importarConciliacion.test.js — importarConciliacion(): el 2do argumento de
// findOneAndUpdate se convirtió de un $set plano a un pipeline update (array) para agregar
// primeraIdentificacionAt/primeraIdentificacionPor de forma inmutable (indicador de tiempo de
// identificación, mismo criterio que resolvePrimeraIdentificacion() pero expresado en Mongo
// porque no hay doc en memoria en este call-site). Se usa ExcelJS real (no mockeado) para
// construir el buffer de entrada — solo se mockea BankMovement.model, mismo patrón que
// bank.service.setErpIds.test.js.
jest.mock('./BankMovement.model');

const ExcelJS      = require('exceljs');
const BankMovement = require('./BankMovement.model');
const bankService  = require('./bank.service');

async function buildBuffer(rows) {
  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet('Conciliacion');
  ws.addRow(['fecha_deposito', 'banco', 'monto_deposito']);
  for (const r of rows) {
    const row = ws.addRow([r.fecha, r.banco, r.monto]);
    row.getCell(1).numFmt = 'yyyy-mm-dd'; // fuerza que ExcelJS lo serialice/lea como fecha
  }
  return wb.xlsx.writeBuffer();
}

describe('importarConciliacion — primeraIdentificacionAt/primeraIdentificacionPor (pipeline update)', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('2do argumento de findOneAndUpdate es un pipeline (array) con $ifNull inmutable, no un $set plano', async () => {
    const buffer = await buildBuffer([{ fecha: new Date('2026-08-01'), banco: 'BBVA', monto: 100.5 }]);
    BankMovement.findOneAndUpdate.mockResolvedValue({ _id: 'mov-1' });

    const user = { _id: 'user-1', nombre: 'Usuario Uno' };
    const resultado = await bankService.importarConciliacion(buffer, user);

    expect(resultado.identificados).toBe(1);
    expect(BankMovement.findOneAndUpdate).toHaveBeenCalledTimes(1);

    const [filter, update] = BankMovement.findOneAndUpdate.mock.calls[0];
    expect(filter.status).toBe('no_identificado');

    // El 2do argumento debe ser un ARRAY de pipeline, no un objeto { $set, $push } plano
    // ($push no es válido dentro de un pipeline update — se reemplazó por $concatArrays).
    expect(Array.isArray(update)).toBe(true);
    const setStage = update[0].$set;
    expect(setStage.status).toBe('identificado');
    expect(setStage.identificadoPor.$concatArrays).toBeDefined();
    expect(setStage.primeraIdentificacionAt).toEqual({
      $ifNull: ['$primeraIdentificacionAt', expect.any(Date)],
    });
    expect(setStage.primeraIdentificacionPor).toEqual({
      $ifNull: ['$primeraIdentificacionPor', { userId: 'user-1', nombre: 'Usuario Uno' }],
    });
  });

  test('fila sin match (findOneAndUpdate devuelve null): no identifica, se reporta como fallido', async () => {
    const buffer = await buildBuffer([{ fecha: new Date('2026-08-01'), banco: 'BBVA', monto: 100.5 }]);
    BankMovement.findOneAndUpdate.mockResolvedValue(null);

    const resultado = await bankService.importarConciliacion(buffer, { _id: 'user-1', nombre: 'Usuario Uno' });

    expect(resultado.identificados).toBe(0);
    expect(resultado.fallidos).toBe(1);
  });
});
