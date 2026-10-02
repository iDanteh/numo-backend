'use strict';

// bank.parser.test.js — primer test de este parser. Cobertura puntual del guard
// agregado el 2026-10-02 para "FUNDACION BBVA MEXIC... / FBB######..." (débito
// periódico del programa de redondeo/donación de BBVA). Caso real: se cargó
// "plantilla-bancos (93).xlsx" (51 BBVA + 3 Banamex + 4 Santander) y el sistema
// reportó "0 movimientos importados · 58 ya existían" — un retiro real de BBVA de
// $4000 (01-oct-2026) nunca se insertó porque ya existía un cargo de $4000 con el
// MISMO token "FBB921214" en junio (Capa 1b del dedup, bank.service.js, deduplica
// por banco+numeroAutorizacion+monto SIN ventana de fecha). Confirmado contra datos
// reales: el token se repite idéntico en cargos de marzo y 3 cargos distintos del
// mismo día en junio, con montos y saldos distintos cada vez — no es un
// identificador de transacción, es un código de programa fijo.
const ExcelJS = require('exceljs');
const {
  parseBankFile, TEMPLATE_SIGNATURE_SHEET, TEMPLATE_SIGNATURE_VALUE,
} = require('./bank.parser');

async function bufferConFilasBBVA(filas) {
  const wb = new ExcelJS.Workbook();
  // Modo individual (banco explícito): parseBankFile toma SIEMPRE worksheets[0] —
  // la hoja de datos debe ir primero, la de firma después (igual que el archivo real).
  const ws = wb.addWorksheet('BBVA');
  for (const [fecha, concepto, cargo, abono, saldo] of filas) {
    ws.addRow([fecha, concepto, cargo, abono, saldo]);
  }

  const sig = wb.addWorksheet(TEMPLATE_SIGNATURE_SHEET);
  sig.getCell('A1').value = TEMPLATE_SIGNATURE_VALUE;

  return wb.xlsx.writeBuffer();
}

describe('parseBankFile — BBVA', () => {
  test('FUNDACION BBVA MEXIC / FBB###### (débito periódico): numeroAutorizacion sale null', async () => {
    const buffer = await bufferConFilasBBVA([
      [
        new Date('2026-10-01T00:00:00.000Z'),
        'FUNDACION BBVA MEXIC08323 / FBB921214 9F3 0000034984CCO011113663',
        4000, null, 2425985.38,
      ],
    ]);
    const { movements } = await parseBankFile(buffer, 'BBVA');

    expect(movements).toHaveLength(1);
    expect(movements[0].numeroAutorizacion).toBeNull();
    expect(movements[0].retiro).toBe(4000);
  });

  test('control: una transferencia SPEI normal con número de autorización real NO se ve afectada', async () => {
    const buffer = await bufferConFilasBBVA([
      [
        new Date('2026-10-01T00:00:00.000Z'),
        'SPEI RECIBIDOBANAMEX / 1234567 002 xyz',
        null, 1500, 500000,
      ],
    ]);
    const { movements } = await parseBankFile(buffer, 'BBVA');

    expect(movements).toHaveLength(1);
    expect(movements[0].numeroAutorizacion).toBe('1234567');
  });
});
