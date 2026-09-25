'use strict';

// netpay-reporte-parser.service.test.js — parsea el Excel REAL de Netpay (2 hojas). Se
// verifica contra los 2 archivos reales que están en la raíz del repo (no fixtures
// sintéticos inventados) — la estructura exacta de columnas fue confirmada por el usuario
// contra estos mismos archivos antes de escribir el parser. Los casos de error (hoja/columna
// faltante, más de un depósito) sí usan workbooks sintéticos armados con ExcelJS, para no
// depender de mutar los archivos reales del repo.

const fs = require('fs');
const path = require('path');
const ExcelJS = require('exceljs');
const {
  parseNetpayReporte, _normalizarHeader, _parseFechaDDMMYYYY, _parseFechaMesEs, _extraerTerminalID,
} = require('./netpay-reporte-parser.service');

const REPO_ROOT = path.join(__dirname, '../../../../..');
const PATH_UN_FOLIO   = path.join(REPO_ROOT, 'F0-Netpay.xlsx');
const PATH_TRES_FOLIOS = path.join(REPO_ROOT, '20260925_DetalleDepósitos.xlsx');

describe('parseNetpayReporte — archivos reales del repo', () => {
  test('F0-Netpay.xlsx (1 folio): claveRastreo, montoDepositoTotal, resumenVentas y folio parseados correctamente', async () => {
    const buffer = fs.readFileSync(PATH_UN_FOLIO);
    const r = await parseNetpayReporte(buffer);

    expect(r.claveRastreo).toBe('42644264202609255810846530');
    expect(r.cuentaDeposito).toBe('012610001090310145');
    expect(r.fechaMovimiento).toEqual(new Date('2026-09-25T00:00:00.000Z'));
    expect(r.periodoDesde).toEqual(new Date('2026-09-25T00:00:00.000Z'));
    expect(r.periodoHasta).toEqual(new Date('2026-09-25T00:00:00.000Z'));
    expect(r.montoDepositoTotal).toBe(27189.68);
    expect(r.resumenVentas).toEqual({
      montoTransaccionado: 27428.22, comisiones: 205.62, iva: 32.92, montoDepositado: 27189.68,
    });

    expect(r.folios).toHaveLength(20);
    expect(r.folios[0]).toEqual({
      referencia: 'F20260924-00311',
      terminalID: '2840746396',
      storeId: '1650292',
      sucursal: 'AV FERROCARRIL 802',
      nombreEmpresa: 'CAR COMERCIALIZADORA',
      fechaTrx: new Date('2026-09-24T00:00:00.000Z'),
      horaTrx: '18:16',
      montoTrx: 822.87,
      comisionBasePct: 0.0075,
      comisionBaseMonto: 6.17,
      ivaComision: 0.99,
      comisionMasIva: 7.16,
      montoDeposito: 815.71,
      banco: 'SANTANDER',
      tipoTarjeta: 'Débito',
      codigoAutorizacion: '062788',
      orderId: '260924181626-2840746396783601',
    });
  });

  test('20260925_DetalleDepósitos.xlsx (3 folios): mismo depósito para las 3 filas, terminalID estable', async () => {
    const buffer = fs.readFileSync(PATH_TRES_FOLIOS);
    const r = await parseNetpayReporte(buffer);

    expect(r.claveRastreo).toBe('42644264202609235803800435');
    expect(r.montoDepositoTotal).toBe(14214.73);
    expect(r.folios).toHaveLength(3);
    // Las 3 filas son de la MISMA terminal/tienda — el terminalID (prefijo estable de
    // Order ID) debe ser idéntico en las 3, aunque el sufijo completo varíe por transacción.
    expect(new Set(r.folios.map(f => f.terminalID))).toEqual(new Set(['2840592988']));
    expect(r.folios.map(f => f.referencia)).toEqual(['F20260922-00055', 'F20260922-00282', 'F20260922-00090']);
  });
});

describe('parseNetpayReporte — edge cases (workbooks sintéticos)', () => {
  async function _bufferDesde(fn) {
    const wb = new ExcelJS.Workbook();
    fn(wb);
    return wb.xlsx.writeBuffer();
  }

  test('falta la hoja "Resumen": BadRequestError con mensaje legible (nunca 500)', async () => {
    const buffer = await _bufferDesde((wb) => {
      wb.addWorksheet('Ventas Tarjeta Presente');
    });
    await expect(parseNetpayReporte(buffer)).rejects.toThrow(/hoja "Resumen"/);
  });

  test('falta la hoja "Ventas Tarjeta Presente": BadRequestError', async () => {
    const buffer = await _bufferDesde((wb) => {
      wb.addWorksheet('Resumen');
    });
    await expect(parseNetpayReporte(buffer)).rejects.toThrow(/Ventas Tarjeta Presente/);
  });

  test('hoja "Resumen" sin la tabla de depósitos (columnas inesperadas): BadRequestError', async () => {
    const buffer = await _bufferDesde((wb) => {
      const s1 = wb.addWorksheet('Resumen');
      s1.addRow(['algo', 'que', 'no', 'es', 'la', 'tabla']);
      wb.addWorksheet('Ventas Tarjeta Presente');
    });
    await expect(parseNetpayReporte(buffer)).rejects.toThrow(/Depósitos y cargos del periodo/);
  });

  test('hoja "Ventas Tarjeta Presente" sin resumen de ventas ni tabla de folios: BadRequestError', async () => {
    const buffer = await _bufferDesde((wb) => {
      const s1 = wb.addWorksheet('Resumen');
      s1.getRow(1).values = [null, null, 'Fecha de Movimiento', 'Clave Rastreo', 'Cuenta Depósito', 'Descripción', 'Monto Depósito'];
      s1.getRow(2).values = [null, null, '25-09-2026', 'CLAVE-1', '012610001090310145', 'PAGO', 100];
      wb.addWorksheet('Ventas Tarjeta Presente'); // vacía a propósito
    });
    await expect(parseNetpayReporte(buffer)).rejects.toThrow(/Resumen de ventas/);
  });

  test('reporte con MÁS de un depósito en "Resumen": BadRequestError (no soportado)', async () => {
    const buffer = await _bufferDesde((wb) => {
      const s1 = wb.addWorksheet('Resumen');
      s1.getRow(1).values = [null, null, 'Fecha de Movimiento', 'Clave Rastreo', 'Cuenta Depósito', 'Descripción', 'Monto Depósito'];
      s1.getRow(2).values = [null, null, '25-09-2026', 'CLAVE-1', '012610001090310145', 'PAGO', 100];
      s1.getRow(3).values = [null, null, '26-09-2026', 'CLAVE-2', '012610001090310145', 'PAGO', 200];

      const s2 = wb.addWorksheet('Ventas Tarjeta Presente');
      s2.getRow(1).values = [null, null, 'Monto transaccionado', null, 100];
      s2.getRow(2).values = [null, null, 'Comisiones', null, 5];
      s2.getRow(3).values = [null, null, 'Iva', null, 1];
      s2.getRow(4).values = [null, null, 'Monto depositado', null, 100];
    });
    await expect(parseNetpayReporte(buffer)).rejects.toThrow(/más de un depósito/);
  });
});

describe('_normalizarHeader', () => {
  test('acentos, espacios y mayúsculas se normalizan igual', () => {
    expect(_normalizarHeader('Fecha de depósito')).toBe('fecha_de_deposito');
    expect(_normalizarHeader('Clave Rastreo')).toBe('clave_rastreo');
  });

  test('% y $ no colisionan entre sí (Comisión Base (%) vs Comisión Base ($))', () => {
    expect(_normalizarHeader('Comisión Base (%)')).toBe('comision_base_pct');
    expect(_normalizarHeader('Comisión Base ($)')).toBe('comision_base_monto');
  });

  test('porcentaje variable dentro del paréntesis (IVA Comisiones (16%))', () => {
    expect(_normalizarHeader('IVA Comisiones (16%)')).toBe('iva_comisiones_16pct');
  });

  test('signo + se colapsa igual que cualquier separador (Comisiones + IVA)', () => {
    expect(_normalizarHeader('Comisiones + IVA')).toBe('comisiones_iva');
  });
});

describe('_parseFechaDDMMYYYY / _parseFechaMesEs', () => {
  test('DD-MM-YYYY válido', () => {
    expect(_parseFechaDDMMYYYY('25-09-2026')).toEqual(new Date(Date.UTC(2026, 8, 25)));
  });

  test('DD-MM-YYYY inválido (ej. "Total") devuelve null', () => {
    expect(_parseFechaDDMMYYYY('Total')).toBeNull();
    expect(_parseFechaDDMMYYYY(null)).toBeNull();
  });

  test('DD-mmm-YYYY con mes en español abreviado', () => {
    expect(_parseFechaMesEs('25-sep-2026')).toEqual(new Date(Date.UTC(2026, 8, 25)));
    expect(_parseFechaMesEs('01-ene-2027')).toEqual(new Date(Date.UTC(2027, 0, 1)));
  });

  test('mes no reconocido devuelve null', () => {
    expect(_parseFechaMesEs('25-xyz-2026')).toBeNull();
  });
});

describe('_extraerTerminalID', () => {
  test('toma los primeros 10 caracteres de la porción posterior al ÚLTIMO guión', () => {
    expect(_extraerTerminalID('260924181626-2840746396783601')).toBe('2840746396');
  });

  test('order ID cuya porción posterior ya tiene exactamente 10 caracteres: se conserva completa', () => {
    expect(_extraerTerminalID('260828163321-2841258490')).toBe('2841258490');
  });

  test('sin guión: null', () => {
    expect(_extraerTerminalID('singuion')).toBeNull();
  });

  test('vacío/null: null', () => {
    expect(_extraerTerminalID(null)).toBeNull();
    expect(_extraerTerminalID('')).toBeNull();
  });
});
