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
  _agruparPorDeposito,
} = require('./netpay-reporte-parser.service');

const REPO_ROOT = path.join(__dirname, '../../../../..');
const PATH_UN_FOLIO   = path.join(REPO_ROOT, 'F0-Netpay.xlsx');
const PATH_TRES_FOLIOS = path.join(REPO_ROOT, '20260925_DetalleDepósitos.xlsx');
// netpay-reporte-global (design.md "Verification against real file"): archivo global REAL
// con 16 depósitos distintos (hoja "Resumen") y 316 folios en total — verificado por el
// orquestador antes de escribir el design: 0 huérfanos, 0 depósitos sin clave, 0 claves
// duplicadas, y el monto de folios sumado por clave_rastreo calza EXACTO (al centavo) con el
// Monto Depósito de esa misma fila en Resumen, 16/16.
const PATH_GLOBAL = path.join(REPO_ROOT, 'Global_DetalleDepositos.xlsx');

// Estos 3 Excel reales viven FUERA de este repo git (carpeta NUMO/ local, nunca commiteada —
// contienen datos reales de clientes). En CI (GitHub Actions clona solo numo-backend) esa ruta
// no puede existir nunca, así que estos tests se saltean ahí en vez de fallar el deploy.
// Localmente, con los archivos presentes, siguen corriendo y verificando contra datos reales.
function testSiExiste(filePath, nombre, fn) {
  if (!fs.existsSync(filePath)) {
    // eslint-disable-next-line no-console
    console.warn(`[netpay-reporte-parser.service.test.js] saltado: falta fixture real ${filePath}`);
    test.skip(nombre, fn);
    return;
  }
  test(nombre, fn);
}

describe('parseNetpayReporte — archivos reales del repo (N=1, un solo depósito)', () => {
  testSiExiste(PATH_UN_FOLIO, 'F0-Netpay.xlsx (1 folio): depositos[0] con claveRastreo, montoDepositoTotal, resumenVentas y folio parseados correctamente', async () => {
    const buffer = fs.readFileSync(PATH_UN_FOLIO);
    const { depositos } = await parseNetpayReporte(buffer);

    expect(depositos).toHaveLength(1);
    const [r] = depositos;
    expect(r.claveRastreo).toBe('42644264202609255810846530');
    expect(r.cuentaDeposito).toBe('012610001090310145');
    expect(r.fechaMovimiento).toEqual(new Date('2026-09-25T00:00:00.000Z'));
    expect(r.periodoDesde).toEqual(new Date('2026-09-25T00:00:00.000Z'));
    expect(r.periodoHasta).toEqual(new Date('2026-09-25T00:00:00.000Z'));
    expect(r.montoDepositoTotal).toBe(27189.68);
    // N=1 conserva el bloque de la hoja tal cual (no recalculado por folio).
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
      // claveRastreo NUNCA se persiste — _agruparPorDeposito la quita de cada folio.
    });
    expect(r.folios[0]).not.toHaveProperty('claveRastreo');
  });

  testSiExiste(PATH_TRES_FOLIOS, '20260925_DetalleDepósitos.xlsx (3 folios): mismo depósito para las 3 filas, terminalID estable', async () => {
    const buffer = fs.readFileSync(PATH_TRES_FOLIOS);
    const { depositos } = await parseNetpayReporte(buffer);

    expect(depositos).toHaveLength(1);
    const [r] = depositos;
    expect(r.claveRastreo).toBe('42644264202609235803800435');
    expect(r.montoDepositoTotal).toBe(14214.73);
    expect(r.folios).toHaveLength(3);
    // Las 3 filas son de la MISMA terminal/tienda — el terminalID (prefijo estable de
    // Order ID) debe ser idéntico en las 3, aunque el sufijo completo varíe por transacción.
    expect(new Set(r.folios.map(f => f.terminalID))).toEqual(new Set(['2840592988']));
    expect(r.folios.map(f => f.referencia)).toEqual(['F20260922-00055', 'F20260922-00282', 'F20260922-00090']);
  });
});

describe('parseNetpayReporte — archivo global real (N=16 depósitos, Global_DetalleDepositos.xlsx)', () => {
  testSiExiste(PATH_GLOBAL, '16 depósitos agrupados, 316 folios repartidos SIN huérfanos, cada uno con su propio resumenVentas recalculado', async () => {
    const buffer = fs.readFileSync(PATH_GLOBAL);
    const { depositos } = await parseNetpayReporte(buffer);

    expect(depositos).toHaveLength(16);
    const totalFolios = depositos.reduce((acc, d) => acc + d.folios.length, 0);
    expect(totalFolios).toBe(316);
    // Ningún depósito del archivo real quedó sin folios (verificado por el orquestador).
    expect(depositos.every(d => d.folios.length > 0)).toBe(true);
    // Ninguna claveRastreo de folio sobrevive a la persistencia.
    expect(depositos.every(d => d.folios.every(f => !('claveRastreo' in f)))).toBe(true);

    const objetivo = depositos.find(d => d.claveRastreo === '42644264202609295825472413');
    expect(objetivo).toBeDefined();
    expect(objetivo.montoDepositoTotal).toBe(1762);
    expect(objetivo.folios).toHaveLength(4);
    // N>1: resumenVentas se recalcula sumando los folios de ESE depósito (no el bloque
    // global de la hoja "Ventas Tarjeta Presente", que es un total de archivo) — valores
    // reales verificados contra el archivo (ver probe del orquestador).
    expect(objetivo.resumenVentas).toEqual({
      montoTransaccionado: 1798.67, comisiones: 31.61, iva: 5.06, montoDepositado: 1762,
    });
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

  // netpay-reporte-global: reemplaza el viejo test "MÁS de un depósito -> rechazado" — ya
  // NO se rechaza, se agrupa (ver describe de abajo). Este describe conserva solo los casos
  // de error que NO dependen de la agrupación (hoja/tabla faltante).
});

// netpay-reporte-global (design.md "Folio grouping" + "Integrity" + "N=1 leniency") —
// workbooks sintéticos multi-depósito, con foco en _agruparPorDeposito vía
// parseNetpayReporte end-to-end (y algunos casos unitarios directos sobre
// _agruparPorDeposito, que es más simple para los casos de integridad).
describe('parseNetpayReporte — múltiples depósitos (N>1): agrupación e integridad', () => {
  function _headerResumen() {
    return [null, null, 'Fecha de Movimiento', 'Clave Rastreo', 'Cuenta Depósito', 'Descripción', 'Monto Depósito'];
  }

  function _headerFolios() {
    return [
      null,
      'Fecha de Depósito', 'Clave Rastreo', 'Cuenta Depósito', 'Nombre Empresa', 'Sucursal', 'Store ID',
      'Monto Depósito', 'Fecha Trx', 'Hora de Trx', 'Monto de Trx', 'Comisión Base (%)', 'Comisión Base ($)',
      'IVA Comisiones (16%)', 'Comisiones + IVA', 'Banco', 'Tipo de Tarjeta', 'Código de Autorización', 'Order ID', 'Referencia',
    ];
  }

  function _filaFolio({
    clave, montoDeposito, montoTrx, comisionBaseMonto, ivaComisionMonto = 1, referencia, orderId = 'ORD-X',
  }) {
    return [
      null,
      '25-09-2026', clave, '012610001090310145', 'EMPRESA', 'SUCURSAL X', '1650292',
      montoDeposito, '24-09-2026', '18:16', montoTrx, 0.0075, comisionBaseMonto,
      ivaComisionMonto, comisionBaseMonto + ivaComisionMonto, 'SANTANDER', 'Débito', '062788', orderId, referencia,
    ];
  }

  async function _bufferDesde(fn) {
    const wb = new ExcelJS.Workbook();
    fn(wb);
    return wb.xlsx.writeBuffer();
  }

  test('3 Resumen rows, cada una con sus propios folios: 3 depositos[], cada uno SOLO con sus folios', async () => {
    const buffer = await _bufferDesde((wb) => {
      const s1 = wb.addWorksheet('Resumen');
      s1.getRow(1).values = _headerResumen();
      s1.getRow(2).values = [null, null, '25-09-2026', 'CLAVE-1', '012610001090310145', 'PAGO', 300];
      s1.getRow(3).values = [null, null, '26-09-2026', 'CLAVE-2', '012610001090310145', 'PAGO', 150];
      s1.getRow(4).values = [null, null, '27-09-2026', 'CLAVE-3', '012610001090310145', 'PAGO', 100];

      const s2 = wb.addWorksheet('Ventas Tarjeta Presente');
      s2.getRow(1).values = [null, null, 'Monto transaccionado', null, 550];
      s2.getRow(2).values = [null, null, 'Comisiones', null, 20];
      s2.getRow(3).values = [null, null, 'Iva', null, 3];
      s2.getRow(4).values = [null, null, 'Monto depositado', null, 550];
      s2.getRow(6).values = _headerFolios();
      s2.getRow(7).values = _filaFolio({ clave: 'CLAVE-1', montoDeposito: 150, montoTrx: 160, comisionBaseMonto: 8, referencia: 'F1' });
      s2.getRow(8).values = _filaFolio({ clave: 'CLAVE-1', montoDeposito: 150, montoTrx: 160, comisionBaseMonto: 8, referencia: 'F2' });
      s2.getRow(9).values = _filaFolio({ clave: 'CLAVE-2', montoDeposito: 150, montoTrx: 160, comisionBaseMonto: 8, referencia: 'F3' });
      s2.getRow(10).values = _filaFolio({ clave: 'CLAVE-3', montoDeposito: 100, montoTrx: 108, comisionBaseMonto: 5, referencia: 'F4' });
    });

    const { depositos } = await parseNetpayReporte(buffer);

    expect(depositos).toHaveLength(3);
    const porClave = Object.fromEntries(depositos.map(d => [d.claveRastreo, d]));
    expect(porClave['CLAVE-1'].folios.map(f => f.referencia)).toEqual(['F1', 'F2']);
    expect(porClave['CLAVE-2'].folios.map(f => f.referencia)).toEqual(['F3']);
    expect(porClave['CLAVE-3'].folios.map(f => f.referencia)).toEqual(['F4']);
    expect(porClave['CLAVE-1'].montoDepositoTotal).toBe(300);
    expect(porClave['CLAVE-2'].montoDepositoTotal).toBe(150);
    expect(porClave['CLAVE-3'].montoDepositoTotal).toBe(100);
  });

  test('resumenVentas por depósito (N>1): se recalcula sumando SOLO los folios de ese depósito', async () => {
    const buffer = await _bufferDesde((wb) => {
      const s1 = wb.addWorksheet('Resumen');
      s1.getRow(1).values = _headerResumen();
      s1.getRow(2).values = [null, null, '25-09-2026', 'CLAVE-1', '012610001090310145', 'PAGO', 300];
      s1.getRow(3).values = [null, null, '26-09-2026', 'CLAVE-2', '012610001090310145', 'PAGO', 100];

      const s2 = wb.addWorksheet('Ventas Tarjeta Presente');
      s2.getRow(1).values = [null, null, 'Monto transaccionado', null, 9999]; // total de archivo — NO debe usarse para N>1
      s2.getRow(2).values = [null, null, 'Comisiones', null, 9999];
      s2.getRow(3).values = [null, null, 'Iva', null, 9999];
      s2.getRow(4).values = [null, null, 'Monto depositado', null, 9999];
      s2.getRow(6).values = _headerFolios();
      s2.getRow(7).values = _filaFolio({ clave: 'CLAVE-1', montoDeposito: 150, montoTrx: 160, comisionBaseMonto: 8, ivaComisionMonto: 1.28, referencia: 'F1' });
      s2.getRow(8).values = _filaFolio({ clave: 'CLAVE-1', montoDeposito: 150, montoTrx: 160, comisionBaseMonto: 8, ivaComisionMonto: 1.28, referencia: 'F2' });
      s2.getRow(9).values = _filaFolio({ clave: 'CLAVE-2', montoDeposito: 100, montoTrx: 108, comisionBaseMonto: 5, ivaComisionMonto: 0.8, referencia: 'F3' });
    });

    const { depositos } = await parseNetpayReporte(buffer);
    const porClave = Object.fromEntries(depositos.map(d => [d.claveRastreo, d]));

    expect(porClave['CLAVE-1'].resumenVentas).toEqual({
      montoTransaccionado: 320, comisiones: 16, iva: 2.56, montoDepositado: 300,
    });
    expect(porClave['CLAVE-2'].resumenVentas).toEqual({
      montoTransaccionado: 108, comisiones: 5, iva: 0.8, montoDepositado: 100,
    });
  });

  test('folio huérfano (clave sin fila de Resumen que lo reclame): rechaza TODO el archivo, ninguna fila válida se guarda', async () => {
    const buffer = await _bufferDesde((wb) => {
      const s1 = wb.addWorksheet('Resumen');
      s1.getRow(1).values = _headerResumen();
      s1.getRow(2).values = [null, null, '25-09-2026', 'CLAVE-1', '012610001090310145', 'PAGO', 150];

      const s2 = wb.addWorksheet('Ventas Tarjeta Presente');
      s2.getRow(1).values = [null, null, 'Monto transaccionado', null, 160];
      s2.getRow(2).values = [null, null, 'Comisiones', null, 8];
      s2.getRow(3).values = [null, null, 'Iva', null, 1];
      s2.getRow(4).values = [null, null, 'Monto depositado', null, 150];
      s2.getRow(6).values = _headerFolios();
      s2.getRow(7).values = _filaFolio({ clave: 'CLAVE-1', montoDeposito: 150, montoTrx: 160, comisionBaseMonto: 8, referencia: 'F1' });
      s2.getRow(8).values = _filaFolio({ clave: 'CLAVE-HUERFANA', montoDeposito: 50, montoTrx: 55, comisionBaseMonto: 3, referencia: 'F2' });
    });

    await expect(parseNetpayReporte(buffer)).rejects.toThrow(/CLAVE-HUERFANA/);
  });

  test('clave duplicada en Resumen (2 filas con la misma Clave Rastreo): rechaza TODO el archivo', async () => {
    const buffer = await _bufferDesde((wb) => {
      const s1 = wb.addWorksheet('Resumen');
      s1.getRow(1).values = _headerResumen();
      s1.getRow(2).values = [null, null, '25-09-2026', 'CLAVE-1', '012610001090310145', 'PAGO', 100];
      s1.getRow(3).values = [null, null, '26-09-2026', 'CLAVE-1', '012610001090310145', 'PAGO', 200];

      const s2 = wb.addWorksheet('Ventas Tarjeta Presente');
      s2.getRow(1).values = [null, null, 'Monto transaccionado', null, 100];
      s2.getRow(2).values = [null, null, 'Comisiones', null, 5];
      s2.getRow(3).values = [null, null, 'Iva', null, 1];
      s2.getRow(4).values = [null, null, 'Monto depositado', null, 100];
      s2.getRow(6).values = _headerFolios();
      s2.getRow(7).values = _filaFolio({ clave: 'CLAVE-1', montoDeposito: 100, montoTrx: 108, comisionBaseMonto: 5, referencia: 'F1' });
    });

    await expect(parseNetpayReporte(buffer)).rejects.toThrow(/repetida/);
  });

  test('Resumen row sin Clave Rastreo: rechaza TODO el archivo', async () => {
    const buffer = await _bufferDesde((wb) => {
      const s1 = wb.addWorksheet('Resumen');
      s1.getRow(1).values = _headerResumen();
      s1.getRow(2).values = [null, null, '25-09-2026', '', '012610001090310145', 'PAGO', 100];

      const s2 = wb.addWorksheet('Ventas Tarjeta Presente');
      s2.getRow(1).values = [null, null, 'Monto transaccionado', null, 100];
      s2.getRow(2).values = [null, null, 'Comisiones', null, 5];
      s2.getRow(3).values = [null, null, 'Iva', null, 1];
      s2.getRow(4).values = [null, null, 'Monto depositado', null, 100];
      s2.getRow(6).values = _headerFolios();
      s2.getRow(7).values = _filaFolio({ clave: '', montoDeposito: 100, montoTrx: 108, comisionBaseMonto: 5, referencia: 'F1' });
    });

    await expect(parseNetpayReporte(buffer)).rejects.toThrow(/no trae Clave Rastreo/);
  });

  test('depósito con CERO folios asignados: NO rechaza el archivo — el resto se agrupa normalmente (error es responsabilidad del service, no del parser)', async () => {
    const buffer = await _bufferDesde((wb) => {
      const s1 = wb.addWorksheet('Resumen');
      s1.getRow(1).values = _headerResumen();
      s1.getRow(2).values = [null, null, '25-09-2026', 'CLAVE-1', '012610001090310145', 'PAGO', 150];
      s1.getRow(3).values = [null, null, '26-09-2026', 'CLAVE-SIN-FOLIOS', '012610001090310145', 'PAGO', 0];

      const s2 = wb.addWorksheet('Ventas Tarjeta Presente');
      s2.getRow(1).values = [null, null, 'Monto transaccionado', null, 160];
      s2.getRow(2).values = [null, null, 'Comisiones', null, 8];
      s2.getRow(3).values = [null, null, 'Iva', null, 1];
      s2.getRow(4).values = [null, null, 'Monto depositado', null, 150];
      s2.getRow(6).values = _headerFolios();
      s2.getRow(7).values = _filaFolio({ clave: 'CLAVE-1', montoDeposito: 150, montoTrx: 160, comisionBaseMonto: 8, referencia: 'F1' });
    });

    const { depositos } = await parseNetpayReporte(buffer);
    expect(depositos).toHaveLength(2);
    const sinFolios = depositos.find(d => d.claveRastreo === 'CLAVE-SIN-FOLIOS');
    expect(sinFolios.folios).toEqual([]);
  });

  // design.md "N=1 leniency": con un solo depósito en Resumen, los folios con Clave Rastreo
  // en blanco se asignan a ese único depósito (no se consideran huérfanos).
  test('N=1 leniency: folios con Clave Rastreo en blanco se asignan al único depósito', async () => {
    const buffer = await _bufferDesde((wb) => {
      const s1 = wb.addWorksheet('Resumen');
      s1.getRow(1).values = _headerResumen();
      s1.getRow(2).values = [null, null, '25-09-2026', 'CLAVE-1', '012610001090310145', 'PAGO', 150];

      const s2 = wb.addWorksheet('Ventas Tarjeta Presente');
      s2.getRow(1).values = [null, null, 'Monto transaccionado', null, 160];
      s2.getRow(2).values = [null, null, 'Comisiones', null, 8];
      s2.getRow(3).values = [null, null, 'Iva', null, 1];
      s2.getRow(4).values = [null, null, 'Monto depositado', null, 150];
      s2.getRow(6).values = _headerFolios();
      s2.getRow(7).values = _filaFolio({ clave: '', montoDeposito: 150, montoTrx: 160, comisionBaseMonto: 8, referencia: 'F1' });
    });

    const { depositos } = await parseNetpayReporte(buffer);
    expect(depositos).toHaveLength(1);
    expect(depositos[0].folios.map(f => f.referencia)).toEqual(['F1']);
  });

  // design.md "N=1 leniency": "A folio clave that is non-blank and different is still an
  // orphan" — la leniencia SOLO aplica a clave en blanco, no a cualquier clave distinta.
  test('N=1: una clave de folio NO vacía pero DISTINTA a la del único depósito sigue siendo huérfana', async () => {
    const buffer = await _bufferDesde((wb) => {
      const s1 = wb.addWorksheet('Resumen');
      s1.getRow(1).values = _headerResumen();
      s1.getRow(2).values = [null, null, '25-09-2026', 'CLAVE-1', '012610001090310145', 'PAGO', 150];

      const s2 = wb.addWorksheet('Ventas Tarjeta Presente');
      s2.getRow(1).values = [null, null, 'Monto transaccionado', null, 160];
      s2.getRow(2).values = [null, null, 'Comisiones', null, 8];
      s2.getRow(3).values = [null, null, 'Iva', null, 1];
      s2.getRow(4).values = [null, null, 'Monto depositado', null, 150];
      s2.getRow(6).values = _headerFolios();
      s2.getRow(7).values = _filaFolio({ clave: 'CLAVE-OTRA', montoDeposito: 150, montoTrx: 160, comisionBaseMonto: 8, referencia: 'F1' });
    });

    await expect(parseNetpayReporte(buffer)).rejects.toThrow(/CLAVE-OTRA/);
  });
});

// _agruparPorDeposito — casos unitarios directos (sin armar un workbook completo), para la
// parte de integridad que no depende de ExcelJS en absoluto.
describe('_agruparPorDeposito (unitario)', () => {
  function row(clave, montoDeposito = 100) {
    return { claveRastreo: clave, cuentaDeposito: 'CTA', fechaMovimiento: new Date('2026-09-25'), montoDeposito, periodoDesde: null, periodoHasta: null };
  }
  function folio(clave, referencia) {
    return { claveRastreo: clave, referencia, montoTrx: 10, comisionBaseMonto: 1, ivaComision: 0.1 };
  }

  test('strips claveRastreo de cada folio antes de devolverlo', () => {
    const [unidad] = _agruparPorDeposito([row('C1')], [folio('C1', 'F1')]);
    expect(unidad.folios[0]).not.toHaveProperty('claveRastreo');
    expect(unidad.folios[0].referencia).toBe('F1');
  });

  test('normaliza espacios de más (trim) al comparar claves', () => {
    const [unidad] = _agruparPorDeposito([row(' C1 ')], [folio('C1', 'F1')]);
    expect(unidad.folios).toHaveLength(1);
  });
});

// netpay-matching-v2 (design.md "Data Model"): folios[].marca — columna "Marca" (AD),
// OPCIONAL (Kore no siempre expone cardTypeName por folio). Se usa un workbook sintético
// completo (no los 2 archivos reales, que no dependemos tener presentes en cualquier
// checkout) para no acoplar este caso a la disponibilidad de esos 2 archivos en el repo.
describe('parseNetpayReporte — columna "Marca" (AD), opcional', () => {
  function _headerFolios(conMarca) {
    const base = [
      null,
      'Fecha de Depósito', 'Clave Rastreo', 'Cuenta Depósito', 'Nombre Empresa', 'Sucursal', 'Store ID',
      'Monto Depósito', 'Fecha Trx', 'Hora de Trx', 'Monto de Trx', 'Comisión Base (%)', 'Comisión Base ($)',
      'Comisiones + IVA', 'Banco', 'Tipo de Tarjeta', 'Código de Autorización', 'Order ID', 'Referencia',
    ];
    if (conMarca) base.push('Marca');
    return base;
  }

  function _dataFolios(conMarca) {
    const base = [
      null,
      '25-09-2026', 'CLAVE-1', '012610001090310145', 'CAR COMERCIALIZADORA', 'AV FERROCARRIL 802', '1650292',
      815.71, '24-09-2026', '18:16', 822.87, 0.0075, 6.17,
      7.16, 'SANTANDER', 'Débito', '062788', '260924181626-2840746396783601', 'F20260924-00311',
    ];
    if (conMarca) base.push('AMEX');
    return base;
  }

  async function _bufferConFolios(conMarca) {
    const wb = new ExcelJS.Workbook();
    const s1 = wb.addWorksheet('Resumen');
    s1.getRow(1).values = [null, null, 'Fecha de Movimiento', 'Clave Rastreo', 'Cuenta Depósito', 'Descripción', 'Monto Depósito'];
    s1.getRow(2).values = [null, null, '25-09-2026', 'CLAVE-1', '012610001090310145', 'PAGO', 815.71];

    const s2 = wb.addWorksheet('Ventas Tarjeta Presente');
    s2.getRow(1).values = [null, null, 'Monto transaccionado', null, 822.87];
    s2.getRow(2).values = [null, null, 'Comisiones', null, 6.17];
    s2.getRow(3).values = [null, null, 'Iva', null, 0.99];
    s2.getRow(4).values = [null, null, 'Monto depositado', null, 815.71];
    s2.getRow(6).values = _headerFolios(conMarca);
    s2.getRow(7).values = _dataFolios(conMarca);

    return wb.xlsx.writeBuffer();
  }

  test('columna "Marca" presente: folios[].marca se parsea tal cual', async () => {
    const buffer = await _bufferConFolios(true);
    const { depositos } = await parseNetpayReporte(buffer);
    const [r] = depositos;
    expect(r.folios).toHaveLength(1);
    expect(r.folios[0].marca).toBe('AMEX');
    // El resto de las columnas siguen parseándose igual, sin que Marca las corra.
    expect(r.folios[0].referencia).toBe('F20260924-00311');
    expect(r.folios[0].orderId).toBe('260924181626-2840746396783601');
  });

  test('columna "Marca" ausente: folios[].marca es null (no undefined, no rompe el parseo)', async () => {
    const buffer = await _bufferConFolios(false);
    const { depositos } = await parseNetpayReporte(buffer);
    const [r] = depositos;
    expect(r.folios).toHaveLength(1);
    expect(r.folios[0].marca).toBeNull();
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
