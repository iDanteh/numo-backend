'use strict';

const ExcelJS                          = require('exceljs');
const CFDI                             = require('../../../visor/models/CFDI');
const { sincronizarCuentasPendientes } = require('./erp-sync.service');

const MOTOR_ID     = 'formas-pago-cxc';
const MOTOR_NOMBRE = 'Excel Formas de Pago CxC';

// Pausa entre llamados a Kore (/cuentas-pendientes) — mismo valor que SYNC_DELAY_MS en
// erp.routes.js: con menos de 1000ms Kore respondía 429 (lección ya aprendida en este código).
const SYNC_DELAY_MS = 1000;
const sleep = ms => new Promise(r => setTimeout(r, ms));

// ── Mismas lógica que _esFormaPagoBancariaKore() en erp.routes.js y _esFormaBancaria() en
// cobro-panel.component.ts (frontend) — transferencia/cheque/depósito en efectivo son las
// únicas formas de pago que representan un movimiento bancario real. Duplicada aquí a
// propósito (no exportada desde erp.routes.js, que solo exporta el router) — es el patrón ya
// establecido en este código para esta función.
function _esFormaPagoBancariaKore(nombreFormaPago) {
  const norm = String(nombreFormaPago ?? '')
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .trim().toUpperCase();
  return norm === 'TRANSFERENCIA'
      || norm === 'CHEQUE'
      || /DEPOSITO.*EFECTIVO/.test(norm);
}

// Normaliza un nombre de forma de pago para almacenarlo en `formasPago` — mismo criterio
// (sin acentos, mayúsculas, recortado) que usa _esFormaPagoBancariaKore() para comparar.
function _normalizeFormaPago(nombreFormaPago) {
  return String(nombreFormaPago ?? '')
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .trim().toUpperCase();
}

// ── Misma lógica que _rangoDesdeFollo() en erp.routes.js — duplicada aquí a propósito
// (no exportada como función standalone, y erp.routes.js YA requiere a este archivo para
// exponer /formas-pago-cxc/upload+export, así que importarla de vuelta crearía un require
// circular real). Kore folios codifican YYMMxxxxx (ej. "260704784" → año=2026, mes=07);
// /cuentas-pendientes EXIGE FechaDesde/FechaHasta (400 si faltan) y un rango máximo de un
// mes, así que esta función siempre devuelve el mes calendario exacto del folio.
function _rangoDesdeFollo(folioExterno) {
  const str = String(folioExterno).trim();
  if (str.length < 4) return null;
  const yy = parseInt(str.slice(0, 2), 10);
  const mm = parseInt(str.slice(2, 4), 10);
  if (isNaN(yy) || isNaN(mm) || mm < 1 || mm > 12) return null;
  const year    = 2000 + yy;
  const mmStr   = String(mm).padStart(2, '0');
  const lastDay = new Date(Date.UTC(year, mm, 0)).getUTCDate();
  const lastStr = String(lastDay).padStart(2, '0');
  return {
    fechaDesde: `${year}-${mmStr}-01T00:00:00Z`,
    fechaHasta: `${year}-${mmStr}-${lastStr}T23:59:59Z`,
  };
}

// ── Extrae el valor "plano" de una celda ExcelJS (Date, fórmula, texto enriquecido, etc.) ────
function _cellValue(raw) {
  if (raw == null) return null;
  if (raw instanceof Date) return raw;
  if (typeof raw === 'object') {
    if ('result' in raw) return raw.result;
    if ('text' in raw)   return String(raw.text);
    if ('richText' in raw) return raw.richText.map(t => t.text).join('');
  }
  return raw;
}

// ── Parse del Excel "Pagos Asociados" ─────────────────────────────────────────
// Fila 1 = headers (21 columnas) — se lee por NOMBRE, no por índice fijo, para tolerar
// reordenamientos de columnas entre corridas del reporte de origen.
async function _parseExcel(buffer) {
  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load(buffer);
  const ws = wb.worksheets[0];
  if (!ws) throw new Error('El archivo no contiene hojas válidas');

  const headerRow = ws.getRow(1);
  const headerMap = new Map(); // texto del header (trim) → número de columna
  headerRow.eachCell({ includeEmpty: false }, (cell, colNumber) => {
    const text = cell.value != null ? String(_cellValue(cell.value) ?? '').trim() : '';
    if (text) headerMap.set(text, colNumber);
  });

  const rows = [];
  ws.eachRow({ includeEmpty: false }, (row, idx) => {
    if (idx === 1) return; // encabezado

    const originalCols = {};
    for (const [header, colIdx] of headerMap) {
      originalCols[header] = _cellValue(row.getCell(colIdx).value);
    }

    rows.push({ fila: idx, originalCols });
  });

  return rows;
}

// ═════════════════════════════════════════════════════════════════════════════
// SERVICIO PRINCIPAL
// ─────────────────────────────────────────────────────────────────────────────
// Pipeline por fila: factura (cfdis, Serie+Folio) → pedido (documentosRelacionados[0]) →
// CxC en Kore (serieExterna/folioExterno = Serie/Folio del pedido) → formas de pago reales
// de los abonos de esa CxC → clasificación bancaria / no bancaria / sin resolver.
//
// SÍNCRONO a propósito (mismo patrón que refacturaciones-cyc / mostrador-cyc / pagos-cyc):
// un solo request que puede tardar varios minutos (hasta ~235 filas × 1s de pausa entre
// llamados a Kore). No hay progreso incremental ni socket — el cliente espera la respuesta.
// ═════════════════════════════════════════════════════════════════════════════
async function procesarFormasPagoCxc(buffer, _usuarioId, _usuarioNombre) {
  const rows  = await _parseExcel(buffer);
  const total = rows.length;

  const detalleBancarias   = [];
  const detalleNoBancarias = [];
  const detalleSinResolver = [];
  let sinFactura    = 0;
  let sinPedido     = 0;
  let sinCxcEnKore  = 0;

  let llamoKoreAlMenosUnaVez = false;

  for (const { fila, originalCols } of rows) {
    const serieRaw = originalCols['Serie'];
    const folioRaw = originalCols['Folio'];
    const serie    = serieRaw != null ? String(serieRaw).trim() : '';
    const folio    = folioRaw != null ? String(folioRaw).trim() : '';

    const sinResolver = (razon, detalle) => {
      if (razon === 'sin_factura')      sinFactura++;
      else if (razon === 'sin_pedido')  sinPedido++;
      else if (razon === 'sin_cxc_en_kore') sinCxcEnKore++;
      detalleSinResolver.push({ ...originalCols, fila, razon, detalle });
    };

    if (!serie || !folio) {
      sinResolver('sin_factura', 'Fila sin Serie/Folio de factura — no se puede ubicar en cfdis.');
      continue;
    }

    // 1. Ubicar la factura (CFDI de ingreso, ERP) por Serie+Folio ────────────────
    const factura = await CFDI.findOne({
      source: 'ERP', serie, folio, tipoDeComprobante: 'I',
    }).lean();

    if (!factura) {
      sinResolver(
        'sin_factura',
        `No se encontró la factura ${serie}-${folio} en cfdis (source ERP, tipoDeComprobante I).`,
      );
      continue;
    }

    // 2. Leer documentosRelacionados[0] → Serie/Folio del PEDIDO ─────────────────
    const pedido       = (factura.documentosRelacionados ?? [])[0];
    const pedidoSerie  = pedido?.Serie != null ? String(pedido.Serie).trim() : '';
    const pedidoFolio  = pedido?.Folio != null ? String(pedido.Folio).trim() : '';

    if (!pedidoSerie || !pedidoFolio) {
      sinResolver(
        'sin_pedido',
        `La factura ${serie}-${folio} no tiene documentosRelacionados (pedido) registrado.`,
      );
      continue;
    }

    // 3. Consultar la CxC en Kore usando Serie/Folio del pedido ──────────────────
    const rango = _rangoDesdeFollo(pedidoFolio);
    if (!rango) {
      sinResolver(
        'sin_cxc_en_kore',
        `No se pudo determinar el rango de fecha para el folio del pedido ${pedidoSerie}-${pedidoFolio}.`,
      );
      continue;
    }

    if (llamoKoreAlMenosUnaVez) await sleep(SYNC_DELAY_MS);
    llamoKoreAlMenosUnaVez = true;

    let raw;
    try {
      ({ raw } = await sincronizarCuentasPendientes({
        serieExterna: pedidoSerie, folioExterno: pedidoFolio,
        fechaDesde: rango.fechaDesde, fechaHasta: rango.fechaHasta,
      }));
    } catch (err) {
      sinResolver(
        'sin_cxc_en_kore',
        `Error consultando Kore para el pedido ${pedidoSerie}-${pedidoFolio}: ${err.message}`,
      );
      continue;
    }

    // Mismo criterio que erp.routes.js (GET /cuentas-pendientes/buscar): matchear el par
    // exacto serieExterna+folioExterno dentro de lo devuelto, nunca solo raw[0] a ciegas.
    const cuenta = (raw ?? []).find(
      c => String(c.folioExterno) === pedidoFolio && String(c.serieExterna) === pedidoSerie,
    );

    if (!cuenta) {
      sinResolver(
        'sin_cxc_en_kore',
        `No se encontró la CxC en Kore para el pedido ${pedidoSerie}-${pedidoFolio}.`,
      );
      continue;
    }

    // 4. Leer las formas de pago reales de los abonos de esa CxC ────────────────
    const formasPagoRaw = [];
    for (const mov of cuenta.movimientos ?? []) {
      for (const fp of mov.formasPago ?? []) {
        if (fp?.nombreFormaPago) formasPagoRaw.push(fp.nombreFormaPago);
      }
    }
    const formasPago = [...new Set(formasPagoRaw.map(_normalizeFormaPago))];
    const esBancaria  = formasPagoRaw.some(fp => _esFormaPagoBancariaKore(fp));

    const item = { ...originalCols, fila, pedidoSerie, pedidoFolio, formasPago };
    if (esBancaria) detalleBancarias.push(item);
    else            detalleNoBancarias.push(item);
  }

  return {
    total,
    bancarias:   detalleBancarias.length,
    noBancarias: detalleNoBancarias.length,
    errors: { sinFactura, sinPedido, sinCxcEnKore },
    detalleBancarias,
    detalleNoBancarias,
    detalleSinResolver,
  };
}

// ═════════════════════════════════════════════════════════════════════════════
// GENERADOR DE EXCEL (export del resultado)
// ─────────────────────────────────────────────────────────────────────────────
// 3 hojas: Concentrado bancarias (verde) · No bancarias (amarillo) · Sin resolver (rojo).
// Mismo estilo (colores, formato moneda, autoFilter) que generarExcelPagosCyc().
// ═════════════════════════════════════════════════════════════════════════════

// Orden canónico de las 21 columnas originales del Excel "Pagos Asociados".
const COLS_ORIGINALES = [
  { header: 'UUID CFDI Pago',     key: 'uuidCfdiPago',    width: 26 },
  { header: 'Estado SAT',         key: 'estadoSat',        width: 12 },
  { header: 'Fecha Pago',         key: 'fechaPago',        width: 12, isDate: true },
  { header: 'UUID Factura',       key: 'uuidFactura',      width: 26 },
  { header: 'Serie',              key: 'serie',            width: 10 },
  { header: 'Folio',              key: 'folio',            width: 10 },
  { header: 'Parcialidad',        key: 'parcialidad',      width: 11 },
  { header: 'Depósito',           key: 'deposito',         width: 13, isMoney: true },
  { header: 'Saldo Anterior',     key: 'saldoAnterior',    width: 13, isMoney: true },
  { header: 'Imp. Pagado',        key: 'impPagado',        width: 13, isMoney: true },
  { header: 'Saldo Insoluto',     key: 'saldoInsoluto',    width: 13, isMoney: true },
  { header: 'Diferencia',         key: 'diferencia',       width: 13, isMoney: true },
  { header: 'Tipo NC',            key: 'tipoNc',           width: 10 },
  { header: 'Monto NC',           key: 'montoNc',          width: 12, isMoney: true },
  { header: 'Tiene Pago',         key: 'tienePago',        width: 11 },
  { header: 'Banco',              key: 'banco',            width: 13 },
  { header: 'Fecha Movimiento',   key: 'fechaMovimiento',  width: 14, isDate: true },
  { header: 'ID NUMO',            key: 'idNumo',           width: 12 },
  { header: 'Núm. Autorización',  key: 'numAutorizacion',  width: 16 },
  { header: 'Saldo Banco',        key: 'saldoBanco',       width: 13, isMoney: true },
  { header: 'Identificado por',   key: 'identificadoPor',  width: 16 },
];

async function generarExcelFormasPagoCxc(resultado) {
  const wb = new ExcelJS.Workbook();
  wb.creator = 'Numo — Formas de Pago CxC';
  wb.created = new Date();

  const HEADER_FILL = {
    type: 'pattern', pattern: 'solid',
    fgColor: { argb: 'FF6D28D9' },   // violeta corporativo
  };
  const HEADER_FONT = { bold: true, color: { argb: 'FFFFFFFF' }, size: 10 };

  const OK_FILL   = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFD1FAE5' } }; // verde
  const ERR_FILL  = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFEE2E2' } }; // rojo
  const WARN_FILL = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFEF9C3' } }; // amarillo

  function formatFecha(d) {
    if (!d) return '';
    const dt = d instanceof Date ? d : new Date(d);
    if (isNaN(dt.getTime())) return '';
    return dt.toLocaleDateString('es-MX', { day: '2-digit', month: '2-digit', year: 'numeric' });
  }

  function styleHeader(ws) {
    ws.getRow(1).eachCell(cell => {
      cell.fill = HEADER_FILL;
      cell.font = HEADER_FONT;
      cell.alignment = { vertical: 'middle', horizontal: 'center' };
    });
    ws.getRow(1).height = 20;
  }

  function baseRowValues(item) {
    const vals = {};
    for (const col of COLS_ORIGINALES) {
      const raw = item[col.header];
      vals[col.key] = col.isDate ? formatFecha(raw) : (raw ?? '');
    }
    return vals;
  }

  function applyMoneyFormats(ws) {
    for (const col of COLS_ORIGINALES) {
      if (col.isMoney) ws.getColumn(col.key).numFmt = '#,##0.00';
    }
  }

  // ── Hoja 1: Concentrado bancarias ────────────────────────────────────────────
  const wsBanc = wb.addWorksheet('Concentrado bancarias');
  wsBanc.columns = [
    ...COLS_ORIGINALES.map(c => ({ header: c.header, key: c.key, width: c.width })),
    { header: 'Pedido Serie',      key: 'pedidoSerie', width: 12 },
    { header: 'Pedido Folio',      key: 'pedidoFolio', width: 12 },
    { header: 'Forma(s) de Pago',  key: 'formasPago',  width: 28 },
  ];
  styleHeader(wsBanc);
  for (const r of (resultado.detalleBancarias ?? [])) {
    const row = wsBanc.addRow({
      ...baseRowValues(r),
      pedidoSerie: r.pedidoSerie ?? '',
      pedidoFolio: r.pedidoFolio ?? '',
      formasPago:  (r.formasPago ?? []).join(', '),
    });
    row.eachCell(cell => { cell.fill = OK_FILL; });
  }
  applyMoneyFormats(wsBanc);
  wsBanc.autoFilter = { from: 'A1', to: wsBanc.lastColumn.letter + '1' };

  // ── Hoja 2: No bancarias ─────────────────────────────────────────────────────
  const wsNoBanc = wb.addWorksheet('No bancarias');
  wsNoBanc.columns = [
    ...COLS_ORIGINALES.map(c => ({ header: c.header, key: c.key, width: c.width })),
    { header: 'Pedido Serie',      key: 'pedidoSerie', width: 12 },
    { header: 'Pedido Folio',      key: 'pedidoFolio', width: 12 },
    { header: 'Forma(s) de Pago',  key: 'formasPago',  width: 28 },
  ];
  styleHeader(wsNoBanc);
  for (const r of (resultado.detalleNoBancarias ?? [])) {
    const row = wsNoBanc.addRow({
      ...baseRowValues(r),
      pedidoSerie: r.pedidoSerie ?? '',
      pedidoFolio: r.pedidoFolio ?? '',
      formasPago:  (r.formasPago ?? []).join(', '),
    });
    row.eachCell(cell => { cell.fill = WARN_FILL; });
  }
  applyMoneyFormats(wsNoBanc);
  wsNoBanc.autoFilter = { from: 'A1', to: wsNoBanc.lastColumn.letter + '1' };

  // ── Hoja 3: Sin resolver ─────────────────────────────────────────────────────
  const wsSinResolver = wb.addWorksheet('Sin resolver');
  wsSinResolver.columns = [
    ...COLS_ORIGINALES.map(c => ({ header: c.header, key: c.key, width: c.width })),
    { header: 'Razón',   key: 'razon',   width: 20 },
    { header: 'Detalle', key: 'detalle', width: 60 },
  ];
  styleHeader(wsSinResolver);

  const RAZON_LABEL = {
    sin_factura:     'Factura no encontrada',
    sin_pedido:       'Sin pedido relacionado',
    sin_cxc_en_kore:  'CxC no encontrada en Kore',
  };

  for (const r of (resultado.detalleSinResolver ?? [])) {
    const row = wsSinResolver.addRow({
      ...baseRowValues(r),
      razon:   RAZON_LABEL[r.razon] ?? r.razon,
      detalle: r.detalle,
    });
    row.eachCell(cell => { cell.fill = ERR_FILL; });
  }
  applyMoneyFormats(wsSinResolver);
  wsSinResolver.autoFilter = { from: 'A1', to: wsSinResolver.lastColumn.letter + '1' };

  return wb.xlsx.writeBuffer();
}

module.exports = { procesarFormasPagoCxc, generarExcelFormasPagoCxc };
