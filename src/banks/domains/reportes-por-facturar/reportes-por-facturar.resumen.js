'use strict';

/**
 * reportes-por-facturar.resumen.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Hoja "RESUMEN POR FACTURAR" (2026-11-12, pedido del usuario: "¿se podría
 * hacer así?" con su Excel "RESUMEN POR FACTURAR.xls" — "para iniciar, en una
 * hoja aparte"). Es la "RELACIÓN DE CHEQUES Y DOCUMENTOS POR FACTURAR DEL DÍA":
 * un renglón por forma de pago de cada depósito / cobro del reporte, con sus
 * mismas columnas y fórmulas:
 *
 *   A Base · B Forma de Pago (01, 02, 03, 04, SF…) · C Serie · D Folio (REM) ·
 *   E Factura · F Cliente · G Importe cobrado · H Abonos anteriores ·
 *   I FACTURADO = G + H − J · J BN (bonificación) = 20 % de G + H si B = "01" ·
 *   K Observaciones · L Valida descuento extra · M Caja
 *   + N Fecha · O ID NUMO · P Banco (extra, para no perder de dónde salió)
 *   + Q No. abonos · R Último abono (2026-11-12)
 *   + S ESTATUS (ABONADA / PAGADA) · T Saldo pendiente · U Pagada el
 *   + V–Z desglose de cada abono anterior: fecha, importe, movimiento, forma
 *     y saldo a esa fecha — un sub-renglón por abono; las columnas A–U de la
 *     venta quedan combinadas a lo alto de sus abonos (2026-11-12, pedido del
 *     usuario: "dentro de la celda de esa venta se hagan más celdas").
 *
 * No consulta nada: se arma de las secciones ya calculadas (MOSTRADOR, REF.,
 * FACT ESPECIALES y X FAC CON FICHA); un depósito MIXTO se parte en un renglón
 * por forma. Se dejan VACÍOS para captura a mano: Observaciones ("LIR" =
 * liquidación), Valida descuento extra y Caja (cajera: Kore no la da). Fuera
 * de esta hoja: "Desglose de depósitos" y el bloque de Contabilidad (en su
 * Excel apuntaban a otra hoja, #REF!).
 *
 * Forma de pago → clave: EFECTIVO y ABONO EN EFECTIVO = 01 (con BN);
 * DEPÓSITO EN EFECTIVO = "01 DEP" (va al banco, sin BN, separado como en las
 * demás hojas); TRANSFERENCIA 03, CHEQUE 02, TARJETA 04 (Kore no distingue
 * débito 28), SALDO A FAVOR / OTRAS = SF.
 */

const BN_PORCENTAJE = 0.2;
const CLAVE_FORMA = {
  efectivo: '01', abonos: '01', depositoEfectivo: '01 DEP', transferencia: '03',
  cheque: '02', tarjeta: '04', otras: 'SF',
};
const MESES = ['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO', 'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE'];
const DINERO = '$#,##0.00';
const MORADO = 'FF8E7CC3';
const _relleno = argb => ({ type: 'pattern', pattern: 'solid', fgColor: { argb } });
const AdmZip = require('adm-zip');

const _r2 = n => Math.round((Number(n) || 0) * 100) / 100;
const _serie = v => String(v ?? '').trim().toUpperCase();
const _diaMx = f => (f ? new Date(new Date(f).getTime() - 6 * 3600000).toISOString().slice(0, 10) : null);
// Día del renglón: Bancos = día del estado de cuenta (medianoche UTC); Kore = instante real.
const _diaFila = d => (!d.fecha ? null : String(d.fecha).length === 10 ? d.fecha : d.deKore ? _diaMx(d.fecha) : new Date(d.fecha).toISOString().slice(0, 10));

/**
 * ESTATUS de la venta (2026-11-12, pedido del usuario: "saber si está abonada o
 * pagada por completo"), al momento de calcular / ↻ Actualizar el reporte:
 * con el historial de Kore, PAGADA si todas sus remisiones quedaron en saldo 0
 * (fecha = la última en saldarse), si no ABONADA con lo que deben. Sin
 * historial: X FAC CON FICHA (todavía deben) → ABONADA; PENDIENTE GENERAR
 * FACTURA (saldo 0 por definición) → PAGADA; lo demás "SIN DATO".
 */
function estatusDe(d) {
  const hs = d.historialAbonos ?? [];
  if (hs.length) {
    const deben = hs.filter(h => !h.saldadaEl);
    if (!deben.length) return { estatus: 'PAGADA', saldo: 0, pagadaEl: hs.map(h => _diaMx(h.saldadaEl)).sort().at(-1) };
    return { estatus: 'ABONADA', saldo: _r2(deben.reduce((s, h) => s + (Number(h.saldo) || 0), 0)), pagadaEl: null };
  }
  if ((Number(d.saldo) || 0) > 0.005 || d._seccion === 'ficha') return { estatus: 'ABONADA', saldo: d.saldo != null ? _r2(d.saldo) : null, pagadaEl: null };
  if (d._seccion === 'pendiente') return { estatus: 'PAGADA', saldo: 0, pagadaEl: null };
  return { estatus: 'SIN DATO', saldo: null, pagadaEl: null };
}

/** Depósitos de las secciones que entran al resumen (con la factura, si ya tiene). */
function _depositos(secciones) {
  const deBloques = (bloques, seccion) => (bloques ?? []).flatMap(b => (b.grupos ?? []).flatMap(g => g.depositos ?? [])
    .map(d => ({ ...d, _seccion: seccion ?? (b.tipo === 'pendiente' ? 'pendiente' : null) })));
  return [
    ...(secciones?.mostrador ?? []).flatMap(a => deBloques(a.bloques)),
    ...(secciones?.ref ?? []).flatMap(a => deBloques(a.bloques)),
    ...deBloques(secciones?.especiales),
    ...deBloques(secciones?.ficha ? [secciones.ficha] : [], 'ficha'),
  ];
}

/**
 * Renglones del resumen, ordenados por almacén (A0, I0…), fecha y cliente.
 * Abonos anteriores = abonos de sus remisiones de días ANTERIORES al renglón
 * (historial de Kore), sin contar los que ya salen como renglón propio en el
 * resumen (mismo ID NUMO) para no sumarlos dos veces. Van solo en el primer
 * renglón de un depósito mixto.
 */
function filasResumen(secciones, formaPagoDe) {
  const deps = _depositos(secciones);
  const idsEnResumen = new Set(deps.map(d => String(d.idNumo ?? '')).filter(id => id && id !== 'S/R'));
  const filas = [];
  for (const d of deps) {
    const remisiones = d.remisiones ?? [];
    const serie = _serie(String(remisiones[0] ?? '').split('-')[0]) || _serie(String(d.factura ?? '').split('-')[0]);
    const dia = _diaFila(d);
    let abonosAnteriores = 0;
    const abonos = []; // los que suman en ABONOS ANTERIORES, con su fecha (2026-11-12)
    for (const h of d.historialAbonos ?? []) {
      for (const a of h.abonos ?? []) {
        const imp = Number(a.importe) || 0;
        if (imp <= 0 || !dia || !a.fecha || _diaMx(a.fecha) >= dia) continue;
        if (a.idNumo && (String(a.idNumo) === String(d.idNumo ?? '') || idsEnResumen.has(String(a.idNumo)))) continue;
        abonosAnteriores += imp;
        abonos.push({ dia: _diaMx(a.fecha), importe: _r2(imp), movimiento: a.movimiento ?? '', forma: a.forma ?? '', remision: h.remision, saldo: a.saldo ?? null });
      }
    }
    const estatus = estatusDe(d);
    const { partes } = formaPagoDe(d);
    const formas = Object.entries(partes).filter(([, m]) => Math.abs(m) > 0.005);
    const lista = formas.length ? formas : [[null, Number(d.importe) || 0]];
    lista.forEach(([forma, monto], i) => filas.push({
      serie,
      forma: forma ? CLAVE_FORMA[forma] ?? '' : '',
      folio: remisiones.map(r => r.slice(r.indexOf('-') + 1)).join(', '),
      factura: d.factura || 'POR FACTURAR',
      cliente: d.cliente || 'CLIENTE NO IDENTIFICADO',
      importe: _r2(monto),
      abonosAnteriores: i === 0 ? _r2(abonosAnteriores) : 0,
      abonos: i === 0 ? abonos.sort((x, y) => x.dia.localeCompare(y.dia)) : [],
      estatus: estatus.estatus,
      saldo: i === 0 ? estatus.saldo : null,
      pagadaEl: estatus.pagadaEl,
      fecha: dia,
      idNumo: d.idNumo ?? '',
      banco: d.banco ?? '',
    }));
  }
  return filas.sort((a, b) => a.serie.localeCompare(b.serie) || String(a.fecha).localeCompare(String(b.fecha)) || a.cliente.localeCompare(b.cliente, 'es'));
}

const _textoDia = dia => { const [y, m, d] = dia.split('-').map(Number); return `${d} ${MESES[m - 1]} ${y}`; };

/**
 * Agrega la hoja al libro. `meta`: { fechaInicio, fechaFin, sucursal }.
 * @returns {boolean} false si no hay renglones.
 */
function agregarHojaResumen(workbook, secciones, formaPagoDe, meta = {}, nombre = 'RESUMEN POR FACTURAR') {
  const filas = filasResumen(secciones, formaPagoDe);
  if (!filas.length) return false;
  const ws = workbook.addWorksheet(nombre);
  ws.columns = [8, 15, 7, 22, 16, 34, 15, 15, 15, 13, 16, 12, 12, 11, 10, 22, 10, 12, 12, 14, 11, 11, 14, 18, 16, 14].map(width => ({ width }));

  const { fechaInicio, fechaFin, sucursal } = meta;
  ws.addRow(['RELACIÓN DE CHEQUES Y DOCUMENTOS']).font = { bold: true, size: 13 };
  const periodo = !fechaInicio ? '' : fechaInicio === fechaFin || !fechaFin ? `POR FACTURAR DEL DÍA ${_textoDia(fechaInicio)}` : `POR FACTURAR DEL ${_textoDia(fechaInicio)} AL ${_textoDia(fechaFin)}`;
  ws.addRow([sucursal ? `SUCURSAL: ${sucursal}` : null, null, null, null, null, periodo]).font = { bold: true };
  ws.addRow([]);
  const enc = ws.addRow(['Base', 'Forma de Pago', 'Serie', 'Folio', 'Factura', 'NOMBRE DEL CLIENTE', 'IMPORTE COBRADO', 'ABONOS ANTERIORES',
    'FACTURADO', 'BN', 'Observaciones', 'Valida Descuento Extra', 'Caja', 'Fecha', 'ID NUMO', 'Banco',
    'No. ABONOS', 'ÚLTIMO ABONO', 'ESTATUS', 'SALDO PENDIENTE', 'PAGADA EL',
    'FECHA ABONO', 'IMPORTE ABONO', 'MOVIMIENTO', 'FORMA', 'SALDO A ESA FECHA']);
  enc.eachCell(c => { c.font = { bold: true, color: { argb: 'FFFFFFFF' } }; c.fill = _relleno(MORADO); c.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true }; });
  ws.views = [{ state: 'frozen', ySplit: enc.number }];

  const primera = enc.number + 1;
  const _dia = v => (v ? new Date(`${v}T12:00:00Z`) : null);
  // Renglones seguidos, sin vacío entre almacenes, para poder usar los filtros
  // (2026-11-12, pedido del usuario). Una venta con N abonos ocupa N
  // sub-renglones: los abonos en V–Z, uno por sub-renglón; A–U combinadas.
  // En las combinadas el texto se repite por debajo (Excel no lo enseña, pero
  // el filtro sí lo ve: filtrar por Serie / Cliente no esconde los abonos);
  // los importes van solo en el primer sub-renglón para no sumar doble.
  const SOLO_PRIMERO = new Set([7, 8, 9, 10, 17, 20]); // G H I J, No. abonos, saldo
  const combinadas = [];
  for (const f of filas) {
    const altura = Math.max(f.abonos.length, 1);
    const venta = ['Kore', f.forma, f.serie, f.folio, f.factura, f.cliente, f.importe, f.abonosAnteriores, null, null, null, null, null,
      _dia(f.fecha), f.idNumo, f.banco,
      f.abonos.length || null, f.abonos.length ? _dia(f.abonos[f.abonos.length - 1].dia) : null,
      f.estatus, f.saldo, _dia(f.pagadaEl)];
    let n = null;
    for (let k = 0; k < altura; k++) {
      const a = f.abonos[k];
      const valores = venta.map((v, c) => (k > 0 && SOLO_PRIMERO.has(c + 1) ? null : v));
      const abono = a ? [_dia(a.dia), a.importe, f.abonos.some(x => x.remision !== a.remision) ? `${a.movimiento} · REM ${a.remision}` : a.movimiento, a.forma, a.saldo] : [];
      const row = ws.addRow([...valores, ...abono]);
      if (k === 0) n = row.number;
      for (const c of [7, 8, 9, 10, 20, 23, 26]) row.getCell(c).numFmt = DINERO;
      for (const c of [14, 18, 21, 22]) row.getCell(c).numFmt = 'd/m/yy';
      row.getCell(19).font = { bold: true, color: { argb: f.estatus === 'PAGADA' ? 'FF15803D' : f.estatus === 'ABONADA' ? 'FFB45309' : 'FF6B7280' } };
    }
    const bn = f.forma === '01' ? _r2((f.importe + f.abonosAnteriores) * BN_PORCENTAJE) : 0;
    ws.getCell(n, 10).value = { formula: `IF(B${n}="01",(G${n}+H${n})*${BN_PORCENTAJE},0)`, result: bn };
    ws.getCell(n, 9).value = { formula: `G${n}+H${n}-J${n}`, result: _r2(f.importe + f.abonosAnteriores - bn) };
    for (let c = 1; c <= 21; c++) {
      ws.getCell(n, c).alignment = { vertical: 'middle', wrapText: c === 4 };
      if (altura > 1) combinadas.push(`${ws.getCell(n, c).address}:${ws.getCell(n + altura - 1, c).address}`);
    }
  }
  // Se combinan al final en el .xlsx (aplicarCombinadas): mergeCells de ExcelJS
  // borraría el texto de abajo y el filtro escondería los abonos.
  if (combinadas.length) (workbook._combinadasOcultas ??= []).push({ ws, rangos: combinadas });
  const ultima = ws.lastRow.number;
  ws.autoFilter = { from: { row: enc.number, column: 1 }, to: { row: ultima, column: 26 } };
  const rango = col => `${col}${primera}:${col}${ultima}`;
  const suma = (col, filtro) => filas.filter(filtro ?? (() => true)).reduce((s, f) => s + (Number(f[col]) || 0), 0);
  const bnDe = f => (f.forma === '01' ? _r2((f.importe + f.abonosAnteriores) * BN_PORCENTAJE) : 0);
  const totBn = _r2(filas.reduce((s, f) => s + bnDe(f), 0));
  const totG = _r2(suma('importe'));
  const totH = _r2(suma('abonosAnteriores'));
  const facturadoDe = f => _r2(f.importe + f.abonosAnteriores - bnDe(f));
  const totI = _r2(filas.reduce((s, f) => s + facturadoDe(f), 0));

  ws.addRow([]);
  const tot = ws.addRow([null, null, null, null, null, 'TOTALES']);
  tot.getCell(7).value = { formula: `SUBTOTAL(9,${rango('G')})`, result: totG };
  tot.getCell(8).value = { formula: `SUBTOTAL(9,${rango('H')})`, result: totH };
  tot.getCell(9).value = { formula: `SUBTOTAL(9,${rango('I')})`, result: totI };
  tot.getCell(10).value = { formula: `SUBTOTAL(9,${rango('J')})`, result: totBn };
  tot.getCell(20).value = { formula: `SUBTOTAL(9,${rango('T')})`, result: _r2(filas.reduce((s, f) => s + (Number(f.saldo) || 0), 0)) };
  tot.getCell(23).value = { formula: `SUBTOTAL(9,${rango('W')})`, result: _r2(filas.reduce((s, f) => s + f.abonos.reduce((x, a) => x + a.importe, 0), 0)) };
  tot.font = { bold: true };
  for (const c of [7, 8, 9, 10, 20, 23]) tot.getCell(c).numFmt = DINERO;
  ws.addRow([]);
  ws.addRow([]);

  // ── Cuadre (mismas fórmulas que su hoja) ─────────────────────────────────────
  // Izquierda: formas de pago (efectivo sobre lo cobrado, las demás sobre lo
  // facturado).
  // Derecha: facturación y otros movimientos ("LIR" en Observaciones).
  const sumIf = (colCrit, crit, colSuma) => `SUMIF($${colCrit}$${primera}:$${colCrit}$${ultima},"${crit}",$${colSuma}$${primera}:$${colSuma}$${ultima})`;
  const resultado = (crit, colSuma) => _r2(filas.filter(f => f.forma === crit).reduce((s, f) => s + (colSuma === 'G' ? f.importe : facturadoDe(f)), 0));
  const izquierda = [
    ['Formas de Pago', null, true],
    ['Efectivo (01)', { formula: sumIf('B', '01', 'G'), result: resultado('01', 'G') }],
    ['Depósito en efectivo (01 DEP)', { formula: sumIf('B', '01 DEP', 'I'), result: resultado('01 DEP', 'I') }],
    ['Transferencia (03)', { formula: sumIf('B', '03', 'I'), result: resultado('03', 'I') }],
    ['Cheque (02)', { formula: sumIf('B', '02', 'I'), result: resultado('02', 'I') }],
    ['Tarjeta (04)', { formula: sumIf('B', '04', 'I'), result: resultado('04', 'I') }],
    ['Tarjeta de Débito (28)', { formula: sumIf('B', '28', 'I'), result: 0 }],
    ['Saldo a Favor (SF)', { formula: sumIf('B', 'SF', 'I'), result: resultado('SF', 'I') }],
    ['Puntos (05)', { formula: sumIf('B', '05', 'I'), result: 0 }],
  ];
  const inicio = ws.lastRow.number + 1;
  const filaTotalValores = inicio + izquierda.length;
  const filaEfe = inicio + 1;
  izquierda.push(['Total Valores', { formula: `SUM(B${inicio + 1}:B${filaTotalValores - 1})`, result: _r2(izquierda.slice(1).reduce((s, [, v]) => s + v.result, 0)) }, true]);
  // Sin "Efectivo (01) Marisol / Elizabeth / Maritza" ni "Efectivo por
  // Entregar" (2026-11-12, usuario: "no iría, no se sabe por caja" — Kore no
  // dice qué cajera cobró).

  const totKore = totI;
  const liquidacion = 0;
  const derecha = [
    ['Facturación Global', 'Importe', true],
    ['Kore ERP', { formula: sumIf('A', 'Kore', 'I'), result: totKore }],
    ['Total Facturación Global', { formula: `G${inicio + 1}`, result: totKore }, true],
    [null],
    ['Otros Movimientos', 'Importe', true],
    ['Liquidación (LIR)', { formula: sumIf('K', 'LIR', 'G'), result: liquidacion }],
    ['Abono', null],
    ['Total Otros Movimientos', { formula: `SUM(G${inicio + 5}:G${inicio + 6})`, result: liquidacion }, true],
    [null],
    ['Global + Liquidación + Abonos', { formula: `G${inicio + 2}+G${inicio + 7}`, result: totKore }, true],
    ['Comisión de Facturado', { formula: `B${filaEfe}-G${inicio + 9}`, result: _r2(izquierda[1][1].result - totKore) }],
    ['Total Movimientos Cobrados', { formula: `G${inicio + 9}+G${inicio + 10}`, result: izquierda[1][1].result }, true],
  ];
  const n = Math.max(izquierda.length, derecha.length);
  for (let i = 0; i < n; i++) {
    const row = ws.addRow([]);
    const [ei, vi, bi] = izquierda[i] ?? [];
    const [ed, vd, bd] = derecha[i] ?? [];
    if (ei) { row.getCell(1).value = ei; row.getCell(1).font = { bold: !!bi }; }
    if (vi !== undefined && vi !== null) { row.getCell(2).value = vi; row.getCell(2).numFmt = DINERO; row.getCell(2).font = { bold: !!bi }; }
    if (ed) { row.getCell(6).value = ed; row.getCell(6).font = { bold: !!bd }; }
    if (vd !== undefined && vd !== null) { row.getCell(7).value = vd; row.getCell(7).numFmt = DINERO; row.getCell(7).font = { bold: !!bd }; }
  }
  ws.getColumn(1).width = 30;
  return true;
}

/**
 * Combina las celdas de la venta en el .xlsx ya generado SIN borrar el texto
 * de las celdas de abajo (Excel lo guarda oculto y el filtro lo usa). ExcelJS
 * no lo permite: su mergeCells vacía esas celdas. Inserta <mergeCells> justo
 * después de <autoFilter> (el orden que pide el formato).
 */
function aplicarCombinadas(buffer, workbook) {
  const pendientes = (workbook?._combinadasOcultas ?? []).filter(p => p.rangos.length);
  if (!pendientes.length) return buffer;
  const zip = new AdmZip(Buffer.from(buffer));
  for (const { ws, rangos } of pendientes) {
    const nombre = `xl/worksheets/sheet${ws.id}.xml`;
    const entry = zip.getEntry(nombre);
    if (!entry) continue;
    let xml = entry.getData().toString('utf8');
    if (xml.includes('<mergeCells')) continue;
    const tag = `<mergeCells count="${rangos.length}">${rangos.map(r => `<mergeCell ref="${r}"/>`).join('')}</mergeCells>`;
    const filtro = xml.match(/<autoFilter[^>]*\/>|<autoFilter[\s\S]*?<\/autoFilter>/);
    xml = filtro ? xml.replace(filtro[0], filtro[0] + tag) : xml.replace('</sheetData>', `</sheetData>${tag}`);
    zip.updateFile(nombre, Buffer.from(xml, 'utf8'));
  }
  return zip.toBuffer();
}

module.exports = { agregarHojaResumen, aplicarCombinadas, filasResumen, estatusDe, CLAVE_FORMA, BN_PORCENTAJE };
