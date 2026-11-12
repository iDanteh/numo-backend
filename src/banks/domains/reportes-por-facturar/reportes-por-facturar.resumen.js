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
 *   + Q No. abonos · R Último abono · S Fecha de cada abono (2026-11-12)
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

const _r2 = n => Math.round((Number(n) || 0) * 100) / 100;
const _serie = v => String(v ?? '').trim().toUpperCase();
const _diaMx = f => (f ? new Date(new Date(f).getTime() - 6 * 3600000).toISOString().slice(0, 10) : null);
// Día del renglón: Bancos = día del estado de cuenta (medianoche UTC); Kore = instante real.
const _diaFila = d => (!d.fecha ? null : String(d.fecha).length === 10 ? d.fecha : d.deKore ? _diaMx(d.fecha) : new Date(d.fecha).toISOString().slice(0, 10));

/** Depósitos de las secciones que entran al resumen (con la factura, si ya tiene). */
function _depositos(secciones) {
  const deBloques = bloques => (bloques ?? []).flatMap(b => (b.grupos ?? []).flatMap(g => g.depositos ?? []));
  return [
    ...(secciones?.mostrador ?? []).flatMap(a => deBloques(a.bloques)),
    ...(secciones?.ref ?? []).flatMap(a => deBloques(a.bloques)),
    ...deBloques(secciones?.especiales),
    ...deBloques(secciones?.ficha ? [secciones.ficha] : []),
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
        abonos.push({ dia: _diaMx(a.fecha), importe: _r2(imp), movimiento: a.movimiento ?? '', forma: a.forma ?? '', remision: h.remision });
      }
    }
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
      fecha: dia,
      idNumo: d.idNumo ?? '',
      banco: d.banco ?? '',
    }));
  }
  return filas.sort((a, b) => a.serie.localeCompare(b.serie) || String(a.fecha).localeCompare(String(b.fecha)) || a.cliente.localeCompare(b.cliente, 'es'));
}

// "5/11/26 $250.00 · ABO-261010874 · EFECTIVO" (+ "· REM …" si el renglón tiene varias remisiones).
function textoAbonoResumen(a, _i, todos) {
  const [y, m, d] = a.dia.split('-');
  const varias = new Set(todos.map(x => x.remision)).size > 1;
  const dinero = `$${a.importe.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  return [`${Number(d)}/${Number(m)}/${y.slice(2)} ${dinero}`, a.movimiento, a.forma, varias ? `REM ${a.remision}` : null].filter(Boolean).join(' · ');
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
  ws.columns = [8, 15, 7, 22, 16, 34, 15, 15, 15, 13, 16, 12, 12, 11, 10, 22, 10, 12, 60].map(width => ({ width }));

  const { fechaInicio, fechaFin, sucursal } = meta;
  ws.addRow(['RELACIÓN DE CHEQUES Y DOCUMENTOS']).font = { bold: true, size: 13 };
  const periodo = !fechaInicio ? '' : fechaInicio === fechaFin || !fechaFin ? `POR FACTURAR DEL DÍA ${_textoDia(fechaInicio)}` : `POR FACTURAR DEL ${_textoDia(fechaInicio)} AL ${_textoDia(fechaFin)}`;
  ws.addRow([sucursal ? `SUCURSAL: ${sucursal}` : null, null, null, null, null, periodo]).font = { bold: true };
  ws.addRow([]);
  const enc = ws.addRow(['Base', 'Forma de Pago', 'Serie', 'Folio', 'Factura', 'NOMBRE DEL CLIENTE', 'IMPORTE COBRADO', 'ABONOS ANTERIORES',
    'FACTURADO', 'BN', 'Observaciones', 'Valida Descuento Extra', 'Caja', 'Fecha', 'ID NUMO', 'Banco',
    'No. ABONOS', 'ÚLTIMO ABONO', 'FECHA DE CADA ABONO']);
  enc.eachCell(c => { c.font = { bold: true, color: { argb: 'FFFFFFFF' } }; c.fill = _relleno(MORADO); c.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true }; });
  ws.views = [{ state: 'frozen', ySplit: enc.number }];

  const primera = enc.number + 1;
  // Renglones seguidos, sin vacío entre almacenes, para poder usar los filtros
  // (2026-11-12, pedido del usuario).
  for (const f of filas) {
    const row = ws.addRow(['Kore', f.forma, f.serie, f.folio, f.factura, f.cliente, f.importe, f.abonosAnteriores, null, null, null, null, null,
      f.fecha ? new Date(`${f.fecha}T12:00:00Z`) : null, f.idNumo, f.banco,
      // Abonos anteriores (2026-11-12, pedido del usuario: "cuántos abonos y la
      // fecha de cada abono"): uno por línea en la misma celda, para no romper los filtros.
      f.abonos.length || null,
      f.abonos.length ? new Date(`${f.abonos[f.abonos.length - 1].dia}T12:00:00Z`) : null,
      f.abonos.map(textoAbonoResumen).join('\n') || null]);
    const n = row.number;
    const bn = f.forma === '01' ? _r2((f.importe + f.abonosAnteriores) * BN_PORCENTAJE) : 0;
    row.getCell(10).value = { formula: `IF(B${n}="01",(G${n}+H${n})*${BN_PORCENTAJE},0)`, result: bn };
    row.getCell(9).value = { formula: `G${n}+H${n}-J${n}`, result: _r2(f.importe + f.abonosAnteriores - bn) };
    for (const c of [7, 8, 9, 10]) row.getCell(c).numFmt = DINERO;
    row.getCell(14).numFmt = 'd/m/yy';
    row.getCell(18).numFmt = 'd/m/yy';
    row.getCell(4).alignment = { wrapText: true };
    row.getCell(19).alignment = { wrapText: true, vertical: 'top' };
  }
  const ultima = ws.lastRow.number;
  ws.autoFilter = { from: { row: enc.number, column: 1 }, to: { row: ultima, column: 19 } };
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
  tot.font = { bold: true };
  for (const c of [7, 8, 9, 10]) tot.getCell(c).numFmt = DINERO;
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

module.exports = { agregarHojaResumen, filasResumen, CLAVE_FORMA, BN_PORCENTAJE };
