'use strict';

/**
 * reportes-por-facturar.secciones.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Las 5 pestañas del reporte "Por facturar" (2026-10-28, pedido del usuario),
 * cada una con su propio Excel:
 *
 *   global          EN FACTURA GLOBAL — una tabla por factura global
 *   pendiente       PENDIENTE GENERAR FACTURA (sin factura global)
 *   ppd             PPD POR FACTURAR YA FACTURADO A NOMBRE DEL CLIENTE
 *   pendientesPago  CON PENDIENTES DE PAGO — POR FACTURAR, TODAVÍA DEBEN SALDO
 *   efectivo        DEPÓSITOS EN EFECTIVO — SIN ASOCIAR A NINGUNA VENTA
 *
 * Las cuatro primeras: separadas por almacén (serie de la remisión de cada
 * depósito → una hoja por almacén), depósitos agrupados por cliente con
 * subtotal, y el cobro de las remisiones por forma de pago (efectivo,
 * tarjeta, transferencia, cheque, otras; "MIXTO" si hay más de una) con su
 * total al final de cada tabla — en la global, el efectivo de cada factura.
 * Efectivo sin asociar no tiene remisión: una sola hoja.
 */

const { tablasMostrador, agregarHojaDepositosEfectivo } = require('../cfdi-mapping/mostrador-hoja');
const { FORMAS_PAGO } = require('../cfdi-mapping/mostrador.service');

const SECCIONES = {
  global:         { titulo: 'EN FACTURA GLOBAL',                                          hoja: 'GLOBAL',     factura: true },
  pendiente:      { titulo: 'PENDIENTE GENERAR FACTURA',                                  hoja: 'PENDIENTE',  identificado: true },
  ppd:            { titulo: 'PPD POR FACTURAR YA FACTURADO A NOMBRE DEL CLIENTE',         hoja: 'PPD CLIENTE', factura: true },
  pendientesPago: { titulo: 'CON PENDIENTES DE PAGO — POR FACTURAR, TODAVÍA DEBEN SALDO', hoja: 'PEND PAGO',  saldo: true, identificado: true },
  efectivo:       { titulo: 'DEPÓSITOS EN EFECTIVO — SIN ASOCIAR A NINGUNA VENTA',        hoja: 'EFECTIVO' },
};

const ETIQUETA_FORMA = {
  efectivo: 'EFECTIVO', depositoEfectivo: 'DEPÓSITO EN EFECTIVO', transferencia: 'TRANSFERENCIA', tarjeta: 'TARJETA',
  cheque: 'CHEQUE', abonos: 'ABONOS EN EFECTIVO', otras: 'OTRAS',
};
const NO_IDENTIFICADO = 'CLIENTE NO IDENTIFICADO';

const _r2 = n => Math.round((Number(n) || 0) * 100) / 100;
const _serie = v => String(v ?? '').trim().toUpperCase();
const _folio = f => `${f.serie ?? ''}-${f.folio ?? ''}`;
// Almacén (serie A0, I0…) de un depósito: el de su remisión; sin remisión, el
// de la factura.
const _serieDeposito = (d, serieFactura = '') => _serie(String(d.remisiones?.[0] ?? '').split('-')[0]) || _serie(serieFactura);

/** Suma de importe, aplicado, saldo y cada forma de pago. */
function totales(depositos) {
  const suma = fn => _r2(depositos.reduce((s, d) => s + (Number(fn(d)) || 0), 0));
  return {
    importe:  suma(d => d.importe),
    aplicado: suma(d => d.aplicado),
    saldo:    suma(d => d.saldo),
    formas:   Object.fromEntries(FORMAS_PAGO.map(f => [f, suma(d => d.formasPago?.[f])])),
  };
}

/** Depósitos agrupados por cliente: alfabético, "CLIENTE NO IDENTIFICADO" al final. */
function agruparPorCliente(depositos) {
  const porCliente = new Map();
  for (const d of depositos) {
    const c = d.cliente || NO_IDENTIFICADO;
    if (!porCliente.has(c)) porCliente.set(c, []);
    porCliente.get(c).push(d);
  }
  return [...porCliente.keys()]
    .sort((a, b) => (a === NO_IDENTIFICADO) - (b === NO_IDENTIFICADO) || a.localeCompare(b, 'es'))
    .map(cliente => ({ cliente, depositos: porCliente.get(cliente), totales: totales(porCliente.get(cliente)) }));
}

/**
 * Las 5 secciones listas para la pantalla y el Excel:
 * { global: [almacén], pendiente: [...], ppd: [...], pendientesPago: [...], efectivo: [...] }
 * almacén = { serie, sucursal, bloques: [{ titulo, factura?, fechaFactura?, totalFactura?,
 *   grupos: [{ cliente, depositos, totales }], totales }] }
 */
function armarSecciones(datos, sucursalDe = () => null) {
  const t = tablasMostrador(datos);
  const porAlmacen = (items) => {
    // items: [{ serie, bloque }] — bloque = { titulo, factura?, depositos, ... }
    const mapa = new Map();
    for (const { serie, bloque } of items) {
      if (!mapa.has(serie)) mapa.set(serie, []);
      const bloques = mapa.get(serie);
      // Mismo título en el mismo almacén → se juntan (tablas de una sola pieza).
      const igual = bloques.find(b => b.titulo === bloque.titulo && b.factura === bloque.factura);
      if (igual) igual.depositos.push(...bloque.depositos);
      else bloques.push({ ...bloque, depositos: [...bloque.depositos] });
    }
    return [...mapa.entries()]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([serie, bloques]) => ({
        serie,
        sucursal: sucursalDe(serie),
        bloques: bloques.map(({ depositos, ...b }) => ({ ...b, grupos: agruparPorCliente(depositos), totales: totales(depositos) })),
      }));
  };
  // Reparte los depósitos de cada factura por el almacén de su remisión.
  const deFacturas = (facturas, bloqueDe) => facturas.flatMap(f => {
    const porSerie = new Map();
    for (const d of f.depositos ?? []) {
      const s = _serieDeposito(d, f.serie);
      if (!porSerie.has(s)) porSerie.set(s, []);
      porSerie.get(s).push({ ...d, factura: _folio(f) });
    }
    return [...porSerie.entries()].map(([serie, depositos]) => ({ serie, bloque: { ...bloqueDe(f), depositos } }));
  });
  const sueltos = (depositos, titulo) => depositos.map(d => ({ serie: _serieDeposito(d), bloque: { titulo, depositos: [d] } }));

  return {
    global: porAlmacen(deFacturas(t.facturasGlobales, f => ({
      titulo: `${SECCIONES.global.titulo} — ${_folio(f)}`, factura: _folio(f), fechaFactura: f.fechaFactura ?? null, totalFactura: f.totalFactura ?? null,
    }))),
    pendiente:      porAlmacen(sueltos(t.depositosPendientes, SECCIONES.pendiente.titulo)),
    ppd:            porAlmacen(deFacturas(t.ppdFacturadoCliente, () => ({ titulo: SECCIONES.ppd.titulo }))),
    pendientesPago: porAlmacen(sueltos(datos?.depositosPendientesPago ?? [], SECCIONES.pendientesPago.titulo)),
    efectivo:       datos?.depositosEfectivoSinAsociar ?? [],
  };
}

// Nombre de hoja de Excel: máx. 31 caracteres y sin : \ / ? * [ ].
const nombreHoja = (...partes) => partes.filter(Boolean).join(' - ').replace(/[:\\/?*[\]]/g, ' ').slice(0, 31);

const fechaHoraMx = d => (d ? new Date(d).toLocaleString('es-MX', {
  timeZone: 'America/Mexico_City', day: 'numeric', month: 'numeric', year: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false,
}).replace(', ', ' ') : '');
const fechaCortaMx = d => new Date(d).toLocaleDateString('es-MX', { timeZone: 'America/Mexico_City', day: 'numeric', month: 'numeric', year: '2-digit' });

/** Hoja de un almacén de una sección (global, pendiente, ppd, pendientesPago). */
function agregarHojaAlmacen(workbook, seccion, almacen, nombre) {
  const cfg = SECCIONES[seccion];
  const ws = workbook.addWorksheet(nombre);
  const columnas = [
    { key: 'fecha', header: 'FECHA', width: 11 },
    { key: 'descripcion', header: 'DESCRIPCIÓN', width: 40 },
    { key: 'importe', header: 'IMPORTE', width: 14, dinero: true },
    { key: 'aplicado', header: 'APLICADO A FACTURA', width: 14, dinero: true },
    { key: 'banco', header: 'BANCO', width: 11 },
    { key: 'rem', header: 'REM', width: 16 },
    { key: 'cliente', header: 'CLIENTE', width: 30 },
    { key: 'idNumo', header: 'ID NUMO', width: 10 },
    { key: 'estatus', header: 'ESTATUS', width: 14 },
    ...(cfg.factura ? [{ key: 'factura', header: 'FACTURA', width: 15 }] : []),
    ...(cfg.saldo ? [{ key: 'saldo', header: 'SALDO PENDIENTE', width: 14, dinero: true }] : []),
    ...FORMAS_PAGO.map(f => ({ key: f, header: ETIQUETA_FORMA[f], width: 14, dinero: true })),
    { key: 'mixto', header: 'MIXTO', width: 8 },
    ...(cfg.identificado ? [{ key: 'identificado', header: 'IDENTIFICADO EL', width: 16 }] : []),
  ];
  ws.columns = columnas.map(({ key, width }) => ({ key, width }));
  const n = columnas.length;
  const relleno = argb => ({ type: 'pattern', pattern: 'solid', fgColor: { argb } });
  const encabezado = (row, argb) => row.eachCell(cell => {
    cell.font = { bold: true, color: { argb: 'FFFFFFFF' } };
    cell.fill = relleno(argb);
    cell.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true };
  });
  const formatoDinero = row => { for (const c of columnas) if (c.dinero) row.getCell(c.key).numFmt = '$#,##0.00'; };
  const filaTotales = (etiqueta, tot, estilo) => {
    const row = ws.addRow({
      descripcion: etiqueta, importe: tot.importe, aplicado: tot.aplicado,
      ...(cfg.saldo ? { saldo: tot.saldo } : {}), ...tot.formas,
    });
    row.font = estilo;
    row.getCell('descripcion').alignment = { horizontal: 'right', wrapText: true };
    formatoDinero(row);
    return row;
  };

  for (const b of almacen.bloques) {
    const titulo = ws.addRow([b.fechaFactura ? `${b.titulo} (${fechaCortaMx(b.fechaFactura)})` : b.titulo]);
    ws.mergeCells(titulo.number, 1, titulo.number, n);
    encabezado(titulo, 'FF5B4A9E');
    encabezado(ws.addRow(columnas.map(c => c.header)), 'FF8E7CC3');
    for (const g of b.grupos) {
      const nombreCliente = ws.addRow([g.cliente]);
      ws.mergeCells(nombreCliente.number, 1, nombreCliente.number, n);
      nombreCliente.font = { bold: true };
      nombreCliente.getCell(1).fill = relleno('FFE4DEF5');
      for (const d of g.depositos) {
        const row = ws.addRow({
          fecha: d.fecha ? new Date(d.fecha) : null, descripcion: d.descripcion, importe: d.importe, aplicado: d.aplicado,
          banco: d.banco, rem: (d.remisiones ?? []).join('\n'), cliente: d.cliente || NO_IDENTIFICADO, idNumo: d.idNumo,
          estatus: d.estatus || 'POR FACTURAR',
          ...(cfg.factura ? { factura: d.factura ?? '' } : {}),
          ...(cfg.saldo ? { saldo: d.saldo } : {}),
          // Sin desglose (reporte calculado antes del 28-oct o Kore no respondió): vacío.
          ...(d.formasPago ? Object.fromEntries(FORMAS_PAGO.map(f => [f, d.formasPago[f] || null])) : {}),
          mixto: d.formasPago ? (d.formasPago.mixto ? 'SÍ' : 'NO') : '',
          ...(cfg.identificado ? { identificado: fechaHoraMx(d.identificadoEl) } : {}),
        });
        row.alignment = { vertical: 'middle', wrapText: true };
        if (row.getCell('fecha').value) row.getCell('fecha').numFmt = 'd/m/yy';
        formatoDinero(row);
      }
      filaTotales(`Subtotal ${g.cliente}`, g.totales, { bold: true, italic: true });
    }
    filaTotales('TOTAL', b.totales, { bold: true });
    if (b.totalFactura != null) {
      const tf = ws.addRow({ descripcion: 'Total factura', importe: b.totalFactura });
      tf.font = { bold: true };
      tf.getCell('descripcion').alignment = { horizontal: 'right' };
      tf.getCell('importe').numFmt = '$#,##0.00';
    }
    // Efectivo de la tabla (en la global, de cada factura) — pedido explícito.
    const ef = ws.addRow({ descripcion: b.factura && seccion === 'global' ? `EFECTIVO DE LA FACTURA ${b.factura}` : 'TOTAL EFECTIVO', importe: b.totales.formas.efectivo });
    ef.font = { bold: true };
    ef.getCell('descripcion').alignment = { horizontal: 'right' };
    ef.getCell('importe').numFmt = '$#,##0.00';
    ef.getCell('descripcion').fill = relleno('FFFEF3C7');
    ef.getCell('importe').fill = relleno('FFFEF3C7');
    ws.addRow([]);
  }
  return ws;
}

/**
 * Excel de una sección (o de todas si `seccion` no viene): una hoja por
 * almacén; con todas, el nombre de la sección va antes del almacén.
 * @returns {boolean} false si no hubo nada (el llamador pone una hoja vacía).
 */
function agregarHojasSeccion(workbook, secciones, seccion) {
  const lista = seccion ? [seccion] : Object.keys(SECCIONES);
  let hojas = 0;
  for (const s of lista) {
    if (s === 'efectivo') {
      if (agregarHojaDepositosEfectivo(workbook, secciones.efectivo, seccion ? 'DEPÓSITOS EN EFECTIVO' : SECCIONES.efectivo.hoja)) hojas++;
      continue;
    }
    for (const a of secciones[s] ?? []) {
      const almacen = [a.serie || 'SIN SERIE', a.sucursal].filter(Boolean).join(' - ');
      agregarHojaAlmacen(workbook, s, a, seccion ? nombreHoja(almacen) : nombreHoja(SECCIONES[s].hoja, almacen));
      hojas++;
    }
  }
  return hojas > 0;
}

module.exports = { SECCIONES, armarSecciones, agregarHojasSeccion, agruparPorCliente, totales };
