'use strict';

/**
 * reportes-por-facturar.secciones.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Pestañas y hojas del reporte "Por facturar" con el formato del Excel del
 * usuario "MOSTRADOR 2026" (2026-10-29, pedido del usuario: "así lo quiero"),
 * con los depósitos agrupados por cliente:
 *
 *   mostrador   MOSTRADOR <serie> — una hoja por almacén (2026-10-30), un
 *               bloque por factura global (orden de fecha): FECHA, DESCRIPCIÓN, IMPORTE,
 *               BANCO, REM, CLIENTE, ID NUMO, con los cobros en EFECTIVO /
 *               SALDO A FAVOR de Kore; al final, un bloque sin folio con los
 *               pendientes de generar factura.
 *   ref         REF. <serie> — PPD ya facturado a nombre del cliente, una hoja
 *               por almacén (como "REF. HIDG1" / "REF. CONTRU."), un bloque
 *               por día: FACTURA, ORIGEN (remisiones), CTE, CTE FACTURADO.
 *   especiales  FACT ESPECIALES — igual que REF., cuando la factura salió a
 *               nombre de OTRO (CRISLA, PROEDISA…), no del cliente.
 *   ficha       X FAC CON FICHA DE DEP — depósitos identificados que solo
 *               abonan (las remisiones todavía deben saldo).
 *   efectivo    DEPÓSITOS EN EFECTIVO — sin asociar a ninguna venta.
 *
 * Antes (2026-10-28): una hoja por sección y almacén con columnas de formas
 * de pago — el usuario pidió quitarlo y dejar su formato de 7 columnas.
 */

const { tablasMostrador, agregarHojaDepositosEfectivo } = require('../cfdi-mapping/mostrador-hoja');

const SECCIONES = {
  mostrador:  { titulo: 'MOSTRADOR',              hoja: 'MOSTRADOR' },
  ref:        { titulo: 'REF. — PPD YA FACTURADO A NOMBRE DEL CLIENTE', hoja: 'REF.' },
  especiales: { titulo: 'FACT ESPECIALES',        hoja: 'FACT ESPECIALES' },
  ficha:      { titulo: 'X FAC CON FICHA DE DEP', hoja: 'X FAC CON FICHA DE DEP' },
  efectivo:   { titulo: 'DEPÓSITOS EN EFECTIVO — SIN ASOCIAR A NINGUNA VENTA', hoja: 'DEPÓSITOS EN EFECTIVO' },
};

const NO_IDENTIFICADO = 'CLIENTE NO IDENTIFICADO';

const _r2 = n => Math.round((Number(n) || 0) * 100) / 100;
const _serie = v => String(v ?? '').trim().toUpperCase();
const _folio = f => `${f.serie ?? ''}-${f.folio ?? ''}`;
const _diaMx = f => (f ? new Date(new Date(f).getTime() - 6 * 3600000).toISOString().slice(0, 10) : null);
const _suma = ds => _r2(ds.reduce((s, d) => s + (Number(d.importe) || 0), 0));
// Almacén (serie A0, I0…) de un depósito: el de su remisión; sin remisión, el de la factura.
const _serieDeposito = (d, serieFactura = '') => _serie(String(d.remisiones?.[0] ?? '').split('-')[0]) || _serie(serieFactura);

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
    .map(cliente => ({ cliente, depositos: porCliente.get(cliente), total: _suma(porCliente.get(cliente)) }));
}

const _bloque = (extra, depositos) => ({ ...extra, grupos: agruparPorCliente(depositos), total: _suma(depositos) });

// Nombre comparable: sin acentos, signos ni "SA DE CV" y similares.
const _norm = s => String(s ?? '').toUpperCase().normalize('NFD').replace(/[̀-ͯ]/g, '')
  .replace(/[^A-Z0-9 ]/g, ' ').replace(/\b(S|SA|DE|CV|C|V|RL|SAPI|SC|SAS|DEL|LA|EL|Y)\b/g, ' ').replace(/\s+/g, ' ').trim();
/**
 * Factura a nombre de OTRO (FACT ESPECIALES): el receptor del CFDI no es
 * ninguno de los clientes de las remisiones. Sin receptor o sin cliente no
 * se puede saber → no es especial.
 */
function esEspecial(cliente, receptor) {
  const r = _norm(receptor);
  const cs = String(cliente ?? '').split(' / ').map(_norm).filter(Boolean);
  if (!r || !cs.length) return false;
  return !cs.some(c => c === r || c.includes(r) || r.includes(c));
}

/**
 * Las secciones listas para la pantalla y el Excel.
 * @param {object} datos  `datos` guardados del reporte
 * @param {(serie) => string|null} sucursalDe
 */
function armarSecciones(datos, sucursalDe = () => null, { fechaFin = null } = {}) {
  const t = tablasMostrador(datos);

  // ── MOSTRADOR: globales (depósitos + cobros de caja de Kore) + pendientes ──
  const globales = new Map();
  for (const f of t.facturasGlobales) globales.set(String(f.uuid ?? _folio(f)).toUpperCase(), { ...f, depositos: [...f.depositos] });
  for (const f of datos?.cobrosCajaGlobales ?? []) {
    const k = String(f.uuid ?? _folio(f)).toUpperCase();
    if (globales.has(k)) {
      // Un depósito de Bancos (ID NUMO) no se repite en el bloque.
      const ya = new Set(globales.get(k).depositos.map(d => String(d.idNumo ?? '')).filter(id => id && id !== 'S/R'));
      globales.get(k).depositos.push(...f.depositos.filter(d => !ya.has(String(d.idNumo ?? ''))));
    }
    else globales.set(k, { ...f, depositos: [...f.depositos] });
  }
  // Una hoja por almacén (2026-10-30, pedido del usuario: "mostrador A0,
  // mostrador I0…"): la global va en el de su serie; un pendiente, en el de
  // su remisión.
  const mostradorPorSerie = new Map();
  const almacenMostrador = serie => {
    if (!mostradorPorSerie.has(serie)) mostradorPorSerie.set(serie, { globales: [], pendientes: [] });
    return mostradorPorSerie.get(serie);
  };
  for (const f of globales.values()) almacenMostrador(_serie(f.serie)).globales.push(f);
  for (const d of t.depositosPendientes) almacenMostrador(_serieDeposito(d)).pendientes.push(d);
  const mostrador = [...mostradorPorSerie.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([serie, { globales: gs, pendientes }]) => {
    const bloques = gs
      .sort((a, b) => String(_diaMx(a.fechaFactura) ?? '').localeCompare(String(_diaMx(b.fechaFactura) ?? '')) || _folio(a).localeCompare(_folio(b)))
      .map(f => _bloque({
        tipo: 'global', serie, sucursal: sucursalDe(serie), factura: _folio(f),
        fechaFactura: f.fechaFactura ?? null, totalFactura: f.totalFactura ?? null,
      }, f.depositos.map(d => ({ ...d, factura: _folio(f) }))));
    if (pendientes.length) bloques.push(_bloque({ tipo: 'pendiente', fecha: fechaFin, serie, sucursal: sucursalDe(serie) }, pendientes));
    return { serie, sucursal: sucursalDe(serie), bloques };
  });

  // ── REF. (por almacén) y FACT ESPECIALES ──────────────────────────────────
  const refPorSerie = new Map();
  const especiales = new Map();
  for (const f of t.ppdFacturadoCliente) {
    const dia = _diaMx(f.fechaFactura);
    for (const d of f.depositos) {
      const fila = { ...d, factura: _folio(f), clienteFacturado: f.receptorNombre ?? '' };
      const serie = _serieDeposito(d, f.serie);
      if (esEspecial(d.cliente, f.receptorNombre)) {
        const k = `${dia}|${serie}`;
        if (!especiales.has(k)) especiales.set(k, { fecha: dia, serie, depositos: [] });
        especiales.get(k).depositos.push(fila);
        continue;
      }
      if (!refPorSerie.has(serie)) refPorSerie.set(serie, new Map());
      const porDia = refPorSerie.get(serie);
      if (!porDia.has(dia)) porDia.set(dia, []);
      porDia.get(dia).push(fila);
    }
  }
  const ref = [...refPorSerie.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([serie, porDia]) => ({
    serie,
    sucursal: sucursalDe(serie),
    bloques: [...porDia.entries()].sort(([a], [b]) => String(a).localeCompare(String(b)))
      .map(([fecha, ds]) => _bloque({ fecha, serie, sucursal: sucursalDe(serie) }, ds)),
  }));

  return {
    mostrador,
    ref,
    especiales: [...especiales.values()].sort((a, b) => String(a.fecha).localeCompare(String(b.fecha)) || a.serie.localeCompare(b.serie))
      .map(({ depositos, ...b }) => _bloque({ ...b, sucursal: sucursalDe(b.serie) }, depositos)),
    ficha: _bloque({}, datos?.depositosPendientesPago ?? []),
    efectivo: datos?.depositosEfectivoSinAsociar ?? [],
  };
}

// ── Excel ─────────────────────────────────────────────────────────────────────
// Nombre de hoja de Excel: máx. 31 caracteres y sin : \ / ? * [ ].
const nombreHoja = (...partes) => partes.filter(Boolean).join(' - ').replace(/[:\\/?*[\]]/g, ' ').slice(0, 31);

const MORADO = 'FF8E7CC3';
const CLIENTE = 'FFE4DEF5';
const GRIS = 'FFF3F3F3';
const DINERO = '$#,##0.00';
const _relleno = argb => ({ type: 'pattern', pattern: 'solid', fgColor: { argb } });
// Fecha de celda (solo día, al mediodía UTC para que Excel no la recorra):
// `YYYY-MM-DD` tal cual; instantes reales (factura, cobro de Kore) → día de
// México; `fecha` de Bancos es el día del estado de cuenta a medianoche UTC.
const _fecha = (v, instante = false) => {
  if (!v) return null;
  const dia = String(v).length === 10 ? v : instante ? _diaMx(v) : new Date(v).toISOString().slice(0, 10);
  return new Date(`${dia}T12:00:00Z`);
};

function _encabezado(row, n) {
  for (let i = 1; i <= n; i++) {
    const cell = row.getCell(i);
    cell.font = { bold: true, color: { argb: 'FFFFFFFF' } };
    cell.fill = _relleno(MORADO);
    cell.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true };
    if (cell.value instanceof Date) cell.numFmt = 'd/m/yy';
  }
}

/**
 * Escribe un bloque: encabezado(s), filas agrupadas por cliente (renglón del
 * cliente + depósitos + subtotal) y TOTAL. `fila(d)` → arreglo de celdas.
 */
function _escribirBloque(ws, { encabezados, columnas, grupos, total, fila, colImporte = 3 }) {
  const n = columnas.length;
  for (const e of encabezados) _encabezado(ws.addRow(e), n);
  _encabezado(ws.addRow(columnas), n);
  for (const g of grupos) {
    const nombre = ws.addRow([g.cliente]);
    ws.mergeCells(nombre.number, 1, nombre.number, n);
    nombre.font = { bold: true };
    nombre.getCell(1).fill = _relleno(CLIENTE);
    for (const d of g.depositos) {
      const row = ws.addRow(fila(d));
      row.alignment = { vertical: 'middle', wrapText: true };
      if (row.getCell(1).value instanceof Date) row.getCell(1).numFmt = 'd/m/yy';
      row.getCell(2).fill = _relleno(GRIS);
      row.getCell(colImporte).numFmt = DINERO;
    }
    const sub = ws.addRow([]);
    sub.getCell(2).value = `Subtotal ${g.cliente}`;
    sub.getCell(colImporte).value = g.total;
    sub.getCell(colImporte).numFmt = DINERO;
    sub.font = { bold: true, italic: true };
  }
  const tot = ws.addRow([]);
  tot.getCell(2).value = 'TOTAL';
  tot.getCell(colImporte).value = total;
  tot.getCell(colImporte).numFmt = DINERO;
  tot.font = { bold: true };
  ws.addRow([]);
}

const _rem = d => (d.remisiones ?? []).join('\n');

function agregarHojaMostrador(workbook, bloques, nombre = SECCIONES.mostrador.hoja) {
  if (!bloques.length) return false;
  const ws = workbook.addWorksheet(nombre);
  ws.columns = [{ width: 11 }, { width: 40 }, { width: 14 }, { width: 13 }, { width: 18 }, { width: 32 }, { width: 10 }];
  for (const b of bloques) {
    _escribirBloque(ws, {
      encabezados: [b.tipo === 'global'
        ? [_fecha(b.fechaFactura, true), null, null, null, b.serie, b.factura, 'ID NUMO']
        : [_fecha(b.fecha), 'PENDIENTE GENERAR FACTURA', null, null, null, null, 'ID NUMO']],
      columnas: ['FECHA', 'DESCRIPCIÓN', 'IMPORTE', 'BANCO', 'REM', 'CLIENTE', null],
      grupos: b.grupos,
      total: b.total,
      fila: d => [_fecha(d.fecha, d.deKore), d.descripcion, d.importe, d.banco, _rem(d), d.cliente || NO_IDENTIFICADO, d.idNumo],
    });
  }
  return true;
}

// REF. y FACT ESPECIALES: mismo formato que las hojas "REF. HIDG1" / "FACT ESPECIALES" del usuario.
function agregarHojaRef(workbook, bloques, nombre, etiquetaSerie) {
  if (!bloques.length) return false;
  const ws = workbook.addWorksheet(nombre);
  ws.columns = [{ width: 11 }, { width: 40 }, { width: 14 }, { width: 13 }, { width: 16 }, { width: 22 }, { width: 30 }, { width: 30 }, { width: 11 }];
  for (const b of bloques) {
    const serie = etiquetaSerie ?? b.serie;
    _escribirBloque(ws, {
      encabezados: [[b.sucursal || b.serie || '', _fecha(b.fecha), null, null, null, 'INFORMACIÓN ADICIONAL', null, null, null]],
      columnas: ['FECHA', 'DESCRIPCIÓN', 'IMPORTE', 'BANCO', serie || 'FACTURA', etiquetaSerie ? 'ORIGEN' : `${serie || ''} ORIGEN`.trim(), 'CTE', 'CTE FACTURADO', 'ID DE PAGO'],
      grupos: b.grupos,
      total: b.total,
      fila: d => [_fecha(d.fecha), d.descripcion, d.importe, d.banco, d.factura ?? '', _rem(d), d.cliente || NO_IDENTIFICADO, d.clienteFacturado ?? '', d.idNumo],
    });
  }
  return true;
}

function agregarHojaFicha(workbook, bloque, nombre = SECCIONES.ficha.hoja) {
  if (!bloque.grupos.length) return false;
  const ws = workbook.addWorksheet(nombre);
  ws.columns = [{ width: 11 }, { width: 40 }, { width: 14 }, { width: 13 }, { width: 30 }, { width: 18 }, { width: 15 }, { width: 12 }, { width: 10 }];
  _escribirBloque(ws, {
    encabezados: [['X FAC CON FICHA DE DEP — POR FACTURAR, TODAVÍA DEBEN SALDO', null, null, null, null, null, null, null, null]],
    columnas: ['FECHA', 'DESCRIPCIÓN', 'IMPORTE', 'BANCO', 'CLIENTE', 'REM', 'SALDO PENDIENTE', 'ESTATUS', 'ID NUMO'],
    grupos: bloque.grupos,
    total: bloque.total,
    fila: d => [_fecha(d.fecha), d.descripcion, d.importe, d.banco, d.cliente || NO_IDENTIFICADO, _rem(d), d.saldo ?? null, 'ABONADO', d.idNumo],
  });
  ws.mergeCells(1, 1, 1, 9);
  ws.eachRow(row => { if (typeof row.getCell(7).value === 'number') row.getCell(7).numFmt = DINERO; });
  return true;
}

/**
 * Excel de una sección (o de todas si `seccion` no viene).
 * @returns {boolean} false si no hubo nada (el llamador pone una hoja vacía).
 */
function agregarHojasSeccion(workbook, secciones, seccion) {
  const lista = seccion ? [seccion] : Object.keys(SECCIONES);
  let hojas = 0;
  for (const s of lista) {
    if (s === 'mostrador') {
      for (const a of secciones.mostrador ?? []) {
        if (agregarHojaMostrador(workbook, a.bloques, nombreHoja(`MOSTRADOR ${a.serie || 'SIN SERIE'}`, a.sucursal))) hojas++;
      }
    }
    if (s === 'ref') {
      for (const a of secciones.ref ?? []) {
        if (agregarHojaRef(workbook, a.bloques, nombreHoja(`REF. ${a.serie}`, a.sucursal))) hojas++;
      }
    }
    if (s === 'especiales' && agregarHojaRef(workbook, secciones.especiales, SECCIONES.especiales.hoja, 'FACTURA')) hojas++;
    if (s === 'ficha' && agregarHojaFicha(workbook, secciones.ficha)) hojas++;
    if (s === 'efectivo' && agregarHojaDepositosEfectivo(workbook, secciones.efectivo, SECCIONES.efectivo.hoja)) hojas++;
  }
  return hojas > 0;
}

module.exports = { SECCIONES, armarSecciones, agregarHojasSeccion, agruparPorCliente, esEspecial };
