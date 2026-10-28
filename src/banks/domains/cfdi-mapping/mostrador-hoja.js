'use strict';

/**
 * mostrador-hoja.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Hoja "MOSTRADOR" del Excel (antes dentro de `_construirWorkbookPoliza`,
 * poliza.service.js). Separada el 2026-10-22 para que el reporte "Por
 * facturar" (Reportes) salga exactamente igual que la pestaña de la póliza.
 *
 * `datos` = { movimientosMostrador, depositosPendientesFactura } — los mismos
 * campos que guarda la póliza (ver mostrador.service.js).
 */

/**
 * Separa los datos en las tablas que enseña la hoja (y la pantalla del
 * reporte "Por facturar").
 */
// "IDENTIFICADO EL" (reporte "Por facturar", 2026-10-23): fecha y hora de
// México como texto — un Date en ExcelJS saldría en UTC.
const fechaHoraMx = d => (d ? new Date(d).toLocaleString('es-MX', {
  timeZone: 'America/Mexico_City', day: 'numeric', month: 'numeric', year: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false,
}).replace(', ', ' ') : '');

function tablasMostrador(datos) {
  const mostrador = datos?.movimientosMostrador ?? [];
  // Global y a nombre del cliente: solo depósitos de remisiones PPD "POR
  // FACTURAR" (2026-10-22, pedido del usuario: "se juntan las PPD que estén
  // en una factura global" — antes la global salía con todo, p. ej. tickets
  // PUE "TICKET SENCILLO" pagados por transferencia en la global M0-261000670).
  // `totalAplicado` es el de lo listado.
  const soloPpd = fs => fs
    .map(f => {
      const depositos = (f.depositos ?? []).filter(d => d.ppdPorFacturar);
      return { ...f, depositos, totalAplicado: Math.round(depositos.reduce((s, d) => s + (Number(d.aplicado) || 0), 0) * 100) / 100 };
    })
    .filter(f => f.depositos.length);
  return {
    facturasGlobales:    soloPpd(mostrador.filter(f => f.esGlobal === true)),
    ppdFacturadoCliente: soloPpd(mostrador.filter(f => f.esGlobal === false)),
    sinClasificar:       mostrador.filter(f => typeof f.esGlobal !== 'boolean'),
    depositosPendientes: datos?.depositosPendientesFactura ?? [],
  };
}

/**
 * Agrega la hoja al workbook (ExcelJS). Devuelve false si no hay nada que
 * enseñar (no se crea la hoja).
 */
function agregarHojaMostrador(workbook, datos, nombreHoja = 'MOSTRADOR', { columnaIdentificado = false, agruparPorCliente = false } = {}) {
  // Hoja "MOSTRADOR" (2026-10-21, formato del PDF del usuario "MOSTRADOR 20
  // OCT 2026"): depósitos de Bancos de cada factura de mostrador que quedó
  // "Venta Sin Cobro" — informativo, NUNCA se contabiliza. Ver
  // mostrador.service.js.
  // 2026-10-22 (pedido del usuario): dos tablas, cada una con todos sus
  // depósitos juntos y columna FACTURA al final — "EN FACTURA GLOBAL" (facturas
  // a Público en General) y "PPD POR FACTURAR YA FACTURADO A NOMBRE DEL
  // CLIENTE" (factura no global cuyas remisiones Kore sigue teniendo PPD "POR
  // FACTURAR" — Kore no cambia ese estatus al facturar, verificado en prod con
  // OFILIO CRUZ HERNANDEZ, factura A0-261009783). Lo demás (p. ej. factura PUE
  // a nombre del cliente, ya FACTURADO) no sale. Pólizas generadas antes de guardar
  // `esGlobal` salen en bloques por factura bajo "SIN CLASIFICAR" hasta
  // regenerarse.
  // Depósitos de remisiones PPD pagadas, "POR FACTURAR" y sin factura
  // (2026-10-22) — ver detectarDepositosPendientesFactura en mostrador.service.js.
  const { facturasGlobales, ppdFacturadoCliente, sinClasificar, depositosPendientes } = tablasMostrador(datos);
  if (!(facturasGlobales.length || ppdFacturadoCliente.length || sinClasificar.length || depositosPendientes.length)) return false;

  const wsMos = workbook.addWorksheet(nombreHoja);
  wsMos.columns = [
    { key: 'fecha',       width: 11 },
    { key: 'descripcion', width: 44 },
    { key: 'importe',     width: 15 },
    { key: 'aplicado',    width: 15 },
    { key: 'banco',       width: 12 },
    { key: 'rem',         width: 16 },
    { key: 'cliente',     width: 30 },
    { key: 'idNumo',      width: 11 },
    { key: 'estatus',     width: 15 },
    { key: 'factura',     width: 15 },
    ...(columnaIdentificado ? [{ key: 'identificado', width: 16 }] : []),
  ];
  const relleno = argb => ({ type: 'pattern', pattern: 'solid', fgColor: { argb } });
  const encabezado = (row, argb) => row.eachCell(cell => {
    cell.font = { bold: true, color: { argb: 'FFFFFFFF' } };
    cell.fill = relleno(argb);
    cell.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true };
  });
  const COLUMNAS = ['FECHA', 'DESCRIPCIÓN', 'IMPORTE', 'APLICADO A FACTURA', 'BANCO', 'REM', 'CLIENTE', 'ID NUMO', 'ESTATUS'];
  const folioFactura = f => `${f.serie ?? ''}-${f.folio ?? ''}`;
  const fechaCorta = d => new Date(d).toLocaleDateString('es-MX', { timeZone: 'America/Mexico_City', day: 'numeric', month: 'numeric', year: '2-digit' });
  const _redondear2 = n => Math.round(n * 100) / 100;
  const seccion = (texto, columnas) => {
    const row = wsMos.addRow([texto]);
    wsMos.mergeCells(row.number, 1, row.number, columnas);
    encabezado(row, 'FF5B4A9E');
  };
  const filaDeposito = (d, factura, conIdentificado = false) => {
    const row = wsMos.addRow({
      fecha:       d.fecha ? new Date(d.fecha) : null,
      descripcion: d.descripcion,
      importe:     d.importe,
      aplicado:    d.aplicado,
      banco:       d.banco,
      rem:         (d.remisiones ?? []).join('\n'),
      cliente:     d.cliente || 'CLIENTE NO IDENTIFICADO',
      idNumo:      d.idNumo,
      estatus:     d.estatus,
      ...(factura ? { factura } : {}),
      ...(conIdentificado ? { identificado: fechaHoraMx(d.identificadoEl) } : {}),
    });
    row.alignment = { vertical: 'middle', wrapText: true };
    if (row.getCell('fecha').value) row.getCell('fecha').numFmt = 'd/m/yy';
    row.getCell('importe').numFmt = '$#,##0.00';
    row.getCell('aplicado').numFmt = '$#,##0.00';
  };
  const filaTotal = (facturas, etiquetaTotalFactura) => {
    const total = wsMos.addRow({
      descripcion: 'TOTAL',
      importe:     _redondear2(facturas.flatMap(f => f.depositos ?? []).reduce((s, d) => s + (Number(d.importe) || 0), 0)),
      aplicado:    _redondear2(facturas.reduce((s, f) => s + (Number(f.totalAplicado) || 0), 0)),
      ...(etiquetaTotalFactura ? {
        rem:     etiquetaTotalFactura,
        cliente: _redondear2(facturas.reduce((s, f) => s + (Number(f.totalFactura) || 0), 0)),
      } : {}),
    });
    total.font = { bold: true };
    total.getCell('descripcion').alignment = { horizontal: 'right' };
    total.getCell('importe').numFmt = '$#,##0.00';
    total.getCell('aplicado').numFmt = '$#,##0.00';
    if (etiquetaTotalFactura) total.getCell('cliente').numFmt = '$#,##0.00';
    wsMos.addRow([]);
  };
  const bloquesPorFactura = facturas => {
    for (const f of facturas) {
      const fechaFactura = f.fechaFactura ? new Date(f.fechaFactura) : null;
      const titulo = wsMos.addRow([
        fechaFactura ? fechaFactura.toLocaleDateString('es-MX', { timeZone: 'America/Mexico_City' }) : '',
        '', '', '', '', f.serie ?? '', folioFactura(f), '', '',
      ]);
      wsMos.mergeCells(titulo.number, 1, titulo.number, 5);
      encabezado(titulo, 'FF8E7CC3');
      encabezado(wsMos.addRow(COLUMNAS), 'FF8E7CC3');
      for (const d of f.depositos ?? []) filaDeposito(d);
      filaTotal([f], 'Total factura');
    }
  };

  // Reporte "Por facturar" (2026-10-28, pedido del usuario): dentro de cada
  // bloque, los depósitos agrupados por cliente — renglón con el nombre,
  // sus depósitos y su subtotal. La póliza los deja en su orden.
  const NO_IDENTIFICADO = 'CLIENTE NO IDENTIFICADO';
  const filasDepositos = (pares, columnas, conIdentificado = false) => {
    if (!agruparPorCliente) {
      for (const [d, factura] of pares) filaDeposito(d, factura, conIdentificado);
      return;
    }
    const porCliente = new Map();
    for (const par of pares) {
      const cliente = par[0].cliente || NO_IDENTIFICADO;
      if (!porCliente.has(cliente)) porCliente.set(cliente, []);
      porCliente.get(cliente).push(par);
    }
    const clientes = [...porCliente.keys()].sort((a, b) =>
      (a === NO_IDENTIFICADO) - (b === NO_IDENTIFICADO) || a.localeCompare(b, 'es'));
    for (const cliente of clientes) {
      const grupo = porCliente.get(cliente);
      const nombre = wsMos.addRow([cliente]);
      wsMos.mergeCells(nombre.number, 1, nombre.number, columnas);
      nombre.font = { bold: true };
      nombre.getCell(1).fill = relleno('FFE4DEF5');
      for (const [d, factura] of grupo) filaDeposito(d, factura, conIdentificado);
      const suma = k => _redondear2(grupo.reduce((s, [d]) => s + (Number(d[k]) || 0), 0));
      const sub = wsMos.addRow({ descripcion: `Subtotal ${cliente}`, importe: suma('importe'), aplicado: suma('aplicado') });
      sub.font = { bold: true, italic: true };
      sub.getCell('descripcion').alignment = { horizontal: 'right', wrapText: true };
      sub.getCell('importe').numFmt = '$#,##0.00';
      sub.getCell('aplicado').numFmt = '$#,##0.00';
    }
  };

  const tablaJunta = (titulo, facturas, etiquetaTotalFactura) => {
    seccion(titulo, 10);
    encabezado(wsMos.addRow([...COLUMNAS, 'FACTURA']), 'FF8E7CC3');
    filasDepositos(facturas.flatMap(f => (f.depositos ?? []).map(d => [d, folioFactura(f)])), 10);
    filaTotal(facturas, etiquetaTotalFactura);
  };

  // Una tabla por factura global, con su fecha en el encabezado y su total
  // (2026-10-23, pedido del usuario — antes todas iban juntas en una sola).
  const conFecha = f => (f.fechaFactura ? `${folioFactura(f)} (${fechaCorta(f.fechaFactura)})` : folioFactura(f));
  for (const f of facturasGlobales) {
    tablaJunta(`EN FACTURA GLOBAL — ${conFecha(f)}`, [f], 'Total factura');
  }
  if (ppdFacturadoCliente.length) {
    // Solo algunos depósitos de cada factura: el total es el de lo listado,
    // sin "Total facturas".
    tablaJunta('PPD POR FACTURAR YA FACTURADO A NOMBRE DEL CLIENTE', ppdFacturadoCliente, null);
  }
  if (sinClasificar.length) {
    seccion('SIN CLASIFICAR (regenerar la póliza)', 9);
    bloquesPorFactura(sinClasificar);
  }
  if (depositosPendientes.length) {
    // Mismas columnas que las otras tablas (2026-10-22, pedido del usuario);
    // FACTURA vacía porque todavía no tienen. En el reporte "Por facturar"
    // también IDENTIFICADO EL (2026-10-23).
    seccion('PENDIENTE GENERAR FACTURA', columnaIdentificado ? 11 : 10);
    encabezado(wsMos.addRow([...COLUMNAS, 'FACTURA', ...(columnaIdentificado ? ['IDENTIFICADO EL'] : [])]), 'FF8E7CC3');
    filasDepositos(depositosPendientes.map(d => [d, '']), columnaIdentificado ? 11 : 10, columnaIdentificado);
    filaTotal([{ depositos: depositosPendientes, totalAplicado: _redondear2(depositosPendientes.reduce((s, d) => s + (Number(d.aplicado) || 0), 0)) }], null);
  }

  return true;
}

/**
 * Hoja "DEPÓSITOS EN EFECTIVO" (reporte "Por facturar", 2026-10-22):
 * depósitos en efectivo sin asociar a ninguna venta. Devuelve false si no hay.
 */
function agregarHojaDepositosEfectivo(workbook, depositos, nombreHoja = 'DEPÓSITOS EN EFECTIVO') {
  if (!depositos?.length) return false;
  const ws = workbook.addWorksheet(nombreHoja);
  ws.columns = [
    { key: 'fecha',       width: 11 },
    { key: 'descripcion', width: 44 },
    { key: 'importe',     width: 15 },
    { key: 'banco',       width: 12 },
    { key: 'ficha',       width: 28 },
    { key: 'idNumo',      width: 11 },
  ];
  const estilo = (row, argb) => row.eachCell(cell => {
    cell.font = { bold: true, color: { argb: 'FFFFFFFF' } };
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb } };
    cell.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true };
  });
  const titulo = ws.addRow(['DEPÓSITOS EN EFECTIVO — SIN ASOCIAR A NINGUNA VENTA']);
  ws.mergeCells(titulo.number, 1, titulo.number, 6);
  estilo(titulo, 'FF5B4A9E');
  estilo(ws.addRow(['FECHA', 'DESCRIPCIÓN', 'IMPORTE', 'BANCO', 'FICHA', 'ID NUMO']), 'FF8E7CC3');
  for (const d of depositos) {
    const row = ws.addRow({ ...d, fecha: d.fecha ? new Date(d.fecha) : null });
    row.alignment = { vertical: 'middle', wrapText: true };
    if (row.getCell('fecha').value) row.getCell('fecha').numFmt = 'd/m/yy';
    row.getCell('importe').numFmt = '$#,##0.00';
  }
  const total = ws.addRow({ descripcion: 'TOTAL', importe: Math.round(depositos.reduce((s, d) => s + (Number(d.importe) || 0), 0) * 100) / 100 });
  total.font = { bold: true };
  total.getCell('descripcion').alignment = { horizontal: 'right' };
  total.getCell('importe').numFmt = '$#,##0.00';
  return true;
}

/**
 * Hoja "CON PENDIENTES DE PAGO" (reporte "Por facturar", 2026-10-23):
 * depósitos de remisiones PPD "POR FACTURAR" sin factura que todavía deben
 * algo — mismas columnas que PENDIENTE GENERAR FACTURA + SALDO PENDIENTE.
 */
function agregarHojaPendientesPago(workbook, depositos, nombreHoja = 'CON PENDIENTES DE PAGO') {
  if (!depositos?.length) return false;
  const ws = workbook.addWorksheet(nombreHoja);
  ws.columns = [
    { key: 'fecha',       width: 11 },
    { key: 'descripcion', width: 44 },
    { key: 'importe',     width: 15 },
    { key: 'aplicado',    width: 15 },
    { key: 'banco',       width: 12 },
    { key: 'rem',         width: 16 },
    { key: 'cliente',     width: 30 },
    { key: 'idNumo',      width: 11 },
    { key: 'estatus',     width: 15 },
    { key: 'saldo',       width: 15 },
    { key: 'identificado', width: 16 },
  ];
  const estilo = (row, argb) => row.eachCell(cell => {
    cell.font = { bold: true, color: { argb: 'FFFFFFFF' } };
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb } };
    cell.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true };
  });
  const titulo = ws.addRow(['CON PENDIENTES DE PAGO — POR FACTURAR, TODAVÍA DEBEN SALDO']);
  ws.mergeCells(titulo.number, 1, titulo.number, 11);
  estilo(titulo, 'FF5B4A9E');
  estilo(ws.addRow(['FECHA', 'DESCRIPCIÓN', 'IMPORTE', 'APLICADO A FACTURA', 'BANCO', 'REM', 'CLIENTE', 'ID NUMO', 'ESTATUS', 'SALDO PENDIENTE', 'IDENTIFICADO EL']), 'FF8E7CC3');
  for (const d of depositos) {
    const row = ws.addRow({
      fecha: d.fecha ? new Date(d.fecha) : null, descripcion: d.descripcion, importe: d.importe, aplicado: d.aplicado,
      banco: d.banco, rem: (d.remisiones ?? []).join('\n'), cliente: d.cliente || 'CLIENTE NO IDENTIFICADO',
      idNumo: d.idNumo, estatus: d.estatus, saldo: d.saldo, identificado: fechaHoraMx(d.identificadoEl),
    });
    row.alignment = { vertical: 'middle', wrapText: true };
    if (row.getCell('fecha').value) row.getCell('fecha').numFmt = 'd/m/yy';
    for (const k of ['importe', 'aplicado', 'saldo']) row.getCell(k).numFmt = '$#,##0.00';
  }
  const suma = k => Math.round(depositos.reduce((s, d) => s + (Number(d[k]) || 0), 0) * 100) / 100;
  const total = ws.addRow({ descripcion: 'TOTAL', importe: suma('importe'), aplicado: suma('aplicado'), saldo: suma('saldo') });
  total.font = { bold: true };
  total.getCell('descripcion').alignment = { horizontal: 'right' };
  for (const k of ['importe', 'aplicado', 'saldo']) total.getCell(k).numFmt = '$#,##0.00';
  return true;
}

module.exports = { agregarHojaMostrador, agregarHojaDepositosEfectivo, agregarHojaPendientesPago, tablasMostrador };
