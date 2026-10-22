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
function tablasMostrador(datos) {
  const mostrador = datos?.movimientosMostrador ?? [];
  return {
    facturasGlobales:    mostrador.filter(f => f.esGlobal === true),
    ppdFacturadoCliente: mostrador
      .filter(f => f.esGlobal === false)
      .map(f => ({ ...f, depositos: (f.depositos ?? []).filter(d => d.ppdPorFacturar) }))
      .filter(f => f.depositos.length),
    sinClasificar:       mostrador.filter(f => typeof f.esGlobal !== 'boolean'),
    depositosPendientes: datos?.depositosPendientesFactura ?? [],
  };
}

/**
 * Agrega la hoja al workbook (ExcelJS). Devuelve false si no hay nada que
 * enseñar (no se crea la hoja).
 */
function agregarHojaMostrador(workbook, datos, nombreHoja = 'MOSTRADOR') {
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
  const filaDeposito = (d, factura) => {
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

  const tablaJunta = (titulo, facturas, etiquetaTotalFactura) => {
    seccion(titulo, 10);
    encabezado(wsMos.addRow([...COLUMNAS, 'FACTURA']), 'FF8E7CC3');
    for (const f of facturas) for (const d of f.depositos ?? []) filaDeposito(d, folioFactura(f));
    filaTotal(facturas, etiquetaTotalFactura);
  };

  if (facturasGlobales.length) {
    // Cada factura con su fecha (2026-10-22, pedido del usuario).
    const conFecha = f => (f.fechaFactura ? `${folioFactura(f)} (${fechaCorta(f.fechaFactura)})` : folioFactura(f));
    tablaJunta(`EN FACTURA GLOBAL — ${facturasGlobales.map(conFecha).join(', ')}`, facturasGlobales, 'Total facturas');
  }
  if (ppdFacturadoCliente.length) {
    // Solo algunos depósitos de cada factura: el total es el de lo listado,
    // sin "Total facturas".
    tablaJunta('PPD POR FACTURAR YA FACTURADO A NOMBRE DEL CLIENTE', ppdFacturadoCliente.map(f => ({
      ...f, totalAplicado: _redondear2(f.depositos.reduce((s, d) => s + (Number(d.aplicado) || 0), 0)),
    })), null);
  }
  if (sinClasificar.length) {
    seccion('SIN CLASIFICAR (regenerar la póliza)', 9);
    bloquesPorFactura(sinClasificar);
  }
  if (depositosPendientes.length) {
    // Mismas columnas que la lista manual del usuario (CEDIS, 2026-10-22).
    seccion('PENDIENTE GENERAR FACTURA', 7);
    encabezado(wsMos.addRow(['FECHA', 'DESCRIPCIÓN', 'IMPORTE', 'BANCO', 'REM', 'CLIENTE', 'ID NUMO']), 'FF8E7CC3');
    for (const d of depositosPendientes) {
      const row = wsMos.addRow([d.fecha ? new Date(d.fecha) : null, d.descripcion, d.importe, d.banco,
        (d.remisiones ?? []).join('\n'), d.cliente || 'CLIENTE NO IDENTIFICADO', d.idNumo]);
      row.alignment = { vertical: 'middle', wrapText: true };
      if (row.getCell(1).value) row.getCell(1).numFmt = 'd/m/yy';
      row.getCell(3).numFmt = '$#,##0.00';
    }
    const total = wsMos.addRow([null, 'TOTAL', _redondear2(depositosPendientes.reduce((s, d) => s + (Number(d.importe) || 0), 0))]);
    total.font = { bold: true };
    total.getCell(2).alignment = { horizontal: 'right' };
    total.getCell(3).numFmt = '$#,##0.00';
  }

  return true;
}

module.exports = { agregarHojaMostrador, tablasMostrador };
