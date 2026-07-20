'use strict';

const AdmZip = require('adm-zip');
const { BadRequestError } = require('../../../shared/errors/AppError');
const generator = require('../cfdi-mapping/cfdi-poliza-generator.service');
const polizaSvc = require('./poliza.service');

// Caracteres fuera de [A-Za-z0-9_-] no son seguros/portables en nombres de
// archivo o carpeta dentro de un ZIP (espacios sí funcionan, pero se
// normalizan aquí para que el resultado sea idéntico en cualquier SO).
const sanitize = (s) => String(s ?? '').replace(/[^\w-]+/g, '_');

/**
 * Genera (o reutiliza el resultado de generar) las pólizas del modo pedido y
 * arma un ZIP con el .xlsx de CONTPAQ de cada una — una carpeta por sucursal
 * cuando el modo incluye sucursal, un archivo por día cuando incluye día.
 *
 * modo: 'porSucursal' | 'porDia' | 'porDiaYSucursal'
 *
 * Devuelve: { buffer: Buffer, nombreZip: string }
 */
async function exportarContpaqZip({ rfc, ejercicio, periodo, tipoCfdi, tipoPropuesta, modo, fechaInicio, fechaFin }) {
  if (!rfc)       throw new BadRequestError('RFC requerido');
  if (!ejercicio) throw new BadRequestError('Ejercicio requerido');
  if (!periodo)   throw new BadRequestError('Periodo requerido');
  if (!tipoCfdi)  throw new BadRequestError('Debes seleccionar el tipo de CFDI a procesar (I, E o P)');

  let resultados;
  if (modo === 'porSucursal') {
    ({ resultados } = await generator.generarYGuardarPorSucursal({ rfc, ejercicio, periodo, tipoPropuesta, tipoCfdi, fechaInicio, fechaFin }));
  } else if (modo === 'porDia') {
    ({ resultados } = await generator.generarYGuardarPorDia({ rfc, ejercicio, periodo, tipoPropuesta, tipoCfdi, fechaInicio, fechaFin }));
  } else if (modo === 'porDiaYSucursal') {
    ({ resultados } = await generator.generarYGuardarPorSucursalYDia({ rfc, ejercicio, periodo, tipoPropuesta, tipoCfdi, fechaInicio, fechaFin }));
  } else {
    throw new BadRequestError(`modo inválido: "${modo}" (debe ser porSucursal, porDia o porDiaYSucursal)`);
  }

  const exitosos = resultados.filter(r => r.polizaId);
  if (!exitosos.length) {
    // Agrupar por mensaje de error para no repetir la misma razón una vez por
    // cada sucursal/día — el usuario necesita saber POR QUÉ, no una lista larga.
    const porRazon = new Map();
    for (const r of resultados) {
      const razon = r.error ?? 'Error desconocido';
      porRazon.set(razon, (porRazon.get(razon) ?? 0) + 1);
    }
    const detalle = [...porRazon.entries()].map(([razon, n]) => `${n}× ${razon}`).join('; ');
    throw new BadRequestError(`No se generó ninguna póliza (${resultados.length} combinación(es) revisada(s)): ${detalle}`);
  }

  const zip = new AdmZip();

  for (const entry of exitosos) {
    const { workbooks, poliza } = await polizaSvc.exportContpaqXlsx(entry.polizaId, {});
    const mesPeriodo = String(poliza.periodo).padStart(2, '0');
    // CEDIS trae 3 workbooks (Ventas, Bonificaciones, Descuentos y
    // Devoluciones — cada uno con su par Contado/Crédito adentro) — cada uno
    // se guarda como su propio archivo .xlsx dentro de la carpeta de la
    // sucursal. El resto de sucursales sigue trayendo un solo workbook (todos
    // los bloques juntos), igual que siempre.
    for (const { tipoVenta, workbook } of workbooks) {
      const buffer = await workbook.xlsx.writeBuffer();
      const sufijo = workbooks.length > 1 ? `_${sanitize(tipoVenta)}` : '';
      const nombreArchivo = entry.fecha
        ? `Poliza_${poliza.tipo}${poliza.numero}_${entry.fecha}${sufijo}_CONTPAQ.xlsx`
        : `Poliza_${poliza.tipo}${poliza.numero}_${poliza.ejercicio}${mesPeriodo}${sufijo}_CONTPAQ.xlsx`;
      const ruta = entry.centroCosto
        ? `${sanitize(entry.centroCosto)}/${nombreArchivo}`
        : nombreArchivo;
      zip.addFile(ruta, Buffer.from(buffer));
    }
  }

  // Resumen de todas las combinaciones (incluidas las que no generaron
  // póliza por falta de CFDIs) — así no se pierde esa información al no
  // poder regresar también un JSON junto con el archivo binario.
  const resumenTxt = resultados.map(r => {
    const etiqueta = [r.centroCosto, r.fecha].filter(Boolean).join(' / ') || '(sin etiqueta)';
    return r.polizaId
      ? `OK  ${etiqueta}: póliza ${r.polizaId} con ${r.totalCfdis} CFDI(s)${r.sinRegla ? ` (${r.sinRegla} sin regla)` : ''}`
      : `--  ${etiqueta}: ${r.error}`;
  }).join('\n');
  zip.addFile('_resumen.txt', Buffer.from(resumenTxt, 'utf-8'));

  const buffer   = zip.toBuffer();
  const nombreZip = `CONTPAQ_${rfc}_${ejercicio}${String(periodo).padStart(2, '0')}_${modo}.zip`;

  return { buffer, nombreZip };
}

module.exports = { exportarContpaqZip };
