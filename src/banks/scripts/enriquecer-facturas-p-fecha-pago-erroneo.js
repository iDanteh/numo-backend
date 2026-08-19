'use strict';

require('dotenv').config();

// enriquecer-facturas-p-fecha-pago-erroneo.js — Fase 1 del fix de fecha_pago erróneo.
// Lee el concentrado de CFDIs de Pago (facturas_P_fecha_pago_erroneo.xlsx, raíz del
// proyecto) y agrega 3 columnas por cada fila, usando SOLO datos ya guardados en Mongo
// (CFDI.complementoPago, parseado al timbrar — nunca se lee ningún XML ni se llama a
// Kore aquí, confirmado con el usuario 2026-08-18):
//   - serieExterna venta / folioExterno venta: de complementoPago.pagos[].doctosRelacionados
//     (la factura de Ingreso que este Pago está liquidando).
//   - fecha pago XML (actual): complementoPago.pagos[].fechaPago, el valor que hoy trae
//     el CFDI — la Fase 2 (pendiente, requiere consultar Kore) comparará esto contra
//     ErpCuentaPendiente.fechaRealPago para confirmar el desfase de +6hrs y escribir la
//     fecha corregida en una columna final.
//
// Solo lectura: no modifica Mongo ni el archivo de entrada — escribe un .xlsx nuevo.
//
// Uso:
//   node src/banks/scripts/enriquecer-facturas-p-fecha-pago-erroneo.js [inputPath] [outputPath]
// Defaults: inputPath  = <raíz del repo>/facturas_P_fecha_pago_erroneo.xlsx
//           outputPath = <raíz del repo>/facturas_P_fecha_pago_erroneo_enriquecido.xlsx

const path    = require('path');
const ExcelJS = require('exceljs');
const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const CFDI = require('../../visor/models/CFDI');

const REPO_ROOT    = path.resolve(__dirname, '../../../../');
const inputPath  = process.argv[2] ? path.resolve(process.argv[2]) : path.join(REPO_ROOT, 'facturas_P_fecha_pago_erroneo.xlsx');
const outputPath = process.argv[3] ? path.resolve(process.argv[3]) : path.join(REPO_ROOT, 'facturas_P_fecha_pago_erroneo_enriquecido.xlsx');

// Múltiples pagos/doctosRelacionados en un mismo complementoPago (parcialidades, o un
// Pago que liquida más de una factura) se concatenan con '; ' — mismo criterio ya usado
// en otros reportes Excel del proyecto (ver collection-request.service.js#_filaReporte).
function formatFechaPago(pagos) {
  const fechas = pagos.map(p => p.fechaPago).filter(Boolean);
  if (fechas.length === 0) return '';
  return fechas.map(f => f.toISOString()).join('; ');
}

function formatVenta(pagos) {
  const doctos = pagos.flatMap(p => p.doctosRelacionados || []);
  if (doctos.length === 0) return { serieExterna: '', folioExterno: '' };
  return {
    serieExterna: doctos.map(d => d.serie ?? '').join('; '),
    folioExterno: doctos.map(d => d.folio ?? '').join('; '),
  };
}

async function main() {
  console.log(`Leyendo ${inputPath}...`);
  const wbIn = new ExcelJS.Workbook();
  await wbIn.xlsx.readFile(inputPath);
  const wsIn = wbIn.worksheets[0];

  const headerRow = wsIn.getRow(1).values.slice(1); // slice(1): ExcelJS values[0] es undefined
  const idCol     = headerRow.indexOf('id') + 1;
  const serieCol  = headerRow.indexOf('serie') + 1;
  const folioCol  = headerRow.indexOf('folio') + 1;
  if (serieCol === 0 || folioCol === 0) throw new Error(`No se encontraron las columnas 'serie'/'folio' en ${inputPath}`);

  // La columna 'id' del Excel NO es el _id de Mongo de Numo (verificado: 0/1478 matchean
  // por _id) — es de otro sistema (probablemente el id interno de Kore). El lookup real
  // es por serie+folio+tipoDeComprobante:'P', y ahí SIEMPRE hay 2 documentos por CFDI
  // (source:'SAT' y source:'ERP', ambos isActive:true) — verificado con 4 casos reales:
  //   - source:'ERP': fechaPago siempre en punto T00:00:00.000Z (Kore ya sólo guarda el
  //     día, sin hora real) y doctosRelacionados SIEMPRE vacío — inservible para esto.
  //   - source:'SAT': fechaPago es el valor real del XML tal cual se timbró (la hora
  //     naive local sin ajustar — el "actual" que pide el usuario) y doctosRelacionados
  //     SÍ trae la venta que este Pago liquida (puede haber más de una).
  // Por eso se filtra explícitamente source:'SAT'.
  const rows = [];
  wsIn.eachRow((row, rowNumber) => {
    if (rowNumber === 1) return;
    rows.push({
      rowNumber,
      serie: String(row.getCell(serieCol).value || '').trim(),
      folio: String(row.getCell(folioCol).value || '').trim(),
    });
  });
  console.log(`${rows.length} filas encontradas.`);

  await connectMongo();

  const pares = [...new Map(rows.map(r => [`${r.serie}|${r.folio}`, { serie: r.serie, folio: r.folio }])).values()];
  const cfdis = await CFDI.find({
    tipoDeComprobante: 'P',
    source: 'SAT',
    $or: pares.map(p => ({ serie: p.serie, folio: p.folio })),
  })
    .select('serie folio complementoPago.pagos.fechaPago complementoPago.pagos.doctosRelacionados')
    .lean();
  const cfdiPorClave = new Map(cfdis.map(c => [`${c.serie}|${c.folio}`, c]));
  console.log(`${cfdiPorClave.size}/${pares.length} CFDIs (source:SAT) encontrados en Mongo.`);

  const wbOut = new ExcelJS.Workbook();
  const wsOut = wbOut.addWorksheet('facturas_P_fecha_pago_erroneo');
  wsOut.columns = [
    { header: 'id',                          key: 'id',           width: 26 },
    { header: 'serie',                       key: 'serie',        width: 10 },
    { header: 'folio',                       key: 'folio',        width: 14 },
    { header: 'estatus',                     key: 'estatus',      width: 14 },
    { header: 'tipo documento',              key: 'tipo',         width: 14 },
    { header: 'fecha generacion',            key: 'fechaGen',     width: 30 },
    { header: 'serieExterna venta',          key: 'serieVenta',   width: 16 },
    { header: 'folioExterno venta',          key: 'folioVenta',   width: 16 },
    { header: 'fecha pago XML (actual)',     key: 'fechaPago',    width: 30 },
    { header: 'observacion',                 key: 'observacion',  width: 30 },
  ];
  wsOut.getRow(1).font = { bold: true };

  let sinCfdi = 0, sinComplementoPago = 0, sinDoctos = 0;

  for (const r of rows) {
    const original = wsIn.getRow(r.rowNumber).values.slice(1);
    const cfdi = cfdiPorClave.get(`${r.serie}|${r.folio}`);

    let serieVenta = '', folioVenta = '', fechaPago = '', observacion = '';

    if (!cfdi) {
      observacion = 'CFDI (source:SAT) no encontrado por serie+folio';
      sinCfdi++;
    } else {
      const pagos = cfdi.complementoPago?.pagos ?? [];
      if (pagos.length === 0) {
        observacion = 'Sin complementoPago.pagos';
        sinComplementoPago++;
      } else {
        fechaPago = formatFechaPago(pagos);
        const venta = formatVenta(pagos);
        serieVenta = venta.serieExterna;
        folioVenta = venta.folioExterno;
        if (!serieVenta && !folioVenta) {
          observacion = 'Sin doctosRelacionados';
          sinDoctos++;
        }
      }
    }

    wsOut.addRow({
      id:          original[idCol - 1],
      serie:       original[headerRow.indexOf('serie')],
      folio:       original[headerRow.indexOf('folio')],
      estatus:     original[headerRow.indexOf('estatus')],
      tipo:        original[headerRow.indexOf('tipo documento')],
      fechaGen:    original[headerRow.indexOf('fecha generacion')],
      serieVenta, folioVenta, fechaPago, observacion,
    });
  }

  await wbOut.xlsx.writeFile(outputPath);
  await disconnectMongo();

  console.log(`\nListo: ${outputPath}`);
  console.log(`Sin CFDI en Mongo: ${sinCfdi}`);
  console.log(`Sin complementoPago.pagos: ${sinComplementoPago}`);
  console.log(`Sin doctosRelacionados: ${sinDoctos}`);
}

main().catch(function (err) { console.error(err); process.exit(1); });
