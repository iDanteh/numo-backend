'use strict';

/**
 * diag-catalogo-cuenta-cliente.js
 *
 * Motor de sugerencias FALLBACK #2 (solo diagnostico, no escribe nada).
 *
 * Idea: bank_movements NUNCA se purga -- todo el historico de depositos ya
 * IDENTIFICADOS (con erpLinks.folioFiscal resuelto) sigue disponible para
 * siempre. Dentro del `concepto` de esos movimientos suele venir una "firma"
 * bancaria reutilizable entre depositos del MISMO cliente aunque no haya
 * numero de autorizacion en comun:
 *
 *   - BBVA "PAGO CUENTA DE TERCERO / {ref} BNET {cuenta} {texto libre}"
 *     -> {cuenta} (10 digitos BNET) es la cuenta de origen del cliente,
 *        se repite en TODOS sus depositos por transferencia de terceros.
 *   - Banamex "... | Nombre del Emisor: {nombre} | Institucion Emisora: ..."
 *     -> nombre del emisor tal cual lo capturo el banco.
 *
 * Paso 1 (catalogo): recorrer bank_movements YA identificados, extraer la
 * firma de su concepto, cruzarla contra `cfdis` via erpLinks.folioFiscal ->
 * uuid -> receptor.rfc/nombre. Asi se construye firma -> cliente real (no
 * un nombre libre cualquiera, sino el RFC/nombre que el CFDI confirma).
 *
 * Paso 2 (sugerencia): para cada movimiento no_identificado, extraer su
 * firma, buscarla en el catalogo -> cliente probable. Si hay cliente
 * probable, CRUZAR CONTRA EL CFDI: buscar entre sus CFDIs tipo P (pagos)
 * alguno cuyo impPagado/monto y fechaPago caigan dentro de tolerancia del
 * deposito -- exactamente igual que diag-match-monto-fecha.js, pero ahora
 * acotado a las facturas de ESE cliente especifico, lo que reduce
 * drasticamente la ambiguedad de monto+fecha solo.
 *
 * Uso:
 *   node src/banks/scripts/diag-catalogo-cuenta-cliente.js <fechaInicio> <fechaFin> [banco]
 */

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const CFDI = require('../models/CFDI');
const BankMovement = require('../../banks/domains/banks/BankMovement.model');

const TOLERANCIA_MONTO = 2;
const TOLERANCIA_DIAS  = 15; // mas laxo que el fallback #1 -- aqui ya tenemos cliente confirmado por firma, así que se puede dar más margen de fecha
const MONTO_MINIMO     = 10;

const fechaInicioArg = process.argv[2];
const fechaFinArg    = process.argv[3];
const bancoArg       = process.argv[4];

if (!fechaInicioArg || !fechaFinArg) {
  console.error('Uso: node diag-catalogo-cuenta-cliente.js <fechaInicio YYYY-MM-DD> <fechaFin YYYY-MM-DD> [banco]');
  process.exit(1);
}

const fechaInicio = new Date(fechaInicioArg + 'T00:00:00Z');
const fechaFin    = new Date(fechaFinArg + 'T23:59:59Z');

// ── Extraccion de firma bancaria desde el concepto ──────────────────────────
// Devuelve un array de { tipo, valor } -- puede haber mas de una firma por concepto.
function extraerFirmas(concepto) {
  if (!concepto) return [];
  const firmas = [];

  const bnet = concepto.match(/BNET\s+(\d{6,12})/i);
  if (bnet) firmas.push({ tipo: 'cuenta_bnet', valor: bnet[1] });

  const emisor = concepto.match(/Nombre del Emisor:\s*([^|]+)/i);
  if (emisor) {
    const nombre = emisor[1].trim().toUpperCase().replace(/\s+/g, ' ');
    if (nombre) firmas.push({ tipo: 'nombre_emisor', valor: nombre });
  }

  return firmas;
}

function diffDias(a, b) {
  return Math.abs(new Date(a).getTime() - new Date(b).getTime()) / 86400000;
}

// RFC generico "Publico en general" (CFDIs a consumidor final) -- muchas
// personas distintas pueden compartirlo, así que reusar una cuenta bancaria
// bajo este RFC no identifica a un cliente real. Se excluye del catalogo.
const RFC_GENERICOS = new Set(['XAXX010101000', 'XEXX010101000']);

async function main() {
  await connectMongo();

  // ── PASO 1: construir catalogo firma -> cliente a partir de movimientos YA identificados ──
  const identificados = await BankMovement.find({
    isActive: true,
    'erpLinks.folioFiscal': { $exists: true, $ne: null },
    concepto: { $regex: 'BNET \\d{6,12}|Nombre del Emisor:', $options: 'i' },
  }).select('concepto erpLinks.folioFiscal').lean();

  console.log(`Movimientos identificados con firma reconocible en concepto: ${identificados.length}`);

  const foliosFiscales = new Set();
  const movFirmas = []; // { firmas: [...], folioFiscal }
  for (const m of identificados) {
    const firmas = extraerFirmas(m.concepto);
    if (!firmas.length) continue;
    for (const link of (m.erpLinks || [])) {
      if (!link.folioFiscal) continue;
      foliosFiscales.add(link.folioFiscal.toUpperCase());
      movFirmas.push({ firmas, folioFiscal: link.folioFiscal.toUpperCase() });
    }
  }

  // Traer receptor.rfc/nombre de esos folioFiscal (variantes de mayus/minus)
  const variantes = [...foliosFiscales].flatMap(f => [f, f.toLowerCase()]);
  const cfdisReceptor = await CFDI.find({ uuid: { $in: variantes } }).select('uuid receptor.rfc receptor.nombre').lean();
  const receptorPorUuid = new Map();
  for (const c of cfdisReceptor) {
    if (c.receptor?.rfc) receptorPorUuid.set(c.uuid.toUpperCase(), { rfc: c.receptor.rfc, nombre: c.receptor.nombre || null });
  }

  // Agregar catalogo: firma -> Map(rfc -> count)
  const catalogo = new Map(); // key "tipo:valor" -> Map(rfc -> { nombre, count })
  for (const { firmas, folioFiscal } of movFirmas) {
    const receptor = receptorPorUuid.get(folioFiscal);
    if (!receptor || RFC_GENERICOS.has(receptor.rfc)) continue;
    for (const f of firmas) {
      const key = `${f.tipo}:${f.valor}`;
      if (!catalogo.has(key)) catalogo.set(key, new Map());
      const porRfc = catalogo.get(key);
      if (!porRfc.has(receptor.rfc)) porRfc.set(receptor.rfc, { nombre: receptor.nombre, count: 0 });
      porRfc.get(receptor.rfc).count++;
    }
  }

  console.log(`Firmas unicas en catalogo: ${catalogo.size}`);

  // Reportar firmas ambiguas (mismo string de firma usado por >1 cliente distinto) -- se descartan como señal
  let firmasAmbiguas = 0;
  const catalogoLimpio = new Map(); // key -> { rfc, nombre, count }
  for (const [key, porRfc] of catalogo) {
    if (porRfc.size > 1) { firmasAmbiguas++; continue; }
    const [rfc, info] = [...porRfc.entries()][0];
    catalogoLimpio.set(key, { rfc, nombre: info.nombre, count: info.count });
  }
  console.log(`Firmas descartadas por ser ambiguas (>1 cliente): ${firmasAmbiguas}`);
  console.log(`Firmas utilizables (1 solo cliente asociado): ${catalogoLimpio.size}`);

  // ── PASO 2: para cada movimiento no_identificado, sugerir cliente por firma ──
  const movimientos = await BankMovement.find({
    isActive: true,
    status:   'no_identificado',
    deposito: { $gt: MONTO_MINIMO },
    fecha:    { $gte: fechaInicio, $lte: fechaFin },
    erpLinks: { $size: 0 }, // ya vinculado (aunque status no se haya recalculado) -- fuera de alcance de este motor
    ...(bancoArg ? { banco: bancoArg } : {}),
  }).select('_id banco fecha deposito folio concepto').sort({ fecha: 1 }).lean();

  console.log(`\nMovimientos no_identificado en rango: ${movimientos.length}`);

  const conFirmaConocida = [];
  for (const mov of movimientos) {
    const firmas = extraerFirmas(mov.concepto);
    for (const f of firmas) {
      const key = `${f.tipo}:${f.valor}`;
      const cliente = catalogoLimpio.get(key);
      if (cliente) {
        conFirmaConocida.push({ mov, firma: f, cliente });
        break; // una firma reconocida por movimiento basta
      }
    }
  }
  console.log(`Movimientos con firma reconocida en el catalogo: ${conFirmaConocida.length}`);

  // Ya vinculadas -- no proponer facturas que ya tienen banco
  const yaVinculadas = new Set(
    (await BankMovement.distinct('erpLinks.folioFiscal', { isActive: true })).filter(Boolean).map(f => f.toUpperCase()),
  );

  // ── PASO 3 ("comparacion con el CFDI"): para cada cliente sugerido, buscar sus CFDIs tipo P
  //    con monto/fecha compatible con el deposito -- confirma o descarta la sugerencia de firma.
  const rfcsNecesarios = [...new Set(conFirmaConocida.map(x => x.cliente.rfc))];
  const pagosPorRfc = new Map(); // rfc -> [{ idDocumento, impPagado, fechaPago, folioFactura, cfdiPagoUuid } , pago_completo...]

  if (rfcsNecesarios.length) {
    const ventanaInicio = new Date(fechaInicio.getTime() - TOLERANCIA_DIAS * 86400000);
    const ventanaFin    = new Date(fechaFin.getTime() + TOLERANCIA_DIAS * 86400000);

    const pagosCfdi = await CFDI.aggregate([
      {
        $match: {
          tipoDeComprobante: 'P',
          isActive: true,
          'receptor.rfc': { $in: rfcsNecesarios },
          'complementoPago.pagos.doctosRelacionados.0': { $exists: true },
          'complementoPago.pagos.fechaPago': { $gte: ventanaInicio, $lte: ventanaFin },
        },
      },
      { $addFields: { _srcOrden: { $cond: [{ $eq: ['$source', 'SAT'] }, 0, 1] } } },
      { $sort: { uuid: 1, _srcOrden: 1 } },
      { $group: { _id: '$uuid', doc: { $first: '$$ROOT' } } },
      { $replaceRoot: { newRoot: '$doc' } },
      { $project: { uuid: 1, folio: 1, 'receptor.rfc': 1, 'complementoPago.pagos': 1 } },
    ]);

    for (const cfdi of pagosCfdi) {
      const rfc = cfdi.receptor?.rfc;
      if (!rfc) continue;
      if (!pagosPorRfc.has(rfc)) pagosPorRfc.set(rfc, []);
      for (const pago of (cfdi.complementoPago?.pagos || [])) {
        if (!pago.fechaPago) continue;
        if (pago.monto != null) {
          pagosPorRfc.get(rfc).push({ tipo: 'pago_completo', cfdiPagoUuid: cfdi.uuid, folioPago: cfdi.folio, monto: pago.monto, fechaPago: pago.fechaPago, facturas: (pago.doctosRelacionados || []).map(d => d.idDocumento).filter(Boolean) });
        }
        for (const d of (pago.doctosRelacionados || [])) {
          if (!d.idDocumento || d.impPagado == null) continue;
          if (yaVinculadas.has(d.idDocumento.toUpperCase())) continue;
          pagosPorRfc.get(rfc).push({ tipo: 'factura', cfdiPagoUuid: cfdi.uuid, folioPago: cfdi.folio, idDocumento: d.idDocumento, serieFactura: d.serie, folioFactura: d.folio, impPagado: d.impPagado, fechaPago: pago.fechaPago });
        }
      }
    }
  }

  const resultados = [];
  let confirmados = 0, soloFirma = 0;
  for (const { mov, firma, cliente } of conFirmaConocida) {
    const candidatosCliente = pagosPorRfc.get(cliente.rfc) || [];
    const hits = [];
    for (const c of candidatosCliente) {
      const monto = c.tipo === 'factura' ? c.impPagado : c.monto;
      if (Math.abs(mov.deposito - monto) <= TOLERANCIA_MONTO && diffDias(mov.fecha, c.fechaPago) <= TOLERANCIA_DIAS) {
        hits.push({ ...c, diffMonto: +(mov.deposito - monto).toFixed(2), diffDias: +diffDias(mov.fecha, c.fechaPago).toFixed(2) });
      }
    }
    hits.sort((a, b) => (a.diffDias - b.diffDias) || (Math.abs(a.diffMonto) - Math.abs(b.diffMonto)));

    if (hits.length > 0) confirmados++; else soloFirma++;

    resultados.push({
      movimiento: { _id: mov._id, banco: mov.banco, fecha: mov.fecha, deposito: mov.deposito, folio: mov.folio, concepto: mov.concepto?.slice(0, 90) },
      firmaDetectada: firma,
      clienteSugerido: { rfc: cliente.rfc, nombre: cliente.nombre, vecesVistoEnHistorico: cliente.count },
      estado: hits.length > 0 ? 'CONFIRMADO_POR_CFDI' : 'SOLO_FIRMA_SIN_CFDI_COMPATIBLE',
      candidatosCfdi: hits.slice(0, 5),
    });
  }

  console.log(`\n=== RESUMEN ===`);
  console.log(`Confirmado por firma + CFDI (monto/fecha):  ${confirmados}`);
  console.log(`Solo firma, sin CFDI compatible en ventana:  ${soloFirma}`);
  console.log(`\n=== DETALLE ===`);
  resultados.sort((a, b) => (a.estado === 'CONFIRMADO_POR_CFDI' ? -1 : 1) - (b.estado === 'CONFIRMADO_POR_CFDI' ? -1 : 1));
  console.log(JSON.stringify(resultados, null, 2));

  await disconnectMongo();
  process.exit(0);
}

main().catch(function (err) { console.error(err); process.exit(1); });
