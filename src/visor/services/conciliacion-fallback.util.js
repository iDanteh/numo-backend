'use strict';

/**
 * Helpers compartidos por los motores de sugerencia FALLBACK (diag-match-monto-fecha,
 * diag-catalogo-cuenta-cliente, diag-sugerencias-conciliacion). Todo de solo lectura --
 * estos motores NO escriben erpLinks, solo proponen candidatos para revisión humana.
 *
 * Contexto: el motor real (matchAutorizacionesDesdeErp) solo cruza contra
 * erp_cuentas_pendientes, que el ERP expone SOLO con cuentas ABIERTAS -- en cuanto
 * una factura se salda, desaparece de ese feed y el motor real ya no tiene con qué
 * cruzarla. Estos fallbacks usan `cfdis` (que no expira) + el propio historial de
 * `bank_movements` (que tampoco se purga) como señales alternas.
 */

// RFC generico "Publico en general" (CFDIs a consumidor final) -- muchas personas
// distintas lo comparten, así que una cuenta bancaria bajo este RFC no identifica
// a un cliente real. Se excluye de cualquier catalogo firma->cliente.
const RFC_GENERICOS = new Set(['XAXX010101000', 'XEXX010101000']);

function diffDias(a, b) {
  return Math.abs(new Date(a).getTime() - new Date(b).getTime()) / 86400000;
}

// Extrae "firmas" bancarias reutilizables del concepto de un movimiento:
//   - BBVA "PAGO CUENTA DE TERCERO / {ref} BNET {cuenta} {texto libre}" -> cuenta BNET
//   - Banamex "... | Nombre del Emisor: {nombre} | ..."                -> nombre del emisor
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

// Construye catalogo firma -> cliente (rfc/nombre) a partir de bank_movements
// YA identificados. Descarta firmas ambiguas (usadas por >1 cliente distinto) y
// cualquier vinculo bajo un RFC generico.
async function construirCatalogoFirmas(BankMovement, CFDI) {
  const identificados = await BankMovement.find({
    isActive: true,
    'erpLinks.folioFiscal': { $exists: true, $ne: null },
    concepto: { $regex: 'BNET \\d{6,12}|Nombre del Emisor:', $options: 'i' },
  }).select('concepto erpLinks.folioFiscal').lean();

  const foliosFiscales = new Set();
  const movFirmas = [];
  for (const m of identificados) {
    const firmas = extraerFirmas(m.concepto);
    if (!firmas.length) continue;
    for (const link of (m.erpLinks || [])) {
      if (!link.folioFiscal) continue;
      foliosFiscales.add(link.folioFiscal.toUpperCase());
      movFirmas.push({ firmas, folioFiscal: link.folioFiscal.toUpperCase() });
    }
  }

  const variantes = [...foliosFiscales].flatMap(f => [f, f.toLowerCase()]);
  const cfdisReceptor = await CFDI.find({ uuid: { $in: variantes } }).select('uuid receptor.rfc receptor.nombre').lean();
  const receptorPorUuid = new Map();
  for (const c of cfdisReceptor) {
    if (c.receptor?.rfc) receptorPorUuid.set(c.uuid.toUpperCase(), { rfc: c.receptor.rfc, nombre: c.receptor.nombre || null });
  }

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

  const catalogoLimpio = new Map();
  let firmasAmbiguas = 0;
  for (const [key, porRfc] of catalogo) {
    if (porRfc.size > 1) { firmasAmbiguas++; continue; }
    const [rfc, info] = [...porRfc.entries()][0];
    catalogoLimpio.set(key, { rfc, nombre: info.nombre, count: info.count });
  }

  return { catalogo: catalogoLimpio, firmasAmbiguas, totalIdentificadosConFirma: identificados.length };
}

// Investigado 2026-07-23 y descartado: erpLinks[].movimientosKore[].formasPago[]
// .adicionales trae dos campos que parecían prometedores para otra huella de
// cliente, pero ninguno sirve --
//   'Aut'   -- es la referencia bancaria de ESA transacción puntual, nunca se
//              repite entre movimientos DISTINTOS (confirmado: 0 matches al
//              cruzar los 256 depósitos sin vincular del histórico completo
//              contra un catálogo de 2,553 autorizaciones ya usadas -- es el
//              mismo campo que ya usa el motor real, así que si no vinculó
//              por ahí la primera vez tampoco lo hará aquí).
//   'Banco' -- solo tiene 4 valores en TODA la base ("BANCOMER 1014",
//              "BANAMEX 6971", "SANTANDER 5405", "BANCO AZTECA") -- son las
//              cuentas RECEPTORAS de la propia empresa, no la cuenta de
//              origen del cliente; no identifica nada.
// No repetir este intento sin una señal nueva y distinta.

// Cliente sugerido para un movimiento segun el catalogo de firmas (o null).
function sugerirClientePorFirma(concepto, catalogoFirmas) {
  for (const f of extraerFirmas(concepto)) {
    const cliente = catalogoFirmas.get(`${f.tipo}:${f.valor}`);
    if (cliente) return { firma: f, cliente };
  }
  return null;
}

// Construye candidatos (factura individual + pago completo) desde CFDIs tipo P,
// dedup SAT/ERP por uuid (SAT preferido -- ERP suele traer doctosRelacionados vacio).
// rfcs: si se da, acota el $match a esos receptor.rfc (mas rapido para catalogo por cliente).
// rfcEmpresa es OBLIGATORIO: un deposito bancario es dinero que ENTRA, así que
// solo sirven los CFDI de pago donde la empresa es el EMISOR (cobra) -- si la
// empresa aparece como RECEPTOR, es un pago que la empresa le hizo a un
// proveedor (cuenta por PAGAR), y cruzarlo contra depositos produce falsos
// positivos (visto en la practica: un candidato con receptor.rfc = la propia
// empresa se colaba como "ambiguo" en un cruce que en realidad no aplicaba).
async function construirCandidatosPago(CFDI, { ventanaInicio, ventanaFin, rfcEmpresa, rfcs = null }) {
  if (!rfcEmpresa) throw new Error('construirCandidatosPago requiere rfcEmpresa');

  const match = {
    tipoDeComprobante: 'P',
    isActive: true,
    'emisor.rfc': rfcEmpresa,
    'complementoPago.pagos.doctosRelacionados.0': { $exists: true },
    'complementoPago.pagos.fechaPago': { $gte: ventanaInicio, $lte: ventanaFin },
  };
  if (rfcs) match['receptor.rfc'] = { $in: rfcs };

  const pagosCfdi = await CFDI.aggregate([
    { $match: match },
    { $addFields: { _srcOrden: { $cond: [{ $eq: ['$source', 'SAT'] }, 0, 1] } } },
    { $sort: { uuid: 1, _srcOrden: 1 } },
    { $group: { _id: '$uuid', doc: { $first: '$$ROOT' } } },
    { $replaceRoot: { newRoot: '$doc' } },
    { $project: { uuid: 1, folio: 1, receptor: 1, 'complementoPago.pagos': 1 } },
  ]);

  const candidatosFactura = [];
  const candidatosPagoCompleto = [];

  for (const cfdi of pagosCfdi) {
    const rfc = cfdi.receptor?.rfc || null;
    const receptorNombre = cfdi.receptor?.nombre || rfc || '(sin nombre)';
    for (const pago of (cfdi.complementoPago?.pagos || [])) {
      const doctos = pago.doctosRelacionados || [];
      const fp = pago.fechaPago;
      if (!fp) continue;

      if (pago.monto != null) {
        candidatosPagoCompleto.push({
          tipo: 'pago_completo',
          cfdiPagoUuid: cfdi.uuid,
          folioPago: cfdi.folio,
          monto: pago.monto,
          fechaPago: fp,
          rfc,
          receptorNombre,
          facturas: doctos.map(d => d.idDocumento).filter(Boolean),
          // Desglose por factura -- para que quien acepte la sugerencia pueda armar
          // un erpLink por cada factura del pago (impPagado individual), no solo el
          // total agregado.
          facturasDetalle: doctos.filter(d => d.idDocumento).map(d => ({
            idDocumento: d.idDocumento, serie: d.serie ?? null, folio: d.folio ?? null, impPagado: d.impPagado ?? null,
          })),
        });
      }

      for (const d of doctos) {
        if (!d.idDocumento || d.impPagado == null) continue;
        candidatosFactura.push({
          tipo: 'factura',
          cfdiPagoUuid: cfdi.uuid,
          folioPago: cfdi.folio,
          idDocumento: d.idDocumento,
          serieFactura: d.serie,
          folioFactura: d.folio,
          impPagado: d.impPagado,
          fechaPago: fp,
          rfc,
          receptorNombre,
        });
      }
    }
  }

  return { candidatosFactura, candidatosPagoCompleto };
}

// Cruza un movimiento contra listas de candidatos (monto+fecha dentro de tolerancia).
// rfcFiltro: si se da, solo considera candidatos de ese RFC (usado cuando ya hay
// cliente confirmado por firma bancaria, para acotar/desambiguar).
function calcularHits(mov, candidatosFactura, candidatosPagoCompleto, { toleranciaMonto, toleranciaDias, rfcFiltro = null, excluirFacturas = null }) {
  const hits = [];

  for (const c of candidatosFactura) {
    if (rfcFiltro && c.rfc !== rfcFiltro) continue;
    if (excluirFacturas && excluirFacturas.has(c.idDocumento.toUpperCase())) continue;
    if (Math.abs(mov.deposito - c.impPagado) <= toleranciaMonto && diffDias(mov.fecha, c.fechaPago) <= toleranciaDias) {
      hits.push({ ...c, diffMonto: +(mov.deposito - c.impPagado).toFixed(2), diffDias: +diffDias(mov.fecha, c.fechaPago).toFixed(2) });
    }
  }
  for (const c of candidatosPagoCompleto) {
    if (rfcFiltro && c.rfc !== rfcFiltro) continue;
    if (Math.abs(mov.deposito - c.monto) <= toleranciaMonto && diffDias(mov.fecha, c.fechaPago) <= toleranciaDias) {
      hits.push({ ...c, diffMonto: +(mov.deposito - c.monto).toFixed(2), diffDias: +diffDias(mov.fecha, c.fechaPago).toFixed(2) });
    }
  }

  // Dedup: un "pago_completo" de una sola factura es el MISMO vinculo que su
  // candidato "factura" homologo (mismo cfdiPagoUuid + misma factura) -- sin esto
  // un pago de 1 sola factura se cuenta 2 veces y parece "ambiguo" sin serlo.
  const vistos = new Set();
  const hitsUnicos = hits.filter(h => {
    const key = (h.tipo === 'pago_completo' && h.facturas.length === 1)
      ? `factura:${h.facturas[0].toUpperCase()}`
      : (h.tipo === 'factura' ? `factura:${h.idDocumento.toUpperCase()}` : `pago:${h.cfdiPagoUuid.toUpperCase()}`);
    if (vistos.has(key)) return false;
    vistos.add(key);
    return true;
  });

  hitsUnicos.sort((a, b) => (a.diffDias - b.diffDias) || (Math.abs(a.diffMonto) - Math.abs(b.diffMonto)));
  return hitsUnicos;
}

module.exports = {
  RFC_GENERICOS,
  diffDias,
  extraerFirmas,
  construirCatalogoFirmas,
  sugerirClientePorFirma,
  construirCandidatosPago,
  calcularHits,
};
