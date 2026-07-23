'use strict';

/**
 * diag-sugerencias-conciliacion.js
 *
 * Une los dos motores fallback (monto+fecha contra `cfdis`, y firma bancaria
 * contra el historico de `bank_movements`) en UNA sola lista de sugerencias
 * por movimiento, en orden de confianza. Solo lectura -- no escribe erpLinks.
 *
 * Nivel de confianza (de mayor a menor):
 *   1. CONFIRMADO_FIRMA_CFDI   -- firma bancaria dice "cliente X" Y hay
 *                                 exactamente 1 CFDI de pago de X que calza
 *                                 en monto+fecha. Si el cruce global (sin
 *                                 filtrar por cliente) era ambiguo, la firma
 *                                 lo resuelve -- se marca `ambiguedadResueltaPorFirma`.
 *   2. MATCH_UNICO_MONTO_FECHA -- sin firma reconocida, pero el cruce global
 *                                 monto+fecha (cualquier cliente) da un solo
 *                                 candidato.
 *   3. SOLO_FIRMA              -- firma reconocida (cliente conocido por
 *                                 historial), pero ningun CFDI de pago de ese
 *                                 cliente calza en la ventana de fecha/monto.
 *                                 Util para revision manual aunque no haya
 *                                 factura especifica identificada.
 *   4. AMBIGUO                 -- >1 candidato por monto+fecha y sin firma
 *                                 que desempate.
 *   5. SIN_SUGERENCIA          -- nada que proponer con las señales actuales.
 *
 * Uso:
 *   node src/banks/scripts/diag-sugerencias-conciliacion.js <fechaInicio> <fechaFin> [banco]
 */

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const CFDI = require('../models/CFDI');
const BankMovement = require('../../banks/domains/banks/BankMovement.model');
const {
  construirCatalogoFirmas,
  sugerirClientePorFirma,
  construirCandidatosPago,
  calcularHits,
} = require('../services/conciliacion-fallback.util');

const TOLERANCIA_MONTO       = 2;   // pesos
const TOLERANCIA_DIAS_GLOBAL = 5;   // ventana para el cruce SIN cliente confirmado
const TOLERANCIA_DIAS_FIRMA  = 15;  // ventana mas laxa cuando ya sabemos el cliente por firma
const MONTO_MINIMO           = 10;  // por debajo de esto son residuos/centavos
const RFC_EMPRESA            = 'CCO011113663'; // un deposito es dinero que ENTRA -- solo sirven CFDI de pago donde la empresa es EMISOR (cobra), no receptor (pago a proveedor)

const fechaInicioArg = process.argv[2];
const fechaFinArg    = process.argv[3];
const bancoArg       = process.argv[4];

if (!fechaInicioArg || !fechaFinArg) {
  console.error('Uso: node diag-sugerencias-conciliacion.js <fechaInicio YYYY-MM-DD> <fechaFin YYYY-MM-DD> [banco]');
  process.exit(1);
}

const fechaInicio = new Date(fechaInicioArg + 'T00:00:00Z');
const fechaFin    = new Date(fechaFinArg + 'T23:59:59Z');

async function main() {
  await connectMongo();

  // ── Movimientos elegibles ────────────────────────────────────────────────
  const movimientos = await BankMovement.find({
    isActive: true,
    status:   'no_identificado',
    deposito: { $gt: MONTO_MINIMO },
    fecha:    { $gte: fechaInicio, $lte: fechaFin },
    erpLinks: { $size: 0 }, // ya vinculado (aunque status no se haya recalculado) -- fuera de alcance de este motor
    ...(bancoArg ? { banco: bancoArg } : {}),
  }).select('_id banco fecha deposito folio concepto').sort({ fecha: 1 }).lean();
  console.log(`Movimientos no_identificado en rango: ${movimientos.length}`);
  if (!movimientos.length) { await disconnectMongo(); return process.exit(0); }

  // ── Catalogo firma -> cliente (historico completo, nunca se purga) ──────
  const { catalogo, firmasAmbiguas, totalIdentificadosConFirma } = await construirCatalogoFirmas(BankMovement, CFDI);
  console.log(`Catalogo: ${totalIdentificadosConFirma} movimientos identificados con firma reconocible, ${catalogo.size} firmas utilizables (${firmasAmbiguas} descartadas por ambiguas)`);

  // ── Facturas ya vinculadas -- no proponer lo ya resuelto ────────────────
  const yaVinculadas = new Set(
    (await BankMovement.distinct('erpLinks.folioFiscal', { isActive: true })).filter(Boolean).map(f => f.toUpperCase()),
  );

  // ── Candidatos globales (ventana ajustada a tolerancia mas amplia, para cubrir ambos casos) ──
  const ventanaInicio = new Date(fechaInicio.getTime() - TOLERANCIA_DIAS_FIRMA * 86400000);
  const ventanaFin    = new Date(fechaFin.getTime() + TOLERANCIA_DIAS_FIRMA * 86400000);
  const { candidatosFactura, candidatosPagoCompleto } = await construirCandidatosPago(CFDI, { ventanaInicio, ventanaFin, rfcEmpresa: RFC_EMPRESA });
  const candidatosFacturaLibres = candidatosFactura.filter(c => !yaVinculadas.has(c.idDocumento.toUpperCase()));
  console.log(`Candidatos factura (no vinculados aun): ${candidatosFacturaLibres.length} · pago completo: ${candidatosPagoCompleto.length}`);

  // ── Cruce por movimiento ─────────────────────────────────────────────────
  const conteo = { CONFIRMADO_FIRMA_CFDI: 0, MATCH_UNICO_MONTO_FECHA: 0, SOLO_FIRMA: 0, AMBIGUO: 0, SIN_SUGERENCIA: 0 };
  const resultados = [];

  for (const mov of movimientos) {
    const sugerencia = sugerirClientePorFirma(mov.concepto, catalogo);

    // Cruce GLOBAL (cualquier cliente) con tolerancia de fecha estricta
    const hitsGlobal = calcularHits(mov, candidatosFacturaLibres, candidatosPagoCompleto, {
      toleranciaMonto: TOLERANCIA_MONTO, toleranciaDias: TOLERANCIA_DIAS_GLOBAL,
    });

    let estado, candidatos, extra = {};

    if (sugerencia) {
      // Cruce acotado al cliente sugerido, con ventana de fecha mas laxa
      const hitsCliente = calcularHits(mov, candidatosFacturaLibres, candidatosPagoCompleto, {
        toleranciaMonto: TOLERANCIA_MONTO, toleranciaDias: TOLERANCIA_DIAS_FIRMA, rfcFiltro: sugerencia.cliente.rfc,
      });

      if (hitsCliente.length === 1) {
        estado = 'CONFIRMADO_FIRMA_CFDI';
        candidatos = hitsCliente;
        extra.clienteSugerido = { rfc: sugerencia.cliente.rfc, nombre: sugerencia.cliente.nombre, vecesVistoEnHistorico: sugerencia.cliente.count, firma: sugerencia.firma };
        if (hitsGlobal.length > 1) extra.ambiguedadResueltaPorFirma = true;
      } else if (hitsCliente.length === 0) {
        estado = 'SOLO_FIRMA';
        candidatos = [];
        extra.clienteSugerido = { rfc: sugerencia.cliente.rfc, nombre: sugerencia.cliente.nombre, vecesVistoEnHistorico: sugerencia.cliente.count, firma: sugerencia.firma };
      } else {
        // Mas de 1 factura del MISMO cliente calza -- sigue ambiguo pero ya acotado a 1 cliente
        estado = 'AMBIGUO';
        candidatos = hitsCliente;
        extra.clienteSugerido = { rfc: sugerencia.cliente.rfc, nombre: sugerencia.cliente.nombre, vecesVistoEnHistorico: sugerencia.cliente.count, firma: sugerencia.firma };
        extra.nota = 'Ambiguo incluso dentro del mismo cliente (varias facturas suyas calzan en monto/fecha)';
      }
    } else if (hitsGlobal.length === 1) {
      estado = 'MATCH_UNICO_MONTO_FECHA';
      candidatos = hitsGlobal;
    } else if (hitsGlobal.length > 1) {
      estado = 'AMBIGUO';
      candidatos = hitsGlobal;
    } else {
      estado = 'SIN_SUGERENCIA';
      candidatos = [];
    }

    conteo[estado]++;
    if (estado !== 'SIN_SUGERENCIA') {
      resultados.push({
        movimiento: { _id: mov._id, banco: mov.banco, fecha: mov.fecha, deposito: mov.deposito, folio: mov.folio, concepto: mov.concepto?.slice(0, 90) },
        estado,
        ...extra,
        candidatos: candidatos.slice(0, 5),
      });
    }
  }

  const orden = ['CONFIRMADO_FIRMA_CFDI', 'MATCH_UNICO_MONTO_FECHA', 'SOLO_FIRMA', 'AMBIGUO'];
  resultados.sort((a, b) => orden.indexOf(a.estado) - orden.indexOf(b.estado));

  console.log(`\n=== RESUMEN (${movimientos.length} movimientos no_identificado) ===`);
  for (const k of [...orden, 'SIN_SUGERENCIA']) console.log(`${k.padEnd(26)}: ${conteo[k]}`);

  console.log(`\n=== DETALLE ===`);
  console.log(JSON.stringify(resultados, null, 2));

  await disconnectMongo();
  process.exit(0);
}

main().catch(function (err) { console.error(err); process.exit(1); });
