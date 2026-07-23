'use strict';

/**
 * diag-match-monto-fecha.js
 *
 * Motor de match FALLBACK (solo diagnostico, no escribe nada) para depositos
 * bancarios 'no_identificado' cuya CxC ya no aparece en erp_cuentas_pendientes
 * (porque el ERP solo expone /cuentas-pendientes -- cuentas ABIERTAS -- y una
 * factura recien saldada desaparece de ese feed antes de que el banco importe
 * el deposito). El motor real (matchAutorizacionesDesdeErp) depende 100% de
 * ese feed via numeroAutorizacion/referenciaNumerica, asi que para esos casos
 * nunca tiene con que cruzar.
 *
 * Aqui se usa una fuente que NUNCA expira: la coleccion `cfdis` (CFDI tipo P,
 * complemento de pago) conserva para siempre impPagado + fechaPago por cada
 * factura pagada, sin importar si la CxC sigue "pendiente" en el ERP.
 *
 * Campos cruzados:
 *   - bank_movements.deposito  <-> complementoPago.pagos.doctosRelacionados[].impPagado (1 factura)
 *   - bank_movements.deposito  <-> complementoPago.pagos[].monto                        (pago completo, N facturas)
 *   - bank_movements.fecha     <-> complementoPago.pagos[].fechaPago  (+-TOLERANCIA_DIAS)
 *
 * No hay numero de autorizacion como ancla, asi que monto+fecha es la unica
 * senal disponible. Por eso NO se auto-vincula nada: se reporta cada
 * candidato y se marca AMBIGUO si mas de una factura/pago cae dentro de la
 * tolerancia para el mismo movimiento (o viceversa), para que un humano
 * decida antes de escribir erpLinks.
 *
 * Uso:
 *   node src/banks/scripts/diag-match-monto-fecha.js <fechaInicio> <fechaFin> [banco]
 *   node src/banks/scripts/diag-match-monto-fecha.js 2026-07-01 2026-07-22
 *   node src/banks/scripts/diag-match-monto-fecha.js 2026-07-01 2026-07-22 Banamex
 */

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const CFDI = require('../models/CFDI');
const BankMovement = require('../../banks/domains/banks/BankMovement.model');

const TOLERANCIA_MONTO = 2;    // pesos
const TOLERANCIA_DIAS  = 5;    // dias entre fecha del deposito y fechaPago del CFDI
const MONTO_MINIMO     = 10;   // pesos -- por debajo de esto son residuos/centavos, no depositos reales a conciliar
const RFC_EMPRESA      = 'CCO011113663'; // un deposito es dinero que ENTRA -- solo sirven CFDI de pago donde la empresa es EMISOR (cobra), no receptor (pago a proveedor)

const fechaInicioArg = process.argv[2];
const fechaFinArg    = process.argv[3];
const bancoArg       = process.argv[4];

if (!fechaInicioArg || !fechaFinArg) {
  console.error('Uso: node diag-match-monto-fecha.js <fechaInicio YYYY-MM-DD> <fechaFin YYYY-MM-DD> [banco]');
  process.exit(1);
}

const fechaInicio = new Date(fechaInicioArg + 'T00:00:00Z');
const fechaFin     = new Date(fechaFinArg + 'T23:59:59Z');

function diffDias(a, b) {
  return Math.abs(new Date(a).getTime() - new Date(b).getTime()) / 86400000;
}

async function main() {
  await connectMongo();

  // ── 1. Movimientos elegibles: mismos criterios de base que el motor real ──
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

  // ── 2. Facturas ya vinculadas -- para no proponer algo que ya se resolvio ──
  const yaVinculadas = new Set(
    (await BankMovement.distinct('erpLinks.folioFiscal', { isActive: true }))
      .filter(Boolean)
      .map(f => f.toUpperCase()),
  );
  console.log(`Facturas ya vinculadas en el sistema: ${yaVinculadas.size}`);

  // ── 3. CFDIs tipo P con doctosRelacionados, en ventana ampliada por tolerancia ──
  const ventanaInicio = new Date(fechaInicio.getTime() - TOLERANCIA_DIAS * 86400000);
  const ventanaFin     = new Date(fechaFin.getTime()   + TOLERANCIA_DIAS * 86400000);

  const pagosCfdi = await CFDI.aggregate([
    {
      $match: {
        tipoDeComprobante: 'P',
        isActive: true,
        'emisor.rfc': RFC_EMPRESA,
        'complementoPago.pagos.doctosRelacionados.0': { $exists: true },
        'complementoPago.pagos.fechaPago': { $gte: ventanaInicio, $lte: ventanaFin },
      },
    },
    // Preferir SAT sobre ERP para el mismo uuid (ERP suele traer doctosRelacionados vacio,
    // pero por si acaso hay casos con datos en ambos, igual se prefiere SAT).
    { $addFields: { _srcOrden: { $cond: [{ $eq: ['$source', 'SAT'] }, 0, 1] } } },
    { $sort: { uuid: 1, _srcOrden: 1 } },
    { $group: { _id: '$uuid', doc: { $first: '$$ROOT' } } },
    { $replaceRoot: { newRoot: '$doc' } },
    { $project: { uuid: 1, folio: 1, serie: 1, receptor: 1, emisor: 1, 'complementoPago.pagos': 1 } },
  ]);
  console.log(`CFDIs de pago (dedup SAT/ERP) en ventana: ${pagosCfdi.length}`);

  // ── 4. Construir lista plana de candidatos: uno por factura (doctoRelacionado) + uno por pago completo ──
  const candidatosFactura = []; // { cfdiPagoUuid, folioPago, idDocumento, impPagado, fechaPago, receptorNombre }
  const candidatosPagoCompleto = []; // { cfdiPagoUuid, folioPago, monto, fechaPago, receptorNombre, facturas: [idDocumento,...] }

  for (const cfdi of pagosCfdi) {
    const receptorNombre = cfdi.receptor?.nombre || cfdi.receptor?.rfc || '(sin nombre)';
    for (const pago of (cfdi.complementoPago?.pagos || [])) {
      const doctos = pago.doctosRelacionados || [];
      const fp = pago.fechaPago;
      if (!fp) continue;

      // 4a. candidato "pago completo" (util cuando 1 deposito cubre N facturas de un mismo pago)
      if (pago.monto != null) {
        candidatosPagoCompleto.push({
          cfdiPagoUuid: cfdi.uuid,
          folioPago:    cfdi.folio,
          monto:        pago.monto,
          fechaPago:    fp,
          receptorNombre,
          facturas:     doctos.map(d => d.idDocumento).filter(Boolean),
        });
      }

      // 4b. candidato "factura individual"
      for (const d of doctos) {
        if (!d.idDocumento || d.impPagado == null) continue;
        if (yaVinculadas.has(d.idDocumento.toUpperCase())) continue; // ya resuelta
        candidatosFactura.push({
          cfdiPagoUuid: cfdi.uuid,
          folioPago:    cfdi.folio,
          idDocumento:  d.idDocumento,
          serieFactura: d.serie,
          folioFactura: d.folio,
          impPagado:    d.impPagado,
          fechaPago:    fp,
          receptorNombre,
        });
      }
    }
  }
  console.log(`Candidatos factura-individual (no vinculados aun): ${candidatosFactura.length}`);
  console.log(`Candidatos pago-completo: ${candidatosPagoCompleto.length}`);

  // ── 5. Cruce por movimiento ──────────────────────────────────────────────
  let conMatchUnico = 0, conMatchAmbiguo = 0, sinMatch = 0;
  const resultados = [];

  for (const mov of movimientos) {
    const hits = [];

    for (const c of candidatosFactura) {
      if (Math.abs(mov.deposito - c.impPagado) <= TOLERANCIA_MONTO && diffDias(mov.fecha, c.fechaPago) <= TOLERANCIA_DIAS) {
        hits.push({ tipo: 'factura', ...c, diffMonto: +(mov.deposito - c.impPagado).toFixed(2), diffDias: +diffDias(mov.fecha, c.fechaPago).toFixed(2) });
      }
    }
    for (const c of candidatosPagoCompleto) {
      if (Math.abs(mov.deposito - c.monto) <= TOLERANCIA_MONTO && diffDias(mov.fecha, c.fechaPago) <= TOLERANCIA_DIAS) {
        hits.push({ tipo: 'pago_completo', ...c, diffMonto: +(mov.deposito - c.monto).toFixed(2), diffDias: +diffDias(mov.fecha, c.fechaPago).toFixed(2) });
      }
    }

    if (hits.length === 0) {
      sinMatch++;
      continue;
    }

    // Dedup: un "pago_completo" de una sola factura es el MISMO vinculo que su
    // candidato "factura" homologo -- sin esto, un pago de 1 factura aparece
    // dos veces y se cuenta como AMBIGUO cuando en realidad es un match unico.
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
    const hits2 = hitsUnicos;
    const esUnico = hits2.length === 1;
    if (esUnico) conMatchUnico++; else conMatchAmbiguo++;

    resultados.push({
      movimiento: { _id: mov._id, banco: mov.banco, fecha: mov.fecha, deposito: mov.deposito, folio: mov.folio, concepto: mov.concepto?.slice(0, 80) },
      estado: esUnico ? 'MATCH_UNICO' : 'AMBIGUO',
      candidatos: hits2.slice(0, 5),
    });
  }

  console.log(`\n=== RESUMEN ===`);
  console.log(`Match unico:  ${conMatchUnico}`);
  console.log(`Ambiguo:      ${conMatchAmbiguo}`);
  console.log(`Sin match:    ${sinMatch}`);
  console.log(`\n=== DETALLE (match unico primero) ===`);
  resultados.sort((a, b) => (a.estado === 'MATCH_UNICO' ? -1 : 1) - (b.estado === 'MATCH_UNICO' ? -1 : 1));
  console.log(JSON.stringify(resultados, null, 2));

  await disconnectMongo();
  process.exit(0);
}

main().catch(function (err) { console.error(err); process.exit(1); });
