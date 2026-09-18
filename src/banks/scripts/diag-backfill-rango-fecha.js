'use strict';

/**
 * Diagnóstico de solo lectura para confirmar/descartar la hipótesis sobre por qué
 * backfill-saldo-erp-finalizado-manualmente.js reporta "sin cambio" en los 10 casos
 * conocidos (ver project_sync_erp_kore_finalizado_manualmente_gap.md).
 *
 * Hipótesis: _rangoDesdeFollo(folioExterno) deriva el rango de fecha del MES DE APERTURA
 * de la CxC, no de cuándo se aplicó el cobro real. Si el cobro se aplicó semanas/meses
 * después (caso típico de Solicitudes de Cobro), Kore nunca reporta esa línea dentro de
 * ese rango y el matching por tag (Numo/Aut/Num Recibo) nunca encuentra nada.
 *
 * Qué hace: para UN movimiento puntual, consulta Kore con (a) el rango derivado del
 * folioExterno (el que ya usa el backfill, reproduce el "sin cambio") y (b) el rango
 * derivado de identificadoPor[].fechaId (cuándo un humano aplicó el cobro en Numo).
 * Imprime movimientos/formasPago/adicionales de ambos para comparar a ojo.
 *
 * No escribe nada en la base de datos.
 *
 * Uso: node src/banks/scripts/diag-backfill-rango-fecha.js [erpId]
 */

require('dotenv').config();

const mongoose     = require('mongoose');
const BankMovement = require('../domains/banks/BankMovement.model');
const erpRoutes    = require('../domains/erp/erp.routes');

const ERP_ID = process.argv[2] ?? '6a7e6386f1a6ec0001f912ab'; // D0-260802730, folio 038309

function describirRaw(raw0, etiqueta) {
  console.log(`\n--- ${etiqueta} ---`);
  if (!raw0) { console.log('  (sin datos)'); return; }
  console.log(`  saldoActual=${raw0.saldoActual}  movimientos=${(raw0.movimientos ?? []).length}`);
  for (const m of raw0.movimientos ?? []) {
    console.log(`  · serie=${m.serie} folio=${m.folio} fecha=${m.fecha} total=${m.total}`);
    for (const fp of m.formasPago ?? []) {
      const tags = (fp.adicionales ?? []).map(a => `${a.nombre}=${a.valor}`).join(', ');
      console.log(`      formaPago=${fp.nombreFormaPago} monto=${fp.monto} [${tags}]`);
    }
  }
}

async function run() {
  await mongoose.connect(process.env.MONGODB_URI);
  console.log('Conectado a MongoDB.');

  const mov = await BankMovement.findOne({ erpIds: ERP_ID }).lean();
  if (!mov) { console.log(`No se encontró ningún BankMovement con erpId=${ERP_ID}`); process.exit(1); }

  const link = mov.erpLinks.find(l => l.erpId === ERP_ID);
  const idEntry = (mov.identificadoPor ?? []).find(ip => ip.erpId === ERP_ID);

  console.log(`\nMovimiento: _id=${mov._id} folio=${mov.folio} numeroAutorizacion=${mov.numeroAutorizacion}`);
  console.log(`Link: serie=${link.serie} folioExterno=${link.folioExterno} saldoErpAportado=${link.saldoErpAportado} saldoPagado=${link.saldoPagado}`);
  console.log(`identificadoPor para este erpId: userId=${idEntry?.userId} fechaId=${idEntry?.fechaId ?? 'null'}`);

  // (a) Rango derivado del folioExterno — el que ya usa el backfill.
  const rangoFolio = erpRoutes._rangoDesdeFollo(link.folioExterno);
  console.log(`\nRango por folioExterno (${link.folioExterno}): ${rangoFolio.fechaDesde} .. ${rangoFolio.fechaHasta}`);
  const { raw: rawFolio } = await erpRoutes._sincronizarConRetry({
    serieExterna: link.serie, folioExterno: String(link.folioExterno),
    fechaDesde: rangoFolio.fechaDesde, fechaHasta: rangoFolio.fechaHasta,
  });
  describirRaw(rawFolio[0], 'Consulta con rango del folioExterno (mes de apertura)');

  // (b) Rango derivado de fechaId — cuándo se aplicó el cobro real en Numo.
  if (idEntry?.fechaId) {
    const f = new Date(idEntry.fechaId);
    const year = f.getUTCFullYear();
    const mes  = f.getUTCMonth(); // 0-indexed
    const fechaDesde = new Date(Date.UTC(year, mes, 1, 0, 0, 0)).toISOString();
    const fechaHasta = new Date(Date.UTC(year, mes + 1, 0, 23, 59, 59)).toISOString();
    console.log(`\nRango por fechaId (${idEntry.fechaId}): ${fechaDesde} .. ${fechaHasta}`);
    const { raw: rawFechaId } = await erpRoutes._sincronizarConRetry({
      serieExterna: link.serie, folioExterno: String(link.folioExterno),
      fechaDesde, fechaHasta,
    });
    describirRaw(rawFechaId[0], 'Consulta con rango de fechaId (mes real del cobro)');
  } else {
    console.log('\nNo hay fechaId registrado en identificadoPor — no se puede probar el rango alternativo.');
  }

  await mongoose.connection.close();
}

run().catch(err => { console.error(err); process.exit(1); });
