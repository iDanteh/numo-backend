'use strict';

/**
 * backfill-discrepancy-tipo.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Rellena `tipoDeComprobante` en las Discrepancy que no lo tienen, tomándolo
 * del CFDI de su `uuid` (copia SAT primero, si no la ERP). La descarga
 * automática del SAT (`guardarResultados` en satSyncJob.js) no lo guardaba
 * hasta 2026-10-08, así que la tabla "Tipos de discrepancia" del dashboard
 * (que ahora cuenta solo Ingresos, Egresos y Pagos) no podía separar esas
 * discrepancias de las de Nómina y Traslados.
 *
 * Solo escribe `tipoDeComprobante` en documentos donde hoy está vacío — nunca
 * pisa un tipo ya guardado ni toca ningún otro campo.
 *
 * Por defecto corre en modo DRY-RUN (solo reporta qué cambiaría). Para
 * escribir de verdad hay que pasar --confirm explícito.
 *
 * Uso:
 *   node src/visor/scripts/backfill-discrepancy-tipo.js            (dry-run)
 *   node src/visor/scripts/backfill-discrepancy-tipo.js --confirm  (escribe)
 */

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const Discrepancy = require('../models/Discrepancy');
const CFDI        = require('../models/CFDI');

const CONFIRM = process.argv.includes('--confirm');
const LOTE = 5000;
const TIPOS_VALIDOS = ['I', 'E', 'T', 'N', 'P'];

async function main() {
  const sinTipo = await Discrepancy.find(
    { $or: [{ tipoDeComprobante: null }, { tipoDeComprobante: { $exists: false } }] },
    { uuid: 1 },
  ).lean();
  console.log(`Discrepancias sin tipoDeComprobante: ${sinTipo.length}`);

  const porTipo = {};
  let sinCfdi = 0;
  let actualizadas = 0;

  for (let i = 0; i < sinTipo.length; i += LOTE) {
    const lote = sinTipo.slice(i, i + LOTE);
    const uuids = [...new Set(lote.map(d => String(d.uuid).toUpperCase()))];
    const cfdis = await CFDI.find({ uuid: { $in: uuids } }, { uuid: 1, source: 1, tipoDeComprobante: 1 }).lean();

    // SAT primero (es el que originó la discrepancia "En SAT, no en ERP");
    // si no hay copia SAT, la de ERP/MANUAL.
    const tipoPorUuid = new Map();
    for (const c of cfdis) {
      const t = String(c.tipoDeComprobante || '').toUpperCase();
      if (!TIPOS_VALIDOS.includes(t)) continue;
      const u = String(c.uuid).toUpperCase();
      if (c.source === 'SAT' || !tipoPorUuid.has(u)) tipoPorUuid.set(u, t);
    }

    const ops = [];
    for (const d of lote) {
      const t = tipoPorUuid.get(String(d.uuid).toUpperCase());
      if (!t) { sinCfdi++; continue; }
      porTipo[t] = (porTipo[t] ?? 0) + 1;
      ops.push({
        updateOne: {
          filter: { _id: d._id, $or: [{ tipoDeComprobante: null }, { tipoDeComprobante: { $exists: false } }] },
          update: { $set: { tipoDeComprobante: t } },
        },
      });
    }
    if (CONFIRM && ops.length) {
      const r = await Discrepancy.bulkWrite(ops, { ordered: false });
      actualizadas += r.modifiedCount ?? 0;
    }
  }

  console.log('Tipo encontrado por CFDI:', porTipo);
  console.log(`Sin CFDI con tipo válido (se dejan igual): ${sinCfdi}`);
  console.log(CONFIRM
    ? `Actualizadas: ${actualizadas}`
    : 'DRY-RUN: no se escribió nada. Corre con --confirm para aplicar.');
}

connectMongo()
  .then(main)
  .then(async () => {
    await disconnectMongo();
    process.exit(0);
  })
  .catch(async (err) => {
    console.error('ERROR', err);
    await disconnectMongo().catch(() => {});
    process.exit(1);
  });
