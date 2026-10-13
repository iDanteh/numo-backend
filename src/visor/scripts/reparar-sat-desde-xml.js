'use strict';

/**
 * reparar-sat-desde-xml.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Restaura, desde su propio `xmlContent`, los datos de las copias SAT que la
 * carga del reporte Excel del SAT (`importExcel`, cfdi.controller.js) pisó
 * hasta 2026-10-13: conceptos (quedaban sin impuestos), impuestos,
 * cfdiRelacionados (relación '04' supuesta cuando el Excel no trae la columna
 * TipoRelacion — 1,255 Egresos con 04 falso, ej. E587E3C2 H0-261000130 cuyo
 * XML dice 03), fecha (sin hora), timbre (sin sellos), emisor/receptor, etc.
 *
 * Re-parsea el XML con el mismo `parseCFDI` de la descarga y solo escribe los
 * campos que difieren. NUNCA toca satStatus, ejercicio/periodo (reclasifica-
 * ciones), comparación/conciliación, tipoOrigen ni documentosRelacionados.
 *
 * Por defecto solo revisa copias que pasaron por la carga de Excel
 * (`uploadedBy` presente); `--todos` revisa todas las que tienen XML.
 * Corre en DRY-RUN (solo reporta); `--confirm` escribe.
 *
 * Uso:
 *   node src/visor/scripts/reparar-sat-desde-xml.js                (dry-run)
 *   node src/visor/scripts/reparar-sat-desde-xml.js --confirm      (escribe)
 *   node src/visor/scripts/reparar-sat-desde-xml.js --todos [--confirm]
 */

// parseCFDI convierte la Fecha del XML (sin zona) con la zona del proceso; la
// descarga SAT corre en el contenedor en UTC, así quedaron guardadas. Fuera de
// ese contexto (ej. una PC en hora de México) todo saldría corrido 6 h.
process.env.TZ = 'UTC';

require('dotenv').config();

const CFDI = require('../models/CFDI');
const { parseCFDI } = require('../services/cfdiParser');

// Solo los campos que guarda la descarga SAT desde el XML (satSyncJob) — los
// que el Excel sobrescribía. formaPago/metodoPago/descuento/lugarExpedicion no
// se incluyen: la descarga no los guarda y "restaurarlos" tocaría copias sanas.
const CAMPOS_XML = [
  'version', 'serie', 'folio', 'fecha', 'subTotal', 'total', 'moneda', 'tipoDeComprobante',
  'emisor', 'receptor', 'conceptos', 'impuestos', 'cfdiRelacionados',
  'complementoPago', 'timbreFiscalDigital',
];

// Forma comparable: sin _id, sin vacíos (null/undefined/''/[]/{}), llaves ordenadas
// y fechas en ISO — la copia guardada y la recién parseada pueden diferir solo
// en eso sin que haya un cambio real.
function normalizar(v) {
  if (v instanceof Date) return isNaN(v.getTime()) ? null : v.toISOString();
  if (v && typeof v === 'object' && typeof v.toHexString === 'function') return null; // ObjectId
  if (Array.isArray(v)) {
    const arr = v.map(normalizar).filter(x => x !== undefined);
    return arr.length ? arr : undefined;
  }
  if (v && typeof v === 'object') {
    const out = {};
    for (const k of Object.keys(v).sort()) {
      if (k === '_id') continue;
      const n = normalizar(v[k]);
      if (n !== undefined) out[k] = n;
    }
    return Object.keys(out).length ? out : undefined;
  }
  if (v === null || v === undefined || v === '') return undefined;
  return v;
}

/**
 * Compara la copia guardada contra su XML re-parseado.
 * @returns {{ set: object, campos: string[] }} campos a restaurar (forma del schema)
 */
function diferenciasContraXml(doc, parsed) {
  // Pasar lo parseado por el schema: mismo casteo que tendría al guardarse.
  const casteado = new CFDI(Object.fromEntries(
    CAMPOS_XML.filter(c => parsed[c] !== undefined).map(c => [c, parsed[c]]),
  )).toObject({ depopulate: true });

  const set = {};
  const campos = [];
  for (const campo of CAMPOS_XML) {
    if (parsed[campo] === undefined || parsed[campo] === null) continue;
    const nuevo = normalizar(casteado[campo]);
    if (nuevo === undefined) continue;
    if (JSON.stringify(nuevo) === JSON.stringify(normalizar(doc[campo]))) continue;
    set[campo] = casteado[campo];
    campos.push(campo);
  }
  return { set, campos };
}

async function repararSatDesdeXml({ confirm = false, todos = false } = {}) {
  const filtro = {
    source: 'SAT',
    xmlContent: { $exists: true, $nin: [null, ''] },
    ...(todos ? {} : { uploadedBy: { $exists: true, $ne: null } }),
  };
  const total = await CFDI.countDocuments(filtro);
  console.log(`[reparar-sat-desde-xml] ${total} copia(s) SAT con XML a revisar${todos ? ' (todas)' : ' (pasaron por carga de Excel)'}.`);

  const porCampo = {};
  const ejemplos = [];
  let revisados = 0, conDiferencias = 0, escritos = 0, errores = 0, uuidDistinto = 0, rel04Falso = 0;
  let ops = [];

  const flush = async () => {
    if (confirm && ops.length) {
      const r = await CFDI.bulkWrite(ops, { ordered: false });
      escritos += r.modifiedCount ?? 0;
    }
    ops = [];
  };

  const cursor = CFDI.find(filtro).select('+xmlContent ' + CAMPOS_XML.join(' ') + ' uuid').lean().cursor({ batchSize: 300 });
  for await (const doc of cursor) {
    revisados++;
    let parsed;
    try {
      parsed = await parseCFDI(doc.xmlContent);
    } catch (err) {
      errores++;
      if (errores <= 10) console.error(`  ✖ ${doc.uuid}: XML no parseable — ${err.message}`);
      continue;
    }
    if ((parsed.uuid || '').toUpperCase() !== (doc.uuid || '').toUpperCase()) {
      uuidDistinto++;
      continue;
    }
    const { set, campos } = diferenciasContraXml(doc, parsed);
    if (!campos.length) continue;

    conDiferencias++;
    campos.forEach(c => { porCampo[c] = (porCampo[c] ?? 0) + 1; });
    const relAntes = (doc.cfdiRelacionados ?? []).map(r => r.tipoRelacion).join(',');
    const relXml   = (parsed.cfdiRelacionados ?? []).map(r => r.tipoRelacion).join(',');
    if (campos.includes('cfdiRelacionados') && relAntes.includes('04') && !relXml.includes('04')) rel04Falso++;
    if (ejemplos.length < 15) {
      ejemplos.push(`${doc.serie ?? ''}-${doc.folio ?? ''} ${doc.uuid} [${doc.tipoDeComprobante}] → ${campos.join(', ')}` +
        (campos.includes('cfdiRelacionados') ? ` (relación ${relAntes || '—'} → ${relXml || '—'})` : ''));
    }
    ops.push({ updateOne: { filter: { _id: doc._id }, update: { $set: set } } });
    if (ops.length >= 500) await flush();
    if (revisados % 5000 === 0) console.log(`  … ${revisados}/${total} revisados, ${conDiferencias} con diferencias`);
  }
  await flush();

  console.log('\n[reparar-sat-desde-xml] === RESUMEN ===');
  console.log(`Revisados:                 ${revisados}`);
  console.log(`Con diferencias vs XML:    ${conDiferencias}`);
  console.log(`  relación 04 falsa:       ${rel04Falso}`);
  console.log('Por campo:', porCampo);
  console.log(`XML no parseable:          ${errores}`);
  console.log(`UUID del XML distinto:     ${uuidDistinto} (se dejan igual)`);
  if (ejemplos.length) console.log('Ejemplos:\n  ' + ejemplos.join('\n  '));
  console.log(confirm
    ? `\n✓ Escritos: ${escritos}`
    : '\nDry-run — no se escribió nada. Volver a correr con --confirm para aplicar.');

  return { revisados, conDiferencias, porCampo, errores, uuidDistinto, rel04Falso, escritos };
}

if (require.main === module) {
  const args = process.argv.slice(2);
  const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
  connectMongo()
    .then(async () => {
      await repararSatDesdeXml({ confirm: args.includes('--confirm'), todos: args.includes('--todos') });
      await disconnectMongo();
      process.exit(0);
    })
    .catch((err) => {
      console.error('[reparar-sat-desde-xml] Error:', err.message);
      process.exit(1);
    });
}

module.exports = { repararSatDesdeXml, diferenciasContraXml, normalizar };
