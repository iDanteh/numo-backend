'use strict';

/**
 * reparar-relacion-04-excel.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Copias SAT SIN XML que la carga del reporte Excel del SAT dejó con relación
 * '04' (sustitución) supuesta — `importExcel` la ponía por default cuando el
 * Excel no trae la columna TipoRelacion (corregido 2026-10-13). Sin XML,
 * `reparar-sat-desde-xml.js` no las alcanza. Caso real 12-oct: C0-261000642 y
 * M0-261000376 (aplicación de anticipo, relación 07 en el ERP) quedaron como
 * sustitutos de sus anticipos C0-261000641/M0-261000375 → Ingreso los trató
 * como cancelados. En Pagos, la "relación" del Excel es la factura pagada, no
 * una sustitución, y el 04 los hacía pasar por sustitutos en Cobranza.
 *
 * Por cada relación '04' de la copia SAT: si la copia ERP del mismo CFDI trae
 * una relación con alguno de esos uuids, se usa SU tipo; si no, queda sin tipo
 * (null). Solo toca `cfdiRelacionados` de copias SAT sin XML que pasaron por la
 * carga de Excel (`uploadedBy`).
 *
 * Uso:
 *   node src/visor/scripts/reparar-relacion-04-excel.js            (dry-run)
 *   node src/visor/scripts/reparar-relacion-04-excel.js --confirm  (escribe)
 */

require('dotenv').config();

const CFDI = require('../models/CFDI');

function relacionesCorregidas(relSat, relErp) {
  let cambio = false;
  const nuevas = (relSat ?? []).map(r => {
    if (r.tipoRelacion !== '04') return { tipoRelacion: r.tipoRelacion ?? null, uuids: r.uuids ?? [] };
    const uuids = (r.uuids ?? []).map(u => String(u).toUpperCase());
    const deErp = (relErp ?? []).find(e => (e.uuids ?? []).some(u => uuids.includes(String(u).toUpperCase())));
    const tipo = deErp?.tipoRelacion ?? null;
    if (tipo !== '04') cambio = true;
    return { tipoRelacion: tipo, uuids: r.uuids ?? [] };
  });
  return { cambio, nuevas };
}

async function repararRelacion04Excel({ confirm = false } = {}) {
  const filtro = {
    source: 'SAT',
    uploadedBy: { $exists: true, $ne: null },
    'cfdiRelacionados.tipoRelacion': '04',
    $or: [{ xmlContent: { $exists: false } }, { xmlContent: null }, { xmlContent: '' }],
  };
  const docs = await CFDI.find(filtro).select('uuid serie folio tipoDeComprobante cfdiRelacionados').lean();
  console.log(`[reparar-relacion-04-excel] ${docs.length} copia(s) SAT sin XML con relación 04 (carga de Excel).`);

  const LOTE = 2000;
  const porResultado = {};
  const ejemplos = [];
  let aCambiar = 0;
  let escritos = 0;
  for (let i = 0; i < docs.length; i += LOTE) {
    const lote = docs.slice(i, i + LOTE);
    const erp = await CFDI.find({ source: 'ERP', uuid: { $in: lote.map(d => d.uuid) } }).select('uuid cfdiRelacionados').lean();
    const erpPorUuid = new Map(erp.map(e => [String(e.uuid).toUpperCase(), e]));
    const ops = [];
    for (const d of lote) {
      const { cambio, nuevas } = relacionesCorregidas(d.cfdiRelacionados, erpPorUuid.get(String(d.uuid).toUpperCase())?.cfdiRelacionados);
      const clave = `${d.tipoDeComprobante} → ${nuevas.map(n => n.tipoRelacion ?? 'sin tipo').join(',')}`;
      porResultado[clave] = (porResultado[clave] ?? 0) + 1;
      if (!cambio) continue;
      aCambiar++;
      if (ejemplos.length < 10) ejemplos.push(`${d.serie ?? ''}-${d.folio ?? ''} ${d.uuid} [${d.tipoDeComprobante}] 04 → ${nuevas.map(n => n.tipoRelacion ?? 'sin tipo').join(',')}`);
      ops.push({ updateOne: { filter: { _id: d._id }, update: { $set: { cfdiRelacionados: nuevas } } } });
    }
    if (confirm && ops.length) {
      const r = await CFDI.bulkWrite(ops, { ordered: false });
      escritos += r.modifiedCount ?? 0;
    }
  }

  console.log('\n[reparar-relacion-04-excel] === RESUMEN ===');
  console.log(`Revisadas:     ${docs.length}`);
  console.log(`A corregir:    ${aCambiar}`);
  console.log('Resultado (tipo → relación final):', porResultado);
  if (ejemplos.length) console.log('Ejemplos:\n  ' + ejemplos.join('\n  '));
  console.log(confirm ? `\n✓ Escritos: ${escritos}` : '\nDry-run — no se escribió nada. Volver a correr con --confirm para aplicar.');
  return { revisadas: docs.length, aCambiar, escritos, porResultado };
}

if (require.main === module) {
  const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
  connectMongo()
    .then(async () => {
      await repararRelacion04Excel({ confirm: process.argv.includes('--confirm') });
      await disconnectMongo();
      process.exit(0);
    })
    .catch((err) => {
      console.error('[reparar-relacion-04-excel] Error:', err.message);
      process.exit(1);
    });
}

module.exports = { repararRelacion04Excel, relacionesCorregidas };
