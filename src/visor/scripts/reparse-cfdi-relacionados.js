'use strict';

/**
 * reparse-cfdi-relacionados.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Re-parsea el `xmlContent` YA GUARDADO de los CFDIs `source: 'SAT'` para
 * rellenar `cfdiRelacionados` — corrige, sin volver a pedirle nada al SAT
 * (sin gastar cuota de descarga ni requerir e.firma), los documentos que se
 * sincronizaron ANTES del fix de `cfdiParser.js` (2026-08-28: `parseCFDI`
 * nunca extraía `<cfdi:CfdiRelacionados>` del XML, para ningún tipoRelacion —
 * ver el commit del fix). Caso real que lo expuso: factura F0-260800426 con
 * anticipo aplicado (tipoRelacion='07') presente en el XML pero ausente en
 * Mongo.
 *
 * Solo toca el campo `cfdiRelacionados` — nunca reescribe ningún otro dato
 * del documento. Solo revisa CFDIs cuyo `cfdiRelacionados` está vacío/ausente
 * (si ya tiene datos, sea de este parser o de otra fuente como el merge con
 * ERP, se deja intacto — no hay riesgo de pisar algo bueno).
 *
 * Por defecto corre en modo DRY-RUN (solo reporta qué cambiaría). Para
 * escribir de verdad hay que pasar --confirm explícito.
 *
 * Uso:
 *   node src/visor/scripts/reparse-cfdi-relacionados.js [fechaInicio] [fechaFin] [--confirm]
 *   node src/visor/scripts/reparse-cfdi-relacionados.js                      (dry-run, 2026-08-20 a 2026-08-28)
 *   node src/visor/scripts/reparse-cfdi-relacionados.js 2026-08-20 2026-08-28 --confirm
 */

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const CFDI = require('../models/CFDI');
const { parseCFDI } = require('../services/cfdiParser');

const DEFAULT_FECHA_INICIO = '2026-08-20';
const DEFAULT_FECHA_FIN    = '2026-08-28';

async function reparseCfdiRelacionados({ fechaInicio, fechaFin, confirm = false } = {}) {
  const desde = new Date(`${fechaInicio}T00:00:00.000Z`);
  const hasta = new Date(`${fechaFin}T23:59:59.999Z`);

  const candidatos = await CFDI.find({
    source:     'SAT',
    fecha:      { $gte: desde, $lte: hasta },
    xmlContent: { $exists: true, $nin: [null, ''] },
    $or: [
      { cfdiRelacionados: { $exists: false } },
      { cfdiRelacionados: { $size: 0 } },
    ],
  }).select('_id uuid serie folio fecha xmlContent').lean();

  console.log(`[reparse-cfdi-relacionados] ${candidatos.length} CFDI(s) SAT candidato(s) entre ${fechaInicio} y ${fechaFin} (sin cfdiRelacionados hoy).`);

  let recuperados = 0;
  let siguenVacios = 0;
  let errores = 0;
  const detalle = [];

  for (const doc of candidatos) {
    let parsed;
    try {
      parsed = await parseCFDI(doc.xmlContent);
    } catch (err) {
      errores++;
      console.error(`[reparse-cfdi-relacionados] ✖ ${doc.serie ?? ''}-${doc.folio ?? ''} (${doc.uuid}): error al re-parsear — ${err.message}`);
      continue;
    }

    const nuevasRelaciones = parsed.cfdiRelacionados ?? [];
    if (!nuevasRelaciones.length) {
      siguenVacios++;
      continue;
    }

    recuperados++;
    const resumen = nuevasRelaciones.map(r => `${r.tipoRelacion}:${r.uuids.join(',')}`).join(' | ');
    detalle.push({ uuid: doc.uuid, serie: doc.serie, folio: doc.folio, cfdiRelacionados: nuevasRelaciones });
    console.log(`[reparse-cfdi-relacionados] ${confirm ? '✓ actualizado' : '  se actualizaría'} ${doc.serie ?? ''}-${doc.folio ?? ''} (${doc.uuid}): ${resumen}`);

    if (confirm) {
      await CFDI.updateOne({ _id: doc._id }, { $set: { cfdiRelacionados: nuevasRelaciones } });
    }
  }

  console.log('\n[reparse-cfdi-relacionados] === RESUMEN ===');
  console.log(`Revisados:                    ${candidatos.length}`);
  console.log(`Con relación recuperada:      ${recuperados}`);
  console.log(`Sin relación (legítimo, [])   ${siguenVacios}`);
  console.log(`Errores de parseo:            ${errores}`);
  if (!confirm && recuperados > 0) {
    console.log('\nDry-run — no se escribió nada. Volvé a correr con --confirm para aplicar.');
  }

  return { revisados: candidatos.length, recuperados, siguenVacios, errores, detalle };
}

// ── Ejecución directa ─────────────────────────────────────────────────────────
if (require.main === module) {
  const args = process.argv.slice(2);
  const confirm = args.includes('--confirm');
  const posicionales = args.filter(a => !a.startsWith('--'));
  const fechaInicio = posicionales[0] || DEFAULT_FECHA_INICIO;
  const fechaFin    = posicionales[1] || DEFAULT_FECHA_FIN;

  connectMongo()
    .then(async () => {
      await reparseCfdiRelacionados({ fechaInicio, fechaFin, confirm });
      await disconnectMongo();
      process.exit(0);
    })
    .catch((err) => {
      console.error('[reparse-cfdi-relacionados] Error:', err.message);
      process.exit(1);
    });
}

module.exports = reparseCfdiRelacionados;
