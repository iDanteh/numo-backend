'use strict';

/**
 * diag-uuid-case-nc.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Diagnóstico puntual: confirma si el uuid relacionado dentro de
 * `cfdiRelacionados` de una NC específica coincide en mayúsculas/minúsculas
 * con su propio campo `uuid` (que se usa en mayúsculas al construir los mapas
 * de `_fetchNotasCreditoParaFusion`). Solo lectura — no modifica nada.
 *
 * Uso:
 *   node src/banks/scripts/diag-uuid-case-nc.js <uuid-de-la-NC>
 */

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const CFDI = require('../../visor/models/CFDI');

const uuidArg = process.argv[2];
if (!uuidArg) {
  console.error('Uso: node diag-uuid-case-nc.js <uuid-de-la-NC>');
  process.exit(1);
}

async function main() {
  await connectMongo();

  const nc = await CFDI.findOne({ uuid: new RegExp(`^${uuidArg}$`, 'i') })
    .select('uuid tipoDeComprobante metodoPago formaPago cfdiRelacionados source')
    .lean();

  if (!nc) {
    console.log('No se encontró ningún CFDI con ese uuid.');
    await cerrar();
    return;
  }

  console.log('\n── NC encontrada ───────────────────────────────────────────');
  console.log({ uuid: nc.uuid, source: nc.source, tipo: nc.tipoDeComprobante, metodoPago: nc.metodoPago, formaPago: nc.formaPago });

  console.log('\n── cfdiRelacionados (raw) ──────────────────────────────────');
  for (const r of nc.cfdiRelacionados ?? []) {
    const uuids = r.uuids ?? (r.uuid ? [r.uuid] : []);
    for (const u of uuids) {
      console.log({
        tipoRelacion: r.tipoRelacion,
        uuid_relacionado: u,
        es_mayusculas: u === u.toUpperCase(),
      });
    }
  }

  // Para cada uuid relacionado tipo 01/03, busca la factura real y compara casing.
  const relUuids = (nc.cfdiRelacionados ?? [])
    .filter(r => r.tipoRelacion === '01' || r.tipoRelacion === '03')
    .flatMap(r => r.uuids ?? (r.uuid ? [r.uuid] : []));

  if (relUuids.length) {
    console.log('\n── Factura(s) relacionada(s) — comparación de casing ──────');
    for (const u of relUuids) {
      const factura = await CFDI.findOne({ uuid: new RegExp(`^${u}$`, 'i') })
        .select('uuid metodoPago formaPago tipoDeComprobante').lean();
      if (!factura) {
        console.log({ uuid_en_relacion: u, encontrada: false });
        continue;
      }
      console.log({
        uuid_en_relacion:        u,
        uuid_real_en_su_propio_doc: factura.uuid,
        coincide_case_exacto:    u === factura.uuid,
        factura_metodoPago:      factura.metodoPago,
        factura_formaPago:       factura.formaPago,
      });
    }
  } else {
    console.log('\n(No se encontraron uuids relacionados tipoRelacion 01/03 en este CFDI)');
  }

  console.log('\n──────────────────────────────────────────────────────────');
  console.log('INTERPRETACIÓN:');
  console.log('  Si "coincide_case_exacto" es false, ahí está el bug: los mapas');
  console.log('  metodoPagoPorFactura/facturaRelacionadaMeta se construyen con');
  console.log('  uuid.toUpperCase() de la factura, pero la búsqueda usa el uuid');
  console.log('  tal cual viene en cfdiRelacionados (sin normalizar) — si ese');
  console.log('  campo no está en mayúsculas, el lookup nunca encuentra nada y');
  console.log('  la NC nunca se corrige a PPD.');

  await cerrar();
}

async function cerrar() {
  await disconnectMongo();
  process.exit(0);
}

main().catch(err => { console.error(err); process.exit(1); });
