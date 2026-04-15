'use strict';

const fs   = require('fs');
const path = require('path');
const FormData = require('form-data');
const axios    = require('axios');

const TOKEN   = process.argv[2];
const DIR     = process.argv[3];
const BASE_URL = process.env.API_URL || 'http://localhost:3000';
const BATCH    = 500;

if (!TOKEN || !DIR) {
  console.error('Uso: node upload-xml-batch.js <token> <directorio>');
  process.exit(1);
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function main() {
  const files = fs.readdirSync(DIR).filter(f => f.toLowerCase().endsWith('.xml'));
  console.log(`Total XMLs: ${files.length}`);

  let inserted = 0, duplicates = 0, errors = 0;
  const totalBatches = Math.ceil(files.length / BATCH);

  for (let i = 0; i < files.length; i += BATCH) {
    const batch = files.slice(i, i + BATCH);
    const batchNum = Math.floor(i / BATCH) + 1;
    process.stdout.write(`Lote ${batchNum}/${totalBatches} (${batch.length} archivos)... `);

    const form = new FormData();
    form.append('source', 'SAT');
    for (const f of batch) {
      form.append('xmlFiles', fs.createReadStream(path.join(DIR, f)), { filename: f });
    }

    try {
      const res = await axios.post(`${BASE_URL}/api/cfdis/upload`, form, {
        headers: { ...form.getHeaders(), Authorization: `Bearer ${TOKEN}` },
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
        timeout: 120000,
      });
      const d = res.data;
      inserted   += d.inserted   ?? d.nuevosInsertados ?? 0;
      duplicates += d.duplicates ?? d.duplicados       ?? 0;
      errors     += d.errors     ?? d.errores          ?? 0;
      console.log(`✓  insertados=${d.inserted ?? d.nuevosInsertados ?? 0}  dup=${d.duplicates ?? d.duplicados ?? 0}  err=${d.errors ?? d.errores ?? 0}`);
    } catch (err) {
      const msg = err.response?.data?.error || err.message;
      console.log(`✗  ERROR: ${msg}`);
      errors += batch.length;
    }

    if (i + BATCH < files.length) await sleep(500);
  }

  console.log('\n─────────────────────────────────');
  console.log(`Insertados:  ${inserted}`);
  console.log(`Duplicados:  ${duplicates}`);
  console.log(`Errores:     ${errors}`);
  console.log('─────────────────────────────────');
}

main().catch(err => { console.error(err.message); process.exit(1); });
