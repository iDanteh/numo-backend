'use strict';
require('dotenv').config();

const fs = require('fs');
const path = require('path');
const { sequelize } = require('./src/config/database.postgres');
const CfdiMappingRule = require('./src/shared/models/postgres/CfdiMappingRule');

const toSnake = s => s.replace(/([A-Z])/g, '_$1').toLowerCase();

async function run() {
  await sequelize.authenticate();

  const todas = await CfdiMappingRule.findAll({ order: [['prioridad', 'ASC']] });

  const cols = [
    'nombre','tipoComprobante','rfcEmisor','rfcReceptor','metodoPago','formaPago',
    'claveProdServ','tipoRelacion','relacionadoTipo','tasaIva','tieneDescuento',
    'conceptoContiene','cuentaCargo','cuentaAbono','cuentaAbono2','cuentaIva',
    'cuentaIvaPPD','cuentaIvaRetenido','cuentaIsrRetenido','cuentaIvaAnticipo',
    'cuentaDeltaAnticipo','ivaHaber','esAplicacionSaldo','cuentaCargo2',
    'cuentaDescuento','cuentaDescuento0','centroCosto','prioridad','isActive',
  ];

  const dbCols = cols.map(c => `"${toSnake(c)}"`).join(', ') + ', "created_at", "updated_at"';

  // Columnas para UPDATE (todo excepto nombre y timestamps)
  const updateCols = cols.filter(c => c !== 'nombre');
  const lines = [`-- Upsert ${todas.length} reglas (UPDATE si existe, INSERT si no)\n`];

  for (const r of todas) {
    const escape = v => {
      if (v === null || v === undefined) return 'NULL';
      if (typeof v === 'boolean') return v ? 'true' : 'false';
      if (typeof v === 'number') return String(v);
      // Encode non-ASCII as PostgreSQL unicode escapes → SQL puro ASCII
      const escaped = String(v)
        .replace(/'/g, "''")
        .replace(/[^\x00-\x7F]/g, c => `\\u${c.charCodeAt(0).toString(16).padStart(4, '0')}`);
      return `E'${escaped}'`;
    };

    const nombreSQL = escape(r.nombre); // E'...' con unicode escapes
    const setClause = updateCols.map(c => `"${toSnake(c)}" = ${escape(r[c])}`).join(', ');
    const vals = cols.map(c => escape(r[c])).join(', ') + ', NOW(), NOW()';

    lines.push(`UPDATE cfdi_mapping_rules SET ${setClause}, "updated_at" = NOW() WHERE nombre = ${nombreSQL};`);
    lines.push(`INSERT INTO cfdi_mapping_rules (${dbCols}) SELECT ${vals} WHERE NOT EXISTS (SELECT 1 FROM cfdi_mapping_rules WHERE nombre = ${nombreSQL});`);
  }

  const outFile = path.join(__dirname, 'reglas-import.sql');
  fs.writeFileSync(outFile, lines.join('\n'), 'utf8');
  console.log(`Archivo generado: ${outFile} (${todas.length} reglas)`);

  await sequelize.close();
  process.exit(0);
}

run().catch(e => { console.error(e); process.exit(1); });
