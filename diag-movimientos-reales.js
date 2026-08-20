'use strict';
require('dotenv').config();
const { sequelize } = require('./src/config/database.postgres');

const FECHA = process.env.DIAG_FECHA || '2026-08-11';
const CENTRO_CLAVE = process.env.DIAG_SERIE || 'B0';

async function main() {
  await sequelize.authenticate();

  const [polizas] = await sequelize.query(`
    SELECT p.id, p.fecha, p.estado, p.created_at, p.centro_costo
    FROM polizas p
    WHERE p.fecha = :fecha AND p.centro_costo = :centro
    ORDER BY p.id DESC
    LIMIT 5
  `, { replacements: { fecha: FECHA, centro: CENTRO_CLAVE } });
  console.log('Polizas encontradas:', JSON.stringify(polizas, null, 2));
  if (!polizas.length) { console.log('NO SE ENCONTRO POLIZA'); process.exit(0); }

  const idPoliza = polizas[0].id;

  const [cuentas] = await sequelize.query(`
    SELECT id, codigo, nombre FROM account_plans WHERE codigo IN ('1101010003', '1102011005')
  `);
  console.log('Cuentas Caja/Bancos:', JSON.stringify(cuentas, null, 2));
  const cuentaCajaId = cuentas.find(c => c.codigo === '1101010003')?.id;

  const [resumen] = await sequelize.query(`
    SELECT tipo_origen, regla_nombre, COUNT(*) AS n, SUM(debe) AS suma_debe, SUM(haber) AS suma_haber
    FROM poliza_movimientos
    WHERE poliza_id = :id AND cuenta_id = :cuentaId
    GROUP BY tipo_origen, regla_nombre
    ORDER BY suma_debe DESC
  `, { replacements: { id: idPoliza, cuentaId: cuentaCajaId } });
  console.log(`\nResumen movimientos de CAJA (cuenta_id=${cuentaCajaId}) en poliza ${idPoliza}, agrupado por tipo_origen/regla_nombre:`);
  console.log(JSON.stringify(resumen, null, 2));

  const totalDebe = resumen.reduce((s, r) => s + (Number(r.suma_debe) || 0), 0);
  const totalHaber = resumen.reduce((s, r) => s + (Number(r.suma_haber) || 0), 0);
  console.log(`\nTOTAL debe (todos los tipo_origen/regla_nombre): ${totalDebe.toFixed(2)}`);
  console.log(`TOTAL haber: ${totalHaber.toFixed(2)}`);
  console.log(`Neto (debe-haber): ${(totalDebe - totalHaber).toFixed(2)}`);

  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
