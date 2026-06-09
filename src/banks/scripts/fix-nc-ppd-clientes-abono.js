'use strict';
/**
 * Fix: NCs con formaPago='99' (PPD) y formaPago='15' (Condonación)
 * generaban HABER a Bancos (1102011005) en lugar de Clientes (1103010001).
 *
 * Para cada regla tipo E con formaPago=null → cuentaAbono=1102011005,
 * crea una versión con formaPago='99' y otra con formaPago='15' que rutea a Clientes.
 *
 * Uso: node src/banks/scripts/fix-nc-ppd-clientes-abono.js [--dry-run]
 */
require('dotenv').config();
const { sequelize } = require('../../config/database.postgres');
const { CfdiMappingRule } = require('../../shared/models/postgres');

const DRY_RUN = process.argv.includes('--dry-run');

async function main() {
  await sequelize.authenticate();

  const reglasBancos = await CfdiMappingRule.findAll({
    where: { isActive: true, tipoComprobante: 'E', formaPago: null, cuentaAbono: '1102011005' },
    order: [['prioridad', 'ASC']],
  });

  console.log(`Reglas NC fP=null → Bancos encontradas: ${reglasBancos.length}`);
  if (DRY_RUN) console.log('[DRY-RUN] No se aplicarán cambios.\n');

  let creadas = 0, existentes = 0;

  for (const fP of ['99', '15']) {
    for (const r of reglasBancos) {
      const base = r.get({ plain: true });
      const abono = base.tasaIva === '0' ? '1103010002' : '1103010001';
      const nombre = (base.nombre + ` fP${fP}`).substring(0, 80);

      const existe = await CfdiMappingRule.findOne({ where: { nombre } });
      if (existe) { existentes++; continue; }

      if (!DRY_RUN) {
        await CfdiMappingRule.create({
          ...base, id: undefined,
          nombre,
          formaPago: fP,
          cuentaAbono: abono,
          isActive: true,
          createdAt: undefined, updatedAt: undefined,
        });
      }
      console.log(DRY_RUN ? '[DRY]' : '[NEW]', `fP=${fP} | ${base.tasaIva || '*'} | ${nombre} → ${abono}`);
      creadas++;
    }
  }

  console.log(`\nReglas creadas: ${creadas} | Ya existían: ${existentes}`);
  await sequelize.close();
}

main().catch(console.error);
