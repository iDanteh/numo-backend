'use strict';

/**
 * seed-tipo-origen-rules.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Reglas de mapeo CFDI → cuentas usando tipoOrigen (campo del ERP).
 * Prioridades 70–73 — ganan sobre las CC rules (conceptoContiene, prio 74-78)
 * cuando el CFDI trae tipoOrigen clasificado desde el ERP.
 *
 * Uso:
 *   node src/banks/scripts/seed-tipo-origen-rules.js
 *   node src/banks/scripts/seed-tipo-origen-rules.js --force   (sobreescribe)
 */

require('dotenv').config();

const { sequelize }   = require('../../config/database.postgres');
const CfdiMappingRule = require('../../shared/models/postgres/CfdiMappingRule');

// ── Cuentas referencia ────────────────────────────────────────────────────────
// 1101010003  Caja por identificar
// 1102011005  Bancos por identificar
// 2103090002  Anticipos Otros Clientes Club Tuberos (monedero electrónico)
// 2104010001  IVA Trasladado (causado definitivo)
// 4200010001  Devoluciones s/Ventas 16%
// 4200010002  Devoluciones s/Ventas 0%
// 4200020001  Descuentos s/Ventas 16%
// 4200020002  Descuentos s/Ventas 0%

// ── Helpers ───────────────────────────────────────────────────────────────────

/** Genera reglas para un TipoOrigen de tipo BONIFICACIÓN (cargo Descuentos). */
function bonReglas(tipoOrigen, prioridad, abonoClubTuberos = false) {
  const cuentaAbonoBancos = abonoClubTuberos ? '2103090002' : '1102011005';
  const cuentaAbonoCaja   = abonoClubTuberos ? '2103090002' : '1101010003';
  const label = tipoOrigen.slice(0, 30);

  const reglas = [
    // 16% — Efectivo
    {
      nombre:          `TO-BON-16-EF — ${label} 16% Efectivo`,
      tipoComprobante: 'E',
      tasaIva:         '16',
      formaPago:       '01',
      tipoOrigen,
      cuentaCargo:     '4200020001',
      cuentaAbono:     cuentaAbonoCaja,
      cuentaIva:       '2104010001',
      prioridad,
    },
    // 16% — Bancos/Transferencia
    {
      nombre:          `TO-BON-16 — ${label} 16%`,
      tipoComprobante: 'E',
      tasaIva:         '16',
      tipoOrigen,
      cuentaCargo:     '4200020001',
      cuentaAbono:     cuentaAbonoBancos,
      cuentaIva:       '2104010001',
      prioridad,
    },
    // 0% — Efectivo
    {
      nombre:          `TO-BON-0-EF — ${label} 0% Efectivo`,
      tipoComprobante: 'E',
      tasaIva:         '0',
      formaPago:       '01',
      tipoOrigen,
      cuentaCargo:     '4200020002',
      cuentaAbono:     cuentaAbonoCaja,
      cuentaIva:       null,
      prioridad,
    },
    // 0% — Bancos
    {
      nombre:          `TO-BON-0 — ${label} 0%`,
      tipoComprobante: 'E',
      tasaIva:         '0',
      tipoOrigen,
      cuentaCargo:     '4200020002',
      cuentaAbono:     cuentaAbonoBancos,
      cuentaIva:       null,
      prioridad,
    },
    // Mixto — Efectivo
    {
      nombre:          `TO-BON-M-EF — ${label} Mixto Efectivo`,
      tipoComprobante: 'E',
      tasaIva:         'mixto',
      formaPago:       '01',
      tipoOrigen,
      cuentaCargo:     '4200020001',
      cuentaAbono:     cuentaAbonoCaja,
      cuentaIva:       '2104010001',
      prioridad,
    },
    // Mixto — Bancos
    {
      nombre:          `TO-BON-M — ${label} Mixto`,
      tipoComprobante: 'E',
      tasaIva:         'mixto',
      tipoOrigen,
      cuentaCargo:     '4200020001',
      cuentaAbono:     cuentaAbonoBancos,
      cuentaIva:       '2104010001',
      prioridad,
    },
  ];

  return reglas;
}

/** Genera reglas para un TipoOrigen de tipo DEVOLUCIÓN/CANCELACIÓN (cargo Devoluciones). */
function devReglas(tipoOrigen, prioridad) {
  const label = tipoOrigen.slice(0, 30);
  return [
    // 16% — Efectivo
    {
      nombre:          `TO-DEV-16-EF — ${label} 16% Efectivo`,
      tipoComprobante: 'E',
      tasaIva:         '16',
      formaPago:       '01',
      tipoOrigen,
      cuentaCargo:     '4200010001',
      cuentaAbono:     '1101010003',
      cuentaIva:       '2104010001',
      prioridad,
    },
    // 16% — Bancos
    {
      nombre:          `TO-DEV-16 — ${label} 16%`,
      tipoComprobante: 'E',
      tasaIva:         '16',
      tipoOrigen,
      cuentaCargo:     '4200010001',
      cuentaAbono:     '1102011005',
      cuentaIva:       '2104010001',
      prioridad,
    },
    // 0% — Efectivo
    {
      nombre:          `TO-DEV-0-EF — ${label} 0% Efectivo`,
      tipoComprobante: 'E',
      tasaIva:         '0',
      formaPago:       '01',
      tipoOrigen,
      cuentaCargo:     '4200010002',
      cuentaAbono:     '1101010003',
      cuentaIva:       null,
      prioridad,
    },
    // 0% — Bancos
    {
      nombre:          `TO-DEV-0 — ${label} 0%`,
      tipoComprobante: 'E',
      tasaIva:         '0',
      tipoOrigen,
      cuentaCargo:     '4200010002',
      cuentaAbono:     '1102011005',
      cuentaIva:       null,
      prioridad,
    },
  ];
}

// ── Catálogo de reglas por TipoOrigen ─────────────────────────────────────────

const reglas = [

  // ── GRUPO 1: Club Tuberos (prio 70) ──────────────────────────────────────
  // cuentaAbono = 2103090002 (monedero electrónico Club Tuberos)
  ...bonReglas('Traslado y Bonificación club tuberos', 70, true),

  // ── GRUPO 2: Bonificaciones genéricas (prio 71) ──────────────────────────
  ...bonReglas('Bonificación',                  71),
  ...bonReglas('Bonificacion cliente Mostador', 71),
  ...bonReglas('Bonificacion Especial',         71),

  // ── GRUPO 3: Devoluciones (prio 72) ──────────────────────────────────────
  ...devReglas('Devolución',          72),
  ...devReglas('Devolucion de Cliente', 72),
  ...devReglas('Devolucion Especial', 72),

  // ── GRUPO 4: Cancelaciones (prio 73) → mismo tratamiento que Devoluciones
  ...devReglas('Cancelacion Especial',   73),
  ...devReglas('Cancelacion de cliente', 73),
  ...devReglas('Cancelacion',            73),

].map(r => ({ isActive: true, ...r }));

// ── Runner ────────────────────────────────────────────────────────────────────

async function main() {
  const force = process.argv.includes('--force');

  await sequelize.authenticate();
  await CfdiMappingRule.sync({ force: false });

  let creadas = 0, omitidas = 0, actualizadas = 0;

  for (const datos of reglas) {
    const [regla, created] = await CfdiMappingRule.findOrCreate({
      where:    { nombre: datos.nombre },
      defaults: datos,
    });

    if (created) {
      creadas++;
    } else if (force) {
      await regla.update(datos);
      actualizadas++;
    } else {
      omitidas++;
    }
  }

  console.log(`\nTipoOrigen rules:`);
  console.log(`  Creadas:     ${creadas}`);
  console.log(`  Actualizadas: ${actualizadas} (--force)`);
  console.log(`  Omitidas:    ${omitidas} (ya existían)`);
  console.log(`  Total:       ${reglas.length} reglas\n`);

  await sequelize.close();
}

main().catch(err => { console.error(err); process.exit(1); });
