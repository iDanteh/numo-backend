'use strict';

/**
 * loyalty/scripts/seed-lealtad.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Seed idempotente del Sistema de Lealtad. Dos piezas, ambas seguras de
 * correr múltiples veces:
 *
 *   1. ConfigSection 'lealtad' en Configuraciones Globales — se asegura que
 *      EXISTA (findOrCreate), igual que `seed-global-config-banks.js`. A
 *      propósito NO siembra `PORCENTAJE_BRONCE/PLATA/ORO`: esos valores
 *      dependen del dueño del negocio (D11 — fallar cerrado es mejor que
 *      inventar un %, acumular con un valor adivinado costaría dinero real).
 *      Se configuran a mano desde Lealtad → Configuración una vez que el
 *      dueño del negocio los define.
 *   2. Catálogo de motivos de ajuste (`loyalty_adjustment_reasons`):
 *      bono/regalo/correccion, vía `findOrCreate` por `clave` — un re-run
 *      nunca duplica ni pisa ediciones que un admin ya haya hecho desde la UI
 *      (a diferencia de `_asegurarSeccion` de seed-banks, acá no hay campos
 *      que el seed deba sobrescribir siempre).
 *
 * Uso:
 *   node src/loyalty/scripts/seed-lealtad.js
 *   npm run seed:lealtad
 */

require('dotenv').config();

const { ConfigSection, LoyaltyAdjustmentReason } = require('../../shared/models/postgres');

const REASONS = [
  { clave: 'bono',       nombre: 'Bono',       signoPermitido: 'POSITIVO' },
  { clave: 'regalo',     nombre: 'Regalo',     signoPermitido: 'POSITIVO' },
  { clave: 'correccion', nombre: 'Corrección', signoPermitido: 'AMBOS' },
];

async function _asegurarSeccionLealtad() {
  const [section, creada] = await ConfigSection.findOrCreate({
    where: { clave: 'lealtad' },
    defaults: {
      nombre: 'Lealtad',
      descripcion: 'Porcentaje de puntos por nivel del Sistema de Lealtad (phone-as-card). Sin valor por defecto: acumular debe fallar hasta que el dueño del negocio los configure (ver loyalty-ledger.service.js, D11).',
      modulosAfectados: [
        'PORCENTAJE_BRONCE — % de puntos acumulados en ventas de tarjetas nivel Bronce. Sin valor hasta que se configure acá.',
        'PORCENTAJE_PLATA — % de puntos acumulados en ventas de tarjetas nivel Plata. Sin valor hasta que se configure acá.',
        'PORCENTAJE_ORO — % de puntos acumulados en ventas de tarjetas nivel Oro. Sin valor hasta que se configure acá.',
      ],
    },
  });
  console.log(`[seed-lealtad] Sección 'lealtad' ${creada ? 'creada' : 'ya existía'} (id=${section.id}). % de nivel NO sembrados a propósito (D11) — configurar desde Lealtad → Configuración.`);
  return section;
}

async function _asegurarReasons() {
  for (const reasonDef of REASONS) {
    // eslint-disable-next-line no-await-in-loop
    const [reason, creado] = await LoyaltyAdjustmentReason.findOrCreate({
      where:    { clave: reasonDef.clave },
      defaults: { ...reasonDef, activo: true },
    });
    console.log(`[seed-lealtad] Motivo '${reasonDef.clave}' ${creado ? 'creado' : 'ya existía'} (id=${reason.id}).`);
  }
}

async function seedLealtad() {
  await _asegurarSeccionLealtad();
  await _asegurarReasons();
  console.log('[seed-lealtad] Listo — sección lealtad y catálogo de motivos sembrados correctamente.');
}

// ── Ejecución directa: node src/loyalty/scripts/seed-lealtad.js ────────────
if (require.main === module) {
  const { connectPostgres, disconnectPostgres } = require('../../config/database.postgres');

  connectPostgres()
    .then(async () => {
      await seedLealtad();
      await disconnectPostgres();
      process.exit(0);
    })
    .catch((err) => {
      console.error('[seed-lealtad] Error:', err.message);
      process.exit(1);
    });
}

module.exports = seedLealtad;
