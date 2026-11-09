'use strict';

/**
 * stress-test/seed-stress-cards.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Siembra N tarjetas de Lealtad dummy para que k6-otp-flow.js tenga un pool de
 * teléfonos distintos para golpear — sin esto, todos los VUs pegarían al mismo
 * teléfono y se pisarían el código/sesión entre sí (una tarjeta = un solo
 * código/sesión vigente a la vez).
 *
 * SOLO para la réplica local de stress-test (ver docker-compose.stress.yml) —
 * nunca correr contra una base con datos reales. Idempotente: usa
 * findOrCreate, correrlo de nuevo no duplica tarjetas.
 *
 * Teléfonos: PHONE_PREFIX ('5550') + índice de 4 dígitos (0000..N-1) — debe
 * coincidir exactamente con PHONE_PREFIX/PHONE_POOL_SIZE en k6-otp-flow.js.
 *
 * Uso: node stress-test/seed-stress-cards.js [cantidad]  (default 500)
 */

const { LoyaltyCard } = require('../src/shared/models/postgres');
const { hashNip } = require('../src/loyalty/shared/nip.util');

const PHONE_PREFIX = '555000'; // 6 dígitos + índice de 4 = 10 exactos (validate: /^\d{10}$/ en LoyaltyCard)
const NIP_DUMMY = '9999'; // irrelevante para este flujo (la consulta pública usa código OTP, no NIP) — un solo hash reusado para no pagar 500x el costo de bcrypt

async function main() {
  const cantidad = Number(process.argv[2]) || 500;
  const nipHash = await hashNip(NIP_DUMMY);

  let creadas = 0;
  for (let i = 0; i < cantidad; i += 1) {
    const telefono = PHONE_PREFIX + String(i).padStart(4, '0');
    const [, created] = await LoyaltyCard.findOrCreate({
      where: { telefono },
      defaults: {
        telefono,
        nipHash,
        nombre: `Stress Test ${i}`,
        nivel: 'Bronce',
        saldoPuntos: 100,
        activa: true,
      },
    });
    if (created) creadas += 1;
  }

  console.log(`[seed-stress-cards] Listo — ${creadas} tarjetas nuevas, ${cantidad - creadas} ya existían (de ${cantidad} pedidas).`);
  process.exit(0);
}

main().catch((err) => {
  console.error('[seed-stress-cards] Error:', err.message);
  process.exit(1);
});
