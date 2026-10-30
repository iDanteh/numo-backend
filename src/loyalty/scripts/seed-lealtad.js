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
const svc = require('../../shared/services/global-config.service');

// puntosFijos: undefined → quedan en modo manual (rediseño 2026-10-23, reemplaza
// signoPermitido) — ninguno de los 3 motivos originales tenía un valor fijo real
// pedido por el negocio, así que no se inventa uno acá.
const REASONS = [
  { clave: 'bono',       nombre: 'Bono' },
  { clave: 'regalo',     nombre: 'Regalo' },
  { clave: 'correccion', nombre: 'Corrección' },
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
        'ESPECIALIDADES — Catálogo de referencia (lista editable desde la UI) de las especialidades válidas para altas futuras de clientes. Informativo: el valor que trae cada cliente del CSV de import se persiste tal cual, sin validar contra este catálogo.',
        // 2026-10-29: NO se siembra con un valor acá a propósito — mismo criterio que
        // FICHAS_IMAGEN_FOLDER_ID en seed-global-config-banks.js (bancos): un folder ID de Drive
        // no es algo que este script pueda adivinar, el usuario lo declara él mismo desde la UI de
        // Configuraciones Globales una vez que crea/comparte la carpeta con la cuenta de servicio.
        'FOTO_CLIENTE_FOLDER_ID — Carpeta de Drive para la foto del cliente de una tarjeta de Lealtad (misma cuenta que FICHAS_IMAGEN_FOLDER_ID en Bancos, GOOGLE_SERVICE_ACCOUNT_KEY2). Declarado desde la UI.',
      ],
    },
  });
  console.log(`[seed-lealtad] Sección 'lealtad' ${creada ? 'creada' : 'ya existía'} (id=${section.id}). % de nivel NO sembrados a propósito (D11) — configurar desde Lealtad → Configuración.`);
  return section;
}

/**
 * ESPECIALIDADES (2026-10-23, enriquecimiento del import de clientes.csv) — catálogo de
 * referencia. `tipo: 'lista'` (igual que bancos.NOMBRE_TIPO_TRANSFERENCIA_PERMITIDOS /
 * NOMBRE_CAJA_DESTINO_PERMITIDAS en seed-global-config-banks.js) para que Configuraciones
 * Globales la muestre con el editor de chips (agregar/quitar valores), no como texto libre — sin
 * esto el admin no tiene forma de dar de alta especialidades nuevas para clientes futuros.
 * Mismo criterio "solo si no existe": un re-run de este seed nunca debe pisar un catálogo que el
 * admin ya haya personalizado desde la UI. Default `[]` — sin especialidades conocidas todavía.
 */
async function _asegurarEspecialidadesDefault() {
  const yaExiste = await svc.getValue('lealtad', 'ESPECIALIDADES').then(() => true).catch(() => false);
  if (yaExiste) {
    console.log('[seed-lealtad] lealtad.ESPECIALIDADES ya existe — no se pisa.');
    return;
  }
  await svc.setValue('lealtad', 'ESPECIALIDADES', '[]', {
    esSecreto: false, tipo: 'lista',
    descripcion: 'Catálogo de especialidades válidas, editable como lista desde la UI — informativo, no bloquea el import.',
    usuarioNombre: 'seed-script',
  });
  console.log('[seed-lealtad] lealtad.ESPECIALIDADES = [] (default vacío — agregar valores desde la UI)');
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
  await _asegurarEspecialidadesDefault();
  console.log('[seed-lealtad] Listo — sección lealtad, catálogo de motivos y catálogo de especialidades sembrados correctamente.');
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
