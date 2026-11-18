'use strict';

/**
 * loyalty-gateway/swagger.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Config de swagger-jsdoc para la documentación interactiva (Swagger UI) de los
 * webhooks que Kore consume en este gateway (`/api/loyalty/kore/*`). Montado
 * desde `app.js` en `/api-docs` (2026-11-18, pedido explícito del usuario):
 * reemplaza/complementa el PDF estático e Insomnia/Postman para que Kore pueda
 * probar los endpoints desde un navegador.
 *
 * Solo apunta a los 3 routers de Kore (ledger/loyalty-kore-venta,
 * kore-match/loyalty-kore-cliente, kore-match/loyalty-kore-nivel) — a propósito
 * NO incluye `loyalty.routes.js` ni el resto de `src/loyalty/**`, que es
 * staff-only y no es parte de este pedido.
 */
const path = require('path');
const swaggerJsdoc = require('swagger-jsdoc');

const options = {
  definition: {
    openapi: '3.0.3',
    info: {
      title: 'Numo — API de integración Lealtad (Kore)',
      description:
        'Documenta los webhooks server-to-server que Kore consume para el Sistema de Lealtad '
        + '(acumulación/canje de puntos, catálogo de clientes y catálogo de niveles), más los '
        + 'endpoints de uso interno de Numo para auditar ese tráfico.',
      version: '1.0.0',
    },
    components: {
      securitySchemes: {
        apiKeyAuth: {
          type: 'apiKey',
          in: 'header',
          name: 'X-Api-Key',
          description:
            'API key server-to-server de Kore, validada contra Configuraciones Globales '
            + '(middleware `verifyKoreApiKey`).',
        },
      },
    },
  },
  // Relativas a este archivo (no al cwd del proceso) — así funcionan sin
  // importar desde dónde se arranque el gateway (local, Docker, etc).
  apis: [
    path.join(__dirname, '../loyalty/domains/ledger/loyalty-kore-venta.routes.js'),
    path.join(__dirname, '../loyalty/domains/kore-match/loyalty-kore-cliente.routes.js'),
    path.join(__dirname, '../loyalty/domains/kore-match/loyalty-kore-nivel.routes.js'),
  ],
};

module.exports = swaggerJsdoc(options);
