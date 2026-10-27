'use strict';

/**
 * loyalty/domains/kore-match/loyalty-kore-monedero.client.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — match de clientes contra Kore (ERP/monedero externo del
 * cliente). Cliente HTTP de SOLO LECTURA de `GET /clientes/monedero`
 * (producción: https://app.login.tubosyconexiones.mx/clientes/monedero),
 * paginado, trayendo TODAS las páginas — usado por `loyalty-kore-match.service.js`
 * para cruzar contra `LoyaltyCard.tarjetaLegada`.
 *
 * Configuraciones Globales (sección `kore`, ver global-config.service.js):
 *   - PUNTOS_KORE → URL base del endpoint (sembrada en
 *     seed-global-config-banks.js).
 *
 * Token: reusa `bancos.TOKEN` (CORRECCIÓN 2026-10-23 — confirmado por el usuario que es el
 * mismo token físico que ya usan erp-sync.service.js/kore-caja.service.js/erp.routes.js/
 * erp.service.js para /cuentas-pendientes y aplicar cobros). Antes existía una clave propia
 * `kore.PUNTOS_KORE_TOKEN`, sembrada vacía y nunca configurada en producción — se eliminó junto
 * con su seed (ver seed-global-config-banks.js).
 *
 * Reintento/backoff: mismo patrón YA resuelto en
 * `banks/domains/erp/erp-sync.service.js#_getConReintento` (axios + timeout
 * 60s + hasta 3 intentos ante 429 — con el "retry after" que manda el ERP en
 * el cuerpo — o timeout, con backoff fijo 3*intento segundos). Se replica acá
 * en vez de importarlo porque esa función está acoplada a
 * `_tokenPolizas()`/la sección `polizas` — este cliente es de otra sección
 * (`kore`) y de otro dominio (loyalty, no banks), y no vale la pena una
 * dependencia cruzada entre dominios por una función de 20 líneas.
 */

const axios = require('axios');
const globalConfigService = require('../../../shared/services/global-config.service');
const { logger } = require('../../../shared/utils/logger');

async function _url() {
  const valor = await globalConfigService.getValue('kore', 'PUNTOS_KORE');
  return valor.replace(/\/$/, '');
}

async function _token() {
  const token = await globalConfigService.getValue('bancos', 'TOKEN');
  if (!token) {
    throw new Error(
      "Configuraciones Globales 'bancos.TOKEN' está vacío — configuralo desde el panel "
      + 'de administración antes de correr el match de clientes con Kore.',
    );
  }
  return token;
}

// Mismo criterio de reintento que erp-sync.service.js#_getConReintento (ver docstring del
// archivo): 429 → espera el "retry after" que manda Kore en el cuerpo (+1s de margen, tope 60s);
// timeout → backoff fijo 3*intento segundos. 3 intentos en total.
const MAX_INTENTOS = 3;
async function _getConReintento(url, params, logLabel) {
  const token = await _token();
  let ultimoError;
  for (let intento = 1; intento <= MAX_INTENTOS; intento++) {
    try {
      // eslint-disable-next-line no-await-in-loop -- reintento secuencial a propósito, mismo
      // criterio que erp-sync.service.js.
      return await axios.get(url, {
        params,
        headers: { Authorization: `Bearer ${token}` },
        timeout: 60000,
      });
    } catch (axErr) {
      ultimoError = axErr;
      const status = axErr.response?.status;
      const esTimeout = axErr.code === 'ECONNABORTED' || /timeout/i.test(axErr.message || '');

      if (status === 429 && intento < MAX_INTENTOS) {
        const dataMsg = String(axErr.response?.data?.Data ?? '');
        const match = /retry after:\s*([\d.]+)/i.exec(dataMsg);
        const esperaSeg = Math.min((match ? Number(match[1]) : 10) + 1, 60);
        logger.warn(`[KoreMonedero] 429 en ${logLabel}, reintentando en ${esperaSeg.toFixed(1)}s (intento ${intento}/${MAX_INTENTOS})`);
        // eslint-disable-next-line no-await-in-loop
        await new Promise((r) => setTimeout(r, esperaSeg * 1000));
        continue;
      }
      if (esTimeout && intento < MAX_INTENTOS) {
        const esperaSeg = 3 * intento;
        logger.warn(`[KoreMonedero] timeout en ${logLabel}, reintentando en ${esperaSeg}s (intento ${intento}/${MAX_INTENTOS})`);
        // eslint-disable-next-line no-await-in-loop
        await new Promise((r) => setTimeout(r, esperaSeg * 1000));
        continue;
      }

      const body = JSON.stringify(axErr.response?.data ?? {});
      logger.error(`[KoreMonedero] ${logLabel} ${status}: ${body}`);
      throw axErr;
    }
  }
  throw ultimoError;
}

// Tope documentado del endpoint (ver contexto del cambio): `limit` máx 100.
const LIMIT_POR_PAGINA = 100;
// Red de seguridad contra un loop infinito si Kore alguna vez deja de reportar `total`
// correctamente — 2,000 páginas de 100 = 200,000 clientes, muy por encima del universo real hoy
// (~1,968). Si se llega a este tope, se corta y se loguea (nunca un loop infinito en producción).
const MAX_PAGINAS = 2000;

/**
 * Trae TODOS los clientes con monedero de Kore, paginando hasta completar `Data.total`.
 * Solo lectura — nunca escribe nada. Devuelve `{ id, codigo, nombre, razonSocial, monedero,
 * tieneMonedero }[]` tal cual los entrega Kore (el `id` NUNCA se reconstruye, mismo criterio que
 * `erpId` en Solicitudes de Cobro).
 */
async function obtenerClientesConMonedero() {
  const baseUrl = await _url();
  const clientes = [];
  let total = Infinity;
  let page = 1;

  while (clientes.length < total && page <= MAX_PAGINAS) {
    // eslint-disable-next-line no-await-in-loop -- paginación secuencial a propósito (depende del
    // `total` que devuelve la página anterior).
    const response = await _getConReintento(baseUrl, {
      page, limit: LIMIT_POR_PAGINA, tieneMonedero: true,
    }, `GET /clientes/monedero (page=${page})`);

    const data = response.data?.Data ?? {};
    const pageClientes = Array.isArray(data.clientes) ? data.clientes : [];
    total = Number.isFinite(data.total) ? data.total : clientes.length + pageClientes.length;

    clientes.push(...pageClientes);

    if (pageClientes.length === 0) break; // Kore dejó de devolver filas — no seguir pidiendo páginas vacías
    page += 1;
  }

  if (page > MAX_PAGINAS) {
    logger.error(`[KoreMonedero] se alcanzó el tope de ${MAX_PAGINAS} páginas sin completar total=${total} — resultado parcial (${clientes.length} clientes).`);
  }

  return clientes;
}

module.exports = { obtenerClientesConMonedero };
