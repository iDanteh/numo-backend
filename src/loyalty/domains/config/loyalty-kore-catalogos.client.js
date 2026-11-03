'use strict';

/**
 * loyalty/domains/config/loyalty-kore-catalogos.client.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — cliente HTTP de SOLO LECTURA de dos catálogos de Kore
 * usados para configurar qué ventas acumulan puntos (webhook Kore→Numo, tema
 * aparte, todavía sin construir): `GET /conceptos` (tipos de venta) y
 * `GET /api/formasdepago` (formas de pago). Ninguno de los dos pagina.
 *
 * Configuraciones Globales (sección `kore`, ver global-config.service.js):
 *   - CONCEPTOS_URL / FORMAS_PAGO_URL → URLs base de cada catálogo.
 *
 * Token: reusa `bancos.TOKEN`, mismo criterio que
 * `loyalty/domains/kore-match/loyalty-kore-monedero.client.js`.
 *
 * Reintento/backoff: mismo patrón ya resuelto ahí (429 con "retry after" del
 * cuerpo, timeout con backoff fijo 3*intento, 3 intentos) — se replica acá en
 * vez de importarlo porque ese cliente vive en otro dominio (`kore-match`) y
 * no vale la pena una dependencia cruzada por una función de 20 líneas (mismo
 * razonamiento que ya documenta ese archivo sobre erp-sync.service.js).
 */

const axios = require('axios');
const globalConfigService = require('../../../shared/services/global-config.service');
const { logger } = require('../../../shared/utils/logger');

async function _token() {
  const token = await globalConfigService.getValue('bancos', 'TOKEN');
  if (!token) {
    throw new Error(
      "Configuraciones Globales 'bancos.TOKEN' está vacío — configuralo desde el panel "
      + 'de administración antes de sincronizar catálogos de Kore.',
    );
  }
  return token;
}

const MAX_INTENTOS = 3;
async function _getConReintento(url, logLabel) {
  const token = await _token();
  let ultimoError;
  for (let intento = 1; intento <= MAX_INTENTOS; intento++) {
    try {
      // eslint-disable-next-line no-await-in-loop -- reintento secuencial a propósito, mismo
      // criterio que loyalty-kore-monedero.client.js.
      return await axios.get(url, {
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
        logger.warn(`[KoreCatalogos] 429 en ${logLabel}, reintentando en ${esperaSeg.toFixed(1)}s (intento ${intento}/${MAX_INTENTOS})`);
        // eslint-disable-next-line no-await-in-loop
        await new Promise((r) => setTimeout(r, esperaSeg * 1000));
        continue;
      }
      if (esTimeout && intento < MAX_INTENTOS) {
        const esperaSeg = 3 * intento;
        logger.warn(`[KoreCatalogos] timeout en ${logLabel}, reintentando en ${esperaSeg}s (intento ${intento}/${MAX_INTENTOS})`);
        // eslint-disable-next-line no-await-in-loop
        await new Promise((r) => setTimeout(r, esperaSeg * 1000));
        continue;
      }

      const body = JSON.stringify(axErr.response?.data ?? {});
      logger.error(`[KoreCatalogos] ${logLabel} ${status}: ${body}`);
      throw axErr;
    }
  }
  throw ultimoError;
}

/**
 * Catálogo de conceptos (tipos de venta) de Kore. Devuelve `Data` tal cual
 * Kore lo entrega — `{ Id, Nombre, Abreviatura, Descripcion, Tipo, ... }[]`.
 */
async function obtenerConceptosKore() {
  const url = await globalConfigService.getValue('kore', 'CONCEPTOS_URL');
  const response = await _getConReintento(url, 'GET /conceptos');
  return Array.isArray(response.data?.Data) ? response.data.Data : [];
}

/**
 * Catálogo de formas de pago de Kore. Devuelve `Data` tal cual Kore lo
 * entrega — `{ ID, Nombre, AcumulaPuntosTuberos, EsPuntos, ... }[]`.
 */
async function obtenerFormasPagoKore() {
  const url = await globalConfigService.getValue('kore', 'FORMAS_PAGO_URL');
  const response = await _getConReintento(url, 'GET /api/formasdepago');
  return Array.isArray(response.data?.Data) ? response.data.Data : [];
}

module.exports = { obtenerConceptosKore, obtenerFormasPagoKore };
