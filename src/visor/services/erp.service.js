'use strict';

/**
 * ERPService
 * ----------
 * Responsabilidad única: comunicarse con el API externo del ERP.
 * No transforma datos ni accede a la base de datos.
 *
 * Configuración vía variables de entorno:
 *   ERP_API_BASE_URL  — URL base del ERP (ej. https://test.facturacion.koreingenieria.com)
 *   ERP_API_TOKEN     — Token de autenticación (KoreToken)
 */

const axios  = require('axios');
const config = require('../../config/env');
const { logger } = require('../../shared/utils/logger');

const LIMITE_PG      = 100;    // registros por página — máximo que permite el ERP
const TIMEOUT        = 30_000; // 30 segundos por petición
const MAX_REINTENTOS = 4;      // intentos totales ante 429 / 5xx
const BACKOFF_BASE   = 2_000;  // espera inicial: 2s, 4s, 8s, 16s

// ─── Helpers ────────────────────────────────────────────────────────────────

/**
 * Crea la instancia de Axios pre-configurada con la autenticación del ERP.
 * ERP_API_BASE_URL y ERP_API_TOKEN son validadas al arrancar en config/env.js,
 * por lo que aquí siempre estarán definidas.
 */
const crearCliente = () => axios.create({
  baseURL: config.erp.baseUrl,
  timeout: TIMEOUT,
  headers: {
    Accept:        'application/json',
    Authorization: config.erp.token,
  },
});

/** Espera ms milisegundos */
const esperar = (ms) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Convierte un error de Axios en un error de dominio con código descriptivo.
 * Facilita que el controlador devuelva mensajes claros al frontend.
 */
const clasificarError = (err) => {
  // Sin respuesta del servidor — red, DNS, timeout de conexión
  if (!err.response) {
    if (err.code === 'ECONNABORTED' || err.code === 'ETIMEDOUT') {
      const e = new Error(`El ERP no respondió en ${TIMEOUT / 1000} segundos (timeout)`);
      e.code   = 'ERP_TIMEOUT';
      e.status = 504;
      return e;
    }
    const e = new Error(`No se pudo conectar con el ERP: ${err.message}`);
    e.code   = 'ERP_CONNECTION_ERROR';
    e.status = 502;
    return e;
  }

  const http = err.response.status;

  if (http === 429) {
    const e = new Error('El ERP está limitando las peticiones (rate limit). Intenta de nuevo en unos segundos.');
    e.code      = 'ERP_RATE_LIMIT';
    e.status    = 429;
    e.retryable = true;
    return e;
  }
  if (http >= 500) {
    const e = new Error(`Error en el servidor ERP (HTTP ${http})`);
    e.code      = 'ERP_SERVER_ERROR';
    e.status    = 502;
    e.retryable = true;
    return e;
  }
  if (http === 401 || http === 403) {
    const e = new Error('Token de autenticación inválido o sin permisos en el ERP');
    e.code   = 'ERP_AUTH_ERROR';
    e.status = 502;
    return e;
  }
  if (http === 400) {
    const msg = err.response.data?.Mensaje ?? err.response.data?.mensaje ?? 'Parámetros inválidos';
    const e   = new Error(`El ERP rechazó la solicitud: ${msg}`);
    e.code    = 'ERP_INVALID_PARAMS';
    e.status  = 502;
    return e;
  }

  const e = new Error(`Error en el servidor ERP (HTTP ${http})`);
  e.code   = 'ERP_SERVER_ERROR';
  e.status = 502;
  return e;
};

// ─── Funciones públicas ──────────────────────────────────────────────────────

/**
 * Descarga una página de facturas del endpoint /api/facturas/reporte.
 *
 * @param {object} params
 * @param {string} params.fechaInicio  — RFC3339, ej. "2026-03-01T00:00:00Z"
 * @param {string} params.fechaFin     — RFC3339, ej. "2026-03-31T23:59:59Z"
 * @param {number} [params.pagina=1]
 * @returns {Promise<{ facturas: object[], paginacion: object }>}
 */
const fetchPagina = async ({ fechaInicio, fechaFin, pagina = 1 }) => {
  const cliente = crearCliente();
  const params = {
    fecha_inicio: fechaInicio,
    fecha_fin:    fechaFin,
    pagina,
    limite:       LIMITE_PG,
  };

  let intento = 0;
  while (true) {
    intento++;
    logger.info(`[ERPService] GET /api/facturas/reporte página=${pagina} intento=${intento}/${MAX_REINTENTOS}`);
    try {
      const { data: respuesta } = await cliente.get('/api/facturas/reporte', { params });

      logger.info(`[ERPService] Respuesta página ${pagina}: ${JSON.stringify(respuesta).slice(0, 600)}`);

      if (respuesta?.Codigo !== 200) {
        const e = new Error(`El ERP respondió con código ${respuesta?.Codigo}: ${respuesta?.Mensaje ?? 'sin mensaje'}`);
        e.code   = 'ERP_INVALID_RESPONSE';
        e.status = 502;
        throw e;
      }

      const data       = respuesta?.Data;
      const facturas   = data?.facturas   ?? data?.Facturas   ?? [];
      const paginacion = data?.paginacion ?? data?.Paginacion ?? {};

      logger.info(`[ERPService] Data.keys=${Object.keys(data ?? {}).join(',')} | facturas=${facturas.length} | paginacion=${JSON.stringify(paginacion)}`);

      if (!Array.isArray(facturas)) {
        const e = new Error(`ERP: "Data.facturas" no es un array (recibido: ${typeof facturas})`);
        e.code   = 'ERP_INVALID_RESPONSE';
        e.status = 502;
        throw e;
      }

      return { facturas, paginacion };

    } catch (err) {
      const domErr = err.code?.startsWith('ERP_') ? err : clasificarError(err);

      if (domErr.retryable && intento < MAX_REINTENTOS) {
        const espera = BACKOFF_BASE * Math.pow(2, intento - 1);
        logger.warn(`[ERPService] ${domErr.code} en página ${pagina} — reintentando en ${espera / 1000}s (intento ${intento}/${MAX_REINTENTOS})`);
        await esperar(espera);
        continue;
      }

      throw domErr;
    }
  }
};

/**
 * Descarga TODAS las facturas del ERP para el rango dado,
 * manejando la paginación de forma completamente transparente.
 *
 * Diseño: el controlador nunca conoce los detalles de paginación;
 * recibe un array plano con todos los registros.
 *
 * @param {object} params
 * @param {string} params.fechaInicio
 * @param {string} params.fechaFin
 * @returns {Promise<object[]>}
 */
const fetchTodasLasFacturas = async ({ fechaInicio, fechaFin }) => {
  logger.info(`[ERPService] Iniciando descarga | rango: ${fechaInicio} → ${fechaFin}`);

  const { facturas: primera, paginacion } = await fetchPagina({ fechaInicio, fechaFin, pagina: 1 });

  const totalPaginas = paginacion?.TotalPaginas ?? paginacion?.totalPaginas ?? paginacion?.total_paginas ?? 1;
  const totalERP     = paginacion?.Total ?? paginacion?.total ?? primera.length;

  logger.info(`[ERPService] ERP reporta ${totalERP} registros en ${totalPaginas} página(s)`);

  if (totalPaginas <= 1) return primera;

  const resto = [];
  for (let p = 2; p <= totalPaginas; p++) {
    await esperar(500); // pausa entre páginas para respetar el rate limit
    logger.info(`[ERPService] Descargando página ${p}/${totalPaginas}...`);
    const { facturas } = await fetchPagina({ fechaInicio, fechaFin, pagina: p });
    resto.push(...facturas);
  }

  const todas = [...primera, ...resto];
  logger.info(`[ERPService] Descarga completada — ${todas.length} facturas totales`);
  return todas;
};

module.exports = { fetchTodasLasFacturas };
