'use strict';

/**
 * loyalty/shared/otp-envio.adapter.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — adapter de envío del código OTP de 6 dígitos usado por la consulta
 * pública de puntos (ver loyalty-recovery.service.js#solicitarCodigo,
 * 2026-11-23: reemplaza la simulación inline que antes vivía hardcodeada ahí). Agnóstico del
 * proveedor concreto: si mañana cambia de WhatsApp a otro canal, se toca SOLO este archivo (un
 * nuevo caso en `PROVEEDORES` + la config correspondiente en loyalty-config.service.js), nunca
 * el flujo de negocio que lo llama.
 *
 * Dos modos, 100% config-driven (Configuraciones Globales, sección `lealtad` — ver
 * `obtenerOtpEnvioModo`/`obtenerOtpEnvioProveedor`/`obtenerOtpWhatsappConfig` en
 * loyalty-config.service.js):
 *   - 'simulado' (default hoy): no llama a ninguna API externa, solo loguea que se "envió" un
 *     código — igual que el comportamiento histórico (`codigoDev` en la respuesta, resuelto
 *     aparte por el llamador, este archivo no lo conoce).
 *   - 'real': llama al proveedor configurado (`OTP_ENVIO_PROVEEDOR`, hoy solo 'whatsapp' tiene
 *     una implementación real). Todavía NO se puede probar contra la API real de Meta (el
 *     usuario está tramitando el alta en developers.facebook.com — Phone Number ID/token
 *     pendientes), por eso `obtenerOtpWhatsappConfig()` puede devolver credenciales vacías: en
 *     ese caso este adapter NUNCA lanza, solo loguea y devuelve `{enviado:false}`.
 *
 * WhatsApp Business Cloud API, directo (sin BSP intermedio), plantilla de categoría
 * "Authentication" — por contrato de Meta para esta categoría, el código viaja DOS veces en el
 * payload: una en el componente `body` (lo que el cliente lee) y otra en un componente `button`
 * `sub_type:"url"` `index:"0"` (el botón "copiar código" que WhatsApp renderiza nativo). El
 * nombre/idioma de la plantilla son configurables (`OTP_WHATSAPP_TEMPLATE_NOMBRE`/`_IDIOMA`): la
 * plantilla en sí se crea en el panel de Meta, no acá (fuera de alcance de este archivo).
 *
 * Contrato de no-ruptura (pedido explícito del usuario): `enviarCodigo` NUNCA lanza una
 * excepción que rompa el flujo de `solicitarCodigo` — cualquier falla del proveedor (red,
 * `insufficient_balance`, credenciales faltantes, timeout) se atrapa, se loguea con detalle
 * completo vía `logger` (nunca se filtra al llamador público) y se devuelve
 * `{enviado:false, motivo}` para que quien llame pueda decidir si quiere registrar algo, sin
 * depender de un try/catch propio.
 *
 * Fuera de alcance explícito (ver tarea): el webhook de estado de entrega de Meta
 * (`sent`/`delivered`/`read`/`failed`) — no hay todavía ni App Secret (necesario para validar la
 * firma del webhook) ni URL pública. `OTP_WHATSAPP_APP_SECRET`/`OTP_WHATSAPP_WABA_ID` ya se
 * guardan en Configuraciones Globales (ver loyalty-config.service.js) para cuando ese trabajo se
 * aborde, pero este archivo no los usa todavía.
 */

const axios = require('axios');
const { logger } = require('../../shared/utils/logger');
const configService = require('../domains/config/loyalty-config.service');

// Versión del Graph API de Meta — constante de código (no Configuraciones Globales): cambiarla
// es una decisión de mantenimiento de este adapter, no una config operativa del día a día, mismo
// criterio que WHATSAPP_GRAPH_VERSION no vive en GlobalConfig en ningún otro cliente de este
// repo (ver loyalty-kore-monedero.client.js, las URLs sí son config pero las versiones de API no).
const WHATSAPP_GRAPH_VERSION = 'v21.0';
const WHATSAPP_TIMEOUT_MS = 10000;

/**
 * Payload de WhatsApp Cloud API para una plantilla "Authentication" — exportado (prefijo `_`,
 * mismo criterio que `loyalty-config.service.js#_buscarOAdoptarNivelKore`) para poder probar la
 * forma exacta del payload sin pegarle a la red real.
 */
function _armarPayloadWhatsapp(telefono, codigo, { templateNombre, templateIdioma }) {
  return {
    messaging_product: 'whatsapp',
    to: telefono,
    type: 'template',
    template: {
      name: templateNombre,
      language: { code: templateIdioma },
      components: [
        { type: 'body', parameters: [{ type: 'text', text: codigo }] },
        {
          type: 'button',
          sub_type: 'url',
          index: '0',
          parameters: [{ type: 'text', text: codigo }],
        },
      ],
    },
  };
}

async function _enviarWhatsapp(telefono, codigo) {
  const cfg = await configService.obtenerOtpWhatsappConfig();

  if (!cfg.phoneNumberId || !cfg.token) {
    // Esperado hoy (2026-11-23): el usuario todavía está tramitando el alta en Meta. Nunca
    // lanzar — mismo contrato de no-ruptura del resto del archivo.
    logger.error(
      '[otp-envio] modo=real proveedor=whatsapp sin credenciales configuradas '
      + '(OTP_WHATSAPP_PHONE_NUMBER_ID/OTP_WHATSAPP_TOKEN en Configuraciones Globales) — no se envió el código.',
    );
    return { enviado: false, motivo: 'credenciales_no_configuradas' };
  }

  const url = `https://graph.facebook.com/${WHATSAPP_GRAPH_VERSION}/${cfg.phoneNumberId}/messages`;
  const payload = _armarPayloadWhatsapp(telefono, codigo, cfg);

  try {
    await axios.post(url, payload, {
      headers: {
        Authorization: `Bearer ${cfg.token}`,
        'Content-Type': 'application/json',
      },
      timeout: WHATSAPP_TIMEOUT_MS,
    });
    return { enviado: true };
  } catch (err) {
    // Nunca filtrar detalle interno (token, payload, stack completo de la respuesta de Meta) al
    // llamador público — se loguea completo acá, el caller solo recibe éxito/fracaso + un motivo
    // corto y no sensible.
    const status = err.response?.status;
    const codigoMeta = err.response?.data?.error?.code;
    const mensajeMeta = err.response?.data?.error?.message;
    logger.error(
      `[otp-envio] whatsapp POST /messages falló status=${status ?? 'sin respuesta'} `
      + `code=${codigoMeta ?? '-'} mensaje=${mensajeMeta ?? err.message}`,
    );
    return { enviado: false, motivo: codigoMeta ? `whatsapp_error_${codigoMeta}` : 'error_envio' };
  }
}

/** El código NUNCA en el log — mismo criterio que el resto del dominio (ver
 *  loyalty-recovery.service.js#solicitarCodigo): solo confirma que el modo simulado corrió. */
function _enviarSimulado() {
  logger.info('[otp-envio] modo=simulado — código generado, no se envía a ningún proveedor real.');
  return { enviado: true, simulado: true };
}

// Proveedores reales soportados hoy — agregar uno nuevo es la ÚNICA línea que este archivo
// necesita tocar para soportar un canal distinto (además de la config que lo seleccione, ver
// loyalty-config.service.js#obtenerOtpEnvioProveedor).
const PROVEEDORES = { whatsapp: _enviarWhatsapp };

/**
 * Punto único de envío de códigos OTP — agnóstico del proveedor concreto, nunca lanza.
 *
 * @param {{telefono: string, codigo: string, canal?: string}} params  `canal` opcional: fuerza
 *   el proveedor para ESTE envío puntual (ej. pruebas/overrides puntuales); por default resuelve
 *   `configService.obtenerOtpEnvioProveedor()`. Solo se consulta si `obtenerOtpEnvioModo()` es
 *   'real' — en 'simulado' (default) no se resuelve proveedor ninguno.
 * @returns {Promise<{enviado: boolean, simulado?: boolean, motivo?: string}>}
 */
async function enviarCodigo({ telefono, codigo, canal } = {}) {
  try {
    const modo = await configService.obtenerOtpEnvioModo();
    if (modo !== 'real') {
      return _enviarSimulado();
    }

    const proveedor = canal || await configService.obtenerOtpEnvioProveedor();
    const implementacion = PROVEEDORES[proveedor];
    if (!implementacion) {
      logger.error(`[otp-envio] modo=real con proveedor desconocido '${proveedor}' — no se envió el código.`);
      return { enviado: false, motivo: 'proveedor_desconocido' };
    }
    return await implementacion(telefono, codigo);
  } catch (err) {
    // Red de seguridad final (ej. una config corrupta o un error de programación en el camino
    // de arriba): CUALQUIER excepción no prevista nunca debe romper solicitarCodigo.
    logger.error(`[otp-envio] fallo inesperado enviando código: ${err.message}`);
    return { enviado: false, motivo: 'error_inesperado' };
  }
}

module.exports = { enviarCodigo, _armarPayloadWhatsapp };
