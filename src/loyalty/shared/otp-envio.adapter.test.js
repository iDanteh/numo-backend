'use strict';

/**
 * otp-envio.adapter.test.js
 * ─────────────────────────────────────────────────────────────────────────────
 * axios y loyalty-config.service se mockean completo — NUNCA se llama a la API
 * real de WhatsApp (todavía no hay credenciales reales, ver docstring del
 * adapter). Cubre: modo simulado (comportamiento preservado), modo real con
 * éxito, modo real con fallo (nunca lanza), proveedor/credenciales faltantes,
 * y que el modo/proveedor se resuelve desde Configuraciones Globales.
 */

jest.mock('axios');
jest.mock('../domains/config/loyalty-config.service', () => ({
  obtenerOtpEnvioModo: jest.fn(),
  obtenerOtpEnvioProveedor: jest.fn(),
  obtenerOtpWhatsappConfig: jest.fn(),
}));
jest.mock('../../shared/utils/logger', () => ({
  logger: { warn: jest.fn(), error: jest.fn(), info: jest.fn(), debug: jest.fn() },
}));

const axios = require('axios');
const configService = require('../domains/config/loyalty-config.service');
const { logger } = require('../../shared/utils/logger');
const { enviarCodigo, _armarPayloadWhatsapp } = require('./otp-envio.adapter');

const TELEFONO = '5512345678';
const CODIGO = '654321';

const WHATSAPP_CFG_COMPLETA = {
  phoneNumberId: '1234567890',
  wabaId: 'waba-1',
  token: 'tok-whatsapp',
  appSecret: 'secreto-app',
  templateNombre: 'verificacion_codigo',
  templateIdioma: 'es_MX',
};

beforeEach(() => {
  jest.clearAllMocks();
  configService.obtenerOtpEnvioProveedor.mockResolvedValue('whatsapp');
});

describe('modo simulado (default) — no llama a ninguna API externa', () => {
  test('obtenerOtpEnvioModo() devuelve "simulado": enviarCodigo no toca axios, devuelve enviado:true', async () => {
    configService.obtenerOtpEnvioModo.mockResolvedValue('simulado');

    const resultado = await enviarCodigo({ telefono: TELEFONO, codigo: CODIGO });

    expect(resultado).toEqual({ enviado: true, simulado: true });
    expect(axios.post).not.toHaveBeenCalled();
    expect(configService.obtenerOtpEnvioProveedor).not.toHaveBeenCalled();
    expect(configService.obtenerOtpWhatsappConfig).not.toHaveBeenCalled();
  });

  test('nunca loguea el código en texto plano', async () => {
    configService.obtenerOtpEnvioModo.mockResolvedValue('simulado');

    await enviarCodigo({ telefono: TELEFONO, codigo: CODIGO });

    for (const call of logger.info.mock.calls) {
      for (const arg of call) {
        expect(String(arg)).not.toContain(CODIGO);
      }
    }
  });
});

describe('modo real, proveedor whatsapp — éxito', () => {
  beforeEach(() => {
    configService.obtenerOtpEnvioModo.mockResolvedValue('real');
    configService.obtenerOtpWhatsappConfig.mockResolvedValue(WHATSAPP_CFG_COMPLETA);
  });

  test('llama a POST /{phone-number-id}/messages con bearer token y el payload de plantilla Authentication', async () => {
    axios.post.mockResolvedValue({ data: { messages: [{ id: 'wamid.123' }] } });

    const resultado = await enviarCodigo({ telefono: TELEFONO, codigo: CODIGO });

    expect(resultado).toEqual({ enviado: true });
    expect(axios.post).toHaveBeenCalledWith(
      `https://graph.facebook.com/v21.0/${WHATSAPP_CFG_COMPLETA.phoneNumberId}/messages`,
      expect.objectContaining({
        messaging_product: 'whatsapp',
        to: TELEFONO,
        type: 'template',
      }),
      expect.objectContaining({
        headers: expect.objectContaining({ Authorization: `Bearer ${WHATSAPP_CFG_COMPLETA.token}` }),
      }),
    );
  });

  test('el código viaja DOS veces: una en el componente body y otra en el botón sub_type:url index:"0"', async () => {
    axios.post.mockResolvedValue({});

    await enviarCodigo({ telefono: TELEFONO, codigo: CODIGO });

    const [, payload] = axios.post.mock.calls[0];
    const bodyComponent = payload.template.components.find((c) => c.type === 'body');
    const buttonComponent = payload.template.components.find((c) => c.type === 'button');

    expect(bodyComponent.parameters).toEqual([{ type: 'text', text: CODIGO }]);
    expect(buttonComponent).toMatchObject({ sub_type: 'url', index: '0' });
    expect(buttonComponent.parameters).toEqual([{ type: 'text', text: CODIGO }]);
  });

  test('_armarPayloadWhatsapp: shape exacto esperado por Meta para una plantilla Authentication', () => {
    const payload = _armarPayloadWhatsapp(TELEFONO, CODIGO, {
      templateNombre: 'verificacion_codigo', templateIdioma: 'es_MX',
    });

    expect(payload).toEqual({
      messaging_product: 'whatsapp',
      to: TELEFONO,
      type: 'template',
      template: {
        name: 'verificacion_codigo',
        language: { code: 'es_MX' },
        components: [
          { type: 'body', parameters: [{ type: 'text', text: CODIGO }] },
          { type: 'button', sub_type: 'url', index: '0', parameters: [{ type: 'text', text: CODIGO }] },
        ],
      },
    });
  });

  test('resuelve el proveedor desde configService.obtenerOtpEnvioProveedor() cuando no se pasa `canal`', async () => {
    axios.post.mockResolvedValue({});

    await enviarCodigo({ telefono: TELEFONO, codigo: CODIGO });

    expect(configService.obtenerOtpEnvioProveedor).toHaveBeenCalled();
  });

  test('`canal` explícito evita resolver el proveedor desde config', async () => {
    axios.post.mockResolvedValue({});

    await enviarCodigo({ telefono: TELEFONO, codigo: CODIGO, canal: 'whatsapp' });

    expect(configService.obtenerOtpEnvioProveedor).not.toHaveBeenCalled();
  });
});

describe('modo real, proveedor whatsapp — fallo (nunca debe lanzar)', () => {
  beforeEach(() => {
    configService.obtenerOtpEnvioModo.mockResolvedValue('real');
  });

  test('credenciales faltantes (Phone Number ID/token todavía no cargados): no llama a axios, enviado:false', async () => {
    configService.obtenerOtpWhatsappConfig.mockResolvedValue({
      ...WHATSAPP_CFG_COMPLETA, phoneNumberId: null, token: null,
    });

    const resultado = await enviarCodigo({ telefono: TELEFONO, codigo: CODIGO });

    expect(resultado).toEqual({ enviado: false, motivo: 'credenciales_no_configuradas' });
    expect(axios.post).not.toHaveBeenCalled();
    expect(logger.error).toHaveBeenCalled();
  });

  test('fallo de red (sin response, ej. timeout/ECONNABORTED): captura el error, enviado:false', async () => {
    configService.obtenerOtpWhatsappConfig.mockResolvedValue(WHATSAPP_CFG_COMPLETA);
    axios.post.mockRejectedValue(new Error('timeout of 10000ms exceeded'));

    await expect(enviarCodigo({ telefono: TELEFONO, codigo: CODIGO })).resolves.toEqual({
      enviado: false, motivo: 'error_envio',
    });
    expect(logger.error).toHaveBeenCalled();
  });

  test('fallo reportado por Meta (ej. insufficient_balance): captura el error, propaga un motivo corto sin detalle interno', async () => {
    configService.obtenerOtpWhatsappConfig.mockResolvedValue(WHATSAPP_CFG_COMPLETA);
    const errorMeta = {
      response: {
        status: 400,
        data: { error: { code: 131042, message: 'Insufficient balance for this account.' } },
      },
    };
    axios.post.mockRejectedValue(errorMeta);

    const resultado = await enviarCodigo({ telefono: TELEFONO, codigo: CODIGO });

    expect(resultado).toEqual({ enviado: false, motivo: 'whatsapp_error_131042' });
    // Nunca filtra el mensaje interno de Meta ni el token al resultado devuelto al llamador.
    expect(JSON.stringify(resultado)).not.toContain(WHATSAPP_CFG_COMPLETA.token);
    expect(JSON.stringify(resultado)).not.toContain('Insufficient balance');
  });

  test('un fallo del proveedor nunca rompe el flujo de solicitarCodigo: enviarCodigo nunca rechaza', async () => {
    configService.obtenerOtpWhatsappConfig.mockResolvedValue(WHATSAPP_CFG_COMPLETA);
    axios.post.mockRejectedValue(new Error('ECONNRESET'));

    await expect(enviarCodigo({ telefono: TELEFONO, codigo: CODIGO })).resolves.toMatchObject({ enviado: false });
  });

  test('proveedor desconocido (config corrupta/canal inválido): enviado:false, no llama a axios', async () => {
    configService.obtenerOtpEnvioProveedor.mockResolvedValue('sms-inexistente');

    const resultado = await enviarCodigo({ telefono: TELEFONO, codigo: CODIGO });

    expect(resultado).toEqual({ enviado: false, motivo: 'proveedor_desconocido' });
    expect(axios.post).not.toHaveBeenCalled();
  });
});

describe('red de seguridad final — una excepción no prevista nunca debe propagarse', () => {
  test('obtenerOtpEnvioModo rechazando (ej. config service caído): enviado:false, nunca lanza', async () => {
    configService.obtenerOtpEnvioModo.mockRejectedValue(new Error('Postgres caído'));

    await expect(enviarCodigo({ telefono: TELEFONO, codigo: CODIGO })).resolves.toEqual({
      enviado: false, motivo: 'error_inesperado',
    });
    expect(logger.error).toHaveBeenCalled();
  });
});
