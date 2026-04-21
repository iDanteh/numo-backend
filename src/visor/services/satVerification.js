const axios = require('axios');
const { parseStringPromise } = require('xml2js');
const { logger } = require('../../shared/utils/logger');

const SAT_ENDPOINT = 'https://consultaqr.facturaelectronica.sat.gob.mx/ConsultaCFDIService.svc';
const SOAP_ACTION  = 'http://tempuri.org/IConsultaCFDIService/Consulta';
const BASE_URL     = 'https://verificacfdi.facturaelectronica.sat.gob.mx/default.aspx';

/**
 * Verifica el estado de un CFDI con el SAT vía SOAP.
 *
 * Estrategia:
 *  1. Intenta con el formato de la versión indicada (default 4.0).
 *  2. Si N-601 con 4.0, reintenta con 3.3 (cubre CFDIs con versión
 *     incorrecta en DB por haber sido importados sin XML).
 *  3. Si ambos dan N-601, el CFDI no está registrado en SAT producción.
 *
 * @param {string} uuid
 * @param {string} rfcEmisor
 * @param {string} rfcReceptor
 * @param {number} total
 * @param {string} [sello]   - SelloCFD del TimbreFiscalDigital (requerido para fe)
 * @param {string} [version] - '3.3' | '4.0'  (default '4.0')
 */
const verifyCFDIWithSAT = async (uuid, rfcEmisor, rfcReceptor, total, sello = '', version = '4.0', tipoDeComprobante = '') => {
  if (!sello) {
    logger.warn(`[SAT] ${uuid} — sello vacío: el campo "fe" irá vacío. CFDI importado sin XML.`);
  }

  // Para Pagos (tipo P), el SAT registra Total=0 en el XML aunque el ERP reporte el importe.
  // Enviar el importe real causa N-601 porque la expresión impresa no coincide.
  const totalEfectivo = tipoDeComprobante?.toUpperCase() === 'P' ? 0 : total;
  if (tipoDeComprobante?.toUpperCase() === 'P') {
    logger.info(`[SAT] ${uuid} — tipo P (Pago), usando tt=0 para la expresión impresa`);
  }

  const result = await _querySAT(uuid, rfcEmisor, rfcReceptor, totalEfectivo, sello, version);

  // N-601 con 4.0 → puede ser CFDI 3.3 guardado con versión incorrecta en DB
  if (result.state === 'Expresión Inválida' && version === '4.0') {
    logger.info(`[SAT] ${uuid} — N-601 en v4.0, reintentando con v3.3 (posible versión incorrecta en DB)`);
    const result33 = await _querySAT(uuid, rfcEmisor, rfcReceptor, totalEfectivo, sello, '3.3');
    if (result33.state !== 'Expresión Inválida') {
      logger.info(`[SAT] ${uuid} — resuelto como CFDI 3.3: ${result33.state}`);
    }
    return result33;
  }

  return result;
};

/**
 * Ejecuta la consulta SOAP al SAT con la versión indicada.
 * Función interna — usar verifyCFDIWithSAT desde fuera.
 */
const _querySAT = async (uuid, rfcEmisor, rfcReceptor, total, sello, version) => {
  const expresion    = buildExpresionImpresa(uuid, rfcEmisor, rfcReceptor, total, sello, version);
  const expresionXml = expresion.replace(/&/g, '&amp;');

  logger.info(`[SAT] Expresión enviada (v${version}): ${expresion}`);

  const envelope = `<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <Consulta xmlns="http://tempuri.org/">
      <expresionImpresa>${expresionXml}</expresionImpresa>
    </Consulta>
  </s:Body>
</s:Envelope>`;

  let response;
  try {
    response = await axios.post(SAT_ENDPOINT, envelope, {
      headers: {
        'Content-Type': 'text/xml; charset=utf-8',
        'SOAPAction': `"${SOAP_ACTION}"`,
      },
      timeout: 20000,
    });
  } catch (err) {
    const detail = err.response ? `HTTP ${err.response.status}` : err.message;
    logger.error(`[SAT] Error de conexión para ${uuid}: ${detail}`);
    throw new Error(`Sin respuesta del SAT: ${err.message}`);
  }

  logger.debug(`[SAT] Respuesta cruda (${uuid}): ${response.data}`);

  return parseSATResponse(response.data, uuid, version, expresion);
};

/**
 * Construye la expresión impresa (URL del portal SAT) según la versión del CFDI.
 *
 * CFDI 4.0: BASE_URL?id=UUID&re=RFC_EMISOR&rr=RFC_RECEPTOR&tt=TOTAL.6DEC&fe=FE8
 * CFDI 3.3: BASE_URL?&id=UUID&re=RFC_EMISOR&rr=RFC_RECEPTOR&tt=TOTAL17PADDED&fe=FE8
 *
 * Diferencias clave:
 *  - 4.0: prefijo "?"  / tt con 6 decimales sin padding (ej. "1543.200000")
 *  - 3.3: prefijo "?&" / tt con 6 decimales padded a 17 chars (ej. "000001543.200000")
 *  - Ambas versiones incluyen el parámetro fe (últimos 8 chars del SelloCFD)
 *  - El fe NO se URL-encoda: el SAT espera chars base64 literales (/, +, =)
 */
const buildExpresionImpresa = (uuid, rfcEmisor, rfcReceptor, total, sello = '', version = '4.0') => {
  const uuidClean   = (uuid       || '').toUpperCase().trim();
  const rfcEm       = (rfcEmisor  || '').toUpperCase().trim();
  const rfcRe       = (rfcReceptor|| '').toUpperCase().trim();
  const fe          = sello ? sello.replace(/\s/g, '').slice(-8) : '';
  const totalNum    = parseFloat(total);

  if (version === '4.0') {
    const tt = totalNum.toFixed(6);
    return `${BASE_URL}?id=${uuidClean}&re=${rfcEm}&rr=${rfcRe}&tt=${tt}&fe=${fe}`;
  }

  // CFDI 3.3 — prefijo ?& y tt padded a 17 chars incluyendo el punto decimal
  const tt = totalNum.toFixed(6).padStart(17, '0');
  return `${BASE_URL}?&id=${uuidClean}&re=${rfcEm}&rr=${rfcRe}&tt=${tt}&fe=${fe}`;
};

const parseSATResponse = async (xmlString, uuid, version = '?', expresionEnviada = '') => {
  let parsed;
  try {
    parsed = await parseStringPromise(xmlString, {
      explicitArray: false,
      tagNameProcessors: [(name) => name.replace(/^.+:/, '')],
    });
  } catch {
    throw new Error('Respuesta SAT no es XML válido');
  }

  const result = parsed?.Envelope?.Body?.ConsultaResponse?.ConsultaResult;

  if (!result) {
    logger.warn(`[SAT] Estructura inesperada para ${uuid}: ${JSON.stringify(parsed).substring(0, 300)}`);
    throw new Error('Respuesta SAT con estructura no reconocida');
  }

  const codigoEstatus = result.CodigoEstatus || '';
  const estado        = result.Estado        || '';
  const esCancelable  = result.EsCancelable  || '';
  const estatusCancel = result.EstatusCancelacion || '';

  const state = resolveState(codigoEstatus, estado);

  if (state === 'Expresión Inválida') {
    logger.error(
      `[SAT] N-601 — Expresión mal formada [v${version}]: ` +
      `uuid=${uuid} | código=${codigoEstatus} | expresión=${expresionEnviada}`
    );
  } else {
    logger.info(`[SAT] ${uuid} → ${state} (${codigoEstatus}) [v${version}]`);
  }

  return {
    state,
    isCancelled:       state === 'Cancelado',
    isCancellable:     esCancelable.includes('Cancelable'),
    estadoCancelacion: estatusCancel,
    codigoEstatus,
    rawResponse: result,
  };
};

const resolveState = (codigo, estado) => {
  const c = (codigo + ' ' + estado).toLowerCase();
  if (c.includes('601'))                                return 'Expresión Inválida';
  if (c.includes('200') || c.includes('vigente'))       return 'Vigente';
  if (c.includes('201') || c.includes('cancelado'))     return 'Cancelado';
  if (c.includes('202') || c.includes('no encontrado')) return 'No Encontrado';
  if (c.includes('400'))                                return 'Error';
  if (!codigo && !estado)                               return 'Error';
  return estado || codigo || 'Desconocido';
};

module.exports = { verifyCFDIWithSAT, buildExpresionImpresa };
