/**
 * Módulo de Descarga Masiva SAT.
 *
 * Expone tres operaciones:
 *  - solicitar()        → SolicitaDescargaMasiva, retorna IdSolicitud
 *  - verificar()        → VerificaSolicitudDescarga con polling cada 60 segundos exactos
 *  - descargarPaquete() → Descarga ZIP en base64, descomprime en memoria, retorna array de XMLs
 *
 * Renovación de token: si han pasado ≥4 minutos desde la última autenticación,
 * se re-autentica antes de cada operación.
 *
 * IMPORTANTE: el polling al SAT es exactamente 60000ms. Nunca menos.
 * El SAT bloquea IPs que hacen polling agresivo.
 */

const axios = require('axios');
const AdmZip = require('adm-zip');
const { autenticar, crearFirmaSolicitud } = require('./auth');
const { logger } = require('../../shared/utils/logger');

const SOLICITUD_URL = (process.env.SAT_DESCARGA_MASIVA_SOLICITUD  || 'https://cfdidescargamasivasolicitud.clouda.sat.gob.mx/SolicitaDescargaService.svc').replace(/\?wsdl$/i, '');
const VERIFICA_URL  = (process.env.SAT_DESCARGA_MASIVA_VERIFICA   || 'https://cfdidescargamasivasolicitud.clouda.sat.gob.mx/VerificaSolicitudDescargaService.svc').replace(/\?wsdl$/i, '');
const DESCARGA_URL  = (process.env.SAT_DESCARGA_MASIVA_DESCARGA   || 'https://cfdidescargamasiva.clouda.sat.gob.mx/DescargaMasivaService.svc').replace(/\?wsdl$/i, '');

const POLLING_INTERVAL_MS = 60000; // 60 segundos exactos — no cambiar
const TOKEN_MAX_AGE_MS    = 4 * 60 * 1000; // 4 minutos

// Cache de tokens por RFC — evita que descargas concurrentes de distintos RFCs
// compartan el mismo token (que pertenece a un certificado específico).
// Map<rfc, { token: string, obtenidoEn: number }>
const _tokenCache = new Map();

/**
 * Retorna el token vigente para el RFC dado, o re-autentica si expiró.
 * @param {string} rfc
 * @param {{cerBuffer: Buffer, keyBuffer: Buffer, passwordBuffer: Buffer}} creds
 * @returns {Promise<string>}
 */
const getToken = async (rfc, creds) => {
  const ahora  = Date.now();
  const cached = _tokenCache.get(rfc);
  const vigente = cached?.token && (ahora - cached.obtenidoEn) < TOKEN_MAX_AGE_MS;

  if (vigente) return cached;

  logger.info(`[SatDownload] Obteniendo nuevo token SAT para ${rfc}...`);

  // Copiar los buffers — autenticar() los limpia con fill(0) en su finally
  const cerCopy = Buffer.from(creds.cerBuffer);
  const keyCopy = Buffer.from(creds.keyBuffer);
  const pwdCopy = Buffer.from(creds.passwordBuffer);

  const { token, rfcCertificado } = await autenticar(cerCopy, keyCopy, pwdCopy);

  if (rfcCertificado && rfcCertificado !== rfc) {
    logger.info(`[SatDownload] RFC del certificado (${rfcCertificado}) difiere del RFC solicitado (${rfc}) — se usará como RfcSolicitante`);
  }

  // Decodificar payload del JWT para extraer el RFC con el que fue emitido
  let rfcDeJwt = null;
  try {
    const parts = token.split('.');
    if (parts.length >= 2) {
      const padded = parts[1].replace(/-/g, '+').replace(/_/g, '/');
      const payload = JSON.parse(Buffer.from(padded, 'base64').toString('utf8'));
      logger.info(`[SatDownload] JWT payload: ${JSON.stringify(payload)}`);
      rfcDeJwt = payload.unique_name ?? payload.RFC ?? payload.rfc ?? payload.sub ?? null;
      if (rfcDeJwt) logger.info(`[SatDownload] RFC en token JWT: ${rfcDeJwt}`);
    }
  } catch (e) {
    logger.info(`[SatDownload] Token no decodificable como JWT estándar: ${e.message}`);
  }

  // Prioridad: RFC del JWT > RFC del certificado > RFC solicitado
  const rfcFinal = rfcDeJwt ?? rfcCertificado ?? rfc;
  if (rfcFinal !== rfc) {
    logger.info(`[SatDownload] RfcSolicitante a usar: ${rfcFinal} (diferente al RFC empresa: ${rfc})`);
  }

  const entry = { token, rfcCertificado: rfcFinal, obtenidoEn: Date.now() };
  _tokenCache.set(rfc, entry);
  return entry;
};

/**
 * Invalida el token en caché para un RFC específico.
 * @param {string} rfc
 */
const invalidarToken = (rfc) => {
  if (rfc) {
    _tokenCache.delete(rfc);
  } else {
    _tokenCache.clear(); // fallback por compatibilidad
  }
};

// ── Helpers SOAP ──────────────────────────────────────────────────────────────

// Patrón antiguo: token en WS-Security header (usado por DescargaMasivaService)
const soapCall = async (url, soapAction, body, token) => {
  const quotedAction = `"${soapAction}"`;
  const envelope = `<?xml version="1.0" encoding="UTF-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Header>
    <h:RequestSecurityTokenResponse xmlns:h="http://docs.oasis-open.org/ws-sx/ws-trust/200512">
      <wsse:BinarySecurityToken xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">${token}</wsse:BinarySecurityToken>
    </h:RequestSecurityTokenResponse>
  </s:Header>
  <s:Body>${body}</s:Body>
</s:Envelope>`;

  try {
    const response = await axios.post(url, envelope, {
      headers: {
        'Content-Type': 'text/xml; charset=utf-8',
        'SOAPAction': quotedAction,
      },
      timeout: 60000,
    });
    return response.data;
  } catch (axiosErr) {
    logger.error(`[SatDownload] Error HTTP en soapCall:`);
    logger.error(`  URL: ${url}`);
    logger.error(`  SOAPAction: ${soapAction}`);
    logger.error(`  STATUS: ${axiosErr.response?.status ?? 'sin respuesta'}`);
    logger.error(`  HEADERS: ${JSON.stringify(axiosErr.response?.headers ?? {})}`);
    logger.error(`  DATA: ${axiosErr.response?.data ?? axiosErr.message}`);
    throw axiosErr;
  }
};

// Patrón SAT oficial: Authorization: WRAP access_token="Token" (Solicitud, Verifica, Descarga)
// Ref: Documentación SAT "Servicio de Verificación de Descarga Masiva 2023" §4 y §5
const soapCallBearer = async (url, soapAction, envelope, token) => {
  const quotedAction = `"${soapAction}"`;
  try {
    const response = await axios.post(url, envelope, {
      headers: {
        'Content-Type': 'text/xml; charset=utf-8',
        'SOAPAction': quotedAction,
        'Authorization': `WRAP access_token="${token}"`,
      },
      timeout: 60000,
    });
    return response.data;
  } catch (axiosErr) {
    logger.error(`[SatDownload] Error HTTP en soapCallBearer:`);
    logger.error(`  URL: ${url}`);
    logger.error(`  SOAPAction: ${soapAction}`);
    logger.error(`  STATUS: ${axiosErr.response?.status ?? 'sin respuesta'}`);
    logger.error(`  HEADERS: ${JSON.stringify(axiosErr.response?.headers ?? {})}`);
    logger.error(`  DATA: ${axiosErr.response?.data ?? axiosErr.message}`);
    throw axiosErr;
  }
};

const extraerValor = (xml, tag) => {
  const match = xml.match(new RegExp(`<[^:]*:?${tag}[^>]*>([^<]+)<`));
  return match ? match[1].trim() : null;
};

const extraerAtributo = (xml, tag, attr) => {
  const match = xml.match(new RegExp(`<[^:]*:?${tag}[^>]*\\s+${attr}="([^"]+)"`));
  return match ? match[1].trim() : null;
};

/**
 * Construye la forma canónica C14N del elemento <des:solicitud> SIN firma.
 * Atributos en orden alfabético; namespaces en scope declarados explícitamente.
 * Se usa para calcular el DigestValue de la firma dentro del body SOAP.
 */
const canonizarSolicitud = (attrs, ns) => {
  const attrsStr = Object.keys(attrs).sort().map(k => `${k}="${attrs[k]}"`).join(' ');
  return `<des:solicitud xmlns:des="${ns}" xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" ${attrsStr}></des:solicitud>`;
};

/**
 * Construye la forma canónica C14N del elemento <des:peticionDescarga> SIN firma.
 * Atributos IdPaquete e RfcSolicitante en orden alfabético (I < R).
 */
const canonizarPeticionDescarga = (idPaquete, rfcSolicitante, ns) => {
  return `<des:peticionDescarga xmlns:des="${ns}" xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" IdPaquete="${idPaquete}" RfcSolicitante="${rfcSolicitante}"></des:peticionDescarga>`;
};

// ── Operaciones principales ───────────────────────────────────────────────────

/**
 * Solicita la descarga masiva al SAT.
 *
 * @param {object} params
 * @param {string} params.rfcSolicitante
 * @param {string} params.fechaInicio     — ISO 8601 sin zona: "2024-01-01T00:00:00"
 * @param {string} params.fechaFin        — ISO 8601 sin zona: "2024-01-31T23:59:59"
 * @param {'CFDI'|'Retenciones'} params.tipoSolicitud
 * @param {'Emitidos'|'Recibidos'|'Traslados'|'Nomina'|'Pagos'} params.tipoComprobante
 * @param {{cerBuffer, keyBuffer, passwordBuffer}} params.creds
 * @returns {Promise<string>} IdSolicitud
 */

// Mapa de tipo de comprobante a operación SOAP y atributos SAT
const TIPO_MAP = {
  Emitidos:  { operacion: 'SolicitaDescargaEmitidos',  rfcAttrKey: 'RfcEmisor',   tipoDeComprobante: null },
  Recibidos: { operacion: 'SolicitaDescargaRecibidos', rfcAttrKey: 'RfcReceptor', tipoDeComprobante: null },
  Ingresos:  { operacion: 'SolicitaDescargaEmitidos',  rfcAttrKey: 'RfcEmisor',   tipoDeComprobante: 'I'  },
  Egresos:   { operacion: 'SolicitaDescargaEmitidos',  rfcAttrKey: 'RfcEmisor',   tipoDeComprobante: 'E'  },
  Traslados: { operacion: 'SolicitaDescargaEmitidos',  rfcAttrKey: 'RfcEmisor',   tipoDeComprobante: 'T'  },
  Nomina:    { operacion: 'SolicitaDescargaEmitidos',  rfcAttrKey: 'RfcEmisor',   tipoDeComprobante: 'N'  },
  Pagos:     { operacion: 'SolicitaDescargaEmitidos',  rfcAttrKey: 'RfcEmisor',   tipoDeComprobante: 'P'  },
};

const solicitar = async (params) => {
  const { rfcSolicitante, fechaInicio, fechaFin, tipoSolicitud = 'CFDI', tipoComprobante = 'Emitidos', creds } = params;

  const { token, rfcCertificado } = await getToken(rfcSolicitante, creds);
  const rfcFirma = rfcCertificado ?? rfcSolicitante;
  logger.info(`[SatDownload] solicitar() | rfcFirma=${rfcFirma} rfcEmpresa=${rfcSolicitante} token: ${token?.slice(0, 40)}...`);

  const cfg = TIPO_MAP[tipoComprobante] ?? TIPO_MAP.Emitidos;
  const { operacion, rfcAttrKey, tipoDeComprobante } = cfg;
  const soapAction = `http://DescargaMasivaTerceros.sat.gob.mx/ISolicitaDescargaService/${operacion}`;
  const ns         = 'http://DescargaMasivaTerceros.sat.gob.mx';

  // Construir firma del body — el SAT requiere Signature dentro de <des:solicitud>
  const buildEnvelope = async (rfcFirmaUsado) => {
    const solicitudAttrs = {
      FechaFinal:     fechaFin,
      FechaInicial:   fechaInicio,
      [rfcAttrKey]:   rfcSolicitante,
      RfcSolicitante: rfcFirmaUsado,
      TipoSolicitud:  tipoSolicitud,
    };
    if (tipoDeComprobante) solicitudAttrs.TipoDeComprobante = tipoDeComprobante;

    const canonical = canonizarSolicitud(solicitudAttrs, ns);
    logger.info(`[SatDownload] solicitar() — canonical solicitud: ${canonical}`);

    const cerCopy = Buffer.from(creds.cerBuffer);
    const keyCopy = Buffer.from(creds.keyBuffer);
    const pwdCopy = Buffer.from(creds.passwordBuffer);
    const firma   = await crearFirmaSolicitud(cerCopy, keyCopy, pwdCopy, canonical);

    const tipoAttr = tipoDeComprobante ? ` TipoDeComprobante="${tipoDeComprobante}"` : '';

    return `<?xml version="1.0" encoding="UTF-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:des="${ns}">
  <s:Header/>
  <s:Body>
    <des:${operacion}>
      <des:solicitud RfcSolicitante="${rfcFirmaUsado}" ${rfcAttrKey}="${rfcSolicitante}" FechaInicial="${fechaInicio}" FechaFinal="${fechaFin}" TipoSolicitud="${tipoSolicitud}"${tipoAttr}>
        ${firma}
      </des:solicitud>
    </des:${operacion}>
  </s:Body>
</s:Envelope>`;
  };

  const resultTag = `${operacion}Result`;

  let envelope   = await buildEnvelope(rfcFirma);
  let xmlResp    = await soapCallBearer(SOLICITUD_URL, soapAction, envelope, token);

  let idSolicitud = extraerAtributo(xmlResp, resultTag, 'IdSolicitud') || extraerValor(xmlResp, 'IdSolicitud');
  let codEstatus  = extraerAtributo(xmlResp, resultTag, 'CodEstatus');
  let mensaje     = extraerAtributo(xmlResp, resultTag, 'Mensaje');

  // Retry si el SAT rechaza el token (300): invalida caché y reintenta con token fresco
  if (!idSolicitud && codEstatus === '300') {
    logger.warn(`[SatDownload] Token rechazado (300) — invalidando caché y reintentando con token fresco...`);
    invalidarToken(rfcSolicitante);
    const fresh        = await getToken(rfcSolicitante, creds);
    const rfcFirmaFresh = fresh.rfcCertificado ?? rfcSolicitante;
    logger.info(`[SatDownload] rfcFirma en retry: ${rfcFirmaFresh}`);

    envelope    = await buildEnvelope(rfcFirmaFresh);
    xmlResp     = await soapCallBearer(SOLICITUD_URL, soapAction, envelope, fresh.token);
    idSolicitud = extraerAtributo(xmlResp, resultTag, 'IdSolicitud') || extraerValor(xmlResp, 'IdSolicitud');
    codEstatus  = extraerAtributo(xmlResp, resultTag, 'CodEstatus');
    mensaje     = extraerAtributo(xmlResp, resultTag, 'Mensaje');
  }

  if (!idSolicitud) {
    logger.error(`[SatDownload] solicitar() — respuesta SAT completa:\n${xmlResp}`);
    invalidarToken(rfcSolicitante);

    // Códigos especiales documentados por el SAT
    if (codEstatus === '5005') {
      throw new Error(
        `SAT [5005]: Solicitud duplicada — ya existe una solicitud activa con los mismos parámetros ` +
        `(mismo RFC, fechas y tipo). Espera a que termine o usa el checkpoint existente.`
      );
    }
    if (codEstatus === '5002') {
      throw new Error(
        `SAT [5002]: Se agotó el límite de solicitudes de por vida para este RFC y rango de fechas. ` +
        `No es posible generar otra solicitud con los mismos parámetros.`
      );
    }
    if (codEstatus === '5003') {
      throw new Error(
        `SAT [5003]: El rango solicitado supera el tope máximo de CFDIs por solicitud. ` +
        `Reduce el rango de fechas a períodos más cortos.`
      );
    }
    if (codEstatus === '404') {
      throw new Error(
        `SAT [404]: Error no controlado en el servidor SAT. Reintenta la operación. ` +
        `Si persiste, genera un RMA con el SAT.`
      );
    }

    throw new Error(`SAT [${codEstatus}]: ${mensaje}`);
  }

  logger.info(`[SatDownload] Solicitud aceptada (${operacion}). IdSolicitud: ${idSolicitud}`);
  return idSolicitud;
};

/**
 * Estados de verificación SAT:
 *  1 = Aceptada
 *  2 = En Proceso
 *  3 = Terminada (listo para descargar)
 *  4 = Error
 *  5 = Rechazada
 *  6 = Vencida (72 horas después de que se generó el paquete — ya no descargable)
 */
const ESTADO_TERMINADA = '3';
const ESTADO_ERROR     = '4';
const ESTADO_RECHAZADA = '5';
const ESTADO_VENCIDA   = '6';

/**
 * Verifica el estado de una solicitud con polling cada 60 segundos exactos.
 * Retorna cuando el estado es Terminada, Error o Rechazada.
 *
 * @param {string} idSolicitud
 * @param {string} rfcSolicitante
 * @param {{cerBuffer, keyBuffer, passwordBuffer}} creds
 * @returns {Promise<{idsPaquetes: string[], totalCfdis: number}>}
 */
const MAX_POLLING_INTENTOS = 60; // máximo 60 minutos de espera

const verificar = async (idSolicitud, rfcSolicitante, creds) => {
  logger.info(`[SatDownload] Iniciando polling para solicitud ${idSolicitud}...`);

  const esperar = () => new Promise(r => setTimeout(r, POLLING_INTERVAL_MS));
  let intentos = 0;

  while (intentos < MAX_POLLING_INTENTOS) {
    intentos++;
    // Si el token expiró durante el polling, reintentar autenticación una vez
    let token, rfcCertificado;
    try {
      ({ token, rfcCertificado } = await getToken(rfcSolicitante, creds));
    } catch (tokenErr) {
      logger.warn(`[SatDownload] Error renovando token en intento ${intentos}: ${tokenErr.message}. Reintentando...`);
      invalidarToken(rfcSolicitante);
      ({ token, rfcCertificado } = await getToken(rfcSolicitante, creds));
    }
    const rfcFirmaVerif = rfcCertificado ?? rfcSolicitante;

    const verNs = 'http://DescargaMasivaTerceros.sat.gob.mx';
    const verifAttrs = { IdSolicitud: idSolicitud, RfcSolicitante: rfcFirmaVerif };
    const verifCanonical = canonizarSolicitud(verifAttrs, verNs);

    const cerCopyV = Buffer.from(creds.cerBuffer);
    const keyCopyV = Buffer.from(creds.keyBuffer);
    const pwdCopyV = Buffer.from(creds.passwordBuffer);
    const firmaV   = await crearFirmaSolicitud(cerCopyV, keyCopyV, pwdCopyV, verifCanonical);

    const envelope = `<?xml version="1.0" encoding="UTF-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:des="${verNs}">
  <s:Header/>
  <s:Body>
    <des:VerificaSolicitudDescarga>
      <des:solicitud IdSolicitud="${idSolicitud}" RfcSolicitante="${rfcFirmaVerif}">
        ${firmaV}
      </des:solicitud>
    </des:VerificaSolicitudDescarga>
  </s:Body>
</s:Envelope>`;

    const xmlResp = await soapCallBearer(
      VERIFICA_URL,
      'http://DescargaMasivaTerceros.sat.gob.mx/IVerificaSolicitudDescargaService/VerificaSolicitudDescarga',
      envelope,
      token
    );

    const estadoSolicitud = extraerAtributo(xmlResp, 'VerificaSolicitudDescargaResult', 'EstadoSolicitud') ||
                            extraerValor(xmlResp, 'EstadoSolicitud');
    const codEstatus      = extraerAtributo(xmlResp, 'VerificaSolicitudDescargaResult', 'CodEstatus');
    const mensaje         = extraerAtributo(xmlResp, 'VerificaSolicitudDescargaResult', 'Mensaje');
    const totalCfdis      = parseInt(extraerAtributo(xmlResp, 'VerificaSolicitudDescargaResult', 'NumeroCFDIs') || '0', 10);

    logger.info(`[SatDownload] Estado solicitud ${idSolicitud}: ${estadoSolicitud} (${descripcionEstado(estadoSolicitud)})`);

    if (estadoSolicitud === ESTADO_TERMINADA) {
      // Extraer IDs de paquetes
      const idsPaquetes = [];
      const regex = /<[^:]*:?IdsPaquetes[^>]*>([^<]+)</g;
      let m;
      while ((m = regex.exec(xmlResp)) !== null) {
        idsPaquetes.push(m[1].trim());
      }
      // También puede venir como atributos
      if (idsPaquetes.length === 0) {
        const attrRegex = /IdsPaquetes="([^"]+)"/g;
        while ((m = attrRegex.exec(xmlResp)) !== null) {
          idsPaquetes.push(...m[1].split(',').map(s => s.trim()).filter(Boolean));
        }
      }

      logger.info(`[SatDownload] Solicitud terminada. Paquetes: ${idsPaquetes.length}, CFDIs totales: ${totalCfdis}`);
      return { idsPaquetes, totalCfdis };
    }

    if (estadoSolicitud === ESTADO_ERROR || estadoSolicitud === ESTADO_RECHAZADA) {
      throw new Error(`SAT [${codEstatus}]: ${mensaje}`);
    }

    if (estadoSolicitud === ESTADO_VENCIDA) {
      throw new Error(`SAT: La solicitud ${idSolicitud} venció (72 horas). Genera una nueva solicitud.`);
    }

    // Errores de verificación aunque el HTTP haya sido exitoso
    if (codEstatus === '5004') {
      throw new Error(`SAT [5004]: No se encontró la información de la solicitud ${idSolicitud}. Puede que haya expirado o nunca existió.`);
    }
    if (codEstatus === '5011') {
      throw new Error(`SAT [5011]: Límite de descargas por folio por día alcanzado. Intenta mañana.`);
    }

    // Estados 1 (Aceptada) y 2 (En Proceso): esperar exactamente 60 segundos
    logger.info(`[SatDownload] Intento ${intentos}/${MAX_POLLING_INTENTOS} — esperando 60s...`);
    await esperar();
  }

  throw new Error(`Timeout: solicitud ${idSolicitud} no terminó después de ${MAX_POLLING_INTENTOS} minutos`);
};

/**
 * Descarga un paquete ZIP en base64, lo descomprime en memoria y retorna los XMLs.
 * El ZIP nunca se escribe en disco.
 *
 * @param {string} idPaquete
 * @param {string} rfcSolicitante
 * @param {{cerBuffer, keyBuffer, passwordBuffer}} creds
 * @returns {Promise<string[]>} array de strings XML (uno por CFDI)
 */
const descargarPaquete = async (idPaquete, rfcSolicitante, creds) => {
  const MAX_INTENTOS  = 3;
  const ESPERA_BASE_MS = 5_000; // 5 s, se duplica en cada intento

  let ultimoError;

  for (let intento = 1; intento <= MAX_INTENTOS; intento++) {
    try {
      // Renovar token en cada intento (invalida caché si el anterior falló)
      if (intento > 1) invalidarToken(rfcSolicitante);
      const { token, rfcCertificado } = await getToken(rfcSolicitante, creds);
      const rfcFirmaDesc = rfcCertificado ?? rfcSolicitante;

      // Namespace correcto del servicio Descarga (sat.gob.mx, no gob.mx)
      // WCF hace ContractFilter matching sobre SOAPAction + namespace del body element.
      const ns = 'http://DescargaMasivaTerceros.sat.gob.mx';

      const canonical = canonizarPeticionDescarga(idPaquete, rfcFirmaDesc, ns);
      logger.info(`[SatDownload] descargarPaquete() — canonical peticionDescarga: ${canonical}`);

      const cerCopy = Buffer.from(creds.cerBuffer);
      const keyCopy = Buffer.from(creds.keyBuffer);
      const pwdCopy = Buffer.from(creds.passwordBuffer);
      const firma   = await crearFirmaSolicitud(cerCopy, keyCopy, pwdCopy, canonical);

      const descEnvelope = `<?xml version="1.0" encoding="UTF-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:des="${ns}">
  <s:Header/>
  <s:Body>
    <des:PeticionDescargaMasivaTercerosEntrada>
      <des:peticionDescarga IdPaquete="${idPaquete}" RfcSolicitante="${rfcFirmaDesc}">
        ${firma}
      </des:peticionDescarga>
    </des:PeticionDescargaMasivaTercerosEntrada>
  </s:Body>
</s:Envelope>`;

      const xmlResp = await soapCallBearer(
        DESCARGA_URL,
        'http://DescargaMasivaTerceros.sat.gob.mx/IDescargaMasivaTercerosService/Descargar',
        descEnvelope,
        token
      );

      // El SAT retorna el ZIP como base64 dentro de <Paquete>
      const paqueteMatch = xmlResp.match(/<[^:]*:?Paquete[^>]*>([\s\S]+?)<\/[^:]*:?Paquete>/);
      if (!paqueteMatch || !paqueteMatch[1]) {
        throw new Error(`No se encontró el paquete en la respuesta del SAT para IdPaquete: ${idPaquete}`);
      }

      // Liberar la string base64 (33% extra de memoria) antes de crear el Buffer
      let zipBase64 = paqueteMatch[1].trim();
      const zipBuffer = Buffer.from(zipBase64, 'base64');
      zipBase64 = null; // eslint-disable-line no-unused-vars

      // Descomprimir en memoria con adm-zip, liberar el buffer zip al terminar
      const zip  = new AdmZip(zipBuffer);
      const xmls = [];
      for (const entrada of zip.getEntries()) {
        if (entrada.entryName.toLowerCase().endsWith('.xml')) {
          const data = entrada.getData();
          xmls.push(data.toString('utf-8'));
          data.fill(0); // limpiar buffer de la entrada después de convertir
        }
      }

      logger.info(`[SatDownload] Paquete ${idPaquete} descomprimido: ${xmls.length} XMLs (intento ${intento})`);
      return xmls;

    } catch (err) {
      ultimoError = err;
      logger.warn(`[SatDownload] descargarPaquete intento ${intento}/${MAX_INTENTOS} fallido para ${idPaquete}: ${err.message}`);

      if (intento < MAX_INTENTOS) {
        const espera = ESPERA_BASE_MS * intento; // 5 s, 10 s
        logger.info(`[SatDownload] Reintentando en ${espera / 1000} s...`);
        await new Promise(r => setTimeout(r, espera));
      }
    }
  }

  throw new Error(`descargarPaquete falló después de ${MAX_INTENTOS} intentos para ${idPaquete}: ${ultimoError?.message}`);
};

// ── Utilidades ────────────────────────────────────────────────────────────────

const descripcionEstado = (estado) => {
  const map = { '1': 'Aceptada', '2': 'En Proceso', '3': 'Terminada', '4': 'Error', '5': 'Rechazada', '6': 'Vencida' };
  return map[estado] || 'Desconocido';
};

module.exports = { solicitar, verificar, descargarPaquete, invalidarToken };
