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
  Emitidos:           { operacion: 'SolicitaDescargaEmitidos',  rfcAttrKey: 'RfcEmisor',   tipoDeComprobante: null },
  Recibidos:          { operacion: 'SolicitaDescargaRecibidos', rfcAttrKey: 'RfcReceptor', tipoDeComprobante: null },
  Ingresos:           { operacion: 'SolicitaDescargaEmitidos',  rfcAttrKey: 'RfcEmisor',   tipoDeComprobante: 'I'  },
  Egresos:            { operacion: 'SolicitaDescargaEmitidos',  rfcAttrKey: 'RfcEmisor',   tipoDeComprobante: 'E'  },
  Traslados:          { operacion: 'SolicitaDescargaEmitidos',  rfcAttrKey: 'RfcEmisor',   tipoDeComprobante: 'T'  },
  Nomina:             { operacion: 'SolicitaDescargaEmitidos',  rfcAttrKey: 'RfcEmisor',   tipoDeComprobante: 'N'  },
  Pagos:              { operacion: 'SolicitaDescargaEmitidos',  rfcAttrKey: 'RfcEmisor',   tipoDeComprobante: 'P'  },
  RecibidosIngresos:  { operacion: 'SolicitaDescargaRecibidos', rfcAttrKey: 'RfcReceptor', tipoDeComprobante: 'I'  },
  RecibidosEgresos:   { operacion: 'SolicitaDescargaRecibidos', rfcAttrKey: 'RfcReceptor', tipoDeComprobante: 'E'  },
  RecibidosTraslados: { operacion: 'SolicitaDescargaRecibidos', rfcAttrKey: 'RfcReceptor', tipoDeComprobante: 'T'  },
  RecibidosNomina:    { operacion: 'SolicitaDescargaRecibidos', rfcAttrKey: 'RfcReceptor', tipoDeComprobante: 'N'  },
  RecibidosPagos:     { operacion: 'SolicitaDescargaRecibidos', rfcAttrKey: 'RfcReceptor', tipoDeComprobante: 'P'  },
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
      // Extraer IDs de paquetes — el SAT usa distintos formatos según versión del WSDL:
      // Formato 1: <IdsPaquetes>id1</IdsPaquetes><IdsPaquetes>id2</IdsPaquetes>  (texto directo)
      // Formato 2: <a:IdsPaquetes><b:string>id1</b:string><b:string>id2</b:string></a:IdsPaquetes>  (WCF arrays)
      // Formato 3: IdsPaquetes="id1,id2" como atributo  (raro)
      const ids = new Set();
      let m;

      // Formato 1: texto directo dentro de cada <IdsPaquetes>texto</IdsPaquetes>
      // Ejemplo: <IdsPaquetes>UUID_01</IdsPaquetes><IdsPaquetes>UUID_02</IdsPaquetes>
      const regex1 = /<[^:]*:?IdsPaquetes[^>]*>([^<]+)/g;
      while ((m = regex1.exec(xmlResp)) !== null) {
        const v = m[1].trim();
        if (v) ids.add(v);
      }

      // Formato 2: <b:string> hijos dentro de bloques <IdsPaquetes>…</IdsPaquetes>
      // Ejemplo: <a:IdsPaquetes><b:string>UUID_01</b:string><b:string>UUID_02</b:string></a:IdsPaquetes>
      // Se corre SIEMPRE (no solo si ids está vacío) para cubrir respuestas de formato mixto.
      const bloqueRegex = /<[^:]*:?IdsPaquetes[^>]*>([\s\S]*?)<\/[^:]*:?IdsPaquetes>/g;
      let bloqueMatch;
      while ((bloqueMatch = bloqueRegex.exec(xmlResp)) !== null) {
        const stringRegex = /<[^:]*:?string[^>]*>([^<]+)/g;
        while ((m = stringRegex.exec(bloqueMatch[1])) !== null) {
          const v = m[1].trim();
          if (v) ids.add(v);
        }
      }

      // Formato 3: atributo IdsPaquetes="UUID_01,UUID_02,…"
      // Se corre SIEMPRE para no perder IDs si se mezcla con otros formatos.
      const attrRegex = /IdsPaquetes="([^"]+)"/g;
      while ((m = attrRegex.exec(xmlResp)) !== null) {
        m[1].split(',').forEach(s => { const v = s.trim(); if (v) ids.add(v); });
      }

      const idsPaquetes = [...ids];
      // Nota: NumeroCFDIs en la respuesta del SAT puede ser "0" aunque haya paquetes válidos
      // (comportamiento documentado en el manual oficial del SAT). No se usa para validar
      // la completitud — el número de paquetes extraídos es la fuente de verdad.
      logger.info(`[SatDownload] Solicitud terminada. Paquetes: ${idsPaquetes.length}, CFDIs reportados por SAT: ${totalCfdis || '(no reportado)'}`);
      if (idsPaquetes.length > 0) {
        logger.info(`[SatDownload] IdsPaquetes extraídos: ${idsPaquetes.join(', ')}`);
      } else {
        logger.warn(`[SatDownload] ⚠ No se pudieron extraer IdsPaquetes. Respuesta SAT:\n${xmlResp.slice(0, 2000)}`);
      }
      return { idsPaquetes, totalCfdis };
    }

    if (estadoSolicitud === ESTADO_RECHAZADA) {
      // EstadoSolicitud=5 significa que el SAT rechazó la solicitud (no el call SOAP).
      // CodEstatus/Mensaje son del response SOAP y pueden ser engañosos (ej. "5000 Solicitud Aceptada").
      // Causas frecuentes: solicitud activa previa para el mismo RFC, límite diario alcanzado,
      // o parámetros duplicados. Se lanza un error con el tag SAT_RECHAZADA para que el
      // caller pueda detectarlo y hacer retry.
      throw new Error(
        `SAT_RECHAZADA: El SAT rechazó la solicitud (EstadoSolicitud=5). ` +
        `Causa probable: solicitud activa previa para este RFC o límite diario de solicitudes alcanzado. ` +
        `(CodEstatus=${codEstatus}, Mensaje="${mensaje}")`
      );
    }
    if (estadoSolicitud === ESTADO_ERROR) {
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
 * Descarga un paquete ZIP del SAT y retorna el Buffer sin descomprimir.
 * Compartido por descargarPaquete() y descargarPaqueteMetadata().
 */
const _descargarZipBuffer = async (idPaquete, rfcSolicitante, creds) => {
  // SAT permite máximo 2 descargas por paquete (error 5008 si se excede).
  // Usamos exactamente 2 intentos para respetar ese límite.
  const MAX_INTENTOS   = 2;
  const ESPERA_BASE_MS = 5_000;
  let ultimoError;

  for (let intento = 1; intento <= MAX_INTENTOS; intento++) {
    try {
      if (intento > 1) invalidarToken(rfcSolicitante);
      const { token, rfcCertificado } = await getToken(rfcSolicitante, creds);
      const rfcFirmaDesc = rfcCertificado ?? rfcSolicitante;

      const ns       = 'http://DescargaMasivaTerceros.sat.gob.mx';
      const canonical = canonizarPeticionDescarga(idPaquete, rfcFirmaDesc, ns);
      logger.info(`[SatDownload] descarga canonical: ${canonical}`);

      const cerCopy = Buffer.from(creds.cerBuffer);
      const keyCopy = Buffer.from(creds.keyBuffer);
      const pwdCopy = Buffer.from(creds.passwordBuffer);
      const firma   = await crearFirmaSolicitud(cerCopy, keyCopy, pwdCopy, canonical);

      const envelope = `<?xml version="1.0" encoding="UTF-8"?>
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
        envelope,
        token,
      );

      const paqueteMatch = xmlResp.match(/<[^:]*:?Paquete[^>]*>([\s\S]+?)<\/[^:]*:?Paquete>/);
      if (!paqueteMatch || !paqueteMatch[1]) {
        throw new Error(`No se encontró el paquete en la respuesta del SAT para IdPaquete: ${idPaquete}`);
      }

      let zipBase64   = paqueteMatch[1].trim();
      const zipBuffer = Buffer.from(zipBase64, 'base64');
      zipBase64 = null; // eslint-disable-line no-unused-vars
      return zipBuffer;

    } catch (err) {
      ultimoError = err;
      logger.warn(`[SatDownload] _descargarZipBuffer intento ${intento}/${MAX_INTENTOS} fallido para ${idPaquete}: ${err.message}`);
      if (intento < MAX_INTENTOS) {
        const espera = ESPERA_BASE_MS * intento;
        logger.info(`[SatDownload] Reintentando en ${espera / 1000} s...`);
        await new Promise(r => setTimeout(r, espera));
      }
    }
  }

  throw new Error(`_descargarZipBuffer falló después de ${MAX_INTENTOS} intento(s) para ${idPaquete}: ${ultimoError?.message}. ` +
    `(SAT limita cada paquete a 2 descargas — error 5008 si se reintenta después de 2 fallos)`);
};

/**
 * Parsea el TXT de metadatos del SAT (pipe o tab separado).
 * Retorna array de objetos con campos normalizados.
 */
const parsearMetadataTxt = (contenido) => {
  const lineas = contenido.split(/\r?\n/).map(l => l.trim()).filter(Boolean);
  if (lineas.length < 2) return [];

  const sep     = lineas[0].includes('|') ? '|' : lineas[0].includes('~') ? '~' : '\t';
  const limpiar = str => str.replace(/^"|"$/g, '').trim();

  const normalizarClave = (nombre) => {
    const n = nombre.toLowerCase()
      .normalize('NFD').replace(/[\u0300-\u036f]/g, '') // quitar acentos
      .trim();
    if (n.includes('folio fiscal'))                              return 'uuid';
    if (n === 'rfc emisor' || n.startsWith('rfc emisor'))        return 'rfcEmisor';
    if (n.includes('emisor') && n.includes('razon'))             return 'nombreEmisor';
    if (n.includes('emisor') && n.includes('nombre'))            return 'nombreEmisor';
    if (n === 'rfc receptor' || n.startsWith('rfc receptor'))    return 'rfcReceptor';
    if (n.includes('receptor') && n.includes('razon'))           return 'nombreReceptor';
    if (n.includes('receptor') && n.includes('nombre'))          return 'nombreReceptor';
    if (n.includes('fecha') && n.includes('emision'))            return 'fecha';
    if (n.includes('fecha') && n.includes('certific'))           return 'fechaCert';
    if (n.includes('pac'))                                       return 'rfcPac';
    if (n === 'total')                                           return 'total';
    if (n.includes('efecto'))                                    return 'efecto';
    if (n.includes('estado'))                                    return 'estado';
    if (n.includes('fecha') && n.includes('cancel'))             return 'fechaCancelacion';
    return n.replace(/\s+/g, '_');
  };

  const claves = lineas[0].split(sep).map(h => normalizarClave(limpiar(h)));

  const registros = [];
  for (let i = 1; i < lineas.length; i++) {
    const campos = lineas[i].split(sep).map(limpiar);
    const obj    = {};
    claves.forEach((clave, idx) => { obj[clave] = campos[idx] ?? ''; });
    if (obj.uuid) registros.push(obj);
  }
  return registros;
};

/**
 * Descarga un paquete ZIP en base64, lo descomprime en memoria y retorna los XMLs.
 * @returns {Promise<string[]>} array de strings XML (uno por CFDI)
 */
const descargarPaquete = async (idPaquete, rfcSolicitante, creds) => {
  const zipBuffer = await _descargarZipBuffer(idPaquete, rfcSolicitante, creds);
  const zip  = new AdmZip(zipBuffer);
  const xmls = [];
  for (const entrada of zip.getEntries()) {
    if (entrada.entryName.toLowerCase().endsWith('.xml')) {
      const data = entrada.getData();
      xmls.push(data.toString('utf-8'));
      data.fill(0);
    }
  }
  logger.info(`[SatDownload] Paquete ${idPaquete}: ${xmls.length} XMLs`);
  return xmls;
};

/**
 * Descarga un paquete de metadatos (.txt), lo descomprime y retorna los registros parseados.
 * @returns {Promise<object[]>} array de objetos con campos uuid, rfcEmisor, rfcReceptor, total, fecha, efecto, estado, etc.
 */
const descargarPaqueteMetadata = async (idPaquete, rfcSolicitante, creds) => {
  const zipBuffer = await _descargarZipBuffer(idPaquete, rfcSolicitante, creds);
  const zip       = new AdmZip(zipBuffer);
  const entradas  = zip.getEntries();

  // Loguear siempre el contenido del ZIP para diagnóstico
  logger.info(`[SatDownload] ZIP ${idPaquete} contiene ${entradas.length} entrada(s): ${entradas.map(e => e.entryName).join(', ')}`);

  const registros = [];

  // Primera pasada: archivos .txt (formato esperado SAT)
  for (const entrada of entradas) {
    if (entrada.entryName.toLowerCase().endsWith('.txt')) {
      // Intentar UTF-8 primero; si falla o da 0 registros, reintentar con latin1
      let contenido = entrada.getData().toString('utf-8');
      let parsed    = parsearMetadataTxt(contenido);

      if (parsed.length === 0) {
        // Diagnóstico: loguear las primeras 3 líneas para ver el formato real
        const primeras = contenido.split(/\r?\n/).slice(0, 3);
        logger.warn(`[SatDownload] TXT '${entrada.entryName}' en UTF-8 dio 0 registros. Primeras líneas:`);
        primeras.forEach((l, i) => logger.warn(`  [${i}] ${JSON.stringify(l)}`));

        // Reintentar decodificando como latin1 (Windows-1252) — común en archivos SAT
        contenido = entrada.getData().toString('latin1');
        parsed    = parsearMetadataTxt(contenido);
        if (parsed.length > 0) {
          logger.info(`[SatDownload] Metadata recuperada con encoding latin1: ${parsed.length} registros`);
        } else {
          const primerasL = contenido.split(/\r?\n/).slice(0, 3);
          logger.warn(`[SatDownload] TXT '${entrada.entryName}' en latin1 también dio 0 registros. Primeras líneas:`);
          primerasL.forEach((l, i) => logger.warn(`  [${i}] ${JSON.stringify(l)}`));
        }
      }

      registros.push(...parsed);
    }
  }

  // Segunda pasada: si no hubo .txt, intentar con cualquier archivo de texto (xml, sin extensión, etc.)
  if (registros.length === 0) {
    logger.warn(`[SatDownload] No se encontraron archivos .txt en ${idPaquete}. Intentando con otras entradas...`);
    for (const entrada of entradas) {
      const nombre = entrada.entryName.toLowerCase();
      if (!nombre.endsWith('.zip')) {
        try {
          const contenido = entrada.getData().toString('utf-8');
          const parsed    = parsearMetadataTxt(contenido);
          if (parsed.length > 0) {
            logger.info(`[SatDownload] Metadata extraída de '${entrada.entryName}': ${parsed.length} registros`);
            registros.push(...parsed);
          }
        } catch { /* ignorar entradas no parseables */ }
      }
    }
  }

  logger.info(`[SatDownload] Paquete metadata ${idPaquete}: ${registros.length} registros`);
  return registros;
};

// ── Utilidades ────────────────────────────────────────────────────────────────

const descripcionEstado = (estado) => {
  const map = { '1': 'Aceptada', '2': 'En Proceso', '3': 'Terminada', '4': 'Error', '5': 'Rechazada', '6': 'Vencida' };
  return map[estado] || 'Desconocido';
};

module.exports = { solicitar, verificar, descargarPaquete, descargarPaqueteMetadata, invalidarToken };
