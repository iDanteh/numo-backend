/**
 * Autenticación con el SAT usando e.firma (FIEL).
 *
 * Flujo:
 *  1. Parsea .cer (DER → cert) y .key (DER cifrado → clave privada) con node-forge.
 *  2. Construye el envelope SOAP firmado con SHA1+RSA (requerimiento del SAT).
 *  3. Llama al servicio AutenticaService del SAT.
 *  4. Extrae y retorna el token.
 *
 * Seguridad:
 *  - Los buffers de .cer, .key y contraseña se limpian con fill(0) después de usarse.
 *  - El token no se persiste en base de datos.
 */

const forge = require('node-forge');
const axios = require('axios');
const { logger } = require('../utils/logger');

const AUTENTICACION_URL = (
  process.env.SAT_DESCARGA_MASIVA_AUTENTICACION ||
  'https://cfdidescargamasivasolicitud.clouda.sat.gob.mx/Autenticacion/Autenticacion.svc'
).replace(/\?wsdl$/i, '');

// ── Parsers tolerantes ────────────────────────────────────────────────────────

/**
 * Parsea un buffer DER de certificado .cer.
 * Intenta tres estrategias en orden para tolerar variaciones de formato.
 *
 * @param {Buffer} cerBuf
 * @returns {{ cert: object|null, b64: string }}
 */
const parseCer = (cerBuf) => {
  const binary = cerBuf.toString('binary');

  try {
    // Intento 1: parseo estándar DER
    const asn1 = forge.asn1.fromDer(forge.util.createBuffer(binary));
    const cert  = forge.pki.certificateFromAsn1(asn1);
    const pem   = forge.pki.certificateToPem(cert);
    const b64   = pem.replace(/-----[^-]+-----|\n/g, '');
    return { cert, b64 };
  } catch {
    try {
      // Intento 2: bytes extra al final — strict: false
      const asn1 = forge.asn1.fromDer(forge.util.createBuffer(binary), { strict: false });
      const cert  = forge.pki.certificateFromAsn1(asn1);
      const pem   = forge.pki.certificateToPem(cert);
      const b64   = pem.replace(/-----[^-]+-----|\n/g, '');
      return { cert, b64 };
    } catch {
      // Intento 3: incapaz de parsear ASN.1 — usar el DER crudo como base64
      // (el SAT acepta el BinarySecurityToken como base64 del DER)
      const b64 = Buffer.isBuffer(cerBuf)
        ? cerBuf.toString('base64')
        : Buffer.from(cerBuf).toString('base64');
      return { cert: null, b64 };
    }
  }
};

/**
 * Parsea y descifra un buffer DER de llave privada .key.
 * Intenta dos estrategias: DER estándar y DER con strict: false.
 *
 * @param {Buffer} keyBuf
 * @param {string} password
 * @returns {object} privateKey de node-forge
 */
const parseKey = (keyBuf, password) => {
  try {
    const bin      = Buffer.isBuffer(keyBuf) ? keyBuf : Buffer.from(keyBuf, 'base64');
    const forgeBuf = forge.util.createBuffer(bin.toString('binary'));
    const asn1     = forge.asn1.fromDer(forgeBuf, { strict: false });
    const keyInfo  = forge.pki.decryptPrivateKeyInfo(asn1, password);
    if (!keyInfo) throw new Error('Contraseña incorrecta');
    return forge.pki.privateKeyFromAsn1(keyInfo);
  } catch (e) {
    throw new Error('No se pudo parsear la llave privada: ' + e.message);
  }
};

// ── Función principal ─────────────────────────────────────────────────────────

/**
 * Obtiene un token SAT para la Descarga Masiva.
 *
 * @param {Buffer} cerBuffer      — contenido del .cer en DER
 * @param {Buffer} keyBuffer      — contenido del .key en DER cifrado
 * @param {Buffer} passwordBuffer — contraseña como Buffer (UTF-8)
 * @returns {Promise<string>} token SAT (válido ~5 minutos según SAT)
 */
const autenticar = async (cerBuffer, keyBuffer, passwordBuffer) => {
  let privateKey = null;

  try {
    const password = passwordBuffer.toString('utf-8');

    // ── 1. Parsear .cer y .key ────────────────────────────────────────────
    const { cert, b64: certB64 } = parseCer(cerBuffer);
    privateKey = parseKey(keyBuffer, password);

    // Número de certificado (serie en decimal, 20 dígitos)
    // Si no se pudo parsear el cert, usamos un serial genérico
    let noCertificado = '00000000000000000000';
    if (cert && cert.serialNumber) {
      try {
        noCertificado = BigInt('0x' + cert.serialNumber).toString().padStart(20, '0');
      } catch {
        noCertificado = cert.serialNumber.padStart(20, '0');
      }
    }

    // ── 2. Validar que el certificado es FIEL y no CSD ───────────────────
    if (cert) {
      const subjStr = cert.subject.attributes.map(a => `${a.shortName}=${a.value}`).join(',').toUpperCase();
      logger.info(`[SatAuth] Certificado subject: ${subjStr}`);
      cert.subject.attributes.forEach((a, i) =>
        logger.info(`[SatAuth]   attr[${i}] type=${a.type} short=${a.shortName} value="${a.value}"`)
      );

      // OIDs de política de certificado emitidos por el SAT (México, OID raíz 2.16.484.101.10.97)
      //  .2.4.4.2 = Sello Digital de CFDI (CSD)
      //  .2.4.4.1 = e.firma (FIEL) — se puede usar como whitelist también
      const SAT_OID_CSD_POLICY = '2.16.484.101.10.97.2.4.4.2';
      // OID heredado de Entrust que SAT también usó en generaciones antiguas de CSD:
      const ENTRUST_OID_CSD    = '2.16.840.1.113839.0.6.3';

      const isCSD =
        subjStr.includes('SELLO') ||
        cert.extensions?.some(e => e.id === SAT_OID_CSD_POLICY) ||
        cert.extensions?.some(e => e.id === ENTRUST_OID_CSD);

      if (isCSD) {
        throw new Error(
          'El certificado es un Sello Digital (CSD), no una e.firma (FIEL). ' +
          'La Descarga Masiva del SAT requiere la e.firma personal del representante legal. ' +
          'Contacta a tu contador para obtener los archivos .cer y .key de la e.firma ' +
          '(no los que usas para emitir facturas).'
        );
      }
    }

    // ── 3. Construir el envelope SOAP firmado ─────────────────────────────
    // Restar 60s al created para absorber desfase de reloj entre el servidor y el SAT.
    // La ventana del SAT es 5 minutos; con este buffer sigue siendo válido si el
    // servidor está hasta 60s adelantado respecto al reloj del SAT.
    const now     = new Date();
    const created = new Date(now.getTime() - 60 * 1000);
    const expires = new Date(now.getTime() + 4 * 60 * 1000);

    const createdStr = created.toISOString().replace(/\.\d{3}Z$/, 'Z');
    const expiresStr = expires.toISOString().replace(/\.\d{3}Z$/, 'Z');

    const timestampBody = `<u:Timestamp xmlns:u="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd" u:Id="_0"><u:Created>${createdStr}</u:Created><u:Expires>${expiresStr}</u:Expires></u:Timestamp>`;

    const md = forge.md.sha1.create();
    md.update(timestampBody, 'utf8');
    const digestB64 = forge.util.encode64(md.digest().bytes());

    const signedInfo = `<SignedInfo xmlns="http://www.w3.org/2000/09/xmldsig#"><CanonicalizationMethod Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"></CanonicalizationMethod><SignatureMethod Algorithm="http://www.w3.org/2000/09/xmldsig#rsa-sha1"></SignatureMethod><Reference URI="#_0"><Transforms><Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"></Transform></Transforms><DigestMethod Algorithm="http://www.w3.org/2000/09/xmldsig#sha1"></DigestMethod><DigestValue>${digestB64}</DigestValue></Reference></SignedInfo>`;

    const mdSig = forge.md.sha1.create();
    mdSig.update(signedInfo, 'utf8');
    const signatureBytes = privateKey.sign(mdSig);
    const signatureB64 = forge.util.encode64(signatureBytes);

    const soapEnvelope = `<?xml version="1.0" encoding="UTF-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:u="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
  <s:Header>
    <o:Security s:mustUnderstand="1" xmlns:o="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
      ${timestampBody}
      <o:BinarySecurityToken u:Id="uuid-${noCertificado}" ValueType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-x509-token-profile-1.0#X509v3" EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary">${certB64}</o:BinarySecurityToken>
      <Signature xmlns="http://www.w3.org/2000/09/xmldsig#">
        ${signedInfo}
        <SignatureValue>${signatureB64}</SignatureValue>
        <KeyInfo>
          <o:SecurityTokenReference>
            <o:Reference URI="#uuid-${noCertificado}" ValueType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-x509-token-profile-1.0#X509v3"/>
          </o:SecurityTokenReference>
        </KeyInfo>
      </Signature>
    </o:Security>
  </s:Header>
  <s:Body>
    <Autentica xmlns="http://DescargaMasivaTerceros.gob.mx"/>
  </s:Body>
</s:Envelope>`;

    // ── 3. Llamar al servicio SAT ─────────────────────────────────────────
    logger.info('[SatAuth] Autenticando RFC con e.firma...');

    let response;
    try {
      response = await axios.post(AUTENTICACION_URL, soapEnvelope, {
        headers: {
          'Content-Type': 'text/xml; charset=utf-8',
          'SOAPAction': 'http://DescargaMasivaTerceros.gob.mx/IAutenticacion/Autentica',
        },
        timeout: 30000,
      });
    } catch (axiosErr) {
      logger.error('[SatAuth] Error HTTP al llamar al SAT:');
      logger.error(`  URL: ${AUTENTICACION_URL}`);
      logger.error(`  STATUS: ${axiosErr.response?.status ?? 'sin respuesta'}`);
      logger.error(`  HEADERS: ${JSON.stringify(axiosErr.response?.headers ?? {})}`);
      logger.error(`  DATA: ${axiosErr.response?.data ?? axiosErr.message}`);
      throw axiosErr;
    }

    // ── 4. Extraer token ──────────────────────────────────────────────────
    const token = extraerToken(response.data);
    if (!token) {
      logger.error('[SatAuth] Token no encontrado. Respuesta del SAT:');
      logger.error(`  STATUS: ${response.status}`);
      logger.error(`  DATA: ${response.data}`);
      throw new Error('Token no encontrado en response del SAT');
    }

    const rfcCertificado = extraerRfcDeCert(cert);
    logger.info(`[SatAuth] Token obtenido correctamente | RFC en certificado: ${rfcCertificado ?? 'no detectado'}`);
    return { token, rfcCertificado };

  } finally {
    // ── 5. Limpiar datos sensibles de memoria ─────────────────────────────
    if (cerBuffer && Buffer.isBuffer(cerBuffer)) cerBuffer.fill(0);
    if (keyBuffer && Buffer.isBuffer(keyBuffer)) keyBuffer.fill(0);
    if (passwordBuffer && Buffer.isBuffer(passwordBuffer)) passwordBuffer.fill(0);
    privateKey = null;
  }
};

/**
 * Extrae el bearer token del XML de respuesta SOAP del SAT.
 *
 * El SAT puede devolver el token en dos formatos:
 *  A) Raw bearer — se usa directamente.
 *  B) WRAP:  "WRAP access_token%3d"<token>"&token_type%3d"WRAP""
 *     → hay que URL-decodificar y extraer el valor de access_token.
 *
 * @param {string} xmlResponse
 * @returns {string|null}
 */
/**
 * Extrae el RFC del subject de un certificado SAT.
 * El RFC puede estar en cualquier atributo del subject como valor exacto
 * (12 chars empresa: AAA######XX) o como prefijo de un valor más largo
 * que incluye CURP (RFC + CURP = 30/31 chars).
 *
 * @param {object} cert — cert de node-forge
 * @returns {string|null}
 */
const extraerRfcDeCert = (cert) => {
  if (!cert) return null;
  const RFC_EMPRESA  = /^[A-Z&Ñ]{3}[0-9]{6}[A-Z0-9]{3}$/;
  const RFC_PERSONA  = /^[A-Z&Ñ]{4}[0-9]{6}[A-Z0-9]{3}$/;
  for (const attr of cert.subject.attributes) {
    const val = (attr.value ?? '').toString().trim().toUpperCase();
    if (RFC_EMPRESA.test(val) || RFC_PERSONA.test(val)) return val;

    // FIEL de empresa (persona moral): OID 2.5.4.45 = "RFC_EMPRESA / RFC_REPRESENTANTE"
    // Se prefiere el RFC de la empresa (para quien se emite el token).
    if (val.includes(' / ')) {
      const parts = val.split(' / ').map(p => p.trim()).filter(Boolean);
      // Preferir RFC_EMPRESA (la empresa) sobre RFC_PERSONA (representante)
      const empresa = parts.find(p => RFC_EMPRESA.test(p));
      if (empresa) return empresa;
      const persona = parts.find(p => RFC_PERSONA.test(p));
      if (persona) return persona;
    }

    // RFC con sufijo (> 13 chars) — intentar slicing
    if (val.length >= 12 && /^[A-Z0-9&Ñ]/.test(val)) {
      const c13 = val.slice(0, 13);
      const c12 = val.slice(0, 12);
      if (RFC_PERSONA.test(c13)) return c13;
      if (RFC_EMPRESA.test(c12)) return c12;
    }
  }
  return null;
};

const extraerToken = (xmlResponse) => {
  // Intentar extraer el contenido de AutenticaResult (texto plano o CDATA)
  let raw = null;

  const match = xmlResponse.match(/<AutenticaResult[^>]*>([\s\S]*?)<\/AutenticaResult>/);
  if (match?.[1]?.trim()) raw = match[1].trim();

  if (!raw) {
    const cdataMatch = xmlResponse.match(/<AutenticaResult[^>]*><!\[CDATA\[([\s\S]*?)\]\]><\/AutenticaResult>/);
    if (cdataMatch?.[1]?.trim()) raw = cdataMatch[1].trim();
  }

  if (!raw) return null;

  // Eliminar TODOS los espacios en blanco internos — el SAT puede formatear el XML
  // con saltos de línea dentro del token, lo que rompe el header Authorization
  raw = raw.replace(/\s+/g, '');

  logger.info(`[SatAuth] Token extraído (primeros 60 chars): ${raw.slice(0, 60)}`);

  // Formato WRAP — extraer y URL-decodificar el access_token.
  // Las comillas pueden llegar como " literales o como %22 URL-encoded.
  if (raw.toLowerCase().includes('access_token')) {
    const wrapMatch = raw.match(/access_token(?:%3[Dd]|=)(?:%22|")(.*?)(?:%22|")(?:&|$)/i);
    if (wrapMatch?.[1]) {
      const token = decodeURIComponent(wrapMatch[1]).replace(/\s+/g, '');
      logger.info('[SatAuth] Token extraído desde formato WRAP');
      return token;
    }
    logger.warn(`[SatAuth] Formato WRAP detectado pero no parseado. Raw (100 chars): ${raw.slice(0, 100)}`);
  }

  if (raw.toLowerCase().startsWith('wrap')) {
    logger.error(`[SatAuth] Token parece WRAP sin parsear — puede causar error 300`);
  }

  // Formato directo (JWT u otro)
  return raw;
};

/**
 * Formatea el DN del emisor del certificado para X509IssuerName.
 * Orden inverso (más específico al más general), formato RFC 2253.
 */
const buildIssuerDN = (issuer) => {
  if (!issuer?.attributes?.length) return '';
  return [...issuer.attributes]
    .reverse()
    .map(a => {
      const name = a.shortName || a.type;
      const val  = (a.value ?? '').toString();
      return val.includes(',') ? `${name}="${val}"` : `${name}=${val}`;
    })
    .join(', ');
};

/**
 * Crea el elemento <Signature> XML-DSig (enveloped) para insertar dentro de <des:solicitud>.
 *
 * El SAT requiere que los servicios SolicitaDescarga y VerificaSolicitudDescarga
 * incluyan una firma digital dentro del elemento <des:solicitud>.
 * Ref: Documentación SAT "Servicio de Verificación de Descarga Masiva 2023" §5.
 *
 * @param {Buffer} cerBuffer
 * @param {Buffer} keyBuffer
 * @param {Buffer} passwordBuffer
 * @param {string} canonicalSolicitud — forma canónica C14N del <des:solicitud> SIN la firma
 * @returns {Promise<string>} XML del elemento <Signature> listo para insertar
 */
const crearFirmaSolicitud = async (cerBuffer, keyBuffer, passwordBuffer, canonicalSolicitud) => {
  let privateKey = null;
  try {
    const password = passwordBuffer.toString('utf-8');
    const { cert, b64: certB64 } = parseCer(cerBuffer);
    privateKey = parseKey(keyBuffer, password);

    // Digest SHA1 del elemento solicitud en forma canónica (sin firma)
    const md = forge.md.sha1.create();
    md.update(canonicalSolicitud, 'utf8');
    const digestB64 = forge.util.encode64(md.digest().bytes());

    // SignedInfo construido en forma canónica (CanonicalizationMethod = c14n estándar)
    const signedInfo =
      `<SignedInfo xmlns="http://www.w3.org/2000/09/xmldsig#">` +
      `<CanonicalizationMethod Algorithm="http://www.w3.org/TR/2001/REC-xml-c14n-20010315"></CanonicalizationMethod>` +
      `<SignatureMethod Algorithm="http://www.w3.org/2000/09/xmldsig#rsa-sha1"></SignatureMethod>` +
      `<Reference URI="">` +
      `<Transforms><Transform Algorithm="http://www.w3.org/2000/09/xmldsig#enveloped-signature"></Transform></Transforms>` +
      `<DigestMethod Algorithm="http://www.w3.org/2000/09/xmldsig#sha1"></DigestMethod>` +
      `<DigestValue>${digestB64}</DigestValue>` +
      `</Reference>` +
      `</SignedInfo>`;

    // Firma RSA-SHA1 del SignedInfo canonico
    const mdSig = forge.md.sha1.create();
    mdSig.update(signedInfo, 'utf8');
    const signatureB64 = forge.util.encode64(privateKey.sign(mdSig));

    // Datos del certificado para X509IssuerSerial
    const issuerName = cert ? buildIssuerDN(cert.issuer) : '';
    let serialDec = '0';
    if (cert?.serialNumber) {
      try { serialDec = BigInt('0x' + cert.serialNumber).toString(); }
      catch { serialDec = cert.serialNumber; }
    }

    return (
      `<Signature xmlns="http://www.w3.org/2000/09/xmldsig#">` +
      signedInfo +
      `<SignatureValue>${signatureB64}</SignatureValue>` +
      `<KeyInfo><X509Data>` +
      `<X509IssuerSerial>` +
      `<X509IssuerName>${issuerName}</X509IssuerName>` +
      `<X509SerialNumber>${serialDec}</X509SerialNumber>` +
      `</X509IssuerSerial>` +
      `<X509Certificate>${certB64}</X509Certificate>` +
      `</X509Data></KeyInfo>` +
      `</Signature>`
    );
  } finally {
    if (cerBuffer && Buffer.isBuffer(cerBuffer))           cerBuffer.fill(0);
    if (keyBuffer && Buffer.isBuffer(keyBuffer))           keyBuffer.fill(0);
    if (passwordBuffer && Buffer.isBuffer(passwordBuffer)) passwordBuffer.fill(0);
    privateKey = null;
  }
};

module.exports = { autenticar, parseCer, parseKey, extraerRfcDeCert, crearFirmaSolicitud };
