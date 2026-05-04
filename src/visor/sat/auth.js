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

const forge  = require('node-forge');
const crypto = require('crypto');
const axios  = require('axios');
const { spawn } = require('child_process');
const fs   = require('fs');
const os   = require('os');
const path = require('path');
const { logger } = require('../../shared/utils/logger');

/**
 * Ejecuta openssl de forma asíncrona sin bloquear el event loop.
 * spawnSync bloquea hasta 10 s — inaceptable en un servidor HTTP concurrente.
 */
const opensslAsync = (args, timeout = 10_000) =>
  new Promise((resolve) => {
    const chunks = { stdout: [], stderr: [] };
    let proc;
    try {
      proc = spawn('openssl', args);
    } catch (spawnErr) {
      // openssl no disponible en PATH
      resolve({ status: -1, stdout: Buffer.alloc(0), stderr: Buffer.from(spawnErr.message) });
      return;
    }

    const timer = setTimeout(() => {
      proc.kill();
      resolve({ status: -1, stdout: Buffer.alloc(0), stderr: Buffer.from('timeout') });
    }, timeout);

    proc.stdout.on('data', (d) => chunks.stdout.push(d));
    proc.stderr.on('data', (d) => chunks.stderr.push(d));
    proc.on('close', (code) => {
      clearTimeout(timer);
      resolve({
        status:  code ?? -1,
        stdout:  Buffer.concat(chunks.stdout),
        stderr:  Buffer.concat(chunks.stderr),
      });
    });
    proc.on('error', (err) => {
      clearTimeout(timer);
      resolve({ status: -1, stdout: Buffer.alloc(0), stderr: Buffer.from(err.message) });
    });
  });

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
 * Normaliza BER (longitudes indefinidas `30 80`) a DER re-serializando con forge.
 * crypto.createPrivateKey y otras APIs requieren DER estricto y rechazan BER con
 * "wrong tag". forge.asn1.fromDer(strict:false) tolera BER; toDer produce DER limpio.
 *
 * @param {Buffer} bin
 * @returns {Buffer|null}
 */
const berToDer = (bin) => {
  try {
    const a = forge.asn1.fromDer(forge.util.createBuffer(bin.toString('binary')), { strict: false });
    return Buffer.from(forge.asn1.toDer(a).getBytes(), 'binary');
  } catch (_) {
    return null;
  }
};

/**
 * Parsea y descifra un buffer DER de llave privada .key del SAT.
 *
 * @param {Buffer} keyBuf
 * @param {string} password
 * @returns {object} privateKey de node-forge
 */
const parseKey = async (keyBuf, password) => {
  const bin = Buffer.isBuffer(keyBuf) ? keyBuf : Buffer.from(keyBuf, 'base64');

  // ── Diagnóstico de formato ──────────────────────────────────────────────────
  const firstBytes = bin.slice(0, 4).toString('hex');
  logger.info(`[parseKey] Primeros 4 bytes: ${firstBytes} | tamaño: ${bin.length} bytes`);

  // ── Detección PEM ──────────────────────────────────────────────────────────
  // Si el archivo fue subido como PEM (comienza con -----BEGIN) en lugar de DER
  const preview = bin.toString('ascii', 0, 27).trim();
  if (preview.startsWith('-----BEGIN')) {
    logger.info('[parseKey] Detectado formato PEM — intentando parseo directo');
    try {
      // PEM no cifrado
      return forge.pki.privateKeyFromPem(bin.toString('ascii'));
    } catch (_) { /* puede ser PEM cifrado, continúa con openssl */ }
  }

  // Intento 1: openssl pkcs8 vía temp file — soporta BER (longitud indefinida)
  // que usan las llaves .key reales del SAT (hex inicia con 30 80).
  // openssl es más tolerante que forge/node-crypto con encodings no-estándar.
  // Se intenta primero sin -legacy y luego con -legacy (necesario en OpenSSL 3.x
  // para esquemas PBES1 como pbeWithMD5AndDES-CBC o pbeWithSHAAndRC2-CBC).
  // opensslAsync() es NO bloqueante — no congela el event loop de Node.js.
  const tmpFile = path.join(os.tmpdir(), `sat_key_${Date.now()}_${Math.random().toString(36).slice(2)}.der`);
  try {
    fs.writeFileSync(tmpFile, bin, { mode: 0o600 });

    const opensslVariants = [
      // Sin proveedor: funciona en OpenSSL 1.x y 3.x para PBES2
      ['pkcs8', '-inform', 'DER', '-in', tmpFile, '-passin', `pass:${password}`, '-nocrypt'],
      // OpenSSL 3.x: -legacy NO es un flag de pkcs8; el proveedor legacy se carga así
      // Necesario para PBES1 (pbeWithMD5AndDES-CBC, pbeWithSHA1And3DES, etc.) que usa el SAT
      ['pkcs8', '-inform', 'DER', '-in', tmpFile, '-passin', `pass:${password}`, '-nocrypt', '-provider', 'legacy', '-provider', 'default'],
      // pkey es más general que pkcs8 y acepta los mismos proveedores
      ['pkey', '-inform', 'DER', '-in', tmpFile, '-passin', `pass:${password}`, '-provider', 'legacy', '-provider', 'default'],
    ];
    for (const args of opensslVariants) {
      const label = args.filter(a => !a.startsWith('-') || a === args[0]).slice(0, 2).join(' ');
      const result = await opensslAsync(args);
      if (result.status === 0 && result.stdout?.length > 0) {
        logger.info(`[parseKey] openssl ${label}: OK`);
        return forge.pki.privateKeyFromPem(result.stdout.toString());
      }
      logger.warn(`[parseKey] openssl ${label} falló: ${result.stderr?.toString()?.trim()?.slice(0, 120)}`);
    }
  } catch (e1) {
    logger.warn(`[parseKey] openssl pkcs8: ${e1.message?.slice(0, 120)}`);
  } finally {
    try { fs.unlinkSync(tmpFile); } catch { /* ya fue borrado o nunca se creó */ }
  }

  // Intento 2: forge decryptPrivateKeyInfo con BER → DER normalizado.
  // forge.asn1.fromDer(strict:false) acepta BER pero decryptPrivateKeyInfo necesita
  // DER bien formado; re-serializamos para obtener longitudes definidas.
  try {
    const derBin   = berToDer(bin) ?? bin;
    const forgeBuf = forge.util.createBuffer(derBin.toString('binary'));
    const asn1     = forge.asn1.fromDer(forgeBuf, { strict: false });
    const keyInfo  = forge.pki.decryptPrivateKeyInfo(asn1, password);
    if (!keyInfo) throw new Error('Contraseña incorrecta');
    logger.info('[parseKey] PKCS#8 forge: OK');
    return forge.pki.privateKeyFromAsn1(keyInfo);
  } catch (e2) {
    logger.warn(`[parseKey] PKCS#8 forge: ${e2.message?.slice(0, 120)}`);
  }

  // Intento 3: crypto nativo vía OpenSSL (raw — falla en BER)
  try {
    const nativeKey = crypto.createPrivateKey({
      key: bin, format: 'der', type: 'pkcs8', passphrase: password,
    });
    logger.info('[parseKey] native crypto: OK');
    return forge.pki.privateKeyFromPem(nativeKey.export({ type: 'pkcs1', format: 'pem' }));
  } catch (e3) {
    logger.warn(`[parseKey] native crypto: ${e3.message?.slice(0, 120)}`);
  }

  // Intento 3b: native crypto con BER → DER normalizado.
  // Node arranca con --openssl-legacy-provider → crypto.createPrivateKey soporta PBES1.
  // El único motivo por el que fallaba antes era el BER indefinido ("wrong tag").
  try {
    const derBin = berToDer(bin);
    if (!derBin) throw new Error('berToDer falló');
    const nativeKey = crypto.createPrivateKey({
      key: derBin, format: 'der', type: 'pkcs8', passphrase: password,
    });
    logger.info('[parseKey] native crypto (BER→DER): OK');
    return forge.pki.privateKeyFromPem(nativeKey.export({ type: 'pkcs1', format: 'pem' }));
  } catch (e3b) {
    logger.warn(`[parseKey] native crypto (BER→DER): ${e3b.message?.slice(0, 120)}`);
  }

  // Intento 4: descifrado manual con parser BER recursivo.
  //
  // forge.asn1.fromDer(strict:false) NO soporta longitud indefinida BER (30 80).
  // Los archivos .key del SAT pueden venir en dos envolturas:
  //
  //   A) PKCS#7 ContentInfo (OID 1.2.840.113549.1.7.2 = pkcs7-signedData):
  //      SEQUENCE { OID, [0] { SignedData { version, digestAlgos, ContentInfo { OID, [0] { keyBytes } }, ... } } }
  //      Se navega recursivamente hasta extraer los bytes de la llave interior.
  //
  //   B) Formato SAT no-estándar (OID de cifrado directo en SEQUENCE exterior):
  //      SEQUENCE { OID, params_SEQUENCE, OCTET_STRING(encryptedKey) }
  //
  //   C) PKCS#8 EncryptedPrivateKeyInfo estándar:
  //      SEQUENCE { SEQUENCE { OID, params }, OCTET_STRING }
  //
  // Se soportan PBES1 y PKCS#12-PBE para los casos B/C.
  try {
    // ── Parser BER recursivo (soporta longitudes indefinidas) ─────────────────
    const readBerItem = (buf, startP) => {
      if (startP >= buf.length) return null;
      let p = startP;
      const tag = buf[p++];
      if (p >= buf.length) return null;
      const lb = buf[p++];
      if (lb === 0x80) {
        // Longitud indefinida: saltar sub-elementos hasta EOC (00 00)
        const valueStart = p;
        while (p < buf.length) {
          if (buf[p] === 0x00 && buf[p + 1] === 0x00) {
            return { tag, value: buf.slice(valueStart, p), totalEnd: p + 2 };
          }
          const sub = readBerItem(buf, p);
          if (!sub) break;
          p = sub.totalEnd;
        }
        return null;
      }
      let len = lb;
      if (lb & 0x80) { len = 0; let n = lb & 0x7f; while (n--) len = (len << 8) | buf[p++]; }
      return { tag, value: buf.slice(p, p + len), totalEnd: p + len };
    };

    const readBerChildren = (buf) => {
      const items = [];
      let p = 0;
      while (p < buf.length) {
        if (buf[p] === 0x00 && p + 1 < buf.length && buf[p + 1] === 0x00) break; // EOC
        const item = readBerItem(buf, p);
        if (!item) break;
        items.push(item);
        p = item.totalEnd;
      }
      return items;
    };

    // ── Parsear SEQUENCE exterior ──────────────────────────────────────────────
    const outer = readBerItem(bin, 0);
    if (!outer || (outer.tag & 0x1f) !== 0x10) {
      throw new Error(`BER: se esperaba SEQUENCE (0x30), recibido 0x${bin[0].toString(16)}`);
    }

    const outerCh = readBerChildren(outer.value);
    if (!outerCh.length) throw new Error('BER: SEQUENCE exterior vacío');

    const first = outerCh[0];
    let algOid, salt, iters, encData;

    if (first.tag === 0x06) {
      algOid = forge.asn1.derToOid(first.value.toString('binary'));
      logger.info(`[parseKey] BER hijo1: tag=0x06 OID=${algOid}`);

      if (algOid === '1.2.840.113549.1.7.2') {
        // ── Caso A: PKCS#7 ContentInfo / SignedData ──────────────────────────
        // Estructura: SEQUENCE { OID pkcs7-signedData, [0] { SignedData } }
        // SignedData: SEQUENCE { version, digestAlgorithms, ContentInfo { OID, [0] { keyBytes } }, ..., signerInfos }
        const a0Item = outerCh[1];
        if (!a0Item || a0Item.tag !== 0xa0) throw new Error('BER PKCS#7: falta [0] EXPLICIT tras OID pkcs7-signedData');

        const sdItem = readBerItem(a0Item.value, 0);
        if (!sdItem || (sdItem.tag & 0x1f) !== 0x10) throw new Error('BER PKCS#7: SignedData no es SEQUENCE');

        const sdCh = readBerChildren(sdItem.value);
        logger.info(`[parseKey] BER PKCS#7 SignedData hijos=${sdCh.length} tags=[${sdCh.map(c => '0x' + c.tag.toString(16)).join(',')}]`);

        // Buscar ContentInfo: primer SEQUENCE cuyo primer hijo sea un OID
        let innerKeyBuf = null;
        for (const child of sdCh) {
          if ((child.tag & 0x1f) !== 0x10) continue;
          const gc = readBerChildren(child.value);
          if (!gc.length || gc[0].tag !== 0x06) continue;

          const ciOid = forge.asn1.derToOid(gc[0].value.toString('binary'));
          logger.info(`[parseKey] BER PKCS#7 ContentInfo OID: ${ciOid}`);

          const contentEl = gc[1]; // [0] EXPLICIT o el propio contenido
          if (!contentEl) continue;

          if (ciOid === '1.2.840.113549.1.7.1') {
            // data: [0] { OCTET STRING { keyBytes } }
            // El OCTET STRING puede ser:
            //   0x04 — primitivo: el valor ES la llave directamente
            //   0x24 — construido BER: concatenar los OCTET STRINGs internos
            const wrapEl = contentEl.tag === 0xa0 ? readBerItem(contentEl.value, 0) : contentEl;
            logger.info(`[parseKey] BER PKCS#7 wrapEl: tag=0x${(wrapEl?.tag ?? 0).toString(16)} len=${wrapEl?.value?.length}`);
            if (wrapEl && wrapEl.tag === 0x04) {
              innerKeyBuf = wrapEl.value;
            } else if (wrapEl && wrapEl.tag === 0x24) {
              // Construido BER: concatenar los OCTET STRINGs internos
              const parts = readBerChildren(wrapEl.value);
              const octetParts = parts.filter(p => p.tag === 0x04 || p.tag === 0x24);
              innerKeyBuf = octetParts.length
                ? Buffer.concat(octetParts.map(p => p.value))
                : wrapEl.value;
            } else {
              innerKeyBuf = wrapEl ? wrapEl.value : contentEl.value;
            }
          } else {
            // encryptedData u otro: extraer el valor del [0] tal cual
            innerKeyBuf = contentEl.tag === 0xa0 ? contentEl.value : contentEl.value;
          }
          if (innerKeyBuf) break;
        }

        if (!innerKeyBuf) throw new Error('BER PKCS#7: no se pudo extraer inner content del SignedData');
        logger.info(`[parseKey] BER PKCS#7 inner: ${innerKeyBuf.length}B primeros4=${innerKeyBuf.slice(0, 4).toString('hex')}`);

        // Intentar descifrar el inner con openssl
        const tmpInner = path.join(os.tmpdir(), `sat_i_${Date.now()}_${Math.random().toString(36).slice(2)}.der`);
        try {
          fs.writeFileSync(tmpInner, innerKeyBuf, { mode: 0o600 });
          const variants = [
            ['pkcs8', '-inform', 'DER', '-in', tmpInner, '-passin', `pass:${password}`, '-nocrypt'],
            ['pkcs8', '-inform', 'DER', '-in', tmpInner, '-passin', `pass:${password}`, '-nocrypt', '-provider', 'legacy', '-provider', 'default'],
            ['pkey',  '-inform', 'DER', '-in', tmpInner, '-passin', `pass:${password}`, '-provider', 'legacy', '-provider', 'default'],
          ];
          for (const args of variants) {
            const r = await opensslAsync(args);
            if (r.status === 0 && r.stdout?.length > 0) {
              logger.info(`[parseKey] PKCS#7 inner openssl ${args[0]}: OK`);
              return forge.pki.privateKeyFromPem(r.stdout.toString());
            }
            logger.warn(`[parseKey] PKCS#7 inner openssl ${args[0]}: ${r.stderr?.toString()?.trim()?.slice(0, 120)}`);
          }
        } finally {
          try { fs.unlinkSync(tmpInner); } catch {}
        }

        // Último recurso: forge decryptPrivateKeyInfo sobre el inner
        try {
          const forgeBuf  = forge.util.createBuffer(innerKeyBuf.toString('binary'));
          const asn1Inner = forge.asn1.fromDer(forgeBuf, { strict: false });
          const keyInfo   = forge.pki.decryptPrivateKeyInfo(asn1Inner, password);
          if (!keyInfo) throw new Error('contraseña incorrecta');
          logger.info('[parseKey] PKCS#7 inner forge decryptPrivateKeyInfo: OK');
          return forge.pki.privateKeyFromAsn1(keyInfo);
        } catch (eInner) {
          logger.warn(`[parseKey] PKCS#7 inner forge: ${eInner.message?.slice(0, 120)}`);
        }

        // Intentar parsear innerKeyBuf directamente como llave no cifrada
        try {
          const iAsn1Nc = forge.asn1.fromDer(forge.util.createBuffer(innerKeyBuf.toString('binary')), { strict: false });
          try {
            const key = forge.pki.privateKeyFromAsn1(iAsn1Nc);
            logger.info('[parseKey] inner sin cifrar → PKCS#1: OK');
            return key;
          } catch (_) {}
          if (iAsn1Nc.value?.[2]?.value) {
            const rsa = forge.asn1.fromDer(forge.util.createBuffer(iAsn1Nc.value[2].value), { strict: false });
            const key = forge.pki.privateKeyFromAsn1(rsa);
            logger.info('[parseKey] inner sin cifrar → PKCS#8: OK');
            return key;
          }
        } catch (eNc) {
          logger.info(`[parseKey] inner sin cifrar: ${eNc.message?.slice(0, 80)}`);
        }

        // Parser manual PBES1/PKCS12 sobre el inner (forge no soporta PKCS#12-PBE)
        try {
          const iOuter = readBerItem(innerKeyBuf, 0);
          if (!iOuter || (iOuter.tag & 0x1f) !== 0x10) throw new Error('inner no es SEQUENCE');
          const iCh = readBerChildren(iOuter.value);
          logger.info(`[parseKey] inner iCh: ${iCh.length} hijos tags=[${iCh.map(c => '0x' + c.tag.toString(16)).join(',')}] iCh0len=${iCh[0]?.value?.length} iCh0val=${iCh[0]?.value?.slice(0,8)?.toString('hex')}`);
          if (iCh.length < 2) throw new Error('inner SEQUENCE con < 2 hijos');

          let iAlgOid, iSalt, iIters, iEncData;
          if (iCh[0].tag === 0x30) {
            // PKCS#8 EncryptedPrivateKeyInfo: SEQUENCE { AlgorithmIdentifier, OCTET STRING }
            const ai = readBerChildren(iCh[0].value);
            logger.info(`[parseKey] inner ai: ${ai.length} tags=[${ai.map(c => '0x' + c.tag.toString(16)).join(',')}] ai0val=${ai[0]?.value?.slice(0,10)?.toString('hex')}`);
            if (!ai.length || ai[0].tag !== 0x06) throw new Error(`inner AlgId inesperado: ai[0].tag=0x${(ai[0]?.tag ?? 0).toString(16)}`);
            iAlgOid = forge.asn1.derToOid(ai[0].value.toString('binary'));
            const prm = readBerChildren(ai[1].value);
            iSalt = prm[0].value; iIters = 0;
            for (let i = 0; i < prm[1].value.length; i++) iIters = (iIters << 8) | prm[1].value[i];
            iEncData = iCh[1].value;
          } else if (iCh[0].tag === 0x06) {
            // OID directo en SEQUENCE exterior (variante SAT)
            iAlgOid = forge.asn1.derToOid(iCh[0].value.toString('binary'));
            const prm = readBerChildren(iCh[1].value);
            iSalt = prm[0].value; iIters = 0;
            for (let i = 0; i < prm[1].value.length; i++) iIters = (iIters << 8) | prm[1].value[i];
            iEncData = iCh[2]?.value;
          } else {
            throw new Error(`inner primer hijo tag inesperado: 0x${iCh[0].tag.toString(16)}`);
          }

          logger.info(`[parseKey] inner manual: OID=${iAlgOid} iters=${iIters} encData=${iEncData?.length}B`);

          const PBES1_M = {
            '1.2.840.113549.1.5.3':  { hash: 'md5',  cipher: 'des-cbc', kLen: 8 },
            '1.2.840.113549.1.5.6':  { hash: 'md5',  cipher: 'rc2-cbc', kLen: 5 },
            '1.2.840.113549.1.5.10': { hash: 'sha1', cipher: 'des-cbc', kLen: 8 },
            '1.2.840.113549.1.5.11': { hash: 'sha1', cipher: 'rc2-cbc', kLen: 5 },
          };
          const PKCS12_M = {
            '1.2.840.113549.1.12.1.3': { cipher: 'des-ede3-cbc', kLen: 24, ivLen: 8 },
            '1.2.840.113549.1.12.1.4': { cipher: 'des-ede-cbc',  kLen: 16, ivLen: 8 },
          };

          let iDecrypted;
          if (PBES1_M[iAlgOid]) {
            const { hash, cipher, kLen } = PBES1_M[iAlgOid];
            let dk = Buffer.concat([Buffer.from(password, 'utf8'), iSalt]);
            for (let i = 0; i < iIters; i++) dk = crypto.createHash(hash).update(dk).digest();
            const dec = crypto.createDecipheriv(cipher, dk.slice(0, kLen), dk.slice(kLen, kLen + 8));
            iDecrypted = Buffer.concat([dec.update(iEncData), dec.final()]);
            logger.info(`[parseKey] inner PBES1/${hash}/${cipher}: OK`);
          } else if (PKCS12_M[iAlgOid]) {
            const { cipher, kLen, ivLen } = PKCS12_M[iAlgOid];
            const saltBin = iSalt.toString('binary');
            const kBuf  = Buffer.from(forge.pkcs12.generateKey(password, saltBin, 1, iIters, kLen,  forge.md.sha1.create()).getBytes(), 'binary');
            const ivBuf = Buffer.from(forge.pkcs12.generateKey(password, saltBin, 2, iIters, ivLen, forge.md.sha1.create()).getBytes(), 'binary');
            const dec = crypto.createDecipheriv(cipher, kBuf, ivBuf);
            iDecrypted = Buffer.concat([dec.update(iEncData), dec.final()]);
            logger.info(`[parseKey] inner PKCS12-PBE/${cipher}: OK`);
          } else {
            throw new Error(`inner OID no soportado: ${iAlgOid}`);
          }

          const iAsn1 = forge.asn1.fromDer(forge.util.createBuffer(iDecrypted.toString('binary')), { strict: false });
          try {
            const key = forge.pki.privateKeyFromAsn1(iAsn1);
            logger.info('[parseKey] inner manual → PKCS#1: OK');
            return key;
          } catch (_) {}
          const iPkcs8 = forge.asn1.fromDer(forge.util.createBuffer(iAsn1.value[2].value), { strict: false });
          const key = forge.pki.privateKeyFromAsn1(iPkcs8);
          logger.info('[parseKey] inner manual → PKCS#8: OK');
          return key;
        } catch (eManual) {
          logger.warn(`[parseKey] inner manual: ${eManual.message?.slice(0, 120)}`);
        }

        throw new Error('BER PKCS#7: no se pudo descifrar el inner key con ningún método');

      } else {
        // ── Caso B: OID de cifrado directo en SEQUENCE exterior ──────────────
        const second = outerCh[1];
        if (!second) throw new Error('BER: falta SEQUENCE de parámetros');
        logger.info(`[parseKey] BER hijo2: tag=0x${second.tag.toString(16)} len=${second.value.length} val=${second.value.slice(0, 20).toString('hex')}`);

        const params = readBerChildren(second.value);
        salt  = params[0].value;
        iters = 0;
        for (let i = 0; i < params[1].value.length; i++) iters = (iters << 8) | params[1].value[i];

        const third = outerCh[2];
        if (!third) throw new Error('BER: falta OCTET STRING de datos cifrados');
        logger.info(`[parseKey] BER hijo3: tag=0x${third.tag.toString(16)} len=${third.value.length}`);
        encData = third.value;
      }

    } else if (first.tag === 0x30) {
      // ── Caso C: PKCS#8 EncryptedPrivateKeyInfo estándar ───────────────────
      const ai     = readBerChildren(first.value);
      algOid       = forge.asn1.derToOid(ai[0].value.toString('binary'));
      const params = readBerChildren(ai[1].value);
      salt  = params[0].value;
      iters = 0;
      for (let i = 0; i < params[1].value.length; i++) iters = (iters << 8) | params[1].value[i];

      const second = outerCh[1];
      if (!second) throw new Error('BER: falta OCTET STRING de datos cifrados (std)');
      encData = second.value;

    } else {
      throw new Error(`BER: elemento inesperado en SEQUENCE exterior: tag=0x${first.tag.toString(16)}`);
    }

    // ── Descifrado PBES1 / PKCS#12 (casos B y C) ──────────────────────────────
    logger.info(`[parseKey] Manual BER: OID=${algOid} salt=${salt.toString('hex')} iters=${iters} encData=${encData.length}B`);

    // PBES1 (PKCS#5 §6.1): PBKDF1 — dk = H^n(password || salt)
    const PBES1 = {
      '1.2.840.113549.1.5.3':  { hash: 'md5',  cipher: 'des-cbc', kLen: 8 },
      '1.2.840.113549.1.5.6':  { hash: 'md5',  cipher: 'rc2-cbc', kLen: 5 },
      '1.2.840.113549.1.5.10': { hash: 'sha1', cipher: 'des-cbc', kLen: 8 },
      '1.2.840.113549.1.5.11': { hash: 'sha1', cipher: 'rc2-cbc', kLen: 5 },
    };

    // PKCS#12 PBE (RFC 7292 Apéndice B, derivación SHA1)
    const PKCS12 = {
      '1.2.840.113549.1.12.1.3': { cipher: 'des-ede3-cbc', kLen: 24, ivLen: 8 },
      '1.2.840.113549.1.12.1.4': { cipher: 'des-ede-cbc',  kLen: 16, ivLen: 8 },
    };

    let decrypted;

    if (PBES1[algOid]) {
      const { hash, cipher, kLen } = PBES1[algOid];
      if (iters < 1) throw new Error(`PBES1: iters inválido (${iters})`);
      if (salt.length < 1) throw new Error('PBES1: salt vacío');
      let dk = Buffer.concat([Buffer.from(password, 'utf8'), salt]);
      for (let i = 0; i < iters; i++) dk = crypto.createHash(hash).update(dk).digest();
      const decipher = crypto.createDecipheriv(cipher, dk.slice(0, kLen), dk.slice(kLen, kLen + 8));
      decrypted = Buffer.concat([decipher.update(encData), decipher.final()]);
      logger.info(`[parseKey] Manual BER PBES1/${hash}/${cipher}: OK — decrypt[0:10]=${decrypted.slice(0, 10).toString('hex')}`);

    } else if (PKCS12[algOid]) {
      const { cipher, kLen, ivLen } = PKCS12[algOid];
      const saltBin    = salt.toString('binary');
      const kForgeBuf  = forge.pkcs12.generateKey(password, saltBin, 1, iters, kLen,  forge.md.sha1.create());
      const ivForgeBuf = forge.pkcs12.generateKey(password, saltBin, 2, iters, ivLen, forge.md.sha1.create());
      const kBuf  = Buffer.from(kForgeBuf.getBytes(), 'binary');
      const ivBuf = Buffer.from(ivForgeBuf.getBytes(), 'binary');
      const decipher = crypto.createDecipheriv(cipher, kBuf, ivBuf);
      decrypted = Buffer.concat([decipher.update(encData), decipher.final()]);
      logger.info(`[parseKey] Manual BER PKCS12-PBE/${cipher}: OK`);

    } else {
      throw new Error(`OID de cifrado no soportado en fallback manual: ${algOid}`);
    }

    const innerAsn1 = forge.asn1.fromDer(
      forge.util.createBuffer(decrypted.toString('binary')), { strict: false },
    );

    // Intentar PKCS#1 RSAPrivateKey directamente
    try {
      const key = forge.pki.privateKeyFromAsn1(innerAsn1);
      logger.info('[parseKey] Manual BER → PKCS#1: OK');
      return key;
    } catch (_) { /* no es PKCS#1, intentar como PKCS#8 PrivateKeyInfo */ }

    // PKCS#8 PrivateKeyInfo: la llave RSA está dentro del OCTET STRING (índice 2)
    const pkcs8Inner = forge.asn1.fromDer(
      forge.util.createBuffer(innerAsn1.value[2].value), { strict: false },
    );
    const key = forge.pki.privateKeyFromAsn1(pkcs8Inner);
    logger.info('[parseKey] Manual BER → PKCS#8 inner: OK');
    return key;

  } catch (e4) {
    logger.error(`[parseKey] todos los intentos fallaron: ${e4.message?.slice(0, 200)}`);
    throw new Error('No se pudo parsear la llave privada: ' + e4.message);
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
    privateKey = await parseKey(keyBuffer, password);

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
    privateKey = await parseKey(keyBuffer, password);

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
