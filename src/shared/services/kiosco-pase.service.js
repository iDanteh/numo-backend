'use strict';

/**
 * shared/services/kiosco-pase.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Pase para entrar al panel del Kiosco de facturación desde "Espacios de NUMO".
 *
 * Es un JWT ES256 firmado con la llave PRIVADA de NUMO (KIOSCO_PASE_LLAVE_PRIVADA);
 * el kiosco solo tiene la pública, así que puede verificarlo pero no fabricarlo.
 * Vale 60 s y una sola vez (el kiosco recuerda su jti). La ruta que lo emite ya
 * revisó el permiso kiosco:admin: el pase solo prueba quién es la persona.
 *
 * Claims (contrato con el kiosco, ver KIOSCOFAC docs/API.md "Entrada desde NUMO"):
 *   iss  KIOSCO_PASE_EMISOR (uno por ambiente)   aud  'kiosco-admin'
 *   sub  auth0 sub — identifica a la persona aunque cambie su correo
 *   email, nombre
 *   mfa  true si Auth0 le pidió segundo factor al entrar a NUMO; si no, el kiosco
 *        pide su propio código TOTP
 *   jti, iat, exp
 */

const { randomUUID } = require('crypto');
const jwt            = require('jsonwebtoken');
const config         = require('../../config/env');

const AUDIENCIA = 'kiosco-admin';
const VIGENCIA_S = 60;
// Claim propio con los métodos de autenticación: Auth0 pone `amr` en el id token,
// no en el access token; una Action del tenant lo copia aquí (ver LEEME del cambio).
const AMR_CLAIM = 'https://cfdi-comparator/amr';

/** true si el access token de Auth0 dice que hubo segundo factor. */
function tuvoMfa(payload = {}) {
  const amr = [payload.amr, payload[AMR_CLAIM]].flat().filter(Boolean);
  return amr.includes('mfa');
}

/**
 * Emite el pase y la dirección a la que hay que llevar al navegador. El pase va
 * en el fragmento (#): el navegador no lo manda a ningún servidor ni queda en
 * sus registros, y el panel lo borra del historial en cuanto lo lee.
 *
 * @param {{ _id: string, nombre: string, email: string }} user  req.user
 * @param {object} payloadAuth0  req.auth?.payload (vacío con tokens de prueba)
 * @returns {{ url: string } | null}  null si el acceso al kiosco no está configurado
 */
function emitirPase(user, payloadAuth0) {
  const { llavePrivada, emisor, urlPanel } = config.kioscoPase;
  if (!llavePrivada || !urlPanel) return null;
  if (!user?._id || !user.email) throw Object.assign(new Error('Tu usuario de NUMO no tiene correo'), { status: 409 });

  const pase = jwt.sign(
    { email: user.email, nombre: user.nombre || user.email, mfa: tuvoMfa(payloadAuth0) },
    llavePrivada,
    { algorithm: 'ES256', issuer: emisor, audience: AUDIENCIA, subject: user._id, expiresIn: VIGENCIA_S, jwtid: randomUUID() },
  );
  return { url: `${urlPanel}/login#pase=${pase}` };
}

module.exports = { emitirPase, tuvoMfa, AUDIENCIA };
