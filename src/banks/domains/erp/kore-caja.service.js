'use strict';

// kore-caja.service.js — funciones puras de comunicación con el sistema de
// caja de Kore (auth, sesión, saldo de cuentas, aplicar cobros). Extraído de
// erp.routes.js (donde vivían mezcladas con las rutas HTTP de CyC/sync/etc.)
// para que collection-request.service.js pueda importarlas directamente sin
// depender de un archivo de rutas. erp.routes.js sigue usando estas mismas
// funciones/constantes para sus propias rutas HTTP (`/cobros/*`), importándolas
// de aquí — no se duplicó nada.

const axios = require('axios');

// KORE_AUTH_URL apunta a producción (auth real de usuarios).
// El resto de las URLs Kore apuntan al ambiente de pruebas.
const KORE_AUTH_URL      = (process.env.KORE_AUTH_URL      || 'https://app.login.tubosyconexiones.mx/logink/tokenKore');
const KORE_SERVICIO      = process.env.KORE_SERVICIO       || '6491faf156358100016565e5';
const KORE_CAJA_URL      = (process.env.KORE_CAJA_URL      || 'https://test.cajas.koreingenieria.com/index');
const KORE_CAJA_BASE_URL = (process.env.KORE_CAJA_BASE_URL || 'https://test.cajas.koreingenieria.com');

// Token Kore por usuario — se guarda cuando verifica sesión de caja, se usa en
// los proxies de cobros de erp.routes.js (getKoreToken). Compartido entre
// ambos archivos: erp.routes.js importa esta misma instancia del Map.
const koreTokenCache = new Map(); // auth0Id → koreToken

// Error tipado para cualquier falla al hablar con Kore (auth, sesión de caja,
// o al aplicar un cobro) — expone statusCode para que error-handler.js lo
// reenvíe tal cual (cae en su rama genérica AppError-like: usa err.statusCode).
class KoreCajaError extends Error {
  constructor(message, statusCode = 502, koreBody = null) {
    super(message);
    this.name = 'KoreCajaError';
    this.statusCode = statusCode;
    if (koreBody) this.koreBody = koreBody;
  }
}

function _mensajeErrorKore(axiosErr, fallback) {
  const koreBody = axiosErr.response?.data ?? {};
  const msg = (typeof koreBody.Data === 'string' ? koreBody.Data : null)
    || koreBody.Mensaje || koreBody.message || koreBody.error || fallback;
  return { msg, koreBody };
}

// Resuelve token + sesión de caja activa en Kore para un usuario dado (por su
// Auth0 sub). Reusada por GET /cobros/sesion-caja (para el usuario logueado
// actual) y por collection-request.service.js al aplicar un cobro automático
// (ahí se usa el Auth0 sub del CAJERO que generó la solicitud, no el del
// usuario de cobranza/contabilidad que la está identificando — el cajero es
// quien realmente tiene una caja abierta en Kore).
async function obtenerSesionCaja(auth0Id) {
  let koreToken;
  try {
    const tokenRes = await axios.get(KORE_AUTH_URL, {
      params:  { id: auth0Id, servicio: KORE_SERVICIO },
      timeout: 10000,
    });
    if (tokenRes.data?.Codigo !== 200 || !tokenRes.data?.Data) {
      console.warn(`[obtenerSesionCaja] Kore respondió 200 pero sin token válido para auth0Id=${auth0Id}:`, tokenRes.data);
      throw new KoreCajaError('No se pudo obtener el token de caja. Verifica el acceso al sistema de caja.');
    }
    koreToken = tokenRes.data.Data;
    koreTokenCache.set(auth0Id, koreToken); // disponible para proxies de cobros
  } catch (err) {
    if (err instanceof KoreCajaError) throw err;
    // Distinguir "Kore respondió pero rechazó" (err.response presente — ej. 400/404
    // porque auth0Id no existe para Kore) de una falla real de red/timeout (sin
    // err.response) — antes ambos casos caían en el mismo mensaje genérico.
    if (err.response) {
      const { msg, koreBody } = _mensajeErrorKore(err, `Kore rechazó la solicitud de token (${err.response.status})`);
      console.warn(`[obtenerSesionCaja] Kore rechazó token para auth0Id=${auth0Id} con ${err.response.status}:`, koreBody);
      throw new KoreCajaError(msg, err.response.status, koreBody);
    }
    console.error(`[obtenerSesionCaja] Sin respuesta de KORE_AUTH_URL para auth0Id=${auth0Id}:`, err.message);
    throw new KoreCajaError(`Error al conectar con el servidor de autenticación de caja: ${err.message}`);
  }

  try {
    const sesionRes = await axios.get(KORE_CAJA_URL, {
      headers: { Authorization: `Bearer ${koreToken}` },
      timeout: 10000,
    });
    if (sesionRes.data?.Codigo !== 200 || !sesionRes.data?.Data?.sesion?.Id) {
      console.warn(`[obtenerSesionCaja] Sin sesión de caja activa para auth0Id=${auth0Id}:`, sesionRes.data);
      throw new KoreCajaError('No se encontró sesión de caja activa para este usuario en Kore.');
    }
    return { sesionId: sesionRes.data.Data.sesion.Id, koreToken };
  } catch (err) {
    if (err instanceof KoreCajaError) throw err;
    if (err.response) {
      const { msg, koreBody } = _mensajeErrorKore(err, `Kore rechazó la consulta de sesión de caja (${err.response.status})`);
      console.warn(`[obtenerSesionCaja] Kore rechazó sesión para auth0Id=${auth0Id} con ${err.response.status}:`, koreBody);
      throw new KoreCajaError(msg, err.response.status, koreBody);
    }
    console.error(`[obtenerSesionCaja] Sin respuesta de KORE_CAJA_URL para auth0Id=${auth0Id}:`, err.message);
    throw new KoreCajaError(`Error al obtener la sesión de caja: ${err.message}`);
  }
}

// Consulta el detalle de una o varias CxC en Kore (saldo actual en vivo, incluyendo
// políticas de descuento por pronto pago). Reusada por GET /cobros/cuentas (panel de
// cobros) y por collection-request.service.js antes de aplicar un cobro automático,
// para calcular erpLinks[].saldoActual con el saldo real al momento de identificar
// (no el que Kore mandó al crear la solicitud, que puede estar desactualizado si
// hubo pagos parciales de por medio).
async function obtenerCuentasKore(koreToken, ids) {
  let r;
  try {
    r = await axios.get(`${KORE_CAJA_BASE_URL}/cuentas`, {
      params:  { ids },
      headers: { Authorization: `Bearer ${koreToken}` },
      timeout: 10000,
    });
  } catch (axiosErr) {
    if (!axiosErr.response) throw axiosErr;
    const body = axiosErr.response.data ?? {};
    const msg  = body.Mensaje || body.message || body.error
      || `Error al consultar cuentas (${axiosErr.response.status})`;
    throw new KoreCajaError(msg, axiosErr.response.status, body);
  }

  return (r.data?.Data?.cuentas ?? []).map(c => ({
    id:                   c.Id,
    serie:                c.Serie            ?? null,
    folio:                c.Folio            ?? null,
    tipoPago:             c.TipoPago         ?? null,
    total:                c.Total,
    saldoActual:          c.SaldoActual,
    saldoActualCalculado: c.SaldoActualCalculado ?? c.SaldoActual,
    descuentos: (c.Descuentos ?? []).map(d => ({
      idPolitica:     d.IDPolitica,
      dias:           d.Dias,
      porcentaje:     d.Porcentaje,
      monto:          d.Monto,
      iniciado:       d.Iniciado      ?? false,
      diasTolerancia: d.DiasTolerancia ?? 0,
    })),
  }));
}

// Aplica un cobro de 1 sola CxC (Modo 1). Ver POST /cobros/operacion/:sesionId
// para el uso como proxy directo desde el panel de cobros. El log se pone AQUÍ
// (no solo en la ruta HTTP) para que también se vea cuando lo llama
// collection-request.service.js directamente, sin pasar por el endpoint.
async function aplicarCobroOperacion(sesionId, koreToken, payload) {
  console.log('[aplicarCobroOperacion] payload →', JSON.stringify({
    sesionId, cuenta: payload.cuenta, concepto: payload.detalle?.concepto,
    formasPago: (payload.detalle?.DetalleFormaPago ?? []).map(f => ({ id: f.FormaPagoID, nombre: f.FormaPagoNombre, monto: f.Monto, bancoId: f.BancoID })),
  }));
  try {
    const r = await axios.post(
      `${KORE_CAJA_BASE_URL}/sesiones/${sesionId}/operaciones`,
      payload,
      { headers: { Authorization: `Bearer ${koreToken}`, 'Content-Type': 'application/json' }, timeout: 15000 },
    );
    return r.data;
  } catch (axiosErr) {
    if (!axiosErr.response) throw axiosErr; // error de red/timeout — dejar que asyncHandler lo maneje
    const { msg, koreBody } = _mensajeErrorKore(axiosErr, `Error al registrar el cobro en caja (${axiosErr.response.status})`);
    console.warn(`[aplicarCobroOperacion] Kore rechazó con ${axiosErr.response.status} — cuenta=${payload.cuenta}, concepto=${payload.detalle?.concepto}:`, JSON.stringify(koreBody));
    throw new KoreCajaError(msg, axiosErr.response.status, koreBody);
  }
}

// Aplica un cobro de N CxC + 1 forma de pago (Modo 2). Ver
// POST /cobros/operacion-multiple/:sesionId para el uso como proxy directo.
async function aplicarCobroOperacionMultiple(sesionId, koreToken, payload) {
  console.log('[aplicarCobroOperacionMultiple] payload →', JSON.stringify({
    sesionId, cuentas: payload.cuentas, concepto: payload.detalle?.concepto,
    formasPago: (payload.detalle?.DetalleFormaPago ?? []).map(f => ({ id: f.FormaPagoID, nombre: f.FormaPagoNombre, monto: f.Monto, bancoId: f.BancoID })),
  }));
  try {
    const r = await axios.post(
      `${KORE_CAJA_BASE_URL}/sesiones/${sesionId}/operacionesmultiples`,
      payload,
      { headers: { Authorization: `Bearer ${koreToken}`, 'Content-Type': 'application/json' }, timeout: 15000 },
    );
    return r.data;
  } catch (axiosErr) {
    if (!axiosErr.response) throw axiosErr;
    const { msg, koreBody } = _mensajeErrorKore(axiosErr, `Error al registrar el cobro múltiple en caja (${axiosErr.response.status})`);
    console.warn(`[aplicarCobroOperacionMultiple] Kore rechazó con ${axiosErr.response.status} — cuentas=${JSON.stringify(payload.cuentas)}, concepto=${payload.detalle?.concepto}:`, JSON.stringify(koreBody));
    throw new KoreCajaError(msg, axiosErr.response.status, koreBody);
  }
}

module.exports = {
  KoreCajaError,
  koreTokenCache,
  KORE_AUTH_URL,
  KORE_SERVICIO,
  KORE_CAJA_URL,
  KORE_CAJA_BASE_URL,
  obtenerSesionCaja,
  obtenerCuentasKore,
  aplicarCobroOperacion,
  aplicarCobroOperacionMultiple,
};
