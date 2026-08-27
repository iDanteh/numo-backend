'use strict';

// kore-caja.service.js — funciones puras de comunicación con el sistema de
// caja de Kore (auth, sesión, saldo de cuentas, aplicar cobros). Extraído de
// erp.routes.js (donde vivían mezcladas con las rutas HTTP de CyC/sync/etc.)
// para que collection-request.service.js pueda importarlas directamente sin
// depender de un archivo de rutas. erp.routes.js sigue usando estas mismas
// funciones/constantes para sus propias rutas HTTP (`/cobros/*`), importándolas
// de aquí — no se duplicó nada.

const axios = require('axios');
const globalConfigService = require('../../../shared/services/global-config.service');

// Configuraciones Globales, sección `kore` (runtime/DB, ver
// shared/services/global-config.service.js — reemplaza KORE_AUTH_URL/KORE_SERVICIO/
// KORE_CAJA_URL/KORE_CAJA_BASE_URL del .env). AUTH_URL apunta a producción (auth real
// de usuarios); el resto de las URLs Kore apuntan al ambiente de pruebas — mismo
// criterio que ya tenía el .env, ahora por fila en Postgres en vez de por variable.
async function _authUrl()     { return globalConfigService.getValue('kore', 'AUTH_URL'); }
async function _servicio()    { return globalConfigService.getValue('kore', 'SERVICIO'); }
async function _cajaUrl()     { return globalConfigService.getValue('kore', 'CAJA_URL'); }
// Exportada (no con guión bajo) porque erp.routes.js también la necesita
// directo para sus propias rutas /cobros/conceptos y /cobros/anticipos/*.
async function obtenerCajaBaseUrl() { return globalConfigService.getValue('kore', 'CAJA_BASE_URL'); }
// Catálogos de bancos y formas de pago — antes exclusivos de erp.routes.js
// (GET /cobros/bancos, /formas-pago); se movieron acá 2026-07-28 para que
// collection-request.service.js pueda resolver BancoID al aplicar un cobro
// automático, con el MISMO criterio que ya usa el panel de cobros manual.
//
// Configuraciones Globales, sección `bancos`, clave FORMASPAGO_BASE_URL (runtime/DB,
// ver shared/services/global-config.service.js) — reemplaza a KORE_FORMASPAGO_BASE_URL
// del .env. Vive en `bancos` (no en `kore`) porque el catálogo de formas de pago se
// usa donde se aplican los cobros (decisión explícita del usuario 2026-08-25). Cada
// ambiente (local/staging/producción) tiene su propia fila en su propia Postgres con
// el valor que le corresponde — ver banks/scripts/seed-global-config-banks.js.
async function _formasPagoBaseUrl() {
  return globalConfigService.getValue('bancos', 'FORMASPAGO_BASE_URL');
}

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

// Obtiene SOLO el token de Kore para un usuario (por su Auth0 sub), sin exigir
// una sesión de caja abierta — a diferencia de obtenerSesionCaja, de abajo, que
// además valida que haya una caja activa (porque va a aplicar un cobro). Esta
// función es para acciones que solo necesitan autenticarse ante Kore sin tocar
// caja, como avisar el estatus de revisión contable de una solicitud (lo hace
// el usuario de cobranza/contabilidad que ejecuta la acción, que normalmente
// no tiene ninguna caja abierta — no es un cajero).
async function obtenerTokenKore(auth0Id) {
  try {
    const tokenRes = await axios.get(await _authUrl(), {
      params:  { id: auth0Id, servicio: await _servicio() },
      timeout: 10000,
    });
    if (tokenRes.data?.Codigo !== 200 || !tokenRes.data?.Data) {
      console.warn(`[obtenerTokenKore] Kore respondió 200 pero sin token válido para auth0Id=${auth0Id}:`, tokenRes.data);
      throw new KoreCajaError('No se pudo obtener el token de Kore. Verifica el acceso al sistema.');
    }
    const koreToken = tokenRes.data.Data;
    koreTokenCache.set(auth0Id, koreToken); // disponible para proxies de cobros
    return koreToken;
  } catch (err) {
    if (err instanceof KoreCajaError) throw err;
    // Distinguir "Kore respondió pero rechazó" (err.response presente — ej. 400/404
    // porque auth0Id no existe para Kore) de una falla real de red/timeout (sin
    // err.response) — antes ambos casos caían en el mismo mensaje genérico.
    if (err.response) {
      const { msg, koreBody } = _mensajeErrorKore(err, `Kore rechazó la solicitud de token (${err.response.status})`);
      console.warn(`[obtenerTokenKore] Kore rechazó token para auth0Id=${auth0Id} con ${err.response.status}:`, koreBody);
      throw new KoreCajaError(msg, err.response.status, koreBody);
    }
    console.error(`[obtenerTokenKore] Sin respuesta de KORE_AUTH_URL para auth0Id=${auth0Id}:`, err.message);
    throw new KoreCajaError(`Error al conectar con el servidor de autenticación de caja: ${err.message}`);
  }
}

// Resuelve token + sesión de caja activa en Kore para un usuario dado (por su
// Auth0 sub). Reusada por GET /cobros/sesion-caja (para el usuario logueado
// actual) y por collection-request.service.js al aplicar un cobro automático
// (ahí se usa el Auth0 sub del CAJERO que generó la solicitud, no el del
// usuario de cobranza/contabilidad que la está identificando — el cajero es
// quien realmente tiene una caja abierta en Kore).
async function obtenerSesionCaja(auth0Id) {
  const koreToken = await obtenerTokenKore(auth0Id);

  try {
    const sesionRes = await axios.get(await _cajaUrl(), {
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
    r = await axios.get(`${await obtenerCajaBaseUrl()}/cuentas`, {
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

// Catálogo de bancos de Kore — mismo mapeo que ya usaba GET /cobros/bancos en
// erp.routes.js (movido acá 2026-07-28 para compartirlo con
// collection-request.service.js). Filtra bancos inactivos, igual que antes.
async function listarBancos(koreToken) {
  const baseUrl = await _formasPagoBaseUrl();
  const r = await axios.get(`${baseUrl}/api/bancos`, {
    headers: { Authorization: `Bearer ${koreToken}` },
    timeout: 10000,
  });
  return (r.data?.Data ?? [])
    .filter(b => b.Activo !== false)
    .map(b => ({
      id:          b.ID,
      nombre:      b.Nombre       ?? '',
      claveBanco:  b.ClaveBanco   ?? '',
      descripcion: b.Descripcion  ?? '',
    }));
}

// Catálogo de formas de pago de Kore — mismo mapeo que ya usaba GET
// /formas-pago en erp.routes.js (movido acá por el mismo motivo que
// listarBancos). claveSAT sirve para replicar el mismo criterio que ya usa
// cobro-panel.component.ts (_mapFormaPago: requiereBanco = claveSAT === '03')
// al decidir si una forma de pago necesita BancoID.
async function listarFormasPago(koreToken) {
  const baseUrl = await _formasPagoBaseUrl();
  const r = await axios.get(`${baseUrl}/api/formasdepago`, {
    headers: { Authorization: `Bearer ${koreToken}` },
    timeout: 10000,
  });
  return (r.data?.Data ?? [])
    .filter(f => f.Estatus === true)
    .map(f => ({
      id:             f.ID,
      nombre:         f.Nombre,
      claveSAT:       f.ClaveSAT,
      esBancarizada:  f.EsBancarizada  ?? false,
      reqNombreBanco: f.ReqNombreBanco ?? false,
      // Objeto crudo completo (2026-08-27, diagnóstico) — Kore rechazó un cobro real
      // exigiendo "el campo extra que empieza con numo, en el catálogo de la forma
      // de pago" para Depósito en efectivo; el mapeo de arriba nunca capturó ese
      // dato. Se guarda el crudo para poder loguearlo/inspeccionarlo (ver
      // identificar() en collection-request.service.js) hasta confirmar el nombre
      // real del campo — quitar este passthrough una vez resuelto.
      raw: f,
    }));
}

// Kore puede rechazar la aplicación de un cobro con "hasta resolver las
// solicitudes generadas anteriormente" justo después de que Numo aprobó la
// solicitud vía revision-contable (200 "ya puede ser aplicado por un
// cajero") — para descartar que sea una condición de carrera (Kore procesa la
// aprobación de forma asíncrona internamente), se reintenta UNA vez tras una
// espera corta antes de darlo por bloqueo real/definitivo.
const CUENTA_BLOQUEADA_RETRY_DELAY_MS = 2000;

function _delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function _esCuentaBloqueadaPorSolicitudesAnteriores(koreBody) {
  const texto = `${koreBody?.Data ?? ''} ${koreBody?.Mensaje ?? ''}`.toLowerCase();
  return texto.includes('solicitudes generadas anteriormente');
}

async function _operacionConReintento(method, url, payload, koreToken, logLabel, logCuenta) {
  const intentar = () => axios({
    method, url, data: payload,
    headers: { Authorization: `Bearer ${koreToken}`, 'Content-Type': 'application/json' },
    timeout: 15000,
  });
  try {
    const r = await intentar();
    return r.data;
  } catch (axiosErr) {
    if (!axiosErr.response) throw axiosErr; // error de red/timeout — dejar que asyncHandler lo maneje
    const { msg, koreBody } = _mensajeErrorKore(axiosErr, `Error al registrar el cobro en caja (${axiosErr.response.status})`);
    if (_esCuentaBloqueadaPorSolicitudesAnteriores(koreBody)) {
      console.warn(`[${logLabel}] cuenta=${logCuenta} bloqueada por "solicitudes generadas anteriormente" — reintentando en ${CUENTA_BLOQUEADA_RETRY_DELAY_MS}ms por si es una condición de carrera con la aprobación recién hecha.`);
      await _delay(CUENTA_BLOQUEADA_RETRY_DELAY_MS);
      try {
        const r2 = await intentar();
        console.log(`[${logLabel}] reintento tras espera funcionó para cuenta=${logCuenta} — SÍ era condición de carrera.`);
        return r2.data;
      } catch (axiosErr2) {
        if (!axiosErr2.response) throw axiosErr2;
        const { msg: msg2, koreBody: koreBody2 } = _mensajeErrorKore(axiosErr2, `Error al registrar el cobro en caja (${axiosErr2.response.status})`);
        console.warn(`[${logLabel}] reintento también rechazado para cuenta=${logCuenta} — NO es condición de carrera, bloqueo real en Kore:`, JSON.stringify(koreBody2));
        throw new KoreCajaError(msg2, axiosErr2.response.status, koreBody2);
      }
    }
    console.warn(`[${logLabel}] Kore rechazó con ${axiosErr.response.status} — cuenta=${logCuenta}:`, JSON.stringify(koreBody));
    throw new KoreCajaError(msg, axiosErr.response.status, koreBody);
  }
}

// Aplica un cobro de 1 sola CxC (Modo 1). Ver POST /cobros/operacion/:sesionId
// para el uso como proxy directo desde el panel de cobros. El log se pone AQUÍ
// (no solo en la ruta HTTP) para que también se vea cuando lo llama
// collection-request.service.js directamente, sin pasar por el endpoint.
// NOTA: el flujo de solicitudes de cobro (identificar(), Modo 1) YA NO usa
// esta función — ver aplicarSolicitudOperacion más abajo. Esta se conserva
// tal cual solo para el proxy manual del panel de cobros, que no tiene un
// solicitudIdErp que mandar.
async function aplicarCobroOperacion(sesionId, koreToken, payload) {
  console.log('[aplicarCobroOperacion] payload →', JSON.stringify({
    sesionId, cuenta: payload.cuenta, concepto: payload.detalle?.concepto,
    formasPago: (payload.detalle?.DetalleFormaPago ?? []).map(f => ({ id: f.FormaPagoID, nombre: f.FormaPagoNombre, monto: f.Monto, bancoId: f.BancoID })),
  }));
  return _operacionConReintento(
    'post', `${await obtenerCajaBaseUrl()}/sesiones/${sesionId}/operaciones`,
    payload, koreToken, 'aplicarCobroOperacion', payload.cuenta,
  );
}

// Endpoint dedicado del flujo ERP-Kore (collection-requests) para aplicar el
// cobro de una solicitud YA aprobada — se llama DESPUÉS de que
// actualizarEstatusSolicitud (revision-contable, más abajo) confirmó APROBADO.
// Body `{ DatosAdicionalesPorFormaPago }`: un elemento por cada forma de pago
// de la solicitud, con su FormaPagoID y DOS datos por separado del movimiento
// identificado — "Aut" (folio interno de Numo) y "Numo"
// (numeroAutorizacion bancario real) — ninguno reemplaza al otro, ver
// collection-request.service.js#identificar (paso 5) para dónde se arman —
// NUNCA el payload de cobro completo (cuenta/detalle/formasPago): Kore ya tiene esos
// datos desde que ÉL creó la solicitud, y los aplica internamente. Confirmado
// con Kore real (pruebas en Insomnia): mandarle el payload de cobro
// (buildPayloadSingle/Multi) causaba 400 "hay solicitudes relacionadas
// pendientes y/o rechazados"; con este body, Kore aplica el cobro y la CxC
// queda cobrada en el ERP. Ambos placeholders de la URL son de la
// solicitud/sesión — NINGUNO es la cuenta o la CxC. `sesionId` es la sesión de
// caja del CAJERO solicitante (obtenerSesionCaja).
// 2026-08-14: cada elemento de datosAdicionalesPorFormaPago también trae
// `fecha_real_pago` (campo hermano de FormaPagoID/BancoID; renombrado por Kore
// a snake_case el 2026-08-20, antes `fechaRealPago`) — ver
// collection-request.service.js#identificar (paso 6) para dónde se arma; esta
// función no cambia, solo pasa el arreglo tal cual en el body.
// 2026-08-20 (mismo día, corrección real): Kore además EXIGE `fecha_real_pago`
// A NIVEL RAÍZ del body ("obligatorio para solicitudes de Tipo=REVISION_CONTABLE",
// rechazo real confirmado por el usuario) — hermano de DatosAdicionalesPorFormaPago,
// no solo dentro de cada elemento. `fechaRealPagoRaiz` se calcula en
// collection-request.service.js (fecha del primer movimiento asignado).
async function aplicarSolicitudOperacion(sesionId, solicitudIdErp, koreToken, datosAdicionalesPorFormaPago, fechaRealPagoRaiz) {
  console.log('[aplicarSolicitudOperacion] payload →', JSON.stringify({ sesionId, solicitudIdErp, datosAdicionalesPorFormaPago, fechaRealPagoRaiz }));
  return _operacionConReintento(
    'put', `${await obtenerCajaBaseUrl()}/solicitud-operacion/${sesionId}/aplicar/${solicitudIdErp}`,
    { DatosAdicionalesPorFormaPago: datosAdicionalesPorFormaPago, fecha_real_pago: fechaRealPagoRaiz }, koreToken, 'aplicarSolicitudOperacion', solicitudIdErp,
  );
}

// Aplica un cobro de N CxC + 1 forma de pago (Modo 2). Ver
// POST /cobros/operacion-multiple/:sesionId para el uso como proxy directo
// desde el panel de cobros. NOTA: el flujo de solicitudes de cobro
// (identificar(), Modo 2) YA NO usa esta función — aplicarSolicitudOperacion
// cubre también Modo 2 con este mismo body simple. Esta se conserva tal cual
// solo para el proxy manual del panel de cobros, que no tiene un
// solicitudIdErp que mandar.
async function aplicarCobroOperacionMultiple(sesionId, koreToken, payload) {
  console.log('[aplicarCobroOperacionMultiple] payload →', JSON.stringify({
    sesionId, cuentas: payload.cuentas, concepto: payload.detalle?.concepto,
    formasPago: (payload.detalle?.DetalleFormaPago ?? []).map(f => ({ id: f.FormaPagoID, nombre: f.FormaPagoNombre, monto: f.Monto, bancoId: f.BancoID })),
  }));
  return _operacionConReintento(
    'post', `${await obtenerCajaBaseUrl()}/sesiones/${sesionId}/operacionesmultiples`,
    payload, koreToken, 'aplicarCobroOperacionMultiple', JSON.stringify(payload.cuentas),
  );
}

// Avisa a Kore el estatus de revisión contable de una solicitud de cobro
// (aprobada/rechazada en Numo) — endpoint distinto al de aplicar el cobro
// (aplicarSolicitudOperacion, arriba): este solo actualiza el estatus de la
// "solicitud-operacion" del lado de Kore, con un comentario. Se llama SIEMPRE
// primero — Kore exige esta aprobación antes de permitir aplicar el cobro (ver
// identificar() en collection-request.service.js). Se identifica por
// solicitudIdErp (el id que Kore usa para esa solicitud), no por el _id
// interno de Numo.
async function actualizarEstatusSolicitud(koreToken, solicitudIdErp, estatus, comentario) {
  try {
    const r = await axios.put(
      `${await obtenerCajaBaseUrl()}/solicitud-operacion/revision-contable/${solicitudIdErp}`,
      { Comentario: comentario, Estatus: estatus },
      { headers: { Authorization: `Bearer ${koreToken}`, 'Content-Type': 'application/json' }, timeout: 10000 },
    );
    return r.data;
  } catch (axiosErr) {
    if (!axiosErr.response) throw axiosErr; // error de red/timeout — dejar que asyncHandler lo maneje
    const { msg, koreBody } = _mensajeErrorKore(axiosErr, `Error al actualizar el estatus de la solicitud en Kore (${axiosErr.response.status})`);
    console.warn(`[actualizarEstatusSolicitud] Kore rechazó con ${axiosErr.response.status} — solicitudIdErp=${solicitudIdErp}, estatus=${estatus}:`, JSON.stringify(koreBody));
    throw new KoreCajaError(msg, axiosErr.response.status, koreBody);
  }
}

// Kore rechaza actualizarEstatusSolicitud con 400 "No puede cambiar el
// estatus de la solicitud con estatus: X" si la solicitud YA está en el
// estatus que se le pide poner — pasa en un reintento después de que un paso
// posterior (ej. aplicar el cobro) falló y dejó la solicitud sin persistir en
// Numo, aunque Kore ya la haya aprobado/rechazado la vez anterior. Detectarlo
// permite tratar el reintento como éxito en vez de trabar el flujo para
// siempre en este paso.
function esErrorYaEnEstatus(err, estatus) {
  if (!(err instanceof KoreCajaError)) return false;
  const texto = `${err.koreBody?.Data ?? ''} ${err.koreBody?.Mensaje ?? ''}`.toLowerCase();
  return texto.includes('no puede cambiar el estatus') && texto.includes(`con estatus: ${estatus}`.toLowerCase());
}

module.exports = {
  KoreCajaError,
  koreTokenCache,
  obtenerCajaBaseUrl,
  obtenerTokenKore,
  obtenerSesionCaja,
  obtenerCuentasKore,
  listarBancos,
  listarFormasPago,
  aplicarCobroOperacion,
  aplicarSolicitudOperacion,
  aplicarCobroOperacionMultiple,
  actualizarEstatusSolicitud,
  esErrorYaEnEstatus,
};
