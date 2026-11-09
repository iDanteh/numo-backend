'use strict';

/**
 * stress-test/k6-otp-flow.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Prueba de carga de la consulta pública de Lealtad (sesión post-código, OTP,
 * límite mensual) contra una réplica LOCAL del servidor (ver docker-compose.stress.yml)
 * — NUNCA correr contra producción.
 *
 * Mide dos cosas a la vez, con dos escenarios en paralelo:
 *   1. `otp_flow`: el flujo real completo (solicitar código → confirmar →
 *      hasta 3 "ver de nuevo" por sesión), en rampa de usuarios concurrentes.
 *   2. `staff_background`: tráfico de fondo a 2 endpoints de staff (Bancos),
 *      representando "los demás usuarios" — la señal que importa es si SU
 *      latencia (staff_endpoint_ms) se degrada cuando el OTP está bajo carga.
 *
 * Requiere:
 *   - Backend corriendo con STRESS_TEST_SOLICITAR_MAX seteado (ver
 *     loyalty-public.routes.js) para no toparse con el límite real de 3/15min
 *     por IP — ese límite es correcto en producción, pero bloquearía esta
 *     prueba casi de inmediato si no se levanta acá.
 *   - TEST_AUTH_SECRET seteado en el backend (modo dev/test) para poder emitir
 *     el token de staff del tráfico de fondo.
 *   - Tarjetas de prueba ya sembradas (ver seed-stress-cards.js) con teléfonos
 *     PHONE_PREFIX + 0000..PHONE_POOL_SIZE-1.
 *
 * Correr: k6 run -e TEST_AUTH_KEY=<mismo valor que TEST_AUTH_SECRET> stress-test/k6-otp-flow.js
 */

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:3000';
const TEST_AUTH_KEY = __ENV.TEST_AUTH_KEY;
const PHONE_POOL_SIZE = Number(__ENV.PHONE_POOL_SIZE) || 500;
// 10 dígitos: PHONE_PREFIX (6) + índice (4) — debe coincidir con seed-stress-cards.js.
const PHONE_PREFIX = '555000';

const otpLatencia = new Trend('otp_flujo_completo_ms');
const otpFallos = new Counter('otp_fallos');
const staffLatencia = new Trend('staff_endpoint_ms');

export const options = {
  scenarios: {
    otp_flow: {
      executor: 'ramping-vus',
      exec: 'otpFlow',
      startVUs: 0,
      // 2026-11-10, recalibrado a pico realista (pedido explícito del usuario, no el estrés
      // extremo de 150 de la corrida anterior — esa sí reveló que el backend es un solo proceso
      // Node sin cluster, limitado a 1 vCPU para JS, pero no es el escenario que importa decidir
      // hoy): 30-40 consultas concurrentes, tipo campaña masiva coordinada.
      stages: [
        { duration: '20s', target: 35 },   // rampa rápida al pico
        { duration: '2m',  target: 35 },   // sostenido — el dato más representativo
        { duration: '20s', target: 0 },    // enfriamiento
      ],
    },
    staff_background: {
      executor: 'constant-vus',
      exec: 'staffBackground',
      vus: 6, // ~6 IPs de staff distintas vistas en el extracto real de logs/app.log analizado
      duration: '2m50s', // cubre toda la ventana de otp_flow (20s+2m+20s)
      startTime: '10s', // arranca después de que setup() ya tiene el token listo
    },
  },
  thresholds: {
    // Umbral del flujo nuevo — informativo, no es la señal principal.
    otp_flujo_completo_ms: ['p(95)<2000'],
    // LA señal que realmente importa: el staff NO debería notar nada aunque el
    // OTP esté a full. Si este umbral falla, el OTP SÍ está afectando a los
    // demás usuarios del servidor.
    staff_endpoint_ms: ['p(95)<1000'],
    http_req_failed: ['rate<0.05'],
  },
};

/**
 * Teléfono fijo por VU (NO por VU+iteración) — bug real encontrado 2026-11-10: con
 * `(__VU*997+__ITER) % POOL`, apenas el total de iteraciones de TODOS los VUs supera el tamaño
 * del pool (pasa rápido: ~1.1k iteraciones contra un pool de 500), dos (VU,ITER) distintos caen
 * en el MISMO teléfono. Si dos flujos quedan en vuelo a la vez sobre ese número, el `solicitarCodigo()`
 * del segundo pisa el hash del código del primero en la tarjeta, y el `confirmarCodigoConsulta()`
 * del primero falla — un ~47-50% de fallos que NO bajaba al reducir VUs (150→35), porque dependía
 * del throughput total acumulado contra el pool, no de la concurrencia instantánea. Un teléfono fijo
 * por VU es inmune a esto: las iteraciones de UN MISMO VU siempre son secuenciales entre sí (k6 lo
 * garantiza), nunca se superponen consigo mismas — alcanza con que #VUs <= PHONE_POOL_SIZE.
 */
function telefonoUnico() {
  const idx = __VU % PHONE_POOL_SIZE;
  return PHONE_PREFIX + String(idx).padStart(4, '0');
}

export function otpFlow() {
  const telefono = telefonoUnico();
  const t0 = Date.now();

  // Paso 1: solicitar código (codigoDev viaja en la respuesta fuera de
  // producción — ver loyalty-recovery.service.js#solicitarCodigo).
  const resSolicitar = http.post(
    `${BASE_URL}/api/loyalty/public/recuperacion/solicitar`,
    JSON.stringify({ telefono }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  const codigo = resSolicitar.json('codigoDev');
  if (!check(resSolicitar, { 'solicitar 200': (r) => r.status === 200 }) || !codigo) {
    otpFallos.add(1);
    sleep(1);
    return;
  }

  // Paso 2: confirmar código — emite la sesión de 3 vistas extra.
  const resConfirmar = http.post(
    `${BASE_URL}/api/loyalty/public/consulta/confirmar`,
    JSON.stringify({ telefono, codigo }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  const token = resConfirmar.json('token');
  if (!check(resConfirmar, { 'confirmar 200': (r) => r.status === 200 }) || !token) {
    otpFallos.add(1);
    sleep(1);
    return;
  }

  sleep(Math.random() * 2 + 1); // think-time antes de "salir y volver a mirar"

  // Paso 3: hasta 3 "ver de nuevo" vía la sesión (reingreso transparente, sin botón).
  for (let i = 0; i < 3; i++) {
    const resVer = http.post(
      `${BASE_URL}/api/loyalty/public/consulta/ver`,
      JSON.stringify({ telefono, token }),
      { headers: { 'Content-Type': 'application/json' } },
    );
    check(resVer, { 'ver 200': (r) => r.status === 200 });
    sleep(Math.random() * 3 + 1);
  }

  otpLatencia.add(Date.now() - t0);
}

export function setup() {
  if (!TEST_AUTH_KEY) {
    throw new Error('Falta -e TEST_AUTH_KEY=<TEST_AUTH_SECRET del backend> para el tráfico de fondo de staff.');
  }
  const res = http.post(
    `${BASE_URL}/api/dev/test-token`,
    JSON.stringify({ role: 'admin' }), // clave real en rbac.js (label "Administrador"), no 'administrador'
    { headers: { 'Content-Type': 'application/json', 'X-Test-Auth-Key': TEST_AUTH_KEY } },
  );
  if (res.status !== 200) {
    throw new Error(`No se pudo obtener el token de prueba de staff: ${res.status} ${res.body}`);
  }
  return { staffToken: res.json('token') };
}

// Endpoints de "acción real de página" (vista de Bancos/Cortes/Pólizas) — en el extracto real
// de logs/app.log aparecen 1 vez cada uno en 7 minutos, nunca en bucle. Se simulan con 5% de
// probabilidad por iteración, nunca como un GET fijo cada ciclo.
const ACCIONES_REALES = ['/api/banks/indicadores', '/api/banks/cortes/ledger-diario', '/api/polizas?page=1&limit=15'];

/**
 * Tráfico de fondo — recalibrado (2026-11-10) contra un extracto REAL de logs/app.log (no
 * inventado): el tráfico de staff hoy es ~0.9 req/seg pico, dominado por POLLING liviano
 * (`/api/notificaciones` 56%, `/api/sat/credenciales/avisos` 17% del total), con vistas reales de
 * página (Bancos/Cortes/Pólizas) apareciendo solo 1 vez cada una en 7 minutos — NO en bucle
 * constante como la primera versión de este escenario asumía. staff_endpoint_ms mide AMBAS cosas
 * mezcladas — la señal real es si el polling (lo que el usuario ve todo el tiempo, "¿la campana de
 * notificaciones tarda más?") se degrada cuando el OTP está a full.
 */
export function staffBackground(data) {
  const headers = { Authorization: `Bearer ${data.staffToken}` };

  let t0 = Date.now();
  let res = http.get(`${BASE_URL}/api/notificaciones`, { headers });
  check(res, { 'notificaciones 200': (r) => r.status === 200 });
  staffLatencia.add(Date.now() - t0);
  sleep(Math.random() * 5 + 3); // observado: ~3-8s entre polls de un mismo usuario

  t0 = Date.now();
  res = http.get(`${BASE_URL}/api/sat/credenciales/avisos`, { headers });
  check(res, { 'avisos 200': (r) => r.status === 200 });
  staffLatencia.add(Date.now() - t0);

  if (Math.random() < 0.05) {
    const ruta = ACCIONES_REALES[Math.floor(Math.random() * ACCIONES_REALES.length)];
    t0 = Date.now();
    res = http.get(`${BASE_URL}${ruta}`, { headers });
    check(res, { 'accion real 200': (r) => r.status === 200 });
    staffLatencia.add(Date.now() - t0);
  }

  sleep(Math.random() * 5 + 3);
}
