'use strict';

// netpay-match.service.js — Fase C del matching Netpay↔BBVA (ver
// project_netpay_transacciones.md): agrupa transacciones Netpay por
// almacen+terminalID+día y busca, para cada grupo sin resolver todavía, el BankMovement
// de BBVA cuyo depósito se acerca al neto (monto - comisión) de ese grupo.
//
// netpay-matching-v2 (PR4, dead-code cleanup): `obtenerBandejaNetpay` (bandeja v1, EN VIVO
// contra Kore en cada request, candidate picker manual) fue ELIMINADA de este archivo —
// `GET /netpay/bandeja` ya no la llama desde PR3 (lee `NetpayMatch` directamente, ver
// erp.routes.js), y `netpay-evaluacion.service.js` reimplementa su propio orquestador
// (`evaluarRango`) sin depender de esta función. Sus tests fueron eliminados junto con ella
// (ver netpay-match.service.test.js). `_buscarCandidatosParaGrupo` (usada por
// netpay-resolver.service.js) y las funciones de agrupamiento siguen vivas abajo.
//
// A diferencia de caja-transferencia-match.service.js (Fase C de Transferencias entre
// cajas), acá NO se filtra por categoria "Depósito en efectivo" — un depósito de
// liquidación de terminal de tarjeta casi seguro trae otra categoría (transferencia
// bancaria del procesador a la cuenta BBVA), y no hay ninguna categoría confirmada
// todavía para este flujo. No se inventa/asume una — queda sin ese filtro hasta que el
// usuario confirme cuál es (viendo la bandeja funcionar con datos reales).

const BankMovement = require('../banks/BankMovement.model');
const { ERP_TOLERANCE } = require('../banks/bank.service');
const globalConfigService = require('../../../shared/services/global-config.service');

// Configuraciones Globales, sección `bancos`, clave NETPAY_DATE_WINDOW_DAYS — distinta de
// TRANSFERENCIAS_DATE_WINDOW_DAYS (ese matching es contra Depósito en efectivo, este es
// contra liquidaciones de terminal a BBVA; acoplarlas haría que ajustar uno afecte al otro
// sin querer). Mismo patrón EXACTO que _ventanaDias() en caja-transferencia-match.service.js.
const VENTANA_DEFAULT_DIAS = 2;

async function _ventanaDiasNetpay() {
  let valor;
  try {
    valor = await globalConfigService.getValue('bancos', 'NETPAY_DATE_WINDOW_DAYS');
  } catch (err) {
    if (err.message?.includes('No existe la configuración')) return VENTANA_DEFAULT_DIAS;
    throw err;
  }
  const dias = parseInt(valor, 10);
  return Number.isFinite(dias) && dias > 0 ? dias : VENTANA_DEFAULT_DIAS;
}

// Bucketiza el timestamp REAL de una transacción de Kore a su día calendario en hora de
// MÉXICO (offset fijo UTC-6, sin horario de verano desde 2022) — CORRECCIÓN 2026-09-22
// (mismo bug de fondo ya corregido en la ventana de consulta, ver
// netpay-transacciones.service.js#_medianocheMx/_finDiaMx): la versión anterior de esta
// función (llamada _diaUTC) truncaba a medianoche UTC SIN desplazar — cualquier
// transacción de las 18:00-23:59 hora MX (que cae en 00:00-05:59 UTC del día calendario
// SIGUIENTE) quedaba agrupada bajo el día equivocado. Verificado que no había ningún
// NetpayMatch confirmado en producción antes de este cambio (confirmado por el usuario
// 2026-09-22) — no hace falta migrar datos existentes.
//
// IMPORTANTE — NUNCA usar esta función sobre un `dia` que YA es un marcador bucketizado
// (ej. el que confirmarMatchNetpay/descartarMatchNetpay reciben de vuelta desde el
// frontend, que a su vez salió de acá mismo) — el desplazamiento de -6h lo correría un
// día para atrás por error. Para normalizar un marcador ya calculado, usar
// _normalizarMarcadorDia() en su lugar (ver netpay-match-confirm.service.js).
function _diaMx(fechaISO) {
  const instanteMx = new Date(new Date(fechaISO).getTime() - 6 * 60 * 60 * 1000);
  return new Date(Date.UTC(instanteMx.getUTCFullYear(), instanteMx.getUTCMonth(), instanteMx.getUTCDate()));
}

// Normaliza un marcador de día YA bucketizado (Date u ISO con T00:00:00.000Z, ej.
// grupo.dia que vuelve del frontend al confirmar/descartar) — trunca a medianoche UTC
// SIN desplazar, a diferencia de _diaMx(). Un marcador no tiene una "hora real" que
// convertir a MX, así que aplicarle el desplazamiento de _diaMx lo movería un día para
// atrás por error.
function _normalizarMarcadorDia(dia) {
  const d = new Date(dia);
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
}

function _claveGrupo(terminalID, diaMx) {
  return `${terminalID}|${diaMx.toISOString()}`;
}

// Agrupa transacciones por (almacen, terminalID, día MX) sumando monto/comisión — la
// unidad que se espera que corresponda a UN depósito general de la terminal (dato de los
// contadores, sin confirmar todavía con datos reales — este panel es justamente para
// probarlo).
function _agruparPorTerminalYDia(transacciones) {
  const grupos = new Map();
  for (const t of transacciones) {
    const diaMx = _diaMx(t.transactionDate);
    const clave = _claveGrupo(t.terminalID, diaMx);
    const g = grupos.get(clave) ?? {
      terminalID: t.terminalID, almacen: t.almacen, dia: diaMx,
      montoBruto: 0, comision: 0, cantidadTransacciones: 0,
    };
    g.montoBruto += t.amount ?? 0;
    g.comision += t.commission ?? 0;
    g.cantidadTransacciones += 1;
    grupos.set(clave, g);
  }
  return [...grupos.values()].map(g => ({ ...g, netoEsperado: g.montoBruto - g.comision }));
}

function _montosIguales(a, b) {
  return Math.abs((a ?? 0) - (b ?? 0)) <= ERP_TOLERANCE;
}

// netpay-matching-v2 (design.md "Lagging brand list"): Configuraciones Globales, sección
// `bancos`, clave NETPAY_MARCAS_DIFERIDAS (CSV, ej. "AMEX,DINERS") — mismo patrón EXACTO
// que _ventanaDiasNetpay(). Default ['AMEX'] cuando la config no está sembrada.
const MARCAS_DIFERIDAS_DEFAULT = ['AMEX'];

async function _marcasDiferidas() {
  let valor;
  try {
    valor = await globalConfigService.getValue('bancos', 'NETPAY_MARCAS_DIFERIDAS');
  } catch (err) {
    if (err.message?.includes('No existe la configuración')) return MARCAS_DIFERIDAS_DEFAULT;
    throw err;
  }
  const marcas = String(valor ?? '')
    .split(',')
    .map(s => s.trim().toUpperCase())
    .filter(Boolean);
  return marcas.length > 0 ? marcas : MARCAS_DIFERIDAS_DEFAULT;
}

// Bucket de una transacción: 'general' salvo que `cardTypeName` EMPIECE con una de las
// marcas diferidas configuradas — NUNCA `cardType` (proposal: "split each terminal-day by
// cardTypeName, not cardType"). "Empieza con" en vez de igualdad exacta: Kore no siempre
// manda el nombre de marca pelado (ej. "AMEX CREDITO"), y la lista de marcas diferidas es
// un prefijo estable configurado por el usuario.
function _bucketDeTransaccion(cardTypeName, marcasDiferidas) {
  const marca = String(cardTypeName ?? '').trim().toUpperCase();
  if (!marca) return 'general';
  const encontrada = marcasDiferidas.find(m => marca.startsWith(m));
  return encontrada ?? 'general';
}

// Agrupa por (terminalID, día MX, bucket) — la unidad de decisión de netpay-matching-v2
// (ver design.md "Technical Approach"). A diferencia de _agruparPorTerminalYDia (que
// nunca separa por marca), acá una venta de una marca diferida (ej. AMEX) queda en SU
// PROPIO bucket, separado del resto del día — para que una venta AMEX pendiente no
// bloquee la confirmación automática del resto. `marcasDiferidas` se recibe YA resuelto
// (ver _marcasDiferidas) para que el caller la lea de Configuraciones Globales UNA sola
// vez para toda la corrida, no una vez por transacción.
function _agruparPorTerminalDiaYMarca(transacciones, marcasDiferidas) {
  const grupos = new Map();
  for (const t of transacciones) {
    const diaMx = _diaMx(t.transactionDate);
    const bucket = _bucketDeTransaccion(t.cardTypeName, marcasDiferidas);
    const clave = `${t.terminalID}|${diaMx.toISOString()}|${bucket}`;
    const g = grupos.get(clave) ?? {
      terminalID: t.terminalID, almacen: t.almacen, dia: diaMx, bucket,
      montoBruto: 0, comision: 0, cantidadTransacciones: 0, folios: [],
    };
    g.montoBruto += t.amount ?? 0;
    g.comision += t.commission ?? 0;
    g.cantidadTransacciones += 1;
    // Detalle por transacción (orderID/folio crudos de Kore, sin remapear — mismo criterio
    // que netpay-transacciones.service.js) — netpay-evaluacion.service.js lo persiste en
    // NetpayMatch.snapshot.folios (design.md "Data Model"), y netpay-reporte.service.js lo
    // usa para saber si un reporte cubre TODOS los folios de un bucket
    // pendiente_por_marca/discrepancia antes de cerrarlo (design.md "(a) Report present").
    g.folios.push({
      orderId: t.orderID ?? null, referencia: t.folio ?? null, marca: t.cardTypeName ?? null,
      monto: t.amount ?? 0, comision: t.commission ?? 0,
    });
    grupos.set(clave, g);
  }
  return [...grupos.values()].map(g => ({ ...g, netoEsperado: g.montoBruto - g.comision }));
}

// Núcleo puro del filtrado (sin I/O): dado un `ventanaDias` YA resuelto, filtra `pool`
// (universo de BankMovement BBVA elegibles) contra la ventana/monto de un grupo puntual.
// Separado de _buscarCandidatosParaGrupo para que obtenerBandejaNetpay pueda leer
// Configuraciones Globales UNA sola vez para TODOS los grupos (mismo motivo que
// buscarCandidatosBatch en caja-transferencia-match.service.js) en vez de una vez por grupo.
function _filtrarCandidatosEnPool(grupo, pool, ventanaDias) {
  const msVentana = ventanaDias * 24 * 60 * 60 * 1000;
  const desde = new Date(grupo.dia.getTime() - msVentana);
  const hasta = new Date(grupo.dia.getTime() + msVentana);

  const coincidencias = pool.filter(m => {
    const fechaMov = new Date(m.fecha);
    return fechaMov >= desde && fechaMov <= hasta && _montosIguales(m.deposito, grupo.netoEsperado);
  });
  return coincidencias.map(m => [m]);
}

// Candidatos para UN grupo puntual — usada por confirmarMatchNetpay/descartarMatchNetpay
// (netpay-match-confirm.service.js) para re-validar server-side sin duplicar el criterio de
// búsqueda. A diferencia de obtenerBandejaNetpay (que arma un pool compartido para varios
// grupos), acá SÍ vale la pena una consulta propia — es un solo grupo, re-validado en el
// momento exacto de confirmar/descartar.
async function _buscarCandidatosParaGrupo(grupo) {
  const ventanaDias = await _ventanaDiasNetpay();
  const msVentana = ventanaDias * 24 * 60 * 60 * 1000;
  const desde = new Date(grupo.dia.getTime() - msVentana);
  const hasta = new Date(grupo.dia.getTime() + msVentana);

  const pool = await BankMovement.find({
    banco: 'BBVA', erpLinks: { $size: 0 }, status: { $ne: 'identificado' },
    fecha: { $gte: desde, $lte: hasta },
  }).lean();

  return _filtrarCandidatosEnPool(grupo, pool, ventanaDias);
}

module.exports = {
  _buscarCandidatosParaGrupo,
  _ventanaDiasNetpay,
  _agruparPorTerminalYDia,
  _agruparPorTerminalDiaYMarca,
  _marcasDiferidas,
  _diaMx,
  _normalizarMarcadorDia,
  _claveGrupo,
  _montosIguales,
  VENTANA_DEFAULT_DIAS,
  MARCAS_DIFERIDAS_DEFAULT,
};
