'use strict';

const ExcelJS = require('exceljs');
const BankMovement = require('./BankMovement.model');
const CorteDescarga = require('./CorteDescarga.model');
const { _rangoAnioMesMexico, _inicioDiaMx, _finDiaMx } = require('./bank.service');
const globalConfigService = require('../../../shared/services/global-config.service');
const { BadRequestError, NotFoundError } = require('../../shared/errors/AppError');
const { subirReporteCorte, descargarReporteCorte } = require('./drive-cortes.service');
const { logger } = require('../../../shared/utils/logger');

// Importación deferida (mismo patrón que users/role.service.js#db y bank.routes.js
// #_postgresModels) para evitar dependencia circular durante el bootstrap.
function _postgresModels() {
  return require('../../../shared/models/postgres');
}

const MS_PER_HOUR = 3600000;

// Boundaries del $bucket de backlog: [0,24) < 24h, [24,72) 1-3d, [72,168) 3-7d, [168,∞) 7d+.
const BACKLOG_BOUNDARIES = [0, 24, 72, 168, Number.MAX_SAFE_INTEGER];

// Fecha de corte del dashboard completo (tiempo Y backlog): decisión explícita del usuario
// (2026-08-17) de medir SOLO desde que se implementa este indicador en adelante, para no
// ensuciar el promedio ni el backlog con historial viejo que nunca se pensó medir. Reemplaza
// al anterior split histórico/nuevo vía `backlogPreExistente` (ver BankMovement.model.js y
// scripts/migrate-backlog-preexistente.js, ahora sin uso). Medianoche en MÉXICO como instante
// UTC real (2026-09-09: antes usaba hora local del proceso, mismo bug de applyDateRange() más
// abajo — corregido con el mismo criterio de offset fijo -06:00). Si el deploy real de este
// cambio ocurre en otra fecha, ACTUALIZAR este valor a mano antes de desplegar.
const INDICADORES_DESDE = new Date(Date.UTC(2026, 7, 17, 6, 0, 0));

// Estatus que cuentan como "pendiente" para el backlog: no_identificado (nunca se tocó) y
// reclasificado (se identificó mal y quedó otra vez esperando revisión) — ambos son trabajo
// real todavía sin cerrar. "otros" queda afuera a propósito: es un estatus terminal, no un
// pendiente disfrazado.
const BACKLOG_STATUSES = ['no_identificado', 'reclasificado'];
const BACKLOG_KEY_BY_BOUNDARY = { 0: 'menos24h', 24: 'de1a3d', 72: 'de3a7d', 168: 'mas7d' };
const BACKLOG_DEFAULT = Object.freeze({ menos24h: 0, de1a3d: 0, de3a7d: 0, mas7d: 0 });

// Horario laboral usado para "horas hábiles" del promedio/mediana de identificación —
// actualizado 2026-09-24 a pedido del usuario: L-V 8:00-20:00, sábado 8:00-15:00 (antes el
// sábado tenía la misma ventana completa que L-V). Domingo completo = 0 horas hábiles sin
// importar el horario.
const HORA_INICIO_LABORAL        = 8;
const HORA_FIN_LABORAL           = 20;
const HORA_FIN_LABORAL_SABADO    = 15;
const DIA_DOMINGO                = 0; // Date#getDay()
const DIA_SABADO                 = 6; // Date#getDay()

/**
 * Filtro plano banco/categoria compartido por las 3 agregaciones. A diferencia de
 * getCards()/getStatusStats() (bank.service.js), que agrupan por banco+categoria para
 * armar el breakdown `porCategoria` de las cards, este endpoint no necesita ese desglose —
 * banco/categoria se aplican directo como campos del $match.
 *
 * `deposito: {$gt:0}` y `oculto: {$ne:true}` replican EXACTO el criterio de getCards()
 * (bank.service.js) — todo este dominio trata "identificado/no_identificado/reclasificado"
 * como estatus de DEPÓSITOS (el KPI de arriba se llama literalmente "Estatus de depósitos");
 * un retiro casi nunca se identifica y quedaba contando como backlog sin que nadie fuera a
 * actuar sobre él. Sin este filtro, el backlog mostraba miles de retiros mezclados con los
 * depósitos reales por identificar — un número técnicamente correcto pero inútil para
 * priorizar trabajo, y además inconsistente con el resto del dashboard.
 */
function buildBaseMatch({ banco, categoria } = {}) {
  const match = { isActive: true, oculto: { $ne: true }, deposito: { $gt: 0 } };
  if (banco) match.banco = banco;
  if (categoria) match.categoria = categoria;
  return match;
}

// Mismo criterio EXACTO de rango de fecha que getCards() (bank.service.js) — reusa el MISMO
// helper (_rangoAnioMesMexico, blindado 2026-09-09 contra el TZ del proceso) para no repetir
// la lógica dos veces y arriesgar que quede desactualizada en un solo lugar.
function applyDateRange(match, year, month) {
  if (!year) return match;
  match.fecha = _rangoAnioMesMexico(year, month);
  return match;
}

// Mapea el array {_id, count}[] que devuelve $bucket a las 4 llaves fijas del backlog.
// Mongo omite las llaves sin documentos — hay que default-earlas explícitamente a 0.
function mapBacklogBuckets(buckets) {
  const out = { ...BACKLOG_DEFAULT };
  for (const b of buckets) {
    const key = BACKLOG_KEY_BY_BOUNDARY[b._id];
    if (key) out[key] = b.count;
  }
  return out;
}

// Offset fijo de México (UTC-6, sin horario de verano desde 2022) — mismo supuesto que ya
// sostiene _rangoAnioMesMexico (bank.service.js). Envuelve un instante real en un Date corrido
// -6h de modo que sus métodos getUTC*/setUTC* devuelvan directamente el reloj de pared en
// México, sin importar el TZ del proceso (el contenedor de producción corre en UTC, sin `TZ`
// fijado). Como el corrimiento es constante, las DIFERENCIAS entre 2 instantes así envueltos
// siguen siendo la duración real — solo se usa para decidir "qué hora/día muestra el reloj",
// nunca para reportar un instante absoluto hacia afuera.
function _comoRelojMexico(fechaReal) {
  return new Date(fechaReal.getTime() - 6 * MS_PER_HOUR);
}

// "Hoy" en horario de México como string YYYY-MM-DD — insumo de _inicioDiaMx/_finDiaMx
// (bank.service.js, ya usados por exportMovements para sus propios filtros de fecha), que
// resuelven el offset fijo -06:00 a instantes UTC reales. Reusa _comoRelojMexico (mismo
// criterio que el resto de este archivo) para no depender del TZ del proceso (el
// contenedor de producción corre en UTC, sin TZ fijado).
function _hoyMexicoStr() {
  const mx = _comoRelojMexico(new Date());
  const yyyy = mx.getUTCFullYear();
  const mm   = String(mx.getUTCMonth() + 1).padStart(2, '0');
  const dd   = String(mx.getUTCDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

// Inicio del mes EN CURSO en México (medianoche del día 1), como instante UTC real — insumo
// de getCorteConciliacion(). Reusa _rangoAnioMesMexico (bank.service.js) para no repetir el
// cálculo de offset fijo -06:00; solo toma su `$gte` (el `$lt` no aplica, el corte es
// siempre "desde el inicio del periodo hasta AHORA", sin límite superior).
function _inicioMesMexico() {
  const mx = _comoRelojMexico(new Date());
  return _rangoAnioMesMexico(mx.getUTCFullYear(), mx.getUTCMonth() + 1).$gte;
}

// Inicio de la semana EN CURSO (lunes) en México, como instante UTC real. getUTCDay() de
// _comoRelojMexico ya devuelve el día de la semana correcto en hora de pared México (el
// corrimiento fijo de -6h nunca cruza un límite de día que cambie el resultado salvo en la
// franja 00:00-06:00 UTC, que es justamente la que _comoRelojMexico existe para corregir).
// Domingo (getUTCDay()===0) retrocede 6 días al lunes anterior, no 0.
function _inicioSemanaMexico() {
  const mx = _comoRelojMexico(new Date());
  const diaSemana = mx.getUTCDay(); // 0=domingo .. 6=sábado
  const diasDesdeLunes = diaSemana === 0 ? 6 : diaSemana - 1;
  const lunes = new Date(Date.UTC(mx.getUTCFullYear(), mx.getUTCMonth(), mx.getUTCDate() - diasDesdeLunes));
  const yyyy = lunes.getUTCFullYear();
  const mm   = String(lunes.getUTCMonth() + 1).padStart(2, '0');
  const dd   = String(lunes.getUTCDate()).padStart(2, '0');
  return _inicioDiaMx(`${yyyy}-${mm}-${dd}`);
}

/**
 * Horas hábiles entre 2 timestamps EN HORA DE MÉXICO (ver _comoRelojMexico — 2026-09-09,
 * antes usaba hora local del proceso, lo que rompía el cálculo si el contenedor corre en
 * UTC): lunes-viernes 8:00-20:00, sábado 8:00-15:00. Domingo completo y las horas fuera de
 * la ventana de cada día NO cuentan. Recorre día por día (acotado: la cantidad de días entre
 * inicio/fin de un caso real de identificación es chica, nunca miles) y suma el solape de
 * cada día con la ventana [inicio, fin] — así una franja que cruza varios días (ej. viernes
 * a la noche → lunes) se reparte bien entre los días que sí cuentan.
 *
 * No calculado en el pipeline de Mongo a propósito: esta lógica de calendario (saltar
 * domingos, recortar cada día a su ventana laboral) sería un `$reduce` de agregación
 * ilegible e imposible de testear con confianza — se resuelve en JS, sobre los pocos
 * cientos/miles de documentos que trae getIndicadoresIdentificacion() con find().lean().
 */
function horasHabilesEntre(inicio, fin) {
  if (!(fin > inicio)) return 0;
  let totalMs = 0;
  const inicioMx = _comoRelojMexico(inicio);
  const finMx    = _comoRelojMexico(fin);
  let cursorMx = new Date(Date.UTC(inicioMx.getUTCFullYear(), inicioMx.getUTCMonth(), inicioMx.getUTCDate()));
  while (cursorMx < finMx) {
    const diaSemana = cursorMx.getUTCDay();
    if (diaSemana !== DIA_DOMINGO) {
      const horaFin = diaSemana === DIA_SABADO ? HORA_FIN_LABORAL_SABADO : HORA_FIN_LABORAL;
      const ventanaInicio = new Date(cursorMx); ventanaInicio.setUTCHours(HORA_INICIO_LABORAL, 0, 0, 0);
      const ventanaFin    = new Date(cursorMx); ventanaFin.setUTCHours(horaFin, 0, 0, 0);
      const solapeInicio = ventanaInicio > inicioMx ? ventanaInicio : inicioMx;
      const solapeFin    = ventanaFin    < finMx   ? ventanaFin    : finMx;
      if (solapeFin > solapeInicio) totalMs += solapeFin.getTime() - solapeInicio.getTime();
    }
    cursorMx = new Date(Date.UTC(cursorMx.getUTCFullYear(), cursorMx.getUTCMonth(), cursorMx.getUTCDate() + 1));
  }
  return totalMs / MS_PER_HOUR;
}

// Reubicada acá 2026-09-17 (antes vivía solo en collection-request-indicadores.service.js,
// que la usaba para acotar Solicitudes de Cobro a uno/varios contadores elegidos por un
// admin) — el dashboard de Cobranza necesita EXACTAMENTE el mismo criterio para acotar
// getIndicadoresIdentificacion() a uno o varios integrantes del equipo, así que se centraliza
// acá (el archivo "base" que ya exporta promedio/mediana/horasHabilesEntre para ambos
// dominios) en vez de mantener 2 copias idénticas. Comportamiento sin cambios: escalar →
// filtro exacto; array → `$in`; array VACÍO → `undefined` (sin filtro, nunca `$in:[]`, que
// matchearía cero documentos).
function _matchScopeUserId(scopeUserId) {
  if (Array.isArray(scopeUserId)) {
    return scopeUserId.length ? { $in: scopeUserId } : undefined;
  }
  return scopeUserId;
}

// Tolerancia de "saldo restante bajo" — decisión explícita del usuario (2026-09-21): un
// movimiento cuya(s) CxC vinculada(s) ya quedaron con saldo restante <= 20% de su total
// cuenta como "identificado" para las MÉTRICAS de ESTE dashboard (promedio/mediana/porUsuario/
// backlog), aunque su `status` real siga en no_identificado/reclasificado — el depósito
// bancario en sí puede seguir sin cubrirse al 100% (ver aplicarLogicaErp, bank.service.js,
// que sigue intacto). Cero cambios en `status`, bloqueo de sobrepago, permisos, ni ningún
// otro flujo — vive 100% acá, solo para lo que se reporta en pantalla/Excel de Cobranza.
const SALDO_RESTANTE_TOLERANCIA = 0.20;

// Suma saldoActual/total de TODAS las erpLinks del movimiento (decisión explícita del
// usuario: el 20% se evalúa sobre el conjunto de CxC vinculadas, no CxC por CxC) y decide si
// entra en la tolerancia de arriba. Si a cualquier link le falta `total` (dato incompleto —
// CxC vinculada sin snapshot completo) o no hay erpLinks, NO califica: no se adivina el %
// con datos parciales, se prefiere dejarlo fuera (mismo criterio que el resto de este ajuste).
function _calificaPorSaldoRestanteBajo(erpLinks) {
  if (!Array.isArray(erpLinks) || erpLinks.length === 0) return false;
  let sumaSaldoActual = 0;
  let sumaTotal = 0;
  for (const l of erpLinks) {
    if (l.total == null || l.total <= 0) return false;
    sumaSaldoActual += l.saldoActual ?? 0;
    sumaTotal += l.total;
  }
  return sumaSaldoActual / sumaTotal <= SALDO_RESTANTE_TOLERANCIA;
}

// Entre las entradas de identificadoPor de un movimiento, la más reciente por fechaId — es
// quien "cerró" el residual dentro de la tolerancia; su fecha se usa como proxy de
// identificación (mismo criterio que fechaAplicacion en bank.service.js#exportMovements,
// que también toma el MÁXIMO de fechas de actividad) y su usuario para porUsuario. Devuelve
// null si ninguna entrada tiene fechaId — decisión explícita del usuario: sin fecha real, el
// movimiento se deja FUERA del cálculo en vez de inventar una.
function _masRecienteIdentificadoPor(identificadoPor) {
  const conFecha = (identificadoPor || []).filter(e => e.fechaId);
  if (!conFecha.length) return null;
  return conFecha.reduce((a, b) => (new Date(b.fechaId) > new Date(a.fechaId) ? b : a));
}

// Mismo criterio de precedencia que primeraIdentificacionAtMatch en _resolverMatchTiempo
// (rango explícito > year sin acotar > default "hoy"), pero evaluado en JS contra una fecha
// ya conocida (proxyFecha) en vez de armar un filtro de Mongo — los candidatos de saldo-
// restante-bajo no tienen un campo real de fecha de identificación que Mongo pueda filtrar,
// se resuelven aparte con la misma fecha proxy usada para horasHabilesEntre.
function _proxyFechaEnRango({ fechaInicio, fechaFin, year }, proxyFecha) {
  if (fechaInicio && fechaFin) {
    return proxyFecha >= _inicioDiaMx(fechaInicio) && proxyFecha <= _finDiaMx(fechaFin);
  }
  if (year) return true; // mismo criterio que $ne:null: cualquier fecha vale, no se acota por día
  const hoy = _hoyMexicoStr();
  return proxyFecha >= _inicioDiaMx(hoy) && proxyFecha <= _finDiaMx(hoy);
}

// Equivalente en JS de _matchScopeUserId, pero para comparar un userId ya resuelto (no para
// armar un filtro de Mongo) — mismo criterio: sin scope = todos entran; array vacío = todos
// entran (nunca "nadie"); array con elementos = debe estar incluido; escalar = igualdad exacta.
function _enScopeUserId(scopeUserId, userId) {
  if (scopeUserId == null) return true;
  if (Array.isArray(scopeUserId)) return scopeUserId.length === 0 || scopeUserId.includes(userId);
  return scopeUserId === userId;
}

// Filtra+resuelve los candidatos crudos (find con status pendiente + erpLinks no vacío) a
// los que de verdad califican por saldo restante bajo, ya con su fecha/usuario proxy y ya
// acotados al mismo rango de fecha/scope que el resto del dashboard. Ver
// SALDO_RESTANTE_TOLERANCIA arriba para el porqué completo de este ajuste.
function _resolverCasiIdentificadosPorSaldoBajo(candidatos, { fechaInicio, fechaFin, year, scopeUserId }) {
  const resultado = [];
  for (const mov of candidatos) {
    if (!_calificaPorSaldoRestanteBajo(mov.erpLinks)) continue;
    const entry = _masRecienteIdentificadoPor(mov.identificadoPor);
    if (!entry) continue;
    const proxyFecha = new Date(entry.fechaId);
    if (!_proxyFechaEnRango({ fechaInicio, fechaFin, year }, proxyFecha)) continue;
    if (!_enScopeUserId(scopeUserId, entry.userId)) continue;
    resultado.push({ _id: mov._id, createdAt: mov.createdAt, proxyFecha, userId: entry.userId ?? null, nombre: entry.nombre ?? null });
  }
  return resultado;
}

function promedio(valores) {
  if (!valores.length) return null;
  return valores.reduce((a, b) => a + b, 0) / valores.length;
}

// Mediana: menos sensible que el promedio a outliers (ej. un puñado de movimientos que
// tardaron semanas por vacaciones/un banco raro) — da una lectura más honesta de "cuánto
// tarda normalmente el equipo" que un promedio que esos casos pueden inflar solos.
function mediana(valores) {
  if (!valores.length) return null;
  const ordenados = [...valores].sort((a, b) => a - b);
  const mid = Math.floor(ordenados.length / 2);
  return ordenados.length % 2 === 0
    ? (ordenados[mid - 1] + ordenados[mid]) / 2
    : ordenados[mid];
}

/**
 * Arma el $match completo de "tiempo/porUsuario" (find() sobre BankMovement) — extraído
 * 2026-09-18 para que getIndicadoresIdentificacion() Y el reporte descargable
 * (buildReporteIdentificacion()) usen EXACTAMENTE el mismo criterio de filtro. Cero margen
 * de que la pantalla y el Excel descargado diverjan.
 *
 * @param {object} [opts]
 * @param {string} [opts.banco]
 * @param {string} [opts.categoria]
 * @param {string|number} [opts.year]  - filtra por `fecha` (la fecha de la transacción
 *   bancaria) vía `_rangoAnioMesMexico`. Ignorado por completo si viene `fechaInicio`+
 *   `fechaFin` (ver abajo) — el rango explícito SIEMPRE gana.
 * @param {string|number} [opts.month]
 * @param {string} [opts.fechaInicio] `YYYY-MM-DD` (hora de México) — junto con `fechaFin`,
 *   acota `primeraIdentificacionAt` a ese rango de DÍAS completos (inicio/fin de día en
 *   México, vía `_inicioDiaMx`/`_finDiaMx`). Mismo criterio de precedencia que
 *   `rangoFechas()` del panel hermano (Solicitudes de Cobro): un rango explícito gana
 *   sobre `year`/`month` y sobre el default "hoy" — se ignora `year`/`month` por completo
 *   en ese caso (ni `matchConFecha` se acota por `fecha`).
 * @param {string} [opts.fechaFin] Ver `fechaInicio`. Ambos deben venir juntos para activar
 *   el rango explícito — si falta uno de los dos, se ignoran los dos y se cae al
 *   comportamiento de `year`/`month`/default-hoy de siempre.
 *
 *   **Sin rango explícito y sin `year` (default, 2026-09-18)**: el indicador NO promedia
 *   todo el histórico desde INDICADORES_DESDE sin tope — se acota a HOY en horario de
 *   México, anclado por `primeraIdentificacionAt` (identificados HOY), NO por
 *   `fecha`/`createdAt` (creados hoy). Motivo: un depósito creado hoy normalmente todavía
 *   no se identificó (tarda horas/días), así que anclar por creación dejaría el indicador
 *   casi vacío la mayor parte del día y sesgaría el promedio hacia los casos más rápidos
 *   (survivorship bias). Anclar por identificación da "cuánto tardaron los que el equipo
 *   cerró hoy, sin importar cuándo llegaron" — la foto diaria correcta. Ver `_hoyMexicoStr()`.
 *
 *   El backlog NO se acota por year/month, rango explícito, NI por el default de "hoy" —
 *   sigue siendo siempre la antigüedad de TODOS los pendientes actuales: la antigüedad de
 *   un pendiente se mide contra AHORA, no contra un periodo pasado (y sigue en tiempo de
 *   RELOJ, no horas hábiles — ver Pipeline 2 en getIndicadoresIdentificacion). El backlog
 *   cuenta status no_identificado + reclasificado (BACKLOG_STATUSES) — ambos son trabajo
 *   real sin cerrar; "otros" queda afuera por ser un estatus terminal.
 *   Tanto el promedio como el backlog están acotados además a `createdAt >= INDICADORES_DESDE`
 *   (ver constante arriba) — el dashboard completo mide solo desde su propia implementación.
 * @param {string|string[]} [opts.scopeUserId] Dashboard de Cobranza (2026-09-17): acota
 *   promedio/mediana/porUsuario a quien IDENTIFICÓ el movimiento
 *   (`primeraIdentificacionPor.userId`) — escalar (ej. un no-admin viendo solo lo suyo) o
 *   array (admin acotando a uno o varios integrantes del equipo elegidos a mano). Mismo
 *   criterio que `_matchScopeUserId` (ver arriba): un array VACÍO se trata como "sin
 *   filtro" (equipo completo), nunca como `$in:[]`.
 */
function _resolverMatchTiempo({ banco, categoria, year, month, fechaInicio, fechaFin, scopeUserId } = {}) {
  const rangoExplicito = !!(fechaInicio && fechaFin);

  // Rango explícito gana por completo: ni year/month se aplican a `fecha`, ni el default
  // "hoy" entra en juego — mismo criterio de precedencia que rangoFechas() del hermano.
  const matchConFecha = rangoExplicito
    ? buildBaseMatch({ banco, categoria })
    : applyDateRange(buildBaseMatch({ banco, categoria }), year, month);

  let primeraIdentificacionAtMatch;
  if (rangoExplicito) {
    primeraIdentificacionAtMatch = { $ne: null, $gte: _inicioDiaMx(fechaInicio), $lte: _finDiaMx(fechaFin) };
  } else if (year) {
    primeraIdentificacionAtMatch = { $ne: null };
  } else {
    // Default "hoy" (2026-09-18, ver JSDoc arriba).
    primeraIdentificacionAtMatch = { $ne: null, $gte: _inicioDiaMx(_hoyMexicoStr()), $lte: _finDiaMx(_hoyMexicoStr()) };
  }

  const matchTiempo = {
    ...matchConFecha,
    status: 'identificado',
    primeraIdentificacionAt: primeraIdentificacionAtMatch,
    createdAt: { $gte: INDICADORES_DESDE },
  };
  // scopeUserId truthy pero resuelto a undefined (array vacío) no debe dejar la clave en el
  // match — mismo cuidado que collection-request-indicadores.service.js#getIndicadoresSolicitudesCobro.
  const scopedUserId = scopeUserId ? _matchScopeUserId(scopeUserId) : undefined;
  if (scopedUserId !== undefined) {
    matchTiempo['primeraIdentificacionPor.userId'] = scopedUserId;
  }
  return matchTiempo;
}

/**
 * Indicadores de tiempo de identificación de movimientos bancarios — cuánto tarda un
 * usuario en marcar un depósito como `identificado` desde que se cargó en Numo
 * (`primeraIdentificacionAt - createdAt`, en HORAS HÁBILES — ver horasHabilesEntre()).
 * Acotado a depósitos (deposito > 0, sin oculto), igual criterio que getCards() — ver
 * buildBaseMatch(). Ver `_resolverMatchTiempo()` para el detalle completo de filtros
 * (`year`/`month`/`fechaInicio`/`fechaFin`/`scopeUserId` y su precedencia).
 *
 * Desde 2026-09-21 (decisión explícita del usuario, SOLO para este dashboard): un
 * movimiento todavía `no_identificado`/`reclasificado` cuya(s) CxC vinculada(s) ya quedaron
 * con saldo restante <= 20% de su total también entra al promedio/mediana/porUsuario (con
 * la fecha del último `identificadoPor` como proxy de identificación) y SALE del backlog —
 * ver SALDO_RESTANTE_TOLERANCIA/`_resolverCasiIdentificadosPorSaldoBajo` arriba. El `status`
 * real del documento y `aplicarLogicaErp()` (bank.service.js) no se tocan.
 *
 * @param {object} [opts] Ver `_resolverMatchTiempo()`.
 */
async function getIndicadoresIdentificacion(opts = {}) {
  const { banco, categoria, fechaInicio, fechaFin, year, scopeUserId } = opts;
  const matchTiempo = _resolverMatchTiempo(opts);
  const matchSoloBancoCategoria = { ...buildBaseMatch({ banco, categoria }), createdAt: { $gte: INDICADORES_DESDE } };

  const [identificados, candidatosSaldoBajoRaw] = await Promise.all([
    // Trae los documentos ya identificados (equipo completo, desde INDICADORES_DESDE) para
    // calcular horas hábiles en JS — ver horasHabilesEntre() arriba sobre por qué esto no
    // se hace dentro de la agregación de Mongo. Con los volúmenes actuales (cientos/pocos
    // miles desde el cutoff) traer los documentos a Node es perfectamente razonable; si el
    // volumen creciera mucho con los años, valdría la pena revisar el enfoque (ej. mover el
    // cálculo a un job que lo materialice), pero no hace falta resolverlo ahora.
    BankMovement.find(matchTiempo)
      .select('createdAt primeraIdentificacionAt primeraIdentificacionPor')
      .lean(),
    // Candidatos crudos para la tolerancia de saldo restante bajo (ver arriba): mismo
    // universo del backlog (status pendiente, banco/categoria, cutoff) pero con erpLinks
    // real ('erpLinks.0' = idioma estándar de Mongo para "array no vacío") — se filtra/
    // resuelve el % y la fecha/scope proxy en JS, ver _resolverCasiIdentificadosPorSaldoBajo.
    BankMovement.find({
      ...matchSoloBancoCategoria,
      status: { $in: BACKLOG_STATUSES },
      'erpLinks.0': { $exists: true },
    }).select('createdAt erpLinks identificadoPor').lean(),
  ]);

  const casiIdentificados = _resolverCasiIdentificadosPorSaldoBajo(
    candidatosSaldoBajoRaw, { fechaInicio, fechaFin, year, scopeUserId },
  );
  const idsCasiIdentificados = casiIdentificados.map(c => c._id);

  // Pipeline 2 — backlog de pendientes (no_identificado + reclasificado, BACKLOG_STATUSES)
  // por antigüedad (sin year/month; equipo completo), desde INDICADORES_DESDE. Sigue en
  // tiempo de RELOJ a propósito — el usuario pidió horas hábiles para los promedios, no
  // para la antigüedad del backlog (decisión de alcance explícita, no un olvido). Excluye
  // los casi-identificados por saldo restante bajo — ya se cuentan como identificados arriba,
  // no deben aparecer TAMBIÉN como pendientes.
  const backlogAgg = await BankMovement.aggregate([
    { $match: { ...matchSoloBancoCategoria, status: { $in: BACKLOG_STATUSES }, _id: { $nin: idsCasiIdentificados } } },
    { $project: { horas: { $divide: [{ $subtract: ['$$NOW', '$createdAt'] }, MS_PER_HOUR] } } },
    { $bucket: { groupBy: '$horas', boundaries: BACKLOG_BOUNDARIES, default: 'otro', output: { count: { $sum: 1 } } } },
  ], { allowDiskUse: true });

  const conHoras = identificados.map(d => ({
    horas:  horasHabilesEntre(d.createdAt, d.primeraIdentificacionAt),
    userId: d.primeraIdentificacionPor?.userId ?? null,
    nombre: d.primeraIdentificacionPor?.nombre ?? null,
  }));
  for (const c of casiIdentificados) {
    conHoras.push({ horas: horasHabilesEntre(c.createdAt, c.proxyFecha), userId: c.userId, nombre: c.nombre });
  }
  const todasLasHoras = conHoras.map(d => d.horas);

  // Desglose por usuario — agrupado en JS sobre las mismas horas hábiles ya calculadas
  // (misma definición de "horas" que el promedio/mediana del equipo, para que sea
  // comparable). Solo promedio por usuario, sin mediana — la tabla ya es compacta.
  const porUsuarioMap = new Map();
  for (const d of conHoras) {
    const key = d.userId ?? '__sin_usuario__';
    if (!porUsuarioMap.has(key)) {
      porUsuarioMap.set(key, { userId: d.userId, nombre: d.nombre, horas: [] });
    }
    porUsuarioMap.get(key).horas.push(d.horas);
  }
  const porUsuario = [...porUsuarioMap.values()]
    .map(u => ({
      userId: u.userId,
      nombre: u.nombre,
      promedioHoras: promedio(u.horas),
      count: u.horas.length,
    }))
    .sort((a, b) => b.count - a.count);

  return {
    promedioHoras: promedio(todasLasHoras),
    medianaHoras:  mediana(todasLasHoras),
    totalIdentificadosConDato: todasLasHoras.length,
    backlog: mapBacklogBuckets(backlogAgg ?? []),
    porUsuario,
  };
}

/**
 * Reporte Excel descargable del dashboard de Cobranza (2026-09-18, pedido explícito del
 * usuario: "el rango de fechas que ya tiene Solicitudes de Cobro, para medir el día o días
 * que se deseen y poder descargar esta información") — mismos `opts` que
 * `getIndicadoresIdentificacion()` (reusa `_resolverMatchTiempo()`, así que el Excel
 * descargado SIEMPRE refleja exactamente lo mismo que está en pantalla, cero margen de
 * divergencia). Un solo worksheet (a diferencia del reporte de Solicitudes de Cobro, acá
 * TODO lo que entra ya es `status:'identificado'` — no hay "Autorizadas"/"Rechazadas" que
 * separar).
 *
 * GAP CONOCIDO (2026-09-21): a diferencia de `getIndicadoresIdentificacion()`, este reporte
 * TODAVÍA NO incluye los "casi identificados por saldo restante bajo" (ver
 * SALDO_RESTANTE_TOLERANCIA arriba) — solo trae `status:'identificado'` real. Se dejó fuera
 * a propósito en esta iteración (alcance explícito: solo el dashboard en pantalla); si se
 * pide extenderlo, agregar esas filas acá rompería la invariante de "un solo criterio de
 * filtro" de este JSDoc y requiere decidir cómo representarlas (no tienen `primeraIdentificacionAt`
 * real ni `folio` de la misma forma).
 */
async function buildReporteIdentificacion(opts = {}) {
  const matchTiempo = _resolverMatchTiempo(opts);

  const movimientos = await BankMovement.find(matchTiempo)
    .select('banco fecha concepto deposito categoria createdAt primeraIdentificacionAt primeraIdentificacionPor')
    .sort({ primeraIdentificacionAt: 1 })
    .lean();

  const wb = new ExcelJS.Workbook();
  wb.creator = 'Numo — Cobranza';
  wb.created = new Date();
  const sheet = wb.addWorksheet('Identificación');

  sheet.columns = [
    { header: 'Banco',                   key: 'banco',               width: 13 },
    { header: 'Fecha depósito',          key: 'fecha',               width: 18 },
    { header: 'Concepto',                key: 'concepto',            width: 45 },
    { header: 'Depósito',                key: 'deposito',            width: 15 },
    { header: 'Categoría',               key: 'categoria',           width: 18 },
    { header: 'Identificado por',        key: 'identificadoPor',     width: 22 },
    { header: 'Fecha de identificación', key: 'identificadoAt',      width: 18 },
    { header: 'Horas hábiles',           key: 'horasHabiles',        width: 14 },
    { header: 'Creado en Numo',          key: 'createdAt',           width: 18 },
  ];

  for (const m of movimientos) {
    sheet.addRow({
      banco:           m.banco ?? null,
      fecha:           m.fecha ?? null,
      concepto:        m.concepto ?? null,
      deposito:        m.deposito ?? null,
      categoria:       m.categoria ?? null,
      identificadoPor: m.primeraIdentificacionPor?.nombre ?? m.primeraIdentificacionPor?.userId ?? null,
      identificadoAt:  m.primeraIdentificacionAt ?? null,
      horasHabiles:    Math.round(horasHabilesEntre(m.createdAt, m.primeraIdentificacionAt) * 10) / 10,
      createdAt:       m.createdAt ?? null,
    });
  }

  const dateFmt = 'dd/mm/yyyy hh:mm';
  ['fecha', 'identificadoAt', 'createdAt'].forEach(key => { sheet.getColumn(key).numFmt = dateFmt; });
  sheet.getColumn('deposito').numFmt = '#,##0.00';

  const headerRow = sheet.getRow(1);
  headerRow.height = 22;
  headerRow.font   = { bold: true, color: { argb: 'FFE0E7FF' }, size: 10 };
  headerRow.fill   = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1E1B4B' } };
  headerRow.alignment = { vertical: 'middle', horizontal: 'center' };

  if (sheet.lastColumn) sheet.autoFilter = { from: 'A1', to: sheet.lastColumn.letter + '1' };
  sheet.views = [{ state: 'frozen', ySplit: 1 }];

  return wb.xlsx.writeBuffer();
}

// Contraparte de listContadoresConSolicitudesIdentificadas() (collection-request-indicadores
// .service.js) pero sobre BankMovement.primeraIdentificacionPor.userId — auth0Subs con
// actividad REAL de identificación (cualquier vía), para poblar un filtro de "elegí a quién
// ver" (dashboard de Cobranza) sin ofrecer usuarios que nunca identificaron nada.
// Deliberadamente SIN cruzar contra el rol actual del usuario (mismo criterio ya corregido
// una vez en el archivo hermano, 2026-09-07): alguien pudo identificar movimientos mientras
// tenía rol 'cobranza' y cambiar de rol después — filtrar por rol ACTUAL excluiría ese
// trabajo histórico real. El caller (frontend) decide cómo cruzar esta lista contra el
// catálogo de usuarios/roles para armar sus chips sugeridos.
async function listUsuariosConIdentificaciones() {
  const ids = await BankMovement.distinct('primeraIdentificacionPor.userId', {
    status: 'identificado',
    primeraIdentificacionPor: { $ne: null },
    createdAt: { $gte: INDICADORES_DESDE },
  });
  return ids.filter(Boolean);
}

/**
 * "Corte" de conciliación (2026-10-02, pedido explícito del usuario) — control periódico
 * para que cobranza (semanal) y contabilidad (mensual) vean, del periodo EN CURSO (lunes /
 * día 1 del mes corriente → ahora, sin navegación a periodos pasados — decisión explícita
 * del usuario, mismo espíritu que el default "hoy" de getIndicadoresIdentificacion):
 *
 *   1. `rezagados` — depósitos con `fecha` ANTERIOR al inicio del periodo que SIGUEN
 *      no_identificado/reclasificado AHORA MISMO (el número baja a medida que el equipo
 *      los identifica, sin esperar a que cierre el periodo).
 *   2. `nuevos` — depósitos con `fecha` DENTRO del periodo, por su estatus actual.
 *   3. `identificadosEnPeriodo` — depósitos identificados (`primeraIdentificacionAt`) DENTRO
 *      del periodo, separados según si el depósito en sí era rezago viejo o del lote nuevo —
 *      pedido explícito del usuario de no mezclar ambos en un solo número.
 *
 * Mismo criterio de "depósito" que el resto de este dashboard (buildBaseMatch: isActive,
 * no oculto, deposito>0) — un retiro no tiene este ciclo de vida. `rezagados`/`nuevos.total`/
 * `nuevos.pendientes`/`nuevos.otros` siguen SIN scope por usuario a propósito (mismo motivo de
 * siempre: "rezagado"/"nuevo"/"pendiente" son propiedades del movimiento, no de quién lo
 * identificó — es un control de EQUIPO completo, cuántos depósitos hay y cuántos faltan).
 *
 * **Scope por ROL para "identificado"** (2026-10-09, pedido explícito del usuario): `banco`
 * aparte, el corte es "de un rol" (cobranza ve semanal, contabilidad ve mensual — ver
 * `getPeriodoCortePorRol`), y antes de esto `nuevos.identificado` e `identificadosEnPeriodo`
 * mezclaban identificaciones de CUALQUIER rol — un corte de cobranza mostraba también lo que
 * identificó contabilidad, y viceversa. Con `rolEquipo` (el rol del usuario autenticado, o
 * `null` para `banks:config`/admin = sin acotar, ve el total real de todos los roles): estos
 * 2 números se acotan a auth0Subs con ese rol ACTUAL (mismo criterio/helper que
 * `banks:cobranza:all`, ver bank.routes.js#_cobranzaTeamAuth0Subs — "equipo HOY", no
 * historial). `nuevos.total`/`pendientes`/`otros` NO cambian: siguen siendo el total real del
 * equipo completo — la aritmética de los 4 buckets de `nuevos` puede dejar de sumar exacto al
 * total cuando hay scope (la diferencia es "identificado por otro rol", a propósito no se
 * expone como bucket nuevo, nadie lo pidió).
 *
 * **Corte histórico personalizado** (2026-10-06, pedido explícito del usuario): si vienen
 * `fechaInicio` Y `fechaFin` (`YYYY-MM-DD`, mismo nombre/convención que `_resolverMatchTiempo`
 * arriba), el corte se congela a esa fecha de cierre pasada en vez de "ahora". `BankMovement`
 * no tiene un log genérico de cambios de `status` (limitación real, discutida y aceptada con
 * el usuario) — se reconstruye el estatus "al cierre" con `primeraIdentificacionAt` (forward-
 * only, confiable) + `historialVinculacion[]` (ya trackea desvinculaciones con timestamp
 * exacto) + `ultimoCambioStatusAt` (marca mínima agregada para los 2 caminos que no dejan
 * rastro — `updateStatus()`/`reclasifyMovements()`, ver BankMovement.model.js). Lo que no se
 * puede reconstruir con certeza se EXPONE como `advertencia`, nunca se oculta. Ver
 * `_getCorteHistorico`/`_estadoAlCierre` abajo para el detalle completo.
 *
 * @param {object} [opts]
 * @param {'semanal'|'mensual'} [opts.periodo='semanal']
 * @param {string} [opts.banco]
 * @param {string} [opts.fechaInicio] `YYYY-MM-DD` — junto con `fechaFin`, activa el modo
 *   histórico. Si falta uno de los dos, se ignoran ambos y se usa el modo tiempo real de
 *   siempre (comportamiento IDÉNTICO, cero regresión).
 * @param {string} [opts.fechaFin] Ver `fechaInicio`.
 * @param {string|null} [opts.rolEquipo] Rol al que acotar "identificado" (ver arriba). `null`
 *   = sin acotar (admin/banks:config).
 */
// Valor que nunca matchea un auth0Sub real — mismo gotcha y misma solución que
// bank.routes.js#SIN_COINCIDENCIA: un array vacío en `$in` no es "no matchea nada", así que si
// el rol pedido no tiene NINGÚN usuario, hay que forzar un `$in` que de verdad no matchee en
// vez de dejar el array vacío (que algunas rutas de este archivo tratarían como "sin filtro").
const SIN_COINCIDENCIA_ROL_CORTE = '__sin-coincidencia-rol-corte__';

/**
 * "CxC retrasadas" del corte (2026-10-22, pedido explícito del usuario): entre las CxC
 * vinculadas a depósitos que SIGUEN sin identificarse (no_identificado/reclasificado, sin
 * importar si el depósito es rezago viejo o del lote nuevo del periodo), cuáles llevan un
 * folio de un mes calendario ANTERIOR al de referencia (hoy, en tiempo real; el cierre del
 * corte, en modo histórico) — "¿hay CxC que ya deberían estar resueltas hace 1+ mes y
 * siguen sin identificar?".
 *
 * Folio real de venta, formato AAMMNNNNNN (ej. "260901542" = año 2026, mes 09, consecutivo
 * 01542) — mismo formato que `_folioFechaCodificada` en erp-modal.component.ts (frontend,
 * buscador de CxC), portado acá. A diferencia del frontend, no valida contra una lista de
 * años disponibles (`erpAnios`, un catálogo de UI) — acá alcanza con el formato numérico y un
 * mes 1-12 válido.
 */
function _folioFechaCodificada(folioExterno) {
  const f = String(folioExterno ?? '').trim();
  if (!/^\d{8,}$/.test(f)) return null;
  const anio = 2000 + parseInt(f.slice(0, 2), 10);
  const mes  = parseInt(f.slice(2, 4), 10);
  if (mes < 1 || mes > 12) return null;
  return { anio, mes };
}

// Diferencia en MESES CALENDARIO (no días) entre 2 pares {anio, mes} — positivo cuando
// `referencia` es posterior a `folioFecha` (el caso "CxC vieja todavía sin resolver").
function _mesesDeAtraso(folioFecha, referencia) {
  return (referencia.anio - folioFecha.anio) * 12 + (referencia.mes - folioFecha.mes);
}

// {anio, mes} de un instante real, en calendario de MÉXICO (reusa _comoRelojMexico — mismo
// criterio que el resto de este archivo, nunca el TZ del proceso).
function _anioMesDeFecha(fechaReal) {
  const mx = _comoRelojMexico(fechaReal);
  return { anio: mx.getUTCFullYear(), mes: mx.getUTCMonth() + 1 };
}

// Equivalente a _hoyMexicoStr() pero devuelve el {anio,mes} de HOY en vez de un string — usado
// como referencia de "CxC retrasada" en el corte en tiempo real (ver getCorteConciliacion).
function _anioMesActualMexico() {
  return _anioMesDeFecha(new Date());
}

// Acotado a los 100 casos más atrasados (orden desc) — `count` siempre es el total real, nunca
// se miente; solo se trunca el DETALLE para no mandar una respuesta JSON gigante si el backlog
// es grande (mismo criterio preventivo de siempre: nunca ocultar el número, sí acotar el
// listado). Folios sin formato reconocible (_folioFechaCodificada → null: anticipos OPA,
// formatos viejos, etc.) se ignoran — no se adivina su antigüedad con datos parciales, mismo
// criterio que _calificaPorSaldoRestanteBajo más arriba.
const CXC_RETRASADAS_DETALLE_LIMITE = 100;

function _calcularCxcRetrasadas(movimientos, referencia) {
  const detalle = [];
  for (const mov of movimientos) {
    for (const link of mov.erpLinks || []) {
      const folioFecha = _folioFechaCodificada(link.folioExterno);
      if (!folioFecha) continue;
      const mesesAtraso = _mesesDeAtraso(folioFecha, referencia);
      if (mesesAtraso <= 0) continue;
      detalle.push({
        movimientoId: String(mov._id),
        banco: mov.banco ?? null,
        serie: link.serie ?? null,
        folioExterno: link.folioExterno,
        mesFolio: `${folioFecha.anio}-${String(folioFecha.mes).padStart(2, '0')}`,
        mesesAtraso,
        // Datos del DEPÓSITO donde esta CxC fue aplicada (2026-10-22, corrección pedida por
        // el usuario: la ficha depende del depósito, no la CxC de la ficha — mostrar acá la
        // ficha era la relación al revés). `folio` es el folio auto-incremental de Numo del
        // movimiento (ver BankMovement.model.js), no el folioExterno del ERP.
        folioNumo:     mov.folio ?? null,
        importe:       mov.deposito ?? null,
        fechaDeposito: mov.fecha ?? null,
      });
    }
  }
  detalle.sort((a, b) => b.mesesAtraso - a.mesesAtraso);
  return { count: detalle.length, detalle: detalle.slice(0, CXC_RETRASADAS_DETALLE_LIMITE) };
}

/**
 * auth0Subs con rol `rol` ACTUAL, o `null` si `rol` es falsy (sin scope — admin/banks:config).
 * Mismo criterio/patrón que bank.routes.js#_cobranzaTeamAuth0Subs: rol HOY, no historial (ver
 * comentario grande de getCorteConciliacion arriba).
 */
async function _equipoAuth0SubsParaRol(rol) {
  if (!rol) return null;
  const { User } = _postgresModels();
  const rows = await User.findAll({ where: { role: rol }, attributes: ['auth0Sub'], raw: true });
  const ids = rows.map((u) => u.auth0Sub ?? u.auth0_sub).filter(Boolean);
  return ids.length ? ids : [SIN_COINCIDENCIA_ROL_CORTE];
}

// `equipoIds` null = sin scope (todo cuenta). Para el `$match` de Mongo (agregaciones).
function _matchIdentificadoEquipo(equipoIds) {
  return equipoIds ? { 'primeraIdentificacionPor.userId': { $in: equipoIds } } : {};
}

// Para el loop en JS sobre documentos ya traídos (rama histórica / Excel detalle).
function _enEquipoRol(userId, equipoIds) {
  return !equipoIds || (!!userId && equipoIds.includes(userId));
}

async function getCorteConciliacion({ periodo = 'semanal', banco = null, fechaInicio = null, fechaFin = null, rolEquipo = null } = {}) {
  const baseMatch  = buildBaseMatch({ banco });
  const equipoIds  = await _equipoAuth0SubsParaRol(rolEquipo);
  const matchEquipo = _matchIdentificadoEquipo(equipoIds);

  if (fechaInicio && fechaFin) {
    const inicio = _inicioDiaMx(fechaInicio);
    const fin    = _finDiaMx(fechaFin);
    if (fin < inicio) {
      throw new BadRequestError('fechaFin no puede ser anterior a fechaInicio');
    }
    const { rezagados, nuevos, identificadosEnPeriodo, cxcRetrasadas, posibleCambioPosterior } =
      await _getCorteHistorico({ baseMatch, inicio, fin, equipoIds, matchEquipo });

    return {
      periodo, inicio, fin, historico: true,
      rezagados, nuevos, identificadosEnPeriodo, cxcRetrasadas,
      ...(posibleCambioPosterior > 0 ? { advertencia: { registrosConCambioPosteriorAlCierre: posibleCambioPosterior } } : {}),
    };
  }

  const inicio = periodo === 'mensual' ? _inicioMesMexico() : _inicioSemanaMexico();

  // `importeTotal` (2026-10-22, pedido explícito del usuario): suma de `deposito` junto a cada
  // conteo existente, para las mismas categorías que ya expone "Ver por estatus" — nunca
  // reemplaza al conteo, se agrega al lado.
  const agruparPorStatus = (extraMatch) => BankMovement.aggregate([
    { $match: { ...baseMatch, ...extraMatch } },
    { $group: { _id: '$status', count: { $sum: 1 }, importeTotal: { $sum: '$deposito' } } },
  ]).then(rows => ({
    counts:   Object.fromEntries(rows.map(r => [r._id, r.count])),
    importes: Object.fromEntries(rows.map(r => [r._id, r.importeTotal ?? 0])),
  }));

  const [rezagadosPorStatus, nuevosPorStatus, nuevosIdentificadoEquipo, identificadosPorOrigen, pendientesConErp] = await Promise.all([
    agruparPorStatus({ fecha: { $lt: inicio }, status: { $in: BACKLOG_STATUSES } }),
    // SIN scope a propósito: esta cuenta los 4 estatus juntos para sacar el total real y
    // 'otros' (ver abajo) — aplicarle matchEquipo acá excluiría de TODO el group-by a los
    // no_identificado/reclasificado (que no tienen primeraIdentificacionPor), no solo a los
    // identificados de otro rol.
    agruparPorStatus({ fecha: { $gte: inicio } }),
    // Conteo + suma de 'identificado' YA acotados al rol — lo que se expone en `nuevos`.
    BankMovement.aggregate([
      { $match: { ...baseMatch, ...matchEquipo, fecha: { $gte: inicio }, status: 'identificado' } },
      { $group: { _id: null, count: { $sum: 1 }, importeTotal: { $sum: '$deposito' } } },
    ]).then(rows => ({ count: rows[0]?.count ?? 0, importeTotal: rows[0]?.importeTotal ?? 0 })),
    BankMovement.aggregate([
      { $match: { ...baseMatch, ...matchEquipo, status: 'identificado', primeraIdentificacionAt: { $gte: inicio } } },
      { $group: { _id: { $cond: [{ $lt: ['$fecha', inicio] }, 'rezagado', 'nuevo'] }, count: { $sum: 1 }, importeTotal: { $sum: '$deposito' } } },
    ]).then(rows => ({
      counts:   Object.fromEntries(rows.map(r => [r._id, r.count])),
      importes: Object.fromEntries(rows.map(r => [r._id, r.importeTotal ?? 0])),
    })),
    // Pendientes (CUALQUIER fecha, rezago o nuevo) con al menos una CxC vinculada — insumo de
    // "CxC retrasadas" (ver _calcularCxcRetrasadas arriba). Sin scope por rol: mismo criterio
    // que rezagados/nuevos.pendientes, es una propiedad del movimiento, no de quién identifica.
    BankMovement.find({ ...baseMatch, status: { $in: BACKLOG_STATUSES }, 'erpLinks.0': { $exists: true } })
      .select('banco fecha folio deposito erpLinks')
      .lean(),
  ]);

  const cxcRetrasadas = _calcularCxcRetrasadas(pendientesConErp, _anioMesActualMexico());

  const rezagados = {
    no_identificado: rezagadosPorStatus.counts.no_identificado ?? 0,
    reclasificado:    rezagadosPorStatus.counts.reclasificado    ?? 0,
  };
  rezagados.total = rezagados.no_identificado + rezagados.reclasificado;
  rezagados.importeTotal = {
    no_identificado: rezagadosPorStatus.importes.no_identificado ?? 0,
    reclasificado:    rezagadosPorStatus.importes.reclasificado    ?? 0,
  };
  rezagados.importeTotal.total = rezagados.importeTotal.no_identificado + rezagados.importeTotal.reclasificado;

  // Total/otros se calculan con el identificado SIN scope (nuevosPorStatus.identificado) —
  // son propiedades del lote completo, no de "mi equipo". Ver comentario grande arriba: la
  // suma de los 4 buckets expuestos puede no coincidir exacto con `nuevos.total` cuando hay
  // scope (la diferencia es "identificado por otro rol", a propósito no se expone). Mismo
  // criterio se aplica a `nuevos.importeTotal`.
  const nuevosTotal = Object.values(nuevosPorStatus.counts).reduce((a, b) => a + b, 0);
  const nuevosImporteTotal = Object.values(nuevosPorStatus.importes).reduce((a, b) => a + b, 0);
  const nuevos = {
    no_identificado: nuevosPorStatus.counts.no_identificado ?? 0,
    reclasificado:    nuevosPorStatus.counts.reclasificado    ?? 0,
    identificado:     nuevosIdentificadoEquipo.count,
  };
  nuevos.otros = nuevosTotal - nuevos.no_identificado - nuevos.reclasificado - (nuevosPorStatus.counts.identificado ?? 0);
  nuevos.pendientes = nuevos.no_identificado + nuevos.reclasificado;
  nuevos.total = nuevosTotal;
  nuevos.importeTotal = {
    no_identificado: nuevosPorStatus.importes.no_identificado ?? 0,
    reclasificado:    nuevosPorStatus.importes.reclasificado    ?? 0,
    identificado:     nuevosIdentificadoEquipo.importeTotal,
  };
  nuevos.importeTotal.otros = nuevosImporteTotal - nuevos.importeTotal.no_identificado - nuevos.importeTotal.reclasificado - (nuevosPorStatus.importes.identificado ?? 0);
  nuevos.importeTotal.total = nuevosImporteTotal;

  const identificadosEnPeriodo = {
    deRezagados: identificadosPorOrigen.counts.rezagado ?? 0,
    deNuevos:    identificadosPorOrigen.counts.nuevo     ?? 0,
  };
  identificadosEnPeriodo.total = identificadosEnPeriodo.deRezagados + identificadosEnPeriodo.deNuevos;
  identificadosEnPeriodo.importeTotal = {
    deRezagados: identificadosPorOrigen.importes.rezagado ?? 0,
    deNuevos:    identificadosPorOrigen.importes.nuevo     ?? 0,
  };
  identificadosEnPeriodo.importeTotal.total = identificadosEnPeriodo.importeTotal.deRezagados + identificadosEnPeriodo.importeTotal.deNuevos;

  return { periodo, inicio, historico: false, rezagados, nuevos, identificadosEnPeriodo, cxcRetrasadas };
}

/**
 * Rama histórica de getCorteConciliacion() — NO usa aggregate para rezagados/nuevos (necesita
 * reconstrucción por documento vía _estadoAlCierre), sí sigue usando aggregate para
 * `identificadosEnPeriodo` (ese cálculo es 100% preciso siempre, con `primeraIdentificacionAt`
 * acotado `{ $gte: inicio, $lte: fin }` — la única diferencia con el modo live es agregar el
 * `$lte: fin`).
 *
 * `equipoIds`/`matchEquipo` (2026-10-09): mismo criterio que la rama live — `nuevos.identificado`
 * se acota al rol, `nuevos.total`/`pendientes`/`otros` se calculan con el identificado SIN
 * acotar (`identificadoTotalSinScope`), para no reducir el total real por scope.
 */
// Campos select() comunes a ambas queries de abajo — se agregaron banco/fecha/folio/deposito/
// erpLinks (2026-10-22) para poder calcular "CxC retrasadas" (ver _calcularCxcRetrasadas)
// reusando estos MISMOS documentos ya traídos, sin una query nueva. folio/deposito son del
// DEPÓSITO (no de la ficha — ver _calcularCxcRetrasadas para el porqué del cambio).
const _CORTE_HISTORICO_SELECT = 'banco fecha folio deposito status primeraIdentificacionAt primeraIdentificacionPor ultimoCambioStatusAt historialVinculacion erpLinks identificadoPor';

async function _getCorteHistorico({ baseMatch, inicio, fin, equipoIds = null, matchEquipo = {} }) {
  const [candidatosRezagados, candidatosNuevos, identificadosPorOrigen] = await Promise.all([
    BankMovement.find({
      ...baseMatch,
      fecha: { $lt: inicio },
      $or: [
        { status: { $in: BACKLOG_STATUSES } },
        { primeraIdentificacionAt: { $ne: null } },
      ],
    }).select(_CORTE_HISTORICO_SELECT).lean(),

    BankMovement.find({ ...baseMatch, fecha: { $gte: inicio, $lte: fin } })
      .select(_CORTE_HISTORICO_SELECT).lean(),

    BankMovement.aggregate([
      { $match: { ...baseMatch, ...matchEquipo, status: 'identificado', primeraIdentificacionAt: { $gte: inicio, $lte: fin } } },
      { $group: { _id: { $cond: [{ $lt: ['$fecha', inicio] }, 'rezagado', 'nuevo'] }, count: { $sum: 1 }, importeTotal: { $sum: '$deposito' } } },
    ]).then(rows => ({
      counts:   Object.fromEntries(rows.map(r => [r._id, r.count])),
      importes: Object.fromEntries(rows.map(r => [r._id, r.importeTotal ?? 0])),
    })),
  ]);

  let posibleCambioPosterior = 0;
  // Pool de movimientos que SIGUEN pendientes al cierre (rezago o nuevo), con CxC vinculada —
  // insumo de "CxC retrasadas" (ver _calcularCxcRetrasadas). Mismo criterio de exclusión que
  // BACKLOG_STATUSES: 'otros' queda afuera, es un estatus terminal, no un pendiente disfrazado.
  const pendientesConErp = [];

  // `importeTotal` (2026-10-22): acumulado en JS junto con el conteo, porque esta rama ya
  // reconstruye el estado "al cierre" documento por documento (no hay $group de Mongo acá).
  const rezagados = { no_identificado: 0, reclasificado: 0 };
  const rezagadosImporte = { no_identificado: 0, reclasificado: 0 };
  for (const mov of candidatosRezagados) {
    const { identificado, posibleCambio } = _estadoAlCierre(mov, fin);
    if (posibleCambio) posibleCambioPosterior++;
    if (!identificado) {
      const key = mov.status === 'reclasificado' ? 'reclasificado' : 'no_identificado';
      rezagados[key]++;
      rezagadosImporte[key] += mov.deposito ?? 0;
      pendientesConErp.push(mov);
    }
  }
  rezagados.total = rezagados.no_identificado + rezagados.reclasificado;
  rezagados.importeTotal = {
    ...rezagadosImporte,
    total: rezagadosImporte.no_identificado + rezagadosImporte.reclasificado,
  };

  const nuevos = { no_identificado: 0, reclasificado: 0, identificado: 0, otros: 0 };
  const nuevosImporte = { no_identificado: 0, reclasificado: 0, otros: 0 };
  let identificadoTotalSinScope = 0;
  let identificadoDelEquipo     = 0;
  let identificadoImporteSinScope = 0;
  let identificadoImporteEquipo   = 0;
  for (const mov of candidatosNuevos) {
    const { identificado, posibleCambio } = _estadoAlCierre(mov, fin);
    if (posibleCambio) posibleCambioPosterior++;
    const importe = mov.deposito ?? 0;
    if (identificado) {
      identificadoTotalSinScope++;
      identificadoImporteSinScope += importe;
      if (_enEquipoRol(mov.primeraIdentificacionPor?.userId, equipoIds)) {
        identificadoDelEquipo++;
        identificadoImporteEquipo += importe;
      }
    }
    else if (mov.status === 'reclasificado') { nuevos.reclasificado++; nuevosImporte.reclasificado += importe; pendientesConErp.push(mov); }
    else if (mov.status === 'otros')         { nuevos.otros++; nuevosImporte.otros += importe; }
    else                                      { nuevos.no_identificado++; nuevosImporte.no_identificado += importe; pendientesConErp.push(mov); }
  }
  nuevos.identificado = identificadoDelEquipo;
  nuevos.pendientes = nuevos.no_identificado + nuevos.reclasificado;
  nuevos.total = nuevos.no_identificado + nuevos.reclasificado + identificadoTotalSinScope + nuevos.otros;
  nuevos.importeTotal = {
    no_identificado: nuevosImporte.no_identificado,
    reclasificado:    nuevosImporte.reclasificado,
    identificado:     identificadoImporteEquipo,
    otros:            nuevosImporte.otros,
    total: nuevosImporte.no_identificado + nuevosImporte.reclasificado + identificadoImporteSinScope + nuevosImporte.otros,
  };

  const identificadosEnPeriodo = {
    deRezagados: identificadosPorOrigen.counts.rezagado ?? 0,
    deNuevos:    identificadosPorOrigen.counts.nuevo ?? 0,
  };
  identificadosEnPeriodo.total = identificadosEnPeriodo.deRezagados + identificadosEnPeriodo.deNuevos;
  identificadosEnPeriodo.importeTotal = {
    deRezagados: identificadosPorOrigen.importes.rezagado ?? 0,
    deNuevos:    identificadosPorOrigen.importes.nuevo ?? 0,
  };
  identificadosEnPeriodo.importeTotal.total = identificadosEnPeriodo.importeTotal.deRezagados + identificadosEnPeriodo.importeTotal.deNuevos;

  const cxcRetrasadas = _calcularCxcRetrasadas(pendientesConErp, _anioMesDeFecha(fin));

  return { rezagados, nuevos, identificadosEnPeriodo, cxcRetrasadas, posibleCambioPosterior };
}

// Bug real encontrado 2026-11-06, verificado contra una copia real de producción: `primeraIdentificacionAt`
// se agregó al schema recién el 2026-08-14 (ver git blame de BankMovement.model.js), SIN backfill
// histórico — ~30,383 de 48,105 movimientos `identificado` nunca tuvieron este campo seteado
// porque se identificaron ANTES de esa fecha. Como `_estadoAlCierre` dependía solo de él, esos
// movimientos quedaban "atrapados" como backlog pendiente para siempre en cualquier corte —
// incluido el de hoy (reproducido: getLedgerMensual daba identificados:0 en mayo/junio/julio
// pese a haber miles de identificaciones reales esos meses, y un total inflado a ~30,615 en
// noviembre contra un backlog real de 210). `identificadoPor[]` sí tiene historial completo desde
// el commit inicial del proyecto (2026-04-15) — se usa acá como respaldo.
//
// Segundo bug, mismo origen, encontrado 2026-11-10: el fallback a `fichaAt` que sí se agregó a
// `_primeraResueltaEfectiva` el 2026-11-06 (identificación por comprobante físico, sin pasar por
// `identificadoPor`) nunca se portó acá — el ledger de Mensual/Diario (`_calcularLedgerBacklog`)
// seguía subestimando identificaciones hechas vía ficha. Misma prioridad de fuentes que la otra
// función: primeraIdentificacionAt > identificadoPor[].fechaId > fichaAt.
function _primeraIdentificacionEfectiva(mov) {
  if (mov.primeraIdentificacionAt) return mov.primeraIdentificacionAt;
  const fechas = (mov.identificadoPor || [])
    .map(p => p.fechaId)
    .filter(Boolean);
  if (fechas.length) return fechas.reduce((min, f) => (f < min ? f : min), fechas[0]);
  if (mov.fichaAt) return mov.fichaAt;
  return null;
}

// Reconstrucción de "¿estaba identificado al cierre `fin` de un corte histórico?".
// `_primeraIdentificacionEfectiva()` es forward-only: si existe y es <= fin, el movimiento SE
// IDENTIFICÓ por primera vez a tiempo. Pero el vínculo pudo haberse revertido (desvinculación de
// CxC) y ANTES del cierre — eso sí lo sabemos con certeza via historialVinculacion (timestamp
// exacto de 'desvinculado'/'ajustado'/'vinculado').
//
// Bug real encontrado 2026-11-12, confirmado con 48 movimientos reales de producción
// ($1,072,364.69): la versión anterior de esta función hacía "¿hubo ALGÚN desvinculado/ajustado
// después de la primera identificación?" (1 solo salto) — eso daba FALSO POSITIVO cuando el
// movimiento se desvinculaba y se volvía a vincular DESPUÉS (ej. Kore revierte una CxC y el
// usuario la re-vincula, o Kore la re-aplica): quedaba marcado como "revertido" para siempre,
// aunque ya estuviera de nuevo 100% cubierto. Fix intermedio (replay de un solo booleano) todavía
// daba falso positivo con 2+ CxC vinculadas simultáneas (desvincular UNA apagaba el booleano
// aunque la otra siguiera cubriendo el depósito) y falso negativo para `status:'no_identificado'`
// puesto por una vía sin rastro en historialVinculacion (ej. reclasificación masiva) — confirmado
// con un cruce completo contra producción (32 falsos positivos / 25 falsos negativos).
//
// Fix real, mismo día: para un `fin` que representa AHORA MISMO (no un corte ya cerrado), no
// hace falta reconstruir NADA — `status` ya es la verdad en tiempo real, porque cualquier
// mutación de vínculo (vincular/desvincular/ajustar) lo recalcula y guarda en el mismo request
// (ver erp-reversion.service.js). La reconstrucción vía historialVinculacion solo tiene sentido
// — y solo se usa de acá en más — para un `fin` genuinamente pasado, donde no se puede confiar
// en el `status` actual porque pudo cambiar después de `fin`. La limitación del booleano único
// para multi-CxC queda, a propósito, sin resolver para el caso histórico (fuera de alcance de
// hoy — los dashboards que importan ahora mismo usan el caso "ahora").
//
// Lo que NO podemos saber con certeza, en el caso histórico, es si hubo un cambio manual
// (updateStatus/reclasifyMovements) después de `fin` que alteró el estatus actual respecto al de
// `fin` — eso se expone como `posibleCambio`, nunca se oculta ni se asume silenciosamente que el
// dato es exacto.
// Tolerancia para decidir "¿fin es ahora mismo?": `fin` siempre se captura (en el enumerador que
// arma los periodos) ANTES de que `_calcularLedgerBacklog` capture `ahora` unas líneas más abajo
// en la misma request — nunca van a ser iguales ni `fin > ahora`. Un corte ya cerrado es, como
// mínimo, horas/días más viejo que `ahora`; 60s de margen separa sin ambigüedad "esto se generó
// como 'ahora' hace un instante" de "esto es un cierre genuinamente pasado".
const TOLERANCIA_AHORA_MS = 60_000;

function _esPeriodoEnVivo(fin, ahora) {
  return ahora.getTime() - fin.getTime() < TOLERANCIA_AHORA_MS;
}

// 2026-11-12 (segunda decisión del usuario, misma sesión): REVIERTE parcialmente el criterio de
// arriba — Cortes vuelve a contar como pendiente CUALQUIER `status:'no_identificado'` (igual que
// el Dashboard "Estatus"), con o sin `saldoErp` parcial. Lo que SÍ sigue cambiando es cómo se
// neteó el monto — ver `_calcularLedgerBacklog` más abajo (`totalSaldoNetoEnVivo`), que resta lo
// ya cubierto por una CxC parcial, igual que `bank.service.js#saldoRestanteCxC` — pero eso es un
// cálculo aparte del `total.saldo`, NO de este booleano `identificado` (que solo decide conteo).
function _estadoAlCierre(mov, fin, ahora = new Date()) {
  if (_esPeriodoEnVivo(fin, ahora)) {
    return { identificado: mov.status === 'identificado', posibleCambio: false };
  }

  const pid = _primeraIdentificacionEfectiva(mov);
  const identificadoBase = !!(pid && pid <= fin);
  const posibleCambio = !!(mov.ultimoCambioStatusAt && mov.ultimoCambioStatusAt > fin);

  if (!identificadoBase) return { identificado: false, posibleCambio };

  let vinculadoNeto = true;
  for (const h of (mov.historialVinculacion || [])) {
    if (h.at <= pid) continue;
    if (h.at > fin) break;
    if (h.accion === 'vinculado') vinculadoNeto = true;
    else if (h.accion === 'desvinculado') vinculadoNeto = false;
    // 'ajustado' no cambia vinculadoNeto — el link sigue vigente, solo corrige el monto.
  }

  return { identificado: vinculadoNeto, posibleCambio };
}

// Periodo de corte por rol (2026-10-05): antes hardcodeado en el frontend
// (bank-corte-panel.component.ts), movido acá porque el backend YA conoce el rol autenticado
// (req.user.role) y porque Configuraciones Globales no expone sus rutas de administración a
// roles que no sean config:manage — exponerlo como un endpoint propio con banks:read evita ese
// problema de permisos. Fallback a los valores que ya eran el hardcode anterior si la config
// todavía no existe (ambiente recién desplegado, antes de correr el seed).
const PERIODO_ROL_DEFAULT = {
  cobranza:     'semanal',
  contabilidad: 'mensual',
};

async function _periodoConfigurado(role) {
  try {
    return await globalConfigService.getValue('bancos', `CORTE_PERIODO_${role.toUpperCase()}`);
  } catch (err) {
    if (err.message?.includes('No existe la configuración')) return PERIODO_ROL_DEFAULT[role];
    throw err;
  }
}

/**
 * Qué periodo de corte ve un rol, y si puede alternar entre semanal/mensual — gate de
 * presentación (mismo criterio ya documentado en bank.routes.js#GET /cortes: el backend no
 * fuerza el periodo por permisos, cualquiera con banks:read puede pedir cualquiera
 * explícitamente). cobranza/contabilidad quedan fijos al periodo configurado; cualquier otro
 * rol puede alternar, con 'semanal' como default.
 */
async function getPeriodoCortePorRol(role) {
  if (role === 'cobranza' || role === 'contabilidad') {
    return { periodo: await _periodoConfigurado(role), puedeAlternar: false };
  }
  return { periodo: 'semanal', puedeAlternar: true };
}

// Mismo criterio de _hoyMexicoStr (línea ~105) pero para un instante arbitrario, no solo "ahora"
// — insumo de _enumerarDiasEnRango para saber si cada día del rango es HOY (fin = ahora real, en
// vivo) o un día ya cerrado (fin = medianoche de ese día). No se tocó _hoyMexicoStr (la siguen
// usando otras funciones de este archivo) para no arriesgar una regresión ajena a este cambio.
function _fechaMexicoStr(fechaReal) {
  const mx = _comoRelojMexico(fechaReal);
  const yyyy = mx.getUTCFullYear();
  const mm = String(mx.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(mx.getUTCDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

/**
 * Ledger de backlog `no_identificado`/`identificado` para UN periodo (2026-10-30, pedido
 * explícito del usuario) — MISMA fórmula sea el periodo un día o una semana, nunca duplicada por
 * granularidad: `iniciales` (lo que ya era no_identificado al INICIO del periodo) + `nuevos` (lo
 * cargado — `createdAt` — DENTRO del periodo) − `identificados` (lo que pasó de no-identificado a
 * identificado DURANTE este mismo periodo) = `total`, que es el `iniciales` del SIGUIENTE periodo
 * (identidad matemática exacta, no una aproximación — ver nota abajo).
 *
 * `identificados` viene además partido en `identificadosDeIniciales`/`identificadosDeNuevos`
 * (2026-10-30, pedido explícito del usuario: quiere ver cuánto de lo resuelto era backlog VIEJO
 * vs. cuánto era del mismo día, no un número combinado) — `identificados` sigue siendo la suma de
 * los dos, no se duplica el cálculo, solo se separa el contador según el pool (`iniciales`/
 * `nuevos`) al que pertenecía el movimiento.
 *
 * NO usa snapshots: recalcula on-demand reusando `_estadoAlCierre()` en AMBOS extremos del
 * periodo (inicio Y fin) — nunca el status "actual" crudo para decidir `identificados`. Esto es
 * a propósito y DISTINTO de cómo lo hacía la versión anterior de este mismo chart
 * (getProgresoResolucionPorCorte, que comparaba contra el status de HOY sin importar el periodo):
 * usar "status de hoy" hace que un mismo movimiento resuelto, por ejemplo, el miércoles, se
 * cuente como "identificado" en CADA periodo que lo tenga en su pool de candidatos (lunes,
 * martes, miércoles...) porque "hoy" no cambia entre esos cálculos — se cuenta varias veces. Con
 * `_estadoAlCierre` evaluado en los dos extremos, la transición no-identificado→identificado
 * queda atada EXACTAMENTE al periodo en que ocurrió (nunca antes, nunca después, nunca dos
 * veces), y por eso: (a) sumar los `nuevos`/`identificados` de los 7 días de una semana da EXACTO
 * lo mismo que calcular la semana directo (test de este archivo lo prueba), y (b) el `total` de
 * un periodo es EXACTO el `iniciales` del siguiente, sin importar si ambos se piden por separado.
 *
 * `mesOrigenInicio`/`mesOrigenFin` (opcional, ambos `Date` o ambos `null`): acota `iniciales`+
 * `nuevos` a lo cargado (`createdAt`) dentro de un mes de origen específico (drill-down "cómo
 * bajó agosto corte a corte") — para un mes ya cerrado consultado desde un periodo posterior,
 * `nuevos` da 0 solo, sin caso especial (nada puede nacer en agosto desde octubre).
 *
 * REAPARECIDOS (bug real 2026-11-10, confirmado con 6 movimientos reales de producción): un
 * depósito ya resuelto ANTES de `periodoInicio` que se DESVINCULA (revierte) dentro del periodo y
 * sigue sin resolver al `periodoFin` se cuenta como "nuevo" de ESTE periodo (`nuevos`, no
 * `iniciales` — nunca estuvo en el backlog al arrancar el periodo, entra recién cuando se
 * revierte). Antes se descartaba sin dejar rastro (invisible para el `total` de este periodo) y
 * reaparecía "de la nada" como `iniciales` del periodo SIGUIENTE, rompiendo la garantía de
 * conservación de arriba. Si en cambio se revierte y se vuelve a resolver DENTRO del mismo
 * periodo (resuelto en ambos extremos), sigue sin contarse en ningún lado — no afecta ningún
 * `total` de borde, es un caso aceptado, no el que rompía la garantía.
 *
 * @param {object} opts
 * @param {object} opts.baseMatch Mismo `buildBaseMatch({banco})` que el resto del dominio.
 * @param {Date} opts.periodoInicio
 * @param {Date} opts.periodoFin
 * @param {Date} [opts.mesOrigenInicio]
 * @param {Date} [opts.mesOrigenFin]
 */
async function _calcularLedgerBacklog({ baseMatch, periodoInicio, periodoFin, mesOrigenInicio = null, mesOrigenFin = null }) {
  // Capturado UNA sola vez y pasado a cada llamada de _estadoAlCierre: así "¿fin es ahora mismo?"
  // se evalúa con el mismo instante para todo este cálculo (periodoInicio y periodoFin de TODOS
  // los movimientos), en vez de llamar new Date() de nuevo en cada invocación (fix 2026-11-12).
  const ahora = new Date();

  const createdAtMatch = { $lte: periodoFin };
  if (mesOrigenInicio) createdAtMatch.$gte = mesOrigenInicio;
  if (mesOrigenFin) createdAtMatch.$lt = mesOrigenFin;

  const candidatos = await BankMovement.find({
    ...baseMatch,
    createdAt: createdAtMatch,
    status: { $in: ['no_identificado', 'identificado'] },
  }).select('deposito createdAt status saldoErp primeraIdentificacionAt historialVinculacion identificadoPor fichaAt').lean();

  let inicialesCount = 0, inicialesSaldo = 0;
  let nuevosCount = 0, nuevosSaldo = 0;
  let idDeInicialesCount = 0, idDeInicialesSaldo = 0;
  let idDeNuevosCount = 0, idDeNuevosSaldo = 0;
  // 2026-11-12: acumulador aparte para el total EN VIVO, neteado igual que
  // bank.service.js#saldoRestanteCxC (resta lo ya cubierto por una CxC parcial). Vive aislado de
  // inicialesSaldo/nuevosSaldo/identificadosSaldo a propósito — esos 3 siguen con el depósito
  // completo sin tocar, porque de ellos depende la garantía `total(A) == iniciales(B)` entre
  // periodos consecutivos (histórica, no debe depender de un saldoErp que cambia con el tiempo).
  // Para un corte histórico (no en vivo) este acumulador se ignora — `total.saldo` sigue siendo
  // la resta de los 3 de siempre, sin neteo (no hay forma confiable de saber el saldoErp pasado).
  let totalSaldoNetoEnVivo = 0;

  for (const mov of candidatos) {
    const importe = mov.deposito ?? 0;
    const importeNeto = mov.saldoErp != null ? Math.max(0, importe - mov.saldoErp) : importe;

    if (mov.createdAt >= periodoInicio) {
      // No existía antes de periodoInicio — no puede estar identificado a esa fecha, ahorra la
      // llamada a _estadoAlCierre(mov, periodoInicio).
      const identificadoAlFin = _estadoAlCierre(mov, periodoFin, ahora).identificado;
      nuevosCount++; nuevosSaldo += importe;
      if (identificadoAlFin) { idDeNuevosCount++; idDeNuevosSaldo += importe; }
      else totalSaldoNetoEnVivo += importeNeto;
      continue;
    }

    const identificadoAlInicio = _estadoAlCierre(mov, periodoInicio, ahora).identificado;
    const identificadoAlFin = _estadoAlCierre(mov, periodoFin, ahora).identificado;

    if (identificadoAlInicio) {
      if (identificadoAlFin) continue; // resuelto antes de empezar y sigue (o se re-)resuelto al cierre: no es backlog de este periodo
      nuevosCount++; nuevosSaldo += importe; // reaparecido dentro del periodo — ver nota REAPARECIDOS arriba
      totalSaldoNetoEnVivo += importeNeto;
      continue;
    }

    inicialesCount++; inicialesSaldo += importe;
    if (identificadoAlFin) { idDeInicialesCount++; idDeInicialesSaldo += importe; }
    else totalSaldoNetoEnVivo += importeNeto;
  }

  const identificadosCount = idDeInicialesCount + idDeNuevosCount;
  const identificadosSaldo = idDeInicialesSaldo + idDeNuevosSaldo;
  const totalSaldoHistorico = inicialesSaldo + nuevosSaldo - identificadosSaldo;

  return {
    iniciales:              { count: inicialesCount, saldo: inicialesSaldo },
    nuevos:                 { count: nuevosCount, saldo: nuevosSaldo },
    identificados:          { count: identificadosCount, saldo: identificadosSaldo },
    identificadosDeIniciales: { count: idDeInicialesCount, saldo: idDeInicialesSaldo },
    identificadosDeNuevos:    { count: idDeNuevosCount, saldo: idDeNuevosSaldo },
    total: {
      count: inicialesCount + nuevosCount - identificadosCount,
      saldo: _esPeriodoEnVivo(periodoFin, ahora) ? totalSaldoNetoEnVivo : totalSaldoHistorico,
    },
  };
}

// Resuelve mesOrigen ('YYYY-MM') a los límites {$gte,$lt} que espera _calcularLedgerBacklog —
// compartido por la vista semanal y la diaria para no repetir la validación/parseo dos veces.
function _resolverMesOrigen(mesOrigen) {
  if (!mesOrigen) return { mesOrigenInicio: null, mesOrigenFin: null };
  if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(mesOrigen)) throw new BadRequestError('mesOrigen inválido (YYYY-MM)');
  const [anio, mesNum] = mesOrigen.split('-').map(Number);
  const { $gte: mesOrigenInicio, $lt: mesOrigenFin } = _rangoAnioMesMexico(anio, mesNum);
  return { mesOrigenInicio, mesOrigenFin };
}

/**
 * Progreso de resolución del backlog, corte a corte (semanal, 2026-10-30 — caso semanal de
 * `_calcularLedgerBacklog`, ver su JSDoc para la fórmula completa y la garantía de consistencia).
 * El corte SIEMPRE es semanal acá — no depende de getPeriodoCortePorRol (el usuario decidió dejar
 * esa infraestructura dormida, no tocarla; este chart nunca dependió de un periodo configurable).
 *
 * `mesOrigen` (opcional, 2026-10-30) acota el ledger a lo cargado ese mes — el drill-down "cómo
 * bajó agosto corte a corte" que pidió el usuario.
 *
 * @param {object} [opts]
 * @param {string} [opts.banco]
 * @param {string} [opts.mesOrigen] `YYYY-MM`.
 * @param {string} [opts.desde] `YYYY-MM-DD` — corte de cierre más antiguo a incluir.
 * @param {string} [opts.hasta] `YYYY-MM-DD` — corte de cierre más reciente a incluir (default:
 *   el último corte semanal YA CERRADO — la semana en curso, todavía abierta, no se incluye).
 * @param {number} [opts.cantidad=8] Cuántos cortes traer hacia atrás cuando no viene `desde`.
 */
async function getProgresoResolucionPorCorte({ banco = null, mesOrigen = null, desde = null, hasta = null, cantidad = 8 } = {}) {
  const baseMatch = buildBaseMatch({ banco });
  const { mesOrigenInicio, mesOrigenFin } = _resolverMesOrigen(mesOrigen);
  const cortes = _enumerarCortesSemanales({ desde, hasta, cantidad });

  const resultados = await Promise.all(cortes.map(async ({ inicio, fin }) => {
    const ledger = await _calcularLedgerBacklog({ baseMatch, periodoInicio: inicio, periodoFin: fin, mesOrigenInicio, mesOrigenFin });
    return { fechaInicio: inicio, fechaCierre: fin, ...ledger };
  }));

  return { cortes: resultados };
}

/**
 * Ledger DIARIO del backlog (2026-10-30, pedido explícito del usuario: "nuevos" son
 * específicamente los depósitos DEL DÍA — lo no resuelto pasa a ser "iniciales" del día
 * siguiente). Caso diario de `_calcularLedgerBacklog` (ver su JSDoc) — a diferencia de la vista
 * semanal, el día de HOY SÍ se incluye (es un periodo en curso: "cuántos nuevos cayeron hoy"
 * tiene sentido en vivo, a diferencia de una semana todavía abierta).
 *
 * @param {object} [opts]
 * @param {string} [opts.banco]
 * @param {string} [opts.mesOrigen] `YYYY-MM`.
 * @param {string} [opts.desde] `YYYY-MM-DD`.
 * @param {string} [opts.hasta] `YYYY-MM-DD` (default: hoy).
 * @param {number} [opts.cantidad=14] Cuántos días traer hacia atrás cuando no viene `desde`.
 */
async function getLedgerDiario({ banco = null, mesOrigen = null, desde = null, hasta = null, cantidad = 14 } = {}) {
  const baseMatch = buildBaseMatch({ banco });
  const { mesOrigenInicio, mesOrigenFin } = _resolverMesOrigen(mesOrigen);
  const dias = _enumerarDiasEnRango({ desde, hasta, cantidad });

  const resultados = await Promise.all(dias.map(async ({ inicio, fin }) => {
    const ledger = await _calcularLedgerBacklog({ baseMatch, periodoInicio: inicio, periodoFin: fin, mesOrigenInicio, mesOrigenFin });
    return { fecha: inicio, ...ledger };
  }));

  return { dias: resultados };
}

// Límites {inicio,fin} de cortes semanales PASADOS (lunes-a-domingo, mismo criterio que
// _inicioSemanaMexico) — insumo de getProgresoResolucionPorCorte. Devuelve el PAR inicio/fin
// (antes solo devolvía `fechaCierre`, suficiente para la versión vieja que no necesitaba saber
// dónde empezaba la semana) porque _calcularLedgerBacklog necesita ambos extremos para separar
// "iniciales" de "nuevos". La semana EN CURSO nunca se incluye — todavía no cerró, "resueltos"/
// "pendientes" de un corte abierto no tiene sentido (compara contra un cierre que no existe).
// `fin` = último instante (23:59:59.999) del domingo de esa semana, en hora México.
function _enumerarCortesSemanales({ desde = null, hasta = null, cantidad = 8 } = {}) {
  const UNA_SEMANA_MS = 7 * 24 * MS_PER_HOUR;
  const cierreUltimaSemanaCerrada = new Date(_inicioSemanaMexico().getTime() - 1);
  const techo = hasta ? Math.min(_finDiaMx(hasta).getTime(), cierreUltimaSemanaCerrada.getTime()) : cierreUltimaSemanaCerrada.getTime();
  const piso  = desde ? _inicioDiaMx(desde).getTime() : null;

  const cortes = [];
  for (let t = techo; ; t -= UNA_SEMANA_MS) {
    if (piso !== null && t < piso) break;
    const fin = new Date(t);
    const inicio = new Date(t - UNA_SEMANA_MS + 1);
    cortes.push({ inicio, fin });
    if (piso === null && cortes.length >= cantidad) break;
  }
  return cortes.reverse(); // cronológico, más viejo primero
}

// Límites {inicio,fin} de días PASADOS + HOY (2026-10-30) — insumo de getLedgerDiario. A
// diferencia de _enumerarCortesSemanales, el día en curso SÍ se incluye: su `fin` es `new Date()`
// real (ahora), no medianoche, para que "nuevos de hoy" refleje lo cargado hasta este instante;
// cualquier día anterior usa `_finDiaMx` (medianoche de ese día, ya cerrado).
function _enumerarDiasEnRango({ desde = null, hasta = null, cantidad = 14 } = {}) {
  const UN_DIA_MS = 24 * MS_PER_HOUR;
  const hoyStr = _hoyMexicoStr();
  const techoInicio = _inicioDiaMx(hasta || hoyStr).getTime();
  const piso = desde ? _inicioDiaMx(desde).getTime() : null;

  const dias = [];
  for (let t = techoInicio; ; t -= UN_DIA_MS) {
    if (piso !== null && t < piso) break;
    const inicio = new Date(t);
    const diaStr = _fechaMexicoStr(inicio);
    const fin = diaStr === hoyStr ? new Date() : _finDiaMx(diaStr);
    dias.push({ inicio, fin });
    if (piso === null && dias.length >= cantidad) break;
  }
  return dias.reverse();
}

// Límites {inicio,fin,mes:'YYYY-MM'} de meses PASADOS + el mes EN CURSO (2026-10-30, insumo de
// getLedgerMensual) — mismo criterio que _enumerarDiasEnRango para "hoy": el mes en curso SÍ se
// incluye, con `fin = new Date()` real (la comparativa "corte anterior vs. corte actual" del
// dashboard mensual necesita ver el mes de hoy en vivo). Reusa `_rangoAnioMesMexico` (mismo
// offset fijo -06:00 ya establecido en bank.service.js) para los límites de cada mes — nunca
// aritmética de fecha nueva.
function _enumerarMesesParaLedger({ desde = null, hasta = null, cantidad = 7 } = {}) {
  const hoyMx = _comoRelojMexico(hasta ? _finDiaMx(hasta) : new Date());
  let anio = hoyMx.getUTCFullYear();
  let mes  = hoyMx.getUTCMonth() + 1; // 1-indexado, lo que espera _rangoAnioMesMexico

  const piso = desde ? _comoRelojMexico(_inicioDiaMx(desde)) : null;
  const pisoAnio = piso ? piso.getUTCFullYear() : null;
  const pisoMes  = piso ? piso.getUTCMonth() + 1 : null;

  const meses = [];
  for (;;) {
    if (piso !== null && (anio < pisoAnio || (anio === pisoAnio && mes < pisoMes))) break;
    const { $gte: inicio, $lt: finExclusivo } = _rangoAnioMesMexico(anio, mes);
    const esMesActual = anio === hoyMx.getUTCFullYear() && mes === hoyMx.getUTCMonth() + 1 && !hasta;
    const fin = esMesActual ? new Date() : new Date(finExclusivo.getTime() - 1);
    meses.push({ inicio, fin, mes: `${anio}-${String(mes).padStart(2, '0')}` });
    mes--;
    if (mes < 1) { mes = 12; anio--; }
    if (piso === null && meses.length >= cantidad) break;
  }
  return meses.reverse(); // cronológico, más viejo primero
}

/**
 * Ledger MENSUAL del backlog (2026-10-30, reemplaza por completo la idea de un corte semanal
 * para este dashboard — pedido explícito del usuario tras ver un mockup de referencia: la
 * comparativa corte-a-corte que importa es mes a mes, no semana a semana). Caso mensual de
 * `_calcularLedgerBacklog` (ver su JSDoc para la fórmula/garantía de consistencia) — el mes EN
 * CURSO SÍ se incluye, con `fin` en vivo (mismo criterio que getLedgerDiario para "hoy"), porque
 * el dashboard necesita comparar "corte anterior (mes cerrado) vs. corte actual (mes en curso)"
 * en toda su UI (KPIs, tabla resumen, avance del mes).
 *
 * @param {object} [opts]
 * @param {string} [opts.banco]
 * @param {string} [opts.mesOrigen] `YYYY-MM`.
 * @param {string} [opts.desde] `YYYY-MM-DD`.
 * @param {string} [opts.hasta] `YYYY-MM-DD` (default: hoy, mes en curso incluido).
 * @param {number} [opts.cantidad=7] Cuántos meses traer hacia atrás cuando no viene `desde`.
 */
async function getLedgerMensual({ banco = null, mesOrigen = null, desde = null, hasta = null, cantidad = 7 } = {}) {
  const baseMatch = buildBaseMatch({ banco });
  const { mesOrigenInicio, mesOrigenFin } = _resolverMesOrigen(mesOrigen);
  const meses = _enumerarMesesParaLedger({ desde, hasta, cantidad });

  const resultados = await Promise.all(meses.map(async ({ inicio, fin, mes }) => {
    const ledger = await _calcularLedgerBacklog({ baseMatch, periodoInicio: inicio, periodoFin: fin, mesOrigenInicio, mesOrigenFin });
    return { mes, fechaInicio: inicio, fechaFin: fin, ...ledger };
  }));

  return { meses: resultados };
}

// Límites {hora:'HH:00',inicio,fin} de cada hora de UN día (2026-10-30, insumo de
// getLedgerPorHora) — `fechaStr` (`YYYY-MM-DD`, default HOY) permite reusar esto para el tab
// "Ayer" del dashboard sin otro mecanismo. Si `fechaStr` es HOY, es una vista "ahora mismo":
// arranca a medianoche (hora México) y corta en la hora en curso, cuyo `fin` es `new Date()` real
// (en vivo). Para cualquier OTRO día (ya cerrado), se enumeran las 24 horas completas, cada una
// con su `fin` fijo (nunca "ahora" — ese día ya pasó).
function _enumerarHorasDelDia(fechaStr = null) {
  const hoyStr = _hoyMexicoStr();
  const esHoy = !fechaStr || fechaStr === hoyStr;
  const ahora = new Date();
  const inicioDelDia = _inicioDiaMx(fechaStr || hoyStr);
  const horaTope = esHoy ? _comoRelojMexico(ahora).getUTCHours() : 23;

  const horas = [];
  for (let h = 0; h <= horaTope; h++) {
    const inicio = new Date(inicioDelDia.getTime() + h * MS_PER_HOUR);
    const esHoraActual = esHoy && h === horaTope;
    const fin = esHoraActual ? ahora : new Date(inicio.getTime() + MS_PER_HOUR - 1);
    horas.push({ hora: `${String(h).padStart(2, '0')}:00`, inicio, fin });
  }
  return horas;
}

/**
 * Ledger POR HORA del backlog, de UN día (2026-10-30, monitoreo en tiempo real pedido por el
 * usuario: "para poder ver cómo se está trabajando en tiempo real y cómo va disminuyendo la
 * información"). Caso horario de `_calcularLedgerBacklog`. Sin `opts.fecha`, es HOY en vivo
 * (`fin` de la última hora es `new Date()` real); con `opts.fecha` (`YYYY-MM-DD`, ej. ayer), las
 * 24 horas quedan cerradas — mismo tab "Ayer" del mockup de referencia, ningún mecanismo nuevo.
 *
 * @param {object} [opts]
 * @param {string} [opts.banco]
 * @param {string} [opts.fecha] `YYYY-MM-DD` (default: hoy).
 */
async function getLedgerPorHora({ banco = null, fecha = null } = {}) {
  const baseMatch = buildBaseMatch({ banco });
  const horas = _enumerarHorasDelDia(fecha);

  const resultados = await Promise.all(horas.map(async ({ hora, inicio, fin }) => {
    const ledger = await _calcularLedgerBacklog({ baseMatch, periodoInicio: inicio, periodoFin: fin });
    return { hora, fechaInicio: inicio, fechaFin: fin, ...ledger };
  }));

  return { horas: resultados };
}

/**
 * Feed de actividad reciente (2026-10-30, "Últimos movimientos" del dashboard de Día actual) —
 * NO reusa `_calcularLedgerBacklog` (es una lista de eventos puntuales, no un agregado por
 * periodo). Combina 2 criterios de "evento reciente" — cargado (`createdAt`) e identificado
 * (`primeraIdentificacionAt`) — porque un movimiento puede ser noticia reciente por cualquiera de
 * los dos motivos y un solo `find` ordenado por un campo se perdería al otro (un inicial
 * identificado HOY tiene `createdAt` viejo, pero es justo la actividad que el usuario quiere ver
 * en el feed).
 *
 * `tipo` se decide por `createdAt`, no por si está identificado o no: 'nuevo' = se cargó HOY
 * (hora México, mismo corte que usa el resto del ledger para "Hoy"), 'inicial' = se cargó antes
 * de hoy (esté o no ya identificado) — es la misma distinción iniciales/nuevos de
 * `_calcularLedgerBacklog`, no un concepto nuevo.
 *
 * @param {object} [opts]
 * @param {string} [opts.banco]
 * @param {number} [opts.limit=10]
 */
async function getUltimosMovimientos({ banco = null, limit = 10 } = {}) {
  const baseMatch = buildBaseMatch({ banco });
  const inicioHoy = _inicioDiaMx(_hoyMexicoStr());
  const campos = 'folio deposito banco status createdAt primeraIdentificacionAt';

  const [recienCargados, recienIdentificados] = await Promise.all([
    BankMovement.find({ ...baseMatch, status: { $in: ['no_identificado', 'identificado'] } })
      .sort({ createdAt: -1 }).limit(limit).select(campos).lean(),
    BankMovement.find({ ...baseMatch, status: 'identificado', primeraIdentificacionAt: { $ne: null } })
      .sort({ primeraIdentificacionAt: -1 }).limit(limit).select(campos).lean(),
  ]);

  const porId = new Map();
  for (const mov of [...recienCargados, ...recienIdentificados]) {
    const esNuevo = mov.createdAt >= inicioHoy;
    const fechaEvento = esNuevo ? mov.createdAt : (mov.primeraIdentificacionAt ?? mov.createdAt);
    const existente = porId.get(String(mov._id));
    // El mismo movimiento puede venir de ambas queries (ej. cargado Y identificado hoy) — se
    // queda con la fecha de evento más reciente entre las dos, nunca se duplica en el feed.
    if (!existente || fechaEvento > existente.fechaEvento) {
      // mesOrigen SIEMPRE por hora México, nunca componentes UTC crudos de createdAt — un
      // createdAt entre las 00:00-05:59 UTC es todavía el día/mes anterior en México (mismo tipo
      // de bug de huso horario ya corregido antes en este proyecto).
      const createdAtMx = _comoRelojMexico(mov.createdAt);
      porId.set(String(mov._id), {
        fechaEvento,
        tipo: esNuevo ? 'nuevo' : 'inicial',
        mesOrigen: `${createdAtMx.getUTCFullYear()}-${String(createdAtMx.getUTCMonth() + 1).padStart(2, '0')}`,
        monto: mov.deposito ?? 0,
        estatus: mov.status,
        banco: mov.banco,
        folio: mov.folio,
      });
    }
  }

  const movimientos = [...porId.values()]
    .sort((a, b) => b.fechaEvento - a.fechaEvento)
    .slice(0, limit);

  return { movimientos };
}

// Rangos de antigüedad del backlog "vacío" (2026-11-06, pedido explícito del usuario tras revisar
// un mockup de referencia: retoma la idea de buckets por rango de días que se había descartado el
// 30-oct para el dashboard mensual/diario/por-hora, con una corrección de diseño sobre esa primera
// versión (getCorteAntiguedadBacklog, ya eliminada de este archivo): agrupa por `fecha` del
// depósito (el día real en el banco), no por `createdAt` (día de carga a Numo).
//
// 2026-11-12 (alineado al Dashboard "Estatus", mismo criterio ya aplicado a Cortes mensual/
// diario): ANTES exigía el depósito completamente vacío (sin ninguna CxC vinculada, ni un pago
// parcial) — un depósito con vinculación parcial quedaba afuera por completo. Eso hacía que el
// total diera distinto al Dashboard (160 vs 183 reales, verificado). AHORA cuenta cualquier
// depósito `no_identificado` sin importar vinculación parcial, pero neteando el monto por lo ya
// cubierto (`saldoErp`) — ver `_bucketsAntiguedadAlCorte`.
const ANTIGUEDAD_RANGOS = Object.freeze(['1 a 30 días', '31 a 60 días', '61 a 90 días', 'Más de 90 días']);

// Índice de rango (0-3) según días de antigüedad — mismos cortes que el mockup de referencia:
// <=30, <=60, <=90, el resto.
function _rangoAntiguedad(dias) {
  if (dias <= 30) return 0;
  if (dias <= 60) return 1;
  if (dias <= 90) return 2;
  return 3;
}

/**
 * Primera vez que un depósito quedó RESUELTO por CUALQUIER vía — vinculación a CxC (parcial o
 * total), ficha física, o cualquier otro camino que haya dejado seteado el campo canónico
 * `primeraIdentificacionAt` (`resolvePrimeraIdentificacion()`, ver `identificacion-timestamp.
 * util.js`). Distinta a propósito de `_primeraIdentificacionEfectiva` (identificación COMPLETA,
 * usada por el ledger — desde 2026-11-10 comparte el mismo fallback a `fichaAt` que esta función,
 * pero sigue siendo una función separada): esta es para la reconstrucción histórica de Antigüedad
 * (`_vacioAlCorteHistorico`), que necesita saber "¿tuvo ALGUNA señal de resolución, sea cual sea?".
 *
 * Tres fuentes, en orden de confiabilidad:
 *  1. `primeraIdentificacionAt` — campo canónico (fix del 2026-11-05).
 *  2. `identificadoPor[].fechaId` — vinculación a CxC, histórico completo desde el commit inicial
 *     del proyecto (2026-04-15).
 *  3. `fichaAt` — identificación por comprobante físico (2026-11-06, bug real: de un residuo de
 *     635 depósitos de enero 2026 que el chart de Antigüedad mostraba como "pendientes" pese a
 *     estar `status:'identificado'`, el 96.5% tenía `fichaAt` poblado sin `primeraIdentificacionAt`
 *     ni `identificadoPor` — ficha más vieja que la adopción de `resolvePrimeraIdentificacion()`).
 *
 * LIMITACIÓN ACEPTADA (mismo criterio que el resto de este archivo): un depósito cuyo `status` se
 * editó a mano sin pasar por ninguna de las 3 vías de arriba no deja ningún rastro — para un corte
 * de fecha pasada no hay forma de saber cuándo se resolvió, así que queda contado como backlog
 * pendiente en los cortes históricos anteriores a hoy. Residuo real pero minoritario (~3.5% de la
 * muestra de enero 2026 investigada), no bloqueante — mismo tipo de residuo ya aceptado por el
 * usuario en el fix del 2026-11-05.
 */
function _primeraResueltaEfectiva(dep) {
  if (dep.primeraIdentificacionAt) return dep.primeraIdentificacionAt;
  const fechas = (dep.identificadoPor || [])
    .map(p => p.fechaId)
    .filter(Boolean);
  if (fechas.length) return fechas.reduce((min, f) => (f < min ? f : min), fechas[0]);
  if (dep.fichaAt) return dep.fichaAt;
  return null;
}

/**
 * Candidatos de antigüedad para HOY — `status:'no_identificado'`, filtro DIRECTO en Mongo, SIN
 * reconstrucción de ningún tipo (2026-11-06, cuarta ronda: simplificación pedida por el usuario
 * tras encontrar que la reconstrucción histórica —pensada para el modo Año— generaba falsos
 * positivos para depósitos ya resueltos sin `identificadoPor`, ej. identificados por ficha o
 * editados a mano). Para "hoy", `status` YA ES la fuente de verdad: no importa CÓMO se resolvió un
 * depósito — si dice `identificado`, no es backlog, punto. Usada por `getAntiguedadDepositos` sin
 * `fecha`, por `getAntiguedadDepositosPorAnio` (siempre "hoy") y por `getAntiguedadPorDiaMesActual`
 * (el día de hoy de la serie, nunca reconstruido).
 *
 * 2026-11-12: el filtro `erpIds: { $size: 0 }` (exigir CERO vinculación) se quitó — alinea el
 * conteo al Dashboard "Estatus" (`bank.service.js#getCards`, que no distingue vinculación parcial
 * para `status:'no_identificado'`). `saldoErp` se trae al `.select()` para que
 * `_bucketsAntiguedadAlCorte` pueda netear el monto de los parcialmente vinculados en vez de
 * sumarlos completos.
 */
async function _antiguedadCandidatosHoy(baseMatch) {
  return BankMovement.find({
    ...baseMatch,
    status: 'no_identificado',
  // `folio`/`banco` (2026-11-06, pedido explícito del usuario: "que al exportar esto me diga qué
  // depósitos son los que están siendo involucrados") — ninguno de los 2 callers existentes
  // (getAntiguedadDepositos/getAntiguedadDepositosPorAnio) los necesitaba, pero agregarlos acá
  // evita que buildReporteCortes#"Detalle de depósitos" tenga que repetir esta misma query con un
  // criterio de filtro que podría desincronizarse del real con el tiempo.
  }).select('folio banco fecha deposito saldoErp').lean();
}

/**
 * Candidatos de antigüedad para un corte de fecha PASADA — trae también los `identificado` de HOY,
 * porque pudieron estar sin resolver en el pasado. El filtrado real ("¿seguía sin resolver a tal
 * corte?") pasa a `_vacioAlCorteHistorico`, que sí necesita reconstruir cuándo (si acaso) se
 * resolvió cada uno vía `_primeraResueltaEfectiva`. `saldoErp` (2026-11-12) viaja para que
 * `_bucketsAntiguedadAlCorte` netee el monto igual que el camino de hoy.
 */
async function _antiguedadCandidatosHistorico(baseMatch) {
  return BankMovement.find({
    ...baseMatch,
    status: { $in: ['no_identificado', 'identificado'] },
  }).select('fecha deposito erpIds identificadoPor primeraIdentificacionAt fichaAt saldoErp').lean();
}

// Agrupa los candidatos ya traídos en los 4 rangos fijos, a una fecha de corte dada — reusada
// tanto por el corte de un solo día (getAntiguedadDepositos) como por cada uno de los 12 cortes
// de fin de mes del modo año (getAntiguedadDepositosPorAnio) y por cada día de
// getAntiguedadPorDiaMesActual, nunca duplicada. Un depósito con fecha POSTERIOR al corte no
// existía todavía a esa fecha — se excluye (nunca antigüedad negativa).
//
// Importe neteado (2026-11-12, mismo criterio que `bank.service.js#saldoRestanteCxC` y que
// `_calcularLedgerBacklog` para el total en vivo de Cortes): un depósito con vinculación parcial
// (`saldoErp` seteado, sin cubrir el 100%) solo aporta el residuo real, no el depósito completo —
// evita inflar el monto "pendiente" con plata que ya tiene una CxC parcialmente cubierta.
function _bucketsAntiguedadAlCorte(depositos, corte) {
  const unDiaMs = 24 * MS_PER_HOUR;
  const rangos = ANTIGUEDAD_RANGOS.map((rango) => ({ rango, count: 0, saldo: 0 }));
  let totalCount = 0, totalSaldo = 0;

  for (const dep of depositos) {
    const inicioDiaDeposito = _inicioDiaMx(_fechaMexicoStr(dep.fecha));
    if (inicioDiaDeposito.getTime() > corte.getTime()) continue;
    const dias = Math.max(0, Math.round((corte.getTime() - inicioDiaDeposito.getTime()) / unDiaMs));
    const importe = dep.saldoErp != null ? Math.max(0, (dep.deposito ?? 0) - dep.saldoErp) : (dep.deposito ?? 0);
    const bucket = rangos[_rangoAntiguedad(dias)];
    bucket.count += 1;
    bucket.saldo += importe;
    totalCount += 1;
    totalSaldo += importe;
  }

  return { rangos, total: { count: totalCount, saldo: totalSaldo } };
}

/**
 * Candidatos que seguían "completamente vacíos" a un corte de fecha PASADA — reconstrucción
 * histórica: un depósito cuenta acá si ya existía (`fecha <= corte`) Y su primera resolución (si
 * la tuvo, ver `_primeraResueltaEfectiva`) fue DESPUÉS de este corte (o nunca la tuvo). Solo se usa
 * para fecha pasada real — el camino "hoy" ya viene filtrado desde Mongo
 * (`_antiguedadCandidatosHoy`), no necesita este paso.
 */
function _vacioAlCorteHistorico(candidatos, corte) {
  const vacio = [];
  for (const dep of candidatos) {
    const inicioDiaDeposito = _inicioDiaMx(_fechaMexicoStr(dep.fecha));
    if (inicioDiaDeposito.getTime() > corte.getTime()) continue; // no existía todavía a este corte
    const primeraResuelta = _primeraResueltaEfectiva(dep);
    if (primeraResuelta && primeraResuelta.getTime() <= corte.getTime()) continue; // ya resuelto a este corte
    vacio.push(dep);
  }
  return vacio;
}

/**
 * Antigüedad del backlog de depósitos no identificados y completamente vacíos, a un corte de UN
 * SOLO DÍA. Dos caminos (2026-11-06, cuarta ronda — separados a propósito, ver
 * `_antiguedadCandidatosHoy`): sin `fecha` (o `fecha` = hoy), status actual directo, sin
 * reconstrucción; con una `fecha` pasada real, reconstrucción histórica vía
 * `_vacioAlCorteHistorico`. Shape idéntico en ambos casos (`rangos`/`total`).
 *
 * @param {object} [opts]
 * @param {string} [opts.banco]
 * @param {string} [opts.fecha] `YYYY-MM-DD` — corte de un día pasado. Default: hoy.
 */
async function getAntiguedadDepositos({ banco = null, fecha = null } = {}) {
  const baseMatch = buildBaseMatch({ banco });
  const hoyStr = _hoyMexicoStr();

  if (!fecha || fecha === hoyStr) {
    const corte = _inicioDiaMx(hoyStr);
    const candidatos = await _antiguedadCandidatosHoy(baseMatch);
    return _bucketsAntiguedadAlCorte(candidatos, corte);
  }

  const corte = _inicioDiaMx(fecha);
  const candidatos = await _antiguedadCandidatosHistorico(baseMatch);
  const vacioAlCorte = _vacioAlCorteHistorico(candidatos, corte);
  return _bucketsAntiguedadAlCorte(vacioAlCorte, corte);
}

/**
 * Antigüedad del backlog ACTUAL (depósitos que siguen sin identificar y completamente vacíos HOY
 * MISMO — NUNCA una reconstrucción histórica), agrupado por el MES DE ORIGEN del depósito (su
 * `fecha`, no un corte de fin de mes artificial) dentro de `anio`.
 *
 * CUARTA RONDA (2026-11-06): reemplaza el diseño anterior (un corte-de-fin-de-mes distinto por
 * mes, con un campo `resuelto` separando "backlog a ese cierre" de "lo que se resolvió después").
 * El usuario reportó que ese diseño, aunque correcto en sus propios términos, confundía: enero
 * mostraba depósitos "pendientes" (en `resuelto`) pese a que HOY no queda ningún depósito de enero
 * sin identificar. Pedido explícito: volver a la lógica simple de antes (antigüedad SIEMPRE contra
 * HOY, igual que `getAntiguedadDepositos` sin `fecha`), agrupada por mes de origen — un depósito
 * viejo de enero que sigue sin identificar hoy cae en "Más de 90 días" dentro del grupo "2026-01",
 * nunca se repite en los demás meses. Un mes sin ningún pendiente hoy devuelve `total.count === 0`
 * — el frontend lo pinta como "resuelto" (verde) a partir de eso, ya no hace falta un campo aparte
 * ni una cantidad.
 *
 * Reusa `_antiguedadCandidatosHoy` (status actual directo, sin reconstrucción — ver su docstring)
 * y `_bucketsAntiguedadAlCorte` para el desglose por rango — nunca duplica la fórmula.
 *
 * @param {object} [opts]
 * @param {string} [opts.banco]
 * @param {string|number} opts.anio `YYYY`.
 */
async function getAntiguedadDepositosPorAnio({ banco = null, anio } = {}) {
  if (!/^\d{4}$/.test(String(anio ?? ''))) {
    throw new BadRequestError('anio inválido (YYYY)');
  }
  const anioNum = Number(anio);
  const baseMatch = buildBaseMatch({ banco });
  const hoyStr = _hoyMexicoStr();
  const hoy = _inicioDiaMx(hoyStr);
  const mesActualStr = hoyStr.slice(0, 7);

  const candidatos = await _antiguedadCandidatosHoy(baseMatch);

  const porMesOrigen = new Map();
  for (const dep of candidatos) {
    const mesOrigen = _fechaMexicoStr(dep.fecha).slice(0, 7);
    if (!porMesOrigen.has(mesOrigen)) porMesOrigen.set(mesOrigen, []);
    porMesOrigen.get(mesOrigen).push(dep);
  }

  const meses = [];
  for (let mes = 1; mes <= 12; mes++) {
    const periodo = `${anioNum}-${String(mes).padStart(2, '0')}`;
    if (periodo > mesActualStr) break; // sin meses futuros
    const { rangos, total } = _bucketsAntiguedadAlCorte(porMesOrigen.get(periodo) || [], hoy);
    meses.push({ periodo, corte: hoy, rangos, total });
  }

  return { meses };
}

/**
 * Serie diaria de Antigüedad del mes EN CURSO + cierre del mes anterior (2026-11-12, pedido
 * explícito del usuario: "ver cómo avanza la distribución día a día dentro del mes, apilando
 * barras"). Reusa los mismos candidatos/buckets que getAntiguedadDepositos/
 * getAntiguedadDepositosPorAnio, nunca duplica la fórmula — 2 consultas a Mongo en total (una para
 * "hoy" en vivo, una para el resto reconstruido en memoria), nunca una consulta por día.
 *
 * SEMÁNTICA DE "CIERRE DE UN DÍA" DISTINTA A PROPÓSITO de `getAntiguedadDepositos({fecha})` (que
 * usa el INICIO del día pasado como corte, vía `_inicioDiaMx`) — acá cada "Día N" representa el
 * backlog DESPUÉS de las operaciones de ese día (corte con `_finDiaMx`), mismo criterio que ya usa
 * el ledger de Cortes para días ya cerrados (ver `_enumerarDiasEnRango`). No es un error de
 * copy-paste si se nota la diferencia contra `getAntiguedadDepositos`: son 2 preguntas distintas
 * ("¿cómo estaba el backlog ENTRANDO a ese día?" vs "¿cómo quedó AL CERRAR ese día?") — esta
 * función responde la segunda, que es la que el usuario pidió para esta serie.
 *
 * HOY siempre usa el camino en vivo (`_antiguedadCandidatosHoy`, status/saldoErp actuales
 * directos) — NUNCA la reconstrucción histórica, mismo criterio ya establecido hoy en
 * `_estadoAlCierre` (Cortes mensual/diario): para "ahora mismo" el estado actual YA ES la verdad;
 * reconstruir introduciría los mismos falsos positivos/negativos que ya se depuraron esta sesión.
 *
 * @param {object} [opts]
 * @param {string} [opts.banco]
 */
async function getAntiguedadPorDiaMesActual({ banco = null } = {}) {
  const baseMatch = buildBaseMatch({ banco });
  const hoyStr = _hoyMexicoStr();
  const [anioActual, mesActual] = hoyStr.slice(0, 7).split('-').map(Number);

  // Último día de calendario antes del 1ro del mes en curso — aritmética de calendario pura
  // (Date.UTC como herramienta neutra para sumar/restar días, no un instante real de México;
  // válido porque México corre en offset fijo -06:00 en todo este archivo, sin horario de verano).
  const primerDiaMesActualUTC = Date.UTC(anioActual, mesActual - 1, 1);
  const ultimoDiaMesAnteriorUTC = new Date(primerDiaMesActualUTC - 24 * MS_PER_HOUR);
  const mesAnteriorStr = `${ultimoDiaMesAnteriorUTC.getUTCFullYear()}-${String(ultimoDiaMesAnteriorUTC.getUTCMonth() + 1).padStart(2, '0')}`;
  const ultimoDiaMesAnteriorStr = `${mesAnteriorStr}-${String(ultimoDiaMesAnteriorUTC.getUTCDate()).padStart(2, '0')}`;

  const [candidatosHistoricos, candidatosHoy] = await Promise.all([
    _antiguedadCandidatosHistorico(baseMatch),
    _antiguedadCandidatosHoy(baseMatch),
  ]);

  const corteMesAnterior = _finDiaMx(ultimoDiaMesAnteriorStr);
  const vacioMesAnterior = _vacioAlCorteHistorico(candidatosHistoricos, corteMesAnterior);
  const cierreMesAnterior = { periodo: mesAnteriorStr, ..._bucketsAntiguedadAlCorte(vacioMesAnterior, corteMesAnterior) };

  const dias = [];
  const hoyNum = Number(hoyStr.slice(8, 10));
  for (let d = 1; d <= hoyNum; d++) {
    const diaStr = `${hoyStr.slice(0, 7)}-${String(d).padStart(2, '0')}`;
    if (diaStr === hoyStr) {
      dias.push({ fecha: diaStr, ..._bucketsAntiguedadAlCorte(candidatosHoy, _inicioDiaMx(hoyStr)) });
      continue;
    }
    const corteDia = _finDiaMx(diaStr);
    const vacioDia = _vacioAlCorteHistorico(candidatosHistoricos, corteDia);
    dias.push({ fecha: diaStr, ..._bucketsAntiguedadAlCorte(vacioDia, corteDia) });
  }

  return { cierreMesAnterior, dias };
}

// Columnas compartidas por las hojas "Mensual"/"Diario" de buildReporteCortes — mismo orden que
// la tabla "Resultados por mes"/"Resultados por día" del frontend (bank-corte-panel.component.html):
// 5 columnas de monto + 5 de conteo de depósitos, con el mismo rótulo.
function _columnasLedgerExcel(etiquetaPeriodo) {
  return [
    { header: etiquetaPeriodo, key: 'periodo', width: 14 },
    { header: 'Saldo inicial', key: 'inicialSaldo', width: 16 },
    { header: '+ Nuevos', key: 'nuevosSaldo', width: 16 },
    { header: '= Al cierre', key: 'cierreSaldo', width: 16 },
    { header: '− Identificados', key: 'idSaldo', width: 16 },
    { header: '= Depósitos por identificar', key: 'totalSaldo', width: 22 },
    { header: 'Dep. saldo inicial', key: 'inicialCount', width: 16 },
    { header: '+ Dep. nuevos', key: 'nuevosCount', width: 14 },
    { header: '= Dep. al cierre', key: 'cierreCount', width: 14 },
    { header: '− Dep. identificados', key: 'idCount', width: 18 },
    { header: '= Dep. por identificar', key: 'totalCount', width: 18 },
  ];
}

// "Al cierre" (iniciales+nuevos) no viene armado en la respuesta de _calcularLedgerBacklog —
// se suma acá mismo, igual que ya lo hace el frontend (bank-corte-panel.component.ts).
function _filaLedgerExcel(periodo, ledger) {
  return {
    periodo,
    inicialSaldo: ledger.iniciales.saldo,
    nuevosSaldo:  ledger.nuevos.saldo,
    cierreSaldo:  ledger.iniciales.saldo + ledger.nuevos.saldo,
    idSaldo:      ledger.identificados.saldo,
    totalSaldo:   ledger.total.saldo,
    inicialCount: ledger.iniciales.count,
    nuevosCount:  ledger.nuevos.count,
    cierreCount:  ledger.iniciales.count + ledger.nuevos.count,
    idCount:      ledger.identificados.count,
    totalCount:   ledger.total.count,
  };
}

function _estilizarHojaCorte(ws) {
  const header = ws.getRow(1);
  header.height = 22;
  header.font = { bold: true, color: { argb: 'FFE0E7FF' }, size: 10 };
  header.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1E1B4B' } };
  header.alignment = { vertical: 'middle', horizontal: 'center' };
  ws.views = [{ state: 'frozen', ySplit: 1 }];
}

/**
 * Excel descargable de TODO el panel de Cortes (2026-11-06, pedido explícito del usuario: un
 * solo botón de exportar para las 3 vistas — Mensual/Diario/Antigüedad —, no uno por tab, con un
 * solo historial de descargas compartido). 3 hojas, mismas columnas que sus tablas en pantalla.
 * La ancla `desde:'2026-05'` de la hoja Mensual es la MISMA que usa el frontend
 * (LEDGER_MENSUAL_DESDE) — verificada contra producción, ver bank-corte-panel.component.ts.
 *
 * Antes de devolver el buffer, deja un registro en `CorteDescarga` con el backlog vigente al
 * momento ("depósitos no identificados", el mismo número del encabezado del panel) — el
 * snapshot sale del día MÁS RECIENTE de la misma llamada a `getLedgerDiario` que ya se usó para
 * la hoja "Diario", nunca de una consulta aparte (ver `listarDescargasCorte` para cómo se arma
 * el delta contra la descarga anterior).
 *
 * @param {object} [opts]
 * @param {string} [opts.banco]
 * @param {object} [opts.usuario] `{userId, nombre}` — quién generó la descarga (puede venir
 *   `null` en contextos sin usuario autenticado, ej. un test directo del servicio).
 */
async function buildReporteCortes({ banco = null, usuario = null } = {}) {
  const baseMatch = buildBaseMatch({ banco });
  const [{ meses }, { dias }, antiguedad, candidatosDetalle] = await Promise.all([
    getLedgerMensual({ banco, desde: '2026-05' }),
    getLedgerDiario({ banco }),
    getAntiguedadDepositos({ banco }),
    // Misma población EXACTA que ya ve el usuario en el tab Antigüedad (nunca una query paralela
    // que pueda desincronizarse) — se re-deriva fecha de origen/antigüedad/rango por depósito acá
    // mismo, reusando _rangoAntiguedad (mismos 4 cortes fijos que el chart).
    _antiguedadCandidatosHoy(baseMatch),
  ]);

  const wb = new ExcelJS.Workbook();
  wb.creator = 'Numo — Cortes de conciliación';
  wb.created = new Date();

  const wsMensual = wb.addWorksheet('Mensual');
  wsMensual.columns = _columnasLedgerExcel('Mes');
  for (const m of meses) wsMensual.addRow(_filaLedgerExcel(m.mes, m));
  ['inicialSaldo', 'nuevosSaldo', 'cierreSaldo', 'idSaldo', 'totalSaldo'].forEach((k) => { wsMensual.getColumn(k).numFmt = '#,##0.00'; });
  _estilizarHojaCorte(wsMensual);

  const wsDiario = wb.addWorksheet('Diario');
  wsDiario.columns = _columnasLedgerExcel('Día');
  for (const d of dias) wsDiario.addRow(_filaLedgerExcel(_fechaMexicoStr(d.fecha), d));
  ['inicialSaldo', 'nuevosSaldo', 'cierreSaldo', 'idSaldo', 'totalSaldo'].forEach((k) => { wsDiario.getColumn(k).numFmt = '#,##0.00'; });
  _estilizarHojaCorte(wsDiario);

  const wsAntiguedad = wb.addWorksheet('Antigüedad');
  wsAntiguedad.columns = [
    { header: 'Rango', key: 'rango', width: 20 },
    { header: 'Depósitos', key: 'count', width: 14 },
    { header: 'Importe', key: 'saldo', width: 18 },
  ];
  for (const r of antiguedad.rangos) wsAntiguedad.addRow(r);
  wsAntiguedad.addRow({ rango: 'Total', count: antiguedad.total.count, saldo: antiguedad.total.saldo });
  wsAntiguedad.getColumn('saldo').numFmt = '#,##0.00';
  _estilizarHojaCorte(wsAntiguedad);

  // "Detalle de depósitos" (2026-11-06, pedido explícito del usuario: "que al exportar esto me
  // diga qué depósitos son los que están siendo involucrados") — el listado real, depósito por
  // depósito, de la MISMA población que arma "Antigüedad" (hoja de arriba) y el saldo final de
  // "Mensual"/"Diario" — es un solo backlog vigente, no hace falta una hoja de detalle por vista.
  // Orden: antigüedad descendente (lo más viejo primero) — es lo que normalmente más le importa
  // revisar primero a alguien auditando este reporte.
  const hoyMx = _inicioDiaMx(_hoyMexicoStr());
  const unDiaMs = 24 * MS_PER_HOUR;
  const filasDetalle = candidatosDetalle
    .map((dep) => {
      const fechaStr = _fechaMexicoStr(dep.fecha);
      const dias = Math.max(0, Math.round((hoyMx.getTime() - _inicioDiaMx(fechaStr).getTime()) / unDiaMs));
      return {
        folio: dep.folio,
        banco: dep.banco,
        fecha: fechaStr,
        importe: dep.deposito ?? 0,
        mesOrigen: fechaStr.slice(0, 7),
        antiguedadDias: dias,
        rango: ANTIGUEDAD_RANGOS[_rangoAntiguedad(dias)],
      };
    })
    .sort((a, b) => b.antiguedadDias - a.antiguedadDias);

  const wsDetalle = wb.addWorksheet('Detalle de depósitos');
  wsDetalle.columns = [
    { header: 'Folio', key: 'folio', width: 12 },
    { header: 'Banco', key: 'banco', width: 14 },
    { header: 'Fecha del depósito', key: 'fecha', width: 18 },
    { header: 'Importe', key: 'importe', width: 16 },
    { header: 'Mes de origen', key: 'mesOrigen', width: 14 },
    { header: 'Antigüedad (días)', key: 'antiguedadDias', width: 16 },
    { header: 'Rango', key: 'rango', width: 16 },
  ];
  for (const fila of filasDetalle) wsDetalle.addRow(fila);
  wsDetalle.getColumn('importe').numFmt = '#,##0.00';
  _estilizarHojaCorte(wsDetalle);

  const buffer = Buffer.from(await wb.xlsx.writeBuffer());

  // Sube el Excel a Drive (2026-11-06, pedido explícito del usuario: poder volver a bajar
  // cualquier reporte de un punto pasado de la historia) — best-effort, NUNCA bloquea la
  // descarga que el usuario está pidiendo en este momento: si Drive falla (carpeta sin
  // configurar, cuota, red), se loguea y `driveFileId` queda `null` — esa fila del historial
  // simplemente no será redescargable después.
  const fechaArchivo = new Date().toISOString().slice(0, 10);
  let driveFileId = null;
  try {
    driveFileId = await subirReporteCorte(buffer, `Cortes-${fechaArchivo}.xlsx`);
  } catch (err) {
    logger.warn(`[cortes] No se pudo subir el reporte a Drive, esta descarga no será redescargable después: ${err.message}`);
  }

  const hoy = dias[dias.length - 1];
  await CorteDescarga.create({
    usuario,
    banco,
    depositosNoIdentificados: { count: hoy.total.count, saldo: hoy.total.saldo },
    driveFileId,
  });

  return buffer;
}

/**
 * Descarga el archivo real de una fila del historial de Cortes (ver listarDescargasCorte) desde
 * Drive — permite volver a bajar cualquier reporte generado en el pasado, no solo ver su delta.
 * `driveFileId` es `null` si la subida a Drive falló en el momento de generarlo (ver
 * buildReporteCortes) — ese caso puntual no es descargable después, por diseño (no bloqueante).
 */
async function descargarReporteCorteHistorico(id) {
  const registro = await CorteDescarga.findById(id).lean();
  if (!registro) throw new NotFoundError('Descarga de Cortes');
  if (!registro.driveFileId) {
    throw new BadRequestError('Esta descarga no tiene un archivo disponible para volver a bajar.');
  }
  const buffer = await descargarReporteCorte(registro.driveFileId);
  const fecha = new Date(registro.fecha).toISOString().slice(0, 10);
  return { buffer, filename: `Cortes-${fecha}.xlsx` };
}

/**
 * Historial de descargas de Cortes (2026-11-06), con el delta de "depósitos no identificados"
 * contra la descarga INMEDIATAMENTE ANTERIOR (cronológicamente) con el MISMO `banco` — nunca
 * compara un snapshot de "BBVA" contra uno de "Todos" (`null`). `delta` es `null` en la primera
 * descarga de cada banco, porque no hay contra qué comparar.
 *
 * @param {object} [opts]
 * @param {string} [opts.banco] Si se omite, trae TODOS los bancos mezclados en una sola lista
 *   (cada fila igual se compara solo contra su propio historial por banco, nunca entre sí).
 */
async function listarDescargasCorte({ banco = null } = {}) {
  const filtro = banco ? { banco } : {};
  const descargas = await CorteDescarga.find(filtro).sort({ fecha: -1 }).lean();

  return descargas.map((d, i) => {
    const clave = d.banco ?? null;
    let delta = null;
    for (let j = i + 1; j < descargas.length; j++) {
      if ((descargas[j].banco ?? null) !== clave) continue;
      const anterior = descargas[j].depositosNoIdentificados;
      const actual = d.depositosNoIdentificados;
      delta = {
        countPct: anterior.count === 0 ? null : ((actual.count - anterior.count) / anterior.count) * 100,
        saldoPct: anterior.saldo === 0 ? null : ((actual.saldo - anterior.saldo) / anterior.saldo) * 100,
        direccion: actual.count > anterior.count ? 'subio' : (actual.count < anterior.count ? 'bajo' : 'igual'),
      };
      break;
    }
    return { ...d, delta };
  });
}

/**
 * Excel descargable del corte de conciliación (2026-10-02, pedido explícito del usuario:
 * "que también traiga el detallado de los movimientos involucrados con su clasificación").
 * Reusa getCorteConciliacion() para la hoja "Resumen" — el Excel SIEMPRE coincide con lo que
 * está en pantalla en ese momento, mismo criterio que buildReporteIdentificacion().
 *
 * Hoja "Detalle": un movimiento puede estar "involucrado" en el corte por 2 vías
 * independientes, nunca ambas (las queries no se superponen):
 *   - `fecha < inicio` Y (sigue pendiente AHORA O se identificó DENTRO del periodo) —
 *     "Origen: Rezagado". Backlog ya resuelto hace tiempo (identificado antes del periodo)
 *     queda afuera a propósito: no es parte de la historia de ESTE corte.
 *   - `fecha >= inicio` — "Origen: Nuevo", cualquier estatus (a diferencia de rezagados, acá
 *     SÍ se listan los ya identificados, para que la columna "Identificado en el periodo"
 *     tenga sentido completo sobre el lote nuevo).
 * La columna "Identificado en el periodo" (Sí/No) es la misma condición que usa
 * `identificadosEnPeriodo` del resumen (incluido el scope por `rolEquipo`, 2026-10-09 — ver
 * comentario grande de getCorteConciliacion) — permite filtrar en Excel y que la suma de "Sí"
 * coincida exactamente con `identificadosEnPeriodo.total`.
 */
async function buildReporteCorte({ periodo = 'semanal', banco = null, fechaInicio = null, fechaFin = null, rolEquipo = null } = {}) {
  const corte = await getCorteConciliacion({ periodo, banco, fechaInicio, fechaFin, rolEquipo });
  const equipoIds = await _equipoAuth0SubsParaRol(rolEquipo);
  const inicio = corte.inicio;
  // null en modo tiempo real, Date en histórico — acota las queries de Detalle con $lte
  // cuando exista (mismo criterio de corte que _getCorteHistorico) y habilita la columna
  // "Posible cambio posterior al corte".
  const fin = corte.fin ?? null;
  const baseMatch = buildBaseMatch({ banco });
  const campos = 'banco fecha concepto deposito categoria status primeraIdentificacionAt primeraIdentificacionPor ultimoCambioStatusAt historialVinculacion';

  const [antesDelCorte, nuevos] = await Promise.all([
    BankMovement.find({
      ...baseMatch,
      fecha: { $lt: inicio },
      $or: [
        { status: { $in: BACKLOG_STATUSES } },
        { status: 'identificado', primeraIdentificacionAt: { $gte: inicio, ...(fin ? { $lte: fin } : {}) } },
      ],
    }).select(campos).lean(),
    BankMovement.find({ ...baseMatch, fecha: { $gte: inicio, ...(fin ? { $lte: fin } : {}) } }).select(campos).lean(),
  ]);

  const filas = [
    ...antesDelCorte.map(m => ({ ...m, origen: 'Rezagado' })),
    ...nuevos.map(m => ({ ...m, origen: 'Nuevo' })),
  ].sort((a, b) => new Date(a.fecha) - new Date(b.fecha));

  const wb = new ExcelJS.Workbook();
  wb.creator = 'Numo — Cortes de conciliación';
  wb.created = new Date();

  const resumen = wb.addWorksheet('Resumen');
  resumen.columns = [
    { header: 'Indicador', key: 'k', width: 46 },
    { header: 'Valor',     key: 'v', width: 18 },
  ];

  const resumenHeader = resumen.getRow(1);
  resumenHeader.height = 22;
  resumenHeader.font   = { bold: true, color: { argb: 'FFE0E7FF' }, size: 10 };
  resumenHeader.fill   = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1E1B4B' } };
  resumenHeader.alignment = { vertical: 'middle', horizontal: 'center' };

  resumen.addRow({ k: 'Periodo', v: periodo === 'mensual' ? 'Mensual' : 'Semanal' });
  const desdeRow = resumen.addRow({ k: 'Desde', v: inicio });
  desdeRow.getCell('v').numFmt = 'dd/mm/yyyy hh:mm';
  if (corte.historico) {
    // Corte histórico (2026-10-06): rango de cierre elegido, no "ahora" — mismo numFmt que
    // "Desde" para que ambas fechas se lean igual.
    const hastaRow = resumen.addRow({ k: 'Hasta', v: corte.fin });
    hastaRow.getCell('v').numFmt = 'dd/mm/yyyy hh:mm';
  }
  resumen.addRow({ k: 'Banco', v: banco || 'Todos' });

  // Misma paleta semántica que el hero del panel en pantalla (bank-corte-panel.component.css:
  // rezagados = alerta/ámbar, identificados = positivo/verde; "nuevos" queda neutro porque ahí
  // no hay alarma, solo información del lote del periodo).
  const SECCION_ESTILO = {
    rezagados:     { fill: 'FFFCEFD8', font: 'FF8A5A0C' },
    nuevos:        { fill: 'FFF3F4F6', font: 'FF374151' },
    identificados: { fill: 'FFDFF5EA', font: 'FF0E6B4E' },
  };

  function addSeccionResumen(tipo, titulo, filas) {
    resumen.addRow({});
    const tituloRowNum = resumen.rowCount + 1;
    resumen.addRow({ k: titulo, v: '' });
    resumen.mergeCells(`A${tituloRowNum}:B${tituloRowNum}`);
    const tituloRow = resumen.getRow(tituloRowNum);
    tituloRow.height = 20;
    tituloRow.font = { bold: true, color: { argb: SECCION_ESTILO[tipo].font }, size: 11 };
    tituloRow.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: SECCION_ESTILO[tipo].fill } };

    filas.forEach(({ label, value, esTotal }) => {
      const row = resumen.addRow({ k: label, v: value });
      const celdaK = row.getCell('k');
      const celdaV = row.getCell('v');
      celdaK.alignment = { indent: 1 };
      celdaV.numFmt = '#,##0';
      celdaV.alignment = { horizontal: 'right' };
      if (esTotal) {
        row.font = { bold: true };
        const borde = { top: { style: 'thin', color: { argb: 'FFD1D5DB' } } };
        celdaK.border = borde;
        celdaV.border = borde;
      }
    });
  }

  addSeccionResumen('rezagados', 'Depósitos no identificados iniciales', [
    { label: 'No identificados iniciales — no identificados', value: corte.rezagados.no_identificado },
    { label: 'No identificados iniciales — por conciliar',    value: corte.rezagados.reclasificado },
    { label: 'No identificados iniciales — total',            value: corte.rezagados.total, esTotal: true },
  ]);
  addSeccionResumen('nuevos', 'Nuevos del periodo', [
    { label: 'Nuevos del periodo — no identificados', value: corte.nuevos.no_identificado },
    { label: 'Nuevos del periodo — por conciliar',    value: corte.nuevos.reclasificado },
    { label: 'Nuevos del periodo — identificados',    value: corte.nuevos.identificado },
    { label: 'Nuevos del periodo — total',            value: corte.nuevos.total, esTotal: true },
  ]);
  addSeccionResumen('identificados', 'Identificados en el periodo', [
    { label: 'Identificados en el periodo — de rezago', value: corte.identificadosEnPeriodo.deRezagados },
    { label: 'Identificados en el periodo — de nuevos', value: corte.identificadosEnPeriodo.deNuevos },
    { label: 'Identificados en el periodo — total',     value: corte.identificadosEnPeriodo.total, esTotal: true },
  ]);

  // Corte histórico con hallazgos que no se pueden reconstruir con certeza (ver
  // _estadoAlCierre/ultimoCambioStatusAt) — EXPUESTO siempre de forma visible, nunca oculto,
  // con la MISMA banda de color ámbar que la sección "Rezagados" de arriba.
  if (corte.advertencia) {
    resumen.addRow({});
    const advRow = resumen.addRow({
      k: '⚠ Registros con cambio posterior al corte (revisar manualmente)',
      v: corte.advertencia.registrosConCambioPosteriorAlCierre,
    });
    advRow.height = 20;
    advRow.font = { bold: true, color: { argb: SECCION_ESTILO.rezagados.font }, size: 11 };
    advRow.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: SECCION_ESTILO.rezagados.fill } };
    advRow.getCell('v').numFmt = '#,##0';
    advRow.getCell('v').alignment = { horizontal: 'right' };
  }

  resumen.views = [{ state: 'frozen', ySplit: 1 }];

  const detalle = wb.addWorksheet('Detalle');
  detalle.columns = [
    { header: 'Origen',                     key: 'origen',                width: 12 },
    { header: 'Banco',                      key: 'banco',                 width: 13 },
    { header: 'Fecha depósito',             key: 'fecha',                 width: 18 },
    { header: 'Concepto',                   key: 'concepto',              width: 45 },
    { header: 'Depósito',                   key: 'deposito',              width: 15 },
    { header: 'Categoría',                  key: 'categoria',             width: 18 },
    { header: 'Estatus',                    key: 'status',                width: 16 },
    { header: 'Identificado en el periodo', key: 'identificadoEnPeriodo', width: 20 },
    { header: 'Identificado por',           key: 'identificadoPor',       width: 22 },
    { header: 'Fecha de identificación',    key: 'identificadoAt',        width: 18 },
    { header: 'Posible cambio posterior al corte', key: 'cambioPosterior', width: 26 },
  ];

  for (const m of filas) {
    const identificadoEnPeriodo = m.status === 'identificado'
      && m.primeraIdentificacionAt != null
      && new Date(m.primeraIdentificacionAt) >= inicio
      && _enEquipoRol(m.primeraIdentificacionPor?.userId, equipoIds);
    detalle.addRow({
      origen:                m.origen,
      banco:                 m.banco ?? null,
      fecha:                 m.fecha ?? null,
      concepto:              m.concepto ?? null,
      deposito:              m.deposito ?? null,
      categoria:             m.categoria ?? null,
      // "reclasificado" se muestra como "Por conciliar" (rename de UI, 2026-10-06, pedido
      // explícito del usuario — el valor interno del campo `status` en Mongo NO cambia, solo
      // el texto que ve el usuario en este Excel y en el panel de Cortes).
      status:                m.status === 'reclasificado' ? 'Por conciliar' : (m.status ?? null),
      identificadoEnPeriodo: identificadoEnPeriodo ? 'Sí' : 'No',
      identificadoPor:       m.primeraIdentificacionPor?.nombre ?? m.primeraIdentificacionPor?.userId ?? null,
      identificadoAt:        m.primeraIdentificacionAt ?? null,
      // Modo tiempo real: el concepto no aplica (no hay "cierre" congelado) — guion, no un
      // 'No' que insinúe certeza que no existe. Modo histórico: ver _estadoAlCierre arriba.
      cambioPosterior:       fin ? (_estadoAlCierre(m, fin).posibleCambio ? 'Sí' : 'No') : '—',
    });
  }

  const dateFmt = 'dd/mm/yyyy hh:mm';
  ['fecha', 'identificadoAt'].forEach(key => { detalle.getColumn(key).numFmt = dateFmt; });
  detalle.getColumn('deposito').numFmt = '#,##0.00';

  const headerRow = detalle.getRow(1);
  headerRow.height = 22;
  headerRow.font   = { bold: true, color: { argb: 'FFE0E7FF' }, size: 10 };
  headerRow.fill   = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1E1B4B' } };
  headerRow.alignment = { vertical: 'middle', horizontal: 'center' };

  if (detalle.lastColumn) detalle.autoFilter = { from: 'A1', to: detalle.lastColumn.letter + '1' };
  detalle.views = [{ state: 'frozen', ySplit: 1 }];

  return wb.xlsx.writeBuffer();
}

// promedio/mediana/_matchScopeUserId también se exportan para
// collection-request-indicadores.service.js (mismo dominio conceptual — tiempo de
// identificación — pero acotado a Solicitudes de Cobro, ver ese archivo).
module.exports = {
  getIndicadoresIdentificacion,
  getCorteConciliacion,
  getProgresoResolucionPorCorte,
  getPeriodoCortePorRol,
  buildReporteCorte,
  buildReporteIdentificacion,
  listUsuariosConIdentificaciones,
  horasHabilesEntre,
  promedio,
  mediana,
  _matchScopeUserId,
  // Tolerancia de saldo restante bajo (2026-09-21, dashboard de Cobranza) — exportadas para
  // poder testearlas directo, mismo criterio que el resto de funciones puras de este archivo.
  _calificaPorSaldoRestanteBajo,
  _masRecienteIdentificadoPor,
  _proxyFechaEnRango,
  _enScopeUserId,
  _resolverCasiIdentificadosPorSaldoBajo,
  SALDO_RESTANTE_TOLERANCIA,
  // "CxC retrasadas" del corte (2026-10-22) — exportadas para poder testearlas directo, mismo
  // criterio que el resto de funciones puras de este archivo.
  _folioFechaCodificada,
  _mesesDeAtraso,
  _calcularCxcRetrasadas,
  // Progreso de resolución (2026-10-30) — exportadas para testearlas directo.
  _enumerarCortesSemanales,
  // Ledger genérico de backlog (2026-10-30, reemplaza la primera versión de
  // getProgresoResolucionPorCorte) — exportadas para testearlas directo.
  getLedgerDiario,
  _calcularLedgerBacklog,
  _enumerarDiasEnRango,
  _resolverMesOrigen,
  _fechaMexicoStr,
  // Fallback de primeraIdentificacionAt vía identificadoPor[] (2026-11-06, bug real) —
  // exportada para testearla directo.
  _primeraIdentificacionEfectiva,
  _estadoAlCierre,
  // Dashboard mensual/por hora/feed de actividad (2026-10-30, reemplaza la vista de antigüedad
  // por rango de días — descartada a pedido del usuario tras ver un mockup de referencia) —
  // exportadas para testearlas directo, mismo criterio que el resto de este archivo.
  getLedgerMensual,
  _enumerarMesesParaLedger,
  getLedgerPorHora,
  _enumerarHorasDelDia,
  getUltimosMovimientos,
  // Antigüedad del backlog vacío (2026-11-06) — exportadas para testearlas directo.
  getAntiguedadDepositos,
  getAntiguedadDepositosPorAnio,
  getAntiguedadPorDiaMesActual,
  _antiguedadCandidatosHoy,
  _antiguedadCandidatosHistorico,
  _bucketsAntiguedadAlCorte,
  _vacioAlCorteHistorico,
  _primeraResueltaEfectiva,
  _rangoAntiguedad,
  ANTIGUEDAD_RANGOS,
  // Exportar + historial de descargas de Cortes (2026-11-06) — exportadas para testearlas directo.
  buildReporteCortes,
  listarDescargasCorte,
  descargarReporteCorteHistorico,
};
