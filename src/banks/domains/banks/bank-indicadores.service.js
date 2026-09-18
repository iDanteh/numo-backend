'use strict';

const ExcelJS = require('exceljs');
const BankMovement = require('./BankMovement.model');
const { _rangoAnioMesMexico, _inicioDiaMx, _finDiaMx } = require('./bank.service');

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
// decisión explícita del usuario (2026-08-17): 8:00-20:00, lunes a SÁBADO (el sábado
// cuenta como día laboral completo — el usuario dijo "excluye noches y domingos", no
// "fines de semana"). Domingo completo = 0 horas hábiles sin importar el horario.
const HORA_INICIO_LABORAL = 8;
const HORA_FIN_LABORAL    = 20;
const DIA_DOMINGO         = 0; // Date#getDay()

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

/**
 * Horas hábiles entre 2 timestamps: lunes-sábado, 8:00-20:00 EN HORA DE MÉXICO (ver
 * _comoRelojMexico — 2026-09-09, antes usaba hora local del proceso, lo que rompía el cálculo
 * si el contenedor corre en UTC). Domingo completo y las horas fuera de 8-20 en cualquier día
 * NO cuentan. Recorre día por día (acotado: la cantidad de días entre inicio/fin de un caso
 * real de identificación es chica, nunca miles) y suma el solape de cada día con la ventana
 * [inicio, fin] — así una franja que cruza varios días (ej. viernes a la noche → lunes) se
 * reparte bien entre los días que sí cuentan.
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
    if (cursorMx.getUTCDay() !== DIA_DOMINGO) {
      const ventanaInicio = new Date(cursorMx); ventanaInicio.setUTCHours(HORA_INICIO_LABORAL, 0, 0, 0);
      const ventanaFin    = new Date(cursorMx); ventanaFin.setUTCHours(HORA_FIN_LABORAL, 0, 0, 0);
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
 * @param {object} [opts] Ver `_resolverMatchTiempo()`.
 */
async function getIndicadoresIdentificacion(opts = {}) {
  const { banco, categoria } = opts;
  const matchTiempo = _resolverMatchTiempo(opts);
  const matchSoloBancoCategoria = { ...buildBaseMatch({ banco, categoria }), createdAt: { $gte: INDICADORES_DESDE } };

  const [identificados, backlogAgg] = await Promise.all([
    // Trae los documentos ya identificados (equipo completo, desde INDICADORES_DESDE) para
    // calcular horas hábiles en JS — ver horasHabilesEntre() arriba sobre por qué esto no
    // se hace dentro de la agregación de Mongo. Con los volúmenes actuales (cientos/pocos
    // miles desde el cutoff) traer los documentos a Node es perfectamente razonable; si el
    // volumen creciera mucho con los años, valdría la pena revisar el enfoque (ej. mover el
    // cálculo a un job que lo materialice), pero no hace falta resolverlo ahora.
    BankMovement.find(matchTiempo)
      .select('createdAt primeraIdentificacionAt primeraIdentificacionPor')
      .lean(),
    // Pipeline 2 — backlog de pendientes (no_identificado + reclasificado, BACKLOG_STATUSES)
    // por antigüedad (sin year/month; equipo completo), desde INDICADORES_DESDE. Sigue en
    // tiempo de RELOJ a propósito — el usuario pidió horas hábiles para los promedios, no
    // para la antigüedad del backlog (decisión de alcance explícita, no un olvido).
    BankMovement.aggregate([
      { $match: { ...matchSoloBancoCategoria, status: { $in: BACKLOG_STATUSES } } },
      { $project: { horas: { $divide: [{ $subtract: ['$$NOW', '$createdAt'] }, MS_PER_HOUR] } } },
      { $bucket: { groupBy: '$horas', boundaries: BACKLOG_BOUNDARIES, default: 'otro', output: { count: { $sum: 1 } } } },
    ], { allowDiskUse: true }),
  ]);

  const conHoras = identificados.map(d => ({
    horas:  horasHabilesEntre(d.createdAt, d.primeraIdentificacionAt),
    userId: d.primeraIdentificacionPor?.userId ?? null,
    nombre: d.primeraIdentificacionPor?.nombre ?? null,
  }));
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

// promedio/mediana/_matchScopeUserId también se exportan para
// collection-request-indicadores.service.js (mismo dominio conceptual — tiempo de
// identificación — pero acotado a Solicitudes de Cobro, ver ese archivo).
module.exports = {
  getIndicadoresIdentificacion,
  buildReporteIdentificacion,
  listUsuariosConIdentificaciones,
  horasHabilesEntre,
  promedio,
  mediana,
  _matchScopeUserId,
};
