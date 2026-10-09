'use strict';

// netpay-reporte.service.js — Implementación 1 de "Netpay: carga manual del reporte como
// fuente de verdad" (ver plan y NetpayReporte.model.js para el porqué). Conciliación 1:1
// (un reporte == un depósito == a lo sumo UN BankMovement) — a diferencia del matching
// automático (netpay-match.service.js), que puede requerir 1 o 2 movimientos (split), acá
// el reporte YA trae el monto exacto depositado por Netpay, así que basta un solo
// BankMovement candidato.

const BankMovement = require('../banks/BankMovement.model');
const NetpayReporte = require('./NetpayReporte.model');
const NetpayMatch = require('./NetpayMatch.model');
const NetpayFolioRegistro = require('./NetpayFolioRegistro.model');
const { setErpIds, ERP_TOLERANCE } = require('../banks/bank.service');
const { buscarTransaccionesNetpay } = require('./kore-caja.service');
const { _ventanaDiasNetpay, _montosIguales: _montosIgualesCompartido } = require('./netpay-match.service');
const { NotFoundError, BadRequestError, ConflictError, UnprocessableError } = require('../../shared/errors/AppError');
// Estados desde los que rechazarReporte YA NO puede actuar — mismo criterio EXACTO que
// netpay-resolver.service.js#ESTATUS_TERMINALES ("any active state -> rechazado",
// spec.md), aplicado a NetpayReporte.
const ESTATUS_TERMINALES = new Set(['rechazado', 'resuelto_manual']);
const { parseNetpayReporte } = require('./netpay-reporte-parser.service');
const { emitToBanco, emitToAll, emitToUser } = require('../../shared/socket');
const { logger } = require('../../../shared/utils/logger');
const { sincronizarComisiones } = require('./netpay-comision-sync.service');

const PREFIJO_ERP_ID_REPORTE = 'NETPAYRPT-';

// netpay-match.service.js exporta _montosIguales pero este archivo se mockea completo en
// los tests de netpay-reporte-revert/netpay-match-confirm (solo exponen _ventanaDiasNetpay)
// — se mantiene la implementación PROPIA (idéntica, ERP_TOLERANCE real) como fallback para
// no romper esos mocks existentes; cuando el mock SÍ expone _montosIguales (tests de este
// archivo), se usa esa.
function _montosIguales(a, b) {
  if (typeof _montosIgualesCompartido === 'function') return _montosIgualesCompartido(a, b);
  return Math.abs((a ?? 0) - (b ?? 0)) <= ERP_TOLERANCE;
}

function _erpIdReporte(claveRastreo) {
  return `${PREFIJO_ERP_ID_REPORTE}${claveRastreo}`;
}

// Usuario sintético para las escrituras automáticas de evaluarReporte — mismo patrón
// EXACTO que netpay-evaluacion.service.js#USUARIO_MOTOR_EVALUACION /
// collection-requests/anticipo-generado.service.js#USUARIO_MOTOR_ANTICIPO: setErpIds()
// exige un `user` real con permiso banks:erp:link/banks:cobro, role:'admin' resuelve el
// permiso vía el wildcard '*' ya sembrado, sin replicar esa lógica acá.
const USUARIO_MOTOR_REPORTE = Object.freeze({
  _id: 'motor-netpay-reporte', role: 'admin', nombre: 'Motor de Evaluación de Reportes Netpay (automático)',
});

// Candidatos BBVA para el depósito del reporte — mismo criterio de ventana de días y
// tolerancia que netpay-match.service.js (_ventanaDiasNetpay, ERP_TOLERANCE), pero acá NO
// hay agrupación/split: se compara 1 BankMovement a la vez contra montoDepositoTotal (el
// reporte YA es el monto exacto, no hay comisión errónea de Kore de por medio).
//
// Recibe el reporte (o su shape parseado, antes de persistir) en vez de los valores sueltos
// — reusada tanto por cargarReporte (reporte recién parseado, todavía no guardado) como por
// buscarCandidatos (reporte ya persistido, ver más abajo) sin duplicar el criterio de
// búsqueda entre ambos casos.
async function _buscarCandidatosParaReporte(reporte) {
  const ventanaDias = await _ventanaDiasNetpay();
  const msVentana = ventanaDias * 24 * 60 * 60 * 1000;
  const fechaMovimiento = new Date(reporte.fechaMovimiento);
  const desde = new Date(fechaMovimiento.getTime() - msVentana);
  const hasta = new Date(fechaMovimiento.getTime() + msVentana);

  const pool = await BankMovement.find({
    banco: 'BBVA', erpLinks: { $size: 0 }, status: { $ne: 'identificado' },
    fecha: { $gte: desde, $lte: hasta },
  }).lean();

  return pool.filter(m => _montosIguales(m.deposito, reporte.montoDepositoTotal));
}

// _buscarCandidatosEnVentana — GET /netpay/reporte/:id/candidatos?modo=ventana (design.md
// API table: "Adds the window mode"). A diferencia de _buscarCandidatosParaReporte (que
// filtra por _montosIguales), acá el reporte YA está en discrepancia — el diálogo de
// resolver manual necesita ver TODOS los elegibles de la ventana, aunque su monto no
// calce, ordenados por |diferencia| ascendente. Mismo criterio EXACTO que
// netpay-resolver.service.js#candidatos (equivalente a nivel bucket).
async function _buscarCandidatosEnVentana(reporte) {
  const ventanaDias = await _ventanaDiasNetpay();
  const msVentana = ventanaDias * 24 * 60 * 60 * 1000;
  const fechaMovimiento = new Date(reporte.fechaMovimiento);
  const desde = new Date(fechaMovimiento.getTime() - msVentana);
  const hasta = new Date(fechaMovimiento.getTime() + msVentana);

  const pool = await BankMovement.find({
    banco: 'BBVA', erpLinks: { $size: 0 }, status: { $ne: 'identificado' },
    fecha: { $gte: desde, $lte: hasta },
  }).lean();

  return pool
    .map(m => ({ ...m, diferencia: (m.deposito ?? 0) - (reporte.montoDepositoTotal ?? 0) }))
    .sort((a, b) => Math.abs(a.diferencia) - Math.abs(b.diferencia));
}

// _clave — clave determinística de idempotencia de un folio (design.md
// "Idempotent folios" / NetpayFolioRegistro.model.js): orderId cuando existe, o
// 'REF:'+referencia cuando el folio no trae Order ID.
function _clave(folio) {
  return folio.orderId ? folio.orderId : `REF:${folio.referencia}`;
}

// _registrarFoliosIdempotente — netpay-matching-v2 (design.md "Idempotent folios"):
// insertMany(ordered:false) contra NetpayFolioRegistro — Mongo mismo rechaza (E11000) cada
// fila cuyo `clave` ya esté registrada por OTRO reporte, sin necesitar un pre-chequeo (que
// tendría ventana de carrera entre dos cargas casi simultáneas). Un folio duplicado NUNCA
// aborta el resto: la fila queda marcada (folios[i].duplicadoDeReporteId) pero el reporte
// sigue su curso normal — evaluarReporte() decide su estatus aparte, sin importar esto.
async function _registrarFoliosIdempotente(reporte) {
  const folios = reporte.folios ?? [];
  if (folios.length === 0) return;

  const registros = folios.map(f => ({
    clave: _clave(f), reporteId: reporte._id, orderId: f.orderId ?? null, referencia: f.referencia ?? null,
  }));

  let indicesFallidos = [];
  try {
    await NetpayFolioRegistro.insertMany(registros, { ordered: false });
  } catch (err) {
    const writeErrors = err.writeErrors ?? [];
    const noEsperados = writeErrors.filter(we => (we.code ?? we.err?.code) !== 11000);
    if (writeErrors.length === 0 || noEsperados.length > 0) throw err; // error real, nunca se absorbe en silencio
    indicesFallidos = writeErrors.map(we => we.index ?? we.err?.index);
  }
  if (indicesFallidos.length === 0) return;

  for (const idx of indicesFallidos) {
    // eslint-disable-next-line no-await-in-loop
    const original = await NetpayFolioRegistro.findOne({
      clave: registros[idx].clave, reporteId: { $ne: reporte._id },
    }).lean();
    reporte.folios[idx].duplicadoDeReporteId = original?.reporteId ?? null;
  }
  await reporte.save();
}

// _poblarMovimientoVinculado — feature "navegación al movimiento bancario" (2026-10-01,
// pedido explícito del usuario): agrega un resumen liviano (banco/fecha/monto) del
// BankMovement vinculado a CUALQUIER reporte que se devuelva al frontend, para que el
// detalle lo muestre sin tener que navegar a Bancos primero y habilite el botón "Ver
// movimiento bancario" (banco/movId ya alcanzan para el deep-link existente de Bancos,
// ver banks.component.ts#openBank). null si no hay movementIdConfirmado, o si el
// movimiento referenciado ya no existe (NUNCA lanza — es informativo, no debe romper el
// detalle de un reporte por esto).
async function _poblarMovimientoVinculado(reporte) {
  if (!reporte) return reporte;
  if (!reporte.movementIdConfirmado) return { ...reporte, movimientoVinculado: null };
  const mov = await BankMovement.findById(reporte.movementIdConfirmado).lean();
  return {
    ...reporte,
    movimientoVinculado: mov ? { banco: mov.banco, fecha: mov.fecha, monto: mov.deposito } : null,
  };
}

// _derivarSucursales — netpay-reporte-global (design.md "Interfaces/Contracts"): sucursal y
// terminalID viven por FOLIO (pueden variar dentro del mismo depósito si el depósito agrupa
// varias cajas/terminales de la misma tienda) — para el listado de resultados del upload se
// necesita un resumen a nivel depósito: los valores ÚNICOS y no-nulos vistos en sus folios.
function _derivarSucursales(folios) {
  const sucursales = [...new Set((folios ?? []).map(f => f.sucursal).filter(Boolean))];
  const terminalIDs = [...new Set((folios ?? []).map(f => f.terminalID).filter(Boolean))];
  return { sucursales, terminalIDs };
}

// _itemBase — campos comunes a CUALQUIER entrada de `reportes[]` (netpay-reporte-global,
// design.md "Interfaces/Contracts"), sin importar en qué `estatusCarga` haya terminado.
function _itemBase(unit) {
  const { sucursales, terminalIDs } = _derivarSucursales(unit.folios);
  return {
    claveRastreo: unit.claveRastreo,
    fechaMovimiento: unit.fechaMovimiento,
    montoDepositoTotal: unit.montoDepositoTotal,
    sucursales,
    terminalIDs,
  };
}

// _crearYEvaluar — create -> _registrarFoliosIdempotente -> consultarFoliosPendientes ->
// evaluarReporte, SIN capturar errores de create/registrar/evaluar (los propaga tal cual) —
// reusada tanto por el camino N=1 (que necesita distinguir 409/rethrow de "ya existe") como
// por _procesarDeposito (N>1, que clasifica el resultado en vez de lanzar).
//
// koreCache completo desde la carga (2026-10-06, pedido explícito del usuario): antes
// koreCache solo se llenaba al abrir un folio a mano o al exportar el Excel — "incompleto por
// default" hasta que alguien lo pidiera. Consultarlo ACÁ, apenas se crea el reporte, aprovecha
// que la venta todavía está fresca en Kore (evita el riesgo real de folios viejos que Kore ya
// no tiene, ver scripts/backfill-netpay-kore-cache.js, que sigue existiendo para el historial
// previo a este cambio). Va ANTES de evaluarReporte() a propósito: esa función siempre
// re-lee el documento con `findById` (nunca reusa el `reporte` en memoria de arriba), así que
// el `reporteEvaluado` que devolvemos ya sale con los folios completos, sin necesidad de un
// segundo fetch.
//
// Best-effort, nunca aborta la carga: un fallo acá (Kore caído, 429 agotado) no debe impedir
// que el reporte quede creado y evaluado — mismo criterio de fallo parcial que
// consultarFoliosPendientes ya aplica folio por folio puertas adentro.
async function _crearYEvaluar(unit, nombreArchivo, user) {
  const reporte = await NetpayReporte.create({
    ...unit,
    estatus: 'discrepancia',
    cargadoPor: { userId: user?._id ?? null, nombre: user?.nombre || user?.email || null },
    cargadoEn: new Date(),
    nombreArchivoOriginal: nombreArchivo ?? null,
  });

  await _registrarFoliosIdempotente(reporte);

  // Sincronización de comisiones hacia Configuraciones Globales (2026-10-07, pedido explícito
  // del usuario) — va ANTES de consultarFoliosPendientes/evaluarReporte a propósito: la
  // comisión por folio ya está completa desde el parseo del Excel, no depende de Kore ni del
  // resultado del matching. sincronizarComisiones() ya es best-effort puertas adentro (nunca
  // tira), este try/catch es defensa en profundidad, mismo patrón que el resto de la función.
  try {
    await sincronizarComisiones(reporte);
  } catch (err) {
    logger.warn(`[NetpayReporte] no se pudo sincronizar comisiones a Configuraciones Globales para el reporte ${reporte._id}: ${err.message}`);
  }

  try {
    await consultarFoliosPendientes(reporte._id);
  } catch (err) {
    logger.warn(`[NetpayReporte] no se pudo completar koreCache al cargar el reporte ${reporte._id}: ${err.message}`);
  }

  const { reporte: reporteEvaluado, candidatos } = await evaluarReporte(reporte._id);
  return { reporte: reporteEvaluado, candidatos, reporteId: reporte._id };
}

// _procesarDeposito — netpay-matching-v2 Fase netpay-reporte-global (design.md
// "Per-deposit failure" + "Duplicate" + "Deposit with 0 folios"): versión N>1 de la lógica
// de arriba, pero clasificando el resultado en vez de lanzar — una falla acá NUNCA aborta el
// resto del archivo (ver cargarReporte, bucle secuencial).
//   0 folios          -> estatusCarga:'error' (zero writes, ni busca duplicado — nada que
//                        pudiera estar duplicado)
//   ya existe          -> estatusCarga:'ya_cargado', reporteId del documento existente
//   create() E11000    -> carrera con otra carga casi simultánea -> 'ya_cargado' igual,
//                        resuelto con un 2do findOne para obtener el _id real
//   cualquier otro error (create, registrar folios, o evaluar) -> estatusCarga:'error' con
//                        el mensaje; si el create ya había tenido éxito, el reporte QUEDA
//                        creado (discrepancia, re-evaluable) y se incluye su reporteId.
async function _procesarDeposito(unit, nombreArchivo, user) {
  const base = _itemBase(unit);

  if ((unit.folios ?? []).length === 0) {
    return { ...base, estatusCarga: 'error', error: 'El depósito no tiene folios asociados — no se puede cargar.' };
  }

  const existente = await NetpayReporte.findOne({ claveRastreo: unit.claveRastreo }).lean();
  if (existente) {
    return { ...base, estatusCarga: 'ya_cargado', reporteId: existente._id };
  }

  let creado;
  try {
    creado = await _crearYEvaluar(unit, nombreArchivo, user);
  } catch (err) {
    if (err.code === 11000) {
      const original = await NetpayReporte.findOne({ claveRastreo: unit.claveRastreo }).lean();
      return { ...base, estatusCarga: 'ya_cargado', reporteId: original?._id ?? null };
    }
    return { ...base, estatusCarga: 'error', error: err.message };
  }

  return {
    ...base, estatusCarga: 'creado', reporte: creado.reporte, candidatos: creado.candidatos, reporteId: creado.reporteId,
  };
}

// cargarReporte — parsea el Excel (ahora SIEMPRE `{ depositos: Parsed[] }`, ver
// netpay-reporte-parser.service.js) y procesa cada depósito. Ya NO deja ningún reporte en
// 'pendiente' — ese valor no existe en el enum v2 (ver NetpayReporte.model.js#estatus).
//
// N=1 (spec.md "Backward-compatible response shape"): comportamiento IDÉNTICO al de hoy —
// duplicado -> ConflictError (409), cualquier otro error -> se propaga tal cual (nunca se
// "atrapa" para convertirlo en un item de lista), éxito -> `{reporte, candidatos}` como
// siempre, MÁS `reportes:[...]` con la misma forma que usa el caso N>1 (para que un cliente
// ya migrado a la forma nueva funcione igual con archivos de un solo depósito).
//
// N>1 (design.md "Loop"): SECUENCIAL (`for...of`, nunca `Promise.all`) — dos depósitos con
// el mismo monto en la misma ventana podrían disputar el mismo BankMovement si corrieran en
// paralelo; al procesar en orden, el depósito #2 ya ve el erpLink que dejó el #1 (vía el
// filtro `erpLinks:{$size:0}` de _buscarCandidatosParaReporte). Cada depósito es
// independiente (un error en uno nunca bloquea a los demás) y la respuesta es SIEMPRE 200.
// Recordatorio de carga (pedido explícito del usuario, 2026-10-08): avisa EN VIVO a quien
// tenga Bancos abierto que el reporte del día ya se cargó — sin esto, un contador que ya
// tiene el recordatorio en pantalla solo se enteraría al recargar la página. Un solo emit por
// archivo subido (no uno por depósito) — a este evento no le importa cuántos depósitos trajo,
// solo que "alguien ya cargó hoy".
function _emitirReporteCargado(nombreArchivo, user) {
  emitToAll('netpay-reporte:cargado', {
    cargadoPor: { userId: user?._id ?? null, nombre: user?.nombre || user?.email || null },
    cargadoEn: new Date(),
    nombreArchivoOriginal: nombreArchivo ?? null,
  });
}

// _procesarDepositosParseados — núcleo real de cargarReporte(), extraído tal cual (sin
// cambios de comportamiento) para que la carga EN BACKGROUND (iniciarCargaReporteJob, más
// abajo) pueda reusarlo con un callback de progreso — cargarReporte() sigue siendo la versión
// síncrona de siempre, usada tal cual por los tests existentes y por cualquier otro llamador
// que no necesite progreso. `onProgress` es opcional (null en el camino síncrono de toda la
// vida) — se invoca UNA vez para N=1 (no hay progreso intermedio posible: es un solo
// depósito) y una vez POR DEPÓSITO en N>1.
async function _procesarDepositosParseados(depositos, nombreArchivo, user, onProgress) {
  if (depositos.length === 1) {
    const [unit] = depositos;
    const existente = await NetpayReporte.findOne({ claveRastreo: unit.claveRastreo }).lean();
    if (existente) {
      throw new ConflictError(`Ya existe un reporte cargado para este depósito (claveRastreo=${unit.claveRastreo}).`);
    }

    let creado;
    try {
      creado = await _crearYEvaluar(unit, nombreArchivo, user);
    } catch (err) {
      if (err.code === 11000) {
        throw new ConflictError(`Ya existe un reporte cargado para este depósito (claveRastreo=${unit.claveRastreo}).`);
      }
      throw err;
    }

    onProgress?.({ procesados: 1, total: 1 });
    const item = {
      ..._itemBase(unit), estatusCarga: 'creado', reporte: creado.reporte, candidatos: creado.candidatos, reporteId: creado.reporteId,
    };
    _emitirReporteCargado(nombreArchivo, user);
    return { reporte: creado.reporte, candidatos: creado.candidatos, reportes: [item] };
  }

  const reportes = [];
  for (const [idx, unit] of depositos.entries()) {
    // eslint-disable-next-line no-await-in-loop
    reportes.push(await _procesarDeposito(unit, nombreArchivo, user));
    onProgress?.({ procesados: idx + 1, total: depositos.length });
  }

  const resumen = {
    total: reportes.length,
    creados: reportes.filter(r => r.estatusCarga === 'creado').length,
    yaCargados: reportes.filter(r => r.estatusCarga === 'ya_cargado').length,
    errores: reportes.filter(r => r.estatusCarga === 'error').length,
  };

  _emitirReporteCargado(nombreArchivo, user);
  return { reportes, resumen };
}

async function cargarReporte(buffer, nombreArchivo, user) {
  let parsed;
  try {
    parsed = await parseNetpayReporte(buffer);
  } catch (err) {
    if (err instanceof BadRequestError) throw err;
    throw new BadRequestError(`Error al leer el archivo: ${err.message}`);
  }

  return _procesarDepositosParseados(parsed.depositos, nombreArchivo, user, null);
}

// ── Carga en BACKGROUND (pedido explícito del usuario, 2026-10-08) ──────────────────────
// Incidente real: un archivo de 33 depósitos/897 folios se cortó a los 5 minutos por el
// req.setTimeout/res.setTimeout explícito de la ruta de upload — Node destruye el socket solo
// cuando nadie escucha su evento 'timeout', sin que quede NINGÚN rastro en los logs de la
// aplicación (no fue una excepción, fue la conexión muriendo a nivel de transporte). La carga
// de reportes grandes (muchos depósitos, cada uno con su propia tanda de consultas a Kore) va
// a seguir siendo recurrente, así que esto deja de depender de que una sola conexión HTTP
// aguante varios minutos sin cortarse.
//
// Mismo patrón YA establecido en este proyecto para trabajos largos (ver Sync ERP-Kore en
// erp.routes.js): la ruta responde 202+jobId de inmediato (después de parsear el archivo, que
// es rápido y 100% local — un Excel inválido sigue fallando al instante, sin pasar por el
// job), el trabajo pesado corre sin bloquear la respuesta, y el progreso/resultado final se
// emite por socket a QUIEN LO SUBIÓ (emitToUser, no emitToAll — es una acción personal, nadie
// más necesita ver el progreso de la carga de otra persona).
//
// A diferencia de Sync ERP-Kore (un solo job global a la vez, guard de un booleano a nivel de
// módulo), una carga de Netpay es una acción personal de quien la sube — varias personas (o la
// misma, con archivos distintos) pueden tener su propia carga corriendo en simultáneo sin
// pisarse, así que NO hay guard de exclusión mutua acá: el peor caso de una doble carga
// accidental es trabajo duplicado, nunca corrupción (cada depósito ya es idempotente por
// claveRastreo, ver _procesarDeposito/cargarReporte de arriba).
const NETPAY_UPLOAD_JOBS = new Map(); // jobId -> { status, userId, nombreArchivo, procesados, total, resultado?, error? }
const NETPAY_UPLOAD_JOB_TTL = 2 * 60 * 60 * 1000; // 2h — mismo criterio que SYNC_JOB_TTL (erp.routes.js)

function _nuevoJobIdCarga() {
  return `netpay-upload-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

async function iniciarCargaReporteJob(buffer, nombreArchivo, user) {
  let parsed;
  try {
    parsed = await parseNetpayReporte(buffer);
  } catch (err) {
    if (err instanceof BadRequestError) throw err;
    throw new BadRequestError(`Error al leer el archivo: ${err.message}`);
  }

  const jobId = _nuevoJobIdCarga();
  const total = parsed.depositos.length;
  NETPAY_UPLOAD_JOBS.set(jobId, {
    status: 'running', userId: user?._id ?? null, nombreArchivo, procesados: 0, total,
  });

  // Sin await a propósito — es justo lo que permite responder 202 de inmediato sin bloquear
  // la request mientras esto corre.
  _correrCargaReporteJob(jobId, parsed.depositos, nombreArchivo, user);

  return { jobId, total };
}

async function _correrCargaReporteJob(jobId, depositos, nombreArchivo, user) {
  const onProgress = ({ procesados, total }) => {
    const job = NETPAY_UPLOAD_JOBS.get(jobId);
    if (job) { job.procesados = procesados; job.total = total; }
    emitToUser(user?._id, 'netpay-reporte:upload:progress', {
      jobId, procesados, total, pct: Math.round((procesados / total) * 100),
    });
  };

  try {
    const resultado = await _procesarDepositosParseados(depositos, nombreArchivo, user, onProgress);
    NETPAY_UPLOAD_JOBS.set(jobId, {
      status: 'done', userId: user?._id ?? null, nombreArchivo, procesados: depositos.length, total: depositos.length, resultado,
    });
    emitToUser(user?._id, 'netpay-reporte:upload:done', { jobId, resultado });
  } catch (err) {
    // ConflictError/BadRequestError (claveRastreo duplicado en N=1, o un Excel que pasó el
    // parseo inicial pero falló después) traen un mensaje ya pensado para mostrarse tal
    // cual; cualquier otro error es inesperado y se envuelve para no filtrar detalles internos.
    const esErrorDeNegocio = err instanceof BadRequestError || err instanceof ConflictError;
    const mensaje = esErrorDeNegocio ? err.message : `Error inesperado al procesar el archivo: ${err.message}`;
    NETPAY_UPLOAD_JOBS.set(jobId, {
      status: 'error', userId: user?._id ?? null, nombreArchivo, procesados: 0, total: depositos.length, error: mensaje,
    });
    emitToUser(user?._id, 'netpay-reporte:upload:error', { jobId, error: mensaje });
    if (!esErrorDeNegocio) logger.error(`[NetpayReporte] job de carga ${jobId} falló de forma inesperada: ${err.message}`);
  } finally {
    setTimeout(() => NETPAY_UPLOAD_JOBS.delete(jobId), NETPAY_UPLOAD_JOB_TTL);
  }
}

// Fallback de recuperación tras un reload de página (mismo criterio que GET
// /sync-erp-kore/:jobId/status) — el socket es la vía normal para enterarse del progreso;
// esto es solo para cuando alguien recarga a mitad del job y pierde la suscripción en memoria.
// A diferencia del job global de Sync ERP-Kore, acá SÍ hay chequeo de dueño (userId) — varias
// cargas de distintas personas conviven en el mismo Map.
function obtenerEstadoJobCarga(jobId, userId) {
  const job = NETPAY_UPLOAD_JOBS.get(jobId);
  if (!job || String(job.userId) !== String(userId)) return null;
  const { userId: _userId, ...estado } = job;
  return estado;
}

// dateFrom/dateTo (pedido explícito del usuario, 2026-10-07): filtra por fechaMovimiento
// (fecha real del depósito) — para no tener que scrollear la lista completa buscando un
// depósito de una fecha puntual. A diferencia del filtro `dia` que ya usa GET /netpay/bandeja
// (erp.routes.js, $lte a medianoche), acá $lte se arma a FIN de día: fechaMovimiento es un
// Date con posible componente de hora real (no un bucket de día ya truncado a medianoche como
// `dia`), así que cortar a las 00:00:00 excluiría depósitos del propio día de corte.
// Buscador por clave de rastreo o importe (pedido explícito del usuario, 2026-10-08) —
// mismo criterio de tolerancia por monto ya usado en collection-request.service.js#
// _buildBusquedaFilter: sin decimales -> ±1 peso; 1 decimal -> ±0.05; 2+ decimales -> ±0.005.
// Agrupar por archivo (ver frontend) hace que la clave de rastreo sola ya no alcance para
// ubicar un depósito puntual entre varios del mismo archivo — este buscador es el mecanismo
// para eso.
function _buildBusquedaFilter(search) {
  if (!search) return null;
  const esc = search.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const re  = new RegExp(esc, 'i');
  const orClauses = [{ claveRastreo: re }];

  const cleanNum = search.replace(/[$,\s]/g, '');
  const num = parseFloat(cleanNum);
  if (!isNaN(num) && num > 0) {
    const decimalPlaces = (cleanNum.split('.')[1] || '').length;
    const tolerance = decimalPlaces === 0 ? 1 : decimalPlaces === 1 ? 0.05 : 0.005;
    orClauses.push({ montoDepositoTotal: { $gte: num - tolerance, $lte: num + tolerance } });
  }

  return { $or: orClauses };
}

async function listar({ estatus, incluirEliminados = false, dateFrom, dateTo, search } = {}) {
  const filter = {};
  if (estatus) filter.estatus = estatus;
  if (!incluirEliminados) filter.eliminado = { $ne: true };
  if (dateFrom) {
    const desde = new Date(`${dateFrom}T00:00:00.000Z`);
    if (isNaN(desde.getTime())) throw new BadRequestError(`dateFrom inválido: ${dateFrom}`);
    filter.fechaMovimiento = { ...(filter.fechaMovimiento ?? {}), $gte: desde };
  }
  if (dateTo) {
    const hasta = new Date(`${dateTo}T23:59:59.999Z`);
    if (isNaN(hasta.getTime())) throw new BadRequestError(`dateTo inválido: ${dateTo}`);
    filter.fechaMovimiento = { ...(filter.fechaMovimiento ?? {}), $lte: hasta };
  }
  const busqueda = _buildBusquedaFilter(search);
  if (busqueda) Object.assign(filter, busqueda);

  const reportes = await NetpayReporte.find(filter).sort({ fechaMovimiento: -1 }).lean();
  return { reportes };
}

// obtenerUltimaCarga — pedido explícito del usuario (2026-10-08): mensaje en el panel de carga
// con la fecha y la persona de la última vez que se subió un reporte de Netpay, para que quien
// va a cargar hoy sepa de un vistazo si ya se hizo. cargadoPor/cargadoEn ya se persisten por
// documento desde _crearYEvaluar/_procesarDeposito (más abajo) — sin campo nuevo, sin
// migración: un archivo global con N depósitos crea N documentos con el MISMO cargadoPor y
// cargadoEn a milisegundos de diferencia (mismo loop secuencial), así que el más reciente de
// CUALQUIERA de ellos identifica correctamente la última carga. No filtra por eliminado: un
// reporte ocultado sigue habiendo sido "cargado" en ese momento.
async function obtenerUltimaCarga() {
  const ultimo = await NetpayReporte.findOne({})
    .sort({ cargadoEn: -1 })
    .select('cargadoEn cargadoPor nombreArchivoOriginal')
    .lean();
  if (!ultimo) return { ultimaCarga: null };
  return {
    ultimaCarga: {
      cargadoEn: ultimo.cargadoEn,
      cargadoPor: ultimo.cargadoPor ?? null,
      nombreArchivoOriginal: ultimo.nombreArchivoOriginal ?? null,
    },
  };
}

async function obtenerDetalle(id) {
  const reporte = await NetpayReporte.findById(id).lean();
  if (!reporte) throw new NotFoundError('Reporte Netpay');
  return { reporte: await _poblarMovimientoVinculado(reporte) };
}

// buscarCandidatos — recalcula EN VIVO los mismos candidatos que cargarReporte ya calculó al
// momento de la carga, para un reporte YA persistido. Existe para que el detalle de un
// reporte 'pendiente' reabierto en una sesión posterior (donde los candidatos originales de
// cargarReporte ya no viven en memoria del lado del frontend) pueda ofrecer la MISMA UX de
// selección por radio buttons — sin esto, la única alternativa era pegar un _id a mano, un
// paso atrás respecto al resto del flujo. Funciona para un reporte en cualquier estatus (no
// se valida acá), pero solo tiene sentido real cuando estatus:'pendiente' — un reporte ya
// 'confirmado'/'descartado' no tiene nada que vincular.
async function buscarCandidatos(reporteId, modo) {
  const reporte = await NetpayReporte.findById(reporteId).lean();
  if (!reporte) throw new NotFoundError('Reporte Netpay');

  const candidatos = modo === 'ventana'
    ? await _buscarCandidatosEnVentana(reporte)
    : await _buscarCandidatosParaReporte(reporte);
  return { candidatos };
}

// resolverReporte — netpay-matching-v2 (design.md API table: "POST /netpay/reporte/:id/
// resolver": New; "Manual Resolve of Discrepancies" applies equally to NetpayReporte).
// Reemplaza confirmarReporte (Implementación 1, ELIMINADO — su guardia
// `estatus !== 'pendiente'` nunca puede calzar contra un documento v2 real). Cierra un
// reporte 'discrepancia' con justificación humana OBLIGATORIA, opcionalmente vinculando UN
// BankMovement — mismas reglas de validación que netpay-resolver.service.js#resolver
// (bucket-level), salvo la cardinalidad: el modelo NetpayReporte solo tiene UN campo
// movementIdConfirmado (1 reporte == a lo sumo 1 depósito), a diferencia de
// NetpayMatch.movementIdsConfirmados[] — se acepta como mucho 1 movementId acá.
//
// GUARD DE DISEÑO NO NEGOCIABLE (spec.md "Manual action cannot force auto-confirmed"): este
// camino JAMÁS escribe estatus:'confirmado_automatico' ni 'resuelto_por_reporte' — el único
// resultado posible es 'resuelto_manual'.
async function resolverReporte(id, { justificacion, movementIds } = {}, user) {
  const justificacionLimpia = justificacion ? String(justificacion).trim() : '';
  if (!justificacionLimpia) throw new BadRequestError('Se requiere una justificación.');

  const ids = [...new Set((movementIds ?? []).map(String))];
  if (ids.length > 1) throw new BadRequestError('Se permite a lo sumo 1 movementId para un reporte.');

  const reporte = await NetpayReporte.findById(id);
  if (!reporte) throw new NotFoundError('Reporte Netpay');
  if (reporte.estatus !== 'discrepancia') {
    throw new ConflictError(`Este reporte no está en discrepancia (estatus=${reporte.estatus}).`);
  }

  const actualizados = [];
  if (ids.length > 0) {
    const movimientos = await BankMovement.find({ _id: { $in: ids } }).lean();
    if (movimientos.length !== ids.length) throw new NotFoundError('Uno o más movimientos bancarios');
    for (const mov of movimientos) {
      if (mov.banco !== 'BBVA') throw new ConflictError(`El movimiento ${mov._id} no es de BBVA.`);
      if ((mov.erpLinks ?? []).length > 0) {
        throw new ConflictError(`El movimiento ${mov._id} ya tiene un ID ERP vinculado — puede que otro usuario ya lo haya usado.`);
      }
    }
    const erpId = `${_erpIdReporte(reporte.claveRastreo)}-MANUAL`;
    for (const mov of movimientos) {
      // eslint-disable-next-line no-await-in-loop
      const actualizado = await setErpIds(mov._id, [{
        erpId, origen: 'netpay-reporte-manual',
        saldoPagadoTotal: mov.deposito, saldoPagado: mov.deposito, total: mov.deposito,
      }], user);
      actualizados.push(actualizado);
    }
  }

  reporte.estatus = 'resuelto_manual';
  reporte.motivoDiscrepancia = null;
  reporte.justificacion = justificacionLimpia;
  reporte.resueltoManualPor = { userId: user?._id ?? null, nombre: user?.nombre || user?.email || null };
  reporte.resueltoManualEn = new Date();
  if (ids.length > 0) reporte.movementIdConfirmado = ids[0];
  await reporte.save();

  for (const actualizado of actualizados) {
    emitToBanco(actualizado.banco, 'bank:movement:updated', actualizado);
    emitToAll('bank:ficha-pendiente:changed', { movementId: actualizado._id });
  }

  return { reporte: await _poblarMovimientoVinculado(reporte.toObject()), movimientos: actualizados };
}

// rechazarReporte — netpay-matching-v2 (design.md API table: "POST /netpay/reporte/:id/
// descartar": "Maps to rechazado, also allowed from discrepancia"). Reemplaza
// descartarReporte (Implementación 1, ELIMINADO — misma razón que confirmarReporte
// arriba). NUNCA vincula nada contra BankMovement, mismo criterio EXACTO que
// netpay-resolver.service.js#rechazar (bucket-level): permitido desde cualquier estado NO
// terminal ("any active state -> rechazado", spec.md).
async function rechazarReporte(id, { motivo } = {}, user) {
  const reporte = await NetpayReporte.findById(id);
  if (!reporte) throw new NotFoundError('Reporte Netpay');
  if (ESTATUS_TERMINALES.has(reporte.estatus)) {
    throw new ConflictError(`Este reporte ya está en un estado terminal (estatus=${reporte.estatus}).`);
  }

  reporte.estatus = 'rechazado';
  reporte.descartadoMotivo = motivo ? String(motivo).trim() || null : null;
  reporte.descartadoPor = { userId: user?._id ?? null, nombre: user?.nombre || user?.email || null };
  reporte.descartadoEn = new Date();
  await reporte.save();

  return { reporte: await _poblarMovimientoVinculado(reporte.toObject()) };
}

// _buscarBucketCorroborable — netpay-matching-v2 (design.md "(a) Report present"): cuando
// NINGÚN BankMovement libre cuadra con el reporte, puede ser porque el matching en vivo
// (netpay-evaluacion.service.js) YA vinculó ese depósito antes de que llegara el reporte —
// en ese caso el reporte no crea un link nuevo, solo CORROBORA el bucket ya confirmado.
async function _buscarBucketCorroborable(reporte) {
  const buckets = await NetpayMatch.find({ estatusMatch: 'confirmado_automatico' }).lean();
  for (const bucket of buckets) {
    const movId = (bucket.movementIdsConfirmados ?? [])[0];
    if (!movId) continue;
    // eslint-disable-next-line no-await-in-loop
    const mov = await BankMovement.findById(movId).lean();
    if (mov && _montosIguales(mov.deposito, reporte.montoDepositoTotal)) {
      return { bucket, mov };
    }
  }
  return null;
}

// _cerrarBucketsCubiertos — netpay-matching-v2 (design.md "(a) Report present"): una vez
// que el reporte quedó resuelto_por_reporte, revisa cada bucket NetpayMatch todavía abierto
// (pendiente_por_marca, o discrepancia que no sea revertido/reporte_revertido) — si TODOS
// sus folios (snapshot.folios, por orderId o referencia) aparecen entre los folios NO
// duplicados de este reporte, el bucket se cierra a resuelto_por_reporte. Si SOLO ALGUNOS
// aparecen, queda discrepancia/cobertura_parcial (señal de inconsistencia real que necesita
// revisión manual). Un bucket sin ningún folio propio en común con el reporte no se toca.
async function _cerrarBucketsCubiertos(reporte) {
  const clavesReporte = new Set(
    (reporte.folios ?? [])
      .filter(f => !f.duplicadoDeReporteId)
      .flatMap(f => [f.orderId, f.referencia].filter(Boolean)),
  );
  if (clavesReporte.size === 0) return;

  const candidatosBucket = await NetpayMatch.find({
    estatusMatch: { $in: ['pendiente_por_marca', 'discrepancia'] },
  }).lean();

  for (const bucket of candidatosBucket) {
    if (bucket.estatusMatch === 'discrepancia'
      && ['revertido', 'reporte_revertido'].includes(bucket.motivoDiscrepancia)) continue;

    const clavesBucket = (bucket.snapshot?.folios ?? [])
      .map(f => f.orderId ?? f.referencia)
      .filter(Boolean);
    if (clavesBucket.length === 0) continue;

    const cubiertos = clavesBucket.filter(c => clavesReporte.has(c));
    if (cubiertos.length === 0) continue;

    if (cubiertos.length === clavesBucket.length) {
      // eslint-disable-next-line no-await-in-loop
      await NetpayMatch.findOneAndUpdate(
        { _id: bucket._id, estatusMatch: bucket.estatusMatch },
        {
          $set: {
            estatusMatch: 'resuelto_por_reporte', motivoDiscrepancia: null,
            'snapshot.reporteIdOrigen': reporte._id, 'snapshot.claveRastreoOrigen': reporte.claveRastreo,
            'snapshot.montoDepositoReporte': reporte.montoDepositoTotal,
          },
        },
      );
    } else {
      // eslint-disable-next-line no-await-in-loop
      await NetpayMatch.findOneAndUpdate(
        { _id: bucket._id, estatusMatch: bucket.estatusMatch },
        { $set: { estatusMatch: 'discrepancia', motivoDiscrepancia: 'cobertura_parcial' } },
      );
    }
  }
}

// evaluarReporte — netpay-matching-v2 (design.md "(a) Report present"): decide (o re-decide,
// ej. tras Reevaluar) el estatus de un reporte YA persistido. Un reporte 'eliminado' se
// salta por completo (soft-delete nunca reabre ni recalcula nada). Precedencia:
//   1 candidato libre exacto        -> resuelto_por_reporte, vinculo:'erp-link' (crea el link)
//   0 candidatos, bucket corroborable -> resuelto_por_reporte, vinculo:'corroborado' (sin link nuevo)
//   cualquier otro caso             -> discrepancia (sin_candidato | multiples_candidatos)
async function evaluarReporte(reporteId) {
  const reporte = await NetpayReporte.findById(reporteId);
  if (!reporte) throw new NotFoundError('Reporte Netpay');
  if (reporte.eliminado) return { reporte: await _poblarMovimientoVinculado(reporte.toObject()), candidatos: [] };

  // Bug real 2026-10-07 (reportado por el usuario: "Reevaluar" tumbaba a discrepancia un
  // reporte que ya estaba resuelto_por_reporte). El movimiento que un reporte YA resolvió
  // queda con erpLinks NO vacío (setErpIds lo escribe) — pero _buscarCandidatosParaReporte de
  // abajo filtra justo `erpLinks:{$size:0}`, así que ese movimiento (el único candidato
  // "correcto" posible) se vuelve invisible para su propia re-evaluación; al no haber tampoco
  // un bucket confirmado_automatico que lo corrobore, caía a discrepancia/sin_candidato sin
  // ningún motivo real. "Reevaluar" existe para RE-decidir un reporte en discrepancia, no para
  // re-litigar uno ya resuelto — mismo criterio que ya aplica resolverReporte (solo actúa si
  // estatus==='discrepancia', ver más abajo).
  if (['resuelto_por_reporte', 'resuelto_manual'].includes(reporte.estatus)) {
    return { reporte: await _poblarMovimientoVinculado(reporte.toObject()), candidatos: [] };
  }

  const candidatos = await _buscarCandidatosParaReporte(reporte);

  if (candidatos.length === 1) {
    const mov = candidatos[0];
    const erpId = _erpIdReporte(reporte.claveRastreo);
    // "Identificado por" en Bancos (pedido explícito del usuario, 2026-10-08): el nombre
    // sintético del motor no le dice nada a nadie en la bandeja — quien debe aparecer ahí es
    // la persona real que subió el reporte (reporte.cargadoPor). role:'admin' se CONSERVA
    // a propósito (nunca el rol real del uploader): setErpIds lo usa para el bypass de
    // permisos de esta escritura interna/automática — si en vez de eso viajara el rol real
    // del uploader, un rol sin banks:erp:link/banks:cobro rompería el matching automático en
    // silencio. reporte.confirmadoPor (abajo) NO cambia — sigue documentando que esto lo
    // resolvió el motor automático, que es un hecho distinto a "quién aparece en Bancos".
    const usuarioParaIdentificar = {
      _id: reporte.cargadoPor?.userId ?? USUARIO_MOTOR_REPORTE._id,
      role: 'admin',
      nombre: reporte.cargadoPor?.nombre || USUARIO_MOTOR_REPORTE.nombre,
    };
    const movActualizado = await setErpIds(mov._id, [{
      erpId, origen: 'netpay-reporte',
      saldoPagadoTotal: mov.deposito, saldoPagado: mov.deposito, total: mov.deposito,
    }], usuarioParaIdentificar, { guardSinVinculos: true });

    reporte.estatus = 'resuelto_por_reporte';
    reporte.vinculo = 'erp-link';
    reporte.motivoDiscrepancia = null;
    reporte.movementIdConfirmado = mov._id;
    reporte.confirmadoPor = { userId: null, nombre: USUARIO_MOTOR_REPORTE.nombre };
    reporte.confirmadoEn = new Date();
    await reporte.save();

    emitToBanco(movActualizado.banco, 'bank:movement:updated', movActualizado);
    emitToAll('bank:ficha-pendiente:changed', { movementId: mov._id });

    await _cerrarBucketsCubiertos(reporte);
    return { reporte: await _poblarMovimientoVinculado(reporte.toObject()), candidatos };
  }

  if (candidatos.length === 0) {
    const corroborable = await _buscarBucketCorroborable(reporte);
    if (corroborable) {
      reporte.estatus = 'resuelto_por_reporte';
      reporte.vinculo = 'corroborado';
      reporte.motivoDiscrepancia = null;
      reporte.movementIdConfirmado = corroborable.mov._id;
      reporte.confirmadoPor = { userId: null, nombre: USUARIO_MOTOR_REPORTE.nombre };
      reporte.confirmadoEn = new Date();
      await reporte.save();

      await _cerrarBucketsCubiertos(reporte);
      return { reporte: await _poblarMovimientoVinculado(reporte.toObject()), candidatos };
    }

    reporte.estatus = 'discrepancia';
    reporte.motivoDiscrepancia = 'sin_candidato';
    await reporte.save();
    return { reporte: await _poblarMovimientoVinculado(reporte.toObject()), candidatos };
  }

  reporte.estatus = 'discrepancia';
  reporte.motivoDiscrepancia = 'multiples_candidatos';
  await reporte.save();
  return { reporte: await _poblarMovimientoVinculado(reporte.toObject()), candidatos };
}

// eliminarReporte — soft-delete (design.md "Report Soft-Delete and Snapshot Integrity"):
// oculta el reporte de listados/cierres SIN tocar su estatus/links — nunca revierte un
// match ya resuelto ni revive uno rechazado (esos invariantes ya están garantizados porque
// esta función no toca ningún otro campo).
async function eliminarReporte(id, motivo, user) {
  const reporte = await NetpayReporte.findById(id);
  if (!reporte) throw new NotFoundError('Reporte Netpay');

  reporte.eliminado = true;
  reporte.eliminadoPor = { userId: user?._id ?? null, nombre: user?.nombre || user?.email || null };
  reporte.eliminadoEn = new Date();
  reporte.eliminadoMotivo = motivo ? String(motivo).trim() || null : null;
  await reporte.save();

  return { reporte: await _poblarMovimientoVinculado(reporte.toObject()) };
}

// restaurarReporte — design.md "Restoring a hidden report": "clears eliminado/
// eliminadoPor/En/Motivo; no other field changes" — nunca toca estatus ni links.
async function restaurarReporte(id) {
  const reporte = await NetpayReporte.findById(id);
  if (!reporte) throw new NotFoundError('Reporte Netpay');

  reporte.eliminado = false;
  reporte.eliminadoPor = null;
  reporte.eliminadoEn = null;
  reporte.eliminadoMotivo = null;
  await reporte.save();

  return { reporte: await _poblarMovimientoVinculado(reporte.toObject()) };
}

// consultarFolioKore — consulta puntual e informativa (withAccountInfo=true) de la CxC
// asociada a UN folio del reporte, para mostrarla en el detalle. NUNCA aplica cobro, NUNCA
// decide el matching contra BBVA (eso ya lo resolvió montoDepositoTotal). El resultado se
// cachea tal cual viene de Kore (sin remapear, ver NetpayReporte.model.js#koreCache) para no
// tener que volver a pegarle a Kore cada vez que se abre el detalle.
async function consultarFolioKore(reporteId, referencia) {
  if (!referencia) throw new BadRequestError('Se requiere referencia.');

  const reporte = await NetpayReporte.findById(reporteId);
  if (!reporte) throw new NotFoundError('Reporte Netpay');

  const folioIdx = (reporte.folios ?? []).findIndex(f => f.referencia === referencia);
  if (folioIdx === -1) throw new NotFoundError(`Folio ${referencia} dentro de este reporte`);

  const { raw } = await buscarTransaccionesNetpay({ folio: referencia, withAccountInfo: true, status: 'completed' });
  const transacciones = raw?.Data?.transactions ?? [];
  const tx = transacciones.find(t => String(t.folio) === String(referencia)) ?? transacciones[0] ?? null;
  const cuenta = tx?.cuentas?.[0] ?? null;

  if (!cuenta) {
    throw new NotFoundError(
      `No se encontró la cuenta en Kore para el folio ${referencia} — puede que la transacción ya no esté disponible o no tenga CxC asociada.`,
    );
  }

  const consultadoEn = new Date();
  reporte.folios[folioIdx].koreCache = { consultadoEn, cuenta };
  await reporte.save();

  return { cuenta, consultadoEn };
}

// Fix 3 (2026-09-25, pedido explícito del usuario): ver folios relacionados desde el modal
// ERP de Bancos (erp-modal.component.ts#esErpIdNetpayReporte) sin depender de abrir el panel
// de Netpay ni exportar el Excel. Busca por movementIdConfirmado (índice propio, ver
// NetpayReporte.model.js) — solo tiene resultado real para un reporte 'confirmado' (el único
// estatus que tiene ese campo poblado), pero no se restringe acá por estatus: si en algún
// momento queda huérfano (reversión a medio camino) es preferible devolver lo que haya a
// esconderlo en silencio.
async function obtenerPorMovimiento(movementId) {
  const reporte = await NetpayReporte.findOne({ movementIdConfirmado: movementId }).lean();
  if (!reporte) throw new NotFoundError('Reporte Netpay para este movimiento');
  return { reporte };
}

function _sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Reintento ante 429 de Kore para UN folio — mismo patrón que erp-sync.service.js#_getConReintento
// (backoff fijo con "retry after: X segundos" que Kore manda en el cuerpo del 429, +1s de
// margen, tope 60s) — no se reusa esa función tal cual porque vive en otro dominio (ERP
// "por centro", axios directo) y acá el llamado real es consultarFolioKore (Kore caja), que
// ya envuelve su propio axios vía kore-caja.service.js#buscarTransaccionesNetpay y propaga
// el KoreCajaError con statusCode. Sin librería nueva (ej. p-limit) — no existe en este
// proyecto, ver comentario del pedido.
const CONSULTAR_FOLIOS_MAX_INTENTOS_429 = 3;
async function _consultarFolioConReintento(reporteId, referencia) {
  for (let intento = 1; intento <= CONSULTAR_FOLIOS_MAX_INTENTOS_429; intento++) {
    try {
      return await consultarFolioKore(reporteId, referencia);
    } catch (err) {
      if (err?.statusCode === 429 && intento < CONSULTAR_FOLIOS_MAX_INTENTOS_429) {
        const dataMsg   = String(err?.koreBody?.Data ?? '');
        const match     = /retry after:\s*([\d.]+)/i.exec(dataMsg);
        const esperaSeg = Math.min((match ? Number(match[1]) : 10) + 1, 60);
        logger.warn(`[NetpayReporte] 429 de Kore al consultar folio ${referencia} (reporte=${reporteId}), reintentando en ${esperaSeg.toFixed(1)}s (intento ${intento}/${CONSULTAR_FOLIOS_MAX_INTENTOS_429})`);
        await _sleep(esperaSeg * 1000);
        continue;
      }
      throw err;
    }
  }
}

// Fix 2a (2026-09-25, pedido explícito del usuario): antes de generar el Excel de un reporte
// (GET .../export), consulta contra Kore los folios que TODAVÍA no tienen koreCache.cuenta —
// para que el Excel exportado incluya el dato de Kore aunque el usuario nunca haya abierto
// cada folio a mano en el panel.
// Fix perf (2026-09-29, pedido explícito del usuario — "es normal que demore la bandeja de
// Netpay - Reporte manual"): el recorrido era 100% secuencial (un folio a la vez, 400ms de
// pausa entre CADA llamada) — con reportes de 30-50 folios sin consultar, minutos de espera.
// Se paraleliza en LOTES de tamaño fijo (Promise.allSettled por lote) — mismo espíritu que
// pagos-cyc/formas-pago-cxc (pausa entre llamadas a Kore, ver erp.routes.js) para no saturarlo,
// pero ahora la pausa de 400ms es ENTRE LOTES, no entre cada folio individual. El reintento
// ante 429 (_consultarFolioConReintento) NO cambia — sigue siendo por folio, con su propio
// backoff; si Kore devuelve 429 bajo carga concurrente, cada folio lo absorbe por su cuenta.
// Fallo parcial, NO todo-o-nada (mismo criterio que otros importadores del proyecto, ej.
// pagos-cyc): un folio individual que falle (404 sin match, red, 429 agotado) se loguea y se
// acumula en `fallos`, sin abortar el resto — el Excel se genera igual con lo que sí se pudo
// resolver. Sin librería nueva (ej. p-limit) — no existe en este proyecto.
const CONSULTAR_FOLIOS_PAUSA_MS = 400;
const CONSULTAR_FOLIOS_CONCURRENCIA = 5;
async function consultarFoliosPendientes(reporteId) {
  const reporte = await NetpayReporte.findById(reporteId).lean();
  if (!reporte) throw new NotFoundError('Reporte Netpay');

  const pendientes = (reporte.folios ?? []).filter(f => f.referencia && !f.koreCache?.cuenta);
  const fallos = [];

  for (let i = 0; i < pendientes.length; i += CONSULTAR_FOLIOS_CONCURRENCIA) {
    const lote = pendientes.slice(i, i + CONSULTAR_FOLIOS_CONCURRENCIA);
    const resultadosLote = await Promise.allSettled(
      lote.map(folio => _consultarFolioConReintento(reporteId, folio.referencia)),
    );
    resultadosLote.forEach((r, idx) => {
      if (r.status === 'rejected') {
        const folio = lote[idx];
        logger.warn(`[NetpayReporte] no se pudo consultar Kore para el folio ${folio.referencia} (reporte=${reporteId}): ${r.reason.message}`);
        fallos.push({ referencia: folio.referencia, error: r.reason.message });
      }
    });
    // Sin pausa después del último lote — no hay un lote siguiente que proteger.
    const esUltimoLote = i + CONSULTAR_FOLIOS_CONCURRENCIA >= pendientes.length;
    if (!esUltimoLote) await _sleep(CONSULTAR_FOLIOS_PAUSA_MS);
  }

  return { consultados: pendientes.length - fallos.length, fallos };
}

// ── Export de un LOTE de reportes (N depósitos de UN MISMO archivo recién cargado) ────────
// netpay-reporte export-lote (2026-10-07, pedido explícito del usuario): "excel general" tras
// cargar un archivo global — la unidad acá es el CONJUNTO de depósitos que ese archivo generó
// (normalmente ya vienen con koreCache lleno, porque cargarReporte() consulta Kore ANTES de
// responder al upload — ver POST .../upload en erp.routes.js), pero un folio puede haber
// quedado pendiente por un fallo parcial de esa carga. Mismo techo/guard anti-doble-disparo
// que netpay-match-export.service.js#consultarFoliosPendientesDeBandeja — acá la unidad
// contada es un REPORTE completo (que internamente ya hace su propio lote de 5 folios), no un
// folio suelto dentro de un bucket.
const MAX_FOLIOS_PENDIENTES_EXPORT_LOTE = 300;
// Deja margen bajo el req.setTimeout(300000)/res.setTimeout(300000) de la ruta para lo que
// viene después (armar el Excel + enviar el buffer) — mismo criterio que
// netpay-match-export.service.js#PRESUPUESTO_TIEMPO_MS.
const PRESUPUESTO_TIEMPO_MS_LOTE = 240000;
// Peor caso de agotar los 3 intentos de UN LOTE de 5 folios (misma fórmula que
// netpay-match-export.service.js#PEOR_CASO_UN_LOTE_MS) — a diferencia de la versión anterior
// de este archivo, esto YA NO se compara tal cual contra cada reporte: se multiplica por la
// cantidad REAL de lotes de 5 que ESE reporte va a correr (ver _peorCasoReporte abajo).
// BLOCKER de revisión de resiliencia (2026-10-07): el comentario original de este archivo
// afirmaba que el presupuesto por lotes "ya lo maneja consultarFoliosPendientes puertas
// adentro" — eso era falso, esa función no tiene ningún control de tiempo interno, así que un
// reporte con muchos folios pendientes (varios lotes de 5) podía tardar muchas veces el peor
// caso de UN SOLO lote que este archivo evaluaba antes de arrancarlo, superando el
// req.setTimeout/res.setTimeout de la ruta sin que nada lo cortara.
const PEOR_CASO_UN_LOTE_DE_FOLIOS_MS =
  (CONSULTAR_FOLIOS_MAX_INTENTOS_429 - 1) * 60000 + CONSULTAR_FOLIOS_MAX_INTENTOS_429 * 5000;

function _peorCasoReporte(cantidadPendientes) {
  return Math.ceil(cantidadPendientes / CONSULTAR_FOLIOS_CONCURRENCIA) * PEOR_CASO_UN_LOTE_DE_FOLIOS_MS;
}

const _exportacionesLoteEnCurso = new Set();

async function consultarFoliosPendientesDeLote(ids) {
  const clave = [...ids].map(String).sort().join('|');
  if (_exportacionesLoteEnCurso.has(clave)) {
    throw new ConflictError('Ya hay una exportación en curso para este lote, esperá a que termine.');
  }
  _exportacionesLoteEnCurso.add(clave);

  try {
    const reportes = await NetpayReporte.find({ _id: { $in: ids } }).select('folios').lean();

    const pendientesPorReporte = new Map(
      reportes.map(r => [String(r._id), (r.folios ?? []).filter(f => f.referencia && !f.koreCache?.cuenta).length]),
    );
    const totalPendientes = [...pendientesPorReporte.values()].reduce((sum, n) => sum + n, 0);
    if (totalPendientes > MAX_FOLIOS_PENDIENTES_EXPORT_LOTE) {
      throw new UnprocessableError(
        `Hay ${totalPendientes} folios sin consultar en Kore dentro de este lote (máximo ${MAX_FOLIOS_PENDIENTES_EXPORT_LOTE}).`,
      );
    }

    const fallos = [];
    const omitidosPorTiempo = [];
    const reportesFallidos = [];
    const inicio = Date.now();
    for (const reporte of reportes) {
      const reporteId = String(reporte._id);
      const peorCaso = _peorCasoReporte(pendientesPorReporte.get(reporteId) ?? 0);
      if (Date.now() - inicio + peorCaso > PRESUPUESTO_TIEMPO_MS_LOTE) {
        omitidosPorTiempo.push(reporteId);
        continue;
      }
      // CRITICAL de revisión de resiliencia (2026-10-07): antes, un error de
      // consultarFoliosPendientes para UN reporte (ej. borrado concurrente por otro usuario a
      // mitad de la corrida -> NotFoundError) abortaba TODO el lote sin try/catch, perdiendo
      // el trabajo ya hecho de los reportes anteriores y sin generar ningún Excel. Ahora se
      // registra en reportesFallidos y se sigue con el resto — mismo criterio de fallo
      // parcial que ya aplica a nivel de folio individual dentro de esa función.
      try {
        // eslint-disable-next-line no-await-in-loop
        const resultado = await consultarFoliosPendientes(reporteId);
        resultado.fallos.forEach(f => fallos.push({ reporteId, ...f }));
      } catch (err) {
        logger.warn(`[NetpayReporte] no se pudo consultar Kore para el reporte ${reporteId} dentro del lote: ${err.message}`);
        reportesFallidos.push({ reporteId, error: err.message });
      }
    }

    return { fallos, omitidosPorTiempo, reportesFallidos };
  } finally {
    // SIEMPRE libera la marca — éxito, error (incluido el 422 del techo), o corte por
    // presupuesto de tiempo caen todos acá.
    _exportacionesLoteEnCurso.delete(clave);
  }
}

// Reportes frescos (post-enriquecimiento de Kore) para armar el Excel del lote — find simple
// por _id, sin populate (generarExcelReportesNetpay no usa movementIdConfirmado poblado).
async function obtenerReportesPorIds(ids) {
  return NetpayReporte.find({ _id: { $in: ids } }).lean();
}

module.exports = {
  cargarReporte,
  iniciarCargaReporteJob,
  obtenerEstadoJobCarga,
  _procesarDepositosParseados,
  _correrCargaReporteJob,
  listar,
  _buildBusquedaFilter,
  obtenerUltimaCarga,
  obtenerDetalle,
  obtenerPorMovimiento,
  buscarCandidatos,
  resolverReporte,
  rechazarReporte,
  consultarFolioKore,
  consultarFoliosPendientes,
  consultarFoliosPendientesDeLote,
  obtenerReportesPorIds,
  evaluarReporte,
  eliminarReporte,
  restaurarReporte,
  PREFIJO_ERP_ID_REPORTE,
  _erpIdReporte,
  _montosIguales,
  _buscarCandidatosParaReporte,
  _clave,
  _derivarSucursales,
  _poblarMovimientoVinculado,
};
