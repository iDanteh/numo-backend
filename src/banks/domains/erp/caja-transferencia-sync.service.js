'use strict';

// caja-transferencia-sync.service.js — Fase A del proceso de matching de
// transferencias entre cajas (ver plan acordado con el usuario 2026-09-01).
// SOLO trae y persiste los datos crudos de buscarTransferenciasCajas
// (kore-caja.service.js) en CajaTransferencia — sin filtro configurable
// (Fase B) ni lógica de matching/huérfanos (Fase C/E), a propósito.

const CajaTransferencia = require('./CajaTransferencia.model');
const { buscarTransferenciasCajas } = require('./kore-caja.service');
const globalConfigService = require('../../../shared/services/global-config.service');

// "Ventana máxima de 1 mes" (pedido explícito del usuario): si el job lleva
// varios días sin correr (o es la primera corrida), el backfill nunca busca
// más atrás que esto — evita una consulta sin límite a Kore.
const VENTANA_MAX_DIAS = 31;

function _mxDateStr(date) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Mexico_City', year: 'numeric', month: '2-digit', day: '2-digit',
  }).format(date);
}

// Calcula el rango [fechaDesde, fechaHasta] a consultar en Kore, en hora de
// México, mismo criterio que _rangoDesdeFollo/ejecutarDescargaMasiva
// (satSyncJob.js) para evitar drift de timezone entre jobs.
// - Caso normal: se sincroniza SOLO el día de ayer (Kore necesita que la
//   sesión de caja del día ya haya cerrado del todo).
// - Catch-up: si la última transferencia guardada es de hace más de un día,
//   el rango se extiende hacia atrás desde el día siguiente a esa, acotado
//   por VENTANA_MAX_DIAS.
// - Ya al día (se llamó más de una vez el mismo día, o no hay nada nuevo que
//   sincronizar): devuelve null.
function _rangoSync(ultimaFechaRecepcion, ahora = new Date()) {
  const hoyMXStr = _mxDateStr(ahora);
  const ayerDate = new Date(`${hoyMXStr}T12:00:00`);
  ayerDate.setDate(ayerDate.getDate() - 1);
  const ayerMXStr = _mxDateStr(ayerDate);

  const topeDate = new Date(`${ayerMXStr}T12:00:00`);
  topeDate.setDate(topeDate.getDate() - VENTANA_MAX_DIAS);
  const topeMXStr = _mxDateStr(topeDate);

  let desdeMXStr = topeMXStr;
  if (ultimaFechaRecepcion) {
    const siguienteDate = new Date(ultimaFechaRecepcion);
    siguienteDate.setDate(siguienteDate.getDate() + 1);
    const siguienteMXStr = _mxDateStr(siguienteDate);
    desdeMXStr = siguienteMXStr > topeMXStr ? siguienteMXStr : topeMXStr;
  }

  if (desdeMXStr > ayerMXStr) return null; // nada nuevo que sincronizar

  return {
    fechaDesde: `${desdeMXStr}T00:00:00Z`,
    fechaHasta: `${ayerMXStr}T23:59:59Z`,
  };
}

// Configuraciones Globales, sección `bancos`, claves NOMBRE_TIPO_TRANSFERENCIA_PERMITIDOS /
// NOMBRE_CAJA_DESTINO_PERMITIDAS (Fase B) — JSON array de strings, sembradas como '[]' por
// seed-global-config-banks.js. null = sin filtro configurado para esa dimensión (se deja
// pasar todo) — tanto si la fila no existe todavía como si el admin la dejó en '[]', para que
// el filtro sea estrictamente opt-in y nunca bloquee la sincronización por accidente.
async function _listaPermitida(clave) {
  let valor;
  try {
    valor = await globalConfigService.getValue('bancos', clave);
  } catch (err) {
    if (err.message?.includes('No existe la configuración')) return null;
    throw err;
  }
  if (!valor) return null;

  let parsed;
  try {
    parsed = JSON.parse(valor);
  } catch {
    console.warn(`[CajaTransferenciaSync] bancos.${clave} no es un JSON array válido — se ignora el filtro para esta dimensión.`);
    return null;
  }
  if (!Array.isArray(parsed) || parsed.length === 0) return null;
  return parsed;
}

// tiposPermitidos/cajasPermitidas === null significa "sin filtro para esa dimensión".
//
// El chequeo de estatus es hardcodeado, no configurable (mismo criterio que el
// estatus=RECIBIDO ya pedido a Kore en kore-caja.service.js#buscarTransferenciasCajas):
// una transferencia sin estatus RECIBIDO nunca tiene fechaRecepcion, así que
// buscarCandidatos() la descarta para siempre (caja-transferencia-match.service.js:87)
// SIN que detectarHuerfanas() la alcance nunca — su query compara fechaRecepcion con
// $lte, y Mongo no matchea null ahí. Sin este chequeo, una transferencia cancelada en
// Kore queda "pendiente" en la bandeja de forma permanente (confirmado 2026-09-02 con
// un caso real: estatusKore:'CANCELADO', fechaRecepcion:null, nunca se limpiaba solo).
//
// `t` llega con dos formas distintas según quién llama: el crudo de Kore en la ingesta
// (campo `estatus`) o el documento ya persistido en reaplicarFiltro (campo `estatusKore`,
// ver el mapeo en _sincronizarRango) — se soportan ambos nombres.
function _pasaFiltro(t, tiposPermitidos, cajasPermitidas) {
  const estatus = t.estatusKore ?? t.estatus;
  if (estatus !== 'RECIBIDO') return false;
  if (tiposPermitidos && !tiposPermitidos.includes(t.nombreTipoTransferencia)) return false;
  if (cajasPermitidas && !cajasPermitidas.includes(t.nombreCajaDestino)) return false;
  return true;
}

// Upserta por koreId — $setOnInsert para estatusMatch: un re-sync (backfill,
// reintento) NUNCA debe pisar el resultado de un matching ya resuelto por
// Fase C/E, aunque Kore haya actualizado otros campos de esa transferencia.
// Compartida entre la corrida automática (rango acotado por _rangoSync) y la
// sincronización manual (rango elegido a mano, sin acotar — ver sincronizarTransferenciasCajasManual).
async function _sincronizarRango(rango) {
  const { raw } = await buscarTransferenciasCajas(rango);

  const [tiposPermitidos, cajasPermitidas] = await Promise.all([
    _listaPermitida('NOMBRE_TIPO_TRANSFERENCIA_PERMITIDOS'),
    _listaPermitida('NOMBRE_CAJA_DESTINO_PERMITIDAS'),
  ]);
  const filtradas   = raw.filter(t => _pasaFiltro(t, tiposPermitidos, cajasPermitidas));
  const descartadas = raw.length - filtradas.length;

  let sincronizadas = 0;
  for (const t of filtradas) {
    if (!t.id) continue; // sin id de Kore no hay forma de deduplicar — se descarta
    await CajaTransferencia.updateOne(
      { koreId: t.id },
      {
        $set: {
          monto:               t.monto,
          estatusKore:         t.estatus ?? null,
          cajaOrigenId:        t.cajaOrigenId ?? null,
          nombreCajaOrigen:    t.nombreCajaOrigen ?? null,
          almacenCajaOrigen:   t.almacenCajaOrigen ?? null,
          cajaDestinoId:       t.cajaDestinoId ?? null,
          nombreCajaDestino:   t.nombreCajaDestino ?? null,
          almacenCajaDestino:  t.almacenCajaDestino ?? null,
          sessionOrigenId:     t.sessionOrigenId ?? null,
          sessionDestinoId:    t.sessionDestinoId ?? null,
          formaPago:           t.formaPago ?? null,
          nombreFormaPago:     t.nombreFormaPago ?? null,
          solicito:            t.solicito ?? null,
          nombreSolicito:      t.nombreSolicito ?? null,
          recibio:             t.recibio ?? null,
          nombreRecibio:       t.nombreRecibio ?? null,
          autorizo:            t.autorizo || null,
          nombreAutorizo:      t.nombreAutorizo || null,
          fechaSolicitud:      t.fechaSolicitud ? new Date(t.fechaSolicitud) : null,
          fechaRecepcion:      t.fechaRecepcion ? new Date(t.fechaRecepcion) : null,
          observacion:         t.observacion ?? null,
          idTipoTransferencia:     t.idTipoTransferencia ?? null,
          nombreTipoTransferencia: t.nombreTipoTransferencia ?? null,
        },
        $setOnInsert: { koreId: t.id, estatusMatch: 'pendiente' },
      },
      { upsert: true },
    );
    sincronizadas++;
  }

  console.log(
    `[CajaTransferenciaSync] ${sincronizadas} transferencias sincronizadas, ${descartadas} descartadas por filtro ` +
    `(${rango.fechaDesde} → ${rango.fechaHasta}).`,
  );
  return { sincronizadas, descartadas, rango };
}

// Corrida automática (cron diario) — calcula su propio rango, con el tope de
// VENTANA_MAX_DIAS. Sin cambios de comportamiento respecto a antes de agregar
// la sincronización manual.
async function sincronizarTransferenciasCajas() {
  const ultima = await CajaTransferencia.findOne().sort({ fechaRecepcion: -1 }).select('fechaRecepcion').lean();
  const rango  = _rangoSync(ultima?.fechaRecepcion ?? null);

  if (!rango) {
    console.log('[CajaTransferenciaSync] Ya está al día, nada que sincronizar.');
    return { sincronizadas: 0, descartadas: 0 };
  }

  return _sincronizarRango(rango);
}

// Sincronización manual bajo demanda (pedido explícito del usuario 2026-09-01: poder elegir
// fechaDesde/fechaHasta a mano, sin esperar al cron ni quedar atado a VENTANA_MAX_DIAS — el
// tope de 1 mes es una protección del catch-up AUTOMÁTICO, no tiene sentido para una acción
// deliberada de un admin, ej. backfill puntual o revisar una fecha vieja). Requiere las 2
// fechas explícitas — a diferencia de sincronizarTransferenciasCajas(), acá no hay "rango
// implícito" razonable que inventar.
let manualSyncRunning = false;

async function sincronizarTransferenciasCajasManual({ fechaDesde, fechaHasta }) {
  if (!fechaDesde || !fechaHasta) {
    throw new Error('Se requieren fechaDesde y fechaHasta.');
  }
  if (manualSyncRunning) {
    throw new Error('Ya hay una sincronización manual de transferencias de caja en curso.');
  }

  manualSyncRunning = true;
  try {
    return await _sincronizarRango({ fechaDesde, fechaHasta });
  } finally {
    manualSyncRunning = false;
  }
}

// Reevalúa el filtro configurado (NOMBRE_TIPO_TRANSFERENCIA_PERMITIDOS/NOMBRE_CAJA_DESTINO_PERMITIDAS)
// contra las CajaTransferencia 'pendiente' YA PERSISTIDAS — el filtro de _sincronizarRango solo corre
// en la ingesta (pregunta abierta del usuario 2026-09-01: cambiar la config no reclasifica nada viejo).
// Nunca toca 'matcheada' ni 'huerfana' (fuera del query por estatusMatch:'pendiente') — una vez que hay
// un link financiero real o se cerró la ventana sin candidatos, un ajuste de config no debe hacerla
// desaparecer ni resucitarla. Dry-run por defecto (mismo criterio cauteloso que
// /sync-erp-kore/desvincular-cancelaciones): hay que pedir explícitamente {dryRun:false} para escribir.
const BATCH = 500;

async function reaplicarFiltro({ dryRun = true } = {}) {
  const [tiposPermitidos, cajasPermitidas] = await Promise.all([
    _listaPermitida('NOMBRE_TIPO_TRANSFERENCIA_PERMITIDOS'),
    _listaPermitida('NOMBRE_CAJA_DESTINO_PERMITIDAS'),
  ]);

  const detalle = [];
  let skip = 0;
  while (true) {
    // eslint-disable-next-line no-await-in-loop
    const docs = await CajaTransferencia.find({ estatusMatch: 'pendiente' })
      .skip(skip).limit(BATCH).lean();
    if (docs.length === 0) break;

    for (const t of docs) {
      const pasa = _pasaFiltro(t, tiposPermitidos, cajasPermitidas);
      if (!pasa && !t.excluidaPorFiltro) {
        detalle.push({
          _id: t._id, koreId: t.koreId, monto: t.monto,
          nombreTipoTransferencia: t.nombreTipoTransferencia, nombreCajaDestino: t.nombreCajaDestino,
          accion: 'excluir',
        });
      } else if (pasa && t.excluidaPorFiltro) {
        detalle.push({
          _id: t._id, koreId: t.koreId, monto: t.monto,
          nombreTipoTransferencia: t.nombreTipoTransferencia, nombreCajaDestino: t.nombreCajaDestino,
          accion: 'reincluir',
        });
      }
    }

    skip += docs.length;
    if (docs.length < BATCH) break;
  }

  if (!dryRun && detalle.length > 0) {
    const ahora = new Date();
    const ops = detalle.map((d) => ({
      updateOne: {
        filter: { _id: d._id },
        update: {
          $set: d.accion === 'excluir'
            ? { excluidaPorFiltro: true, excluidaEn: ahora }
            : { excluidaPorFiltro: false, excluidaEn: null },
        },
      },
    }));
    await CajaTransferencia.bulkWrite(ops, { ordered: false });
  }

  console.log(
    `[CajaTransferenciaSync] reaplicarFiltro (dryRun:${dryRun}) — ${detalle.length} transferencias con cambio de exclusión.`,
  );
  return { ok: true, dryRun, encontrados: detalle.length, aplicados: dryRun ? 0 : detalle.length, detalle };
}

// Registra en global-config.service.js el hook que reaplica el filtro de forma
// automática cuando cambian las 2 claves relevantes — pedido explícito del usuario
// (2026-09-02): "no quiero que el usuario tenga control de hacerlo, pueden causar un
// desastre". Se registra al cargar este módulo desde erp.routes.js. dryRun:false
// siempre — este hook ES la única vía de ejecución real, no hay control manual (el
// endpoint HTTP que existía se eliminó por completo, ver erp.routes.js).
function init() {
  globalConfigService.registerConfigChangeHook(async ({ sectionClave, clave }) => {
    if (sectionClave !== 'bancos') return;
    if (!['NOMBRE_TIPO_TRANSFERENCIA_PERMITIDOS', 'NOMBRE_CAJA_DESTINO_PERMITIDAS'].includes(clave)) return;
    await reaplicarFiltro({ dryRun: false });
  });
}

module.exports = {
  sincronizarTransferenciasCajas,
  sincronizarTransferenciasCajasManual,
  reaplicarFiltro,
  init,
  _rangoSync,
  _pasaFiltro,
  _listaPermitida,
  VENTANA_MAX_DIAS,
};
