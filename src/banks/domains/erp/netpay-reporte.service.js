'use strict';

// netpay-reporte.service.js — Implementación 1 de "Netpay: carga manual del reporte como
// fuente de verdad" (ver plan y NetpayReporte.model.js para el porqué). Conciliación 1:1
// (un reporte == un depósito == a lo sumo UN BankMovement) — a diferencia del matching
// automático (netpay-match.service.js), que puede requerir 1 o 2 movimientos (split), acá
// el reporte YA trae el monto exacto depositado por Netpay, así que basta un solo
// BankMovement candidato.

const BankMovement = require('../banks/BankMovement.model');
const NetpayReporte = require('./NetpayReporte.model');
const { setErpIds, ERP_TOLERANCE } = require('../banks/bank.service');
const { conTransaccion } = require('../../shared/utils/mongo-tx');
const { buscarTransaccionesNetpay } = require('./kore-caja.service');
const { _ventanaDiasNetpay } = require('./netpay-match.service');
const { NotFoundError, BadRequestError, ConflictError } = require('../../shared/errors/AppError');
const { parseNetpayReporte } = require('./netpay-reporte-parser.service');
const { emitToBanco, emitToAll } = require('../../shared/socket');
const { logger } = require('../../../shared/utils/logger');

const PREFIJO_ERP_ID_REPORTE = 'NETPAYRPT-';

function _montosIguales(a, b) {
  return Math.abs((a ?? 0) - (b ?? 0)) <= ERP_TOLERANCE;
}

function _erpIdReporte(claveRastreo) {
  return `${PREFIJO_ERP_ID_REPORTE}${claveRastreo}`;
}

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

// cargarReporte — parsea el Excel, valida que el depósito (claveRastreo) no esté cargado
// todavía, busca candidatos BBVA y persiste el reporte completo en 'pendiente'. NUNCA
// vincula nada solo — la confirmación es un paso humano aparte (confirmarReporte).
async function cargarReporte(buffer, nombreArchivo, user) {
  let parsed;
  try {
    parsed = await parseNetpayReporte(buffer);
  } catch (err) {
    if (err instanceof BadRequestError) throw err;
    throw new BadRequestError(`Error al leer el archivo: ${err.message}`);
  }

  const existente = await NetpayReporte.findOne({ claveRastreo: parsed.claveRastreo }).lean();
  if (existente) {
    throw new ConflictError(`Ya existe un reporte cargado para este depósito (claveRastreo=${parsed.claveRastreo}).`);
  }

  const candidatos = await _buscarCandidatosParaReporte(parsed);

  let reporte;
  try {
    reporte = await NetpayReporte.create({
      ...parsed,
      estatus: 'pendiente',
      cargadoPor: { userId: user?._id ?? null, nombre: user?.nombre || user?.email || null },
      cargadoEn: new Date(),
      nombreArchivoOriginal: nombreArchivo ?? null,
    });
  } catch (err) {
    // Última línea de defensa (índice único claveRastreo) — condición de carrera entre el
    // findOne de arriba y este create, dos cargas casi simultáneas del mismo depósito.
    if (err.code === 11000) {
      throw new ConflictError(`Ya existe un reporte cargado para este depósito (claveRastreo=${parsed.claveRastreo}).`);
    }
    throw err;
  }

  return { reporte, candidatos };
}

async function listar({ estatus } = {}) {
  const filter = {};
  if (estatus) filter.estatus = estatus;
  const reportes = await NetpayReporte.find(filter).sort({ fechaMovimiento: -1 }).lean();
  return { reportes };
}

async function obtenerDetalle(id) {
  const reporte = await NetpayReporte.findById(id).lean();
  if (!reporte) throw new NotFoundError('Reporte Netpay');
  return { reporte };
}

// buscarCandidatos — recalcula EN VIVO los mismos candidatos que cargarReporte ya calculó al
// momento de la carga, para un reporte YA persistido. Existe para que el detalle de un
// reporte 'pendiente' reabierto en una sesión posterior (donde los candidatos originales de
// cargarReporte ya no viven en memoria del lado del frontend) pueda ofrecer la MISMA UX de
// selección por radio buttons — sin esto, la única alternativa era pegar un _id a mano, un
// paso atrás respecto al resto del flujo. Funciona para un reporte en cualquier estatus (no
// se valida acá), pero solo tiene sentido real cuando estatus:'pendiente' — un reporte ya
// 'confirmado'/'descartado' no tiene nada que vincular.
async function buscarCandidatos(reporteId) {
  const reporte = await NetpayReporte.findById(reporteId).lean();
  if (!reporte) throw new NotFoundError('Reporte Netpay');

  const candidatos = await _buscarCandidatosParaReporte(reporte);
  return { candidatos };
}

// confirmarReporte — vincula el reporte a UN BankMovement de BBVA elegido por el usuario
// (candidato de la bandeja o cualquier otro _id, se RE-VALIDA server-side sin confiar en lo
// que manda el cliente). Usa el MISMO erpId sintético NETPAYRPT-<claveRastreo> para
// setErpIds + para que netpay-reporte-revert.service.js pueda reconocerlo al desvincular.
async function confirmarReporte(id, movementId, user) {
  if (!movementId) throw new BadRequestError('Se requiere movementId.');

  const reporte = await NetpayReporte.findById(id);
  if (!reporte) throw new NotFoundError('Reporte Netpay');
  if (reporte.estatus !== 'pendiente') {
    throw new ConflictError(`Este reporte ya no está pendiente (estatus=${reporte.estatus}).`);
  }

  const mov = await BankMovement.findById(movementId);
  if (!mov) throw new NotFoundError('Movimiento bancario');
  if (mov.banco !== 'BBVA') {
    throw new ConflictError(`El movimiento ${mov._id} no es de BBVA.`);
  }
  if ((mov.erpLinks ?? []).length > 0) {
    throw new ConflictError(`El movimiento ${mov._id} ya tiene un ID ERP vinculado — puede que otro usuario ya lo haya usado.`);
  }
  if (!_montosIguales(mov.deposito, reporte.montoDepositoTotal)) {
    throw new ConflictError(
      `El monto del movimiento (${mov.deposito}) no coincide con el depósito del reporte (${reporte.montoDepositoTotal}).`,
    );
  }

  const erpId = _erpIdReporte(reporte.claveRastreo);
  const movActualizado = await conTransaccion(async (session) => {
    const actualizado = await setErpIds(mov._id, [{
      erpId, origen: 'netpay-reporte',
      saldoPagadoTotal: mov.deposito, saldoPagado: mov.deposito, total: mov.deposito,
    }], user, { session });

    reporte.estatus = 'confirmado';
    reporte.movementIdConfirmado = mov._id;
    reporte.confirmadoPor = { userId: user?._id ?? null, nombre: user?.nombre || user?.email || null };
    reporte.confirmadoEn = new Date();
    await reporte.save(session ? { session } : undefined);

    return actualizado;
  });

  emitToBanco(movActualizado.banco, 'bank:movement:updated', movActualizado);
  emitToAll('bank:ficha-pendiente:changed', { movementId: mov._id });

  return { reporte: reporte.toObject(), movimiento: movActualizado };
}

// descartarReporte — el usuario decide que este reporte NO se va a conciliar (ej. ya se
// identificó por fuera de este flujo). NUNCA vincula nada contra BankMovement — a
// diferencia de confirmarReporte.
async function descartarReporte(id, motivo, user) {
  const reporte = await NetpayReporte.findById(id);
  if (!reporte) throw new NotFoundError('Reporte Netpay');
  if (reporte.estatus !== 'pendiente') {
    throw new ConflictError(`Este reporte ya no está pendiente (estatus=${reporte.estatus}).`);
  }

  reporte.estatus = 'descartado';
  reporte.descartadoPor = { userId: user?._id ?? null, nombre: user?.nombre || user?.email || null };
  reporte.descartadoEn = new Date();
  reporte.descartadoMotivo = motivo ? String(motivo).trim() || null : null;
  await reporte.save();

  return { reporte: reporte.toObject() };
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
// cada folio a mano en el panel. SECUENCIAL (no Promise.all/paralelo) con una pausa fija
// entre llamadas — mismo espíritu que pagos-cyc/formas-pago-cxc (pausa de 1s entre llamadas
// a Kore, ver erp.routes.js) para no saturarlo; acá 400ms porque withAccountInfo=true es una
// consulta puntual por folio (mucho más liviana que un "por centro"), no 20 sucursales.
// Fallo parcial, NO todo-o-nada (mismo criterio que otros importadores del proyecto, ej.
// pagos-cyc): un folio individual que falle (404 sin match, red, 429 agotado) se loguea y se
// acumula en `fallos`, sin abortar el resto — el Excel se genera igual con lo que sí se pudo
// resolver.
const CONSULTAR_FOLIOS_PAUSA_MS = 400;
async function consultarFoliosPendientes(reporteId) {
  const reporte = await NetpayReporte.findById(reporteId).lean();
  if (!reporte) throw new NotFoundError('Reporte Netpay');

  const pendientes = (reporte.folios ?? []).filter(f => f.referencia && !f.koreCache?.cuenta);
  const fallos = [];

  for (let i = 0; i < pendientes.length; i++) {
    const folio = pendientes[i];
    try {
      await _consultarFolioConReintento(reporteId, folio.referencia);
    } catch (err) {
      logger.warn(`[NetpayReporte] no se pudo consultar Kore para el folio ${folio.referencia} (reporte=${reporteId}): ${err.message}`);
      fallos.push({ referencia: folio.referencia, error: err.message });
    }
    // Sin pausa después del último folio — no hay una llamada siguiente que proteger.
    if (i < pendientes.length - 1) await _sleep(CONSULTAR_FOLIOS_PAUSA_MS);
  }

  return { consultados: pendientes.length - fallos.length, fallos };
}

module.exports = {
  cargarReporte,
  listar,
  obtenerDetalle,
  obtenerPorMovimiento,
  buscarCandidatos,
  confirmarReporte,
  descartarReporte,
  consultarFolioKore,
  consultarFoliosPendientes,
  PREFIJO_ERP_ID_REPORTE,
  _erpIdReporte,
  _montosIguales,
  _buscarCandidatosParaReporte,
};
