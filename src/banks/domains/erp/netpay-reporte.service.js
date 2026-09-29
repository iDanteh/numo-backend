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
const { NotFoundError, BadRequestError, ConflictError } = require('../../shared/errors/AppError');
// Estados desde los que rechazarReporte YA NO puede actuar — mismo criterio EXACTO que
// netpay-resolver.service.js#ESTATUS_TERMINALES ("any active state -> rechazado",
// spec.md), aplicado a NetpayReporte.
const ESTATUS_TERMINALES = new Set(['rechazado', 'resuelto_manual']);
const { parseNetpayReporte } = require('./netpay-reporte-parser.service');
const { emitToBanco, emitToAll } = require('../../shared/socket');
const { logger } = require('../../../shared/utils/logger');

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

// cargarReporte — parsea el Excel, valida que el depósito (claveRastreo) no esté cargado
// todavía, registra sus folios (idempotencia) y llama evaluarReporte() para decidir su
// estatus automáticamente en el mismo paso. Ya NO deja el reporte en 'pendiente' — ese
// valor no existe en el enum v2 (ver NetpayReporte.model.js#estatus).
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

  let reporte;
  try {
    reporte = await NetpayReporte.create({
      ...parsed,
      estatus: 'discrepancia',
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

  await _registrarFoliosIdempotente(reporte);

  const { reporte: reporteEvaluado, candidatos } = await evaluarReporte(reporte._id);
  return { reporte: reporteEvaluado, candidatos };
}

async function listar({ estatus, incluirEliminados = false } = {}) {
  const filter = {};
  if (estatus) filter.estatus = estatus;
  if (!incluirEliminados) filter.eliminado = { $ne: true };
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

  return { reporte: reporte.toObject(), movimientos: actualizados };
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

  return { reporte: reporte.toObject() };
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
  if (reporte.eliminado) return { reporte: reporte.toObject(), candidatos: [] };

  const candidatos = await _buscarCandidatosParaReporte(reporte);

  if (candidatos.length === 1) {
    const mov = candidatos[0];
    const erpId = _erpIdReporte(reporte.claveRastreo);
    const movActualizado = await setErpIds(mov._id, [{
      erpId, origen: 'netpay-reporte',
      saldoPagadoTotal: mov.deposito, saldoPagado: mov.deposito, total: mov.deposito,
    }], USUARIO_MOTOR_REPORTE, { guardSinVinculos: true });

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
    return { reporte: reporte.toObject(), candidatos };
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
      return { reporte: reporte.toObject(), candidatos };
    }

    reporte.estatus = 'discrepancia';
    reporte.motivoDiscrepancia = 'sin_candidato';
    await reporte.save();
    return { reporte: reporte.toObject(), candidatos };
  }

  reporte.estatus = 'discrepancia';
  reporte.motivoDiscrepancia = 'multiples_candidatos';
  await reporte.save();
  return { reporte: reporte.toObject(), candidatos };
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

  return { reporte: reporte.toObject() };
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
  resolverReporte,
  rechazarReporte,
  consultarFolioKore,
  consultarFoliosPendientes,
  evaluarReporte,
  eliminarReporte,
  restaurarReporte,
  PREFIJO_ERP_ID_REPORTE,
  _erpIdReporte,
  _montosIguales,
  _buscarCandidatosParaReporte,
  _clave,
};
