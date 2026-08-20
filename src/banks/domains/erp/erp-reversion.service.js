'use strict';

const BankMovement = require('../banks/BankMovement.model');
const ErpReversion  = require('./ErpReversion.model');
const { aplicarLogicaErp }        = require('../banks/bank.service');
const { resolvePrimeraIdentificacion } = require('../banks/identificacion-timestamp.util');
const { emitToBanco }             = require('../../shared/socket');
const { logger }                  = require('../../../shared/utils/logger');
// Mismo patrón ya usado por collection-request.service.js (erpRoutes._rangoDesdeFollo/
// _sincronizarConRetry): erp.routes.js re-expone sus helpers internos en el objeto router
// para que otros dominios los reutilicen sin duplicar la lógica de sync contra Kore. Sin
// require circular — erp.routes.js no requiere nada de este archivo.
const erpRoutes = require('./erp.routes');

// Mismo tamaño de página que ya usa el resto del módulo ERP (ver ERP_PAGE_SIZE en erp.routes.js).
const ERP_REVERSION_PAGE_SIZE = 50;

// Clona un subdocumento/entrada de Mongoose (o un objeto plano) para guardar un snapshot
// que no cambie si el original se sigue mutando después. `.toObject()` cuando es un
// subdocumento Mongoose real; JSON.parse(JSON.stringify(...)) para objetos planos (tests).
function _snapshot(value) {
  if (value == null) return null;
  return typeof value.toObject === 'function' ? value.toObject() : JSON.parse(JSON.stringify(value));
}

// Quita el erpId de UN movimiento bancario POR COMPLETO: guarda snapshots de lo que había
// (rastro de auditoría), limpia erpLinks/erpIds/identificadoPor, recalcula
// saldoErp/uuidXML/status con la misma lógica pura que usa el flujo manual (aplicarLogicaErp,
// bank.service.js) y emite el mismo shape que ya emite setErpIds.
//
// Uso: (a) fallback cuando no se pudo reconsultar Kore en vivo (ver _aplicarReversionAMovimiento
// más abajo) — mejor desvincular con lo que hay que dejar un link con datos potencialmente
// incorrectos sin ninguna corrección; (b) cuando la reconsulta SÍ confirma que ya no queda
// ningún aporte real de este movimiento a la CxC.
//
// NO se usa setErpIds aquí: esa función asume un `user` de Auth0 para decidir permisos de
// ownership (quién puede desvincular qué) que no aplican a un webhook server-to-server sin
// sesión, y además su limpieza de `identificadoPor` SIEMPRE quita las entradas
// 'erp-auto' sin importar qué se esté agregando/quitando — correcto para el flujo humano
// (ceder ownership al humano que vincula a mano), pero no para esto: acá no hay ningún
// humano tomando posesión, así que la limpieza debe ser puntual, solo la entrada del erpId
// que se está removiendo.
async function _removerErpIdDeMovimiento(mov, erpId, { serieExterna, folioExterno } = {}) {
  const link = (mov.erpLinks ?? []).find(l => l.erpId === erpId) ?? null;
  const erpLinkRemovido = _snapshot(link);

  // Mismatch: Kore mandó su propia serie/folio Y el link vinculado tenía su propia
  // serie/folio guardados Y no coinciden — señal de que quizás se está desvinculando la
  // CxC equivocada (o que Kore reusó el erpId). Se reporta, no bloquea la reversión.
  let mismatch = false;
  if (link) {
    if (serieExterna != null && link.serie != null && String(link.serie) !== String(serieExterna)) {
      mismatch = true;
    }
    if (folioExterno != null && link.folioExterno != null && String(link.folioExterno) !== String(folioExterno)) {
      mismatch = true;
    }
  }

  const identificadoPorEntry = (mov.identificadoPor ?? []).find(e => e.erpId === erpId) ?? null;
  const identificadoPorRemovido = _snapshot(identificadoPorEntry);

  mov.erpLinks        = (mov.erpLinks ?? []).filter(l => l.erpId !== erpId);
  mov.erpIds          = (mov.erpIds ?? []).filter(id => id !== erpId);
  mov.identificadoPor = (mov.identificadoPor ?? []).filter(e => e.erpId !== erpId);

  const { saldoErp, uuidXML, status } = aplicarLogicaErp(mov);
  mov.saldoErp = saldoErp;
  mov.uuidXML  = uuidXML;
  mov.status   = status;

  // Webhook server-to-server (Kore), sin sesión Auth0 real detrás — no hay `user` que pasar.
  // El helper es no-op en casi todos los casos aquí (esta operación suele desvincular, es decir
  // llevar el status HACIA no_identificado), pero se llama igual por si otro link deja el
  // movimiento igual identificado.
  const { primeraIdentificacionAt, primeraIdentificacionPor } =
    resolvePrimeraIdentificacion(status, mov, null);
  mov.primeraIdentificacionAt  = primeraIdentificacionAt;
  mov.primeraIdentificacionPor = primeraIdentificacionPor;

  await mov.save();

  emitToBanco(mov.banco, 'bank:movement:updated', {
    _id:             mov._id,
    banco:           mov.banco,
    erpIds:          mov.erpIds,
    erpLinks:        mov.erpLinks,
    saldoErp:        mov.saldoErp,
    uuidXML:         mov.uuidXML,
    status:          mov.status,
    identificadoPor: mov.identificadoPor,
  });

  return { movementId: mov._id, tipo: 'desvinculado', erpLinkRemovido, identificadoPorRemovido, mismatch };
}

// 2026-08-20 (fix real, reportado por el usuario con caso real: CxC pagada con 3 abonos de
// $100, ya sea en 3 depósitos DISTINTOS o los 3 en el MISMO depósito — Kore avisa la
// reversión de UNO solo, pero el código viejo desvinculaba el erpId de TODOS los
// BankMovement que lo tuvieran, sin importar cuántos abonos seguían vigentes).
//
// Corrige el link con datos FRESCOS de Kore (mismo mecanismo que ya usa
// POST /erp-links/:erpId/refrescar, sin el "ratchet" de _syncErpKoreJob/_recomputeErpKoreJob
// — ese ratchet existe a propósito para que una corrida rutinaria nunca BAJE un aporte ya
// confirmado; acá es lo opuesto, Kore avisó explícitamente que algo SÍ bajó, y este es
// justamente el único lugar del sistema donde bajar el número es lo correcto).
//
// erpRoutes._montoSaldoLinkPorMovimiento (link humano) / _montoSaldoLinkPorAutorizacion
// (link de motor) ya saben separar, DENTRO del mismo erpId, qué parte del historial de Kore
// le pertenece a ESTE movimiento específico (por Aut/Numo o por número de autorización) y
// netear reversas CON SIGNO — exactamente el problema de "abonos parciales, uno se
// revirtió" que hace falta resolver acá, ya resuelto para el job de sync. Por eso:
//   - Si el recálculo da > 0: TODAVÍA queda un aporte real de este movimiento — se corrigen
//     los números (saldoErpAportado/movimientosKore/saldoActual/etc.), el link NO se quita.
//   - Si el recálculo da null o 0: ya no queda nada atribuible a este movimiento — se
//     desvincula por completo (mismo comportamiento de siempre).
async function _aplicarReversionAMovimiento(mov, erpId, { serieExterna, folioExterno, raw0 }) {
  if (!raw0) {
    // Sin datos frescos de Kore (no se pudo reconsultar — ver procesarReversionKore) — cae
    // al comportamiento anterior en vez de dejar el link con un número potencialmente
    // incorrecto sin ninguna corrección.
    return _removerErpIdDeMovimiento(mov, erpId, { serieExterna, folioExterno });
  }

  const link      = mov.erpLinks.find(l => l.erpId === erpId) ?? null;
  const esHumano  = erpRoutes._erpIdIdentificadoPorHumano(mov.identificadoPor, erpId);
  const calculado = esHumano
    ? erpRoutes._montoSaldoLinkPorMovimiento(raw0, mov)
    : erpRoutes._montoSaldoLinkPorAutorizacion(raw0, mov.numeroAutorizacion);

  // 2026-08-20: log de la decisión en sí — sin esto, un caso "se ajustó en vez de
  // desvincularse" no dejaba NINGÚN rastro de por qué (a diferencia de los casos de
  // respaldo/mismatch, que ya logueaban). `antes` es el aporte que había ANTES de esta
  // reversión — compararlo contra `calculado` (el valor recién recalculado contra Kore)
  // es la pista si algún día el número no bajó cuando debería: si son iguales, Kore
  // todavía no reflejaba la reversión en el momento exacto de esta reconsulta (posible
  // carrera entre el webhook y su propio ledger consultable).
  logger.info(`[erp-reversion] erpId=${erpId} movementId=${mov._id}: aporte antes=${link?.saldoErpAportado ?? null}, recalculado tras reversión=${calculado} (esHumano=${esHumano}) -> ${(calculado == null || calculado === 0) ? 'DESVINCULADO' : 'AJUSTADO'}.`);

  if (calculado == null || calculado === 0) {
    return _removerErpIdDeMovimiento(mov, erpId, { serieExterna, folioExterno });
  }

  return _ajustarLinkTrasReversion(mov, erpId, { serieExterna, folioExterno, raw0, esHumano, calculado });
}

async function _ajustarLinkTrasReversion(mov, erpId, { serieExterna, folioExterno, raw0, esHumano, calculado }) {
  const link = mov.erpLinks.find(l => l.erpId === erpId);

  let mismatch = false;
  if (serieExterna != null && link.serie != null && String(link.serie) !== String(serieExterna)) mismatch = true;
  if (folioExterno != null && link.folioExterno != null && String(link.folioExterno) !== String(folioExterno)) mismatch = true;

  const erpLinkAntes = _snapshot(link);

  const backfill  = erpRoutes._backfillFormasPagoYFolioFiscal(link, raw0, mov, esHumano, calculado);
  const retencion = erpRoutes._retencionVigente(raw0);

  link.movimientosKore  = erpRoutes._movimientosKoreDesde(raw0);
  link.saldoErpAportado = calculado;
  link.saldoPagadoTotal = backfill.saldoPagadoTotal;
  link.saldoPagado      = backfill.saldoPagado;
  link.folioFiscal       = backfill.folioFiscal;
  link.tieneRetencion    = retencion.tieneRetencion;
  link.montoRetenido     = retencion.montoRetenido;
  link.saldoActual       = raw0.saldoActual ?? link.saldoActual ?? null;

  const { saldoErp, uuidXML, status } = aplicarLogicaErp(mov);
  mov.saldoErp = saldoErp;
  mov.uuidXML  = uuidXML;
  mov.status   = status;

  // El link sigue vigente (no se está desvinculando) — nada que resolver de
  // primeraIdentificacionAt, ese timestamp es inmutable y no depende de este ajuste.

  await mov.save();

  emitToBanco(mov.banco, 'bank:movement:updated', {
    _id:             mov._id,
    banco:           mov.banco,
    erpIds:          mov.erpIds,
    erpLinks:        mov.erpLinks,
    saldoErp:        mov.saldoErp,
    uuidXML:         mov.uuidXML,
    status:          mov.status,
    identificadoPor: mov.identificadoPor,
  });

  return {
    movementId: mov._id,
    tipo: 'ajustado',
    erpLinkAjustado: { antes: erpLinkAntes, despues: _snapshot(link) },
    mismatch,
  };
}

// Punto de entrada del webhook: Kore avisó que revirtió/canceló (total o parcialmente) una
// CxC. Busca todos los movimientos bancarios que la tengan vinculada (puede haber 0, 1 o
// varios) y para cada uno decide si corresponde desvincular por completo o solo corregir
// el aporte — ver _aplicarReversionAMovimiento — dejando siempre un rastro de auditoría
// (ErpReversion) para la bandeja de consulta.
async function procesarReversionKore({ erpId, motivo, fecha, serieExterna, folioExterno, referencia, payloadOriginal } = {}) {
  const movs = await BankMovement.find({ erpIds: erpId });

  if (movs.length === 0) {
    // Kore avisó una reversión que ya no encontramos vinculada a ningún movimiento — ya sea
    // porque nunca se vinculó de este lado, o porque este mismo evento (o un reintento suyo)
    // ya se procesó antes. No se persiste ningún ErpReversion: no hay nada que auditar
    // (movimientosAfectados quedaría vacío igual), y guardar un registro por cada llamada
    // repetida inflaba la bandeja con filas idénticas sin información nueva (detectado
    // 2026-08-10, probando el endpoint varias veces seguidas).
    // La respuesta sigue siendo 200/yaEstabaDesvinculada:true — el contrato con Kore no cambia.
    // 2026-08-20: sí se deja un log (no persistido) — este caso no genera fila en la
    // bandeja, así que sin esto no quedaba NINGÚN rastro de que Kore mandó este aviso.
    logger.warn(`[erp-reversion] erpId=${erpId} (serieExterna=${serieExterna}, folioExterno=${folioExterno}): no hay ningún BankMovement vinculado — no se persiste ErpReversion, se responde yaEstabaDesvinculada:true.`);
    return { reversionId: null, movimientosAfectados: 0, yaEstabaDesvinculada: true };
  }

  // 2026-08-20: reconsulta a Kore EN VIVO, una sola vez para todos los movimientos (es la
  // misma respuesta a nivel CxC/erpId para cualquiera de ellos) — mismo mecanismo que
  // POST /erp-links/:erpId/refrescar. Si falla (Kore no responde, o falta serie/folio para
  // construir el rango), se sigue igual con el comportamiento anterior (desvincular todo)
  // en vez de bloquear la reversión que Kore ya aplicó de su lado.
  let raw0 = null;
  const rango = erpRoutes._rangoDesdeFollo(folioExterno);
  if (rango) {
    try {
      const { raw } = await erpRoutes._sincronizarConRetry({
        serieExterna, folioExterno: String(folioExterno),
        fechaDesde: rango.fechaDesde, fechaHasta: rango.fechaHasta,
      });
      raw0 = raw[0] ?? null;
    } catch (err) {
      logger.error(`[erp-reversion] No se pudo reconsultar Kore en vivo para erpId=${erpId} (se sigue con el comportamiento de respaldo: desvincular por completo): ${err.message}`);
    }
  } else {
    logger.warn(`[erp-reversion] erpId=${erpId}: folioExterno=${folioExterno} no tiene el formato esperado para calcular el rango de fecha — se sigue con el comportamiento de respaldo: desvincular por completo.`);
  }
  if (!raw0) {
    logger.warn(`[erp-reversion] erpId=${erpId}: sin datos frescos de Kore, cada movimiento vinculado se desvinculará POR COMPLETO (comportamiento de respaldo).`);
  }

  const resultados = [];
  for (const mov of movs) {
    resultados.push(await _aplicarReversionAMovimiento(mov, erpId, { serieExterna, folioExterno, raw0 }));
  }

  const serieFolioMismatch = resultados.some(r => r.mismatch);
  // 2026-08-20: la reversión se aplica IGUAL cuando hay mismatch (decisión ya documentada
  // en _removerErpIdDeMovimiento — "se reporta, no bloquea") — pero hasta ahora esa señal
  // quedaba solo como un flag mudo en Mongo, sin nada visible en consola en el momento en
  // que pasa. Si "el proceso corre pero mal" es justo este caso (se desvincula un link con
  // serie/folio distinto al que mandó Kore), este log es la pista.
  if (serieFolioMismatch) {
    logger.warn(`[erp-reversion] MISMATCH serie/folio al revertir erpId=${erpId}: Kore mandó serieExterna=${serieExterna}/folioExterno=${folioExterno}, pero al menos un erpLink vinculado tenía otra serie/folio guardada. Se aplicó igual (no se bloquea) — revisar movimientosAfectados en la bandeja.`);
  }

  const reversion = await ErpReversion.create({
    erpId,
    motivo:             motivo ?? null,
    fechaKore:          fecha ?? null,
    serieExterna:       serieExterna ?? null,
    folioExterno:       folioExterno ?? null,
    referencia:         referencia ?? null,
    serieFolioMismatch,
    payloadOriginal:    payloadOriginal ?? null,
    movimientosAfectados: resultados.map(r => ({
      movementId:              r.movementId,
      tipo:                    r.tipo,
      erpLinkRemovido:         r.erpLinkRemovido ?? null,
      identificadoPorRemovido: r.identificadoPorRemovido ?? null,
      erpLinkAjustado:         r.erpLinkAjustado ?? null,
    })),
  });

  return { reversionId: reversion._id, movimientosAfectados: movs.length, yaEstabaDesvinculada: false };
}

// Listado paginado para la UI (bandeja de reversiones aplicadas por Kore).
async function listarReversiones({ page = 1, estado, q } = {}) {
  const filtro = {};
  if (estado) filtro.estado = estado;

  const texto = (q ?? '').toString().trim();
  if (texto) {
    const escapeRegex = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const regex = new RegExp(escapeRegex(texto), 'i');
    filtro.$or = [
      { erpId:        regex },
      { serieExterna: regex },
      { folioExterno: regex },
    ];
  }

  const pageNum = Math.max(1, parseInt(page, 10) || 1);
  const skip    = (pageNum - 1) * ERP_REVERSION_PAGE_SIZE;

  const [data, total] = await Promise.all([
    ErpReversion.find(filtro).sort({ createdAt: -1 }).skip(skip).limit(ERP_REVERSION_PAGE_SIZE),
    ErpReversion.countDocuments(filtro),
  ]);

  return {
    data,
    pagination: {
      page:        pageNum,
      totalPaginas: Math.max(1, Math.ceil(total / ERP_REVERSION_PAGE_SIZE)),
      total,
    },
  };
}

module.exports = { procesarReversionKore, listarReversiones };
