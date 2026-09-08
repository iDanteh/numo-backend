'use strict';

const BankMovement = require('../banks/BankMovement.model');
const ErpReversion  = require('./ErpReversion.model');
const { aplicarLogicaErp }        = require('../banks/bank.service');
const { resolvePrimeraIdentificacion } = require('../banks/identificacion-timestamp.util');
const { emitToBanco, emitToAll }  = require('../../shared/socket');
const { logger }                  = require('../../../shared/utils/logger');
// Mismo patrón ya usado por collection-request.service.js (erpRoutes._rangoDesdeFollo/
// _sincronizarConRetry): erp.routes.js re-expone sus helpers internos en el objeto router
// para que otros dominios los reutilicen sin duplicar la lógica de sync contra Kore. Sin
// require circular — erp.routes.js no requiere nada de este archivo.
const erpRoutes = require('./erp.routes');

// Mismo tamaño de página que ya usa el resto del módulo ERP (ver ERP_PAGE_SIZE en erp.routes.js).
const ERP_REVERSION_PAGE_SIZE = 50;

// 2026-08-21 (fix real, confirmado con caso real folioExterno 260800152/erpId
// 6a8760103bfaed00011c8e68): Kore tarda en reflejar su PROPIO reverso en el endpoint que
// reconsultamos — en ese caso real, más de 1min42s después de mandar el webhook, el
// reverso seguía sin aparecer en /cuentas-pendientes. Sin este retry, `_aplicarReversionAMovimiento`
// recalculaba con datos viejos y confirmaba como vigente un aporte que Kore ya había
// revertido — la reversión quedaba "aplicada" en la bandeja sin bajar ni un peso.
// Decisión explícita del usuario: reintentos DENTRO del mismo webhook (bloqueante), no un
// job aparte — así Kore conserva su propia señal de éxito/fallo (un 500 real si algo truena),
// en vez de recibir un 200 inmediato que oculte un fallo posterior en segundo plano.
const REVERSION_CONFIRM_DELAYS_MS = [15_000, 30_000, 45_000];
const _sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

// ¿La reconsulta a Kore (`raw0.movimientos`) ya incluye la reversión puntual que Kore avisó
// por webhook? Señal usada: la `fecha` que Kore manda en el webhook coincide, al segundo,
// con la `fecha` de una entrada en su propio historial — confirmado con el caso real (la
// entrada `REV ABO` trae la fecha EXACTA, al microsegundo, del payload del webhook). Sin
// `fecha` en el payload no hay nada contra qué confirmar — se trata como "no confirmable" a
// propósito (ver _sincronizarConfirmandoReversion: en ese caso se hace UN solo intento, sin
// reintentos, mismo comportamiento que antes de este fix).
function _reversionReflejadaEnKore(raw0, fechaReversion) {
  if (!raw0 || !fechaReversion) return false;
  const target = new Date(fechaReversion).getTime();
  if (Number.isNaN(target)) return false;
  return (raw0.movimientos ?? []).some(m => {
    const t = new Date(m?.fecha).getTime();
    return !Number.isNaN(t) && Math.abs(t - target) < 1000;
  });
}

// Reconsulta a Kore reintentando con espera incremental (15s/30s/45s) hasta que la propia
// reversión avisada por el webhook aparezca reflejada en la respuesta, o se agoten los
// intentos — en ese caso se sigue con los datos más recientes disponibles (pueden seguir
// desactualizados). Si Kore no mandó `fecha` en el webhook, se hace un único intento (no hay
// forma de confirmar nada, reintentar sería solo esperar a ciegas).
//
// Devuelve `confirmada:false` en TODO caso donde no se haya podido verificar el match
// contra Kore (agotados los reintentos, o sin `fecha` para comparar) — 2026-08-21, caso
// real: Kore avisó una reversión de $100 (folioExterno 260800164) que JAMÁS aplicó de su
// lado, ni siquiera minutos después de agotar los reintentos. Desde acá no hay forma de
// distinguir "todavía no lo aplicó" de "falló y nunca lo va a aplicar" — ambos casos agotan
// los reintentos por igual. `confirmada` es la señal que usa procesarReversionKore para NO
// etiquetar esto como una reversión resuelta cuando en realidad nadie la verificó.
async function _sincronizarConfirmandoReversion({ erpId, serieExterna, folioExterno, rango, fecha }) {
  let raw0 = null;
  const totalIntentos = REVERSION_CONFIRM_DELAYS_MS.length + 1;
  for (let intento = 0; intento < totalIntentos; intento++) {
    if (intento > 0) {
      await _sleep(REVERSION_CONFIRM_DELAYS_MS[intento - 1]);
    }
    const { raw } = await erpRoutes._sincronizarConRetry({
      serieExterna, folioExterno: String(folioExterno),
      fechaDesde: rango.fechaDesde, fechaHasta: rango.fechaHasta,
    });
    raw0 = raw[0] ?? null;
    if (fecha && _reversionReflejadaEnKore(raw0, fecha)) return { raw0, confirmada: true };
    if (!fecha) return { raw0, confirmada: false };
    logger.warn(`[erp-reversion] erpId=${erpId}: intento ${intento + 1}/${totalIntentos}, Kore todavía no refleja la reversión (fecha=${fecha}) en su propia consulta — reintentando.`);
  }
  const totalSegundos = REVERSION_CONFIRM_DELAYS_MS.reduce((a, b) => a + b, 0) / 1000;
  logger.warn(`[erp-reversion] erpId=${erpId}: Kore no reflejó la reversión (fecha=${fecha}) tras ${totalIntentos} intentos (~${totalSegundos}s) — se continúa con los datos más recientes disponibles, marcada sin confirmar.`);
  return { raw0, confirmada: false };
}

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
async function _removerErpIdDeMovimiento(mov, erpId, { serieExterna, folioExterno, motivo } = {}) {
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

  // 2026-09-01 (pedido explícito del usuario) — mismo historialVinculacion que ya alimenta
  // el flujo manual (bank.service.js), acá con origen:'kore-reversion' y sin userId (webhook
  // server-to-server, sin sesión humana detrás). Si no había link (linkRemovido null, mismo
  // caso de "no hay nada que auditar" de arriba) no se registra nada.
  if (link) {
    mov.historialVinculacion = [...(mov.historialVinculacion || []), {
      at: new Date(), accion: 'desvinculado', erpId, origen: 'kore-reversion',
      userId: null, userNombre: null, motivo: motivo ?? null, snapshot: erpLinkRemovido,
    }];
  }

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
// 2026-08-21 (caso real, folioExterno 260800164): las entradas 'REV ABO' de Kore NUNCA
// traen Aut/Numo propio — _montoSaldoLinkPorMovimiento adivina a qué movimiento pertenece
// viendo si el monto cancela EXACTO el acumulador "mío" de ESE movimiento en particular. Con
// 2+ movimientos pagando la misma CxC, si sus acumuladores coinciden en magnitud en el mismo
// punto de la secuencia, AMBOS cálculos (corridos por separado, cada uno ciego del otro)
// pueden reclamar la MISMA reversión como propia — bug preexistente de esa función
// (2026-07-28), invisible hasta ahora porque los jobs de sync normales corren con "ratchet"
// (nunca bajan un número, así que un 0 falso ahí no hace daño); la reversión corre SIN
// ratchet a propósito, así que acá sí puede desvincular algo que sigue vigente de verdad.
// Confirmado con el caso real: 2 movimientos dieron calculado=0 cuando Kore en verdad tenía
// $150 pagados entre los dos (100+50, ninguno de los 2 abonos originales revertido).
//
// Chequeo: la SUMA de lo calculado para TODOS los movimientos de este erpId debe reconciliar
// (misma tolerancia de $1 que el resto del dominio) contra lo que Kore dice pagado
// (total - saldoActual). Si no reconcilia, el cálculo no es confiable — se prefiere no tocar
// NINGÚN link (ni desvincular ni ajustar) antes que desvincular algo que puede seguir
// vigente. Devuelve false (confiable) cuando no se puede verificar (raw0 sin total/saldoActual
// numéricos) — no se bloquea por falta de dato, solo ante evidencia concreta de que está mal.
function _atribucionInconsistente(raw0, calculosPorMovimiento) {
  if (!raw0 || typeof raw0.total !== 'number' || typeof raw0.saldoActual !== 'number') return false;
  const totalPagadoSegunKore = raw0.total - raw0.saldoActual;
  const sumaCalculada = calculosPorMovimiento.reduce((a, b) => a + (b ?? 0), 0);
  return Math.abs(sumaCalculada - totalPagadoSegunKore) > 1;
}

async function _aplicarReversionAMovimiento(mov, erpId, { serieExterna, folioExterno, raw0, calculadoPrevio, calculadoBancarioPrevio, motivo } = {}) {
  if (!raw0) {
    // Sin datos frescos de Kore (no se pudo reconsultar — ver procesarReversionKore) — cae
    // al comportamiento anterior en vez de dejar el link con un número potencialmente
    // incorrecto sin ninguna corrección.
    return _removerErpIdDeMovimiento(mov, erpId, { serieExterna, folioExterno, motivo });
  }

  const link      = mov.erpLinks.find(l => l.erpId === erpId) ?? null;
  const esHumano  = erpRoutes._erpIdIdentificadoPorHumano(mov.identificadoPor, erpId);
  // calculadoPrevio: ya viene calculado desde procesarReversionKore (necesita los valores de
  // TODOS los movimientos por adelantado para el chequeo de reconciliación de arriba) — se
  // reusa acá para no recalcular ni arriesgar que diverja del que ya se usó en el chequeo.
  const calculado = calculadoPrevio !== undefined ? calculadoPrevio : (esHumano
    ? erpRoutes._montoSaldoLinkPorMovimiento(raw0, mov)
    : erpRoutes._montoSaldoLinkPorAutorizacion(raw0, mov.numeroAutorizacion));

  // 2026-08-20: log de la decisión en sí — sin esto, un caso "se ajustó en vez de
  // desvincularse" no dejaba NINGÚN rastro de por qué (a diferencia de los casos de
  // respaldo/mismatch, que ya logueaban). `antes` es el aporte que había ANTES de esta
  // reversión — compararlo contra `calculado` (el valor recién recalculado contra Kore)
  // es la pista si algún día el número no bajó cuando debería: si son iguales, Kore
  // todavía no reflejaba la reversión en el momento exacto de esta reconsulta (posible
  // carrera entre el webhook y su propio ledger consultable).
  logger.info(`[erp-reversion] erpId=${erpId} movementId=${mov._id}: aporte antes=${link?.saldoErpAportado ?? null}, recalculado tras reversión=${calculado} (esHumano=${esHumano}) -> ${(calculado == null || calculado === 0) ? 'DESVINCULADO' : 'AJUSTADO'}.`);

  if (calculado == null || calculado === 0) {
    return _removerErpIdDeMovimiento(mov, erpId, { serieExterna, folioExterno, motivo });
  }

  return _ajustarLinkTrasReversion(mov, erpId, { serieExterna, folioExterno, raw0, esHumano, calculado, calculadoBancarioPrevio, motivo });
}

async function _ajustarLinkTrasReversion(mov, erpId, { serieExterna, folioExterno, raw0, esHumano, calculado, calculadoBancarioPrevio, motivo }) {
  const link = mov.erpLinks.find(l => l.erpId === erpId);

  let mismatch = false;
  if (serieExterna != null && link.serie != null && String(link.serie) !== String(serieExterna)) mismatch = true;
  if (folioExterno != null && link.folioExterno != null && String(link.folioExterno) !== String(folioExterno)) mismatch = true;

  const erpLinkAntes = _snapshot(link);

  // 2026-08-21 (bug real, folioExterno 260800164/260800166): _backfillFormasPagoYFolioFiscal
  // calcula `saldoPagado` (bancario-únicamente) con su propio criterio interno — sin pasarle
  // calculadoBancarioPrevio, volvía a evaluar el movimiento AISLADO del resto (el mismo bug
  // de atribución cruzada que ya se corrigió arriba para saldoErpAportado), dejando
  // `saldoPagado` en $0 aunque saldoErpAportado ya viniera bien. El dropdown "CxC vinculadas"
  // (banks.component.html) muestra justo `saldoPagado` con prioridad sobre saldoActual — por
  // eso se veía "$0.00" pese a que el aporte real ya estaba correcto por dentro.
  const backfill  = erpRoutes._backfillFormasPagoYFolioFiscal(link, raw0, mov, esHumano, calculado, calculadoBancarioPrevio);
  const retencion = erpRoutes._retencionVigente(raw0);

  link.movimientosKore  = erpRoutes._movimientosKoreDesde(raw0);
  link.saldoErpAportado = calculado;
  link.saldoPagadoTotal = backfill.saldoPagadoTotal;
  link.saldoPagado      = backfill.saldoPagado;
  link.folioFiscal       = backfill.folioFiscal;
  link.tieneRetencion    = retencion.tieneRetencion;
  link.montoRetenido     = retencion.montoRetenido;
  link.saldoActual       = raw0.saldoActual ?? link.saldoActual ?? null;

  // 2026-09-01 (pedido explícito del usuario): 'ajustado' también deja rastro en el
  // historial propio del movimiento, mismo criterio que 'desvinculado' arriba.
  mov.historialVinculacion = [...(mov.historialVinculacion || []), {
    at: new Date(), accion: 'ajustado', erpId, origen: 'kore-reversion',
    userId: null, userNombre: null, motivo: motivo ?? null, snapshot: erpLinkAntes,
  }];

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
  // 2026-08-21: por defecto false — solo se marca true cuando _sincronizarConfirmandoReversion
  // efectivamente encuentra el match de `fecha` contra el historial de Kore. Ver
  // ErpReversion.model.js#confirmadaEnKore para el porqué (caso real: reversión avisada por
  // Kore que nunca aplicó de su lado).
  let confirmadaEnKore = false;
  const rango = erpRoutes._rangoDesdeFollo(folioExterno);
  if (rango) {
    try {
      ({ raw0, confirmada: confirmadaEnKore } = await _sincronizarConfirmandoReversion({ erpId, serieExterna, folioExterno, rango, fecha }));
    } catch (err) {
      logger.error(`[erp-reversion] No se pudo reconsultar Kore en vivo para erpId=${erpId} (se sigue con el comportamiento de respaldo: desvincular por completo): ${err.message}`);
    }
  } else {
    logger.warn(`[erp-reversion] erpId=${erpId}: folioExterno=${folioExterno} no tiene el formato esperado para calcular el rango de fecha — se sigue con el comportamiento de respaldo: desvincular por completo.`);
  }
  if (!raw0) {
    logger.warn(`[erp-reversion] erpId=${erpId}: sin datos frescos de Kore, cada movimiento vinculado se desvinculará POR COMPLETO (comportamiento de respaldo).`);
  }

  // 2026-08-21: calcular ANTES de tocar nada, y para los vínculos HUMANOS en una sola pasada
  // cronológica compartida (_aportesPorErpIdCronologico, erp.routes.js) — reemplaza llamar
  // _montoSaldoLinkPorMovimiento por separado para cada uno (ese enfoque no sabe nada de los
  // otros movimientos que comparten la misma CxC, y puede atribuir la MISMA reversión sin
  // Aut/Numo a 2 movimientos distintos si sus acumuladores privados coinciden en magnitud —
  // bug real confirmado en producción, folioExterno 260800164/260800166, resuelto por la
  // función cronológica en ambos casos). Vínculos de motor automático (esHumano=false) siguen
  // con _montoSaldoLinkPorAutorizacion sin cambios — es un algoritmo distinto, sin el mismo
  // riesgo de colisión (no hace neteo con signo entre movimientos).
  // El chequeo de reconciliación (_atribucionInconsistente) de abajo sigue como red de
  // seguridad final — ya no debería dispararse en los casos que motivaron este fix, pero
  // queda para cualquier escenario todavía no visto.
  let calculosPorMovimiento = null;
  let calculosBancarioPorMovimiento = null;
  if (raw0) {
    const esHumanoPorMov = movs.map(mov => erpRoutes._erpIdIdentificadoPorHumano(mov.identificadoPor, erpId));
    const movsHumanos    = movs.filter((mov, i) => esHumanoPorMov[i]);
    const aportesHumanos = erpRoutes._aportesPorErpIdCronologico(raw0, movsHumanos);
    // 2026-08-21: mismo pase cronológico compartido, filtrado a formas de pago bancarias —
    // alimenta calculadoBancarioPrevio (ver _ajustarLinkTrasReversion) para que `saldoPagado`
    // (bancario-únicamente, lo que muestra el dropdown "CxC vinculadas") no vuelva a caer en
    // el cálculo aislado ambiguo.
    const aportesHumanosBancario = erpRoutes._aportesPorErpIdCronologico(
      raw0, movsHumanos, fp => erpRoutes._esFormaPagoBancariaKore(fp.nombreFormaPago),
    );
    calculosPorMovimiento = movs.map((mov, i) => {
      if (!esHumanoPorMov[i]) return erpRoutes._montoSaldoLinkPorAutorizacion(raw0, mov.numeroAutorizacion);
      const idxEnHumanos = movsHumanos.indexOf(mov);
      return aportesHumanos.get(idxEnHumanos) ?? null;
    });
    calculosBancarioPorMovimiento = movs.map((mov, i) => {
      // Vínculos de motor (esHumano=false): _backfillFormasPagoYFolioFiscal usa `aporteNuevo`
      // directo para saldoPagado en ese caso (sin distinguir bancario) — mismo criterio acá.
      if (!esHumanoPorMov[i]) return calculosPorMovimiento[i];
      const idxEnHumanos = movsHumanos.indexOf(mov);
      return aportesHumanosBancario.get(idxEnHumanos) ?? null;
    });
  }

  const resultados = [];
  let atribucionConfiable = true;
  if (calculosPorMovimiento && _atribucionInconsistente(raw0, calculosPorMovimiento)) {
    atribucionConfiable = false;
    const sumaCalculada = calculosPorMovimiento.reduce((a, b) => a + (b ?? 0), 0);
    const totalPagadoSegunKore = raw0.total - raw0.saldoActual;
    logger.error(`[erp-reversion] erpId=${erpId}: atribución ambigua entre ${movs.length} movimientos vinculados — suma calculada=${sumaCalculada}, Kore reporta pagado (total-saldoActual)=${totalPagadoSegunKore}. NO se toca ningún link (ni se desvincula ni se ajusta) — queda para revisión manual.`);
    for (const mov of movs) {
      resultados.push({ movementId: mov._id, tipo: 'sin_tocar', mismatch: false });
    }
  } else {
    for (let i = 0; i < movs.length; i++) {
      const calculadoPrevio         = calculosPorMovimiento ? calculosPorMovimiento[i] : undefined;
      const calculadoBancarioPrevio = calculosBancarioPorMovimiento ? calculosBancarioPorMovimiento[i] : undefined;
      resultados.push(await _aplicarReversionAMovimiento(movs[i], erpId, { serieExterna, folioExterno, raw0, calculadoPrevio, calculadoBancarioPrevio, motivo }));
    }
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
    confirmadaEnKore,
    atribucionConfiable,
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

  // Señal para que la bandeja "Reversiones CxC" (admin-ops-panel) se autorefresque en vez de
  // depender de que el usuario recargue a mano — no es específico de un banco (la bandeja lista
  // reversiones de todos), por eso emitToAll y no emitToBanco. Solo un id: el frontend
  // reconsulta vía GET /api/erp/cxc-reversiones, que ya está protegido por el permiso
  // BANKS_ERP_REVERSIONES, así que no hay fuga de datos aunque el evento llegue a sockets sin
  // ese permiso.
  emitToAll('erp:reversion:created', { reversionId: reversion._id });

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
