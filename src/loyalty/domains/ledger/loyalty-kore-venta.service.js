'use strict';

/**
 * domains/ledger/loyalty-kore-venta.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — webhook Kore→Numo de venta (acumulación y/o devengación
 * de puntos combinadas en una sola operación atómica). Contrato HTTP completo
 * en `Docs/integracion-kore-puntos-lealtad-DRAFT.md` (v2, 2026-11-06) — este
 * archivo es la implementación de esa §3/§4, no la repite.
 *
 * REDISEÑO 2026-11-06 (pedido explícito del usuario): Kore puede realizar
 * varios ABONOS (cobros parciales) contra la MISMA CxC a lo largo del tiempo.
 * El payload pasa de describir una venta plana a describir una CxC con un
 * arreglo `abonos[]` — campos que identifican la CxC (`telefono`,
 * `koreClienteId`, `serieFolio`, `conceptoId`, `metodoPago`) quedan a nivel
 * raíz, y campos que varían por pago (`fecha`, `subtotal`, `total`,
 * `formasPago`, `usuarioAplicaId`, `puntosAbonados`) bajan a cada abono,
 * identificado por `idAbono` (el id real del movimiento que ya manda Kore —
 * también sirve como llave para futuros ajustes puntuales por CxC). El campo
 * `usuarioAplicaNombre` del contrato v1 se ELIMINÓ por completo.
 *
 * Decisiones de negocio clave (todas confirmadas con el usuario, no inferidas):
 *  - La acumulación y el canje son gates COMPLETAMENTE INDEPENDIENTES: que la
 *    venta no califique para ganar puntos nuevos nunca bloquea el descuento de
 *    puntos que el cliente ya tenía (y viceversa). Esto aplica POR ABONO.
 *  - Formas de pago mixtas: el monto calificable se calcula PROPORCIONALMENTE
 *    al subtotal pagado con formas de pago que sí acumulan (nunca todo-o-nada),
 *    usando el `subtotal`/`total`/`formasPago` de CADA ABONO por separado.
 *  - Nivel (2026-11-06): `puntosAbonados` es OPCIONAL por abono. Si Kore lo
 *    manda, Numo confía en ese número directamente (ya NO recalcula con
 *    `calcularPuntos()`). Si NO lo manda (ej. el adaptador interno de
 *    Solicitudes de Cobro, que no tiene un número ya calculado por Kore para
 *    ese flujo), Numo lo calcula POR SU CUENTA con
 *    `calcularPuntos(montoBaseCalculoCents, %nivel)`, usando como `%` la
 *    MISMA fuente autoritativa que ahora gestiona Kore:
 *    `LoyaltyKoreNivel.porcentaje` (ver `loyalty-config.service.js#obtenerPorcentajeNivel`,
 *    ya NO `GlobalConfig.lealtad.PORCENTAJE_<NIVEL>`). Si ese cálculo de
 *    respaldo no tiene `%` configurado para el nivel de la tarjeta, ESE abono
 *    puntual no puede acreditar nivel — se registra como proceso fallido
 *    (`registrarVentaFallida`, con el `idAbono`) y sus puntos de nivel quedan
 *    en 0, sin bloquear canje ni promoción de ese mismo abono (gates
 *    independientes, igual criterio que concepto/forma de pago desconocidos).
 *    En cualquiera de los dos caminos, Numo NUNCA cede su propia validación de
 *    negocio: el gate `califica` (PUE + `participaLealtad` +
 *    `concepto.acumulaPuntos` + monto calificable > 0) sigue aplicando — si un
 *    abono no califica, sus puntos de nivel NO se acreditan aunque Kore los
 *    haya mandado. El `%` de nivel se guarda siempre como metadata informativa
 *    (`porcentajeAplicado`) en la fila, venga o no `puntosAbonados`.
 *  - Promoción: SIGUE calculándose 100% del lado de Numo
 *    (`calcularPuntos(montoProrrateado, %promo)`), sobre el monto prorrateado
 *    de CADA ABONO — confirmado explícitamente por el usuario.
 *  - Un `koreId` de concepto/forma de pago desconocido en el catálogo local
 *    intenta UN resync on-the-fly contra Kore (una sola vez por REQUEST
 *    completo, no por abono); si sigue sin aparecer, se trata como "no
 *    acumula" (conservador, nunca bloquea el abono) Y se registra como
 *    proceso fallido para que alguien lo configure.
 *  - `origen:'kore'` en el ledger. El índice único sigue siendo
 *    `(origen, referencia_externa)`: cada ABONO puede generar hasta TRES filas
 *    (ACUMULACION de nivel, ACUMULACION de promoción, CANJE), así que
 *    `referenciaExterna` ahora lleva el `idAbono` ADEMÁS del sufijo de tipo
 *    (`"<serieFolio>:<idAbono>:ACUMULACION"` / `":PROMOCION"` / `":CANJE"`,
 *    ver `_refAcumulacion`/`_refPromocion`/`_refCanje`) — esto hace que la
 *    idempotencia sea por `(serieFolio, idAbono)`, no solo por `serieFolio`:
 *    un abono nuevo de una CxC ya conocida se procesa igual, uno repetido se
 *    responde idempotente sin duplicar.
 *  - Cada abono se aplica en su propio SAVEPOINT anidado dentro de la
 *    transacción que bloquea la tarjeta UNA sola vez para todo el request (son
 *    la misma tarjeta) — un abono que falla por una regla de negocio (tarjeta
 *    inactiva, saldo insuficiente, reenvío con datos distintos) hace
 *    `ROLLBACK TO SAVEPOINT` de ESE abono únicamente, sin afectar a los demás
 *    abonos ya aplicados en el mismo request. Solo un error INESPERADO (no de
 *    negocio, ni una carrera de índice único recuperable) aborta el request
 *    completo y activa el reintento acotado de `procesarVentaKore`.
 *  - Reintentos acotados DENTRO de la misma llamada ante errores inesperados
 *    (mismo criterio que `erp-reversion.service.js`), nunca un 200 que oculte
 *    un fallo en segundo plano.
 *  - `procesarLoteVentasKore` (2026-10-27, pedido explícito del usuario): variante en lote de
 *    `POST /venta`, hasta `MAX_VENTAS_POR_LOTE` ventas por request, procesadas en serie, cada una
 *    independiente (una que falla no aborta las demás) — cada "venta" del lote trae su propio
 *    `abonos[]` con el contrato nuevo, ver su propio docstring más abajo.
 */

const { Transaction, Op } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');
const {
  LoyaltyCard, LoyaltyPointsLedger, LoyaltyKoreConcepto, LoyaltyKoreFormaPago,
} = require('../../../shared/models/postgres');
const { BadRequestError, ConflictError, NotFoundError } = require('../../../shared/errors/AppError');
const { toCents, fromCents, calcularPuntos, normalizarTelefono } = require('../../shared/puntos.util');
const configService = require('../config/loyalty-config.service');
const { _resolverCentroCostoPorSerieFolio, _obtenerCentroCosto, _aplicarMovimiento } = require('./loyalty-ledger.service');
const { registrarVentaFallida } = require('./loyalty-venta-fallida.service');
const { logger } = require('../../../shared/utils/logger');

const ORIGEN_KORE = 'kore';
const TOLERANCIA_PESOS = 0.01;
// Mismo formato que loyalty-ledger.service.js#SERIE_FOLIO_RE — DUPLICADO a propósito (misma razón
// documentada ahí: evitar un acoplamiento extra por una sola línea de regex entre dos servicios).
// Validarlo acá primero permite distinguir "serieFolio con formato inválido" (400, error de
// integración real) de "la serie no tiene centro de costo mapeado" (soft-fail más abajo, no
// bloquea la venta) — _resolverCentroCostoPorSerieFolio lanza el mismo BadRequestError para ambos
// casos, así que sin esta validación previa no se podrían distinguir.
const SERIE_FOLIO_RE = /^([A-Za-z0-9]{1,10})-(\d{1,20})$/;
const REINTENTOS_DELAYS_MS = [500, 1500]; // 3 intentos en total (1 + 2 reintentos)
const _sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

// `usuarioAplicaNombre` desapareció del contrato (2026-11-06) — el ledger sigue queriendo un
// `usuarioNombre` legible para el detalle de movimientos, así que se usa un valor fijo descriptivo,
// mismo criterio que `ACTOR_WEBHOOK` en loyalty-kore-cliente.service.js. `usuarioId` SÍ sigue
// viniendo real (`abono.usuarioAplicaId`).
const USUARIO_NOMBRE_WEBHOOK = 'Kore (webhook venta)';

function _refAcumulacion(serieFolio, idAbono) { return `${serieFolio}:${idAbono}:ACUMULACION`; }
function _refCanje(serieFolio, idAbono) { return `${serieFolio}:${idAbono}:CANJE`; }
// Fila separada para el bono de promoción (2026-10-27, pedido explícito del usuario) — mismo
// patrón de sufijo que ACUMULACION/CANJE, para que las tres convivan bajo el mismo índice único
// (origen, referencia_externa) sin chocar entre sí.
function _refPromocion(serieFolio, idAbono) { return `${serieFolio}:${idAbono}:PROMOCION`; }
/** Las 3 referencias 'kore' posibles de UN abono puntual — usado tanto para el chequeo de
 *  idempotencia inicial como para recuperar el resultado ya aplicado tras una carrera. */
function _refsKoreDeAbono(serieFolio, idAbono) {
  return [_refAcumulacion(serieFolio, idAbono), _refCanje(serieFolio, idAbono), _refPromocion(serieFolio, idAbono)];
}

// Blindaje cruzado manual↔kore (2026-10-27, pedido explícito del usuario, escenario simétrico al
// ya resuelto del lado manual en loyalty-ledger.service.js#registrarAcumulacion/registrarCanje): el
// botón manual escribe la fila de nivel con referenciaExterna = EL SERIE-FOLIO PLANO (sin sufijo —
// ver registrarAcumulacion), y `${serieFolio}:PROMOCION`/`${serieFolio}:CANJE` para promoción y
// canje. El flujo manual NO tiene noción de abonos individuales — si la venta ya fue acreditada a
// mano, TODOS los abonos que Kore mande para ese serieFolio deben tratarse como ya cubiertos,
// cualquiera sea su `idAbono`.
function _refsManual(serieFolio) {
  return [serieFolio, `${serieFolio}:PROMOCION`, `${serieFolio}:CANJE`];
}

/** Resuelve la tarjeta por koreClienteId (si viene) o por teléfono normalizado — mismo criterio
 *  de prioridad documentado en §3/§4 del contrato. */
async function _resolverTarjeta({ telefono, koreClienteId }) {
  if (koreClienteId) {
    const card = await LoyaltyCard.findOne({ where: { koreClienteId: String(koreClienteId).trim() } });
    if (card) return card;
  }
  if (!telefono) throw new NotFoundError('Tarjeta de Lealtad');
  let telefonoNorm;
  try {
    telefonoNorm = normalizarTelefono(telefono);
  } catch (err) {
    throw new BadRequestError(err.message);
  }
  const card = await LoyaltyCard.findOne({ where: { telefono: telefonoNorm } });
  if (!card) throw new NotFoundError('Tarjeta de Lealtad');
  return card;
}

async function consultarSaldoKore({ telefono, koreClienteId } = {}) {
  const card = await _resolverTarjeta({ telefono, koreClienteId });
  return {
    tieneTarjeta: true,
    saldoDisponible: Number(card.saldoPuntos),
    nivel: card.nivel,
  };
}

/** Valida UN abono dentro de `abonos[]`. `index` es solo para mensajes de error legibles. */
function _validarAbono(raw, index) {
  const {
    idAbono, fecha, subtotal, total, formasPago, usuarioAplicaId, puntosAbonados,
  } = raw ?? {};

  const faltantes = [];
  if (!idAbono) faltantes.push(`abonos[${index}].idAbono`);
  if (!fecha) faltantes.push(`abonos[${index}].fecha`);
  if (!(Number(subtotal) > 0)) faltantes.push(`abonos[${index}].subtotal`);
  if (!(Number(total) > 0)) faltantes.push(`abonos[${index}].total`);
  if (!Array.isArray(formasPago) || formasPago.length === 0) faltantes.push(`abonos[${index}].formasPago`);
  if (!usuarioAplicaId) faltantes.push(`abonos[${index}].usuarioAplicaId`);
  // `puntosAbonados` es OPCIONAL (2026-11-06, pedido explícito del usuario): si viene, debe ser un
  // número >= 0 (igual validación que antes); si se omite, no es un error — Numo lo calcula por su
  // cuenta con el % real del nivel (ver _procesarUnAbonoNuevo).
  const puntosAbonadosProvisto = puntosAbonados !== undefined && puntosAbonados !== null;
  if (puntosAbonadosProvisto && (Number.isNaN(Number(puntosAbonados)) || Number(puntosAbonados) < 0)) {
    faltantes.push(`abonos[${index}].puntosAbonados`);
  }
  if (faltantes.length) {
    throw new BadRequestError(`Faltan campos requeridos: ${faltantes.join(', ')}.`);
  }

  const formasPagoParsed = formasPago.map(f => ({
    formaPagoId: String(f?.formaPagoId ?? '').trim(),
    importe: Number(f?.importe),
  }));
  const lineaInvalida = formasPagoParsed.find(f => !f.formaPagoId || !(f.importe > 0));
  if (lineaInvalida) {
    throw new BadRequestError(`Cada línea de formasPago del abono "${idAbono}" requiere formaPagoId e importe > 0.`);
  }

  const totalNum = Math.round(Number(total) * 100) / 100;
  const sumaFormasPago = Math.round(formasPagoParsed.reduce((s, f) => s + f.importe, 0) * 100) / 100;
  if (Math.abs(sumaFormasPago - totalNum) > TOLERANCIA_PESOS) {
    throw new BadRequestError(`La suma de formasPago del abono "${idAbono}" no coincide con su total.`);
  }

  return {
    idAbono: String(idAbono).trim(),
    fecha: String(fecha),
    subtotal: Number(subtotal),
    total: totalNum,
    formasPago: formasPagoParsed,
    usuarioAplicaId: String(usuarioAplicaId).trim(),
    puntosAbonados: puntosAbonadosProvisto ? Number(puntosAbonados) : null,
  };
}

function _validarPayloadVenta(payload) {
  const {
    telefono, koreClienteId, serieFolio, conceptoId, metodoPago, abonos,
  } = payload ?? {};

  const faltantes = [];
  if (!telefono) faltantes.push('telefono');
  if (!serieFolio) faltantes.push('serieFolio');
  if (!conceptoId) faltantes.push('conceptoId');
  if (!metodoPago) faltantes.push('metodoPago');
  if (!Array.isArray(abonos) || abonos.length === 0) faltantes.push('abonos');
  if (faltantes.length) {
    throw new BadRequestError(`Faltan campos requeridos: ${faltantes.join(', ')}.`);
  }

  if (!['PUE', 'PPD'].includes(String(metodoPago).toUpperCase())) {
    throw new BadRequestError('metodoPago debe ser "PUE" o "PPD".');
  }
  if (!SERIE_FOLIO_RE.test(String(serieFolio).trim())) {
    throw new BadRequestError('serieFolio debe tener el formato SERIE-FOLIO (ej. A0-260605320).');
  }

  const abonosParsed = abonos.map((a, i) => _validarAbono(a, i));
  const idsVistos = new Set();
  for (const a of abonosParsed) {
    if (idsVistos.has(a.idAbono)) {
      throw new BadRequestError(`idAbono duplicado dentro del mismo request: "${a.idAbono}".`);
    }
    idsVistos.add(a.idAbono);
  }

  return {
    telefono: String(telefono).trim(),
    koreClienteId: koreClienteId ? String(koreClienteId).trim() : null,
    serieFolio: String(serieFolio).trim(),
    conceptoId: String(conceptoId).trim(),
    metodoPago: String(metodoPago).toUpperCase(),
    abonos: abonosParsed,
  };
}

/** Busca el concepto/forma de pago por koreId; si no existe, intenta UN resync del catálogo
 *  correspondiente y vuelve a buscar — conservador: un id que sigue sin aparecer se trata como "no
 *  acumula"/"no es Puntos" (nunca bloquea el abono) y queda registrado como proceso fallido para
 *  que alguien lo configure. `estado` se comparte entre TODOS los abonos de un mismo request, así
 *  que el resync ocurre A LO SUMO una vez por request, nunca una vez por abono. */
async function _resolverConcepto(conceptoId, estado) {
  let fila = await LoyaltyKoreConcepto.findOne({ where: { koreId: conceptoId } });
  if (!fila && !estado.conceptosResincronizados) {
    estado.conceptosResincronizados = true;
    try {
      await configService.sincronizarConceptosKore();
    } catch (err) {
      logger.warn(`[loyalty-kore-venta] No se pudo resincronizar conceptos de Kore: ${err.message}`);
    }
    fila = await LoyaltyKoreConcepto.findOne({ where: { koreId: conceptoId } });
  }
  if (!fila) {
    estado.conceptoDesconocido = conceptoId;
    return { acumulaPuntos: false, esPuntos: false };
  }
  return { acumulaPuntos: fila.acumulaPuntos, esPuntos: false };
}

async function _resolverFormaPago(formaPagoId, estado) {
  let fila = await LoyaltyKoreFormaPago.findOne({ where: { koreId: formaPagoId } });
  if (!fila && !estado.formasPagoResincronizadas) {
    estado.formasPagoResincronizadas = true;
    try {
      await configService.sincronizarFormasPagoKore();
    } catch (err) {
      logger.warn(`[loyalty-kore-venta] No se pudo resincronizar formas de pago de Kore: ${err.message}`);
    }
    fila = await LoyaltyKoreFormaPago.findOne({ where: { koreId: formaPagoId } });
  }
  if (!fila) {
    estado.formasPagoDesconocidas = estado.formasPagoDesconocidas || [];
    estado.formasPagoDesconocidas.push(formaPagoId);
    return { acumulaPuntos: false, esPuntos: false };
  }
  return { acumulaPuntos: fila.acumulaPuntos, esPuntos: fila.esPuntos };
}

/** Reconstruye el resultado de UN abono YA procesado antes (idempotencia por `idAbono`) sin volver
 *  a escribir ni recalcular elegibilidad. */
function _respuestaIdempotenteAbono(idAbono, filas) {
  const canje = filas.find(f => f.tipo === 'CANJE');
  const puntosAcumulados = filas.filter(f => f.tipo === 'ACUMULACION').reduce((s, f) => s + Number(f.puntos), 0);
  const puntosDescontados = canje ? Math.abs(Number(canje.puntos)) : 0;
  return {
    idAbono, success: true, status: 'aplicado', puntosDescontados, puntosAcumulados,
  };
}

/**
 * Aplica UN abono nuevo (nunca antes visto) dentro de su propio savepoint (`t` ya es la transacción
 * del savepoint, no la del request completo — ver el caller). Puede lanzar `BadRequestError` (tarjeta
 * inactiva, saldo insuficiente) o `SequelizeUniqueConstraintError` (carrera) — ambos los maneja el
 * caller, nunca este helper.
 */
async function _procesarUnAbonoNuevo({
  t, card, datos, abono, estado, centroCosto, participaLealtad, concepto, pctNivelInfo, promoVigente,
}) {
  // ── Canje: gate independiente, se resuelve ANTES que la acumulación ──────
  let formaPuntosLinea = null;
  const formasResueltas = [];
  for (const linea of abono.formasPago) {
    // eslint-disable-next-line no-await-in-loop -- universo chico (formas de pago de un solo abono).
    const resuelta = await _resolverFormaPago(linea.formaPagoId, estado);
    formasResueltas.push({ ...linea, ...resuelta });
    if (resuelta.esPuntos) formaPuntosLinea = { ...linea, ...resuelta };
  }
  if (estado.formasPagoDesconocidas?.length) {
    await registrarVentaFallida(
      datos.serieFolio, datos,
      `Forma(s) de pago desconocida(s) en Numo (abono ${abono.idAbono}): ${estado.formasPagoDesconocidas.join(', ')}`,
    );
    // Ya se reportó para ESTE abono — se limpia para que un id desconocido repetido en un abono
    // siguiente del mismo request se reporte de nuevo (con su propio idAbono en el mensaje) en vez
    // de quedar arrastrado en silencio.
    estado.formasPagoDesconocidas = [];
  }

  const puntosDescontadosPesos = formasResueltas.filter(f => f.esPuntos).reduce((s, f) => s + f.importe, 0);
  const puntosDescontadosCents = toCents(puntosDescontadosPesos);
  if (puntosDescontadosCents > 0 && !formaPuntosLinea) {
    // No debería poder pasar (puntosDescontadosPesos solo suma líneas con esPuntos:true), queda
    // como salvaguarda defensiva.
    throw new BadRequestError(`No se pudo resolver la forma de pago "Puntos" del abono "${abono.idAbono}".`);
  }

  // ── Acumulación: gate independiente, nunca bloqueado por el canje ───────
  const montoCalificablePesos = formasResueltas
    .filter(f => !f.esPuntos && f.acumulaPuntos)
    .reduce((s, f) => s + f.importe, 0);
  const califica = datos.metodoPago === 'PUE' && participaLealtad && concepto.acumulaPuntos && montoCalificablePesos > 0;

  let puntosNivelCents = 0;
  let puntosPromoCents = 0;
  let montoBaseCalculoCents = null;
  let pctNivelAplicado = null;

  if (califica) {
    const montoProrrateadoPesos = (montoCalificablePesos / abono.total) * abono.subtotal;
    montoBaseCalculoCents = toCents(montoProrrateadoPesos);
    pctNivelAplicado = pctNivelInfo != null ? Number(pctNivelInfo) : null;
    if (abono.puntosAbonados != null) {
      // Kore ya mandó el número calculado para este abono — se confía directo, sin recalcular.
      puntosNivelCents = toCents(abono.puntosAbonados);
    } else if (pctNivelInfo != null) {
      // Cálculo de respaldo (ej. adaptador de Solicitudes de Cobro, que no recibe un número ya
      // calculado por Kore) — misma fuente de % autoritativa que Kore gestiona.
      puntosNivelCents = calcularPuntos(montoBaseCalculoCents, pctNivelInfo);
    } else {
      // Sin `puntosAbonados` Y sin `%` configurado para este nivel: este abono puntual no puede
      // acreditar nivel — canje/promoción siguen su propio camino, gates independientes.
      await registrarVentaFallida(
        datos.serieFolio, datos,
        `No se pudo calcular la acumulación de nivel del abono "${abono.idAbono}": falta "puntosAbonados" `
        + `y el nivel "${card.nivel}" no tiene % configurado en el catálogo de niveles de Kore.`,
      );
    }
    // Promoción: SIGUE siendo 100% cálculo de Numo, sobre el mismo monto prorrateado del abono.
    puntosPromoCents = promoVigente ? calcularPuntos(montoBaseCalculoCents, Number(promoVigente.porcentajeBono)) : 0;
  }

  if (puntosDescontadosCents > 0) {
    if (!card.activa) throw new BadRequestError(`La tarjeta está inactiva, no se puede descontar puntos (abono "${abono.idAbono}").`);
    const saldoActualCents = toCents(card.saldoPuntos);
    if (puntosDescontadosCents > saldoActualCents) {
      throw new BadRequestError(`El monto de puntos a usar supera el saldo disponible del cliente (abono "${abono.idAbono}").`);
    }
  }
  if ((puntosNivelCents > 0 || puntosPromoCents > 0) && !card.activa) {
    throw new BadRequestError(`La tarjeta está inactiva, no se puede acumular puntos (abono "${abono.idAbono}").`);
  }

  let entryCanje = null;
  let entryAcumulacionNivel = null;
  let entryAcumulacionPromocion = null;

  if (puntosDescontadosCents > 0) {
    entryCanje = await _aplicarMovimiento(t, card, {
      tipo: 'CANJE',
      puntos: fromCents(-puntosDescontadosCents),
      centroCostoId: centroCosto?.id ?? null,
      origen: ORIGEN_KORE,
      referenciaExterna: _refCanje(datos.serieFolio, abono.idAbono),
      usuarioId: abono.usuarioAplicaId,
      usuarioNombre: USUARIO_NOMBRE_WEBHOOK,
    });
  }

  if (puntosNivelCents > 0) {
    entryAcumulacionNivel = await _aplicarMovimiento(t, card, {
      tipo: 'ACUMULACION',
      puntos: fromCents(puntosNivelCents),
      montoVenta: abono.subtotal,
      // Monto prorrateado del ABONO — metadata informativa (el detalle en pantalla puede mostrar
      // sobre qué monto se prorrateó), ya no es la base matemática de `puntosNivelCents`.
      montoBaseCalculo: fromCents(montoBaseCalculoCents),
      montoVentaTotal: abono.total,
      porcentajeAplicado: pctNivelAplicado,
      nivelAplicado: card.nivel,
      centroCostoId: centroCosto?.id ?? null,
      origen: ORIGEN_KORE,
      referenciaExterna: _refAcumulacion(datos.serieFolio, abono.idAbono),
      usuarioId: abono.usuarioAplicaId,
      usuarioNombre: USUARIO_NOMBRE_WEBHOOK,
    });
  }

  // Fila separada de la promoción (sufijo `:PROMOCION`) — nunca pisa el referenciaExterna de la
  // fila de nivel, así ambas conviven bajo el mismo índice único.
  if (puntosPromoCents > 0) {
    entryAcumulacionPromocion = await _aplicarMovimiento(t, card, {
      tipo: 'ACUMULACION',
      puntos: fromCents(puntosPromoCents),
      montoVenta: abono.subtotal,
      montoBaseCalculo: fromCents(montoBaseCalculoCents),
      montoVentaTotal: abono.total,
      porcentajeAplicado: Number(promoVigente.porcentajeBono),
      promocionNombre: promoVigente.nombre,
      promocionPorcentaje: promoVigente.porcentajeBono,
      centroCostoId: centroCosto?.id ?? null,
      origen: ORIGEN_KORE,
      referenciaExterna: _refPromocion(datos.serieFolio, abono.idAbono),
      usuarioId: abono.usuarioAplicaId,
      usuarioNombre: USUARIO_NOMBRE_WEBHOOK,
    });
  }

  const huboMovimiento = !!(entryCanje || entryAcumulacionNivel || entryAcumulacionPromocion);
  const puntosAcumulados = (entryAcumulacionNivel ? Number(entryAcumulacionNivel.puntos) : 0)
    + (entryAcumulacionPromocion ? Number(entryAcumulacionPromocion.puntos) : 0);

  return {
    idAbono: abono.idAbono,
    success: true,
    status: huboMovimiento ? 'aplicado' : 'no_califica',
    puntosDescontados: entryCanje ? Math.abs(Number(entryCanje.puntos)) : 0,
    puntosAcumulados,
  };
}

async function _procesarVentaKoreInterno(datos) {
  const card = await _resolverTarjeta({ telefono: datos.telefono, koreClienteId: datos.koreClienteId });

  // Blindaje cruzado manual↔kore — buscar ANTES de tocar nada más. Si la venta COMPLETA ya fue
  // acreditada a mano, NINGÚN abono de Kore debe duplicarla (el flujo manual no distingue abonos).
  const filasManual = await LoyaltyPointsLedger.findAll({
    where: { origen: 'manual', referenciaExterna: { [Op.in]: _refsManual(datos.serieFolio) } },
  });
  if (filasManual.length) {
    const canje = filasManual.find(f => f.tipo === 'CANJE');
    const puntosAcumulados = filasManual.filter(f => f.tipo === 'ACUMULACION').reduce((s, f) => s + Number(f.puntos), 0);
    const puntosDescontados = canje ? Math.abs(Number(canje.puntos)) : 0;
    return {
      serieFolio: datos.serieFolio,
      status: 'aplicado',
      puntosDescontados,
      puntosAcumulados,
      saldoResultante: Number(card.saldoPuntos),
      abonos: datos.abonos.map(a => ({
        idAbono: a.idAbono,
        success: true,
        status: 'aplicado',
        puntosDescontados: null,
        puntosAcumulados: null,
        nota: 'Esta venta ya fue acreditada manualmente — el flujo manual no distingue abonos individuales.',
      })),
    };
  }

  // Idempotencia por abono — se consulta ANTES de resolver concepto/centro de costo, para no hacer
  // ese trabajo de más si todos los abonos del request ya se procesaron antes.
  const todasLasRefsKore = datos.abonos.flatMap(a => _refsKoreDeAbono(datos.serieFolio, a.idAbono));
  const filasKoreExistentes = await LoyaltyPointsLedger.findAll({
    where: { origen: ORIGEN_KORE, referenciaExterna: { [Op.in]: todasLasRefsKore } },
  });
  const filasPorAbono = new Map();
  for (const fila of filasKoreExistentes) {
    // referenciaExterna = "<serieFolio>:<idAbono>:<TIPO>" — serieFolio nunca lleva ':' (ver
    // SERIE_FOLIO_RE) e idAbono es el id de Mongo de Kore (hex, tampoco lleva ':'), así que split
    // por ':' separa los 3 segmentos de forma determinística.
    const idAbono = fila.referenciaExterna.split(':')[1];
    if (!filasPorAbono.has(idAbono)) filasPorAbono.set(idAbono, []);
    filasPorAbono.get(idAbono).push(fila);
  }

  const estado = {};
  const concepto = await _resolverConcepto(datos.conceptoId, estado);
  if (estado.conceptoDesconocido) {
    await registrarVentaFallida(datos.serieFolio, datos, `Concepto desconocido en Numo: ${estado.conceptoDesconocido}`);
  }

  let centroCosto = null;
  let participaLealtad = false;
  try {
    const { centroCostoId } = await _resolverCentroCostoPorSerieFolio(datos.serieFolio);
    centroCosto = await _obtenerCentroCosto(centroCostoId);
    participaLealtad = !!centroCosto.participaLealtad;
  } catch (err) {
    // Serie sin centro de costo mapeado: no bloquea el request, simplemente no acumula ningún
    // abono (conservador, mismo criterio que un concepto/forma de pago desconocidos) — SÍ se
    // registra como proceso fallido porque normalmente indica una serie mal configurada.
    await registrarVentaFallida(datos.serieFolio, datos, `Serie sin centro de costo mapeado: ${err.message}`);
  }

  // Pct de nivel y promoción vigente son los mismos para TODOS los abonos del request (no varían
  // por abono) — se resuelven una sola vez acá, no dentro del loop. `pctNivelInfo` sirve DOS
  // propósitos: (1) metadata informativa en la fila (`porcentajeAplicado`) venga o no
  // `puntosAbonados`; (2) cálculo de respaldo cuando un abono NO trae `puntosAbonados` (ver
  // _procesarUnAbonoNuevo) — fuente única `LoyaltyKoreNivel.porcentaje`, nunca lanza si no está
  // configurado (el caller decide: metadata nula, o proceso fallido solo si hace falta para calcular).
  const pctNivelInfo = await configService.obtenerPorcentajeNivel(card.nivel);
  const promoVigente = await configService.obtenerPromocionVigente();

  const resultadosAbonos = [];

  await sequelize.transaction(async (t) => {
    let cardLocked = await LoyaltyCard.findByPk(card.id, { transaction: t, lock: Transaction.LOCK.UPDATE });
    if (!cardLocked) throw new NotFoundError('Tarjeta de Lealtad');

    for (const abono of datos.abonos) {
      try {
        const existentes = filasPorAbono.get(abono.idAbono);
        if (existentes?.length) {
          const nivelRow = existentes.find(f => f.referenciaExterna?.endsWith(':ACUMULACION'));
          // 409 (aislado a ESTE abono, no aborta el request): mismo idAbono, pero el subtotal que
          // generó la acumulación guardada no coincide con el que llega ahora.
          if (nivelRow && Math.abs(Number(nivelRow.montoVenta) - abono.subtotal) > TOLERANCIA_PESOS) {
            throw new ConflictError(`El abono "${abono.idAbono}" ya fue reportado antes con un subtotal distinto.`);
          }
          resultadosAbonos.push(_respuestaIdempotenteAbono(abono.idAbono, existentes));
          continue; // eslint-disable-line no-continue
        }

        // eslint-disable-next-line no-await-in-loop -- cada abono depende del saldo que dejó el
        // anterior dentro del mismo request; no se pueden paralelizar.
        const resultadoAbono = await sequelize.transaction(
          { transaction: t },
          t2 => _procesarUnAbonoNuevo({
            t: t2, card: cardLocked, datos, abono, estado, centroCosto, participaLealtad, concepto, pctNivelInfo, promoVigente,
          }),
        );
        resultadosAbonos.push(resultadoAbono);
      } catch (err) {
        if (err?.statusCode) {
          // Error de negocio (400/404/409/422) aislado a este abono — el savepoint ya se revirtió
          // solo, los demás abonos del request no se ven afectados.
          resultadosAbonos.push({
            idAbono: abono.idAbono, success: false, statusCode: err.statusCode, error: err.message,
          });
        } else if (err?.name === 'SequelizeUniqueConstraintError') {
          // Carrera: otro request con el mismo (serieFolio, idAbono) ganó el insert entre nuestro
          // chequeo de idempotencia y este punto. El savepoint de este abono ya se revirtió solo —
          // `t` (la transacción del request) sigue viva, así que se puede releer acá mismo.
          // eslint-disable-next-line no-await-in-loop
          const filasGanador = await LoyaltyPointsLedger.findAll({
            where: { origen: ORIGEN_KORE, referenciaExterna: { [Op.in]: _refsKoreDeAbono(datos.serieFolio, abono.idAbono) } },
            transaction: t,
          });
          if (filasGanador.length) {
            resultadosAbonos.push(_respuestaIdempotenteAbono(abono.idAbono, filasGanador));
          } else {
            // El ganador de la carrera todavía no comiteó (transacción concurrente en vuelo) — no
            // hay forma segura de resolverlo leyendo DENTRO de nuestra propia transacción (aislamiento
            // READ COMMITTED no ve commits ajenos pendientes). Se escala para abortar el request
            // COMPLETO y dejar que el reintento acotado de procesarVentaKore lo reprocese desde cero
            // (REINTENTOS_DELAYS_MS le da tiempo al otro request a terminar de comitear).
            throw err;
          }
        } else {
          throw err; // error inesperado -> aborta el request completo, lo maneja procesarVentaKore
        }
      }

      // Releer el saldo real tras CADA abono (éxito o fallo aislado por savepoint) para que el
      // siguiente abono parta del valor YA comprometido en esta transacción, nunca de un
      // `card.saldoPuntos` en memoria que pudo quedar desincronizado por un ROLLBACK TO SAVEPOINT.
      // eslint-disable-next-line no-await-in-loop
      cardLocked = await LoyaltyCard.findByPk(card.id, { transaction: t, lock: Transaction.LOCK.UPDATE });
    }
  });

  const puntosAcumulados = resultadosAbonos.reduce((s, r) => s + (r.success ? Number(r.puntosAcumulados || 0) : 0), 0);
  const puntosDescontados = resultadosAbonos.reduce((s, r) => s + (r.success ? Number(r.puntosDescontados || 0) : 0), 0);
  const cardFinal = await LoyaltyCard.findByPk(card.id);

  let status;
  if (resultadosAbonos.every(r => !r.success)) status = 'fallido';
  else if (resultadosAbonos.some(r => !r.success)) status = 'parcial';
  else if (resultadosAbonos.some(r => r.status === 'aplicado')) status = 'aplicado';
  else status = 'no_califica';

  return {
    serieFolio: datos.serieFolio,
    status,
    puntosDescontados,
    puntosAcumulados,
    saldoResultante: Number(cardFinal.saldoPuntos),
    abonos: resultadosAbonos,
  };
}

/**
 * Punto de entrada del webhook `POST /api/loyalty/kore/venta`. Reintenta
 * acotadamente errores inesperados (no de negocio) DENTRO de la misma llamada
 * — nunca responde 200 para fallar después en segundo plano (ver docstring).
 * Un error de negocio aislado a UN abono nunca llega hasta acá (se captura y
 * se refleja en `abonos[]` de la respuesta 200) — solo un error que abortó el
 * request COMPLETO (ver `_procesarVentaKoreInterno`) pasa por este reintento.
 */
async function procesarVentaKore(payload) {
  const datos = _validarPayloadVenta(payload);

  const totalIntentos = REINTENTOS_DELAYS_MS.length + 1;
  let ultimoError;
  for (let intento = 0; intento < totalIntentos; intento++) {
    if (intento > 0) {
      // eslint-disable-next-line no-await-in-loop
      await _sleep(REINTENTOS_DELAYS_MS[intento - 1]);
    }
    try {
      // eslint-disable-next-line no-await-in-loop
      return await _procesarVentaKoreInterno(datos);
    } catch (err) {
      // Errores de negocio (AppError: 400/404/409/422) nunca se reintentan — reintentar no los
      // arregla, y el contrato exige que Kore reciba el rechazo real de inmediato.
      if (err?.statusCode) throw err;
      ultimoError = err;
      logger.warn(`[loyalty-kore-venta] serieFolio=${datos.serieFolio}: intento ${intento + 1}/${totalIntentos} falló (${err.message}).`);
    }
  }

  logger.error(`[loyalty-kore-venta] serieFolio=${datos.serieFolio}: agotados ${totalIntentos} intentos, error final: ${ultimoError.message}`);
  await registrarVentaFallida(datos.serieFolio, payload, `Error interno tras ${totalIntentos} intentos: ${ultimoError.message}`);
  throw ultimoError;
}

const MAX_VENTAS_POR_LOTE = 100;

/**
 * `POST /api/loyalty/kore/ventas` (plural, 2026-10-27, pedido explícito del usuario): procesa
 * varias ventas (cada una con su propio `abonos[]`, contrato 2026-11-06) en un solo request sin
 * tocar `procesarVentaKore` (el endpoint singular sigue funcionando exactamente igual, cero riesgo
 * de regresión ahí). Cada venta se procesa EN SERIE (no `Promise.all`): varias ventas del mismo lote
 * pueden ser de la misma tarjeta, y procesarlas en paralelo pelearía por el mismo `LOCK.UPDATE` de
 * fila — en serie es más lento pero determinístico. Una venta que falla NUNCA aborta las demás: se
 * reusa `procesarVentaKore` tal cual (con sus propios reintentos/idempotencia/transacción) y se
 * captura su error por item, igual que un lote de facturas o pólizas en otros módulos de este
 * proyecto.
 */
async function procesarLoteVentasKore(ventas) {
  if (!Array.isArray(ventas) || ventas.length === 0) {
    throw new BadRequestError('ventas debe ser un arreglo no vacío.');
  }
  if (ventas.length > MAX_VENTAS_POR_LOTE) {
    throw new BadRequestError(`No se pueden procesar más de ${MAX_VENTAS_POR_LOTE} ventas en un solo lote.`);
  }

  const resultados = [];
  for (const venta of ventas) {
    try {
      // eslint-disable-next-line no-await-in-loop -- en serie a propósito, ver docstring arriba.
      const resultado = await procesarVentaKore(venta);
      resultados.push({ serieFolio: resultado.serieFolio, success: true, ...resultado });
    } catch (err) {
      resultados.push({
        serieFolio: venta?.serieFolio ?? null,
        success: false,
        error: err.message,
        statusCode: err.statusCode ?? 500,
      });
    }
  }
  return { resultados };
}

module.exports = { consultarSaldoKore, procesarVentaKore, procesarLoteVentasKore };
