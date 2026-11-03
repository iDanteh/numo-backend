'use strict';

/**
 * domains/ledger/loyalty-kore-venta.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — webhook Kore→Numo de venta (acumulación y/o devengación
 * de puntos combinadas en una sola operación atómica). Contrato HTTP completo
 * en `Docs/integracion-kore-puntos-lealtad-DRAFT.md` (v1, 2026-10-26) — este
 * archivo es la implementación de esa §3/§4, no la repite.
 *
 * Decisiones de negocio clave (todas confirmadas con el usuario, no inferidas):
 *  - La acumulación y el canje son gates COMPLETAMENTE INDEPENDIENTES: que la
 *    venta no califique para ganar puntos nuevos nunca bloquea el descuento de
 *    puntos que el cliente ya tenía (y viceversa).
 *  - Formas de pago mixtas: los puntos nuevos se calculan PROPORCIONALMENTE al
 *    subtotal pagado con formas de pago que sí acumulan (nunca todo-o-nada).
 *  - Un `koreId` de concepto/forma de pago desconocido en el catálogo local
 *    intenta UN resync on-the-fly contra Kore; si sigue sin aparecer, se trata
 *    como "no acumula" (conservador, nunca bloquea la venta) Y se registra como
 *    proceso fallido para que alguien lo configure — no hace falta que sea la
 *    forma de pago "Puntos" específicamente: cualquier id desconocido entra por
 *    este mismo camino, la forma "Puntos" se reconoce con el flag `esPuntos`
 *    de la fila ya resuelta, sin un caso especial aparte.
 *  - `origen:'kore'` en el ledger (valor ya anticipado en el comentario del
 *    modelo `LoyaltyPointsLedger`). El índice único es `(origen, referencia_
 *    externa)`: como una misma venta puede generar hasta TRES filas (ACUMULACION
 *    de nivel, ACUMULACION de promoción, CANJE), `referenciaExterna` lleva el
 *    sufijo correspondiente (`"<serieFolio>:ACUMULACION"` / `":PROMOCION"` /
 *    `":CANJE"`, ver `_refAcumulacion`/`_refPromocion`/`_refCanje`) para que
 *    las tres convivan sin chocar contra ese índice — el folio solo no alcanza
 *    como llave si las filas comparten `origen`.
 *  - Nivel y promoción (2026-10-27, pedido explícito del usuario) generan CADA
 *    UNO su propia fila ACUMULACION, en vez de un solo % combinado — así el
 *    detalle de movimientos muestra el registro de cada fuente por separado.
 *  - Reintentos acotados DENTRO de la misma llamada ante errores inesperados
 *    (mismo criterio que `erp-reversion.service.js`), nunca un 200 que oculte
 *    un fallo en segundo plano.
 *  - `procesarLoteVentasKore` (2026-10-27, pedido explícito del usuario): variante en lote de
 *    `POST /venta`, hasta `MAX_VENTAS_POR_LOTE` ventas por request, procesadas en serie, cada una
 *    independiente (una que falla no aborta las demás) — ver su propio docstring más abajo.
 */

const { Transaction, Op } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');
const {
  LoyaltyCard, LoyaltyPointsLedger, LoyaltyKoreConcepto, LoyaltyKoreFormaPago,
} = require('../../../shared/models/postgres');
const { BadRequestError, ConflictError, NotFoundError } = require('../../../shared/errors/AppError');
const { toCents, fromCents, calcularPuntos, normalizarTelefono } = require('../../shared/puntos.util');
const globalConfigService = require('../../../shared/services/global-config.service');
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

function _refAcumulacion(serieFolio) { return `${serieFolio}:ACUMULACION`; }
function _refCanje(serieFolio) { return `${serieFolio}:CANJE`; }
// Fila separada para el bono de promoción (2026-10-27, pedido explícito del usuario) — mismo
// patrón de sufijo que ACUMULACION/CANJE, para que las tres convivan bajo el mismo índice único
// (origen, referencia_externa) sin chocar entre sí.
function _refPromocion(serieFolio) { return `${serieFolio}:PROMOCION`; }

// Blindaje cruzado manual↔kore (2026-10-27, pedido explícito del usuario, escenario simétrico al
// ya resuelto del lado manual en loyalty-ledger.service.js#registrarAcumulacion/registrarCanje): el
// botón manual escribe la fila de nivel con referenciaExterna = EL SERIE-FOLIO PLANO (sin sufijo —
// ver registrarAcumulacion), y `${serieFolio}:PROMOCION`/`${serieFolio}:CANJE` para promoción y
// canje (mismos sufijos que acá, ver registrarCanje tras su propio blindaje 2026-10-27). Se incluye
// en el MISMO chequeo de idempotencia que ya existe para 'kore' — si la venta ya fue acreditada a
// mano, el webhook no debe duplicarla ni fallar: debe responder "aplicado" con lo que ya hay, igual
// que ante un reenvío propio.
function _condicionIdempotencia(serieFolio) {
  return {
    [Op.or]: [
      { origen: ORIGEN_KORE, referenciaExterna: { [Op.in]: [_refAcumulacion(serieFolio), _refCanje(serieFolio), _refPromocion(serieFolio)] } },
      { origen: 'manual', referenciaExterna: { [Op.in]: [serieFolio, `${serieFolio}:PROMOCION`, `${serieFolio}:CANJE`] } },
    ],
  };
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

function _validarPayloadVenta(payload) {
  const {
    telefono, koreClienteId, serieFolio, conceptoId, metodoPago, formasPago,
    subtotal, total, usuarioAplicaId, usuarioAplicaNombre, fecha,
  } = payload ?? {};

  const faltantes = [];
  if (!telefono) faltantes.push('telefono');
  if (!serieFolio) faltantes.push('serieFolio');
  if (!conceptoId) faltantes.push('conceptoId');
  if (!metodoPago) faltantes.push('metodoPago');
  if (!Array.isArray(formasPago) || formasPago.length === 0) faltantes.push('formasPago');
  if (!(Number(subtotal) > 0)) faltantes.push('subtotal');
  if (!(Number(total) > 0)) faltantes.push('total');
  if (!usuarioAplicaId) faltantes.push('usuarioAplicaId');
  if (!usuarioAplicaNombre) faltantes.push('usuarioAplicaNombre');
  if (!fecha) faltantes.push('fecha');
  if (faltantes.length) {
    throw new BadRequestError(`Faltan campos requeridos: ${faltantes.join(', ')}.`);
  }

  if (!['PUE', 'PPD'].includes(String(metodoPago).toUpperCase())) {
    throw new BadRequestError('metodoPago debe ser "PUE" o "PPD".');
  }

  if (serieFolio && !SERIE_FOLIO_RE.test(String(serieFolio).trim())) {
    throw new BadRequestError('serieFolio debe tener el formato SERIE-FOLIO (ej. A0-260605320).');
  }

  const formasPagoParsed = formasPago.map(f => ({
    formaPagoId: String(f?.formaPagoId ?? '').trim(),
    importe: Number(f?.importe),
  }));
  const lineaInvalida = formasPagoParsed.find(f => !f.formaPagoId || !(f.importe > 0));
  if (lineaInvalida) {
    throw new BadRequestError('Cada línea de formasPago requiere formaPagoId e importe > 0.');
  }

  const sumaFormasPago = Math.round(formasPagoParsed.reduce((s, f) => s + f.importe, 0) * 100) / 100;
  const totalNum = Math.round(Number(total) * 100) / 100;
  if (Math.abs(sumaFormasPago - totalNum) > TOLERANCIA_PESOS) {
    throw new BadRequestError('La suma de formasPago no coincide con total.');
  }

  return {
    telefono: String(telefono).trim(),
    koreClienteId: koreClienteId ? String(koreClienteId).trim() : null,
    serieFolio: String(serieFolio).trim(),
    conceptoId: String(conceptoId).trim(),
    metodoPago: String(metodoPago).toUpperCase(),
    formasPago: formasPagoParsed,
    subtotal: Number(subtotal),
    total: totalNum,
    usuarioAplicaId: String(usuarioAplicaId).trim(),
    usuarioAplicaNombre: String(usuarioAplicaNombre).trim(),
    fecha: String(fecha),
  };
}

/** Busca el concepto/forma de pago por koreId; si no existe, intenta UN resync del catálogo
 *  correspondiente y vuelve a buscar — conservador: un id que sigue sin aparecer se trata como "no
 *  acumula"/"no es Puntos" (nunca bloquea la venta) y queda registrado como proceso fallido para
 *  que alguien lo configure. `yaResincronizado` evita pedir el resync más de una vez por llamada
 *  aunque varias líneas de formasPago traigan ids desconocidos. */
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

/** Reconstruye la respuesta de una venta YA procesada antes (idempotencia por serieFolio) sin
 *  volver a escribir ni recalcular elegibilidad. */
async function _respuestaIdempotente(serieFolio, filas, card, datos) {
  // Nivel y promoción pueden ser DOS filas tipo ACUMULACION (2026-10-27) — se identifica la de
  // nivel como la que NO lleva el sufijo `:PROMOCION` (sirve tanto para la fila 'kore', que lleva
  // su propio sufijo `:ACUMULACION`, como para una fila 'manual', cuya referenciaExterna es el
  // serieFolio plano — ver _condicionIdempotencia) para el chequeo de consistencia del subtotal, y
  // se suman TODAS las ACUMULACION (sin importar origen) para el total de puntos.
  const acumulacionNivel = filas.find(f => f.tipo === 'ACUMULACION' && !f.referenciaExterna?.endsWith(':PROMOCION'));
  const canje = filas.find(f => f.tipo === 'CANJE');

  // 409: mismo serieFolio, pero el subtotal que generó la acumulación guardada no coincide con el
  // que llega ahora — posible reenvío con datos distintos (ver contrato §7).
  if (acumulacionNivel && Math.abs(Number(acumulacionNivel.montoVenta) - Number(datos.subtotal)) > TOLERANCIA_PESOS) {
    throw new ConflictError(`La venta con serieFolio "${serieFolio}" ya fue reportada antes con un subtotal distinto.`);
  }

  const puntosAcumulados = filas
    .filter(f => f.tipo === 'ACUMULACION')
    .reduce((s, f) => s + Number(f.puntos), 0);
  const puntosDescontados = canje ? Math.abs(Number(canje.puntos)) : 0;

  return {
    serieFolio,
    status: 'aplicado',
    puntosDescontados,
    puntosAcumulados,
    saldoResultante: Number(card.saldoPuntos),
  };
}

async function _procesarVentaKoreInterno(datos) {
  const card = await _resolverTarjeta({ telefono: datos.telefono, koreClienteId: datos.koreClienteId });

  // Idempotencia — buscar ANTES de tocar nada más. Incluye 'manual' (ver _condicionIdempotencia):
  // si alguien ya acreditó esta venta a mano antes de que llegara el webhook, no se duplica.
  const filasExistentes = await LoyaltyPointsLedger.findAll({
    where: _condicionIdempotencia(datos.serieFolio),
  });
  if (filasExistentes.length) {
    return _respuestaIdempotente(datos.serieFolio, filasExistentes, card, datos);
  }

  const estado = {};

  // ── Canje: gate independiente, se resuelve ANTES que la acumulación ──────
  let formaPuntosLinea = null;
  const formasResueltas = [];
  for (const linea of datos.formasPago) {
    // eslint-disable-next-line no-await-in-loop -- universo chico (líneas de una sola venta).
    const resuelta = await _resolverFormaPago(linea.formaPagoId, estado);
    formasResueltas.push({ ...linea, ...resuelta });
    if (resuelta.esPuntos) formaPuntosLinea = { ...linea, ...resuelta };
  }
  const puntosDescontadosPesos = formasResueltas
    .filter(f => f.esPuntos)
    .reduce((s, f) => s + f.importe, 0);

  // ── Acumulación: gate independiente, nunca bloqueado por el canje ───────
  const concepto = await _resolverConcepto(datos.conceptoId, estado);
  let centroCosto = null;
  let participaLealtad = false;
  try {
    const { centroCostoId } = await _resolverCentroCostoPorSerieFolio(datos.serieFolio);
    centroCosto = await _obtenerCentroCosto(centroCostoId);
    participaLealtad = !!centroCosto.participaLealtad;
  } catch (err) {
    // Serie sin centro de costo mapeado: no bloquea la venta, simplemente no acumula (conservador,
    // mismo criterio que un concepto/forma de pago desconocidos) — SÍ se registra como proceso
    // fallido porque, a diferencia de un id de catálogo nuevo, esto normalmente indica una serie
    // mal configurada que alguien debe revisar.
    await registrarVentaFallida(datos.serieFolio, datos, `Serie sin centro de costo mapeado: ${err.message}`);
  }

  const montoCalificablePesos = formasResueltas
    .filter(f => !f.esPuntos && f.acumulaPuntos)
    .reduce((s, f) => s + f.importe, 0);

  let puntosNivelCents = 0;
  let puntosPromoCents = 0;
  let montoBaseCalculoCents = null;
  let pctNivel = null;
  let promoVigente = null;
  const califica = datos.metodoPago === 'PUE' && participaLealtad && concepto.acumulaPuntos && montoCalificablePesos > 0;

  if (califica) {
    let pctRaw;
    try {
      pctRaw = await globalConfigService.getValue('lealtad', `PORCENTAJE_${card.nivel.toUpperCase()}`);
    } catch (err) {
      pctRaw = null;
    }
    if (pctRaw == null) {
      await registrarVentaFallida(datos.serieFolio, datos, `Porcentaje de nivel ${card.nivel} no configurado en Lealtad → Configuración.`);
    } else {
      // Nivel y promoción quedan como DOS movimientos separados en el ledger (2026-10-27, pedido
      // explícito del usuario) — cada uno calculado sobre el mismo monto prorrateado, en vez de un
      // único % combinado (ver el mismo cambio en loyalty-ledger.service.js#registrarAcumulacion).
      promoVigente = await configService.obtenerPromocionVigente();
      pctNivel = Number(pctRaw);
      const montoProrrateadoPesos = (montoCalificablePesos / datos.total) * datos.subtotal;
      const montoProrrateadoCents = toCents(montoProrrateadoPesos);
      montoBaseCalculoCents = montoProrrateadoCents;
      puntosNivelCents = calcularPuntos(montoProrrateadoCents, pctNivel);
      puntosPromoCents = promoVigente ? calcularPuntos(montoProrrateadoCents, Number(promoVigente.porcentajeBono)) : 0;
    }
  }

  if (estado.conceptoDesconocido) {
    await registrarVentaFallida(datos.serieFolio, datos, `Concepto desconocido en Numo: ${estado.conceptoDesconocido}`);
  }
  if (estado.formasPagoDesconocidas?.length) {
    await registrarVentaFallida(datos.serieFolio, datos, `Forma(s) de pago desconocida(s) en Numo: ${estado.formasPagoDesconocidas.join(', ')}`);
  }

  const puntosDescontadosCents = toCents(puntosDescontadosPesos);
  if (puntosDescontadosCents > 0) {
    if (!formaPuntosLinea) {
      // No debería poder pasar (puntosDescontadosPesos solo suma líneas con esPuntos:true), queda
      // como salvaguarda defensiva.
      throw new BadRequestError('No se pudo resolver la forma de pago "Puntos" de esta venta.');
    }
  }

  return sequelize.transaction(async (t) => {
    const cardLocked = await LoyaltyCard.findByPk(card.id, { transaction: t, lock: Transaction.LOCK.UPDATE });
    if (!cardLocked) throw new NotFoundError('Tarjeta de Lealtad');

    if (puntosDescontadosCents > 0) {
      if (!cardLocked.activa) {
        throw new BadRequestError('La tarjeta está inactiva, no se puede descontar puntos.');
      }
      const saldoActualCents = toCents(cardLocked.saldoPuntos);
      if (puntosDescontadosCents > saldoActualCents) {
        throw new BadRequestError('El monto de puntos a usar supera el saldo disponible del cliente.');
      }
    }

    let entryCanje = null;
    let entryAcumulacionNivel = null;
    let entryAcumulacionPromocion = null;

    if (puntosDescontadosCents > 0) {
      entryCanje = await _aplicarMovimiento(t, cardLocked, {
        tipo: 'CANJE',
        puntos: fromCents(-puntosDescontadosCents),
        centroCostoId: centroCosto?.id ?? null,
        origen: ORIGEN_KORE,
        referenciaExterna: _refCanje(datos.serieFolio),
        usuarioId: datos.usuarioAplicaId,
        usuarioNombre: datos.usuarioAplicaNombre,
      });
    }

    if (puntosNivelCents > 0 || puntosPromoCents > 0) {
      if (!cardLocked.activa) {
        throw new BadRequestError('La tarjeta está inactiva, no se puede acumular puntos.');
      }
    }

    if (puntosNivelCents > 0) {
      entryAcumulacionNivel = await _aplicarMovimiento(t, cardLocked, {
        tipo: 'ACUMULACION',
        puntos: fromCents(puntosNivelCents),
        montoVenta: datos.subtotal,
        // Monto REAL usado en calcularPuntos — con formas de pago mixtas es el prorrateado, no el
        // subtotal completo (ver montoProrrateadoCents arriba).
        montoBaseCalculo: fromCents(montoBaseCalculoCents),
        montoVentaTotal: datos.total,
        porcentajeAplicado: pctNivel,
        nivelAplicado: cardLocked.nivel,
        centroCostoId: centroCosto?.id ?? null,
        origen: ORIGEN_KORE,
        referenciaExterna: _refAcumulacion(datos.serieFolio),
        usuarioId: datos.usuarioAplicaId,
        usuarioNombre: datos.usuarioAplicaNombre,
      });
    }

    // Fila separada de la promoción (sufijo `:PROMOCION`, ver _refPromocion) — nunca pisa el
    // referenciaExterna de la fila de nivel, así ambas conviven bajo el mismo índice único.
    if (puntosPromoCents > 0) {
      entryAcumulacionPromocion = await _aplicarMovimiento(t, cardLocked, {
        tipo: 'ACUMULACION',
        puntos: fromCents(puntosPromoCents),
        montoVenta: datos.subtotal,
        montoBaseCalculo: fromCents(montoBaseCalculoCents),
        montoVentaTotal: datos.total,
        porcentajeAplicado: Number(promoVigente.porcentajeBono),
        promocionNombre: promoVigente.nombre,
        promocionPorcentaje: promoVigente.porcentajeBono,
        centroCostoId: centroCosto?.id ?? null,
        origen: ORIGEN_KORE,
        referenciaExterna: _refPromocion(datos.serieFolio),
        usuarioId: datos.usuarioAplicaId,
        usuarioNombre: datos.usuarioAplicaNombre,
      });
    }

    const huboMovimiento = !!(entryCanje || entryAcumulacionNivel || entryAcumulacionPromocion);
    const puntosAcumulados = (entryAcumulacionNivel ? Number(entryAcumulacionNivel.puntos) : 0)
      + (entryAcumulacionPromocion ? Number(entryAcumulacionPromocion.puntos) : 0);
    return {
      serieFolio: datos.serieFolio,
      status: huboMovimiento ? 'aplicado' : 'no_califica',
      puntosDescontados: entryCanje ? Math.abs(Number(entryCanje.puntos)) : 0,
      puntosAcumulados,
      saldoResultante: Number(cardLocked.saldoPuntos),
    };
  }).catch((err) => {
    if (err?.name === 'SequelizeUniqueConstraintError') {
      // Carrera: otro request con el mismo serieFolio ganó la escritura entre el chequeo de
      // idempotencia de arriba y este insert — se recupera el resultado ya aplicado. Se relee la
      // tarjeta (no la instancia pre-transacción, que no refleja el saldo del ganador de la carrera).
      return Promise.all([
        LoyaltyPointsLedger.findAll({ where: _condicionIdempotencia(datos.serieFolio) }),
        LoyaltyCard.findByPk(card.id),
      ]).then(([filas, cardFresca]) => _respuestaIdempotente(datos.serieFolio, filas, cardFresca, datos));
    }
    throw err;
  });
}

/**
 * Punto de entrada del webhook `POST /api/loyalty/kore/venta`. Reintenta
 * acotadamente errores inesperados (no de negocio) DENTRO de la misma llamada
 * — nunca responde 200 para fallar después en segundo plano (ver docstring).
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
 * varias ventas en un solo request sin tocar `procesarVentaKore` (el endpoint singular sigue
 * funcionando exactamente igual, cero riesgo de regresión ahí). Cada venta se procesa EN SERIE
 * (no `Promise.all`): varias ventas del mismo lote pueden ser de la misma tarjeta, y procesarlas en
 * paralelo pelearía por el mismo `LOCK.UPDATE` de fila — en serie es más lento pero determinístico.
 * Una venta que falla NUNCA aborta las demás: se reusa `procesarVentaKore` tal cual (con sus propios
 * reintentos/idempotencia/transacción) y se captura su error por item, igual que un lote de
 * facturas o pólizas en otros módulos de este proyecto.
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
