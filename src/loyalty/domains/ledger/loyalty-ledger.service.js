'use strict';

/**
 * domains/ledger/loyalty-ledger.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — único punto de escritura de puntos (D4). Toda mutación
 * pasa por `_aplicarMovimiento`, que corre dentro de la transacción abierta
 * por el caller con la tarjeta ya bloqueada (`LOCK.UPDATE`), igual que
 * `poliza.repository.js#setEstado`. El CHECK `saldo_puntos >= 0` en Postgres
 * (PR1) es el respaldo de última línea si algún día algo escribe fuera de acá.
 *
 * Toda la aritmética de puntos vive en centavos enteros (D3, ver
 * `shared/puntos.util.js`) — el modelo guarda/lee DECIMAL como string.
 */

const { Transaction } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');
const {
  LoyaltyCard, LoyaltyPointsLedger, LoyaltyTierHistory, LoyaltyAdjustmentReason, CentroCosto,
} = require('../../../shared/models/postgres');
const { BadRequestError, ConflictError, UnprocessableError, NotFoundError } = require('../../../shared/errors/AppError');
const { toCents, fromCents, calcularPuntos } = require('../../shared/puntos.util');
const globalConfigService = require('../../../shared/services/global-config.service');
// Sin require circular: loyalty-config.service.js no importa nada de este
// archivo (verificado), así que un require normal al tope basta — no hace
// falta el patrón de require perezoso que usa p.ej. shared/middleware/auth.js.
const configService = require('../config/loyalty-config.service');
// Reutilizado (vía require, nunca copiando lógica — mismo criterio que
// cfdi-poliza-generator.service.js/cobranza-poliza-generator.service.js) para
// resolver la serie de un Serie-Folio de Kore (p.ej. "A0" en "A0-260605320")
// a un centro de costo, sin que el cajero tenga que elegir sucursal a mano.
const centrosCostoService = require('../../../banks/domains/centros-costo/centros-costo.service');
// Reutilizado (vía require) para consultar la CxC real en Kore a partir del
// Serie-Folio de la venta (ACUMULACION, 2026-10-21) — misma función ya usada
// por Bancos/Pólizas para /cuentas-pendientes, con su propio reintento/backoff
// contra 429/timeout ya resuelto ahí (ver erp-sync.service.js).
const erpSyncService = require('../../../banks/domains/erp/erp-sync.service');

// Formato Kore: "SERIE-FOLIO" (p.ej. "A0-260605320"). La serie es alfanumérica
// corta (clave de sucursal/almacén); el folio es el consecutivo de venta.
const SERIE_FOLIO_RE = /^([A-Za-z0-9]{1,10})-(\d{1,20})$/;
// CANJE (2026-10-21, pedido explícito del usuario): mismo reemplazo del select
// de sucursal, pero solo con la serie (sin folio) — no hay una venta externa
// que referenciar, solo hace falta saber a qué sucursal pertenece el canje.
const SERIE_RE = /^[A-Za-z0-9]{1,10}$/;

async function _resolverCentroCostoIdPorSerie(serie) {
  const ccBySerieMap = await centrosCostoService.resolveBySerieMap();
  const entry = Object.entries(ccBySerieMap).find(([s]) => s.toUpperCase() === serie);
  if (!entry) {
    throw new BadRequestError(
      `No se encontró ninguna sucursal con la serie "${serie}". Verifica la serie o configúrala en Centros de Costo.`,
    );
  }
  return entry[1].id;
}

/**
 * ACUMULACION (2026-10-21, pedido explícito del usuario): el select manual de
 * sucursal se reemplaza por el Serie-Folio real de la venta en Kore. La serie
 * se resuelve contra `CentroCosto.serieFacturacion` — el mismo mapa que ya usa
 * el generador de pólizas para asignar centro de costo desde un CFDI
 * (`resolveBySerieMap`) — y el Serie-Folio completo (normalizado) se guarda
 * como `referenciaExterna`, que ya tiene un índice único parcial
 * `(origen, referencia_externa)` en el modelo: dos acumulaciones con el MISMO
 * folio chocan ahí (ver `registrarAcumulacion`), lo cual es el comportamiento
 * correcto — un folio de Kore solo debe generar puntos una vez.
 */
async function _resolverCentroCostoPorSerieFolio(serieFolioRaw) {
  const serieFolio = String(serieFolioRaw ?? '').trim();
  const match = SERIE_FOLIO_RE.exec(serieFolio);
  if (!match) {
    throw new BadRequestError('El Serie-Folio debe tener el formato SERIE-FOLIO (ej. A0-260605320).');
  }
  const [, serieRaw, folio] = match;
  const serie = serieRaw.toUpperCase();
  const centroCostoId = await _resolverCentroCostoIdPorSerie(serie);
  return { serie, folio, centroCostoId, referenciaExterna: `${serie}-${folio}` };
}

async function _resolverCentroCostoPorSerie(serieRaw) {
  const serie = String(serieRaw ?? '').trim().toUpperCase();
  if (!SERIE_RE.test(serie)) {
    throw new BadRequestError('Captura la serie de la sucursal (ej. A0).');
  }
  return _resolverCentroCostoIdPorSerie(serie);
}

// Kore folios encode YYMMxxxxx (ej. "260600250" → año=2026, mes=06) — mismo
// encoding que erp.routes.js#_rangoDesdeFollo/_rangoSpilloverSiguienteMes,
// DUPLICADO a propósito acá: esas funciones son privadas de ese router (no
// exportadas) y Lealtad necesita el mismo rango para consultarle a Kore la
// CxC de UN Serie-Folio sin pedirle una fecha al cajero. Si Kore cambia el
// encoding del folio, actualizar AMBAS copias. Kore exige un rango máximo de
// un mes (responde 400 si se excede), por eso el rango nunca es más amplio
// que el mes calendario exacto del folio.
function _rangoDesdeFollo(folioExterno) {
  const str = String(folioExterno).trim();
  if (str.length < 4) return null;
  const yy = parseInt(str.slice(0, 2), 10);
  const mm = parseInt(str.slice(2, 4), 10);
  if (isNaN(yy) || isNaN(mm) || mm < 1 || mm > 12) return null;
  const year    = 2000 + yy;
  const mmStr   = String(mm).padStart(2, '0');
  const lastDay = new Date(Date.UTC(year, mm, 0)).getUTCDate();
  const lastStr = String(lastDay).padStart(2, '0');
  return {
    fechaDesde: `${year}-${mmStr}-01T00:00:00Z`,
    fechaHasta: `${year}-${mmStr}-${lastStr}T23:59:59Z`,
  };
}

// Ventana de "rescate" de fin de mes (mismo motivo que la copia en
// erp.routes.js: el reloj del ERP puede adelantar una venta de último día de
// mes al día 1 del mes siguiente) — solo se usa como segundo intento si la
// consulta normal no encuentra nada.
function _rangoSpilloverSiguienteMes(folioExterno) {
  const str = String(folioExterno).trim();
  if (str.length < 4) return null;
  const yy = parseInt(str.slice(0, 2), 10);
  const mm = parseInt(str.slice(2, 4), 10);
  if (isNaN(yy) || isNaN(mm) || mm < 1 || mm > 12) return null;
  const year = 2000 + yy;
  const desde = new Date(Date.UTC(year, mm, 1, 0, 0, 0));
  const hasta = new Date(Date.UTC(year, mm, 1, 23, 59, 59));
  return { fechaDesde: desde.toISOString(), fechaHasta: hasta.toISOString() };
}

/**
 * Consulta la CxC real en Kore para UN Serie-Folio (ACUMULACION, 2026-10-21):
 * evita que el cajero capture un monto de venta con libertad total — el monto
 * siempre sale de la venta real. El cálculo de puntos usa `subtotal` (antes de
 * impuesto, pedido explícito del usuario); `total` se guarda solo para
 * auditoría (nunca se muestra en la UI, ver `registrarAcumulacion`).
 */
async function _obtenerCuentaKorePorSerieFolio(serie, folio) {
  const rango = _rangoDesdeFollo(folio);
  if (!rango) {
    throw new BadRequestError('No se pudo determinar el periodo del folio. Verifica el Serie-Folio.');
  }

  let raw;
  try {
    ({ raw } = await erpSyncService.sincronizarCuentasPendientes({
      serieExterna: serie, folioExterno: folio, fechaDesde: rango.fechaDesde, fechaHasta: rango.fechaHasta,
    }));
    if (!raw.length) {
      const spillover = _rangoSpilloverSiguienteMes(folio);
      if (spillover) {
        ({ raw } = await erpSyncService.sincronizarCuentasPendientes({
          serieExterna: serie, folioExterno: folio, fechaDesde: spillover.fechaDesde, fechaHasta: spillover.fechaHasta,
        }));
      }
    }
  } catch (err) {
    throw new UnprocessableError('No se pudo consultar la venta en Kore. Intenta de nuevo.');
  }

  const cuenta = (raw || []).find(
    (c) => String(c.folioExterno) === String(folio) && String(c.serieExterna) === serie,
  );
  if (!cuenta) {
    throw new BadRequestError(
      `No se encontró la venta con Serie-Folio "${serie}-${folio}" en Kore. Verifica el folio e intenta de nuevo.`,
    );
  }
  if (!(Number(cuenta.subtotal) > 0)) {
    throw new UnprocessableError('La venta encontrada en Kore no tiene un subtotal válido.');
  }
  return cuenta;
}

function _userLabel(user) {
  return {
    usuarioId:     user?.dbId != null ? String(user.dbId) : (user?._id != null ? String(user._id) : null),
    usuarioNombre: user?.nombre ?? null,
  };
}

/**
 * Único escritor del ledger (D4). Recibe la tarjeta YA bloqueada
 * (`findByPk(..., {lock: LOCK.UPDATE})`) dentro de la transacción `t` del
 * caller. Inserta la fila y actualiza el saldo denormalizado en la misma
 * transacción — nunca hay un momento en que uno exista sin el otro.
 */
async function _aplicarMovimiento(t, card, mov) {
  if (!card.activa) {
    throw new UnprocessableError('La tarjeta está inactiva.');
  }

  const saldoActualCents = toCents(card.saldoPuntos);
  const puntosCents = toCents(mov.puntos);
  const saldoResultanteCents = saldoActualCents + puntosCents;

  const entry = await LoyaltyPointsLedger.create({
    cardId:             card.id,
    tipo:               mov.tipo,
    puntos:             fromCents(puntosCents),
    saldoResultante:    fromCents(saldoResultanteCents),
    montoVenta:          mov.montoVenta ?? null,
    // Total de la venta en Kore (CON impuesto) — solo auditoría, nunca se
    // expone en la UI (2026-10-21, pedido explícito del usuario). El cálculo
    // de puntos usa SIEMPRE `montoVenta` (subtotal), nunca este campo.
    montoVentaTotal:     mov.montoVentaTotal ?? null,
    porcentajeAplicado:  mov.porcentajeAplicado ?? null,
    nivelAplicado:       mov.nivelAplicado ?? null,
    promocionNombre:     mov.promocionNombre ?? null,
    promocionPorcentaje: mov.promocionPorcentaje ?? null,
    centroCostoId:      mov.centroCostoId ?? null,
    reasonId:           mov.reasonId ?? null,
    justificacion:      mov.justificacion ?? null,
    origen:             mov.origen ?? 'manual',
    referenciaExterna:  mov.referenciaExterna ?? null,
    usuarioId:          mov.usuarioId ?? null,
    usuarioNombre:      mov.usuarioNombre ?? null,
  }, { transaction: t });

  await card.update({ saldoPuntos: fromCents(saldoResultanteCents) }, { transaction: t });

  return entry;
}

async function _obtenerCentroCosto(centroCostoId) {
  const centroCosto = await CentroCosto.findByPk(centroCostoId);
  if (!centroCosto || !centroCosto.isActive) {
    throw new UnprocessableError('El centro de costo no existe o está inactivo.');
  }
  return centroCosto;
}

/**
 * ACUMULACION: `puntos = montoVenta × %nivel`, tomado como snapshot (D5) — el
 * % se congela en la fila y una config futura nunca lo recalcula. Falla
 * cerrado (D11) si el % del nivel no está configurado: nunca acumula al 0%.
 *
 * `montoVenta` (2026-10-21, pedido explícito del usuario) YA NO viene del
 * body — se resuelve consultando la CxC real en Kore a partir del
 * Serie-Folio, usando su `subtotal` (antes de impuesto). Esto evita que el
 * cajero capture un importe con libertad total; `total` se guarda aparte
 * solo para auditoría.
 */
async function registrarAcumulacion({ cardId, serieFolio }, user) {
  const { serie, folio, centroCostoId, referenciaExterna } = await _resolverCentroCostoPorSerieFolio(serieFolio);

  const centroCosto = await _obtenerCentroCosto(centroCostoId);
  if (!centroCosto.participaLealtad) {
    throw new UnprocessableError('Esta sucursal no participa en el programa de lealtad.');
  }

  const cuentaKore = await _obtenerCuentaKorePorSerieFolio(serie, folio);
  const montoVenta = cuentaKore.subtotal;
  const montoVentaTotal = cuentaKore.total ?? null;

  try {
    return await sequelize.transaction(async (t) => {
      const card = await LoyaltyCard.findByPk(cardId, { transaction: t, lock: Transaction.LOCK.UPDATE });
      if (!card) throw new NotFoundError('Tarjeta');
      if (!card.activa) throw new UnprocessableError('La tarjeta está inactiva.');

      let pctRaw;
      try {
        pctRaw = await globalConfigService.getValue('lealtad', `PORCENTAJE_${card.nivel.toUpperCase()}`);
      } catch (err) {
        throw new UnprocessableError(
          `Configure el % del nivel ${card.nivel} en Lealtad → Configuración.`,
        );
      }
      const pct = Number(pctRaw);

      // Promoción vigente (2026-10-20): su % de bono se SUMA al % del nivel —
      // sin promo vigente, pctTotal === pct (comportamiento idéntico a hoy).
      const promoVigente = await configService.obtenerPromocionVigente();
      const pctTotal = promoVigente ? pct + Number(promoVigente.porcentajeBono) : pct;

      const montoCents = toCents(montoVenta);
      const puntosCents = calcularPuntos(montoCents, pctTotal);
      if (puntosCents === 0) {
        throw new BadRequestError('El monto de venta no genera puntos con el % configurado.');
      }

      const { usuarioId, usuarioNombre } = _userLabel(user);
      const entry = await _aplicarMovimiento(t, card, {
        tipo:                'ACUMULACION',
        puntos:              fromCents(puntosCents),
        montoVenta,
        montoVentaTotal,
        porcentajeAplicado:  pctTotal,
        nivelAplicado:       card.nivel,
        promocionNombre:     promoVigente?.nombre ?? null,
        promocionPorcentaje: promoVigente?.porcentajeBono ?? null,
        centroCostoId,
        referenciaExterna,
        origen: 'manual',
        usuarioId,
        usuarioNombre,
      });

      return { movimiento: entry, saldoPuntos: entry.saldoResultante };
    });
  } catch (err) {
    // Índice único parcial (origen, referencia_externa) — el mismo folio ya
    // generó puntos antes. No se reintenta ni se recalcula (misma filosofía
    // que inconsistenciaPostKore en collection-requests): si el monto estaba
    // mal, se corrige con un AJUSTE, nunca reenviando el mismo folio.
    if (err?.name === 'SequelizeUniqueConstraintError') {
      throw new ConflictError(
        `Ya se registró una acumulación con el folio "${referenciaExterna}". Si el monto estaba mal, corrígelo con un ajuste.`,
      );
    }
    throw err;
  }
}

/**
 * CANJE: debita a 1 punto = $1.00 MXN. El tope contra el saldo se valida
 * DESPUÉS de bloquear la fila (evita TOCTOU entre dos canjes concurrentes).
 *
 * `serie` (2026-10-21, pedido explícito del usuario): mismo reemplazo del
 * select de sucursal que ACUMULACION, pero sin folio — el canje no referencia
 * una venta externa, solo necesita saber la sucursal.
 */
async function registrarCanje({ cardId, puntos, serie }, user) {
  if (!(Number(puntos) > 0)) {
    throw new BadRequestError('puntos debe ser mayor a 0.');
  }

  const centroCostoId = await _resolverCentroCostoPorSerie(serie);
  const centroCosto = await _obtenerCentroCosto(centroCostoId);
  if (!centroCosto.permiteCanjeLealtad) {
    throw new UnprocessableError('Esta sucursal no permite canjes de lealtad.');
  }

  return sequelize.transaction(async (t) => {
    const card = await LoyaltyCard.findByPk(cardId, { transaction: t, lock: Transaction.LOCK.UPDATE });
    if (!card) throw new NotFoundError('Tarjeta');
    if (!card.activa) throw new UnprocessableError('La tarjeta está inactiva.');

    const puntosCents = toCents(puntos);
    const saldoCents  = toCents(card.saldoPuntos);
    if (puntosCents > saldoCents) {
      throw new ConflictError('El canje excede el saldo de puntos disponible.');
    }

    const { usuarioId, usuarioNombre } = _userLabel(user);
    const entry = await _aplicarMovimiento(t, card, {
      tipo:   'CANJE',
      puntos: fromCents(-puntosCents),
      centroCostoId,
      origen: 'manual',
      usuarioId,
      usuarioNombre,
    });

    return { movimiento: entry, saldoPuntos: entry.saldoResultante };
  });
}

/**
 * AJUSTE: requiere un motivo activo cuyo `signoPermitido` concuerde con el
 * signo del movimiento, y una justificación no trivial.
 *
 * Motivos con `reason.porcentaje` configurado (2026-10-20) calculan los
 * puntos automáticamente desde `montoBase` (igual criterio D5/D3 que
 * ACUMULACION: snapshot de % + aritmética en centavos) en vez de tomar
 * `puntos` directo del body — ese es el camino manual de siempre, intacto
 * cuando el motivo no tiene % configurado.
 */
async function registrarAjuste({ cardId, puntos, montoBase, reasonId, justificacion }, user) {
  const justificacionTrim = String(justificacion ?? '').trim();
  if (justificacionTrim.length < 5) {
    throw new BadRequestError('justificacion es requerida (mínimo 5 caracteres).');
  }

  const reason = await LoyaltyAdjustmentReason.findByPk(reasonId);
  if (!reason || !reason.activo) {
    throw new BadRequestError('El motivo de ajuste no existe o está inactivo.');
  }

  let puntosCents;
  let montoBaseAplicado = null;
  let porcentajeAplicado = null;

  if (reason.porcentaje != null) {
    // Camino automático: el signo ya NO se decide por el valor de `puntos`
    // del body — lo decide `reason.signoPermitido` (bloqueado a POSITIVO o
    // NEGATIVO al crear/editar el motivo con %, nunca AMBOS).
    if (!(Number(montoBase) > 0)) {
      throw new BadRequestError('montoBase debe ser mayor a 0 para un motivo con % de cálculo automático.');
    }
    if (reason.signoPermitido !== 'POSITIVO' && reason.signoPermitido !== 'NEGATIVO') {
      // Defensivo: no debería pasar (crearReason/editarReason lo bloquean),
      // pero datos legados podrían tener AMBOS + porcentaje configurado.
      throw new BadRequestError(
        `El motivo "${reason.nombre}" tiene % de cálculo automático configurado con un signoPermitido inválido (${reason.signoPermitido}); debe ser POSITIVO o NEGATIVO.`,
      );
    }

    const montoCents = toCents(montoBase);
    const puntosCalculados = calcularPuntos(montoCents, reason.porcentaje);
    puntosCents = reason.signoPermitido === 'NEGATIVO' ? -puntosCalculados : puntosCalculados;
    if (puntosCents === 0) {
      throw new BadRequestError('El monto no genera puntos con el % configurado para este motivo.');
    }

    montoBaseAplicado = montoBase;
    porcentajeAplicado = reason.porcentaje;
  } else {
    // Camino manual, igual que siempre: `puntos` viene directo del body.
    const puntosNum = Number(puntos);
    if (!Number.isFinite(puntosNum) || puntosNum === 0) {
      throw new BadRequestError('puntos debe ser un número distinto de 0.');
    }
    puntosCents = toCents(puntosNum);
  }

  const signo = puntosCents > 0 ? 'POSITIVO' : 'NEGATIVO';
  if (reason.signoPermitido !== 'AMBOS' && reason.signoPermitido !== signo) {
    throw new BadRequestError(
      `El motivo "${reason.nombre}" no permite ajustes de signo ${signo === 'POSITIVO' ? 'positivo' : 'negativo'}.`,
    );
  }

  return sequelize.transaction(async (t) => {
    const card = await LoyaltyCard.findByPk(cardId, { transaction: t, lock: Transaction.LOCK.UPDATE });
    if (!card) throw new NotFoundError('Tarjeta');
    if (!card.activa) throw new UnprocessableError('La tarjeta está inactiva.');

    const saldoCents = toCents(card.saldoPuntos);
    if (saldoCents + puntosCents < 0) {
      throw new ConflictError('El ajuste dejaría el saldo de puntos en negativo.');
    }

    const { usuarioId, usuarioNombre } = _userLabel(user);
    const entry = await _aplicarMovimiento(t, card, {
      tipo:               'AJUSTE',
      puntos:             fromCents(puntosCents),
      montoVenta:         montoBaseAplicado,
      porcentajeAplicado,
      reasonId,
      justificacion: justificacionTrim,
      origen: 'manual',
      usuarioId,
      usuarioNombre,
    });

    return { movimiento: entry, saldoPuntos: entry.saldoResultante };
  });
}

async function listarMovimientos(cardId, { page = 1, limit = 50 } = {}) {
  const p = Math.max(1, Number(page) || 1);
  const l = Math.min(100, Number(limit) || 50);

  const { count, rows } = await LoyaltyPointsLedger.findAndCountAll({
    where:  { cardId },
    order:  [['createdAt', 'DESC']],
    limit:  l,
    offset: (p - 1) * l,
  });

  return { total: count, page: p, limit: l, pages: Math.ceil(count / l), movimientos: rows };
}

async function listarNiveles(cardId) {
  return LoyaltyTierHistory.findAll({ where: { cardId }, order: [['at', 'DESC']] });
}

module.exports = {
  registrarAcumulacion,
  registrarCanje,
  registrarAjuste,
  listarMovimientos,
  listarNiveles,
  _aplicarMovimiento,
};
