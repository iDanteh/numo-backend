'use strict';

/**
 * domains/ledger/loyalty-kore-venta-cancelada.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — webhook Kore→Numo de CANCELACIÓN de una venta ya
 * procesada por `loyalty-kore-venta.service.js#procesarVentaKore`. Contrato
 * diseñado y aprobado explícitamente con el usuario (2026-11-04), basado en el
 * patrón YA existente de `POST /api/erp/cxc-reversiones` (ver
 * `banks/domains/erp/erp-reversion.service.js`), reusando lo que aplica (webhook
 * server-to-server, rastro de auditoría propio) y descartando lo que no:
 *
 *  - `cxc-reversiones` es "convergente sin validar": siempre recalcula y
 *    corrige el link vigente, sin importar cuántas veces Kore reenvíe el mismo
 *    aviso — ahí no hay "doble aplicación" posible porque el estado final
 *    siempre se deriva de cero contra Kore. ACÁ no alcanza con ese criterio:
 *    el ledger de Lealtad es APPEND-ONLY (D4, nunca se actualiza una fila
 *    existente) con un CHECK `saldo_puntos >= 0` en Postgres — "aplicar de
 *    nuevo" significaría insertar una SEGUNDA fila de reversión y descontar
 *    el doble. Por eso acá la idempotencia es REAL: se verifica
 *    explícitamente si YA existe la fila `:REVERSION` antes de hacer
 *    cualquier otra cosa (ver `procesarVentaCanceladaKore`).
 *  - Misma filosofía de "nunca bloquear al llamador con un error de negocio
 *    esperado": si el saldo no alcanza para revertir (el cliente ya gastó los
 *    puntos que esta venta había acreditado), no se lanza una excepción — se
 *    registra como proceso fallido (misma bandeja "Ventas fallidas" que ya
 *    usa el webhook de venta, ver `loyalty-venta-fallida.service.js`) y se
 *    responde 200 igual: es un caso de negocio esperado, con infraestructura
 *    propia para atenderlo, no un error de Kore.
 *  - Sin reintentos acotados (a diferencia de `/venta`,
 *    `REINTENTOS_DELAYS_MS` en `loyalty-kore-venta.service.js`): esta
 *    operación no hace NINGUNA llamada externa a Kore (solo lee/escribe
 *    Postgres) — no hay latencia de red que reintentar. Un error acá es un
 *    error real (ej. la base de datos caída), y se propaga tal cual.
 *
 * `listarVentasCanceladasConDeposito` (bandeja de auditoría cruzada) cruza las
 * reversiones ya aplicadas acá contra `CollectionRequest` (Mongo, dominio
 * `banks/domains/collection-requests`) para detectar el caso que le importa al
 * equipo de Numo: una venta que SÍ llegó a tener un depósito bancario real
 * vinculado y que terminó cancelándose del lado de Kore después.
 */

const { Transaction, Op } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');
const { LoyaltyCard, LoyaltyPointsLedger } = require('../../../shared/models/postgres');
const { BadRequestError, NotFoundError } = require('../../../shared/errors/AppError');
const { toCents, fromCents } = require('../../shared/puntos.util');
const { _aplicarMovimiento } = require('./loyalty-ledger.service');
const { registrarVentaFallida } = require('./loyalty-venta-fallida.service');
const { logger } = require('../../../shared/utils/logger');
// Cruzado con Bancos/Solicitudes de Cobro (Mongo) para la bandeja de auditoría
// (Pieza 2) — mismo sentido de import que ya usa, en reversa,
// `collection-request.service.js` para llamar a
// `loyalty-kore-venta.service.js#procesarVentaKore` (ver el comentario de ese
// require ahí: cruce intencional entre dominios, sin dependencia circular —
// este archivo no es requerido desde collection-request.service.js ni
// viceversa). Desde `loyalty/domains/ledger/` hacia `banks/domains/
// collection-requests/` son 3 niveles arriba (../../../) + la ruta del dominio.
const CollectionRequest = require('../../../banks/domains/collection-requests/CollectionRequest.model');
const { movimientosDe } = require('../../../banks/domains/collection-requests/collection-request-asignaciones');

const ORIGEN_KORE = 'kore';
// Mismo formato que loyalty-kore-venta.service.js#SERIE_FOLIO_RE —
// DUPLICADO a propósito (mismo criterio ya documentado ahí y en
// loyalty-ledger.service.js#SERIE_FOLIO_RE: evitar un acoplamiento extra por
// una sola línea de regex entre servicios de dominios/archivos distintos).
const SERIE_FOLIO_RE = /^([A-Za-z0-9]{1,10})-(\d{1,20})$/;

function _refReversion(serieFolio) { return `${serieFolio}:REVERSION`; }

/**
 * Punto de entrada del webhook `POST /api/loyalty/kore/venta-cancelada`. Kore
 * avisa que canceló (del lado de él) una venta cuyo serieFolio ya había
 * generado puntos (ACUMULACION de nivel y/o promoción) y/o descontado puntos
 * (CANJE) vía `procesarVentaKore`. Revierte el NETO de esos movimientos con
 * UNA sola fila `AJUSTE` nueva — nunca modifica ni borra las filas originales
 * (ledger append-only, D4).
 */
async function procesarVentaCanceladaKore({ serieFolio, motivoCancelacion } = {}) {
  const serieFolioTrim = String(serieFolio ?? '').trim();
  if (!SERIE_FOLIO_RE.test(serieFolioTrim)) {
    throw new BadRequestError('serieFolio debe tener el formato SERIE-FOLIO (ej. A0-260605320).');
  }

  const refReversion = _refReversion(serieFolioTrim);

  // Idempotencia REAL (ver docstring del archivo) — se busca ANTES de tocar
  // cualquier otra cosa. Si ya existe, Kore puede estar reenviando el mismo
  // aviso (timeout de su lado, reintento propio) — se responde "ya revertida"
  // sin duplicar nada.
  const yaRevertida = await LoyaltyPointsLedger.findOne({
    where: { origen: ORIGEN_KORE, referenciaExterna: refReversion },
  });
  if (yaRevertida) {
    return { ok: true, yaRevertida: true };
  }

  // Todas las filas que procesarVentaKore generó para este serieFolio (hasta
  // 3: ACUMULACION de nivel, ACUMULACION de promoción, CANJE — ver
  // loyalty-kore-venta.service.js#_refAcumulacion/_refPromocion/_refCanje).
  // Se excluye cualquier `:REVERSION` por defensividad (no debería poder
  // aparecer acá — la idempotencia de arriba ya la hubiera atrapado antes).
  const filasVenta = (await LoyaltyPointsLedger.findAll({
    where: { origen: ORIGEN_KORE, referenciaExterna: { [Op.like]: `${serieFolioTrim}:%` } },
  })).filter((f) => !f.referenciaExterna?.endsWith(':REVERSION'));

  if (!filasVenta.length) {
    throw new NotFoundError(
      `No se encontró ninguna venta de Kore con serieFolio "${serieFolioTrim}" para revertir.`,
    );
  }

  // `puntos` ya viene con signo (ACUMULACION positivo, CANJE negativo) — la
  // suma neta es exactamente lo que esta venta le dejó a la tarjeta; revertir
  // es aplicar el MISMO monto con el signo invertido.
  const netoCents = filasVenta.reduce((acc, f) => acc + toCents(f.puntos), 0);
  const montoRevertirCents = -netoCents;
  const cardId = filasVenta[0].cardId;

  const justificacion = `Reversión automática de ${serieFolioTrim} por cancelación Kore`
    + (motivoCancelacion ? `: ${motivoCancelacion}` : '');

  try {
    const resultado = await sequelize.transaction(async (t) => {
      // Mismo patrón que _procesarVentaKoreInterno (loyalty-kore-venta.service.js):
      // la tarjeta se bloquea (LOCK.UPDATE) ANTES de decidir si el saldo
      // alcanza — evita TOCTOU contra cualquier otro movimiento concurrente
      // de la misma tarjeta.
      const cardLocked = await LoyaltyCard.findByPk(cardId, { transaction: t, lock: Transaction.LOCK.UPDATE });
      if (!cardLocked) throw new NotFoundError('Tarjeta de Lealtad');

      const saldoActualCents = toCents(cardLocked.saldoPuntos);
      // Validado ACÁ, antes de intentar el insert — nunca se depende del
      // CHECK saldo_puntos >= 0 de Postgres para manejar este caso (ver
      // docstring del archivo): queremos decidir nosotros el 200 de negocio,
      // no recibir un error crudo de la base de datos.
      if (saldoActualCents + montoRevertirCents < 0) {
        return { saldoInsuficiente: true };
      }

      // _aplicarMovimiento (loyalty-ledger.service.js) YA acepta origen/
      // referenciaExterna como parámetros opcionales (default 'manual'/null)
      // — no hizo falta extender su firma para este caso.
      const entry = await _aplicarMovimiento(t, cardLocked, {
        tipo: 'AJUSTE',
        puntos: fromCents(montoRevertirCents),
        justificacion,
        origen: ORIGEN_KORE,
        referenciaExterna: refReversion,
      });

      return {
        ok: true,
        revertido: true,
        puntosRevertidos: fromCents(montoRevertirCents),
        saldoResultante: Number(entry.saldoResultante),
      };
    });

    if (resultado.saldoInsuficiente) {
      await registrarVentaFallida(
        serieFolioTrim,
        { serieFolio: serieFolioTrim, motivoCancelacion },
        'Saldo insuficiente para revertir cancelación de venta',
      );
      return { ok: true, revertido: false, motivo: 'saldo_insuficiente' };
    }
    return resultado;
  } catch (err) {
    if (err?.name === 'SequelizeUniqueConstraintError') {
      // Carrera: otro request con el mismo serieFolio (reenvío de Kore en
      // paralelo) ganó el insert de la fila :REVERSION entre el chequeo de
      // idempotencia de arriba y este insert — ya quedó revertida.
      logger.warn(`[loyalty-kore-venta-cancelada] serieFolio=${serieFolioTrim}: carrera detectada al insertar la reversión, ya la aplicó otro request concurrente.`);
      return { ok: true, yaRevertida: true };
    }
    throw err;
  }
}

/**
 * Bandeja de auditoría cruzada: reversiones de Kore (Pieza 1, arriba) cuyo
 * serieFolio SÍ llegó a tener un depósito bancario real vinculado en algún
 * `CollectionRequest` (Mongo) — el caso que le interesa al equipo de Numo
 * revisar (el cliente recibió/gastó puntos de una venta con plata real de por
 * medio que terminó cancelándose). Sin paginar (mismo criterio que
 * `listarVentasFallidas`): se espera que sean pocas filas.
 */
async function listarVentasCanceladasConDeposito() {
  const reversiones = await LoyaltyPointsLedger.findAll({
    where: {
      tipo: 'AJUSTE',
      origen: ORIGEN_KORE,
      referenciaExterna: { [Op.like]: '%:REVERSION' },
    },
    include: [{ model: LoyaltyCard, as: 'card', attributes: ['id', 'telefono', 'nombre'] }],
    order: [['createdAt', 'DESC']],
  });

  const resultado = [];
  for (const rev of reversiones) {
    const serieFolio = rev.referenciaExterna.replace(/:REVERSION$/, '');
    const match = SERIE_FOLIO_RE.exec(serieFolio);
    if (!match) continue; // defensivo — nunca debería pasar, ya se validó al crear la fila (Pieza 1)
    const [, serie, folioExterno] = match;

    // eslint-disable-next-line no-await-in-loop -- volumen bajo esperado (mismo
    // criterio que listarVentasFallidas), no vale la pena paralelizar contra Mongo.
    const cr = await CollectionRequest.findOne({ 'cxcs.serie': serie, 'cxcs.folioExterno': folioExterno })
      .sort({ createdAt: -1 });
    if (!cr) continue; // nunca tuvo una Solicitud de Cobro vinculada — fuera de la bandeja

    // `movimientosDe` ya resuelve formasPago[].bankMovementId +
    // depositosAdicionales + el fallback al campo raíz deprecado
    // bankMovementId (ver collection-request-asignaciones.js) — se reusa tal
    // cual para no duplicar ese criterio acá.
    const movs = movimientosDe(cr);
    if (!movs.length) continue; // nunca tuvo un depósito bancario real asignado

    resultado.push({
      cardId: rev.card?.id ?? null,
      collectionRequestId: cr._id,
      serieFolio,
      bankMovementId: movs[0],
      telefono: rev.card?.telefono ?? null,
      nombre: rev.card?.nombre ?? null,
      puntosRevertidos: rev.puntos,
      fecha: rev.createdAt,
    });
  }

  return resultado;
}

module.exports = { procesarVentaCanceladaKore, listarVentasCanceladasConDeposito };
