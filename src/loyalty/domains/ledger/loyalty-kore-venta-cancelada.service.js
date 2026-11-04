'use strict';

/**
 * domains/ledger/loyalty-kore-venta-cancelada.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — webhook Kore→Numo de CANCELACIÓN (total O PARCIAL) de
 * una venta ya procesada por `loyalty-kore-venta.service.js#procesarVentaKore`.
 *
 * REDISEÑO 2026-11-04 (pedido explícito del usuario, todavía sin tráfico real
 * contra Kore — checklist de integración pendiente, seguro de rediseñar sin
 * preocuparse de compatibilidad con datos ya escritos): antes solo soportaba
 * UNA reversión total por venta, para siempre
 * (`referenciaExterna = "<serieFolio>:REVERSION"`). Ahora Kore puede avisar
 * VARIAS cancelaciones (parciales) contra la misma venta, cada una con su
 * propio `serieFolioCancelacion` (el identificador que Kore le da a CADA aviso
 * de cancelación, distinto del `serieFolio` de la venta original) —
 * `referenciaExterna = "<serieFolio>:REVERSION:<serieFolioCancelacion>"`, lo
 * que preserva la compatibilidad con el `LIKE '${serieFolio}:%'` ya usado para
 * traer TODAS las filas de una venta (originales ACUMULACION/PROMOCION/CANJE +
 * cualquier REVERSION previa): basta con distinguir las dos categorías por si
 * el string contiene `:REVERSION:` o no (ver `procesarVentaCanceladaKore`).
 *
 * Basado en el patrón YA existente de `POST /api/erp/cxc-reversiones` (ver
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
 *    explícitamente si YA existe la fila `:REVERSION:<serieFolioCancelacion>`
 *    de ESTE aviso puntual antes de hacer cualquier otra cosa (ver
 *    `procesarVentaCanceladaKore`) — cada aviso de cancelación tiene su propia
 *    llave de idempotencia, independiente de cualquier cancelación previa
 *    contra la misma venta.
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
 * Cálculo de la cancelación PARCIAL (prorrateo, decisión explícita del
 * usuario): se revierte la MISMA fracción (`importeCancelado /
 * montoVentaOriginal`) tanto de ACUMULACION+PROMOCION como de CANJE, en vez
 * de, por ejemplo, revertir primero el canje y dejar la acumulación intacta
 * hasta agotar el importe. Es más simple de razonar (una sola fracción,
 * aplicada parejo a todo lo que la venta generó) y evita definir un orden de
 * prioridad arbitrario entre ACUMULACION/PROMOCION/CANJE que Kore nunca
 * especificó.
 *
 * `listarVentasCanceladasConDeposito` (bandeja de auditoría cruzada) cruza las
 * reversiones ya aplicadas acá contra `CollectionRequest` (Mongo, dominio
 * `banks/domains/collection-requests`) para detectar el caso que le importa al
 * equipo de Numo: una venta que SÍ llegó a tener un depósito bancario real
 * vinculado y que terminó cancelándose (total o parcialmente) del lado de
 * Kore después.
 */

const { Transaction, Op } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');
const { LoyaltyCard, LoyaltyPointsLedger } = require('../../../shared/models/postgres');
const { BadRequestError, ConflictError, NotFoundError } = require('../../../shared/errors/AppError');
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
// Mismo criterio de tolerancia que loyalty-kore-venta.service.js#TOLERANCIA_PESOS (chequeo de
// consistencia del `subtotal` reenviado contra el guardado, y del prorrateo de formas de pago
// mixtas) — DUPLICADO a propósito, mismo patrón ya aplicado arriba para SERIE_FOLIO_RE.
const TOLERANCIA_PESOS = 0.01;

function _refReversion(serieFolio, serieFolioCancelacion) {
  return `${serieFolio}:REVERSION:${serieFolioCancelacion}`;
}

function _esReversion(referenciaExterna) {
  return !!referenciaExterna?.includes(':REVERSION:');
}

function _validarPayloadCancelacion(payload) {
  const {
    serieFolio, serieFolioCancelacion, tipoCancelacion, importeCancelado,
    montoVenta, motivoCancelacion, clienteId, nombreCliente,
  } = payload ?? {};

  const serieFolioTrim = String(serieFolio ?? '').trim();
  if (!SERIE_FOLIO_RE.test(serieFolioTrim)) {
    throw new BadRequestError('serieFolio debe tener el formato SERIE-FOLIO (ej. A0-260605320).');
  }

  const serieFolioCancelacionTrim = String(serieFolioCancelacion ?? '').trim();
  if (!SERIE_FOLIO_RE.test(serieFolioCancelacionTrim)) {
    throw new BadRequestError('serieFolioCancelacion debe tener el formato SERIE-FOLIO (ej. A0-260605321).');
  }

  if (tipoCancelacion !== 'total' && tipoCancelacion !== 'parcial') {
    throw new BadRequestError('tipoCancelacion debe ser "total" o "parcial".');
  }

  let importeCanceladoNum = null;
  if (tipoCancelacion === 'parcial') {
    importeCanceladoNum = Number(importeCancelado);
    if (!(importeCanceladoNum > 0)) {
      throw new BadRequestError('importeCancelado debe ser un número mayor a 0 para una cancelación parcial.');
    }
  }

  return {
    serieFolio: serieFolioTrim,
    serieFolioCancelacion: serieFolioCancelacionTrim,
    tipoCancelacion,
    importeCancelado: importeCanceladoNum,
    montoVenta: montoVenta != null && montoVenta !== '' ? Number(montoVenta) : null,
    motivoCancelacion: motivoCancelacion ?? null,
    clienteId: clienteId ?? null,
    nombreCliente: nombreCliente ?? null,
  };
}

/**
 * Resuelve `montoVentaOriginal` (en pesos) para prorratear una cancelación
 * parcial. Fuente autoritativa: la fila ACUMULACION ya guardada (nunca se
 * recalcula a partir de lo que reenvíe Kore). Si no existe (venta 100% CANJE,
 * nunca acumuló puntos nuevos), cae al `montoVenta` del body — ahí no hay 2
 * fuentes de verdad en conflicto porque Numo nunca tuvo una propia. Lanza
 * `ConflictError` (409) si ambas fuentes existen y no coinciden dentro de
 * `TOLERANCIA_PESOS`.
 */
function _resolverMontoVentaOriginal({ originales, montoVentaBody }) {
  const filaAcumulacion = originales.find((f) => f.tipo === 'ACUMULACION' && f.montoVenta != null);

  if (filaAcumulacion) {
    const montoVentaOriginal = Number(filaAcumulacion.montoVenta);
    if (montoVentaBody != null && Math.abs(montoVentaOriginal - montoVentaBody) > TOLERANCIA_PESOS) {
      throw new ConflictError(
        `El montoVenta recibido ($${montoVentaBody}) no coincide con el monto de la venta original guardado ($${montoVentaOriginal}).`,
      );
    }
    return montoVentaOriginal;
  }

  if (montoVentaBody != null) {
    return montoVentaBody;
  }

  return null;
}

/**
 * Punto de entrada del webhook `POST /api/loyalty/kore/venta-cancelada`. Kore
 * avisa que canceló (total o parcialmente, del lado de él) una venta cuyo
 * serieFolio ya había generado puntos (ACUMULACION de nivel y/o promoción)
 * y/o descontado puntos (CANJE) vía `procesarVentaKore`. Revierte el monto
 * correspondiente con UNA sola fila `AJUSTE` nueva por aviso — nunca modifica
 * ni borra las filas originales ni las de cancelaciones previas (ledger
 * append-only, D4).
 */
async function procesarVentaCanceladaKore(payload) {
  const datos = _validarPayloadCancelacion(payload);
  const {
    serieFolio, serieFolioCancelacion, tipoCancelacion, importeCancelado,
    montoVenta, motivoCancelacion, clienteId, nombreCliente,
  } = datos;

  const refReversion = _refReversion(serieFolio, serieFolioCancelacion);

  // Idempotencia REAL (ver docstring del archivo) — se busca ANTES de tocar
  // cualquier otra cosa, por la llave de ESTE aviso puntual
  // (serieFolio+serieFolioCancelacion). Si ya existe, Kore puede estar
  // reenviando el mismo aviso (timeout de su lado, reintento propio) — se
  // responde "ya revertida" sin duplicar nada.
  const yaRevertida = await LoyaltyPointsLedger.findOne({
    where: { origen: ORIGEN_KORE, referenciaExterna: refReversion },
  });
  if (yaRevertida) {
    return { ok: true, yaRevertida: true };
  }

  // Todas las filas 'kore' de este serieFolio: originales (ACUMULACION de
  // nivel, ACUMULACION de promoción, CANJE) + cualquier REVERSION previa.
  const filas = await LoyaltyPointsLedger.findAll({
    where: { origen: ORIGEN_KORE, referenciaExterna: { [Op.like]: `${serieFolio}:%` } },
  });
  const originales = filas.filter((f) => !_esReversion(f.referenciaExterna));
  const reversionesPrevias = filas.filter((f) => _esReversion(f.referenciaExterna));

  if (!originales.length) {
    throw new NotFoundError(
      `No se encontró ninguna venta de Kore con serieFolio "${serieFolio}" para revertir.`,
    );
  }

  // `puntos` ya viene con signo (ACUMULACION positivo, CANJE negativo) — la suma neta es
  // exactamente lo que esta venta le dejó a la tarjeta antes de cualquier cancelación.
  const netoOriginalCents = originales.reduce((acc, f) => acc + toCents(f.puntos), 0);
  // Las reversiones previas ya tienen signo OPUESTO al original (son AJUSTE negativos de una venta
  // que sumaba, o positivos de una venta que restaba) — sumarlas tal cual da lo ya revertido.
  const yaRevertidoCents = reversionesPrevias.reduce((acc, f) => acc + toCents(f.puntos), 0);
  const cardId = originales[0].cardId;

  let montoRevertirCents;
  if (tipoCancelacion === 'total') {
    // Revierte TODO lo que quede sin revertir — soporta tanto una cancelación total directa como
    // una cancelación total después de una o más parciales previas.
    const remanenteCents = netoOriginalCents + yaRevertidoCents;
    montoRevertirCents = -remanenteCents;
  } else {
    const montoVentaOriginal = _resolverMontoVentaOriginal({ originales, montoVentaBody: montoVenta });
    if (montoVentaOriginal == null) {
      throw new BadRequestError(
        'No se puede calcular la proporción de la cancelación: falta montoVenta y la venta no tiene un monto de acumulación guardado.',
      );
    }

    const fraccionYaRevertida = netoOriginalCents !== 0
      ? Math.abs(yaRevertidoCents) / Math.abs(netoOriginalCents)
      : 0;
    const importeYaCancelado = fraccionYaRevertida * montoVentaOriginal;
    if (importeYaCancelado + importeCancelado > montoVentaOriginal + TOLERANCIA_PESOS) {
      throw new BadRequestError('La cancelación acumulada superaría el monto original de la venta.');
    }

    const fraccion = importeCancelado / montoVentaOriginal;
    // Prorratea TANTO acumulación como canje con la MISMA fracción (decisión explícita del
    // usuario, ver docstring del archivo).
    montoRevertirCents = -Math.round(netoOriginalCents * fraccion);
  }

  const detalle = [`tipoCancelacion=${tipoCancelacion}`];
  if (tipoCancelacion === 'parcial') detalle.push(`importeCancelado=$${importeCancelado}`);
  if (motivoCancelacion) detalle.push(`motivo: ${motivoCancelacion}`);
  if (clienteId) detalle.push(`clienteId=${clienteId}`);
  if (nombreCliente) detalle.push(`cliente: ${nombreCliente}`);
  const justificacion = `Reversión automática de ${serieFolio} por cancelación Kore (aviso ${serieFolioCancelacion}) — ${detalle.join(', ')}`;

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
        tipoCancelacion,
        puntosRevertidos: fromCents(montoRevertirCents),
        saldoResultante: Number(entry.saldoResultante),
      };
    });

    if (resultado.saldoInsuficiente) {
      await registrarVentaFallida(
        serieFolio,
        {
          serieFolio, serieFolioCancelacion, tipoCancelacion, importeCancelado, motivoCancelacion,
        },
        'Saldo insuficiente para revertir cancelación de venta',
      );
      return { ok: true, revertido: false, motivo: 'saldo_insuficiente' };
    }
    return resultado;
  } catch (err) {
    if (err?.name === 'SequelizeUniqueConstraintError') {
      // Carrera: otro request con el mismo serieFolio+serieFolioCancelacion (reenvío de Kore en
      // paralelo) ganó el insert de la fila :REVERSION:... entre el chequeo de idempotencia de
      // arriba y este insert — ya quedó revertida.
      logger.warn(`[loyalty-kore-venta-cancelada] serieFolio=${serieFolio} serieFolioCancelacion=${serieFolioCancelacion}: carrera detectada al insertar la reversión, ya la aplicó otro request concurrente.`);
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
      referenciaExterna: { [Op.like]: '%:REVERSION:%' },
    },
    include: [{ model: LoyaltyCard, as: 'card', attributes: ['id', 'telefono', 'nombre'] }],
    order: [['createdAt', 'DESC']],
  });

  const resultado = [];
  for (const rev of reversiones) {
    const [serieFolio, serieFolioCancelacion] = rev.referenciaExterna.split(':REVERSION:');
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
      serieFolioCancelacion,
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
