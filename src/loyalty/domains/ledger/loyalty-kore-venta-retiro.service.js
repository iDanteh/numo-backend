'use strict';

/**
 * domains/ledger/loyalty-kore-venta-retiro.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — webhook Kore→Numo de RETIRO (total O PARCIAL) de lo
 * acreditado/descontado por una venta ya procesada por
 * `loyalty-kore-venta.service.js#procesarVentaKore`.
 *
 * RENOMBRE 2026-11-20 (pedido explícito del usuario): este endpoint tenía antes
 * el nombre asociado a la cancelación de venta, pero nunca fue, en los
 * hechos, eso — es un retiro (total o parcial) de lo ya
 * acreditado/descontado por un cobro procesado. Kore avisó que la
 * cancelación REAL de una venta va a tener su propio tratamiento, todavía sin
 * definir (fuera de alcance acá) — este rename libera ese nombre anterior
 * para ese futuro flujo y deja este, que siempre fue un retiro, con el nombre
 * correcto. La lógica/prorrateo no cambia (ya verificada correcta antes de
 * este rename).
 *
 * REDISEÑO 2026-11-04 (pedido explícito del usuario, todavía sin tráfico real
 * contra Kore — checklist de integración pendiente, seguro de rediseñar sin
 * preocuparse de compatibilidad con datos ya escritos): antes solo soportaba
 * UN retiro total por venta, para siempre
 * (`referenciaExterna = "<serieFolio>:REVERSION"`). Ahora Kore puede avisar
 * VARIOS retiros (parciales) contra la misma venta, cada uno con su
 * propio `serieFolioRetiro` (el identificador que Kore le da a CADA aviso
 * de retiro, distinto del `serieFolio` de la venta original) —
 * `referenciaExterna = "<serieFolio>:REVERSION:<serieFolioRetiro>"`, lo
 * que preserva la compatibilidad con el `LIKE '${serieFolio}:%'` ya usado para
 * traer TODAS las filas de una venta (originales ACUMULACION/PROMOCION/CANJE +
 * cualquier REVERSION previa): basta con distinguir las dos categorías por si
 * el string contiene `:REVERSION:` o no (ver `procesarVentaRetiroKore`).
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
 *    explícitamente si YA existe la fila `:REVERSION:<serieFolioRetiro>`
 *    de ESTE aviso puntual antes de hacer cualquier otra cosa (ver
 *    `procesarVentaRetiroKore`) — cada aviso de retiro tiene su propia
 *    llave de idempotencia, independiente de cualquier retiro previo
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
 * Cálculo del retiro PARCIAL (prorrateo, decisión explícita del
 * usuario): se revierte la MISMA fracción (`importeRetirado /
 * montoVentaOriginal`) tanto de ACUMULACION+PROMOCION como de CANJE, en vez
 * de, por ejemplo, revertir primero el canje y dejar la acumulación intacta
 * hasta agotar el importe. Es más simple de razonar (una sola fracción,
 * aplicada parejo a todo lo que la venta generó) y evita definir un orden de
 * prioridad arbitrario entre ACUMULACION/PROMOCION/CANJE que Kore nunca
 * especificó.
 *
 * `listarVentasRetiradasConDeposito` (bandeja de auditoría cruzada) cruza las
 * reversiones ya aplicadas acá contra `CollectionRequest` (Mongo, dominio
 * `banks/domains/collection-requests`) para detectar el caso que le importa al
 * equipo de Numo: una venta que SÍ llegó a tener un depósito bancario real
 * vinculado y que terminó con un retiro (total o parcial) del lado de
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
const { emitToLoyaltyCard } = require('../../../banks/shared/socket');
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

function _refReversion(serieFolio, serieFolioRetiro) {
  return `${serieFolio}:REVERSION:${serieFolioRetiro}`;
}

function _esReversion(referenciaExterna) {
  return !!referenciaExterna?.includes(':REVERSION:');
}

function _validarPayloadRetiro(payload) {
  const {
    serieFolio, serieFolioRetiro, tipoRetiro, importeRetirado,
    montoVenta, motivoRetiro, clienteId, nombreCliente,
  } = payload ?? {};

  const serieFolioTrim = String(serieFolio ?? '').trim();
  if (!SERIE_FOLIO_RE.test(serieFolioTrim)) {
    throw new BadRequestError('serieFolio debe tener el formato SERIE-FOLIO (ej. A0-260605320).');
  }

  const serieFolioRetiroTrim = String(serieFolioRetiro ?? '').trim();
  if (!SERIE_FOLIO_RE.test(serieFolioRetiroTrim)) {
    throw new BadRequestError('serieFolioRetiro debe tener el formato SERIE-FOLIO (ej. A0-260605321).');
  }

  if (tipoRetiro !== 'total' && tipoRetiro !== 'parcial') {
    throw new BadRequestError('tipoRetiro debe ser "total" o "parcial".');
  }

  let importeRetiradoNum = null;
  if (tipoRetiro === 'parcial') {
    importeRetiradoNum = Number(importeRetirado);
    if (!(importeRetiradoNum > 0)) {
      throw new BadRequestError('importeRetirado debe ser un número mayor a 0 para un retiro parcial.');
    }
  }

  return {
    serieFolio: serieFolioTrim,
    serieFolioRetiro: serieFolioRetiroTrim,
    tipoRetiro,
    importeRetirado: importeRetiradoNum,
    montoVenta: montoVenta != null && montoVenta !== '' ? Number(montoVenta) : null,
    motivoRetiro: motivoRetiro ?? null,
    clienteId: clienteId ?? null,
    nombreCliente: nombreCliente ?? null,
  };
}

/**
 * Resuelve `montoVentaOriginal` (en pesos) para prorratear un retiro
 * parcial. Fuente autoritativa: la SUMA de todas las filas ACUMULACION ya
 * guardadas (nunca se recalcula a partir de lo que reenvíe Kore). Si no existe
 * ninguna (venta 100% CANJE, nunca acumuló puntos nuevos), cae al `montoVenta`
 * del body — ahí no hay 2 fuentes de verdad en conflicto porque Numo nunca
 * tuvo una propia. Lanza `ConflictError` (409) si ambas fuentes existen y no
 * coinciden dentro de `TOLERANCIA_PESOS`.
 *
 * FIX 2026-11-19 (bug real encontrado leyendo el código, no hipotético):
 * `loyalty-kore-venta.service.js` puede generar 2+ filas ACUMULACION para el
 * MISMO `serieFolio` cuando la CxC tiene varios abonos (una por `idAbono`,
 * cada una con su propio `montoVenta = abono.subtotal` — ver
 * `_procesarUnAbonoNuevo`). Antes este resolver tomaba (`.find()`) solo la
 * PRIMERA fila, usando el subtotal de un solo abono como si fuera "el monto
 * total de la venta" que pide la sección 6 del contrato — con 2+ abonos, la
 * fracción de cualquier retiro parcial quedaba mal calculada.
 *
 * LIMITACIÓN CONOCIDA que este fix NO resuelve: un abono que no calificó para
 * acumular (ej. pagado 100% con una forma de pago que no suma puntos) no deja
 * ninguna fila ACUMULACION — su subtotal sigue siendo invisible acá, así que
 * la suma puede seguir subestimando el monto real de la venta si la CxC tiene
 * abonos mixtos (algunos acumulan, otros no). Resolverlo del todo requiere
 * trackear el subtotal de cada abono que llega, califique o no, en algún
 * lugar durable — queda como pendiente de diseño aparte.
 */
function _resolverMontoVentaOriginal({ originales, montoVentaBody }) {
  const filasAcumulacion = originales.filter((f) => f.tipo === 'ACUMULACION' && f.montoVenta != null);

  if (filasAcumulacion.length) {
    const montoVentaOriginal = filasAcumulacion.reduce((acc, f) => acc + Number(f.montoVenta), 0);
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
 * Punto de entrada del webhook `POST /api/loyalty/kore/venta-retiro`. Kore
 * avisa que retiró (total o parcialmente, del lado de él) una venta cuyo
 * serieFolio ya había generado puntos (ACUMULACION de nivel y/o promoción)
 * y/o descontado puntos (CANJE) vía `procesarVentaKore`. Revierte el monto
 * correspondiente con UNA sola fila `AJUSTE` nueva por aviso — nunca modifica
 * ni borra las filas originales ni las de retiros previos (ledger
 * append-only, D4).
 */
async function procesarVentaRetiroKore(payload) {
  const datos = _validarPayloadRetiro(payload);
  const {
    serieFolio, serieFolioRetiro, tipoRetiro, importeRetirado,
    montoVenta, motivoRetiro, clienteId, nombreCliente,
  } = datos;

  const refReversion = _refReversion(serieFolio, serieFolioRetiro);

  // Idempotencia REAL (ver docstring del archivo) — se busca ANTES de tocar
  // cualquier otra cosa, por la llave de ESTE aviso puntual
  // (serieFolio+serieFolioRetiro). Si ya existe, Kore puede estar
  // reenviando el mismo aviso (timeout de su lado, reintento propio) — se
  // responde "ya revertida" sin duplicar nada.
  const yaRevertida = await LoyaltyPointsLedger.findOne({
    where: { origen: ORIGEN_KORE, referenciaExterna: refReversion },
  });
  if (yaRevertida) {
    // Notificación en vivo (2026-11-10): aviso reenviado (Kore reintentando), nada cambió de
    // verdad, pero emitir igual es inofensivo y evita tratar este caso distinto del resto —
    // mismo criterio que el blindaje cruzado manual↔kore de loyalty-kore-venta.service.js.
    emitToLoyaltyCard(yaRevertida.cardId, 'lealtad-tarjeta:actualizada', { cardId: yaRevertida.cardId, motivo: 'retiro-kore' });
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
  // exactamente lo que esta venta le dejó a la tarjeta antes de cualquier retiro.
  const netoOriginalCents = originales.reduce((acc, f) => acc + toCents(f.puntos), 0);
  // Las reversiones previas ya tienen signo OPUESTO al original (son AJUSTE negativos de una venta
  // que sumaba, o positivos de una venta que restaba) — sumarlas tal cual da lo ya revertido.
  const yaRevertidoCents = reversionesPrevias.reduce((acc, f) => acc + toCents(f.puntos), 0);
  const cardId = originales[0].cardId;

  let montoRevertirCents;
  if (tipoRetiro === 'total') {
    // Revierte TODO lo que quede sin revertir — soporta tanto un retiro total directo como
    // un retiro total después de uno o más parciales previos.
    const remanenteCents = netoOriginalCents + yaRevertidoCents;
    montoRevertirCents = -remanenteCents;
  } else {
    const montoVentaOriginal = _resolverMontoVentaOriginal({ originales, montoVentaBody: montoVenta });
    if (montoVentaOriginal == null) {
      throw new BadRequestError(
        'No se puede calcular la proporción del retiro: falta montoVenta y la venta no tiene un monto de acumulación guardado.',
      );
    }

    const fraccionYaRevertida = netoOriginalCents !== 0
      ? Math.abs(yaRevertidoCents) / Math.abs(netoOriginalCents)
      : 0;
    const importeYaRetirado = fraccionYaRevertida * montoVentaOriginal;
    if (importeYaRetirado + importeRetirado > montoVentaOriginal + TOLERANCIA_PESOS) {
      throw new BadRequestError('El retiro acumulado superaría el monto original de la venta.');
    }

    const fraccion = importeRetirado / montoVentaOriginal;
    // Prorratea TANTO acumulación como canje con la MISMA fracción (decisión explícita del
    // usuario, ver docstring del archivo).
    montoRevertirCents = -Math.round(netoOriginalCents * fraccion);
  }

  const detalle = [`tipoRetiro=${tipoRetiro}`];
  if (tipoRetiro === 'parcial') detalle.push(`importeRetirado=$${importeRetirado}`);
  if (motivoRetiro) detalle.push(`motivo: ${motivoRetiro}`);
  if (clienteId) detalle.push(`clienteId=${clienteId}`);
  if (nombreCliente) detalle.push(`cliente: ${nombreCliente}`);
  const justificacion = `Reversión automática de ${serieFolio} por retiro Kore (aviso ${serieFolioRetiro}) — ${detalle.join(', ')}`;

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
        tipoRetiro,
        puntosRevertidos: fromCents(montoRevertirCents),
        saldoResultante: Number(entry.saldoResultante),
      };
    });

    if (resultado.saldoInsuficiente) {
      await registrarVentaFallida(
        serieFolio,
        {
          serieFolio, serieFolioRetiro, tipoRetiro, importeRetirado, motivoRetiro,
        },
        'Saldo insuficiente para revertir retiro de venta',
      );
      // Sin emitir acá a propósito: no se escribió ningún movimiento nuevo en el ledger, un
      // refetch de tarjeta-detail no tendría nada distinto que mostrar.
      return { ok: true, revertido: false, motivo: 'saldo_insuficiente' };
    }
    // Notificación en vivo (2026-11-10) — ver docstring del archivo/pedido del usuario. `cardId`
    // ya resuelto arriba (línea de `originales[0].cardId`), antes de entrar a la transacción.
    emitToLoyaltyCard(cardId, 'lealtad-tarjeta:actualizada', { cardId, motivo: 'retiro-kore' });
    return resultado;
  } catch (err) {
    if (err?.name === 'SequelizeUniqueConstraintError') {
      // Carrera: otro request con el mismo serieFolio+serieFolioRetiro (reenvío de Kore en
      // paralelo) ganó el insert de la fila :REVERSION:... entre el chequeo de idempotencia de
      // arriba y este insert — ya quedó revertida. Mismo criterio que el `yaRevertida` de arriba:
      // emite igual, es inofensivo.
      logger.warn(`[loyalty-kore-venta-retiro] serieFolio=${serieFolio} serieFolioRetiro=${serieFolioRetiro}: carrera detectada al insertar la reversión, ya la aplicó otro request concurrente.`);
      emitToLoyaltyCard(cardId, 'lealtad-tarjeta:actualizada', { cardId, motivo: 'retiro-kore' });
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
 * medio que terminó con un retiro). Sin paginar (mismo criterio que
 * `listarVentasFallidas`): se espera que sean pocas filas.
 */
async function listarVentasRetiradasConDeposito() {
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
    const [serieFolio, serieFolioRetiro] = rev.referenciaExterna.split(':REVERSION:');
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
      serieFolioRetiro,
      bankMovementId: movs[0],
      telefono: rev.card?.telefono ?? null,
      nombre: rev.card?.nombre ?? null,
      puntosRevertidos: rev.puntos,
      fecha: rev.createdAt,
    });
  }

  return resultado;
}

module.exports = { procesarVentaRetiroKore, listarVentasRetiradasConDeposito };
