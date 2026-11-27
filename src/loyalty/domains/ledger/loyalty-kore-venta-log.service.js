'use strict';

/**
 * domains/ledger/loyalty-kore-venta-log.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — escritura y lectura de `LoyaltyKoreVentaLog` (auditoría
 * append-only de cada request real que Kore manda a /venta y /ventas, 2026-11-17,
 * pedido explícito del usuario). `registrarVentaLog` nunca debe tumbar el webhook
 * real ni su respuesta — ver el try/catch del lado del caller en
 * loyalty-kore-venta.routes.js, mismo criterio que el resto de la auditoría
 * no-bloqueante de este proyecto (emisión de socket, etc.).
 */
// cast/col/where son exports nombrados del paquete `sequelize`, igual que Op. `where` se importa
// como `sequelizeWhere` porque la función listarVentasLog ya usa `where` como nombre de su objeto
// de condiciones — con el mismo nombre, el `const where = {}` local taparía el import (TDZ) y
// romperia la llamada a la función de Sequelize.
const {
  Op, cast, col, where: sequelizeWhere,
} = require('sequelize');
const { LoyaltyKoreVentaLog } = require('../../../shared/models/postgres');
const { BadRequestError } = require('../../../shared/errors/AppError');

const LIMITE_DEFAULT = 100;
const LIMITE_MAX = 500;

function _parseFecha(valor, nombreCampo) {
  if (!valor) return null;
  const fecha = new Date(valor);
  if (Number.isNaN(fecha.getTime())) {
    throw new BadRequestError(`${nombreCampo} no es una fecha válida (usar ISO 8601).`);
  }
  return fecha;
}

/** @param {{endpoint: 'venta'|'ventas', payload: object, resultado: 'exito'|'error', detalle?: string}} datos */
async function registrarVentaLog({
  endpoint, payload, resultado, detalle,
}) {
  return LoyaltyKoreVentaLog.create({
    endpoint, payload, resultado, detalle: detalle ?? null,
  });
}

/**
 * Lista logs de venta Kore, más recientes primero. Todos los filtros son opcionales.
 *
 * @param {{resultado?: 'exito'|'error', endpoint?: 'venta'|'ventas', desde?: string,
 *          hasta?: string, limit?: number, offset?: number, serieFolio?: string,
 *          telefono?: string, puntosAbonados?: string}} filtros
 */
async function listarVentasLog({
  resultado, endpoint, desde, hasta, limit, offset, serieFolio, telefono, puntosAbonados,
} = {}) {
  if (resultado && !['exito', 'error'].includes(resultado)) {
    throw new BadRequestError('resultado debe ser "exito" o "error".');
  }
  if (endpoint && !['venta', 'ventas'].includes(endpoint)) {
    throw new BadRequestError('endpoint debe ser "venta" o "ventas".');
  }

  const desdeFecha = _parseFecha(desde, 'desde');
  const hastaFecha = _parseFecha(hasta, 'hasta');

  const where = {};
  if (resultado) where.resultado = resultado;
  if (endpoint) where.endpoint = endpoint;
  if (desdeFecha || hastaFecha) {
    where.createdAt = {};
    if (desdeFecha) where.createdAt[Op.gte] = desdeFecha;
    if (hastaFecha) where.createdAt[Op.lte] = hastaFecha;
  }

  // Búsqueda de texto sobre el payload crudo: serieFolio/telefono/puntosAbonados
  // viven en niveles distintos del JSON según endpoint ('venta' vs 'ventas' en lote),
  // así que no hay un único JSON path exacto; ILIKE sobre payload::text cubre ambas formas.
  const condicionesTexto = [];
  if (serieFolio) condicionesTexto.push(sequelizeWhere(cast(col('payload'), 'text'), { [Op.iLike]: `%${serieFolio}%` }));
  if (telefono) condicionesTexto.push(sequelizeWhere(cast(col('payload'), 'text'), { [Op.iLike]: `%${telefono}%` }));
  if (puntosAbonados) condicionesTexto.push(sequelizeWhere(cast(col('payload'), 'text'), { [Op.iLike]: `%${puntosAbonados}%` }));
  if (condicionesTexto.length) where[Op.and] = condicionesTexto;

  const limitFinal = Number(limit) > 0 ? Math.min(Number(limit), LIMITE_MAX) : LIMITE_DEFAULT;
  const offsetFinal = Number(offset) > 0 ? Number(offset) : 0;

  const { rows, count } = await LoyaltyKoreVentaLog.findAndCountAll({
    where,
    order: [['createdAt', 'DESC']],
    limit: limitFinal,
    offset: offsetFinal,
  });

  return { total: count, logs: rows };
}

module.exports = { registrarVentaLog, listarVentasLog };
