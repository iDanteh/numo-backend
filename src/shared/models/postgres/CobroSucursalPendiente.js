'use strict';

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

/**
 * Cola de cobros cruzados de sucursal detectados desde la sucursal
 * VENDEDORA — la única que puede verlos, ya que `construirMovimientosPuente`
 * (cobros-sucursal-puente.service.js) acota su búsqueda de CFDIs a la propia
 * serie de facturación de la sucursal que se está generando (el "documento
 * relacionado" que revela un cobro cruzado SIEMPRE sale de un CFDI de la
 * serie de quien vendió, nunca de quien solo cobró).
 *
 * Antes de esto se intentó ampliar esa búsqueda a TODAS las sucursales para
 * que la cobradora también pudiera verlo — saturaba el ERP con 429 y, aun
 * con caché+reintento, tardaba 5+ minutos en la primera sucursal generada de
 * cada periodo (confirmado con el usuario 2026-08-05). En vez de eso: cuando
 * la vendedora detecta el cobro cruzado, encola aquí lo que la cobradora
 * necesita para su propio asiento; cuando la cobradora genera su póliza, lo
 * lee de esta tabla sin volver a tocar el ERP.
 */
const CobroSucursalPendiente = sequelize.define('CobroSucursalPendiente', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  rfc: {
    type:      DataTypes.STRING(20),
    allowNull: false,
  },
  centroCostoIdOrigen: {
    type:      DataTypes.INTEGER,
    allowNull: true,
  },
  centroCostoIdDestino: {
    type:      DataTypes.INTEGER,
    allowNull: false,
  },
  serieFolioTicket: {
    type:      DataTypes.STRING(50),
    allowNull: true,
  },
  // `cobro.folioOrigen` — mismo campo que usa el flujo normal para
  // idempotencia (PolizaMovimiento.folio) — también la llave de upsert aquí
  // junto con rfc/centroCostoIdDestino, para que re-generar la vendedora no
  // acumule filas duplicadas.
  folioOrigen: {
    type:      DataTypes.STRING(50),
    allowNull: false,
  },
  cfdiUuid: {
    type:      DataTypes.STRING(36),
    allowNull: true,
  },
  nombreCliente: {
    type:      DataTypes.STRING(255),
    allowNull: true,
  },
  montoTotal: {
    type:      DataTypes.DECIMAL(14, 2),
    allowNull: false,
  },
  // [{ cuentaId, monto, reglaNombre, esSF }] — ya resuelto (Caja/Bancos real,
  // saldo a favor, etc.) por construirMovimientosPuente al momento de
  // encolar; la cobradora solo las reproduce, no vuelve a resolver nada.
  lineas: {
    type:      DataTypes.JSONB,
    allowNull: false,
  },
  // 'PUE': la cobradora hace Cargo+Abono a la MISMA cuenta de cada línea
  // (self-balancing, cuadra contra el Cargo sin contrapartida que la
  // vendedora ya dejó en su propia póliza).
  // 'HUERFANO': la cobradora hace Cargo a Caja/Bancos + Abono a la cuenta
  // puente (cuadra contra el Cargo a la cuenta puente que la vendedora ya
  // dejó) — ticket sin factura todavía (ver "Pendientes por facturar").
  tratamiento: {
    type:      DataTypes.STRING(20),
    allowNull: false,
  },
  fechaCobro: {
    type:      DataTypes.DATE,
    allowNull: true,
  },
}, {
  tableName:   'cobros_sucursal_pendientes',
  underscored: true,
  indexes: [
    {
      name:   'cobros_sucursal_pendientes_rfc_destino_folio',
      fields: ['rfc', 'centro_costo_id_destino', 'folio_origen'],
      unique: true,
    },
  ],
});

module.exports = CobroSucursalPendiente;
