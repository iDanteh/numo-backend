'use strict';

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

/**
 * Cola de cierres de CxC pendientes entre sucursales — Cobranza (Pagos),
 * completamente independiente de `CobroSucursalPendiente` (esa es exclusiva
 * de Ingreso/ventas). Un Pago puede liquidar una factura PPD emitida por una
 * sucursal DISTINTA a la que procesó el cobro (ej. cliente paga en CEDIS una
 * factura de Santa Rosa) — solo la sucursal COBRADORA puede ver ese Pago (su
 * propia serie de facturación), así que encola aquí lo que la sucursal
 * VENDEDORA (dueña de la factura/CxC) necesita para cerrar su propio asiento,
 * sin que la vendedora tenga que volver a tocar el ERP.
 *
 * Mismo patrón que `CobroSucursalPendiente` (ver ese archivo): la cobradora
 * encola al generar su propia póliza; la vendedora lo consume al generar la
 * suya (aunque sea otro día — no hay orden garantizado entre ambas).
 */
const CobroSucursalPendienteCobranza = sequelize.define('CobroSucursalPendienteCobranza', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  rfc: {
    type:      DataTypes.STRING(20),
    allowNull: false,
  },
  // Sucursal que COBRÓ (procesó el Pago) — solo informativo.
  centroCostoIdOrigen: {
    type:      DataTypes.INTEGER,
    allowNull: true,
  },
  // Sucursal VENDEDORA (dueña de la factura/CxC) — quien debe consumir esta fila.
  centroCostoIdDestino: {
    type:      DataTypes.INTEGER,
    allowNull: false,
  },
  // Serie-folio de la FACTURA liquidada (no del Pago) — ej. "B0-260700335".
  serieFolioFactura: {
    type:      DataTypes.STRING(50),
    allowNull: false,
  },
  // UUID del CFDI de Pago que generó este cobro cruzado — trazabilidad.
  cfdiUuidPago: {
    type:      DataTypes.STRING(36),
    allowNull: true,
  },
  nombreCliente: {
    type:      DataTypes.STRING(255),
    allowNull: true,
  },
  // Monto neto (sin IVA) y su IVA por separado — la vendedora arma su propio
  // asiento (Cargo cuenta puente + Cargo IVA por trasladar + Abono IVA
  // trasladado + Abono Clientes) a partir de estos dos montos, igual que
  // cualquier factura liquidada normal.
  montoSubtotal: {
    type:      DataTypes.DECIMAL(14, 2),
    allowNull: false,
  },
  montoIva: {
    type:      DataTypes.DECIMAL(14, 2),
    allowNull: false,
    defaultValue: 0,
  },
  fechaCobro: {
    type:      DataTypes.DATE,
    allowNull: true,
  },
  // Llave de idempotencia junto con rfc/centroCostoIdDestino — regenerar la
  // póliza cobradora no debe duplicar filas en la cola.
  folioOrigen: {
    type:      DataTypes.STRING(80),
    allowNull: false,
  },
  // true una vez que la vendedora ya incluyó esta fila en una póliza propia
  // (contabilizada, no solo generada) — evita consumirla dos veces si la
  // vendedora regenera antes de contabilizar.
  consumido: {
    type:         DataTypes.BOOLEAN,
    allowNull:    false,
    defaultValue: false,
  },
}, {
  tableName:   'cobros_sucursal_pendientes_cobranza',
  underscored: true,
  indexes: [
    {
      name:   'cobros_sucursal_pend_cobranza_rfc_destino_folio',
      fields: ['rfc', 'centro_costo_id_destino', 'folio_origen'],
      unique: true,
    },
  ],
});

module.exports = CobroSucursalPendienteCobranza;
