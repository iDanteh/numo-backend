'use strict';

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const PolizaMovimiento = sequelize.define('PolizaMovimiento', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  polizaId: {
    type:       DataTypes.INTEGER,
    allowNull:  false,
    references: { model: 'polizas', key: 'id' },
    onDelete:   'CASCADE',
  },
  orden: {
    type:         DataTypes.INTEGER,
    allowNull:    false,
    defaultValue: 0,
  },
  cuentaId: {
    type:       DataTypes.INTEGER,
    allowNull:  true,           // null cuando la cuenta no existe en el catálogo
    references: { model: 'account_plans', key: 'id' },
  },
  cuentaFaltante: {
    type:         DataTypes.BOOLEAN,
    allowNull:    false,
    defaultValue: false,        // true = cuenta configurada en la regla no existe en catálogo
  },
  concepto: {
    type:      DataTypes.STRING(500),
    allowNull: false,
  },
  debe: {
    type:         DataTypes.DECIMAL(18, 2),
    allowNull:    false,
    defaultValue: 0,
  },
  haber: {
    type:         DataTypes.DECIMAL(18, 2),
    allowNull:    false,
    defaultValue: 0,
  },
  serie: {
    type:      DataTypes.STRING(25),
    allowNull: true,
  },
  ventaFecha: {
    type:      DataTypes.DATEONLY,
    allowNull: true,
  },
  centroCosto: {
    type:      DataTypes.STRING(100),
    allowNull: true,
  },
  centroCostoId: {
    type:       DataTypes.INTEGER,
    allowNull:  true,
    references: { model: 'centros_costo', key: 'id' },
    comment:    'FK al catálogo de centros de costo (resuelto por clave al generar póliza)',
  },
  cfdiUuid: {
    type:      DataTypes.STRING(36),
    allowNull: true,
  },
  rfcTercero: {
    type:      DataTypes.STRING(13),
    allowNull: true,
  },
  // ── Campos SAT del CFDI origen ───────────────────────────────────────────
  tipoComprobante: {
    type:      DataTypes.STRING(1),
    allowNull: true,
    comment:   'I=Ingreso E=Egreso P=Pago (del CFDI SAT)',
  },
  metodoPago: {
    type:      DataTypes.STRING(3),
    allowNull: true,
    comment:   'PPD o PUE (del CFDI SAT)',
  },
  formaPago: {
    type:      DataTypes.STRING(3),
    allowNull: true,
    comment:   'c_FormaPago SAT: 01=Efectivo 03=Trans 04=Cheque etc.',
  },
  folio: {
    type:      DataTypes.STRING(40),
    allowNull: true,
    comment:   'Folio del CFDI origen',
  },
  rfcEmisor: {
    type:      DataTypes.STRING(13),
    allowNull: true,
    comment:   'RFC del emisor del CFDI',
  },
  rfcReceptor: {
    type:      DataTypes.STRING(13),
    allowNull: true,
    comment:   'RFC del receptor del CFDI',
  },
}, {
  tableName:   'poliza_movimientos',
  underscored: true,
  indexes: [
    { fields: ['poliza_id'] },
    { fields: ['cfdi_uuid'] },
    { fields: ['cuenta_id'] },
  ],
});

module.exports = PolizaMovimiento;
