'use strict';

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

/**
 * Reglas globales de mapeo CFDI → cuentas contables.
 *
 * Matching (de más específica a más genérica):
 *   1. tipoComprobante + rfcEmisor exacto
 *   2. tipoComprobante sin importar rfcEmisor
 *   3. Sin restricción (comodín)
 *
 * La regla con menor `prioridad` numérica gana.
 */
const CfdiMappingRule = sequelize.define('CfdiMappingRule', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  nombre: {
    type:      DataTypes.STRING(150),
    allowNull: false,
  },
  // Filtros de matching (null = cualquiera)
  tipoComprobante: {
    type:      DataTypes.ENUM('I', 'E', 'P'),
    allowNull: true,
  },
  rfcEmisor: {
    type:      DataTypes.STRING(13),
    allowNull: true,
  },
  metodoPago: {
    type:      DataTypes.STRING(3),
    allowNull: true,
    comment:   'Método de pago (PPD/PUE). null = cualquiera',
  },
  formaPago: {
    type:      DataTypes.STRING(2),
    allowNull: true,
    comment:   'Forma de pago SAT (01=Efectivo, 02=Cheque, 03=Transferencia, etc.). null = cualquiera',
  },
  // Cuentas contables (código SAT, ej. "501.01")
  cuentaCargo: {
    type:      DataTypes.STRING(20),
    allowNull: false,
    comment:   'Cuenta Debe principal (subtotal)',
  },
  cuentaAbono: {
    type:      DataTypes.STRING(20),
    allowNull: false,
    comment:   'Cuenta Haber principal (total)',
  },
  cuentaIva: {
    type:      DataTypes.STRING(20),
    allowNull: true,
    comment:   'IVA causado/acreditable (PUE o reconocimiento final en Pago)',
  },
  cuentaIvaPPD: {
    type:      DataTypes.STRING(20),
    allowNull: true,
    comment:   'IVA por cobrar/por pagar en PPD — se traspasa a cuentaIva al recibir el CFDI de Pago',
  },
  cuentaIvaRetenido: {
    type:      DataTypes.STRING(20),
    allowNull: true,
    comment:   'Cuenta IVA retenido',
  },
  cuentaIsrRetenido: {
    type:      DataTypes.STRING(20),
    allowNull: true,
    comment:   'Cuenta ISR retenido',
  },
  centroCosto: {
    type:      DataTypes.STRING(100),
    allowNull: true,
    comment:   'Centro de costo que se asigna a los movimientos generados',
  },
  prioridad: {
    type:         DataTypes.INTEGER,
    allowNull:    false,
    defaultValue: 50,
    comment:      'Menor número = más específica, gana primero',
  },
  isActive: {
    type:         DataTypes.BOOLEAN,
    defaultValue: true,
  },
}, {
  tableName:   'cfdi_mapping_rules',
  underscored: true,
});

module.exports = CfdiMappingRule;
