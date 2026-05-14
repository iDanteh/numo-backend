'use strict';

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const Poliza = sequelize.define('Poliza', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  tipo: {
    type:      DataTypes.ENUM('A', 'I', 'E', 'D', 'N', 'C'),
    allowNull: false,
    comment:   'A=Apertura I=Ingreso E=Egreso D=Diario N=Nomina C=Cheque',
  },
  numero: {
    type:      DataTypes.INTEGER,
    allowNull: false,
  },
  fecha: {
    type:      DataTypes.DATEONLY,
    allowNull: false,
  },
  concepto: {
    type:      DataTypes.STRING(500),
    allowNull: false,
  },
  ejercicio: {
    type:      DataTypes.INTEGER,
    allowNull: false,
  },
  periodo: {
    type:      DataTypes.INTEGER,
    allowNull: false,
  },
  rfc: {
    type:      DataTypes.STRING(20),
    allowNull: false,
  },
  estado: {
    type:         DataTypes.ENUM('borrador', 'contabilizada', 'cancelada'),
    allowNull:    false,
    defaultValue: 'borrador',
  },
  folio: {
    type:      DataTypes.STRING(50),
    allowNull: true,
  },
  centroCosto: {
    type:      DataTypes.STRING(100),
    allowNull: true,
  },
  creadoPor: {
    type:      DataTypes.STRING(150),
    allowNull: true,
  },
  // ── Auditoría de cambios de estado ────────────────────────────────────────
  contabilizadoPor: {
    type:      DataTypes.STRING(150),
    allowNull: true,
  },
  contabilizadaAt: {
    type:      DataTypes.DATE,
    allowNull: true,
  },
  canceladoPor: {
    type:      DataTypes.STRING(150),
    allowNull: true,
  },
  canceladaAt: {
    type:      DataTypes.DATE,
    allowNull: true,
  },
  motivoCancelacion: {
    type:      DataTypes.STRING(500),
    allowNull: true,
  },
  revertidoPor: {
    type:      DataTypes.STRING(150),
    allowNull: true,
  },
  revertidaAt: {
    type:      DataTypes.DATE,
    allowNull: true,
  },
  motivoReversion: {
    type:      DataTypes.STRING(500),
    allowNull: true,
  },
}, {
  tableName:   'polizas',
  underscored: true,
  indexes: [
    { fields: ['rfc', 'ejercicio', 'periodo'] },
    { fields: ['tipo', 'numero', 'rfc', 'ejercicio', 'periodo'], unique: true },
  ],
});

module.exports = Poliza;
