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
    type:      DataTypes.ENUM('A', 'I', 'E', 'D', 'N', 'C', 'P'),
    allowNull: false,
    comment:   'A=Apertura I=Ingreso E=Egreso D=Diario N=Nomina C=Cheque P=Pago',
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
  // ── Folio real asociado en CONTPAQi tras importar el export ───────────────
  contpaqFolioContado: {
    type:      DataTypes.INTEGER,
    allowNull: true,
  },
  contpaqFolioCredito: {
    type:      DataTypes.INTEGER,
    allowNull: true,
  },
  contpaqAsociadoPor: {
    type:      DataTypes.STRING(150),
    allowNull: true,
  },
  contpaqAsociadoEn: {
    type:      DataTypes.DATE,
    allowNull: true,
  },
  // CFDIs sustitutos (tipoRelacion='04') detectados y excluidos automáticamente
  // del cálculo al generar esta póliza — se listan aparte (no se contabilizan)
  // para que el contador decida caso por caso si ya se contabilizaron en el
  // periodo del CFDI original o si deben incorporarse manualmente.
  sustitutosExcluidos: {
    type:      DataTypes.JSONB,
    allowNull: true,
  },
}, {
  tableName:   'polizas',
  underscored: true,
  indexes: [
    { fields: ['rfc', 'ejercicio', 'periodo'] },
    { fields: ['rfc', 'ejercicio', 'periodo', 'fecha'] }, // cubre ORDER BY fecha DESC
    { fields: ['tipo', 'numero', 'rfc', 'ejercicio', 'periodo'], unique: true },
  ],
});

module.exports = Poliza;
