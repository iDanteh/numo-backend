'use strict';

/**
 * shared/models/postgres/CierreMesHistorico.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Historial de reportes de Cierre de Mes generados (uno por cada vez que se
 * presiona "Cerrar mes" en el Dashboard — ver periodoFiscal.controller.js
 * `cerrar()`). PeriodoFiscal solo guarda el ESTADO actual (cerrado/no cerrado,
 * último cerradoEn/cerradoPorId); esta tabla guarda cada evento de cierre por
 * separado (incluyendo el .xlsx generado en ese momento) para poder listarlos
 * y redescargarlos aunque el período se haya revertido y vuelto a cerrar.
 */

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const CierreMesHistorico = sequelize.define('CierreMesHistorico', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  /** FK al período — puede quedar NULL si el período se llegó a eliminar */
  periodoFiscalId: {
    type:       DataTypes.INTEGER,
    allowNull:  true,
    references: { model: 'periodos_fiscales', key: 'id' },
    onDelete:   'SET NULL',
  },
  ejercicio: {
    type:      DataTypes.INTEGER,
    allowNull: false,
  },
  periodo: {
    type:      DataTypes.INTEGER,
    allowNull: true,
  },
  /** Etiqueta legible del período al momento del cierre (ej. "Septiembre 2026") */
  periodoLabel: {
    type:      DataTypes.STRING(100),
    allowNull: true,
  },
  rfcEmisor: {
    type:      DataTypes.STRING(13),
    allowNull: true,
  },
  filename: {
    type:      DataTypes.STRING(255),
    allowNull: false,
  },
  /** Contenido binario del .xlsx generado en ese cierre */
  fileData: {
    type:      DataTypes.BLOB,
    allowNull: false,
  },
  fileSize: {
    type:      DataTypes.INTEGER,
    allowNull: false,
  },
  /** FK al usuario que realizó este cierre en particular */
  cerradoPorId: {
    type:       DataTypes.INTEGER,
    allowNull:  true,
    references: { model: 'users', key: 'id' },
    onDelete:   'SET NULL',
  },
}, {
  tableName:   'cierres_mes_historico',
  underscored: true,
});

module.exports = CierreMesHistorico;
