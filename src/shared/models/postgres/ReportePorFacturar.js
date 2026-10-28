'use strict';

/**
 * shared/models/postgres/ReportePorFacturar.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Historial del reporte "Por facturar" (Reportes, 2026-10-22): cada vez que
 * alguien lo genera (por día o rango de fechas, por sucursal) se guarda aquí
 * con quién lo creó y los datos tal como salieron en ese momento — mismas
 * tablas que la pestaña MOSTRADOR de la póliza (ver mostrador-hoja.js).
 */

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const ReportePorFacturar = sequelize.define('ReportePorFacturar', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  rfc: {
    type:      DataTypes.STRING(13),
    allowNull: false,
  },
  /** NULL = todas las sucursales */
  centroCostoId: {
    type:      DataTypes.INTEGER,
    allowNull: true,
  },
  /** Nombre de la sucursal al momento de generarlo ("Todas" si no hay centro) */
  sucursal: {
    type:      DataTypes.STRING(100),
    allowNull: true,
  },
  fechaInicio: {
    type:      DataTypes.DATEONLY,
    allowNull: false,
  },
  fechaFin: {
    type:      DataTypes.DATEONLY,
    allowNull: false,
  },
  /** { movimientosMostrador, depositosPendientesFactura } — ver mostrador.service.js */
  datos: {
    type:      DataTypes.JSONB,
    allowNull: false,
  },
  /** Conteos y totales por tabla, para el historial sin cargar `datos` */
  resumen: {
    type:      DataTypes.JSONB,
    allowNull: true,
  },
  advertencias: {
    type:      DataTypes.JSONB,
    allowNull: true,
  },
  creadoPorId: {
    type:       DataTypes.INTEGER,
    allowNull:  true,
    references: { model: 'users', key: 'id' },
    onDelete:   'SET NULL',
  },
  /**
   * Eliminado (2026-10-22, pedido del usuario): borrado lógico — el reporte
   * ya no se ve en el historial, pero queda guardado quién y cuándo lo quitó.
   */
  eliminadoPorId: {
    type:       DataTypes.INTEGER,
    allowNull:  true,
    references: { model: 'users', key: 'id' },
    onDelete:   'SET NULL',
  },
  eliminadoEn: {
    type:      DataTypes.DATE,
    allowNull: true,
  },
  /** Botón "Actualizar" (2026-10-23): último recálculo del mismo reporte. */
  actualizadoPorId: {
    type:       DataTypes.INTEGER,
    allowNull:  true,
    references: { model: 'users', key: 'id' },
    onDelete:   'SET NULL',
  },
  actualizadoEn: {
    type:      DataTypes.DATE,
    allowNull: true,
  },
}, {
  tableName:   'reportes_por_facturar',
  underscored: true,
});

module.exports = ReportePorFacturar;
