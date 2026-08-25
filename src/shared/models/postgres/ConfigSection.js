'use strict';

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

/**
 * ConfigSection.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Catálogo de "Configuraciones Globales" (runtime, ver global-config.service.js) —
 * cada fila es una sección temática (ej. `erp-caja`) que agrupa uno o más
 * `GlobalConfig`. `modulosAfectados` es la lista explícita de qué módulos/acciones
 * de la app consumen esta sección — se muestra en la UI de administración ANTES
 * de los valores editables, para que quien edite sepa qué puede romper.
 *
 * Catálogo relacional ESTRICTO (confirmado con el usuario): `GlobalConfig.sectionId`
 * es una FK real contra esta tabla, no una etiqueta de texto libre.
 */
const ConfigSection = sequelize.define('ConfigSection', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  clave: {
    type:      DataTypes.STRING(100),
    allowNull: false,
    unique:    true,
  },
  nombre: {
    type:      DataTypes.STRING(255),
    allowNull: false,
  },
  descripcion: {
    type:      DataTypes.TEXT,
    allowNull: true,
  },
  modulosAfectados: {
    type:         DataTypes.ARRAY(DataTypes.STRING(255)),
    allowNull:    false,
    defaultValue: [],
  },
}, {
  tableName:   'config_sections',
  underscored: true,
});

module.exports = ConfigSection;
