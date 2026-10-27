'use strict';

/**
 * shared/models/postgres/LoyaltyKoreVentaFallida.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — registro de "procesos fallidos" del webhook Kore→Numo de
 * venta (ver loyalty-kore-venta.service.js). NO es un mecanismo de reintento
 * automático en segundo plano (decisión confirmada con el usuario,
 * 2026-10-26: el reintento real es que Kore reenvíe el mismo POST con el mismo
 * `serieFolio`, que ya es idempotente) — es solo visibilidad y reproceso MANUAL
 * para el equipo de Numo: ver qué vino, por qué falló, y marcarlo resuelto una
 * vez corregida la causa raíz (ej. un concepto/forma de pago sin configurar).
 *
 * Un solo registro "vivo" (resuelto:false) por `serieFolio` — si el mismo
 * serieFolio vuelve a fallar, se incrementa `intentos` en la misma fila en vez
 * de crear una nueva (ver loyalty-venta-fallida.service.js#registrarVentaFallida).
 */

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const LoyaltyKoreVentaFallida = sequelize.define('LoyaltyKoreVentaFallida', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  serieFolio: {
    type:      DataTypes.STRING(100),
    allowNull: false,
    comment:   'Serie-Folio de la venta que falló — NO es unique a nivel de columna: un serieFolio puede tener una fila resuelta vieja y una nueva sin resolver (ver servicio).',
  },
  payloadRecibido: {
    type:      DataTypes.TEXT,
    allowNull: true,
    comment:   'JSON.stringify del body completo recibido de Kore, para diagnóstico.',
  },
  motivo: {
    type:      DataTypes.STRING(300),
    allowNull: false,
  },
  intentos: {
    type:         DataTypes.INTEGER,
    allowNull:    false,
    defaultValue: 1,
  },
  resuelto: {
    type:         DataTypes.BOOLEAN,
    allowNull:    false,
    defaultValue: false,
  },
  resueltoPorUserId: {
    type:      DataTypes.STRING(200),
    allowNull: true,
  },
  resueltoPorNombre: {
    type:      DataTypes.STRING(200),
    allowNull: true,
  },
  resueltoEn: {
    type:      DataTypes.DATE,
    allowNull: true,
  },
}, {
  tableName:   'loyalty_kore_ventas_fallidas',
  underscored: true,
  indexes: [
    { name: 'loyalty_kore_ventas_fallidas_serie_folio', fields: ['serie_folio'] },
    { name: 'loyalty_kore_ventas_fallidas_resuelto', fields: ['resuelto'] },
  ],
});

module.exports = LoyaltyKoreVentaFallida;
