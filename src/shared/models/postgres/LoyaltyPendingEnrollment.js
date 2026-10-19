'use strict';

/**
 * shared/models/postgres/LoyaltyPendingEnrollment.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — migración de clientes legados ("Club Tuberos") que NO
 * traen ningún teléfono (ni Celular ni Telefono en clientes.csv). Decisión de
 * negocio 2026-10-19: estos clientes NO se descartan ni se les crea una
 * `LoyaltyCard` con un teléfono falso/null — eso violaría D1 (el teléfono es
 * la única llave de identidad de la tarjeta, `allowNull:false` + `unique` en
 * LoyaltyCard.js, nunca se toca ese contrato). En su lugar quedan en esta
 * tabla de espera hasta que el administrador les consiga/actualice un
 * teléfono real (ver `loyalty-import-apply.service.js#completarRegistroPendiente`,
 * que crea la `LoyaltyCard` definitiva y acredita `saldoLegado` en ese momento).
 */

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const LoyaltyPendingEnrollment = sequelize.define('LoyaltyPendingEnrollment', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  nombre: {
    type:      DataTypes.STRING(200),
    allowNull: false,
    comment:   'Nombre del cliente dueño del registro legado.',
  },
  correo: {
    type:      DataTypes.STRING(200),
    allowNull: true,
    comment:   'Informativo únicamente (para que el admin intente contactarlo) — no es llave de identidad, ningún flujo de login/búsqueda lo usa.',
  },
  tarjetaLegada: {
    type:      DataTypes.STRING(50),
    allowNull: true,
    comment:   'Tarjeta original del reporte legado (clientes.csv/puntos.pdf), para trazabilidad/auditoría.',
  },
  saldoLegado: {
    type:         DataTypes.DECIMAL(12, 2),
    allowNull:    false,
    defaultValue: 0,
    comment:      'Saldo de puntos legado a acreditar (vía MIGRACION) en cuanto se complete el registro con un teléfono real.',
  },
  nivelSugerido: {
    type:         DataTypes.STRING(10),
    allowNull:    false,
    defaultValue: 'Bronce',
    validate:     { isIn: [['Bronce', 'Plata', 'Oro']] },
    comment:      'Nivel con el que se creará la tarjeta al completar el registro (mismo criterio que LoyaltyCard.nivel).',
  },
  estado: {
    type:         DataTypes.STRING(20),
    allowNull:    false,
    defaultValue: 'pendiente',
    // 'rechazado' (2026-10-19, pedido explícito del usuario): el admin puede decidir que un
    // cliente legado sin teléfono ya no se va a migrar — nunca se borra la fila (trazabilidad),
    // solo deja de aparecer en listarPendientes (que sigue filtrando where:{estado:'pendiente'}).
    validate:     { isIn: [['pendiente', 'completado', 'rechazado']] },
  },
  resueltoPorId: {
    type:      DataTypes.STRING(200),
    allowNull: true,
    comment:   'Quién completó o rechazó este registro (createdById/Nombre es quién lo IMPORTÓ, no quién lo resolvió).',
  },
  resueltoPorNombre: {
    type:      DataTypes.STRING(200),
    allowNull: true,
  },
  resueltoAt: {
    type:      DataTypes.DATE,
    allowNull: true,
  },
  cardIdResultante: {
    type:       DataTypes.INTEGER,
    allowNull:  true,
    references: { model: 'loyalty_cards', key: 'id' },
    onDelete:   'SET NULL',
    comment:    'Se llena al completar el registro (completarRegistroPendiente) — la tarjeta real ya creada para este cliente.',
  },
  createdById: {
    type:      DataTypes.STRING(200),
    allowNull: true,
  },
  createdByNombre: {
    type:      DataTypes.STRING(200),
    allowNull: true,
  },
}, {
  tableName:   'loyalty_pending_enrollments',
  underscored: true,
});

module.exports = LoyaltyPendingEnrollment;
