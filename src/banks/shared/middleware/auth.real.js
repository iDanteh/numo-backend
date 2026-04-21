'use strict';
// Re-exporta el middleware de autenticación/autorización compartido.
// 'authorize' se mantiene como alias de 'permit' para compatibilidad.
const { authenticate, permit } = require('../../../shared/middleware/auth');

module.exports = { authenticate, authorize: permit, permit };
