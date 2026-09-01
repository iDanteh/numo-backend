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
  cuentaAnteriorId: {
    type:       DataTypes.INTEGER,
    allowNull:  true,
    references: { model: 'account_plans', key: 'id' },
    comment:    'Cuenta antes del cruce banco-real/reemplazo manual (ver poliza.service.js) — se restaura al revertir la póliza a borrador y se limpia después.',
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
  // Ticket real de cajas (serieVenta/folioVenta) al que pertenece esta
  // porción del cobro — distinto de `serie` (serie-folio del CFDI propio,
  // que en una Factura Global es la factura completa, no el ticket
  // individual). Solo se llena en líneas de Tarjeta partidas por el
  // desglose real de cobro (ver `_prefetchAjustesFacturaPropia`/
  // `splitPorFormaPagoReal`) — permite resolver la autorización bancaria
  // real POR TICKET al exportar, sin mezclar tickets de una misma Global
  // (ver `construirAutorizacionTarjetaPorTicket`, poliza.service.js).
  serieVentaTicket: {
    type:      DataTypes.STRING(25),
    allowNull: true,
  },
  folioVentaTicket: {
    type:      DataTypes.STRING(40),
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
  // uuid real de la FACTURA que este Pago liquida (SAT IdDocumento del
  // doctoRelacionado) — distinto de `cfdiUuid` (uuid del Pago/Complemento).
  // Solo se llena en líneas de Cobranza partidas por factura (ver
  // `cobranza-poliza-generator.service.js`): bank_movements.erpLinks.
  // folioFiscal se liga al uuid de la factura original, no al del Pago
  // (confirmado con datos reales 2026-09-01).
  facturaUuid: {
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
  tipoOrigen: {
    type:      DataTypes.STRING(100),
    allowNull: true,
    comment:   'Clasificación de negocio del ERP: Venta, Bonificación, Devolución, Pago, etc.',
  },
  // ── Trazabilidad de la regla de mapeo usada ──────────────────────────────
  reglaId: {
    type:       DataTypes.INTEGER,
    allowNull:  true,
    references: { model: 'cfdi_mapping_rules', key: 'id' },
    onDelete:   'SET NULL',
    comment:    'FK a la regla de mapeo usada al generar este movimiento',
  },
  reglaNombre: {
    type:      DataTypes.STRING(200),
    allowNull: true,
    comment:   'Nombre de la regla al momento de generar (snapshot histórico)',
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
