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
  rfcReceptor: {
    type:      DataTypes.STRING(13),
    allowNull: true,
    comment:   'RFC del receptor para reglas intercompañía. null = cualquiera',
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
  claveProdServ: {
    type:      DataTypes.STRING(8),
    allowNull: true,
    comment:   'ClaveProdServ SAT del primer concepto del CFDI. null = cualquiera',
  },
  tipoRelacion: {
    type:      DataTypes.STRING(2),
    allowNull: true,
    comment:   'TipoRelacion del nodo CfdiRelacionados. null = cualquiera',
  },
  relacionadoTipo: {
    type:      DataTypes.STRING(1),
    allowNull: true,
    comment:   'TipoDeComprobante del primer CFDI relacionado (I/E/P, lookup en MongoDB). null = cualquiera. Úsese para distinguir Reg 22B (rel=I anticipo) de Reg 24C (rel=E NC saldo).',
  },
  tasaIva: {
    type:      DataTypes.STRING(6),
    allowNull: true,
    comment:   'Tasa IVA detectada en conceptos: "0"=solo 0%, "16"=solo 16%, "mixto"=ambas. null=cualquiera',
  },
  tieneDescuento: {
    type:      DataTypes.BOOLEAN,
    allowNull: true,
    comment:   'true=solo CFDIs con descuento>0, false=excluye CFDIs con descuento, null=cualquiera',
  },
  conceptoContiene: {
    type:      DataTypes.STRING(200),
    allowNull: true,
    comment:   'Texto que debe contener la descripción del primer concepto (case-insensitive). null=cualquiera',
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
  cuentaIvaAnticipo: {
    type:      DataTypes.STRING(20),
    allowNull: true,
    comment:   'IVA Trasladado Anticipos (2104010002). Cuando está presente el motor cancela esta cuenta (DEBE) y reconoce cuentaIva como definitivo (HABER). Úsese en Reg 22C y Reg 23.',
  },
  cuentaDeltaAnticipo: {
    type:      DataTypes.STRING(20),
    allowNull: true,
    comment:   'Cuenta para el delta (saldo pendiente) cuando total_factura > total_anticipo en Reg 22C. El motor genera DEBE delta en esta cuenta si context.totalRelacionado está disponible.',
  },
  ivaHaber: {
    type:      DataTypes.BOOLEAN,
    allowNull: true,
    comment:   'true = IVA va al HABER aunque sea tipo E (NC correctiva que actúa como ingreso). Úsese en Reg 19.',
  },
  esAplicacionSaldo: {
    type:      DataTypes.BOOLEAN,
    allowNull: true,
    comment:   'true = el motor divide el cargo entre saldo a favor (cuentaCargo) y efectivo (cuentaCargo2) usando context.saldoDisponible. Úsese en Reg 24B/25B.',
  },
  cuentaCargo2: {
    type:      DataTypes.STRING(20),
    allowNull: true,
    comment:   'Cuenta secundaria de cargo para el efectivo/banco cuando parte del pago viene de saldo a favor (esAplicacionSaldo=true).',
  },
  cuentaAbono2: {
    type:      DataTypes.STRING(20),
    allowNull: true,
    comment:   'Cuenta secundaria para porción tasa 0% en reglas mixtas (I→HABER Ingresos0%; E→DEBE Devoluciones0%)',
  },
  cuentaIvaAbono: {
    type:      DataTypes.STRING(20),
    allowNull: true,
    comment:   'Cuenta IVA del lado HABER para NCs con monedero/anticipos (ej. 2104010002 IVA Trasladado Anticipos). Cuando está presente, el HABER de cuentaAbono usa solo subTotal y este campo recibe el IVA separado.',
  },
  cuentaCargoMixto0: {
    type:      DataTypes.STRING(20),
    allowNull: true,
    comment:   'Cuenta CxC tasa 0% en facturas mixtas tipo I PPD (ej. 1103010002 Clientes 0%). Cuando está presente, el cargo principal (cuentaCargo) recibe solo la porción 16%+IVA y este campo recibe el subTotal0% por separado.',
  },
  cuentaDescuento: {
    type:      DataTypes.STRING(20),
    allowNull: true,
    comment:   'Cuenta Descuentos s/Ventas 16% — se agrega como DEBE en reglas con tieneDescuento=true',
  },
  cuentaDescuento0: {
    type:      DataTypes.STRING(20),
    allowNull: true,
    comment:   'Cuenta Descuentos s/Ventas 0% — solo en Reg 16 (mixto+descuento)',
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
