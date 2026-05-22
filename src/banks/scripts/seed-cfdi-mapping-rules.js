'use strict';

/**
 * seed-cfdi-mapping-rules.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Carga masiva de las 40 reglas de mapeo CFDI → cuentas contables.
 * (Reglas 2A–2E y 6-IC de intercompañía requieren campo rfcReceptor en el
 *  modelo — no se incluyen aquí.)
 * (Reglas 24B, 24C, 25B, 25C de aplicación de saldo requieren lógica
 *  multi-paso en el motor — no tienen discriminador CFDI propio.)
 *
 * Uso local:
 *   node src/banks/scripts/seed-cfdi-mapping-rules.js
 *   node src/banks/scripts/seed-cfdi-mapping-rules.js --force   (sobreescribe)
 *
 * Uso en Docker:
 *   docker exec numo-backend node src/banks/scripts/seed-cfdi-mapping-rules.js
 */

require('dotenv').config();

const { sequelize }      = require('../../config/database.postgres');
const CfdiMappingRule    = require('../../shared/models/postgres/CfdiMappingRule');

// ── Catálogo de cuentas referencia (formato 10 dígitos sin guiones) ──────────
// 1101010003  Caja por identificar
// 1102011005  Bancos por identificar
// 1103010001  Clientes Nac Gral 16%
// 1103010002  Clientes Nac Gral 0%
// 2103010001  Anticipos De Clientes General
// 2103090001  Anticipos Otros (saldo a favor pendientes de aplicar)
// 2103090002  Anticipos Otros Clientes Club Tuberos (monedero electrónico)
// 2104010001  IVA Trasladado (causado definitivo)
// 2104010002  IVA Trasladado – Anticipos (diferido al recibir el anticipo/monedero)
// 2105010001  IVA Por Trasladar PPD (cuenta puente crédito)
// 4100010001  Ingresos Por Ventas Contado 16%
// 4100010002  Ingresos Por Ventas Contado 0%
// 4100020001  Ingresos Por Ventas Crédito 16%
// 4200010001  Devoluciones s/Ventas 16%
// 4200010002  Devoluciones s/Ventas 0%
// 4200020001  Descuentos s/Ventas 16%
// 4200020002  Descuentos s/Ventas 0%

const reglas = [

  // ── 0. ANTICIPOS (Regla 22) ────────────────────────────────────────────────
  // Prioridad 9 — antes que 1A–1E para que el claveProdServ gane.
  {
    nombre:          'Reg 22 — Recepción de Anticipo (ClaveProdServ 84111506)',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    claveProdServ:   '84111506',
    cuentaCargo:     '1102011005',   // Bancos (dinero recibido)
    cuentaAbono:     '2103010001',   // Anticipos de Clientes General
    cuentaIva:       '2104010002',   // IVA Trasladado – Anticipos
    prioridad:       9,
  },

  // ── 1. INGRESOS PUE 16% (Reglas 1A–1F) ────────────────────────────────────
  // Prioridades 10–14. Venta de contado, IVA causado al momento de emitir.
  // tasaIva='16' + tieneDescuento=false: excluyen facturas con descuento (→ Reg 14)
  // y facturas 0%/mixtas (→ Reg 10/12).
  {
    nombre:          'Reg 1A — Venta PUE Efectivo 16%',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       '01',
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '1101010003',   // Caja
    cuentaAbono:     '4100010001',   // Ingresos Contado 16%
    cuentaIva:       '2104010001',   // IVA Trasladado
    prioridad:       10,
  },
  {
    nombre:          'Reg 1B — Venta PUE Transferencia 16%',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       '03',
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '1102011005',   // Bancos
    cuentaAbono:     '4100010001',
    cuentaIva:       '2104010001',
    prioridad:       11,
  },
  {
    nombre:          'Reg 1C — Venta PUE Cheque 16%',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       '04',
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '1102011005',
    cuentaAbono:     '4100010001',
    cuentaIva:       '2104010001',
    prioridad:       12,
  },
  {
    nombre:          'Reg 1F — Venta PUE Cheque Nominativo 16%',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       '02',
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '1102011005',
    cuentaAbono:     '4100010001',
    cuentaIva:       '2104010001',
    prioridad:       12,
  },
  {
    nombre:          'Reg 1D — Venta PUE Tarjeta Débito 16%',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       '28',
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '1102011005',
    cuentaAbono:     '4100010001',
    cuentaIva:       '2104010001',
    prioridad:       13,
  },
  {
    nombre:          'Reg 1E — Venta PUE Tarjeta Crédito 16%',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       '29',
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '1102011005',
    cuentaAbono:     '4100010001',
    cuentaIva:       '2104010001',
    prioridad:       14,
  },

  // ── 1B. MONEDERO ELECTRÓNICO CLUB TUBEROS (Reglas 1G, 10B) ───────────────
  // formaPago=05 (monedero electrónico). El saldo del monedero vive en 2103090002.
  // Asiento estándar de ingreso PUE: cargo consume el saldo del monedero (total),
  // IVA va al HABER de 2104010002 (IVA Trasladado Anticipos), abono a Ingresos.
  // Reg 1G: sin tasaIva porque los CFDIs de monedero pueden no tener IVA
  // desglosado por concepto en MongoDB (solo viene en el header).
  // _detectTasaIva devuelve null en ese caso y tasaIva='16' no matchearía.
  // Reg 10B tiene tasaIva='0' (más específica) → gana en empate por spec cuando
  // el CFDI sí tiene conceptos con tasa 0%.
  {
    nombre:          'Reg 1G — Venta PUE Monedero Electrónico (Club Tuberos)',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       '05',
    // tasaIva: null — captura 16%, null y mixto; Reg 10B cubre el 0% con más spec
    cuentaCargo:     '2103090002',  // Anticipos Otros Club Tuberos (consume saldo monedero)
    cuentaAbono:     '4100010001',  // Ingresos Contado 16%
    cuentaIva:       '2104010001',  // IVA Trasladado definitivo (PUE: IVA causado al momento de la venta)
    prioridad:       10,
  },
  {
    nombre:          'Reg 10B — Venta PUE Monedero Electrónico Tasa 0% (Club Tuberos)',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       '05',
    tasaIva:         '0',           // más específica que Reg 1G → gana en empate de prioridad
    cuentaCargo:     '2103090002',  // Anticipos Otros Club Tuberos
    cuentaAbono:     '4100010002',  // Ingresos Contado 0%
    cuentaIva:       null,          // sin IVA (tasa 0%)
    prioridad:       10,            // mismo número que 1G; spec mayor gana (tiene tasaIva)
  },

  // ── 2. IVA TASA 0% — PUE (Regla 10) ──────────────────────────────────────
  // Prioridad 15. Exportaciones, alimentos, medicamentos. Sin IVA.
  {
    nombre:          'Reg 10 — Venta PUE Tasa 0%',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       null,
    tasaIva:         '0',
    tieneDescuento:  false,
    cuentaCargo:     '1102011005',
    cuentaAbono:     '4100010002',  // Ingresos Contado 0%
    cuentaIva:       null,
    prioridad:       15,
  },

  // ── 3. CFDI MIXTO PUE (0%+16%) — Regla 12 ──────────────────────────────
  // Prioridad 16. Motor agrega partida Ingresos 0% (cuentaAbono2) por subtotal exento.
  {
    nombre:          'Reg 12 — Venta Mixta PUE (0%+16%)',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       null,
    tasaIva:         'mixto',
    tieneDescuento:  false,
    cuentaCargo:     '1102011005',
    cuentaAbono:     '4100010001',  // Ingresos 16% (partida principal)
    cuentaAbono2:    '4100010002',  // Ingresos 0% (partida adicional — motor)
    cuentaIva:       '2104010001',
    prioridad:       16,
  },

  // ── 4. DESCUENTOS PUE 16% (Regla 14) ─────────────────────────────────────
  // Prioridad 17. Motor agrega línea Descuentos s/Ventas 16% (cuentaDescuento).
  {
    nombre:          'Reg 14 — Venta con Descuento PUE 16%',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       null,
    tasaIva:         '16',
    tieneDescuento:  true,
    cuentaCargo:     '1102011005',
    cuentaAbono:     '4100010001',
    cuentaIva:       '2104010001',
    cuentaDescuento: '4200020001',  // Descuentos s/Ventas 16%
    prioridad:       17,
  },

  // ── 5. DESCUENTOS PUE 0% (Regla 15) ──────────────────────────────────────
  {
    nombre:          'Reg 15 — Venta con Descuento PUE Tasa 0%',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       null,
    tasaIva:         '0',
    tieneDescuento:  true,
    cuentaCargo:     '1102011005',
    cuentaAbono:     '4100010002',  // Ingresos 0%
    cuentaIva:       null,
    cuentaDescuento: '4200020002',  // Descuentos s/Ventas 0%
    prioridad:       18,
  },

  // ── 6. DESCUENTOS MIXTO (Regla 16) ───────────────────────────────────────
  // Prioridad 19. Mixto con descuentos en ambas tasas.
  {
    nombre:          'Reg 16 — Venta con Descuento Mixto PUE (0%+16%)',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       null,
    tasaIva:         'mixto',
    tieneDescuento:  true,
    cuentaCargo:     '1102011005',
    cuentaAbono:     '4100010001',
    cuentaAbono2:    '4100010002',  // Ingresos 0% (motor)
    cuentaIva:       '2104010001',
    cuentaDescuento:  '4200020001',  // Descuentos s/Ventas 16%
    cuentaDescuento0: '4200020002',  // Descuentos s/Ventas 0%
    prioridad:       19,
  },

  // ── 7. FACTURA FINAL ANTICIPO PUE (Regla 22C) ────────────────────────────
  {
    nombre:              'Reg 22C — Factura Final Anticipo PUE (formaPago 30)',
    tipoComprobante:     'I',
    metodoPago:          'PUE',
    formaPago:           '30',
    cuentaCargo:         '2103010001',  // Anticipos de Clientes General (subtotal — cancela pasivo)
    cuentaAbono:         '4100010001',  // Ingresos Contado 16%
    cuentaIva:           '2104010001',  // IVA Trasladado definitivo (HABER)
    cuentaIvaAnticipo:   '2104010002',  // IVA Trasladado Anticipos (DEBE — cancela diferido)
    cuentaDeltaAnticipo: '1102011005',  // Bancos (5° mov: cash por saldo > anticipo, si aplica)
    prioridad:           12,
  },

  // ── 7B. FACTURA FINAL ANTICIPO PPD (Regla 22B) ───────────────────────────
  // relacionadoTipo='I': el CFDI relacionado es el anticipo (tipo I, claveProdServ=84111506).
  // Distingue de Reg 24C donde el relacionado es una NC (tipo E).
  {
    nombre:          'Reg 22B — Factura Final Anticipo PPD (TipoRelacion 07)',
    tipoComprobante: 'I',
    metodoPago:      'PPD',
    tipoRelacion:    '07',
    relacionadoTipo: 'I',            // CFDI relacionado = anticipo (tipo Ingreso)
    cuentaCargo:     '1103010001',   // Clientes Nac Gral 16%
    cuentaAbono:     '4100020001',   // Ingresos Crédito 16%
    cuentaIvaPPD:    '2105010001',   // IVA Por Trasladar PPD (diferido)
    prioridad:       21,
  },

  // ── 7D. APLICACIÓN DE SALDO A FAVOR PPD (Reglas 24C, 25C) ────────────────
  // relacionadoTipo='E': el CFDI relacionado es la NC de saldo a favor (tipo E).
  // Motor esAplicacionSaldo: divide cargo entre saldo (2103090001) y CxC (1103010001).
  {
    nombre:            'Reg 24C — Aplicación Saldo a Favor 16% PPD',
    tipoComprobante:   'I',
    metodoPago:        'PPD',
    tipoRelacion:      '07',
    relacionadoTipo:   'E',          // CFDI relacionado = NC saldo a favor (tipo Egreso)
    tasaIva:           '16',
    esAplicacionSaldo: true,
    cuentaCargo:       '2103090001',  // Anticipos Otros (saldo a favor)
    cuentaCargo2:      '1103010001',  // Clientes (CxC residual)
    cuentaAbono:       '4100020001',  // Ingresos Crédito 16%
    cuentaIvaPPD:      '2105010001',  // IVA Por Trasladar PPD
    prioridad:         22,
  },
  {
    nombre:            'Reg 25C — Aplicación Saldo a Favor Tasa 0% PPD',
    tipoComprobante:   'I',
    metodoPago:        'PPD',
    tipoRelacion:      '07',
    relacionadoTipo:   'E',
    tasaIva:           '0',
    esAplicacionSaldo: true,
    cuentaCargo:       '2103090001',
    cuentaCargo2:      '1103010001',
    cuentaAbono:       '4100010002',  // Ingresos Contado 0%
    cuentaIvaPPD:      null,
    prioridad:         22,
  },

  // ── 7C. APLICACIÓN DE SALDO A FAVOR PUE (Reglas 24B, 25B) ───────────────
  // Cuando el cliente tiene saldo a favor (2103090001) y lo aplica contra una nueva
  // factura PUE con tipoRelacion=07. El motor divide el cargo:
  //   DEBE 2103090001 = min(saldo, total)   — consume el saldo a favor
  //   DEBE 1102011005 = total - saldo       — cash residual (si hay)
  //   IVA / Ingresos = igual que PUE normal.
  // Reg 22C (prio 12) gana cuando formaPago=30; estas reglas (prio 20) capturan
  // el resto (formaPago=01/03/28/29…) con tipoRelacion=07.
  // Reg 24C/25C (PPD) se omiten: tipoRelacion=07+PPD ya lo maneja Reg 22B (prio 21).
  {
    nombre:            'Reg 24B — Aplicación Saldo a Favor 16% PUE',
    tipoComprobante:   'I',
    metodoPago:        'PUE',
    tipoRelacion:      '07',
    tasaIva:           '16',
    esAplicacionSaldo: true,
    cuentaCargo:       '2103090001',  // Anticipos Otros (saldo a favor — DEBE saldo aplicado)
    cuentaCargo2:      '1102011005',  // Bancos (DEBE cash residual)
    cuentaAbono:       '4100010001',  // Ingresos Contado 16%
    cuentaIva:         '2104010001',  // IVA Trasladado
    prioridad:         20,
  },
  {
    nombre:            'Reg 25B — Aplicación Saldo a Favor Tasa 0% PUE',
    tipoComprobante:   'I',
    metodoPago:        'PUE',
    tipoRelacion:      '07',
    tasaIva:           '0',
    esAplicacionSaldo: true,
    cuentaCargo:       '2103090001',  // Anticipos Otros (saldo a favor)
    cuentaCargo2:      '1102011005',  // Bancos (cash residual)
    cuentaAbono:       '4100010002',  // Ingresos Contado 0%
    cuentaIva:         null,
    prioridad:         20,
  },

  // ── 8. INGRESOS PPD 16% — Factura a Crédito (Regla 6) ────────────────────
  // Prioridad 60. tasaIva='16' + tieneDescuento=false: excluye facturas con descuento (→ Reg 6B).
  {
    nombre:          'Reg 6 — Venta PPD 16% (Factura a Crédito)',
    tipoComprobante: 'I',
    metodoPago:      'PPD',
    formaPago:       '99',
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '1103010001',  // Clientes Nac Gral 16%
    cuentaAbono:     '4100020001',  // Ingresos Crédito 16%
    cuentaIvaPPD:    '2105010001',  // IVA Por Trasladar PPD (diferido)
    prioridad:       60,
  },

  // ── 8B. DESCUENTOS PPD 16% (Regla 6B) ────────────────────────────────────
  // Prioridad 59 (gana sobre Reg 6). Motor agrega línea Descuentos s/Ventas 16%.
  {
    nombre:          'Reg 6B — Venta con Descuento PPD 16%',
    tipoComprobante: 'I',
    metodoPago:      'PPD',
    formaPago:       '99',
    tasaIva:         '16',
    tieneDescuento:  true,
    cuentaCargo:     '1103010001',   // Clientes Nac Gral 16%
    cuentaAbono:     '4100020001',   // Ingresos Crédito 16%
    cuentaIvaPPD:    '2105010001',   // IVA Por Trasladar PPD (diferido)
    cuentaDescuento: '4200020001',   // Descuentos s/Ventas 16%
    prioridad:       59,
  },

  // ── 8. IVA TASA 0% — PPD (Regla 11) ──────────────────────────────────────
  // Prioridad 65. tasaIva='0' + tieneDescuento=false: excluye facturas con descuento (→ Reg 6C).
  {
    nombre:          'Reg 11 — Venta PPD Tasa 0%',
    tipoComprobante: 'I',
    metodoPago:      'PPD',
    formaPago:       '99',
    tasaIva:         '0',
    tieneDescuento:  false,
    cuentaCargo:     '1103010002',  // Clientes Nac Gral 0%
    cuentaAbono:     '4100010002',  // Ingresos Contado 0%
    cuentaIvaPPD:    null,
    prioridad:       65,
  },

  // ── 8C. DESCUENTOS PPD 0% (Regla 6C) ─────────────────────────────────────
  // Prioridad 64 (gana sobre Reg 11). Motor agrega línea Descuentos s/Ventas 0%.
  {
    nombre:          'Reg 6C — Venta con Descuento PPD 0%',
    tipoComprobante: 'I',
    metodoPago:      'PPD',
    formaPago:       '99',
    tasaIva:         '0',
    tieneDescuento:  true,
    cuentaCargo:     '1103010002',   // Clientes Nac Gral 0%
    cuentaAbono:     '4100010002',   // Ingresos Contado 0%
    cuentaIvaPPD:    null,
    cuentaDescuento: '4200020002',   // Descuentos s/Ventas 0%
    prioridad:       64,
  },

  // ── 9. CFDI MIXTO PPD (Regla 13) ─────────────────────────────────────────
  // Prioridad 66. Solo el IVA 16% se difiere; la porción 0% no genera IVA.
  // tieneDescuento=false: CFDIs mixtos con descuento deben manejarse manualmente.
  {
    nombre:          'Reg 13 — Venta Mixta PPD (0%+16%)',
    tipoComprobante: 'I',
    metodoPago:      'PPD',
    formaPago:       '99',
    tasaIva:         'mixto',
    tieneDescuento:  false,
    cuentaCargo:     '1103010001',  // Clientes 16%
    cuentaAbono:     '4100020001',  // Ingresos Crédito 16%
    cuentaAbono2:    '4100010002',  // Ingresos 0% (motor)
    cuentaIvaPPD:    '2105010001',
    prioridad:       66,
  },

  // ── 10. COBROS PPD — COMPLEMENTO DE PAGO (Reglas 7A–7F) ──────────────────
  // tipoComprobante = P. Reconoce IVA definitivo y liquida la CxC.
  {
    nombre:          'Reg 7A — Cobro PPD Efectivo',
    tipoComprobante: 'P',
    formaPago:       '01',
    cuentaCargo:     '1101010003',  // Caja
    cuentaAbono:     '1103010001',  // Clientes (liquida CxC)
    cuentaIva:       '2104010001',  // IVA causado definitivo
    cuentaIvaPPD:    '2105010001',  // cancela cuenta puente
    prioridad:       70,
  },
  {
    nombre:          'Reg 7B — Cobro PPD Transferencia',
    tipoComprobante: 'P',
    formaPago:       '03',
    cuentaCargo:     '1102011005',
    cuentaAbono:     '1103010001',
    cuentaIva:       '2104010001',
    cuentaIvaPPD:    '2105010001',
    prioridad:       71,
  },
  {
    nombre:          'Reg 7C — Cobro PPD Cheque',
    tipoComprobante: 'P',
    formaPago:       '04',
    cuentaCargo:     '1102011005',
    cuentaAbono:     '1103010001',
    cuentaIva:       '2104010001',
    cuentaIvaPPD:    '2105010001',
    prioridad:       72,
  },
  {
    nombre:          'Reg 7F — Cobro PPD Cheque Nominativo',
    tipoComprobante: 'P',
    formaPago:       '02',
    cuentaCargo:     '1102011005',
    cuentaAbono:     '1103010001',
    cuentaIva:       '2104010001',
    cuentaIvaPPD:    '2105010001',
    prioridad:       72,
  },
  {
    nombre:          'Reg 7D — Cobro PPD Tarjeta Débito',
    tipoComprobante: 'P',
    formaPago:       '28',
    cuentaCargo:     '1102011005',
    cuentaAbono:     '1103010001',
    cuentaIva:       '2104010001',
    cuentaIvaPPD:    '2105010001',
    prioridad:       73,
  },
  {
    nombre:          'Reg 7E — Cobro PPD Tarjeta Crédito',
    tipoComprobante: 'P',
    formaPago:       '29',
    cuentaCargo:     '1102011005',
    cuentaAbono:     '1103010001',
    cuentaIva:       '2104010001',
    cuentaIvaPPD:    '2105010001',
    prioridad:       74,
  },

  // ── 10B. COBRO PPD VÍA MONEDERO CLUB TUBEROS (Regla 7G) ──────────────────
  // Tipo P, formaPago=05. Idéntico a Reg 7A–7F pero usando el saldo del monedero
  // (2103090002) en lugar de Bancos/Caja. Motor estándar tipo P:
  //   DEBE 2103090002 (aplica saldo), HABER 1103010001 (liquida CxC),
  //   DEBE 2105010001 (cancela IVA diferido), HABER 2104010001 (IVA definitivo).
  {
    nombre:       'Reg 7G — Cobro PPD Monedero Electrónico (Club Tuberos)',
    tipoComprobante: 'P',
    formaPago:    '05',
    cuentaCargo:  '2103090002',  // Anticipos Otros Club Tuberos (aplica saldo monedero)
    cuentaAbono:  '1103010001',  // Clientes (liquida CxC)
    cuentaIva:    '2104010001',  // IVA Trasladado definitivo (HABER)
    cuentaIvaPPD: '2105010001',  // IVA Por Trasladar PPD (DEBE — cancela diferido)
    prioridad:    70,
  },

  // ── 11. NOTAS DE CRÉDITO PUE (Reglas 8A–8F) ──────────────────────────────
  // tipoComprobante = E + tipoRelacion=01 + formaPago específica.
  // tasaIva='16' + tieneDescuento=false: no compiten con Reg 17-19 (tasa 0%/mixto/descuento).
  {
    nombre:          'Reg 8A — NC PUE Devolución Efectivo',
    tipoComprobante: 'E',
    metodoPago:      'PUE',
    formaPago:       '01',
    tipoRelacion:    '01',
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '4200010001',  // Devoluciones s/Ventas 16%
    cuentaAbono:     '1101010003',  // Caja (salida de dinero)
    cuentaIva:       '2104010001',  // IVA a cancelar
    prioridad:       80,
  },
  {
    nombre:          'Reg 8B — NC PUE Devolución Transferencia',
    tipoComprobante: 'E',
    metodoPago:      'PUE',
    formaPago:       '03',
    tipoRelacion:    '01',
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '4200010001',
    cuentaAbono:     '1102011005',
    cuentaIva:       '2104010001',
    prioridad:       81,
  },
  {
    nombre:          'Reg 8C — NC PUE Devolución Cheque',
    tipoComprobante: 'E',
    metodoPago:      'PUE',
    formaPago:       '04',
    tipoRelacion:    '01',
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '4200010001',
    cuentaAbono:     '1102011005',
    cuentaIva:       '2104010001',
    prioridad:       82,
  },
  {
    nombre:          'Reg 8F — NC PUE Devolución Cheque Nominativo',
    tipoComprobante: 'E',
    metodoPago:      'PUE',
    formaPago:       '02',
    tipoRelacion:    '01',
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '4200010001',
    cuentaAbono:     '1102011005',
    cuentaIva:       '2104010001',
    prioridad:       82,
  },
  {
    nombre:          'Reg 8D — NC PUE Devolución Tarjeta Débito',
    tipoComprobante: 'E',
    metodoPago:      'PUE',
    formaPago:       '28',
    tipoRelacion:    '01',
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '4200010001',
    cuentaAbono:     '1102011005',
    cuentaIva:       '2104010001',
    prioridad:       83,
  },
  {
    nombre:          'Reg 8E — NC PUE Devolución Tarjeta Crédito',
    tipoComprobante: 'E',
    metodoPago:      'PUE',
    formaPago:       '29',
    tipoRelacion:    '01',
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '4200010001',
    cuentaAbono:     '1102011005',
    cuentaIva:       '2104010001',
    prioridad:       84,
  },

  // ── 12. DEVOLUCIONES NC POR TASA (Reglas 17–19) ───────────────────────────
  // Fallback para E+tipoRelacion=01 sin formaPago conocida.
  // tasaIva discrimina la ruta: 0% → Reg 17, mixto → Reg 18, descuento → Reg 19.
  {
    nombre:          'Reg 17 — NC Devolución Tasa 0%',
    tipoComprobante: 'E',
    tipoRelacion:    '01',
    tasaIva:         '0',
    tieneDescuento:  false,         // NCs 0% con descuento → Reg 19B
    cuentaCargo:     '4200010002',  // Devoluciones s/Ventas 0%
    cuentaAbono:     '1102011005',  // Bancos
    cuentaIva:       null,
    prioridad:       85,
  },
  {
    nombre:          'Reg 18 — NC Devolución Mixta (0%+16%)',
    tipoComprobante: 'E',
    tipoRelacion:    '01',
    tasaIva:         'mixto',
    cuentaCargo:     '4200010001',  // Devoluciones s/Ventas 16% (partida principal)
    cuentaAbono:     '1102011005',  // Bancos
    cuentaAbono2:    '4200010002',  // Devoluciones s/Ventas 0% (motor)
    cuentaIva:       '2104010001',  // IVA 16% a cancelar
    prioridad:       86,
  },
  {
    nombre:          'Reg 19 — NC Devolución sobre Descuento',
    tipoComprobante: 'E',
    tipoRelacion:    '01',
    tieneDescuento:  true,
    ivaHaber:        true,           // IVA va al HABER: la empresa cobra IVA que faltó cobrar
    cuentaCargo:     '1103010001',  // Clientes (cobra la diferencia al cliente)
    cuentaAbono:     '4200020001',  // Descuentos s/Ventas 16% (cancela el exceso de descuento)
    cuentaIva:       '2104010001',  // IVA Trasladado (IVA ahora causado)
    prioridad:       87,
  },
  {
    // NC tasa 0% con descuento: revierte la venta bruta, cancela el descuento original,
    // devuelve al cliente el importe neto (sin IVA porque tasa=0%).
    // Asiento: DEBE Devoluciones 0% = subtotal; HABER Descuentos 0% = descuento (motor);
    //          HABER Bancos = total (= subtotal - descuento). Cuadra ✓
    nombre:          'Reg 19B — NC Devolución Tasa 0% con Descuento',
    tipoComprobante: 'E',
    tipoRelacion:    '01',
    tasaIva:         '0',
    tieneDescuento:  true,
    cuentaCargo:     '4200010002',   // Devoluciones s/Ventas 0%
    cuentaAbono:     '1102011005',   // Bancos (devolución al cliente)
    cuentaDescuento: '4200020002',   // Descuentos s/Ventas 0% (HABER — cancela el descuento original)
    cuentaIva:       null,
    prioridad:       86,
  },

  // ── 13. BONIFICACIONES NC RETROACTIVAS (Reglas 20–21) ────────────────────
  // Rappel anual / descuento retroactivo. Cargo a Ingresos, no a Devoluciones.
  {
    nombre:          'Reg 20 — Bonificación s/Ventas 16% (NC retroactiva)',
    tipoComprobante: 'E',
    tipoRelacion:    '01',
    tasaIva:         '16',
    cuentaCargo:     '4100010001',  // Ingresos Por Ventas Contado 16%
    cuentaAbono:     '1102011005',  // Bancos
    cuentaIva:       '2104010001',
    prioridad:       88,
  },
  {
    nombre:          'Reg 21 — Bonificación s/Ventas Tasa 0% (NC retroactiva)',
    tipoComprobante: 'E',
    tipoRelacion:    '01',
    tasaIva:         '0',
    cuentaCargo:     '4100010002',  // Ingresos Por Ventas Contado 0%
    cuentaAbono:     '1102011005',  // Bancos
    cuentaIva:       null,
    prioridad:       89,
  },

  // ── 14. NC APLICACIÓN DE ANTICIPO (Regla 23) ─────────────────────────────
  // E + tipoRelacion=07. Liquida pasivo anticipos y reconoce IVA definitivo.
  {
    nombre:            'Reg 23 — NC Aplicación de Anticipo (TipoRelacion 07)',
    tipoComprobante:   'E',
    tipoRelacion:      '07',
    cuentaCargo:       '2103010001',  // Anticipos De Clientes General (subtotal — cancela pasivo)
    cuentaAbono:       '1103010001',  // Clientes Nac Gral 16% (reduce CxC de factura final)
    cuentaIva:         '2104010001',  // IVA Trasladado definitivo (HABER)
    cuentaIvaAnticipo: '2104010002',  // IVA Trasladado Anticipos (DEBE — cancela diferido)
    prioridad:         90,
  },

  // ── 15. SALDO A FAVOR DEL CLIENTE (Reglas 25A, 24A) ─────────────────────
  // NC sin reembolso — importe queda como saldo en Anticipos Otros (2103090001).
  // 25A (prio 91) = tasa 0%; 24A (prio 92) = tasa 16%.
  {
    nombre:          'Reg 25A — Generación Saldo a Favor Tasa 0% (sin reembolso)',
    tipoComprobante: 'E',
    tipoRelacion:    '01',
    tasaIva:         '0',
    cuentaCargo:     '4200010002',  // Devoluciones s/Ventas 0%
    cuentaAbono:     '2103090001',  // Anticipos Otros
    cuentaIva:       null,
    prioridad:       91,
  },
  {
    nombre:          'Reg 24A — Generación Saldo a Favor 16% (sin reembolso)',
    tipoComprobante: 'E',
    tipoRelacion:    '01',
    tasaIva:         '16',
    cuentaCargo:     '4200010001',  // Devoluciones s/Ventas 16%
    cuentaAbono:     '2103090001',  // Anticipos Otros
    cuentaIva:       '2104010001',  // IVA Trasladado
    prioridad:       92,
  },

  // ── 16. COMODÍN — Sin coincidencia (Regla 9) ──────────────────────────────
  // Prioridad 99 (último recurso). Genera póliza con cuentas genéricas para revisión.
  {
    nombre:          'Reg 9 — Comodín General (Sin coincidencia)',
    tipoComprobante: null,
    metodoPago:      null,
    formaPago:       null,
    cuentaCargo:     '1102011005',  // Bancos
    cuentaAbono:     '4100020001',  // Ingresos Crédito 16%
    cuentaIva:       '2104010001',  // IVA Trasladado (PUE)
    cuentaIvaPPD:    '2105010001',  // IVA Por Trasladar PPD (PPD)
    prioridad:       99,
  },
];

// ── Runner ────────────────────────────────────────────────────────────────────

async function run() {
  await sequelize.authenticate();
  console.log('PostgreSQL conectado.');

  const force = process.argv.includes('--force');
  const existing = await CfdiMappingRule.count();

  if (existing > 0 && !force) {
    console.log(`Ya existen ${existing} reglas en cfdi_mapping_rules.`);
    console.log('Usa --force para eliminar las existentes y volver a insertar.');
    await sequelize.close();
    return;
  }

  if (existing > 0 && force) {
    await CfdiMappingRule.destroy({ where: {} });
    console.log(`${existing} reglas eliminadas (--force).`);
  }

  await CfdiMappingRule.bulkCreate(reglas);
  console.log(`✓ ${reglas.length} reglas insertadas correctamente.`);
  console.log('');
  console.log('IMPLEMENTADO:');
  console.log('  ✓ rfcReceptor — modelo + matching (listo para definir Reg 2A–2E/6-IC con las cuentas intercompañía).');
  console.log('  ✓ cuentaDeltaAnticipo — Reg 22C genera 5° mov (Bancos) cuando total_factura > total_anticipo.');
  console.log('  ✓ esAplicacionSaldo + cuentaCargo2 — Reg 24B/25B dividen cargo saldo/cash automáticamente.');
  console.log('');
  console.log('PENDIENTES — requieren trabajo adicional:');
  console.log('  • Reg 2A–2E, 6-IC (intercompañía): definir cuentas y agregar reglas al seed con rfcReceptor.');
  console.log('  ✓ Reg 24C, 25C (saldo PPD): implementadas con relacionadoTipo=E para distinguir de Reg 22B (relacionadoTipo=I).');
  console.log('  • Reg 24A, 25A (saldo a favor): verificar que exista cuenta 2103090001 en el catálogo.');
  console.log('  • Las columnas nuevas (rfc_receptor, cuenta_delta_anticipo, etc.) se agregan automáticamente al reiniciar el servidor (sync alter).');

  await sequelize.close();
  process.exit(0);
}

run().catch((err) => {
  console.error('Error en seed-cfdi-mapping-rules:', err);
  process.exit(1);
});
