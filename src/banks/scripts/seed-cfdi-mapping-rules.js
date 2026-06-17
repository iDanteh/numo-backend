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
 *   node src/banks/scripts/seed-cfdi-mapping-rules.js           (primera vez, BD vacía)
 *   node src/banks/scripts/seed-cfdi-mapping-rules.js --sync    (actualiza por nombre, seguro)
 *   node src/banks/scripts/seed-cfdi-mapping-rules.js --force   (borra todo y re-inserta)
 *
 * Uso en Docker:
 *   docker exec numo-backend node src/banks/scripts/seed-cfdi-mapping-rules.js --sync
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
// 4100020002  Ingresos Por Ventas Crédito Tasa 0%
// 4200010001  Devoluciones s/Ventas 16%
// 4200010002  Devoluciones s/Ventas 0%
// 4200020001  Descuentos s/Ventas 16%
// 4200020002  Descuentos s/Ventas 0%

const reglas = [

  // ── 0. ANTICIPOS (Reglas 22A + 22) ───────────────────────────────────────
  // Prioridad 9 — antes que 1A–1E para que el claveProdServ gane.
  // Reg 22A (formaPago=01) gana por spec sobre Reg 22 (formaPago=null) al desempatar.
  {
    nombre:          'Reg 22A — Recepción de Anticipo Efectivo (ClaveProdServ 84111506)',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       '01',
    claveProdServ:   '84111506',
    cuentaCargo:     '1101010003',   // Caja (formaPago=01 → efectivo)
    cuentaAbono:     '2103010001',   // Anticipos de Clientes General
    cuentaIva:       '2104010002',   // IVA Trasladado – Anticipos (diferido al recibir el anticipo)
    conceptoContiene: null,
    prioridad:       9,
  },
  {
    nombre:          'Reg 22 — Recepción de Anticipo (ClaveProdServ 84111506)',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    claveProdServ:   '84111506',
    cuentaCargo:     '1102011005',   // Bancos (dinero recibido)
    cuentaAbono:     '2103010001',   // Anticipos de Clientes General
    cuentaIva:       '2104010002',   // IVA Trasladado – Anticipos (diferido al recibir el anticipo)
    conceptoContiene: null,
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
    conceptoContiene: null,
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
    conceptoContiene: null,
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
    conceptoContiene: null,
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
    conceptoContiene: null,
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
    conceptoContiene: null,
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
    conceptoContiene: null,
    prioridad:       14,
  },
  {
    // Fallback para formas de pago no cubiertas por 1A–1E (ej. 06=Dinero electrónico,
    // 17=Compensación, etc.). formaPago=null: gana por prioridad solo cuando ninguna
    // regla más específica aplica. Spec-count: 1A–1E tienen formaPago → ganan sobre esta.
    nombre:          'Reg 1X — Venta PUE 16% (Forma de Pago No Especificada)',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       null,
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '1102011005',   // Bancos (forma de pago desconocida → cuenta genérica)
    cuentaAbono:     '4100010001',   // Ingresos Contado 16%
    cuentaIva:       '2104010001',   // IVA Trasladado
    conceptoContiene: null,
    prioridad:       14,
  },
  {
    // PUE tasa 0% sin forma de pago especificada. Sin cuentaIva (exento).
    nombre:          'Reg 1X-0 — Venta PUE 0% (Forma de Pago No Especificada)',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       null,
    tasaIva:         '0',
    tieneDescuento:  false,
    cuentaCargo:     '1102011005',   // Bancos por identificar
    cuentaAbono:     '4100010002',   // Ingresos Contado 0%
    prioridad:       15,
  },
  {
    // Fallback PUE cuando _detectTasaIva devuelve null (Metadata, CFDI 3.3 sin desglose IVA,
    // o productos exentos sin tasa explícita). Evita que caigan a Reg 9 → Ingresos Crédito.
    nombre:          'Reg 1X-N — Venta PUE (Tasa y Forma de Pago desconocidas)',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       null,
    tasaIva:         null,
    tieneDescuento:  false,
    cuentaCargo:     '1102011005',   // Bancos por identificar
    cuentaAbono:     '4100010001',   // Ingresos Contado 16% (cuenta genérica PUE)
    cuentaIva:       '2104010001',
    prioridad:       16,             // después de 1X (14) y 1X-0 (15); gana sobre Reg 9 (99)
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
    conceptoContiene: null,
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
    conceptoContiene: null,
    prioridad:       10,            // mismo número que 1G; spec mayor gana (tiene tasaIva)
  },

  // ── 2. IVA TASA 0% — PUE (Reglas 10A + 10) ───────────────────────────────
  // Prioridad 15. Exportaciones, alimentos, medicamentos. Sin IVA.
  // Reg 10A (formaPago=01) gana por spec sobre Reg 10 (formaPago=null) al desempatar.
  {
    nombre:          'Reg 10A — Venta PUE Efectivo Tasa 0%',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       '01',
    tasaIva:         '0',
    tieneDescuento:  false,
    cuentaCargo:     '1101010003',  // Caja (formaPago=01 → efectivo)
    cuentaAbono:     '4100010002',  // Ingresos Contado 0%
    cuentaIva:       null,
    conceptoContiene: null,
    prioridad:       15,
  },
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
    conceptoContiene: null,
    prioridad:       15,
  },

  // ── 2B. SIN TASA IVA / EXENTO — PUE (Reglas 1NA + 1NX) ──────────────────
  // Prioridad 16 (mismo que Reg 12 mixto — en empate de prio, spec mayor gana).
  // Captura CFDIs exentos (alimentos, medicamentos, exportaciones) donde
  // _detectTasaIva devuelve null por ausencia de nodo traslados.
  // Reg 10/10A (tasaIva='0', prio 15) tienen más spec → ganan para 0% reales.
  // Reg 12/12A (tasaIva='mixto') tienen más spec → ganan para mixtos.
  {
    nombre:          'Reg 1NA — Venta PUE Efectivo Exento (Sin Tasa IVA)',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       '01',
    tasaIva:         null,
    tieneDescuento:  false,
    cuentaCargo:     '1101010003',   // Caja (formaPago=01 → efectivo)
    cuentaAbono:     '4100010002',   // Ingresos Contado 0% (exento = sin IVA)
    cuentaIva:       null,
    conceptoContiene: null,
    prioridad:       16,
  },
  {
    nombre:          'Reg 1NX — Venta PUE Exento Fallback (Sin Tasa IVA)',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       null,
    tasaIva:         null,
    tieneDescuento:  false,
    cuentaCargo:     '1102011005',   // Bancos
    cuentaAbono:     '4100010002',   // Ingresos Contado 0%
    cuentaIva:       null,
    conceptoContiene: null,
    prioridad:       16,
  },

  // ── 3. CFDI MIXTO PUE (0%+16%) — Reglas 12A + 12 ────────────────────────
  // Prioridad 16. Motor agrega partida Ingresos 0% (cuentaAbono2) por subtotal exento.
  // Reg 12A (formaPago=01) gana por spec sobre Reg 12 (formaPago=null) al desempatar.
  {
    nombre:          'Reg 12A — Venta Mixta PUE Efectivo (0%+16%)',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       '01',
    tasaIva:         'mixto',
    tieneDescuento:  false,
    cuentaCargo:     '1101010003',  // Caja (formaPago=01 → efectivo)
    cuentaAbono:     '4100010001',  // Ingresos 16% (partida principal)
    cuentaAbono2:    '4100010002',  // Ingresos 0% (partida adicional — motor)
    cuentaIva:       '2104010001',
    conceptoContiene: null,
    prioridad:       16,
  },
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
    conceptoContiene: null,
    prioridad:       16,
  },

  // ── 4. DESCUENTOS PUE 16% (Regla 14 + 14A) ───────────────────────────────
  // Prioridad 17. Motor agrega línea Descuentos s/Ventas 16% (cuentaDescuento).
  // Reg 14A (formaPago=01) gana por spec sobre Reg 14 (formaPago=null) al desempatar.
  {
    nombre:          'Reg 14A — Venta con Descuento PUE Efectivo 16%',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       '01',
    tasaIva:         '16',
    tieneDescuento:  true,
    cuentaCargo:     '1101010003',  // Caja (formaPago=01 → efectivo)
    cuentaAbono:     '4100010001',
    cuentaIva:       '2104010001',
    cuentaDescuento: '4200020001',  // Descuentos s/Ventas 16%
    conceptoContiene: null,
    prioridad:       17,
  },
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
    conceptoContiene: null,
    prioridad:       17,
  },

  // ── 5. DESCUENTOS PUE 0% (Reglas 15A + 15) ───────────────────────────────
  // Reg 15A (formaPago=01) gana por spec sobre Reg 15 (formaPago=null) al desempatar.
  {
    nombre:          'Reg 15A — Venta con Descuento PUE Efectivo Tasa 0%',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       '01',
    tasaIva:         '0',
    tieneDescuento:  true,
    cuentaCargo:     '1101010003',  // Caja (formaPago=01 → efectivo)
    cuentaAbono:     '4100010002',  // Ingresos 0%
    cuentaIva:       null,
    cuentaDescuento: '4200020002',  // Descuentos s/Ventas 0%
    conceptoContiene: null,
    prioridad:       18,
  },
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
    conceptoContiene: null,
    prioridad:       18,
  },

  // ── 5B. DESCUENTO PUE EXENTO (Reglas 15NA + 15NX) ────────────────────────
  // Prioridad 19 (mismo que Reg 16/16A mixto-descuento — spec mayor gana en empate).
  // Reg 15/15A (tasaIva='0') y Reg 16/16A (tasaIva='mixto') tienen más spec → ganan.
  {
    nombre:          'Reg 15NA — Venta con Descuento PUE Efectivo Exento (Sin Tasa IVA)',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       '01',
    tasaIva:         null,
    tieneDescuento:  true,
    cuentaCargo:     '1101010003',   // Caja
    cuentaAbono:     '4100010002',   // Ingresos Contado 0%
    cuentaIva:       null,
    cuentaDescuento: '4200020002',   // Descuentos s/Ventas 0%
    conceptoContiene: null,
    prioridad:       19,
  },
  {
    nombre:          'Reg 15NX — Venta con Descuento PUE Exento Fallback (Sin Tasa IVA)',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       null,
    tasaIva:         null,
    tieneDescuento:  true,
    cuentaCargo:     '1102011005',   // Bancos
    cuentaAbono:     '4100010002',   // Ingresos Contado 0%
    cuentaIva:       null,
    cuentaDescuento: '4200020002',   // Descuentos s/Ventas 0%
    conceptoContiene: null,
    prioridad:       19,
  },

  // ── 6. DESCUENTOS MIXTO (Reglas 16A + 16) ────────────────────────────────
  // Prioridad 19. Mixto con descuentos en ambas tasas.
  // Reg 16A (formaPago=01) gana por spec sobre Reg 16 (formaPago=null) al desempatar.
  {
    nombre:          'Reg 16A — Venta con Descuento Mixto PUE Efectivo (0%+16%)',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       '01',
    tasaIva:         'mixto',
    tieneDescuento:  true,
    cuentaCargo:     '1101010003',  // Caja (formaPago=01 → efectivo)
    cuentaAbono:     '4100010001',
    cuentaAbono2:    '4100010002',  // Ingresos 0% (motor)
    cuentaIva:       '2104010001',
    cuentaDescuento:  '4200020001',  // Descuentos s/Ventas 16%
    cuentaDescuento0: '4200020002',  // Descuentos s/Ventas 0%
    conceptoContiene: null,
    prioridad:       19,
  },
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
    conceptoContiene: null,
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
    conceptoContiene: null,
    prioridad:           12,
  },

  // ── 7B2. SALDO A FAVOR DE TIMBRE PPD (Reglas SF-TIM) ─────────────────────
  // tipo I + PUE + tipoRelacion=07 SIN formaPago=30 (formaPago='03', '01', etc.)
  // El cliente tenía saldo en Anticipos Otros (2103090001) y se aplicó al facturar.
  // Diferencia con Reg 22C: cargo a 2103090001 (no 2103010001) + IVA usa 2104010002.
  {
    nombre:           'Reg SF-TIM-16 — Saldo a Favor Timbrado PUE 16%',
    tipoComprobante:  'I',
    metodoPago:       'PUE',
    tipoRelacion:     '07',
    tasaIva:          '16',
    cuentaCargo:      '2103090001',    // Anticipos Otros (saldo a favor consume)
    cuentaAbono:      '4100010001',    // Ingresos Contado 16%
    cuentaIva:        '2104010001',    // IVA Trasladado definitivo (HABER)
    cuentaIvaAnticipo:'2104010002',    // IVA Trasladado Anticipos (DEBE — cancela diferido)
    conceptoContiene: null,
    prioridad:        13,
  },
  {
    nombre:           'Reg SF-TIM-0 — Saldo a Favor Timbrado PUE 0%',
    tipoComprobante:  'I',
    metodoPago:       'PUE',
    tipoRelacion:     '07',
    tasaIva:          '0',
    cuentaCargo:      '2103090001',
    cuentaAbono:      '4100010002',    // Ingresos Contado 0%
    cuentaIva:        null,
    cuentaIvaAnticipo:null,
    conceptoContiene: null,
    prioridad:        13,
  },

  // ── 7A-BIS. COBRO ANTICIPO — TIPO P (Reglas P-ANT) ──────────────────────────
  // CFDI P con tipoRelacion='07': el pago referencia directamente un anticipo.
  // El ERP no lo contabiliza en Clientes (CxC) sino como ingreso inmediato (Contado),
  // porque el cliente pagó el anticipo al momento —sin una CxC abierta previa.
  // Prioridad 14: gana sobre Reg 7A–7Z (prio 70-99); pierde ante IC-P (prio 5-6).
  // NO se usa cuentaIvaPPD (no hay cuenta puente de IVA para un anticipo).
  {
    nombre:          'Reg P-ANT-16 — Cobro Anticipo TipoRelacion 07 tasa 16%',
    tipoComprobante: 'P',
    tipoRelacion:    '07',
    tasaIva:         '16',
    cuentaCargo:     '1102011005',   // Bancos por identificar (cash del anticipo)
    cuentaAbono:     '4100010001',   // Ingresos Contado 16% (ingreso inmediato PUE)
    cuentaIva:       '2104010001',   // IVA Trasladado definitivo
    cuentaIvaPPD:    null,
    conceptoContiene: null,
    prioridad:       14,
  },
  {
    nombre:          'Reg P-ANT-0 — Cobro Anticipo TipoRelacion 07 tasa 0%',
    tipoComprobante: 'P',
    tipoRelacion:    '07',
    tasaIva:         '0',
    cuentaCargo:     '1102011005',   // Bancos por identificar
    cuentaAbono:     '4100010002',   // Ingresos Contado 0%
    cuentaIva:       null,
    cuentaIvaPPD:    null,
    conceptoContiene: null,
    prioridad:       14,
  },
  {
    nombre:          'Reg P-ANT-N — Cobro Anticipo TipoRelacion 07 sin tasa (Metadata)',
    tipoComprobante: 'P',
    tipoRelacion:    '07',
    tasaIva:         null,
    cuentaCargo:     '1102011005',   // Bancos por identificar
    cuentaAbono:     '4100010001',   // Ingresos Contado 16% (fallback — tasa no determinada)
    cuentaIva:       null,
    cuentaIvaPPD:    null,
    conceptoContiene: null,
    prioridad:       15,             // prio 15 (< que P-ANT-16/0 con tasaIva definido)
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
    conceptoContiene: null,
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
    conceptoContiene: null,
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
    cuentaCargo2:      '1103010002',  // Clientes Nac Gral 0%
    cuentaAbono:       '4100020002',  // Ingresos Por Ventas Crédito Tasa 0% (PPD → crédito, no contado)
    cuentaIvaPPD:      null,
    conceptoContiene: null,
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
    conceptoContiene: null,
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
    conceptoContiene: null,
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
    conceptoContiene: null,
    prioridad:       60,
  },

  // ── 8A. SIN TASA IVA / EXENTO — PPD (Regla 11N) ──────────────────────────
  // Prioridad 66 (mismo que Reg 13 mixto PPD — spec mayor gana en empate).
  // Captura CFDIs PPD exentos donde _detectTasaIva devuelve null.
  // Reg 11 (tasaIva='0', prio 65) y Reg 13 (tasaIva='mixto', prio 66) tienen más spec.
  {
    nombre:          'Reg 11N — Venta PPD Exento (Sin Tasa IVA)',
    tipoComprobante: 'I',
    metodoPago:      'PPD',
    formaPago:       '99',
    tasaIva:         null,
    tieneDescuento:  false,
    cuentaCargo:     '1103010002',   // Clientes Nac Gral 0%
    cuentaAbono:     '4100020002',   // Ingresos Crédito 0%
    cuentaIvaPPD:    null,
    conceptoContiene: null,
    prioridad:       66,
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
    conceptoContiene: null,
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
    cuentaAbono:     '4100020002',  // Ingresos Por Ventas Crédito Tasa 0%
    cuentaIvaPPD:    null,
    conceptoContiene: null,
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
    cuentaAbono:     '4100020002',   // Ingresos Por Ventas Crédito Tasa 0%
    cuentaIvaPPD:    null,
    cuentaDescuento: '4200020002',   // Descuentos s/Ventas 0%
    conceptoContiene: null,
    prioridad:       64,
  },

  // ── 8D. DESCUENTO PPD EXENTO (Regla 6D) ──────────────────────────────────
  // Prioridad 66. Exento con descuento PPD donde _detectTasaIva devuelve null.
  {
    nombre:          'Reg 6D — Venta con Descuento PPD Exento (Sin Tasa IVA)',
    tipoComprobante: 'I',
    metodoPago:      'PPD',
    formaPago:       '99',
    tasaIva:         null,
    tieneDescuento:  true,
    cuentaCargo:     '1103010002',   // Clientes Nac Gral 0%
    cuentaAbono:     '4100020002',   // Ingresos Crédito 0%
    cuentaIvaPPD:    null,
    cuentaDescuento: '4200020002',   // Descuentos s/Ventas 0%
    conceptoContiene: null,
    prioridad:       66,
  },

  // ── 8E. DESCUENTO PPD MIXTO (Regla 6B-M) ────────────────────────────────
  // Prioridad 59 (igual que Reg 6B). Factura mixta PPD (0%+16%) con línea de descuento.
  // _detectTasaIva='mixto' + tieneDescuento=true → activa esta regla antes que Reg 13 (prio 66).
  {
    nombre:          'Reg 6B-M — Venta con Descuento PPD Mixta (0%+16%)',
    tipoComprobante: 'I',
    metodoPago:      'PPD',
    formaPago:       '99',
    tasaIva:         'mixto',
    tieneDescuento:  true,
    cuentaCargo:     '1103010001',   // Clientes 16%
    cuentaAbono:     '4100020001',   // Ingresos Crédito 16%
    cuentaAbono2:    '4100020002',   // Ingresos Crédito 0%
    cuentaCargoMixto0: '1103010002', // Clientes 0%
    cuentaIvaPPD:    '2105010001',
    cuentaDescuento: '4200020001',   // Descuentos s/Ventas 16%
    cuentaDescuento0:'4200020002',   // Descuentos s/Ventas 0%
    conceptoContiene: null,
    prioridad:       59,
  },

  // ── 9. CFDI MIXTO PPD (Regla 13) ─────────────────────────────────────────
  // Prioridad 66. Solo el IVA 16% se difiere; la porción 0% no genera IVA.
  // tieneDescuento=false: CFDIs mixtos con descuento deben manejarse manualmente.
  {
    nombre:              'Reg 13 — Venta Mixta PPD (0%+16%)',
    tipoComprobante:     'I',
    metodoPago:          'PPD',
    formaPago:           '99',
    tasaIva:             'mixto',
    tieneDescuento:      false,
    cuentaCargo:         '1103010001',   // Clientes 16% + IVA (motor ajusta a subtotal16+IVA si cuentaCargoMixto0 presente)
    cuentaAbono:         '4100020001',   // Ingresos Crédito 16%
    cuentaAbono2:        '4100020002',   // Ingresos Crédito 0% (PPD → crédito, no contado)
    cuentaCargoMixto0:   '1103010002',   // Clientes 0% (split porción 0% del subtotal)
    cuentaIvaPPD:        '2105010001',
    conceptoContiene:    null,
    prioridad:           66,
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
    conceptoContiene: null,
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
    conceptoContiene: null,
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
    conceptoContiene: null,
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
    conceptoContiene: null,
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
    conceptoContiene: null,
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
    conceptoContiene: null,
    prioridad:       74,
  },

  // ── 10A. COBROS PPD TASA 0% (Reglas 7A-0% – 7G-0%) ──────────────────────
  // Prioridad igual que sus equivalentes 16% — ganan por spec (tasaIva='0' definido).
  // Abonan a 1103010002 (Clientes 0%) en lugar de 1103010001 (Clientes 16%).
  // _detectTasaIva tipo P: si totalTrasladosImpuestoIVA16 = 0 y hay monto → devuelve '0'.
  {
    nombre:       'Reg 7A-0% — Cobro PPD Efectivo Tasa 0%',
    tipoComprobante: 'P',
    formaPago:    '01',
    tasaIva:      '0',
    cuentaCargo:  '1101010003',  // Caja
    cuentaAbono:  '1103010002',  // Clientes 0% (liquida CxC 0%)
    cuentaIva:    '2104010001',
    cuentaIvaPPD: '2105010001',
    conceptoContiene: null,
    prioridad:    70,
  },
  {
    nombre:       'Reg 7B-0% — Cobro PPD Transferencia Tasa 0%',
    tipoComprobante: 'P',
    formaPago:    '03',
    tasaIva:      '0',
    cuentaCargo:  '1102011005',
    cuentaAbono:  '1103010002',
    cuentaIva:    '2104010001',
    cuentaIvaPPD: '2105010001',
    conceptoContiene: null,
    prioridad:    71,
  },
  {
    nombre:       'Reg 7C-0% — Cobro PPD Cheque Tasa 0%',
    tipoComprobante: 'P',
    formaPago:    '04',
    tasaIva:      '0',
    cuentaCargo:  '1102011005',
    cuentaAbono:  '1103010002',
    cuentaIva:    '2104010001',
    cuentaIvaPPD: '2105010001',
    conceptoContiene: null,
    prioridad:    72,
  },
  {
    nombre:       'Reg 7F-0% — Cobro PPD Cheque Nominativo Tasa 0%',
    tipoComprobante: 'P',
    formaPago:    '02',
    tasaIva:      '0',
    cuentaCargo:  '1102011005',
    cuentaAbono:  '1103010002',
    cuentaIva:    '2104010001',
    cuentaIvaPPD: '2105010001',
    conceptoContiene: null,
    prioridad:    72,
  },
  {
    nombre:       'Reg 7D-0% — Cobro PPD Tarjeta Débito Tasa 0%',
    tipoComprobante: 'P',
    formaPago:    '28',
    tasaIva:      '0',
    cuentaCargo:  '1102011005',
    cuentaAbono:  '1103010002',
    cuentaIva:    '2104010001',
    cuentaIvaPPD: '2105010001',
    conceptoContiene: null,
    prioridad:    73,
  },
  {
    nombre:       'Reg 7E-0% — Cobro PPD Tarjeta Crédito Tasa 0%',
    tipoComprobante: 'P',
    formaPago:    '29',
    tasaIva:      '0',
    cuentaCargo:  '1102011005',
    cuentaAbono:  '1103010002',
    cuentaIva:    '2104010001',
    cuentaIvaPPD: '2105010001',
    conceptoContiene: null,
    prioridad:    74,
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
    conceptoContiene: null,
    prioridad:    70,
  },

  {
    nombre:       'Reg 7G-0% — Cobro PPD Monedero Club Tuberos Tasa 0%',
    tipoComprobante: 'P',
    formaPago:    '05',
    tasaIva:      '0',
    cuentaCargo:  '2103090002',  // Anticipos Otros Club Tuberos
    cuentaAbono:  '1103010002',  // Clientes 0%
    cuentaIva:    '2104010001',
    cuentaIvaPPD: '2105010001',
    conceptoContiene: null,
    prioridad:    70,
  },

  // ── 10B. COBROS PPD MIXTO (16%+0%) (Reglas 7A-mixto – 7Z-mixto) ────────────
  // Se activan cuando _detectTasaIva devuelve 'mixto' para CP 2.0 que cubren
  // facturas 16% Y facturas 0% en el mismo complemento de pago.
  // cuentaAbono  = Clientes 16% recibe base16 + iva16 (del <Totales>).
  // cuentaAbono2 = Clientes 0% recibe montoTotalPagos - (base16+iva16).
  // El motor calcula el split en el bloque "cobros MIXTOS tipo P".
  {
    nombre:       'Reg 7A-mixto — Cobro PPD Mixto Efectivo (16%+0%)',
    tipoComprobante: 'P', formaPago: '01', tasaIva: 'mixto',
    cuentaCargo:  '1101010003', cuentaAbono: '1103010001', cuentaAbono2: '1103010002',
    cuentaIva:    '2104010001', cuentaIvaPPD: '2105010001', prioridad: 70,
  },
  {
    nombre:       'Reg 7B-mixto — Cobro PPD Mixto Transferencia (16%+0%)',
    tipoComprobante: 'P', formaPago: '03', tasaIva: 'mixto',
    cuentaCargo:  '1102011005', cuentaAbono: '1103010001', cuentaAbono2: '1103010002',
    cuentaIva:    '2104010001', cuentaIvaPPD: '2105010001', prioridad: 71,
  },
  {
    nombre:       'Reg 7C-mixto — Cobro PPD Mixto Cheque (16%+0%)',
    tipoComprobante: 'P', formaPago: '04', tasaIva: 'mixto',
    cuentaCargo:  '1102011005', cuentaAbono: '1103010001', cuentaAbono2: '1103010002',
    cuentaIva:    '2104010001', cuentaIvaPPD: '2105010001', prioridad: 72,
  },
  {
    nombre:       'Reg 7F-mixto — Cobro PPD Mixto Cheque Nominativo (16%+0%)',
    tipoComprobante: 'P', formaPago: '02', tasaIva: 'mixto',
    cuentaCargo:  '1102011005', cuentaAbono: '1103010001', cuentaAbono2: '1103010002',
    cuentaIva:    '2104010001', cuentaIvaPPD: '2105010001', prioridad: 72,
  },
  {
    nombre:       'Reg 7D-mixto — Cobro PPD Mixto Tarjeta Débito (16%+0%)',
    tipoComprobante: 'P', formaPago: '28', tasaIva: 'mixto',
    cuentaCargo:  '1102011005', cuentaAbono: '1103010001', cuentaAbono2: '1103010002',
    cuentaIva:    '2104010001', cuentaIvaPPD: '2105010001', prioridad: 73,
  },
  {
    nombre:       'Reg 7E-mixto — Cobro PPD Mixto Tarjeta Crédito (16%+0%)',
    tipoComprobante: 'P', formaPago: '29', tasaIva: 'mixto',
    cuentaCargo:  '1102011005', cuentaAbono: '1103010001', cuentaAbono2: '1103010002',
    cuentaIva:    '2104010001', cuentaIvaPPD: '2105010001', prioridad: 74,
  },
  {
    nombre:       'Reg 7G-mixto — Cobro PPD Mixto Monedero Club Tuberos (16%+0%)',
    tipoComprobante: 'P', formaPago: '05', tasaIva: 'mixto',
    cuentaCargo:  '2103090002', cuentaAbono: '1103010001', cuentaAbono2: '1103010002',
    cuentaIva:    '2104010001', cuentaIvaPPD: '2105010001', prioridad: 70,
  },
  {
    nombre:       'Reg 7Z-mixto — Cobro PPD Mixto genérico (sin formaPago conocida)',
    tipoComprobante: 'P', formaPago: null, tasaIva: 'mixto',
    cuentaCargo:  '1101010003', cuentaAbono: '1103010001', cuentaAbono2: '1103010002',
    cuentaIva:    '2104010001', cuentaIvaPPD: '2105010001', prioridad: 97,
  },

  // ── 10B-2. COBRO PPD COMPENSACIÓN formaPago=17 (Reg 7X-17) ─────────────────
  // formaPago='17' = Compensación SAT: el cliente liquida su CxC mediante saldo a favor.
  // Sin movimiento bancario → cuentaCargo = Anticipos de Clientes (aplica saldo).
  {
    nombre:          'Reg 7X-17 — Cobro PPD Compensación 16%',
    tipoComprobante: 'P',
    formaPago:       '17',
    tasaIva:         '16',
    cuentaCargo:     '2103090001',  // Anticipos de Clientes (aplica saldo a favor)
    cuentaAbono:     '1103010001',  // Clientes 16% (liquida CxC)
    cuentaIva:       '2104010001',  // IVA causado definitivo
    cuentaIvaPPD:    '2105010001',  // cancela cuenta puente
    conceptoContiene: null,
    prioridad:       70,
  },
  {
    nombre:          'Reg 7X-17-0 — Cobro PPD Compensación 0%',
    tipoComprobante: 'P',
    formaPago:       '17',
    tasaIva:         '0',
    cuentaCargo:     '2103090001',  // Anticipos de Clientes (aplica saldo a favor)
    cuentaAbono:     '1103010002',  // Clientes 0% (liquida CxC)
    cuentaIva:       null,
    cuentaIvaPPD:    null,
    conceptoContiene: null,
    prioridad:       70,
  },

  // ── 10C. COBRO GENÉRICO TIPO P — FALLBACK CP 1.0 (Reg 7Z-0 y Reg 7Z) ──────
  // Reg 7Z-0 (prio 98): tasa '0' confirmada pero sin formaPago conocida.
  // Captura CFDIs Metadata P cuya tasaIvaInferida='0' y que no tienen formaPago.
  // Gana sobre Reg 7Z (prio 99) gracias a tasaIva más específico + menor prioridad.
  {
    nombre:          'Reg 7Z-0 — Cobro Genérico Tasa 0% (sin formaPago)',
    tipoComprobante: 'P',
    formaPago:       null,
    tasaIva:         '0',
    cuentaCargo:     '1101010003',  // Caja por identificar
    cuentaAbono:     '1103010002',  // Clientes Nac Gral 0%
    cuentaIva:       null,
    cuentaIvaPPD:    '2105010001',
    conceptoContiene: null,
    prioridad:       98,
  },
  // Reg 7Z (prio 99): último recurso para tasa no determinada → Clientes 16%.
  {
    nombre:          'Reg 7Z — Cobro Genérico (CP 1.0 / forma de pago no clasificada)',
    tipoComprobante: 'P',
    formaPago:       null,
    tasaIva:         null,
    cuentaCargo:     '1101010003',  // Caja por identificar
    cuentaAbono:     '1103010001',  // Clientes Nac Gral 16% (default)
    cuentaIva:       '2104010001',
    cuentaIvaPPD:    '2105010001',
    conceptoContiene: null,
    prioridad:       99,
  },

  // ── CC. REGLAS POR CONCEPTO (conceptoContiene) ────────────────────────────
  // Prioridades 74–78 — se disparan ANTES que las reglas genéricas tipo E (80+).
  // Sub-clasifican NCs según la descripción del primer concepto del CFDI.
  //
  // JERARQUÍA:
  //   74 — Club Tuberos (más específico: gana sobre 'bonificaci' genérico)
  //   75 — Bonificación genérica
  //   76 — Devolución de Cliente
  //   77 — Cancelación de Cliente
  //   78 — Aplicación de Anticipo (Reg 23 prio 90 gana por spec si tipoRelacion=07)
  //
  // POR QUÉ SE NECESITAN:
  //   Sin estas reglas, una NC E+tipoRelacion=01+tasaIva=16 con descripción
  //   "BONIFICACIÓN" cae en Reg 8A (prio 80 → Devoluciones 4200010001) en lugar de
  //   Reg 20 (prio 88 → Descuentos 4200020001). La prioridad numérica gana siempre.
  //   Las CC-rules a prio 74-78 fuerzan la ruta correcta antes de que lleguen las 8A+.
  //
  // KEYWORD 'bonificaci':
  //   Es substring de 'bonificación'.toLowerCase() Y de 'bonificacion'.toLowerCase()
  //   → captura ambas ortografías (con y sin acento) con una sola cadena.

  // ── CC0. BONIFICACIÓN ERP (tipoOrigen) (prio 69) ──────────────────────────
  // CFDIs del ERP tienen conceptos:[] → conceptoContiene nunca matchea.
  // El único discriminador disponible es tipoOrigen (campo ERP).
  // tipoOrigen='Bonificación' debe ir a Descuentos, NO a Devoluciones.
  // Prioridad 69 para ganar sobre TO-BON (70) que tiene misma spec (3 campos) y
  // enviaría el HABER a Bancos en lugar de Clientes.
  // cuentaAbono = Clientes (cancela CxC pendiente, sin movimiento de efectivo).
  // cuentaIva/cuentaIvaPPD: el motor elige entre ellas según esPPD del CFDI.
  {
    nombre:        'Reg CC-BON-ERP-16 — NC Bonificación ERP 16% (cancela CxC)',
    tipoComprobante: 'E',
    tipoOrigen:    'Bonificación',
    tasaIva:       '16',
    cuentaCargo:   '4200020001',  // Descuentos s/Ventas 16%
    cuentaAbono:   '1103010001',  // Clientes 16% (extingue CxC)
    cuentaIva:     '2104010001',  // IVA Trasladado (PUE)
    cuentaIvaPPD:  '2105010001', // IVA Por Trasladar (PPD)
    prioridad:     69,
  },
  {
    nombre:        'Reg CC-BON-ERP-0 — NC Bonificación ERP 0% (cancela CxC)',
    tipoComprobante: 'E',
    tipoOrigen:    'Bonificación',
    tasaIva:       '0',
    cuentaCargo:   '4200020002',  // Descuentos s/Ventas 0%
    cuentaAbono:   '1103010002',  // Clientes 0%
    cuentaIva:     null,
    cuentaIvaPPD:  null,
    prioridad:     69,
  },
  {
    nombre:        'Reg CC-BON-ERP-M — NC Bonificación ERP Mixta (cancela CxC)',
    tipoComprobante: 'E',
    tipoOrigen:    'Bonificación',
    tasaIva:       'mixto',
    cuentaCargo:   '4200020001',  // Descuentos s/Ventas 16% (partida principal)
    cuentaAbono:   '1103010001',  // Clientes 16%
    cuentaAbono2:  '4200020002', // Descuentos 0% (motor mixto)
    cuentaIva:     '2104010001',
    cuentaIvaPPD:  '2105010001',
    prioridad:     69,
  },

  // ── CC1. BONIFICACIÓN CLUB TUBEROS (prio 74) ──────────────────────────────
  // 'club tuberos' es substring de 'bonificacion club tuberos'.
  // Reg CC-CLT gana sobre Reg CC-BON (prio 75) porque 74 < 75.
  // cuentaAbono = 2103090002 (Monedero Club Tuberos): la bonificación acredita el saldo
  // del cliente en el monedero electrónico sin movimiento de efectivo.
  // ⚠️ CONFIRMAR: si la bonificación se paga en efectivo/transferencia, cambiar
  //    cuentaAbono a 1101010003 (Caja) o 1102011005 (Bancos) según el caso.
  {
    nombre:           'Reg CC-CLT-16 — NC Bonificación Club Tuberos 16%',
    tipoComprobante:  'E',
    tasaIva:          '16',
    conceptoContiene: 'club tuberos',
    cuentaCargo:      '4200020001',  // Descuentos s/Ventas 16%
    cuentaAbono:      '2103090002',  // Anticipos Otros Club Tuberos (acredita monedero)
    cuentaIva:        '2104010001',  // IVA Trasladado
    prioridad:        74,
  },
  {
    nombre:           'Reg CC-CLT-0 — NC Bonificación Club Tuberos 0%',
    tipoComprobante:  'E',
    tasaIva:          '0',
    conceptoContiene: 'club tuberos',
    cuentaCargo:      '4200020002',  // Descuentos s/Ventas 0%
    cuentaAbono:      '2103090002',  // Anticipos Otros Club Tuberos
    cuentaIva:        null,
    prioridad:        74,
  },
  {
    // Captura bonificaciones Club Tuberos donde _detectTasaIva devuelve null
    // (sin traslados en conceptos ni totalImpuestosTrasladados en header).
    // Gana sobre CC-BON genérico (prio 75) por tener prio 74.
    nombre:           'Reg CC-CLT-N — NC Bonificación Club Tuberos Sin Tasa',
    tipoComprobante:  'E',
    tasaIva:          null,
    conceptoContiene: 'club tuberos',
    cuentaCargo:      '4200020001',  // Descuentos s/Ventas 16% (fallback conservador)
    cuentaAbono:      '2103090002',  // Anticipos Otros Club Tuberos
    cuentaIva:        '2104010001',  // IVA Trasladado — evita desbalance si CFDI tiene IVA real
    prioridad:        74,
  },

  // ── CC1B. BONIFICACIÓN CLUB TUBEROS POR SERIE BCT (prio 74) ─────────────────
  // Detectadas vía ERP: documentosRelacionados[].Serie === 'BCT'.
  // El enriquecimiento en balanza-preliminar.service.js establece
  //   tipoOrigen = 'Bonificación Club Tuberos' para estas NCs.
  // Se cubren las tres variantes de tasa para no dejar huecos.
  {
    nombre:          'Reg CC-BCT-16 — NC Bonificación Club Tuberos BCT 16%',
    tipoComprobante: 'E',
    tipoOrigen:      'Bonificación Club Tuberos',
    tasaIva:         '16',
    cuentaCargo:     '4200020001',  // Descuentos s/Ventas 16%
    cuentaAbono:     '2103090002',  // Anticipos Otros Club Tuberos
    cuentaIva:       '2104010001',  // IVA Trasladado
    prioridad:       74,
  },
  {
    nombre:          'Reg CC-BCT-0 — NC Bonificación Club Tuberos BCT 0%',
    tipoComprobante: 'E',
    tipoOrigen:      'Bonificación Club Tuberos',
    tasaIva:         '0',
    cuentaCargo:     '4200020002',  // Descuentos s/Ventas 0%
    cuentaAbono:     '2103090002',  // Anticipos Otros Club Tuberos
    cuentaIva:       null,
    prioridad:       74,
  },
  {
    // Fallback BCT cuando _detectTasaIva no puede determinar la tasa.
    // tipoOrigen='Bonificación Club Tuberos' es más específico (spec 2) que
    // CC-BON-16 (spec 3 con tasaIva definido) — solo gana cuando tasaIva=null
    // porque CC-BCT-16 y CC-BCT-0 requieren tasa explícita.
    nombre:          'Reg CC-BCT-N — NC Bonificación Club Tuberos BCT Sin Tasa',
    tipoComprobante: 'E',
    tipoOrigen:      'Bonificación Club Tuberos',
    tasaIva:         null,
    cuentaCargo:     '4200020001',  // Descuentos s/Ventas 16% (fallback conservador)
    cuentaAbono:     '2103090002',  // Anticipos Otros Club Tuberos
    cuentaIva:       '2104010001',  // IVA Trasladado — evita desbalance si CFDI tiene IVA real
    prioridad:       74,
  },

  // ── CC2. BONIFICACIÓN GENÉRICA (prio 75) ─────────────────────────────────
  // Captura 'BONIFICACIÓN' y 'Bonificacion' (con/sin acento).
  // formaPago=01 gana por spec sobre formaPago=null en empate de prioridad.
  {
    nombre:           'Reg CC-BON-16-EF — NC Bonificación 16% Efectivo',
    tipoComprobante:  'E',
    formaPago:        '01',
    tasaIva:          '16',
    conceptoContiene: 'bonificaci',
    cuentaCargo:      '4200020001',  // Descuentos s/Ventas 16%
    cuentaAbono:      '1101010003',  // Caja (formaPago=01 → efectivo)
    cuentaIva:        '2104010001',
    prioridad:        75,
  },
  {
    nombre:           'Reg CC-BON-16 — NC Bonificación 16%',
    tipoComprobante:  'E',
    tasaIva:          '16',
    conceptoContiene: 'bonificaci',
    cuentaCargo:      '4200020001',  // Descuentos s/Ventas 16%
    cuentaAbono:      '1102011005',  // Bancos
    cuentaIva:        '2104010001',
    prioridad:        75,
  },
  {
    nombre:           'Reg CC-BON-0-EF — NC Bonificación 0% Efectivo',
    tipoComprobante:  'E',
    formaPago:        '01',
    tasaIva:          '0',
    conceptoContiene: 'bonificaci',
    cuentaCargo:      '4200020002',  // Descuentos s/Ventas 0%
    cuentaAbono:      '1101010003',  // Caja
    cuentaIva:        null,
    prioridad:        75,
  },
  {
    nombre:           'Reg CC-BON-0 — NC Bonificación 0%',
    tipoComprobante:  'E',
    tasaIva:          '0',
    conceptoContiene: 'bonificaci',
    cuentaCargo:      '4200020002',  // Descuentos s/Ventas 0%
    cuentaAbono:      '1102011005',  // Bancos
    cuentaIva:        null,
    prioridad:        75,
  },

  // ── CC2A. BONIFICACIÓN MIXTA (prio 75) ────────────────────────────────────
  // Bonificaciones con IVA mixto (0%+16%) en el mismo CFDI.
  {
    nombre:           'Reg CC-BON-M-EF — NC Bonificación Mixta Efectivo',
    tipoComprobante:  'E',
    formaPago:        '01',
    tasaIva:          'mixto',
    conceptoContiene: 'bonificaci',
    cuentaCargo:      '4200020001',  // Descuentos s/Ventas 16%
    cuentaAbono:      '1101010003',  // Caja
    cuentaIva:        '2104010001',
    prioridad:        75,
  },
  {
    nombre:           'Reg CC-BON-M — NC Bonificación Mixta',
    tipoComprobante:  'E',
    tasaIva:          'mixto',
    conceptoContiene: 'bonificaci',
    cuentaCargo:      '4200020001',  // Descuentos s/Ventas 16%
    cuentaAbono:      '1102011005',  // Bancos
    cuentaIva:        '2104010001',
    prioridad:        75,
  },

  // ── CC2B. DESCUENTO / DTO (prio 75) ──────────────────────────────────────
  // Captura 'DTO', 'Dto', 'dto' — abreviatura de descuento en CFDIs.
  // Substring 'dto' no colisiona con palabras comunes del español.
  // formaPago=01 gana por spec sobre formaPago=null en empate de prioridad.
  {
    nombre:           'Reg CC-DTO-16-EF — NC Descuento 16% Efectivo',
    tipoComprobante:  'E',
    formaPago:        '01',
    tasaIva:          '16',
    conceptoContiene: 'dto',
    cuentaCargo:      '4200020001',  // Descuentos s/Ventas 16%
    cuentaAbono:      '1101010003',  // Caja (formaPago=01 → efectivo)
    cuentaIva:        '2104010001',
    prioridad:        75,
  },
  {
    nombre:           'Reg CC-DTO-16 — NC Descuento 16%',
    tipoComprobante:  'E',
    tasaIva:          '16',
    conceptoContiene: 'dto',
    cuentaCargo:      '4200020001',  // Descuentos s/Ventas 16%
    cuentaAbono:      '1102011005',  // Bancos
    cuentaIva:        '2104010001',
    prioridad:        75,
  },
  {
    nombre:           'Reg CC-DTO-0-EF — NC Descuento 0% Efectivo',
    tipoComprobante:  'E',
    formaPago:        '01',
    tasaIva:          '0',
    conceptoContiene: 'dto',
    cuentaCargo:      '4200020002',  // Descuentos s/Ventas 0%
    cuentaAbono:      '1101010003',  // Caja
    cuentaIva:        null,
    prioridad:        75,
  },
  {
    nombre:           'Reg CC-DTO-0 — NC Descuento 0%',
    tipoComprobante:  'E',
    tasaIva:          '0',
    conceptoContiene: 'dto',
    cuentaCargo:      '4200020002',  // Descuentos s/Ventas 0%
    cuentaAbono:      '1102011005',  // Bancos
    cuentaIva:        null,
    prioridad:        75,
  },

  // ── CC2C. DESCUENTO DTO MIXTO (prio 75) ──────────────────────────────────
  // DTO en CFDIs con IVA mixto (0%+16%).
  {
    nombre:           'Reg CC-DTO-M-EF — NC Descuento DTO Mixto Efectivo',
    tipoComprobante:  'E',
    formaPago:        '01',
    tasaIva:          'mixto',
    conceptoContiene: 'dto',
    cuentaCargo:      '4200020001',  // Descuentos s/Ventas 16%
    cuentaAbono:      '1101010003',  // Caja
    cuentaIva:        '2104010001',
    prioridad:        75,
  },
  {
    nombre:           'Reg CC-DTO-M — NC Descuento DTO Mixto',
    tipoComprobante:  'E',
    tasaIva:          'mixto',
    conceptoContiene: 'dto',
    cuentaCargo:      '4200020001',  // Descuentos s/Ventas 16%
    cuentaAbono:      '1102011005',  // Bancos
    cuentaIva:        '2104010001',
    prioridad:        75,
  },

  // ── CC2D. DESCUENTO PALABRA COMPLETA (prio 75) ───────────────────────────
  // Captura "DESCUENTO xxx" cuando el emisor usa la palabra completa en vez de "dto".
  // "dto" no es substring de "descuento" — ambos patrones son necesarios.
  // Misma prioridad que CC-DTO para que el más específico (con formaPago) gane.
  {
    nombre:           'Reg CC-DSC-16-EF — NC Descuento (palabra completa) 16% Efectivo',
    tipoComprobante:  'E',
    formaPago:        '01',
    tasaIva:          '16',
    conceptoContiene: 'descuento',
    cuentaCargo:      '4200020001',  // Descuentos s/Ventas 16%
    cuentaAbono:      '1101010003',  // Caja
    cuentaIva:        '2104010001',
    prioridad:        75,
  },
  {
    nombre:           'Reg CC-DSC-16 — NC Descuento (palabra completa) 16%',
    tipoComprobante:  'E',
    tasaIva:          '16',
    conceptoContiene: 'descuento',
    cuentaCargo:      '4200020001',  // Descuentos s/Ventas 16%
    cuentaAbono:      '1102011005',  // Bancos
    cuentaIva:        '2104010001',
    prioridad:        75,
  },
  {
    nombre:           'Reg CC-DSC-0-EF — NC Descuento (palabra completa) 0% Efectivo',
    tipoComprobante:  'E',
    formaPago:        '01',
    tasaIva:          '0',
    conceptoContiene: 'descuento',
    cuentaCargo:      '4200020002',  // Descuentos s/Ventas 0%
    cuentaAbono:      '1101010003',  // Caja
    cuentaIva:        null,
    prioridad:        75,
  },
  {
    nombre:           'Reg CC-DSC-0 — NC Descuento (palabra completa) 0%',
    tipoComprobante:  'E',
    tasaIva:          '0',
    conceptoContiene: 'descuento',
    cuentaCargo:      '4200020002',  // Descuentos s/Ventas 0%
    cuentaAbono:      '1102011005',  // Bancos
    cuentaIva:        null,
    prioridad:        75,
  },
  {
    nombre:           'Reg CC-DSC-M-EF — NC Descuento (palabra completa) Mixto Efectivo',
    tipoComprobante:  'E',
    formaPago:        '01',
    tasaIva:          'mixto',
    conceptoContiene: 'descuento',
    cuentaCargo:      '4200020001',  // Descuentos s/Ventas 16%
    cuentaAbono:      '1101010003',  // Caja
    cuentaIva:        '2104010001',
    prioridad:        75,
  },
  {
    nombre:           'Reg CC-DSC-M — NC Descuento (palabra completa) Mixto',
    tipoComprobante:  'E',
    tasaIva:          'mixto',
    conceptoContiene: 'descuento',
    cuentaCargo:      '4200020001',  // Descuentos s/Ventas 16%
    cuentaAbono:      '1102011005',  // Bancos
    cuentaIva:        '2104010001',
    prioridad:        75,
  },

  // ── CC2E. CANCELACIÓN COMO DESCUENTO (prio 75, tipoRelacion='01') ──────────
  // Captura NCs tipo01 cuyo concepto dice "CANCELACION" — créditos de precio,
  // no devolución física de mercancía.  tipoRelacion='01' es obligatorio para
  // que las NCs tipo03 con "CANCELACION" (devoluciones reales) sigan yendo a
  // CC-DEV / Devoluciones s/Ventas.
  {
    nombre:           'Reg CC-CAN-D-16-EF — NC Cancelación Descuento 16% Efectivo',
    tipoComprobante:  'E',
    tipoRelacion:     '01',
    formaPago:        '01',
    tasaIva:          '16',
    conceptoContiene: 'cancelaci',
    cuentaCargo:      '4200020001',  // Descuentos s/Ventas 16%
    cuentaAbono:      '1101010003',  // Caja
    cuentaIva:        '2104010001',
    prioridad:        75,
  },
  {
    nombre:           'Reg CC-CAN-D-16 — NC Cancelación Descuento 16%',
    tipoComprobante:  'E',
    tipoRelacion:     '01',
    tasaIva:          '16',
    conceptoContiene: 'cancelaci',
    cuentaCargo:      '4200020001',  // Descuentos s/Ventas 16%
    cuentaAbono:      '1102011005',  // Bancos
    cuentaIva:        '2104010001',
    prioridad:        75,
  },
  {
    nombre:           'Reg CC-CAN-D-0-EF — NC Cancelación Descuento 0% Efectivo',
    tipoComprobante:  'E',
    tipoRelacion:     '01',
    formaPago:        '01',
    tasaIva:          '0',
    conceptoContiene: 'cancelaci',
    cuentaCargo:      '4200020002',  // Descuentos s/Ventas 0%
    cuentaAbono:      '1101010003',  // Caja
    cuentaIva:        null,
    prioridad:        75,
  },
  {
    nombre:           'Reg CC-CAN-D-0 — NC Cancelación Descuento 0%',
    tipoComprobante:  'E',
    tipoRelacion:     '01',
    tasaIva:          '0',
    conceptoContiene: 'cancelaci',
    cuentaCargo:      '4200020002',  // Descuentos s/Ventas 0%
    cuentaAbono:      '1102011005',  // Bancos
    cuentaIva:        null,
    prioridad:        75,
  },
  {
    nombre:           'Reg CC-CAN-D-M-EF — NC Cancelación Descuento Mixto Efectivo',
    tipoComprobante:  'E',
    tipoRelacion:     '01',
    formaPago:        '01',
    tasaIva:          'mixto',
    conceptoContiene: 'cancelaci',
    cuentaCargo:      '4200020001',  // Descuentos s/Ventas 16%
    cuentaAbono:      '1101010003',  // Caja
    cuentaIva:        '2104010001',
    prioridad:        75,
  },
  {
    nombre:           'Reg CC-CAN-D-M — NC Cancelación Descuento Mixto',
    tipoComprobante:  'E',
    tipoRelacion:     '01',
    tasaIva:          'mixto',
    conceptoContiene: 'cancelaci',
    cuentaCargo:      '4200020001',  // Descuentos s/Ventas 16%
    cuentaAbono:      '1102011005',  // Bancos
    cuentaIva:        '2104010001',
    prioridad:        75,
  },

  // ── CC3. DEVOLUCIÓN DE CLIENTE (prio 76) ──────────────────────────────────
  // DEBE Devoluciones s/Ventas, HABER Bancos/Caja (reembolso al cliente).
  // Si la devolución no genera reembolso (queda como saldo a favor),
  // cambiar cuentaAbono a 2103090001 (Anticipos Otros).
  {
    nombre:           'Reg CC-DEV-16-EF — NC Devolución de Cliente 16% Efectivo',
    tipoComprobante:  'E',
    formaPago:        '01',
    tasaIva:          '16',
    conceptoContiene: 'devolucion',
    cuentaCargo:      '4200010001',  // Devoluciones s/Ventas 16%
    cuentaAbono:      '1101010003',  // Caja (formaPago=01 → efectivo)
    cuentaIva:        '2104010001',
    prioridad:        76,
  },
  {
    nombre:           'Reg CC-DEV-16 — NC Devolución de Cliente 16%',
    tipoComprobante:  'E',
    tasaIva:          '16',
    conceptoContiene: 'devolucion',
    cuentaCargo:      '4200010001',  // Devoluciones s/Ventas 16%
    cuentaAbono:      '1102011005',  // Bancos
    cuentaIva:        '2104010001',
    prioridad:        76,
  },
  {
    nombre:           'Reg CC-DEV-0-EF — NC Devolución de Cliente 0% Efectivo',
    tipoComprobante:  'E',
    formaPago:        '01',
    tasaIva:          '0',
    conceptoContiene: 'devolucion',
    cuentaCargo:      '4200010002',  // Devoluciones s/Ventas 0%
    cuentaAbono:      '1101010003',  // Caja
    cuentaIva:        null,
    prioridad:        76,
  },
  {
    nombre:           'Reg CC-DEV-0 — NC Devolución de Cliente 0%',
    tipoComprobante:  'E',
    tasaIva:          '0',
    conceptoContiene: 'devolucion',
    cuentaCargo:      '4200010002',  // Devoluciones s/Ventas 0%
    cuentaAbono:      '1102011005',  // Bancos
    cuentaIva:        null,
    prioridad:        76,
  },

  // ── CC4. CANCELACIÓN DE CLIENTE (prio 77) ─────────────────────────────────
  // Tratamiento contable idéntico a Devolución: DEBE Devoluciones, HABER Bancos/Caja.
  // ⚠️ CONFIRMAR: si la cancelación no genera reembolso en efectivo (queda como
  //    saldo a favor del cliente), cambiar cuentaAbono a 2103090001 (Anticipos Otros).
  {
    nombre:           'Reg CC-CAN-16-EF — NC Cancelación de Cliente 16% Efectivo',
    tipoComprobante:  'E',
    formaPago:        '01',
    tasaIva:          '16',
    conceptoContiene: 'cancelacion',
    cuentaCargo:      '4200010001',  // Devoluciones s/Ventas 16%
    cuentaAbono:      '1101010003',  // Caja
    cuentaIva:        '2104010001',
    prioridad:        77,
  },
  {
    nombre:           'Reg CC-CAN-16 — NC Cancelación de Cliente 16%',
    tipoComprobante:  'E',
    tasaIva:          '16',
    conceptoContiene: 'cancelacion',
    cuentaCargo:      '4200010001',  // Devoluciones s/Ventas 16%
    cuentaAbono:      '1102011005',  // Bancos
    cuentaIva:        '2104010001',
    prioridad:        77,
  },
  {
    nombre:           'Reg CC-CAN-0-EF — NC Cancelación de Cliente 0% Efectivo',
    tipoComprobante:  'E',
    formaPago:        '01',
    tasaIva:          '0',
    conceptoContiene: 'cancelacion',
    cuentaCargo:      '4200010002',  // Devoluciones s/Ventas 0%
    cuentaAbono:      '1101010003',  // Caja
    cuentaIva:        null,
    prioridad:        77,
  },
  {
    nombre:           'Reg CC-CAN-0 — NC Cancelación de Cliente 0%',
    tipoComprobante:  'E',
    tasaIva:          '0',
    conceptoContiene: 'cancelacion',
    cuentaCargo:      '4200010002',  // Devoluciones s/Ventas 0%
    cuentaAbono:      '1102011005',  // Bancos
    cuentaIva:        null,
    prioridad:        77,
  },

  // ── CC5. APLICACIÓN DE ANTICIPO (prio 78) ─────────────────────────────────
  // Fallback para NCs con 'anticipo' en descripción que NO tengan tipoRelacion=07.
  // Cuando tipoRelacion=07 está presente, Reg 23 (prio 90) también matchea, pero
  // CC-ANT tiene prioridad=78 < 90 → CC-ANT gana. Las cuentas son idénticas
  // a Reg 23 → mismo asiento contable, sin impacto.
  // Motor: cuentaIvaAnticipo activa la lógica de swap IVA-diferido → IVA-definitivo.
  {
    nombre:            'Reg CC-ANT — NC Aplicación de Anticipo (por descripción)',
    tipoComprobante:   'E',
    conceptoContiene:  'anticipo',
    cuentaCargo:       '2103010001',  // Anticipos De Clientes General (cancela pasivo)
    cuentaAbono:       '1103010001',  // Clientes Nac Gral 16% (reduce CxC)
    cuentaIva:         '2104010001',  // IVA Trasladado definitivo (HABER)
    cuentaIvaAnticipo: '2104010002',  // IVA Trasladado Anticipos (DEBE — cancela diferido)
    prioridad:         78,
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
    conceptoContiene: null,
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
    conceptoContiene: null,
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
    conceptoContiene: null,
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
    conceptoContiene: null,
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
    conceptoContiene: null,
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
    conceptoContiene: null,
    prioridad:       84,
  },

  // ── 11A-PPD. DEVOLUCIÓN NC PPD (Reglas 8PPD, 17PPD) ─────────────────────────
  // metodoPago='PPD': la NC cancela una CxC abierta en cartera (factura PPD no cobrada).
  // NO hay movimiento de efectivo → cuentaAbono = Clientes (extingue CxC), NO Bancos.
  // IVA PPD: el diferido (2105010001) se cancela al DEBE porque nunca se causó definitivamente.
  // Asiento NC-PPD 16%: DEBE Devoluciones (subtotal) + DEBE 2105010001 (IVA diferido) = HABER Clientes (total)
  {
    nombre:          'Reg 8PPD — NC PPD Devolución 16% (cancela CxC pendiente)',
    tipoComprobante: 'E',
    metodoPago:      'PPD',
    tipoRelacion:    '01',
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '4200010001',   // Devoluciones s/Ventas 16%
    cuentaAbono:     '1103010001',   // Clientes 16% (extingue CxC — sin efectivo)
    cuentaIva:       '2104010001',   // IVA Trasladado definitivo (PUE fallback)
    cuentaIvaPPD:    '2105010001',   // IVA Por Trasladar PPD (cancela diferido)
    conceptoContiene: null,
    prioridad:       84,
  },
  {
    nombre:          'Reg 17PPD — NC PPD Devolución 0% (cancela CxC pendiente)',
    tipoComprobante: 'E',
    metodoPago:      'PPD',
    tipoRelacion:    '01',
    tasaIva:         '0',
    tieneDescuento:  false,
    cuentaCargo:     '4200010002',   // Devoluciones s/Ventas 0%
    cuentaAbono:     '1103010002',   // Clientes 0% (extingue CxC — sin efectivo)
    cuentaIva:       null,
    cuentaIvaPPD:    null,
    conceptoContiene: null,
    prioridad:       85,
  },
  {
    // Devolución de CFDI mixto (0%+16%) PPD: cancela CxC sin movimiento de efectivo.
    // cuentaAbono2 = Devoluciones 0% (DEBE motor mixto E). HABER unificado a Clientes 16%
    // porque el motor tipo E no soporta aún split de HABER entre Clientes 16% y 0%.
    nombre:          'Reg 8PPD-M — NC PPD Devolución Mixta (cancela CxC pendiente)',
    tipoComprobante: 'E',
    metodoPago:      'PPD',
    tipoRelacion:    '01',
    tasaIva:         'mixto',
    tieneDescuento:  false,
    cuentaCargo:     '4200010001',   // Devoluciones s/Ventas 16%
    cuentaAbono:     '1103010001',   // Clientes 16% (extingue CxC)
    cuentaAbono2:    '4200010002',   // Devoluciones s/Ventas 0% (motor mixto E)
    cuentaIva:       '2104010001',   // IVA Trasladado (PUE fallback)
    cuentaIvaPPD:    '2105010001',   // IVA Por Trasladar PPD (cancela diferido)
    conceptoContiene: null,
    prioridad:       84,
  },

  // ── 11B. CONDONACIÓN formaPago=15 (Reglas 8A-15, 8X-15, 17-15) ──────────────
  // formaPago='15' = "Condonación" en catálogo SAT. El vendedor perdona la deuda
  // sin movimiento de efectivo → cuentaAbono = Clientes (no Bancos).
  // Compiten con Reg 8A-8F (mismo prioridad) pero formaPago='15' no está cubierto
  // por ninguna de ellas → estas reglas son las únicas que matchean.
  // IVA PUE (Reg 8A-15): el IVA causó definitivamente al emitir la PUE → cancela 2104010001.
  // IVA PPD (Reg 8X-15): el IVA nunca se causó (diferido) → cancela 2105010001.
  // Art. 11 LIVA: IVA PUE se causa en el momento de la enajenación, no al cobro.
  {
    nombre:          'Reg 8A-15 — NC PUE Condonación Efectivo 16%',
    tipoComprobante: 'E',
    metodoPago:      'PUE',
    formaPago:       '15',
    tipoRelacion:    '01',
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '4200020001',  // Descuentos s/Ventas 16% (pérdida por condonar)
    cuentaAbono:     '1103010001',  // Clientes 16% (cancela CxC — sin efectivo)
    cuentaIva:       '2104010001',  // IVA Trasladado definitivo (PUE ya causó al emitir)
    cuentaIvaPPD:    '2105010001',  // IVA Por Trasladar PPD (fallback si esPPD=true)
    conceptoContiene: null,
    prioridad:       80,
  },
  {
    // Fallback para tipoRelacion distinto de '01' (ej. '03', '04') con formaPago=15
    nombre:          'Reg 8X-15 — NC PUE Condonación 16% (Fallback tipoRelacion)',
    tipoComprobante: 'E',
    metodoPago:      'PUE',
    formaPago:       '15',
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '4200020001',
    cuentaAbono:     '1103010001',
    cuentaIva:       '2104010001',  // IVA Trasladado definitivo (PUE ya causó al emitir)
    cuentaIvaPPD:    '2105010001',
    conceptoContiene: null,
    prioridad:       85,
  },
  {
    nombre:          'Reg 17-15 — NC Condonación Tasa 0%',
    tipoComprobante: 'E',
    formaPago:       '15',
    tasaIva:         '0',
    tieneDescuento:  false,
    cuentaCargo:     '4200020002',  // Descuentos s/Ventas 0%
    cuentaAbono:     '1103010002',  // Clientes 0%
    cuentaIva:       null,
    conceptoContiene: null,
    prioridad:       85,
  },

  // ── 12. DEVOLUCIONES NC POR TASA (Reglas 17–19) ───────────────────────────
  // Fallback para E+tipoRelacion=01. Variantes *A (formaPago=01) ganan por spec.
  // tasaIva discrimina la ruta: 0% → Reg 17, mixto → Reg 18, descuento → Reg 19.
  {
    nombre:          'Reg 17A — NC Devolución Tasa 0% Efectivo',
    tipoComprobante: 'E',
    tipoRelacion:    '01',
    formaPago:       '01',
    tasaIva:         '0',
    tieneDescuento:  false,
    cuentaCargo:     '4200010002',  // Devoluciones s/Ventas 0% (fix: devolución ≠ descuento)
    cuentaAbono:     '1101010003',  // Caja (formaPago=01 → efectivo)
    cuentaIva:       null,
    conceptoContiene: null,
    prioridad:       85,
  },
  {
    nombre:          'Reg 17 — NC Devolución Tasa 0%',
    tipoComprobante: 'E',
    tipoRelacion:    '01',
    tasaIva:         '0',
    tieneDescuento:  false,         // NCs 0% con descuento → Reg 19B/19C
    cuentaCargo:     '4200010002',  // Devoluciones s/Ventas 0% (fix: devolución ≠ descuento)
    cuentaAbono:     '1102011005',  // Bancos
    cuentaIva:       null,
    conceptoContiene: null,
    prioridad:       85,
  },
  {
    nombre:          'Reg 18A — NC Devolución Mixta Efectivo (0%+16%)',
    tipoComprobante: 'E',
    tipoRelacion:    '01',
    formaPago:       '01',
    tasaIva:         'mixto',
    cuentaCargo:     '4200010001',  // Devoluciones s/Ventas 16% (partida principal)
    cuentaAbono:     '1101010003',  // Caja (formaPago=01 → efectivo)
    cuentaAbono2:    '4200010002',  // Devoluciones s/Ventas 0% (motor)
    cuentaIva:       '2104010001',  // IVA 16% a cancelar
    conceptoContiene: null,
    prioridad:       86,
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
    conceptoContiene: null,
    prioridad:       86,
  },
  {
    nombre:          'Reg 19 — NC Devolución sobre Descuento',
    tipoComprobante: 'E',
    tipoRelacion:    '01',
    tieneDescuento:  true,
    cuentaCargo:     '4200020001',  // Descuentos s/Ventas 16% (DEBE — registra el descuento adicional)
    cuentaAbono:     '1103010001',  // Clientes Nac Gral 16% (HABER — reduce la CxC del cliente)
    cuentaIva:       '2104010001',  // IVA Trasladado (DEBE — reduce el IVA causado)
    conceptoContiene: null,
    prioridad:       87,
  },
  {
    // NC tasa 0% con descuento, efectivo: igual que 19B pero abono=Caja.
    nombre:          'Reg 19C — NC Devolución Tasa 0% con Descuento Efectivo',
    tipoComprobante: 'E',
    tipoRelacion:    '01',
    formaPago:       '01',
    tasaIva:         '0',
    tieneDescuento:  true,
    cuentaCargo:     '4200010002',   // Devoluciones s/Ventas 0%
    cuentaAbono:     '1101010003',   // Caja (formaPago=01 → efectivo)
    cuentaDescuento: '4200020002',   // Descuentos s/Ventas 0% (HABER — cancela el descuento original)
    cuentaIva:       null,
    conceptoContiene: null,
    prioridad:       86,
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
    conceptoContiene: null,
    prioridad:       86,
  },

  // ── 12B. DEVOLUCIONES MERCANCÍA tipoRelacion=03 (Reglas 8A-3 … 8X-3) ──────
  // tipoRelacion='03' = "Devolución de mercancía sobre facturas o traslados previos".
  // Mismo tratamiento contable que tipoRelacion='01' (devolución normal):
  //   DEBE Devoluciones, HABER Bancos/Caja, HABER IVA.
  // Prioridades idénticas a sus equivalentes '01' para no alterar el ranking.
  {
    nombre:          'Reg 8A-3 — NC PUE Devolución Mercancía Efectivo 16%',
    tipoComprobante: 'E',
    metodoPago:      'PUE',
    formaPago:       '01',
    tipoRelacion:    '03',
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '4200010001',  // Devoluciones s/Ventas 16%
    cuentaAbono:     '1101010003',  // Caja (formaPago=01 → efectivo)
    cuentaIva:       '2104010001',
    conceptoContiene: null,
    prioridad:       80,
  },
  {
    nombre:          'Reg 8B-3 — NC PUE Devolución Mercancía Transferencia 16%',
    tipoComprobante: 'E',
    metodoPago:      'PUE',
    formaPago:       '03',
    tipoRelacion:    '03',
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '4200010001',
    cuentaAbono:     '1102011005',  // Bancos
    cuentaIva:       '2104010001',
    conceptoContiene: null,
    prioridad:       81,
  },
  {
    nombre:          'Reg 8C-3 — NC PUE Devolución Mercancía Cheque 16%',
    tipoComprobante: 'E',
    metodoPago:      'PUE',
    formaPago:       '04',
    tipoRelacion:    '03',
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '4200010001',
    cuentaAbono:     '1102011005',
    cuentaIva:       '2104010001',
    conceptoContiene: null,
    prioridad:       82,
  },
  {
    nombre:          'Reg 8D-3 — NC PUE Devolución Mercancía Tarjeta Débito 16%',
    tipoComprobante: 'E',
    metodoPago:      'PUE',
    formaPago:       '28',
    tipoRelacion:    '03',
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '4200010001',
    cuentaAbono:     '1102011005',
    cuentaIva:       '2104010001',
    conceptoContiene: null,
    prioridad:       83,
  },
  {
    // Fallback 16% para formaPago no especificado (incluye '99', '30', etc.)
    nombre:          'Reg 8X-3 — NC PUE Devolución Mercancía 16% (Fallback)',
    tipoComprobante: 'E',
    metodoPago:      'PUE',
    formaPago:       null,
    tipoRelacion:    '03',
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '4200010001',
    cuentaAbono:     '1102011005',
    cuentaIva:       '2104010001',
    conceptoContiene: null,
    prioridad:       85,
  },
  {
    nombre:          'Reg 17-3 — NC Devolución Mercancía Tasa 0%',
    tipoComprobante: 'E',
    tipoRelacion:    '03',
    tasaIva:         '0',
    tieneDescuento:  false,
    cuentaCargo:     '4200010002',  // Devoluciones s/Ventas 0%
    cuentaAbono:     '1102011005',
    cuentaIva:       null,
    conceptoContiene: null,
    prioridad:       85,
  },
  {
    // NC PPD tipoRelacion='03': devolución de mercancía contra factura PPD no cobrada.
    // Sin movimiento de efectivo → HABER a Clientes (extingue CxC), no a Bancos.
    nombre:          'Reg 8PPD-3 — NC PPD Devolución Mercancía 16% (tipoRelacion 03)',
    tipoComprobante: 'E',
    metodoPago:      'PPD',
    tipoRelacion:    '03',
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '4200010001',  // Devoluciones s/Ventas 16%
    cuentaAbono:     '1103010001',  // Clientes 16% (extingue CxC — sin efectivo)
    cuentaIva:       '2104010001',  // IVA Trasladado (PUE fallback)
    cuentaIvaPPD:    '2105010001',  // IVA Por Trasladar PPD (cancela diferido)
    conceptoContiene: null,
    prioridad:       84,
  },
  {
    nombre:          'Reg 17PPD-3 — NC PPD Devolución Mercancía 0% (tipoRelacion 03)',
    tipoComprobante: 'E',
    metodoPago:      'PPD',
    tipoRelacion:    '03',
    tasaIva:         '0',
    tieneDescuento:  false,
    cuentaCargo:     '4200010002',  // Devoluciones s/Ventas 0%
    cuentaAbono:     '1103010002',  // Clientes 0% (extingue CxC — sin efectivo)
    cuentaIva:       null,
    cuentaIvaPPD:    null,
    conceptoContiene: null,
    prioridad:       85,
  },

  // ── 11C. CONDONACIÓN formaPago=15 + tipoRelacion=03/04 ───────────────────────
  // Estos CFDIs tienen tipoRelacion='03' (devolución mercancía) o '04' (sustitución)
  // Y formaPago='15' (condonación — sin efectivo). Sin estas reglas, Reg 8X-3/17-3
  // los capturan antes (formaPago=null) y los mandan a Bancos HABER en vez de Clientes.
  // Prioridad=80 + formaPago especificado → más específicas que Reg 8X-3 (formaPago=null, prio=85).
  {
    nombre:          'Reg 8X-15-3 — Condonación Mercancía 16% (tipoRelacion=03, formaPago=15)',
    tipoComprobante: 'E',
    metodoPago:      'PUE',
    formaPago:       '15',
    tipoRelacion:    '03',
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '4200020001',  // Descuentos s/Ventas 16%
    cuentaAbono:     '1103010001',  // Clientes (sin efectivo)
    cuentaIva:       '2105010001',  // IVA Por Trasladar (condonación = diferido)
    cuentaIvaPPD:    '2105010001',
    conceptoContiene: null,
    prioridad:       80,
  },
  {
    nombre:          'Reg 17-15-3 — Condonación Mercancía 0% (tipoRelacion=03, formaPago=15)',
    tipoComprobante: 'E',
    formaPago:       '15',
    tipoRelacion:    '03',
    tasaIva:         '0',
    tieneDescuento:  false,
    cuentaCargo:     '4200020002',  // Descuentos s/Ventas 0%
    cuentaAbono:     '1103010002',  // Clientes 0%
    cuentaIva:       null,
    conceptoContiene: null,
    prioridad:       85,
  },
  {
    nombre:          'Reg 8X-15-4 — Condonación Sustitución 16% (tipoRelacion=04, formaPago=15)',
    tipoComprobante: 'E',
    formaPago:       '15',
    tipoRelacion:    '04',
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '4200020001',
    cuentaAbono:     '1103010001',
    cuentaIva:       '2105010001',  // IVA Por Trasladar (condonación = diferido)
    cuentaIvaPPD:    '2105010001',
    conceptoContiene: null,
    prioridad:       80,
  },

  // ── 12C. SUSTITUCIÓN DE CFDI tipoRelacion=04 (Reglas 8A-4 … 8X-4) ─────────
  // tipoRelacion='04' = "Sustitución de los CFDI previos".
  // Contablemente idéntico a devolución: cancela el CFDI anterior.
  {
    nombre:          'Reg 8A-4 — NC PUE Sustitución CFDI Efectivo 16%',
    tipoComprobante: 'E',
    metodoPago:      'PUE',
    formaPago:       '01',
    tipoRelacion:    '04',
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '4200010001',
    cuentaAbono:     '1101010003',  // Caja
    cuentaIva:       '2104010001',
    conceptoContiene: null,
    prioridad:       80,
  },
  {
    // Fallback 16% para el resto de formas de pago (formaPago=15, etc.)
    nombre:          'Reg 8X-4 — NC PUE Sustitución CFDI 16% (Fallback)',
    tipoComprobante: 'E',
    tipoRelacion:    '04',
    tasaIva:         '16',
    tieneDescuento:  false,
    cuentaCargo:     '4200010001',
    cuentaAbono:     '1102011005',
    cuentaIva:       '2104010001',
    conceptoContiene: null,
    prioridad:       85,
  },

  // ── 12D. CONDONACIÓN tipoRelacion=15 (Reglas 15A + 15B) ──────────────────
  // tipoRelacion='15' = "Condonación". El vendedor perdona una deuda PPD no cobrada.
  // No hay movimiento de efectivo → cuentaAbono es Clientes (reduce CxC), NO Bancos.
  // El IVA que se cancela es el DIFERIDO (2105010001 = IVA Por Trasladar PPD),
  // porque el CFDI original era PPD y nunca se cobró → nunca se causó el definitivo.
  // Asiento E condonación:
  //   DEBE  Descuentos s/Ventas (subtotal condonado)
  //   HABER IVA Por Trasladar PPD (IVA diferido que se extingue)
  //   HABER Clientes (total condonado — liquida la CxC)
  {
    // Condonación PUE: IVA ya causado vive en 2104010001 (definitivo).
    // Condonación PPD: IVA diferido vive en 2105010001 — motor elige según cfdi.metodoPago.
    // cuentaAbono = Clientes (no Bancos) porque no hay movimiento de efectivo.
    // Asiento: DEBE Descuentos (subtotal) + DEBE IVA (2104010001 PUE / 2105010001 PPD)
    //          HABER Clientes (total)  → cuadra exacto: subtotal + IVA = total ✓
    nombre:          'Reg 15A — Condonación Tasa 16%',
    tipoComprobante: 'E',
    tipoRelacion:    '15',
    tasaIva:         '16',
    cuentaCargo:     '4200020001',  // Descuentos s/Ventas 16% (pérdida por condonación)
    cuentaAbono:     '1103010001',  // Clientes 16% (extingue CxC — sin efectivo)
    cuentaIva:       '2104010001',  // IVA Trasladado definitivo (PUE → cancela causado)
    cuentaIvaPPD:    '2105010001',  // IVA Por Trasladar PPD (PPD → cancela diferido)
    conceptoContiene: null,
    prioridad:       87,
  },
  {
    nombre:          'Reg 15B — Condonación Tasa 0%',
    tipoComprobante: 'E',
    tipoRelacion:    '15',
    tasaIva:         '0',
    cuentaCargo:     '4200020002',  // Descuentos s/Ventas 0%
    cuentaAbono:     '1103010002',  // Clientes 0%
    cuentaIva:       null,
    cuentaIvaPPD:    null,
    conceptoContiene: null,
    prioridad:       87,
  },

  // ── 13. BONIFICACIONES NC RETROACTIVAS (Reglas 20–21) ────────────────────
  // Rappel anual / descuento retroactivo. Cargo a Descuentos sobre Ventas.
  // Variantes *A (formaPago=01) ganan por spec sobre fallback (formaPago=null).
  {
    nombre:          'Reg 20A — Bonificación s/Ventas 16% Efectivo (NC retroactiva)',
    tipoComprobante: 'E',
    tipoRelacion:    '01',
    formaPago:       '01',
    tasaIva:         '16',
    cuentaCargo:     '4200020001',  // Descuentos sobre Ventas 16%
    cuentaAbono:     '1101010003',  // Caja (formaPago=01 → efectivo)
    cuentaIva:       '2104010001',
    conceptoContiene: null,
    prioridad:       88,
  },
  {
    nombre:          'Reg 20 — Bonificación s/Ventas 16% (NC retroactiva)',
    tipoComprobante: 'E',
    tipoRelacion:    '01',
    tasaIva:         '16',
    cuentaCargo:     '4200020001',  // Descuentos sobre Ventas 16%
    cuentaAbono:     '1102011005',  // Bancos
    cuentaIva:       '2104010001',
    conceptoContiene: null,
    prioridad:       88,
  },
  {
    nombre:          'Reg 21A — Bonificación s/Ventas Tasa 0% Efectivo (NC retroactiva)',
    tipoComprobante: 'E',
    tipoRelacion:    '01',
    formaPago:       '01',
    tasaIva:         '0',
    cuentaCargo:     '4200020002',  // Descuentos sobre Ventas 0%
    cuentaAbono:     '1101010003',  // Caja (formaPago=01 → efectivo)
    cuentaIva:       null,
    conceptoContiene: null,
    prioridad:       89,
  },
  {
    nombre:          'Reg 21 — Bonificación s/Ventas Tasa 0% (NC retroactiva)',
    tipoComprobante: 'E',
    tipoRelacion:    '01',
    tasaIva:         '0',
    cuentaCargo:     '4200020002',  // Descuentos sobre Ventas 0%
    cuentaAbono:     '1102011005',  // Bancos
    cuentaIva:       null,
    conceptoContiene: null,
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
    conceptoContiene: null,
    prioridad:         90,
  },

  // ── 15. SALDO A FAVOR DEL CLIENTE ────────────────────────────────────────────
  // Ruta activa: Reg 24A-17 / 25A-17 (empresa-específicas, formaPago='17' compensación).
  // Reg 24A / 25A eliminadas — eran dead rules: Reg 20 (prio 88) y Reg 21 (prio 89)
  // tienen los mismos criterios con prioridad menor y siempre ganaban antes.
  // El ERP debe estampar formaPago='17' en NCs de saldo a favor para activar la ruta correcta.

  // ── IC. INTERCOMPAÑÍAS (Reg IC-I-PUE, IC-I-PPD, IC-E) ───────────────────────
  // Prioridad 5 — antes que todas las reglas genéricas.
  // rfcReceptor filtra exactamente los 7 RFC del grupo.
  // Cuentas destino:
  //   4100030001  Ingresos Intercompañías 16%
  //   4100030002  Ingresos Intercompañías 0%
  //   4200030001  Devoluciones s/Ventas Intercompañías 16%
  //   1103020001  Clientes IC (CxC PPD — simétrico con cuentaAbono de IC-P cobros)
  // ─────────────────────────────────────────────────────────────────────────────

  // GAAA5403026G2 — Alberto Neftali Garcia Arango (Física)
  {
    nombre:          'Reg IC-I-PUE — Ingreso Intercompañía PUE (GAAA5403026G2)',
    tipoComprobante: 'I', metodoPago: 'PUE', rfcReceptor: 'GAAA5403026G2',
    cuentaCargo: '1102011005', cuentaAbono: '4100030001', cuentaIva: '2104010001', prioridad: 5,
  },
  {
    nombre:          'Reg IC-I-PUE-0 — Ingreso IC PUE Tasa 0% (GAAA5403026G2)',
    tipoComprobante: 'I', metodoPago: 'PUE', rfcReceptor: 'GAAA5403026G2', tasaIva: '0',
    cuentaCargo: '1102011005', cuentaAbono: '4100030002', prioridad: 5,
  },
  {
    nombre:          'Reg IC-I-PPD — Ingreso Intercompañía PPD (GAAA5403026G2)',
    tipoComprobante: 'I', metodoPago: 'PPD', rfcReceptor: 'GAAA5403026G2',
    cuentaCargo: '1103020001', cuentaAbono: '4100030001', cuentaIva: '2104010001', cuentaIvaPPD: '2105010001', prioridad: 5,
  },
  {
    nombre:          'Reg IC-I-PPD-0 — Ingreso IC PPD Tasa 0% (GAAA5403026G2)',
    tipoComprobante: 'I', metodoPago: 'PPD', rfcReceptor: 'GAAA5403026G2', tasaIva: '0',
    cuentaCargo: '1103020001', cuentaAbono: '4100030002', prioridad: 5,
  },
  {
    nombre:          'Reg IC-E-PPD — NC Devolución Intercompañía PPD (GAAA5403026G2)',
    tipoComprobante: 'E', metodoPago: 'PPD', rfcReceptor: 'GAAA5403026G2',
    cuentaCargo: '4200030001', cuentaAbono: '1103020001', cuentaIvaPPD: '2105010001', prioridad: 5,
  },
  {
    nombre:          'Reg IC-E — NC Devolución Intercompañía (GAAA5403026G2)',
    tipoComprobante: 'E', rfcReceptor: 'GAAA5403026G2',
    cuentaCargo: '4200030001', cuentaAbono: '1102011005', cuentaIva: '2104010001', prioridad: 5,
  },

  // GAFA850630542 — Alberto Neftali Garcia Fernandez del Campo (Física)
  {
    nombre:          'Reg IC-I-PUE — Ingreso Intercompañía PUE (GAFA850630542)',
    tipoComprobante: 'I', metodoPago: 'PUE', rfcReceptor: 'GAFA850630542',
    cuentaCargo: '1102011005', cuentaAbono: '4100030001', cuentaIva: '2104010001', prioridad: 5,
  },
  {
    nombre:          'Reg IC-I-PUE-0 — Ingreso IC PUE Tasa 0% (GAFA850630542)',
    tipoComprobante: 'I', metodoPago: 'PUE', rfcReceptor: 'GAFA850630542', tasaIva: '0',
    cuentaCargo: '1102011005', cuentaAbono: '4100030002', prioridad: 5,
  },
  {
    nombre:          'Reg IC-I-PPD — Ingreso Intercompañía PPD (GAFA850630542)',
    tipoComprobante: 'I', metodoPago: 'PPD', rfcReceptor: 'GAFA850630542',
    cuentaCargo: '1103020001', cuentaAbono: '4100030001', cuentaIva: '2104010001', cuentaIvaPPD: '2105010001', prioridad: 5,
  },
  {
    nombre:          'Reg IC-I-PPD-0 — Ingreso IC PPD Tasa 0% (GAFA850630542)',
    tipoComprobante: 'I', metodoPago: 'PPD', rfcReceptor: 'GAFA850630542', tasaIva: '0',
    cuentaCargo: '1103020001', cuentaAbono: '4100030002', prioridad: 5,
  },
  {
    nombre:          'Reg IC-E-PPD — NC Devolución Intercompañía PPD (GAFA850630542)',
    tipoComprobante: 'E', metodoPago: 'PPD', rfcReceptor: 'GAFA850630542',
    cuentaCargo: '4200030001', cuentaAbono: '1103020001', cuentaIvaPPD: '2105010001', prioridad: 5,
  },
  {
    nombre:          'Reg IC-E — NC Devolución Intercompañía (GAFA850630542)',
    tipoComprobante: 'E', rfcReceptor: 'GAFA850630542',
    cuentaCargo: '4200030001', cuentaAbono: '1102011005', cuentaIva: '2104010001', prioridad: 5,
  },

  // AVA1002023N7 — Arrendadora de Vehiculos SA de CV (Moral)
  {
    nombre:          'Reg IC-I-PUE — Ingreso Intercompañía PUE (AVA1002023N7)',
    tipoComprobante: 'I', metodoPago: 'PUE', rfcReceptor: 'AVA1002023N7',
    cuentaCargo: '1102011005', cuentaAbono: '4100030001', cuentaIva: '2104010001', prioridad: 5,
  },
  {
    nombre:          'Reg IC-I-PUE-0 — Ingreso IC PUE Tasa 0% (AVA1002023N7)',
    tipoComprobante: 'I', metodoPago: 'PUE', rfcReceptor: 'AVA1002023N7', tasaIva: '0',
    cuentaCargo: '1102011005', cuentaAbono: '4100030002', prioridad: 5,
  },
  {
    nombre:          'Reg IC-I-PPD — Ingreso Intercompañía PPD (AVA1002023N7)',
    tipoComprobante: 'I', metodoPago: 'PPD', rfcReceptor: 'AVA1002023N7',
    cuentaCargo: '1103020001', cuentaAbono: '4100030001', cuentaIva: '2104010001', cuentaIvaPPD: '2105010001', prioridad: 5,
  },
  {
    nombre:          'Reg IC-I-PPD-0 — Ingreso IC PPD Tasa 0% (AVA1002023N7)',
    tipoComprobante: 'I', metodoPago: 'PPD', rfcReceptor: 'AVA1002023N7', tasaIva: '0',
    cuentaCargo: '1103020001', cuentaAbono: '4100030002', prioridad: 5,
  },
  {
    nombre:          'Reg IC-E-PPD — NC Devolución Intercompañía PPD (AVA1002023N7)',
    tipoComprobante: 'E', metodoPago: 'PPD', rfcReceptor: 'AVA1002023N7',
    cuentaCargo: '4200030001', cuentaAbono: '1103020001', cuentaIvaPPD: '2105010001', prioridad: 5,
  },
  {
    nombre:          'Reg IC-E — NC Devolución Intercompañía (AVA1002023N7)',
    tipoComprobante: 'E', rfcReceptor: 'AVA1002023N7',
    cuentaCargo: '4200030001', cuentaAbono: '1102011005', cuentaIva: '2104010001', prioridad: 5,
  },

  // GIN121109RX4 — Gane Inmobiliaria SA de CV (Moral)
  {
    nombre:          'Reg IC-I-PUE — Ingreso Intercompañía PUE (GIN121109RX4)',
    tipoComprobante: 'I', metodoPago: 'PUE', rfcReceptor: 'GIN121109RX4',
    cuentaCargo: '1102011005', cuentaAbono: '4100030001', cuentaIva: '2104010001', prioridad: 5,
  },
  {
    nombre:          'Reg IC-I-PUE-0 — Ingreso IC PUE Tasa 0% (GIN121109RX4)',
    tipoComprobante: 'I', metodoPago: 'PUE', rfcReceptor: 'GIN121109RX4', tasaIva: '0',
    cuentaCargo: '1102011005', cuentaAbono: '4100030002', prioridad: 5,
  },
  {
    nombre:          'Reg IC-I-PPD — Ingreso Intercompañía PPD (GIN121109RX4)',
    tipoComprobante: 'I', metodoPago: 'PPD', rfcReceptor: 'GIN121109RX4',
    cuentaCargo: '1103020001', cuentaAbono: '4100030001', cuentaIva: '2104010001', cuentaIvaPPD: '2105010001', prioridad: 5,
  },
  {
    nombre:          'Reg IC-I-PPD-0 — Ingreso IC PPD Tasa 0% (GIN121109RX4)',
    tipoComprobante: 'I', metodoPago: 'PPD', rfcReceptor: 'GIN121109RX4', tasaIva: '0',
    cuentaCargo: '1103020001', cuentaAbono: '4100030002', prioridad: 5,
  },
  {
    nombre:          'Reg IC-E-PPD — NC Devolución Intercompañía PPD (GIN121109RX4)',
    tipoComprobante: 'E', metodoPago: 'PPD', rfcReceptor: 'GIN121109RX4',
    cuentaCargo: '4200030001', cuentaAbono: '1103020001', cuentaIvaPPD: '2105010001', prioridad: 5,
  },
  {
    nombre:          'Reg IC-E — NC Devolución Intercompañía (GIN121109RX4)',
    tipoComprobante: 'E', rfcReceptor: 'GIN121109RX4',
    cuentaCargo: '4200030001', cuentaAbono: '1102011005', cuentaIva: '2104010001', prioridad: 5,
  },

  // KTE180215FE1 — Kore Tecnologia SA de CV (Moral)
  {
    nombre:          'Reg IC-I-PUE — Ingreso Intercompañía PUE (KTE180215FE1)',
    tipoComprobante: 'I', metodoPago: 'PUE', rfcReceptor: 'KTE180215FE1',
    cuentaCargo: '1102011005', cuentaAbono: '4100030001', cuentaIva: '2104010001', prioridad: 5,
  },
  {
    nombre:          'Reg IC-I-PUE-0 — Ingreso IC PUE Tasa 0% (KTE180215FE1)',
    tipoComprobante: 'I', metodoPago: 'PUE', rfcReceptor: 'KTE180215FE1', tasaIva: '0',
    cuentaCargo: '1102011005', cuentaAbono: '4100030002', prioridad: 5,
  },
  {
    nombre:          'Reg IC-I-PPD — Ingreso Intercompañía PPD (KTE180215FE1)',
    tipoComprobante: 'I', metodoPago: 'PPD', rfcReceptor: 'KTE180215FE1',
    cuentaCargo: '1103020001', cuentaAbono: '4100030001', cuentaIva: '2104010001', cuentaIvaPPD: '2105010001', prioridad: 5,
  },
  {
    nombre:          'Reg IC-I-PPD-0 — Ingreso IC PPD Tasa 0% (KTE180215FE1)',
    tipoComprobante: 'I', metodoPago: 'PPD', rfcReceptor: 'KTE180215FE1', tasaIva: '0',
    cuentaCargo: '1103020001', cuentaAbono: '4100030002', prioridad: 5,
  },
  {
    nombre:          'Reg IC-E-PPD — NC Devolución Intercompañía PPD (KTE180215FE1)',
    tipoComprobante: 'E', metodoPago: 'PPD', rfcReceptor: 'KTE180215FE1',
    cuentaCargo: '4200030001', cuentaAbono: '1103020001', cuentaIvaPPD: '2105010001', prioridad: 5,
  },
  {
    nombre:          'Reg IC-E — NC Devolución Intercompañía (KTE180215FE1)',
    tipoComprobante: 'E', rfcReceptor: 'KTE180215FE1',
    cuentaCargo: '4200030001', cuentaAbono: '1102011005', cuentaIva: '2104010001', prioridad: 5,
  },

  // FEUL5811155D9 — Luz Maria Fernandez del Campo Urzua (Física)
  {
    nombre:          'Reg IC-I-PUE — Ingreso Intercompañía PUE (FEUL5811155D9)',
    tipoComprobante: 'I', metodoPago: 'PUE', rfcReceptor: 'FEUL5811155D9',
    cuentaCargo: '1102011005', cuentaAbono: '4100030001', cuentaIva: '2104010001', prioridad: 5,
  },
  {
    nombre:          'Reg IC-I-PUE-0 — Ingreso IC PUE Tasa 0% (FEUL5811155D9)',
    tipoComprobante: 'I', metodoPago: 'PUE', rfcReceptor: 'FEUL5811155D9', tasaIva: '0',
    cuentaCargo: '1102011005', cuentaAbono: '4100030002', prioridad: 5,
  },
  {
    nombre:          'Reg IC-I-PPD — Ingreso Intercompañía PPD (FEUL5811155D9)',
    tipoComprobante: 'I', metodoPago: 'PPD', rfcReceptor: 'FEUL5811155D9',
    cuentaCargo: '1103020001', cuentaAbono: '4100030001', cuentaIva: '2104010001', cuentaIvaPPD: '2105010001', prioridad: 5,
  },
  {
    nombre:          'Reg IC-I-PPD-0 — Ingreso IC PPD Tasa 0% (FEUL5811155D9)',
    tipoComprobante: 'I', metodoPago: 'PPD', rfcReceptor: 'FEUL5811155D9', tasaIva: '0',
    cuentaCargo: '1103020001', cuentaAbono: '4100030002', prioridad: 5,
  },
  {
    nombre:          'Reg IC-E-PPD — NC Devolución Intercompañía PPD (FEUL5811155D9)',
    tipoComprobante: 'E', metodoPago: 'PPD', rfcReceptor: 'FEUL5811155D9',
    cuentaCargo: '4200030001', cuentaAbono: '1103020001', cuentaIvaPPD: '2105010001', prioridad: 5,
  },
  {
    nombre:          'Reg IC-E — NC Devolución Intercompañía (FEUL5811155D9)',
    tipoComprobante: 'E', rfcReceptor: 'FEUL5811155D9',
    cuentaCargo: '4200030001', cuentaAbono: '1102011005', cuentaIva: '2104010001', prioridad: 5,
  },

  // RSI051018GL6 — Red de Servicios a Inmuebles SA (Moral)
  {
    nombre:          'Reg IC-I-PUE — Ingreso Intercompañía PUE (RSI051018GL6)',
    tipoComprobante: 'I', metodoPago: 'PUE', rfcReceptor: 'RSI051018GL6',
    cuentaCargo: '1102011005', cuentaAbono: '4100030001', cuentaIva: '2104010001', prioridad: 5,
  },
  {
    nombre:          'Reg IC-I-PUE-0 — Ingreso IC PUE Tasa 0% (RSI051018GL6)',
    tipoComprobante: 'I', metodoPago: 'PUE', rfcReceptor: 'RSI051018GL6', tasaIva: '0',
    cuentaCargo: '1102011005', cuentaAbono: '4100030002', prioridad: 5,
  },
  {
    nombre:          'Reg IC-I-PPD — Ingreso Intercompañía PPD (RSI051018GL6)',
    tipoComprobante: 'I', metodoPago: 'PPD', rfcReceptor: 'RSI051018GL6',
    cuentaCargo: '1103020001', cuentaAbono: '4100030001', cuentaIva: '2104010001', cuentaIvaPPD: '2105010001', prioridad: 5,
  },
  {
    nombre:          'Reg IC-I-PPD-0 — Ingreso IC PPD Tasa 0% (RSI051018GL6)',
    tipoComprobante: 'I', metodoPago: 'PPD', rfcReceptor: 'RSI051018GL6', tasaIva: '0',
    cuentaCargo: '1103020001', cuentaAbono: '4100030002', prioridad: 5,
  },
  {
    nombre:          'Reg IC-E-PPD — NC Devolución Intercompañía PPD (RSI051018GL6)',
    tipoComprobante: 'E', metodoPago: 'PPD', rfcReceptor: 'RSI051018GL6',
    cuentaCargo: '4200030001', cuentaAbono: '1103020001', cuentaIvaPPD: '2105010001', prioridad: 5,
  },
  {
    nombre:          'Reg IC-E — NC Devolución Intercompañía (RSI051018GL6)',
    tipoComprobante: 'E', rfcReceptor: 'RSI051018GL6',
    cuentaCargo: '4200030001', cuentaAbono: '1102011005', cuentaIva: '2104010001', prioridad: 5,
  },

  // ── IC-P. COBROS INTERCOMPAÑÍAS (tipo P, rfcReceptor = IC) ──────────────────
  // Se activan cuando TIH u otra entidad del grupo EMITE el CFDI P y el RFC pagador
  // (receptor) es uno de los 7 RFC intercompañía. Captura cobros PPD a IC.
  // tasaIva '16' → CP 2.0 con <Totales> detectados.
  // tasaIva null  → CP 1.0 / Metadata sin <Totales> (fallback).

  // GAAA5403026G2
  { nombre: 'Reg IC-P-16 — Cobro IC rfcReceptor (GAAA5403026G2)',
    tipoComprobante: 'P', rfcReceptor: 'GAAA5403026G2', tasaIva: '16',
    cuentaCargo: '1102011005', cuentaAbono: '1103020001',
    cuentaIva: '2104010001', cuentaIvaPPD: '2105010001', prioridad: 5 },
  { nombre: 'Reg IC-P-null — Cobro IC rfcReceptor sin totales (GAAA5403026G2)',
    tipoComprobante: 'P', rfcReceptor: 'GAAA5403026G2', tasaIva: null,
    cuentaCargo: '1102011005', cuentaAbono: '1103020001',
    cuentaIva: '2104010001', cuentaIvaPPD: '2105010001', prioridad: 6 },

  // GAFA850630542
  { nombre: 'Reg IC-P-16 — Cobro IC rfcReceptor (GAFA850630542)',
    tipoComprobante: 'P', rfcReceptor: 'GAFA850630542', tasaIva: '16',
    cuentaCargo: '1102011005', cuentaAbono: '1103020001',
    cuentaIva: '2104010001', cuentaIvaPPD: '2105010001', prioridad: 5 },
  { nombre: 'Reg IC-P-null — Cobro IC rfcReceptor sin totales (GAFA850630542)',
    tipoComprobante: 'P', rfcReceptor: 'GAFA850630542', tasaIva: null,
    cuentaCargo: '1102011005', cuentaAbono: '1103020001',
    cuentaIva: '2104010001', cuentaIvaPPD: '2105010001', prioridad: 6 },

  // AVA1002023N7
  { nombre: 'Reg IC-P-16 — Cobro IC rfcReceptor (AVA1002023N7)',
    tipoComprobante: 'P', rfcReceptor: 'AVA1002023N7', tasaIva: '16',
    cuentaCargo: '1102011005', cuentaAbono: '1103020001',
    cuentaIva: '2104010001', cuentaIvaPPD: '2105010001', prioridad: 5 },
  { nombre: 'Reg IC-P-null — Cobro IC rfcReceptor sin totales (AVA1002023N7)',
    tipoComprobante: 'P', rfcReceptor: 'AVA1002023N7', tasaIva: null,
    cuentaCargo: '1102011005', cuentaAbono: '1103020001',
    cuentaIva: '2104010001', cuentaIvaPPD: '2105010001', prioridad: 6 },

  // GIN121109RX4
  { nombre: 'Reg IC-P-16 — Cobro IC rfcReceptor (GIN121109RX4)',
    tipoComprobante: 'P', rfcReceptor: 'GIN121109RX4', tasaIva: '16',
    cuentaCargo: '1102011005', cuentaAbono: '1103020001',
    cuentaIva: '2104010001', cuentaIvaPPD: '2105010001', prioridad: 5 },
  { nombre: 'Reg IC-P-null — Cobro IC rfcReceptor sin totales (GIN121109RX4)',
    tipoComprobante: 'P', rfcReceptor: 'GIN121109RX4', tasaIva: null,
    cuentaCargo: '1102011005', cuentaAbono: '1103020001',
    cuentaIva: '2104010001', cuentaIvaPPD: '2105010001', prioridad: 6 },

  // KTE180215FE1
  { nombre: 'Reg IC-P-16 — Cobro IC rfcReceptor (KTE180215FE1)',
    tipoComprobante: 'P', rfcReceptor: 'KTE180215FE1', tasaIva: '16',
    cuentaCargo: '1102011005', cuentaAbono: '1103020001',
    cuentaIva: '2104010001', cuentaIvaPPD: '2105010001', prioridad: 5 },
  { nombre: 'Reg IC-P-null — Cobro IC rfcReceptor sin totales (KTE180215FE1)',
    tipoComprobante: 'P', rfcReceptor: 'KTE180215FE1', tasaIva: null,
    cuentaCargo: '1102011005', cuentaAbono: '1103020001',
    cuentaIva: '2104010001', cuentaIvaPPD: '2105010001', prioridad: 6 },

  // FEUL5811155D9
  { nombre: 'Reg IC-P-16 — Cobro IC rfcReceptor (FEUL5811155D9)',
    tipoComprobante: 'P', rfcReceptor: 'FEUL5811155D9', tasaIva: '16',
    cuentaCargo: '1102011005', cuentaAbono: '1103020001',
    cuentaIva: '2104010001', cuentaIvaPPD: '2105010001', prioridad: 5 },
  { nombre: 'Reg IC-P-null — Cobro IC rfcReceptor sin totales (FEUL5811155D9)',
    tipoComprobante: 'P', rfcReceptor: 'FEUL5811155D9', tasaIva: null,
    cuentaCargo: '1102011005', cuentaAbono: '1103020001',
    cuentaIva: '2104010001', cuentaIvaPPD: '2105010001', prioridad: 6 },

  // RSI051018GL6
  { nombre: 'Reg IC-P-16 — Cobro IC rfcReceptor (RSI051018GL6)',
    tipoComprobante: 'P', rfcReceptor: 'RSI051018GL6', tasaIva: '16',
    cuentaCargo: '1102011005', cuentaAbono: '1103020001',
    cuentaIva: '2104010001', cuentaIvaPPD: '2105010001', prioridad: 5 },
  { nombre: 'Reg IC-P-null — Cobro IC rfcReceptor sin totales (RSI051018GL6)',
    tipoComprobante: 'P', rfcReceptor: 'RSI051018GL6', tasaIva: null,
    cuentaCargo: '1102011005', cuentaAbono: '1103020001',
    cuentaIva: '2104010001', cuentaIvaPPD: '2105010001', prioridad: 6 },

  // ── 16a. FALLBACK TIPO E — NC sin clasificar ──────────────────────────────
  // Prioridad 98: captura cualquier CFDI tipo E que no matcheó ninguna regla específica.
  // Evita que caigan al comodín Reg 9, que los abonaría a Ingresos (incorrecto para NCs).
  // Abona a Clientes (no Bancos) para no generar un movimiento de efectivo ficticio.
  {
    nombre:          'Reg E-FALL — NC sin clasificar (fallback E)',
    tipoComprobante: 'E',
    cuentaCargo:     '4200010001',  // Devoluciones s/Ventas 16%
    cuentaAbono:     '1103010001',  // Clientes Nacionales (sin efectivo)
    cuentaIva:       '2104010001',
    cuentaIvaPPD:    '2105010001',
    prioridad:       98,
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
    conceptoContiene: null,
    prioridad:       99,
  },
  // ════════════════════════════════════════════════════════════════════════════
  // REGLAS EMPRESA-ESPECÍFICAS (CCO011113663 — CAR COMERCIALIZADORA SA DE CV)
  // ════════════════════════════════════════════════════════════════════════════
  // Estas reglas cubren:
  //   • TO-BON/TO-DEV/TO-CAN — Clasificación por tipoOrigen (Bonificación/Devolución/Cancelación)
  //   • CC-CLT / CLT-DEV / CLT-CAN — Programa Club Tuberos (monedero 2103090002)
  //   • Reg IC-P-16 — Cobros PPD Intercompañías (7 RFCs del grupo)
  //   • Variantes 0% de cobros PPD (Reg 7x-0) y otros fallbacks
  //   • Reg 24A-17/25A-17 — Saldo a favor con formaPago=17 (compensación)
  //   • Reg SF-TIM — Saldo a Favor Timbrado (aplicación de 2103090001)
  //
  // Para regenerar desde la BD: node src/banks/scripts/seed-cfdi-mapping-rules.js --sync
  // ════════════════════════════════════════════════════════════════════════════

  // ── REGLAS EMPRESA-ESPECÍFICAS ────────────────────────────────────────────
  {
    nombre: 'Reg IC-P-16 — Cobro PPD Intercompañía (AVA1002023N7)',
    tipoComprobante: 'P',
    rfcEmisor: 'AVA1002023N7',
    tasaIva: '16',
    cuentaCargo: '1102011005',
    cuentaAbono: '1103020001',
    cuentaIva: '2104010001',
    cuentaIvaPPD: '2105010001',
    prioridad: 5,
  },
  {
    nombre: 'Reg IC-P-16 — Cobro PPD Intercompañía (FEUL5811155D9)',
    tipoComprobante: 'P',
    rfcEmisor: 'FEUL5811155D9',
    tasaIva: '16',
    cuentaCargo: '1102011005',
    cuentaAbono: '1103020001',
    cuentaIva: '2104010001',
    cuentaIvaPPD: '2105010001',
    prioridad: 5,
  },
  {
    nombre: 'Reg IC-P-16 — Cobro PPD Intercompañía (GAAA5403026G2)',
    tipoComprobante: 'P',
    rfcEmisor: 'GAAA5403026G2',
    tasaIva: '16',
    cuentaCargo: '1102011005',
    cuentaAbono: '1103020001',
    cuentaIva: '2104010001',
    cuentaIvaPPD: '2105010001',
    prioridad: 5,
  },
  {
    nombre: 'Reg IC-P-16 — Cobro PPD Intercompañía (GAFA850630542)',
    tipoComprobante: 'P',
    rfcEmisor: 'GAFA850630542',
    tasaIva: '16',
    cuentaCargo: '1102011005',
    cuentaAbono: '1103020001',
    cuentaIva: '2104010001',
    cuentaIvaPPD: '2105010001',
    prioridad: 5,
  },
  {
    nombre: 'Reg IC-P-16 — Cobro PPD Intercompañía (GIN121109RX4)',
    tipoComprobante: 'P',
    rfcEmisor: 'GIN121109RX4',
    tasaIva: '16',
    cuentaCargo: '1102011005',
    cuentaAbono: '1103020001',
    cuentaIva: '2104010001',
    cuentaIvaPPD: '2105010001',
    prioridad: 5,
  },
  {
    nombre: 'Reg IC-P-16 — Cobro PPD Intercompañía (KTE180215FE1)',
    tipoComprobante: 'P',
    rfcEmisor: 'KTE180215FE1',
    tasaIva: '16',
    cuentaCargo: '1102011005',
    cuentaAbono: '1103020001',
    cuentaIva: '2104010001',
    cuentaIvaPPD: '2105010001',
    prioridad: 5,
  },
  {
    nombre: 'Reg IC-P-16 — Cobro PPD Intercompañía (RSI051018GL6)',
    tipoComprobante: 'P',
    rfcEmisor: 'RSI051018GL6',
    tasaIva: '16',
    cuentaCargo: '1102011005',
    cuentaAbono: '1103020001',
    cuentaIva: '2104010001',
    cuentaIvaPPD: '2105010001',
    prioridad: 5,
  },
  {
    nombre: 'Reg 22-0 — Recepción de Anticipo Tasa 0% (ClaveProdServ 84111506)',
    tipoComprobante: 'I',
    metodoPago: 'PUE',
    claveProdServ: '84111506',
    tasaIva: '0',
    cuentaCargo: '1102011005',
    cuentaAbono: '2103010001',
    prioridad: 8,
  },
  {
    nombre: 'Reg 22C-DESC — Recepción Anticipo por Descripción Efectivo',
    tipoComprobante: 'I',
    formaPago: '01',
    conceptoContiene: 'anticipo',
    cuentaCargo: '1101010003',
    cuentaAbono: '2103010001',
    cuentaIva: '2104010002',  // IVA Trasladado – Anticipos (diferido al recibir el anticipo)
    prioridad: 8,
  },
  {
    nombre: 'Reg 22C — Recepción Anticipo por Descripción',
    tipoComprobante: 'I',
    conceptoContiene: 'anticipo',
    cuentaCargo: '1102011005',
    cuentaAbono: '2103010001',
    cuentaIva: '2104010002',  // IVA Trasladado – Anticipos (diferido al recibir el anticipo)
    prioridad: 8,
  },
  {
    nombre: 'Reg 1A-X — Venta Efectivo (tasa desconocida)',
    tipoComprobante: 'I',
    metodoPago: 'PUE',
    formaPago: '01',
    cuentaCargo: '1101010003',
    cuentaAbono: '4100010001',
    cuentaIva: '2104010001',
    prioridad: 50,
  },
  {
    nombre: 'Reg 1B-X — Venta Transferencia (tasa desconocida)',
    tipoComprobante: 'I',
    metodoPago: 'PUE',
    formaPago: '03',
    cuentaCargo: '1102011005',
    cuentaAbono: '4100010001',
    cuentaIva: '2104010001',
    prioridad: 50,
  },
  {
    nombre: 'Reg 1C-X — Venta Cheque (tasa desconocida)',
    tipoComprobante: 'I',
    metodoPago: 'PUE',
    formaPago: '04',
    cuentaCargo: '1102011005',
    cuentaAbono: '4100010001',
    cuentaIva: '2104010001',
    prioridad: 50,
  },
  {
    nombre: 'Reg 1D-X — Venta Tarjeta Débito (tasa desconocida)',
    tipoComprobante: 'I',
    metodoPago: 'PUE',
    formaPago: '28',
    cuentaCargo: '1102011005',
    cuentaAbono: '4100010001',
    cuentaIva: '2104010001',
    prioridad: 50,
  },
  {
    nombre: 'Reg 1E-X — Venta Tarjeta Crédito (tasa desconocida)',
    tipoComprobante: 'I',
    metodoPago: 'PUE',
    formaPago: '29',
    cuentaCargo: '1102011005',
    cuentaAbono: '4100010001',
    cuentaIva: '2104010001',
    prioridad: 50,
  },
  {
    nombre: 'Reg 1F-X — Venta Cheque Nominativo (tasa desconocida)',
    tipoComprobante: 'I',
    metodoPago: 'PUE',
    formaPago: '02',
    cuentaCargo: '1102011005',
    cuentaAbono: '4100010001',
    cuentaIva: '2104010001',
    prioridad: 50,
  },
  {
    nombre: 'Reg 1G-X — Venta Monedero (tasa desconocida)',
    tipoComprobante: 'I',
    metodoPago: 'PUE',
    formaPago: '05',
    cuentaCargo: '2103090002',
    cuentaAbono: '4100010001',
    cuentaIva: '2104010001',
    prioridad: 50,
  },
  {
    nombre: 'Reg CLT-CAN-0 — NC Cancelación Club Tuberos 0%',
    tipoComprobante: 'E',
    tasaIva: '0',
    conceptoContiene: 'club tuberos',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010002',
    cuentaAbono: '2103090002',
    cuentaIvaAbono: '2104010002',
    prioridad: 62,
  },
  {
    nombre: 'Reg CLT-CAN-16 — NC Cancelación Club Tuberos 16%',
    tipoComprobante: 'E',
    tasaIva: '16',
    conceptoContiene: 'club tuberos',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010001',
    cuentaAbono: '2103090002',
    cuentaIva: '2104010001',
    cuentaIvaAbono: '2104010002',
    prioridad: 62,
  },
  {
    nombre: 'Reg CLT-CAN-M — NC Cancelación Club Tuberos Mixto',
    tipoComprobante: 'E',
    tasaIva: 'mixto',
    conceptoContiene: 'club tuberos',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010001',
    cuentaAbono: '2103090002',
    cuentaIva: '2104010001',
    cuentaIvaAbono: '2104010002',
    prioridad: 62,
  },
  {
    nombre: 'Reg CLT-DEV-0 — NC Devolución Club Tuberos 0%',
    tipoComprobante: 'E',
    tasaIva: '0',
    conceptoContiene: 'club tuberos',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010002',
    cuentaAbono: '2103090002',
    cuentaIvaAbono: '2104010002',
    prioridad: 62,
  },
  {
    nombre: 'Reg CLT-DEV-16 — NC Devolución Club Tuberos 16%',
    tipoComprobante: 'E',
    tasaIva: '16',
    conceptoContiene: 'club tuberos',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010001',
    cuentaAbono: '2103090002',
    cuentaIva: '2104010001',
    cuentaIvaAbono: '2104010002',
    prioridad: 62,
  },
  {
    nombre: 'Reg CLT-DEV-M — NC Devolución Club Tuberos Mixto',
    tipoComprobante: 'E',
    tasaIva: 'mixto',
    conceptoContiene: 'club tuberos',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010001',
    cuentaAbono: '2103090002',
    cuentaIva: '2104010001',
    cuentaIvaAbono: '2104010002',
    prioridad: 62,
  },
  {
    nombre: 'TO-CAN-0-EF — Traslado y Bonificación club t 0% Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: '0',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010002',
    cuentaAbono: '1101010003',
    prioridad: 65,
  },
  {
    nombre: 'TO-CAN-0 — Traslado y Bonificación club t 0%',
    tipoComprobante: 'E',
    tasaIva: '0',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010002',
    cuentaAbono: '1102011005',
    prioridad: 65,
  },
  {
    nombre: 'TO-CAN-16-EF — Traslado y Bonificación club t 16% Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: '16',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010001',
    cuentaAbono: '1101010003',
    cuentaIva: '2104010001',
    prioridad: 65,
  },
  {
    nombre: 'TO-CAN-16 — Traslado y Bonificación club t 16%',
    tipoComprobante: 'E',
    tasaIva: '16',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010001',
    cuentaAbono: '1102011005',
    cuentaIva: '2104010001',
    prioridad: 65,
  },
  {
    nombre: 'TO-CAN-M-EF — Traslado y Bonificación club t Mixto Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: 'mixto',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010001',
    cuentaAbono: '1101010003',
    cuentaIva: '2104010001',
    prioridad: 65,
  },
  {
    nombre: 'TO-CAN-M — Traslado y Bonificación club t Mixto',
    tipoComprobante: 'E',
    tasaIva: 'mixto',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010001',
    cuentaAbono: '1102011005',
    cuentaIva: '2104010001',
    prioridad: 65,
  },
  {
    nombre: 'TO-DEV-0-EF — Traslado y Bonificación club t 0% Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: '0',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010002',
    cuentaAbono: '1101010003',
    prioridad: 65,
  },
  {
    nombre: 'TO-DEV-0 — Traslado y Bonificación club t 0%',
    tipoComprobante: 'E',
    tasaIva: '0',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010002',
    cuentaAbono: '1102011005',
    prioridad: 65,
  },
  {
    nombre: 'TO-DEV-16-EF — Traslado y Bonificación club t 16% Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: '16',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010001',
    cuentaAbono: '1101010003',
    cuentaIva: '2104010001',
    prioridad: 65,
  },
  {
    nombre: 'TO-DEV-16 — Traslado y Bonificación club t 16%',
    tipoComprobante: 'E',
    tasaIva: '16',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010001',
    cuentaAbono: '1102011005',
    cuentaIva: '2104010001',
    prioridad: 65,
  },
  {
    nombre: 'TO-DEV-M-EF — Traslado y Bonificación club t Mixto Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: 'mixto',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010001',
    cuentaAbono: '1101010003',
    cuentaIva: '2104010001',
    prioridad: 65,
  },
  {
    nombre: 'TO-DEV-M — Traslado y Bonificación club t Mixto',
    tipoComprobante: 'E',
    tasaIva: 'mixto',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010001',
    cuentaAbono: '1102011005',
    cuentaIva: '2104010001',
    prioridad: 65,
  },
  {
    nombre: 'TO-CAN-0-EF — Bonificación 0% Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: '0',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010002',
    cuentaAbono: '1101010003',
    prioridad: 66,
  },
  {
    nombre: 'TO-CAN-0-EF — Bonificacion cliente Mostador 0% Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: '0',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010002',
    cuentaAbono: '1101010003',
    prioridad: 66,
  },
  {
    nombre: 'TO-CAN-0-EF — Bonificacion Especial 0% Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: '0',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010002',
    cuentaAbono: '1101010003',
    prioridad: 66,
  },
  {
    nombre: 'TO-CAN-0 — Bonificación 0%',
    tipoComprobante: 'E',
    tasaIva: '0',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010002',
    cuentaAbono: '2103090001',
    prioridad: 66,
  },
  {
    nombre: 'TO-CAN-0 — Bonificacion cliente Mostador 0%',
    tipoComprobante: 'E',
    tasaIva: '0',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010002',
    cuentaAbono: '2103090001',
    prioridad: 66,
  },
  {
    nombre: 'TO-CAN-0 — Bonificacion Especial 0%',
    tipoComprobante: 'E',
    tasaIva: '0',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010002',
    cuentaAbono: '2103090001',
    prioridad: 66,
  },
  {
    nombre: 'TO-CAN-16-EF — Bonificación 16% Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: '16',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010001',
    cuentaAbono: '1101010003',
    cuentaIva: '2104010001',
    prioridad: 66,
  },
  {
    nombre: 'TO-CAN-16-EF — Bonificacion cliente Mostador 16% Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: '16',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010001',
    cuentaAbono: '1101010003',
    cuentaIva: '2104010001',
    prioridad: 66,
  },
  {
    nombre: 'TO-CAN-16-EF — Bonificacion Especial 16% Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: '16',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010001',
    cuentaAbono: '1101010003',
    cuentaIva: '2104010001',
    prioridad: 66,
  },
  {
    nombre: 'TO-CAN-16 — Bonificación 16%',
    tipoComprobante: 'E',
    tasaIva: '16',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010001',
    cuentaAbono: '2103090001',
    cuentaIva: '2104010001',
    prioridad: 66,
  },
  {
    nombre: 'TO-CAN-16 — Bonificacion cliente Mostador 16%',
    tipoComprobante: 'E',
    tasaIva: '16',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010001',
    cuentaAbono: '2103090001',
    cuentaIva: '2104010001',
    prioridad: 66,
  },
  {
    nombre: 'TO-CAN-16 — Bonificacion Especial 16%',
    tipoComprobante: 'E',
    tasaIva: '16',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010001',
    cuentaAbono: '2103090001',
    cuentaIva: '2104010001',
    prioridad: 66,
  },
  {
    nombre: 'TO-CAN-M-EF — Bonificacion cliente Mostador Mixto Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: 'mixto',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010001',
    cuentaAbono: '1101010003',
    cuentaIva: '2104010001',
    prioridad: 66,
  },
  {
    nombre: 'TO-CAN-M-EF — Bonificacion Especial Mixto Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: 'mixto',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010001',
    cuentaAbono: '1101010003',
    cuentaIva: '2104010001',
    prioridad: 66,
  },
  {
    nombre: 'TO-CAN-M-EF — Bonificación Mixto Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: 'mixto',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010001',
    cuentaAbono: '1101010003',
    cuentaIva: '2104010001',
    prioridad: 66,
  },
  {
    nombre: 'TO-CAN-M — Bonificacion cliente Mostador Mixto',
    tipoComprobante: 'E',
    tasaIva: 'mixto',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010001',
    cuentaAbono: '2103090001',
    cuentaIva: '2104010001',
    prioridad: 66,
  },
  {
    nombre: 'TO-CAN-M — Bonificacion Especial Mixto',
    tipoComprobante: 'E',
    tasaIva: 'mixto',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010001',
    cuentaAbono: '2103090001',
    cuentaIva: '2104010001',
    prioridad: 66,
  },
  {
    nombre: 'TO-CAN-M — Bonificación Mixto',
    tipoComprobante: 'E',
    tasaIva: 'mixto',
    tipoOrigen: 'Cancelación',
    cuentaCargo: '4200010001',
    cuentaAbono: '2103090001',
    cuentaIva: '2104010001',
    prioridad: 66,
  },
  {
    nombre: 'TO-DEV-0-EF — Bonificación 0% Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: '0',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010002',
    cuentaAbono: '1101010003',
    prioridad: 66,
  },
  {
    nombre: 'TO-DEV-0-EF — Bonificacion cliente Mostador 0% Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: '0',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010002',
    cuentaAbono: '1101010003',
    prioridad: 66,
  },
  {
    nombre: 'TO-DEV-0-EF — Bonificacion Especial 0% Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: '0',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010002',
    cuentaAbono: '1101010003',
    prioridad: 66,
  },
  {
    nombre: 'TO-DEV-0 — Bonificación 0%',
    tipoComprobante: 'E',
    tasaIva: '0',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010002',
    cuentaAbono: '2103090001',
    prioridad: 66,
  },
  {
    nombre: 'TO-DEV-0 — Bonificacion cliente Mostador 0%',
    tipoComprobante: 'E',
    tasaIva: '0',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010002',
    cuentaAbono: '2103090001',
    prioridad: 66,
  },
  {
    nombre: 'TO-DEV-0 — Bonificacion Especial 0%',
    tipoComprobante: 'E',
    tasaIva: '0',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010002',
    cuentaAbono: '2103090001',
    prioridad: 66,
  },
  {
    nombre: 'TO-DEV-16-EF — Bonificación 16% Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: '16',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010001',
    cuentaAbono: '1101010003',
    cuentaIva: '2104010001',
    prioridad: 66,
  },
  {
    nombre: 'TO-DEV-16-EF — Bonificacion cliente Mostador 16% Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: '16',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010001',
    cuentaAbono: '1101010003',
    cuentaIva: '2104010001',
    prioridad: 66,
  },
  {
    nombre: 'TO-DEV-16-EF — Bonificacion Especial 16% Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: '16',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010001',
    cuentaAbono: '1101010003',
    cuentaIva: '2104010001',
    prioridad: 66,
  },
  {
    nombre: 'TO-DEV-16 — Bonificación 16%',
    tipoComprobante: 'E',
    tasaIva: '16',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010001',
    cuentaAbono: '2103090001',
    cuentaIva: '2104010001',
    prioridad: 66,
  },
  {
    nombre: 'TO-DEV-16 — Bonificacion cliente Mostador 16%',
    tipoComprobante: 'E',
    tasaIva: '16',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010001',
    cuentaAbono: '2103090001',
    cuentaIva: '2104010001',
    prioridad: 66,
  },
  {
    nombre: 'TO-DEV-16 — Bonificacion Especial 16%',
    tipoComprobante: 'E',
    tasaIva: '16',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010001',
    cuentaAbono: '2103090001',
    cuentaIva: '2104010001',
    prioridad: 66,
  },
  {
    nombre: 'TO-DEV-M-EF — Bonificacion cliente Mostador Mixto Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: 'mixto',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010001',
    cuentaAbono: '1101010003',
    cuentaIva: '2104010001',
    prioridad: 66,
  },
  {
    nombre: 'TO-DEV-M-EF — Bonificacion Especial Mixto Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: 'mixto',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010001',
    cuentaAbono: '1101010003',
    cuentaIva: '2104010001',
    prioridad: 66,
  },
  {
    nombre: 'TO-DEV-M-EF — Bonificación Mixto Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: 'mixto',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010001',
    cuentaAbono: '1101010003',
    cuentaIva: '2104010001',
    prioridad: 66,
  },
  {
    nombre: 'TO-DEV-M — Bonificacion cliente Mostador Mixto',
    tipoComprobante: 'E',
    tasaIva: 'mixto',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010001',
    cuentaAbono: '2103090001',
    cuentaIva: '2104010001',
    prioridad: 66,
  },
  {
    nombre: 'TO-DEV-M — Bonificacion Especial Mixto',
    tipoComprobante: 'E',
    tasaIva: 'mixto',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010001',
    cuentaAbono: '2103090001',
    cuentaIva: '2104010001',
    prioridad: 66,
  },
  {
    nombre: 'TO-DEV-M — Bonificación Mixto',
    tipoComprobante: 'E',
    tasaIva: 'mixto',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010001',
    cuentaAbono: '2103090001',
    cuentaIva: '2104010001',
    prioridad: 66,
  },
  {
    nombre: 'TO-BON-0-EF — Traslado y Bonificación club t 0% Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: '0',
    tipoOrigen: 'Bonificación',
    cuentaCargo: '4200020002',
    cuentaAbono: '1101010003',
    prioridad: 70,
  },
  {
    nombre: 'TO-BON-0 — Traslado y Bonificación club t 0%',
    tipoComprobante: 'E',
    tasaIva: '0',
    tipoOrigen: 'Bonificación',
    cuentaCargo: '4200020002',
    cuentaAbono: '1102011005',
    prioridad: 70,
  },
  {
    nombre: 'TO-BON-16-EF — Traslado y Bonificación club t 16% Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: '16',
    tipoOrigen: 'Bonificación',
    cuentaCargo: '4200020001',
    cuentaAbono: '1101010003',
    cuentaIva: '2104010001',
    prioridad: 70,
  },
  {
    nombre: 'TO-BON-16 — Traslado y Bonificación club t 16%',
    tipoComprobante: 'E',
    tasaIva: '16',
    tipoOrigen: 'Bonificación',
    cuentaCargo: '4200020001',
    cuentaAbono: '1102011005',
    cuentaIva: '2104010001',
    prioridad: 70,
  },
  {
    nombre: 'TO-BON-M-EF — Traslado y Bonificación club t Mixto Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: 'mixto',
    tipoOrigen: 'Bonificación',
    cuentaCargo: '4200020001',
    cuentaAbono: '1101010003',
    cuentaIva: '2104010001',
    prioridad: 70,
  },
  {
    nombre: 'TO-BON-M — Traslado y Bonificación club t Mixto',
    tipoComprobante: 'E',
    tasaIva: 'mixto',
    tipoOrigen: 'Bonificación',
    cuentaCargo: '4200020001',
    cuentaAbono: '1102011005',
    cuentaIva: '2104010001',
    prioridad: 70,
  },
  {
    nombre: 'TO-BON-0-EF — Bonificación 0% Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: '0',
    tipoOrigen: 'Bonificación',
    cuentaCargo: '4200020002',
    cuentaAbono: '1101010003',
    prioridad: 71,
  },
  {
    nombre: 'TO-BON-0-EF — Bonificacion cliente Mostador 0% Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: '0',
    tipoOrigen: 'Bonificación',
    cuentaCargo: '4200020002',
    cuentaAbono: '1101010003',
    prioridad: 71,
  },
  {
    nombre: 'TO-BON-0-EF — Bonificacion Especial 0% Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: '0',
    tipoOrigen: 'Bonificación',
    cuentaCargo: '4200020002',
    cuentaAbono: '1101010003',
    prioridad: 71,
  },
  {
    nombre: 'TO-BON-0 — Bonificación 0%',
    tipoComprobante: 'E',
    tasaIva: '0',
    tipoOrigen: 'Bonificación',
    cuentaCargo: '4200020002',
    cuentaAbono: '2103090001',
    prioridad: 71,
  },
  {
    nombre: 'TO-BON-0 — Bonificacion cliente Mostador 0%',
    tipoComprobante: 'E',
    tasaIva: '0',
    tipoOrigen: 'Bonificación',
    cuentaCargo: '4200020002',
    cuentaAbono: '2103090001',
    prioridad: 71,
  },
  {
    nombre: 'TO-BON-0 — Bonificacion Especial 0%',
    tipoComprobante: 'E',
    tasaIva: '0',
    tipoOrigen: 'Bonificación',
    cuentaCargo: '4200020002',
    cuentaAbono: '2103090001',
    prioridad: 71,
  },
  {
    nombre: 'TO-BON-16-EF — Bonificación 16% Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: '16',
    tipoOrigen: 'Bonificación',
    cuentaCargo: '4200020001',
    cuentaAbono: '1101010003',
    cuentaIva: '2104010001',
    prioridad: 71,
  },
  {
    nombre: 'TO-BON-16-EF — Bonificacion cliente Mostador 16% Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: '16',
    tipoOrigen: 'Bonificación',
    cuentaCargo: '4200020001',
    cuentaAbono: '1101010003',
    cuentaIva: '2104010001',
    prioridad: 71,
  },
  {
    nombre: 'TO-BON-16-EF — Bonificacion Especial 16% Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: '16',
    tipoOrigen: 'Bonificación',
    cuentaCargo: '4200020001',
    cuentaAbono: '1101010003',
    cuentaIva: '2104010001',
    prioridad: 71,
  },
  {
    nombre: 'TO-BON-16 — Bonificación 16%',
    tipoComprobante: 'E',
    tasaIva: '16',
    tipoOrigen: 'Bonificación',
    cuentaCargo: '4200020001',
    cuentaAbono: '2103090001',
    cuentaIva: '2104010001',
    prioridad: 71,
  },
  {
    nombre: 'TO-BON-16 — Bonificacion cliente Mostador 16%',
    tipoComprobante: 'E',
    tasaIva: '16',
    tipoOrigen: 'Bonificación',
    cuentaCargo: '4200020001',
    cuentaAbono: '2103090001',
    cuentaIva: '2104010001',
    prioridad: 71,
  },
  {
    nombre: 'TO-BON-16 — Bonificacion Especial 16%',
    tipoComprobante: 'E',
    tasaIva: '16',
    tipoOrigen: 'Bonificación',
    cuentaCargo: '4200020001',
    cuentaAbono: '2103090001',
    cuentaIva: '2104010001',
    prioridad: 71,
  },
  {
    nombre: 'TO-BON-M-EF — Bonificacion cliente Mostador Mixto Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: 'mixto',
    tipoOrigen: 'Bonificación',
    cuentaCargo: '4200020001',
    cuentaAbono: '1101010003',
    cuentaIva: '2104010001',
    prioridad: 71,
  },
  {
    nombre: 'TO-BON-M-EF — Bonificacion Especial Mixto Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: 'mixto',
    tipoOrigen: 'Bonificación',
    cuentaCargo: '4200020001',
    cuentaAbono: '1101010003',
    cuentaIva: '2104010001',
    prioridad: 71,
  },
  {
    nombre: 'TO-BON-M-EF — Bonificación Mixto Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: 'mixto',
    tipoOrigen: 'Bonificación',
    cuentaCargo: '4200020001',
    cuentaAbono: '1101010003',
    cuentaIva: '2104010001',
    prioridad: 71,
  },
  {
    nombre: 'TO-BON-M — Bonificacion cliente Mostador Mixto',
    tipoComprobante: 'E',
    tasaIva: 'mixto',
    tipoOrigen: 'Bonificación',
    cuentaCargo: '4200020001',
    cuentaAbono: '2103090001',
    cuentaIva: '2104010001',
    prioridad: 71,
  },
  {
    nombre: 'TO-BON-M — Bonificacion Especial Mixto',
    tipoComprobante: 'E',
    tasaIva: 'mixto',
    tipoOrigen: 'Bonificación',
    cuentaCargo: '4200020001',
    cuentaAbono: '2103090001',
    cuentaIva: '2104010001',
    prioridad: 71,
  },
  {
    nombre: 'TO-BON-M — Bonificación Mixto',
    tipoComprobante: 'E',
    tasaIva: 'mixto',
    tipoOrigen: 'Bonificación',
    cuentaCargo: '4200020001',
    cuentaAbono: '2103090001',
    cuentaIva: '2104010001',
    prioridad: 71,
  },
  // ── TO-EGR. EGRESO ERP (tipoOrigen='Egreso') (prio 69) ──────────────────
  // NCs del ERP con tipoOrigen='Egreso': ajustes administrativos que cancelan CxC
  // sin categoría específica (ni Bonificación ni Devolución ni Cancelación).
  // Tratamiento conservador: Devoluciones DEBE, Clientes HABER (sin efectivo).
  // Prioridad 69 para estar al mismo nivel que CC-BON-ERP y ganar sobre reglas
  // genéricas de concepto (prio 74+) que no tienen acceso al campo tipoOrigen.
  {
    nombre:        'Reg TO-EGR-16 — NC Egreso ERP 16% (cancela CxC)',
    tipoComprobante: 'E',
    tipoOrigen:    'Egreso',
    tasaIva:       '16',
    cuentaCargo:   '4200010001',  // Devoluciones s/Ventas 16%
    cuentaAbono:   '1103010001',  // Clientes 16% (extingue CxC — sin efectivo)
    cuentaIva:     '2104010001',  // IVA Trasladado (PUE)
    cuentaIvaPPD:  '2105010001', // IVA Por Trasladar (PPD)
    prioridad:     69,
  },
  {
    nombre:        'Reg TO-EGR-0 — NC Egreso ERP 0% (cancela CxC)',
    tipoComprobante: 'E',
    tipoOrigen:    'Egreso',
    tasaIva:       '0',
    cuentaCargo:   '4200010002',  // Devoluciones s/Ventas 0%
    cuentaAbono:   '1103010002',  // Clientes 0%
    cuentaIva:     null,
    cuentaIvaPPD:  null,
    prioridad:     69,
  },
  {
    nombre:        'Reg TO-EGR-M — NC Egreso ERP Mixta (cancela CxC)',
    tipoComprobante: 'E',
    tipoOrigen:    'Egreso',
    tasaIva:       'mixto',
    cuentaCargo:   '4200010001',  // Devoluciones s/Ventas 16%
    cuentaAbono:   '1103010001',  // Clientes 16%
    cuentaAbono2:  '4200010002',  // Devoluciones 0% (motor mixto E)
    cuentaIva:     '2104010001',
    cuentaIvaPPD:  '2105010001',
    prioridad:     69,
  },
  {
    nombre: 'TO-DEV-0-EF — Devolución 0% Efectivo',
    tipoComprobante: 'E',
    formaPago: '01',
    tasaIva: '0',
    tipoOrigen: 'Devolución',
    cuentaCargo: '4200010002',
    cuentaAbono: '1101010003',
    prioridad: 72,
  },
  // [Bloque TO-DEV/TO-CAN prio 72-73 sin tipoOrigen eliminado: eran catch-alls genéricos
  //  que capturaban cualquier tipo E y causaban clasificación incorrecta. Las variantes
  //  válidas (con tipoOrigen) están cubiertas por TO-DEV prio 65 y TO-CAN prio 66.
  //  La única excepción útil (TO-DEV-0-EF) se conserva arriba con tipoOrigen='Devolución'.]

  {
    nombre: 'Reg 24A-17 — Generación Saldo a Favor 16% Compensación',
    tipoComprobante: 'E',
    formaPago: '17',
    tipoRelacion: '01',
    tasaIva: '16',
    cuentaCargo: '4200010001',
    cuentaAbono: '2103090001',
    cuentaIva: '2104010001',
    prioridad: 84,
  },
  {
    nombre: 'Reg 25A-17 — Generación Saldo a Favor Tasa 0% Compensación',
    tipoComprobante: 'E',
    formaPago: '17',
    tipoRelacion: '01',
    tasaIva: '0',
    cuentaCargo: '4200010002',
    cuentaAbono: '2103090001',
    prioridad: 84,
  },

];

// ── Runner ────────────────────────────────────────────────────────────────────

async function main() {
  const force = process.argv.includes('--force');

  await sequelize.authenticate();
  await CfdiMappingRule.sync({ force: false });

  let creadas = 0, omitidas = 0, actualizadas = 0;

  for (const datos of reglas) {
    const [regla, created] = await CfdiMappingRule.findOrCreate({
      where:    { nombre: datos.nombre },
      defaults: datos,
    });

    if (created) {
      creadas++;
    } else if (force) {
      await regla.update(datos);
      actualizadas++;
    } else {
      omitidas++;
    }
  }

  console.log(`\nCfdi mapping rules:`);
  console.log(`  Creadas:      ${creadas}`);
  console.log(`  Actualizadas: ${actualizadas} (--force)`);
  console.log(`  Omitidas:     ${omitidas} (ya existían)`);
  console.log(`  Total:        ${reglas.length} reglas\n`);

  await sequelize.close();
}

main().catch(err => { console.error(err); process.exit(1); });