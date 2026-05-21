'use strict';

/**
 * seed-cfdi-mapping-rules.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Carga masiva de las 24 reglas de mapeo CFDI → cuentas contables.
 * (Reglas 2A–2E y 6-IC de intercompañía requieren campo rfcReceptor en el
 *  modelo — no se incluyen aquí.)
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
// 2104010001  IVA Trasladado (causado definitivo)
// 2105010001  IVA Por Trasladar PPD (cuenta puente)
// 4100010001  Ingresos Por Ventas Contado 16%
// 4100010002  Ingresos Por Ventas Contado 0%
// 4100020001  Ingresos Por Ventas Crédito 16%
// 4200010001  Devoluciones s/Ventas 16%
// 4200020001  Descuentos s/Ventas 16%
// 4200020002  Descuentos s/Ventas 0%

const reglas = [

  // ── 0. ANTICIPOS (Regla 22) ────────────────────────────────────────────────
  // Prioridad 9 — antes que 1A–1E para que el claveProdServ gane.
  // El CFDI de anticipo usa ClaveProdServ=84111506 (Servicios de subcontratación
  // de anticipos / "Anticipo" en el catálogo SAT).
  // Asiento: Dr Bancos (total) | Cr Anticipos de Clientes (subtotal) + Cr IVA
  {
    nombre:          'Reg 22 — Recepción de Anticipo (ClaveProdServ 84111506)',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    claveProdServ:   '84111506',
    cuentaCargo:     '1102011005',   // Bancos (dinero recibido)
    cuentaAbono:     '2103010001',   // Anticipos de Clientes General
    cuentaIva:       '2104010001',   // IVA Trasladado
    prioridad:       9,
  },

  // ── 1. INGRESOS PUE 16% (Reglas 1A–1E) ────────────────────────────────────
  // Prioridades 10–14. Venta de contado, IVA causado al momento de emitir.
  {
    nombre:          'Reg 1A — Venta PUE Efectivo 16%',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       '01',
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
    cuentaCargo:     '1102011005',
    cuentaAbono:     '4100010001',
    cuentaIva:       '2104010001',
    prioridad:       14,
  },

  // ── 2. IVA TASA 0% — PUE (Regla 10) ──────────────────────────────────────
  // Prioridad 15. Exportaciones, alimentos, medicamentos. Sin IVA.
  // formaPago=null → aplica a cualquier forma de pago; el motor usa la misma
  // cuenta de cobro que 1A–1E según la forma de pago real.
  {
    nombre:          'Reg 10 — Venta PUE Tasa 0%',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       null,               // cualquier forma de pago
    cuentaCargo:     '1102011005',  // Bancos (default; motor ajusta a Caja si FP=01)
    cuentaAbono:     '4100010002',  // Ingresos Contado 0%
    cuentaIva:       null,               // sin IVA
    prioridad:       15,
  },

  // ── 3. CFDI MIXTO PUE (0% + 16%) — Regla 12 ──────────────────────────────
  // Prioridad 16. Contiene conceptos gravados 16% Y conceptos 0% en la misma factura.
  // cuentaAbono apunta al ingreso 16%; el motor debe agregar partida adicional
  // con Ingresos 0% (4-1-00-01-0002) por el subtotal exento.
  {
    nombre:          'Reg 12 — Venta Mixta PUE (0%+16%)',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       null,
    cuentaCargo:     '1102011005',
    cuentaAbono:     '4100010001',  // Ingresos 16% (partida principal)
    cuentaIva:       '2104010001',
    // partida adicional (0%): 4-1-00-01-0002 — el motor la añade al detectar dos tasas
    prioridad:       16,
  },

  // ── 4. DESCUENTOS PUE 16% (Regla 14) ─────────────────────────────────────
  // Prioridad 17. Igual que 1A–1E pero el XML lleva campo Descuento > 0.
  // El motor agrega partida de Descuentos s/Ventas 16% (4-2-00-02-0001).
  {
    nombre:          'Reg 14 — Venta con Descuento PUE 16%',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       null,
    cuentaCargo:     '1102011005',
    cuentaAbono:     '4100010001',
    cuentaIva:       '2104010001',
    // descuento: 4-2-00-02-0001 — el motor lo agrega al detectar descuento > 0
    prioridad:       17,
  },

  // ── 5. DESCUENTOS PUE 0% (Regla 15) ──────────────────────────────────────
  {
    nombre:          'Reg 15 — Venta con Descuento PUE Tasa 0%',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       null,
    cuentaCargo:     '1102011005',
    cuentaAbono:     '4100010002',  // Ingresos 0%
    cuentaIva:       null,
    // descuento: 4-2-00-02-0002 — el motor lo agrega
    prioridad:       18,
  },

  // ── 6. DESCUENTOS MIXTO (Regla 16) ───────────────────────────────────────
  // Prioridad 19. Mixto con descuentos en ambas tasas.
  {
    nombre:          'Reg 16 — Venta con Descuento Mixto PUE (0%+16%)',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       null,
    cuentaCargo:     '1102011005',
    cuentaAbono:     '4100010001',
    cuentaIva:       '2104010001',
    // descuentos 16%: 4-2-00-02-0001 / descuentos 0%: 4-2-00-02-0002
    prioridad:       19,
  },

  // ── 7. FACTURA FINAL ANTICIPO PUE (Regla 22C) ────────────────────────────
  // Prioridad 20. PUE + formaPago=30: la factura queda liquidada íntegramente
  // con el anticipo ya recibido. No entra dinero; se cancela el pasivo de anticipos.
  {
    nombre:          'Reg 22C — Factura Final Anticipo PUE (formaPago 30)',
    tipoComprobante: 'I',
    metodoPago:      'PUE',
    formaPago:       '30',
    cuentaCargo:     '2103010001',   // Anticipos de Clientes General
    cuentaAbono:     '4100010001',   // Ingresos Contado 16%
    cuentaIva:       '2104010001',   // IVA Trasladado
    prioridad:       12,
  },

  // ── 7B. FACTURA FINAL ANTICIPO PPD (Regla 22B) ───────────────────────────
  // Prioridad 21. I + PPD + TipoRelacion=07 (relaciona con CFDI de anticipo).
  // Genera CxC y difiere el IVA igual que una venta a crédito normal,
  // pero discriminada por TipoRelacion para separar el ciclo de anticipos.
  {
    nombre:          'Reg 22B — Factura Final Anticipo PPD (TipoRelacion 07)',
    tipoComprobante: 'I',
    metodoPago:      'PPD',
    tipoRelacion:    '07',
    cuentaCargo:     '1103010001',   // Clientes Nac Gral 16%
    cuentaAbono:     '4100020001',   // Ingresos Crédito 16%
    cuentaIvaPPD:    '2105010001',   // IVA Por Trasladar PPD (diferido)
    prioridad:       21,
  },

  // ── 8. INGRESOS PPD 16% — Factura a Crédito (Regla 6) ────────────────────
  // Prioridad 60. IVA diferido (cuenta puente). El cobro llega con tipo P.
  {
    nombre:          'Reg 6 — Venta PPD 16% (Factura a Crédito)',
    tipoComprobante: 'I',
    metodoPago:      'PPD',
    formaPago:       '99',
    cuentaCargo:     '1103010001',  // Clientes Nac Gral 16%
    cuentaAbono:     '4100020001',  // Ingresos Crédito 16%
    cuentaIvaPPD:    '2105010001',  // IVA Por Trasladar PPD (diferido)
    prioridad:       60,
  },

  // ── 8. IVA TASA 0% — PPD (Regla 11) ──────────────────────────────────────
  // Prioridad 65. CxC sin IVA diferido. El cobro solo mueve Bancos vs Clientes.
  {
    nombre:          'Reg 11 — Venta PPD Tasa 0%',
    tipoComprobante: 'I',
    metodoPago:      'PPD',
    formaPago:       '99',
    cuentaCargo:     '1103010002',  // Clientes Nac Gral 0%
    cuentaAbono:     '4100010002',  // Ingresos Contado 0%
    cuentaIvaPPD:    null,               // sin IVA diferido
    prioridad:       65,
  },

  // ── 9. CFDI MIXTO PPD (Regla 13) ─────────────────────────────────────────
  // Prioridad 66. Solo el IVA 16% se difiere; la porción 0% no genera IVA.
  {
    nombre:          'Reg 13 — Venta Mixta PPD (0%+16%)',
    tipoComprobante: 'I',
    metodoPago:      'PPD',
    formaPago:       '99',
    cuentaCargo:     '1103010001',  // Clientes 16%
    cuentaAbono:     '4100020001',  // Ingresos Crédito 16%
    cuentaIvaPPD:    '2105010001',
    // partida adicional 0%: 4-1-00-01-0002 — motor la agrega
    prioridad:       66,
  },

  // ── 10. COBROS PPD — COMPLEMENTO DE PAGO (Reglas 7A–7E) ──────────────────
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

  // ── 11. NOTAS DE CRÉDITO PUE (Reglas 8A–8E) ──────────────────────────────
  // tipoComprobante = E. Tratamiento inverso: revierte ingreso y cancela IVA.
  // cuentaCargo = Devoluciones; cuentaIva = IVA a cancelar (cargo); cuentaAbono = Caja/Bancos.
  {
    nombre:          'Reg 8A — NC PUE Devolución Efectivo',
    tipoComprobante: 'E',
    metodoPago:      'PUE',
    formaPago:       '01',
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
    cuentaCargo:     '4200010001',
    cuentaAbono:     '1102011005',
    cuentaIva:       '2104010001',
    prioridad:       84,
  },

  // ── 12. COMODÍN — Sin coincidencia (Regla 9) ──────────────────────────────
  // Prioridad 99 (último recurso). Forma de pago no parametrizada (ej. 05, 06, 08, 13…).
  // Genera póliza con cuentas genéricas y queda marcada para revisión manual.
  // cuentaIva cubre PUE; cuentaIvaPPD cubre PPD — ambas deben estar presentes.
  {
    nombre:          'Reg 9 — Comodín General (Sin coincidencia)',
    tipoComprobante: null,               // cualquier tipo
    metodoPago:      null,
    formaPago:       null,
    cuentaCargo:     '1102011005',  // Bancos (default conservador)
    cuentaAbono:     '4100020001',  // Ingresos Crédito 16% (conservador)
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
  console.log('NOTA: Las reglas intercompañía (2A–2E, 6-IC, prios 5–9 y 61)');
  console.log('      requieren el campo rfcReceptor en el modelo.');
  console.log('      Se deben insertar manualmente una vez agregado ese campo.');

  await sequelize.close();
  process.exit(0);
}

run().catch((err) => {
  console.error('Error en seed-cfdi-mapping-rules:', err);
  process.exit(1);
});
