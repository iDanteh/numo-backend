'use strict';

// NetpayReporte.model.test.js — verifica el esquema v2 (netpay-matching-v2, PR1/Fase 1):
// nuevo enum de estatus (mismo enum de 6 estados que NetpayMatch — ver design.md "Data
// Model": "estatus: same enum"), motivoDiscrepancia(+folio_duplicado), vinculo,
// folios[].marca/duplicadoDeReporteId, eliminado + trazabilidad de soft-delete, resolve
// manual, revertido y estatusLegacy. No requiere conexión real a Mongo (mismo patrón que
// CollectionRequest.model.test.js).
const NetpayReporte = require('./NetpayReporte.model');

function baseDocData(overrides = {}) {
  return {
    claveRastreo: 'CR-1',
    fechaMovimiento: new Date('2026-09-10T00:00:00Z'),
    montoDepositoTotal: 1000,
    ...overrides,
  };
}

describe('NetpayReporte schema — v2 (estatus, vinculo, folios.marca, eliminado, resolve)', () => {
  test('estatus: mismo enum de 6 estados que NetpayMatch.estatusMatch', () => {
    const path = NetpayReporte.schema.path('estatus');
    expect(path.enumValues.sort()).toEqual([
      'confirmado_automatico',
      'discrepancia',
      'pendiente_por_marca',
      'rechazado',
      'resuelto_manual',
      'resuelto_por_reporte',
    ]);
  });

  test('estatus ya NO acepta los estados legacy (pendiente/confirmado/descartado)', () => {
    const path = NetpayReporte.schema.path('estatus');
    expect(path.enumValues).not.toContain('pendiente');
    expect(path.enumValues).not.toContain('confirmado');
    expect(path.enumValues).not.toContain('descartado');
  });

  test('motivoDiscrepancia: mismo set que NetpayMatch más folio_duplicado, default null', () => {
    const path = NetpayReporte.schema.path('motivoDiscrepancia');
    expect(path.enumValues.sort()).toEqual([
      'candidato_en_conflicto',
      'cobertura_parcial',
      'folio_duplicado',
      'multiples_candidatos',
      'reporte_revertido',
      'revertido',
      'sin_candidato',
      'vinculo_huerfano',
    ]);
    expect(path.options.default ?? null).toBeNull();
  });

  test('vinculo: enum erp-link|corroborado, default null', () => {
    const path = NetpayReporte.schema.path('vinculo');
    expect(path.enumValues.sort()).toEqual(['corroborado', 'erp-link']);
    expect(path.options.default ?? null).toBeNull();
  });

  test('folios[].marca: opcional, null si la columna Marca está ausente', () => {
    const doc = new NetpayReporte(baseDocData({
      folios: [{ referencia: 'R1', orderId: 'O1' }],
    }));

    expect(doc.folios[0].marca).toBeNull();

    doc.folios[0].marca = 'AMEX';
    expect(doc.folios[0].marca).toBe('AMEX');
  });

  test('folios[].duplicadoDeReporteId: ObjectId ref NetpayReporte, default null', () => {
    const folioSchema = NetpayReporte.schema.path('folios').schema;
    const path = folioSchema.path('duplicadoDeReporteId');

    expect(path).toBeDefined();
    expect(path.instance).toBe('ObjectId');
    expect(path.options.ref).toBe('NetpayReporte');
    expect(path.options.default ?? null).toBeNull();
  });

  test('eliminado: Boolean, default false; eliminadoPor/En/Motivo default null', () => {
    const doc = new NetpayReporte(baseDocData());

    expect(doc.eliminado).toBe(false);
    expect(doc.eliminadoPor).toBeNull();
    expect(doc.eliminadoEn).toBeNull();
    expect(doc.eliminadoMotivo).toBeNull();

    doc.eliminado = true;
    doc.eliminadoPor = { userId: 'u1', nombre: 'Ana' };
    doc.eliminadoEn = new Date('2026-09-29T00:00:00Z');
    doc.eliminadoMotivo = 'Archivo cargado por error';

    expect(doc.eliminado).toBe(true);
    expect(doc.eliminadoPor.nombre).toBe('Ana');
  });

  test('resueltoManualPor/resueltoManualEn/justificacion: mismo patrón de resolve que NetpayMatch', () => {
    const doc = new NetpayReporte(baseDocData());

    expect(doc.resueltoManualPor).toBeNull();
    expect(doc.resueltoManualEn).toBeNull();
    expect(doc.justificacion).toBeNull();

    doc.resueltoManualPor = { userId: 'u1', nombre: 'Ana' };
    doc.resueltoManualEn = new Date('2026-09-29T00:00:00Z');
    doc.justificacion = 'Depósito confirmado a mano';
    doc.estatus = 'resuelto_manual';

    expect(doc.estatus).toBe('resuelto_manual');
  });

  test('revertido: {en, movementIds}, default null (mismo patrón que NetpayMatch)', () => {
    const doc = new NetpayReporte(baseDocData());
    expect(doc.revertido).toBeNull();

    doc.revertido = { en: new Date('2026-09-29T00:00:00Z'), movementIds: ['507f1f77bcf86cd799439011'] };
    expect(doc.revertido.movementIds).toHaveLength(1);
  });

  test('estatusLegacy: String, default null', () => {
    const path = NetpayReporte.schema.path('estatusLegacy');
    expect(path).toBeDefined();
    expect(path.instance).toBe('String');
    expect(path.options.default ?? null).toBeNull();
  });

  test('campos existentes (movementIdConfirmado, confirmadoPor/En, descartadoPor/En/Motivo, claveRastreo único) se conservan sin romper (back-compat)', () => {
    expect(NetpayReporte.schema.path('movementIdConfirmado')).toBeDefined();
    expect(NetpayReporte.schema.path('confirmadoPor')).toBeDefined();
    expect(NetpayReporte.schema.path('confirmadoEn')).toBeDefined();
    expect(NetpayReporte.schema.path('descartadoPor')).toBeDefined();
    expect(NetpayReporte.schema.path('descartadoEn')).toBeDefined();
    expect(NetpayReporte.schema.path('descartadoMotivo')).toBeDefined();

    const indexes = NetpayReporte.schema.indexes();
    const claveIdx = indexes.find(([fields]) => 'claveRastreo' in fields);
    expect(claveIdx[1].unique).toBe(true);
  });
});
