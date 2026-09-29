'use strict';

// NetpayMatch.model.test.js — verifica el esquema v2 (netpay-matching-v2, PR1/Fase 1):
// bucket (nueva clave del grupo), índice único {terminalID,dia,bucket}, el nuevo enum de
// estatusMatch (6 estados, ver design.md "Data Model"), motivoDiscrepancia, snapshot,
// campos de resolución manual (resueltoManualPor/En, justificacion, rechazoMotivo),
// revertido y estatusLegacy. No requiere conexión real a Mongo: mongoose.model() solo
// compila el esquema (mismo patrón que CollectionRequest.model.test.js).
const NetpayMatch = require('./NetpayMatch.model');

describe('NetpayMatch schema — v2 (bucket, estados automáticos, snapshot, resolve)', () => {
  test('bucket: String, default "general"', () => {
    const path = NetpayMatch.schema.path('bucket');
    expect(path).toBeDefined();
    expect(path.instance).toBe('String');
    expect(path.options.default).toBe('general');
  });

  test('índice único es {terminalID,dia,bucket}, ya no {terminalID,dia}', () => {
    const indexes = NetpayMatch.schema.indexes();
    const compuesto = indexes.find(([fields]) => 'bucket' in fields);
    expect(compuesto).toBeDefined();
    const [fields, options] = compuesto;
    expect(fields).toEqual({ terminalID: 1, dia: 1, bucket: 1 });
    expect(options.unique).toBe(true);

    const viejo = indexes.find(([fields]) => (
      Object.keys(fields).length === 2 && 'terminalID' in fields && 'dia' in fields
    ));
    expect(viejo).toBeUndefined();
  });

  test('estatusMatch acepta los 6 estados nuevos', () => {
    const path = NetpayMatch.schema.path('estatusMatch');
    expect(path.enumValues.sort()).toEqual([
      'confirmado_automatico',
      'discrepancia',
      'pendiente_por_marca',
      'rechazado',
      'resuelto_manual',
      'resuelto_por_reporte',
    ]);
  });

  test('estatusMatch ya NO acepta los estados legacy (matcheada/descartada-manual)', () => {
    const path = NetpayMatch.schema.path('estatusMatch');
    expect(path.enumValues).not.toContain('matcheada');
    expect(path.enumValues).not.toContain('descartada-manual');
  });

  test('motivoDiscrepancia: enum con las 7 razones, default null', () => {
    const path = NetpayMatch.schema.path('motivoDiscrepancia');
    expect(path.enumValues.sort()).toEqual([
      'candidato_en_conflicto',
      'cobertura_parcial',
      'multiples_candidatos',
      'reporte_revertido',
      'revertido',
      'sin_candidato',
      'vinculo_huerfano',
    ]);
    expect(path.options.default ?? null).toBeNull();
  });

  test('snapshot: guarda terminalID/dia/montoBruto/comision/netoEsperado/folios/reporteIdOrigen/claveRastreoOrigen/montoDepositoReporte', () => {
    const doc = new NetpayMatch({
      terminalID: 'T1', dia: new Date('2026-09-10T00:00:00Z'), netoEsperado: 100, estatusMatch: 'discrepancia',
      snapshot: {
        terminalID: 'T1',
        dia: new Date('2026-09-10T00:00:00Z'),
        montoBruto: 120,
        comision: 20,
        netoEsperado: 100,
        folios: [{ orderId: 'O1', referencia: 'R1', marca: 'VISA', monto: 100, comision: 5 }],
        reporteIdOrigen: null,
        claveRastreoOrigen: null,
        montoDepositoReporte: null,
      },
    });

    expect(doc.snapshot.montoBruto).toBe(120);
    expect(doc.snapshot.comision).toBe(20);
    expect(doc.snapshot.folios).toHaveLength(1);
    expect(doc.snapshot.folios[0]).toMatchObject({ orderId: 'O1', referencia: 'R1', marca: 'VISA', monto: 100, comision: 5 });
  });

  test('snapshot es null por default cuando no se asigna', () => {
    const doc = new NetpayMatch({ terminalID: 'T1', dia: new Date(), netoEsperado: 0, estatusMatch: 'discrepancia' });
    expect(doc.snapshot).toBeNull();
  });

  test('resueltoManualPor/resueltoManualEn/justificacion: default null, asignables (resolve manual)', () => {
    const doc = new NetpayMatch({ terminalID: 'T1', dia: new Date(), netoEsperado: 0, estatusMatch: 'discrepancia' });
    expect(doc.resueltoManualPor).toBeNull();
    expect(doc.resueltoManualEn).toBeNull();
    expect(doc.justificacion).toBeNull();

    doc.resueltoManualPor = { userId: 'u1', nombre: 'Ana' };
    doc.resueltoManualEn = new Date('2026-09-29T00:00:00Z');
    doc.justificacion = 'Confirmado a mano contra el estado de cuenta';
    doc.estatusMatch = 'resuelto_manual';

    expect(doc.resueltoManualPor.nombre).toBe('Ana');
    expect(doc.estatusMatch).toBe('resuelto_manual');
  });

  test('rechazoMotivo: String, default null', () => {
    const path = NetpayMatch.schema.path('rechazoMotivo');
    expect(path).toBeDefined();
    expect(path.instance).toBe('String');
    expect(path.options.default ?? null).toBeNull();
  });

  test('revertido: {en, movementIds}, default null, asignable en el unlink hook', () => {
    const doc = new NetpayMatch({ terminalID: 'T1', dia: new Date(), netoEsperado: 0, estatusMatch: 'discrepancia' });
    expect(doc.revertido).toBeNull();

    doc.revertido = { en: new Date('2026-09-29T00:00:00Z'), movementIds: ['507f1f77bcf86cd799439011'] };
    expect(doc.revertido.movementIds).toHaveLength(1);
  });

  test('estatusLegacy: String, default null — preserva el valor original en la migración', () => {
    const path = NetpayMatch.schema.path('estatusLegacy');
    expect(path).toBeDefined();
    expect(path.instance).toBe('String');
    expect(path.options.default ?? null).toBeNull();
  });

  test('campos existentes (movementIdsConfirmados, confirmadoPor/En, descartadoManualmentePor/En) se conservan sin romper (back-compat)', () => {
    expect(NetpayMatch.schema.path('movementIdsConfirmados')).toBeDefined();
    expect(NetpayMatch.schema.path('confirmadoPor')).toBeDefined();
    expect(NetpayMatch.schema.path('confirmadoEn')).toBeDefined();
    expect(NetpayMatch.schema.path('descartadoManualmentePor')).toBeDefined();
    expect(NetpayMatch.schema.path('descartadoManualmenteEn')).toBeDefined();
  });
});
