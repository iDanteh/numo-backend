'use strict';

// NetpayFolioRegistro.model.test.js — verifica el esquema nuevo (netpay-matching-v2,
// PR1/Fase 1, design.md "Data Model" y "Architecture Decisions"/"Idempotent folios"):
// {clave (unique), reporteId, orderId, referencia}. `clave` es orderId, o
// 'REF:'+referencia cuando orderId está ausente (folio sin Order ID) — se resuelve en
// netpay-reporte.service.js#cargarReporte al hacer insertMany(ordered:false), este
// esquema solo modela la forma final. Nunca se borra, incluso en soft-delete de su
// reporte (ver design.md "Report present" flow). No requiere conexión real a Mongo.
const NetpayFolioRegistro = require('./NetpayFolioRegistro.model');

describe('NetpayFolioRegistro schema — registro idempotente de folios (nunca se borra)', () => {
  test('clave: String, required, único', () => {
    const path = NetpayFolioRegistro.schema.path('clave');
    expect(path).toBeDefined();
    expect(path.instance).toBe('String');
    expect(path.isRequired).toBe(true);

    const indexes = NetpayFolioRegistro.schema.indexes();
    const claveIdx = indexes.find(([fields]) => 'clave' in fields);
    expect(claveIdx).toBeDefined();
    expect(claveIdx[1].unique).toBe(true);
  });

  test('reporteId: ObjectId ref NetpayReporte, required', () => {
    const path = NetpayFolioRegistro.schema.path('reporteId');
    expect(path).toBeDefined();
    expect(path.instance).toBe('ObjectId');
    expect(path.options.ref).toBe('NetpayReporte');
    expect(path.isRequired).toBe(true);
  });

  test('orderId/referencia: String, default null — al menos uno se usa para construir clave', () => {
    const orderIdPath = NetpayFolioRegistro.schema.path('orderId');
    const referenciaPath = NetpayFolioRegistro.schema.path('referencia');

    expect(orderIdPath.instance).toBe('String');
    expect(orderIdPath.options.default ?? null).toBeNull();
    expect(referenciaPath.instance).toBe('String');
    expect(referenciaPath.options.default ?? null).toBeNull();
  });

  test('un documento válido se construye con clave=orderId cuando orderId existe', () => {
    const doc = new NetpayFolioRegistro({
      clave: 'O1',
      reporteId: '507f1f77bcf86cd799439011',
      orderId: 'O1',
      referencia: 'R1',
    });

    expect(doc.clave).toBe('O1');
    expect(doc.orderId).toBe('O1');
  });

  test('un documento válido se construye con clave=REF:<referencia> cuando orderId está ausente', () => {
    const doc = new NetpayFolioRegistro({
      clave: 'REF:R2',
      reporteId: '507f1f77bcf86cd799439011',
      orderId: null,
      referencia: 'R2',
    });

    expect(doc.clave).toBe('REF:R2');
    expect(doc.orderId).toBeNull();
  });
});
