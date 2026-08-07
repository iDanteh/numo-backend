'use strict';

// bank.routes.test.js — primer test de este router (no existía ninguno). Cobertura
// mínima de la ruta nueva GET /cfdis/buscar (2026-08-07, permiso banks:cfdi:read):
// gate de permiso, filtro source='ERP' siempre fijo, escapado de regex, y el
// short-circuit de "sin serie ni folio". Requerir el router real no pega a Mongo —
// los modelos solo declaran esquemas, las llamadas HTTP viven dentro de handlers,
// nunca a nivel de módulo (mismo criterio ya documentado en erp.routes.test.js).
jest.mock('../../shared/middleware/auth.real', () => ({
  authenticate: (req, _res, next) => {
    req.user = {
      _id:  'user-test',
      role: 'test-role',
      extraPermissions: [],
    };
    next();
  },
  permit: (...perms) => (req, res, next) => {
    const granted = JSON.parse(req.headers['x-test-permissions'] || '[]');
    const ok = perms.every(p => granted.includes(p));
    if (!ok) {
      return res.status(403).json({ error: 'Permisos insuficientes para esta acción.', required: perms });
    }
    next();
  },
}));

// CFDI (dominio visor) mockeado — límite de I/O real de este endpoint. El mock
// replica la cadena .select().sort().limit().maxTimeMS().lean() que usa la ruta.
const mockLean = jest.fn();
jest.mock('../../../visor/models/CFDI', () => ({
  find: jest.fn(() => ({
    select:    jest.fn().mockReturnThis(),
    sort:      jest.fn().mockReturnThis(),
    limit:     jest.fn().mockReturnThis(),
    maxTimeMS: jest.fn().mockReturnThis(),
    lean:      mockLean,
  })),
}));

const express = require('express');
const request = require('supertest');
const router  = require('./bank.routes');
const CFDI    = require('../../../visor/models/CFDI');
const { PERMISSIONS } = require('../../../shared/config/rbac');

describe('GET /cfdis/buscar', () => {
  let app;

  beforeEach(() => {
    jest.clearAllMocks();
    mockLean.mockResolvedValue([]);
    app = express();
    app.use(express.json());
    app.use('/', router);
  });

  test('responde 403 sin banks:cfdi:read', async () => {
    const res = await request(app)
      .get('/cfdis/buscar')
      .query({ serie: 'A0', folio: '123' })
      .set('x-test-permissions', JSON.stringify([]));

    expect(res.status).toBe(403);
    expect(res.body.required).toEqual([PERMISSIONS.BANKS_CFDI_READ]);
    expect(CFDI.find).not.toHaveBeenCalled();
  });

  test('sin serie ni folio: responde [] sin consultar Mongo', async () => {
    const res = await request(app)
      .get('/cfdis/buscar')
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_CFDI_READ]));

    expect(res.status).toBe(200);
    expect(res.body).toEqual([]);
    expect(CFDI.find).not.toHaveBeenCalled();
  });

  test('filtra siempre por source=ERP, sin importar qué mande el cliente', async () => {
    await request(app)
      .get('/cfdis/buscar')
      .query({ serie: 'A0', folio: '123' })
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_CFDI_READ]));

    expect(CFDI.find).toHaveBeenCalledTimes(1);
    const filtroUsado = CFDI.find.mock.calls[0][0];
    expect(filtroUsado.source).toBe('ERP');
    expect(filtroUsado.serie).toBeInstanceOf(RegExp);
    expect(filtroUsado.folio).toBeInstanceOf(RegExp);
    // Match exacto (anclado), case-insensitive — no texto libre.
    expect(filtroUsado.serie.source).toBe('^A0$');
    expect(filtroUsado.serie.flags).toContain('i');
  });

  test('escapa caracteres especiales de regex en serie/folio (sin inyección de patrón)', async () => {
    await request(app)
      .get('/cfdis/buscar')
      .query({ serie: 'A.*', folio: '1(2)3' })
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_CFDI_READ]));

    const filtroUsado = CFDI.find.mock.calls[0][0];
    // Los caracteres especiales quedan escapados: el regex busca el texto LITERAL "A.*",
    // no "A seguido de cualquier cosa" (que sería el comportamiento sin escapar).
    expect(filtroUsado.serie.source).toBe('^A\\.\\*$');
    expect(filtroUsado.folio.source).toBe('^1\\(2\\)3$');
  });

  test('acepta solo serie o solo folio (el otro no se agrega al filtro)', async () => {
    await request(app)
      .get('/cfdis/buscar')
      .query({ folio: '456' })
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_CFDI_READ]));

    const filtroUsado = CFDI.find.mock.calls[0][0];
    expect(filtroUsado.serie).toBeUndefined();
    expect(filtroUsado.folio).toBeInstanceOf(RegExp);
  });

  test('devuelve los resultados de Mongo tal cual (uuid/serie/folio/fecha/total)', async () => {
    mockLean.mockResolvedValue([
      { uuid: 'U1', serie: 'A0', folio: '123', fecha: '2026-08-01T00:00:00.000Z', total: 1500.5 },
    ]);

    const res = await request(app)
      .get('/cfdis/buscar')
      .query({ serie: 'A0', folio: '123' })
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.BANKS_CFDI_READ]));

    expect(res.status).toBe(200);
    expect(res.body).toEqual([
      { uuid: 'U1', serie: 'A0', folio: '123', fecha: '2026-08-01T00:00:00.000Z', total: 1500.5 },
    ]);
  });
});
