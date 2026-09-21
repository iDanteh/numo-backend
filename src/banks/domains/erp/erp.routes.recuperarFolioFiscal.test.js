'use strict';

// erp.routes.recuperarFolioFiscal.test.js — cubre _recuperarFolioFiscalJob() y el
// apagado de /sync-erp-kore y /sync-erp-kore/recompute (2026-09-21, decisión explícita
// del usuario: ningún job automático debe volver a tocar saldoErp/status de ningún
// movimiento — solo se recupera folioFiscal de CxC ya identificadas/cobradas). Archivo
// separado de erp.routes.test.js para poder mockear BankMovement.model limpio, sin
// arriesgar los demás tests de ese archivo (que corren contra el modelo real, sin mock,
// confiando en que nunca tocan Mongo de verdad).
jest.mock('../../shared/middleware/auth.real', () => ({
  authenticate: (req, _res, next) => {
    req.user = { _id: 'user-test', role: 'test-role', extraPermissions: [] };
    next();
  },
  permit: () => (req, res, next) => next(),
}));

jest.mock('../../../shared/services/rbac-store', () => ({
  hasPermission:     jest.fn(),
  hasAllPermissions: jest.fn(),
  invalidate:        jest.fn(),
  getPermissions:    jest.fn(),
  roleExists:        jest.fn(),
}));

jest.mock('./kore-caja.service', () => ({
  KoreCajaError:                 class KoreCajaError extends Error {},
  koreTokenCache:                new Map(),
  obtenerCajaBaseUrl:            jest.fn().mockResolvedValue('http://kore.test'),
  obtenerSesionCaja:             jest.fn(),
  obtenerCuentasKore:            jest.fn(),
  aplicarCobroOperacion:         jest.fn(),
  aplicarCobroOperacionMultiple: jest.fn(),
  listarBancos:                  jest.fn(),
  listarFormasPago:              jest.fn(),
  buscarTransferenciasCajas:     jest.fn(),
}));

jest.mock('./erp-sync.service', () => ({ sincronizarCuentasPendientes: jest.fn() }));
jest.mock('../../../visor/models/CFDI', () => ({ findOne: jest.fn(() => ({ lean: jest.fn() })) }));
jest.mock('./CajaTransferencia.model');
jest.mock('./caja-transferencia-match.service', () => ({ buscarCandidatosBatch: jest.fn() }));
jest.mock('./caja-transferencia-confirm.service', () => ({ confirmarMatch: jest.fn() }));
jest.mock('./caja-transferencia-descartar-manual.service', () => ({ descartarManual: jest.fn() }));
jest.mock('./caja-transferencia-sync.service', () => ({ sincronizarTransferenciasCajasManual: jest.fn(), init: jest.fn() }));
jest.mock('./netpay-transacciones.service', () => ({ consultarTransaccionesNetpay: jest.fn() }));
jest.mock('../banks/BankMovement.model');

const express   = require('express');
const request   = require('supertest');
const router    = require('./erp.routes');
const BankMovement = require('../banks/BankMovement.model');
const { sincronizarCuentasPendientes } = require('./erp-sync.service');

const app = express();
app.use(express.json());
app.use('/api/erp', router);

beforeEach(() => {
  jest.clearAllMocks();
});

describe('POST /sync-erp-kore y /sync-erp-kore/recompute — DESHABILITADAS (2026-09-21)', () => {
  test('/sync-erp-kore devuelve 409 sin tocar BankMovement', async () => {
    const res = await request(app).post('/api/erp/sync-erp-kore').send({});
    expect(res.status).toBe(409);
    expect(res.body.error).toMatch(/recuperar-folio-fiscal/);
    expect(BankMovement.find).not.toHaveBeenCalled();
  });

  test('/sync-erp-kore/recompute devuelve 409 sin tocar BankMovement', async () => {
    const res = await request(app).post('/api/erp/sync-erp-kore/recompute').send({});
    expect(res.status).toBe(409);
    expect(res.body.error).toMatch(/recuperar-folio-fiscal/);
    expect(BankMovement.find).not.toHaveBeenCalled();
  });
});

describe('_recuperarFolioFiscalJob — alcance angosto (solo folioFiscal de movimientos ya identificado)', () => {
  function mockCandidatos(docs) {
    const lean = jest.fn().mockResolvedValue(docs);
    const select = jest.fn().mockReturnValue({ lean });
    BankMovement.find.mockReturnValue({ select });
    return { select, lean };
  }

  test('el filtro de Mongo exige status:identificado + al menos un link con serie/folioExterno/folioFiscal:null', async () => {
    mockCandidatos([]);
    await router._recuperarFolioFiscalJob(null, 'job-1', null, null);

    const filtro = BankMovement.find.mock.calls[0][0];
    expect(filtro.status).toBe('identificado');
    expect(filtro.erpLinks.$elemMatch).toEqual({
      serie: { $ne: null }, folioExterno: { $ne: null }, folioFiscal: null,
    });
  });

  test('cuando Kore devuelve un folioFiscal real, se escribe SOLO erpLinks.$.folioFiscal (nunca saldoErp/status)', async () => {
    mockCandidatos([{
      _id: 'mov-1', folio: '099999', banco: 'BBVA', concepto: 'x', deposito: 1000, fecha: new Date(),
      erpLinks: [{ erpId: 'erp-1', serie: 'A0', folioExterno: '260900001', folioFiscal: null, conciliacionFinalizadaAt: null }],
      identificadoPor: [{ erpId: 'erp-1', userId: 'user-1', fechaId: new Date() }],
    }]);
    sincronizarCuentasPendientes.mockResolvedValue({ raw: [{ folioFiscal: 'UUID-REAL-123' }] });
    BankMovement.updateOne.mockResolvedValue({});

    await router._recuperarFolioFiscalJob(null, 'job-2', null, null);

    expect(BankMovement.updateOne).toHaveBeenCalledTimes(1);
    const [filtro, update] = BankMovement.updateOne.mock.calls[0];
    expect(filtro).toEqual({ _id: 'mov-1', 'erpLinks.erpId': 'erp-1' });
    expect(update.$set).toEqual({ 'erpLinks.$.folioFiscal': 'UUID-REAL-123' });
    expect(update.$set).not.toHaveProperty('saldoErp');
    expect(update.$set).not.toHaveProperty('status');
    expect(update.$push._changelog.via).toBe('recuperar-folio-fiscal');
  });

  test('si Kore todavía no trae folioFiscal, no escribe nada (se reintenta en la próxima corrida)', async () => {
    mockCandidatos([{
      _id: 'mov-2', folio: '099998', banco: 'BBVA', erpLinks: [
        { erpId: 'erp-2', serie: 'A0', folioExterno: '260900002', folioFiscal: null, conciliacionFinalizadaAt: null },
      ],
      identificadoPor: [{ erpId: 'erp-2', userId: 'user-1', fechaId: new Date() }],
    }]);
    sincronizarCuentasPendientes.mockResolvedValue({ raw: [{ folioFiscal: null }] });

    await router._recuperarFolioFiscalJob(null, 'job-3', null, null);

    expect(BankMovement.updateOne).not.toHaveBeenCalled();
  });

  test('un link fuera de la ventana de 60 días se salta SIN consultar Kore', async () => {
    const hace90dias = new Date(Date.now() - 90 * 86400000);
    mockCandidatos([{
      _id: 'mov-3', folio: '099997', banco: 'BBVA', erpLinks: [
        { erpId: 'erp-3', serie: 'A0', folioExterno: '260900003', folioFiscal: null, conciliacionFinalizadaAt: hace90dias },
      ],
      identificadoPor: [],
    }]);

    await router._recuperarFolioFiscalJob(null, 'job-4', null, null);

    expect(sincronizarCuentasPendientes).not.toHaveBeenCalled();
    expect(BankMovement.updateOne).not.toHaveBeenCalled();
  });

  test('un link con folioFiscal ya resuelto se ignora (no vuelve a consultar Kore)', async () => {
    mockCandidatos([{
      _id: 'mov-4', folio: '099996', banco: 'BBVA', erpLinks: [
        { erpId: 'erp-4', serie: 'A0', folioExterno: '260900004', folioFiscal: 'UUID-YA-RESUELTO', conciliacionFinalizadaAt: null },
      ],
      identificadoPor: [],
    }]);

    await router._recuperarFolioFiscalJob(null, 'job-5', null, null);

    expect(sincronizarCuentasPendientes).not.toHaveBeenCalled();
  });

  test('sin candidatos: no consulta Kore ni escribe nada', async () => {
    mockCandidatos([]);
    await router._recuperarFolioFiscalJob(null, 'job-6', null, null);
    expect(sincronizarCuentasPendientes).not.toHaveBeenCalled();
    expect(BankMovement.updateOne).not.toHaveBeenCalled();
  });
});
