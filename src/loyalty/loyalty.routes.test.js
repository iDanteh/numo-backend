'use strict';

// loyalty.routes.test.js — PR4/8 sistema-lealtad. Solo gate de permiso + wiring
// (mismo criterio que system-monitor.routes.test.js/bank.routes.test.js): cada
// servicio se mockea completo, la lógica real vive y se prueba en sus propios
// *.service.test.js (PR2/PR3). Este archivo cubre la matriz de independencia de
// permisos exigida por specs/loyalty-permissions/spec.md — cada uno de los 7
// `loyalty:*` debe gatear SOLO su propia acción.
jest.mock('../shared/middleware/auth', () => ({
  authenticate: (req, _res, next) => {
    req.user = { _id: 'user-test', dbId: '1', nombre: 'Tester', role: 'test-role', extraPermissions: [] };
    next();
  },
  permit: (...perms) => (req, res, next) => {
    const granted = JSON.parse(req.headers['x-test-permissions'] || '[]');
    const ok = perms.every((p) => granted.includes(p));
    if (!ok) {
      return res.status(403).json({ error: 'Permisos insuficientes para esta acción.', required: perms });
    }
    next();
  },
}));

jest.mock('./domains/cards/loyalty-card.service', () => ({
  crearTarjeta: jest.fn(),
  listar: jest.fn(),
  obtener: jest.fn(),
  obtenerParaEditar: jest.fn(),
  editarTarjeta: jest.fn(),
  cambiarNip: jest.fn(),
  cambiarNivel: jest.fn(),
  subirFoto: jest.fn(),
  obtenerFoto: jest.fn(),
  listarPendientesRevisionKore: jest.fn(),
  marcarRevisada: jest.fn(),
}));
jest.mock('./domains/cards/loyalty-card-audit.service', () => ({
  listarHistorialCambios: jest.fn(),
}));
jest.mock('./domains/ledger/loyalty-ledger.service', () => ({
  registrarAcumulacion: jest.fn(),
  registrarCanje: jest.fn(),
  registrarAjuste: jest.fn(),
  listarMovimientos: jest.fn(),
  listarNiveles: jest.fn(),
  listarMovimientosGlobal: jest.fn(),
}));
jest.mock('./domains/config/loyalty-config.service', () => ({
  obtenerPorcentajes: jest.fn(),
  guardarPorcentajes: jest.fn(),
  obtenerConsultaCodigoMaxMes: jest.fn(),
  guardarConsultaCodigoMaxMes: jest.fn(),
  obtenerSesionConsultaUsosMax: jest.fn(),
  guardarSesionConsultaUsosMax: jest.fn(),
  obtenerPermitirSaldoNegativoCanje: jest.fn(),
  guardarPermitirSaldoNegativoCanje: jest.fn(),
  obtenerOtpEnvioModo: jest.fn(),
  guardarOtpEnvioModo: jest.fn(),
  obtenerOtpEnvioProveedor: jest.fn(),
  guardarOtpEnvioProveedor: jest.fn(),
  obtenerOtpWhatsappConfig: jest.fn(),
  guardarOtpWhatsappConfig: jest.fn(),
  obtenerEspecialidades: jest.fn(),
  guardarEspecialidades: jest.fn(),
  obtenerFotoFolderId: jest.fn(),
  guardarFotoFolderId: jest.fn(),
  listarReasons: jest.fn(),
  crearReason: jest.fn(),
  editarReason: jest.fn(),
  listarSucursales: jest.fn(),
  actualizarSucursal: jest.fn(),
  listarPromociones: jest.fn(),
  crearPromocion: jest.fn(),
  editarPromocion: jest.fn(),
  listarConceptosKore: jest.fn(),
  sincronizarConceptosKore: jest.fn(),
  actualizarConceptoKore: jest.fn(),
  listarFormasPagoKore: jest.fn(),
  sincronizarFormasPagoKore: jest.fn(),
  actualizarFormaPagoKore: jest.fn(),
  listarNivelesKore: jest.fn(),
  sincronizarNivelesKore: jest.fn(),
  actualizarNivelKore: jest.fn(),
}));
jest.mock('./domains/security/loyalty-security-event.service', () => ({
  listarEventosSeguridad: jest.fn(),
}));

const express = require('express');
const request = require('supertest');
const router = require('./loyalty.routes');
const cardService = require('./domains/cards/loyalty-card.service');
const cardAuditService = require('./domains/cards/loyalty-card-audit.service');
const ledgerService = require('./domains/ledger/loyalty-ledger.service');
const configService = require('./domains/config/loyalty-config.service');
const securityEventService = require('./domains/security/loyalty-security-event.service');
const { PERMISSIONS } = require('../shared/config/rbac');

function perms(...list) {
  return { 'x-test-permissions': JSON.stringify(list) };
}

let app;
beforeEach(() => {
  jest.clearAllMocks();
  // Default sano para la matriz de permisos genérica de abajo — a diferencia del resto de rutas
  // mockeadas (que solo hacen res.json(await ...), inofensivo con undefined), GET /cards/:id/foto
  // desestructura { data, mimetype } del resultado.
  cardService.obtenerFoto.mockResolvedValue({ data: Buffer.from('img'), mimetype: 'image/png' });
  // Default sano para la matriz de permisos genérica de abajo — a diferencia del resto de mocks
  // de configService (inofensivos con undefined, solo se envuelven en res.json), las rutas de
  // GET/PUT /config/otp-whatsapp desestructuran campos del resultado para enmascarar los secretos.
  const otpWhatsappDefault = {
    phoneNumberId: null, wabaId: null, token: null, appSecret: null,
    templateNombre: 'verificacion_codigo', templateIdioma: 'es_MX',
  };
  configService.obtenerOtpWhatsappConfig.mockResolvedValue(otpWhatsappDefault);
  configService.guardarOtpWhatsappConfig.mockResolvedValue(otpWhatsappDefault);
  app = express();
  app.use(express.json());
  app.use('/', router);
});

// ── Matriz: cada ruta exige EXACTAMENTE su permiso, ningún otro lo sustituye ──
describe('matriz de permisos — independencia (specs/loyalty-permissions)', () => {
  const casos = [
    { metodo: 'get',   ruta: '/cards',                    permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'get',   ruta: '/cards/pendientes-revision-kore', permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'patch', ruta: '/cards/1/marcar-revisada',  permiso: PERMISSIONS.LOYALTY_TARJETAS_EDITAR },
    { metodo: 'post',  ruta: '/cards',                     permiso: PERMISSIONS.LOYALTY_TARJETAS_CREAR, body: { telefono: '5512345678', nombre: 'Ana' } },
    { metodo: 'get',   ruta: '/cards/1',                   permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'get',   ruta: '/cards/1/movimientos',       permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'get',   ruta: '/cards/1/niveles',           permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'get',   ruta: '/cards/1/historial',         permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'get',   ruta: '/ledger/export',             permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'patch', ruta: '/cards/1',                   permiso: PERMISSIONS.LOYALTY_TARJETAS_EDITAR, body: { nombre: 'Ana' } },
    { metodo: 'post',  ruta: '/cards/1/nip',                permiso: PERMISSIONS.LOYALTY_TARJETAS_EDITAR },
    { metodo: 'get',   ruta: '/cards/1/foto',               permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'put',   ruta: '/cards/1/nivel',              permiso: PERMISSIONS.LOYALTY_TARJETAS_NIVEL, body: { nivel: 'Plata' } },
    { metodo: 'post',  ruta: '/cards/1/acumulaciones',      permiso: PERMISSIONS.LOYALTY_MOVIMIENTOS, body: { montoVenta: 100, serieFolio: 'A0-123' } },
    { metodo: 'post',  ruta: '/cards/1/canjes',             permiso: PERMISSIONS.LOYALTY_MOVIMIENTOS, body: { puntos: 10, serieFolio: 'A0-123' } },
    { metodo: 'post',  ruta: '/cards/1/ajustes',            permiso: PERMISSIONS.LOYALTY_AJUSTES, body: { puntos: 10, reasonId: 1, justificacion: 'bono' } },
    { metodo: 'get',   ruta: '/config',                     permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'put',   ruta: '/config/porcentajes',         permiso: PERMISSIONS.LOYALTY_CONFIG, body: { Bronce: 1 } },
    { metodo: 'get',   ruta: '/config/consulta-codigo-max-mes', permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'put',   ruta: '/config/consulta-codigo-max-mes', permiso: PERMISSIONS.LOYALTY_CONFIG, body: { max: 1 } },
    { metodo: 'get',   ruta: '/config/sesion-consulta-usos-max', permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'put',   ruta: '/config/sesion-consulta-usos-max', permiso: PERMISSIONS.LOYALTY_CONFIG, body: { max: 3 } },
    { metodo: 'get',   ruta: '/config/permitir-saldo-negativo-canje', permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'put',   ruta: '/config/permitir-saldo-negativo-canje', permiso: PERMISSIONS.LOYALTY_CONFIG, body: { habilitado: true } },
    { metodo: 'get',   ruta: '/config/otp-envio-modo',       permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'put',   ruta: '/config/otp-envio-modo',       permiso: PERMISSIONS.LOYALTY_CONFIG, body: { modo: 'simulado' } },
    { metodo: 'get',   ruta: '/config/otp-envio-proveedor',  permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'put',   ruta: '/config/otp-envio-proveedor',  permiso: PERMISSIONS.LOYALTY_CONFIG, body: { proveedor: 'whatsapp' } },
    { metodo: 'get',   ruta: '/config/otp-whatsapp',         permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'put',   ruta: '/config/otp-whatsapp',         permiso: PERMISSIONS.LOYALTY_CONFIG, body: { phoneNumberId: '123' } },
    { metodo: 'get',   ruta: '/config/especialidades',      permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'put',   ruta: '/config/especialidades',      permiso: PERMISSIONS.LOYALTY_CONFIG, body: { especialidades: ['a'] } },
    { metodo: 'get',   ruta: '/config/foto-folder',         permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'put',   ruta: '/config/foto-folder',         permiso: PERMISSIONS.LOYALTY_CONFIG, body: { folderId: 'abc123' } },
    { metodo: 'get',   ruta: '/reasons',                    permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'post',  ruta: '/reasons',                    permiso: PERMISSIONS.LOYALTY_CONFIG, body: { clave: 'x', nombre: 'x' } },
    { metodo: 'patch', ruta: '/reasons/1',                  permiso: PERMISSIONS.LOYALTY_CONFIG, body: { activo: false } },
    { metodo: 'get',   ruta: '/sucursales',                 permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'patch', ruta: '/sucursales/1',                permiso: PERMISSIONS.LOYALTY_CONFIG, body: { participaLealtad: false } },
    { metodo: 'get',   ruta: '/promociones',                 permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'post',  ruta: '/promociones',                 permiso: PERMISSIONS.LOYALTY_CONFIG, body: { nombre: 'Buen Fin', fechaInicio: '2026-11-01', fechaFin: '2026-11-17', porcentajeBono: 5 } },
    { metodo: 'patch', ruta: '/promociones/1',                permiso: PERMISSIONS.LOYALTY_CONFIG, body: { activa: false } },
    { metodo: 'get',   ruta: '/config/conceptos-kore',         permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'post',  ruta: '/config/conceptos-kore/sync',    permiso: PERMISSIONS.LOYALTY_CONFIG },
    { metodo: 'patch', ruta: '/config/conceptos-kore/1',       permiso: PERMISSIONS.LOYALTY_CONFIG, body: { acumulaPuntos: true } },
    { metodo: 'get',   ruta: '/config/formas-pago-kore',       permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'post',  ruta: '/config/formas-pago-kore/sync',  permiso: PERMISSIONS.LOYALTY_CONFIG },
    { metodo: 'patch', ruta: '/config/formas-pago-kore/1',     permiso: PERMISSIONS.LOYALTY_CONFIG, body: { esPuntos: true } },
    { metodo: 'get',   ruta: '/config/niveles-kore',           permiso: PERMISSIONS.LOYALTY_READ },
    { metodo: 'post',  ruta: '/config/niveles-kore/sync',      permiso: PERMISSIONS.LOYALTY_CONFIG },
    { metodo: 'patch', ruta: '/config/niveles-kore/1',         permiso: PERMISSIONS.LOYALTY_CONFIG, body: { esInicial: true } },
    { metodo: 'get',   ruta: '/auditoria/eventos-seguridad',   permiso: PERMISSIONS.LOYALTY_AUDITORIA },
  ];

  const TODOS = Object.values(PERMISSIONS).filter((p) => p.startsWith('loyalty:'));

  test.each(casos)('$metodo $ruta exige $permiso y rechaza con cualquier OTRO permiso de loyalty', async ({ metodo, ruta, permiso, body }) => {
    // Con TODOS los demás permisos de loyalty menos el requerido → 403
    const otros = TODOS.filter((p) => p !== permiso);
    let req = request(app)[metodo](ruta).set(perms(...otros));
    if (body) req = req.send(body);
    const sinElSuyo = await req;
    expect(sinElSuyo.status).toBe(403);

    // Con el permiso exacto → nunca 403 (puede ser 200/201 según el mock)
    let req2 = request(app)[metodo](ruta).set(perms(permiso));
    if (body) req2 = req2.send(body);
    const conElSuyo = await req2;
    expect(conElSuyo.status).not.toBe(403);
  });
});

describe('loyalty:ajustes y loyalty:tarjetas:nivel no se implican entre sí', () => {
  test('ajustes NO permite cambiar nivel', async () => {
    const res = await request(app).put('/cards/1/nivel')
      .set(perms(PERMISSIONS.LOYALTY_AJUSTES))
      .send({ nivel: 'Plata' });
    expect(res.status).toBe(403);
  });

  test('tarjetas:nivel NO permite crear un ajuste', async () => {
    const res = await request(app).post('/cards/1/ajustes')
      .set(perms(PERMISSIONS.LOYALTY_TARJETAS_NIVEL))
      .send({ puntos: 10, reasonId: 1, justificacion: 'bono' });
    expect(res.status).toBe(403);
  });
});

describe('read-only (solo loyalty:read) no puede escribir nada', () => {
  test('no puede crear tarjeta', async () => {
    const res = await request(app).post('/cards')
      .set(perms(PERMISSIONS.LOYALTY_READ))
      .send({ telefono: '5512345678', nombre: 'Ana' });
    expect(res.status).toBe(403);
  });

  test('no puede registrar acumulación ni canje', async () => {
    const r1 = await request(app).post('/cards/1/acumulaciones').set(perms(PERMISSIONS.LOYALTY_READ)).send({ serieFolio: 'A0-1' });
    const r2 = await request(app).post('/cards/1/canjes').set(perms(PERMISSIONS.LOYALTY_READ)).send({ puntos: 1, serieFolio: 'A0-1' });
    expect(r1.status).toBe(403);
    expect(r2.status).toBe(403);
  });

  test('no puede ajustar, cambiar nivel, ni escribir config', async () => {
    const r1 = await request(app).post('/cards/1/ajustes').set(perms(PERMISSIONS.LOYALTY_READ)).send({ puntos: 1, reasonId: 1, justificacion: 'bono' });
    const r2 = await request(app).put('/cards/1/nivel').set(perms(PERMISSIONS.LOYALTY_READ)).send({ nivel: 'Plata' });
    const r3 = await request(app).put('/config/porcentajes').set(perms(PERMISSIONS.LOYALTY_READ)).send({ Bronce: 1 });
    expect(r1.status).toBe(403);
    expect(r2.status).toBe(403);
    expect(r3.status).toBe(403);
  });
});

describe('wiring hacia los servicios (happy path)', () => {
  test('POST /cards llama a crearTarjeta y devuelve 201', async () => {
    cardService.crearTarjeta.mockResolvedValue({ card: { id: 1 }, nip: '5678' });
    const res = await request(app).post('/cards')
      .set(perms(PERMISSIONS.LOYALTY_TARJETAS_CREAR))
      .send({ telefono: '5512345678', nombre: 'Ana' });
    expect(res.status).toBe(201);
    expect(res.body).toEqual({ card: { id: 1 }, nip: '5678' });
    expect(cardService.crearTarjeta).toHaveBeenCalledWith(
      { telefono: '5512345678', nombre: 'Ana' },
      expect.objectContaining({ dbId: '1' }),
    );
  });

  test('GET /cards/:id/editar llama a obtenerParaEditar y exige loyalty:tarjetas:editar, no loyalty:read', async () => {
    cardService.obtenerParaEditar.mockResolvedValue({ id: 42, apodo: 'Juancho' });

    const sinPermiso = await request(app).get('/cards/42/editar').set(perms(PERMISSIONS.LOYALTY_READ));
    expect(sinPermiso.status).toBe(403);

    const res = await request(app).get('/cards/42/editar').set(perms(PERMISSIONS.LOYALTY_TARJETAS_EDITAR));
    expect(res.status).toBe(200);
    expect(cardService.obtenerParaEditar).toHaveBeenCalledWith('42');
  });

  test('GET /cards/:id/movimientos llama a listarMovimientos con el id', async () => {
    ledgerService.listarMovimientos.mockResolvedValue({ movimientos: [] });
    const res = await request(app).get('/cards/42/movimientos').set(perms(PERMISSIONS.LOYALTY_READ));
    expect(res.status).toBe(200);
    expect(ledgerService.listarMovimientos).toHaveBeenCalledWith('42', expect.any(Object));
  });

  test('GET /cards/:id/historial llama a listarHistorialCambios con el id', async () => {
    cardAuditService.listarHistorialCambios.mockResolvedValue([]);
    const res = await request(app).get('/cards/42/historial').set(perms(PERMISSIONS.LOYALTY_READ));
    expect(res.status).toBe(200);
    expect(cardAuditService.listarHistorialCambios).toHaveBeenCalledWith('42');
  });

  // Reporte "Movimientos" (2026-10-29).
  test('GET /ledger/export pasa los filtros de query a listarMovimientosGlobal y responde { movimientos }', async () => {
    ledgerService.listarMovimientosGlobal.mockResolvedValue([{ id: 1 }, { id: 2 }]);

    const res = await request(app).get(
      '/ledger/export?fechaInicio=2026-10-01&fechaFin=2026-10-29&tipo=ACUMULACION,CANJE'
      + '&nivelAplicado=Oro&origen=kore&reasonId=7&usuarioNombre=Ana&puntosMin=-50&puntosMax=100',
    ).set(perms(PERMISSIONS.LOYALTY_READ));

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ movimientos: [{ id: 1 }, { id: 2 }] });
    expect(ledgerService.listarMovimientosGlobal).toHaveBeenCalledWith({
      fechaInicio: '2026-10-01', fechaFin: '2026-10-29', tipo: 'ACUMULACION,CANJE',
      nivelAplicado: 'Oro', origen: 'kore', reasonId: '7', usuarioNombre: 'Ana',
      puntosMin: '-50', puntosMax: '100',
    });
  });

  // Filtro "Teléfono/tarjeta" (2026-11-20) — pasa el query param al servicio igual que el resto.
  test('GET /ledger/export pasa telefono a listarMovimientosGlobal', async () => {
    ledgerService.listarMovimientosGlobal.mockResolvedValue([]);

    const res = await request(app).get('/ledger/export?telefono=5551234567')
      .set(perms(PERMISSIONS.LOYALTY_READ));

    expect(res.status).toBe(200);
    expect(ledgerService.listarMovimientosGlobal).toHaveBeenCalledWith(
      expect.objectContaining({ telefono: '5551234567' }),
    );
  });

  // Reporte "Tarjetas" (2026-10-29).
  test('GET /cards pasa exportar/fechaAlta/revisionPendiente al servicio', async () => {
    cardService.listar.mockResolvedValue({ total: 0, cards: [] });

    const res = await request(app).get(
      '/cards?exportar=true&fechaAltaInicio=2026-10-01&fechaAltaFin=2026-10-29&revisionPendiente=true',
    ).set(perms(PERMISSIONS.LOYALTY_READ));

    expect(res.status).toBe(200);
    expect(cardService.listar).toHaveBeenCalledWith(expect.objectContaining({
      exportar: true, fechaAltaInicio: '2026-10-01', fechaAltaFin: '2026-10-29', revisionPendiente: true,
    }));
  });

  test('GET /cards con revisionPendiente=false pasa false explícito (no undefined)', async () => {
    cardService.listar.mockResolvedValue({ total: 0, cards: [] });

    await request(app).get('/cards?revisionPendiente=false').set(perms(PERMISSIONS.LOYALTY_READ));

    expect(cardService.listar).toHaveBeenCalledWith(expect.objectContaining({ revisionPendiente: false }));
  });

  test('GET /cards sin exportar ni revisionPendiente: exportar=false, revisionPendiente=undefined', async () => {
    cardService.listar.mockResolvedValue({ total: 0, cards: [] });

    await request(app).get('/cards').set(perms(PERMISSIONS.LOYALTY_READ));

    expect(cardService.listar).toHaveBeenCalledWith(expect.objectContaining({
      exportar: false, revisionPendiente: undefined,
    }));
  });

  test('PUT /config/porcentajes llama a guardarPorcentajes', async () => {
    configService.guardarPorcentajes.mockResolvedValue({ Bronce: 1 });
    const res = await request(app).put('/config/porcentajes')
      .set(perms(PERMISSIONS.LOYALTY_CONFIG))
      .send({ Bronce: 1 });
    expect(res.status).toBe(200);
    expect(configService.guardarPorcentajes).toHaveBeenCalledWith({ Bronce: 1 }, expect.any(Object));
  });

  test('POST /cards/:id/ajustes pasa puntos/reasonId/justificacion al servicio', async () => {
    ledgerService.registrarAjuste.mockResolvedValue({ movimiento: { id: 1 } });
    const res = await request(app).post('/cards/1/ajustes')
      .set(perms(PERMISSIONS.LOYALTY_AJUSTES))
      .send({ puntos: '20.00', reasonId: 10, justificacion: 'ajuste manual' });
    expect(res.status).toBe(201);
    expect(ledgerService.registrarAjuste).toHaveBeenCalledWith(
      { cardId: '1', puntos: '20.00', reasonId: 10, justificacion: 'ajuste manual' },
      expect.any(Object),
    );
  });

  test('GET /config/consulta-codigo-max-mes llama a obtenerConsultaCodigoMaxMes', async () => {
    configService.obtenerConsultaCodigoMaxMes.mockResolvedValue(1);
    const res = await request(app).get('/config/consulta-codigo-max-mes').set(perms(PERMISSIONS.LOYALTY_READ));
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ max: 1 });
    expect(configService.obtenerConsultaCodigoMaxMes).toHaveBeenCalled();
  });

  test('PUT /config/consulta-codigo-max-mes llama a guardarConsultaCodigoMaxMes con max y el usuario', async () => {
    configService.guardarConsultaCodigoMaxMes.mockResolvedValue(1);
    const res = await request(app).put('/config/consulta-codigo-max-mes')
      .set(perms(PERMISSIONS.LOYALTY_CONFIG))
      .send({ max: 1 });
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ max: 1 });
    expect(configService.guardarConsultaCodigoMaxMes).toHaveBeenCalledWith(1, expect.objectContaining({ dbId: '1' }));
  });

  test('GET /config/sesion-consulta-usos-max llama a obtenerSesionConsultaUsosMax', async () => {
    configService.obtenerSesionConsultaUsosMax.mockResolvedValue(3);
    const res = await request(app).get('/config/sesion-consulta-usos-max').set(perms(PERMISSIONS.LOYALTY_READ));
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ max: 3 });
    expect(configService.obtenerSesionConsultaUsosMax).toHaveBeenCalled();
  });

  test('PUT /config/sesion-consulta-usos-max llama a guardarSesionConsultaUsosMax con max y el usuario', async () => {
    configService.guardarSesionConsultaUsosMax.mockResolvedValue(0);
    const res = await request(app).put('/config/sesion-consulta-usos-max')
      .set(perms(PERMISSIONS.LOYALTY_CONFIG))
      .send({ max: 0 });
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ max: 0 });
    expect(configService.guardarSesionConsultaUsosMax).toHaveBeenCalledWith(0, expect.objectContaining({ dbId: '1' }));
  });

  test('GET /config/permitir-saldo-negativo-canje llama a obtenerPermitirSaldoNegativoCanje', async () => {
    configService.obtenerPermitirSaldoNegativoCanje.mockResolvedValue(false);
    const res = await request(app).get('/config/permitir-saldo-negativo-canje').set(perms(PERMISSIONS.LOYALTY_READ));
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ habilitado: false });
    expect(configService.obtenerPermitirSaldoNegativoCanje).toHaveBeenCalled();
  });

  test('PUT /config/permitir-saldo-negativo-canje llama a guardarPermitirSaldoNegativoCanje con habilitado y el usuario', async () => {
    configService.guardarPermitirSaldoNegativoCanje.mockResolvedValue(true);
    const res = await request(app).put('/config/permitir-saldo-negativo-canje')
      .set(perms(PERMISSIONS.LOYALTY_CONFIG))
      .send({ habilitado: true });
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ habilitado: true });
    expect(configService.guardarPermitirSaldoNegativoCanje).toHaveBeenCalledWith(true, expect.objectContaining({ dbId: '1' }));
  });

  test('GET /config/otp-envio-modo llama a obtenerOtpEnvioModo', async () => {
    configService.obtenerOtpEnvioModo.mockResolvedValue('simulado');
    const res = await request(app).get('/config/otp-envio-modo').set(perms(PERMISSIONS.LOYALTY_READ));
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ modo: 'simulado' });
    expect(configService.obtenerOtpEnvioModo).toHaveBeenCalled();
  });

  test('PUT /config/otp-envio-modo llama a guardarOtpEnvioModo con modo y el usuario', async () => {
    configService.guardarOtpEnvioModo.mockResolvedValue('real');
    const res = await request(app).put('/config/otp-envio-modo')
      .set(perms(PERMISSIONS.LOYALTY_CONFIG))
      .send({ modo: 'real' });
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ modo: 'real' });
    expect(configService.guardarOtpEnvioModo).toHaveBeenCalledWith('real', expect.objectContaining({ dbId: '1' }));
  });

  test('PUT /config/otp-envio-modo propaga el BadRequestError de un modo inválido', async () => {
    const { BadRequestError } = require('../shared/errors/AppError');
    configService.guardarOtpEnvioModo.mockRejectedValue(new BadRequestError('El modo de envío de código debe ser uno de: simulado, real.'));
    const res = await request(app).put('/config/otp-envio-modo')
      .set(perms(PERMISSIONS.LOYALTY_CONFIG))
      .send({ modo: 'telepatia' });
    expect(res.status).toBe(400);
    expect(configService.guardarOtpEnvioModo).toHaveBeenCalledWith('telepatia', expect.objectContaining({ dbId: '1' }));
  });

  test('GET /config/otp-envio-proveedor llama a obtenerOtpEnvioProveedor', async () => {
    configService.obtenerOtpEnvioProveedor.mockResolvedValue('whatsapp');
    const res = await request(app).get('/config/otp-envio-proveedor').set(perms(PERMISSIONS.LOYALTY_READ));
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ proveedor: 'whatsapp' });
    expect(configService.obtenerOtpEnvioProveedor).toHaveBeenCalled();
  });

  test('PUT /config/otp-envio-proveedor llama a guardarOtpEnvioProveedor con proveedor y el usuario', async () => {
    configService.guardarOtpEnvioProveedor.mockResolvedValue('otro-proveedor');
    const res = await request(app).put('/config/otp-envio-proveedor')
      .set(perms(PERMISSIONS.LOYALTY_CONFIG))
      .send({ proveedor: 'otro-proveedor' });
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ proveedor: 'otro-proveedor' });
    expect(configService.guardarOtpEnvioProveedor).toHaveBeenCalledWith('otro-proveedor', expect.objectContaining({ dbId: '1' }));
  });

  test('GET /config/otp-whatsapp llama a obtenerOtpWhatsappConfig y enmascara token/appSecret', async () => {
    configService.obtenerOtpWhatsappConfig.mockResolvedValue({
      phoneNumberId: '1234567890', wabaId: '999', token: 'token-real-super-secreto',
      appSecret: 'app-secret-real', templateNombre: 'verificacion_codigo', templateIdioma: 'es_MX',
    });
    const res = await request(app).get('/config/otp-whatsapp').set(perms(PERMISSIONS.LOYALTY_READ));
    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      phoneNumberId: '1234567890', wabaId: '999', token: '••••••••',
      appSecret: '••••••••', templateNombre: 'verificacion_codigo', templateIdioma: 'es_MX',
    });
    expect(configService.obtenerOtpWhatsappConfig).toHaveBeenCalled();
  });

  test('GET /config/otp-whatsapp nunca filtra el valor real de token/appSecret en el response', async () => {
    configService.obtenerOtpWhatsappConfig.mockResolvedValue({
      phoneNumberId: '1234567890', wabaId: '999', token: 'token-real-super-secreto',
      appSecret: 'app-secret-real', templateNombre: 'verificacion_codigo', templateIdioma: 'es_MX',
    });
    const res = await request(app).get('/config/otp-whatsapp').set(perms(PERMISSIONS.LOYALTY_READ));
    const crudo = JSON.stringify(res.body);
    expect(crudo).not.toContain('token-real-super-secreto');
    expect(crudo).not.toContain('app-secret-real');
  });

  test('GET /config/otp-whatsapp devuelve null (no enmascarado) para token/appSecret aún no configurados', async () => {
    configService.obtenerOtpWhatsappConfig.mockResolvedValue({
      phoneNumberId: null, wabaId: null, token: null, appSecret: null,
      templateNombre: 'verificacion_codigo', templateIdioma: 'es_MX',
    });
    const res = await request(app).get('/config/otp-whatsapp').set(perms(PERMISSIONS.LOYALTY_READ));
    expect(res.status).toBe(200);
    expect(res.body.token).toBeNull();
    expect(res.body.appSecret).toBeNull();
  });

  test('PUT /config/otp-whatsapp llama a guardarOtpWhatsappConfig con el payload y el usuario, y enmascara la respuesta', async () => {
    configService.guardarOtpWhatsappConfig.mockResolvedValue({
      phoneNumberId: '1234567890', wabaId: null, token: 'token-nuevo',
      appSecret: null, templateNombre: 'verificacion_codigo', templateIdioma: 'es_MX',
    });
    const res = await request(app).put('/config/otp-whatsapp')
      .set(perms(PERMISSIONS.LOYALTY_CONFIG))
      .send({ phoneNumberId: '1234567890', token: 'token-nuevo' });
    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      phoneNumberId: '1234567890', wabaId: null, token: '••••••••',
      appSecret: null, templateNombre: 'verificacion_codigo', templateIdioma: 'es_MX',
    });
    expect(configService.guardarOtpWhatsappConfig).toHaveBeenCalledWith(
      {
        phoneNumberId: '1234567890', wabaId: undefined, token: 'token-nuevo',
        appSecret: undefined, templateNombre: undefined, templateIdioma: undefined,
      },
      expect.objectContaining({ dbId: '1' }),
    );
  });

  test('PUT /config/otp-envio-proveedor propaga el BadRequestError de un proveedor vacío', async () => {
    const { BadRequestError } = require('../shared/errors/AppError');
    configService.guardarOtpEnvioProveedor.mockRejectedValue(new BadRequestError('proveedor es requerido.'));
    const res = await request(app).put('/config/otp-envio-proveedor')
      .set(perms(PERMISSIONS.LOYALTY_CONFIG))
      .send({ proveedor: '' });
    expect(res.status).toBe(400);
  });

  test('GET /config/foto-folder llama a obtenerFotoFolderId', async () => {
    configService.obtenerFotoFolderId.mockResolvedValue('abc123');
    const res = await request(app).get('/config/foto-folder').set(perms(PERMISSIONS.LOYALTY_READ));
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ folderId: 'abc123' });
    expect(configService.obtenerFotoFolderId).toHaveBeenCalled();
  });

  test('PUT /config/foto-folder llama a guardarFotoFolderId con folderId y el usuario', async () => {
    configService.guardarFotoFolderId.mockResolvedValue('nuevo-id');
    const res = await request(app).put('/config/foto-folder')
      .set(perms(PERMISSIONS.LOYALTY_CONFIG))
      .send({ folderId: 'nuevo-id' });
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ folderId: 'nuevo-id' });
    expect(configService.guardarFotoFolderId).toHaveBeenCalledWith('nuevo-id', expect.objectContaining({ dbId: '1' }));
  });

  test('GET /cards/pendientes-revision-kore pasa search al servicio', async () => {
    cardService.listarPendientesRevisionKore.mockResolvedValue({ total: 0, cards: [] });
    const res = await request(app).get('/cards/pendientes-revision-kore?search=juan')
      .set(perms(PERMISSIONS.LOYALTY_READ));
    expect(res.status).toBe(200);
    expect(cardService.listarPendientesRevisionKore).toHaveBeenCalledWith(
      expect.objectContaining({ search: 'juan' }),
    );
  });

  test('GET /promociones llama a listarPromociones con soloActivas', async () => {
    configService.listarPromociones.mockResolvedValue([]);
    const res = await request(app).get('/promociones?soloActivas=true').set(perms(PERMISSIONS.LOYALTY_READ));
    expect(res.status).toBe(200);
    expect(configService.listarPromociones).toHaveBeenCalledWith({ soloActivas: true });
  });

  test('POST /promociones llama a crearPromocion y devuelve 201', async () => {
    configService.crearPromocion.mockResolvedValue({ id: 1 });
    const res = await request(app).post('/promociones')
      .set(perms(PERMISSIONS.LOYALTY_CONFIG))
      .send({ nombre: 'Buen Fin', fechaInicio: '2026-11-01', fechaFin: '2026-11-17', porcentajeBono: 5 });
    expect(res.status).toBe(201);
    expect(configService.crearPromocion).toHaveBeenCalledWith(
      expect.objectContaining({ nombre: 'Buen Fin' }),
    );
  });

  test('PATCH /promociones/:id llama a editarPromocion', async () => {
    configService.editarPromocion.mockResolvedValue({ id: 1, activa: false });
    const res = await request(app).patch('/promociones/1')
      .set(perms(PERMISSIONS.LOYALTY_CONFIG))
      .send({ activa: false });
    expect(res.status).toBe(200);
    expect(configService.editarPromocion).toHaveBeenCalledWith('1', { activa: false });
  });

  // Split de catálogo (2026-11-03): el query param categoria filtra entre "Motivos de ajuste" y
  // "Catálogo de bonificaciones" — ver loyalty-config.service.js#listarReasons.
  test('GET /reasons pasa categoria y soloActivos al service', async () => {
    configService.listarReasons.mockResolvedValue([]);
    const res = await request(app).get('/reasons?categoria=bonificacion&soloActivos=true')
      .set(perms(PERMISSIONS.LOYALTY_READ));
    expect(res.status).toBe(200);
    expect(configService.listarReasons).toHaveBeenCalledWith({ soloActivos: true, categoria: 'bonificacion' });
  });

  test('GET /config/conceptos-kore llama a listarConceptosKore con soloActivos', async () => {
    configService.listarConceptosKore.mockResolvedValue([]);
    const res = await request(app).get('/config/conceptos-kore?soloActivos=true')
      .set(perms(PERMISSIONS.LOYALTY_READ));
    expect(res.status).toBe(200);
    expect(configService.listarConceptosKore).toHaveBeenCalledWith({ soloActivos: true });
  });

  test('POST /config/conceptos-kore/sync llama a sincronizarConceptosKore', async () => {
    configService.sincronizarConceptosKore.mockResolvedValue({ nuevos: 1, actualizados: 0, desactivados: 0 });
    const res = await request(app).post('/config/conceptos-kore/sync')
      .set(perms(PERMISSIONS.LOYALTY_CONFIG));
    expect(res.status).toBe(200);
    expect(configService.sincronizarConceptosKore).toHaveBeenCalled();
  });

  test('PATCH /config/conceptos-kore/:id llama a actualizarConceptoKore', async () => {
    configService.actualizarConceptoKore.mockResolvedValue({ id: 1, acumulaPuntos: true });
    const res = await request(app).patch('/config/conceptos-kore/1')
      .set(perms(PERMISSIONS.LOYALTY_CONFIG))
      .send({ acumulaPuntos: true });
    expect(res.status).toBe(200);
    expect(configService.actualizarConceptoKore).toHaveBeenCalledWith('1', { acumulaPuntos: true });
  });

  test('GET /config/formas-pago-kore llama a listarFormasPagoKore con soloActivos', async () => {
    configService.listarFormasPagoKore.mockResolvedValue([]);
    const res = await request(app).get('/config/formas-pago-kore')
      .set(perms(PERMISSIONS.LOYALTY_READ));
    expect(res.status).toBe(200);
    expect(configService.listarFormasPagoKore).toHaveBeenCalledWith({ soloActivos: false });
  });

  test('POST /config/formas-pago-kore/sync llama a sincronizarFormasPagoKore', async () => {
    configService.sincronizarFormasPagoKore.mockResolvedValue({ nuevos: 0, actualizados: 1, desactivados: 0 });
    const res = await request(app).post('/config/formas-pago-kore/sync')
      .set(perms(PERMISSIONS.LOYALTY_CONFIG));
    expect(res.status).toBe(200);
    expect(configService.sincronizarFormasPagoKore).toHaveBeenCalled();
  });

  test('PATCH /config/formas-pago-kore/:id llama a actualizarFormaPagoKore', async () => {
    configService.actualizarFormaPagoKore.mockResolvedValue({ id: 1, esPuntos: true });
    const res = await request(app).patch('/config/formas-pago-kore/1')
      .set(perms(PERMISSIONS.LOYALTY_CONFIG))
      .send({ esPuntos: true });
    expect(res.status).toBe(200);
    expect(configService.actualizarFormaPagoKore).toHaveBeenCalledWith('1', { esPuntos: true });
  });

  test('GET /config/niveles-kore llama a listarNivelesKore con soloActivos', async () => {
    configService.listarNivelesKore.mockResolvedValue([]);
    const res = await request(app).get('/config/niveles-kore?soloActivos=true')
      .set(perms(PERMISSIONS.LOYALTY_READ));
    expect(res.status).toBe(200);
    expect(configService.listarNivelesKore).toHaveBeenCalledWith({ soloActivos: true });
  });

  test('POST /config/niveles-kore/sync llama a sincronizarNivelesKore', async () => {
    configService.sincronizarNivelesKore.mockResolvedValue({ nuevos: 1, actualizados: 0, desactivados: 0 });
    const res = await request(app).post('/config/niveles-kore/sync')
      .set(perms(PERMISSIONS.LOYALTY_CONFIG));
    expect(res.status).toBe(200);
    expect(configService.sincronizarNivelesKore).toHaveBeenCalled();
  });

  test('PATCH /config/niveles-kore/:id llama a actualizarNivelKore', async () => {
    configService.actualizarNivelKore.mockResolvedValue({ id: 1, esInicial: true });
    const res = await request(app).patch('/config/niveles-kore/1')
      .set(perms(PERMISSIONS.LOYALTY_CONFIG))
      .send({ esInicial: true });
    expect(res.status).toBe(200);
    expect(configService.actualizarNivelKore).toHaveBeenCalledWith('1', { esInicial: true });
  });
});

// POST /cards/:id/foto (2026-10-29) — foto del cliente, 100% capturada en Numo (cámara del
// staff). Mismo criterio de test que POST /movements/:id/ficha/imagen en bank.routes.test.js:
// requiere .attach() (multipart), separado de la matriz de permisos de arriba (esa manda JSON).
describe('POST /cards/:id/foto', () => {
  test('responde 403 sin loyalty:tarjetas:editar', async () => {
    const res = await request(app)
      .post('/cards/1/foto')
      .attach('foto', Buffer.from('fake-image'), { filename: 'foto.png', contentType: 'image/png' })
      .set(perms());

    expect(res.status).toBe(403);
    expect(cardService.subirFoto).not.toHaveBeenCalled();
  });

  test('loyalty:read NO sustituye a loyalty:tarjetas:editar', async () => {
    const res = await request(app)
      .post('/cards/1/foto')
      .attach('foto', Buffer.from('fake-image'), { filename: 'foto.png', contentType: 'image/png' })
      .set(perms(PERMISSIONS.LOYALTY_READ));

    expect(res.status).toBe(403);
    expect(cardService.subirFoto).not.toHaveBeenCalled();
  });

  test('responde 400 si no se envía ningún archivo', async () => {
    const res = await request(app)
      .post('/cards/1/foto')
      .set(perms(PERMISSIONS.LOYALTY_TARJETAS_EDITAR));

    expect(res.status).toBe(400);
    expect(cardService.subirFoto).not.toHaveBeenCalled();
  });

  test('con archivo y permiso, delega en cardService.subirFoto con el archivo recibido', async () => {
    cardService.subirFoto.mockResolvedValue({ id: 1, fotoUrl: 'https://drive/file-1' });

    const res = await request(app)
      .post('/cards/1/foto')
      .attach('foto', Buffer.from('fake-image'), { filename: 'foto.png', contentType: 'image/png' })
      .set(perms(PERMISSIONS.LOYALTY_TARJETAS_EDITAR));

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ id: 1, fotoUrl: 'https://drive/file-1' });
    expect(cardService.subirFoto).toHaveBeenCalledTimes(1);
    const [id, file] = cardService.subirFoto.mock.calls[0];
    expect(id).toBe('1');
    expect(file.originalname).toBe('foto.png');
    expect(file.mimetype).toBe('image/png');
    expect(Buffer.isBuffer(file.buffer)).toBe(true);
  });
});

// GET /cards/:id/foto (2026-10-29) — bug real corregido: card.fotoUrl es el webViewLink de la
// página visor de Drive, nunca embebible en un <img src>. Este proxy autenticado sirve el binario
// real. Permiso loyalty:read (no loyalty:tarjetas:editar) — ver la foto es lectura, mismo criterio
// que GET /movements/:id/ficha/imagen en bank.routes.js (banks:read, no banks:ficha).
describe('GET /cards/:id/foto', () => {
  test('responde 403 sin loyalty:read', async () => {
    const res = await request(app).get('/cards/1/foto').set(perms());

    expect(res.status).toBe(403);
    expect(cardService.obtenerFoto).not.toHaveBeenCalled();
  });

  test('con loyalty:read, sirve el binario con el Content-Type correcto', async () => {
    cardService.obtenerFoto.mockResolvedValue({ data: Buffer.from('contenido-binario'), mimetype: 'image/png' });

    const res = await request(app).get('/cards/1/foto').set(perms(PERMISSIONS.LOYALTY_READ));

    expect(res.status).toBe(200);
    expect(res.headers['content-type']).toBe('image/png');
    expect(res.body).toEqual(Buffer.from('contenido-binario'));
    expect(cardService.obtenerFoto).toHaveBeenCalledWith('1');
  });

  test('sin mimetype guardado, cae a application/octet-stream', async () => {
    cardService.obtenerFoto.mockResolvedValue({ data: Buffer.from('x'), mimetype: null });

    const res = await request(app).get('/cards/1/foto').set(perms(PERMISSIONS.LOYALTY_READ));

    expect(res.status).toBe(200);
    expect(res.headers['content-type']).toBe('application/octet-stream');
  });
});

// GET /cards/pendientes-revision-kore + PATCH /cards/:id/marcar-revisada (2026-10-29) — altas del
// webhook de catálogo de clientes de Kore marcadas `revisionPendiente: true`
// (loyalty-kore-cliente.service.js#_procesarAlta).
describe('GET /cards/pendientes-revision-kore', () => {
  test('exige loyalty:read y delega en listarPendientesRevisionKore', async () => {
    cardService.listarPendientesRevisionKore.mockResolvedValue({ total: 0, page: 1, limit: 50, pages: 0, cards: [] });

    const sinPermiso = await request(app).get('/cards/pendientes-revision-kore').set(perms());
    expect(sinPermiso.status).toBe(403);

    const res = await request(app).get('/cards/pendientes-revision-kore').set(perms(PERMISSIONS.LOYALTY_READ));
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ total: 0, page: 1, limit: 50, pages: 0, cards: [] });
    expect(cardService.listarPendientesRevisionKore).toHaveBeenCalledWith(
      expect.objectContaining({ page: undefined, limit: undefined }),
    );
  });

  test('ruta estática: no la captura GET /cards/:id (obtener no se llama)', async () => {
    cardService.listarPendientesRevisionKore.mockResolvedValue({ total: 0, page: 1, limit: 50, pages: 0, cards: [] });

    await request(app).get('/cards/pendientes-revision-kore').set(perms(PERMISSIONS.LOYALTY_READ));

    expect(cardService.obtener).not.toHaveBeenCalled();
  });
});

describe('PATCH /cards/:id/marcar-revisada', () => {
  test('exige loyalty:tarjetas:editar, no loyalty:read', async () => {
    cardService.marcarRevisada.mockResolvedValue({ id: 42, revisionPendiente: false });

    const sinPermiso = await request(app).patch('/cards/42/marcar-revisada').set(perms(PERMISSIONS.LOYALTY_READ));
    expect(sinPermiso.status).toBe(403);

    const res = await request(app).patch('/cards/42/marcar-revisada').set(perms(PERMISSIONS.LOYALTY_TARJETAS_EDITAR));
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ id: 42, revisionPendiente: false });
    expect(cardService.marcarRevisada).toHaveBeenCalledWith('42', expect.objectContaining({ dbId: '1' }));
  });
});

describe('GET /auditoria/eventos-seguridad (2026-11-13, pedido explícito del usuario)', () => {
  test('exige loyalty:auditoria, no loyalty:config ni loyalty:read', async () => {
    securityEventService.listarEventosSeguridad.mockResolvedValue({ total: 0, eventos: [] });

    const sinPermiso = await request(app).get('/auditoria/eventos-seguridad').set(perms(PERMISSIONS.LOYALTY_READ));
    expect(sinPermiso.status).toBe(403);

    const conConfigNoAlcanza = await request(app).get('/auditoria/eventos-seguridad').set(perms(PERMISSIONS.LOYALTY_CONFIG));
    expect(conConfigNoAlcanza.status).toBe(403);

    const res = await request(app).get('/auditoria/eventos-seguridad').set(perms(PERMISSIONS.LOYALTY_AUDITORIA));
    expect(res.status).toBe(200);
  });

  test('pasa los filtros de querystring tal cual al service', async () => {
    securityEventService.listarEventosSeguridad.mockResolvedValue({ total: 2, eventos: [{ id: 1 }, { id: 2 }] });

    const res = await request(app).get('/auditoria/eventos-seguridad')
      .query({ telefono: '5512345678', tipo: 'solicitud_codigo_canje', resultado: 'fallo', limit: '10' })
      .set(perms(PERMISSIONS.LOYALTY_AUDITORIA));

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ total: 2, eventos: [{ id: 1 }, { id: 2 }] });
    expect(securityEventService.listarEventosSeguridad).toHaveBeenCalledWith(expect.objectContaining({
      telefono: '5512345678', tipo: 'solicitud_codigo_canje', resultado: 'fallo', limit: '10',
    }));
  });
});
