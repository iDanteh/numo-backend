'use strict';

// Empresas fijas del usuario aplicadas en el backend del visor (2026-10-14):
// antes solo se escondían del selector y cualquiera podía pedir otro RFC a mano.
const {
  limitarAEmpresasDelUsuario, soloSinEmpresasAsignadas, cfdiDeEmpresasPermitidas,
} = require('./empresas-usuario');

const PROPIA = 'AAA010101AAA';
const OTRA   = 'BBB020202BBB';

function correr(mw, { empresaRfcs = [], query = {} } = {}) {
  const req = { user: { empresaRfcs }, query: { ...query } };
  const res = { statusCode: 200, body: null, status(c) { this.statusCode = c; return this; }, json(b) { this.body = b; return this; } };
  let siguio = false;
  mw(req, res, () => { siguio = true; });
  return { req, res, siguio };
}

describe('sin empresas asignadas', () => {
  test('pasa igual que antes, con cualquier RFC o sin RFC', () => {
    const r = correr(limitarAEmpresasDelUsuario({ inyectar: ['rfcEmisor'] }), { query: { rfcEmisor: OTRA } });
    expect(r.siguio).toBe(true);
    expect(r.req.empresasPermitidas).toBeNull();
    expect(r.req.query.rfcEmisor).toBe(OTRA);
    expect(correr(soloSinEmpresasAsignadas).siguio).toBe(true);
  });
});

describe('con empresas asignadas', () => {
  const base = { empresaRfcs: [PROPIA.toLowerCase()] };

  test('RFC de otra empresa → 403', () => {
    const r = correr(limitarAEmpresasDelUsuario(), { ...base, query: { rfcEmisor: OTRA } });
    expect(r.siguio).toBe(false);
    expect(r.res.statusCode).toBe(403);
  });

  test('recibidos: receptor propio + filtro de proveedor → pasa', () => {
    const r = correr(limitarAEmpresasDelUsuario(), { ...base, query: { rfcReceptor: PROPIA, rfcEmisor: 'PRO' } });
    expect(r.siguio).toBe(true);
    expect(r.req.empresasPermitidas).toEqual([PROPIA]);
  });

  test('con `campos`, el campo de la ruta debe ser propio aunque el otro lo sea', () => {
    const r = correr(limitarAEmpresasDelUsuario({ campos: ['rfcEmisor'] }), { ...base, query: { rfcEmisor: OTRA, rfcReceptor: PROPIA } });
    expect(r.res.statusCode).toBe(403);
  });

  test('reporte sin RFC → se llena con su empresa', () => {
    const r = correr(limitarAEmpresasDelUsuario({ inyectar: ['rfcEmisor', 'rfcReceptor'] }), base);
    expect(r.siguio).toBe(true);
    expect(r.req.query).toEqual({ rfcEmisor: PROPIA, rfcReceptor: PROPIA });
  });

  test('reporte sin RFC y varias empresas → 400 (debe elegir una)', () => {
    const r = correr(limitarAEmpresasDelUsuario({ inyectar: ['rfcEmisor'] }), { empresaRfcs: [PROPIA, OTRA] });
    expect(r.res.statusCode).toBe(400);
  });

  test('operadores en el RFC → 400', () => {
    const r = correr(limitarAEmpresasDelUsuario(), { ...base, query: { rfcEmisor: { $ne: null } } });
    expect(r.res.statusCode).toBe(400);
  });

  test('rutas que mezclan todas las empresas → 403', () => {
    expect(correr(soloSinEmpresasAsignadas, base).res.statusCode).toBe(403);
  });

  test('CFDI por ID: propio si es emisor o receptor', () => {
    expect(cfdiDeEmpresasPermitidas({ emisor: { rfc: OTRA }, receptor: { rfc: PROPIA } }, [PROPIA])).toBe(true);
    expect(cfdiDeEmpresasPermitidas({ emisor: { rfc: OTRA }, receptor: { rfc: 'XAXX010101000' } }, [PROPIA])).toBe(false);
    expect(cfdiDeEmpresasPermitidas({ emisor: { rfc: OTRA } }, null)).toBe(true);
  });
});
