'use strict';

// espacios.routes.test.js — pase al panel del Kiosco de facturación ("Espacios de NUMO").
// Se firma con una llave ES256 real generada aquí y se verifica con su pública, igual que
// lo hará el kiosco: si el formato del pase cambia, esta prueba lo detecta.
const { generateKeyPairSync } = require('crypto');
const express = require('express');
const jwt = require('jsonwebtoken');
const request = require('supertest');

const { privateKey, publicKey } = generateKeyPairSync('ec', { namedCurve: 'P-256' });
const PRIVADA = privateKey.export({ type: 'pkcs8', format: 'pem' });
const PUBLICA = publicKey.export({ type: 'spki', format: 'pem' });

jest.mock('../middleware/auth', () => ({
  authenticate: (req, _res, next) => {
    req.user = { _id: 'auth0|ana', nombre: 'Ana López', email: 'ana@tubosyconexiones.mx', role: 'cobranza', extraPermissions: [] };
    req.auth = { payload: JSON.parse(req.headers['x-test-auth0'] || '{}') };
    next();
  },
  permit: (...perms) => (req, res, next) => {
    const granted = JSON.parse(req.headers['x-test-permissions'] || '[]');
    if (!perms.every(p => granted.includes(p))) return res.status(403).json({ error: 'Permisos insuficientes para esta acción.' });
    next();
  },
}));

function app(env) {
  let router;
  jest.isolateModules(() => {
    Object.assign(process.env, env);
    router = require('./espacios.routes');
  });
  const a = express();
  a.use(express.json());
  a.use('/api/espacios', router);
  return a;
}

const CONFIGURADO = {
  KIOSCO_PASE_LLAVE_PRIVADA: PRIVADA.replace(/\n/g, '\\n'),
  KIOSCO_PASE_EMISOR: 'numo-pruebas',
  KIOSCO_ADMIN_URL: 'https://kiosko-pruebas.tubosyconexiones.mx/admin/',
};

afterEach(() => {
  delete process.env.KIOSCO_PASE_LLAVE_PRIVADA;
  delete process.env.KIOSCO_PASE_EMISOR;
  delete process.env.KIOSCO_ADMIN_URL;
});

const paseDe = (url) => new URL(url).hash.replace(/^#pase=/, '');

describe('POST /api/espacios/kiosco/pase', () => {
  it('sin el permiso kiosco:admin no da pase', async () => {
    await request(app(CONFIGURADO)).post('/api/espacios/kiosco/pase').set('x-test-permissions', '["loyalty:read"]').expect(403);
  });

  it('con el permiso da la url del panel con un pase ES256 que el kiosco puede verificar', async () => {
    const r = await request(app(CONFIGURADO)).post('/api/espacios/kiosco/pase').set('x-test-permissions', '["kiosco:admin"]').expect(200);
    expect(r.headers['cache-control']).toBe('no-store');
    expect(r.body.url).toMatch(/^https:\/\/kiosko-pruebas\.tubosyconexiones\.mx\/admin\/login#pase=[\w-]+\.[\w-]+\.[\w-]+$/);

    const pase = paseDe(r.body.url);
    expect(jwt.decode(pase, { complete: true }).header.alg).toBe('ES256');
    const claims = jwt.verify(pase, PUBLICA, { algorithms: ['ES256'], issuer: 'numo-pruebas', audience: 'kiosco-admin' });
    expect(claims).toMatchObject({ sub: 'auth0|ana', email: 'ana@tubosyconexiones.mx', nombre: 'Ana López', mfa: false });
    expect(claims.exp - claims.iat).toBe(60);
    expect(claims.jti).toMatch(/^[0-9a-f-]{36}$/);
  });

  it('marca mfa: true cuando el token de Auth0 dice que hubo segundo factor', async () => {
    const r = await request(app(CONFIGURADO))
      .post('/api/espacios/kiosco/pase')
      .set('x-test-permissions', '["kiosco:admin"]')
      .set('x-test-auth0', JSON.stringify({ 'https://cfdi-comparator/amr': ['pwd', 'mfa'] }))
      .expect(200);
    expect(jwt.verify(paseDe(r.body.url), PUBLICA, { algorithms: ['ES256'] }).mfa).toBe(true);
  });

  it('cada pase es distinto (un solo uso en el kiosco)', async () => {
    const a = app(CONFIGURADO);
    const [x, y] = await Promise.all([1, 2].map(() => request(a).post('/api/espacios/kiosco/pase').set('x-test-permissions', '["kiosco:admin"]')));
    expect(jwt.decode(paseDe(x.body.url)).jti).not.toBe(jwt.decode(paseDe(y.body.url)).jti);
  });

  it('sin llave o sin url del panel responde 503', async () => {
    const r = await request(app({ KIOSCO_ADMIN_URL: CONFIGURADO.KIOSCO_ADMIN_URL })).post('/api/espacios/kiosco/pase').set('x-test-permissions', '["kiosco:admin"]').expect(503);
    expect(r.body.error).toMatch(/no está configurado/);
  });
});
