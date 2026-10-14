'use strict';

// solicitar(): el filtro por tipo va en el atributo `TipoComprobante`. Con
// `TipoDeComprobante` el SAT lo ignoraba y cada sub-tipo bajaba el día completo.

jest.mock('axios', () => ({ post: jest.fn() }));
jest.mock('./auth', () => ({
  autenticar: jest.fn(async () => ({ token: 'tok', rfcCertificado: 'CCO011113663' })),
  crearFirmaSolicitud: jest.fn(async () => '<firma/>'),
}));

const axios = require('axios');
const { crearFirmaSolicitud } = require('./auth');
const { solicitar } = require('./download');

const creds = { cerBuffer: Buffer.from('c'), keyBuffer: Buffer.from('k'), passwordBuffer: Buffer.from('p') };

const aceptada = (operacion) =>
  `<s:Envelope><s:Body><${operacion}Response><${operacion}Result ` +
  `IdSolicitud="abc-123" CodEstatus="5000" Mensaje="Solicitud Aceptada"/></${operacion}Response></s:Body></s:Envelope>`;

const pedir = (tipoComprobante) => solicitar({
  rfcSolicitante: 'CCO011113663',
  fechaInicio: '2026-10-05T00:00:00',
  fechaFin: '2026-10-05T23:59:59',
  tipoComprobante,
  tipoSolicitud: 'CFDI',
  creds,
});

beforeEach(() => {
  axios.post.mockReset();
  crearFirmaSolicitud.mockClear();
});

test('Pagos → TipoComprobante="P" en el envelope y en lo que se firma', async () => {
  axios.post.mockResolvedValue({ data: aceptada('SolicitaDescargaEmitidos') });
  await expect(pedir('Pagos')).resolves.toBe('abc-123');

  const envelope = axios.post.mock.calls[0][1];
  expect(envelope).toContain('TipoComprobante="P"');
  expect(envelope).not.toContain('TipoDeComprobante');

  const canonical = crearFirmaSolicitud.mock.calls[0][3];
  expect(canonical).toContain('TipoComprobante="P"');
  expect(canonical).not.toContain('TipoDeComprobante');
});

test('Emitidos sin sub-tipo → no manda filtro de tipo', async () => {
  axios.post.mockResolvedValue({ data: aceptada('SolicitaDescargaEmitidos') });
  await pedir('Emitidos');
  expect(axios.post.mock.calls[0][1]).not.toMatch(/Tipo(De)?Comprobante=/);
});
