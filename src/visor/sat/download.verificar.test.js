'use strict';

// verificar(): un rechazo del SAT (EstadoSolicitud=5) debe decir el motivo real
// que viene en CodigoEstadoSolicitud, no una causa supuesta.

jest.mock('axios', () => ({ post: jest.fn() }));
jest.mock('./auth', () => ({
  autenticar: jest.fn(async () => 'tok'),
  crearFirmaSolicitud: jest.fn(async () => '<firma/>'),
}));

const axios = require('axios');
const { verificar } = require('./download');

const creds = { cerBuffer: Buffer.from('c'), keyBuffer: Buffer.from('k'), passwordBuffer: Buffer.from('p') };

const respuesta = ({ estado, codigo, codEstatus = '5000', mensaje = 'Solicitud Aceptada' }) =>
  `<s:Envelope><s:Body><VerificaSolicitudDescargaResponse><VerificaSolicitudDescargaResult ` +
  `CodEstatus="${codEstatus}" EstadoSolicitud="${estado}"` +
  (codigo ? ` CodigoEstadoSolicitud="${codigo}"` : '') +
  ` NumeroCFDIs="0" Mensaje="${mensaje}"/></VerificaSolicitudDescargaResponse></s:Body></s:Envelope>`;

const correr = async () => {
  const p = verificar('abc-123', 'CCO011113663', creds, { maxIntentos: 1 }).catch(e => e);
  await jest.advanceTimersByTimeAsync(20_000);
  return p;
};

beforeEach(() => {
  jest.useFakeTimers();
  axios.post.mockReset();
});
afterEach(() => jest.useRealTimers());

test('rechazo 5005 → mensaje con el motivo real (duplicada)', async () => {
  axios.post.mockResolvedValue({ data: respuesta({ estado: '5', codigo: '5005' }) });
  const err = await correr();
  expect(err.message.startsWith('SAT_RECHAZADA')).toBe(true);
  expect(err.message).toContain('CodigoEstadoSolicitud=5005');
  expect(err.message).toContain('duplicada');
  expect(err.message).not.toContain('Causa probable');
});

test('rechazo 5002 → solicitudes de por vida agotadas', async () => {
  axios.post.mockResolvedValue({ data: respuesta({ estado: '5', codigo: '5002' }) });
  const err = await correr();
  expect(err.message).toContain('CodigoEstadoSolicitud=5002');
  expect(err.message).toContain('de por vida');
});

test('rechazo sin CodigoEstadoSolicitud → lo indica sin inventar causa', async () => {
  axios.post.mockResolvedValue({ data: respuesta({ estado: '5' }) });
  const err = await correr();
  expect(err.message).toContain('CodigoEstadoSolicitud=no informado');
  expect(err.message).toContain('el SAT no informó el motivo');
});

test('estado 4 (Error) incluye el motivo si viene CodigoEstadoSolicitud', async () => {
  axios.post.mockResolvedValue({ data: respuesta({ estado: '4', codigo: '5003', codEstatus: '5000' }) });
  const err = await correr();
  expect(err.message).toContain('SAT [5000]');
  expect(err.message).toContain('CodigoEstadoSolicitud=5003');
  expect(err.message).toContain('tope máximo');
});
