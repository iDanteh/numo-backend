'use strict';

// Descarga SAT (2026-10-26, repaso mensual 1–25 oct "Incompleta (6,965 de
// 13,957)" todas las noches): el XML de Ingresos llega parcial y el fallback a
// Metadata recupera el resto, pero los CFDIs cuya copia SAT ya tenía XML (de la
// descarga diaria) se intentaban insertar → E11000 por el índice único
// uuid+source → el fallback caía al catch y la descarga quedaba incompleta.

jest.mock('node-cron', () => ({ schedule: jest.fn(() => ({ stop: jest.fn() })) }));
jest.mock('../models/AppConfig', () => ({ find: () => ({ lean: async () => [] }) }));
jest.mock('../controllers/schedule.controller', () => ({ restaurarProgramados: jest.fn() }));
jest.mock('../models/SatDescargaLog', () => ({ create: jest.fn(async () => ({ _id: 'log1' })), updateOne: jest.fn(async () => ({})) }));
jest.mock('../models/SatJobCheckpoint', () => ({
  findOne: jest.fn(async () => null), find: jest.fn(),
  findById: jest.fn(() => ({ lean: async () => ({ paquetesProcesados: ['P1'], paquetesFallidos: [] }) })),
  findOneAndUpdate: jest.fn(async () => ({ _id: 'cp1' })),
  updateOne: jest.fn(async () => ({})), updateMany: jest.fn(async () => ({})),
}));
jest.mock('../sat/download', () => ({
  solicitar: jest.fn(async () => 'sol1'),
  verificar: jest.fn(async () => ({ idsPaquetes: ['P1'], totalCfdis: 3 })),
  descargarPaquete: jest.fn(),
  descargarPaqueteMetadata: jest.fn(),
}));
jest.mock('../sat/credenciales', () => ({ obtener: jest.fn(async () => ({ cer: 1 })), tieneCredenciales: jest.fn() }));
jest.mock('../sat/rateLimiter', () => ({ puedeIniciar: jest.fn(async () => ({ puede: true })), registrarInicio: jest.fn(), registrarFin: jest.fn() }));
jest.mock('../services/reclasificacionGlobal.service', () => ({ aplicarReclasificacion: jest.fn(async () => ({ totalModificados: 0 })) }));

// Modelos Mongo: `find` encadenable que responde según el filtro.
const mockConsultas = [];
const mockChain = (valor) => {
  const p = Promise.resolve(valor);
  const c = { select: () => c, lean: () => p, sort: () => c, limit: () => c, then: p.then.bind(p) };
  return c;
};
let mockErpDocs = [];
let mockSatConXml = [];
let mockSatOrigenXml = [];
jest.mock('../models/CFDI', () => ({
  find: jest.fn((q) => {
    mockConsultas.push(q);
    if (q?.source === 'ERP') return mockChain(mockErpDocs);
    if (q?.source === 'SAT' && q.xmlContent) return mockChain(mockSatConXml);
    if (q?.source === 'SAT' && q.origenDescarga === 'xml') return mockChain(mockSatOrigenXml);
    return mockChain([]);
  }),
  bulkWrite: jest.fn(async (ops) => { if (ops.some(op => op.updateOne?.filter?.origenDescarga?.$ne === 'xml' && mockSatOrigenXml.some(x => x.uuid === op.updateOne.filter.uuid))) throw new Error('E11000 duplicate key'); return { modifiedCount: 0, upsertedCount: ops.length }; }),
  updateMany: jest.fn(async () => ({})),
  findOne: jest.fn(() => mockChain(null)),
  countDocuments: jest.fn(async () => 0),
}));
const mockModeloGenerico = () => ({
  bulkWrite: jest.fn(async () => ({})), updateMany: jest.fn(async () => ({})), deleteMany: jest.fn(async () => ({})),
  find: jest.fn(() => mockChain([])), findOne: jest.fn(() => mockChain(null)), create: jest.fn(async () => ({ _id: 'x' })),
  insertMany: jest.fn(async () => []), updateOne: jest.fn(async () => ({})),
});
jest.mock('../models/Comparison', () => mockModeloGenerico());
jest.mock('../models/Discrepancy', () => mockModeloGenerico());
jest.mock('../models/ComparisonSession', () => mockModeloGenerico());


const CFDI = require('../models/CFDI');
const { descargarPaquete, descargarPaqueteMetadata } = require('../sat/download');
const { procesarDescarga } = require('./satSyncJob');

const A = 'AAAAAAAA-0000-4000-8000-000000000001';
const B = 'BBBBBBBB-0000-4000-8000-000000000002';
const C = 'CCCCCCCC-0000-4000-8000-000000000003';
const xml = (uuid) => `<?xml version="1.0" encoding="UTF-8"?>
<cfdi:Comprobante xmlns:cfdi="http://www.sat.gob.mx/cfd/4" xmlns:tfd="http://www.sat.gob.mx/TimbreFiscalDigital" Version="4.0" Serie="C0" Folio="1" Fecha="2026-10-12T09:05:40" FormaPago="01" MetodoPago="PUE" TipoDeComprobante="I" SubTotal="100" Total="116" Moneda="MXN" LugarExpedicion="68000">
  <cfdi:Emisor Rfc="CCO011113663" Nombre="EMISOR" RegimenFiscal="601"/>
  <cfdi:Receptor Rfc="XAXX010101000" Nombre="PUBLICO" UsoCFDI="S01" DomicilioFiscalReceptor="68000" RegimenFiscalReceptor="616"/>
  <cfdi:Conceptos><cfdi:Concepto ClaveProdServ="01010101" Cantidad="1" ClaveUnidad="H87" Descripcion="X" ValorUnitario="100" Importe="100" ObjetoImp="02"/></cfdi:Conceptos>
  <cfdi:Complemento><tfd:TimbreFiscalDigital Version="1.1" UUID="${uuid}" FechaTimbrado="2026-10-12T09:05:42" SelloCFD="a" SelloSAT="b"/></cfdi:Complemento>
</cfdi:Comprobante>`;
const meta = (uuid) => ({ uuid, rfcEmisor: 'CCO011113663', rfcReceptor: 'XAXX010101000', total: '116', fecha: '2026-10-12T09:05:40', efecto: 'I', estado: 'Vigente' });
const erp = (uuid) => ({ uuid, source: 'ERP', serie: 'C0', folio: '1', fecha: new Date('2026-10-12T15:05:40Z'), tipoDeComprobante: 'I', subTotal: 100, total: 116, moneda: 'MXN', emisor: { rfc: 'CCO011113663' }, receptor: { rfc: 'XAXX010101000' } });

beforeAll(() => {
  // Sin esperas reales (cooldowns, re-verificaciones de 60 s, 2 min antes del fallback).
  jest.spyOn(global, 'setTimeout').mockImplementation((fn) => { fn(); return 0; });
});
afterAll(() => global.setTimeout.mockRestore());

beforeEach(() => {
  CFDI.bulkWrite.mockClear();
  descargarPaquete.mockResolvedValue([xml(A)]);              // XML: 1 de 3
  descargarPaqueteMetadata.mockResolvedValue([meta(A), meta(B), meta(C)]);
  mockErpDocs = [erp(A), erp(B), erp(C)];
});

const correr = () => procesarDescarga({
  rfc: 'CCO011113663', fechaInicio: '2026-10-01T00:00:00', fechaFin: '2026-10-25T23:59:59',
  tipoComprobante: 'Ingresos', tipoSolicitud: 'CFDI', creds: {}, ejercicio: 2026, periodo: 10, tipo: 'manual',
});

const escriturasMetadata = () => CFDI.bulkWrite.mock.calls
  .flatMap(([ops]) => ops)
  .filter(op => op.updateOne?.update?.$set?.origenDescarga === 'metadata');

test('el que ya tiene XML en base no se re-inserta, pero cuenta como recuperado', async () => {
  mockSatOrigenXml = [{ uuid: B }];
  const res = await correr();
  expect(escriturasMetadata().map(op => op.updateOne.filter.uuid)).toEqual([C]);
  expect(res.incompleta).toBe(false);
  expect(res.totalSAT).toBe(3);
  expect(res.coinciden).toBe(3);
  expect(res.soloEnERP).toBe(0);
});

test('ninguno estaba en base → se guardan los dos que faltaron en el XML', async () => {
  mockSatOrigenXml = [];
  const res = await correr();
  expect(escriturasMetadata().map(op => op.updateOne.filter.uuid).sort()).toEqual([B, C]);
  expect(res.incompleta).toBe(false);
});
