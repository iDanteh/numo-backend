'use strict';

// Descarga SAT (2026-10-13): un CFDI cuyo XML llega y que ya está en el ERP
// ("coincide") debe dejar su copia SAT CON XML — antes solo se guardaba la de
// los "solo en SAT", y la copia que no existía o la que creó la carga de Excel
// (sin XML, fecha sin hora, relación 04 supuesta) se quedaba así para siempre.

jest.mock('node-cron', () => ({ schedule: jest.fn(() => ({ stop: jest.fn() })) }));
jest.mock('../models/AppConfig', () => ({ find: () => ({ lean: async () => [] }) }));
jest.mock('../controllers/schedule.controller', () => ({ restaurarProgramados: jest.fn() }));
jest.mock('../models/SatDescargaLog', () => ({ create: jest.fn(async () => ({ _id: 'log1' })), updateOne: jest.fn(async () => ({})) }));
jest.mock('../models/SatJobCheckpoint', () => ({
  findOne: jest.fn(async () => null), find: jest.fn(), findById: jest.fn(),
  findOneAndUpdate: jest.fn(async () => ({ _id: 'cp1' })),
  updateOne: jest.fn(async () => ({})), updateMany: jest.fn(async () => ({})),
}));
jest.mock('../sat/download', () => ({
  solicitar: jest.fn(async () => 'sol1'),
  verificar: jest.fn(async () => ({ idsPaquetes: ['P1'], totalCfdis: 1 })),
  descargarPaquete: jest.fn(),
  descargarPaqueteMetadata: jest.fn(async () => []),
}));
jest.mock('../sat/credenciales', () => ({ obtener: jest.fn(async () => ({ cer: 1 })), tieneCredenciales: jest.fn() }));
jest.mock('../sat/rateLimiter', () => ({ puedeIniciar: jest.fn(async () => ({ puede: false })), registrarInicio: jest.fn(), registrarFin: jest.fn() }));
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
jest.mock('../models/CFDI', () => ({
  find: jest.fn((q) => {
    mockConsultas.push(q);
    if (q?.source === 'ERP') return mockChain(mockErpDocs);
    if (q?.source === 'SAT' && q.xmlContent) return mockChain(mockSatConXml);
    return mockChain([]);
  }),
  bulkWrite: jest.fn(async (ops) => ({ modifiedCount: 0, upsertedCount: ops.length })),
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
const { descargarPaquete } = require('../sat/download');
const { procesarDescarga } = require('./satSyncJob');

const UUID = 'C42E0C8A-C940-428D-B6CB-99F3E9C8D2A3';
const XML = `<?xml version="1.0" encoding="UTF-8"?>
<cfdi:Comprobante xmlns:cfdi="http://www.sat.gob.mx/cfd/4" xmlns:tfd="http://www.sat.gob.mx/TimbreFiscalDigital" Version="4.0" Serie="C0" Folio="261000641" Fecha="2026-10-12T09:05:40" FormaPago="04" MetodoPago="PUE" TipoDeComprobante="I" SubTotal="1223.98" Total="1419.82" Moneda="MXN" LugarExpedicion="68000">
  <cfdi:Emisor Rfc="CCO011113663" Nombre="EMISOR" RegimenFiscal="601"/>
  <cfdi:Receptor Rfc="PASZ900101AAA" Nombre="ZOACHINELLY ROSY PALACIOS SANTIAGO" UsoCFDI="G03" DomicilioFiscalReceptor="68000" RegimenFiscalReceptor="612"/>
  <cfdi:Conceptos><cfdi:Concepto ClaveProdServ="84111506" Cantidad="1" ClaveUnidad="ACT" Descripcion="ANTICIPO" ValorUnitario="1223.98" Importe="1223.98" ObjetoImp="02"/></cfdi:Conceptos>
  <cfdi:Impuestos TotalImpuestosTrasladados="195.84"/>
  <cfdi:Complemento><tfd:TimbreFiscalDigital Version="1.1" UUID="${UUID}" FechaTimbrado="2026-10-12T09:05:42" SelloCFD="a" SelloSAT="b"/></cfdi:Complemento>
</cfdi:Comprobante>`;

const correr = () => procesarDescarga({
  rfc: 'CCO011113663', fechaInicio: '2026-10-12T00:00:00', fechaFin: '2026-10-12T23:59:59',
  tipoComprobante: 'Ingresos', tipoSolicitud: 'CFDI', creds: {}, ejercicio: 2026, periodo: 10, tipo: 'manual',
}).catch(e => e);

const escrituraXml = () => CFDI.bulkWrite.mock.calls
  .flatMap(([ops]) => ops)
  .find(op => op.updateOne?.filter?.source === 'SAT' && op.updateOne.update?.$set?.xmlContent);

beforeEach(() => {
  CFDI.bulkWrite.mockClear();
  descargarPaquete.mockResolvedValue([XML]);
  // El ERP ya lo tiene (coincide): mismos datos que el XML.
  mockErpDocs = [{
    uuid: UUID, source: 'ERP', serie: 'C0', folio: '261000641', fecha: new Date('2026-10-12T15:05:40Z'),
    tipoDeComprobante: 'I', subTotal: 1223.98, total: 1419.82, moneda: 'MXN',
    emisor: { rfc: 'CCO011113663' }, receptor: { rfc: 'PASZ900101AAA' }, impuestos: { totalImpuestosTrasladados: 195.84 },
  }];
});

test('coincide con el ERP y su copia SAT no tiene XML → se guarda el XML (upsert)', async () => {
  mockSatConXml = [];
  await correr();
  const op = escrituraXml();
  expect(op).toBeDefined();
  expect(op.updateOne.upsert).toBe(true);
  expect(op.updateOne.update.$set).toMatchObject({ origenDescarga: 'xml', serie: 'C0', folio: '261000641' });
  expect(op.updateOne.update.$set.xmlContent).toContain(UUID);
  expect(op.updateOne.update.$setOnInsert).toMatchObject({ satStatus: 'Vigente' });
  expect(op.updateOne.update.$set).not.toHaveProperty('satStatus');
});

test('su copia SAT ya tiene XML → no se reescribe', async () => {
  mockSatConXml = [{ uuid: UUID }];
  await correr();
  expect(escrituraXml()).toBeUndefined();
});
