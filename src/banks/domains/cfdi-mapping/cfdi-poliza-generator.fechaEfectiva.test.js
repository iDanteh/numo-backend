'use strict';

// Día de una factura en la póliza de Ingreso (2026-10-14, Puerto 12-oct):
// copia ERP con fecha justo a medianoche (fecha de día sin hora, ya en hora
// local) va a SU día; antes caía en el anterior y no salía en ninguna póliza
// (O0-261000076, fecha 2026-10-12T00:00Z, timbrada el 13). Las demás copias
// ERP siguen con la ventana de huso horario de México de siempre.
jest.mock('../banks/BankMovement.model', () => ({ find: jest.fn() }));
jest.mock('../../../visor/models/CFDI', () => ({ find: jest.fn() }));
jest.mock('../../../shared/models/postgres', () => ({
  Poliza: {}, PolizaMovimiento: {}, AccountPlan: {}, CfdiMappingRule: {}, CobroSucursalPendienteCobranza: {}, CentroCosto: {},
}));

const CFDI = require('../../../visor/models/CFDI');
const { _uuidsPorFechaEfectiva } = require('./cfdi-poliza-generator.service');

// Simula Mongo: aplica el filtro de `fecha` ($gte/$lte, $in, $or) sobre los docs.
const DOCS = [
  { uuid: 'MEDIANOCHE12', source: 'ERP', fecha: new Date('2026-10-12T00:00:00.000Z') }, // O0-261000076
  { uuid: 'MEDIANOCHE13', source: 'ERP', fecha: new Date('2026-10-13T00:00:00.000Z') },
  { uuid: 'TARDE12', source: 'ERP', fecha: new Date('2026-10-12T19:30:00.000Z') },
  { uuid: 'UTCREAL11NOCHE', source: 'ERP', fecha: new Date('2026-10-12T01:15:22.123Z') },  // copia vieja en UTC real (11-oct 19:15 MX)
  { uuid: 'UTCREAL12NOCHE', source: 'ERP', fecha: new Date('2026-10-13T02:40:10.500Z') },  // 12-oct 20:40 MX
];
const cumpleFecha = (f, cond) => {
  if (!cond) return true;
  if (cond.$in) return cond.$in.some(d => d.getTime() === f.getTime());
  return (!cond.$gte || f >= cond.$gte) && (!cond.$lte || f <= cond.$lte);
};
const cumple = (d, q) => {
  if (q.source && d.source !== q.source) return false;
  if (q.uuid?.$in && !q.uuid.$in.includes(d.uuid)) return false;
  if (q.$or) return q.$or.some(c => cumpleFecha(d.fecha, c.fecha));
  return cumpleFecha(d.fecha, q.fecha);
};
beforeEach(() => {
  CFDI.find.mockImplementation(q => ({ select: () => ({ lean: async () => DOCS.filter(d => cumple(d, q)) }) }));
});

const dia = async (d) => [...await _uuidsPorFechaEfectiva({ rfc: 'X', ejercicio: 2026, periodo: 10, tipoCfdi: 'I', fechaInicio: d, fechaFin: d })].sort();

test('factura a medianoche va a su día; las demás como antes', async () => {
  expect(await dia('2026-10-12')).toEqual(['MEDIANOCHE12', 'TARDE12', 'UTCREAL12NOCHE']);
  expect(await dia('2026-10-11')).toEqual(['UTCREAL11NOCHE']);
  expect(await dia('2026-10-13')).toEqual(['MEDIANOCHE13']);
});
