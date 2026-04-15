/**
 * seedTest.js — Inserta un par de CFDIs de prueba (ERP + SAT) con discrepancias
 * intencionales para validar el motor de comparación.
 *
 * Uso: node src/utils/seedTest.js
 *
 * Discrepancias que se generarán al comparar:
 *  [critical] total               ERP=$5,900.00  SAT=$5,800.00  (diferencia $100)
 *  [critical] IVA trasladado      ERP=$900.00    SAT=$800.00    (diferencia $100)
 *  [warning]  emisor.regimenFiscal ERP=612        SAT=601
 */
require('dotenv').config();
const mongoose = require('mongoose');
const CFDI = require('../models/CFDI');

const UUID_TEST = 'AAAAAAAA-1111-2222-3333-BBBBBBBBBBBB';

const BASE = {
  uuid: UUID_TEST,
  tipoDeComprobante: 'I',
  fecha: new Date('2025-07-15T10:00:00'),
  version: '4.0',
  serie: 'A',
  folio: '1001',
  moneda: 'MXN',
  tipoCambio: 1,
  formaPago: '03',
  metodoPago: 'PUE',
  lugarExpedicion: '64000',
  emisor: {
    rfc: 'CCO011113663',
    nombre: 'CAR COMERCIALIZADORA SA DE CV',
    regimenFiscal: '601',
  },
  receptor: {
    rfc: 'AOJD9306022E5',
    nombre: 'DAVID ANTONIO OJEDA',
    usoCFDI: 'G03',
    regimenFiscalReceptor: '612',
    domicilioFiscalReceptor: '64000',
  },
  conceptos: [{
    claveProdServ: '81112100',
    claveUnidad: 'E48',
    descripcion: 'Servicio de consultoría TEST',
    cantidad: 1,
    valorUnitario: 5000.00,
    importe: 5000.00,
  }],
  sello: 'SIMULADO_TEST_NO_ES_REAL_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopq',
  noCertificado: '30001000000400002495',
  timbreFiscalDigital: {
    uuid: UUID_TEST,
    fechaTimbrado: new Date('2025-07-15T10:05:00'),
    rfcProvCertif: 'SAT970701NN3',
    noCertificadoSAT: '20001000000300022323',
    selloCFD: 'SIMULADO_TEST',
    selloSAT: 'SIMULADO_SAT',
  },
  isActive: true,
};

const SAT_DOC = {
  ...BASE,
  source: 'SAT',
  subTotal: 5000.00,
  total: 5800.00,                          // ← valor correcto según SAT
  impuestos: {
    totalImpuestosTrasladados: 800.00,     // ← IVA correcto 16%
    traslados: [{ impuesto: '002', tipoFactor: 'Tasa', tasaOCuota: '0.160000', importe: 800.00, base: 5000.00 }],
  },
  emisor: { ...BASE.emisor, regimenFiscal: '601' },
  satStatus: 'Vigente',
  satLastCheck: new Date(),
};

const ERP_DOC = {
  ...BASE,
  source: 'ERP',
  subTotal: 5000.00,
  total: 5900.00,                          // ← ERROR: ERP capturó mal el total
  impuestos: {
    totalImpuestosTrasladados: 900.00,     // ← ERROR: ERP calculó 18% en lugar de 16%
    traslados: [{ impuesto: '002', tipoFactor: 'Tasa', tasaOCuota: '0.180000', importe: 900.00, base: 5000.00 }],
  },
  emisor: { ...BASE.emisor, regimenFiscal: '612' }, // ← ERROR: régimen fiscal distinto
  satStatus: null,
};

async function run() {
  await mongoose.connect(process.env.MONGODB_URI);
  console.log('Conectado a MongoDB');

  const satDoc = await CFDI.findOneAndUpdate(
    { uuid: UUID_TEST, source: 'SAT' },
    SAT_DOC,
    { upsert: true, new: true, setDefaultsOnInsert: true }
  );

  const erpDoc = await CFDI.findOneAndUpdate(
    { uuid: UUID_TEST, source: 'ERP' },
    ERP_DOC,
    { upsert: true, new: true, setDefaultsOnInsert: true }
  );

  console.log('\n✓ Par de CFDIs de prueba listo:');
  console.log(`  SAT _id: ${satDoc._id}   total=$${satDoc.total}   IVA=$${satDoc.impuestos.totalImpuestosTrasladados}   régimen=${satDoc.emisor.regimenFiscal}`);
  console.log(`  ERP _id: ${erpDoc._id}   total=$${erpDoc.total}   IVA=$${erpDoc.impuestos.totalImpuestosTrasladados}   régimen=${erpDoc.emisor.regimenFiscal}`);
  console.log(`\n  UUID: ${UUID_TEST}`);
  console.log('\n  Discrepancias esperadas:');
  console.log('    [critical] total                              ERP=5900  SAT=5800  Δ=$100');
  console.log('    [critical] impuestos.totalImpuestosTrasladados ERP=900   SAT=800   Δ=$100');
  console.log('    [warning]  emisor.regimenFiscal               ERP=612   SAT=601');
  console.log('\n  → En la UI: tab CFDIs, origen ERP, click "Comparar"');
  console.log(`  → O directo: POST /api/cfdis/${erpDoc._id}/compare\n`);

  await mongoose.disconnect();
}

run().catch(err => { console.error(err); process.exit(1); });
