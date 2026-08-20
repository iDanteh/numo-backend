'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const CFDI = require('./src/visor/models/CFDI');
const { _uuidsPorFechaEfectiva } = require('./src/banks/domains/cfdi-mapping/cfdi-poliza-generator.service.js');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const FECHA = process.env.DIAG_FECHA || '2026-08-11';
const EJERCICIO = process.env.DIAG_EJERCICIO || '2026';
const PERIODO = process.env.DIAG_PERIODO || '8';

// Replica EXACTA de la seleccion real de CFDIs que usa generarPropuesta/
// generarYGuardar (_uuidsPorFechaEfectiva + filtroBase con source='SAT'),
// para comparar contra lo que mis diagnosticos de hoy leyeron (source='ERP').
async function main() {
  await connectMongo();

  const uuidsPorFecha = await _uuidsPorFechaEfectiva({
    rfc: RFC, ejercicio: EJERCICIO, periodo: PERIODO, tipoCfdi: 'I',
    fechaInicio: FECHA, fechaFin: FECHA,
  });
  console.log('Total UUIDs por fecha efectiva:', uuidsPorFecha.size);

  const filtroBaseReal = {
    'emisor.rfc': RFC, ejercicio: Number(EJERCICIO), periodo: Number(PERIODO),
    tipoDeComprobante: 'I', source: 'SAT', satStatus: 'Vigente',
    uuid: { $in: [...uuidsPorFecha] }, isActive: true,
  };
  const cfdisSat = await CFDI.find(filtroBaseReal).select('uuid serie folio total formaPago').lean();
  console.log('Total CFDIs SAT (filtro real de generacion):', cfdisSat.length);

  const cfdisSatB0 = cfdisSat.filter(c => c.serie === 'B0');
  console.log('De esos, serie B0:', cfdisSatB0.length);
  const totalSatB0 = cfdisSatB0.reduce((s, c) => s + (Number(c.total) || 0), 0);
  console.log('Suma total (SAT, B0):', totalSatB0.toFixed(2));

  // Comparar contra la version ERP de los MISMOS uuids.
  const uuidsB0 = cfdisSatB0.map(c => c.uuid);
  const cfdisErp = await CFDI.find({ uuid: { $in: uuidsB0 }, source: 'ERP' }).select('uuid serie folio total formaPago').lean();
  console.log('De esos mismos uuids, cuantos tienen version ERP:', cfdisErp.length);
  const totalErpB0 = cfdisErp.reduce((s, c) => s + (Number(c.total) || 0), 0);
  console.log('Suma total (ERP, mismos uuids):', totalErpB0.toFixed(2));

  // Diferencias por CFDI (SAT vs ERP) donde el total no coincide, o falta la
  // version ERP, o falta la version SAT.
  const erpPorUuid = new Map(cfdisErp.map(c => [c.uuid, c]));
  console.log('\nDiferencias SAT vs ERP (total distinto o falta ERP):');
  for (const satC of cfdisSatB0) {
    const erpC = erpPorUuid.get(satC.uuid);
    if (!erpC) {
      console.log(`SIN VERSION ERP: uuid=${satC.uuid} folio=${satC.folio} total_SAT=${satC.total}`);
      continue;
    }
    if (Math.abs((Number(satC.total) || 0) - (Number(erpC.total) || 0)) > 0.01) {
      console.log(`TOTAL DISTINTO: uuid=${satC.uuid} folio=${satC.folio} total_SAT=${satC.total} total_ERP=${erpC.total} folio_ERP=${erpC.folio}`);
    }
  }

  // Uuids que la version ERP SI tiene con serie B0 pero que NO estan en la
  // lista SAT B0 -- candidatos a explicar la brecha (existen en ERP pero no
  // fueron seleccionados por el filtro real).
  const uuidsSatB0Set = new Set(cfdisSatB0.map(c => c.uuid));
  const erpTodosB0 = await CFDI.find({ 'emisor.rfc': RFC, serie: 'B0', tipoDeComprobante: 'I', source: 'ERP',
    fecha: { $gte: new Date(`${FECHA}T00:00:00-06:00`), $lte: new Date(`${FECHA}T23:59:59.999-06:00`) } })
    .select('uuid folio total').lean();
  const soloEnErp = erpTodosB0.filter(c => !uuidsSatB0Set.has(c.uuid));
  console.log(`\nCFDIs ERP con serie B0 y fecha ${FECHA} que NO estan en la seleccion SAT real:`, soloEnErp.length);
  for (const c of soloEnErp) console.log(JSON.stringify(c));
  const totalSoloEnErp = soloEnErp.reduce((s, c) => s + (Number(c.total) || 0), 0);
  console.log('Suma de esos (solo en ERP, no en SAT seleccionado):', totalSoloEnErp.toFixed(2));

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
