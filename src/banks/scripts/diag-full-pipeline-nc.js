'use strict';

/**
 * diag-full-pipeline-nc.js
 * Replica el pipeline completo de generarYGuardar/_fetchNotasCreditoParaFusion
 * para UNA NC especifica: enriquecimiento SAT+ERP, normalizaciones en memoria,
 * matching de regla, y calculo de esPPD/cuentaIvaAplicable. Solo lectura.
 *
 * Uso:
 *   node src/banks/scripts/diag-full-pipeline-nc.js <uuid-de-la-NC>
 */

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const { sequelize } = require('../../config/database.postgres');
const CFDI = require('../../visor/models/CFDI');
const mappingSvc = require('../domains/cfdi-mapping/cfdi-mapping.service');
const {
  _getRulesActive,
  _normalizarEgresoPue99,
  _normalizarEgresoCondonacion,
  _normalizarEgresoSegunFacturaRelacionada,
} = require('../domains/cfdi-mapping/balanza-preliminar.service');

const uuidArg = process.argv[2];
if (!uuidArg) {
  console.error('Uso: node diag-full-pipeline-nc.js <uuid>');
  process.exit(1);
}

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  const docs = await CFDI.find({ uuid: new RegExp('^' + uuidArg + '$', 'i') })
    .select('uuid tipoDeComprobante metodoPago formaPago fecha folio serie emisor receptor subTotal total descuento impuestos complementoPago conceptos cfdiRelacionados tasaIvaInferida documentosRelacionados tipoOrigen source')
    .lean();

  const sat = docs.find(function (d) { return d.source === 'SAT'; });
  const erp = docs.find(function (d) { return d.source === 'ERP'; });

  if (!sat) {
    console.log('No hay documento SAT para este uuid -- el pipeline real no lo procesaria (requiere source=SAT).');
    await cerrar();
    return;
  }

  console.log('--- Paso 0: documento SAT crudo ---');
  console.log({ metodoPago: sat.metodoPago, formaPago: sat.formaPago, tipoOrigen: sat.tipoOrigen });

  // Paso 1: enriquecimiento SAT+ERP (misma logica que _fetchNotasCreditoParaFusion / generarYGuardar)
  var cfdi = Object.assign({}, sat);
  if (erp) {
    var satHasTraslados = (cfdi.conceptos || []).some(function (con) { return con.impuestos && con.impuestos.traslados && con.impuestos.traslados.length; });
    var relSAT = cfdi.cfdiRelacionados || [];
    var tiposEnSAT = new Set(relSAT.map(function (r) { return r.tipoRelacion; }));
    var relERP = (erp.cfdiRelacionados || []).filter(function (r) { return !tiposEnSAT.has(r.tipoRelacion); });
    var esBCT = (erp.documentosRelacionados || []).some(function (d) { return d.Serie === 'BCT'; });
    var esBON = !esBCT && (erp.documentosRelacionados || []).some(function (d) { return (d.Serie || '').indexOf('BON') === 0; });
    cfdi.formaPago = cfdi.formaPago || erp.formaPago;
    cfdi.metodoPago = cfdi.metodoPago || erp.metodoPago;
    cfdi.conceptos = satHasTraslados ? cfdi.conceptos : ((erp.conceptos && erp.conceptos.length) ? erp.conceptos : (cfdi.conceptos || []));
    cfdi.impuestos = erp.impuestos || cfdi.impuestos;
    cfdi.tipoOrigen = esBCT ? 'Bonificacion Club Tuberos' : (esBON ? 'Bonificacion' : (cfdi.tipoOrigen || erp.tipoOrigen || null));
    cfdi.documentosRelacionados = erp.documentosRelacionados || cfdi.documentosRelacionados || [];
    cfdi.cfdiRelacionados = relERP.length ? relSAT.concat(relERP) : relSAT;
  }

  console.log('--- Paso 1: despues de enriquecimiento SAT+ERP ---');
  console.log({ metodoPago: cfdi.metodoPago, formaPago: cfdi.formaPago, tipoOrigen: cfdi.tipoOrigen });

  // Paso 2: resolver factura(s) relacionada(s) tipoRelacion 01/03
  var relUuids = (cfdi.cfdiRelacionados || [])
    .filter(function (r) { return r.tipoRelacion === '01' || r.tipoRelacion === '03'; })
    .reduce(function (acc, r) { return acc.concat(r.uuids || (r.uuid ? [r.uuid] : [])); }, []);

  var metodoPagoPorFactura = {};
  var facturaRelacionadaMeta = {};
  for (var i = 0; i < relUuids.length; i++) {
    var u = relUuids[i].toUpperCase();
    var facturaDocs = await CFDI.find({ uuid: new RegExp('^' + relUuids[i] + '$', 'i') })
      .select('uuid metodoPago formaPago source').lean();
    console.log('Factura relacionada ' + relUuids[i] + ' -- documentos encontrados: ' + facturaDocs.length);
    facturaDocs.forEach(function (f) { console.log('  source=' + f.source + ' metodoPago=' + f.metodoPago + ' formaPago=' + f.formaPago); });
    var facturaSat = facturaDocs.find(function (f) { return f.source === 'SAT'; }) || facturaDocs[0];
    if (facturaSat) {
      metodoPagoPorFactura[u] = facturaSat.metodoPago;
      facturaRelacionadaMeta[u] = { metodoPago: facturaSat.metodoPago, formaPago: facturaSat.formaPago };
    }
  }

  // Paso 3: normalizaciones en memoria, mismo orden que el pipeline real
  var arr = [cfdi];
  _normalizarEgresoPue99(arr);
  console.log('--- Paso 3a: despues de _normalizarEgresoPue99 ---');
  console.log({ metodoPago: cfdi.metodoPago, formaPago: cfdi.formaPago });

  _normalizarEgresoCondonacion(arr, metodoPagoPorFactura);
  console.log('--- Paso 3b: despues de _normalizarEgresoCondonacion ---');
  console.log({ metodoPago: cfdi.metodoPago, formaPago: cfdi.formaPago });

  _normalizarEgresoSegunFacturaRelacionada(arr, facturaRelacionadaMeta);
  console.log('--- Paso 3c: despues de _normalizarEgresoSegunFacturaRelacionada ---');
  console.log({ metodoPago: cfdi.metodoPago, formaPago: cfdi.formaPago });

  // Paso 4: matching de regla (mismas reglas activas que usa produccion)
  var rules = await _getRulesActive();
  var rule = mappingSvc.findRuleInList(cfdi, rules);

  if (!rule) {
    console.log('--- Paso 4: NINGUNA regla hizo match ---');
    await cerrar();
    return;
  }

  console.log('--- Paso 4: regla que hace match ---');
  console.log({
    id: rule.id,
    nombre: rule.nombre,
    prioridad: rule.prioridad,
    tipoComprobante: rule.tipoComprobante,
    metodoPago: rule.metodoPago,
    formaPago: rule.formaPago,
    tipoOrigen: rule.tipoOrigen,
    tipoRelacion: rule.tipoRelacion,
    cuentaIva: rule.cuentaIva,
    cuentaIvaPPD: rule.cuentaIvaPPD,
  });

  // Paso 5: calculo manual de esPPD y cuentaIvaAplicable (misma logica que cfdi-mapping.service.js)
  var context = {};
  // (en el pipeline real, context.metodoPagoRelacionado se llena si el uuid
  // relacionado esta en el mapa de la factura ya cargada en el batch -- aqui
  // lo replicamos siempre que se encontro la factura)
  var relUuidTop = relUuids[0];
  if (relUuidTop && metodoPagoPorFactura[relUuidTop.toUpperCase()]) {
    context.metodoPagoRelacionado = metodoPagoPorFactura[relUuidTop.toUpperCase()];
  }

  var esPPD = (cfdi.tipoDeComprobante === 'E' && context.metodoPagoRelacionado)
    ? context.metodoPagoRelacionado === 'PPD'
    : cfdi.metodoPago === 'PPD';

  var cuentaIvaAplicable = (esPPD && rule.cuentaIvaPPD) ? rule.cuentaIvaPPD : rule.cuentaIva;

  console.log('--- Paso 5: resultado final ---');
  console.log({
    'context.metodoPagoRelacionado': context.metodoPagoRelacionado,
    'cfdi.metodoPago (final)': cfdi.metodoPago,
    esPPD: esPPD,
    cuentaIvaAplicable: cuentaIvaAplicable,
  });

  await cerrar();
}

async function cerrar() {
  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(function (err) { console.error(err); process.exit(1); });
