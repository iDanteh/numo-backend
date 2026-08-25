'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { sequelize } = require('./src/config/database.postgres');
const CFDI = require('./src/visor/models/CFDI');
const mappingSvc = require('./src/banks/domains/cfdi-mapping/cfdi-mapping.service.js');
const CfdiMappingRule = require('./src/shared/models/postgres/CfdiMappingRule');
const { _prefetchAjustesFacturaPropia } = require('./src/banks/domains/cfdi-mapping/cfdi-poliza-generator.service.js');
const { obtenerDesglosesCobroAlmacenPorCentro } = require('./src/banks/domains/erp/erp-sync.service');
const { SERIES_CON_AUTH } = require('./src/banks/domains/erp/erp-auth.utils');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const SERIE = process.env.DIAG_SERIE || 'B0';
const FOLIO = process.env.DIAG_FOLIO || '260801859';
const FECHA = process.env.DIAG_FECHA || '2026-08-11';

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  const cfdi = await CFDI.findOne({ 'emisor.rfc': RFC, serie: SERIE, folio: FOLIO, source: 'SAT', satStatus: 'Vigente' }).lean();
  if (!cfdi) { console.log('CFDI NO ENCONTRADO (SAT Vigente) para', SERIE, FOLIO); process.exit(1); }
  console.log('CFDI:', JSON.stringify({
    uuid: cfdi.uuid, total: cfdi.total, subTotal: cfdi.subTotal, formaPago: cfdi.formaPago,
    metodoPago: cfdi.metodoPago, fecha: cfdi.fecha, receptorNombre: cfdi.receptor?.nombre,
    tipoDeComprobante: cfdi.tipoDeComprobante,
  }, null, 2));

  const rules = await CfdiMappingRule.findAll({ where: { isActive: true }, order: [['prioridad', 'ASC']] });
  const rule = mappingSvc.findRuleInList(cfdi, rules);
  console.log('\nRegla matcheada:', rule ? JSON.stringify({ nombre: rule.nombre, cuentaCargo: rule.cuentaCargo, tasaIva: rule.tasaIva, formaPago: rule.formaPago }) : 'NINGUNA');

  const desde = new Date(`${FECHA}T00:00:00-06:00`);
  const hasta = new Date(`${FECHA}T23:59:59.999-06:00`);
  const { desglosePagoReal, saldoFavorUsado, puntosUsado } = await _prefetchAjustesFacturaPropia(
    [{ cfdi, rule }], RFC, { centroPropioClave: SERIE, fechaDesde: desde, fechaHasta: hasta },
  );
  const key = `${cfdi.serie}|${cfdi.folio}`;
  const formasPago = desglosePagoReal.get(key) ?? [];
  const totalFormasPagoReal = formasPago.reduce((s, fp) => s + (Number(fp.monto) || 0), 0);
  console.log('\ndesglosePagoReal encontrado:', JSON.stringify(formasPago, null, 2));
  console.log('totalFormasPagoReal:', totalFormasPagoReal.toFixed(2));
  console.log('montoSFUsado:', saldoFavorUsado.get(key)?.monto ?? 0);
  console.log('montoPuntosUsado:', puntosUsado.get(key) ?? 0);
  console.log('\nmontoCargo (total CFDI):', cfdi.total);
  console.log('excesoCasoNormal (montoCargo - totalFormasPagoReal):', (Number(cfdi.total) - totalFormasPagoReal).toFixed(2));

  // Buscar TODOS los cobros crudos (sin filtrar por dedupe/origen) ligados a
  // esta factura, para ver si hay algo que el split normal no esta tomando.
  const fechaDesdeIso = new Date(`${FECHA}T00:00:00-06:00`).toISOString();
  const fechaHastaIso = new Date(`${FECHA}T23:59:59.999-06:00`).toISOString();
  const resultado = await obtenerDesglosesCobroAlmacenPorCentro({ rfc: RFC, centro: SERIE, fechaDesde: fechaDesdeIso, fechaHasta: fechaHastaIso });
  const cuentasDeEstaFactura = resultado.filter(c => c.serieFactura === SERIE && String(c.folioFactura) === FOLIO);
  console.log(`\nTickets (cuentas) ligados a esta factura en /desgloses-cobro/almacen: ${cuentasDeEstaFactura.length}`);
  for (const c of cuentasDeEstaFactura) {
    console.log(JSON.stringify({ serieVenta: c.serieVenta, folioVenta: c.folioVenta, cobros: c.cobros }, null, 2));
  }

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
