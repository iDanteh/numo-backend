'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { sequelize } = require('./src/config/database.postgres');
const { Op } = require('sequelize');
const CFDI = require('./src/visor/models/CFDI');
const CentroCosto = require('./src/shared/models/postgres/CentroCosto');
const CobroSucursalPendiente = require('./src/shared/models/postgres/CobroSucursalPendiente');
const PolizaMovimiento = require('./src/shared/models/postgres/PolizaMovimiento');
const Poliza = require('./src/shared/models/postgres/Poliza');
const { obtenerDesglosesCobroAlmacen } = require('./src/banks/domains/erp/erp-sync.service');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const UUID_FACTURA = process.env.DIAG_UUID || 'E48070D3-0456-4B1A-83FA-630BF7ACE3A6';
const SERIE_VENTA = 'B0';
const FOLIO_VENTA = '260801859';
const SERIE_COBRO = 'CBT';
const FOLIO_COBRO = '260821267';

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  const cfdi = await CFDI.findOne({ uuid: UUID_FACTURA, source: 'SAT' })
    .select('uuid serie folio total fecha emisor.rfc receptor.nombre tipoDeComprobante metodoPago satStatus').lean();
  console.log('1) CFDI real:', JSON.stringify(cfdi, null, 2));

  // Consultar el ticket/cuenta directamente por su serie-folio de venta,
  // sin restringir por fecha -- para ver el cobro CBT y su claveCentro crudo,
  // tal como lo regresa el ERP.
  const resultado = await obtenerDesglosesCobroAlmacen({
    rfc: RFC, series: [SERIE_VENTA], folios: [FOLIO_VENTA],
  });
  console.log(`\n2) /desgloses-cobro/almacen por serie+folio (${SERIE_VENTA}-${FOLIO_VENTA}):`, JSON.stringify(resultado, null, 2));

  const centros = await CentroCosto.findAll({ attributes: ['id', 'clave', 'sucursal', 'serieFacturacion'], raw: true });
  console.log('\n3) Catalogo CentroCosto (id/clave/sucursal/serieFacturacion):', JSON.stringify(centros));

  const pendientes = await CobroSucursalPendiente.findAll({
    where: {
      [Op.or]: [
        { serieFolioTicket: `${SERIE_VENTA}-${FOLIO_VENTA}` },
        { folioOrigen: FOLIO_COBRO },
        { cfdiUuid: UUID_FACTURA },
      ],
    },
    raw: true,
  });
  console.log('\n4) CobroSucursalPendiente ligado a este ticket/cfdi:', JSON.stringify(pendientes, null, 2));

  // 5. TODOS los movimientos de poliza (cualquier poliza) ligados a este
  // cfdiUuid -- para ver exactamente que genero cada linea y en que poliza
  // quedo (la factura real es del 11-ago segun el CFDI, no del 7).
  const movs = await PolizaMovimiento.findAll({
    where: { cfdiUuid: UUID_FACTURA },
    attributes: ['id', 'polizaId', 'cuentaId', 'debe', 'haber', 'tipoOrigen', 'reglaNombre', 'concepto', 'serie', 'folio', 'formaPago', 'centroCosto'],
    raw: true,
  });
  console.log(`\n5) PolizaMovimiento con cfdiUuid=${UUID_FACTURA}: ${movs.length}`);
  for (const m of movs) console.log(JSON.stringify(m));

  if (movs.length) {
    const polizaIds = [...new Set(movs.map(m => m.polizaId))];
    const polizas = await Poliza.findAll({ where: { id: { [Op.in]: polizaIds } }, attributes: ['id', 'numero', 'fecha', 'estado', 'createdAt'], raw: true });
    console.log('\n6) Polizas donde aparece esta factura:', JSON.stringify(polizas, null, 2));
  }

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
