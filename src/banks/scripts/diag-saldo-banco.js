'use strict';

/**
 * diag-saldo-banco.js
 * Replica (solo lectura) la logica de `calcularSaldosBanco` en
 * report.controller.js para un numero de autorizacion especifico: lista
 * TODAS las aplicaciones (pago-CFDI + factura) que tocan ese deposito,
 * ordenadas cronologicamente, y el saldo corriente del deposito despues de
 * cada una. Sirve para validar los numeros antes de desplegar el cambio de
 * "Saldo Banco" en el reporte de Pagos Asociados.
 *
 * Uso:
 *   node src/banks/scripts/diag-saldo-banco.js <numeroAutorizacion>
 */

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const CFDI = require('../../visor/models/CFDI');
const BankMovement = require('../domains/banks/BankMovement.model');

const numAutorizacion = process.argv[2];
if (!numAutorizacion) {
  console.error('Uso: node diag-saldo-banco.js <numeroAutorizacion>');
  process.exit(1);
}

const DEDUP_PAGO_PREFIERE_SAT = [
  { $addFields: { _srcOrden: { $cond: [{ $eq: ['$source', 'SAT'] }, 0, 1] } } },
  { $sort: { uuid: 1, _srcOrden: 1 } },
  { $group: { _id: '$uuid', doc: { $first: '$$ROOT' } } },
  { $replaceRoot: { newRoot: '$doc' } },
];

async function main() {
  await connectMongo();

  const movimientos = await BankMovement.find({
    $or: [
      { numeroAutorizacion: numAutorizacion },
      { referenciaNumerica: numAutorizacion },
    ],
  }, { banco: 1, fecha: 1, deposito: 1, folio: 1, numeroAutorizacion: 1, referenciaNumerica: 1, 'erpLinks.folioFiscal': 1 }).lean();

  console.log(`Movimientos bancarios encontrados con num. autorizacion "${numAutorizacion}": ${movimientos.length}`);
  for (const m of movimientos) {
    console.log('---');
    console.log(`Movimiento _id=${m._id} banco=${m.banco} fecha=${m.fecha} deposito=${m.deposito} folio=${m.folio}`);
    const folios = (m.erpLinks ?? []).map((l) => (l.folioFiscal || '').toUpperCase()).filter(Boolean);
    const foliosUnicos = [...new Set(folios)];
    console.log(`Facturas ligadas (erpLinks.folioFiscal): ${foliosUnicos.length}`);
    console.log(foliosUnicos);

    if (foliosUnicos.length === 0) continue;

    const aplicaciones = await CFDI.aggregate([
      {
        $match: {
          tipoDeComprobante: 'P',
          isActive: true,
          'complementoPago.pagos.doctosRelacionados.idDocumento': {
            $in: foliosUnicos.map((f) => new RegExp(`^${f}$`, 'i')),
          },
        },
      },
      ...DEDUP_PAGO_PREFIERE_SAT,
      { $unwind: '$complementoPago.pagos' },
      { $unwind: '$complementoPago.pagos.doctosRelacionados' },
      {
        $project: {
          _id:         0,
          cfdiUuid:    { $toUpper: '$uuid' },
          source:      '$source',
          facturaUuid: { $toUpper: '$complementoPago.pagos.doctosRelacionados.idDocumento' },
          fechaPago:   '$complementoPago.pagos.fechaPago',
          impPagado:   '$complementoPago.pagos.doctosRelacionados.impPagado',
        },
      },
    ]);

    const relevantes = aplicaciones
      .filter((a) => foliosUnicos.includes(a.facturaUuid))
      .sort((a, b) => new Date(a.fechaPago) - new Date(b.fechaPago));

    console.log(`Aplicaciones encontradas (pago-CFDI + factura), orden cronologico: ${relevantes.length}`);
    let acumulado = 0;
    for (const a of relevantes) {
      acumulado += Number(a.impPagado) || 0;
      const saldoTrasEsta = Math.round(((m.deposito ?? 0) - acumulado) * 100) / 100;
      console.log({
        cfdiPago: a.cfdiUuid,
        source: a.source,
        factura: a.facturaUuid,
        fechaPago: a.fechaPago,
        impPagado: a.impPagado,
        acumulado: Math.round(acumulado * 100) / 100,
        saldoBancoTrasEsta: saldoTrasEsta,
      });
    }
  }

  await disconnectMongo();
  process.exit(0);
}

main().catch((err) => { console.error(err); process.exit(1); });
