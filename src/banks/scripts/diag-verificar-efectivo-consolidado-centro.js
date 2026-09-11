'use strict';

/**
 * diag-verificar-efectivo-consolidado-centro.js
 *
 * Diagnostico de SOLO LECTURA: recalcula, de forma INDEPENDIENTE y
 * simplificada, cuanto Efectivo real se cobro en un centro (sucursal) en un
 * dia especifico, para comparar contra "Depositos consolidados (Efectivo)"
 * de la poliza real (ese numero NO existe como fila en poliza_movimientos --
 * se calcula al exportar, en consolidarCargos/poliza.service.js).
 *
 * QUE HACE (aproximado, no es una replica exacta del pipeline real):
 *   1. Busca en Mongo (real) todos los CFDI tipo Ingreso de la serie del
 *      centro, con fecha de emision en el dia pedido.
 *   2. Llama a /desgloses-cobro/almacen (por esos series+folios, el mismo
 *      endpoint que usa cfdi-mapping.service.js) para traer el desglose real
 *      de cobro de cada cuenta.
 *   3. Suma, por cada cobro real (series ABO/CBT/CPF/CFC + APS/MIS, igual
 *      criterio que cobros-sucursal-puente.service.js), la porcion cubierta
 *      por formaPago Efectivo (claveSat '01').
 *
 * QUE NO CUBRE (por eso es un gut-check, no una verificacion exacta):
 *   - Facturas tardias (CFDI timbrado en OTRO dia distinto al cobro real) --
 *     el mecanismo "SF usado en el dia real del cobro" (10-sep) y "Cobros sin
 *     factura" pueden sumar/restar montos que este script no ve porque solo
 *     mira CFDIs cuya FECHA DE EMISION cae en el dia pedido.
 *   - Cobros cruzados de otra sucursal (_extraerCobrosSucursal) -- esos NO
 *     deben entrar a "Depositos consolidados", asi que su ausencia aqui es
 *     correcta, no un error.
 *   - SF-oculto (genera y usa el saldo el mismo dia/almacen) y otros ajustes
 *     de saldo a favor/puntos.
 *   - Residuos <$10, devoluciones, cancelaciones.
 *
 * Un desfase pequeño (unos cuantos cientos de pesos) puede ser normal por lo
 * de arriba. Un desfase GRANDE (miles de pesos) sí amerita investigar.
 *
 * Uso:
 *   node src/banks/scripts/diag-verificar-efectivo-consolidado-centro.js <rfc> <serieCentro> <fecha YYYY-MM-DD>
 *   ej:  node src/banks/scripts/diag-verificar-efectivo-consolidado-centro.js CCO011113663 B0 2026-09-02
 */

require('dotenv').config();

const mongoose = require('mongoose');
const { sequelize } = require('../../config/database.postgres');
const { obtenerDesglosesCobroAlmacen } = require('../domains/erp/erp-sync.service');
const { SERIES_CON_AUTH } = require('../domains/erp/erp-auth.utils');

const CLAVE_SAT_EFECTIVO = '01';
const LOTE = 100;

const [rfc, serieCentro, fecha] = process.argv.slice(2);
if (!rfc || !serieCentro || !fecha) {
  console.error('Uso: node diag-verificar-efectivo-consolidado-centro.js <rfc> <serieCentro> <fecha YYYY-MM-DD>');
  process.exit(1);
}

async function main() {
  await mongoose.connect(process.env.MONGODB_URI);
  await sequelize.authenticate();

  const desde = new Date(`${fecha}T00:00:00.000Z`);
  const hasta = new Date(`${fecha}T23:59:59.999Z`);

  const db = mongoose.connection.db;
  const cfdis = await db.collection('cfdis').find({
    'emisor.rfc':       rfc,
    serie:              serieCentro,
    tipoDeComprobante:  'I',
    isActive:           { $ne: false },
    fecha:              { $gte: desde, $lte: hasta },
  }, { projection: { serie: 1, folio: 1, total: 1 } }).toArray();

  console.log(`CFDIs de Ingreso ${serieCentro} el ${fecha}: ${cfdis.length}`);
  if (!cfdis.length) {
    console.log('Sin CFDIs -- nada que sumar. Revisa el RFC/serie/fecha.');
    process.exit(0);
  }

  const series = cfdis.map(c => c.serie);
  const folios = cfdis.map(c => String(c.folio));

  let totalEfectivo   = 0;
  let totalCobrosVisto = 0;
  const detalle = [];

  for (let i = 0; i < folios.length; i += LOTE) {
    const loteSeries = series.slice(i, i + LOTE);
    const loteFolios = folios.slice(i, i + LOTE);
    const cuentas = await obtenerDesglosesCobroAlmacen({ rfc, series: loteSeries, folios: loteFolios });

    for (const cuenta of cuentas) {
      for (const cobro of (cuenta.cobros ?? [])) {
        const origen = (cobro.serieOrigen ?? '').toUpperCase();
        if (origen !== 'APS' && origen !== 'MIS' && !SERIES_CON_AUTH.includes(origen)) continue;

        const fechaCobro = cobro.fecha ? new Date(cobro.fecha) : null;
        if (!fechaCobro || fechaCobro < desde || fechaCobro > hasta) continue;

        const montoCobro = Math.abs(Number(cobro.monto) || 0);
        totalCobrosVisto += montoCobro;

        const formasPago = (cobro.formasPago ?? []).length
          ? cobro.formasPago
          : [{ claveSat: null, monto: montoCobro }];
        const totalFp = formasPago.reduce((s, fp) => s + (Number(fp.monto) || 0), 0);

        let acumulado = 0;
        formasPago.forEach((fp, idx) => {
          const esUltimo = idx === formasPago.length - 1;
          const share = totalFp > 0 ? (Number(fp.monto) || 0) / totalFp : 1 / formasPago.length;
          const montoAsignado = esUltimo
            ? Math.round((montoCobro - acumulado) * 100) / 100
            : Math.round(montoCobro * share * 100) / 100;
          acumulado += montoAsignado;

          if ((fp.claveSat ?? '').trim() === CLAVE_SAT_EFECTIVO) {
            totalEfectivo += montoAsignado;
            detalle.push({
              serieVenta: cuenta.serieVenta, folioVenta: cuenta.folioVenta,
              folioOrigen: cobro.folioOrigen, monto: montoAsignado,
            });
          }
        });
      }
    }
  }

  console.log(`\nTotal cobros reales vistos (todas las formas de pago): $${totalCobrosVisto.toFixed(2)}`);
  console.log(`Total Efectivo recalculado (independiente):            $${totalEfectivo.toFixed(2)}`);
  console.log(`\nCompara este numero contra "Depositos consolidados (Efectivo)" del export.`);
  console.log(`Detalle (${detalle.length} cobros Efectivo):`);
  for (const d of detalle) {
    console.log(`  ${d.serieVenta}-${d.folioVenta} (${d.folioOrigen}): $${d.monto.toFixed(2)}`);
  }

  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e.stack || e.message); process.exit(1); });
