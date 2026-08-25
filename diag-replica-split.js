'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { sequelize } = require('./src/config/database.postgres');
const CFDI = require('./src/visor/models/CFDI');
const { _prefetchAjustesFacturaPropia } = require('./src/banks/domains/cfdi-mapping/cfdi-poliza-generator.service.js');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const SERIE = process.env.DIAG_SERIE || 'B0';
const FECHA = process.env.DIAG_FECHA || '2026-08-11';

// Replica EXACTA de la logica de esCasoAjusteSFPuntos/esCasoNormalParaSplit en
// cfdi-mapping.service.js (lineas ~766-1016) para TODAS las facturas del
// batch, sumando el resultado final de Efectivo/Tarjeta/Transferencia -- para
// comparar contra el total real de la poliza generada ($219,089.06) y
// encontrar EXACTAMENTE que facturas/mecanismos explican el gap contra el
// reporte real del ERP ($256,295.27).
async function main() {
  await connectMongo();
  await sequelize.authenticate();

  const desde = new Date(`${FECHA}T00:00:00-06:00`);
  const hasta = new Date(`${FECHA}T23:59:59.999-06:00`);
  const cfdis = await CFDI.find({
    'emisor.rfc': RFC, serie: SERIE, tipoDeComprobante: 'I', source: 'ERP',
    fecha: { $gte: desde, $lte: hasta },
  }).select('uuid serie folio fecha total metodoPago formaPago tipoDeComprobante receptor.nombre').lean();

  const cfdiConRegla = cfdis.map(cfdi => ({ cfdi, rule: { cuentaCargo: '1101010003' } }));
  const { desglosePagoReal, puntosUsado, saldoFavorUsado } = await _prefetchAjustesFacturaPropia(cfdiConRegla, RFC, {
    centroPropioClave: SERIE, fechaDesde: desde, fechaHasta: hasta,
  });

  const totales = { '01': 0, '28': 0, '04': 0, '03': 0, otros: 0 };
  const detalle = [];

  for (const cfdi of cfdis) {
    const key = `${cfdi.serie}|${cfdi.folio}`;
    const formasPago = desglosePagoReal.get(key) ?? [];
    const totalFormasPagoReal = formasPago.reduce((s, fp) => s + (Number(fp.monto) || 0), 0);
    const montoSFUsado = Number(saldoFavorUsado.get(key)?.monto) || 0;
    const montoPuntosUsado = Number(puntosUsado.get(key)) || 0;
    const montoCargo = Number(cfdi.total) || 0;

    const esCasoAjusteSFPuntos = (montoSFUsado > 0 || montoPuntosUsado > 0);
    const esCasoNormalParaSplit = !esCasoAjusteSFPuntos && formasPago.length > 0;

    const CLAVES_CONOCIDAS = ['01', '28', '04', '03'];
    const sumaPorLinea = () => {
      formasPago.forEach(fp => {
        const m = Math.round((Number(fp.monto) || 0) * 100) / 100;
        if (m <= 0) return;
        const clave = (fp.claveSat ?? '').trim();
        if (CLAVES_CONOCIDAS.includes(clave)) totales[clave] += m; else totales.otros += m;
      });
    };

    let excesoAsignado = 0;
    let excesoExcluido = false;

    if (esCasoAjusteSFPuntos) {
      let restante = montoCargo - montoSFUsado - Math.min(montoPuntosUsado, montoCargo - montoSFUsado);
      restante = Math.round(restante * 100) / 100;
      if (formasPago.length > 0) {
        const excesoCubrir = totalFormasPagoReal > 0 ? Math.round((restante - totalFormasPagoReal) * 100) / 100 : 0;
        if (excesoCubrir > 0.01 && totalFormasPagoReal > 0) {
          totales['01'] += excesoCubrir; // esCasoAjusteSFPuntos NUNCA excluye el exceso (va a CAJA sin tag)
          excesoAsignado = excesoCubrir;
          sumaPorLinea();
        } else {
          // restante se reparte proporcional -- pero el codigo real usa splitPorFormaPagoReal
          // anclado en `restante` (no en totalFormasPagoReal) solo cuando NO hay exceso;
          // aqui simplificamos: si no hay exceso, los montos de formasPago YA representan
          // el 100% de `restante` (validado con datos reales que closes).
          sumaPorLinea();
        }
      } else {
        // Sin desglose -- todo el restante cae a la cuenta de la regla (Caja, en nuestro caso).
        totales['01'] += restante;
      }
    } else if (esCasoNormalParaSplit) {
      const excesoCasoNormal = totalFormasPagoReal > 0 ? Math.round((montoCargo - totalFormasPagoReal) * 100) / 100 : 0;
      if (excesoCasoNormal > 0.01 && totalFormasPagoReal > 0) {
        excesoExcluido = true; // 'Venta Sin Cobro' -- SI se excluye del consolidado
        excesoAsignado = excesoCasoNormal;
        sumaPorLinea();
      } else {
        sumaPorLinea();
      }
    } else {
      // Fallback: cae al formaPago declarado del CFDI.
      const fp = (cfdi.formaPago ?? '').trim() || 'otros';
      if (totales[fp] === undefined) totales.otros += montoCargo; else totales[fp] += montoCargo;
    }

    detalle.push({
      folio: cfdi.folio, total: cfdi.total, formaPagoDeclarado: cfdi.formaPago,
      esCasoAjusteSFPuntos, esCasoNormalParaSplit, montoSFUsado, montoPuntosUsado,
      totalFormasPagoReal, excesoAsignado, excesoExcluido,
    });
  }

  console.log('Totales replicados por claveSat:', JSON.stringify(totales, null, 2));
  console.log('\nEfectivo (01):', totales['01'].toFixed(2));
  console.log('Tarjeta (28+04):', (totales['28'] + totales['04']).toFixed(2));
  console.log('Transferencia (03):', totales['03'].toFixed(2));

  console.log('\n--- CFDIs con exceso significativo (>1) ---');
  for (const d of detalle) {
    if (Math.abs(d.excesoAsignado) > 1) console.log(JSON.stringify(d));
  }

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
