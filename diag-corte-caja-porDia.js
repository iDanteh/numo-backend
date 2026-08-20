'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { obtenerDesglosesCobroAlmacenPorCentro } = require('./src/banks/domains/erp/erp-sync.service');
const { SERIES_CON_AUTH } = require('./src/banks/domains/erp/erp-auth.utils');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const CENTRO = process.env.DIAG_SERIE || 'C0';

async function main() {
  await connectMongo();

  const desde = new Date('2026-08-10T00:00:00-06:00').toISOString();
  const hasta = new Date('2026-08-12T23:59:59.999-06:00').toISOString();
  const resultado = await obtenerDesglosesCobroAlmacenPorCentro({ rfc: RFC, centro: CENTRO, fechaDesde: desde, fechaHasta: hasta });
  console.log('Total cuentas (ventana ampliada 10-12 ago):', resultado.length);

  const porDia = {};
  let excluidosPorOrigen = 0;
  for (const cuenta of resultado) {
    for (const cobro of (cuenta.cobros ?? [])) {
      if (cobro.claveCentro !== CENTRO) continue;
      const origen = (cobro.serieOrigen ?? '').toUpperCase();
      const esOrigenValido = origen === 'CBT' || SERIES_CON_AUTH.includes(origen);
      const d = new Date(cobro.fecha);
      d.setHours(d.getHours() - 6);
      const dia = d.toISOString().slice(0, 10);
      if (!esOrigenValido) {
        excluidosPorOrigen++;
        porDia[dia] = porDia[dia] || {};
        porDia[dia]['EXCLUIDO_' + origen] = (porDia[dia]['EXCLUIDO_' + origen] || 0) + Math.abs(Number(cobro.monto) || 0);
        continue;
      }
      for (const fp of (cobro.formasPago ?? [])) {
        if (/puntos/i.test(fp.nombre ?? '') || /saldo\s*a\s*favor/i.test(fp.nombre ?? '')) continue;
        const clave = fp.claveSat ?? '??';
        const monto = (cobro.formasPago.length === 1 && cobro.monto != null)
          ? Math.abs(Number(cobro.monto) || 0)
          : (Number(fp.monto) || 0);
        porDia[dia] = porDia[dia] || {};
        porDia[dia][clave] = (porDia[dia][clave] || 0) + monto;
      }
    }
  }
  console.log('Cobros excluidos por origen no reconocido:', excluidosPorOrigen);
  console.log(JSON.stringify(porDia, null, 2));

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
