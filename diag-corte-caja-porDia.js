'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { obtenerDesglosesCobroAlmacenPorCentro, obtenerSaldosFavorPorCentro } = require('./src/banks/domains/erp/erp-sync.service');
const { SERIES_CON_AUTH } = require('./src/banks/domains/erp/erp-auth.utils');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const CENTRO = process.env.DIAG_SERIE || 'C0';

async function main() {
  await connectMongo();

  const desde = new Date('2026-08-10T00:00:00-06:00').toISOString();
  const hasta = new Date('2026-08-12T23:59:59.999-06:00').toISOString();
  const [resultado, resultadosSaldos] = await Promise.all([
    obtenerDesglosesCobroAlmacenPorCentro({ rfc: RFC, centro: CENTRO, fechaDesde: desde, fechaHasta: hasta }),
    obtenerSaldosFavorPorCentro({ rfc: RFC, centro: CENTRO, fechaDesde: desde, fechaHasta: hasta }),
  ]);
  console.log('Total cuentas (ventana ampliada 10-12 ago):', resultado.length);

  // Ventas canceladas/devueltas (2026-08-21, caso real B0-260802634
  // OPERADORA DE FRANQUICIAS SEB): cuando un ticket se cobra y LUEGO se
  // cancela/devuelve en caja (RETD), el ERP no borra el cobro original en
  // `/desgloses-cobro/almacen` — en vez de eso, `/saldos-favor` muestra una
  // Devolución (`serieOrigen: 'DEV'`) generada por la MISMA venta
  // (serieVenta/folioVenta), por el monto cancelado. Sin restar esto, el
  // corte de caja calculado solo con `/desgloses-cobro/almacen` sobreestima
  // el Efectivo/Tarjeta por cada venta cancelada el mismo día (confirmado:
  // exactamente $132.59 de la diferencia contra el reporte oficial de
  // Movimientos en Caja de Hidalgo 11-ago).
  const devGeneradoPorVenta = new Map(); // `${serieVenta}|${folioVenta}` -> monto DEV total
  for (const cuenta of resultadosSaldos) {
    const ventaKey = `${cuenta.serieVenta}|${cuenta.folioVenta}`;
    for (const gen of (cuenta.saldosFavorGenerados ?? [])) {
      if ((gen.serieOrigen ?? '').toUpperCase() !== 'DEV') continue;
      devGeneradoPorVenta.set(ventaKey, (devGeneradoPorVenta.get(ventaKey) ?? 0) + (Math.abs(Number(gen.monto)) || 0));
    }
  }

  // Primera pasada: acumular cobros POR VENTA (no directo a porDia) para
  // poder restarles su devolución antes de sumar al total del día.
  const porVenta = new Map(); // ventaKey -> [{ dia, clave, monto }]
  let excluidosPorOrigen = 0;
  const excluidos = {}; // dia -> { 'EXCLUIDO_x': monto }
  for (const cuenta of resultado) {
    const ventaKey = `${cuenta.serieVenta}|${cuenta.folioVenta}`;
    for (const cobro of (cuenta.cobros ?? [])) {
      if (cobro.claveCentro !== CENTRO) continue;
      const origen = (cobro.serieOrigen ?? '').toUpperCase();
      // APS/MIS son dinero real (igual que en cfdi-poliza-generator.service.js) —
      // APA es el único que se excluye por completo (espejo de atribución de saldo
      // a favor, sin dinero nuevo).
      const esOrigenValido = origen === 'APS' || origen === 'MIS' || SERIES_CON_AUTH.includes(origen);
      const d = new Date(cobro.fecha);
      d.setHours(d.getHours() - 6);
      const dia = d.toISOString().slice(0, 10);
      if (!esOrigenValido) {
        excluidosPorOrigen++;
        excluidos[dia] = excluidos[dia] || {};
        excluidos[dia]['EXCLUIDO_' + origen] = (excluidos[dia]['EXCLUIDO_' + origen] || 0) + Math.abs(Number(cobro.monto) || 0);
        continue;
      }
      for (const fp of (cobro.formasPago ?? [])) {
        if (/puntos/i.test(fp.nombre ?? '') || /saldo\s*a\s*favor/i.test(fp.nombre ?? '')) continue;
        const clave = fp.claveSat ?? '??';
        const monto = (cobro.formasPago.length === 1 && cobro.monto != null)
          ? Math.abs(Number(cobro.monto) || 0)
          : (Number(fp.monto) || 0);
        if (!porVenta.has(ventaKey)) porVenta.set(ventaKey, []);
        porVenta.get(ventaKey).push({ dia, clave, monto });
      }
    }
  }

  // Segunda pasada: restar la Devolución de cada venta cancelada de sus
  // propios renglones (más recientes primero — la cancelación reversa el
  // cobro más reciente de esa venta) y sumar lo que sobreviva a `porDia`.
  const porDia = { ...excluidos };
  let totalNeteadoPorDevolucion = 0;
  for (const [ventaKey, renglones] of porVenta) {
    let devRestante = devGeneradoPorVenta.get(ventaKey) ?? 0;
    for (let i = renglones.length - 1; i >= 0 && devRestante > 0.01; i--) {
      const r = renglones[i];
      const reduccion = Math.min(r.monto, devRestante);
      r.monto -= reduccion;
      devRestante -= reduccion;
      totalNeteadoPorDevolucion += reduccion;
    }
    for (const r of renglones) {
      if (r.monto <= 0) continue;
      porDia[r.dia] = porDia[r.dia] || {};
      porDia[r.dia][r.clave] = (porDia[r.dia][r.clave] || 0) + r.monto;
    }
  }

  console.log('Cobros excluidos por origen no reconocido:', excluidosPorOrigen);
  console.log('Ventas con Devolucion (DEV) detectada:', devGeneradoPorVenta.size);
  console.log('Total neteado por devoluciones/cancelaciones:', totalNeteadoPorDevolucion.toFixed(2));
  console.log(JSON.stringify(porDia, null, 2));

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
