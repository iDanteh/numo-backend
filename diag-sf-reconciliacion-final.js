'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { sequelize } = require('./src/config/database.postgres');
const { obtenerSaldosFavor } = require('./src/banks/domains/erp/erp-sync.service');

const RFC = process.env.DIAG_RFC || 'CCO011113663';

// Candidatos externos a la Global (generados FUERA de sus 200 tickets) —
// tomados del run anterior de diag-sf-en-global.js: marcador (serieOrigen-
// folioOrigen) + ticket que lo usó dentro de esta Global el 3-ago.
const CANDIDATOS = [
  { marcador: 'DEV-055991', usoTicket: 'C0-260800475', montoUsado: 423.58 },
  { marcador: 'DEV-056086', usoTicket: 'C0-260800431', montoUsado: 85.36 },
  { marcador: 'DEV-056084', usoTicket: 'C0-260800406', montoUsado: 74.53 },
  { marcador: 'DEV-056077', usoTicket: 'C0-260800367', montoUsado: 166.68 },
  { marcador: 'DEV-056074', usoTicket: 'C0-260800351', montoUsado: 3805.71 },
  { marcador: 'CAC-075406', usoTicket: 'C0-260800350', montoUsado: 100.33 },
  { marcador: 'CAC-077160', usoTicket: 'C0-260800541', montoUsado: 97.36 },
];

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  console.log('=== Buscando el origen (generacion) de cada marcador ===\n');
  let totalOculto = 0;
  let totalVisible = 0;

  for (const c of CANDIDATOS) {
    const [serieMarcador, folioMarcador] = c.marcador.split('-');
    // Buscar la cuenta que GENERO este saldo: se busca por el propio marcador
    // como si fuera una venta (asi lo hace _claveCentroPorMonto/almacen), pero
    // lo mas confiable es buscarlo como documento relacionado -- probamos
    // consultando el marcador directamente como serie/folio de venta.
    const resultado = await obtenerSaldosFavor({ rfc: RFC, series: [serieMarcador], folios: [folioMarcador] });
    let genInfo = null;
    for (const cuenta of resultado) {
      for (const gen of (cuenta.saldosFavorGenerados ?? [])) {
        if (gen.serieOrigen === serieMarcador && String(gen.folioOrigen) === folioMarcador) {
          genInfo = { cuenta, gen };
        }
      }
    }
    if (!genInfo) {
      // El marcador puede ser la venta MISMA que genero el saldo (serieVenta/folioVenta = marcador)
      for (const cuenta of resultado) {
        if (cuenta.serieVenta === serieMarcador && String(cuenta.folioVenta) === folioMarcador) {
          for (const gen of (cuenta.saldosFavorGenerados ?? [])) genInfo = { cuenta, gen };
        }
      }
    }

    if (!genInfo) {
      console.log(`${c.marcador} -> uso ${c.usoTicket} $${c.montoUsado}: NO SE ENCONTRO el origen (queda sin clasificar)`);
      continue;
    }

    const usos = genInfo.gen.usos ?? [];
    const usoUnico = usos.length === 1 ? usos[0] : null;
    const diaGen = genInfo.gen.fecha ? genInfo.gen.fecha.slice(0, 10) : null;
    const diaUso = usoUnico?.fecha ? usoUnico.fecha.slice(0, 10) : null;
    const usoCompleto = usoUnico && Math.abs(Number(usoUnico.montoSobrante) || 0) < 0.01;
    const mismoAlmacen = usoUnico && usoUnico.serieVenta === genInfo.cuenta.serieVenta;
    const oculto = !!(usoUnico && usoCompleto && diaGen && diaGen === diaUso && mismoAlmacen);

    console.log(`${c.marcador} -> uso ${c.usoTicket} $${c.montoUsado}`);
    console.log(`  generado: ${genInfo.cuenta.serieVenta}-${genInfo.cuenta.folioVenta} el ${genInfo.gen.fecha} por $${genInfo.gen.monto}`);
    console.log(`  usos.length=${usos.length}, diaGen=${diaGen}, diaUso=${diaUso}, usoCompleto=${usoCompleto}, mismoAlmacen=${mismoAlmacen}`);
    console.log(`  => ${oculto ? 'OCULTO' : 'VISIBLE (SF)'}\n`);

    if (oculto) totalOculto += c.montoUsado;
    else totalVisible += c.montoUsado;
  }

  console.log('=== TOTALES ===');
  console.log('Total OCULTO:', totalOculto.toFixed(2));
  console.log('Total VISIBLE (SF):', totalVisible.toFixed(2));
  console.log('(Esperado en poliza: OCULTO 4046.92, VISIBLE 523.91)');

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
