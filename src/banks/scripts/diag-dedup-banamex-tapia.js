'use strict';

/**
 * diag-dedup-banamex-tapia.js
 *
 * Diagnostico de solo lectura: por que el import de plantilla-bancos.xlsx (hoja
 * BANAMEX) marco "2 ya existian" en vez de "4 importados". El sospechoso es la
 * Capa 1c de deduplicacion (bank.service.js) — dedup por banco+referenciaNumerica+
 * monto — porque CONSTRUCCIONES TAPIA SA DE CV reutiliza la MISMA "Referencia
 * numerica" (DEPOS 0001969020) en varios depositos reales distintos, y dos de los
 * 4 movimientos del archivo comparten esa referencia Y el mismo monto exacto
 * entre si (20-may y 25-may, ambos $4,463.17).
 *
 * Este script NO reimplementa la logica de dedup — solo trae los movimientos
 * YA EXISTENTES en la base que coinciden con las 2 referencias/montos en juego,
 * para ver CONTRA QUE se estan comparando los 2 "ya existian" reales.
 *
 * Uso:
 *   node src/banks/scripts/diag-dedup-banamex-tapia.js
 */

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const BankMovement = require('../domains/banks/BankMovement.model');

// Los 4 movimientos reales del archivo (plantilla-bancos.xlsx, hoja BANAMEX).
const MOVS_ARCHIVO = [
  { fecha: '2026-05-20', deposito: 4463.17, auth: '00256604', ref: '0001969020' },
  { fecha: '2026-05-25', deposito: 4463.17, auth: '00061264', ref: '0001969020' },
  { fecha: '2026-06-08', deposito: 5459.28, auth: '00938384', ref: '0001969020' },
  { fecha: '2026-06-10', deposito: 5459.28, auth: '00235718', ref: '0000235718' },
];

async function main() {
  await connectMongo();

  console.log('== Historial completo en BD de depositos con la referencia 0001969020 (o su version sin ceros) ==');
  const porRef1 = await BankMovement.find({
    isActive: true,
    banco: 'Banamex',
    referenciaNumerica: { $regex: '^0*1969020$' },
  }, 'fecha deposito retiro saldo numeroAutorizacion referenciaNumerica concepto').sort({ fecha: 1 }).lean();
  console.log(`Encontrados: ${porRef1.length}`);
  for (const m of porRef1) {
    console.log(`  ${m.fecha?.toISOString?.().slice(0, 10)} | deposito=${m.deposito} | saldo=${m.saldo} | auth=${m.numeroAutorizacion} | ref=${m.referenciaNumerica} | ${(m.concepto || '').slice(0, 60)}`);
  }

  console.log('\n== Historial completo en BD de depositos con la referencia/auth 235718 ==');
  const porRef2 = await BankMovement.find({
    isActive: true,
    banco: 'Banamex',
    $or: [
      { referenciaNumerica: { $regex: '^0*235718$' } },
      { numeroAutorizacion: { $regex: '^0*235718$' } },
    ],
  }, 'fecha deposito retiro saldo numeroAutorizacion referenciaNumerica concepto').sort({ fecha: 1 }).lean();
  console.log(`Encontrados: ${porRef2.length}`);
  for (const m of porRef2) {
    console.log(`  ${m.fecha?.toISOString?.().slice(0, 10)} | deposito=${m.deposito} | saldo=${m.saldo} | auth=${m.numeroAutorizacion} | ref=${m.referenciaNumerica} | ${(m.concepto || '').slice(0, 60)}`);
  }

  console.log('\n== Para cada uno de los 4 movimientos del archivo, que ya existe en BD con el mismo auth O la misma ref+monto ==');
  for (const mov of MOVS_ARCHIVO) {
    console.log(`\n--- Archivo: ${mov.fecha} | deposito=${mov.deposito} | auth=${mov.auth} | ref=${mov.ref} ---`);
    const authNorm = mov.auth.replace(/^0+/, '');
    const refNorm  = mov.ref.replace(/^0+/, '');
    const candidatos = await BankMovement.find({
      isActive: true,
      banco: 'Banamex',
      $or: [
        { numeroAutorizacion: { $regex: `^0*${authNorm}$` } },
        { referenciaNumerica: { $regex: `^0*${refNorm}$` } },
      ],
    }, 'fecha deposito saldo numeroAutorizacion referenciaNumerica concepto').lean();
    if (candidatos.length === 0) {
      console.log('  (nada en BD coincide por auth ni por referencia)');
      continue;
    }
    for (const c of candidatos) {
      const mismoMonto = Math.abs((c.deposito ?? 0) - mov.deposito) < 0.01;
      console.log(`  ${c.fecha?.toISOString?.().slice(0, 10)} | deposito=${c.deposito} (${mismoMonto ? 'MISMO MONTO' : 'monto distinto'}) | auth=${c.numeroAutorizacion} | ref=${c.referenciaNumerica}`);
    }
  }

  await disconnectMongo();
  process.exit(0);
}

main().catch((err) => { console.error(err); process.exit(1); });
