'use strict';

/**
 * scripts/migrate-lealtad-csv.js — Sistema de Lealtad (PR8, LAST task of the change,
 * design.md "Migration / Rollout" §3 + specs/loyalty-migration/spec.md). Same operational
 * shape as `migrate-netpay-v2.js`/`backfill-netpay-kore-cache.js`: `--dry-run` (default) vs
 * `--apply`, classify-then-report, abort before any write if anything is unmapped/blocking.
 *
 * Modes:
 *
 *   node scripts/migrate-lealtad-csv.js --file=<ruta> [--dry-run]
 *     Report-only. Zero writes. Reads the CSV/XLSX, classifies every row, prints the summary.
 *
 *   node scripts/migrate-lealtad-csv.js --file=<ruta> --apply --out=<ruta-fuera-del-repo> [--url=<url consulta pública>]
 *     Creates one LoyaltyCard + one MIGRACION LoyaltyPointsLedger entry per valid row, all
 *     inside ONE Postgres transaction (design.md: "One Postgres transaction for all ok
 *     rows") — aborts the whole run (zero writes) if ANY row is `invalido`, `duplicado_en_csv`,
 *     or `conflicto`. `ya_migrada` rows are skipped silently (idempotent re-run). The output
 *     dataset (telefono,nip,mensaje) is written and fsynced to `--out` from inside the same
 *     transaction, right before it resolves, and deleted again if the transaction still ends
 *     up rolling back afterward (e.g. a failed COMMIT) — this prevents both failure modes:
 *     committed cards with a lost NIP file, and a NIP file for cards that were never committed.
 *
 * Card creation and the ledger write both go through the REAL PR2 services
 * (`loyalty-card.service.js#crearTarjeta`, `loyalty-ledger.service.js#_aplicarMovimiento`) —
 * this script never writes to LoyaltyCard/LoyaltyPointsLedger directly. `crearTarjeta` already
 * generates and hashes the NIP (nip.util.js) and returns the plaintext once; this script only
 * forwards that plaintext into the output dataset, never persists or logs it elsewhere.
 * `crearTarjeta` accepts an optional 3rd-argument `{ transaction }` (added in this PR, see
 * `loyalty-card.service.js` — additive/backward-compatible, existing routes call it unchanged)
 * so it can be composed inside this script's single batch transaction instead of bypassing the
 * service with a raw `LoyaltyCard.create()`.
 *
 * SAFETY: the output file contains every imported card's NIP IN PLAIN TEXT. `--out` must point
 * OUTSIDE both repos (e.g. a path on the operator's machine, never inside numo-backend/). Never
 * commit it. Delete it as soon as it has been handed off to whichever messaging provider
 * (Brevo / WhatsApp Business API) the business owner ends up choosing — this script does not
 * send anything itself, by design (see proposal.md "Decisions confirmed after proposal
 * review" #4 and design.md's Migration/Rollout §3 "Safety").
 *
 * Opens its own Postgres connection only when run directly (`require.main === module`), same
 * convention as every script under scripts/ — every test in migrate-lealtad-csv.test.js mocks
 * the DB layer (models + services) and never touches a live Postgres instance.
 */

const fs = require('fs');
const path = require('path');
const ExcelJS = require('exceljs');
const { Transaction } = require('sequelize');

const { sequelize } = require('../src/config/database.postgres');
const { LoyaltyCard, LoyaltyPointsLedger } = require('../src/shared/models/postgres');
const cardService = require('../src/loyalty/domains/cards/loyalty-card.service');
const ledgerService = require('../src/loyalty/domains/ledger/loyalty-ledger.service');
const { normalizarTelefono, toCents, fromCents } = require('../src/loyalty/shared/puntos.util');

const NIVELES_VALIDOS = ['Bronce', 'Plata', 'Oro'];
const NIVEL_DEFAULT = 'Bronce';

// Alias de encabezado reconocidos por default (case/acento-insensitive, ver
// _normalizarEncabezado) — el archivo real del dueño del negocio aún no existe, así que las
// columnas exactas son desconocidas (proposal.md "Dependencies"). `--col-*` permite overridear
// cualquiera de los 4 sin tocar código cuando el archivo real llegue con otros encabezados.
const COLUMNAS_DEFAULT = {
  telefono: ['telefono', 'tel', 'celular', 'movil', 'phone'],
  nombre:   ['nombre', 'cliente', 'name'],
  saldo:    ['saldo', 'puntos', 'points', 'balance'],
  nivel:    ['nivel', 'tier', 'categoria'],
};

const SALDO_RE = /^-?\d+(\.\d{1,2})?$/;

const SYSTEM_USER = { dbId: null, nombre: 'migracion-csv' };

// ───────────────────────── parseArgs ─────────────────────────

const FLAGS_MODO = ['--dry-run', '--apply'];
const FLAGS_VALOR = ['--file', '--out', '--url', '--col-telefono', '--col-nombre', '--col-saldo', '--col-nivel'];

function _quitarAcentos(str) {
  return String(str ?? '').normalize('NFD').replace(/[̀-ͯ]/g, '');
}

function _normalizarEncabezado(str) {
  return _quitarAcentos(str).trim().toLowerCase();
}

function parseArgs(argv) {
  const args = argv ?? [];
  const modos = [];
  const valores = {};
  const desconocidos = [];

  for (const arg of args) {
    if (FLAGS_MODO.includes(arg)) { modos.push(arg); continue; }
    const igualdad = arg.indexOf('=');
    const clave = igualdad === -1 ? arg : arg.slice(0, igualdad);
    if (FLAGS_VALOR.includes(clave) && igualdad !== -1) {
      valores[clave] = arg.slice(igualdad + 1);
      continue;
    }
    desconocidos.push(arg);
  }

  if (desconocidos.length > 0) {
    throw new Error(
      `Flag(s) no reconocido(s): ${desconocidos.join(', ')}. `
      + 'Use --file=<ruta> [--dry-run|--apply] [--out=<ruta>] [--url=<url>] [--col-*=<encabezado>].',
    );
  }
  if (modos.length > 1) {
    throw new Error(`Solo se permite un modo a la vez, recibidos: ${modos.join(', ')}.`);
  }
  if (!valores['--file']) {
    throw new Error('--file=<ruta> es requerido.');
  }

  const modo = modos[0] === '--apply' ? 'apply' : 'dry-run';
  if (modo === 'apply' && !valores['--out']) {
    throw new Error('--out=<ruta, FUERA del repo> es requerido con --apply.');
  }

  const colOverrides = {};
  for (const campo of ['telefono', 'nombre', 'saldo', 'nivel']) {
    const valor = valores[`--col-${campo}`];
    colOverrides[campo] = valor ? [_normalizarEncabezado(valor)] : null;
  }

  return {
    modo,
    file: valores['--file'],
    out: valores['--out'] ?? null,
    url: valores['--url'] ?? null,
    colOverrides,
  };
}

// ───────────────────────── Lectura (exceljs) ─────────────────────────

function _celda(valor) {
  if (valor === null || valor === undefined) return '';
  if (typeof valor === 'object' && valor.richText) return valor.richText.map((r) => r.text).join('').trim();
  if (typeof valor === 'object' && 'text' in valor) return String(valor.text).trim();
  return String(valor).trim();
}

async function _cargarWorkbook(filePath) {
  const workbook = new ExcelJS.Workbook();
  const ext = path.extname(filePath).toLowerCase();
  if (ext === '.csv') {
    await workbook.csv.readFile(filePath);
  } else {
    await workbook.xlsx.readFile(filePath);
  }
  return workbook;
}

function _detectarColumnas(encabezados, colOverrides = {}) {
  const celdas = encabezados.map((nombre, i) => ({ idx: i + 1, nombre: _normalizarEncabezado(nombre) }));
  const mapa = {};
  for (const campo of ['telefono', 'nombre', 'saldo', 'nivel']) {
    const alias = colOverrides[campo] ?? COLUMNAS_DEFAULT[campo];
    const encontrada = celdas.find((c) => alias.includes(c.nombre));
    mapa[campo] = encontrada ? encontrada.idx : null;
  }
  if (!mapa.telefono) throw new Error('No se encontró la columna de teléfono en el archivo (use --col-telefono= si el encabezado es distinto).');
  if (!mapa.nombre) throw new Error('No se encontró la columna de nombre en el archivo (use --col-nombre= si el encabezado es distinto).');
  if (!mapa.saldo) throw new Error('No se encontró la columna de saldo en el archivo (use --col-saldo= si el encabezado es distinto).');
  return mapa;
}

/** Lee el archivo y devuelve las filas crudas (sin validar), una por cliente. */
async function leerFilas(filePath, colOverrides = {}) {
  const workbook = await _cargarWorkbook(filePath);
  const sheet = workbook.worksheets[0];
  if (!sheet) throw new Error('El archivo no contiene ninguna hoja de trabajo.');

  let mapaColumnas = null;
  const filas = [];
  let numeroFila = 0;

  sheet.eachRow((row) => {
    numeroFila += 1;
    const valores = row.values; // exceljs: índice 0 vacío, datos desde el índice 1
    if (!mapaColumnas) {
      const encabezados = valores.slice(1).map(_celda);
      mapaColumnas = _detectarColumnas(encabezados, colOverrides);
      return; // fila de encabezado, no es un dato
    }

    const telefonoRaw = _celda(valores[mapaColumnas.telefono]);
    const nombreRaw    = _celda(valores[mapaColumnas.nombre]);
    const saldoRaw      = _celda(valores[mapaColumnas.saldo]);
    const nivelRaw        = mapaColumnas.nivel ? _celda(valores[mapaColumnas.nivel]) : '';

    if (!telefonoRaw && !nombreRaw && !saldoRaw) return; // fila vacía, se ignora
    filas.push({ numeroFila, telefonoRaw, nombreRaw, saldoRaw, nivelRaw });
  });

  return filas;
}

// ───────────────────────── Clasificación ─────────────────────────

function _validarSaldoRaw(raw) {
  const str = String(raw ?? '').trim();
  if (!SALDO_RE.test(str)) {
    return { valido: false, motivo: 'saldo no numérico o con más de 2 decimales' };
  }
  if (str.startsWith('-')) {
    return { valido: false, motivo: 'saldo negativo' };
  }
  return { valido: true, valor: str };
}

function _analizarFila(f) {
  const errores = [];
  let telefono = null;
  try {
    telefono = normalizarTelefono(f.telefonoRaw);
  } catch (err) {
    errores.push('teléfono inválido (no normaliza a 10 dígitos)');
  }

  const nombre = String(f.nombreRaw ?? '').trim();
  if (!nombre) errores.push('nombre vacío');

  const saldoCheck = _validarSaldoRaw(f.saldoRaw);
  if (!saldoCheck.valido) errores.push(saldoCheck.motivo);

  const nivelTrim = String(f.nivelRaw ?? '').trim();
  let nivel = NIVEL_DEFAULT;
  if (nivelTrim) {
    const match = NIVELES_VALIDOS.find((n) => n.toLowerCase() === nivelTrim.toLowerCase());
    if (!match) errores.push(`nivel desconocido: "${nivelTrim}"`);
    else nivel = match;
  }

  return {
    numeroFila: f.numeroFila,
    telefono,
    nombre,
    saldo: saldoCheck.valido ? fromCents(toCents(saldoCheck.valor)) : null,
    nivel,
    errores,
  };
}

/** Validación de forma (sin tocar la DB): invalido / duplicado_en_csv / candidatas. */
function _clasificarSinDB(filas) {
  const analizadas = filas.map(_analizarFila);

  const conteoTelefonos = new Map();
  for (const a of analizadas) {
    if (!a.telefono) continue;
    conteoTelefonos.set(a.telefono, (conteoTelefonos.get(a.telefono) ?? 0) + 1);
  }

  const invalido = [];
  const duplicadoEnCsv = [];
  const candidatas = [];

  for (const a of analizadas) {
    if (a.telefono && conteoTelefonos.get(a.telefono) > 1) {
      duplicadoEnCsv.push({ numeroFila: a.numeroFila, telefono: a.telefono });
      continue;
    }
    if (a.errores.length > 0) {
      invalido.push({ numeroFila: a.numeroFila, motivo: a.errores.join('; ') });
      continue;
    }
    candidatas.push(a);
  }

  return { invalido, duplicadoEnCsv, candidatas };
}

/** Para cada fila candidata (forma válida, no duplicada en el CSV), consulta la DB real. */
async function _clasificarContraDB(candidatas) {
  const ok = [];
  const yaMigrada = [];
  const conflicto = [];

  for (const fila of candidatas) {
    const existente = await LoyaltyCard.findOne({ where: { telefono: fila.telefono } });
    if (!existente) {
      ok.push(fila);
      continue;
    }
    const migracionPrevia = await LoyaltyPointsLedger.findOne({
      where: { cardId: existente.id, origen: 'migracion' },
    });
    if (migracionPrevia) {
      yaMigrada.push({ numeroFila: fila.numeroFila, telefono: fila.telefono });
    } else {
      conflicto.push({ numeroFila: fila.numeroFila, telefono: fila.telefono });
    }
  }

  return { ok, yaMigrada, conflicto };
}

async function clasificar(filas) {
  const sinDB = _clasificarSinDB(filas);
  const conDB = await _clasificarContraDB(sinDB.candidatas);
  return {
    ok: conDB.ok,
    invalido: sinDB.invalido,
    duplicadoEnCsv: sinDB.duplicadoEnCsv,
    yaMigrada: conDB.yaMigrada,
    conflicto: conDB.conflicto,
  };
}

function reportar(clasificacion) {
  const { ok, invalido, duplicadoEnCsv, yaMigrada, conflicto } = clasificacion;
  console.log('── Resumen de clasificación (migrate-lealtad-csv) ──');
  console.log(`ok (a crear): ${ok.length}`);
  console.log(`invalido: ${invalido.length}`);
  console.log(`duplicado_en_csv: ${duplicadoEnCsv.length}`);
  console.log(`ya_migrada (se omite, NO bloquea): ${yaMigrada.length}`);
  console.log(`conflicto (teléfono ya existe sin migración previa): ${conflicto.length}`);
  for (const f of invalido) console.log(`  INVALIDO    fila=${f.numeroFila} motivo=${f.motivo}`);
  for (const f of duplicadoEnCsv) console.log(`  DUPLICADO   fila=${f.numeroFila} telefono=${f.telefono}`);
  for (const f of conflicto) console.log(`  CONFLICTO   fila=${f.numeroFila} telefono=${f.telefono}`);

  const totalBloqueante = invalido.length + duplicadoEnCsv.length + conflicto.length;
  if (totalBloqueante === 0) {
    console.log('Listo para --apply: 0 fila(s) bloqueante(s).');
  } else {
    console.log(`${totalBloqueante} fila(s) bloqueante(s) — revisar antes de --apply.`);
  }
  return { totalBloqueante };
}

// ───────────────────────── Mensaje / dataset de salida ─────────────────────────

/** Texto final pendiente del dueño del negocio (design.md Open Questions) — placeholder razonable. */
function construirMensaje({ nombre, nip, url }) {
  const urlTexto = url ? ` Consulta tu saldo en ${url}.` : '';
  return `Hola ${nombre}, tu nuevo número de tarjeta de lealtad es tu propio celular. Tu NIP es ${nip}.${urlTexto}`;
}

function _escaparCampoCsv(valor) {
  const str = String(valor ?? '');
  if (/[",\n]/.test(str)) return `"${str.replace(/"/g, '""')}"`;
  return str;
}

/** Escribe telefono,nip,mensaje y fsyncea antes de devolver el control (design.md §3). */
function escribirDataset(outPath, dataset) {
  const encabezado = 'telefono,nip,mensaje';
  const filas = dataset.map((d) => [d.telefono, d.nip, d.mensaje].map(_escaparCampoCsv).join(','));
  const contenido = `${[encabezado, ...filas].join('\n')}\n`;

  const fd = fs.openSync(outPath, 'w');
  try {
    fs.writeSync(fd, contenido);
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
}

// ───────────────────────── Dry-run / Apply ─────────────────────────

async function runDryRun({ file, colOverrides }) {
  const filas = await leerFilas(file, colOverrides);
  const clasificacion = await clasificar(filas);
  const resumen = reportar(clasificacion);
  return { ...clasificacion, ...resumen };
}

async function runApply({ file, out, url, colOverrides }) {
  const filas = await leerFilas(file, colOverrides);
  const clasificacion = await clasificar(filas);
  const resumen = reportar(clasificacion);

  if (resumen.totalBloqueante > 0) {
    throw new Error(
      `--apply abortado: ${resumen.totalBloqueante} fila(s) bloqueante(s) `
      + '(invalida / duplicado_en_csv / conflicto). Corrija el archivo antes de reintentar. Cero escrituras realizadas.',
    );
  }

  const dataset = [];
  let archivoEscrito = false;

  try {
    await sequelize.transaction(async (t) => {
      for (const fila of clasificacion.ok) {
        // 1) Crea la tarjeta vía el servicio real (PR2) — genera y hashea el NIP, nunca lo
        //    persiste en texto plano (D6). `transaction` compone esta creación dentro del
        //    único batch transaccional de esta corrida (design.md).
        const { card, nip } = await cardService.crearTarjeta(
          { telefono: fila.telefono, nombre: fila.nombre, nivel: fila.nivel },
          SYSTEM_USER,
          { transaction: t },
        );

        // 2) Bloquea la fila recién creada (mismo patrón que todo el resto del ledger
        //    service) antes de aplicar el movimiento MIGRACION.
        const tarjetaBloqueada = await LoyaltyCard.findByPk(card.id, { transaction: t, lock: Transaction.LOCK.UPDATE });

        // 3) Única escritura del ledger (D4, `_aplicarMovimiento`) — nunca un INSERT manual.
        //    El saldo legado se escribe tal cual como `puntos` sobre una tarjeta en 0, así
        //    `saldoResultante` termina siendo exactamente el saldo del CSV.
        await ledgerService._aplicarMovimiento(t, tarjetaBloqueada, {
          tipo: 'MIGRACION',
          puntos: fila.saldo,
          origen: 'migracion',
          usuarioId: null,
          usuarioNombre: SYSTEM_USER.nombre,
        });

        dataset.push({
          telefono: fila.telefono,
          nip,
          mensaje: construirMensaje({ nombre: fila.nombre, nip, url }),
        });
      }

      // Se escribe DENTRO de la transacción, justo antes de que resuelva (COMMIT implícito
      // de Sequelize al terminar el callback sin errores) — si cualquier paso de arriba
      // lanzó, nunca llegamos aquí y el archivo nunca se crea.
      escribirDataset(out, dataset);
      archivoEscrito = true;
    });
  } catch (err) {
    // Cubre tanto "la transacción se revirtió antes de escribir el archivo" (nunca se
    // escribió, nada que borrar) como "el archivo se escribió pero el COMMIT igual falló"
    // (hay que borrar el archivo huérfano — design.md "deleted if the transaction rolls back").
    if (archivoEscrito && fs.existsSync(out)) {
      fs.unlinkSync(out);
    }
    throw err;
  }

  console.log(`Importadas ${dataset.length} tarjeta(s). Dataset (telefono,nip,mensaje) escrito en: ${out}`);
  console.log('IMPORTANTE: el archivo contiene NIPs en texto plano. Entréguelo únicamente al proveedor de mensajería elegido y bórrelo después de usarlo.');

  return { creadas: dataset.length, dataset, resumen };
}

async function main(argv = process.argv.slice(2)) {
  const opts = parseArgs(argv);
  await sequelize.authenticate();
  console.log(`Conectado a Postgres. Modo: --${opts.modo}.`);
  if (opts.modo === 'dry-run') return runDryRun(opts);
  return runApply(opts);
}

if (require.main === module) {
  main().catch((err) => {
    console.error('Error:', err.message);
    process.exit(1);
  });
}

module.exports = {
  parseArgs,
  leerFilas,
  clasificar,
  reportar,
  construirMensaje,
  escribirDataset,
  runDryRun,
  runApply,
  main,
  COLUMNAS_DEFAULT,
  NIVELES_VALIDOS,
  NIVEL_DEFAULT,
  SYSTEM_USER,
};
