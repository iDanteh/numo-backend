'use strict';

/**
 * clientes-codigo.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Indicador de depósitos identificados por número de cliente (Cortes). El
 * catálogo (`codigo` → cliente, ver ClienteCodigo.model.js) ocupa el 99.73%
 * del rango 1-33333 en el export real del ERP — matchear el código pelado
 * contra texto libre no distingue un código real de una coincidencia
 * (cantidad, folio, OC, nombre de producto: "WD-40"). Esta capa aplica el
 * filtrado validado a mano sobre el Excel real antes de clasificar cada
 * match por nivel de confianza — ver cada función para el detalle.
 *
 * `validarCodigoConDigito` (codigo-cliente-digito-verificador.js) da la única
 * vía de confianza 'alta': hoy casi nunca dispara porque los clientes todavía
 * no conocen su dígito verificador (comunicárselo es un paso fuera de este
 * alcance), pero el campo queda listo para cuando se empiece a comunicar.
 */

const ExcelJS = require('exceljs');
const ClienteCodigo = require('./ClienteCodigo.model');
const BankMovementClienteCodigoMatch = require('./BankMovementClienteCodigoMatch.model');
const BankMovement = require('../banks/BankMovement.model');
const { validarCodigoConDigito } = require('../../shared/utils/codigo-cliente-digito-verificador');

const BANCOS_DETERMINABLES = ['Banamex', 'Santander'];

// ── Importación del catálogo (acumulativa: nunca borra códigos ausentes de una corrida) ──
//
// Dispara actualizarMatches() al final — cada importación periódica del catálogo cubre tanto
// los depósitos nuevos que llegaron desde la última corrida como los que antes no matcheaban
// ningún código y ahora sí (código nuevo en esta importación).
async function importarCatalogo(filas) {
  if (!filas.length) return { importados: 0, creados: 0, actualizados: 0, matchesNuevos: 0 };

  const ops = filas.map((f) => ({
    updateOne: {
      filter: { codigo: f.codigo },
      update: { $set: { nombre: f.nombre, rfc: f.rfc, importadoEn: new Date() } },
      upsert: true,
    },
  }));
  const resultado = await ClienteCodigo.bulkWrite(ops);
  const { matchesNuevos } = await actualizarMatches();

  return {
    importados: filas.length,
    creados: resultado.upsertedCount ?? 0,
    actualizados: resultado.modifiedCount ?? 0,
    matchesNuevos,
  };
}

// ── Aislar el texto que escribió el cliente, por banco ──
//
// Banamex: único banco con estructura delimitada de verdad —
// "... | Concepto del Pago: <texto cliente> | Referencia numérica: ...".
// Santander: "... | <texto cliente>     <numero de ruta 8+ digitos pegado>" — se aísla
// cortando en el primer '|' y recortando el número de ruta pegado al final.
// BBVA/Azteca: el texto libre (si existe) queda pegado sin delimitador a la referencia
// del banco — no son "determinables", se devuelve null a propósito.
function _textoAisladoPorBanco(banco, concepto) {
  if (!concepto) return null;

  if (banco === 'Banamex') {
    const marcador = 'Concepto del Pago:';
    const idx = concepto.indexOf(marcador);
    if (idx === -1) return null;
    const despues = concepto.slice(idx + marcador.length);
    const pipeIdx = despues.indexOf('|');
    return (pipeIdx === -1 ? despues : despues.slice(0, pipeIdx)).trim();
  }

  if (banco === 'Santander') {
    const pipeIdx = concepto.indexOf('|');
    if (pipeIdx === -1) return null;
    const despues = concepto.slice(pipeIdx + 1);
    return despues.replace(/\s{2,}\d{8,}\s*$/, '').trim();
  }

  return null;
}

// ── Filtros de exclusión + clasificación de confianza, validados a mano sobre el Excel
// real (865 matches crudos → 454 confiables, el resto demostrado ruido) ──

// Prefijo tipo serie-folio ERP (A0-, D0-, O0-, etc.) pegado justo antes del número — son
// referencias internas, no códigos de cliente. Prefijos reales vistos en producción.
const PREFIJO_ERP_RE = /[A-Za-z]\d-?\s*$/;

// Palabra de cantidad/unidad propia del rubro (ferretería/construcción) pegada justo
// después del número — delata una cantidad ("40 acrilicos"), no un código.
const UNIDAD_CANTIDAD_RE = /^\s*(tuboa?s?|acr[ií]licos?|coladeras?|regaderas?|chorros?|toneladas?|tinacos?|v[aá]lvulas?|piezas?|rollos?|varillas?|bultos?|sacos?|metros?|kgs?|kilos?|litros?|codos?|coples?|llaves?|mangueras?|placas?|l[aá]minas?|cubetas?|galones?|cajas?|unidades?|facturas?|facuras?)\b/i;

// Palabras de contexto que delatan OC/cotización/folio/factura en cualquier parte del
// texto aislado — si aparecen, el texto completo se descarta (no solo el número cercano).
const CONTEXTO_RUIDO_RE = /\bOC\d|\bOC\b|\bORD\b|COTIZACI|\bCOT\b|FOLIO|\bFOL\b|FAC\w*\s*\d|\bMEN\s*\d|FACTURA/i;

const TOKEN_NUMERICO_RE = /\d+/g;

function _clasificarConfianza(codigo, textoAislado, posicion) {
  // intento de validar codigo+siguiente-digito como código con dígito verificador
  const siguienteCharIdx = posicion + String(codigo).length;
  const siguienteChar = textoAislado[siguienteCharIdx];
  if (siguienteChar && /\d/.test(siguienteChar)) {
    const resultado = validarCodigoConDigito(`${codigo}${siguienteChar}`);
    if (resultado.valido && resultado.codigoBase === codigo) return 'alta';
  }
  return String(codigo).length >= 5 ? 'media' : 'baja';
}

// Devuelve los códigos del catálogo que aparecen en `texto` y sobreviven los 3 filtros de
// exclusión, cada uno con su nivel de confianza. No decide ambigüedad — eso lo resuelve
// quien llama (buscarMatches), que descarta un depósito si matchea más de un código distinto.
function _candidatosConfiables(texto, codigosSet) {
  const out = [];
  if (!texto) return out;

  const hayRuidoGlobal = CONTEXTO_RUIDO_RE.test(texto);

  let match;
  TOKEN_NUMERICO_RE.lastIndex = 0;
  while ((match = TOKEN_NUMERICO_RE.exec(texto)) !== null) {
    const codigo = Number(match[0]);
    if (!codigosSet.has(codigo)) continue;

    const antes = texto.slice(0, match.index);
    const despues = texto.slice(match.index + match[0].length);

    if (PREFIJO_ERP_RE.test(antes)) continue;
    if (UNIDAD_CANTIDAD_RE.test(despues)) continue;
    if (hayRuidoGlobal) continue;

    out.push({ codigo, confianza: _clasificarConfianza(codigo, texto, match.index) });
  }
  return out;
}

// ── Actualización incremental de los matches persistidos ──
//
// Un match persistido es DEFINITIVO (ver BankMovementClienteCodigoMatch.model.js): esta función
// NUNCA reevalúa un BankMovement que ya tenga un doc en BankMovementClienteCodigoMatch, aunque
// el catálogo haya cambiado desde entonces (ej. un código nuevo que ahora crearía una
// ambigüedad con un movimiento ya matcheado — ese movimiento ya matcheado se queda como está).
// Los movimientos sin match (0 candidatos, o ambiguos con 2+) tampoco se marcan de ninguna
// forma — simplemente se vuelven a mirar en la próxima corrida, es barato (su único costo es
// que BankMovement.find() los siga trayendo mientras no tengan doc en la colección de matches).
async function actualizarMatches() {
  const idsYaMatcheados = await BankMovementClienteCodigoMatch.distinct('bankMovementId');

  const [movimientos, catalogo] = await Promise.all([
    BankMovement.find({
      banco: { $in: BANCOS_DETERMINABLES },
      _id: { $nin: idsYaMatcheados },
    }).select('banco fecha concepto deposito').lean(),
    ClienteCodigo.find().lean(),
  ]);

  const codigosSet = new Set(catalogo.map((c) => c.codigo));

  const docsNuevos = [];
  for (const mov of movimientos) {
    const textoAislado = _textoAisladoPorBanco(mov.banco, mov.concepto);
    if (!textoAislado) continue;

    const candidatos = _candidatosConfiables(textoAislado, codigosSet);
    const codigosUnicos = [...new Set(candidatos.map((c) => c.codigo))];
    if (codigosUnicos.length !== 1) continue; // 0 candidatos o ambiguo — se descarta, se reintenta en la próxima corrida

    const candidato = candidatos[0];
    docsNuevos.push({
      bankMovementId: mov._id,
      codigo: candidato.codigo,
      confianza: candidato.confianza,
      banco: mov.banco,
      fecha: mov.fecha,
      deposito: mov.deposito ?? null,
    });
  }

  if (docsNuevos.length) {
    await BankMovementClienteCodigoMatch.insertMany(docsNuevos);
  }

  return { matchesNuevos: docsNuevos.length };
}

// ── Lectura de los matches YA persistidos, con el nombre del cliente resuelto al momento de
// leer (nunca denormalizado — ver BankMovementClienteCodigoMatch.model.js) ──
async function listarMatchesPersistidos({ fechaInicio = null, fechaFin = null, banco = null } = {}) {
  const query = {};
  if (banco) query.banco = banco;
  if (fechaInicio || fechaFin) {
    query.fecha = {};
    if (fechaInicio) query.fecha.$gte = new Date(fechaInicio);
    if (fechaFin) query.fecha.$lte = new Date(fechaFin);
  }

  const matchesPersistidos = await BankMovementClienteCodigoMatch.find(query)
    .select('bankMovementId codigo confianza banco fecha deposito')
    .lean();
  if (!matchesPersistidos.length) return [];

  const [movimientos, catalogo] = await Promise.all([
    BankMovement.find({ _id: { $in: matchesPersistidos.map((m) => m.bankMovementId) } })
      .select('concepto').lean(),
    ClienteCodigo.find({ codigo: { $in: matchesPersistidos.map((m) => m.codigo) } }).lean(),
  ]);

  const conceptoPorMovId = new Map(movimientos.map((m) => [String(m._id), m.concepto]));
  const nombrePorCodigo = new Map(catalogo.map((c) => [c.codigo, c.nombre]));

  return matchesPersistidos.map((m) => {
    const concepto = conceptoPorMovId.get(String(m.bankMovementId)) ?? null;
    return {
      fecha: m.fecha,
      banco: m.banco,
      concepto,
      textoAislado: _textoAisladoPorBanco(m.banco, concepto),
      codigo: m.codigo,
      nombreCliente: nombrePorCodigo.get(m.codigo) ?? null,
      deposito: m.deposito,
      confianza: m.confianza,
    };
  });
}

// ── Búsqueda de matches sobre BankMovement real (en vivo, SIN persistir) — usada como base de
// actualizarMatches() y queda disponible/testeada directo; el endpoint de reporte YA NO la
// llama (lee de lo persistido vía listarMatchesPersistidos, ver arriba) ──
async function buscarMatches({ fechaInicio = null, fechaFin = null, banco = null } = {}) {
  const bancosFiltro = banco
    ? BANCOS_DETERMINABLES.filter((b) => b === banco)
    : BANCOS_DETERMINABLES;

  const query = { banco: { $in: bancosFiltro } };
  if (fechaInicio || fechaFin) {
    query.fecha = {};
    if (fechaInicio) query.fecha.$gte = new Date(fechaInicio);
    if (fechaFin) query.fecha.$lte = new Date(fechaFin);
  }

  const [movimientos, catalogo] = await Promise.all([
    bancosFiltro.length
      ? BankMovement.find(query).select('banco fecha concepto deposito').lean()
      : Promise.resolve([]),
    ClienteCodigo.find().lean(),
  ]);

  const codigoAInfo = new Map(catalogo.map((c) => [c.codigo, c]));
  const codigosSet = new Set(catalogo.map((c) => c.codigo));

  const matches = [];
  for (const mov of movimientos) {
    const textoAislado = _textoAisladoPorBanco(mov.banco, mov.concepto);
    if (!textoAislado) continue;

    const candidatos = _candidatosConfiables(textoAislado, codigosSet);
    const codigosUnicos = [...new Set(candidatos.map((c) => c.codigo))];
    if (codigosUnicos.length !== 1) continue; // 0 candidatos o ambiguo (2+ códigos distintos) — se descarta

    const candidato = candidatos[0];
    const info = codigoAInfo.get(candidato.codigo);

    matches.push({
      fecha: mov.fecha,
      banco: mov.banco,
      concepto: mov.concepto,
      textoAislado,
      codigo: candidato.codigo,
      nombreCliente: info?.nombre ?? null,
      deposito: mov.deposito ?? null,
      confianza: candidato.confianza,
    });
  }

  return matches;
}

// ── Reporte Excel ──

async function buildReporteMatches(matches) {
  const wb = new ExcelJS.Workbook();
  wb.creator = 'Numo — Indicador de códigos de cliente';
  wb.created = new Date();

  const porConfianza = { alta: 0, media: 0, baja: 0 };
  matches.forEach((m) => { porConfianza[m.confianza] = (porConfianza[m.confianza] ?? 0) + 1; });

  const resumen = wb.addWorksheet('Resumen');
  resumen.columns = [
    { header: 'Indicador', key: 'k', width: 46 },
    { header: 'Valor', key: 'v', width: 18 },
  ];
  resumen.getRow(1).font = { bold: true };
  resumen.addRow({ k: 'Total de matches', v: matches.length });
  resumen.addRow({ k: 'Confianza alta (dígito verificador válido)', v: porConfianza.alta });
  resumen.addRow({ k: 'Confianza media (código de 5 dígitos)', v: porConfianza.media });
  resumen.addRow({ k: 'Confianza baja (código de 4 dígitos o menos)', v: porConfianza.baja });

  const porCliente = new Map();
  for (const m of matches) {
    if (!porCliente.has(m.codigo)) {
      porCliente.set(m.codigo, { codigo: m.codigo, nombre: m.nombreCliente, depositos: 0, montoTotal: 0 });
    }
    const entry = porCliente.get(m.codigo);
    entry.depositos += 1;
    entry.montoTotal += m.deposito ?? 0;
  }

  const resumenCliente = wb.addWorksheet('Resumen por cliente');
  resumenCliente.columns = [
    { header: 'Código', key: 'codigo', width: 12 },
    { header: 'Cliente', key: 'nombre', width: 40 },
    { header: 'Depósitos', key: 'depositos', width: 12 },
    { header: 'Monto total', key: 'montoTotal', width: 16 },
  ];
  resumenCliente.getRow(1).font = { bold: true };
  [...porCliente.values()]
    .sort((a, b) => b.depositos - a.depositos)
    .forEach((row) => resumenCliente.addRow(row));

  const hojaMatches = wb.addWorksheet('Matches');
  hojaMatches.columns = [
    { header: 'Fecha', key: 'fecha', width: 14 },
    { header: 'Banco', key: 'banco', width: 12 },
    { header: 'Concepto', key: 'concepto', width: 50 },
    { header: 'Texto aislado', key: 'textoAislado', width: 35 },
    { header: 'Código', key: 'codigo', width: 10 },
    { header: 'Cliente', key: 'nombreCliente', width: 35 },
    { header: 'Depósito', key: 'deposito', width: 14 },
    { header: 'Confianza', key: 'confianza', width: 12 },
  ];
  hojaMatches.getRow(1).font = { bold: true };
  matches.forEach((m) => hojaMatches.addRow(m));

  return wb.xlsx.writeBuffer();
}

module.exports = {
  importarCatalogo,
  actualizarMatches,
  listarMatchesPersistidos,
  buscarMatches,
  buildReporteMatches,
  // exportadas para testearlas directo (mismo criterio que bank-indicadores.service.js)
  _textoAisladoPorBanco,
  _candidatosConfiables,
  _clasificarConfianza,
};
