'use strict';

// traspasos-internos.service.js — motor de detección de traspasos entre cuentas propias.
// Encuentra pares BBVA (depósito, categorizado por el usuario con una BankRule propia,
// ej. "Traspaso entre cuentas propias") ↔ banco contraparte (retiro) que en realidad son
// la misma transferencia interna: mismo día (UTC) + mismo monto, con exactamente 1
// candidato de cada lado. El banco contraparte NO es fijo — varía por movimiento y se
// extrae del propio `concepto` del depósito BBVA (ej. "SPEI RECIBIDOBANAMEX / ...",
// "SPEI RECIBIDOSANTANDER / ...") — nunca se asume un banco al azar; si no se puede
// determinar, el movimiento va al bucket `sinBancoDetectado`, visible en el reporte.
// Cualquier ambigüedad (2+ candidatos de cualquier lado en el mismo bucket) nunca se
// auto-matchea — queda reportada para revisión manual.
//
// Sigue el mismo patrón que importarConciliacion/revertirConciliacion (bank.service.js):
// pipeline update con $ifNull para los campos inmutables de primera identificación,
// $concatArrays para identificadoPor, runId único por corrida para revert selectivo.

const { randomUUID }   = require('crypto');
const ExcelJS           = require('exceljs');
const BankMovement      = require('./BankMovement.model');
const { TODOS_MOTORES_HISTORICO } = require('./bank.service');
const { ejecutarBulkConTransaccion, normalizarBanco } = require('./bank-autorizaciones.service');

// String exacto, con guion — identificación humana asistida por este motor, NO se agrega
// a TODOS_MOTORES_HISTORICO (a propósito: el usuario real revisó y confirmó el par antes
// de ejecutar, no es un motor 100% automático sin supervisión).
const SOURCE_TRASPASO_INTERNO = 'traspaso-interno';

// ── Extracción del banco contraparte desde el concepto BBVA ─────────────────────────
// Muestra real de concepto (confirmada contra Mongo del usuario, 2026-08-14):
//   "SPEI RECIBIDOSANTANDER / 0192794826 014 8351574TRASPASO ENTRE CUENTAS PROPIAS"
//   "SPEI RECIBIDOBANAMEX / 0192646443 002 0300426TRASPASO ENTRE CUENTAS PROPIAS"
// El nombre del banco viene pegado sin espacio justo después de "RECIBIDO", antes de " / ".
// Nunca se asume un banco al azar: si el regex no matchea, si el resultado normalizado no
// es un valor válido del enum real de `banco`, o si el resultado es 'BBVA' (self-referencial,
// defensivo — no debería ocurrir), se devuelve null y el movimiento va a sinBancoDetectado.
const RE_BANCO_RECIBIDO = /RECIBIDO\s*([A-Za-zÁÉÍÓÚÑáéíóúñ]+)/i;

function _extraerBancoContraparte(concepto) {
  if (!concepto) return null;
  const match = String(concepto).match(RE_BANCO_RECIBIDO);
  if (!match) return null;

  const normalizado = normalizarBanco(match[1]);
  if (!normalizado || normalizado === 'BBVA') return null;

  const enumValues = BankMovement.schema.path('banco').enumValues;
  if (!Array.isArray(enumValues) || !enumValues.includes(normalizado)) return null;

  return normalizado;
}

// ── Helpers de bucket (día UTC + monto) ──────────────────────────────────────────
// "Mismo día" se compara en UTC (Date.UTC), igual criterio que ya usa
// importarConciliacion en bank.service.js. Mismo caveat: un depósito hecho tarde en el
// día en horario local de México (UTC-6) puede caer en el día UTC siguiente y no
// matchear con su contraparte aunque ambos ocurrieron "el mismo día" para el usuario.
// No se intenta resolver aquí — documentado nada más, igual que en el precedente.
function diaUtcKey(fecha) {
  const d = new Date(fecha);
  return `${d.getUTCFullYear()}-${d.getUTCMonth()}-${d.getUTCDate()}`;
}

// Monto con 2 decimales de precisión, expresado en centavos enteros para evitar
// comparaciones de punto flotante al usarlo como parte de la clave de bucket.
function centavos(monto) {
  return Math.round(Number(monto) * 100);
}

// El banco contraparte forma parte de la clave — antes solo era día+monto, porque el
// banco contraparte era siempre el mismo (Banamex). Ahora dos pares del mismo día y monto
// pero con bancos contraparte distintos nunca se mezclan ni se marcan como ambiguos entre sí.
function bucketKey(banco, fecha, monto) {
  return `${banco}|${diaUtcKey(fecha)}|${centavos(monto)}`;
}

// Proyección compartida — solo los campos que necesita la clasificación, el Excel y el
// snapshot de traspasoInterno.
const SELECT_CANDIDATO = '_id banco fecha deposito retiro folio concepto categoria status';

async function _cargarCandidatos(categoriaBbva) {
  const candidatosBbvaRaw = await BankMovement.find({
    banco:     'BBVA',
    categoria: categoriaBbva,
    status:    { $in: ['no_identificado', 'otros'] },
    isActive:  true,
    deposito:  { $gt: 0 },
    erpIds:    { $size: 0 },
  }).select(SELECT_CANDIDATO).lean();

  // Cada candidato BBVA se anota en memoria con su banco contraparte detectado
  // (mov._bancoContraparte — nunca se escribe en Mongo, es solo para esta corrida). Los
  // que no tienen banco detectable van directo a sinBancoDetectado, sin pasar por el
  // resto del pipeline de bucketing.
  const candidatosBbva    = [];
  const sinBancoDetectado = [];
  const bancosNecesarios  = new Set();

  for (const mov of candidatosBbvaRaw) {
    const bancoContraparte = _extraerBancoContraparte(mov.concepto);
    if (!bancoContraparte) {
      sinBancoDetectado.push(mov);
      continue;
    }
    mov._bancoContraparte = bancoContraparte;
    bancosNecesarios.add(bancoContraparte);
    candidatosBbva.push(mov);
  }

  // Una sola query para todos los bancos contraparte realmente necesarios — nunca se
  // consultan los 15 bancos del enum siempre. Si no hay ningún banco detectado, no hace
  // falta ni consultar.
  const candidatosContraparte = bancosNecesarios.size > 0
    ? await BankMovement.find({
        banco:    { $in: [...bancosNecesarios] },
        status:   { $in: ['no_identificado', 'otros'] },
        isActive: true,
        retiro:   { $gt: 0 },
        erpIds:   { $size: 0 },
      }).select(SELECT_CANDIDATO).lean()
    : [];

  return { candidatosBbva, candidatosContraparte, sinBancoDetectado };
}

// ── Clasificación por bucket ──────────────────────────────────────────────────────
// Por cada bucket bancoContraparte+día-UTC+monto:
//   · 1 BBVA + 1 contraparte           → par confirmado (relacionados)
//   · N BBVA + 0 contraparte (N>=1)    → sinContraparteBbva (no hay ambigüedad real, solo
//                                         falta la contraparte)
//   · 0 BBVA + N contraparte (N>=1)    → sinContraparteOtros
//   · cualquier otra combinación con AMBOS lados >0 (ej. 2+1, 1+2, 2+2)
//                                       → ambiguos (no se puede determinar cuál es la pareja)
function _clasificar(candidatosBbva, candidatosContraparte) {
  const buckets = new Map(); // bucketKey → { bbva: [...], contraparte: [...] }

  for (const mov of candidatosBbva) {
    const key = bucketKey(mov._bancoContraparte, mov.fecha, mov.deposito);
    if (!buckets.has(key)) buckets.set(key, { bbva: [], contraparte: [] });
    buckets.get(key).bbva.push(mov);
  }
  for (const mov of candidatosContraparte) {
    const key = bucketKey(mov.banco, mov.fecha, mov.retiro);
    if (!buckets.has(key)) buckets.set(key, { bbva: [], contraparte: [] });
    buckets.get(key).contraparte.push(mov);
  }

  const relacionados       = [];
  const ambiguos            = [];
  const sinContraparteBbva  = [];
  const sinContraparteOtros = [];

  for (const { bbva, contraparte } of buckets.values()) {
    if (bbva.length === 1 && contraparte.length === 1) {
      relacionados.push({ bbva: bbva[0], contraparte: contraparte[0] });
    } else if (bbva.length > 0 && contraparte.length > 0) {
      ambiguos.push(...bbva, ...contraparte);
    } else if (bbva.length > 0) {
      sinContraparteBbva.push(...bbva);
    } else if (contraparte.length > 0) {
      sinContraparteOtros.push(...contraparte);
    }
  }

  return { relacionados, ambiguos, sinContraparteBbva, sinContraparteOtros };
}

// ── Construcción de la operación de escritura para un lado del par ──────────────────
// Pipeline update (array), mismo patrón que importarConciliacion:
//   · identificadoPor: $concatArrays agrega la entrada nueva (source + runId propios).
//   · primeraIdentificacionAt/Por: $ifNull — inmutable, solo se setea si aún es null.
//   · traspasoInterno: $set directo (no necesita $ifNull) con el snapshot de la
//     CONTRAPARTE — mov apunta a contraparte, contraparte apunta a mov (se arma 1 op
//     por cada lado del par, llamando a este helper dos veces con los args invertidos).
// Filtro ACID: solo escribe si el movimiento sigue en un estado elegible (nadie lo
// identificó ni le vinculó una CxC entre la lectura y esta escritura).
function _buildIdentificarOp(mov, contraparte, user, runId, now) {
  const montoContraparte = contraparte.banco === 'BBVA' ? contraparte.deposito : contraparte.retiro;

  return {
    updateOne: {
      filter: {
        _id:      mov._id,
        isActive: true,
        status:   { $in: ['no_identificado', 'otros'] },
        erpIds:   { $size: 0 },
      },
      update: [
        {
          $set: {
            status: 'identificado',
            identificadoPor: {
              $concatArrays: [
                { $ifNull: ['$identificadoPor', []] },
                [{
                  userId:  user._id,
                  nombre:  user.nombre ?? user._id,
                  fechaId: now,
                  erpId:   null,
                  source:  SOURCE_TRASPASO_INTERNO,
                  runId,
                }],
              ],
            },
            primeraIdentificacionAt: { $ifNull: ['$primeraIdentificacionAt', now] },
            primeraIdentificacionPor: {
              $ifNull: ['$primeraIdentificacionPor', { userId: user._id, nombre: user.nombre ?? user._id }],
            },
            traspasoInterno: {
              movimientoId: contraparte._id,
              banco:        contraparte.banco,
              folio:        contraparte.folio ?? null,
              fecha:        contraparte.fecha,
              monto:        montoContraparte,
              runId,
            },
          },
        },
      ],
    },
  };
}

/**
 * Motor de detección de traspasos entre cuentas propias (BBVA depósito ↔ banco contraparte
 * retiro — el banco contraparte se determina por movimiento, no es fijo).
 *
 * @param {{ categoriaBbva: string, dryRun?: boolean }} params
 * @param {{ _id: string, nombre?: string }} user - usuario real que ejecuta (no un motor).
 * @returns {Promise<{
 *   relacionados: Array<{ bbva: object, contraparte: object }>,
 *   ambiguos: object[],
 *   sinContraparteBbva: object[],
 *   sinContraparteOtros: object[],
 *   sinBancoDetectado: object[],
 *   runId: string|null,
 * }>}
 */
async function matchTraspasosInternos({ categoriaBbva, dryRun = true } = {}, user) {
  const { candidatosBbva, candidatosContraparte, sinBancoDetectado } = await _cargarCandidatos(categoriaBbva);
  const clasificacion = _clasificar(candidatosBbva, candidatosContraparte);

  if (dryRun) {
    return { ...clasificacion, sinBancoDetectado, runId: null };
  }

  const runId = randomUUID();
  const now   = new Date();
  const ops   = [];

  for (const { bbva, contraparte } of clasificacion.relacionados) {
    ops.push(_buildIdentificarOp(bbva, contraparte, user, runId, now));
    ops.push(_buildIdentificarOp(contraparte, bbva, user, runId, now));
  }

  if (ops.length > 0) {
    await ejecutarBulkConTransaccion(ops);
  }

  return { ...clasificacion, sinBancoDetectado, runId };
}

// ── revertirTraspasosInternos ────────────────────────────────────────────────────
// Deshace exactamente lo que aplicó matchTraspasosInternos({dryRun:false}) para un
// runId concreto — simétrico a revertirConciliacion (bank.service.js):
//   · Solo actúa sobre movimientos con esa combinación userId + source + runId.
//   · Elimina la entrada del array identificadoPor.
//   · Limpia traspasoInterno SOLO si traspasoInterno.runId === runId (no toca el
//     campo si el movimiento fue re-vinculado por una corrida posterior).
//   · Resetea status a 'no_identificado' ÚNICAMENTE si no quedan entradas de
//     identificación humana (userId fuera de TODOS_MOTORES_HISTORICO, excluyendo la
//     entrada que se está quitando) — mismo criterio exacto que revertirConciliacion.
async function revertirTraspasosInternos(runId, userId) {
  const resultado = await BankMovement.updateMany(
    {
      isActive: true,
      status:   'identificado',
      identificadoPor: {
        $elemMatch: { userId, source: SOURCE_TRASPASO_INTERNO, runId },
      },
    },
    [
      {
        $set: {
          identificadoPor: {
            $filter: {
              input: { $ifNull: ['$identificadoPor', []] },
              as:    'entry',
              cond: {
                $not: {
                  $and: [
                    { $eq: ['$$entry.userId',  userId] },
                    { $eq: ['$$entry.source',  SOURCE_TRASPASO_INTERNO] },
                    { $eq: ['$$entry.runId',   runId] },
                  ],
                },
              },
            },
          },
          traspasoInterno: {
            $cond: {
              if:   { $eq: [{ $ifNull: ['$traspasoInterno.runId', null] }, runId] },
              then: null,
              else: '$traspasoInterno',
            },
          },
          status: {
            $cond: {
              if: {
                $eq: [
                  {
                    $size: {
                      $filter: {
                        input: { $ifNull: ['$identificadoPor', []] },
                        as:    'e',
                        cond: {
                          $and: [
                            { $not: { $in: ['$$e.userId', TODOS_MOTORES_HISTORICO] } },
                            {
                              $not: {
                                $and: [
                                  { $eq: ['$$e.userId',  userId] },
                                  { $eq: ['$$e.source',  SOURCE_TRASPASO_INTERNO] },
                                  { $eq: ['$$e.runId',   runId] },
                                ],
                              },
                            },
                          ],
                        },
                      },
                    },
                  },
                  0,
                ],
              },
              then: 'no_identificado',
              else: '$status',
            },
          },
        },
      },
    ],
  );

  return {
    revertidos: resultado.modifiedCount,
    message:    `${resultado.modifiedCount} movimiento(s) revertido(s) a "no identificado"`,
  };
}

// ── generarExcelTraspasosInternos ────────────────────────────────────────────────
// Mismo patrón ExcelJS que generarExcelMostradorCyc (erp/mostrador-cyc.service.js):
// header azul, fills de color por hoja, retorna el buffer (wb.xlsx.writeBuffer()).
async function generarExcelTraspasosInternos(resultado) {
  const wb = new ExcelJS.Workbook();
  wb.creator = 'Numo — Traspasos Internos';
  wb.created = new Date();

  const HEADER_FILL = {
    type: 'pattern', pattern: 'solid',
    fgColor: { argb: 'FF1D4ED8' },      // azul corporativo
  };
  const HEADER_FONT = { bold: true, color: { argb: 'FFFFFFFF' }, size: 10 };

  const OK_FILL   = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFD1FAE5' } }; // verde — relacionados
  const WARN_FILL = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFEF9C3' } }; // amarillo — ambiguos
  const GRAY_FILL = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF3F4F6' } }; // gris — sin contraparte / sin banco detectado

  function formatFecha(d) {
    if (!d) return '';
    const dt = d instanceof Date ? d : new Date(d);
    if (isNaN(dt.getTime())) return '';
    return dt.toLocaleDateString('es-MX', { day: '2-digit', month: '2-digit', year: 'numeric' });
  }

  function styleHeader(ws) {
    ws.getRow(1).eachCell(cell => {
      cell.fill = HEADER_FILL;
      cell.font = HEADER_FONT;
      cell.alignment = { vertical: 'middle', horizontal: 'center' };
    });
    ws.getRow(1).height = 20;
  }

  // ── Hoja 1: Relacionados ────────────────────────────────────────────────────
  const wsRel = wb.addWorksheet('Relacionados');
  wsRel.columns = [
    { header: 'Folio BBVA',        key: 'folioBbva',        width: 14 },
    { header: 'Banco contraparte', key: 'bancoContraparte', width: 16 },
    { header: 'Folio contraparte', key: 'folioContraparte', width: 16 },
    { header: 'Fecha',             key: 'fecha',             width: 12 },
    { header: 'Monto',             key: 'monto',             width: 14 },
  ];
  styleHeader(wsRel);

  for (const { bbva, contraparte } of (resultado.relacionados ?? [])) {
    const row = wsRel.addRow({
      folioBbva:        bbva.folio ?? '',
      bancoContraparte: contraparte.banco,
      folioContraparte: contraparte.folio ?? '',
      fecha:            formatFecha(bbva.fecha),
      monto:            bbva.deposito ?? contraparte.retiro,
    });
    row.eachCell(cell => { cell.fill = OK_FILL; });
  }
  wsRel.getColumn('monto').numFmt = '#,##0.00';
  wsRel.autoFilter = { from: 'A1', to: wsRel.lastColumn.letter + '1' };

  // ── Hoja 2: Ambiguos ────────────────────────────────────────────────────────
  const wsAmb = wb.addWorksheet('Ambiguos');
  wsAmb.columns = [
    { header: 'Banco',  key: 'banco',  width: 12 },
    { header: 'Folio',  key: 'folio',  width: 14 },
    { header: 'Fecha',  key: 'fecha',  width: 12 },
    { header: 'Monto',  key: 'monto',  width: 14 },
    { header: 'Status', key: 'status', width: 16 },
  ];
  styleHeader(wsAmb);

  for (const m of (resultado.ambiguos ?? [])) {
    const row = wsAmb.addRow({
      banco:  m.banco,
      folio:  m.folio ?? '',
      fecha:  formatFecha(m.fecha),
      monto:  m.banco === 'BBVA' ? m.deposito : m.retiro,
      status: m.status,
    });
    row.eachCell(cell => { cell.fill = WARN_FILL; });
  }
  wsAmb.getColumn('monto').numFmt = '#,##0.00';
  wsAmb.autoFilter = { from: 'A1', to: wsAmb.lastColumn.letter + '1' };

  // ── Hoja 3: Sin contraparte BBVA ────────────────────────────────────────────
  const wsSinBbva = wb.addWorksheet('Sin contraparte BBVA');
  wsSinBbva.columns = [
    { header: 'Folio',  key: 'folio',  width: 14 },
    { header: 'Fecha',  key: 'fecha',  width: 12 },
    { header: 'Monto',  key: 'monto',  width: 14 },
    { header: 'Status', key: 'status', width: 16 },
  ];
  styleHeader(wsSinBbva);

  for (const m of (resultado.sinContraparteBbva ?? [])) {
    const row = wsSinBbva.addRow({
      folio:  m.folio ?? '',
      fecha:  formatFecha(m.fecha),
      monto:  m.deposito,
      status: m.status,
    });
    row.eachCell(cell => { cell.fill = GRAY_FILL; });
  }
  wsSinBbva.getColumn('monto').numFmt = '#,##0.00';
  wsSinBbva.autoFilter = { from: 'A1', to: wsSinBbva.lastColumn.letter + '1' };

  // ── Hoja 4: Sin contraparte (otro banco) ────────────────────────────────────
  const wsSinOtros = wb.addWorksheet('Sin contraparte (otro banco)');
  wsSinOtros.columns = [
    { header: 'Banco',  key: 'banco',  width: 12 },
    { header: 'Folio',  key: 'folio',  width: 14 },
    { header: 'Fecha',  key: 'fecha',  width: 12 },
    { header: 'Monto',  key: 'monto',  width: 14 },
    { header: 'Status', key: 'status', width: 16 },
  ];
  styleHeader(wsSinOtros);

  for (const m of (resultado.sinContraparteOtros ?? [])) {
    const row = wsSinOtros.addRow({
      banco:  m.banco,
      folio:  m.folio ?? '',
      fecha:  formatFecha(m.fecha),
      monto:  m.retiro,
      status: m.status,
    });
    row.eachCell(cell => { cell.fill = GRAY_FILL; });
  }
  wsSinOtros.getColumn('monto').numFmt = '#,##0.00';
  wsSinOtros.autoFilter = { from: 'A1', to: wsSinOtros.lastColumn.letter + '1' };

  // ── Hoja 5: Sin banco detectado ──────────────────────────────────────────────
  // Depósitos BBVA de la categoría cuyo concepto no permitió determinar el banco
  // contraparte (regex no matcheó, o el resultado no es un banco válido del enum). Se
  // muestra el concepto crudo para que el usuario pueda entender por qué no se detectó.
  const wsSinBanco = wb.addWorksheet('Sin banco detectado');
  wsSinBanco.columns = [
    { header: 'Folio',    key: 'folio',    width: 14 },
    { header: 'Fecha',    key: 'fecha',    width: 12 },
    { header: 'Monto',    key: 'monto',    width: 14 },
    { header: 'Concepto', key: 'concepto', width: 60 },
  ];
  styleHeader(wsSinBanco);

  for (const m of (resultado.sinBancoDetectado ?? [])) {
    const row = wsSinBanco.addRow({
      folio:    m.folio ?? '',
      fecha:    formatFecha(m.fecha),
      monto:    m.deposito,
      concepto: m.concepto ?? '',
    });
    row.eachCell(cell => { cell.fill = GRAY_FILL; });
  }
  wsSinBanco.getColumn('monto').numFmt = '#,##0.00';
  wsSinBanco.autoFilter = { from: 'A1', to: wsSinBanco.lastColumn.letter + '1' };

  return wb.xlsx.writeBuffer();
}

module.exports = {
  matchTraspasosInternos,
  revertirTraspasosInternos,
  generarExcelTraspasosInternos,
};
