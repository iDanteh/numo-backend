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
const AdmZip            = require('adm-zip');
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

// ── generarPolizaContpaqTraspasos ────────────────────────────────────────────────
// Arma el Excel de póliza en formato de importación CONTPAQ (filas 'P'/'M1') para un
// lote de `resultado.relacionados` ya confirmado — NO crea nada en Postgres/Poliza
// (a propósito: el usuario eligió el camino "solo Excel", sin acoplar este dominio de
// bancos al de pólizas). Mismo esquema de filas/columnas que ya usa
// `_construirWorkbookPoliza` en poliza.service.js (verificado contra dos plantillas
// reales armadas a mano: "Ejemplo traspaso entre cuentas propias.xls" y
// "POLIZA PARA TRASPASOS.xls", ambas en la raíz del proyecto).
//
// Mapeo banco→cuenta contable: igual que `BANCO_A_CODIGO_CUENTA` en poliza.service.js
// — duplicado a propósito (mismo criterio ya usado en cfdi-mapping.service.js) para no
// acoplar el dominio de bancos al de pólizas.
const BANCO_A_CODIGO_CUENTA = {
  'Banamex':    '1102012001',
  'BBVA':       '1102011001',
  'Santander':  '1102013001',
  'Banorte':    '1102014001',
  'Scotiabank': '1102015001',
  'Azteca':     '1102016001',
};

// Categoría BBVA fija para la póliza CONTPAQ por rango de fechas — este flujo YA NO pide
// categoría al usuario (confirmado 2026-08-18: siempre es la misma categoría de traspaso
// entre cuentas propias). No se hardcodea un string exacto porque `categoria` en Mongo es
// texto libre capturado por BankRule y puede variar en mayúsculas/plural/redacción exacta
// ("Traspaso entre cuentas propias", "TRASPASOS ENTRE CUENTAS PROPIAS", etc. — confirmado
// con el usuario: debe tolerar mayúsculas/minúsculas y variaciones de letras de más/menos).
// Exige los 3 radicales clave en orden, insensible a mayúsculas — sin acentos porque los
// radicales elegidos (traspas/cuenta/propi) no los llevan.
const RE_CATEGORIA_TRASPASO_INTERNO = /traspas.*cuenta.*propi/i;

// Códigos del catálogo CONTPAQ "CATALOGO DE CUENTA DE FLUJOS" (hoja embebida en
// "POLIZA PARA TRASPASOS.xls"): 26 "Ingr traspasos" se ocupa para el cargo (lado BBVA,
// que recibe el depósito), 46 "Egr traspasos" se ocupa para el abono (lado contraparte,
// que hace el retiro). No existían en el código antes de esto — son específicos de
// traspasos, ningún otro generador de pólizas los usa.
const SUBCODIGO_CARGO_TRASPASO = 26;
const SUBCODIGO_ABONO_TRASPASO = 46;

// Dos conceptos distintos, confirmados contra las dos plantillas reales: el encabezado
// 'P' va en Title Case, el concepto de cada movimiento 'M1' va en mayúsculas — no es el
// mismo texto reutilizado, CONTPAQ los trae así en ambos archivos de ejemplo.
const CONCEPTO_TRASPASO_HEADER = 'Traspaso Entre Cuentas Propias';
const CONCEPTO_TRASPASO_MOVIMIENTO = 'TRASPASO ENTRE CUENTAS PROPIAS';

/**
 * Genera el Excel de póliza CONTPAQ (una fila 'P' + 2 filas 'M1' por cada par) a partir
 * de los pares ya confirmados en `resultado.relacionados` de `matchTraspasosInternos`.
 * No toca Mongo ni Postgres — solo arma el buffer .xlsx.
 *
 * @param {Array<{ bbva: object, contraparte: object }>} relacionados
 * @param {{ folio: number|string, fecha?: Date|string }} opts
 * @returns {Promise<Buffer>}
 */
async function generarPolizaContpaqTraspasos(relacionados, { folio, fecha } = {}) {
  if (!Array.isArray(relacionados) || relacionados.length === 0) {
    throw new Error('Se requiere al menos un par relacionado para generar la póliza.');
  }
  if (!folio) {
    throw new Error('Se requiere el folio de la póliza.');
  }

  // Cuentas faltantes en el catálogo — se juntan TODAS antes de tirar el error (no una
  // por una) para que el usuario vea de una vez qué bancos hace falta mapear, mismo
  // criterio que la validación de "cuenta faltante" en exportContpaqXlsx.
  const bancosFaltantes = new Set();
  for (const { bbva, contraparte } of relacionados) {
    if (!BANCO_A_CODIGO_CUENTA[bbva.banco])        bancosFaltantes.add(bbva.banco);
    if (!BANCO_A_CODIGO_CUENTA[contraparte.banco]) bancosFaltantes.add(contraparte.banco);
  }
  if (bancosFaltantes.size > 0) {
    throw new Error(
      `Banco(s) sin cuenta contable mapeada para exportar a CONTPAQ: ${[...bancosFaltantes].join(', ')}.`,
    );
  }

  const wb = new ExcelJS.Workbook();
  wb.creator = 'Numo — Traspasos Internos';
  wb.created = new Date();

  const sheet = wb.addWorksheet('poliza');
  sheet.columns = [
    { width: 6 }, { width: 14 }, { width: 10 }, { width: 10 }, { width: 16 },
    { width: 10 }, { width: 65 }, { width: 50 }, { width: 14 }, { width: 10 },
  ];

  const fechaPoliza = fecha ? new Date(fecha) : new Date();

  // Columna I (posición 8) = 1 tanto en el encabezado como en cada M1 — verificado celda
  // por celda contra "Ejemplo traspaso entre cuentas propias.xls" (no vacío/'0' como se
  // había dejado antes; corregido 2026-08-18 tras comparación exacta con el usuario).
  const headerRow = sheet.addRow(['P', fechaPoliza, '3', folio, '1', '0', CONCEPTO_TRASPASO_HEADER, '11', 1, '0']);
  headerRow.getCell(2).numFmt = 'dd/mm/yyyy';
  // Sin relleno de color: verificado con formatting_info contra los dos .xls reales — ninguno
  // usa fill (el patrón negro + fuente blanca es de _construirWorkbookPoliza en
  // poliza.service.js, para CFDI, NO aplica aquí). Negrita sí, sin forzar color de fuente
  // (coincide con "POLIZA PARA TRASPASOS.xls", la plantilla más reciente).
  headerRow.eachCell({ includeEmpty: true }, (cell) => {
    cell.font = { bold: true };
  });

  for (const { bbva, contraparte } of relacionados) {
    const monto = bbva.deposito ?? contraparte.retiro;

    const rowCargo = sheet.addRow([
      'M1', BANCO_A_CODIGO_CUENTA[bbva.banco], 'TRASP', 0, monto,
      SUBCODIGO_CARGO_TRASPASO, 0, CONCEPTO_TRASPASO_MOVIMIENTO, 1,
    ]);
    rowCargo.getCell(5).numFmt = '#,##0.00';

    const rowAbono = sheet.addRow([
      'M1', BANCO_A_CODIGO_CUENTA[contraparte.banco], 'TRASP', 1, monto,
      SUBCODIGO_ABONO_TRASPASO, 0, CONCEPTO_TRASPASO_MOVIMIENTO, 1,
    ]);
    rowAbono.getCell(5).numFmt = '#,##0.00';
  }

  return wb.xlsx.writeBuffer();
}

// Medianoche UTC del día calendario de `fecha` (Date o string ISO) — usado para comparar
// contra el rango fechaInicio/fechaFin sin arrastrar hora/timezone, mismo criterio de "día
// UTC" que ya usa diaUtcKey/bucketKey para el matching (ver comentario de diaUtcKey arriba).
function _utcMidnight(fecha) {
  const d = new Date(fecha);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
}

const UN_DIA_MS = 24 * 60 * 60 * 1000;

// ── _buscarYaRelacionadosEnRango ─────────────────────────────────────────────────
// Como _cargarCandidatos excluye status:'identificado', regenerar la MISMA póliza
// (mismo rango) una segunda vez encuentra 0 candidatos — no porque nunca hubo pares,
// sino porque la corrida anterior ya los relacionó. Esta función distingue ambos casos
// para poder dar un mensaje de error específico en vez del genérico "no hay traspasos".
// Busca solo del lado BBVA (mismo campo que ancla la búsqueda de candidatos) un
// movimiento ya identificado vía este motor, con fecha dentro del rango — y devuelve
// la entrada de identificadoPor más reciente de este motor (recorrida desde el final,
// por si el movimiento tiene más de una identificación con distinto runId a través del
// tiempo — solo interesa la última).
async function _buscarYaRelacionadosEnRango(inicioUtc, finUtc) {
  const mov = await BankMovement.findOne({
    banco:           'BBVA',
    categoria:        RE_CATEGORIA_TRASPASO_INTERNO,
    status:          'identificado',
    traspasoInterno: { $ne: null },
    isActive:        true,
    deposito:        { $gt: 0 },
    fecha:           { $gte: new Date(inicioUtc), $lt: new Date(finUtc + UN_DIA_MS) },
  }).select('identificadoPor').lean();

  if (!mov) return null;

  const entrada = [...(mov.identificadoPor ?? [])].reverse()
    .find((e) => e.source === SOURCE_TRASPASO_INTERNO);

  if (!entrada) return null;

  return { fecha: entrada.fechaId, nombre: entrada.nombre, runId: entrada.runId };
}

/**
 * Arma un ZIP con una póliza CONTPAQ por cada día (UTC) dentro de [fechaInicio, fechaFin]
 * que tenga al menos un par relacionado. Los traspasos siempre son mismo-día (ver
 * `bucketKey` más arriba — el motor nunca empareja movimientos de días distintos), así que
 * agrupar por día antes de armar cada Excel respeta esa misma premisa en vez de mezclar
 * movimientos de días distintos bajo un solo encabezado 'P'.
 *
 * La CLASIFICACIÓN corre en dry-run (matchTraspasosInternos({dryRun:true})) — solo lee.
 * Pero una vez armado el ZIP completo en memoria sin errores, esta función SÍ PERSISTE: cada
 * par que efectivamente entró en la póliza generada queda relacionado 1-1 (`traspasoInterno`
 * en ambos lados, mismo runId nuevo) y marcado `identificado`, con el usuario que generó la
 * póliza en `identificadoPor` — puramente para trazabilidad/consulta interna, reversible con
 * `revertirTraspasosInternos(runId, userId)` (ya existe, sin cambios). El requisito de que
 * un par relacionado no vuelva a aparecer en el Excel se cumple gratis: `_cargarCandidatos`
 * ya excluye `status:'identificado'`, así que una vez relacionado deja de ser candidato en
 * cualquier corrida futura del matching (incluida la de esta misma función).
 *
 * Efecto colateral de lo anterior: regenerar la MISMA póliza (mismo rango) una segunda vez
 * ya no encuentra esos pares (ver `_buscarYaRelacionadosEnRango`, da un mensaje de error
 * específico en vez del genérico "no hay traspasos" para no confundir con "nunca hubo
 * pares").
 *
 * Folio PROVISIONAL: número secuencial pequeño (1, 2, 3... por día dentro de la corrida),
 * NO derivado de la fecha — un folio tipo "20260808" no coincide con el estilo de las
 * plantillas reales (folios chicos: 1, 8). Es un placeholder hasta que el departamento de
 * contabilidad defina la nomenclatura real de folio para este tipo de póliza (pendiente,
 * confirmado con el usuario 2026-08-18 — no es la implementación final).
 *
 * Ya NO recibe categoriaBbva — este flujo siempre busca sobre la categoría de traspaso
 * entre cuentas propias (ver RE_CATEGORIA_TRASPASO_INTERNO arriba), nunca otra. Único
 * input real: el rango de fechas.
 *
 * @param {{ fechaInicio: string, fechaFin: string }} params
 * @param {{ _id: string, nombre?: string }} user
 * @returns {Promise<{ buffer: Buffer, nombreZip: string, runId: string }>}
 */
async function generarPolizasContpaqTraspasosPorRango({ fechaInicio, fechaFin } = {}, user) {
  if (!fechaInicio || !fechaFin) throw new Error('Se requiere fechaInicio y fechaFin.');

  const inicioUtc = _utcMidnight(fechaInicio);
  const finUtc    = _utcMidnight(fechaFin);

  const resultado = await matchTraspasosInternos({ categoriaBbva: RE_CATEGORIA_TRASPASO_INTERNO, dryRun: true }, user);

  // diaUtcKey → { fechaIso, pares[] } — agrupa los pares ya filtrados al rango por su día
  // UTC (mismo criterio que bucketKey usa para el matching).
  const porDia = new Map();
  for (const par of resultado.relacionados) {
    const movUtc = _utcMidnight(par.bbva.fecha);
    if (movUtc < inicioUtc || movUtc > finUtc) continue;

    const key = diaUtcKey(par.bbva.fecha);
    if (!porDia.has(key)) {
      porDia.set(key, { fechaIso: new Date(movUtc).toISOString().slice(0, 10), pares: [] });
    }
    porDia.get(key).pares.push(par);
  }

  if (porDia.size === 0) {
    const yaRelacionado = await _buscarYaRelacionadosEnRango(inicioUtc, finUtc);
    if (yaRelacionado) {
      const fechaStr = yaRelacionado.fecha
        ? new Date(yaRelacionado.fecha).toLocaleString('es-MX', { dateStyle: 'medium', timeStyle: 'short' })
        : 'fecha desconocida';
      const quien = yaRelacionado.nombre ?? 'un usuario';
      const pistaRevertir = yaRelacionado.runId
        ? ` Usá "Revertir" con el runId "${yaRelacionado.runId}" si necesitás regenerar la póliza.`
        : '';
      throw new Error(
        `Los traspasos de este rango ya fueron relacionados el ${fechaStr} por ${quien}.${pistaRevertir}`,
      );
    }
    throw new Error('No hay traspasos relacionados en el rango de fechas seleccionado.');
  }

  const dias = [...porDia.values()].sort((a, b) => a.fechaIso.localeCompare(b.fechaIso));

  // Folio PROVISIONAL: número secuencial pequeño (1, 2, 3...) por día dentro de esta
  // corrida — NO un valor derivado de la fecha (ej. 20260811), que no coincide con el
  // estilo de folio de las plantillas reales (ahí son números chicos: 1, 8). Sigue
  // pendiente de que contabilidad defina la nomenclatura real (ver docstring arriba).
  const zip = new AdmZip();
  let folio = 1;
  for (const { fechaIso, pares } of dias) {
    const buffer = await generarPolizaContpaqTraspasos(pares, { folio, fecha: fechaIso });
    zip.addFile(`Poliza_Traspasos_${fechaIso.replace(/-/g, '')}_CONTPAQ.xlsx`, Buffer.from(buffer));
    folio++;
  }

  // Persistir la relación 1-1 recién ahora, con el ZIP completo ya armado en memoria sin
  // errores (ej. banco sin cuenta contable mapeada) — así una falla generando el Excel
  // nunca deja movimientos marcados `identificado` sin que exista la póliza correspondiente.
  const runId = randomUUID();
  const now   = new Date();
  const ops   = [];
  for (const { pares } of dias) {
    for (const { bbva, contraparte } of pares) {
      ops.push(_buildIdentificarOp(bbva, contraparte, user, runId, now));
      ops.push(_buildIdentificarOp(contraparte, bbva, user, runId, now));
    }
  }
  if (ops.length > 0) {
    await ejecutarBulkConTransaccion(ops);
  }

  const nombreZip = `Traspasos_CONTPAQ_${fechaInicio}_a_${fechaFin}.zip`;
  return { buffer: zip.toBuffer(), nombreZip, runId };
}

module.exports = {
  matchTraspasosInternos,
  revertirTraspasosInternos,
  generarExcelTraspasosInternos,
  generarPolizaContpaqTraspasos,
  generarPolizasContpaqTraspasosPorRango,
  RE_CATEGORIA_TRASPASO_INTERNO,
  // Exportado además para poliza.service.js#generarYGuardarTraspasos (Pólizas Traspasos
  // C.P., 2026-08-25) — reusa el mismo pipeline de relación 1-1 en vez de duplicarlo.
  _buildIdentificarOp,
};
