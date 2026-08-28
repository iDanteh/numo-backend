'use strict';

// compensaciones-intereses.service.js — Pólizas Compensaciones Bancarias / Intereses
// Ganados (2026-08-27). Réplica de la póliza mensual que hoy arma contabilidad a mano
// en Excel ("D-185 COMP 186 INT GANADOS.xls") para BBVA/Banamex: a diferencia de
// Traspasos (pares 1-1), acá son MUCHAS líneas de banco individuales (una por
// BankMovement candidato) contra UNA sola línea de cierre agregada (la suma de todas)
// — Compensaciones cierra contra "Otros Ingresos" (5204990001), Intereses Ganados
// contra "Intereses Ganados" (5203010001), ambas cuentas fijas ya en el catálogo
// semilla (seed-account-plan.js), sin mapeo dinámico necesario ahí.
//
// La selección de candidatos depende de 2 BankRules ("COMPENSACIONES"/"INTERESES
// GANADOS", BBVA + Banamex) creadas/actualizadas con el usuario el 2026-08-27 —
// a diferencia de Traspasos, acá SÍ se usa `categoria` con match exacto (no regex
// tolerante): los nombres de esas reglas los definimos nosotros mismos hoy, no hay
// variación histórica de redacción que tolerar.

const ExcelJS       = require('exceljs');
const BankMovement  = require('./BankMovement.model');
const { TODOS_MOTORES_HISTORICO } = require('./bank.service');
const { ejecutarBulkConTransaccion } = require('./bank-autorizaciones.service');

// Identificación humana asistida por este motor — NO se agrega a TODOS_MOTORES_HISTORICO
// (mismo criterio que SOURCE_TRASPASO_INTERNO: el usuario revisó/confirmó antes de
// generar, no es un motor 100% automático sin supervisión).
const SOURCE_COMPENSACION_INTERES = 'compensacion-interes-bancario';

// Arreglo, no string único: "SPEI COMPENSACION" (BankRule BBVA, 2026-08-27) cubre
// las líneas "SPEI RECIBIDO..." con depósito < $1.00 que "COMPENSACIONES" no podía
// distinguir por concepto (no llevan ninguna palabra de compensación) — categoría
// propia a propósito, pero el mismo cierre de Compensaciones las debe incluir.
const CATEGORIAS_COMPENSACIONES    = ['COMPENSACIONES', 'SPEI COMPENSACION'];
const CATEGORIAS_INTERESES_GANADOS = ['INTERESES GANADOS'];

// Mapeo banco→cuenta contable: igual que `BANCO_A_CODIGO_CUENTA` en poliza.service.js
// — duplicado a propósito (mismo criterio ya usado en traspasos-internos.service.js/
// cfdi-mapping.service.js) para no acoplar el dominio de bancos al de pólizas.
const BANCO_A_CODIGO_CUENTA = {
  'Banamex':    '1102012001',
  'BBVA':       '1102011001',
  'Santander':  '1102013001',
  'Banorte':    '1102014001',
  'Scotiabank': '1102015001',
  'Azteca':     '1102016001',
};

// Cuentas de cierre — FIJAS (no dependen del banco), ya existen en el catálogo semilla.
const CUENTA_OTROS_INGRESOS_CODIGO    = '5204990001'; // "Otros Ingresos" — cierre de Compensaciones
const CUENTA_INTERESES_GANADOS_CODIGO = '5203010001'; // "Intereses Ganados" — cierre de Intereses Ganados

// Subcódigo de las líneas de banco — verificado contra "D-185 COMP 186 INT GANADOS.xls"
// (columna F de cada M1 bancario, ver inspección celda por celda 2026-08-27). Distinto
// del 26/46 que usa Traspasos — cada generador tiene los suyos, no hay tabla compartida.
const SUBCODIGO_BANCO_COMP_INT = 22;

const TAG_LINEA_BANCO           = 'REF.';
const TAG_CIERRE_COMPENSACIONES = 'OTRO INGRESOS';
const TAG_CIERRE_INTERESES      = 'REF.';

const CONCEPTO_CIERRE_COMPENSACIONES = 'OTROS INGRESOS BANCARIOS';
const CONCEPTO_CIERRE_INTERESES      = 'INTERESES GANADOS';

const SELECT_CANDIDATO = '_id banco fecha deposito retiro folio concepto categoria status';

/** Monto real de un candidato — depósito si lo hay, si no retiro (ver nota de firma arriba). */
function _montoCandidato(mov) {
  const dep = Number(mov.deposito) || 0;
  if (dep > 0) return dep;
  return Number(mov.retiro) || 0;
}

/**
 * Candidatos BBVA/Banamex para uno de los 2 grupos de categorías (ver
 * CATEGORIAS_COMPENSACIONES/CATEGORIAS_INTERESES_GANADOS — un grupo puede tener más
 * de una BankRule detrás, ej. "COMPENSACIONES" + "SPEI COMPENSACION"), dentro del
 * rango de fechas (inclusive). Mismo criterio de elegibilidad que Traspasos: activo,
 * no vinculado a ninguna CxC (erpIds vacío), y en 'no_identificado' u 'otros' (las
 * BankRules ya los mueven a 'otros', pero se tolera 'no_identificado' por si la
 * regla todavía no corrió sobre ese movimiento).
 */
async function _cargarCandidatos(categorias, fechaInicio, fechaFin) {
  return BankMovement.find({
    banco:     { $in: ['BBVA', 'Banamex'] },
    categoria: { $in: categorias },
    status:    { $in: ['no_identificado', 'otros'] },
    isActive:  true,
    erpIds:    { $size: 0 },
    fecha:     { $gte: fechaInicio, $lte: fechaFin },
  }).select(SELECT_CANDIDATO).lean();
}

// Filtro ACID: solo escribe si el movimiento sigue en un estado elegible (nadie lo
// identificó ni le vinculó una CxC entre la lectura y esta escritura) — mismo patrón
// que traspasos-internos.service.js#_buildIdentificarOp, sin el campo `traspasoInterno`
// (acá no hay contraparte 1-1 que apuntar).
function _buildIdentificarOp(mov, user, runId, now) {
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
                  source:  SOURCE_COMPENSACION_INTERES,
                  runId,
                }],
              ],
            },
            primeraIdentificacionAt: { $ifNull: ['$primeraIdentificacionAt', now] },
            primeraIdentificacionPor: {
              $ifNull: ['$primeraIdentificacionPor', { userId: user._id, nombre: user.nombre ?? user._id }],
            },
          },
        },
      ],
    },
  };
}

// Deshace exactamente lo que aplicó _buildIdentificarOp para un runId concreto — mismo
// patrón que revertirTraspasosInternos/revertirConciliacion (bank.service.js): solo
// actúa sobre movimientos con esa combinación userId+source+runId, resetea status a
// 'no_identificado' únicamente si no quedan entradas de identificación humana.
async function revertirCompensacionesIntereses(runId, userId) {
  const resultado = await BankMovement.updateMany(
    {
      isActive: true,
      status:   'identificado',
      identificadoPor: {
        $elemMatch: { userId, source: SOURCE_COMPENSACION_INTERES, runId },
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
                    { $eq: ['$$entry.source',  SOURCE_COMPENSACION_INTERES] },
                    { $eq: ['$$entry.runId',   runId] },
                  ],
                },
              },
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
                                  { $eq: ['$$e.source',  SOURCE_COMPENSACION_INTERES] },
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

/**
 * Arma el Excel CONTPAQ (formato P/M1) de UNA póliza — Compensaciones o Intereses
 * Ganados, según `opts.tag`/`opts.concepto`/`opts.cuentaCierreCodigo`. Verificado
 * celda por celda contra "D-185 COMP 186 INT GANADOS.xls" (2026-08-27): a diferencia
 * de generarPolizaContpaqTraspasos (traspasos-internos.service.js), acá la columna I
 * (posición 8) del encabezado 'P' es 0 (no 1) y NINGUNA celda del encabezado va en
 * negrita — ese archivo real no la usa, aunque los dos de Traspasos sí.
 */
async function generarPolizaContpaqCompensacionesIntereses(candidatos, opts) {
  const { folio, fecha, concepto, cuentaCierreCodigo, tagCierre, conceptoCierre } = opts || {};
  if (!Array.isArray(candidatos) || candidatos.length === 0) {
    throw new Error('Se requiere al menos un movimiento candidato para generar la póliza.');
  }
  if (!folio) throw new Error('Se requiere el folio de la póliza.');

  const wb = new ExcelJS.Workbook();
  wb.creator = 'Numo — Compensaciones e Intereses Ganados';
  wb.created = new Date();

  const sheet = wb.addWorksheet('poliza');
  sheet.columns = [
    { width: 6 }, { width: 14 }, { width: 10 }, { width: 10 }, { width: 16 },
    { width: 10 }, { width: 65 }, { width: 50 }, { width: 14 }, { width: 10 },
  ];

  const fechaPoliza = fecha ? new Date(fecha) : new Date();
  const headerRow = sheet.addRow(['P', fechaPoliza, '3', folio, '1', '0', concepto, '11', 0, '0']);
  headerRow.getCell(2).numFmt = 'm/d/yy';

  let total = 0;
  for (const cand of candidatos) {
    const monto = _montoCandidato(cand);
    total += monto;
    const row = sheet.addRow([
      'M1', BANCO_A_CODIGO_CUENTA[cand.banco], TAG_LINEA_BANCO, 0, monto,
      SUBCODIGO_BANCO_COMP_INT, 0, cand.concepto, 1,
    ]);
    row.getCell(5).numFmt = '#,##0.00';
  }

  const rowCierre = sheet.addRow(['M1', cuentaCierreCodigo, tagCierre, 1, total, 0, 0, conceptoCierre, 1]);
  rowCierre.getCell(5).numFmt = '#,##0.00';

  return wb.xlsx.writeBuffer();
}

module.exports = {
  CATEGORIAS_COMPENSACIONES, CATEGORIAS_INTERESES_GANADOS,
  CUENTA_OTROS_INGRESOS_CODIGO, CUENTA_INTERESES_GANADOS_CODIGO,
  TAG_CIERRE_COMPENSACIONES, TAG_CIERRE_INTERESES,
  CONCEPTO_CIERRE_COMPENSACIONES, CONCEPTO_CIERRE_INTERESES,
  BANCO_A_CODIGO_CUENTA,
  _cargarCandidatos, _montoCandidato, _buildIdentificarOp,
  revertirCompensacionesIntereses, generarPolizaContpaqCompensacionesIntereses,
};
