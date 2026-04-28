'use strict';

/**
 * visor/services/periodoFiscal.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Utilidades de período fiscal consumidas por el satSyncJob y otros servicios.
 * Ahora consulta PostgreSQL via periodo-fiscal.repository.
 */

const periodoRepo = require('../repositories/periodo-fiscal.repository');

/**
 * Verifica que el PeriodoFiscal (ejercicio + periodo mensual) exista en BD.
 * Lanza un error con status 400 si no existe.
 *
 * @param {number} ejercicio
 * @param {number} periodo   — mes (1–12), no acepta null (año completo)
 * @returns {Promise<PeriodoFiscal>}
 */
const resolverPeriodo = async (ejercicio, periodo) => {
  if (!Number.isInteger(ejercicio) || !Number.isInteger(periodo)) {
    const err = new Error('ejercicio y periodo deben ser números enteros.');
    err.status = 400;
    throw err;
  }

  const doc = await periodoRepo.findByEjercicioPeriodo(ejercicio, periodo);
  if (!doc) {
    const err = new Error(
      `El periodo ${periodo}/${ejercicio} no existe. Créalo primero en la sección Ejercicios.`,
    );
    err.status = 400;
    throw err;
  }

  return doc;
};

/**
 * Busca el PeriodoFiscal en BD y lo crea automáticamente si no existe.
 * Usado exclusivamente por los jobs automáticos nocturnos.
 *
 * @param {number} ejercicio
 * @param {number} periodo   — mes (1–12)
 * @returns {Promise<{ doc: PeriodoFiscal, creado: boolean }>}
 */
const MESES = ['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'];

const resolverOCrearPeriodo = async (ejercicio, periodo) => {
  if (!Number.isInteger(ejercicio) || !Number.isInteger(periodo)) {
    const err = new Error('ejercicio y periodo deben ser números enteros.');
    err.status = 400;
    throw err;
  }

  let doc = await periodoRepo.findByEjercicioPeriodo(ejercicio, periodo);
  if (doc) return { doc, creado: false };

  const label = `${MESES[periodo - 1]} ${ejercicio}`;
  doc = await periodoRepo.create({ ejercicio, periodo, label, createdBy: null });
  return { doc, creado: true };
};

/**
 * Deriva ejercicio y periodo de una fecha sin consultar la BD.
 * Usado por los jobs automáticos donde el periodo viene de la fecha del día.
 *
 * @param {Date} date
 * @returns {{ ejercicio: number, periodo: number }}
 */
const derivarPeriodoDesdeFecha = (date) => ({
  ejercicio: date.getFullYear(),
  periodo:   date.getMonth() + 1,
});

module.exports = { resolverPeriodo, resolverOCrearPeriodo, derivarPeriodoDesdeFecha };
