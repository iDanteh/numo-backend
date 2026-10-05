'use strict';

/**
 * Comentarios de discrepancias al recomparar (2026-10-05, reportado por el
 * usuario: ningún CFDI tenía comentarios). Cada comparación borra las
 * discrepancias abiertas del CFDI y crea otras nuevas; los comentarios
 * (`Discrepancy.comentarios`) vivían solo en las viejas y se perdían en la
 * siguiente corrida del job diario. Estas utilidades los leen antes del
 * borrado y los pasan a las nuevas del mismo tipo.
 */

const Discrepancy = require('../models/Discrepancy');

/** Comentarios de las discrepancias que cumplen `filter`, agrupados por tipo. */
async function comentariosPorTipo(filter) {
  const previas = await Discrepancy.find({ ...filter, 'comentarios.0': { $exists: true } }, 'type comentarios').lean();
  const mapa = new Map();
  for (const d of previas) {
    const lista = mapa.get(d.type) ?? [];
    for (const c of d.comentarios) {
      const clave = `${c.motivo}|${c.descripcion}|${c.creadoPor}|${new Date(c.creadoEn).getTime()}`;
      if (!lista.some(x => x._clave === clave)) lista.push({ ...c, _clave: clave });
    }
    mapa.set(d.type, lista);
  }
  return mapa;
}

/**
 * Para cada tipo de las discrepancias nuevas (en orden), los comentarios que
 * le tocan: los de su mismo tipo; los de tipos que ya no aparecen se pasan a
 * la primera, para no perderlos.
 */
function repartirComentarios(mapa, tipos) {
  const resultado = tipos.map(() => []);
  if (!mapa.size || !tipos.length) return resultado;
  const usados = new Set();
  tipos.forEach((t, i) => {
    if (mapa.has(t) && !usados.has(t)) { resultado[i].push(...mapa.get(t)); usados.add(t); }
  });
  for (const [t, lista] of mapa) if (!usados.has(t)) resultado[0].push(...lista);
  return resultado.map(lista => lista
    .sort((a, b) => new Date(a.creadoEn) - new Date(b.creadoEn))
    .map(({ _clave, ...c }) => c));
}

module.exports = { comentariosPorTipo, repartirComentarios };
