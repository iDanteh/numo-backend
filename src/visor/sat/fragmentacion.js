/**
 * Genera rangos de fechas para la descarga masiva SAT según el volumen esperado.
 *
 * Reglas:
 *   < 5,000 CFDIs  → mes completo
 *   5,000–50,000   → quincenas
 *   50,000–200,000 → semanas
 *   > 200,000      → días
 */

/**
 * @param {Date|string} fechaInicio
 * @param {Date|string} fechaFin
 * @param {'mes'|'quincena'|'semana'|'dia'} fragmento
 * @returns {Array<{inicio: Date, fin: Date}>}
 */
const generarRangos = (fechaInicio, fechaFin, fragmento) => {
  const inicio = new Date(fechaInicio);
  const fin = new Date(fechaFin);

  // Normalizar a inicio/fin del día
  inicio.setHours(0, 0, 0, 0);
  fin.setHours(23, 59, 59, 999);

  if (fragmento === 'mes') return rangoPorMes(inicio, fin);
  if (fragmento === 'quincena') return rangoPorQuincena(inicio, fin);
  if (fragmento === 'semana') return rangoPorSemana(inicio, fin);
  return rangoPorDia(inicio, fin);
};

/**
 * Determina el fragmento sugerido según el volumen estimado de CFDIs.
 * @param {number} volumenEstimado
 * @returns {'mes'|'quincena'|'semana'|'dia'}
 */
const fragmentoPorVolumen = (volumenEstimado) => {
  if (volumenEstimado < 5000)   return 'mes';
  if (volumenEstimado < 50000)  return 'quincena';
  if (volumenEstimado < 200000) return 'semana';
  return 'dia';
};

// ── Generadores internos ──────────────────────────────────────────────────────

const rangoPorMes = (inicio, fin) => {
  const rangos = [];
  let cur = primerDiaDelMes(inicio);

  while (cur <= fin) {
    const finMes = ultimoDiaDelMes(cur);
    rangos.push({
      inicio: new Date(cur),
      fin: min(finMes, fin),
    });
    cur = new Date(finMes);
    cur.setDate(cur.getDate() + 1);
    cur.setHours(0, 0, 0, 0);
  }
  return rangos;
};

const rangoPorQuincena = (inicio, fin) => {
  const rangos = [];
  let cur = new Date(inicio);

  while (cur <= fin) {
    let finQuincena;
    if (cur.getDate() <= 15) {
      finQuincena = new Date(cur.getFullYear(), cur.getMonth(), 15, 23, 59, 59, 999);
    } else {
      finQuincena = ultimoDiaDelMes(cur);
    }
    rangos.push({
      inicio: new Date(cur),
      fin: min(finQuincena, fin),
    });
    cur = new Date(finQuincena);
    cur.setDate(cur.getDate() + 1);
    cur.setHours(0, 0, 0, 0);
  }
  return rangos;
};

const rangoPorSemana = (inicio, fin) => {
  const rangos = [];
  let cur = new Date(inicio);

  while (cur <= fin) {
    const finSemana = new Date(cur);
    finSemana.setDate(finSemana.getDate() + 6);
    finSemana.setHours(23, 59, 59, 999);
    rangos.push({
      inicio: new Date(cur),
      fin: min(finSemana, fin),
    });
    cur = new Date(finSemana);
    cur.setDate(cur.getDate() + 1);
    cur.setHours(0, 0, 0, 0);
  }
  return rangos;
};

const rangoPorDia = (inicio, fin) => {
  const rangos = [];
  let cur = new Date(inicio);

  while (cur <= fin) {
    const finDia = new Date(cur);
    finDia.setHours(23, 59, 59, 999);
    rangos.push({
      inicio: new Date(cur),
      fin: new Date(finDia),
    });
    cur.setDate(cur.getDate() + 1);
    cur.setHours(0, 0, 0, 0);
  }
  return rangos;
};

// ── Utilidades ────────────────────────────────────────────────────────────────

const primerDiaDelMes = (fecha) => {
  return new Date(fecha.getFullYear(), fecha.getMonth(), 1, 0, 0, 0, 0);
};

const ultimoDiaDelMes = (fecha) => {
  return new Date(fecha.getFullYear(), fecha.getMonth() + 1, 0, 23, 59, 59, 999);
};

const min = (a, b) => (a < b ? a : b);

module.exports = { generarRangos, fragmentoPorVolumen };
