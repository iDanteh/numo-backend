/**
 * Construye el objeto de respuesta paginada estándar.
 * @param {any[]} data
 * @param {number} total
 * @param {number} page
 * @param {number} limit
 * @returns {{ data: any[], pagination: { total: number, page: number, limit: number, pages: number } }}
 */
const paginate = (data, total, page, limit) => ({
  data,
  pagination: {
    total,
    page,
    limit,
    pages: Math.ceil(total / limit),
  },
});

/**
 * Calcula el número de documentos a saltar.
 * @param {number} page
 * @param {number} limit
 * @returns {number}
 */
const skip = (page, limit) => (page - 1) * limit;

module.exports = { paginate, skip };
