'use strict';

/**
 * Servicio de Validación y Reclasificación de Facturas Globales.
 *
 * Regla de negocio:
 *   La factura global (InformacionGlobal=true) se clasifica según InformacionGlobal.Mes
 *   y InformacionGlobal.Anio, que indican el mes contable al que realmente pertenecen
 *   las ventas consolidadas de esa factura.
 *   Se corrigen los campos `periodo` y `ejercicio` del documento en MongoDB para que
 *   coincidan con InformacionGlobal.Mes / InformacionGlobal.Anio.
 *   InformacionGlobal es la fuente de verdad — NO se modifica.
 *
 * Aplica SOLO a CFDIs que tengan el nodo cfdi:InformacionGlobal.
 *
 * Flujo recomendado:
 *   1. Llamar a generarPlan()   → devuelve análisis completo SIN modificar datos.
 *   2. Revisar el plan.
 *   3. Llamar a aplicarReclasificacion() → ejecuta los cambios en MongoDB.
 */

const CFDI       = require('../models/CFDI');
const Comparison = require('../models/Comparison');
const { logger } = require('../../shared/utils/logger');
// ── Helpers ───────────────────────────────────────────────────────────────────

/**
 * Extrae InformacionGlobal directamente del string XML (regex, sin parseo completo).
 * Útil para CFDIs existentes que aún no tienen el campo en MongoDB.
 * @param {string} xmlString
 * @returns {{ periodicidad: string|null, mes: string|null, anio: string|null }|null}
 */
const _extraerDeXML = (xmlString) => {
  if (!xmlString) return null;
  const match = xmlString.match(/<[^:]*:?InformacionGlobal([^/>]*)\/?>/);
  if (!match) return null;
  const tag = match[1];
  const get = (pattern) => { const m = tag.match(pattern); return m ? m[1] : null; };
  const mes  = get(/Meses="([^"]+)"/) || get(/\bMes="([^"]+)"/);
  const anio = get(/A[ñÑn]o="([^"]+)"/) || get(/Anio="([^"]+)"/);
  if (!mes && !anio) return null;
  return {
    periodicidad: get(/Periodicidad="([^"]+)"/),
    mes,
    anio,
  };
};

/**
 * Resuelve el objeto informacionGlobal de un documento CFDI.
 * Prioriza el campo en MongoDB; si no existe, intenta extraerlo del xmlContent.
 * @returns {{ periodicidad, mes, anio }|null}
 */
const _resolverInfoGlobal = (cfdi) => {
  if (cfdi.informacionGlobal?.mes || cfdi.informacionGlobal?.anio) {
    return cfdi.informacionGlobal;
  }
  if (cfdi.xmlContent) {
    return _extraerDeXML(cfdi.xmlContent);
  }
  return null;
};

/**
 * Analiza un documento CFDI y determina si requiere reclasificación.
 * Fuente de verdad: InformacionGlobal.Mes y InformacionGlobal.Anio.
 * Se corrige `periodo` y `ejercicio` para que coincidan con esos valores.
 * @returns {object} Resultado del análisis para este CFDI.
 */
const _analizarCFDI = (cfdi, infoGlobal) => {
  // InformacionGlobal.Mes es el mes contable al que realmente pertenece la factura global.
  // Es la fuente de verdad — se usa para corregir `periodo` y `ejercicio`.
  const mesCorrecto = infoGlobal.mes  ? parseInt(infoGlobal.mes,  10) : null;
  const anoCorrecto = infoGlobal.anio ? parseInt(infoGlobal.anio, 10) : null;

  const mesERP = cfdi.periodo   ?? null;
  const anoERP = cfdi.ejercicio ?? null;

  if (mesCorrecto === null || anoCorrecto === null) {
    return {
      _id:                    cfdi._id,
      uuid:                   cfdi.uuid,
      source:                 cfdi.source,
      mesInformacionGlobal:   infoGlobal.mes  ?? null,
      anioInformacionGlobal:  infoGlobal.anio ?? null,
      mesCorrecto,
      anoCorrecto,
      mesERP,
      ejercicioERP:           anoERP,
      requiereReclasificacion: false,
      motivo:                 'Sin InformacionGlobal.Mes o Anio — omitido',
      cambiosProyectados:     null,
    };
  }

  const motivos = [];
  if (mesERP !== null && mesERP !== mesCorrecto) motivos.push('Mes ERP incorrecto');
  if (anoERP !== null && anoERP !== anoCorrecto) motivos.push('Ejercicio ERP incorrecto');

  const requiereReclasificacion = motivos.length > 0;

  return {
    _id:                    cfdi._id,
    uuid:                   cfdi.uuid,
    source:                 cfdi.source,
    mesInformacionGlobal:   infoGlobal.mes  ?? null,
    anioInformacionGlobal:  infoGlobal.anio ?? null,
    mesCorrecto,
    anoCorrecto,
    mesERP,
    ejercicioERP:           anoERP,
    requiereReclasificacion,
    motivo: requiereReclasificacion
      ? motivos.join('; ')
      : 'Clasificación correcta',
    cambiosProyectados: requiereReclasificacion ? {
      periodo:   { antes: mesERP, despues: mesCorrecto },
      ejercicio: { antes: anoERP, despues: anoCorrecto },
    } : null,
  };
};

// ── Construcción del query de filtro ─────────────────────────────────────────

const _buildFiltro = ({ ejercicio, periodo, rfc, source, mesIG } = {}) => {
  const filtro = { isActive: true };
  if (ejercicio) filtro.ejercicio = Number(ejercicio);
  if (periodo)   filtro.periodo   = Number(periodo);
  if (rfc)       filtro['emisor.rfc'] = rfc.toUpperCase().trim();
  if (source)    filtro.source    = source.toUpperCase();
  // mesIG filtra por InformacionGlobal.Mes directamente (con y sin cero inicial)
  if (mesIG) {
    const n = Number(mesIG);
    filtro['informacionGlobal.mes'] = { $in: [String(n), String(n).padStart(2, '0')] };
  }
  return filtro;
};

// ── API pública ───────────────────────────────────────────────────────────────

/**
 * Genera el plan de reclasificación (DRY RUN — NO modifica datos).
 *
 * Para CFDIs SAT existentes sin el campo `informacionGlobal` en MongoDB,
 * extrae la información directamente del xmlContent almacenado.
 *
 * @param {object} filtros
 * @param {number}  [filtros.ejercicio]  — Año fiscal (ej. 2026)
 * @param {number}  [filtros.periodo]    — Mes fiscal 1-12
 * @param {string}  [filtros.rfc]        — RFC del emisor
 * @param {string}  [filtros.source]     — 'ERP' | 'SAT' (omitir para ambos)
 * @returns {Promise<object>} Plan completo con totales y detalle
 */
const generarPlan = async (filtros = {}) => {
  logger.info('[ReclasificacionGlobal] Generando plan de reclasificación...');
  const filtroBase = _buildFiltro(filtros);

  // ── Consulta 1: CFDIs que ya tienen informacionGlobal en MongoDB
  const conCampo = await CFDI.find({
    ...filtroBase,
    'informacionGlobal.mes': { $exists: true, $ne: null },
  }, 'uuid source fecha periodo ejercicio informacionGlobal').lean();

  // ── Consulta 2: CFDIs SAT sin el campo pero con xmlContent que contenga InformacionGlobal
  //    (datos existentes antes de esta actualización)
  const sinCampo = filtros.source === 'ERP' ? [] : await CFDI.find({
    ...filtroBase,
    'informacionGlobal.mes': { $exists: false },
    xmlContent:              { $regex: 'InformacionGlobal' },
  }).select('uuid source fecha periodo ejercicio +xmlContent').lean();

  const uuidsYaIncluidos = new Set(conCampo.map(c => c.uuid));
  const sinCampoFiltrado = sinCampo.filter(c => !uuidsYaIncluidos.has(c.uuid));

  logger.info(`[ReclasificacionGlobal] CFDIs con campo: ${conCampo.length}, extraídos de XML: ${sinCampoFiltrado.length}`);

  // ── Análisis ────────────────────────────────────────────────────────────────
  const detalle         = [];
  const motivoConteo    = {};
  let correctas         = 0;
  let reclasificadas    = 0;

  const analizar = (cfdi, infoGlobal) => {
    const resultado = _analizarCFDI(cfdi, infoGlobal);
    if (resultado.requiereReclasificacion) {
      reclasificadas++;
      resultado.motivo.split('; ').forEach(m => {
        motivoConteo[m] = (motivoConteo[m] || 0) + 1;
      });
    } else {
      correctas++;
    }
    detalle.push(resultado);
  };

  for (const cfdi of conCampo) {
    analizar(cfdi, cfdi.informacionGlobal);
  }

  for (const cfdi of sinCampoFiltrado) {
    const ig = _extraerDeXML(cfdi.xmlContent);
    if (ig) analizar(cfdi, ig);
  }

  // ── Ejemplos de inconsistencias (primeras 5) ─────────────────────────────
  const ejemplos = detalle
    .filter(d => d.requiereReclasificacion)
    .slice(0, 5)
    .map(({ uuid, mesCorrecto, mesERP, mesInformacionGlobal, motivo, cambiosProyectados }) =>
      ({ uuid, mesCorrecto, mesERP, mesInformacionGlobal, motivo, cambiosProyectados })
    );

  const plan = {
    generadoEn:               new Date().toISOString(),
    filtrosAplicados:         filtros,
    totalAnalizadas:          detalle.length,
    correctas,
    requierenReclasificacion: reclasificadas,
    camposQueSeModificaran:   ['periodo', 'ejercicio', 'informacionGlobal.mes', 'informacionGlobal.anio'],
    ejemplosInconsistencias:  ejemplos,
    resumen: {
      motivoConteo,
      reglaAplicada: 'Clasificación basada en fecha de emisión (CFDI), ignorando fecha de timbrado.',
    },
    detalle,
  };

  logger.info(
    `[ReclasificacionGlobal] Plan generado: ${detalle.length} analizadas, ` +
    `${correctas} correctas, ${reclasificadas} requieren reclasificación.`
  );
  Object.entries(motivoConteo).forEach(([motivo, count]) =>
    logger.info(`  → ${count}: ${motivo}`)
  );

  return plan;
};

/**
 * Aplica la reclasificación en MongoDB para todos los CFDIs que lo requieran.
 *
 * @param {object} filtros   — Filtros para buscar CFDIs (se ignoran si se pasa itemsExplicitos)
 * @param {Array}  [itemsExplicitos] — Items del plan ya calculado: [{ uuid, mesCorrecto, anoCorrecto, ... }]
 *   Si se proporciona, se usan directamente sin volver a consultar MongoDB.
 * @returns {Promise<object>} Resumen de lo aplicado
 */
const aplicarReclasificacion = async (filtros = {}, itemsExplicitos = null) => {
  logger.info('[ReclasificacionGlobal] Iniciando aplicación de reclasificación...');

  let aReclasificar;
  let plan = null;

  if (Array.isArray(itemsExplicitos) && itemsExplicitos.length > 0) {
    // Modo directo: usar exactamente los items que ya calculó el plan en el frontend
    aReclasificar = itemsExplicitos;
    logger.info(`[ReclasificacionGlobal] Modo directo: ${aReclasificar.length} items a migrar`);
  } else {
    // Modo plan: re-ejecutar la consulta (usado en contextos automáticos, ej. upload)
    plan = await generarPlan(filtros);
    aReclasificar = plan.detalle.filter(d => d.requiereReclasificacion);
  }

  if (aReclasificar.length === 0) {
    logger.info('[ReclasificacionGlobal] No hay CFDIs que requieran reclasificación.');
    const correctasPlan = plan
      ? plan.detalle.map(d => ({ uuid: d.uuid, source: d.source, periodo: d.mesCorrecto, ejercicio: d.anoCorrecto }))
      : [];
    return {
      aplicadoEn:       new Date().toISOString(),
      totalAnalizadas:  plan ? plan.totalAnalizadas : 0,
      totalCorrectas:   plan ? plan.correctas : 0,
      totalModificados: 0,
      resumen: { motivoConteo: {}, reglaAplicada: 'Clasificación basada en InformacionGlobal.Mes del XML SAT.' },
      modificadas: [],
      correctas:   correctasPlan,
    };
  }

  logger.info(`[ReclasificacionGlobal] Aplicando ${aReclasificar.length} reclasificaciones...`);

  // IMPORTANTE: El índice único de CFDI es { uuid, source }, puede haber un documento
  // SAT y uno ERP con el mismo UUID. El filtro DEBE incluir source para actualizar
  // exactamente el documento que analizó el plan (el SAT con InformacionGlobal).
  const ops = aReclasificar.map(d => ({
    updateOne: {
      filter: { uuid: d.uuid, source: d.source },
      update: { $set: { periodo: d.mesCorrecto, ejercicio: d.anoCorrecto } },
    },
  }));

  const resultado = await CFDI.bulkWrite(ops, { ordered: false });
  logger.info(`[ReclasificacionGlobal] bulkWrite CFDI: matched=${resultado.matchedCount}, modified=${resultado.modifiedCount}`);

  // Sincronizar registros Comparison al periodo/ejercicio correcto
  await Comparison.bulkWrite(aReclasificar.map(d => ({
    updateMany: {
      filter: { uuid: d.uuid },
      update: { $set: { periodo: d.mesCorrecto, ejercicio: d.anoCorrecto } },
    },
  })), { ordered: false });

  // Log detallado de cada factura migrada
  logger.info('[ReclasificacionGlobal] ── Facturas migradas ──────────────────────────────');
  for (const d of aReclasificar) {
    const mesAnt = d.mesAnterior ?? d.cambiosProyectados?.periodo?.antes ?? '?';
    const mesNvo = d.mesCorrecto ?? d.cambiosProyectados?.periodo?.despues ?? '?';
    logger.info(`  UUID: ${d.uuid} | Source: ${d.source ?? '?'} | Mes: ${mesAnt} → ${mesNvo} | Motivo: ${d.motivo ?? ''}`);
  }

  const resumenMotivos = {};
  aReclasificar.forEach(d => {
    const motivo = d.motivo || 'Mes incorrecto';
    motivo.split('; ').forEach(m => {
      resumenMotivos[m] = (resumenMotivos[m] || 0) + 1;
    });
  });

  logger.info(
    `[ReclasificacionGlobal] Completado: ${resultado.modifiedCount} documentos modificados.`
  );

  // Correctas: solo disponibles si se corrió el plan internamente
  const correctas = plan
    ? plan.detalle
        .filter(d => !d.requiereReclasificacion)
        .map(d => ({ uuid: d.uuid, source: d.source, periodo: d.mesCorrecto, ejercicio: d.anoCorrecto, modificada: false }))
    : [];

  return {
    aplicadoEn:       new Date().toISOString(),
    totalAnalizadas:  plan ? plan.totalAnalizadas : aReclasificar.length,
    totalCorrectas:   plan ? plan.correctas : 0,
    totalModificados: resultado.modifiedCount,
    resumen: { motivoConteo: resumenMotivos, reglaAplicada: 'Clasificación basada en InformacionGlobal.Mes del XML SAT.' },
    modificadas: aReclasificar.map(d => ({
      uuid:        d.uuid,
      source:      d.source,
      mesAnterior: d.mesAnterior ?? d.cambiosProyectados?.periodo?.antes,
      mesNuevo:    d.mesCorrecto,
      anoAnterior: d.anoAnterior ?? d.cambiosProyectados?.ejercicio?.antes,
      anoNuevo:    d.anoCorrecto,
      motivo:      d.motivo ?? 'Mes incorrecto',
    })),
    correctas,
  };
};

module.exports = { generarPlan, aplicarReclasificacion };
