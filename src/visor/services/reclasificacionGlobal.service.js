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
const { obtenerDesglosesCobroAlmacen } = require('../../banks/domains/erp/erp-sync.service');
const { derivarPeriodoDesdeFecha }     = require('./periodoFiscal.service');
// ── Helpers ───────────────────────────────────────────────────────────────────

/**
 * Excepción a la regla de InformacionGlobal (2026-09-08, caso real
 * CONSTRUCASA C0-260900073, ticket C0-260806153): cuando la Global incluye
 * un ticket cobrado por 'CCE' (Cobro Contra Entrega), esa factura se
 * clasifica por su FECHA de timbrado, no por InformacionGlobal.Mes/Año — el
 * cobro real ocurre (y se concilia contra el banco) el día del timbrado, sin
 * importar el mes de las ventas que la Global consolida. Confirmado con el
 * usuario: la regla general de InformacionGlobal sigue aplicando para
 * cualquier otro origen de cobro.
 *
 * Extrae los folios de ticket (`conceptos[].noIdentificacion`) de cada CFDI
 * Global candidato, agrupa por RFC+serie (misma serie que la propia
 * factura — los tickets de una sucursal comparten su serie) y hace UNA sola
 * consulta batched a `/desgloses-cobro/almacen` por grupo, para no golpear
 * el ERP real una vez por CFDI.
 *
 * @param {Array<{uuid, serie, emisor: {rfc}, conceptos}>} cfdis
 * @returns {Promise<Set<string>>} UUIDs (mayúsculas) con al menos un cobro CCE
 */
const _detectarUuidsConCCE = async (cfdis) => {
  const uuidsConCCE = new Set();
  const grupos = new Map(); // `${rfc}|${serie}` -> { rfc, serie, folios: Set, cfdisPorFolio: Map<folio, uuid[]> }

  for (const cfdi of cfdis) {
    const rfc   = cfdi.emisor?.rfc;
    const serie = cfdi.serie;
    if (!rfc || !serie || !Array.isArray(cfdi.conceptos)) continue;
    const key = `${rfc}|${serie}`;
    if (!grupos.has(key)) grupos.set(key, { rfc, serie, folios: new Set(), cfdisPorFolio: new Map() });
    const grupo = grupos.get(key);
    for (const c of cfdi.conceptos) {
      const folio = (c.noIdentificacion ?? '').toString().trim();
      if (!folio) continue;
      grupo.folios.add(folio);
      if (!grupo.cfdisPorFolio.has(folio)) grupo.cfdisPorFolio.set(folio, []);
      grupo.cfdisPorFolio.get(folio).push(cfdi.uuid.toUpperCase());
    }
  }

  // Chunks de 100 folios por request — un batch con cientos/miles de tickets
  // (normal en un periodo completo) rebasa el límite de longitud de URL del
  // ERP real (confirmado 2026-09-08: "414 Request-URI Too Large" con ~350
  // folios en una sola llamada).
  const TAMANO_LOTE_ERP = 100;
  for (const { rfc, serie, folios, cfdisPorFolio } of grupos.values()) {
    if (folios.size === 0) continue;
    const foliosArr = [...folios];
    const cuentas = [];
    for (let i = 0; i < foliosArr.length; i += TAMANO_LOTE_ERP) {
      const lote = foliosArr.slice(i, i + TAMANO_LOTE_ERP);
      try {
        cuentas.push(...await obtenerDesglosesCobroAlmacen({ rfc, series: [serie], folios: lote }));
      } catch (err) {
        logger.warn(`[ReclasificacionGlobal] Consulta CCE falló para ${rfc}/${serie} (lote ${i}-${i + lote.length}, no crítico, se omite la excepción): ${err.message}`);
      }
    }
    for (const cuenta of cuentas) {
      const tieneCCE = (cuenta.cobros ?? []).some(c => (c.serieOrigen ?? '').toUpperCase() === 'CCE');
      if (!tieneCCE) continue;
      const folioVenta = (cuenta.folioVenta ?? '').toString().trim();
      for (const uuid of (cfdisPorFolio.get(folioVenta) ?? [])) uuidsConCCE.add(uuid);
    }
  }

  return uuidsConCCE;
};

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
 * Fuente de verdad: InformacionGlobal.Mes y InformacionGlobal.Anio — EXCEPTO
 * cuando la Global trae un ticket cobrado por 'CCE' (Cobro Contra Entrega,
 * ver `_detectarUuidsConCCE`), en cuyo caso la fuente de verdad es la fecha
 * de timbrado del propio CFDI (`cfdi.fecha`).
 * Se corrige `periodo` y `ejercicio` para que coincidan con esos valores.
 * @param {boolean} tieneCCE — true si esta Global tiene al menos un ticket
 *   cobrado por 'CCE' (ver `_detectarUuidsConCCE`).
 * @returns {object} Resultado del análisis para este CFDI.
 */
const _analizarCFDI = (cfdi, infoGlobal, tieneCCE = false) => {
  // InformacionGlobal.Mes es el mes contable al que realmente pertenece la factura global.
  // Es la fuente de verdad — se usa para corregir `periodo` y `ejercicio`.
  // Excepción CCE: se usa la fecha de timbrado en su lugar (ver comentario arriba).
  const fechaTimbrado = cfdi.fecha ? new Date(cfdi.fecha) : null;
  const derivadoDeFecha = fechaTimbrado ? derivarPeriodoDesdeFecha(fechaTimbrado) : null;
  const mesCorrecto = tieneCCE
    ? derivadoDeFecha?.periodo ?? null
    : (infoGlobal.mes  ? parseInt(infoGlobal.mes,  10) : null);
  const anoCorrecto = tieneCCE
    ? derivadoDeFecha?.ejercicio ?? null
    : (infoGlobal.anio ? parseInt(infoGlobal.anio, 10) : null);

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
      tieneCCE,
      requiereReclasificacion: false,
      motivo: tieneCCE ? 'CCE sin fecha de timbrado — omitido' : 'Sin InformacionGlobal.Mes o Anio — omitido',
      cambiosProyectados:     null,
    };
  }

  const motivos = [];
  if (mesERP !== null && mesERP !== mesCorrecto) motivos.push(tieneCCE ? 'CCE: Mes ERP no coincide con fecha de timbrado' : 'Mes ERP incorrecto');
  if (anoERP !== null && anoERP !== anoCorrecto) motivos.push(tieneCCE ? 'CCE: Ejercicio ERP no coincide con fecha de timbrado' : 'Ejercicio ERP incorrecto');

  const requiereReclasificacion = motivos.length > 0;

  return {
    _id:                    cfdi._id,
    uuid:                   cfdi.uuid,
    source:                 cfdi.source,
    subTotal:               cfdi.subTotal ?? null,
    total:                  cfdi.total ?? null,
    mesInformacionGlobal:   infoGlobal.mes  ?? null,
    anioInformacionGlobal:  infoGlobal.anio ?? null,
    mesCorrecto,
    anoCorrecto,
    mesERP,
    ejercicioERP:           anoERP,
    tieneCCE,
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

const _buildFiltro = ({ ejercicio, periodo, rfc, source } = {}) => {
  const filtro = { isActive: true };
  if (ejercicio) filtro.ejercicio = Number(ejercicio);
  if (periodo)   filtro.periodo   = Number(periodo);
  if (rfc)       filtro['emisor.rfc'] = rfc.toUpperCase().trim();
  if (source)    filtro.source    = source.toUpperCase();
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
  const mesIGVals = filtros.mesIG
    ? [String(Number(filtros.mesIG)), String(Number(filtros.mesIG)).padStart(2, '0')]
    : null;
  const conCampo = await CFDI.find({
    ...filtroBase,
    'informacionGlobal.mes': mesIGVals
      ? { $in: mesIGVals }
      : { $exists: true, $ne: null },
  }, 'uuid source fecha periodo ejercicio informacionGlobal subTotal total serie emisor.rfc conceptos.noIdentificacion').lean();

  // ── Consulta 2: CFDIs SAT sin el campo pero con xmlContent que contenga InformacionGlobal
  //    (datos existentes antes de esta actualización)
  //    No se puede filtrar por informacionGlobal.mes aquí ($exists: false), se filtra post-extracción
  const sinCampo = filtros.source === 'ERP' ? [] : await CFDI.find({
    ...filtroBase,
    'informacionGlobal.mes': { $exists: false },
    xmlContent:              { $regex: 'InformacionGlobal' },
  }).select('uuid source fecha periodo ejercicio subTotal total serie emisor.rfc conceptos.noIdentificacion +xmlContent').lean();

  const uuidsYaIncluidos = new Set(conCampo.map(c => c.uuid));
  const sinCampoFiltrado = sinCampo.filter(c => !uuidsYaIncluidos.has(c.uuid));

  logger.info(`[ReclasificacionGlobal] CFDIs con campo: ${conCampo.length}, extraídos de XML: ${sinCampoFiltrado.length}`);

  // ── Análisis ────────────────────────────────────────────────────────────────
  const detalle         = [];
  const motivoConteo    = {};
  let correctas         = 0;
  let reclasificadas    = 0;

  // Solo se consulta el ERP (CCE) para los candidatos que YA se ven
  // inconsistentes bajo la regla normal de InformacionGlobal — la inmensa
  // mayoría de CFDIs Global están correctamente clasificados y no necesitan
  // ningún dato adicional para confirmarlo. Esto evita golpear el ERP real
  // en cada sync/upload con TODOS los Global del periodo (cientos/miles).
  const _infoGlobalDe = (cfdi) => cfdi.informacionGlobal
    ?? (cfdi.xmlContent ? _extraerDeXML(cfdi.xmlContent) : null);
  const _requiereBajoReglaNormal = (cfdi, ig) => {
    if (!ig) return false;
    const mesIG = ig.mes  ? parseInt(ig.mes,  10) : null;
    const anoIG = ig.anio ? parseInt(ig.anio, 10) : null;
    if (mesIG === null || anoIG === null) return false;
    return (cfdi.periodo ?? null) !== mesIG || (cfdi.ejercicio ?? null) !== anoIG;
  };
  const candidatosParaCCE = [...conCampo, ...sinCampoFiltrado]
    .filter(cfdi => _requiereBajoReglaNormal(cfdi, _infoGlobalDe(cfdi)));
  const uuidsConCCE = await _detectarUuidsConCCE(candidatosParaCCE);
  if (uuidsConCCE.size > 0) {
    logger.info(`[ReclasificacionGlobal] ${uuidsConCCE.size} CFDI(s) con cobro CCE — se clasifican por fecha de timbrado, no por InformacionGlobal.`);
  }

  const analizar = (cfdi, infoGlobal) => {
    const tieneCCE = uuidsConCCE.has((cfdi.uuid || '').toUpperCase());
    const resultado = _analizarCFDI(cfdi, infoGlobal, tieneCCE);
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
    if (!ig) continue;
    // Si se filtró por mes, verificar que el mes extraído del XML coincida
    if (mesIGVals && !mesIGVals.includes(ig.mes)) continue;
    analizar(cfdi, ig);
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
