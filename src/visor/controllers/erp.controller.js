'use strict';

/**
 * ERPController
 * -------------
 * Responsabilidad: orquestar el flujo de carga y consulta de CFDIs desde el ERP.
 *
 * Endpoints:
 *   POST /api/erp/cargar           — Descarga, transforma y persiste en MongoDB
 *   GET  /api/erp/facturas         — Previsualiza facturas del ERP sin persistir
 */

const { validationResult } = require('express-validator');
const { resolverPeriodo }      = require('../services/periodoFiscal.service');
const { fetchTodasLasFacturas } = require('../services/erp.service');
const { transformarTolerante }  = require('../services/erp-transformer.service');
const { upsertFromERP }         = require('../repositories/cfdi.repository');
const { asyncHandler }          = require('../../shared/middleware/error-handler');
const { logger }                = require('../../shared/utils/logger');
const CFDI                      = require('../models/CFDI');
const { parseCFDI }             = require('../services/cfdiParser');

// ─── Mapeo de tipos — compartido por ambos handlers ──────────────────────────

const TIPOS_VALIDOS = new Set(['I', 'E', 'P', 'T', 'N']);

const TIPO_DESCRIPCION = {
  I: 'INGRESO',
  E: 'EGRESO',
  P: 'PAGO',
  T: 'TRASLADO',
  N: 'NOMINA',
};

// ─── Helpers de presentación ─────────────────────────────────────────────────

/**
 * Normaliza una factura cruda del ERP a la forma que consume el frontend.
 *
 * Política: nunca lanza. Campos nulos o ausentes producen valores por defecto
 * seguros. El proceso nunca se detiene por una factura mal formada.
 *
 * @param {object} f  — Factura tal como llega del ERP
 * @returns {object}  — Factura normalizada para el frontend
 */
const normalizarFactura = (f) => {
  if (!f || typeof f !== 'object') {
    logger.warn('[ERPController] normalizarFactura recibió un valor no-objeto; se omitirá.');
    return null;
  }

  // UUID: intenta múltiples ubicaciones
  const uuid =
    f?.UUID?.toString().trim() ||
    f?.uuid?.toString().trim() ||
    f?.TimbreFiscalDigital?.UUID?.toString().trim() ||
    f?.Complemento?.TimbreFiscalDigital?.UUID?.toString().trim() ||
    '';

  // TipoComprobante: normalizar a mayúsculas para el lookup
  const tipoRaw = (f?.TipoComprobante ?? '').toString().trim().toUpperCase();
  const tipo    = TIPO_DESCRIPCION[tipoRaw] ? tipoRaw : null;

  if (!tipo) {
    logger.warn(
      `[ERPController] TipoComprobante desconocido: "${f?.TipoComprobante}" ` +
      `(UUID: "${uuid || 'sin UUID'}") — se incluye con tipo null`,
    );
  }

  return {
    id:              (f?.ID        ?? '').toString().trim() || null,
    uuid:            uuid || null,
    tieneUUID:       uuid.length > 0,
    tipo:            tipo,
    tipoDescripcion: tipo ? TIPO_DESCRIPCION[tipo] : 'DESCONOCIDO',
    fecha:           f?.FechaGeneracion ?? f?.Fecha ?? f?.FechaEmision ?? null,
    serie:           f?.Serie   ?? null,
    folio:           f?.Folio   ?? null,
    rfcEmisor:       (f?.RFCEmisor   ?? f?.RfcEmisor   ?? '').toString().trim().toUpperCase() || null,
    receptor:        (f?.NombreReceptor ?? '').toString().trim() || null,
    rfcReceptor:     (f?.RFCReceptor ?? f?.RfcReceptor ?? '').toString().trim().toUpperCase() || null,
    subtotal:        parseFloat(f?.Subtotal)   || 0,
    totalIVA:        parseFloat(f?.TotalIVA)   || 0,
    totalRetenciones: parseFloat(f?.TotalRetenciones) || 0,
    importe:         parseFloat(f?.Importe ?? f?.Total) || 0,
    moneda:          (f?.Moneda ?? 'MXN').toString().trim(),
    estatus:         f?.Estatus    ?? null,
    estatusSAT:      f?.EstatusSAT ?? null,
    tieneRelaciones: Array.isArray(f?.Relaciones) ? f.Relaciones.length > 0 : false,
  };
};

/**
 * Filtra un array de facturas normalizadas por TipoComprobante.
 * Si `tipo` es null o undefined devuelve todas.
 *
 * @param {object[]} facturas   — Facturas ya normalizadas
 * @param {string|null} tipo    — 'I' | 'E' | 'P' | 'T' | 'N' | null
 * @returns {object[]}
 */
const filtrarPorTipo = (facturas, tipo) => {
  if (!tipo) return facturas;
  return facturas.filter((f) => f.tipo === tipo);
};

// ─── Helper ──────────────────────────────────────────────────────────────────

/**
 * Convierte (ejercicio, periodo) a fechas RFC3339 para el ERP.
 * T06:00:00Z = medianoche hora México (UTC-6/CST).
 * Patrón: día 1 del mes T06:00:00Z → último día del mes T06:00:00Z.
 * Se usa el último día del mes (no el primero del siguiente) porque el ERP
 * trata fecha_fin como inclusivo para todo el día, y enviarle el día 1 del
 * mes siguiente provocaba que devolviera documentos de ese día.
 *
 * Ejemplo: ejercicio=2026, periodo=3 (marzo)
 *   → fechaInicio = "2026-03-01T06:00:00Z"
 *   → fechaFin    = "2026-03-31T06:00:00Z"
 */
const derivarFechas = (ejercicio, periodo) => {
  const mes = String(periodo).padStart(2, '0');
  // Date.UTC(año, mes, 0) = último día del mes (mes es 0-indexed, día 0 = último del anterior)
  const ultimoDia = new Date(Date.UTC(ejercicio, periodo, 0)).getUTCDate();
  return {
    fechaInicio: `${ejercicio}-${mes}-01T06:00:00Z`,
    fechaFin:    `${ejercicio}-${mes}-${String(ultimoDia).padStart(2, '0')}T06:00:00Z`,
  };
};

// ─── Enriquecimiento Complemento de Pago ─────────────────────────────────────

/**
 * Para CFDIs tipo "P" cuyo ERP no proporciona Monto explícito,
 * consulta las facturas relacionadas (TipoRelacion "08") en la BD
 * y construye el complementoPago con sus totales.
 *
 * Se llama después del upsert, de modo que también funciona si las
 * facturas relacionadas ya existían en BD de periodos anteriores.
 *
 * @param {object} doc          — documento transformado (post-upsert)
 * @param {object} facturaRaw   — objeto original del ERP (para FechaPago/FormaPago)
 */
const enrichComplementoPago = async (doc, facturaRaw) => {
  // Solo aplica si el transformer NO pudo construir el complemento
  if (doc.complementoPago) return;

  // Extraer UUIDs de relaciones tipo "08" (aplicación de pagos)
  const uuidsRelacionados = (doc.cfdiRelacionados ?? [])
    .filter((r) => r.tipoRelacion === '08')
    .flatMap((r) => r.uuids)
    .filter(Boolean);

  if (!uuidsRelacionados.length) return;

  // Buscar las facturas relacionadas en la BD (deduplicar por UUID — puede existir en ERP y SAT)
  const relacionadasRaw = await CFDI.find(
    { uuid: { $in: uuidsRelacionados } },
    { uuid: 1, total: 1, moneda: 1, serie: 1, folio: 1, source: 1 },
  ).lean();

  // Preferir ERP sobre SAT si el mismo UUID existe en ambos orígenes
  const porUUID = new Map();
  for (const r of relacionadasRaw) {
    if (!porUUID.has(r.uuid) || r.source === 'ERP') porUUID.set(r.uuid, r);
  }
  const relacionadas = [...porUUID.values()];

  if (!relacionadas.length) {
    logger.debug(
      `[ERPController] Complemento de Pago UUID ${doc.uuid}: ` +
      `${uuidsRelacionados.length} relación(es) aún no en BD, se omite enriquecimiento`,
    );
    return;
  }

  const doctosRelacionados = relacionadas.map((r) => ({
    idDocumento: r.uuid,
    serie:       r.serie  || undefined,
    folio:       r.folio  || undefined,
    monedaDR:    r.moneda || 'MXN',
    impPagado:   r.total  ?? 0,
  }));

  const monto = doctosRelacionados.reduce((s, d) => s + (d.impPagado ?? 0), 0);

  const fechaPagoRaw = facturaRaw?.FechaPago ?? facturaRaw?.FechaGeneracion ?? null;
  const fechaPago    = fechaPagoRaw ? new Date(fechaPagoRaw) : undefined;
  const formaDePagoP = facturaRaw?.FormaPago || undefined;
  const monedaP      = facturaRaw?.Moneda || 'MXN';

  const complementoPago = {
    pagos: [{
      fechaPago,
      formaDePagoP,
      monedaP,
      monto,
      doctosRelacionados,
    }],
    totales: { montoTotalPagos: monto },
  };

  await CFDI.updateOne(
    { uuid: doc.uuid, source: 'ERP' },
    { $set: { complementoPago } },
  );

  logger.debug(
    `[ERPController] Complemento de Pago UUID ${doc.uuid} enriquecido: ` +
    `monto=${monto} (${relacionadas.length}/${uuidsRelacionados.length} docs encontrados)`,
  );
};

// ─── Handler ─────────────────────────────────────────────────────────────────

/**
 * POST /api/erp/cargar
 */
const cargar = asyncHandler(async (req, res) => {
  // ── Validación del request ─────────────────────────────────────────────────
  const errors = validationResult(req);
  if (!errors.isEmpty()) return res.status(400).json({ errors: errors.array() });

  const ejercicio      = parseInt(req.body.ejercicio, 10);
  const periodo        = parseInt(req.body.periodo,   10);
  const uploadedBy     = req.user._id;
  // Filtro opcional por estatus ERP. Si viene vacío o no viene, se importan todos.
  const estatusFiltro  = Array.isArray(req.body.estatusFiltro) && req.body.estatusFiltro.length > 0
    ? new Set(req.body.estatusFiltro)
    : null;
  // Filtro opcional por tipo de comprobante. Si viene vacío o no viene, se importan todos.
  const tipoFiltro = Array.isArray(req.body.tipoFiltro) && req.body.tipoFiltro.length > 0
    ? new Set(req.body.tipoFiltro.map(t => t.toUpperCase()))
    : null;

  // ── 1. Verificar que el PeriodoFiscal exista ───────────────────────────────
  try {
    await resolverPeriodo(ejercicio, periodo);
  } catch (err) {
    return res.status(err.status || 400).json({ error: err.message });
  }

  // ── 2. Derivar fechas de inicio y fin del periodo ──────────────────────────
  const { fechaInicio, fechaFin } = derivarFechas(ejercicio, periodo);
  logger.info(
    `[ERPController] Iniciando carga | ejercicio=${ejercicio} periodo=${periodo} | ` +
    `${fechaInicio} → ${fechaFin} | usuario=${req.user.email ?? req.user._id}`,
  );

  // ── 3. Descargar facturas del ERP (manejo de paginación transparente) ──────
  let facturas;
  try {
    facturas = await fetchTodasLasFacturas({ fechaInicio, fechaFin });
  } catch (err) {
    logger.error(`[ERPController] Error al conectar con el ERP: ${err.message} (${err.code})`);
    return res.status(err.status || 502).json({
      error: err.message,
      code:  err.code ?? 'ERP_ERROR',
    });
  }

  if (facturas.length === 0) {
    logger.info(`[ERPController] El ERP no devolvió registros para ${ejercicio}/${periodo}`);
    return res.json({
      totalRecibidos: 0, nuevosInsertados: 0,
      duplicados: 0, errores: 0, detalleErrores: [],
      message: 'El ERP no devolvió registros para el periodo seleccionado.',
    });
  }

  // ── 3b. Filtrar por estatus ERP si se solicitó ─────────────────────────────
  if (estatusFiltro) {
    const antes = facturas.length;
    facturas = facturas.filter(f => estatusFiltro.has(f.Estatus ?? ''));
    logger.info(`[ERPController] Filtro estatus [${[...estatusFiltro].join(', ')}]: ${antes} → ${facturas.length} facturas`);
  }

  // ── 3c. Filtrar por tipo de comprobante si se solicitó ─────────────────────
  if (tipoFiltro) {
    const antes = facturas.length;
    facturas = facturas.filter(f => tipoFiltro.has((f.TipoComprobante ?? '').toUpperCase()));
    logger.info(`[ERPController] Filtro tipo [${[...tipoFiltro].join(', ')}]: ${antes} → ${facturas.length} facturas`);
  }

  logger.info(`[ERPController] ${facturas.length} factura(s) a procesar. Transformando y persistiendo...`);

  // ── 4 & 5. Transformar + persistir ────────────────────────────────────────
  // Política: ninguna factura se descarta. transformarTolerante nunca lanza;
  // los campos inválidos se reemplazan con fallbacks y quedan registrados en
  // el array `errores` del documento.
  let guardadas   = 0;
  let duplicadas  = 0;
  let conErrores  = 0;
  let omitidas    = 0;
  const detalleErrores = [];

  for (let i = 0; i < facturas.length; i++) {
    const factura = facturas[i];

    // ── Transformación (nunca lanza) ──────────────────────────────────────
    let doc;
    let erroresTransform = [];
    try {
      ({ doc, errores: erroresTransform } = transformarTolerante(factura, { ejercicio, periodo, uploadedBy }));
    } catch (err) {
      // No debería ocurrir, pero si ocurre no detiene el proceso.
      logger.error(`[ERPController] Error inesperado en transformarTolerante [${i + 1}]: ${err.message}`);
      conErrores++;
      detalleErrores.push({ uuid: factura?.UUID ?? null, error: err.message });
      continue;
    }

    // ── Filtrar Traslados (tipo T) — no se persisten en el sistema ────────
    // Los comprobantes de Traslado no participan en la conciliación SAT vs ERP.
    if (doc.tipoDeComprobante === 'T') {
      logger.debug(`[ERPController] [${i + 1}/${facturas.length}] Traslado omitido (tipoDeComprobante=T): UUID=${doc.uuid}`);
      omitidas++;
      continue;
    }

    // ── Persistencia ──────────────────────────────────────────────────────
    try {
      const { isNew, isDuplicate } = await upsertFromERP(doc);

      if (isDuplicate) {
        duplicadas++;
        logger.debug(`[ERPController] [${i + 1}/${facturas.length}] UUID ${doc.uuid} → DUPLICADO (índice)`);
      } else {
        isNew ? guardadas++ : duplicadas++;
        logger.debug(
          `[ERPController] [${i + 1}/${facturas.length}] UUID ${doc.uuid} → ` +
          `${isNew ? 'GUARDADA' : 'ACTUALIZADA'} | errores campo: ${erroresTransform.length}`,
        );
      }

      // Para tipo P: intentar enriquecer el complementoPago desde las facturas relacionadas
      if (doc.tipoDeComprobante === 'P') {
        await enrichComplementoPago(doc, factura);
      }
    } catch (err) {
      // Error genuino (no duplicado) — registrar y continuar
      logger.error(`[ERPController] Error al guardar UUID ${doc.uuid}: ${err.message}`);
      erroresTransform.push(`Error al guardar: ${err.message}`);
    }

    // Registrar errores de campo solo si los hubo (nunca doble conteo)
    if (erroresTransform.length > 0) {
      conErrores++;
      detalleErrores.push({ uuid: doc.uuid, errores: erroresTransform });
    }
  }

  // ── 6. Log de auditoría ────────────────────────────────────────────────────
  logger.info(
    `[ERPController] Carga completada | ejercicio=${ejercicio} periodo=${periodo} | ` +
    `recibidas=${facturas.length} guardadas=${guardadas} duplicadas=${duplicadas} omitidas=${omitidas} conErrores=${conErrores} | ` +
    `usuario=${req.user.email ?? req.user._id}`,
  );
  if (conErrores > 0) {
    logger.warn(`[ERPController] ${conErrores} factura(s) con errores:`);
    detalleErrores.slice(0, 10).forEach((e, i) =>
      logger.warn(`  [${i + 1}] UUID=${e.uuid} → ${JSON.stringify(e.errores ?? e.error)}`),
    );
    if (detalleErrores.length > 10) logger.warn(`  ... y ${detalleErrores.length - 10} más`);
  }

  // ── 7. Respuesta ───────────────────────────────────────────────────────────
  return res.json({
    procesadas:   facturas.length,
    guardadas,
    duplicadas,
    omitidas,
    conErrores,
    detalleErrores,
    message:
      `${facturas.length} facturas procesadas: ` +
      `${guardadas} guardadas, ${duplicadas} duplicadas, ${omitidas} omitidas (Traslado), ${conErrores} con errores de campo.`,
  });
});

// ─── GET /api/erp/facturas ────────────────────────────────────────────────────

/**
 * Descarga las facturas del ERP para el ejercicio y periodo indicados,
 * las normaliza para el frontend y aplica filtro por tipo si se solicita.
 *
 * NO persiste nada en MongoDB. Es un endpoint de previsualización.
 *
 * Query params:
 *   ejercicio  {number}  Requerido — Año fiscal (ej. 2026)
 *   periodo    {number}  Requerido — Mes (1–12)
 *   tipo       {string}  Opcional  — I | E | P | T | N
 *
 * Respuesta 200:
 *   {
 *     total:        number,      // facturas después de filtrar
 *     totalERP:     number,      // facturas recibidas del ERP antes de filtrar
 *     tipoFiltrado: string|null,
 *     ejercicio:    number,
 *     periodo:      number,
 *     facturas:     FacturaNormalizada[]
 *   }
 */
const previsualizar = asyncHandler(async (req, res) => {
  const errors = validationResult(req);
  if (!errors.isEmpty()) return res.status(400).json({ errors: errors.array() });

  const ejercicio = parseInt(req.query.ejercicio, 10);
  const periodo   = parseInt(req.query.periodo,   10);

  // tipo es opcional — undefined si no se envía
  const tipoRaw    = (req.query.tipo ?? '').toString().trim().toUpperCase() || null;
  const tipoFiltro = tipoRaw && TIPOS_VALIDOS.has(tipoRaw) ? tipoRaw : null;

  if (tipoRaw && !tipoFiltro) {
    return res.status(400).json({
      error: `Tipo "${req.query.tipo}" no válido. Valores aceptados: I, E, P, T, N`,
      code:  'INVALID_TIPO',
    });
  }

  // Verificar que el PeriodoFiscal exista en BD
  try {
    await resolverPeriodo(ejercicio, periodo);
  } catch (err) {
    return res.status(err.status || 400).json({ error: err.message });
  }

  const { fechaInicio, fechaFin } = derivarFechas(ejercicio, periodo);

  logger.info(
    `[ERPController] Previsualizar | ejercicio=${ejercicio} periodo=${periodo} ` +
    `tipo=${tipoFiltro ?? 'todos'} | ${fechaInicio} → ${fechaFin} | ` +
    `usuario=${req.user.email ?? req.user._id}`,
  );

  // Descargar del ERP
  let facturasRaw;
  try {
    facturasRaw = await fetchTodasLasFacturas({ fechaInicio, fechaFin });
  } catch (err) {
    logger.error(`[ERPController] Error conectando con ERP: ${err.message} (${err.code})`);
    return res.status(err.status || 502).json({
      error: err.message,
      code:  err.code ?? 'ERP_ERROR',
    });
  }

  logger.info(`[ERPController] ERP devolvió ${facturasRaw.length} factura(s)`);

  // Normalizar — las que producen null (no-objeto) se descartan
  const normalizadas = facturasRaw
    .map((f, i) => {
      const resultado = normalizarFactura(f);
      if (!resultado) {
        logger.warn(`[ERPController] Factura ${i + 1} descartada por ser no-objeto`);
      }
      return resultado;
    })
    .filter(Boolean);

  // Filtrar por tipo si se solicitó
  const filtradas = filtrarPorTipo(normalizadas, tipoFiltro);

  logger.info(
    `[ERPController] Previsualización completada | ` +
    `totalERP=${facturasRaw.length} normalizadas=${normalizadas.length} filtradas=${filtradas.length}`,
  );

  return res.json({
    total:        filtradas.length,
    totalERP:     facturasRaw.length,
    tipoFiltrado: tipoFiltro,
    tipoDescripcion: tipoFiltro ? TIPO_DESCRIPCION[tipoFiltro] : null,
    ejercicio,
    periodo,
    facturas: filtradas,
  });
});

// ─── POST /api/erp/enriquecer-pagos ──────────────────────────────────────────

/**
 * Reprocesa CFDIs tipo P sin complementoPago usando dos estrategias:
 *
 *   SAT / MANUAL / RECEPTOR  → re-parsea xmlContent almacenado en BD
 *   ERP                      → busca facturas relacionadas (TipoRelacion "08")
 *
 * Body (opcional): { ejercicio, periodo }
 *   Sin parámetros → procesa TODOS los P sin complementoPago.
 */
const enriquecerPagos = asyncHandler(async (req, res) => {
  const ejercicio = req.body.ejercicio ? parseInt(req.body.ejercicio, 10) : null;
  const periodo   = req.body.periodo   ? parseInt(req.body.periodo,   10) : null;

  const filtro = {
    tipoDeComprobante: 'P',
    complementoPago: { $exists: false },
  };
  if (ejercicio) filtro.ejercicio = ejercicio;
  if (periodo)   filtro.periodo   = periodo;

  // SAT/MANUAL: traer xmlContent (select: false en schema, necesita proyección explícita)
  const pendientes = await CFDI.find(filtro)
    .select('+xmlContent')
    .lean();

  if (!pendientes.length) {
    return res.json({ procesados: 0, enriquecidos: 0, message: 'Sin CFDIs tipo P pendientes de enriquecer.' });
  }

  logger.info(`[ERPController] enriquecerPagos: ${pendientes.length} CFDIs tipo P pendientes`);

  let enriquecidos = 0;
  let omitidos = 0;

  for (const doc of pendientes) {

    // ── Estrategia 1: re-parsear XML (SAT / MANUAL / RECEPTOR) ───────────────
    if (doc.source !== 'ERP' && doc.xmlContent) {
      try {
        const parsed = await parseCFDI(doc.xmlContent);
        if (parsed.complementoPago) {
          await CFDI.updateOne(
            { _id: doc._id },
            { $set: { complementoPago: parsed.complementoPago } },
          );
          enriquecidos++;
          continue;
        }
      } catch (err) {
        logger.warn(`[ERPController] enriquecerPagos: error re-parseando UUID ${doc.uuid}: ${err.message}`);
      }
    }

    // ── Estrategia 2: relaciones TipoRelacion "08" (ERP o SAT sin XML) ───────
    const uuidsRelacionados = (doc.cfdiRelacionados ?? [])
      .filter((r) => r.tipoRelacion === '08')
      .flatMap((r) => r.uuids)
      .filter(Boolean);

    if (!uuidsRelacionados.length) { omitidos++; continue; }

    const relacionadasRaw = await CFDI.find(
      { uuid: { $in: uuidsRelacionados } },
      { uuid: 1, total: 1, moneda: 1, serie: 1, folio: 1, source: 1 },
    ).lean();

    // Deduplicar por UUID (puede existir como ERP y SAT)
    const porUUID = new Map();
    for (const r of relacionadasRaw) {
      if (!porUUID.has(r.uuid) || r.source === 'ERP') porUUID.set(r.uuid, r);
    }
    const relacionadas = [...porUUID.values()];

    if (!relacionadas.length) { omitidos++; continue; }

    const doctosRelacionados = relacionadas.map((r) => ({
      idDocumento: r.uuid,
      serie:       r.serie  || undefined,
      folio:       r.folio  || undefined,
      monedaDR:    r.moneda || 'MXN',
      impPagado:   r.total  ?? 0,
    }));

    const monto = doctosRelacionados.reduce((s, d) => s + (d.impPagado ?? 0), 0);

    await CFDI.updateOne(
      { _id: doc._id },
      {
        $set: {
          complementoPago: {
            pagos: [{
              monedaP:      doc.moneda   || 'MXN',
              formaDePagoP: doc.formaPago || undefined,
              monto,
              doctosRelacionados,
            }],
            totales: { montoTotalPagos: monto },
          },
        },
      },
    );
    enriquecidos++;
  }

  logger.info(`[ERPController] enriquecerPagos: ${enriquecidos} enriquecidos, ${omitidos} sin datos suficientes`);

  return res.json({
    procesados:  pendientes.length,
    enriquecidos,
    omitidos,
    message: `${enriquecidos} CFDIs tipo P enriquecidos. ${omitidos} sin datos suficientes (sin XML ni relaciones en BD).`,
  });
});

module.exports = { cargar, previsualizar, enriquecerPagos };
