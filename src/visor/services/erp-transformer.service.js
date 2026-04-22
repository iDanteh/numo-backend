'use strict';

/**
 * ERPTransformerService
 * ---------------------
 * Responsabilidad única: transformar un registro de factura del ERP
 * al formato del modelo interno de CFDI.
 *
 * Decisión de diseño: función pura sin efectos secundarios.
 * No hace llamadas HTTP ni operaciones de BD.
 * Si el ERP cambia sus nombres de campo, este es el único archivo
 * que necesita modificarse.
 */

const { logger } = require('../../shared/utils/logger');

// ─── Mapeos ──────────────────────────────────────────────────────────────────

/**
 * TipoComprobante del ERP → código interno CFDI.
 *
 * ⚠️ Las claves están en minúsculas porque el lookup normaliza la entrada
 * con .toLowerCase() antes de consultar. Esto soporta 'I', 'i', 'Ingreso', etc.
 */
const TIPO_MAP = {
  i: 'I', ingreso: 'I', ingresos: 'I',
  e: 'E', egreso:  'E', egresos:  'E',
  t: 'T', traslado: 'T', traslados: 'T',
  n: 'N', nomina: 'N', 'nómina': 'N',
  p: 'P', pago: 'P', pagos: 'P',
};

/** Descripción legible por humanos — para logs y auditoría */
const TIPO_LABEL = {
  I: 'INGRESO', E: 'EGRESO', P: 'PAGO', T: 'TRASLADO', N: 'NOMINA',
};

const SAT_STATUS_VALIDOS = new Set([
  'Vigente', 'Cancelado', 'No Encontrado', 'Pendiente', 'Error', 'Deshabilitado',
]);

// ─── Normalización de estructura anidada ─────────────────────────────────────

/**
 * Algunas versiones del ERP devuelven campos anidados en sub-objetos.
 * Esta función aplana la factura a una estructura plana compatible con el
 * transformer, sin modificar el objeto original.
 *
 * Soporta:
 *   factura.DatosFinancieros.{ Importe, Subtotal, Moneda, FormaPago, MetodoPago, Impuesto }
 *   factura.DatosReceptor.{ RFC, NombreRazonSocial, UsoCFDI, CP }
 *   factura.DatosEmisor.{ RFC, Nombre, RegimenFiscal }
 */
const aplanarFactura = (factura) => {
  const df = factura?.DatosFinancieros ?? {};
  const dr = factura?.DatosReceptor   ?? {};
  const de = factura?.DatosEmisor     ?? {};

  return {
    ...factura,
    // Total: raíz > DatosFinancieros.Importe
    Total:   factura.Total   ?? df.Importe  ?? df.Total  ?? null,
    Importe: factura.Importe ?? df.Importe  ?? null,
    // SubTotal
    SubTotal: factura.SubTotal ?? factura.Subtotal ?? df.Subtotal ?? df.SubTotal ?? null,
    // Moneda / TipoCambio / FormaPago / MetodoPago
    Moneda:      factura.Moneda      ?? df.Moneda      ?? 'MXN',
    FormaPago:   factura.FormaPago   ?? df.FormaPago   ?? null,
    MetodoPago:  factura.MetodoPago  ?? df.MetodoPago  ?? null,
    // Impuestos
    TotalIVA:    factura.TotalIVA    ?? df.Impuesto    ?? df.TotalIVA    ?? null,
    // Receptor
    RFCReceptor:    factura.RFCReceptor    ?? dr.RFC               ?? null,
    NombreReceptor: factura.NombreReceptor ?? dr.NombreRazonSocial ?? null,
    UsoCfdi:        factura.UsoCfdi        ?? factura.UsoCDFI      ?? dr.UsoCFDI ?? dr.UsoCdfi ?? null,
    // Emisor (DatosEmisor tiene prioridad; ReferenciaEmisor es un ID interno, no RFC)
    RFCEmisor:    factura.RFCEmisor    ?? de.RFC    ?? null,
    NombreEmisor: factura.NombreEmisor ?? de.Nombre ?? null,
  };
};

// ─── Utilidades internas ─────────────────────────────────────────────────────

const parseNum = (v) => {
  if (v == null || v === '') return null;
  const n = parseFloat(String(v).replace(/,/g, ''));
  return isNaN(n) ? null : n;
};

const parseDate = (v) => {
  if (!v) return null;
  const d = new Date(v);
  return isNaN(d.getTime()) ? null : d;
};

// ─── Extractor de Complemento de Pago ────────────────────────────────────────

/**
 * Intenta extraer datos del Complemento de Pago de una factura ERP tipo "P".
 *
 * Rutas soportadas (en orden de prioridad):
 *   factura.ComplementoPago.Pagos[]          → estructura anidada del ERP
 *   factura.Complemento.Pagos20.Pago[]       → variante XML-like del ERP
 *   factura.Pagos[]                          → array directo de pagos
 *   factura.FechaPago + factura.MontoPago    → campos planos mínimos
 *
 * Si el ERP no envía ningún campo de pago, devuelve undefined.
 *
 * @param {object} factura — objeto aplanado de la factura ERP
 * @returns {object|undefined}
 */
const extraerComplementoPago = (factura) => {
  // ── Ruta 1: ComplementoPago.Pagos ─────────────────────────────────────────
  const cpNode = factura?.ComplementoPago ?? factura?.Complemento?.Pagos20 ?? null;
  if (cpNode) {
    const pagosRaw = cpNode.Pagos ?? cpNode.Pago ?? [];
    const pagosList = Array.isArray(pagosRaw) ? pagosRaw : [pagosRaw];
    if (pagosList.length) {
      return normalizarPagosERP(pagosList, cpNode.Version);
    }
  }

  // ── Ruta 2: factura.Pagos[] ───────────────────────────────────────────────
  if (Array.isArray(factura?.Pagos) && factura.Pagos.length) {
    return normalizarPagosERP(factura.Pagos, null);
  }

  // ── Ruta 3: campos planos mínimos (FechaPago / MontoPago / Importe) ──────
  const montoPlano = parseNum(factura?.MontoPago ?? factura?.MontoPagado ?? factura?.Importe ?? null);
  if (montoPlano !== null && montoPlano > 0) {
    return {
      pagos: [{
        fechaPago:    parseDate(factura.FechaPago) ?? undefined,
        formaDePagoP: factura.FormaPago ?? undefined,
        monedaP:      factura.Moneda ?? 'MXN',
        monto:        montoPlano,
      }],
      totales: { montoTotalPagos: montoPlano },
    };
  }

  return undefined;
};

/**
 * Normaliza un array de objetos de pago del ERP al sub-schema interno.
 */
const normalizarPagosERP = (pagosList, version) => {
  const pagos = pagosList.map((p) => {
    const drRaw = p.DoctoRelacionado ?? p.DocumentosRelacionados ?? p.Documentos ?? [];
    const drList = Array.isArray(drRaw) ? drRaw : [drRaw];

    const doctosRelacionados = drList.filter(Boolean).map((dr) => ({
      idDocumento:      (dr.IdDocumento ?? dr.UUID ?? '').toString().toUpperCase() || undefined,
      serie:            dr.Serie            || undefined,
      folio:            dr.Folio            || undefined,
      monedaDR:         dr.MonedaDR         || 'MXN',
      tipoCambioDR:     parseNum(dr.TipoCambioDR)     ?? undefined,
      metodoDePagoDR:   dr.MetodoDePagoDR   || undefined,
      numParcialidad:   parseInt(dr.NumParcialidad) || undefined,
      impSaldoAnt:      parseNum(dr.ImpSaldoAnt)      ?? undefined,
      impPagado:        parseNum(dr.ImpPagado)         ?? undefined,
      impSaldoInsoluto: parseNum(dr.ImpSaldoInsoluto)  ?? undefined,
    }));

    return {
      fechaPago:    parseDate(p.FechaPago)   ?? undefined,
      formaDePagoP: p.FormaDePagoP ?? p.FormaPago ?? undefined,
      monedaP:      p.MonedaP ?? p.Moneda  ?? 'MXN',
      tipoCambioP:  parseNum(p.TipoCambioP) ?? undefined,
      monto:        parseNum(p.Monto ?? p.MontoPago) ?? 0,
      numOperacion: p.NumOperacion || undefined,
      doctosRelacionados: doctosRelacionados.length ? doctosRelacionados : undefined,
    };
  });

  const montoTotalPagos = pagos.reduce((s, p) => s + (p.monto ?? 0), 0);

  return {
    version: version || undefined,
    pagos,
    totales: { montoTotalPagos },
  };
};

/**
 * Extrae el UUID de una factura del ERP probando múltiples ubicaciones
 * en orden de prioridad.
 *
 * Soporta:
 *   factura.UUID
 *   factura.uuid
 *   factura.TimbreFiscalDigital.UUID
 *   factura.Complemento.TimbreFiscalDigital.UUID
 *
 * @param {object} factura
 * @returns {string} UUID en mayúsculas, o cadena vacía si no se encontró
 */
const extraerUUID = (factura) => {
  const candidatos = [
    factura?.UUID,
    factura?.uuid,
    factura?.TimbreFiscalDigital?.UUID,
    factura?.Complemento?.TimbreFiscalDigital?.UUID,
  ];

  for (const candidato of candidatos) {
    const valor = (candidato ?? '').toString().trim().toUpperCase();
    if (valor) return valor;
  }

  return '';
};

/**
 * Resuelve el TipoComprobante a su código interno.
 * Devuelve { tipo, warn } donde warn es un mensaje o null.
 *
 * @param {*} raw  — Valor crudo del ERP
 * @returns {{ tipo: string|null, warn: string|null }}
 */
const resolverTipo = (raw) => {
  const key  = (raw ?? '').toString().trim().toLowerCase();
  const tipo = TIPO_MAP[key] ?? null;

  if (!tipo) {
    return {
      tipo: null,
      warn: `TipoComprobante no reconocido: "${raw}" — factura omitida`,
    };
  }

  return { tipo, warn: null };
};

// ─── Transformador principal ──────────────────────────────────────────────────

/**
 * Transforma una factura del ERP al documento interno de CFDI.
 *
 * Política de errores:
 *  - UUID ausente o Fecha inválida → lanza (no hay forma de identificar el registro)
 *  - TipoComprobante desconocido   → lanza (el modelo requiere un tipo válido)
 *  - Total inválido                → lanza (dato financiero crítico)
 *  - RFCReceptor ausente           → lanza (identidad del receptor requerida)
 *  - Campos opcionales inválidos   → se ignoran con valor por defecto
 *
 * @param {object} factura   — Objeto tal como lo devuelve el ERP
 * @param {object} ctx
 * @param {number} ctx.ejercicio    — Año fiscal seleccionado por el usuario
 * @param {number} ctx.periodo      — Mes fiscal (1–12)
 * @param {string} ctx.uploadedBy   — ObjectId del usuario que inició la carga
 * @returns {object} Documento listo para pasar al repositorio
 * @throws {Error} Si un campo obligatorio es inválido o está ausente
 */
const transformar = (factura, { ejercicio, periodo, uploadedBy }) => {
  factura = aplanarFactura(factura);

  // ── UUID ───────────────────────────────────────────────────────────────────
  // Intenta múltiples ubicaciones antes de descartar la factura.
  const uuid = extraerUUID(factura);
  if (!uuid) {
    logger.warn('[ERPTransformer] Factura descartada: UUID vacío en todas las ubicaciones conocidas', {
      campos: { UUID: factura?.UUID, uuid: factura?.uuid,
                tfd: factura?.TimbreFiscalDigital?.UUID,
                complemento: factura?.Complemento?.TimbreFiscalDigital?.UUID },
    });
    throw new Error('UUID vacío o ausente');
  }

  // ── TipoComprobante ────────────────────────────────────────────────────────
  // Un tipo desconocido se registra como warning y detiene esta factura,
  // pero el loop del controlador continúa con la siguiente.
  const { tipo, warn: tipoWarn } = resolverTipo(factura.TipoComprobante);
  if (!tipo) {
    logger.warn(`[ERPTransformer] UUID ${uuid} — ${tipoWarn}`);
    throw new Error(tipoWarn);
  }
  logger.debug(`[ERPTransformer] UUID ${uuid} → tipo ${tipo} (${TIPO_LABEL[tipo]})`);

  // ── Fecha ──────────────────────────────────────────────────────────────────
  // El ERP puede devolver el campo con distintos nombres según la versión.
  const fechaRaw = factura.Fecha ?? factura.FechaGeneracion ?? factura.FechaEmision ?? null;
  const fecha    = parseDate(fechaRaw);
  if (!fecha) {
    logger.warn(`[ERPTransformer] UUID ${uuid} — Fecha inválida: "${fechaRaw}"`);
    throw new Error(`Fecha inválida: "${fechaRaw}"`);
  }

  // ── Total ──────────────────────────────────────────────────────────────────
  const totalRaw = factura.Total ?? factura.Importe ?? null;
  const total    = parseNum(totalRaw);
  if (total === null) {
    logger.warn(`[ERPTransformer] UUID ${uuid} — Total inválido: "${totalRaw}"`);
    throw new Error(`Total inválido: "${totalRaw}"`);
  }

  // ── RFCReceptor ────────────────────────────────────────────────────────────
  const rfcReceptor = (factura.RFCReceptor ?? factura.RfcReceptor ?? '').toString().trim().toUpperCase();
  if (!rfcReceptor) {
    logger.warn(`[ERPTransformer] UUID ${uuid} — RFCReceptor vacío o ausente`);
    throw new Error('RFCReceptor vacío o ausente');
  }

  // ── Campos opcionales ──────────────────────────────────────────────────────
  const rfcEmisor  = (factura.RFCEmisor ?? factura.RfcEmisor ?? 'DESCONOCIDO').toString().trim().toUpperCase();
  const subTotal   = parseNum(factura.SubTotal ?? factura.Subtotal) ?? total;
  const tipoCambio = parseNum(factura.TipoCambio) ?? 1;

  const totalIVA         = parseNum(factura.TotalIVA ?? factura.TotalImpuestosTrasladados) ?? 0;
  const totalRetenciones = 0; // TotalRetenciones del ERP duplica TotalIVA, no es retención real

  const satStatusRaw = factura.EstatusSAT ?? null;
  const satStatus    = SAT_STATUS_VALIDOS.has(satStatusRaw) ? satStatusRaw : null;
  const erpStatus    = factura.Estatus ?? null;

  // ── Relaciones CFDI ────────────────────────────────────────────────────────
  const cfdiRelacionados = [];
  if (Array.isArray(factura.relaciones)) {
    for (const rel of factura.relaciones) {
      const uuidRel = (rel.UUID ?? '').toString().trim().toUpperCase();
      if (uuidRel) {
        cfdiRelacionados.push({
          tipoRelacion: (rel.TipoRelacion ?? '04').toString().trim(),
          uuids:        [uuidRel],
        });
      }
    }
  }

  // ── Timbre Fiscal Digital ──────────────────────────────────────────────────
  // Puede llegar en TimbreFiscalDigital, dentro de Complemento, o directamente
  // en la raíz (formato plano del ERP de pagos).
  const tfd   = factura.TimbreFiscalDigital ?? factura.Complemento?.TimbreFiscalDigital ?? {};
  const timbre = {};
  const _ft = tfd.FechaTimbrado    ?? factura.FechaTimbrado;    if (_ft)  timbre.fechaTimbrado    = parseDate(_ft);
  const _ss = tfd.SelloSAT         ?? factura.SelloSAT;         if (_ss)  timbre.selloSAT         = _ss;
  const _sc = tfd.SelloCFD         ?? factura.SelloCFD;         if (_sc)  timbre.selloCFD         = _sc;
  const _nc = tfd.NoCertificadoSAT ?? factura.NoCertificadoSAT; if (_nc)  timbre.noCertificadoSAT = String(_nc);
  const _rp = tfd.RfcProvCertif    ?? factura.RfcProvCertif;    if (_rp)  timbre.rfcProvCertif    = _rp;
  if (tfd.Version) timbre.version = tfd.Version;

  // ── Documento final ────────────────────────────────────────────────────────
  logger.debug(`[ERPTransformer] UUID ${uuid} — insertando: tipo=${tipo} total=${total} fecha=${fecha.toISOString()} rfcReceptor=${rfcReceptor}`);

  const cfdiDoc = {
    uuid,
    source:            'ERP',
    ejercicio,
    periodo,
    fecha,
    tipoDeComprobante: tipo,
    total,
    subTotal,
    moneda:    factura.Moneda || 'MXN',
    tipoCambio,
    emisor:    { rfc: rfcEmisor, nombre: factura.NombreEmisor || undefined },
    receptor:  {
      rfc:     rfcReceptor,
      nombre:  factura.NombreReceptor || undefined,
      usoCFDI: factura.UsoCfdi        || undefined,
    },
    impuestos: {
      totalImpuestosTrasladados: totalIVA,
      totalImpuestosRetenidos:   totalRetenciones,
    },
    uploadedBy,
  };

  if (factura.Serie)              cfdiDoc.serie              = factura.Serie;
  if (factura.Folio)              cfdiDoc.folio              = factura.Folio;
  if (factura.FormaPago)          cfdiDoc.formaPago          = factura.FormaPago;
  if (factura.MetodoPago)         cfdiDoc.metodoPago         = factura.MetodoPago;
  // SelloCFD: buscar primero top-level, luego dentro del TFD
  const selloCFD = factura.SelloCFD ?? tfd.SelloCFD ?? null;
  if (selloCFD)                   cfdiDoc.sello              = selloCFD;
  if (factura.NoCertificado)      cfdiDoc.noCertificado      = String(factura.NoCertificado);
  if (factura.ID)                 cfdiDoc.erpId              = String(factura.ID);
  if (satStatus)                  cfdiDoc.satStatus          = satStatus;
  if (erpStatus)                  cfdiDoc.erpStatus          = erpStatus;
  if (cfdiRelacionados.length)    cfdiDoc.cfdiRelacionados   = cfdiRelacionados;
  if (Object.keys(timbre).length) cfdiDoc.timbreFiscalDigital = timbre;
  // Versión CFDI: desde el TFD si está disponible
  if (tfd.Version && ['3.3', '4.0'].includes(tfd.Version)) cfdiDoc.version = tfd.Version;

  return cfdiDoc;
};

// ─── Transformador tolerante ──────────────────────────────────────────────────

/**
 * Igual que `transformar`, pero NUNCA lanza.
 *
 * Política de errores:
 *   - Cada campo inválido o ausente se registra en `errores[]` y se usa un
 *     valor de fallback seguro — el proceso continúa sin interrupciones.
 *   - UUID ausente → se genera un identificador sintético determinístico
 *     a partir de RFCEmisor + Serie + Folio + Total. Mismo ERP + mismos
 *     campos = mismo UUID sintético → deduplicación correcta en upsert.
 *   - TipoComprobante desconocido → se almacena null (el schema lo permite).
 *
 * @param {object} factura   — Objeto tal como lo devuelve el ERP
 * @param {object} ctx
 * @param {number} ctx.ejercicio
 * @param {number} ctx.periodo
 * @param {string} ctx.uploadedBy
 * @returns {{ doc: object, errores: string[] }}
 *   doc     → documento listo para el repositorio (siempre presente)
 *   errores → lista de advertencias (vacía si todo llegó correcto)
 */
const transformarTolerante = (factura, { ejercicio, periodo, uploadedBy }) => {
  factura = aplanarFactura(factura);
  const errores = [];

  // ── UUID ───────────────────────────────────────────────────────────────────
  let uuid        = extraerUUID(factura);
  let uuidGenerado = false;

  if (!uuid) {
    errores.push('UUID ausente — se generó un identificador sintético');
    uuidGenerado = true;

    // UUID sintético determinístico: misma factura → mismo UUID en cada carga
    const rfcE    = (factura?.RFCEmisor ?? factura?.RfcEmisor ?? 'DESCONOCIDO').toString().trim().toUpperCase();
    const serie   = (factura?.Serie  ?? 'S').toString().trim().toUpperCase();
    const folio   = (factura?.Folio  ?? '0').toString().trim();
    const importe = parseNum(factura?.Total ?? factura?.Importe) ?? 0;
    uuid = `SINUUID-${rfcE}-${serie}-${folio}-${importe}`;
  }

  // ── TipoComprobante ────────────────────────────────────────────────────────
  const { tipo, warn: tipoWarn } = resolverTipo(factura?.TipoComprobante);
  if (!tipo) {
    errores.push(`TipoComprobante no reconocido: "${factura?.TipoComprobante}" — almacenado como null`);
    logger.warn(`[ERPTransformer] UUID ${uuid} — ${tipoWarn}`);
  }

  // ── Fecha ──────────────────────────────────────────────────────────────────
  const fechaRaw = factura?.Fecha ?? factura?.FechaGeneracion ?? factura?.FechaEmision ?? null;
  let fecha      = parseDate(fechaRaw);
  if (!fecha) {
    errores.push(`Fecha inválida: "${fechaRaw}" — se usó la fecha actual`);
    fecha = new Date();
  }

  // ── Total / Importe ────────────────────────────────────────────────────────
  const totalRaw = factura?.Total ?? factura?.Importe ?? null;
  let total      = parseNum(totalRaw);
  if (total === null) {
    errores.push(`Total/Importe inválido: "${totalRaw}" — se usó 0`);
    total = 0;
  }

  // ── RFCs ───────────────────────────────────────────────────────────────────
  const rfcReceptor = (factura?.RFCReceptor ?? factura?.RfcReceptor ?? '').toString().trim().toUpperCase() || 'DESCONOCIDO';
  if (rfcReceptor === 'DESCONOCIDO') errores.push('RFCReceptor ausente — se usó "DESCONOCIDO"');

  const rfcEmisor = (factura?.RFCEmisor ?? factura?.RfcEmisor ?? '').toString().trim().toUpperCase() || 'DESCONOCIDO';
  if (rfcEmisor === 'DESCONOCIDO') errores.push('RFCEmisor ausente — se usó "DESCONOCIDO"');

  // ── Campos opcionales ──────────────────────────────────────────────────────
  const subTotal         = parseNum(factura?.SubTotal ?? factura?.Subtotal) ?? total;
  const tipoCambio       = parseNum(factura?.TipoCambio) ?? 1;
  const totalIVA         = parseNum(factura?.TotalIVA ?? factura?.TotalImpuestosTrasladados) ?? 0;
  const totalRetenciones = 0; // TotalRetenciones del ERP duplica TotalIVA, no es retención real
  const satStatusRaw     = factura?.EstatusSAT ?? null;
  const satStatus        = SAT_STATUS_VALIDOS.has(satStatusRaw) ? satStatusRaw : null;
  const erpStatus        = factura?.Estatus ?? null;

  // ── Relaciones ─────────────────────────────────────────────────────────────
  const cfdiRelacionados = [];
  const relaciones = factura?.Relaciones ?? factura?.relaciones;
  if (Array.isArray(relaciones)) {
    for (const rel of relaciones) {
      const uuidRel = (rel?.UUID ?? '').toString().trim().toUpperCase();
      if (uuidRel) {
        cfdiRelacionados.push({
          tipoRelacion: (rel?.TipoRelacion ?? '04').toString().trim(),
          uuids: [uuidRel],
        });
      }
    }
  }

  // ── Timbre Fiscal Digital ──────────────────────────────────────────────────
  // Puede llegar en TimbreFiscalDigital, dentro de Complemento, o directamente
  // en la raíz (formato plano del ERP de pagos).
  const tfd    = factura?.TimbreFiscalDigital ?? factura?.Complemento?.TimbreFiscalDigital ?? {};
  const timbre = {};
  const _ft = tfd?.FechaTimbrado    ?? factura?.FechaTimbrado;    if (_ft)  timbre.fechaTimbrado    = parseDate(_ft);
  const _ss = tfd?.SelloSAT         ?? factura?.SelloSAT;         if (_ss)  timbre.selloSAT         = _ss;
  const _sc = tfd?.SelloCFD         ?? factura?.SelloCFD;         if (_sc)  timbre.selloCFD         = _sc;
  const _nc = tfd?.NoCertificadoSAT ?? factura?.NoCertificadoSAT; if (_nc)  timbre.noCertificadoSAT = String(_nc);
  const _rp = tfd?.RfcProvCertif    ?? factura?.RfcProvCertif;    if (_rp)  timbre.rfcProvCertif    = _rp;
  if (tfd?.Version) timbre.version = tfd.Version;

  // ── Documento final ────────────────────────────────────────────────────────
  if (errores.length > 0) {
    logger.debug(`[ERPTransformer] UUID ${uuid} guardado con ${errores.length} error(es): ${errores.join(' | ')}`);
  }

  const doc = {
    uuid,
    uuidGenerado,
    tieneErrores: errores.length > 0,
    errores,
    source:            'ERP',
    ejercicio,
    periodo,
    fecha,
    tipoDeComprobante: tipo ?? null,
    total,
    subTotal,
    moneda:    factura?.Moneda || 'MXN',
    tipoCambio,
    emisor:   { rfc: rfcEmisor, nombre: factura?.NombreEmisor || undefined },
    receptor: {
      rfc:     rfcReceptor,
      nombre:  factura?.NombreReceptor || undefined,
      usoCFDI: factura?.UsoCfdi        || undefined,
    },
    impuestos: {
      totalImpuestosTrasladados: totalIVA,
      totalImpuestosRetenidos:   totalRetenciones,
    },
    uploadedBy,
  };

  if (factura?.Serie)          doc.serie         = factura.Serie;
  if (factura?.Folio)          doc.folio         = factura.Folio;
  if (factura?.FormaPago)      doc.formaPago     = factura.FormaPago;
  if (factura?.MetodoPago)     doc.metodoPago    = factura.MetodoPago;
  // SelloCFD: buscar primero top-level, luego dentro del TFD
  const selloCFD = factura?.SelloCFD ?? tfd?.SelloCFD ?? null;
  if (selloCFD)                doc.sello         = selloCFD;
  if (factura?.NoCertificado)  doc.noCertificado = String(factura.NoCertificado);
  if (factura?.ID)             doc.erpId         = String(factura.ID);
  if (satStatus)               doc.satStatus     = satStatus;
  if (erpStatus)               doc.erpStatus     = erpStatus;
  if (cfdiRelacionados.length) doc.cfdiRelacionados    = cfdiRelacionados;
  if (Object.keys(timbre).length) doc.timbreFiscalDigital = timbre;
  const complementoPago = extraerComplementoPago(factura);
  if (complementoPago) doc.complementoPago = complementoPago;
  // Versión CFDI: desde el TFD si está disponible
  if (tfd?.Version && ['3.3', '4.0'].includes(tfd.Version)) doc.version = tfd.Version;

  // InformacionGlobal (Factura Global)
  const ig = factura?.InformacionGlobal ?? null;
  if (ig) {
    doc.informacionGlobal = {
      periodicidad: ig.Periodicidad || null,
      mes:          ig.Mes || ig.Meses || null,
      anio:         ig.Anio || ig['Año'] || ig.Ano || null,
    };
  }

  return { doc, errores };
};

module.exports = { transformar, transformarTolerante, extraerUUID, resolverTipo };
