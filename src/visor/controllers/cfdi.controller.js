const { validationResult } = require('express-validator');
const AdmZip = require('adm-zip');
const ExcelJS = require('exceljs');
const axios = require('axios');
const CFDI = require('../models/CFDI');
const { parseCFDI } = require('../services/cfdiParser');
const { compareCFDI } = require('../services/comparisonEngine');
const { verifyCFDIWithSAT } = require('../services/satVerification');
const { asyncHandler } = require('../../shared/middleware/error-handler');
const { normalizeSource } = require('../utils/validators');
const { paginate, skip } = require('../utils/pagination');
const { generarPlan, aplicarReclasificacion } = require('../services/reclasificacionGlobal.service');
const Comparison = require('../models/Comparison');
const { logger } = require('../../shared/utils/logger');

const TIPOS_COMPROBANTE = {
  ingreso: 'I', egreso: 'E', traslado: 'T',
  nómina: 'N', nomina: 'N', pago: 'P',
  i: 'I', e: 'E', t: 'T', n: 'N', p: 'P',
};

const SAT_STATUS_VALIDOS = new Set([
  'Vigente', 'Cancelado', 'No Encontrado',
  'Pendiente', 'Error', 'Expresión Inválida', 'Desconocido',
]);

/**
 * Verifica el estado SAT de un CFDI en background (fire-and-forget).
 * Actualiza todos los documentos con ese UUID (ERP + SAT) para mantener consistencia.
 */
const verificarSATBackground = (cfdiData) => {
  const rfcEmisor = cfdiData.emisor?.rfc || '';
  const rfcReceptor = cfdiData.receptor?.rfc || '';
  if (!/^[A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3}$/i.test(rfcEmisor)) return;

  const totalParaSAT = cfdiData.tipoDeComprobante === 'P' ? 0 : cfdiData.total;
  verifyCFDIWithSAT(
    cfdiData.uuid,
    rfcEmisor,
    rfcReceptor,
    totalParaSAT,
    cfdiData.timbreFiscalDigital?.selloCFD || cfdiData.sello || '',
    cfdiData.version || '4.0',
  ).then(satResponse => {
    const estado = SAT_STATUS_VALIDOS.has(satResponse.state) ? satResponse.state : 'Error';
    return CFDI.updateMany(
      { uuid: cfdiData.uuid.toUpperCase() },
      { $set: { satStatus: estado, satLastCheck: new Date() } },
    );
  }).catch(() => { /* best-effort, no bloquea */ });
};

// ── Helpers internos ──────────────────────────────────────────────────────────

/**
 * Extrae entradas XML de un archivo (directo o dentro de un ZIP).
 * @param {{ originalname: string, buffer: Buffer }} file
 * @returns {{ name: string, buffer: Buffer|null, error?: string }[]}
 */
const extractEntries = (file) => {
  if (file.originalname.toLowerCase().endsWith('.zip')) {
    try {
      const zip = new AdmZip(file.buffer);
      const xmlEntries = zip.getEntries()
        .filter(e => e.entryName.toLowerCase().endsWith('.xml') && !e.isDirectory);
      if (xmlEntries.length === 0) {
        return [{ name: file.originalname, buffer: null, error: 'El ZIP no contiene archivos XML' }];
      }
      return xmlEntries.map(e => ({ name: e.entryName, buffer: e.getData() }));
    } catch (err) {
      return [{ name: file.originalname, buffer: null, error: `Error al descomprimir ZIP: ${err.message}` }];
    }
  }
  return [{ name: file.originalname, buffer: file.buffer }];
};

// ── Controladores ─────────────────────────────────────────────────────────────

/**
 * GET /api/cfdis
 */
const list = asyncHandler(async (req, res) => {
  const {
    page = 1, limit = 20, source, tipoDeComprobante,
    rfcEmisor, rfcReceptor, satStatus, erpStatus, lastComparisonStatus,
    fechaInicio, fechaFin, search, uuids, uuid,
    ejercicio, periodo,
  } = req.query;

  const pg = parseInt(page);
  const lm = parseInt(limit);

  const filter = { isActive: true };
  if (uuids) {
    const uuidList = uuids.split(',').map(u => u.trim().toUpperCase()).filter(Boolean);
    if (uuidList.length) filter.uuid = { $in: uuidList };
  }
  if (uuid) filter.uuid = { $regex: uuid.trim(), $options: 'i' };
  if (source) {
    const sources = source.toUpperCase().split(',').map(s => s.trim()).filter(Boolean);
    filter.source = sources.length === 1 ? sources[0] : { $in: sources };
  }
  if (tipoDeComprobante)  filter.tipoDeComprobante   = tipoDeComprobante;
  if (rfcEmisor)          filter['emisor.rfc']        = rfcEmisor.toUpperCase();
  if (rfcReceptor)        filter['receptor.rfc']      = rfcReceptor.toUpperCase();
  if (satStatus)          filter.satStatus            = satStatus;
  if (erpStatus)          filter.erpStatus            = erpStatus;
  if (fechaInicio || fechaFin) {
    filter.fecha = {};
    if (fechaInicio) {
      const d = fechaInicio.split('T')[0];
      filter.fecha.$gte = new Date(`${d}T06:00:00Z`);
    }
    if (fechaFin) {
      const d   = fechaFin.split('T')[0];
      const fin = new Date(`${d}T06:00:00Z`);
      fin.setUTCDate(fin.getUTCDate() + 1); // siguiente día a las 06:00Z = excluye el día siguiente
      filter.fecha.$lt = fin;
    }
  }
  const ej = ejercicio ? parseInt(ejercicio) : null;
  const pe = periodo   ? parseInt(periodo)   : null;
  if (ej) filter.ejercicio = ej;
  if (pe) filter.periodo   = pe;

  if (lastComparisonStatus) {
    if (lastComparisonStatus === 'migrar') {
      // 1. Buscar SAT/MANUAL con 'match' o 'not_in_erp' (filtrar por periodo/ejercicio si el usuario los seleccionó)
      const candidateSatCfdis = await CFDI.find(
        { source: { $in: ['SAT', 'MANUAL'] }, lastComparisonStatus: { $in: ['match', 'not_in_erp'] }, isActive: true,
          ...(ej ? { ejercicio: ej } : {}), ...(pe ? { periodo: pe } : {}) },
        'uuid _id ejercicio periodo lastComparisonStatus'
      ).lean();

      // 2. Buscar contrapartes ERP por UUID SIN filtro de periodo/ejercicio.
      //    Así detectamos cross-period aunque el usuario no tenga un periodo seleccionado,
      //    comparando directamente los campos ejercicio/periodo de cada par SAT↔ERP.
      let crossPeriodIds = [];
      if (candidateSatCfdis.length > 0) {
        const uuids = candidateSatCfdis.map(c => c.uuid);
        const erpCounterparts = await CFDI.find(
          { source: 'ERP', uuid: { $in: uuids }, isActive: true },
          'uuid ejercicio periodo'
        ).lean();

        // Mapa uuid → { ejercicio, periodo } del ERP
        const erpMap = new Map();
        for (const erp of erpCounterparts) {
          erpMap.set(erp.uuid.toUpperCase(), erp);
        }

        crossPeriodIds = candidateSatCfdis
          .filter(sat => {
            const erp = erpMap.get(sat.uuid.toUpperCase());
            if (!erp) return sat.lastComparisonStatus === 'match'; // Sin contraparte ERP (status 'match' obsoleto)
            return erp.ejercicio !== sat.ejercicio || erp.periodo !== sat.periodo;
          })
          .map(c => c._id);
      }

      // 3. $or: factura global SAT not_in_erp ó match/not_in_erp cross-period
      // Para not_in_erp sin InformacionGlobal también incluimos los que tienen
      // contraparte ERP en otro periodo (subidos al mes equivocado).
      // Para match con InformacionGlobal también incluimos: factura global conciliada
      // en el mes de emisión pero que debe moverse al mes que cubre (informacionGlobal.mes).
      filter.source = { $in: ['SAT', 'MANUAL'] };
      filter.$or = [
        {
          lastComparisonStatus: { $in: ['not_in_erp', 'match'] },
          'informacionGlobal.mes': { $exists: true, $ne: null },
        },
        ...(crossPeriodIds.length ? [{ _id: { $in: crossPeriodIds } }] : []),
      ];
    } else {
      filter.lastComparisonStatus = lastComparisonStatus;
    }
  }
  if (search) filter.$text = { $search: search };

  const [cfdis, total] = await Promise.all([
    CFDI.find(filter, { xmlContent: 0 })
      .sort({ fecha: -1 })
      .skip(skip(pg, lm))
      .limit(lm)
      .lean(),
    CFDI.countDocuments(filter),
  ]);

  res.json(paginate(cfdis, total, pg, lm));
});

/**
 * GET /api/cfdis/:id
 */
const getById = asyncHandler(async (req, res) => {
  const cfdi = await CFDI.findById(req.params.id, { xmlContent: 0 });
  if (!cfdi) return res.status(404).json({ error: 'CFDI no encontrado' });
  res.json(cfdi);
});

/**
 * GET /api/cfdis/:id/xml
 */
const getXml = asyncHandler(async (req, res) => {
  const cfdi = await CFDI.findById(req.params.id).select('+xmlContent');
  if (!cfdi || !cfdi.xmlContent) return res.status(404).json({ error: 'XML no disponible' });
  res.setHeader('Content-Type', 'application/xml');
  res.setHeader('Content-Disposition', `attachment; filename="${cfdi.uuid}.xml"`);
  res.send(cfdi.xmlContent);
});

/**
 * POST /api/cfdis/upload
 */
const upload = asyncHandler(async (req, res) => {
  if (!req.files?.length) return res.status(400).json({ error: 'No se enviaron archivos' });

  const source = normalizeSource(req.body.source);
  const ejercicio = req.body.ejercicio ? parseInt(req.body.ejercicio) : undefined;
  const periodo   = req.body.periodo   ? parseInt(req.body.periodo)   : undefined;
  let nuevos = 0, actualizados = 0;
  const success = [], failed = [], duplicados = [];

  for (const file of req.files) {
    for (const entry of extractEntries(file)) {
      if (!entry.buffer) {
        failed.push({ filename: entry.name, error: entry.error });
        continue;
      }
      try {
        const cfdiData = await parseCFDI(entry.buffer.toString('utf8'));
        // CFDIs de SAT/MANUAL existen en SAT por definición; marcar Vigente hasta verificación formal
        if (['SAT', 'MANUAL'].includes(source) && !cfdiData.satStatus) {
          cfdiData.satStatus = 'Vigente';
        }
        if (ejercicio) cfdiData.ejercicio = ejercicio;
        if (periodo)   cfdiData.periodo   = periodo;
        const prev = await CFDI.findOneAndUpdate(
          { uuid: cfdiData.uuid, source },
          { ...cfdiData, source, uploadedBy: req.user._id },
          { upsert: true, new: false, setDefaultsOnInsert: true },
        );
        prev === null ? nuevos++ : actualizados++;
        if (prev !== null) duplicados.push({ uuid: cfdiData.uuid, filename: entry.name });
        success.push({
          uuid: cfdiData.uuid,
          filename: entry.name,
          nuevo: prev === null,
          satStatus: prev?.satStatus ?? null,
          lastComparisonStatus: prev?.lastComparisonStatus ?? null,
        });
        // Verificar estado SAT en background para todos los archivos subidos
        verificarSATBackground(cfdiData);
      } catch (err) {
        failed.push({ filename: entry.name, error: err.message });
      }
    }
  }

  const procesados = nuevos + actualizados;
  res.status(207).json({
    message: `${procesados} CFDIs procesados (${nuevos} nuevos, ${actualizados} duplicados), ${failed.length} con error`,
    procesados, nuevos, actualizados, duplicados,
    errores: failed, success, failed,
  });

  // Reclasificación automática en background para facturas globales SAT/MANUAL
  if (['SAT', 'MANUAL'].includes(source) && ejercicio && procesados > 0) {
    setImmediate(() => {
      aplicarReclasificacion({ ejercicio, source })
        .catch(err => logger.warn(`[upload] reclasificación background falló: ${err.message}`));
    });
  }
});

/**
 * POST /api/cfdis/import-excel
 *
 * Soporta el formato de reporte SAT con columnas:
 *   Verificado ó Asoc. | Estado SAT | Version | Tipo | Fecha Emision | Fecha Timbrado |
 *   EstadoPago | FechaPago | Serie | Folio | UUID | TipoRelacion | UUID Relacion |
 *   RFC Emisor | Nombre Emisor | LugarDeExpedicion | RFC Receptor | Nombre Receptor |
 *   ResidenciaFiscal | NumRegIdTrib | UsoCFDI | SubTotal | Descuento | Total IEPS |
 *   IVA 16% | Retenido IVA | Retenido ISR | ISH | Total | Complemento | Moneda |
 *   Tipo De Cambio | FormaDePago | Metodo de Pago | NumCtaPago | Conceptos |
 *   Combustible | Archivo XML | Direccion Emisor | Localidad Emisor |
 *   Direccion Receptor | Localidad Receptor | IVA 8% | IEPS 30.4% | IVA Ret 6% |
 *   RegimenFiscalReceptor | DomicilioFiscalReceptor
 *
 * Fechas aceptadas: DD/MM/YYYY, DD/MM/YYYY HH:MM, YYYY-MM-DD, ISO 8601
 */
const importExcel = asyncHandler(async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No se envió ningún archivo Excel' });

  const { ejercicio, periodo } = req.body;
  if (!ejercicio || !periodo) {
    return res.status(400).json({
      error: 'El ejercicio y periodo son obligatorios',
      code:  'PERIODO_REQUERIDO',
    });
  }
  const ejercicioNum = parseInt(ejercicio);
  const periodoNum   = parseInt(periodo);
  if (periodoNum < 1 || periodoNum > 12) {
    return res.status(400).json({
      error: 'El periodo debe ser entre 1 y 12',
      code:  'PERIODO_INVALIDO',
    });
  }

  const source = normalizeSource(req.body.source);

  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.load(req.file.buffer);
  const sheet = workbook.worksheets[0];
  if (!sheet) return res.status(400).json({ error: 'El archivo Excel no contiene hojas' });

  // ── Mapeo de encabezados (insensible a espacios extras) ───────────────────
  const headers = {};
  sheet.getRow(1).eachCell((cell, col) => {
    if (cell.value != null) {
      const key = cell.value.toString().trim();
      if (key) headers[key] = col;
    }
  });

  /**
   * Obtiene el valor de una celda buscando por múltiples nombres de columna.
   * Maneja celdas normales, fórmulas y rich-text de ExcelJS.
   * Devuelve el primer valor no-nulo encontrado, o null.
   */
  const getCell = (row, ...names) => {
    for (const name of names) {
      const c = headers[name];
      if (!c) continue;
      let v = row.getCell(c).value;
      if (v === null || v === undefined) continue;

      // Celda con fórmula: ExcelJS devuelve { formula, result } o { sharedFormula, result }
      if (typeof v === 'object' && v !== null && !Array.isArray(v) && !(v instanceof Date)) {
        if ('result' in v) v = v.result;          // fórmula calculada
        else if ('richText' in v) {               // rich text
          v = v.richText.map(r => r.text ?? '').join('');
        }
      }

      if (v === null || v === undefined) continue;
      if (v instanceof Date) return isNaN(v.getTime()) ? null : v;
      const s = v.toString().trim();
      if (s) return s;
    }
    return null;
  };

  /**
   * Extrae solo el código del catálogo SAT eliminando la descripción.
   * "PPD-Pago en parcialidades" → "PPD"
   * "99-Por definir"            → "99"
   * "01-Efectivo"               → "01"
   * "PPD"                       → "PPD"  (sin cambio)
   */
  const extraerCodigo = (val) => {
    if (!val) return undefined;
    return val.toString().trim().split('-')[0].trim() || undefined;
  };

  /**
   * Parser de fechas tolerante. Acepta:
   *   - DD/MM/YYYY
   *   - DD/MM/YYYY HH:MM
   *   - DD/MM/YYYY HH:MM:SS
   *   - YYYY-MM-DD
   *   - ISO 8601 completo
   *   - Objeto Date de ExcelJS (serialización numérica ya resuelta)
   */
  const parseDate = (val) => {
    if (!val) return null;
    if (val instanceof Date) return isNaN(val.getTime()) ? null : val;

    const s = val.toString().trim();
    if (!s) return null;

    // DD/MM/YYYY [HH:MM[:SS]]
    const mDDMM = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:[T\s]+(\d{1,2}):(\d{2})(?::(\d{2}))?)?/);
    if (mDDMM) {
      const [, dd, mm, yyyy, hh = '00', min = '00', ss = '00'] = mDDMM;
      const iso = `${yyyy}-${mm.padStart(2, '0')}-${dd.padStart(2, '0')}T${hh.padStart(2, '0')}:${min.padStart(2, '0')}:${ss.padStart(2, '0')}`;
      const d = new Date(iso);
      return isNaN(d.getTime()) ? null : d;
    }

    // YYYY-MM-DD / ISO 8601 / cualquier otro
    const d = new Date(s);
    return isNaN(d.getTime()) ? null : d;
  };

  /** Parsea número eliminando comas de miles. */
  const parseNum = (val) => {
    if (val === null || val === undefined || val === '') return null;
    const n = parseFloat(val.toString().replace(/,/g, '').trim());
    return isNaN(n) ? null : n;
  };

  // Mapeo de Tipo → código SAT.
  // Acepta letra SAT, nombre estándar y variantes comunes de ERPs.
  const TIPO_MAP = {
    // Letras SAT directas
    i: 'I', e: 'E', t: 'T', n: 'N', p: 'P',
    // Nombres estándar SAT
    ingreso: 'I', egreso: 'E', traslado: 'T',
    nomina: 'N', nómina: 'N', pago: 'P',
    // Variantes de ERP (factura, nota de crédito, etc.)
    factura: 'I', facturas: 'I', invoice: 'I',
    'nota de credito': 'E', 'nota de crédito': 'E',
    notacredito: 'E', 'nota credito': 'E', 'nota crédito': 'E',
    'notas de credito': 'E', 'notas de crédito': 'E',
    'nota de cargo': 'I', notacargo: 'I',
    'complemento de pago': 'P', complementopago: 'P', complemento: 'P',
    'recibo de nomina': 'N', 'recibo de nómina': 'N', recibodenomina: 'N',
    'carta porte': 'T', cartaporte: 'T',
  };

  // Mapeo normalizado de Estado SAT (tolerante a mayúsculas/minúsculas y acentos)
  const normalizarEstadoSAT = (raw) => {
    if (!raw) return null;
    const s = raw.toString().trim().toLowerCase()
      .normalize('NFD').replace(/[\u0300-\u036f]/g, ''); // quitar acentos
    if (s === 'vigente')        return 'Vigente';
    if (s === 'cancelado')      return 'Cancelado';
    if (s === 'no encontrado')  return 'No Encontrado';
    if (s === 'pendiente')      return 'Pendiente';
    if (s === 'error')          return 'Error';
    // Si ya viene en formato válido, devolver tal cual
    if (SAT_STATUS_VALIDOS.has(raw.toString().trim())) return raw.toString().trim();
    return null;
  };

  const dataRows = [];
  sheet.eachRow((row, rowNum) => { if (rowNum > 1) dataRows.push(row); });

  let nuevos = 0, actualizados = 0;
  const success = [], failed = [], duplicados = [];

  for (const row of dataRows) {
    // UUID — columna clave; si está vacía se omite la fila completa
    const uuidRaw = getCell(row, 'UUID');
    if (!uuidRaw) continue;
    const uuid = uuidRaw.toString().trim().toUpperCase();
    if (!uuid) continue;

    try {
      // ── Tipo de comprobante ──────────────────────────────────────────────
      const tipoRaw = getCell(row, 'Tipo', 'TipoComprobante', 'Tipo Comprobante');
      // Normalizar: minúsculas, sin acentos
      const tipoKey = tipoRaw
        ? tipoRaw.toString().trim().toLowerCase()
            .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
        : null;
      // Si no se reconoce el tipo se guarda null — en la comparación prevalece el valor del ERP
      const tipo = tipoKey ? (TIPO_MAP[tipoKey] ?? null) : null;

      // ── Fecha de emisión ─────────────────────────────────────────────────
      const fechaRaw = getCell(row, 'Fecha Emision', 'Fecha Emisión', 'FechaEmision', 'FechaGeneracion', 'Fecha');
      const fecha = parseDate(fechaRaw);
      if (!fecha) throw new Error(`Fecha Emision inválida o vacía: "${fechaRaw ?? ''}"`);

      // ── Totales ──────────────────────────────────────────────────────────
      // Los Complementos de Pago (tipo P) tienen Total=0 en el CFDI por diseño
      // del SAT; el importe real del cobro está en la columna 'Monto'.
      // Si Total existe pero vale 0, se usa Monto como respaldo para que
      // el dashboard refleje el monto real del pago.
      const totalRaw    = getCell(row, 'Total', 'Importe', 'Total CFDI');
      const montoRaw    = getCell(row, 'Monto');
      const totalParsed = parseNum(totalRaw);
      const montoParsed = parseNum(montoRaw);
      const total    = (totalParsed !== null && totalParsed !== 0) ? totalParsed : (montoParsed ?? totalParsed);
      const subTotalRaw = getCell(row, 'SubTotal', 'Subtotal', 'Sub Total');
      const subTotal = parseNum(subTotalRaw) ?? total;
      if (total === null) throw new Error(`Total / Monto inválido o vacío`);

      // ── RFC Receptor ─────────────────────────────────────────────────────
      const rfcReceptorRaw = getCell(row, 'RFC Receptor', 'RFCReceptor', 'RfcReceptor', 'RFC_Receptor');
      if (!rfcReceptorRaw) throw new Error('RFC Receptor vacío');
      const rfcReceptor = rfcReceptorRaw.toString().trim().toUpperCase();

      // ── RFC Emisor ───────────────────────────────────────────────────────
      const rfcEmisorRaw = getCell(row, 'RFC Emisor', 'RFCEmisor', 'RfcEmisor', 'RFC_Emisor');
      const rfcEmisor = rfcEmisorRaw ? rfcEmisorRaw.toString().trim().toUpperCase() : 'DESCONOCIDO';

      // ── Versión ──────────────────────────────────────────────────────────
      const versionRaw = getCell(row, 'Version', 'Versión', 'VersionCFDI', 'Version CFDI');
      const version = ['3.3', '4.0'].includes(versionRaw?.toString().trim()) ? versionRaw.toString().trim() : undefined;

      // ── Estado SAT ───────────────────────────────────────────────────────
      const estadoRaw = getCell(row, 'Estado SAT', 'EstadoSAT', 'Estatus', 'EstatusSAT');
      const satStatus = normalizarEstadoSAT(estadoRaw) ??
        (['SAT', 'MANUAL'].includes(source) ? 'Vigente' : null);

      // ── Fechas complementarias ───────────────────────────────────────────
      const fechaTimbradoRaw = getCell(row, 'Fecha Timbrado', 'FechaTimbrado', 'Fecha Timbrado');
      const fechaTimbrado    = parseDate(fechaTimbradoRaw);

      // ── Impuestos ────────────────────────────────────────────────────────
      // Suma todos los IVA trasladados disponibles como totalImpuestosTrasladados
      const iva16   = parseNum(getCell(row, 'IVA 16%',   'IVA16', 'IVA'))  ?? 0;
      const iva8    = parseNum(getCell(row, 'IVA 8%',    'IVA8'))            ?? 0;
      const ieps    = parseNum(getCell(row, 'Total IEPS', 'IEPS'))           ?? 0;
      const ieps304 = parseNum(getCell(row, 'IEPS 30.4%','IEPS304'))         ?? 0;
      const totalTrasladados = iva16 + iva8 + ieps + ieps304;

      // Suma todos los impuestos retenidos
      const retIVA  = parseNum(getCell(row, 'Retenido IVA', 'Ret IVA'))     ?? 0;
      const retISR  = parseNum(getCell(row, 'Retenido ISR', 'Ret ISR'))     ?? 0;
      const ivaRet6 = parseNum(getCell(row, 'IVA Ret 6%',  'IVA Ret'))     ?? 0;
      const ish     = parseNum(getCell(row, 'ISH'))                          ?? 0;
      const totalRetenidos = retIVA + retISR + ivaRet6 + ish;

      // ── Descuento ────────────────────────────────────────────────────────
      const descuento = parseNum(getCell(row, 'Descuento')) ?? 0;

      // ── UUID relacionado — 'UUIDRel' es el nombre en Complementos de Pago ─
      const uuidRelacionRaw = getCell(row, 'UUID Relacion', 'UUIDRelacion', 'UUID Relación', 'UUIDRel');
      const uuidRelacion    = uuidRelacionRaw ? uuidRelacionRaw.toString().trim().toUpperCase() : null;
      const tipoRelacion    = getCell(row, 'TipoRelacion', 'Tipo Relacion', 'Tipo Relación') ?? '04';

      // ── Datos receptor adicionales ───────────────────────────────────────
      const regimenFiscalReceptor   = getCell(row, 'RegimenFiscalReceptor', 'Regimen Fiscal Receptor') || undefined;
      const domicilioFiscalReceptor = getCell(row, 'DomicilioFiscalReceptor', 'Domicilio Fiscal Receptor') || undefined;

      // ── Construcción del documento CFDI ─────────────────────────────────
      const cfdiData = {
        uuid,
        source,
        ejercicio: ejercicioNum,
        periodo:   periodoNum,
        fecha,
        tipoDeComprobante: tipo,
        subTotal,
        total,
        descuento,
        moneda:     getCell(row, 'Moneda', 'MonedaP')  || 'MXN',
        tipoCambio: parseNum(getCell(row, 'Tipo De Cambio', 'TipoCambio')) ?? undefined,
        // 'FormaDePagoP' es el nombre en reportes de Complementos de Pago
        formaPago:  extraerCodigo(getCell(row, 'FormaDePago', 'FormaPago', 'Forma De Pago', 'FormaDePagoP')),
        metodoPago: extraerCodigo(getCell(row, 'Metodo de Pago', 'MetodoPago', 'Método de Pago')),
        serie:              getCell(row, 'Serie')                 || undefined,
        folio:              getCell(row, 'Folio')                 || undefined,
        lugarExpedicion:    getCell(row, 'LugarDeExpedicion', 'Lugar De Expedicion') || undefined,
        ...(version && { version }),
        ...(satStatus && { satStatus }),
        ...(source === 'ERP' && { erpStatus: getCell(row, 'Estatus', 'EstatusERP', 'Estatus ERP') || 'Timbrado' }),
        emisor: {
          rfc:    rfcEmisor,
          nombre: getCell(row, 'Nombre Emisor', 'NombreEmisor') || undefined,
        },
        receptor: {
          rfc:                      rfcReceptor,
          nombre:                   getCell(row, 'Nombre Receptor', 'NombreReceptor') || undefined,
          usoCFDI:                  extraerCodigo(getCell(row, 'UsoCFDI', 'Uso CFDI', 'UsoCfdi')) || undefined,
          residenciaFiscal:         getCell(row, 'ResidenciaFiscal', 'Residencia Fiscal') || undefined,
          numRegIdTrib:             getCell(row, 'NumRegIdTrib', 'Num Reg Id Trib')    || undefined,
          regimenFiscal:            regimenFiscalReceptor,
          domicilioFiscalReceptor:  domicilioFiscalReceptor,
        },
        impuestos: {
          totalImpuestosTrasladados: totalTrasladados,
          totalImpuestosRetenidos:   totalRetenidos,
        },
        ...(fechaTimbrado && {
          timbreFiscalDigital: { fechaTimbrado },
        }),
        ...(uuidRelacion && {
          // extraerCodigo: "03 - Devolución de mercancía..." → "03"
          cfdiRelacionados: [{ tipoRelacion: extraerCodigo(tipoRelacion) ?? '04', uuids: [uuidRelacion] }],
        }),
      };

      const prev = await CFDI.findOneAndUpdate(
        { uuid: cfdiData.uuid, source },
        { ...cfdiData, uploadedBy: req.user._id },
        { upsert: true, new: false, setDefaultsOnInsert: true },
      );

      prev === null ? nuevos++ : actualizados++;
      if (prev !== null) duplicados.push({ uuid: cfdiData.uuid, filename: `Fila ${row.number}` });
      success.push({
        uuid: cfdiData.uuid,
        filename: `Fila ${row.number}`,
        nuevo: prev === null,
        satStatus: cfdiData.satStatus ?? null,
        lastComparisonStatus: prev?.lastComparisonStatus ?? null,
      });

      // Verificar estado SAT en background
      verificarSATBackground(cfdiData);

    } catch (err) {
      failed.push({ filename: `Fila ${row.number}`, uuid: uuid ?? null, error: err.message });
    }
  }

  const procesados = nuevos + actualizados;
  res.status(207).json({
    message: `${procesados} CFDIs procesados (${nuevos} nuevos, ${actualizados} actualizados), ${failed.length} con error`,
    procesados, nuevos, actualizados, duplicados,
    errores: failed, success,
  });

  // Reclasificación automática en background para facturas globales SAT/MANUAL
  if (['SAT', 'MANUAL'].includes(source) && ejercicioNum && procesados > 0) {
    setImmediate(() => {
      aplicarReclasificacion({ ejercicio: ejercicioNum, source })
        .catch(err => logger.warn(`[importExcel] reclasificación background falló: ${err.message}`));
    });
  }
});

/**
 * POST /api/cfdis  (integración ERP vía JSON)
 */
const create = asyncHandler(async (req, res) => {
  const errors = validationResult(req);
  if (!errors.isEmpty()) return res.status(400).json({ errors: errors.array() });

  const cfdi = await CFDI.findOneAndUpdate(
    { uuid: req.body.uuid.toUpperCase(), source: 'ERP' },
    { ...req.body, source: 'ERP', uploadedBy: req.user._id },
    { upsert: true, new: true, setDefaultsOnInsert: true },
  );

  res.status(201).json(cfdi);
});

/**
 * POST /api/cfdis/:id/compare
 */
const compare = asyncHandler(async (req, res) => {
  const comparison = await compareCFDI(req.params.id, { triggeredBy: req.user._id });
  res.json(comparison);
});

/**
 * DELETE /api/cfdis/:id  (soft delete)
 */
const remove = asyncHandler(async (req, res) => {
  const cfdi = await CFDI.findByIdAndUpdate(req.params.id, { isActive: false }, { new: true });
  if (!cfdi) return res.status(404).json({ error: 'CFDI no encontrado' });
  res.json({ message: 'CFDI desactivado', id: cfdi._id });
});

/**
 * POST /api/cfdis/import-erp-api
 * Recupera CFDIs directamente desde el endpoint REST del ERP del cliente.
 * El frontend pasa { ejercicio, periodo, erpUrl } en el body.
 * El backend hace el llamado al ERP (evitando CORS) y procesa la respuesta.
 *
 * Campos esperados del ERP:
 *   ID, UUID, TipoComprobante, FechaGeneracion, Serie, Folio, UUIDRelacion,
 *   RFCEmisor, RFCReceptor, NombreReceptor, UsoCfdi, Subtotal, TotalIVA,
 *   TotalRetenciones, Importe, Moneda, TipoCambio, FormaPago, MetodoPago,
 *   FechaPago, SelloCFD, SelloSAT, NoCertificado, NoCertificadoSAT,
 *   FechaTimbrado, RfcProvCertif, Estatus, EstatusSAT, FechaCancelacion,
 *   TipoRelacion
 */
const importFromErpApi = asyncHandler(async (req, res) => {
  const { ejercicio, periodo, erpUrl } = req.body;

  if (!ejercicio || !periodo) {
    return res.status(400).json({ error: 'El ejercicio y periodo son obligatorios', code: 'PERIODO_REQUERIDO' });
  }
  if (!erpUrl) {
    return res.status(400).json({ error: 'La URL del endpoint ERP es obligatoria', code: 'ERP_URL_REQUERIDA' });
  }

  const ejercicioNum = parseInt(ejercicio);
  const periodoNum   = parseInt(periodo);
  if (periodoNum < 1 || periodoNum > 12) {
    return res.status(400).json({ error: 'El periodo debe ser entre 1 y 12', code: 'PERIODO_INVALIDO' });
  }

  // ── Llamar al ERP ──────────────────────────────────────────────────────────
  let erpData;
  try {
    const response = await axios.get(erpUrl, {
      timeout: 30000,
      headers: {
        'Accept': 'application/json',
        // Si el ERP requiere auth básica o token, se pueden pasar como headers extra
        // desde el body: req.body.erpHeaders
        ...(req.body.erpHeaders || {}),
      },
    });
    // El ERP puede devolver { data: [...] } o directamente un arreglo
    erpData = Array.isArray(response.data)
      ? response.data
      : (response.data?.data ?? response.data?.cfdis ?? response.data?.cfdi ?? []);
  } catch (err) {
    return res.status(502).json({
      error: `No se pudo conectar al ERP: ${err.message}`,
      code: 'ERP_CONNECTION_ERROR',
    });
  }

  if (!Array.isArray(erpData) || erpData.length === 0) {
    return res.status(200).json({
      message: 'El ERP no devolvió registros para este periodo.',
      procesados: 0, nuevos: 0, actualizados: 0,
      duplicados: [], errores: [], success: [],
    });
  }

  // ── Procesar cada registro ──────────────────────────────────────────────────
  const parseNum = (v) => { if (v == null) return null; const n = parseFloat(String(v).replace(/,/g, '')); return isNaN(n) ? null : n; };
  const parseDate = (v) => { if (!v) return null; const d = new Date(v); return isNaN(d.getTime()) ? null : d; };

  let nuevos = 0, actualizados = 0;
  const success = [], failed = [], duplicados = [];

  for (let i = 0; i < erpData.length; i++) {
    const row = erpData[i];
    const rowLabel = `Registro ${i + 1}`;

    const uuid = (row.UUID || row.uuid || '').toString().trim().toUpperCase();
    if (!uuid) { failed.push({ filename: rowLabel, error: 'UUID vacío' }); continue; }

    try {
      const tipoRaw = (row.TipoComprobante || row.tipoComprobante || '').toString().toLowerCase();
      const tipo = TIPOS_COMPROBANTE[tipoRaw];
      if (!tipo) throw new Error(`TipoComprobante inválido: "${row.TipoComprobante}"`);

      const fecha = parseDate(row.FechaGeneracion || row.fechaGeneracion);
      if (!fecha) throw new Error('FechaGeneracion inválida o vacía');

      const importe = parseNum(row.Importe || row.importe);
      if (importe === null) throw new Error('Importe inválido o vacío');

      const rfcReceptor = (row.RFCReceptor || row.rfcReceptor || '').toString().trim().toUpperCase();
      if (!rfcReceptor) throw new Error('RFCReceptor vacío');

      const rfcEmisor = (row.RFCEmisor || row.rfcEmisor || 'DESCONOCIDO').toString().trim().toUpperCase();

      const estatusRaw = row.EstatusSAT || row.estatusSAT || null;
      const satStatus  = SAT_STATUS_VALIDOS.has(estatusRaw) ? estatusRaw : null;
      const erpStatus  = row.Estatus || row.estatus || null;

      const uuidRelacion = (row.UUIDRelacion || row.uuidRelacion || '').toString().trim().toUpperCase() || null;
      const tipoRelacion = (row.TipoRelacion || row.tipoRelacion || '04').toString().trim();

      const totalIVA        = parseNum(row.TotalIVA        || row.totalIVA)        ?? 0;
      const totalRetenciones = parseNum(row.TotalRetenciones || row.totalRetenciones) ?? 0;

      const cfdiData = {
        uuid,
        source: 'ERP',
        ejercicio: ejercicioNum,
        periodo:   periodoNum,
        fecha,
        tipoDeComprobante: tipo,
        serie:      row.Serie      || row.serie      || undefined,
        folio:      row.Folio      || row.folio      || undefined,
        formaPago:  row.FormaPago  || row.formaPago  || undefined,
        metodoPago: row.MetodoPago || row.metodoPago || undefined,
        moneda:     row.Moneda     || row.moneda     || 'MXN',
        tipoCambio: parseNum(row.TipoCambio || row.tipoCambio) ?? undefined,
        subTotal:   parseNum(row.Subtotal   || row.subtotal)   ?? importe,
        total:      importe,
        emisor:   { rfc: rfcEmisor },
        receptor: {
          rfc:     rfcReceptor,
          nombre:  row.NombreReceptor || row.nombreReceptor || undefined,
          usoCFDI: row.UsoCfdi        || row.usoCfdi        || undefined,
        },
        ...(row.ID     && { erpId: String(row.ID) }),
        ...(satStatus  && { satStatus }),
        ...(erpStatus  && { erpStatus }),
        ...(row.NoCertificado && { noCertificado: String(row.NoCertificado) }),
        ...(uuidRelacion && {
          cfdiRelacionados: [{ tipoRelacion, uuids: [uuidRelacion] }],
        }),
        impuestos: {
          totalImpuestosTrasladados: totalIVA,
          totalImpuestosRetenidos:   totalRetenciones,
        },
        ...(row.SelloCFD && { sello: row.SelloCFD }),
        timbreFiscalDigital: {
          ...(row.FechaTimbrado    && { fechaTimbrado:    parseDate(row.FechaTimbrado) }),
          ...(row.SelloSAT         && { selloSAT:         row.SelloSAT }),
          ...(row.NoCertificadoSAT && { noCertificadoSAT: String(row.NoCertificadoSAT) }),
          ...(row.RfcProvCertif    && { rfcProvCertif:    row.RfcProvCertif }),
        },
      };

      const prev = await CFDI.findOneAndUpdate(
        { uuid, source: 'ERP' },
        { ...cfdiData, uploadedBy: req.user._id },
        { upsert: true, new: false, setDefaultsOnInsert: true },
      );

      prev === null ? nuevos++ : actualizados++;
      if (prev !== null) duplicados.push({ uuid, filename: rowLabel });
      success.push({
        uuid, filename: rowLabel,
        nuevo: prev === null,
        satStatus: prev?.satStatus ?? satStatus ?? null,
        lastComparisonStatus: prev?.lastComparisonStatus ?? null,
      });
    } catch (err) {
      failed.push({ filename: rowLabel, error: err.message });
    }
  }

  const procesados = nuevos + actualizados;
  res.status(207).json({
    message: `${procesados} CFDIs procesados (${nuevos} nuevos, ${actualizados} duplicados), ${failed.length} con error`,
    procesados, nuevos, actualizados, duplicados,
    errores: failed, success,
  });

  // Reclasificación automática en background: busca CFDIs SAT del mismo ejercicio
  // con InformacionGlobal.Mes y alinea también los ERP recién importados.
  if (procesados > 0) {
    setImmediate(() => {
      aplicarReclasificacion({ ejercicio: ejercicioNum, source: 'SAT' })
        .catch(err => logger.warn(`[importFromErpApi] reclasificación background falló: ${err.message}`));
    });
  }
});

/**
 * GET /api/cfdis/export
 * Genera y descarga un .xlsx con los CFDIs que coincidan con los filtros.
 * Acepta los mismos query params que GET /api/cfdis.
 */
const exportExcel = asyncHandler(async (req, res) => {
  const {
    source, tipoDeComprobante, rfcEmisor, rfcReceptor,
    satStatus, erpStatus, lastComparisonStatus, fechaInicio, fechaFin,
    search, ejercicio, periodo,
  } = req.query;

  const filter = { isActive: { $ne: false } };
  if (source) {
    const sources = source.toUpperCase().split(',').map(s => s.trim()).filter(Boolean);
    filter.source = sources.length === 1 ? sources[0] : { $in: sources };
  }
  if (tipoDeComprobante) filter.tipoDeComprobante  = tipoDeComprobante;
  if (rfcEmisor)         filter['emisor.rfc']       = rfcEmisor.toUpperCase();
  if (rfcReceptor)       filter['receptor.rfc']     = rfcReceptor.toUpperCase();
  if (satStatus)  filter.satStatus = satStatus;
  if (erpStatus) {
    const valores = erpStatus.split(',').map(v => v.trim()).filter(Boolean);
    filter.erpStatus = valores.length === 1 ? valores[0] : { $in: valores };
  }
  if (fechaInicio || fechaFin) {
    filter.fecha = {};
    if (fechaInicio) {
      const d = fechaInicio.split('T')[0];
      filter.fecha.$gte = new Date(`${d}T06:00:00Z`);
    }
    if (fechaFin) {
      const d   = fechaFin.split('T')[0];
      const fin = new Date(`${d}T06:00:00Z`);
      fin.setUTCDate(fin.getUTCDate() + 1);
      filter.fecha.$lt = fin;
    }
  }
  if (ejercicio)            filter.ejercicio            = parseInt(ejercicio);
  if (periodo)              filter.periodo              = parseInt(periodo);
  if (lastComparisonStatus) filter.lastComparisonStatus = lastComparisonStatus;
  if (search) filter.$text = { $search: search };

  const cfdis = await CFDI.find(filter, { xmlContent: 0 }).sort({ fecha: -1 }).lean();

  const workbook = new ExcelJS.Workbook();
  const sheet    = workbook.addWorksheet('CFDIs');

  sheet.columns = [
    { header: 'UUID',               key: 'uuid',           width: 38 },
    { header: 'Origen',             key: 'source',         width: 8  },
    { header: 'Tipo',               key: 'tipo',           width: 12 },
    { header: 'RFC Emisor',         key: 'rfcEmisor',      width: 16 },
    { header: 'Nombre Emisor',      key: 'nombreEmisor',   width: 30 },
    { header: 'RFC Receptor',       key: 'rfcReceptor',    width: 16 },
    { header: 'Nombre Receptor',    key: 'nombreReceptor', width: 30 },
    { header: 'Serie',              key: 'serie',          width: 8  },
    { header: 'Folio',              key: 'folio',          width: 14 },
    { header: 'Fecha',              key: 'fecha',          width: 12 },
    { header: 'Subtotal',           key: 'subTotal',       width: 14 },
    { header: 'Total',              key: 'total',          width: 14 },
    { header: 'Moneda',             key: 'moneda',         width: 8  },
    { header: 'Estado SAT',         key: 'satStatus',      width: 14 },
    { header: 'Estado ERP',         key: 'erpStatus',      width: 14 },
    { header: 'Estado Comparación', key: 'compStatus',     width: 22 },
  ];

  const headerRow = sheet.getRow(1);
  headerRow.font      = { bold: true, color: { argb: 'FF1F3864' } };
  headerRow.fill      = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFD9E1F2' } };
  headerRow.alignment = { vertical: 'middle' };
  headerRow.commit();

  const tipoLabel = { I: 'Ingreso', E: 'Egreso', T: 'Traslado', N: 'Nómina', P: 'Pago' };
  const compLabel = {
    match:       'Conciliado',
    discrepancy: 'Con discrepancias',
    not_in_sat:  'No en SAT',
    not_in_erp:  'No en ERP',
    cancelled:   'Cancelado en SAT',
    warning:     'Advertencias',
  };

  for (const c of cfdis) {
    let total = c.total;
    if (c.tipoDeComprobante === 'P' && c.complementoPago) {
      total = c.complementoPago.totales?.montoTotalPagos
           ?? c.complementoPago.pagos?.[0]?.monto
           ?? c.total;
    }
    sheet.addRow({
      uuid:          c.uuid,
      source:        c.source,
      tipo:          tipoLabel[c.tipoDeComprobante] ?? c.tipoDeComprobante,
      rfcEmisor:     c.emisor?.rfc      ?? '',
      nombreEmisor:  c.emisor?.nombre   ?? '',
      rfcReceptor:   c.receptor?.rfc    ?? '',
      nombreReceptor:c.receptor?.nombre ?? '',
      serie:         c.serie  ?? '',
      folio:         c.folio  ?? '',
      fecha:         c.fecha  ? new Date(c.fecha) : '',
      subTotal:      c.subTotal ?? 0,
      total:         total      ?? 0,
      moneda:        c.moneda   ?? '',
      satStatus:     c.satStatus  ?? '',
      erpStatus:     c.erpStatus  ?? '',
      compStatus:    compLabel[c.lastComparisonStatus] ?? c.lastComparisonStatus ?? '',
    });
  }

  sheet.getColumn('fecha').numFmt   = 'dd/mm/yyyy';
  sheet.getColumn('subTotal').numFmt = '#,##0.00';
  sheet.getColumn('total').numFmt    = '#,##0.00';

  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', 'attachment; filename="cfdis.xlsx"');
  await workbook.xlsx.write(res);
  res.end();
});

// ── Reclasificación Global ────────────────────────────────────────────────────

/**
 * GET /api/cfdis/reclasificacion-global/plan
 * Genera el plan de reclasificación SIN modificar datos.
 * Query params: ejercicio, periodo, rfc, source
 */
const planReclasificacionGlobal = asyncHandler(async (req, res) => {
  const { ejercicio, periodo, rfc, source, mesIG } = req.query;
  const plan = await generarPlan({
    ejercicio: ejercicio ? Number(ejercicio) : undefined,
    periodo:   periodo   ? Number(periodo)   : undefined,
    rfc:       rfc       || undefined,
    source:    source    || undefined,
    mesIG:     mesIG     ? Number(mesIG)     : undefined,
  });
  res.json({ success: true, data: plan });
});

/**
 * POST /api/cfdis/reclasificacion-global/aplicar
 * Aplica la reclasificación. Requiere { confirmar: true } en el body.
 * Body: { ejercicio, periodo, rfc, source, confirmar }
 */
const aplicarReclasificacionGlobal = asyncHandler(async (req, res) => {
  const { ejercicio, periodo, rfc, source, confirmar, items } = req.body;

  if (!confirmar) {
    return res.status(400).json({
      success: false,
      error:   'Debes enviar { confirmar: true } para ejecutar la reclasificación. Primero genera el plan con GET /plan.',
    });
  }

  // Si vienen items explícitos del plan (modo directo), usarlos directamente
  const itemsExplicitos = Array.isArray(items) && items.length > 0 ? items : null;

  const resultado = await aplicarReclasificacion({
    ejercicio: ejercicio ? Number(ejercicio) : undefined,
    periodo:   periodo   ? Number(periodo)   : undefined,
    rfc:       rfc       || undefined,
    source:    source    || undefined,
  }, itemsExplicitos);

  res.json({ success: true, data: resultado });
});

/**
 * POST /api/cfdis/migrar-periodo-bulk
 * Migra varios CFDIs SAT/MANUAL al mismo periodo/ejercicio en una sola llamada.
 * Body: { ids: string[], ejercicio: number, periodo: number }
 */
const migrarPeriodoBulk = asyncHandler(async (req, res) => {
  const { ids, ejercicio, periodo } = req.body;

  if (!Array.isArray(ids) || ids.length === 0) {
    return res.status(400).json({ error: 'Se requiere un arreglo de IDs.' });
  }
  if (!ejercicio || !periodo) {
    return res.status(400).json({ error: 'Se requieren ejercicio y periodo.' });
  }

  const ej = parseInt(ejercicio, 10);
  const pe = parseInt(periodo,   10);

  if (isNaN(ej) || ej < 2000 || ej > 2100) return res.status(400).json({ error: 'Ejercicio inválido.' });
  if (isNaN(pe) || pe < 1    || pe > 12)   return res.status(400).json({ error: 'Periodo inválido (1-12).' });

  // 1. Obtener todos los CFDIs SAT/MANUAL de la lista
  const cfdis = await CFDI.find(
    { _id: { $in: ids }, source: { $in: ['SAT', 'MANUAL'] }, isActive: true },
    'uuid ejercicio periodo lastComparisonStatus informacionGlobal xmlContent'
  ).select('+xmlContent').lean();

  // 2. Pre-cargar contrapartes ERP para los que tienen status 'match' sin InformacionGlobal
  const matchUuids = cfdis
    .filter(c => c.lastComparisonStatus === 'match' && !c.informacionGlobal?.mes)
    .map(c => c.uuid);

  const erpMap = new Map();
  if (matchUuids.length > 0) {
    const erpCfdis = await CFDI.find(
      { source: 'ERP', uuid: { $in: matchUuids }, isActive: true },
      'uuid ejercicio periodo informacionGlobal xmlContent'
    ).select('+xmlContent').lean();
    for (const erp of erpCfdis) erpMap.set(erp.uuid.toUpperCase(), erp);
  }

  // 3. Validar cada CFDI y separar migrables de fallidos
  const uuidsMigrar = [];
  const migrados    = [];
  const fallidos    = [];

  for (const cfdi of cfdis) {
    const tieneInfoGlobal = cfdi.informacionGlobal?.mes ||
      (cfdi.xmlContent && /InformacionGlobal/i.test(cfdi.xmlContent));

    let erpEsFG = false;
    if (!tieneInfoGlobal && cfdi.lastComparisonStatus === 'match') {
      const erp = erpMap.get(cfdi.uuid.toUpperCase());
      if (erp) {
        const erpDistintoPeriodo = erp.ejercicio !== cfdi.ejercicio || erp.periodo !== cfdi.periodo;
        const erpTieneIG = erp.informacionGlobal?.mes ||
          (erp.xmlContent && /InformacionGlobal/i.test(erp.xmlContent));
        erpEsFG = erpDistintoPeriodo && erpTieneIG;
      }
    }

    if (!tieneInfoGlobal && !erpEsFG) {
      fallidos.push({ id: String(cfdi._id), uuid: cfdi.uuid, error: 'No es factura global ni su match ERP lo es.' });
      continue;
    }

    uuidsMigrar.push(cfdi.uuid);
    migrados.push({ id: String(cfdi._id), uuid: cfdi.uuid });
  }

  // 4. Actualizar en bulk (2 queries, sin N+1)
  if (uuidsMigrar.length > 0) {
    await CFDI.updateMany(
      { uuid: { $in: uuidsMigrar } },
      { $set: { periodo: pe, ejercicio: ej } }
    );
    await Comparison.updateMany(
      { uuid: { $in: uuidsMigrar } },
      { $set: { periodo: pe, ejercicio: ej } }
    );
  }

  logger.info(
    `[MigrarPeriodoBulk] ${migrados.length} CFDI(s) migrados → ${ej}/${pe} por usuario ${req.user._id}`
  );

  res.json({
    success: true,
    migrados: migrados.length,
    fallidos: fallidos.length,
    ...(fallidos.length > 0 && { detalleFallidos: fallidos }),
  });
});

/**
 * PATCH /api/cfdis/:id/migrar-periodo
 * Mueve una factura global SAT/MANUAL a un periodo/ejercicio diferente.
 * Solo aplica a CFDIs con source SAT o MANUAL que tengan InformacionGlobal.
 * Body: { ejercicio: number, periodo: number }
 */
const migrarPeriodo = asyncHandler(async (req, res) => {
  const { id } = req.params;
  const { ejercicio, periodo } = req.body;

  if (!ejercicio || !periodo) {
    return res.status(400).json({ error: 'Se requieren ejercicio y periodo.' });
  }

  const ej = parseInt(ejercicio, 10);
  const pe = parseInt(periodo,   10);

  if (isNaN(ej) || ej < 2000 || ej > 2100) {
    return res.status(400).json({ error: 'Ejercicio inválido.' });
  }
  if (isNaN(pe) || pe < 1 || pe > 12) {
    return res.status(400).json({ error: 'Periodo inválido (1-12).' });
  }

  const cfdi = await CFDI.findById(id).select('+xmlContent');
  if (!cfdi) return res.status(404).json({ error: 'CFDI no encontrado.' });

  if (!['SAT', 'MANUAL'].includes(cfdi.source)) {
    return res.status(400).json({ error: 'Solo se pueden migrar CFDIs de origen SAT o MANUAL.' });
  }

  // Caso 1: el propio CFDI SAT tiene InformacionGlobal → siempre permitido
  const tieneInfoGlobal = cfdi.informacionGlobal?.mes ||
    (cfdi.xmlContent && /InformacionGlobal/i.test(cfdi.xmlContent));

  // Caso 2: el CFDI SAT tiene 'match' con un CFDI ERP que es factura global y está en otro periodo
  // Caso 3: el CFDI SAT tiene 'not_in_erp' pero existe contraparte ERP con mismo UUID en otro periodo
  //         (subido al mes equivocado)
  let erpEnOtroPeriodo = false;
  if (!tieneInfoGlobal && ['match', 'not_in_erp'].includes(cfdi.lastComparisonStatus)) {
    const erpMatch = await CFDI.findOne(
      { uuid: cfdi.uuid, source: 'ERP', isActive: true },
      'ejercicio periodo informacionGlobal xmlContent'
    ).select('+xmlContent');

    if (erpMatch) {
      const erpDistintoPeriodo = erpMatch.ejercicio !== cfdi.ejercicio || erpMatch.periodo !== cfdi.periodo;
      if (cfdi.lastComparisonStatus === 'match') {
        // Caso 2: match cross-period donde el ERP es factura global
        const erpTieneIG = erpMatch.informacionGlobal?.mes ||
          (erpMatch.xmlContent && /InformacionGlobal/i.test(erpMatch.xmlContent));
        erpEnOtroPeriodo = erpDistintoPeriodo && erpTieneIG;
      } else {
        // Caso 3: not_in_erp pero hay contraparte ERP en otro periodo (mes equivocado al subir)
        erpEnOtroPeriodo = erpDistintoPeriodo;
      }
    }
  }

  if (!tieneInfoGlobal && !erpEnOtroPeriodo) {
    return res.status(400).json({ error: 'El CFDI no es una factura global ni tiene contraparte ERP en otro periodo.' });
  }

  const periodoAnterior  = cfdi.periodo;
  const ejercicioAnterior = cfdi.ejercicio;

  // Actualizar CFDI (ambos documentos SAT y ERP con mismo UUID si existen)
  await CFDI.updateMany(
    { uuid: cfdi.uuid },
    { $set: { periodo: pe, ejercicio: ej } },
  );

  // Actualizar todos los Comparison records del mismo UUID
  await Comparison.updateMany(
    { uuid: cfdi.uuid },
    { $set: { periodo: pe, ejercicio: ej } },
  );

  logger.info(
    `[MigrarPeriodo] UUID ${cfdi.uuid} migrado de ${ejercicioAnterior}/${periodoAnterior} ` +
    `→ ${ej}/${pe} por usuario ${req.user._id}`,
  );

  res.json({
    success: true,
    uuid: cfdi.uuid,
    periodoAnterior,
    ejercicioAnterior,
    periodoNuevo:   pe,
    ejercicioNuevo: ej,
  });
});

module.exports = {
  list, getById, getXml, upload, importExcel, importFromErpApi, create, compare, remove, exportExcel,
  planReclasificacionGlobal, aplicarReclasificacionGlobal, migrarPeriodo, migrarPeriodoBulk,
};
