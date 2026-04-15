const CFDI = require('../models/CFDI');
const Comparison = require('../models/Comparison');
const ComparisonSession = require('../models/ComparisonSession');
const Discrepancy = require('../models/Discrepancy');
const { verifyCFDIWithSAT } = require('./satVerification');
const { logger } = require('../utils/logger');

const formatSessionName = (date) => {
  return `Sesión ${date.toLocaleDateString('es-MX', {
    day: '2-digit', month: '2-digit', year: 'numeric',
  })} ${date.toLocaleTimeString('es-MX', { hour: '2-digit', minute: '2-digit' })}`;
};

const TOLERANCE_AMOUNT = 0.01;

/**
 * Compara un CFDI del ERP contra:
 *  a) El estado en vivo del SAT (SOAP)
 *  b) La copia local SAT si fue descargada manualmente
 *
 * El chequeo en vivo del SAT es "best-effort": si falla o el UUID no
 * está registrado, el engine igual hace la comparación campo a campo
 * si existe una copia local con source='SAT'.
 */
const compareCFDI = async (erpCfdiId, options = {}) => {
  const erpCfdi = await CFDI.findById(erpCfdiId);
  if (!erpCfdi) throw new Error(`CFDI ERP no encontrado: ${erpCfdiId}`);

  const triggeredBy = options.triggeredBy;
  const sessionId   = options.sessionId ?? null;

  // Ejercicio y periodo fiscales: usar campos explícitos si existen, si no derivar de fecha
  const cfdiDate  = new Date(erpCfdi.fecha);
  const ejercicio = erpCfdi.ejercicio ?? cfdiDate.getFullYear();
  const periodo   = erpCfdi.periodo   ?? (cfdiDate.getMonth() + 1);

  // ── 1. Buscar copia local SAT (siempre, independiente del live check) ──
  // Busca en source='SAT' y también 'MANUAL' (portal SAT descargado manualmente)
  const satCfdi = await CFDI.findOne({ uuid: erpCfdi.uuid, source: { $in: ['SAT', 'MANUAL'] } });

  // ── 2. Live check SAT (best-effort, no bloquea la comparación local) ──
  // RFC válido: 12-13 caracteres alfanuméricos (empresas 12, personas físicas 13)
  const rfcEmisorValido = /^[A-Z&Ñ]{3,4}\d{6}[A-Z0-9]{3}$/i.test(erpCfdi.emisor.rfc);
  let satResponse = null;
  const sello = erpCfdi.timbreFiscalDigital?.selloCFD || erpCfdi.sello
    || satCfdi?.timbreFiscalDigital?.selloCFD || satCfdi?.sello || '';

  if (!rfcEmisorValido) {
    logger.warn(`[Engine] RFC emisor inválido ("${erpCfdi.emisor.rfc}") para ${erpCfdi.uuid} — live check SAT omitido.`);
  } else if (!sello) {
    // Sin sello no hay fe= y el SAT siempre devuelve N-601 — no contaminar satStatus
    logger.warn(`[Engine] ${erpCfdi.uuid} — sin sello (ERP sin XML, sin copia SAT local). Live check omitido.`);
    if (erpCfdi.satStatus !== 'Vigente' && erpCfdi.satStatus !== 'Cancelado') {
      await updateSATStatus(erpCfdi, satCfdi ? 'Verificado Local' : 'Pendiente');
    }
  } else {
    try {
      satResponse = await verifyCFDIWithSAT(
        erpCfdi.uuid,
        erpCfdi.emisor.rfc,
        erpCfdi.receptor.rfc,
        erpCfdi.total,
        sello,
        erpCfdi.version || '4.0',
        erpCfdi.tipoDeComprobante
      );
      await updateSATStatus(erpCfdi, satResponse.state);
    } catch (err) {
      logger.warn(`[Engine] Live SAT check falló para ${erpCfdi.uuid}: ${err.message}. Continuando con copia local.`);
      await updateSATStatus(erpCfdi, satCfdi ? 'Verificado Local' : 'Error');
    }
  }

  const differences = [];

  // ── 3. Discrepancias de estado SAT ──
  if (satResponse) {
    if (satResponse.state === 'No Encontrado') {
      differences.push({
        field: 'sat.uuid',
        erpValue: 'Registrado en ERP',
        satValue: 'No encontrado en SAT',
        severity: 'critical',
        type: 'UUID_NOT_FOUND_SAT',
      });
    } else if (satResponse.isCancelled) {
      // Si el ERP también lo tiene como Cancelado → ambos coinciden, no es discrepancia
      if (erpCfdi.satStatus !== 'Cancelado') {
        differences.push({
          field: 'sat.estado',
          erpValue: erpCfdi.satStatus || 'Sin estatus',
          satValue: 'Cancelado',
          severity: 'critical',
          type: 'CANCELLED_IN_SAT',
        });
      }
    }
  }

  // ── 4. Comparación campo a campo (solo si existe copia local SAT y SAT no dijo "No Encontrado") ──
  const satConfirmaNoEncontrado = satResponse?.state === 'No Encontrado';
  if (satCfdi && !satConfirmaNoEncontrado) {
    const esPago = erpCfdi.tipoDeComprobante === 'P';
    if (esPago) {
      // Para tipo P: total raíz siempre es 0 por definición del SAT.
      // Comparar el Monto del Complemento de Pago en lugar de total/subTotal.
      differences.push(...compareComplementoPago(erpCfdi, satCfdi));
    } else {
      differences.push(...compareAmounts(erpCfdi, satCfdi));
      differences.push(...compareTaxes(erpCfdi, satCfdi));
    }
    differences.push(...compareParties(erpCfdi, satCfdi));
    differences.push(...compareDates(erpCfdi, satCfdi));
    differences.push(...compareGeneralFields(erpCfdi, satCfdi));
  }

  // Diferencias críticas con impacto fiscal <= $0.01 se degradan a warning
  // Se usa 0.0101 como umbral para absorber imprecisión de punto flotante
  differences.forEach(d => {
    if (d.severity === 'critical' && d.fiscalImpact?.amount > 0 && d.fiscalImpact.amount <= 0.0101) {
      d.severity = 'warning';
    }
  });

  const criticalCount = differences.filter(d => d.severity === 'critical').length;
  const warningCount  = differences.filter(d => d.severity === 'warning').length;

  const SAT_VALID_STATES = ['Vigente', 'Cancelado', 'No Encontrado'];
  const satIsUnreachable = satResponse && !SAT_VALID_STATES.includes(satResponse.state);

  // ── Determinación de status con lógica correcta ───────────────────────────
  //
  // Regla principal: un CFDI se marca 'match' SOLO cuando existe en AMBOS lados
  // (ERP local + SAT local) y todos los campos comparados coinciden.
  //
  // El live check del SAT (satResponse) confirma validez/cancelación pero NO
  // reemplaza la copia local: si no hay copia local SAT → not_in_sat.
  //
  // Tabla de decisión:
  //  satResponse=null, satCfdi=null            → error (no hay datos SAT)
  //  satResponse=unreachable, satCfdi=null     → error (SAT inalcanzable, sin local)
  //  satResponse.isCancelled=true              → cancelled (SAT confirmó cancelación)
  //  satResponse.state='No Encontrado'         → not_in_sat (SAT confirmó que no existe)
  //  satCfdi=null  (sin copia local SAT)       → not_in_sat (solo existe en ERP)
  //  satCfdi existe, differences.length=0     → match (existe en ambos, campos iguales)
  //  satCfdi existe, differences.length>0     → discrepancy (existe en ambos, campos distintos)

  let status;
  if (!satCfdi && !satResponse) {
    // Sin ninguna fuente SAT disponible
    status = 'error';
  } else if (satIsUnreachable && !satCfdi) {
    // SAT devolvió estado inesperado y no hay copia local
    status = 'error';
  } else if (satResponse?.isCancelled) {
    // Si ERP también lo tiene como Cancelado → conciliados (ambos coinciden)
    status = erpCfdi.satStatus === 'Cancelado' ? 'match' : 'cancelled';
  } else if (satResponse?.state === 'No Encontrado') {
    // SAT confirmó que el UUID no existe
    status = 'not_in_sat';
  } else if (!satCfdi) {
    // No hay copia local SAT: el CFDI solo existe en ERP.
    // Aunque SAT live diga 'Vigente', sin copia local no se puede verificar
    // campos (total, RFC, etc.) → se trata como not_in_sat.
    status = 'not_in_sat';
  } else if (differences.length === 0) {
    // Existe en ambos lados y todos los campos coinciden
    status = 'match';
  } else {
    // Existe en ambos lados pero hay diferencias de campo
    // Si no hay críticas (solo advertencias) → 'warning' para distinguir en UI
    status = criticalCount > 0 ? 'discrepancy' : 'warning';
  }

  // Persistir resultado en el propio documento CFDI para sobrevivir recargas
  await CFDI.findByIdAndUpdate(erpCfdiId, {
    lastComparisonStatus: status,
    lastComparisonAt: new Date(),
  });

  const comp = await saveComparison({
    uuid: erpCfdi.uuid,
    erpCfdiId,
    satCfdiId: satCfdi?._id,
    status,
    differences,
    criticalCount,
    warningCount,
    satRawResponse: satResponse?.rawResponse ?? null,
    triggeredBy,
    sessionId,
    hasLocalSATCopy: !!satCfdi,
    ejercicio,
    periodo,
    tipoDeComprobante: erpCfdi.tipoDeComprobante ?? undefined,
  });

  // Recuperar resoluciones previas para este UUID antes de borrar
  // Clave: "field|erpValue|satValue" → resolución previa
  const prevResolved = await Discrepancy.find({
    uuid: erpCfdi.uuid,
    status: { $in: ['resolved', 'ignored', 'accepted'] },
  }).lean();

  const resolutionMap = new Map();
  for (const d of prevResolved) {
    // Extraer el campo de la descripción guardada o usar type como fallback
    const key = `${d.type}|${d.erpValue}|${d.satValue}`;
    resolutionMap.set(key, {
      status: d.status,
      resolutionType: d.resolutionType,
      notes: d.notes,
      resolvedAt: d.resolvedAt,
      resolvedBy: d.resolvedBy,
    });
  }

  // Eliminar discrepancias abiertas previas del mismo UUID para evitar duplicados entre sesiones
  await Discrepancy.deleteMany({
    uuid: erpCfdi.uuid,
    status: { $nin: ['resolved', 'ignored', 'accepted'] },
  });

  if (differences.length > 0) {
    await Promise.all(differences.map(diff => {
      const type = diff.type || mapDiffToType(diff.field);
      const erpVal = String(diff.erpValue ?? '');
      const satVal = String(diff.satValue ?? '');
      const key = `${type}|${erpVal}|${satVal}`;
      const prev = resolutionMap.get(key);

      return saveDiscrepancy({
        comparisonId: comp._id,
        uuid: erpCfdi.uuid,
        type,
        severity: diff.severity,
        description: `Campo '${diff.field}': ERP="${erpVal}", SAT="${satVal}"`,
        erpValue: erpVal,
        satValue: satVal,
        rfcEmisor: erpCfdi.emisor.rfc,
        rfcReceptor: erpCfdi.receptor.rfc,
        fiscalImpact: diff.fiscalImpact,
        ejercicio,
        periodo,
        tipoDeComprobante: erpCfdi.tipoDeComprobante ?? undefined,
        satStatus: erpCfdi.satStatus ?? undefined,
        // Heredar resolución previa si la diferencia exacta ya fue atendida
        ...(prev && {
          status: prev.status,
          resolutionType: prev.resolutionType,
          notes: prev.notes,
          resolvedAt: prev.resolvedAt,
          resolvedBy: prev.resolvedBy,
        }),
      });
    }));
  }

  return comp;
};

// ── Comparadores ──────────────────────────────────────────────────────────────

/**
 * Comparación específica para CFDIs tipo P (Complemento de Pago).
 * Compara el MontoTotalPagos del complemento en lugar del total raíz (que es 0 por spec SAT).
 * También verifica FormaDePagoP y MonedaP del primer pago.
 */
/**
 * Extrae el monto total de un CFDI tipo P.
 * Prioridad: complementoPago.totales > suma de pagos > cfdi.total (fallback Excel)
 */
const montoTotalPago = (cfdi) => {
  const cp = cfdi.complementoPago;
  if (cp?.totales?.montoTotalPagos != null) return cp.totales.montoTotalPagos;
  if (cp?.pagos?.length) return cp.pagos.reduce((s, p) => s + (p.monto ?? 0), 0);
  // Fallback: Excel guarda el monto en cfdi.total para tipo P
  return cfdi.total ?? 0;
};

/**
 * Comparación específica para CFDIs tipo P (Complemento de Pago).
 * Soporta tres casos:
 *   ERP enriquecido + SAT con XML   → compara complementoPago vs complementoPago
 *   ERP enriquecido + SAT de Excel  → compara complementoPago.monto vs sat.total
 *   Ninguno con complemento         → compara cfdi.total vs cfdi.total (ambos de Excel)
 */
const compareComplementoPago = (erp, sat) => {
  const diffs = [];

  const erpMonto = montoTotalPago(erp);
  const satMonto = montoTotalPago(sat);

  // Comparar monto total
  if (Math.abs(erpMonto - satMonto) > TOLERANCE_AMOUNT) {
    diffs.push({
      field: 'complementoPago.montoTotalPagos',
      erpValue: erpMonto,
      satValue: satMonto,
      severity: 'critical',
      type: 'AMOUNT_MISMATCH',
      fiscalImpact: { amount: Math.abs(erpMonto - satMonto), currency: erp.moneda || 'MXN' },
    });
  }

  // Comparar moneda y forma de pago solo si ambos tienen complemento estructurado
  const erpCP = erp.complementoPago;
  const satCP = sat.complementoPago;
  if (erpCP && satCP) {
    const erpMonedaP = erpCP.pagos?.[0]?.monedaP || 'MXN';
    const satMonedaP = satCP.pagos?.[0]?.monedaP || 'MXN';
    if (erpMonedaP !== satMonedaP) {
      diffs.push({ field: 'complementoPago.monedaP', erpValue: erpMonedaP, satValue: satMonedaP, severity: 'warning' });
    }

    const erpFP = normCodigo(erpCP.pagos?.[0]?.formaDePagoP);
    const satFP = normCodigo(satCP.pagos?.[0]?.formaDePagoP);
    if (erpFP && satFP && erpFP !== satFP) {
      diffs.push({ field: 'complementoPago.formaDePagoP', erpValue: erpFP, satValue: satFP, severity: 'warning' });
    }
  }

  return diffs;
};

const compareAmounts = (erp, sat) => {
  const diffs = [];
  for (const [field, severity] of [['total', 'critical'], ['subTotal', 'warning'], ['descuento', 'warning']]) {
    const erpVal = erp[field] ?? 0;
    const satVal = sat[field] ?? 0;
    if (Math.abs(erpVal - satVal) > TOLERANCE_AMOUNT) {
      diffs.push({
        field,
        erpValue: erpVal,
        satValue: satVal,
        severity,
        fiscalImpact: { amount: Math.abs(erpVal - satVal), currency: erp.moneda || 'MXN' },
      });
    }
  }
  return diffs;
};

const compareParties = (erp, sat) => {
  const diffs = [];
  // RFC (crítico)
  if (erp.emisor.rfc !== sat.emisor.rfc)
    diffs.push({ field: 'emisor.rfc', erpValue: erp.emisor.rfc, satValue: sat.emisor.rfc, severity: 'critical' });
  if (erp.receptor.rfc !== sat.receptor.rfc)
    diffs.push({ field: 'receptor.rfc', erpValue: erp.receptor.rfc, satValue: sat.receptor.rfc, severity: 'critical' });
  // Régimen fiscal (advertencia)
  if ((erp.emisor.regimenFiscal || '') !== (sat.emisor.regimenFiscal || ''))
    diffs.push({ field: 'emisor.regimenFiscal', erpValue: erp.emisor.regimenFiscal, satValue: sat.emisor.regimenFiscal, severity: 'warning' });
  // Nombres (advertencia — pueden diferir por abreviaturas)
  if ((erp.emisor.nombre || '').toUpperCase().trim() !== (sat.emisor.nombre || '').toUpperCase().trim() &&
      erp.emisor.nombre && sat.emisor.nombre)
    diffs.push({ field: 'emisor.nombre', erpValue: erp.emisor.nombre, satValue: sat.emisor.nombre, severity: 'warning' });
  if ((erp.receptor.nombre || '').toUpperCase().trim() !== (sat.receptor.nombre || '').toUpperCase().trim() &&
      erp.receptor.nombre && sat.receptor.nombre)
    diffs.push({ field: 'receptor.nombre', erpValue: erp.receptor.nombre, satValue: sat.receptor.nombre, severity: 'warning' });
  // Uso CFDI — comparar solo el código (antes del guión)
  // "G02 - Devoluciones..." → "G02"
  const erpUso = normCodigo(erp.receptor.usoCFDI);
  const satUso = normCodigo(sat.receptor.usoCFDI);
  if (erpUso && satUso && erpUso !== satUso)
    diffs.push({ field: 'receptor.usoCFDI', erpValue: erp.receptor.usoCFDI, satValue: sat.receptor.usoCFDI, severity: 'warning' });
  return diffs;
};

// Extrae YYYY-MM-DD usando hora local (evita desfase UTC en zonas horarias negativas)
const toLocalYMD = (d) => {
  const dt = new Date(d);
  const y  = dt.getFullYear();
  const m  = String(dt.getMonth() + 1).padStart(2, '0');
  const day = String(dt.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
};

const compareDates = (erp, sat) => {
  const erpDate = toLocalYMD(erp.fecha);
  const satDate = toLocalYMD(sat.fecha);
  if (erpDate !== satDate)
    return [{ field: 'fecha', erpValue: erpDate, satValue: satDate, severity: 'warning' }];
  return [];
};

const compareTaxes = (erp, sat) => {
  const diffs = [];
  const erpTras = erp.impuestos?.totalImpuestosTrasladados ?? 0;
  const satTras = sat.impuestos?.totalImpuestosTrasladados ?? 0;
  // Solo comparar trasladados si el SAT tiene un valor positivo (0 puede indicar
  // que la columna estaba ausente en el reporte Excel, no que realmente no hay IVA).
  const difTras = Math.abs(erpTras - satTras);
  if (satTras > 0 && difTras > 0) {
    diffs.push({
      field: 'impuestos.totalImpuestosTrasladados',
      erpValue: erpTras,
      satValue: satTras,
      severity: difTras <= TOLERANCE_AMOUNT + 0.0001 ? 'warning' : 'critical',
      fiscalImpact: { amount: difTras, currency: erp.moneda || 'MXN', taxType: 'IVA' },
    });
  }
  const erpRet = erp.impuestos?.totalImpuestosRetenidos ?? 0;
  const satRet = sat.impuestos?.totalImpuestosRetenidos ?? 0;
  const difRet = Math.abs(erpRet - satRet);
  if (satRet > 0 && difRet > 0) {
    diffs.push({
      field: 'impuestos.totalImpuestosRetenidos',
      erpValue: erpRet,
      satValue: satRet,
      severity: difRet <= TOLERANCE_AMOUNT + 0.0001 ? 'warning' : 'critical',
      fiscalImpact: { amount: difRet, currency: erp.moneda || 'MXN', taxType: 'Retención' },
    });
  }
  return diffs;
};

// Normaliza códigos de catálogo SAT que algunos sistemas guardan con descripción:
// "PPD-Pago en parcialidades o diferido" → "PPD"
// "99-Por definir"                       → "99"
// "G02 - Devoluciones..."               → "G02"
// Valores sin guión (ya normalizados) se devuelven igual.
const normCodigo = (v) => (v || '').toString().trim().split('-')[0].trim();

const compareGeneralFields = (erp, sat) => {
  const diffs = [];
  const esPago = erp.tipoDeComprobante === 'P';

  // Tipo de comprobante (crítico — I/E/T/N/P)
  // Si el lado SAT viene null (Excel con tipo no reconocido), no se marca diferencia —
  // se asume que el valor del ERP es el correcto.
  const erpTipo = erp.tipoDeComprobante || '';
  const satTipo = sat.tipoDeComprobante || '';
  if (satTipo && erpTipo !== satTipo)
    diffs.push({ field: 'tipoDeComprobante', erpValue: erp.tipoDeComprobante, satValue: sat.tipoDeComprobante, severity: 'critical' });

  // Moneda (crítico — afecta conversión)
  // Para tipo P, el SAT exige moneda="XXX" en el comprobante raíz; no comparar contra SAT Excel
  // que puede traer "MXN". La moneda real del pago está en complementoPago.pagos[n].monedaP.
  if (!esPago && (erp.moneda || 'MXN') !== (sat.moneda || 'MXN'))
    diffs.push({ field: 'moneda', erpValue: erp.moneda, satValue: sat.moneda, severity: 'critical' });

  // Tipo de cambio (advertencia si moneda != MXN, no aplica a P)
  if (!esPago && (erp.moneda || 'MXN') !== 'MXN') {
    const erpTC = erp.tipoCambio ?? 1;
    const satTC = sat.tipoCambio ?? 1;
    if (Math.abs(erpTC - satTC) > 0.0001)
      diffs.push({ field: 'tipoCambio', erpValue: erpTC, satValue: satTC, severity: 'warning' });
  }

  // Forma de pago — no aplica a tipo P (la forma de pago está en complementoPago.pagos[n].formaDePagoP)
  if (!esPago) {
    const erpFP = normCodigo(erp.formaPago);
    const satFP = normCodigo(sat.formaPago);
    if (erpFP && satFP && erpFP !== satFP)
      diffs.push({ field: 'formaPago', erpValue: erp.formaPago, satValue: sat.formaPago, severity: 'warning' });
  }

  // Método de pago — no aplica a tipo P
  if (!esPago) {
    const erpMP = normCodigo(erp.metodoPago);
    const satMP = normCodigo(sat.metodoPago);
    if (erpMP && satMP && erpMP !== satMP)
      diffs.push({ field: 'metodoPago', erpValue: erp.metodoPago, satValue: sat.metodoPago, severity: 'warning' });
  }

  // Versión CFDI (advertencia)
  if ((erp.version || '4.0') !== (sat.version || '4.0'))
    diffs.push({ field: 'version', erpValue: erp.version, satValue: sat.version, severity: 'warning' });

  return diffs;
};

// ── Helpers ───────────────────────────────────────────────────────────────────

const updateSATStatus = async (cfdi, state) => {
  cfdi.satStatus = ['Vigente', 'Cancelado', 'No Encontrado', 'Pendiente', 'Error', 'Expresión Inválida', 'Desconocido'].includes(state)
    ? state : 'Error';
  cfdi.satLastCheck = new Date();
  await cfdi.save();
};

const saveComparison = async (data) => {
  const payload = { ...data, totalDifferences: data.differences?.length || 0, comparedAt: new Date() };

  if (data.sessionId) {
    // Batch con sesión → insertar siempre para preservar historial
    const doc = await Comparison.create(payload);
    logger.info(`[Comparison] Guardada ${doc._id} para UUID ${data.uuid} en sesión ${data.sessionId}`);
    return doc;
  }

  // Comparación individual → upsert
  return Comparison.findOneAndUpdate(
    { uuid: data.uuid, sessionId: null },
    payload,
    { upsert: true, new: true }
  );
};

const saveDiscrepancy = async (data) => Discrepancy.create(data);

const mapDiffToType = (field) => {
  if (field.includes('rfc'))             return 'RFC_MISMATCH';
  if (field === 'total' || field === 'subTotal' || field === 'descuento') return 'AMOUNT_MISMATCH';
  if (field.includes('impuesto'))        return 'TAX_CALCULATION_ERROR';
  if (field === 'fecha')                 return 'DATE_MISMATCH';
  if (field === 'tipoDeComprobante')     return 'OTHER';
  if (field === 'moneda' || field === 'tipoCambio') return 'AMOUNT_MISMATCH';
  if (field === 'version')               return 'CFDI_VERSION_MISMATCH';
  if (field.includes('regimenFiscal'))   return 'REGIME_MISMATCH';
  return 'OTHER';
};

/**
 * Registra un CFDI que existe en SAT pero no tiene contraparte en ERP.
 */
const compareSATOnlyCFDI = async (satCfdiId, options = {}) => {
  const satCfdi = await CFDI.findById(satCfdiId);
  if (!satCfdi) throw new Error(`CFDI SAT no encontrado: ${satCfdiId}`);

  const cfdiDate  = new Date(satCfdi.fecha);
  const ejercicio = satCfdi.ejercicio ?? cfdiDate.getFullYear();
  const periodo   = satCfdi.periodo   ?? (cfdiDate.getMonth() + 1);

  const diff = {
    field: 'erp.uuid',
    erpValue: 'No registrado en ERP',
    satValue: satCfdi.uuid,
    severity: 'critical',
    type: 'MISSING_IN_ERP',
  };

  const comp = await saveComparison({
    uuid:           satCfdi.uuid,
    satCfdiId:      satCfdi._id,
    status:         'not_in_erp',
    differences:    [diff],
    criticalCount:  1,
    warningCount:   0,
    triggeredBy:    options.triggeredBy,
    sessionId:      options.sessionId ?? null,
    hasLocalSATCopy: true,
    ejercicio,
    periodo,
  });

  // Eliminar discrepancias abiertas previas del mismo UUID para evitar duplicados
  await Discrepancy.deleteMany({
    uuid: satCfdi.uuid,
    type: 'MISSING_IN_ERP',
    status: { $nin: ['resolved', 'ignored', 'accepted'] },
  });

  await saveDiscrepancy({
    comparisonId: comp._id,
    uuid:         satCfdi.uuid,
    type:         'MISSING_IN_ERP',
    severity:     'critical',
    description:  'CFDI existe en SAT/MANUAL pero no fue encontrado en ERP',
    erpValue:     'No registrado',
    satValue:     satCfdi.uuid,
    rfcEmisor:    satCfdi.emisor?.rfc ?? '',
    rfcReceptor:  satCfdi.receptor?.rfc ?? '',
    ejercicio,
    periodo,
  });

  return comp;
};

const batchCompareCFDIs = async (erpCfdiIds, options = {}) => {
  const satOnlyIds    = options.satOnlyIds ?? [];
  const totalCFDIs    = erpCfdiIds.length + satOnlyIds.length;

  // Si ya viene sessionId del route, usarlo directo. Si no, crear una nueva sesión.
  const sessionId = options.sessionId || (await ComparisonSession.create({
    name: formatSessionName(new Date()),
    triggeredBy: options.triggeredBy,
    totalCFDIs,
    status: 'running',
    filters: options.filters,
  }))._id;

  logger.info(`[Batch] Iniciando sesión ${sessionId} con ${erpCfdiIds.length} ERP + ${satOnlyIds.length} solo-SAT`);

  const results = { success: 0, failed: 0, discrepancies: 0, errors: [], sessionId };
  const statusCounts = { match: 0, discrepancy: 0, not_in_sat: 0, not_in_erp: 0, cancelled: 0, error: 0 };
  const concurrency = options.concurrency || 5;

  // ── 1. Comparar CFDIs ERP ──────────────────────────────────────────────────
  for (let i = 0; i < erpCfdiIds.length; i += concurrency) {
    const chunk = erpCfdiIds.slice(i, i + concurrency);
    await Promise.all(chunk.map(id =>
      compareCFDI(id, { ...options, sessionId })
        .then(comp => {
          results.success++;
          statusCounts[comp.status] = (statusCounts[comp.status] || 0) + 1;
          if (['discrepancy', 'not_in_sat', 'cancelled'].includes(comp.status)) results.discrepancies++;
        })
        .catch(err => {
          results.failed++;
          statusCounts.error++;
          results.errors.push({ id, error: err.message });
          logger.error(`Error en comparación ERP CFDI ${id}: ${err?.stack || err?.message || String(err)}`);
        })
    ));
    if (i + concurrency < erpCfdiIds.length) await new Promise(r => setTimeout(r, 1000));
  }

  // ── 2. Registrar CFDIs solo en SAT ────────────────────────────────────────
  for (let i = 0; i < satOnlyIds.length; i += concurrency) {
    const chunk = satOnlyIds.slice(i, i + concurrency);
    await Promise.all(chunk.map(id =>
      compareSATOnlyCFDI(id, { ...options, sessionId })
        .then(() => {
          results.success++;
          statusCounts.not_in_erp++;
          results.discrepancies++;
        })
        .catch(err => {
          results.failed++;
          statusCounts.error++;
          results.errors.push({ id, error: err.message });
          logger.error(`Error en SAT-only CFDI ${id}: ${err?.stack || err?.message || String(err)}`);
        })
    ));
  }

  await ComparisonSession.findByIdAndUpdate(sessionId, {
    status: results.failed === totalCFDIs ? 'failed' : 'completed',
    processed:   results.success,
    failedCount: results.failed,
    completedAt: new Date(),
    results:     statusCounts,
  });

  logger.info(`[Batch] Sesión ${sessionId} completada: ${results.success} ok, ${results.failed} error`);
  return results;
};

/**
 * Compara dos arrays de CFDIs normalizados (con UUID como llave) y retorna
 * las diferencias agrupadas por categoría.
 *
 * Se usa para el módulo de Descarga Masiva: compara lo que el SAT reporta
 * contra lo que el ERP tiene registrado.
 *
 * @param {object[]} cfdisSAT — array de CFDIs normalizados provenientes del SAT
 * @param {object[]} cfdisERP — array de CFDIs normalizados provenientes del ERP
 * @returns {{
 *   coinciden: object[],
 *   soloEnSAT: object[],
 *   soloEnERP: object[],
 *   conDiferencia: Array<{sat: object, erp: object, diferencias: object[]}>
 * }}
 */
const compararArrays = (cfdisSAT, cfdisERP) => {
  const mapSAT = new Map(cfdisSAT.map(c => [c.uuid.toUpperCase(), c]));
  const mapERP = new Map(cfdisERP.map(c => [c.uuid.toUpperCase(), c]));

  const coinciden = [];
  const soloEnSAT = [];
  const soloEnERP = [];
  const conDiferencia = [];

  // CFDIs en SAT: buscar coincidencias y diferencias
  for (const [uuid, sat] of mapSAT) {
    if (!mapERP.has(uuid)) {
      soloEnSAT.push(sat);
      continue;
    }

    const erp = mapERP.get(uuid);
    const diferencias = detectarDiferencias(sat, erp);

    if (diferencias.length === 0) {
      coinciden.push(sat);
    } else {
      conDiferencia.push({ sat, erp, diferencias });
    }
  }

  // CFDIs en ERP que no están en SAT
  for (const [uuid, erp] of mapERP) {
    if (!mapSAT.has(uuid)) {
      soloEnERP.push(erp);
    }
  }

  return { coinciden, soloEnSAT, soloEnERP, conDiferencia };
};

/**
 * Detecta diferencias campo a campo entre un CFDI del SAT y uno del ERP.
 * @returns {Array<{campo: string, valorSAT: any, valorERP: any}>}
 */
const detectarDiferencias = (sat, erp) => {
  const diffs = [];

  const camposNumericos = [['total', 0.01], ['subtotal', 0.01]];
  for (const [campo, tolerancia] of camposNumericos) {
    if (Math.abs((sat[campo] || 0) - (erp[campo] || 0)) > tolerancia) {
      diffs.push({ campo, valorSAT: sat[campo], valorERP: erp[campo] });
    }
  }

  const camposTexto = ['rfcEmisor', 'rfcReceptor', 'moneda', 'tipoComprobante'];
  for (const campo of camposTexto) {
    if ((sat[campo] || '') !== (erp[campo] || '')) {
      diffs.push({ campo, valorSAT: sat[campo], valorERP: erp[campo] });
    }
  }

  // Fecha: comparar solo la parte de la fecha (YYYY-MM-DD)
  const fechaSAT = sat.fecha ? new Date(sat.fecha).toISOString().slice(0, 10) : null;
  const fechaERP = erp.fecha ? new Date(erp.fecha).toISOString().slice(0, 10) : null;
  if (fechaSAT !== fechaERP) {
    diffs.push({ campo: 'fecha', valorSAT: fechaSAT, valorERP: fechaERP });
  }

  return diffs;
};

module.exports = { compareCFDI, batchCompareCFDIs, compararArrays, formatSessionName };
