const xml2js = require('xml2js');
const crypto = require('crypto');

/**
 * Parsea un XML de CFDI 3.3 o 4.0 a un objeto estructurado
 * compatible con el modelo CFDI de MongoDB.
 */
const parseCFDI = async (xmlString) => {
  const parser = new xml2js.Parser({
    explicitArray: false,
    ignoreAttrs: false,
    attrkey: '$',
    charkey: '_',
    mergeAttrs: true,
  });

  const result = await parser.parseStringPromise(xmlString);

  // El nodo raíz puede ser cfdi:Comprobante o tfd:TimbreFiscalDigital
  const comprobante = result['cfdi:Comprobante'] || result['Comprobante'];
  if (!comprobante) {
    throw new Error('XML no es un CFDI válido: nodo cfdi:Comprobante no encontrado');
  }

  const attrs = comprobante;
  const emisorNode = comprobante['cfdi:Emisor'] || comprobante['Emisor'] || {};
  const receptorNode = comprobante['cfdi:Receptor'] || comprobante['Receptor'] || {};
  const conceptosNode = comprobante['cfdi:Conceptos'] || comprobante['Conceptos'] || {};
  const impuestosNode = comprobante['cfdi:Impuestos'] || comprobante['Impuestos'] || {};
  const timbreNode = getTimbre(comprobante);
  const complementoPago = getComplementoPago(comprobante);

  const cfdiData = {
    uuid: timbreNode?.UUID || timbreNode?.['$']?.UUID || null,
    version: attrs.Version || attrs.version || '4.0',
    serie: attrs.Serie,
    folio: attrs.Folio,
    fecha: new Date(attrs.Fecha),
    sello: attrs.Sello,
    formaPago: attrs.FormaPago,
    noCertificado: attrs.NoCertificado,
    certificado: attrs.Certificado,
    condicionesDePago: attrs.CondicionesDePago,
    subTotal: parseFloat(attrs.SubTotal) || 0,
    descuento: parseFloat(attrs.Descuento) || 0,
    moneda: attrs.Moneda || 'MXN',
    tipoCambio: parseFloat(attrs.TipoCambio) || 1,
    total: parseFloat(attrs.Total) || 0,
    tipoDeComprobante: attrs.TipoDeComprobante,
    exportacion: attrs.Exportacion,
    metodoPago: attrs.MetodoPago,
    lugarExpedicion: attrs.LugarExpedicion,

    emisor: {
      rfc: emisorNode.Rfc || '',
      nombre: emisorNode.Nombre,
      regimenFiscal: emisorNode.RegimenFiscal,
    },

    receptor: {
      rfc: receptorNode.Rfc || '',
      nombre: receptorNode.Nombre,
      domicilioFiscalReceptor: receptorNode.DomicilioFiscalReceptor,
      regimenFiscal: receptorNode.RegimenFiscalReceptor,
      usoCFDI: receptorNode.UsoCFDI,
      residenciaFiscal: receptorNode.ResidenciaFiscal,
      numRegIdTrib: receptorNode.NumRegIdTrib,
    },

    conceptos: parseConceptos(conceptosNode),
    impuestos: parseImpuestos(impuestosNode),

    timbreFiscalDigital: timbreNode ? {
      uuid: timbreNode.UUID || timbreNode['$']?.UUID,
      fechaTimbrado: timbreNode.FechaTimbrado ? new Date(timbreNode.FechaTimbrado) : null,
      rfcProvCertif: timbreNode.RfcProvCertif,
      selloCFD: timbreNode.SelloCFD,
      noCertificadoSAT: timbreNode.NoCertificadoSAT,
      selloSAT: timbreNode.SelloSAT,
      version: timbreNode.Version,
    } : null,

    xmlContent: xmlString,
    xmlHash: crypto.createHash('sha256').update(xmlString).digest('hex'),
  };

  if (complementoPago) cfdiData.complementoPago = complementoPago;

  if (!cfdiData.uuid) {
    throw new Error('CFDI sin UUID (TimbreFiscalDigital no encontrado o UUID vacío)');
  }

  return cfdiData;
};

/**
 * Extrae el Complemento de Pago (pago20 o pago10) del nodo Complemento.
 * Soporta CFDI 4.0 (pago20) y 3.3 (pago10).
 * @returns {object|null}
 */
const getComplementoPago = (comprobante) => {
  try {
    const complemento = comprobante['cfdi:Complemento'] || comprobante['Complemento'];
    if (!complemento) return null;

    // CFDI 4.0 → pago20:Pagos  |  CFDI 3.3 → pago10:Pagos
    const pagosNode =
      complemento['pago20:Pagos'] ||
      complemento['pago10:Pagos'] ||
      complemento['Pagos'] ||
      null;

    if (!pagosNode) return null;

    // Versión del complemento
    const version = pagosNode['$']?.Version || pagosNode.Version || null;

    // Totales (solo pago20)
    const totalesNode = pagosNode['pago20:Totales'] || pagosNode['Totales'] || null;
    const totales = totalesNode
      ? { montoTotalPagos: parseFloat(totalesNode['$']?.MontoTotalPagos ?? totalesNode.MontoTotalPagos) || 0 }
      : undefined;

    // Pagos individuales
    const pagoRaw = pagosNode['pago20:Pago'] || pagosNode['pago10:Pago'] || pagosNode['Pago'] || null;
    if (!pagoRaw) return null;

    const pagoList = Array.isArray(pagoRaw) ? pagoRaw : [pagoRaw];

    const pagos = pagoList.map((p) => {
      const pa = p['$'] ? { ...p['$'], ...p } : p;

      // Documentos relacionados
      const drRaw = p['pago20:DoctoRelacionado'] || p['pago10:DoctoRelacionado'] || p['DoctoRelacionado'] || [];
      const drList = Array.isArray(drRaw) ? drRaw : [drRaw];

      const doctosRelacionados = drList
        .filter(Boolean)
        .map((dr) => {
          const d = dr['$'] ? { ...dr['$'], ...dr } : dr;
          return {
            idDocumento:      d.IdDocumento      || undefined,
            serie:            d.Serie            || undefined,
            folio:            d.Folio            || undefined,
            monedaDR:         d.MonedaDR         || 'MXN',
            tipoCambioDR:     parseFloat(d.TipoCambioDR) || undefined,
            metodoDePagoDR:   d.MetodoDePagoDR   || undefined,
            numParcialidad:   parseInt(d.NumParcialidad) || undefined,
            impSaldoAnt:      parseFloat(d.ImpSaldoAnt)      || undefined,
            impPagado:        parseFloat(d.ImpPagado)        || undefined,
            impSaldoInsoluto: parseFloat(d.ImpSaldoInsoluto) || undefined,
          };
        });

      return {
        fechaPago:    pa.FechaPago ? new Date(pa.FechaPago) : undefined,
        formaDePagoP: pa.FormaDePagoP || undefined,
        monedaP:      pa.MonedaP     || 'MXN',
        tipoCambioP:  parseFloat(pa.TipoCambioP) || undefined,
        monto:        parseFloat(pa.Monto) || 0,
        numOperacion: pa.NumOperacion || undefined,
        doctosRelacionados: doctosRelacionados.length ? doctosRelacionados : undefined,
      };
    });

    return { version, pagos, totales };
  } catch {
    return null;
  }
};

const getTimbre = (comprobante) => {
  try {
    const complemento = comprobante['cfdi:Complemento'] || comprobante['Complemento'];
    if (!complemento) return null;
    const tfd = complemento['tfd:TimbreFiscalDigital'] || complemento['TimbreFiscalDigital'];
    if (!tfd) return null;
    return tfd['$'] ? { ...tfd['$'] } : tfd;
  } catch {
    return null;
  }
};

const parseConceptos = (conceptosNode) => {
  if (!conceptosNode) return [];
  const concepto = conceptosNode['cfdi:Concepto'] || conceptosNode['Concepto'];
  if (!concepto) return [];
  const list = Array.isArray(concepto) ? concepto : [concepto];

  return list.map((c) => ({
    claveProdServ: c.ClaveProdServ,
    noIdentificacion: c.NoIdentificacion,
    cantidad: parseFloat(c.Cantidad) || 0,
    claveUnidad: c.ClaveUnidad,
    unidad: c.Unidad,
    descripcion: c.Descripcion,
    valorUnitario: parseFloat(c.ValorUnitario) || 0,
    importe: parseFloat(c.Importe) || 0,
    descuento: parseFloat(c.Descuento) || 0,
    objetoImp: c.ObjetoImp,
  }));
};

const parseImpuestos = (impuestosNode) => {
  if (!impuestosNode) return { totalImpuestosTrasladados: 0, totalImpuestosRetenidos: 0 };
  return {
    totalImpuestosTrasladados: parseFloat(impuestosNode.TotalImpuestosTrasladados) || 0,
    totalImpuestosRetenidos: parseFloat(impuestosNode.TotalImpuestosRetenidos) || 0,
  };
};

/**
 * Normaliza cualquier objeto CFDI (ya parseado o documento MongoDB) al esquema
 * mínimo uniforme para comparación entre SAT y ERP.
 *
 * @param {object} cfdi — resultado de parseCFDI() o documento CFDI de MongoDB
 * @returns {{
 *   uuid: string,
 *   serie: string,
 *   folio: string,
 *   fecha: Date,
 *   rfcEmisor: string,
 *   nombreEmisor: string,
 *   rfcReceptor: string,
 *   nombreReceptor: string,
 *   subtotal: number,
 *   total: number,
 *   moneda: string,
 *   tipoComprobante: string,
 *   estatus: string
 * }}
 */
const normalizarCFDI = (cfdi) => {
  const imp = cfdi.impuestos ?? {};
  const cp  = cfdi.complementoPago ?? {};
  const montoTotalPagos =
    cp.totales?.montoTotalPagos ??
    (Array.isArray(cp.pagos) ? cp.pagos.reduce((s, p) => s + (p.monto ?? 0), 0) : null) ??
    null;

  return {
    uuid:                  (cfdi.uuid || '').toUpperCase().trim(),
    serie:                 cfdi.serie || '',
    folio:                 cfdi.folio || '',
    fecha:                 cfdi.fecha ? new Date(cfdi.fecha) : null,
    rfcEmisor:             (cfdi.rfcEmisor || cfdi.emisor?.rfc || '').toUpperCase().trim(),
    nombreEmisor:          cfdi.nombreEmisor || cfdi.emisor?.nombre || '',
    rfcReceptor:           (cfdi.rfcReceptor || cfdi.receptor?.rfc || '').toUpperCase().trim(),
    nombreReceptor:        cfdi.nombreReceptor || cfdi.receptor?.nombre || '',
    subtotal:              parseFloat(cfdi.subtotal ?? cfdi.subTotal ?? 0),
    total:                 parseFloat(cfdi.total ?? 0),
    moneda:                cfdi.moneda || 'MXN',
    tipoComprobante:       cfdi.tipoComprobante || cfdi.tipoDeComprobante || '',
    estatus:               cfdi.estatus || cfdi.satStatus || 'Pendiente',
    ivaTrasladadoTotal:    parseFloat(imp.totalImpuestosTrasladados ?? 0),
    ivaRetenidoTotal:      parseFloat(imp.totalImpuestosRetenidos   ?? 0),
    montoTotalPagos:       montoTotalPagos !== null ? parseFloat(montoTotalPagos) : null,
  };
};

module.exports = { parseCFDI, normalizarCFDI };
