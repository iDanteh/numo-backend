'use strict';

const CFDI        = require('../models/CFDI');
const { parseCFDI } = require('./cfdiParser');

/**
 * Repara en vuelo CFDIs con subTotal=0 que tengan xmlContent guardado.
 * Lee el XML original del SAT, extrae el SubTotal real, corrige en-memoria
 * y persiste en BD para que próximas consultas no necesiten re-parsear.
 * Tipo P se omite: su subTotal=0 en el header es legítimo por especificación SAT.
 *
 * @param {object[]} cfdis  — array de documentos CFDI (lean)
 */
async function repararSubtotalDesdeXml(cfdis) {
  const rotos = cfdis.filter(c => !c.subTotal && c.tipoDeComprobante !== 'P');
  if (!rotos.length) return;

  const ids     = rotos.map(c => c._id);
  const xmlDocs = await CFDI.find({ _id: { $in: ids } }).select('+xmlContent').lean();
  const xmlMap  = new Map(xmlDocs.map(d => [String(d._id), d.xmlContent]));

  for (const cfdi of rotos) {
    const xmlContent = xmlMap.get(String(cfdi._id));
    if (!xmlContent) continue;
    try {
      const parsed = await parseCFDI(xmlContent);
      if ((parsed.subTotal || 0) > 0) {
        cfdi.subTotal  = parsed.subTotal;
        cfdi.descuento = parsed.descuento;
        cfdi.impuestos = parsed.impuestos;
        CFDI.updateOne({ _id: cfdi._id }, {
          $set: {
            subTotal:       parsed.subTotal,
            descuento:      parsed.descuento,
            impuestos:      parsed.impuestos,
            origenDescarga: 'xml',
          },
        }).catch(() => {});
      }
    } catch (_) { /* XML malformado — el motor usará fallback total−iva */ }
  }
}

module.exports = { repararSubtotalDesdeXml };
