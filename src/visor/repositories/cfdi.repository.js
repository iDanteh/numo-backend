'use strict';

/**
 * CFDIRepository
 * --------------
 * Responsabilidad única: persistir y consultar documentos CFDI en MongoDB.
 */

const CFDI = require('../models/CFDI');

/** Código de error MongoDB para violación de índice único */
const MONGO_DUPLICATE_KEY = 11000;

/**
 * Prepara el objeto de actualización para un upsert ERP.
 *
 * Reglas:
 *  - Nunca incluir fileHash si es null/undefined — los documentos ERP no tienen
 *    archivo físico y el índice único solo debe aplicar a XMLs reales.
 *  - Si ya existe un fileHash real en el doc (caso excepcional), sí se incluye.
 */
const prepararSetData = (cfdiData) => {
  const setData = { ...cfdiData };
  if (setData.fileHash == null) {
    delete setData.fileHash; // campo ausente = no indexado por el índice parcial
  }
  return setData;
};

/**
 * Inserta o actualiza un CFDI proveniente del ERP.
 *
 * Clave única: { uuid, source: 'ERP' } — índice compuesto en el schema.
 *
 * Usa $set (NO reemplazo) para preservar campos como isActive que no vienen
 * del ERP. $setOnInsert garantiza isActive:true en documentos nuevos.
 *
 * @param {object} cfdiData  — Documento transformado por erp-transformer
 * @returns {Promise<{ isNew: boolean, isDuplicate: boolean }>}
 */
const upsertFromERP = async (cfdiData) => {
  const setData = prepararSetData(cfdiData);

  try {
    const previo = await CFDI.findOneAndUpdate(
      { uuid: cfdiData.uuid, source: 'ERP' },
      {
        $set:         setData,
        $setOnInsert: { isActive: true },
      },
      { upsert: true, new: false },
    );
    return { isNew: previo === null, isDuplicate: false };
  } catch (err) {
    // E11000 en fileHash_1 o en uuid+source = el documento ya existe.
    // Se trata como duplicado, no como error crítico.
    if (err.code === MONGO_DUPLICATE_KEY) {
      return { isNew: false, isDuplicate: true };
    }
    throw err; // cualquier otro error sí es crítico
  }
};

module.exports = { upsertFromERP };
