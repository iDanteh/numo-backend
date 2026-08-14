'use strict';

/**
 * sustitutos-cfdi.util.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Detección y clasificación de CFDIs sustitutos (tipoRelacion='04') compartida
 * entre cfdi-poliza-generator.service.js y balanza-preliminar.service.js.
 * Vive en su propio módulo para evitar el require circular entre esos dos
 * archivos (el generador ya requiere balanza-preliminar.service.js).
 */

const CFDI = require('../../../visor/models/CFDI');

// Algunos CFDIs fuente SAT traen dos UUIDs relacionados juntos en un solo
// string separados por espacio (o, en otras fuentes, por "|") en vez de dos
// entradas separadas — bug de datos real observado en producción (ej.
// "C1D75E31-... 1A1C9DBE-..." dentro de un mismo tipoRelacion='04'). Sin este
// split, ese string nunca hace match contra ningún UUID real y el sustituto
// nunca se detecta como riesgo.
const _splitUuids = (raw) => String(raw).split(/[\s|]+/).map(u => u.trim()).filter(Boolean);

// Mapa uuid(upper) → {ejercicio, periodo, tipoDeComprobante} de los CFDIs
// originales sustituidos. uuidsYaUsados solo detecta facturas contabilizadas
// DENTRO de Numo; una factura de meses anteriores contabilizada fuera de Numo
// (ej. directo en CONTPAQi) no deja rastro ahí — de ahí esta consulta aparte,
// que compara el periodo propio del original contra el periodo actual.
// tipoDeComprobante se usa para descartar falsos positivos (ver
// _enriquecerSustitutosConPeriodoOriginal).
async function _fetchPeriodosOriginales(uuidsOriginalesSet) {
  if (!uuidsOriginalesSet.size) return {};
  const rows = await CFDI.find({ uuid: { $in: [...uuidsOriginalesSet] } })
    .select('uuid ejercicio periodo tipoDeComprobante')
    .lean();
  return Object.fromEntries(
    rows.map(r => [(r.uuid || '').toUpperCase(), { ejercicio: r.ejercicio, periodo: r.periodo, tipoDeComprobante: r.tipoDeComprobante }]),
  );
}

// Extrae los CFDIs sustitutos (tipoRelacion='04') presentes en el batch —
// aplica a cualquier tipo de comprobante: una factura (I) puede sustituir a
// otra factura, no solo Egresos/Pagos. Todos se excluyen del cálculo
// automático — ver _particionarSustitutosPorRiesgo.
function _extraerSustitutos(cfdis) {
  return cfdis
    .filter(c => c.cfdiRelacionados?.some(r => r.tipoRelacion === '04'))
    .map(c => ({
      uuid:              c.uuid,
      serie:             c.serie ?? null,
      folio:             c.folio ?? null,
      fecha:             c.fecha,
      total:             c.total,
      tipoDeComprobante: c.tipoDeComprobante,
      sustituyeA: [...new Set(
        (c.cfdiRelacionados || [])
          .filter(r => r.tipoRelacion === '04')
          .flatMap(r => r.uuids ?? (r.uuid ? [r.uuid] : []))
          .flatMap(_splitUuids)
          .map(u => u.toUpperCase()),
      )],
    }));
}

// Enriquece cada sustituto con el periodo propio del/los CFDI(s) original(es)
// que reemplaza — para que el contador vea si es del mismo mes o de un
// periodo ya cerrado.
//
// Filtro de tipo coincidente: una sustitución real siempre reemplaza un CFDI
// del MISMO tipo (factura↔factura, pago↔pago, egreso↔egreso) — nunca cruza
// tipos. Esto descarta un bug de datos real y masivo: la fuente SAT etiqueta
// SIEMPRE como tipoRelacion='04' la relación normal REP→factura que en
// realidad es '08' (Factura generada por pagos en parcialidades) — 9,964
// CFDIs de pago fuente SAT tienen '04' y CERO tienen '08', mientras que ERP sí
// trae los 10,259 correctos con '08'. Sin este filtro, básicamente todo pago
// PPD se marcaría como "sustituto de su propia factura" (falso positivo).
// Si tras filtrar por tipo no queda ningún original válido, este CFDI no es
// realmente un sustituto y se descarta de la lista.
async function _enriquecerSustitutosConPeriodoOriginal(sustitutos) {
  if (!sustitutos.length) return sustitutos;
  const uuidsOriginales = new Set(sustitutos.flatMap(s => s.sustituyeA));
  const periodos = await _fetchPeriodosOriginales(uuidsOriginales);
  return sustitutos
    .map(s => {
      const originales = s.sustituyeA
        .map(uA => ({ uuid: uA, ...(periodos[uA] ?? {}) }))
        .filter(o => o.tipoDeComprobante == null || o.tipoDeComprobante === s.tipoDeComprobante);
      return {
        ...s,
        sustituyeA: originales.map(o => o.uuid),
        originales,
      };
    })
    .filter(s => s.originales.length > 0);
}

// Confirmado con el usuario 2026-08-13: cuando el/los CFDI(s) original(es)
// sustituido(s) son del MISMO periodo que se está generando (y se pudo
// determinar su periodo — si el original no se encontró en Mongo no hay nada
// que reversar), el par se contabiliza automático — el original con su
// asiento normal MÁS un asiento de reversión (ver `mismoPeriodo` más abajo y
// su uso en cfdi-poliza-generator.service.js), y el sustituto se contabiliza
// normal como cualquier otro CFDI. Esto aplica sin importar `motivo` (incluso
// si el original ya tiene póliza en Numo — también se reversa). Solo cuando
// el original es de un periodo YA CERRADO (`periodoAnterior`) se mantiene la
// política anterior: excluir ambos lados y dejar para revisión manual (hoja
// "CFDIs Sustitutos" / campo `sustitutos`), porque reabrir/reversar un
// periodo cerrado no es seguro hacerlo automático.
function _particionarSustitutosPorRiesgo(sustitutosEnriquecidos, { uuidsYaUsados, ejercicio, periodo }) {
  const excluidos = sustitutosEnriquecidos.map(s => {
    const yaEnNumo = s.sustituyeA.some(uA => uuidsYaUsados.has(uA));
    const periodoAnterior = s.originales.some(o =>
      o.ejercicio != null && o.periodo != null && (
        Number(o.ejercicio) < Number(ejercicio) ||
        (Number(o.ejercicio) === Number(ejercicio) && Number(o.periodo) < Number(periodo))
      ),
    );
    const mismoPeriodo = s.originales.length > 0 && s.originales.every(o =>
      o.ejercicio != null && o.periodo != null &&
      Number(o.ejercicio) === Number(ejercicio) && Number(o.periodo) === Number(periodo),
    );
    const motivo = yaEnNumo ? 'ya_contabilizado_en_numo' : periodoAnterior ? 'periodo_anterior' : 'sin_riesgo_detectado';
    return { ...s, motivo, mismoPeriodo };
  });
  return { excluidos, normales: [] };
}

module.exports = {
  _splitUuids,
  _fetchPeriodosOriginales,
  _extraerSustitutos,
  _enriquecerSustitutosConPeriodoOriginal,
  _particionarSustitutosPorRiesgo,
};
