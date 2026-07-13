'use strict';

/**
 * paddle-ocr.service.js — Motor 1 de OCR: PaddleOCR (PP-OCRv6) vía ONNX Runtime.
 *
 * 100% Node.js, sin Python ni microservicios externos — corre embebido en el
 * mismo proceso del backend. Los modelos (~25MB) se descargan una sola vez
 * a ~/.cache/ppu-paddle-ocr (o se hornean en la imagen Docker, ver Dockerfile)
 * y no requieren red después de eso. Gratuito y sin límite de cuota.
 */

const { withTimeout } = require('../../../shared/utils/with-timeout');
const { logger } = require('../../../shared/utils/logger');

// require() perezoso, NO al nivel de módulo: `ppu-paddle-ocr` se publica como
// ESM puro ("type": "module", sin build CJS) — cargarlo aquí arriba rompería
// CUALQUIER caller que solo requiera este archivo bajo un runtime que no
// interopera ESM/CJS igual que Node (ej. Jest sin transform de Babel para
// este paquete). Node interopera bien y lo carga sin problema; diferirlo
// hasta el primer uso real evita pagar ese costo (y ese riesgo) para quien
// nunca llega a usar Paddle (ej. el camino de pdf-parse, o tests que no
// ejercitan OCR de imágenes).
let _ppuPaddleOcr = null;
function _getPpuPaddleOcr() {
  if (!_ppuPaddleOcr) _ppuPaddleOcr = require('ppu-paddle-ocr');
  return _ppuPaddleOcr;
}

// El reconocimiento real toma <1s por imagen (ver benchmark en memoria del proyecto) —
// 30s solo cubre un cuelgue real del motor (imagen corrupta, sesión ONNX atorada).
const RECOGNIZE_TIMEOUT_MS = 30000;

// Nombre legible del modelo activo — busca la clave del preset (ej. "v6-small")
// que coincide con DEFAULT_MODEL; si en el futuro DEFAULT_MODEL cambia a algo
// fuera de MODEL_PRESETS, cae al nombre del archivo .ort de detección.
function _nombreModelo(model, MODEL_PRESETS) {
  const key = Object.entries(MODEL_PRESETS).find(([, v]) => v === model)?.[0];
  if (key) return key;
  return (model.detection || '').split('/').pop() || 'desconocido';
}

// Singleton perezoso — evita el costo de inicializar la sesión ONNX (~300ms)
// en cada comprobante. Si la inicialización falla (ej. red caída en la
// primera descarga del modelo), se limpia la promesa para reintentar en la
// siguiente llamada en lugar de dejar el motor muerto para siempre.
let _servicePromise = null;

function getService() {
  if (!_servicePromise) {
    _servicePromise = (async () => {
      const { PaddleOcrService, DEFAULT_MODEL, MODEL_PRESETS } = _getPpuPaddleOcr();
      const svc = new PaddleOcrService({ model: DEFAULT_MODEL });
      await svc.initialize();
      console.log(`[PaddleOCR] Motor OCR activo: ${_nombreModelo(DEFAULT_MODEL, MODEL_PRESETS)} (ONNX Runtime, embebido, sin Python)`);
      return svc;
    })().catch(err => {
      _servicePromise = null;
      throw err;
    });
  }
  return _servicePromise;
}

function toArrayBuffer(buf) {
  return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
}

/**
 * Reconoce texto en una imagen (JPEG/PNG/WEBP — no PDF).
 *
 * Devuelve { text, lines, confidence } en el mismo formato que ya consumen
 * normalizeOcrText/extractAllFields/extractFieldsFromLines en receipt.service.js:
 * `lines` es un arreglo plano de { text } (uno por línea detectada).
 *
 * result.lines de ppu-paddle-ocr viene agrupado por línea, donde cada línea es
 * a su vez un arreglo de cajas OCR individuales — se aplanan uniendo el texto
 * de cada caja con espacio.
 */
async function recognize(imageBuffer) {
  const svc = await getService();

  const result = await withTimeout(
    svc.recognize(toArrayBuffer(imageBuffer)),
    RECOGNIZE_TIMEOUT_MS,
    'PaddleOCR.recognize',
    () => {
      logger.error('[PaddleOCR] recognize() excedió el timeout — se descarta la sesión y se reinicia en la próxima llamada');
      _servicePromise = null;
      svc.destroy().catch(() => {});
    },
  );

  const lines = (result.lines || []).map(group => ({
    text: group.map(box => box.text).join(' '),
  }));

  return { text: result.text || '', lines, confidence: result.confidence || 0 };
}

module.exports = { recognize };
