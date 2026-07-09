'use strict';

/**
 * paddle-ocr.service.js — Motor 1 de OCR: PaddleOCR (PP-OCRv6) vía ONNX Runtime.
 *
 * 100% Node.js, sin Python ni microservicios externos — corre embebido en el
 * mismo proceso del backend. Los modelos (~25MB) se descargan una sola vez
 * a ~/.cache/ppu-paddle-ocr (o se hornean en la imagen Docker, ver Dockerfile)
 * y no requieren red después de eso. Gratuito y sin límite de cuota.
 */

const { PaddleOcrService, DEFAULT_MODEL, MODEL_PRESETS } = require('ppu-paddle-ocr');

// Nombre legible del modelo activo — busca la clave del preset (ej. "v6-small")
// que coincide con DEFAULT_MODEL; si en el futuro DEFAULT_MODEL cambia a algo
// fuera de MODEL_PRESETS, cae al nombre del archivo .ort de detección.
function _nombreModelo(model) {
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
      const svc = new PaddleOcrService({ model: DEFAULT_MODEL });
      await svc.initialize();
      console.log(`[PaddleOCR] Motor OCR activo: ${_nombreModelo(DEFAULT_MODEL)} (ONNX Runtime, embebido, sin Python)`);
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
  const svc    = await getService();
  const result = await svc.recognize(toArrayBuffer(imageBuffer));

  const lines = (result.lines || []).map(group => ({
    text: group.map(box => box.text).join(' '),
  }));

  return { text: result.text || '', lines, confidence: result.confidence || 0 };
}

module.exports = { recognize };
