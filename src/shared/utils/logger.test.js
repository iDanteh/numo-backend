'use strict';

// Regresión del bug real encontrado 2026-08-17: `logger.warn('etiqueta:', variable)`
// perdía silenciosamente el 2do argumento — el log de producción mostraba literalmente
// "etiqueta:" sin el mensaje real, en 16 lugares del backend (incluido el error de OCR
// que motivó este fix, ver [[project_ocr_pdf_canvas_bug]]). Prueba `logFormat` directo
// (`.transform(info)`) para no depender de I/O real de archivo/consola.

const { logFormat } = require('./logger');

const SPLAT = Symbol.for('splat');

function formatMessage(level, message, extraArgs = []) {
  const info = { level, message, timestamp: '12:00:00', [SPLAT]: extraArgs };
  const transformed = logFormat.transform(info);
  return transformed[Symbol.for('message')];
}

describe('logFormat — argumento extra de logger.warn/error/info ya no se pierde', () => {
  test('un string como 2do argumento se agrega al mensaje (antes desaparecía)', () => {
    const out = formatMessage('warn', '[extractReceiptDataPdfScanned] renderPdfToImages falló:', ['Image or Canvas expected']);
    expect(out).toBe('12:00:00 [warn]: [extractReceiptDataPdfScanned] renderPdfToImages falló: Image or Canvas expected');
  });

  test('el .message de un Error real como 2do argumento también se agrega', () => {
    const err = new Error('el disco está lleno');
    const out = formatMessage('warn', '[analyzeStoredComprobantes] Comprobante #0 no se pudo leer:', [err.message]);
    expect(out).toContain('el disco está lleno');
  });

  test('un objeto Error completo (no solo .message) expande su .stack', () => {
    const err = new Error('fallo con stack');
    const out = formatMessage('error', 'Ocurrió un error:', [err]);
    expect(out).toContain('fallo con stack');
    expect(out).toContain('at '); // parte típica de un stack trace
  });

  test('un objeto plano se serializa con JSON.stringify', () => {
    const out = formatMessage('warn', 'Datos:', [{ statusCode: 422, campo: 'monto' }]);
    expect(out).toBe('12:00:00 [warn]: Datos: {"statusCode":422,"campo":"monto"}');
  });

  test('null/undefined como argumento extra NO imprime la palabra "undefined"/"null"', () => {
    const out = formatMessage('warn', 'Sin dato adicional:', [undefined]);
    expect(out).toBe('12:00:00 [warn]: Sin dato adicional:');
    expect(out).not.toContain('undefined');
  });

  test('regresión: un mensaje ya interpolado en template literal (sin argumento extra) sigue funcionando igual', () => {
    const out = formatMessage('info', `Ya interpolado: ${123}`, []);
    expect(out).toBe('12:00:00 [info]: Ya interpolado: 123');
  });

  test('múltiples argumentos extra se concatenan en orden', () => {
    const out = formatMessage('warn', 'Varios:', ['uno', 'dos', 'tres']);
    expect(out).toBe('12:00:00 [warn]: Varios: uno dos tres');
  });
});
