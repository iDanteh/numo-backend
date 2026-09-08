'use strict';

// Tests unitarios de los extractores de ocr-engine.js — no dependen de OCR real
// (Paddle no carga bajo Jest, ver receipt.service.test.js) ni de archivos externos.
// Cubren los 4 fixes del comprobante Mercado Pago (2026-08-17) más al menos un
// caso de regresión por fix con un formato YA soportado, para no romper sin
// darnos cuenta algo que ya andaba bien.

const {
  normalizeOcrText,
  extractMonto,
  extractHora,
  extractReferencia,
  extractFieldsFromLines,
  extractNumeroAutorizacion,
  extractMontoFromLines,
} = require('./ocr-engine');

describe('normalizeOcrText — centavos pegados a la moneda sin espacio (Mercado Pago)', () => {
  test('"2,99546MXN" inserta el punto decimal antes de la moneda pegada', () => {
    expect(normalizeOcrText('$ 2,99546MXN')).toContain('2,995.46');
  });

  test('regresión: "5,15943" (bug original 2026-07-29, sin sufijo de moneda) sigue funcionando', () => {
    expect(normalizeOcrText('$5,15943')).toContain('5,159.43');
  });

  test('regresión: un folio largo sin comas no se toca', () => {
    expect(normalizeOcrText('Comprobante\n#172880688243')).toContain('172880688243');
  });
});

describe('extractMonto — centavos pegados a la moneda ("2,995.46MXN" ya normalizado)', () => {
  test('extrae el monto completo aunque "MXN" quede pegado sin espacio', () => {
    expect(extractMonto('$ 2,995.46MXN')).toBe(2995.46);
  });

  test('regresión: "$100.50" (con espacio, formato normal) sigue igual', () => {
    expect(extractMonto('$100.50')).toBe(100.5);
  });

  test('regresión: "$1,200" sin decimales sigue igual', () => {
    expect(extractMonto('$1,200')).toBe(1200);
  });

  test('regresión: etiqueta "Total transferido: 15,000.00" (E1) sigue igual', () => {
    expect(extractMonto('Total transferido: 15,000.00')).toBe(15000);
  });
});

describe('extractHora — hora sin cero a la izquierda (Mercado Pago: "a las 9:00")', () => {
  test('"9:00" se extrae y se normaliza a "09:00"', () => {
    expect(extractHora('14/agosto/2026 a las 9:00.')).toBe('09:00');
  });

  test('regresión: "14:30" (2 dígitos, formato ya soportado) sigue igual', () => {
    expect(extractHora('Hora: 14:30')).toBe('14:30');
  });

  test('regresión: "23:59" (límite superior) sigue igual', () => {
    expect(extractHora('23:59')).toBe('23:59');
  });

  test('no confunde un fragmento tipo "5:1" (minutos inválidos) con una hora', () => {
    expect(extractHora('proporción 5:1 de prueba')).toBeNull();
  });
});

describe('extractReferencia — etiqueta "Comprobante" (Mercado Pago)', () => {
  test('"Comprobante\\n#172880688243" extrae el folio', () => {
    expect(extractReferencia('Comprobante\n#172880688243')).toBe('172880688243');
  });

  test('no confunde el título "Comprobante de transferencia" (encabezado, no es la etiqueta del folio)', () => {
    const texto = 'Comprobante de transferencia\n14/agosto/2026 a las 9:00.\n...\nComprobante\n#172880688243';
    expect(extractReferencia(texto)).toBe('172880688243');
  });

  test('regresión: "Folio: 123456" (formato ya soportado) sigue igual', () => {
    expect(extractReferencia('Folio: 123456')).toBe('123456');
  });

  test('regresión: "Referencia numérica: 987654321" (Vault México) sigue igual', () => {
    expect(extractReferencia('Referencia numérica: 987654321')).toBe('987654321');
  });
});

describe('extractFieldsFromLines — encabezado combinado "Origen y destino" (Mercado Pago)', () => {
  const asLines = texts => texts.map(text => ({ text }));

  test('resuelve titularOrigen/titularDestino sin etiqueta "De:"/"Para:" por parte', () => {
    const lines = asLines([
      'mercado pago WALLET',
      'Comprobante de transferencia',
      '14/agosto/2026 a las 9:00.',
      '$ 2,995.46MXN',
      'Origen y destino',
      'Claudia Camiro',
      'Mercado Pago Wallet',
      'CLABE: ****4125',
      'Car Comercializadora SA De CV',
      'BANAMEX',
      'CLABE: ****9717',
      'Comprobante',
      '#172880688243',
    ]);

    const result = extractFieldsFromLines(lines);
    expect(result.titularOrigen).toBe('CLAUDIA CAMIRO');
    expect(result.titularDestino).toBe('CAR COMERCIALIZADORA SA DE CV');
  });

  test('regresión: formato con etiquetas explícitas "De:"/"Para:" (Banorte/apps P2P) sigue igual y no se pisa con el fallback combinado', () => {
    const lines = asLines([
      'De',
      'Juan Pérez López',
      'Para',
      'María García Ruiz',
    ]);

    const result = extractFieldsFromLines(lines);
    expect(result.titularOrigen).toBe('JUAN PÉREZ LÓPEZ');
    expect(result.titularDestino).toBe('MARÍA GARCÍA RUIZ');
  });

  test('regresión: formato con etiquetas de sección BBVA-style ("Ordenante"/"Beneficiario") sigue igual', () => {
    const lines = asLines([
      'Ordenante',
      'JUAN PEREZ GARCIA',
      'Beneficiario',
      'MARIA LOPEZ SANCHEZ',
    ]);

    const result = extractFieldsFromLines(lines);
    expect(result.titularOrigen).toBe('JUAN PEREZ GARCIA');
    expect(result.titularDestino).toBe('MARIA LOPEZ SANCHEZ');
  });

  test('sin "Origen y destino" en el texto, no agrega nada por el fallback combinado', () => {
    const lines = asLines(['Monto', '100.00', 'Fecha', '01/01/2026']);
    const result = extractFieldsFromLines(lines);
    expect(result.titularOrigen).toBeUndefined();
    expect(result.titularDestino).toBeUndefined();
  });
});

describe('Ticket físico BBVA depósito en efectivo (bug real 2026-09-07, monto "8")', () => {
  // Texto REAL devuelto por PaddleOCR (motor primario, corrido contra la foto
  // real del ticket — no es una transcripción limpia) tras normalizeOcrText().
  // El comprobante trae $3,120.00 de IMPORTE, pero el OCR real NUNCA leyó esos
  // dígitos (glare/mano tapando la zona en la foto): la única huella que queda
  // es un "8" suelto pegado a "EFEC. DEPOSITADO" — antes del fix, extractMonto()
  // lo devolvía con confianza como si fuera el monto real.
  const clean = `CANN 25620 ULA 4
S
4
** BBVa mEXICO, S.A, **
RFc 88a830831lJ2
04/09/26.17:23 FECHA MM 4639 CAJERO
DEPOSITO EN EFECTIVO
CLIENTE: CAR COMERCIALIZADORA SA D
UBICADO EN:SUC TLAXIACO PC
FECHA DE APLICACION: 04/09/26
FOLIO NUMERO: 9785AUT: 796662
CUENTA/TARJETA DE ABONO:*********1014
IMPORTE:
8 EFEC. DEPOSITADO CAMBIO ENTREGADO 80.00 CAMBIO EN RECIBO MOTIVO DE PAGO: PAGO BILMA
UCCAEAT
800226 2663`;

  test('extractMonto NO confunde el "8" fusionado con EFEC. DEPOSITADO, ni el 80.00 de CAMBIO ENTREGADO, con IMPORTE — el monto real es irrecuperable de este OCR y debe dar null, nunca un valor falso', () => {
    expect(extractMonto(clean)).toBeNull();
  });

  // rawLines REAL devuelto por PaddleOCR.recognize() (paddle-ocr.service.js) para
  // este mismo ticket — capturado corriendo extractReceiptDataPaddle() contra la
  // foto real (TicketBug.jpeg), NO reconstruido a mano a partir de `clean`. Es la
  // forma que consume extractMontoFromLines(rawLines) en producción vía el `??`
  // de extractReceiptDataPaddle (ocr-engine.js ~línea 1596) cuando extractMonto(clean)
  // da null — el `clean` de arriba solo cubre extractMonto(), nunca esta ruta.
  // Hallazgo de revisión de confiabilidad 2026-09-07: sin este test, un bug latente
  // en extractMontoFromLines sobre la forma real de líneas (distinta al string
  // aplanado) podía resucitar el "8" o el "80.00" en producción con los 23/23
  // tests previos en verde, porque ninguno ejercitaba esta función.
  const rawLines = [
    { text: 'CANN 25620 ULA 4' },
    { text: 'S' },
    { text: '4' },
    { text: '** BBVa mEXICO, S.A, **' },
    { text: 'RFc 88a830831lJ2' },
    { text: '04/09/26 17:23 FECHA MM 4639 CAJERO' },
    { text: 'DEPOSITO EN EFECTIVO' },
    { text: 'CLIENTE: CAR COMERCIALIZADORA SA D' },
    { text: 'UBICADO EN:SUC TLAXIACO PC' },
    { text: 'FECHA DE APLICACION: 04/09/26' },
    { text: 'FOLIO NUMERO: 9785AUT: 796662' },
    { text: 'CUENTA/TARJETA DE ABONO:*********1014' },
    { text: 'IMPORTE:' },
    { text: '8 EFEC. DEPOSITADO CAMBIO ENTREGADO 80.00 CAMBIO EN RECIBO MOTIVO DE PAGO: PAGO BILMA' },
    { text: 'UCCAEAT' },
    { text: '800 226 2663' },
    { text: '' },
    { text: '八' },
  ];

  test('extractMontoFromLines(rawLines REAL) — la ruta que en producción realmente decide el monto vía "??" — también da null, no resucita el "8" ni el "80.00"', () => {
    expect(extractMontoFromLines(rawLines)).toBeNull();
  });

  test('extractReferencia extrae el folio pese a "FOLIO NUMERO:" (la palabra NUMERO ya no rompe el match)', () => {
    expect(extractReferencia(clean)).toBe('9785');
  });

  test('extractNumeroAutorizacion reconoce la abreviatura "AUT:" pegada sin espacio al folio anterior', () => {
    expect(extractNumeroAutorizacion(clean)).toBe('796662');
  });

  test('regresión: "AUT" no dispara falsos positivos dentro de "automático" ni pisa "autorización" ya soportada', () => {
    expect(extractNumeroAutorizacion('Proceso automático, sin datos')).toBeNull();
    expect(extractNumeroAutorizacion('Autorización: 1234567')).toBe('1234567');
  });
});
