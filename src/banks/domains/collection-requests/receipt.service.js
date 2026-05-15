'use strict';

/**
 * receiptService.js — Extracción de datos de comprobantes de transferencia
 *
 * Motor 1 (primario) : Gemini 2.0 Flash — entiende imagen/PDF directamente,
 *                      devuelve JSON estructurado sin regex.
 *                      Requiere GEMINI_API_KEY en .env (gratuito hasta 1,500 req/día).
 * Motor 2 (secundario): Google Cloud Vision API — DOCUMENT_TEXT_DETECTION
 *                      Usa el service account ya configurado para Drive.
 * Motor 3 (fallback)  : Tesseract.js — completamente local, sin dependencias externas.
 *
 * PDFs : Gemini y Vision los leen de forma nativa (base64).
 *        Si ambos fallan, se intenta pdf-parse para texto embebido.
 */

const Tesseract    = require('tesseract.js');
const BankMovement = require('../banks/BankMovement.model');

// ── Gemini (lazy) ────────────────────────────────────────────────────────────
const GEMINI_PROMPT = `Eres un extractor experto de datos de comprobantes de pago bancarios mexicanos.

Analiza el comprobante adjunto (transferencia, recibo, ticket, factura) y devuelve ÚNICAMENTE un objeto JSON. Sin texto adicional, sin bloques markdown.

{
  "monto": <número decimal sin símbolo de moneda — ej: 1500.20>,
  "fecha": "<YYYY-MM-DD>",
  "hora": "<HH:MM> o null",
  "titularOrigen": "<NOMBRE EN MAYÚSCULAS del remitente/ordenante> o null",
  "titularDestino": "<NOMBRE EN MAYÚSCULAS del beneficiario/destinatario> o null",
  "bancoOrigen": "<banco que envía> o null",
  "bancoDestino": "<banco que recibe> o null",
  "claveRastreo": "<clave alfanumérica SPEI, 18-30 caracteres> o null",
  "referencia": "<número de referencia, folio u operación> o null",
  "numeroAutorizacion": "<código de autorización o aprobación> o null",
  "clabe": "<CLABE interbancaria 18 dígitos> o null",
  "cuentaOrigenUltimos4": "<últimos 4 dígitos de cuenta origen> o null",
  "cuentaDestinoUltimos4": "<últimos 4 dígitos de cuenta destino> o null",
  "concepto": "<descripción o motivo del pago> o null"
}

Reglas:
- Formato MXN: coma separa miles (1,500 = 1500), punto separa decimales (1,500.20 = 1500.20)
- Si ves superíndice o decimales en renglón separado ("1,500" + "20"), el monto es 1500.20
- El banco del encabezado suele ser el banco destino (quien generó el comprobante)
- La clave SPEI suele iniciar con letras del banco (BBVAMEX..., BNAM..., HDNX...)
- Solo 4 dígitos para cuentaOrigenUltimos4 y cuentaDestinoUltimos4
- null si no encuentras el campo con certeza`;

// ── Google Cloud Vision (lazy) ───────────────────────────────────────────────
let _visionClient = null;
function getVisionClient() {
  if (_visionClient) return _visionClient;
  const keyPath = (process.env.GOOGLE_SERVICE_ACCOUNT_KEY_PATH || '').trim();
  if (!keyPath) {
    throw new Error('GOOGLE_SERVICE_ACCOUNT_KEY_PATH no está configurado — Vision desactivado');
  }
  const fs = require('fs');
  if (!fs.existsSync(keyPath)) {
    throw new Error(`Archivo de credenciales no encontrado: "${keyPath}" — Vision desactivado`);
  }
  const { ImageAnnotatorClient } = require('@google-cloud/vision');
  _visionClient = new ImageAnnotatorClient({ keyFilename: keyPath });
  return _visionClient;
}

const DATE_WINDOW_DAYS = 30;
const FALLBACK_WINDOW  = 90;
const SUPPORTED_MIME   = ['image/jpeg', 'image/png', 'image/webp', 'image/gif', 'application/pdf'];

// ── Catálogo de bancos ────────────────────────────────────────────────────────

const BANCOS_MAP = [
  { pattern: /CITIBANAMEX|CITI\s*BANAMEX/i, nombre: 'Banamex'    },
  { pattern: /BANAMEX/i,                     nombre: 'Banamex'    },
  { pattern: /BBVA\s*BANCOMER|BBVA/i,        nombre: 'BBVA'       },
  { pattern: /SANTANDER/i,                   nombre: 'Santander'  },
  { pattern: /BANORTE/i,                     nombre: 'Banorte'    },
  { pattern: /HSBC/i,                        nombre: 'HSBC'       },
  { pattern: /AZTECA/i,                      nombre: 'Azteca'     },
  { pattern: /INBURSA/i,                     nombre: 'Inbursa'    },
  { pattern: /SCOTIABANK/i,                  nombre: 'Scotiabank' },
  { pattern: /BANBAJ[IÍ]O|BAJIO/i,          nombre: 'BanBajío'   },
  { pattern: /AFIRME/i,                      nombre: 'Afirme'     },
  { pattern: /INTERCAM/i,                    nombre: 'Intercam'   },
  { pattern: /NU\s*BANK|NUBANK|NU\b/i,       nombre: 'Nu'         },
  { pattern: /SPIN\s*BY\s*OXXO|SPIN/i,       nombre: 'Spin'       },
  { pattern: /HEY\s*BANCO|HEY\b/i,           nombre: 'Hey Banco'  },
  { pattern: /ALBO/i,                        nombre: 'Albo'       },
];

const MESES_ES = {
  enero:1, febrero:2, marzo:3, abril:4, mayo:5, junio:6,
  julio:7, agosto:8, septiembre:9, octubre:10, noviembre:11, diciembre:12,
};
// Abreviaturas de 3 letras — usadas en comprobantes Banamex ("13 may 2026")
const MESES_ABBR = {
  ene:1, feb:2, mar:3, abr:4, may:5, jun:6,
  jul:7, ago:8, sep:9, oct:10, nov:11, dic:12,
};

// ════════════════════════════════════════════════════════════════════════════
// MOTOR 1 — TESSERACT  (workers singleton — se inicializan una vez y se reusan)
// ════════════════════════════════════════════════════════════════════════════

/**
 * Workers persistentes: evitan el overhead de ~2-4s por llamada de crear/destruir
 * procesos de Tesseract. Se inicializan la primera vez y permanecen vivos mientras
 * el servidor esté corriendo. Tesseract.js encola internamente las llamadas
 * concurrentes a recognize(), por lo que es seguro reutilizarlos.
 *
 * _workerFullPromise  — spa+eng, PSM 4 (SINGLE_COLUMN): recibos verticales (apps bancarias).
 * _workerBlockPromise — spa+eng, PSM 6 (SINGLE_BLOCK):  recibos con pares label:valor en fila,
 *                       PDFs y capturas de pantalla con layout más amplio.
 * _workerNumsPromise  — eng,     PSM 11 (SPARSE_TEXT):  barrido numérico de montos.
 */
let _workerFullPromise  = null;
let _workerBlockPromise = null;
let _workerNumsPromise  = null;

function getFullWorker() {
  if (!_workerFullPromise) {
    _workerFullPromise = (async () => {
      // OEM 1 = LSTM_ONLY: motor neuronal puro, más preciso que el motor clásico (OEM 0)
      const w = await Tesseract.createWorker(['spa', 'eng'], 1, { logger: () => {} });
      await w.setParameters({
        tessedit_pageseg_mode:     Tesseract.PSM.SINGLE_COLUMN,  // PSM 4 — columna única
        tessedit_char_whitelist:   '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzÁÉÍÓÚáéíóúÑñ$.,/:- ',
        preserve_interword_spaces: '1',
        user_defined_dpi:          '300', // evita "Invalid resolution XX dpi" en imágenes sin metadata DPI
      });
      return w;
    })();
  }
  return _workerFullPromise;
}

function getBlockWorker() {
  if (!_workerBlockPromise) {
    _workerBlockPromise = (async () => {
      const w = await Tesseract.createWorker(['spa', 'eng'], 1, { logger: () => {} });
      await w.setParameters({
        // PSM 6 (SINGLE_BLOCK): trata la imagen como un bloque uniforme de texto.
        // Captura mejor los recibos con layout horizontal (label izquierda, valor derecha),
        // PDFs con múltiples columnas y screenshots de apps con secciones anchas.
        tessedit_pageseg_mode:     Tesseract.PSM.SINGLE_BLOCK,
        tessedit_char_whitelist:   '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzÁÉÍÓÚáéíóúÑñ$.,/:- ',
        preserve_interword_spaces: '1',
        user_defined_dpi:          '300',
      });
      return w;
    })();
  }
  return _workerBlockPromise;
}

function getNumsWorker() {
  if (!_workerNumsPromise) {
    _workerNumsPromise = (async () => {
      const w = await Tesseract.createWorker(['eng'], 1, { logger: () => {} });
      await w.setParameters({
        // PSM 11 (SPARSE_TEXT): busca texto disperso sin asumir layout uniforme.
        // Más adecuado que PSM 6 (SINGLE_BLOCK) para encontrar montos sueltos en recibos.
        tessedit_pageseg_mode:     Tesseract.PSM.SPARSE_TEXT,
        tessedit_char_whitelist:   '0123456789$.,: ',
        preserve_interword_spaces: '1',
        user_defined_dpi:          '300',
      });
      return w;
    })();
  }
  return _workerNumsPromise;
}

async function runOCR(imageBuffer, mimeType = 'image/jpeg') {
  // PSM 4 = columna única — layout real de recibos bancarios.
  const worker  = await getFullWorker();
  const dataUrl = `data:${mimeType};base64,${imageBuffer.toString('base64')}`;
  const { data: { text, confidence } } = await worker.recognize(dataUrl);
  return { text, confidence };
}

async function runOCRBlock(imageBuffer, mimeType = 'image/jpeg') {
  // PSM 6 = bloque uniforme — layouts horizontales, PDFs y capturas de pantalla anchas.
  const worker  = await getBlockWorker();
  const dataUrl = `data:${mimeType};base64,${imageBuffer.toString('base64')}`;
  const { data: { text, confidence } } = await worker.recognize(dataUrl);
  return { text, confidence };
}

/**
 * Segunda pasada OCR con whitelist exclusiva de dígitos.
 * Elimina la ambigüedad O↔0 e l/I↔1 que Tesseract comete en fuentes serif.
 * Usa PSM 11 (SPARSE_TEXT) para capturar montos dispersos sin importar su posición.
 */
async function runOCRAmounts(imageBuffer, mimeType = 'image/jpeg') {
  const worker  = await getNumsWorker();
  const dataUrl = `data:${mimeType};base64,${imageBuffer.toString('base64')}`;
  const { data: { text } } = await worker.recognize(dataUrl);
  return text;
}

/**
 * Pre-procesa el texto OCR para corregir artefactos comunes:
 *
 *  1. Formato europeo: 1.500,00 → 1500.00
 *  2. Miles con espacio: 1 500.00 → 1500.00
 *  3. Decimales superíndice: "1,500 20" → "1,500.20"
 *     (Tesseract lee el superíndice como texto suelto pegado al número)
 */
function normalizeOcrText(raw) {
  let t = raw
    .replace(/[|¡¿]/g, '')
    .replace(/[ \t]{2,}/g, ' ')
    .replace(/\r\n/g, '\n')
    .trim();

  // 0. Corregir confusiones clásicas de Tesseract en contextos numéricos:
  //    O/o → 0  y  l/I → 1  cuando están rodeados de dígitos o separadores de número.
  //    Se aplica ANTES de las normalizaciones de formato para no interferir con ellas.
  t = t
    // "1O5" → "105", "2o0" → "200"  (O/o entre dígitos)
    .replace(/(\d)[Oo](\d)/g, '$10$2')
    // "1,O00" → "1,000"  (O/o después de coma de miles)
    .replace(/,([Oo])(\d{2})\b/g, ',0$2')
    // "l50" / "I50" → "150"  (l/I al inicio de un número)
    .replace(/\b([lI])(\d)/g, '1$2')
    // "15l" / "15I" → "151"  (l/I al final de un número)
    .replace(/(\d)([lI])\b/g, '$11')
    // "1.5OO" → "1.500"  (O/o después de punto decimal o en bloque de dígitos)
    .replace(/(\d\.\d*)[Oo](\d*)/g, (_, a, b) => `${a}0${b}`);

  // 1. Formato europeo: 1.500,00 → 1500.00
  t = t.replace(/(\d{1,3})\.(\d{3}),(\d{2})\b/g, (_, a, b, c) => `${a}${b}.${c}`);

  // 2. Miles separados por espacio: "1 500.00" → "1500.00"
  t = t.replace(/(\d{1,3}) (\d{3})(?=[.,\s]|\b)/g, (_, a, b) => `${a}${b}`);

  // 3. Decimales superíndice — patrón: número grande seguido de exactamente
  //    2 dígitos separados por espacio que parecen centavos (00-99).
  //    Solo aplica cuando el número entero es > 9 (no confundir con "15 20" como dos datos).
  //    Ejemplos: "1,500 20" → "1,500.20" / "750 00" → "750.00"
  t = t.replace(
    /(\b\d{1,3}(?:,\d{3})*)\s+(0\d|[1-9]\d)\b(?!\s*[\d,])/g,
    (match, integer, cents) => {
      const intVal = parseInt(integer.replace(/,/g, ''), 10);
      // Solo aplica a montos plausibles (≥ 10 para evitar ambigüedades)
      return intVal >= 10 ? `${integer}.${cents}` : match;
    }
  );

  // 4. Superíndice pegado sin espacio: "150020" podría ser "1500.20"
  //    Solo aplica con etiqueta de monto inmediatamente antes (muy restrictivo)
  t = t.replace(
    /(?:monto|importe|total|enviado|transferido)[:\s]*\$?\s*(\d{3,6})(0\d|[1-9]\d)\b(?!\d)/gi,
    (match, integer, cents) => {
      const label = match.split(integer)[0];
      return `${label}${integer}.${cents}`;
    }
  );

  // 5. PaddleOCR — decimal en línea siguiente: "1,500\n20" o "1500\n20" → "1,500.20"
  //    BUG FRECUENTE: el paso 2 ya eliminó los espacios de "1 500" → "1500",
  //    entonces el patrón debe aceptar dígitos con O SIN separador de miles.
  t = t.replace(
    /(\b[\d,]+)\n(0\d|[1-9]\d)\b(?!\d)/g,
    (match, integer, cents) => {
      const intVal = parseInt(integer.replace(/,/g, ''), 10);
      return intVal >= 10 && intVal < 100_000_000 ? `${integer}.${cents}` : match;
    }
  );

  // 6. PaddleOCR — punto decimal en línea siguiente: "1,500\n.20" → "1,500.20"
  t = t.replace(/(\b[\d,]+)\n(\.\d{2})\b/g, (_, i, d) => `${i}${d}`);

  // 7. PaddleOCR — signo $ en línea propia: "$\n1,500.20" → "$1,500.20"
  t = t.replace(/\$\s*\n\s*([\d,]+(?:\.\d{1,2})?)/g, (_, n) => `$${n}`);

  return t;
}

// ── Parsers individuales (Tesseract) ─────────────────────────────────────────

function extractMonto(text) {
  let m;

  // E1: etiqueta explícita + número (con o sin $ y decimales)
  //     Acepta salto de línea entre la etiqueta y el monto (PaddleOCR devuelve bloques separados)
  m = text.match(
    /(?:monto|importe|cantidad|total\s*(?:transferido|a\s*pagar|pagado|enviado|de\s*pago))\s*[:\-]?\s*\n?\s*\$?\s*([\d,]+(?:\.\d{1,2})?)/i
  );
  if (m) { const v = parseFloat(m[1].replace(/,/g, '')); if (ok(v)) return v; }

  // E2: $ + número MXN (con o sin separador de miles)
  m = text.match(/\$\s*(\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?|\d{1,9}(?:\.\d{1,2})?)\b/);
  if (m) { const v = parseFloat(m[1].replace(/,/g, '')); if (ok(v)) return v; }

  // E3: prefijo MXN / MX$ / USD — solo en la misma línea (no cruzar \n)
  // Bug conocido: "8,165.99 MXN\n08 de mayo" matcheaba "MXN\n08" → monto=8
  m = text.match(/(?:MXN|MX\$|USD)[^\S\n]*([\d,]+(?:\.\d{1,2})?)\b/i);
  if (m) { const v = parseFloat(m[1].replace(/,/g, '')); if (ok(v)) return v; }

  // E4: número con coma como separador de miles: 15,000.00
  const c4 = [];
  const r4 = /\b(\d{1,3}(?:,\d{3})+(?:\.\d{1,2})?)\b/g;
  while ((m = r4.exec(text)) !== null) {
    const v = parseFloat(m[1].replace(/,/g, ''));
    if (ok(v)) c4.push(v);
  }
  if (c4.length > 0) return c4[0];

  // E5: decimal sin separador de miles: 1500.00, 750.50
  //     Mínimo 2 dígitos antes del punto; excluye años (2000-2099)
  const c5 = [];
  const r5 = /\b(\d{2,7}\.\d{2})\b/g;
  while ((m = r5.exec(text)) !== null) {
    const v = parseFloat(m[1]);
    if (v >= 10 && v < 100_000_000 && !(v >= 2000 && v <= 2099)) c5.push(v);
  }
  if (c5.length > 0) { c5.sort((a, b) => b - a); return c5[0]; }

  return null;
}

function ok(v) { return !isNaN(v) && v >= 1 && v < 100_000_000; }

/**
 * Extracción de monto directamente desde el array de líneas de PaddleOCR.
 * Úsalo como fallback cuando extractMonto(text) devuelve null.
 *
 * Estrategia:
 *  1. Busca una línea con etiqueta de monto → toma la siguiente línea como valor.
 *  2. Si la línea siguiente al valor tiene exactamente 2 dígitos, son los centavos.
 *  3. Si no hay etiqueta, busca cualquier línea que empiece con $ o MXN.
 */
function extractMontoFromLines(lines) {
  if (!lines || !lines.length) return null;

  const texts    = lines.map(l => (l.text || '').trim());
  const isLabel  = t => /^(monto|importe|cantidad|total(\s*(transferido|pagado|enviado|de\s*pago))?)\s*[:\-]?$/i.test(t);
  const isCents  = t => /^(0\d|[1-9]\d)$/.test(t);          // exactamente 2 dígitos
  const parseCur = t => {
    const v = parseFloat(t.replace(/^[$S]\s*/, '').replace(/^MXN\s*/i, '').replace(/,/g, ''));
    return ok(v) ? v : null;
  };

  // Paso 1 — etiqueta → valor en línea(s) siguientes
  for (let i = 0; i < texts.length; i++) {
    if (!isLabel(texts[i])) continue;

    for (let j = i + 1; j <= Math.min(i + 3, texts.length - 1); j++) {
      let amountTxt = texts[j];
      if (!amountTxt || isLabel(amountTxt)) break;

      // Centavos en la línea siguiente (superíndice detectado como bloque aparte)
      if (!amountTxt.includes('.') && j + 1 < texts.length && isCents(texts[j + 1])) {
        amountTxt = `${amountTxt}.${texts[j + 1]}`;
      }

      const v = parseCur(amountTxt);
      if (v) return v;
    }
  }

  // Paso 2 — cualquier línea que empiece con $ o MXN
  for (const t of texts) {
    if (/^[$S]\s*[\d,]+(?:\.\d{1,2})?$/.test(t) || /^MXN\s*[\d,]+/i.test(t)) {
      const v = parseCur(t);
      if (v) return v;
    }
  }

  return null;
}

/**
 * Extrae campos desde el array estructurado de líneas de PaddleOCR,
 * usando la estrategia etiqueta → siguiente línea(s).
 * Úsalo como fallback cuando los extractores de texto plano no encuentran el valor.
 */
function extractFieldsFromLines(lines) {
  if (!lines || !lines.length) return {};

  const texts = lines.map(l => (l.text || '').trim());
  const result = {};

  const labelMap = [
    {
      field: 'titularOrigen',
      re: /^(ordenante|remitente|emisor|nombre\s+del?\s+(emisor|ordenante|remitente))\s*[:\-]?$/i,
    },
    {
      field: 'titularDestino',
      re: /^(beneficiario|destinatario|receptor|nombre\s+del?\s+(beneficiario|receptor|destinatario)|nombre|para)\s*[:\-]?$/i,
    },
    {
      field: 'claveRastreo',
      re: /^(clave\s+(de\s+)?rastreo|rastreo\s+spei|tracking\s*(key|id)?|folio\s+[úu]nico)\s*[:\-]?$/i,
    },
    {
      field: 'referencia',
      re: /^(referencia|folio(?:\s+de\s+operaci[oó]n)?|folio\s+[úu]nico|no\.?\s*operaci[oó]n|n[úu]mero\s+de\s+operaci[oó]n|confirmaci[oó]n|contrato)\s*[:\-]?$/i,
    },
    {
      field: 'numeroAutorizacion',
      re: /^(autorizaci[oó]n|c[oó]digo\s+(de\s+)?autorizaci[oó]n|no\.?\s*autorizaci[oó]n|aprobaci[oó]n)\s*[:\-]?$/i,
    },
    {
      field: 'clabe',
      re: /^(clabe(\s+interbancaria)?|cuenta\s+clabe)\s*[:\-]?$/i,
    },
    {
      field: 'concepto',
      re: /^(concepto|descripci[oó]n|motivo|leyenda|referencia\s+de\s+pago)\s*[:\-]?$/i,
    },
    {
      field: 'fecha',
      re: /^(fecha(\s+(de\s+)?(operaci[oó]n|transferencia|pago|movimiento|env[ií]o))?)\s*[:\-]?$/i,
    },
    {
      field: 'hora',
      re: /^(hora(\s+(de\s+)?(operaci[oó]n|pago|env[ií]o))?)\s*[:\-]?$/i,
    },
    {
      field: 'cuentaOrigen',
      re: /^(cuenta\s+(de\s+)?origen|de\s+cuenta|cuenta\s+remitente|cuenta\s+origen)\s*[:\-]?$/i,
    },
    {
      field: 'cuentaDestino',
      re: /^(cuenta\s+(de\s+)?destino|cuenta\s+beneficiario|cuenta\s+destino)\s*[:\-]?$/i,
    },
    {
      field: 'bancoOrigen',
      re: /^(banco\s+(de\s+)?origen|banco\s+emisor|banco\s+remitente)\s*[:\-]?$/i,
    },
    {
      field: 'bancoDestino',
      re: /^(banco\s+(de\s+)?destino|banco\s+beneficiario|banco\s+receptor)\s*[:\-]?$/i,
    },
  ];

  // Patrón genérico de "esto es una etiqueta conocida" (para no tomar otra etiqueta como valor)
  const isAnyLabel = t =>
    labelMap.some(({ re }) => re.test(t)) ||
    /^(monto|importe|total|cantidad|tipo\s+de\s+movimiento|spei|transferencia|pago)\s*[:\-]?$/i.test(t);

  for (let i = 0; i < texts.length; i++) {
    const t = texts[i];
    for (const { field, re } of labelMap) {
      if (result[field]) continue;
      if (!re.test(t)) continue;

      // Buscar valor en las siguientes líneas (hasta 3)
      for (let j = i + 1; j <= Math.min(i + 3, texts.length - 1); j++) {
        const val = texts[j];
        if (!val || isAnyLabel(val)) break;
        result[field] = val;
        break;
      }
    }
  }

  // Post-procesamiento de campos extraídos
  if (result.titularOrigen)
    result.titularOrigen = result.titularOrigen.trim().toUpperCase().slice(0, 60);
  if (result.titularDestino)
    result.titularDestino = result.titularDestino.trim().toUpperCase().slice(0, 60);
  if (result.claveRastreo)
    result.claveRastreo = result.claveRastreo.replace(/\s/g, '').toUpperCase();

  // Últimos 4 dígitos de cuenta
  if (result.cuentaOrigen) {
    const m = result.cuentaOrigen.match(/(\d{4})\s*$/);
    result.cuentaOrigenUltimos4 = m ? m[1] : extractUltimos4(result.cuentaOrigen);
  }
  if (result.cuentaDestino) {
    const m = result.cuentaDestino.match(/(\d{4})\s*$/);
    result.cuentaDestinoUltimos4 = m ? m[1] : extractUltimos4(result.cuentaDestino);
  }

  // Parsear fecha desde el valor extraído
  if (result.fecha) {
    const parsed = extractFecha(result.fecha);
    if (parsed) result.fecha = parsed;
  }

  // Detectar banco desde fragmento de texto
  if (result.bancoOrigen) result.bancoOrigen = detectarBanco(result.bancoOrigen) ?? result.bancoOrigen;
  if (result.bancoDestino) result.bancoDestino = detectarBanco(result.bancoDestino) ?? result.bancoDestino;

  return result;
}

function extractFecha(text) {
  let m;

  m = text.match(/\b(\d{1,2})\/(\d{1,2})\/(20\d{2})\b/);
  if (m) { const [,d,mo,y]=m; return `${y}-${mo.padStart(2,'0')}-${d.padStart(2,'0')}`; }

  m = text.match(/\b(20\d{2})-(\d{2})-(\d{2})\b/);
  if (m) return m[0];

  // Formato completo con o sin "de": "03 de marzo de 2026" / "03 marzo 2026"
  m = text.match(/(\d{1,2})\s+(?:de\s+)?(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+(?:de\s+)?(20\d{2})/i);
  if (m) {
    const mes = MESES_ES[m[2].toLowerCase()];
    if (mes) return `${m[3]}-${String(mes).padStart(2,'0')}-${m[1].padStart(2,'0')}`;
  }

  // Meses abreviados (3 letras): "13 may 2026", "13 ene 2026" — frecuente en Banamex y Banorte
  m = text.match(/(\d{1,2})\s+(ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)\b\w*\s+(?:de\s+)?(20\d{2})/i);
  if (m) {
    const mes = MESES_ABBR[m[2].toLowerCase().slice(0, 3)];
    if (mes) return `${m[3]}-${String(mes).padStart(2,'0')}-${m[1].padStart(2,'0')}`;
  }

  m = text.match(/\b(\d{2})\/(\d{2})\/(\d{2})\b/);
  if (m) { const y = parseInt(m[3])>50?`19${m[3]}`:`20${m[3]}`; return `${y}-${m[2]}-${m[1]}`; }

  m = text.match(/\b(\d{1,2})-(\d{1,2})-(20\d{2})\b/);
  if (m) { const [,d,mo,y]=m; return `${y}-${mo.padStart(2,'0')}-${d.padStart(2,'0')}`; }

  return null;
}

function extractHora(text) {
  const m = text.match(/\b([01]\d|2[0-3]):([0-5]\d)(?::[0-5]\d)?\b/);
  return m ? `${m[1]}:${m[2]}` : null;
}

function extractClaveRastreo(text) {
  // P1: etiqueta explícita (incluye "folio único" de BBVA mismo banco)
  let m = text.match(/(?:clave\s+(?:de\s+)?rastreo|rastreo\s*(?:spei)?|tracking\s*(?:key|id)?|folio\s+[úu]nico)[:\s#]*([A-Z0-9]{8,35})/i);
  if (m) return m[1].toUpperCase().replace(/\s/g, '');

  // P2: prefijo bancario SPEI (BBVAMEX..., BNAM..., HDNX...) + dígitos
  m = text.match(/\b([A-Z]{2,8}\d{8,22})\b/);
  if (m) return m[1];

  // P3: secuencia alfanumérica 18–35 chars con letras Y dígitos mezclados
  //     (cubre folios hex de 32 chars como los de BBVA mismo banco)
  m = text.match(/\b([A-Z0-9]{18,35})\b/);
  if (m && /[A-Z]/.test(m[1]) && /\d/.test(m[1])) return m[1];

  return null;
}

function extractReferencia(text) {
  // "folio(?:\s+de\s+operación)?" cubre tanto "Folio: X" como "Folio de operación\nX"
  // "[:\s#\n]*" usa \n explícito para cruzar línea cuando el valor está en la siguiente
  const m = text.match(
    /(?:referencia|folio(?:\s+de\s+operaci[oó]n)?|folio\s+[úu]nico|n[úu]mero\s+(?:de\s+)?(?:operaci[oó]n|confirmaci[oó]n|transacci[oó]n)|no\.?\s*op(?:eraci[oó]n)?|confirmaci[oó]n|contrato)[:\s#\n]*(\d{4,20})/i
  );
  return m ? m[1] : null;
}

function extractNumeroAutorizacion(text) {
  const m = text.match(
    /(?:autorizaci[oó]n|auth(?:orization)?|aprobaci[oó]n|c[oó]digo\s+(?:de\s+)?auth)[:\s#]*(\d{6,15})/i
  );
  return m ? m[1] : null;
}

function extractClabe(text) {
  let m = text.match(/(?:clabe|cuenta\s+clabe|clabe\s+interbancaria)[:\s]*(\d[\d\s]{16,20}\d)/i);
  if (m) { const d = m[1].replace(/\s/g,''); if (d.length===18) return d; }

  m = text.match(/\b(\d{18})\b/);
  if (m) return m[1];

  return null;
}

function detectarBanco(fragment) {
  if (!fragment) return null;
  for (const { pattern, nombre } of BANCOS_MAP) {
    if (pattern.test(fragment)) return nombre;
  }
  return null;
}

function extractBancos(text) {
  const lines = text.split('\n').map(l => l.trim()).filter(Boolean);
  const mid   = Math.floor(text.length / 2);
  const top   = text.slice(0, mid);
  const bot   = text.slice(mid);

  // E1: par explícito con etiquetas
  const pairM = text.match(
    /(?:origen|remitente|banco\s+emisor|banco\s+de\s+origen)[:\s]+([^\n]{2,40})\n[\s\S]{0,200}?(?:destino|beneficiario|banco\s+receptor|banco\s+destino)[:\s]+([^\n]{2,40})/i
  );
  if (pairM) {
    const bo = detectarBanco(pairM[1]), bd = detectarBanco(pairM[2]);
    if (bo || bd) return { bancoOrigen: bo, bancoDestino: bd };
  }

  // E2: secciones De/Para
  const deM   = text.match(/(?:^|\n)\s*(?:de|from)[:\s]+([^\n]{2,40})/i);
  const paraM = text.match(/(?:^|\n)\s*(?:para|to)[:\s]+([^\n]{2,40})/i);
  if (deM && paraM) {
    const bo = detectarBanco(deM[1]), bd = detectarBanco(paraM[1]);
    if (bo || bd) return { bancoOrigen: bo, bancoDestino: bd };
  }

  // E3: heurística posicional — el banco del encabezado es el banco destino
  //     (el comprobante lo genera la app del receptor del pago)
  const header      = lines.slice(0, 3).join(' ');
  const bancoHeader = detectarBanco(header);
  if (bancoHeader) {
    const resto = detectarBanco(bot.replace(header, '')) || detectarBanco(top);
    return {
      bancoOrigen:  resto !== bancoHeader ? resto : null,
      bancoDestino: bancoHeader,
    };
  }

  return { bancoOrigen: detectarBanco(top), bancoDestino: detectarBanco(bot) };
}

function extractUltimos4(text) {
  let m;

  // Bullet "•" / "·" — formato BBVA: "CUENTA • 14588", "•4352"
  // Toma los últimos 4 dígitos si el grupo capturado tiene 3–5 dígitos
  m = text.match(/[•·\u2022\u00b7]\s*(\d{3,5})\b/);
  if (m) return m[1].slice(-4);

  // Asteriscos dobles — formato Banamex: "Priority **546", "**120/971"
  m = text.match(/\*{2,4}\s*(\d{3,4})(?:\/\d+)?\b/);
  if (m) return m[1].slice(-4);

  // Asteriscos/X/puntos — formato estándar: "****1234", "XX1234"
  m = text.match(/[*Xx\.]{3,4}[\s-]?(\d{4})\b/);
  if (m) return m[1];

  m = text.match(/(?:termina(?:ndo)?|ending|últ(?:imos)?\.?)\s+(?:en\s+)?(\d{4})\b/i);
  if (m) return m[1];

  m = text.match(/(?:cuenta|clabe|n[úu]mero\s+de\s+cuenta)[:\s]+[\d\s]{6,}(\d{4})\b/i);
  if (m) return m[1];

  return null;
}

function extractTitular(text, role) {
  const labels = role === 'origen'
    ? ['ordenante','remitente','emisor','nombre del emisor','nombre de origen','nombre del ordenante']
    // "nombre" solo aplica a destino: en BBVA mismo banco, "Nombre: X" es el beneficiario
    : ['beneficiario','destinatario','receptor','nombre del receptor','nombre del beneficiario','nombre','para'];

  const re = new RegExp(
    `(?:${labels.join('|')})[:\\s]+([A-ZÁÉÍÓÚÑÜ][A-ZÁÉÍÓÚÑÜ\\s\\.]{3,60})`, 'i'
  );
  const m = text.match(re);
  return m ? m[1].split('\n')[0].trim().toUpperCase().slice(0, 60) : null;
}

// Etiquetas que NO deben tomarse como valor de concepto (evitar capturar la siguiente etiqueta)
const CONCEPTO_LABEL_BLACKLIST = /^(?:importe|monto|total|fecha|hora|titular|cuenta|banco|clave\s+rastreo|folio|rastreo|referencia|contrato|n[úu]mero|autorizaci[oó]n|tipo)\s*:?$/i;

function extractConcepto(text) {
  // Iterar todos los matches para saltar etiquetas falsas (ej. "Concepto:\nImporte:")
  const re = /(?:concepto|descripci[oó]n|motivo|referencia\s+de\s+pago|leyenda)[:\s]+([^\n]{3,100})/gi;
  let m;
  while ((m = re.exec(text)) !== null) {
    const val = m[1].trim();
    if (!CONCEPTO_LABEL_BLACKLIST.test(val)) return val.slice(0, 120);
  }
  return null;
}

function calcConfianza(fields) {
  const { monto, fecha, claveRastreo, referencia, bancoOrigen, bancoDestino, titularOrigen, titularDestino } = fields;
  let c = 0;
  if (monto)                           c += 40;
  if (fecha)                           c += 25;
  if (claveRastreo || referencia)      c += 20;
  if (bancoOrigen  || bancoDestino)    c += 10;
  if (titularOrigen || titularDestino) c += 5;
  return Math.min(c, 100);
}

/**
 * Extrae todos los campos de un texto OCR ya normalizado.
 * Se usa independientemente del PSM que generó el texto, permitiendo
 * reutilizar la misma lógica para PSM 4 y PSM 6.
 */
function extractAllFields(clean) {
  const lines = clean.split('\n');
  const half  = Math.floor(lines.length / 2);
  return {
    monto:                 extractMonto(clean),
    fecha:                 extractFecha(clean),
    hora:                  extractHora(clean),
    claveRastreo:          extractClaveRastreo(clean),
    referencia:            extractReferencia(clean),
    numeroAutorizacion:    extractNumeroAutorizacion(clean),
    clabe:                 extractClabe(clean),
    ...extractBancos(clean),
    cuentaOrigenUltimos4:  extractUltimos4(lines.slice(0, half).join('\n')),
    cuentaDestinoUltimos4: extractUltimos4(lines.slice(half).join('\n')),
    titularOrigen:         extractTitular(clean, 'origen'),
    titularDestino:        extractTitular(clean, 'destino'),
    concepto:              extractConcepto(clean),
  };
}

/**
 * Fusiona los campos extraídos por PSM 4 y PSM 6.
 *
 * Reglas de fusión:
 *  - Si solo uno tiene el campo → se usa ese.
 *  - monto: ambas pasadas suelen coincidir; si difieren, se prefiere PSM 4
 *    (validado y estable para recibos verticales).
 *  - Strings: se prefiere el valor más largo, ya que implica más información
 *    capturada (ej. titular completo vs truncado, clave de rastreo completa).
 *  - Campos de 4/8 dígitos (cuentaOrigenUltimos4, etc.): longitud fija → PSM 4.
 */
function mergeOcrFields(f4, f6) {
  const CAMPOS = [
    'monto', 'fecha', 'hora', 'claveRastreo', 'referencia',
    'numeroAutorizacion', 'clabe', 'bancoOrigen', 'bancoDestino',
    'cuentaOrigenUltimos4', 'cuentaDestinoUltimos4',
    'titularOrigen', 'titularDestino', 'concepto',
  ];
  const LONGITUD_FIJA = new Set(['cuentaOrigenUltimos4', 'cuentaDestinoUltimos4', 'fecha', 'hora']);

  const merged = {};
  for (const campo of CAMPOS) {
    const v4 = f4[campo] ?? null;
    const v6 = f6[campo] ?? null;

    if (v4 !== null && v6 === null)  { merged[campo] = v4; continue; }
    if (v4 === null && v6 !== null)  { merged[campo] = v6; continue; }
    if (v4 === null && v6 === null)  { merged[campo] = null; continue; }

    // Ambas pasadas tienen valor
    if (campo === 'monto' || LONGITUD_FIJA.has(campo)) {
      merged[campo] = v4; // PSM 4 prioridad para numérico y longitud fija
    } else {
      // Preferir el string más largo (más información capturada)
      merged[campo] = String(v4).length >= String(v6).length ? v4 : v6;
    }
  }
  return merged;
}

async function extractReceiptDataTesseract(imageBuffer, mimeType = 'image/jpeg') {
  // Preprocesar una sola vez — las tres pasadas comparten el mismo buffer PNG.
  const processedBuffer = await preprocessImage(imageBuffer);

  // ── Tres pasadas OCR en paralelo ──────────────────────────────────────────
  // PSM 4, PSM 6 y PSM 11 usan workers independientes (procesos separados),
  // por lo que Promise.all no añade latencia frente a una pasada secuencial.
  //
  //  PSM 4  (SINGLE_COLUMN) — recibos bancarios verticales (apps móviles).
  //  PSM 6  (SINGLE_BLOCK)  — layouts horizontales, PDFs, capturas de pantalla amplias.
  //  PSM 11 (SPARSE_TEXT)   — barrido numérico puro para montos difíciles.
  const [
    { text: raw4, confidence: conf4 },
    { text: raw6, confidence: conf6 },
    rawAmounts,
  ] = await Promise.all([
    runOCR(processedBuffer, 'image/png'),
    runOCRBlock(processedBuffer, 'image/png'),
    runOCRAmounts(processedBuffer, 'image/png'),
  ]);

  const clean4      = normalizeOcrText(raw4);
  const clean6      = normalizeOcrText(raw6);
  const cleanAmounts = normalizeOcrText(rawAmounts);

  // Extraer campos de cada pasada y fusionarlos
  const fields4  = extractAllFields(clean4);
  const fields6  = extractAllFields(clean6);
  const fields   = mergeOcrFields(fields4, fields6);

  // Monto: fusionado ?? pasada numérica PSM 11 como último recurso
  fields.monto = fields.monto ?? extractMonto(cleanAmounts);

  // Confianza: usar el máximo entre PSM 4 y PSM 6
  const ocrConfidence = Math.max(conf4, conf6);

  const baseConfianza = calcConfianza(fields);
  const adjustedConfianza = ocrConfidence < 60
    ? Math.round(baseConfianza * 0.8)
    : baseConfianza;

  return {
    ...fields,
    confianza:      adjustedConfianza,
    _engine:        'tesseract',
    _ocrConfidence: ocrConfidence,
    _ocrText:       process.env.NODE_ENV !== 'production' ? clean4       : undefined,
    _ocrText6:      process.env.NODE_ENV !== 'production' ? clean6       : undefined,
    _ocrAmounts:    process.env.NODE_ENV !== 'production' ? cleanAmounts : undefined,
  };
}

// ════════════════════════════════════════════════════════════════════════════
// MOTOR 1 — GEMINI 2.0 FLASH
// ════════════════════════════════════════════════════════════════════════════

/**
 * Envía imagen o PDF directamente a Gemini y extrae campos estructurados.
 * Gemini entiende el documento completo — no necesita regex ni normalización.
 * Requiere GEMINI_API_KEY en .env (gratuito en Google AI Studio).
 */
async function extractReceiptDataGemini(imageBuffer, mimeType) {
  if (!process.env.GEMINI_API_KEY) throw new Error('GEMINI_API_KEY no configurada en .env');

  const { GoogleGenerativeAI } = require('@google/generative-ai');
  const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
  const model = genAI.getGenerativeModel({
    model:            'gemini-2.0-flash',
    generationConfig: { temperature: 0.05, topP: 0.95 },
  });

  const result = await model.generateContent([
    GEMINI_PROMPT,
    { inlineData: { data: imageBuffer.toString('base64'), mimeType } },
  ]);

  const raw   = result.response.text().trim();
  const clean = raw.replace(/^```(?:json)?\s*/i, '').replace(/\s*```\s*$/i, '').trim();
  const data  = JSON.parse(clean);

  const parseMonto = (v) => {
    if (v === null || v === undefined) return null;
    const n = typeof v === 'string' ? parseFloat(v.replace(/,/g, '')) : Number(v);
    return ok(n) ? n : null;
  };

  // Normalizar fecha — Gemini debe devolver YYYY-MM-DD, pero por si acaso
  const parseFecha = (d) => {
    if (!d) return null;
    if (/^\d{4}-\d{2}-\d{2}$/.test(d)) return d;
    return extractFecha(d);
  };

  const fields = {
    monto:                 parseMonto(data.monto),
    fecha:                 parseFecha(data.fecha),
    hora:                  data.hora                  || null,
    claveRastreo:          data.claveRastreo          || null,
    referencia:            data.referencia            || null,
    numeroAutorizacion:    data.numeroAutorizacion    || null,
    clabe:                 data.clabe                 || null,
    bancoOrigen:           data.bancoOrigen           || null,
    bancoDestino:          data.bancoDestino          || null,
    cuentaOrigenUltimos4:  data.cuentaOrigenUltimos4  || null,
    cuentaDestinoUltimos4: data.cuentaDestinoUltimos4 || null,
    titularOrigen:         data.titularOrigen         || null,
    titularDestino:        data.titularDestino        || null,
    concepto:              data.concepto              || null,
  };

  const confianza = calcConfianza(fields);
  // Si Gemini no extrajo ningún campo útil (ej. imagen ilegible o fuera de contexto),
  // lanzar error para que el dispatcher pruebe el siguiente motor en lugar de
  // retornar un resultado vacío que bloquea la cadena de fallback.
  if (confianza === 0) {
    throw new Error('Gemini no extrajo datos útiles del comprobante (todos los campos null)');
  }

  return {
    ...fields,
    confianza,
    _engine: 'gemini-2.0-flash',
  };
}

// ════════════════════════════════════════════════════════════════════════════
// MOTOR 2 — GOOGLE CLOUD VISION API
// ════════════════════════════════════════════════════════════════════════════

// ════════════════════════════════════════════════════════════════════════════
// HELPERS DE PREPROCESAMIENTO
// ════════════════════════════════════════════════════════════════════════════

/**
 * Clasifica la imagen como screenshot digital o fotografía física.
 *
 * Heurística: los screenshots bancarios tienen fondos casi puros (blanco, negro
 * o un color corporativo sólido). Se mide la proporción de píxeles extremos
 * (>242 ó <13 en escala de grises) sobre un muestreo de 150×150 px.
 * Screenshots suelen superar el 35 % de píxeles "puros"; las fotos, no.
 *
 * El resultado determina el agresividad del pipeline:
 *   screenshot → conservador (no destruir píxeles perfectos)
 *   foto       → agresivo   (compensar ruido, blur, inclinación)
 */
async function detectIsScreenshot(buffer) {
  try {
    const sharp = require('sharp');
    const { data } = await sharp(buffer)
      .grayscale()
      .resize({ width: 150, height: 150, fit: 'fill' })
      .raw()
      .toBuffer({ resolveWithObject: true });

    let pureWhite = 0, pureBlack = 0;
    for (const px of data) {
      if (px > 242) pureWhite++;
      else if (px < 13) pureBlack++;
    }
    return (pureWhite + pureBlack) / data.length > 0.35;
  } catch {
    return false; // asumir foto en caso de error
  }
}

/**
 * Upscaling diferenciado según tipo de imagen.
 *
 * Screenshots → target 1 400 px en la dimensión menor (ya son nítidos,
 *               solo necesitan resolución suficiente para el LSTM).
 * Fotos       → target 1 800 px (compensar blur de cámara, compresión
 *               JPEG/WhatsApp y pérdida de detalle por distancia).
 * Máximo 3× para no introducir artefactos de escalado excesivo.
 * Kernel Lanczos3: mayor fidelidad que bilineal o bicúbico al escalar.
 */
async function smartUpscale(buffer, isScreenshot) {
  try {
    const sharp = require('sharp');
    const { width: w, height: h } = await sharp(buffer).metadata();
    if (!w || !h) return buffer;

    const minDim    = Math.min(w, h);
    const targetMin = isScreenshot ? 1400 : 1800;
    if (minDim >= targetMin) return buffer;

    const scale = Math.min(3, targetMin / minDim);
    return sharp(buffer)
      .resize(Math.round(w * scale), Math.round(h * scale), {
        kernel:             'lanczos3',
        withoutEnlargement: false,
      })
      .toBuffer();
  } catch {
    return buffer;
  }
}

/**
 * Detecta el ángulo de inclinación fino del texto (rango ±10°) usando el
 * método de varianza de proyecciones horizontales.
 *
 * Principio: cuando las líneas de texto están perfectamente horizontales,
 * las sumas de píxeles por fila muestran alta varianza (filas densas de
 * texto alternan con filas vacías de espacio). Al rotar la imagen en el
 * ángulo correcto, esa varianza se maximiza.
 *
 * Se trabaja sobre una copia reducida (≤ 400 px de ancho) para eficiencia.
 * Resolución angular: 0.5° — suficiente para OCR con LSTM.
 *
 * @returns {number} ángulo de corrección en grados (0 si la inclinación < 0.5°)
 */
async function detectSkewAngle(grayBuffer) {
  try {
    const sharp = require('sharp');
    const { data, info } = await sharp(grayBuffer)
      .resize({ width: 400, withoutEnlargement: true })
      .threshold(128)
      .raw()
      .toBuffer({ resolveWithObject: true });

    const { width, height } = info;
    const cx = width  / 2;
    const cy = height / 2;

    let bestAngle    = 0;
    let bestVariance = -1;

    for (let deg = -10; deg <= 10; deg += 0.5) {
      const rad     = deg * Math.PI / 180;
      const cosA    = Math.cos(rad);
      const sinA    = Math.sin(rad);
      const rowSums = new Int32Array(height);

      for (let y = 0; y < height; y++) {
        for (let x = 0; x < width; x++) {
          // Rotación inversa: encontrar el píxel fuente para la posición rotada
          const sx = Math.round(cosA * (x - cx) + sinA * (y - cy) + cx);
          const sy = Math.round(-sinA * (x - cx) + cosA * (y - cy) + cy);
          if (sx >= 0 && sx < width && sy >= 0 && sy < height) {
            if (data[sy * width + sx] < 128) rowSums[y]++; // píxel oscuro = texto
          }
        }
      }

      let sum = 0;
      for (let i = 0; i < height; i++) sum += rowSums[i];
      const mean = sum / height;
      let variance = 0;
      for (let i = 0; i < height; i++) variance += (rowSums[i] - mean) ** 2;
      variance /= height;

      if (variance > bestVariance) {
        bestVariance = variance;
        bestAngle    = deg;
      }
    }

    return Math.abs(bestAngle) >= 0.5 ? bestAngle : 0;
  } catch {
    return 0;
  }
}

/**
 * Binarización adaptativa Bradley-Roth con imagen integral — O(n).
 *
 * Ventaja sobre threshold global: calcula un umbral diferente para cada
 * píxel basado en el promedio local de sus vecinos en una ventana
 * windowSize×windowSize. Esto preserva texto fino en zonas con:
 *   • Fondos con gradiente (BBVA azul, Nu morado, Santander rojo)
 *   • Iluminación no uniforme (foto de papel bajo luz lateral)
 *   • Texto de múltiples tamaños en la misma imagen
 *
 * Fórmula: texto si pixel < mean_local × (1 – k)
 *   k menor (0.10–0.15) → conserva más texto; puede capturar algo de ruido.
 *   k mayor (0.18–0.25) → imagen más limpia; puede perder trazos muy finos.
 *
 * La imagen integral permite calcular la suma de cualquier rectángulo
 * en O(1) con 4 accesos, llevando la complejidad total a O(n).
 *
 * @param {Buffer} grayBuffer  PNG en escala de grises (fondo claro asumido)
 * @param {number} windowSize  Tamaño de ventana local en px (impar, default 29)
 * @param {number} k           Factor de offset del umbral (default 0.15)
 */
async function adaptiveThreshold(grayBuffer, windowSize = 29, k = 0.15) {
  const sharp = require('sharp');

  const { data, info } = await sharp(grayBuffer)
    .grayscale()
    .raw()
    .toBuffer({ resolveWithObject: true });
  const { width, height } = info;
  const half = Math.floor(windowSize / 2);

  // Imagen integral: integral[y*w+x] = suma de todos los px en rect (0,0)→(x,y)
  const integral = new Float64Array(width * height);
  for (let y = 0; y < height; y++) {
    for (let x = 0; x < width; x++) {
      const i        = y * width + x;
      const above    = y > 0             ? integral[(y - 1) * width + x]          : 0;
      const left     = x > 0             ? integral[y * width + (x - 1)]          : 0;
      const aboveLft = (y > 0 && x > 0) ? integral[(y - 1) * width + (x - 1)]    : 0;
      integral[i]    = data[i] + above + left - aboveLft;
    }
  }

  const output = Buffer.alloc(width * height);
  for (let y = 0; y < height; y++) {
    for (let x = 0; x < width; x++) {
      const x1 = Math.max(0, x - half);
      const y1 = Math.max(0, y - half);
      const x2 = Math.min(width  - 1, x + half);
      const y2 = Math.min(height - 1, y + half);

      const count     = (x2 - x1 + 1) * (y2 - y1 + 1);
      const sum       = integral[y2 * width + x2]
        - (x1 > 0            ? integral[y2 * width + (x1 - 1)]          : 0)
        - (y1 > 0            ? integral[(y1 - 1) * width + x2]          : 0)
        + (x1 > 0 && y1 > 0 ? integral[(y1 - 1) * width + (x1 - 1)]    : 0);

      const localMean = sum / count;
      // Texto (oscuro) si cae por debajo del umbral local adaptativo
      output[y * width + x] = data[y * width + x] < localMean * (1 - k) ? 0 : 255;
    }
  }

  return sharp(output, { raw: { width, height, channels: 1 } }).png().toBuffer();
}

// ════════════════════════════════════════════════════════════════════════════
// PREPROCESADOR PRINCIPAL
// ════════════════════════════════════════════════════════════════════════════

/**
 * Preprocesa una imagen de comprobante para maximizar la precisión del OCR.
 *
 * Pipeline de 8 pasos — cada uno mejora una dimensión distinta:
 *
 *  0. Auto-rotación EXIF        Corrige fotos tomadas con el teléfono girado
 *                               (sharp.rotate() sin args aplica el metadato EXIF).
 *  1. Clasificación de imagen   screenshot vs fotografía física → determina
 *                               la agresividad de todos los pasos siguientes.
 *  2. Upscaling inteligente     screenshot ≥1 400 px / foto ≥1 800 px (min-dim).
 *                               Lanczos3 para máxima fidelidad. Máximo 3×.
 *  3. Escala de grises + stats  Base para decisiones adaptativas posteriores:
 *                               avgBrightness (gamma) y stdDev (binarización).
 *  4. Deskew fino (solo fotos)  Detecta inclinaciones de ±0.5–10° y las
 *                               corrige antes del OCR. El LSTM de Tesseract no
 *                               compensa inclinaciones > 2–3° por sí solo.
 *  5. Corrección gamma          Aclara imágenes oscuras (fotos nocturnas,
 *                               WhatsApp con poca luz). Adaptativa al brillo.
 *  6. Sharpen diferenciado      Fotos: agresivo (compensa blur de cámara).
 *                               Screenshots: suave (ya son nítidos).
 *  7. CLAHE                     Ecualización de histograma adaptativa local en
 *                               tiles de 64×64 px. Preserva contraste local en
 *                               fondos de gradiente e iluminación no uniforme.
 *                               Muy superior a .normalize() global para fondos
 *                               de color (BBVA azul, Nu morado, Santander rojo).
 *  8. Binarización adaptativa   Bradley-Roth (imagen integral, O(n)): umbral
 *                               local por píxel. Funciona en gradientes, texto
 *                               pequeño y fondos con marca de agua donde el
 *                               threshold global falla por completo.
 *
 * Si cualquier paso falla, devuelve el buffer original sin modificar.
 */
async function preprocessImage(imageBuffer) {
  try {
    const sharp = require('sharp');

    // ── 0. Auto-rotación EXIF ──────────────────────────────────────────────
    // sharp.rotate() sin argumento lee el campo Orientation del EXIF y aplica
    // la rotación correspondiente. Resuelve el 90 % de los casos de fotos
    // tomadas con el teléfono en posición no estándar sin coste adicional.
    let buf = await sharp(imageBuffer, { failOn: 'none' }).rotate().toBuffer();

    // ── 1. Clasificar tipo de imagen ───────────────────────────────────────
    const isScreenshot = await detectIsScreenshot(buf);

    // ── 2. Upscaling inteligente ───────────────────────────────────────────
    buf = await smartUpscale(buf, isScreenshot);

    // ── 3. Escala de grises y estadísticas de brillo ───────────────────────
    const grayBuf       = await sharp(buf).grayscale().png().toBuffer();
    const stats         = await sharp(grayBuf).stats();
    const avgBrightness = stats.channels[0].mean;   // 0–255
    const stdDev        = stats.channels[0].stdev;  // variación de iluminación

    // ── 4. Corrección de inclinación (solo fotografías) ────────────────────
    // Los screenshots siempre están perfectamente alineados (el SO lo garantiza).
    // Las fotos de papel o de pantalla pueden tener inclinaciones de 2–10° que
    // el LSTM de Tesseract no puede compensar y que reducen la precisión
    // al confundir el detector de líneas de texto.
    let correctedBuf = grayBuf;
    if (!isScreenshot) {
      const skewAngle = await detectSkewAngle(grayBuf);
      if (Math.abs(skewAngle) >= 0.5) {
        correctedBuf = await sharp(grayBuf)
          .rotate(-skewAngle, { background: { r: 255, g: 255, b: 255, alpha: 1 } })
          .png()
          .toBuffer();
      }
    }

    // ── 5. Corrección gamma ────────────────────────────────────────────────
    let pipeline = sharp(correctedBuf, { failOn: 'none' });
    if      (avgBrightness < 50)  pipeline = pipeline.gamma(3.0); // muy oscura
    else if (avgBrightness < 100) pipeline = pipeline.gamma(2.2); // moderadamente oscura

    // ── 6. Sharpen diferenciado ────────────────────────────────────────────
    pipeline = isScreenshot
      ? pipeline.sharpen({ sigma: 0.8, m1: 1.0, m2: 0.3 })  // suave: ya es nítido
      : pipeline.sharpen({ sigma: 1.5, m1: 2.0, m2: 0.7 }); // agresivo: compensar blur

    // ── 7. CLAHE — ecualización de histograma adaptativa local ─────────────
    // A diferencia de .normalize() que estira el histograma globalmente,
    // CLAHE ajusta el contraste de forma independiente en tiles de 64×64 px.
    // Resultado: texto en zonas claras y oscuras de la misma imagen queda
    // igualmente definido. maxSlope:4 limita la amplificación de ruido
    // en zonas homogéneas (fondos lisos sin texto).
    pipeline = pipeline.clahe({ width: 64, height: 64, maxSlope: 4 });

    // Invertir fondos oscuros ANTES de la binarización adaptativa para
    // garantizar que la salida siempre sea texto oscuro sobre fondo claro
    // (convención que Tesseract LSTM espera).
    if (avgBrightness < 80) pipeline = pipeline.negate();

    const enhancedBuf = await pipeline.png().toBuffer();

    // ── 8. Binarización adaptativa Bradley-Roth ────────────────────────────
    // Se aplica cuando hay variación de iluminación (stdDev > 35) o cuando
    // es un screenshot con fondo de color (donde el threshold global destruye
    // texto en la zona del header de color corporativo).
    // Para imágenes uniformes y muy claras, threshold global es más rápido.
    const useAdaptive = isScreenshot || stdDev > 35;

    if (useAdaptive) {
      if (isScreenshot) {
        // Detectar si la parte superior del screenshot tiene header oscuro (BBVA verde,
        // Nu morado, Santander rojo). En ese caso, Bradley-Roth falla porque marca el fondo
        // oscuro como "texto" y el texto blanco como "fondo" — el resultado es texto blanco
        // sobre fondo blanco: invisible para Tesseract.
        // Solución: devolver la imagen CLAHE-enhanced sin binarizar — Tesseract LSTM maneja
        // grayscale directamente y no necesita binarización cuando hay zonas mixtas.
        const { data: topData, info: topInfo } = await sharp(enhancedBuf)
          .extract({ left: 0, top: 0, width: (await sharp(enhancedBuf).metadata()).width, height: Math.max(1, Math.floor((await sharp(enhancedBuf).metadata()).height * 0.30)) })
          .grayscale()
          .raw()
          .toBuffer({ resolveWithObject: true });
        const topBrightness = topData.reduce((s, v) => s + v, 0) / topData.length;

        if (topBrightness < 120) {
          // Header oscuro detectado — saltar binarización para conservar texto blanco
          return enhancedBuf;
        }
      }

      // Screenshots sin header oscuro y fotos con variación de iluminación
      const windowSize = isScreenshot ? 25 : 45;
      const k          = isScreenshot ? 0.12 : 0.20;
      return await adaptiveThreshold(enhancedBuf, windowSize, k);
    }

    // Imagen de alto contraste uniforme → threshold global más rápido
    return await sharp(enhancedBuf).threshold(140).png().toBuffer();

  } catch (prepErr) {
    // Loguear el paso que falló para facilitar diagnóstico (ej. GIF animado, buffer corrupto)
    const logger = require('../../../shared/utils/logger');
    logger.warn('[preprocessImage] Pipeline de preprocesamiento falló, usando buffer original:', prepErr.message);
    return imageBuffer;
  }
}

/**
 * Extrae texto de un PDF digital (vectorial).
 * Los PDFs generados por apps bancarias siempre tienen texto embebido.
 * No requiere renderizado ni OCR — es extracción directa.
 */
async function extractTextFromPdf(pdfBuffer) {
  const pdfParse = require('pdf-parse');
  const data     = await pdfParse(pdfBuffer);
  return (data.text || '').trim();
}

// ════════════════════════════════════════════════════════════════════════════
// PDF ESCANEADO → IMAGEN  (pdfjs-dist v3 + canvas)
// ════════════════════════════════════════════════════════════════════════════

/**
 * Renderiza las primeras N páginas de un PDF a buffers PNG de alta resolución.
 *
 * Usa pdfjs-dist (Motor JavaScript puro, sin Ghostscript ni ImageMagick) +
 * el módulo `canvas` que ya se encuentra disponible como dependencia transitiva.
 *
 * Scale 2.5 ≈ 187 DPI sobre un PDF de 72 DPI base → suficiente para Tesseract LSTM.
 * Para PDFs de tamaño carta (8.5×11 in) produce ≈ 1590×2063 px, óptimo para OCR.
 */
async function renderPdfToImages(pdfBuffer, maxPages = 2) {
  // Requiere lazy para no romper el arranque si pdfjs-dist no está instalado
  let pdfjsLib;
  try {
    pdfjsLib = require('pdfjs-dist/legacy/build/pdf.js');
  } catch {
    throw new Error('pdfjs-dist no está instalado — ejecuta: npm install pdfjs-dist@3.11.174');
  }

  let createCanvas;
  try {
    ({ createCanvas } = require('canvas'));
  } catch {
    throw new Error('El módulo canvas no está disponible — ejecuta: npm install canvas');
  }

  pdfjsLib.GlobalWorkerOptions.workerSrc =
    require.resolve('pdfjs-dist/legacy/build/pdf.worker.js');

  const data = new Uint8Array(pdfBuffer);
  const pdf  = await pdfjsLib.getDocument({ data, verbosity: 0 }).promise;
  const pagesToRender = Math.min(pdf.numPages, maxPages);

  const images = [];
  for (let i = 1; i <= pagesToRender; i++) {
    const page   = await pdf.getPage(i);
    const vp     = page.getViewport({ scale: 2.5 });
    const canvas = createCanvas(Math.ceil(vp.width), Math.ceil(vp.height));
    const ctx    = canvas.getContext('2d');
    await page.render({ canvasContext: ctx, viewport: vp }).promise;
    images.push(canvas.toBuffer('image/png'));
  }
  return images;
}

/**
 * Motor para PDFs escaneados (sin texto embebido).
 *
 * Flujo: renderizar cada página a PNG → preprocessImage → Tesseract (3 PSMs).
 * Si la primera página produce confianza baja (< 40), intenta la segunda.
 * Retorna el resultado de mayor confianza.
 */
async function extractReceiptDataPdfAsImage(pdfBuffer) {
  const logger  = require('../../../shared/utils/logger');
  const pages   = await renderPdfToImages(pdfBuffer, 2);

  if (!pages.length) {
    throw new Error('renderPdfToImages no devolvió ninguna página');
  }

  const results = [];
  for (const pagePng of pages) {
    try {
      const r = await extractReceiptDataTesseract(pagePng, 'image/png');
      results.push(r);
      // Si ya tenemos buena confianza en la primera página, no renderizar más
      if (r.confianza >= 40) break;
    } catch (pageErr) {
      logger.warn('[extractReceiptDataPdfAsImage] Error procesando página PDF→imagen:', pageErr.message);
    }
  }

  if (!results.length) {
    throw new Error('Tesseract no pudo extraer datos de ninguna página del PDF renderizado');
  }

  // Elegir la página con mayor confianza
  const best = results.reduce((a, b) => (b.confianza > a.confianza ? b : a));
  return { ...best, _engine: 'tesseract-pdf-render' };
}

/**
 * Motor principal — Google Cloud Vision API.
 *
 * Imágenes : preprocessImage → DOCUMENT_TEXT_DETECTION → extracción de campos
 * PDFs     : pdf-parse (texto embebido) → extracción de campos
 *            Si el PDF está vacío (escaneado), lanza error claro.
 *
 * Auth     : usa GOOGLE_SERVICE_ACCOUNT_KEY_PATH del .env
 *            (el mismo service account ya configurado para Drive)
 */
async function extractReceiptDataVision(imageBuffer, mimeType = 'image/jpeg') {
  const isPdf = mimeType === 'application/pdf';

  let rawText;
  let engine;

  if (isPdf) {
    rawText = await extractTextFromPdf(imageBuffer);
    engine  = 'pdf-parse';

    if (!rawText || rawText.length < 20) {
      throw new Error(
        'El PDF no contiene texto extraíble (posiblemente escaneado). ' +
        'Envía una foto del documento como imagen (JPG/PNG).'
      );
    }
  } else {
    // Preprocesar imagen para mejorar reconocimiento de imágenes de WhatsApp
    const processedBuffer = await preprocessImage(imageBuffer);

    const client = getVisionClient();
    const [result] = await client.documentTextDetection({
      image:        { content: processedBuffer },
      imageContext: { languageHints: ['es', 'es-MX', 'es-419'] },
    });

    if (result.error && result.error.code) {
      throw new Error(`Vision API error ${result.error.code}: ${result.error.message}`);
    }

    if (!result.fullTextAnnotation || !result.fullTextAnnotation.text) {
      throw new Error('Google Vision no detectó texto en la imagen.');
    }

    rawText = result.fullTextAnnotation.text;
    engine  = 'google-vision';
  }

  const clean    = normalizeOcrText(rawText);
  const lines    = clean.split('\n');
  const half     = Math.floor(lines.length / 2);
  const rawLines = rawText.split('\n').filter(Boolean).map(t => ({ text: t.trim() }));

  const lf     = extractFieldsFromLines(rawLines);
  const bancos = extractBancos(clean);

  const fields = {
    monto:                 extractMonto(clean)              ?? extractMontoFromLines(rawLines),
    fecha:                 extractFecha(clean)              ?? lf.fecha,
    hora:                  extractHora(clean)               ?? lf.hora,
    claveRastreo:          extractClaveRastreo(clean)       ?? lf.claveRastreo,
    referencia:            extractReferencia(clean)         ?? lf.referencia,
    numeroAutorizacion:    extractNumeroAutorizacion(clean) ?? lf.numeroAutorizacion,
    clabe:                 extractClabe(clean)              ?? lf.clabe,
    bancoOrigen:           bancos.bancoOrigen               ?? lf.bancoOrigen,
    bancoDestino:          bancos.bancoDestino              ?? lf.bancoDestino,
    cuentaOrigenUltimos4:  extractUltimos4(lines.slice(0, half).join('\n')) ?? lf.cuentaOrigenUltimos4,
    cuentaDestinoUltimos4: extractUltimos4(lines.slice(half).join('\n'))    ?? lf.cuentaDestinoUltimos4,
    titularOrigen:         extractTitular(clean, 'origen')  ?? lf.titularOrigen,
    titularDestino:        extractTitular(clean, 'destino') ?? lf.titularDestino,
    concepto:              extractConcepto(clean)           ?? lf.concepto,
  };

  return {
    ...fields,
    confianza: calcConfianza(fields),
    _engine:   engine,
    _ocrText:  process.env.NODE_ENV !== 'production' ? clean    : undefined,
    _ocrLines: process.env.NODE_ENV !== 'production' ? rawLines : undefined,
  };
}

// ════════════════════════════════════════════════════════════════════════════
// DISPATCHER PÚBLICO
// ════════════════════════════════════════════════════════════════════════════

/**
 * Extrae datos de un comprobante de pago.
 *
 * Cadena de motores (imágenes):
 *   1. Gemini 2.0 Flash  — mejor precisión, entiende PDF e imagen de forma nativa
 *   2. Google Vision API — DOCUMENT_TEXT_DETECTION + preprocessImage
 *   3. Tesseract.js      — fallback local completo (3 PSMs en paralelo)
 *
 * Cadena de motores (PDF):
 *   1. Gemini           — acepta PDF base64 de forma nativa
 *   2. Vision + pdf-parse — para PDFs con texto embebido (vectorial/digital)
 *   3. pdfjs + Tesseract — renderiza páginas a PNG y aplica OCR (PDFs escaneados)
 */
async function extractReceiptData(imageBuffer, mimeType) {
  if (!SUPPORTED_MIME.includes(mimeType))
    throw new Error(`Tipo no soportado: "${mimeType}". Usa JPG, PNG, WEBP o PDF.`);

  const isPdf = mimeType === 'application/pdf';

  // ── Motor 1: Gemini ───────────────────────────────────────────
  try {
    return await extractReceiptDataGemini(imageBuffer, mimeType);
  } catch (geminiErr) {
    console.warn('[receiptService] Gemini falló:', geminiErr.message);
  }

  // ── Motor 2: Google Vision ────────────────────────────────────
  try {
    return await extractReceiptDataVision(imageBuffer, mimeType);
  } catch (visionErr) {
    console.warn('[receiptService] Vision falló:', visionErr.message);

    if (isPdf) {
      // Motor 3-PDF: renderizar cada página del PDF a imagen PNG y aplicar Tesseract.
      // Cubre PDFs escaneados (imagen dentro de PDF) que pdf-parse no puede extraer.
      console.warn('[receiptService] PDF sin texto embebido — intentando render PDF→imagen con Tesseract.');
      try {
        return await extractReceiptDataPdfAsImage(imageBuffer);
      } catch (pdfRenderErr) {
        console.warn('[receiptService] Render PDF→imagen falló:', pdfRenderErr.message);
        throw new Error(
          'No se pudo procesar el PDF con ningún motor disponible. ' +
          `Vision: ${visionErr.message} | Render: ${pdfRenderErr.message}`
        );
      }
    }
  }

  // ── Motor 3: Tesseract (imágenes) ─────────────────────────────
  console.warn('[receiptService] Usando Tesseract como último fallback.');
  return {
    ...(await extractReceiptDataTesseract(imageBuffer, mimeType)),
    _engine: 'tesseract',
  };
}

// ════════════════════════════════════════════════════════════════════════════
// SCORING Y BÚSQUEDA DE CANDIDATOS
// ════════════════════════════════════════════════════════════════════════════

/**
 * Puntúa un movimiento bancario contra los datos extraídos.
 * Retorna null si el monto difiere más de la tolerancia.
 *
 * Puntuación máxima: 100
 *   monto exacto / ±0.5%    40 pts  (obligatorio)
 *   fecha                    25 pts
 *   clave rastreo / ref      20 pts
 *   banco (origen o destino) 15 pts
 *   cuenta últimos 4 dígitos  5 pts  (—> suma sin superar 100)
 */
const BANCO_ALIASES = {
  'banamex': ['banamex','citibanamex','citi'],
  'bbva':    ['bbva','bancomer','bbva bancomer'],
  'santander':['santander'],
  'banorte': ['banorte','ixe'],
  'hsbc':    ['hsbc'],
  'azteca':  ['azteca','banco azteca'],
  'inbursa': ['inbursa'],
  'scotiabank':['scotiabank','scotiabank mexico'],
  'banbajio':['banbajío','bajío','banbajio'],
  'nu':      ['nu','nubank','nu bank'],
  'spin':    ['spin','spin by oxxo'],
  'hey':     ['hey banco','hey'],
  'albo':    ['albo'],
  'afirme':  ['afirme'],
};

function normalizarBanco(nombre) {
  if (!nombre) return null;
  const n = nombre.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  for (const [canonical, aliases] of Object.entries(BANCO_ALIASES)) {
    if (aliases.some(a => n.includes(a))) return canonical;
  }
  return n.trim();
}

function scoreMovement(mov, ext) {
  const movMonto = mov.deposito || mov.retiro || 0;
  let score = 0;
  const reasons = [];

  // ── Monto (40 pts) — tolerancias escalonadas ──────────────────────────────
  const diff = Math.abs(movMonto - ext.monto);
  const pct  = diff / ext.monto;

  if      (diff < 0.01)  { score += 40; reasons.push('Monto exacto'); }
  else if (diff <= 0.05) { score += 38; reasons.push('Monto ±$0.05 (redondeo banco)'); }
  else if (pct  <= 0.005){ score += 35; reasons.push('Monto ±0.5%'); }
  else if (diff <= 1.0)  { score += 30; reasons.push('Monto ±$1'); }
  else                   { return null; }  // descartado

  // ── Fecha (25 pts) ────────────────────────────────────────────────────────
  if (ext.fecha) {
    const days = Math.abs(
      (new Date(new Date(mov.fecha).toDateString()).getTime() -
       new Date(new Date(ext.fecha).toDateString()).getTime()) / 86_400_000
    );
    if      (days === 0) { score += 25; reasons.push('Misma fecha'); }
    else if (days <= 1)  { score += 20; reasons.push('±1 día'); }
    else if (days <= 3)  { score += 15; reasons.push('±3 días'); }
    else if (days <= 7)  { score +=  8; reasons.push('±7 días'); }
    else if (days <= 14) { score +=  4; reasons.push('±14 días'); }
  }

  // ── Clave rastreo / referencia (20 pts) ───────────────────────────────────
  const mAuth  = (mov.numeroAutorizacion || '').replace(/\s/g,'').toLowerCase();
  const mRefN  = (mov.referenciaNumerica || '').replace(/\s/g,'').toLowerCase();
  const eClave = (ext.claveRastreo       || '').replace(/\s/g,'').toLowerCase();
  const eRef   = (ext.referencia || ext.numeroAutorizacion || '').replace(/\s/g,'').toLowerCase();

  if (eClave && mAuth && (mAuth === eClave || mAuth.includes(eClave) || eClave.includes(mAuth)))
    { score += 20; reasons.push('Clave rastreo exacta'); }
  else if (eRef && mRefN && (mRefN === eRef || mRefN.includes(eRef) || eRef.includes(mRefN)))
    { score += 15; reasons.push('Referencia numérica'); }
  else if (eClave && mRefN && eClave.length >= 12 && mRefN.includes(eClave.slice(-12)))
    { score +=  8; reasons.push('Clave rastreo parcial'); }

  // ── Banco (15 pts) — comparación por alias normalizado ────────────────────
  if (mov.banco) {
    const movBancoNorm = normalizarBanco(mov.banco);
    const extBancos    = [ext.bancoOrigen, ext.bancoDestino]
      .filter(Boolean).map(normalizarBanco);

    if (extBancos.includes(movBancoNorm)) {
      score += 15; reasons.push(`Banco: ${mov.banco}`);
    }
  }

  // ── Cuenta últimos 4 (5 pts) ──────────────────────────────────────────────
  const last4 = ext.cuentaDestinoUltimos4 || ext.cuentaOrigenUltimos4;
  if (last4 && mov.concepto && mov.concepto.includes(last4)) {
    score += 5; reasons.push(`Cta ****${last4}`);
  }

  // ── Titular del comprobante en el concepto del movimiento (10 pts) ─────────
  // Los movimientos SPEI suelen incluir el nombre del remitente en el concepto,
  // ej: "SPEI DE EDGAR CORTES GONZALEZ". Comparar con titularOrigen/titularDestino.
  const movConceptoNorm = (mov.concepto || '')
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '').toUpperCase();

  const titular = ext.titularOrigen || ext.titularDestino || '';
  if (titular && movConceptoNorm) {
    const titNorm  = titular.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toUpperCase();
    // Filtrar tokens cortos (artículos, preposiciones) para evitar falsos positivos
    const tokens   = titNorm.split(/\s+/).filter(t => t.length > 2);
    if (tokens.length > 0) {
      const matched = tokens.filter(t => movConceptoNorm.includes(t));
      const ratio   = matched.length / tokens.length;
      if      (ratio >= 0.6) { score += 10; reasons.push(`Titular: ${titular.slice(0, 25)}…`); }
      else if (ratio >= 0.3) { score +=  5; reasons.push('Titular parcial'); }
    }
  }

  // ── Concepto extraído vs concepto del movimiento (5 pts) ──────────────────
  // El concepto del comprobante ("pago renta feb", "factura 234") puede coincidir
  // con palabras clave del concepto del banco.
  const extConcepto = (ext.concepto || '');
  if (extConcepto && movConceptoNorm) {
    const extNorm  = extConcepto.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toUpperCase();
    const extTokens = extNorm.split(/\s+/).filter(t => t.length > 3);
    if (extTokens.length > 0) {
      const matched = extTokens.filter(t => movConceptoNorm.includes(t));
      if (matched.length / extTokens.length >= 0.5) {
        score += 5; reasons.push('Concepto coincide');
      }
    }
  }

  // ── CLABE — últimos 8 dígitos como señal de cuenta (5 pts) ────────────────
  // La CLABE completa raramente aparece en el concepto, pero los últimos 8 dígitos
  // (que identifican al beneficiario + dígito de control) sí pueden estar presentes
  // en referenciaNumerica o en el concepto del banco.
  // Nota: los últimos 4 ya están cubiertos por la regla de cuenta arriba;
  //       aquí se buscan los 8 para sumar puntos adicionales sin duplicar.
  if (ext.clabe && ext.clabe.length === 18) {
    const last8   = ext.clabe.slice(-8);
    const haystack = [mov.concepto, mov.referenciaNumerica, mov.numeroAutorizacion]
      .filter(Boolean).join(' ');
    if (haystack.includes(last8) && !(last4 && last8.endsWith(last4) && haystack.includes(last4))) {
      score += 5; reasons.push(`CLABE ****${last8}`);
    }
  }

  return { score: Math.min(score, 100), reasons };
}

/**
 * Busca movimientos bancarios candidatos para el comprobante analizado.
 * Si no hay monto, devuelve los 15 más recientes para selección manual.
 */
async function findMatchingMovements(ext) {
  if (!ext.monto) {
    const recent = await BankMovement.find({
      isActive: true,
      fecha:    { $gte: new Date(Date.now() - FALLBACK_WINDOW * 86_400_000) },
    }).sort({ fecha: -1 }).limit(15).lean();

    return recent.map(mov => ({
      movement: mov,
      score:    0,
      reasons:  ['Sin monto extraído — selección manual'],
      nivel:    'bajo',
    }));
  }

  const tol = Math.max(0.50, ext.monto * 0.005);
  const filter = {
    isActive: true,
    $or: [
      { deposito: { $gte: ext.monto - tol, $lte: ext.monto + tol } },
      { retiro:   { $gte: ext.monto - tol, $lte: ext.monto + tol } },
    ],
  };

  if (ext.fecha) {
    const base = new Date(ext.fecha);
    filter.fecha = {
      $gte: new Date(base.getTime() - DATE_WINDOW_DAYS * 86_400_000),
      $lte: new Date(base.getTime() + DATE_WINDOW_DAYS * 86_400_000),
    };
  } else {
    filter.fecha = { $gte: new Date(Date.now() - FALLBACK_WINDOW * 86_400_000) };
  }

  const candidates = await BankMovement.find(filter)
    .sort({ fecha: -1 }).limit(150).lean();

  return candidates
    .map(mov => {
      const r = scoreMovement(mov, ext);
      if (!r) return null;
      return { movement: mov, score: r.score, reasons: r.reasons,
               nivel: r.score >= 80 ? 'alto' : r.score >= 50 ? 'medio' : 'bajo' };
    })
    .filter(Boolean)
    .sort((a, b) => b.score - a.score)
    .slice(0, 10);
}

module.exports = { extractReceiptData, findMatchingMovements };
