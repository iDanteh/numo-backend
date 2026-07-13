'use strict';

const fs   = require('fs');
const path = require('path');
const { extractReceiptData } = require('./receipt.service');

// Fixtures reales del usuario — viven FUERA de los repos (NUMO/Docs/Comprobantes/,
// un directorio hermano de numo-backend/ y numo-frontend/, ninguno de los dos es
// repositorio git para esa carpeta) A PROPÓSITO: son comprobantes bancarios reales
// con nombres, cuentas y CLABEs reales — nunca deben commitearse a git.
//
// Por eso este archivo NO copia los comprobantes al repo: lee la carpeta externa
// y se SALTA (no falla) si no existe, para no romper CI ni el checkout de otra
// persona. Solo corre con valor real en una máquina que ya tenga esa carpeta
// localmente (ver memoria del proyecto, sesión 2026-07-13).
const FIXTURES_DIR    = path.join(__dirname, '..', '..', '..', '..', '..', 'Docs', 'Comprobantes');
const fixturesExist   = fs.existsSync(FIXTURES_DIR);
const describeReal    = fixturesExist ? describe : describe.skip;

// OCR real (PaddleOCR primero, Tesseract como fallback) puede tardar
// bastantes segundos por archivo — no el default de 5s de Jest.
jest.setTimeout(30000);

// LIMITACIÓN CONOCIDA (no introducida por este archivo): `ppu-paddle-ocr` se
// publica como ESM puro ("type": "module") y Jest no lo puede parsear sin un
// transform de Babel (@babel/preset-env) que este proyecto no tiene instalado
// — ver paddle-ocr.service.js, require perezoso. Bajo Jest, PaddleOCR SIEMPRE
// falla al cargar y el dispatcher cae automáticamente a Tesseract (el mismo
// fallback que corre en producción cuando Paddle falla por cualquier otra
// razón) — así que estos tests SÍ ejercitan un camino real del pipeline, solo
// que no el motor primario. Si en algún momento se quiere probar Paddle
// también bajo Jest, hace falta: `npm i -D @babel/preset-env`, un
// `babel.config.js` con ese preset, y `transformIgnorePatterns:
// ['node_modules/(?!ppu-paddle-ocr)']` en la config de Jest — no se hizo hoy
// a propósito, para no meter una dependencia nueva sin acordarlo antes.
//
// Nota aparte: por los workers persistentes de Tesseract (y el transporte a
// archivo de winston), Jest puede quedarse "abierto" un momento tras
// terminar — es esperado (son singletons de larga vida, por diseño, ver
// receipt.service.js), no una fuga introducida aquí.

// Bajo Jest, ppu-paddle-ocr nunca carga (ver nota arriba) — se detecta acá
// con el mismo patrón de require perezoso para saber si toca saltar los
// casos que solo Paddle lee bien.
let paddleDisponibleEnEsteEntorno = true;
try { require('ppu-paddle-ocr'); } catch { paddleDisponibleEnEsteEntorno = false; }

// Valores validados manualmente el 2026-07-13 corriendo el pipeline real contra
// estos 11 archivos (ver conversación / project_ocr_audit.md) — sirven de
// regresión: si `monto` cambia para alguno de estos sin que el cambio de código
// haya sido verificado a propósito contra el comprobante real, algo se rompió.
//
// `requierePaddle: true` marca el único caso donde Tesseract (el fallback que
// SÍ corre bajo Jest hoy) da un resultado distinto al de Paddle (el motor
// primario real): confunde un dígito en 5547.84.jpg y lee 5947.84. No es una
// regresión de hoy — es una limitación ya conocida de Tesseract en fuentes
// serif (ver [[project_collection_requests]], el mismo tipo de ambigüedad
// O↔0 / l↔1 documentada ahí). Se salta explícitamente en vez de afirmar un
// monto que no es el real.
const CASOS = [
  { archivo: '5547.84.jpg',     mime: 'image/jpeg',      monto: 5547.84, requierePaddle: true },
  { archivo: '5672.9.jpg',      mime: 'image/jpeg',      monto: 5672.9 },
  { archivo: 'Comprobante.jpg', mime: 'image/jpeg',      monto: 4969.8 },
  { archivo: 'Ticket.jpg',      mime: 'image/jpeg',      monto: 37174 },
  { archivo: '1614.5.jpg',      mime: 'image/jpeg',      monto: 1614.5 },
  { archivo: '2184.77.jpg',     mime: 'image/jpeg',      monto: 2184.77 },
  { archivo: '2309.jpg',        mime: 'image/jpeg',      monto: 2309 },
  { archivo: '2383.jpg',        mime: 'image/jpeg',      monto: 2383 },
  { archivo: '2639.01.jpg',     mime: 'image/jpeg',      monto: 2639.01 },
  { archivo: '11536.63.pdf',    mime: 'application/pdf', monto: 11536.63 },
  { archivo: '1802.53.pdf',     mime: 'application/pdf', monto: 1802.53 },
];

describeReal('extractReceiptData — regresión con comprobantes reales (Docs/Comprobantes)', () => {
  for (const { archivo, mime, monto, requierePaddle } of CASOS) {
    const saltar = requierePaddle && !paddleDisponibleEnEsteEntorno;
    const runner = saltar ? test.skip : test;
    const titulo = saltar
      ? `${archivo} → requiere PaddleOCR real (Tesseract confunde un dígito en este archivo) — saltado bajo Jest`
      : `${archivo} → extrae el monto correcto (${monto})`;

    runner(titulo, async () => {
      const buffer    = fs.readFileSync(path.join(FIXTURES_DIR, archivo));
      const resultado = await extractReceiptData(buffer, mime, archivo);
      expect(resultado.monto).toBe(monto);
    });
  }
});

describe('extractReceiptData — validaciones sin depender de archivos reales', () => {
  test('rechaza un mimetype no soportado', async () => {
    await expect(extractReceiptData(Buffer.from('x'), 'application/zip'))
      .rejects.toThrow(/Tipo no soportado/);
  });
});
