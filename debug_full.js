process.env.NODE_ENV = 'development';
process.env.GEMINI_API_KEY = '';
process.env.GOOGLE_SERVICE_ACCOUNT_KEY_PATH = '';

const Module = require('module');
const orig = Module._load;
Module._load = function(req, parent, isMain) {
  if (req.includes('BankMovement')) return { find: async () => [] };
  return orig.apply(this, arguments);
};

// Patch extractMonto to debug it
const src = require('fs').readFileSync('./src/banks/domains/collection-requests/receipt.service.js', 'utf8');
const patched = src.replace(
  'function extractMonto(text) {',
  `function extractMonto(text) {
  const DEBUG_MONTO = process.env.DEBUG_MONTO;
  if (DEBUG_MONTO) { console.log('[extractMonto] input length:', text.length, '| first 200:', JSON.stringify(text.slice(0,200))); }
  const origMatch = text.match;`
);

const fs = require('fs');
const path = require('path');
fs.writeFileSync('./debug_receipt_service.js', patched);

// Use the module with debug enabled
process.env.DEBUG_MONTO = '1';
const svc = require('./debug_receipt_service.js');

const buf = fs.readFileSync('../errorPDF1.pdf');
svc.extractReceiptData(buf, 'application/pdf').then(r => {
  console.log('\nRESULT monto:', r.monto, '| confianza:', r.confianza, '| engine:', r._engine);
}).catch(e => console.error('ERROR:', e.message)).finally(() => {
  fs.unlinkSync('./debug_receipt_service.js');
  process.exit(0);
});
