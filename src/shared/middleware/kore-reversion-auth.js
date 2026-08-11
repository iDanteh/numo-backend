'use strict';

const crypto = require('crypto');

// verifyKoreApiKey — autenticación server-to-server para el webhook de reversión
// de CxC que consume Kore (sin JWT/Auth0, ver POST /api/erp/cxc-reversiones).
// Mismo criterio defensivo que ya usa erp-sync.service.js para ERP_CAJA_BASE_URL
// ausente: si la variable de entorno no está configurada, se rechaza TODO con
// 503 en vez de aceptar cualquier llamada sin validar nada.
// Comparación en tiempo constante (crypto.timingSafeEqual) para evitar timing
// attacks — se verifica primero que ambos strings tengan la MISMA longitud,
// porque timingSafeEqual lanza si los buffers difieren de tamaño.
function verifyKoreApiKey(req, res, next) {
  const expected = process.env.KORE_REVERSION_API_KEY;
  if (!expected) {
    return res.status(503).json({ error: 'Reversión de CxC no configurada' });
  }

  const received = req.headers['x-api-key'] || '';
  const a = Buffer.from(received);
  const b = Buffer.from(expected);
  const valid = a.length === b.length && crypto.timingSafeEqual(a, b);

  if (!valid) {
    return res.status(401).json({ error: 'API key inválida o ausente.' });
  }

  next();
}

module.exports = { verifyKoreApiKey };
