'use strict';

const crypto = require('crypto');

// verifyKoreApiKey — autenticación server-to-server para TODOS los endpoints que
// consume Kore directamente (sin JWT/Auth0): el webhook de reversión de CxC
// (POST /api/erp/cxc-reversiones) y los endpoints de Solicitudes de Cobro que llama
// el ERP (crear solicitud, consultar por id, cancelar). Un solo token compartido
// (KORE_API_KEY) para toda esta superficie — antes había 2 variables independientes
// (COLLECTION_REQUESTS_API_KEY / KORE_REVERSION_API_KEY) protegiendo el mismo tipo
// de llamada server-to-server, sin motivo real para que fueran secretos distintos.
// Si la variable de entorno no está configurada, se rechaza TODO con 503 en vez de
// aceptar cualquier llamada sin validar nada.
// Comparación en tiempo constante (crypto.timingSafeEqual) para evitar timing
// attacks — se verifica primero que ambos strings tengan la MISMA longitud, porque
// timingSafeEqual lanza si los buffers difieren de tamaño.
function verifyKoreApiKey(req, res, next) {
  const expected = process.env.KORE_API_KEY;
  if (!expected) {
    return res.status(503).json({ error: 'Integración con Kore no configurada en el servidor' });
  }

  const received = req.get('X-Api-Key') || '';
  const a = Buffer.from(received);
  const b = Buffer.from(expected);
  const valid = a.length === b.length && crypto.timingSafeEqual(a, b);

  if (!valid) {
    return res.status(401).json({ error: 'API key inválida o ausente.' });
  }

  next();
}

module.exports = { verifyKoreApiKey };
