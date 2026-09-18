'use strict';

/**
 * ⚠️ ESTE SCRIPT ESCRIBE EN PRODUCCIÓN — no es un dry-run, no existe modo de solo lectura
 * posible acá: llama directo a procesarReversionKore(), la MISMA función que corre el
 * webhook real de Kore, que reconsulta Kore EN VIVO (con reintentos de hasta ~90s) y aplica
 * el resultado a Mongo. Solo correr esto:
 *   1) después de confirmar que el fix de _atribucionInconsistente (erp-reversion.service.js,
 *      2026-09-18 — restar la retención vigente antes de comparar) ya está desplegado en el
 *      ambiente donde se corre este script.
 *   2) contra el ambiente correcto (Kore PRODUCCIÓN, no Test — "son ambientes separados").
 *
 * Reprocesa puntualmente la reversión que Kore avisó el 2026-09-17 para erpId
 * 6a623ad30c57b7000171373b (CxC A0-260703646) — en ese momento el bug de retención hizo que
 * se marcara "atribución ambigua" en falso y NINGÚN movimiento se tocara (el depósito bancario
 * del usuario, folio 044518, se quedó con un link fantasma marcado 'identificado'). Kore no
 * reenvía el webhook solo — con el fix ya desplegado, este script vuelve a correr exactamente
 * la misma lógica con los datos reales de aquel evento para que ahora sí se resuelva bien.
 *
 * Uso: node src/banks/scripts/reprocesar-reversion-erpid.js
 */

require('dotenv').config();

const mongoose = require('mongoose');
const { procesarReversionKore } = require('../domains/erp/erp-reversion.service');

// Payload EXACTO del webhook real de Kore para este caso (confirmado por el usuario).
const PAYLOAD = {
  erpId:        '6a623ad30c57b7000171373b',
  fecha:        '2026-09-17T20:00:59.261479Z',
  folioExterno: '260703646',
  monto:        10024.57,
  motivo:       'DALI',
  referencia:   '6aac451e8faa5c000173e53a',
  serieExterna: 'A0',
};

async function run() {
  await mongoose.connect(process.env.MONGODB_URI);
  console.log('Conectado a MongoDB:', mongoose.connection.name, '@', mongoose.connection.host);
  console.log('Reprocesando reversión con payload:', JSON.stringify(PAYLOAD, null, 2));

  const resultado = await procesarReversionKore({
    erpId:           PAYLOAD.erpId,
    motivo:          PAYLOAD.motivo,
    fecha:           PAYLOAD.fecha,
    serieExterna:    PAYLOAD.serieExterna,
    folioExterno:    PAYLOAD.folioExterno,
    referencia:      PAYLOAD.referencia,
    payloadOriginal: PAYLOAD,
  });

  console.log('\nResultado:', JSON.stringify(resultado, null, 2));
  console.log('\nRevisá la bandeja de Reversiones CxC (GET /api/erp/cxc-reversiones) para confirmar');
  console.log('que este nuevo registro ya NO viene con atribucionConfiable:false.');

  await mongoose.connection.close();
}

run().catch(err => {
  console.error('Error:', err.message, err.stack);
  process.exit(1);
});
