'use strict';

const mongoose = require('mongoose');

// conTransaccion — ejecuta `fn(session)` dentro de una transacción Mongo
// cuando la topología de la conexión soporta réplica set/sharded, o
// directamente `fn(null)` sin sesión cuando es standalone. Puerto genérico
// (D5) de la misma detección + fallback que
// bank-autorizaciones.service.js#ejecutarBulkConTransaccion (líneas
// 161-197 de ese archivo), generalizada para cualquier función async en vez
// de solo BankMovement.bulkWrite — así setErpIds()/cr.save() (que además
// corren checks de RBAC/identificadoPor/aplicarLogicaErp que no se pueden
// expresar como bulk ops) pueden compartir la misma semántica de detección.
//
// La topología se detecta ANTES de abrir sesión para evitar que
// startSession() quede bufferizada y provoque un timeout de 10s en
// standalone (mismo motivo que en ejecutarBulkConTransaccion).
async function conTransaccion(fn) {
  const topologyType = mongoose.connection.client?.topology?.description?.type;
  const esReplicaSet = topologyType === 'ReplicaSetWithPrimary'
    || topologyType === 'ReplicaSetNoPrimary'
    || topologyType === 'Sharded';

  if (!esReplicaSet) {
    return fn(null);
  }

  let session = null;
  try {
    session = await mongoose.connection.startSession();
    session.startTransaction();
    const resultado = await fn(session);
    await session.commitTransaction();
    return resultado;
  } catch (err) {
    if (session?.inTransaction?.()) {
      try { await session.abortTransaction(); } catch (_) { /* ignorar */ }
    }
    // Fallback por si la detección de topología no fue suficiente — mismos
    // criterios que ejecutarBulkConTransaccion. La transacción abortada
    // garantiza que ningún write de `fn` haya quedado comiteado, así que
    // repetir `fn(null)` es seguro.
    const sinSoporte = err.code === 20
      || /transaction numbers are only allowed/i.test(err.message)
      || /replica/i.test(err.message);
    if (sinSoporte) {
      return fn(null);
    }
    throw err;
  } finally {
    if (session) {
      try { await session.endSession(); } catch (_) { /* ignorar */ }
    }
  }
}

module.exports = { conTransaccion };
