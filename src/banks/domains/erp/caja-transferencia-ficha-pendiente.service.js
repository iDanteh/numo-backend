'use strict';

// caja-transferencia-ficha-pendiente.service.js — pedido explícito del usuario 2026-09-03:
// un BankMovement puede quedar 'identificado' por un match automático de transferencia
// entre cajas (origen:'transferencia-caja', ver caja-transferencia-confirm.service.js) sin
// que el contador haya cargado todavía la `ficha` (folio del comprobante físico,
// BankMovement.model.js) como respaldo documental. Este servicio lista esos movimientos
// pendientes para el ícono de aviso por fila que ya existe en la tabla de Bancos (frontend).

const BankMovement = require('../banks/BankMovement.model');

const LIMIT = 200; // volumen esperado bajo (mismo orden de magnitud que transferencias-caja) — sin paginación real por ahora

// Nota: si movimientos.length === LIMIT, `total` queda truncado (no refleja el total real
// en Mongo, solo lo que trajo esta página). No hace falta resolverlo ahora, pero queda
// señalado para quien lo retome si el volumen crece.
async function listarPendientesDeFicha() {
  const movimientos = await BankMovement.find({ 'erpLinks.origen': 'transferencia-caja', ficha: null })
    .sort({ fecha: -1 })
    .limit(LIMIT)
    // Bug real 2026-09-14: esta proyección solo traía lo que la propia bandeja mostraba en
    // cada fila — pero erp-modal.component.ts (el modal que este panel abre vía "Cargar
    // ficha") depende de `erpIds` para detectar modoSoloFicha (sin él, cae al flujo normal
    // de CxC: sección completa visible + consulta al ERP, "diseño completamente distinto"
    // reportado por el usuario) y de los campos `fichaDrive*`/`retiro`/`status` para
    // renderizar el resto de la tarjeta de ficha correctamente. Se agregan TODOS los campos
    // que erp-modal.component lee de `movement` (mismo criterio que confirmarTransferencia
    // CajaMatch, que ya devuelve el documento completo) — `ficha`/`fichaAt`/`fichaBy`/
    // `fichaNombre` siempre van a venir null/vacíos acá (la query ya filtra `ficha:null`),
    // se incluyen igual por si ese filtro cambia en el futuro.
    .select('_id banco fecha concepto deposito retiro folio status erpIds erpLinks '
      + 'ficha fichaAt fichaBy fichaNombre fichaDriveFileId fichaDriveWebViewLink fichaDriveMimeType')
    .lean();
  return { total: movimientos.length, movimientos };
}

module.exports = { listarPendientesDeFicha };
