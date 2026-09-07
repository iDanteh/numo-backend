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
    .select('_id banco fecha concepto deposito folio erpLinks')
    .lean();
  return { total: movimientos.length, movimientos };
}

module.exports = { listarPendientesDeFicha };
