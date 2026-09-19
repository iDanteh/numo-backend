'use strict';
const mongoose = require('mongoose');

// NetpayMatch — persistencia MÍNIMA del matching Netpay↔BBVA (ver netpay-match.service.js).
// A diferencia de CajaTransferencia (que sincroniza y guarda CADA transferencia que reporta
// Kore, sin importar su estado), esta colección NO tiene sync ni cron: la bandeja se calcula
// en vivo contra Kore en cada request (consultarTransaccionesNetpay ya existente), agrupando
// por terminalID+día. Acá solo se persiste lo YA RESUELTO por un humano — un grupo
// "pendiente" NUNCA tiene documento propio, es simplemente la ausencia de uno para esa clave
// (terminalID, dia). Esto evita modelar un estado 'pendiente' que habría que mantener
// sincronizado con Kore para nada (Kore no tiene noción de "grupo día+terminal", eso lo
// inventamos nosotros al agregar).
const netpayMatchSchema = new mongoose.Schema({
  terminalID: { type: String, required: true },
  almacen:    { type: String, default: null },

  // Medianoche UTC del día agrupado (mismo criterio de "solo fecha, sin hora" que
  // CajaTransferencia.fechaRecepcion truncada) — junto con terminalID es la clave real
  // de un grupo. No se usa terminalID+almacen porque un almacén puede tener más de una
  // terminal (ver project_netpay_transacciones.md) y el depósito es POR terminal.
  dia: { type: Date, required: true },

  // Snapshot del neto (monto - comisión) calculado al momento de resolver — para
  // trazabilidad/auditoría; el valor vivo se sigue recalculando en cada carga de la
  // bandeja contra Kore, este campo nunca se usa para decidir nada después de guardarse.
  netoEsperado: { type: Number, required: true },

  estatusMatch: {
    type: String,
    enum: ['matcheada', 'descartada-manual'],
    required: true,
  },

  // Trazabilidad de confirmación (solo si estatusMatch:'matcheada') — mismo patrón que
  // CajaTransferencia.confirmadoPor/confirmadoEn/movementIdsConfirmados.
  movementIdsConfirmados: { type: [mongoose.Schema.Types.ObjectId], ref: 'BankMovement', default: [] },
  confirmadoPor: {
    type: {
      userId: { type: String, default: null },
      nombre: { type: String, default: null },
    },
    default: null,
  },
  confirmadoEn: { type: Date, default: null },

  // Trazabilidad de descarte manual (solo si estatusMatch:'descartada-manual') — mismo
  // patrón que caja-transferencia-descartar-manual.service.js.
  descartadoManualmentePor: {
    type: {
      userId: { type: String, default: null },
      nombre: { type: String, default: null },
    },
    default: null,
  },
  descartadoManualmenteEn: { type: Date, default: null },
}, { timestamps: true, collection: 'netpay_matches' });

// Una clave (terminalID, dia), un resultado — evita doble-resolución del mismo grupo
// por una condición de carrera (dos admins confirmando/descartando el mismo grupo casi
// a la vez); confirmarMatchNetpay/descartarMatchNetpay igual re-validan antes de escribir,
// este índice es la última línea de defensa a nivel de datos.
netpayMatchSchema.index({ terminalID: 1, dia: 1 }, { unique: true });

module.exports = mongoose.model('NetpayMatch', netpayMatchSchema);
