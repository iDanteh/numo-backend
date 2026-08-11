'use strict';
const mongoose = require('mongoose');

const movimientoAfectadoSchema = new mongoose.Schema({
    movementId: { type: mongoose.Schema.Types.ObjectId, ref: 'BankMovement', required: true },

    // Snapshot COMPLETO de lo que había en el movimiento antes de removerlo — permite que
    // "revertir" restaure exactamente lo mismo que Kore hizo desaparecer, tal cual estaba.
    erpLinkRemovido:         { type: mongoose.Schema.Types.Mixed, required: true },
    identificadoPorRemovido: { type: mongoose.Schema.Types.Mixed, default: null },
}, { _id: false });

const erpReversionSchema = new mongoose.Schema({

    // CxC (ID del ERP) que Kore avisó que revirtió/canceló.
    erpId: { type: String, required: true, index: true },

    motivo:             { type: String, default: null },
    fechaKore:          { type: Date, default: null },
    serieExterna:       { type: String, default: null },
    folioExterno:       { type: String, default: null },
    referencia:         { type: String, default: null },
    serieFolioMismatch: { type: Boolean, default: false },

    // Payload crudo tal cual lo mandó Kore, para auditoría/depuración.
    payloadOriginal: { type: mongoose.Schema.Types.Mixed, default: null },

    movimientosAfectados: { type: [movimientoAfectadoSchema], default: [] },

    estado:       { type: String, enum: ['aplicada', 'revertida'], default: 'aplicada' },
    revertidoPor: { type: String, default: null },
    revertidoEn:  { type: Date, default: null },
}, { timestamps: true, collection: 'erp_reversiones' });

module.exports = mongoose.model('ErpReversion', erpReversionSchema);
