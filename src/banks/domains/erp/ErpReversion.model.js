'use strict';
const mongoose = require('mongoose');

const movimientoAfectadoSchema = new mongoose.Schema({
    movementId: { type: mongoose.Schema.Types.ObjectId, ref: 'BankMovement', required: true },

    // 2026-08-20: una reversión de Kore ya no siempre implica desvincular el erpId por
    // completo de este movimiento — si la CxC tenía varios abonos y solo uno se revirtió,
    // el link se AJUSTA (se corrige el aporte) en vez de desaparecer. 'desvinculado'
    // (comportamiento original) rellena erpLinkRemovido/identificadoPorRemovido;
    // 'ajustado' rellena erpLinkAjustado. Default 'desvinculado' por compatibilidad con
    // documentos ya existentes de antes de este campo.
    tipo: { type: String, enum: ['desvinculado', 'ajustado'], default: 'desvinculado' },

    // Snapshot COMPLETO de lo que había en el movimiento antes de removerlo — permite que
    // "revertir" restaure exactamente lo mismo que Kore hizo desaparecer, tal cual estaba.
    // Solo aplica cuando tipo==='desvinculado'.
    erpLinkRemovido:         { type: mongoose.Schema.Types.Mixed, default: null },
    identificadoPorRemovido: { type: mongoose.Schema.Types.Mixed, default: null },

    // Snapshot antes/después del erpLink cuando tipo==='ajustado' (el link sigue existiendo,
    // solo cambiaron sus números tras la reconsulta a Kore).
    erpLinkAjustado: { type: mongoose.Schema.Types.Mixed, default: null },
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
