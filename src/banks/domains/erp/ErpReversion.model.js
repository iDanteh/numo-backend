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
    // 2026-08-21: 'sin_tocar' — la suma de aportes calculados para TODOS los movimientos de
    // este erpId no reconcilió contra lo que Kore dice pagado (ver
    // ErpReversion.atribucionConfiable) — ninguno de los campos de abajo aplica, el link de
    // este movimiento se dejó exactamente como estaba.
    tipo: { type: String, enum: ['desvinculado', 'ajustado', 'sin_tocar'], default: 'desvinculado' },

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

    // 2026-08-21 (caso real: Kore avisó una reversión de $100 que JAMÁS aplicó de su lado —
    // no fue una demora, el reverso no existe en su propio historial ni tiempo después).
    // Desde acá no hay forma de distinguir "Kore todavía no lo aplicó" de "Kore falló y
    // nunca lo va a aplicar" — ambos casos agotan los reintentos de erp-reversion.service.js
    // sin encontrar el match de `fecha` contra el historial de Kore. true cuando SÍ se
    // confirmó la reversión puntual contra Kore en vivo; false cuando se agotaron los
    // reintentos (o no se pudo reconsultar) sin lograrlo — los números aplicados igual son
    // los más recientes disponibles, esto solo marca que nadie verificó que Kore ya lo haya
    // reflejado de su lado. Default true: documentos de antes de este campo nunca pasaron
    // por esta verificación, no hay evidencia para marcarlos como dudosos retroactivamente.
    confirmadaEnKore: { type: Boolean, default: true },

    // 2026-08-21 (caso real, folioExterno 260800164, CxC pagada por 2 movimientos bancarios
    // distintos): las entradas 'REV ABO' de Kore no traen Aut/Numo propio — el algoritmo que
    // decide cuánto le corresponde a cada movimiento puede atribuir la MISMA reversión a 2
    // movimientos distintos cuando sus acumuladores coinciden en magnitud (bug preexistente
    // de _montoSaldoLinkPorMovimiento, normalmente tapado por el "ratchet" de los jobs de
    // sync — la reversión corre sin ese ratchet a propósito). false cuando la suma de lo
    // calculado para todos los movimientos de este erpId NO reconcilió contra lo que Kore
    // dice pagado — en ese caso NINGÚN link se tocó (todos quedan 'sin_tocar'), a propósito,
    // para no desvincular algo que puede seguir vigente de verdad. Default true: documentos
    // de antes de este campo, o con un solo movimiento vinculado (sin ambigüedad posible).
    atribucionConfiable: { type: Boolean, default: true },

    movimientosAfectados: { type: [movimientoAfectadoSchema], default: [] },

    estado:       { type: String, enum: ['aplicada', 'revertida'], default: 'aplicada' },
    revertidoPor: { type: String, default: null },
    revertidoEn:  { type: Date, default: null },
}, { timestamps: true, collection: 'erp_reversiones' });

module.exports = mongoose.model('ErpReversion', erpReversionSchema);
