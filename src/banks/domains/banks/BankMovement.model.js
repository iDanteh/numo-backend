const mongoose = require('mongoose');

const bankMovementSchema = new mongoose.Schema({
  banco: {
    type: String,
    enum: [
      'Banamex', 'BBVA', 'Santander', 'Azteca',
      'Banorte', 'HSBC', 'Inbursa', 'Scotiabank',
      'BanBajío', 'Afirme', 'Intercam', 'Nu',
      'Spin', 'Hey Banco', 'Albo',
    ],
    required: true,
    index: true,
  },

  fecha: { type: Date, required: true, index: true },

  // Concepto completo (concatenación de todas las sub-filas donde aplique)
  concepto: { type: String, trim: true },

  // Montos
  deposito: { type: Number, default: null },
  retiro:   { type: Number, default: null },
  saldo:    { type: Number, default: null },

  // Identificadores del movimiento
  numeroAutorizacion: { type: String, trim: true, default: null },
  referenciaNumerica: { type: String, trim: true, default: null },

  // Estado de conciliación del movimiento
  status: {
    type:    String,
    enum:    ['no_identificado', 'identificado', 'otros', 'reclasificado'],
    default: 'no_identificado',
    index:   true,
  },

  // Categoría inferida del concepto
  categoria: {
    type:    String,
    default: null,
    index:   true,
  },

  // Folio auto-incremental de 6 dígitos (000001, 000002…)
  folio: { type: String, default: null },

  // UUID del CFDI; sólo se gestiona automáticamente vía folioFiscal del ERP
  uuidXML: { type: String, default: null },

  // IDs de CxC provenientes del ERP externo (N por movimiento)
  erpIds: { type: [String], default: [] },

  // Snapshot por cada CxC vinculada: saldoActual, folioFiscal, serie y folioExterno al momento de la vinculación
  erpLinks: {
    type: [{
      erpId:            { type: String, required: true },
      saldoActual:      { type: Number, default: 0 },
      // Acumulado cobrado por transferencia/depósito en efectivo (forma "bancaria") —
      // alimenta el badge/dropdown "CxC vinculadas" en la tabla de movimientos.
      saldoPagado:      { type: Number, default: null },
      // Acumulado cobrado por TODAS las formas de pago (transferencia, efectivo, cheque,
      // tarjeta, etc.) — es la fuente de saldoErp/status en aplicarLogicaErp. Ver
      // bank.service.js. Deliberadamente separado de saldoPagado: ese sigue siendo
      // bancario-only para no inflar el dropdown de CxC vinculadas con formas no bancarias.
      saldoPagadoTotal: { type: Number, default: null },
      folioFiscal:    { type: String, default: null },
      total:          { type: Number, default: null },
      serie:          { type: String, default: null },
      folioExterno:   { type: String, default: null },
      tieneRetencion: { type: Boolean, default: false },
      tipoPago:       { type: String, default: null },
      // Snapshot de movimientos Kore para esta CxC (todos menos el primero — el primero
      // es el cargo original, no aporta al rastreo de conciliación). Lo llena el job
      // "Sync Histórico Kore" (independiente de Sync Saldo ERP — solo enriquece, nunca
      // toca saldoErp/tipoPago/total).
      movimientosKore: {
        type: [{
          serie:         { type: String, default: null },
          folio:         { type: String, default: null },
          serieOrigen:   { type: String, default: null },
          folioOrigen:   { type: String, default: null },
          fecha:         { type: Date,   default: null },
          saldoAnterior: { type: Number, default: null },
          saldoActual:   { type: Number, default: null },
          subtotal:      { type: Number, default: null },
          impuesto:      { type: Number, default: null },
          total:         { type: Number, default: null },
        }],
        default: [],
      },
      // Checkpoint de "Sync Histórico Kore" — se marca SOLO cuando Kore respondió con
      // éxito para este link (aunque el movimientosKore resultante quede vacío). Si Kore
      // falla, queda null y la siguiente corrida reintenta sola (no necesita un botón de
      // "reiniciar checkpoint" como el de Sync Saldo ERP).
      movimientosKoreSyncedAt: { type: Date, default: null },
      // jobId de la corrida que escribió el movimientosKore actual — permite revertir
      // selectivamente solo los links tocados por esa corrida específica.
      movimientosKoreRunId: { type: String, default: null },
    }],
    default: [],
  },

  // Suma de saldoActual de todos los erpLinks; null cuando no hay vínculos
  saldoErp: { type: Number, default: null },

  // Timestamp que indica cuándo fue procesado por el job Sync Saldo ERP.
  // null = pendiente (se incluye en un "reanudar"); Date = ya procesado (se salta en reanudar).
  saldoErpSyncedAt: { type: Date, default: null },

  // Nombre del cliente identificado mediante el catálogo auxiliar
  auxNombre: { type: String, default: null, index: true },

  // Hash de deduplicación: SHA-256 de campos clave, evita duplicados al
  // volver a cargar el mismo archivo.
  hash: { type: String },

  // Historial de usuarios que han relacionado una CxC a este movimiento.
  // Cada entrada representa una asociación (userId + CxC específica).
  // Se añade al vincular una CxC nueva y se elimina al desvincularla.
  identificadoPor: {
    type: [{
      userId:  { type: String, default: null },
      nombre:  { type: String, default: null },
      fechaId: { type: Date,   default: null },
      erpId:   { type: String, default: null },  // CxC que este usuario asoció
      source:  { type: String, default: null },  // 'conciliacion-import' para bulk imports por Excel
      runId:   { type: String, default: null },  // ID único de la operación bulk (permite revert selectivo)
    }],
    default: [],
  },

  // Ficha física (ticket de depósito): folio del comprobante físico
  ficha:       { type: String, default: null },
  fichaBy:     { type: String, default: null },   // userId que registró la ficha
  fichaNombre: { type: String, default: null },   // nombre display del usuario
  fichaAt:     { type: Date,   default: null },

  // Oculto por regla — el movimiento existe pero no aparece en vistas normales
  oculto: { type: Boolean, default: false, index: true },

  // Oculto solo para estos roles (regla 'ocultar' con ocultarRoles). Independiente de `oculto`.
  ocultoRoles: { type: [String], default: [] },

  // Auditoría
  uploadedBy: { type: String, default: null },
  isActive:   { type: Boolean, default: true, index: true },

  // Registro de cambios automáticos durante importación/enriquecimiento.
  // Cada entrada es inmutable: documenta qué se modificó, por qué capa y desde qué archivo.
  _changelog: {
    type: [{
      at:         { type: Date,   required: true },
      via:        { type: String, required: true },  // 'capa1e' | 'enrich-capa1a' | …
      campo:      { type: String, default: null },   // campo modificado, o null si son varios
      campos:     { type: [String], default: [] },   // lista de campos (para enriquecimientos)
      de:         { type: mongoose.Schema.Types.Mixed, default: null },
      a:          { type: mongoose.Schema.Types.Mixed, default: null },
      importFile: { type: String, default: null },
      runId:      { type: String, default: null },   // ID de la corrida bulk que generó este cambio (permite revert selectivo)
      revertedAt: { type: Date,   default: null },    // se marca al revertir, la entrada nunca se borra (rastro de auditoría)
    }],
    default: [],
  },
}, {
  timestamps: true,
  collection: 'bank_movements',
});

// Índice único sobre el hash — garantiza que el mismo movimiento no se duplique
bankMovementSchema.index({ hash:  1 }, { unique: true, sparse: true });
bankMovementSchema.index({ folio: 1 }, { unique: true, sparse: true });

// Índices compuestos para consultas comunes
bankMovementSchema.index({ banco: 1, fecha: -1 });
bankMovementSchema.index({ fecha: -1, banco: 1 });
bankMovementSchema.index({ numeroAutorizacion: 1, banco: 1 });
bankMovementSchema.index({ banco: 1, status: 1 });
bankMovementSchema.index({ banco: 1, categoria: 1 });
// Índices para el motor Match ERP
bankMovementSchema.index({ isActive: 1, status: 1, deposito: 1 });
bankMovementSchema.index({ 'identificadoPor.userId': 1 });
bankMovementSchema.index({ erpIds: 1, isActive: 1 });

// Índice de texto para el buscador
bankMovementSchema.index({
  concepto:           'text',
  numeroAutorizacion: 'text',
  referenciaNumerica: 'text',
}, { default_language: 'spanish' });

module.exports = mongoose.model('BankMovement', bankMovementSchema);
