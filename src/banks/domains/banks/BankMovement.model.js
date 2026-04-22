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
    enum:    ['no_identificado', 'identificado', 'otros'],
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

  // Snapshot por cada CxC vinculada: saldoActual y folioFiscal al momento de la vinculación
  erpLinks: {
    type: [{
      erpId:       { type: String, required: true },
      saldoActual: { type: Number, default: 0 },
      folioFiscal: { type: String, default: null },
      total:       { type: Number, default: null },
    }],
    default: [],
  },

  // Suma de saldoActual de todos los erpLinks; null cuando no hay vínculos
  saldoErp: { type: Number, default: null },

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
    }],
    default: [],
  },

  // Auditoría
  uploadedBy: { type: String, default: null },
  isActive:   { type: Boolean, default: true, index: true },
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

// Índice de texto para el buscador
bankMovementSchema.index({
  concepto:           'text',
  numeroAutorizacion: 'text',
  referenciaNumerica: 'text',
}, { default_language: 'spanish' });

module.exports = mongoose.model('BankMovement', bankMovementSchema);
