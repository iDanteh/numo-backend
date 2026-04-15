const mongoose = require('mongoose');

// ── Helpers ─────────────────────────────────────────────────────────────────
/**
 * Nivel jerárquico para el formato SAT de 10 dígitos.
 *   sig.length 1 → nivel 1
 *   sig.length 2 → nivel 2
 *   sig.length 4 → nivel 3  (sig.length / 2 + 1)
 *   sig.length 6 → nivel 4
 */
function codigoToNivel(codigo = '') {
  const sig = codigo.replace(/0+$/, '') || codigo[0] || '0';
  return sig.length === 1 ? 1 : Math.floor(sig.length / 2) + 1;
}

/**
 * Infiere tipo contable y naturaleza del primer dígito del código SAT.
 *   1 → ACTIVO / DEUDORA
 *   2 → PASIVO / ACREEDORA
 *   3 → CAPITAL / ACREEDORA
 *   4 → INGRESO / ACREEDORA
 *   5-9 → GASTO / DEUDORA
 */
function inferTipoNat(codigo) {
  const map = {
    '1': { tipo: 'ACTIVO',  naturaleza: 'DEUDORA'   },
    '2': { tipo: 'PASIVO',  naturaleza: 'ACREEDORA'  },
    '3': { tipo: 'CAPITAL', naturaleza: 'ACREEDORA'  },
    '4': { tipo: 'INGRESO', naturaleza: 'ACREEDORA'  },
    '5': { tipo: 'GASTO',   naturaleza: 'DEUDORA'    },
    '6': { tipo: 'GASTO',   naturaleza: 'DEUDORA'    },
    '7': { tipo: 'GASTO',   naturaleza: 'DEUDORA'    },
    '8': { tipo: 'GASTO',   naturaleza: 'DEUDORA'    },
    '9': { tipo: 'GASTO',   naturaleza: 'DEUDORA'    },
  };
  return map[String(codigo).trim()[0]] || null;
}

// ── Schema ───────────────────────────────────────────────────────────────────
const accountPlanSchema = new mongoose.Schema(
  {
    codigo: {
      type: String,
      required: [true, 'El código es obligatorio'],
      unique: true,
      trim: true,
    },

    nombre: {
      type: String,
      required: [true, 'El nombre es obligatorio'],
      trim: true,
    },

    // Código de la cuenta de mayor a la que pertenece (estructura SAT).
    // Null = cuenta raíz o cuenta de mayor sin padre.
    ctaMayor: { type: String, trim: true, default: null, index: true },

    // Auto-derivados del primer dígito del código (nunca se ingresan manualmente)
    tipo: {
      type: String,
      enum: ['ACTIVO', 'PASIVO', 'CAPITAL', 'INGRESO', 'GASTO'],
      required: true,
    },
    naturaleza: {
      type: String,
      enum: ['DEUDORA', 'ACREEDORA'],
      required: true,
    },

    // Nivel jerárquico (auto-calculado en pre-save)
    nivel: { type: Number, min: 1 },

    // Referencia al padre en el árbol (auto-calculada desde ctaMayor)
    parentId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: 'AccountPlan',
      default: null,
      index: true,
    },

    isActive: { type: Boolean, default: true, index: true },
  },
  {
    timestamps: true,
    collection: 'account_plan',
  },
);

// ── Índices ──────────────────────────────────────────────────────────────────
accountPlanSchema.index({ tipo: 1 });
accountPlanSchema.index({ naturaleza: 1 });
accountPlanSchema.index({ nivel: 1, codigo: 1 });
accountPlanSchema.index(
  { codigo: 'text', nombre: 'text' },
  { default_language: 'spanish' },
);

// ── Pre-validate: auto-derivar tipo, naturaleza y nivel ──────────────────────
// Corre ANTES de la validación de campos requeridos.
accountPlanSchema.pre('validate', function (next) {
  if (!this.nivel) this.nivel = codigoToNivel(this.codigo);
  const inferred = inferTipoNat(this.codigo);
  if (inferred) {
    this.tipo       = inferred.tipo;
    this.naturaleza = inferred.naturaleza;
  }
  next();
});

// ── Static helpers ───────────────────────────────────────────────────────────
accountPlanSchema.statics.computeNivel  = codigoToNivel;
accountPlanSchema.statics.inferTipoNat  = inferTipoNat;

module.exports = mongoose.model('AccountPlan', accountPlanSchema);
