'use strict';

const BankRule     = require('./BankRule.model');
const BankMovement = require('./BankMovement.model');
const { NotFoundError, BadRequestError } = require('../../shared/errors/AppError');

// ── Catálogos ─────────────────────────────────────────────────────────────────

const CAMPOS_VALIDOS = [
  'concepto', 'deposito', 'retiro', 'referenciaNumerica', 'numeroAutorizacion',
];
const OPERADORES_VALIDOS = [
  'contiene', 'no_contiene', 'igual',
  'empieza_con', 'termina_con',
  'mayor_que', 'menor_que', 'mayor_igual', 'menor_igual',
];
const OPERADORES_NUMERICOS = ['mayor_que', 'menor_que', 'mayor_igual', 'menor_igual'];

// ── Validación ────────────────────────────────────────────────────────────────

function validarRegla(data) {
  if (!data.nombre || !String(data.nombre).trim()) {
    throw new BadRequestError('El nombre de la regla es requerido');
  }
  if (!Array.isArray(data.condiciones) || data.condiciones.length === 0) {
    throw new BadRequestError('Se requiere al menos una condición');
  }
  for (const c of data.condiciones) {
    if (!CAMPOS_VALIDOS.includes(c.campo)) {
      throw new BadRequestError(`Campo inválido: ${c.campo}`);
    }
    if (!OPERADORES_VALIDOS.includes(c.operador)) {
      throw new BadRequestError(`Operador inválido: ${c.operador}`);
    }
    if (c.valor === undefined || c.valor === null || !String(c.valor).trim()) {
      throw new BadRequestError('El valor de la condición es requerido');
    }
    if (OPERADORES_NUMERICOS.includes(c.operador) && isNaN(parseFloat(c.valor))) {
      throw new BadRequestError(`El operador "${c.operador}" requiere un valor numérico`);
    }
  }
  if (data.logica !== undefined && !['Y', 'O'].includes(data.logica)) {
    throw new BadRequestError('Lógica debe ser "Y" o "O"');
  }
}

// ── Evaluación de condiciones ─────────────────────────────────────────────────

function matchCondicion(mov, cond) {
  const { campo, operador, valor } = cond;
  const fieldVal = mov[campo];

  if (['deposito', 'retiro'].includes(campo)) {
    const num      = parseFloat(valor);
    const fieldNum = Number(fieldVal) || 0;
    if (isNaN(num)) return false;
    switch (operador) {
      case 'mayor_que':   return fieldNum > num;
      case 'menor_que':   return fieldNum < num;
      case 'mayor_igual': return fieldNum >= num;
      case 'menor_igual': return fieldNum <= num;
      case 'igual':       return Math.abs(fieldNum - num) < 0.005;
      default: return false;
    }
  }

  const str = String(fieldVal || '').toLowerCase();
  const val = String(valor || '').toLowerCase().trim();
  switch (operador) {
    case 'contiene':    return str.includes(val);
    case 'no_contiene': return !str.includes(val);
    case 'igual':       return str === val;
    case 'empieza_con': return str.startsWith(val);
    case 'termina_con': return str.endsWith(val);
    default: return false;
  }
}

function matchRegla(mov, regla) {
  const { condiciones, logica } = regla;
  if (!condiciones || condiciones.length === 0) return false;
  if (logica === 'O') return condiciones.some(c => matchCondicion(mov, c));
  return condiciones.every(c => matchCondicion(mov, c));
}

// ── CRUD ──────────────────────────────────────────────────────────────────────

async function listRules(banco) {
  return BankRule.find({ banco }).sort({ orden: 1, createdAt: 1 }).lean();
}

async function createRule(banco, data) {
  validarRegla(data);
  const rule = await BankRule.create({
    banco,
    nombre:      String(data.nombre).trim(),
    condiciones: data.condiciones.map(c => ({
      campo:    c.campo,
      operador: c.operador,
      valor:    String(c.valor).trim(),
    })),
    logica: data.logica || 'Y',
    orden:  Number(data.orden) || 0,
  });
  return rule.toObject();
}

async function updateRule(id, data) {
  validarRegla(data);
  const rule = await BankRule.findById(id);
  if (!rule) throw new NotFoundError('Regla');
  rule.nombre      = String(data.nombre).trim();
  rule.condiciones = data.condiciones.map(c => ({
    campo: c.campo, operador: c.operador, valor: String(c.valor).trim(),
  }));
  rule.logica = data.logica ?? rule.logica;
  if (data.orden !== undefined) rule.orden = Number(data.orden);
  await rule.save();
  return rule.toObject();
}

async function deleteRule(id) {
  const rule = await BankRule.findByIdAndDelete(id);
  if (!rule) throw new NotFoundError('Regla');
  return { deleted: true };
}

async function reorderRules(ids) {
  if (!Array.isArray(ids)) throw new BadRequestError('ids debe ser un arreglo');
  const ops = ids.map((id, idx) => ({
    updateOne: { filter: { _id: id }, update: { $set: { orden: idx } } },
  }));
  await BankRule.bulkWrite(ops, { ordered: false });
  return { ok: true };
}

// ── Aplicar reglas a movimientos ──────────────────────────────────────────────

async function applyRules(banco, soloSinCategoria = false) {
  const rules = await BankRule.find({ banco }).sort({ orden: 1, createdAt: 1 }).lean();

  const matchFilter = { banco, isActive: true };
  if (soloSinCategoria) {
    matchFilter.$or = [{ categoria: null }, { categoria: { $exists: false } }];
  }

  const BATCH = 500;
  let skip       = 0;
  let actualizados = 0;
  let sinCambio    = 0;

  while (true) {
    const docs = await BankMovement.find(matchFilter).skip(skip).limit(BATCH).lean();
    if (docs.length === 0) break;

    const ops = [];
    for (const mov of docs) {
      let matched = null;
      for (const rule of rules) {
        if (matchRegla(mov, rule)) { matched = rule.nombre; break; }
      }
      const newCat = matched ?? null;
      const oldCat = mov.categoria ?? null;

      if (newCat !== oldCat) {
        ops.push({
          updateOne: {
            filter: { _id: mov._id },
            update: { $set: { categoria: newCat } },
          },
        });
        actualizados++;
      } else {
        sinCambio++;
      }
    }

    if (ops.length) await BankMovement.bulkWrite(ops, { ordered: false });

    skip += docs.length;
    if (docs.length < BATCH) break;
  }

  return { actualizados, sinCambio };
}

module.exports = {
  listRules, createRule, updateRule, deleteRule, reorderRules, applyRules, matchRegla
};
