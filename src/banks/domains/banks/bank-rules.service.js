'use strict';

const bankRuleRepo  = require('./repositories/bank-rule.repository');
const BankMovement  = require('./BankMovement.model');
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
const ACCIONES_VALIDAS     = ['categorizar', 'bloquear_identificacion'];

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
  if (data.accion !== undefined && !ACCIONES_VALIDAS.includes(data.accion)) {
    throw new BadRequestError(`Acción inválida. Debe ser: ${ACCIONES_VALIDAS.join(', ')}`);
  }
  // mensajeBloqueo solo aplica cuando la acción es bloquear
  if (data.accion === 'bloquear_identificacion' && data.mensajeBloqueo) {
    if (String(data.mensajeBloqueo).trim().length > 500) {
      throw new BadRequestError('mensajeBloqueo no puede superar 500 caracteres');
    }
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
  const val = String(valor    || '').toLowerCase().trim();
  switch (operador) {
    case 'contiene':    return str.includes(val);
    case 'no_contiene': return !str.includes(val);
    case 'igual':       return str === val;
    case 'empieza_con': return str.startsWith(val);
    case 'termina_con': return str.endsWith(val);
    default: return false;
  }
}

/**
 * Evalúa si un movimiento cumple todas (Y) o alguna (O) condición de la regla.
 * Funciona con instancias Sequelize y documentos Mongoose (acceso por propiedad).
 */
function matchRegla(mov, regla) {
  const condiciones = regla.condiciones ?? [];
  const logica      = regla.logica      ?? 'Y';
  if (condiciones.length === 0) return false;
  if (logica === 'O') return condiciones.some(c => matchCondicion(mov, c));
  return condiciones.every(c => matchCondicion(mov, c));
}

// ── CRUD ──────────────────────────────────────────────────────────────────────

async function listRules(banco) {
  const rules = await bankRuleRepo.listByBanco(banco);
  return rules.map(r => r.toJSON());
}

async function createRule(banco, data) {
  validarRegla(data);
  const rule = await bankRuleRepo.create(banco, data);
  return rule.toJSON();
}

async function updateRule(id, data) {
  validarRegla(data);
  const rule = await bankRuleRepo.update(id, data);
  if (!rule) throw new NotFoundError('Regla');
  return rule.toJSON();
}

async function deleteRule(id) {
  const result = await bankRuleRepo.remove(id);
  if (!result) throw new NotFoundError('Regla');
  return result;
}

async function reorderRules(ids) {
  if (!Array.isArray(ids)) throw new BadRequestError('ids debe ser un arreglo');
  return bankRuleRepo.reorder(ids);
}

// ── Aplicar reglas a movimientos ──────────────────────────────────────────────

/**
 * Recorre todos los movimientos de un banco y aplica las reglas con
 * accion='categorizar'. Las reglas de bloqueo no participan en este proceso.
 */
async function applyRules(banco, soloSinCategoria = false) {
  // Solo reglas de categorización — el bloqueo aplica al momento de identificar
  const rules = await bankRuleRepo.listByBanco(banco, { accion: 'categorizar' });

  const matchFilter = { banco, isActive: true };
  if (soloSinCategoria) {
    matchFilter.$or = [{ categoria: null }, { categoria: { $exists: false } }];
  }

  const BATCH = 500;
  let skip        = 0;
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
  listRules, createRule, updateRule, deleteRule, reorderRules, applyRules, matchRegla,
};
