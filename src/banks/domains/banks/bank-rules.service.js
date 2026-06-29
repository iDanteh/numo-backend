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
const ACCIONES_VALIDAS       = ['categorizar', 'bloquear_identificacion', 'ocultar', 'cambiar_estado'];
const ESTADOS_DESTINO_VALIDOS = ['no_identificado', 'otros', 'reclasificado'];

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
  // estadoDestino obligatorio para cambiar_estado; opcional (pero validado) para categorizar
  if (data.accion === 'cambiar_estado') {
    if (!data.estadoDestino || !ESTADOS_DESTINO_VALIDOS.includes(data.estadoDestino)) {
      throw new BadRequestError(`estadoDestino debe ser: ${ESTADOS_DESTINO_VALIDOS.join(', ')}`);
    }
  } else if (data.estadoDestino && !ESTADOS_DESTINO_VALIDOS.includes(data.estadoDestino)) {
    throw new BadRequestError(`estadoDestino debe ser: ${ESTADOS_DESTINO_VALIDOS.join(', ')}`);
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

/**
 * Maps a Sequelize BankRule instance to the JSON shape expected by the frontend.
 * Adds `_id` as a string alias for `id` to maintain backward compatibility with
 * the frontend interface (originally designed for MongoDB where `_id` was the PK).
 */
function toRuleJSON(rule) {
  const json = rule.toJSON ? rule.toJSON() : rule;
  return { ...json, _id: String(json.id) };
}

/** Parses and validates a rule ID. Throws BadRequestError for non-integer values. */
function parseRuleId(id) {
  const n = parseInt(id, 10);
  if (isNaN(n)) throw new BadRequestError(`ID de regla inválido: "${id}"`);
  return n;
}

async function listRules(banco) {
  const rules = await bankRuleRepo.listByBanco(banco);
  return rules.map(toRuleJSON);
}

async function createRule(banco, data) {
  validarRegla(data);
  const rule = await bankRuleRepo.create(banco, data);
  return toRuleJSON(rule);
}

async function updateRule(id, data) {
  validarRegla(data);
  const ruleId = parseRuleId(id);

  const existing = await bankRuleRepo.findById(ruleId);
  if (!existing) throw new NotFoundError('Regla');

  const oldNombre      = existing.nombre;
  const oldEstado      = existing.estadoDestino ?? null;
  const { accion, banco } = existing;

  const rule = await bankRuleRepo.update(ruleId, data);
  if (!rule) throw new NotFoundError('Regla');

  let movSincronizados = 0;

  if (accion === 'categorizar') {
    const newNombre = String(data.nombre).trim();
    const newEstado = data.estadoDestino !== undefined ? (data.estadoDestino || null) : oldEstado;

    // Si cambió el nombre, reasignar categoria en movimientos que tenían el nombre anterior
    if (oldNombre !== newNombre) {
      const r = await BankMovement.updateMany(
        { banco, categoria: oldNombre, status: { $nin: ['reclasificado'] } },
        { $set: { categoria: newNombre } },
      );
      movSincronizados = r.modifiedCount ?? 0;
    }

    // Si cambió el estadoDestino, actualizar status en movimientos ya categorizados por esta regla
    if (oldEstado !== newEstado) {
      if (oldEstado) {
        // Revertir status anterior solo en movimientos que lo tienen igual al que puso la regla
        await BankMovement.updateMany(
          {
            banco,
            categoria: newNombre, // usar el nombre nuevo (ya sincronizado arriba)
            status: oldEstado,
          },
          { $set: { status: newEstado ?? 'no_identificado' } },
        );
      } else if (newEstado) {
        // La regla ahora cambia estado: aplicar a movimientos ya categorizados
        await BankMovement.updateMany(
          {
            banco,
            categoria: newNombre,
            status: { $nin: ['identificado', 'reclasificado'] },
          },
          { $set: { status: newEstado } },
        );
      }
    }
  }

  return { ...toRuleJSON(rule), movSincronizados };
}

async function deleteRule(id) {
  const ruleId = parseRuleId(id);

  const rule = await bankRuleRepo.findById(ruleId);
  if (!rule) throw new NotFoundError('Regla');

  let movRevertidos = 0;

  // Solo las reglas de categorización dejan huella rastreable en categoria
  if (rule.accion === 'categorizar') {
    const baseFilter = {
      banco:     rule.banco,
      categoria: rule.nombre,
      status:    { $nin: ['identificado', 'reclasificado'] },
    };

    if (rule.estadoDestino) {
      // Movimientos donde la regla también cambió el status → revertir ambos
      const r1 = await BankMovement.updateMany(
        { ...baseFilter, status: rule.estadoDestino },
        { $set: { categoria: null, status: 'no_identificado' } },
      );
      // Movimientos donde la regla solo categorizó (status diferente al que pone la regla)
      const r2 = await BankMovement.updateMany(
        { ...baseFilter, status: { $nin: ['identificado', 'reclasificado', rule.estadoDestino] } },
        { $set: { categoria: null } },
      );
      movRevertidos = (r1.modifiedCount ?? 0) + (r2.modifiedCount ?? 0);
    } else {
      const r = await BankMovement.updateMany(baseFilter, { $set: { categoria: null } });
      movRevertidos = r.modifiedCount ?? 0;
    }
  }

  const result = await bankRuleRepo.remove(ruleId);
  return { ...result, movRevertidos };
}

async function reorderRules(ids) {
  if (!Array.isArray(ids)) throw new BadRequestError('ids debe ser un arreglo');
  return bankRuleRepo.reorder(ids);
}

// ── Aplicar reglas a movimientos ──────────────────────────────────────────────

/**
 * Recorre todos los movimientos de un banco y aplica reglas de tipo
 * 'categorizar' y 'ocultar'. Las reglas de bloqueo aplican al identificar.
 */
async function applyRules(banco, soloSinCategoria = false) {
  const [catRules, ocultarRules, cambiarEstadoRules] = await Promise.all([
    bankRuleRepo.listByBanco(banco, { accion: 'categorizar' }),
    bankRuleRepo.listByBanco(banco, { accion: 'ocultar' }),
    bankRuleRepo.listByBanco(banco, { accion: 'cambiar_estado' }),
  ]);

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
      const isIdentificado = mov.status === 'identificado';

      // Categorizar: primera regla que aplica gana.
      // Reglas de prioridad de status:
      //   'reclasificado' → categoría manual, conservar; reglas de estado no aplican
      //   'identificado'  → movimiento confirmado; la categoría puede actualizarse pero el
      //                     status NUNCA se toca (protege datos que dependen de este estado)
      let matchedCat      = null;
      let matchedCatEstado = null; // estadoDestino de la catRule ganadora (si lo tiene)
      if (mov.status === 'reclasificado') {
        matchedCat = mov.categoria ?? null;
      } else {
        for (const rule of catRules) {
          if (matchRegla(mov, rule)) {
            matchedCat       = rule.nombre;
            // estadoDestino solo aplica si el movimiento no está identificado
            matchedCatEstado = !isIdentificado ? (rule.estadoDestino ?? null) : null;
            break;
          }
        }
      }
      // Ocultar: basta con que una regla aplique
      let shouldOcultar = false;
      for (const rule of ocultarRules) {
        if (matchRegla(mov, rule)) { shouldOcultar = true; break; }
      }
      // Cambiar estado: primero el estadoDestino de la catRule ganadora (si tiene),
      // luego las reglas cambiar_estado independientes.
      // Los movimientos identificados quedan excluidos de cualquier cambio de status.
      let matchedEstado = matchedCatEstado;
      if (!matchedEstado && !isIdentificado) {
        for (const rule of cambiarEstadoRules) {
          if (matchRegla(mov, rule)) { matchedEstado = rule.estadoDestino; break; }
        }
      }

      const newCat      = matchedCat ?? null;
      const oldCat      = mov.categoria ?? null;
      const oldOculto   = mov.oculto    ?? false;
      const catChanged    = newCat !== oldCat;
      const ocultoChanged = shouldOcultar !== oldOculto;
      const statusChanged = matchedEstado !== null && matchedEstado !== mov.status;

      if (catChanged || ocultoChanged || statusChanged) {
        const $set = {};
        if (catChanged)    $set.categoria = newCat;
        if (ocultoChanged) $set.oculto    = shouldOcultar;
        if (statusChanged) $set.status    = matchedEstado;
        ops.push({
          updateOne: {
            filter: { _id: mov._id },
            update: { $set },
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
