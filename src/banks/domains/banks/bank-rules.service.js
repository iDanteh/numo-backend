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
const ACCIONES_VALIDAS       = ['categorizar', 'bloquear_identificacion', 'cambiar_estado'];
const ESTADOS_DESTINO_VALIDOS = ['no_identificado', 'otros', 'reclasificado'];
// Roles a los que se les puede ocultar una categoría (campo extra de 'categorizar').
// Cerrado a propósito: son los únicos roles con acceso a movimientos bancarios además
// de admin (que siempre ve todo vía banks:admin) — Tienda no tiene banks:read.
const OCULTAR_ROLES_VALIDOS = ['contabilidad', 'cobranza'];

// ── Validación ────────────────────────────────────────────────────────────────

function validarRegla(data) {
  if (!data.nombre || !String(data.nombre).trim()) {
    throw new BadRequestError('El nombre de la regla es requerido');
  }
  if (!Array.isArray(data.condiciones)) {
    throw new BadRequestError('condiciones debe ser un arreglo');
  }
  // 'categorizar' puede quedar sin condiciones: sirve para definir el catálogo de una
  // categoría (nombre, estadoDestino, ocultarRoles) sin match automático — matchRegla()
  // ya la excluye siempre de import/applyRules() cuando no tiene condiciones, así que
  // nunca se auto-aplica; solo queda disponible para asignación manual (updateCategoria).
  if (data.condiciones.length === 0 && data.accion !== 'categorizar') {
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
  // ocultarRoles es un campo extra de 'categorizar' (igual que "también cambiar estado")
  if (data.ocultarRoles !== undefined && data.ocultarRoles !== null && data.ocultarRoles.length > 0) {
    if (!Array.isArray(data.ocultarRoles) || data.ocultarRoles.some(r => !OCULTAR_ROLES_VALIDOS.includes(r))) {
      throw new BadRequestError(`ocultarRoles debe ser un subconjunto de: ${OCULTAR_ROLES_VALIDOS.join(', ')}`);
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

/** Compara dos arreglos de roles sin importar el orden. */
function sameRoleSet(a, b) {
  if (a.length !== b.length) return false;
  const sortedA = [...a].sort();
  const sortedB = [...b].sort();
  return sortedA.every((v, i) => v === sortedB[i]);
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

  const oldNombre        = existing.nombre;
  const oldEstado        = existing.estadoDestino ?? null;
  const oldOcultarRoles  = existing.ocultarRoles  ?? [];
  const { accion, banco } = existing;

  const rule = await bankRuleRepo.update(ruleId, data);
  if (!rule) throw new NotFoundError('Regla');

  let movSincronizados = 0;

  if (accion === 'categorizar') {
    const newNombre = String(data.nombre).trim();
    const newEstado = data.estadoDestino !== undefined ? (data.estadoDestino || null) : oldEstado;
    const newOcultarRoles = data.ocultarRoles !== undefined ? (data.ocultarRoles || []) : oldOcultarRoles;

    // Si cambió el nombre, reasignar categoria en movimientos que tenían el nombre anterior.
    // Sin filtro de status: 'reclasificado' es justamente el status por defecto que esta
    // misma regla les asigna cuando no tiene estadoDestino, así que excluirlo dejaba
    // huérfanos (nombre viejo, sin ocultarRoles/estadoDestino) a la mayoría de los
    // movimientos ya categorizados por la regla.
    if (oldNombre !== newNombre) {
      const r = await BankMovement.updateMany(
        { banco, categoria: oldNombre },
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
        // La regla ahora cambia estado: aplicar a movimientos ya categorizados.
        // 'identificado' es el único status protegido de cambios de status por reglas;
        // 'reclasificado' (Por conciliar) sí debe recibir el cambio de estado.
        await BankMovement.updateMany(
          {
            banco,
            categoria: newNombre,
            status: { $ne: 'identificado' },
          },
          { $set: { status: newEstado } },
        );
      }
    }

    // Si cambiaron los roles a los que se oculta, propagar a los movimientos ya
    // categorizados por esta regla (sin excluir ningún status: ocultar es visibilidad,
    // no afecta el status protegido de 'identificado').
    if (!sameRoleSet(oldOcultarRoles, newOcultarRoles)) {
      await BankMovement.updateMany(
        { banco, categoria: newNombre },
        { $set: { ocultoRoles: newOcultarRoles } },
      );
    }
  }

  return { ...toRuleJSON(rule), movSincronizados };
}

async function deleteRule(id) {
  const ruleId = parseRuleId(id);

  const rule = await bankRuleRepo.findById(ruleId);
  if (!rule) throw new NotFoundError('Regla');

  let movRevertidos = 0;

  // Solo las reglas de categorización dejan huella rastreable en categoria.
  // La categoría huérfana se limpia en TODOS los movimientos que la tengan, sin importar
  // su status — una regla eliminada no debe seguir apareciendo en movimientos ni en el
  // filtro del reporte de Excel (que se alimenta de los valores distintos de `categoria`
  // en Mongo, ver listCategories()). El único campo protegido para 'identificado' es el
  // `status` (nunca se le cambia por una regla), no la etiqueta de `categoria`.
  if (rule.accion === 'categorizar') {
    const baseFilter = { banco: rule.banco, categoria: rule.nombre };

    if (rule.estadoDestino) {
      // Movimientos donde la regla también había cambiado el status → revertir ambos.
      // estadoDestino nunca puede ser 'identificado' (ver ESTADOS_DESTINO_VALIDOS),
      // así que este revert de status jamás toca un movimiento identificado.
      const r1 = await BankMovement.updateMany(
        { ...baseFilter, status: rule.estadoDestino },
        { $set: { categoria: null, status: 'no_identificado', ocultoRoles: [] } },
      );
      // Resto de movimientos con esta categoría (incluye 'identificado' y 'reclasificado'):
      // solo se limpia la categoría huérfana (y el ocultamiento por rol), sin tocar su status.
      const r2 = await BankMovement.updateMany(
        { ...baseFilter, status: { $ne: rule.estadoDestino } },
        { $set: { categoria: null, ocultoRoles: [] } },
      );
      movRevertidos = (r1.modifiedCount ?? 0) + (r2.modifiedCount ?? 0);
    } else {
      const r = await BankMovement.updateMany(baseFilter, { $set: { categoria: null, ocultoRoles: [] } });
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
 * Recorre todos los movimientos de un banco y aplica reglas de tipo 'categorizar'
 * (incluye su campo extra de ocultar-por-rol) y 'cambiar_estado'. Las reglas de
 * bloqueo aplican al identificar.
 */
async function applyRules(banco, soloSinCategoria = false) {
  const [catRules, cambiarEstadoRules] = await Promise.all([
    bankRuleRepo.listByBanco(banco, { accion: 'categorizar' }),
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
      //   'reclasificado' → SÍ se re-evalúan condiciones (2026-08-27, antes se
      //                     conservaba la categoría manual sin tocar): si una regla
      //                     matchea por concepto/depósito/etc., ajusta la categoría
      //                     igual que a cualquier otro movimiento — permite que una
      //                     regla nueva (ej. "SPEI COMPENSACION") alcance movimientos
      //                     que alguien ya había reclasificado a mano. Si NINGUNA
      //                     regla matchea, se conserva la categoría manual tal cual
      //                     (NO se vacía) — protege categorías de asignación puramente
      //                     manual sin condiciones (ver validarRegla: 'categorizar' sin
      //                     condiciones = catálogo, nunca auto-matchea por diseño).
      //   'identificado'  → movimiento confirmado; la categoría puede actualizarse pero el
      //                     status NUNCA se toca (protege datos que dependen de este estado)
      let matchedCat         = null;
      let matchedCatEstado   = null; // estadoDestino de la catRule ganadora (si lo tiene)
      let matchedOcultaRoles = [];   // ocultarRoles de la catRule ganadora (si lo tiene)
      let matcheoPorRegla    = false;
      for (const rule of catRules) {
        if (matchRegla(mov, rule)) {
          matchedCat       = rule.nombre;
          // estadoDestino solo aplica si el movimiento no está identificado
          matchedCatEstado   = !isIdentificado ? (rule.estadoDestino ?? null) : null;
          matchedOcultaRoles = rule.ocultarRoles?.length ? rule.ocultarRoles : [];
          matcheoPorRegla    = true;
          break;
        }
      }
      if (!matcheoPorRegla && mov.status === 'reclasificado') {
        matchedCat = mov.categoria ?? null;
        const catRuleVigente = matchedCat ? catRules.find(r => r.nombre === matchedCat) : null;
        matchedCatEstado   = catRuleVigente ? (catRuleVigente.estadoDestino ?? null) : null;
        matchedOcultaRoles = catRuleVigente?.ocultarRoles?.length ? catRuleVigente.ocultarRoles : [];
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

      const newCat         = matchedCat ?? null;
      const oldCat         = mov.categoria    ?? null;
      const oldOcultaRoles = mov.ocultoRoles  ?? [];
      const catChanged         = newCat !== oldCat;
      const ocultaRolesChanged = !sameRoleSet(matchedOcultaRoles, oldOcultaRoles);
      const statusChanged      = matchedEstado !== null && matchedEstado !== mov.status;

      if (catChanged || ocultaRolesChanged || statusChanged) {
        const $set = {};
        if (catChanged)         $set.categoria   = newCat;
        if (ocultaRolesChanged) $set.ocultoRoles = matchedOcultaRoles;
        if (statusChanged)      $set.status      = matchedEstado;
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
