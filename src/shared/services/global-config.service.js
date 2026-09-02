'use strict';

/**
 * shared/services/global-config.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * "Configuraciones Globales" — configuración runtime editable desde la UI de
 * administración, sin redeploy. Reemplaza gradualmente al .env, sección por
 * sección (piloto: `erp-caja`, ver migración en erp-sync.service.js).
 *
 * Piezas:
 *   - ConfigSection: catálogo de secciones (FK real desde GlobalConfig, no
 *     texto libre — "catálogo relacional estricto", confirmado con el usuario).
 *   - GlobalConfig: un valor. Si `esSecreto`, se guarda cifrado con pgcrypto
 *     (pgp_sym_encrypt/pgp_sym_decrypt) en `valorCifrado`, nunca en `valor`.
 *   - ConfigAuditLog: historial completo. Para configs secretos, `valorAnterior`/
 *     `valorNuevo` quedan SIEMPRE null — se registra que hubo un cambio, nunca
 *     el valor real (evita una segunda puerta trasera al secreto).
 *
 * Caché: un solo proceso Node por ambiente (sin cluster) → Map en memoria,
 * invalidada de forma síncrona en el mismo momento del write. Sin TTL: no hace
 * falta, no hay otro proceso con el que reconciliar estado.
 *
 * La passphrase de pgcrypto sale de CONFIG_MASTER_KEY (única variable de .env
 * que sobrevive para este sistema). Sin default hardcodeado: cualquier
 * operación de cifrado/descifrado falla explícito si no está definida.
 */

const { QueryTypes } = require('sequelize');
const { sequelize }  = require('../../config/database.postgres');
const { ConfigSection, GlobalConfig, ConfigAuditLog } = require('../models/postgres');
const { logger }     = require('../utils/logger');
const { emitToAll }  = require('../../banks/shared/socket');

const _cache = new Map(); // `${sectionClave}.${clave}` → valor ya resuelto (descifrado si aplica)

// Hooks registrables sobre cambios de config — este módulo es GENÉRICO (lo usan muchos
// dominios), así que no conoce a quién le importa qué clave: cada dominio se suscribe y
// filtra adentro de su propio hook (mismo patrón "hook registrable" que
// bank.service.js#registerErpUnlinkHook, evita imports circulares y mantiene este módulo
// ciego a sus suscriptores).
const configChangeHooks = [];
function registerConfigChangeHook(fn) { configChangeHooks.push(fn); }

function _cacheKey(sectionClave, clave) {
  return `${sectionClave}.${clave}`;
}

function _invalidar(sectionClave, clave) {
  _cache.delete(_cacheKey(sectionClave, clave));
}

/** Nunca loguear el valor devuelto — puede ser un secreto descifrado. */
function _passphrase() {
  const key = process.env.CONFIG_MASTER_KEY;
  if (!key) {
    throw new Error(
      'CONFIG_MASTER_KEY no está definida — Configuraciones Globales no puede cifrar/descifrar ' +
      'secretos sin ella. Definila en el .env de este ambiente antes de usar valores esSecreto=true.',
    );
  }
  return key;
}

function userLabel(user) {
  return user?.nombre || user?.email || String(user?.dbId ?? user?._id ?? 'sistema');
}

/**
 * Devuelve el valor YA RESUELTO (descifrado si es secreto) de una config.
 * Usa/pobla la caché en memoria. Tira error explícito si no existe la fila —
 * el caller decide qué hacer (ej. erp-sync.service.js corta la operación).
 */
async function getValue(sectionClave, clave) {
  const cacheKey = _cacheKey(sectionClave, clave);
  if (_cache.has(cacheKey)) return _cache.get(cacheKey);

  // No pedimos la passphrase por adelantado: una config NO secreta debe poder
  // leerse aunque CONFIG_MASTER_KEY no esté definida (no la necesita). Si la fila
  // resulta ser secreta, atrapamos el error crudo de pgcrypto/Postgres abajo y lo
  // reemplazamos por el mensaje explícito de _passphrase().
  const rows = await sequelize.query(
    `SELECT gc.valor, gc.es_secreto,
            CASE WHEN gc.es_secreto THEN pgp_sym_decrypt(gc.valor_cifrado, :passphrase) ELSE NULL END AS valor_descifrado
       FROM global_configs gc
       JOIN config_sections cs ON cs.id = gc.section_id
      WHERE cs.clave = :sectionClave AND gc.clave = :clave
      LIMIT 1`,
    {
      replacements: { sectionClave, clave, passphrase: process.env.CONFIG_MASTER_KEY || '' },
      type: QueryTypes.SELECT,
    },
  ).catch((err) => {
    if (!process.env.CONFIG_MASTER_KEY) _passphrase(); // tira el error explícito
    throw err;
  });

  const row = rows[0];
  if (!row) {
    throw new Error(
      `No existe la configuración '${sectionClave}.${clave}' — cargala desde Configuraciones Globales.`,
    );
  }
  const valor = row.es_secreto ? row.valor_descifrado : row.valor;
  _cache.set(cacheKey, valor);
  return valor;
}

/**
 * Crea o actualiza un valor dentro de una sección YA EXISTENTE en el catálogo
 * (las secciones se dan de alta explícitamente vía createSection — setValue
 * nunca las auto-crea, es a propósito: el catálogo es curado, no incidental).
 */
async function setValue(sectionClave, clave, valor, { esSecreto = false, tipo = 'texto', descripcion = null, usuarioId, usuarioNombre } = {}) {
  const section = await ConfigSection.findOne({ where: { clave: sectionClave } });
  if (!section) {
    throw new Error(`La sección '${sectionClave}' no existe en el catálogo de Configuraciones Globales.`);
  }

  const existente = await GlobalConfig.findOne({ where: { sectionId: section.id, clave } });
  const accion         = existente ? 'editado' : 'creado';
  const valorAnteriorAudit = existente && !existente.esSecreto ? existente.valor : null;
  const valorNuevoAudit    = !esSecreto ? valor : null;

  let configId;
  if (esSecreto) {
    const passphrase = _passphrase();
    const [result] = await sequelize.query(
      `INSERT INTO global_configs
         (section_id, clave, valor, valor_cifrado, es_secreto, tipo, descripcion, updated_by, created_at, updated_at)
       VALUES
         (:sectionId, :clave, NULL, pgp_sym_encrypt(:valor, :passphrase), TRUE, :tipo, :descripcion, :updatedBy, NOW(), NOW())
       ON CONFLICT (section_id, clave) DO UPDATE SET
         valor          = NULL,
         valor_cifrado  = EXCLUDED.valor_cifrado,
         es_secreto     = TRUE,
         tipo           = EXCLUDED.tipo,
         descripcion    = EXCLUDED.descripcion,
         updated_by     = EXCLUDED.updated_by,
         updated_at     = NOW()
       RETURNING id`,
      {
        replacements: {
          sectionId: section.id, clave, valor, passphrase, tipo,
          descripcion, updatedBy: userLabel({ nombre: usuarioNombre }),
        },
        type: QueryTypes.SELECT,
      },
    );
    configId = result.id;
  } else {
    const [result] = await sequelize.query(
      `INSERT INTO global_configs
         (section_id, clave, valor, valor_cifrado, es_secreto, tipo, descripcion, updated_by, created_at, updated_at)
       VALUES
         (:sectionId, :clave, :valor, NULL, FALSE, :tipo, :descripcion, :updatedBy, NOW(), NOW())
       ON CONFLICT (section_id, clave) DO UPDATE SET
         valor          = EXCLUDED.valor,
         valor_cifrado  = NULL,
         es_secreto     = FALSE,
         tipo           = EXCLUDED.tipo,
         descripcion    = EXCLUDED.descripcion,
         updated_by     = EXCLUDED.updated_by,
         updated_at     = NOW()
       RETURNING id`,
      {
        replacements: {
          sectionId: section.id, clave, valor, tipo,
          descripcion, updatedBy: userLabel({ nombre: usuarioNombre }),
        },
        type: QueryTypes.SELECT,
      },
    );
    configId = result.id;
  }

  await ConfigAuditLog.create({
    configId,
    usuarioId:     usuarioId != null ? String(usuarioId) : null,
    usuarioNombre: usuarioNombre ?? null,
    accion,
    valorAnterior: valorAnteriorAudit,
    valorNuevo:    valorNuevoAudit,
  });

  _invalidar(sectionClave, clave);
  emitToAll('config:updated', { sectionClave, clave });
  // Fire-and-forget: no bloquea la respuesta del guardado, y un hook que tira error no
  // debe romper el guardado de config ni afectar a los demás hooks registrados.
  for (const hook of configChangeHooks) {
    Promise.resolve()
      .then(() => hook({ sectionClave, clave, valor }))
      .catch((err) => logger.error(`[GlobalConfig] hook de config:updated falló (${sectionClave}.${clave}): ${err.message}`));
  }
  logger.info(`[GlobalConfig] ${accion}: ${sectionClave}.${clave} por ${userLabel({ nombre: usuarioNombre })}`);

  return configId;
}

/**
 * Descifra y devuelve el valor real de un secreto por `id` — para la acción
 * explícita "Revelar" en la UI (permiso config:secrets:reveal). Siempre lee
 * en vivo (no usa la caché de getValue) y deja registro en el audit log.
 */
async function revealSecret(configId, { usuarioId, usuarioNombre } = {}) {
  const passphrase = _passphrase();
  const rows = await sequelize.query(
    `SELECT pgp_sym_decrypt(valor_cifrado, :passphrase) AS valor
       FROM global_configs
      WHERE id = :configId AND es_secreto = TRUE
      LIMIT 1`,
    { replacements: { configId, passphrase }, type: QueryTypes.SELECT },
  );
  const row = rows[0];
  if (!row) throw new Error(`No existe un valor secreto con id ${configId}.`);

  await ConfigAuditLog.create({
    configId,
    usuarioId:     usuarioId != null ? String(usuarioId) : null,
    usuarioNombre: usuarioNombre ?? null,
    accion:        'secreto_revelado',
    valorAnterior: null,
    valorNuevo:    null,
  });
  logger.info(`[GlobalConfig] secreto revelado: config id=${configId} por ${userLabel({ nombre: usuarioNombre })}`);

  return row.valor;
}

/** Catálogo completo de secciones, para la pantalla de administración. */
async function listSections() {
  return ConfigSection.findAll({ order: [['nombre', 'ASC']] });
}

async function createSection({ clave, nombre, descripcion = null, modulosAfectados = [] }) {
  return ConfigSection.create({ clave, nombre, descripcion, modulosAfectados });
}

/**
 * Edita metadata de una sección ya existente. `clave` NUNCA se edita acá —
 * es el identificador natural que usan getValue/setValue en todo el resto
 * del sistema (erp-caja, kore-formaspago, etc.); cambiarla rompería esas
 * migraciones en silencio.
 */
async function updateSection(id, { nombre, descripcion = null, modulosAfectados = [] }) {
  const section = await ConfigSection.findByPk(id);
  if (!section) throw new Error(`No existe la sección con id ${id}.`);
  section.nombre = nombre;
  section.descripcion = descripcion;
  section.modulosAfectados = modulosAfectados;
  await section.save();
  return section;
}

/**
 * Configs de una sección, para la pantalla de administración. Los valores
 * secretos vienen enmascarados con un placeholder fijo — NO se descifra nada
 * acá (evita descifrar de más en un simple listado); `revealSecret` es la
 * única vía para ver el valor real.
 */
async function listConfigsBySection(sectionId) {
  const configs = await GlobalConfig.findAll({ where: { sectionId }, order: [['clave', 'ASC']] });
  return configs.map((c) => ({
    id:          c.id,
    sectionId:   c.sectionId,
    clave:       c.clave,
    valor:       c.esSecreto ? '••••••••' : c.valor,
    esSecreto:   c.esSecreto,
    tipo:        c.tipo,
    descripcion: c.descripcion,
    updatedBy:   c.updatedBy,
    updatedAt:   c.updatedAt,
  }));
}

async function listAuditLog(configId) {
  return ConfigAuditLog.findAll({ where: { configId }, order: [['fecha', 'DESC']] });
}

module.exports = {
  getValue, setValue, revealSecret, registerConfigChangeHook,
  listSections, createSection, updateSection, listConfigsBySection, listAuditLog,
};
