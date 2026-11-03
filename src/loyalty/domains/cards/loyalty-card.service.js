'use strict';

/**
 * domains/cards/loyalty-card.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — ciclo de vida de la tarjeta (D1: el teléfono es la
 * única llave de identidad). El NIP en texto plano (D6) solo existe en el
 * valor de retorno de `crearTarjeta`/`cambiarNip` — nunca se persiste ni se
 * loguea, y el modelo excluye `nipHash` por `defaultScope` (PR1).
 */

const path = require('path');
const { Transaction, Op } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');
const { LoyaltyCard, LoyaltyTierHistory } = require('../../../shared/models/postgres');
const { BadRequestError, ConflictError, NotFoundError } = require('../../../shared/errors/AppError');
const { normalizarTelefono } = require('../../shared/puntos.util');
const { generarNip, hashNip } = require('../../shared/nip.util');
const { subirFotoCliente, descargarFotoCliente } = require('./drive-loyalty-foto.service');

const NIVELES_VALIDOS = ['Bronce', 'Plata', 'Oro'];

// Mismo literal que el actor sintético de loyalty-kore-cliente.service.js (webhook de catálogo de
// clientes) — único discriminador de origen, no hace falta una columna nueva: cualquier otro valor
// de `createdById` (incluido `null`, tarjetas migradas sin ese dato histórico) es origen "Numo"
// (alta manual o import de CSV, ambos corridos por un usuario real).
const CREATED_BY_KORE_WEBHOOK = 'kore-webhook';

// "Datos adicionales" (2026-10-23) — mismos campos que el enriquecimiento del import de
// clientes.csv (ver loyalty-import-apply.service.js#CAMPOS_ENRIQUECIMIENTO), ahora también
// editables a mano para altas que no vienen de un CSV. Nunca obligatorios, nunca usados para
// lógica de negocio (nivel/puntos/identidad siguen siendo teléfono+categoría).
const CAMPOS_OPCIONALES = [
  'apellidos', 'apodo', 'sexo', 'fechaNacimiento', 'especialidades', 'sucursal', 'calle',
  'numExterior', 'numInterior', 'colonia', 'municipio', 'estado', 'cp', 'comentarios',
];

/** Solo toca los campos presentes en `input` (`!== undefined`) — a diferencia del import, acá
 *  NO se fuerza "" → null para un campo que el caller no mandó, para no borrar un dato ya
 *  cargado por error de un payload parcial (ej. PATCH que solo manda `nombre`). */
function _datosOpcionales(input) {
  const datos = {};
  for (const campo of CAMPOS_OPCIONALES) {
    if (input[campo] !== undefined) {
      const v = String(input[campo] ?? '').trim();
      datos[campo] = v || null;
    }
  }
  return datos;
}

function _userLabel(user) {
  return {
    usuarioId:     user?.dbId != null ? String(user.dbId) : (user?._id != null ? String(user._id) : null),
    usuarioNombre: user?.nombre ?? null,
  };
}

function _esDuplicadoUnico(err) {
  return err?.name === 'SequelizeUniqueConstraintError';
}

/**
 * Crea una tarjeta nueva. El teléfono se normaliza (D1/puntos.util) antes de
 * validar unicidad — rechaza duplicados contra tarjetas activas E inactivas
 * (un teléfono inactivo sigue siendo "la misma persona" para el programa).
 */
async function crearTarjeta({ telefono, nombre, nivel = 'Bronce', ...opcionales }, user, { transaction } = {}) {
  const telefonoNorm = normalizarTelefono(telefono);
  const nombreTrim = String(nombre ?? '').trim();
  if (!nombreTrim) {
    throw new BadRequestError('nombre es requerido.');
  }
  if (!NIVELES_VALIDOS.includes(nivel)) {
    throw new BadRequestError(`nivel inválido: "${nivel}".`);
  }

  // `transaction` es opcional (PR8: el script de migración reutiliza este servicio dentro
  // de UNA sola transacción para todo el lote, design.md "Apply. One Postgres transaction
  // for all ok rows"). Se pasa condicionalmente (nunca como `undefined` explícito) para no
  // alterar la cantidad de argumentos de las llamadas existentes desde las rutas (PR4).
  const existente = transaction
    ? await LoyaltyCard.findOne({ where: { telefono: telefonoNorm }, transaction })
    : await LoyaltyCard.findOne({ where: { telefono: telefonoNorm } });
  if (existente) {
    throw new ConflictError('Ya existe una tarjeta registrada con ese teléfono.');
  }

  const nip = generarNip();
  const nipHash = await hashNip(nip);
  const { usuarioId, usuarioNombre } = _userLabel(user);

  const datosCard = {
    telefono: telefonoNorm,
    nombre:   nombreTrim,
    nivel,
    nipHash,
    saldoPuntos: 0,
    activa: true,
    createdById:     usuarioId,
    createdByNombre: usuarioNombre,
    ..._datosOpcionales(opcionales),
  };

  let card;
  try {
    card = transaction
      ? await LoyaltyCard.create(datosCard, { transaction })
      : await LoyaltyCard.create(datosCard);
  } catch (err) {
    if (_esDuplicadoUnico(err)) {
      throw new ConflictError('Ya existe una tarjeta registrada con ese teléfono.');
    }
    throw err;
  }

  return { card, nip };
}

// Whitelist de columnas ordenables — nunca interpolar `sort` directo en el
// `order` de Sequelize (inyección de SQL vía nombre de columna/dirección).
// Cualquier `sort` fuera de este mapa cae al default (`nombre` ASC).
const SORT_COLUMNAS = {
  telefono: 'telefono',
  nombre:   'nombre',
  nivel:    'nivel',
  saldo:    'saldoPuntos',
  activa:   'activa',
  creado:   'createdAt',
};

function _resolverOrden(sort, sortDir) {
  const columna = SORT_COLUMNAS[String(sort ?? '').trim()];
  if (!columna) return [['nombre', 'ASC']];
  const dir = String(sortDir ?? '').trim().toUpperCase() === 'DESC' ? 'DESC' : 'ASC';
  return [[columna, dir]];
}

/** `search` matchea prefijo de teléfono o ILIKE sobre nombre. `origen`: 'kore' (creadas por el
 *  webhook de catálogo de clientes) | 'numo' (alta manual o import de CSV, incluye `createdById`
 *  nulo de tarjetas migradas) — cualquier otro valor no filtra, igual que el resto de filtros.
 *
 * Reporte "Tarjetas" (2026-10-29, pedido explícito del usuario): `exportar`, `fechaAltaInicio`/
 * `fechaAltaFin` y `revisionPendiente` son parámetros NUEVOS y opcionales — sin ellos, el
 * comportamiento paginado de siempre queda 100% intacto (mismos argumentos, mismo resultado).
 *
 * `fechaAltaInicio`/`fechaAltaFin` acotan `createdAt`. No existe un helper de rango de fechas
 * compartido/importable en el backend: `_inicioDiaMx`/`_finDiaMx` (bank.service.js) resuelven el
 * mismo tipo de bug de huso horario (ya corregido ahí, y también en el `erp-modal` del frontend —
 * ver isoFirstDay/isoLastDay) pero son privados de ese módulo y están DUPLICADOS a propósito en
 * cada lugar que los necesita (ver su propio comentario) — no están pensados para importarse fuera
 * de Bancos. `createdAt` acá es un timestamp de auditoría, no un campo de negocio con semántica de
 * "día calendario de México" (a diferencia de `BankMovement.fecha`), así que alcanza con
 * `Op.gte`/`Op.lte` directos; `fechaAltaFin` se trata como fin de día UTC (`T23:59:59.999Z`,
 * mismo criterio ya usado en `bank.service.js#listMovements` para `fechaAplicacion`) para no
 * cortar registros dados de alta ese mismo día.
 *
 * `exportar===true`: trae TODAS las filas que matchean (sin `limit`/`offset`) — mismo criterio
 * que `listarPendientes({exportar})` en loyalty-import-apply.service.js.
 */
async function listar({
  search, nivel, activa, origen, page = 1, limit = 50, sort, sortDir,
  exportar = false, fechaAltaInicio, fechaAltaFin, revisionPendiente,
} = {}) {
  const where = {};
  // Grupos OR independientes (origen, search) van en `Op.and` — nunca compartiendo la misma clave
  // `Op.or` del `where`, porque una asignación pisaría a la otra en silencio si ambos filtros se
  // mandan juntos (ej. buscar por nombre DENTRO del origen "numo").
  const condiciones = [];
  if (nivel) where.nivel = nivel;
  if (activa !== undefined && activa !== null && activa !== '') {
    where.activa = activa === true || activa === 'true';
  }
  if (revisionPendiente !== undefined && revisionPendiente !== null && revisionPendiente !== '') {
    where.revisionPendiente = revisionPendiente === true || revisionPendiente === 'true';
  }
  if (fechaAltaInicio) {
    where.createdAt = { ...(where.createdAt ?? {}), [Op.gte]: new Date(fechaAltaInicio) };
  }
  if (fechaAltaFin) {
    where.createdAt = { ...(where.createdAt ?? {}), [Op.lte]: new Date(`${fechaAltaFin}T23:59:59.999Z`) };
  }
  if (origen === 'kore') {
    where.createdById = CREATED_BY_KORE_WEBHOOK;
  } else if (origen === 'numo') {
    // Op.ne no matchea filas NULL en Postgres — hace falta el Op.or explícito para no dejar
    // afuera a las tarjetas migradas que nunca tuvieron createdById.
    condiciones.push({
      [Op.or]: [
        { createdById: { [Op.ne]: CREATED_BY_KORE_WEBHOOK } },
        { createdById: { [Op.is]: null } },
      ],
    });
  }
  if (search) {
    const s = String(search).trim();
    condiciones.push({
      [Op.or]: [
        { telefono: { [Op.like]: `${s}%` } },
        { nombre:   { [Op.iLike]: `%${s}%` } },
      ],
    });
  }
  if (condiciones.length) {
    where[Op.and] = condiciones;
  }

  const order = _resolverOrden(sort, sortDir);

  if (exportar) {
    const rows = await LoyaltyCard.findAll({ where, order });
    return { total: rows.length, page: 1, limit: rows.length, pages: 1, cards: rows };
  }

  const p = Math.max(1, Number(page) || 1);
  const l = Math.min(100, Number(limit) || 50);
  const { count, rows } = await LoyaltyCard.findAndCountAll({
    where,
    order,
    limit:  l,
    offset: (p - 1) * l,
  });

  return { total: count, page: p, limit: l, pages: Math.ceil(count / l), cards: rows };
}

async function obtener(id) {
  const card = await LoyaltyCard.findByPk(id);
  if (!card) throw new NotFoundError('Tarjeta');
  return card;
}

/**
 * Variante de `obtener` para precargar el formulario de edición (2026-10-23) — a diferencia de
 * `obtener`/`GET /cards`, acá SÍ se necesitan los campos PII que `defaultScope` excluye (apodo,
 * dirección, etc. — ver LoyaltyCard.js) para poder editarlos. Gateada en la ruta por
 * `loyalty:tarjetas:editar` (no `loyalty:read`): el mismo permiso que ya hace falta para llamar
 * `editarTarjeta` a continuación, no una exposición nueva de PII a un permiso más amplio.
 * `.unscoped()` en vez de combinar scopes de atributos (include+exclude) porque Sequelize no
 * garantiza un merge predecible entre dos scopes de `attributes` — se excluyen a mano los dos
 * hashes de seguridad, que nunca deben salir de acá bajo ningún permiso.
 */
async function obtenerParaEditar(id) {
  const card = await LoyaltyCard.unscoped().findByPk(id, {
    attributes: { exclude: ['nipHash', 'recuperacionCodigoHash'] },
  });
  if (!card) throw new NotFoundError('Tarjeta');
  return card;
}

/** Whitelist de campos editables. `telefono` re-aplica la misma unicidad que `crearTarjeta`. */
async function editarTarjeta(id, { nombre, telefono, activa, ...opcionales } = {}, user) {
  const card = await LoyaltyCard.findByPk(id);
  if (!card) throw new NotFoundError('Tarjeta');

  const updates = { ..._datosOpcionales(opcionales) };
  if (nombre !== undefined) {
    updates.nombre = String(nombre).trim();
  }
  if (telefono !== undefined) {
    const telefonoNorm = normalizarTelefono(telefono);
    if (telefonoNorm !== card.telefono) {
      const existente = await LoyaltyCard.findOne({ where: { telefono: telefonoNorm, id: { [Op.ne]: id } } });
      if (existente) {
        throw new ConflictError('Ya existe una tarjeta registrada con ese teléfono.');
      }
      updates.telefono = telefonoNorm;
    }
  }
  if (activa !== undefined) {
    updates.activa = Boolean(activa);
  }

  const { usuarioId, usuarioNombre } = _userLabel(user);
  updates.updatedById = usuarioId;
  updates.updatedByNombre = usuarioNombre;

  try {
    await card.update(updates);
  } catch (err) {
    if (_esDuplicadoUnico(err)) {
      throw new ConflictError('Ya existe una tarjeta registrada con ese teléfono.');
    }
    throw err;
  }

  return card;
}

/**
 * Genera un NIP nuevo y resetea el lockout (D7) — el anterior deja de autenticar de inmediato.
 *
 * Bug real corregido (2026-10-19, pedido explícito del usuario — "agrega solo el quién y
 * cuándo"): esta función nunca tocaba `updatedById`/`updatedByNombre` pese a que `LoyaltyCard`
 * ya tiene esas columnas y `editarTarjeta`/`cambiarNivel` sí las setean — cambiar el NIP de una
 * tarjeta no dejaba NINGÚN rastro de quién lo hizo. `updatedAt` ya se actualizaba solo (Sequelize
 * lo maneja automático en cualquier `.update()`), pero sin saber QUIÉN eso no sirve para
 * trazabilidad real. No se agrega un historial completo (ver discovery sobre
 * PolizaAuditLog/ConfigAuditLog) — decisión explícita del usuario de ir con el mínimo.
 */
async function cambiarNip(cardId, user) {
  const card = await LoyaltyCard.findByPk(cardId);
  if (!card) throw new NotFoundError('Tarjeta');

  const nip = generarNip();
  const nipHash = await hashNip(nip);
  const { usuarioId, usuarioNombre } = _userLabel(user);
  await card.update({
    nipHash, nipIntentosFallidos: 0, nipBloqueadoHasta: null,
    updatedById: usuarioId, updatedByNombre: usuarioNombre,
  });

  return { nip };
}

/** Cambia el nivel y audita en `loyalty_tier_history`, en una sola transacción. No mueve `saldoPuntos`. */
async function cambiarNivel(cardId, nuevoNivel, { motivo } = {}, user) {
  if (!NIVELES_VALIDOS.includes(nuevoNivel)) {
    throw new BadRequestError(`nivel inválido: "${nuevoNivel}".`);
  }

  return sequelize.transaction(async (t) => {
    const card = await LoyaltyCard.findByPk(cardId, { transaction: t, lock: Transaction.LOCK.UPDATE });
    if (!card) throw new NotFoundError('Tarjeta');
    if (card.nivel === nuevoNivel) {
      throw new BadRequestError('La tarjeta ya está en ese nivel.');
    }

    const nivelAnterior = card.nivel;
    await card.update({ nivel: nuevoNivel }, { transaction: t });

    const { usuarioId, usuarioNombre } = _userLabel(user);
    await LoyaltyTierHistory.create({
      cardId: card.id,
      nivelAnterior,
      nivelNuevo: nuevoNivel,
      motivo: motivo ?? null,
      usuarioId,
      usuarioNombre,
    }, { transaction: t });

    return card;
  });
}

// Extensión de respaldo cuando el archivo llega sin una en el nombre original (ej. una foto de
// cámara/celular sin extensión) — mismos tipos que acepta el multer dedicado de loyalty.routes.js.
const FOTO_MIME_EXT = {
  'image/jpeg': '.jpg',
  'image/png':  '.png',
  'image/webp': '.webp',
};

/** Nombre con el que la foto del cliente se guarda en Drive: "teléfono - nombre", mismo criterio
 *  que _nombreArchivoFichaDrive en bank.service.js (dos datos para poder rastrearla en Drive). */
function _nombreArchivoFotoCliente(card, imagen) {
  const extension = path.extname(imagen.originalname || '') || FOTO_MIME_EXT[imagen.mimetype] || '';
  const nombre = String(card.nombre ?? '').replace(/[\\/:*?"<>|]/g, '').trim();
  return `${card.telefono} - ${nombre}${extension}`;
}

/**
 * Sube la foto del cliente a Drive y guarda el link en `fotoUrl`. `imagen` es el shape de
 * req.file de multer: { buffer, mimetype, originalname }.
 *
 * Bug real corregido (2026-10-29): `fotoUrl`/`driveWebViewLink` es la página VISOR de Drive, no
 * una imagen embebible — un `<img src>` nunca puede renderizarla. Ahora también se persiste
 * `fotoDriveFileId` (ya venía en el resultado de `subirFotoCliente`, antes se descartaba) y
 * `fotoMimeType`, para poder descargar el binario real vía `obtenerFoto()` más abajo.
 */
async function subirFoto(cardId, imagen, user) {
  const card = await LoyaltyCard.findByPk(cardId);
  if (!card) throw new NotFoundError('Tarjeta');

  const nombreArchivo = _nombreArchivoFotoCliente(card, imagen);
  const { driveFileId, driveWebViewLink } = await subirFotoCliente(imagen.buffer, imagen.mimetype, nombreArchivo);

  const { usuarioId, usuarioNombre } = _userLabel(user);
  await card.update({
    fotoUrl: driveWebViewLink,
    fotoDriveFileId: driveFileId,
    fotoMimeType: imagen.mimetype,
    updatedById: usuarioId,
    updatedByNombre: usuarioNombre,
  });

  return card;
}

/**
 * Descarga el binario de la foto del cliente ya subida — proxy autenticado para el `<img>` del
 * detalle de tarjeta (bug real corregido 2026-10-29: `fotoUrl` es la página visor de Drive,
 * nunca embebible). Mismo patrón que bank.service.js#obtenerImagenFicha.
 */
async function obtenerFoto(cardId) {
  const card = await LoyaltyCard.findByPk(cardId);
  if (!card) throw new NotFoundError('Tarjeta');
  if (!card.fotoDriveFileId) throw new NotFoundError('Esta tarjeta no tiene ninguna foto adjunta');

  const data = await descargarFotoCliente(card.fotoDriveFileId);
  return { data, mimetype: card.fotoMimeType };
}

/**
 * Altas creadas por el webhook de catálogo de clientes de Kore (`_procesarAlta`), pendientes de
 * que staff las revise/complete a mano. Mismo criterio de paginación que `listar` (page/limit con
 * el mismo tope), orden ASC por `createdAt` (las más viejas primero: las que llevan más tiempo
 * esperando revisión).
 */
async function listarPendientesRevisionKore({ page = 1, limit = 50, search } = {}) {
  const p = Math.max(1, Number(page) || 1);
  const l = Math.min(100, Number(limit) || 50);
  const where = { revisionPendiente: true };
  // Mismo criterio de búsqueda que `listar()` arriba: prefijo de teléfono O ILIKE sobre nombre.
  if (search) {
    const s = String(search).trim();
    where[Op.and] = [
      {
        [Op.or]: [
          { telefono: { [Op.like]: `${s}%` } },
          { nombre:   { [Op.iLike]: `%${s}%` } },
        ],
      },
    ];
  }
  const { count, rows } = await LoyaltyCard.findAndCountAll({
    where,
    order:  [['createdAt', 'ASC']],
    limit:  l,
    offset: (p - 1) * l,
  });

  return { total: count, page: p, limit: l, pages: Math.ceil(count / l), cards: rows };
}

/**
 * Marca una tarjeta como revisada (contraparte de `revisionPendiente: true` seteado por
 * `_procesarAlta`). Idempotente: si ya estaba revisada (`revisionPendiente === false`), no es un
 * error — simplemente devuelve la tarjeta sin tocarla (no pisa quién/cuándo la revisó la primera
 * vez con una segunda llamada).
 */
async function marcarRevisada(cardId, user) {
  const card = await LoyaltyCard.findByPk(cardId);
  if (!card) throw new NotFoundError('Tarjeta');

  if (!card.revisionPendiente) return card;

  const { usuarioId, usuarioNombre } = _userLabel(user);
  await card.update({
    revisionPendiente: false,
    revisadoPorId:      usuarioId,
    revisadoPorNombre:  usuarioNombre,
    revisadoAt:         new Date(),
  });

  return card;
}

module.exports = {
  crearTarjeta, listar, obtener, obtenerParaEditar, editarTarjeta, cambiarNip, cambiarNivel,
  subirFoto, obtenerFoto, listarPendientesRevisionKore, marcarRevisada,
};
