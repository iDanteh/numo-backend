'use strict';

/**
 * domains/config/loyalty-config.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — dominio de configuración (PR3): % por nivel
 * (Bronce/Plata/Oro), catálogo de motivos de ajuste y flags de sucursal
 * (`participaLealtad`/`permiteCanjeLealtad`, columnas agregadas en PR1).
 *
 * Los % viven en `GlobalConfig` sección `lealtad` (D10): esta capa NO escribe
 * la tabla directo, delega en `global-config.service.setValue()`, que ya
 * resuelve auditoría (`ConfigAuditLog`), invalidación de caché y el emit
 * `config:updated` (consumido por el frontend vía `SocketService.configUpdated$`
 * filtrado a `sectionClave === 'lealtad'`, igual que `bank-corte-panel`).
 * Así Lealtad gana el refresh en vivo gratis, sin reimplementarlo acá.
 *
 * El gating por permiso (`loyalty:config` en vez del `config:manage` genérico)
 * es responsabilidad de la ruta (PR4) — este servicio no conoce permisos.
 */

const { LoyaltyAdjustmentReason, CentroCosto } = require('../../../shared/models/postgres');
const { BadRequestError, NotFoundError } = require('../../../shared/errors/AppError');
const globalConfigService = require('../../../shared/services/global-config.service');

const SECTION = 'lealtad';
const NIVELES = ['BRONCE', 'PLATA', 'ORO'];
const SIGNOS_VALIDOS = ['POSITIVO', 'NEGATIVO', 'AMBOS'];

// Máximo 2 decimales, mayor a 0 — validado sobre el STRING crudo (nunca sobre
// el Number ya parseado) para no heredar el mismo riesgo de IEEE-754 que
// `puntos.util.js#toCents` documenta para el resto del dominio.
const PORCENTAJE_RE = /^\d+(\.\d{1,2})?$/;

function _userLabel(user) {
  return {
    usuarioId:     user?.dbId != null ? String(user.dbId) : (user?._id != null ? String(user._id) : null),
    usuarioNombre: user?.nombre ?? null,
  };
}

function _capitalizar(nivel) {
  return nivel.charAt(0) + nivel.slice(1).toLowerCase();
}

function _validarPorcentaje(nivel, valor) {
  const str = String(valor ?? '').trim();
  if (!PORCENTAJE_RE.test(str)) {
    throw new BadRequestError(
      `El % de ${_capitalizar(nivel)} debe ser un número mayor a 0, con máximo 2 decimales.`,
    );
  }
  const n = Number(str);
  if (n <= 0 || n > 100) {
    throw new BadRequestError(`El % de ${_capitalizar(nivel)} debe ser mayor a 0 y menor o igual a 100.`);
  }
  return str;
}

/**
 * % vigente de cada nivel. `null` cuando el nivel aún no está configurado
 * (D11: nunca se asume 0 — la UI debe mostrarlo como "sin configurar").
 */
async function obtenerPorcentajes() {
  const resultado = {};
  for (const nivel of NIVELES) {
    // eslint-disable-next-line no-await-in-loop
    const valor = await globalConfigService.getValue(SECTION, `PORCENTAJE_${nivel}`).catch(() => null);
    resultado[_capitalizar(nivel)] = valor === null ? null : Number(valor);
  }
  return resultado;
}

/**
 * Guarda uno o más % de nivel. Payload parcial: solo escribe las llaves
 * presentes (`undefined` se omite, permitiendo editar un solo nivel a la vez
 * desde la UI). Valida ANTES de llamar a `setValue` — ninguna escritura
 * parcial queda a mitad si un valor es inválido.
 */
async function guardarPorcentajes({ Bronce, Plata, Oro } = {}, user) {
  const { usuarioId, usuarioNombre } = _userLabel(user);
  const entradas = { BRONCE: Bronce, PLATA: Plata, ORO: Oro };

  const aEscribir = Object.entries(entradas).filter(([, valor]) => valor !== undefined);
  const validadas = aEscribir.map(([nivel, valor]) => [nivel, _validarPorcentaje(nivel, valor)]);

  const resultado = {};
  for (const [nivel, valorValidado] of validadas) {
    // eslint-disable-next-line no-await-in-loop
    await globalConfigService.setValue(SECTION, `PORCENTAJE_${nivel}`, valorValidado, {
      tipo: 'numero', usuarioId, usuarioNombre,
    });
    resultado[_capitalizar(nivel)] = Number(valorValidado);
  }
  return resultado;
}

/**
 * Catálogo de motivos de ajuste. `soloActivos` es lo que debe consumir el
 * selector de un AJUSTE nuevo (un motivo desactivado no es seleccionable);
 * sin el flag trae todo, para la pantalla de administración del catálogo
 * (donde también se ven/reactivan los inactivos).
 */
async function listarReasons({ soloActivos = false } = {}) {
  const where = soloActivos ? { activo: true } : {};
  return LoyaltyAdjustmentReason.findAll({ where, order: [['nombre', 'ASC']] });
}

async function crearReason({ clave, nombre, signoPermitido }) {
  if (!SIGNOS_VALIDOS.includes(signoPermitido)) {
    throw new BadRequestError(`signoPermitido debe ser uno de: ${SIGNOS_VALIDOS.join(', ')}.`);
  }
  return LoyaltyAdjustmentReason.create({ clave, nombre, signoPermitido, activo: true });
}

/**
 * Edita un motivo existente. Desactivar (`activo:false`) SOLO cambia el flag
 * — nunca se borra la fila, porque `loyalty_points_ledger.reasonId` sigue
 * referenciándola en entradas históricas (requisito del spec: "Historical
 * entries keep their original reason").
 */
async function editarReason(id, { nombre, signoPermitido, activo } = {}) {
  const reason = await LoyaltyAdjustmentReason.findByPk(id);
  if (!reason) throw new NotFoundError('Motivo de ajuste');

  if (signoPermitido !== undefined && !SIGNOS_VALIDOS.includes(signoPermitido)) {
    throw new BadRequestError(`signoPermitido debe ser uno de: ${SIGNOS_VALIDOS.join(', ')}.`);
  }

  const cambios = {};
  if (nombre !== undefined) cambios.nombre = nombre;
  if (signoPermitido !== undefined) cambios.signoPermitido = signoPermitido;
  if (activo !== undefined) cambios.activo = activo;

  await reason.update(cambios);
  return reason;
}

/** Sucursales con sus dos flags de lealtad, para la pantalla de configuración. */
async function listarSucursales() {
  return CentroCosto.findAll({
    attributes: ['id', 'clave', 'sucursal', 'isActive', 'participaLealtad', 'permiteCanjeLealtad'],
    order: [['sucursal', 'ASC']],
  });
}

/**
 * Actualiza los flags de lealtad de UNA sucursal, independientemente
 * (`participaLealtad` controla acumulación, `permiteCanjeLealtad` controla
 * canje — cualquier combinación es válida). Deliberadamente NO pasa por
 * `centros-costo.service.js#update`: esa ruta está gateada por
 * `account-plan:write` y su whitelist de campos no incluye estos dos flags
 * (D12) — acá se escriben solo si vienen explícitos en el payload.
 */
async function actualizarSucursal(id, { participaLealtad, permiteCanjeLealtad } = {}) {
  const centro = await CentroCosto.findByPk(id);
  if (!centro) throw new NotFoundError('Centro de costo');

  const cambios = {};
  if (participaLealtad !== undefined) cambios.participaLealtad = !!participaLealtad;
  if (permiteCanjeLealtad !== undefined) cambios.permiteCanjeLealtad = !!permiteCanjeLealtad;

  await centro.update(cambios);
  return centro;
}

module.exports = {
  obtenerPorcentajes,
  guardarPorcentajes,
  listarReasons,
  crearReason,
  editarReason,
  listarSucursales,
  actualizarSucursal,
};
