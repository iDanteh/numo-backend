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

const { Op } = require('sequelize');
const { LoyaltyAdjustmentReason, LoyaltyPromotion, CentroCosto } = require('../../../shared/models/postgres');
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

/**
 * Validador genérico de % (0 < n <= 100, máx 2 decimales), reusado por el %
 * de nivel, el % de motivos de ajuste y el % de bono de promociones — misma
 * regla, distinta etiqueta en el mensaje de error.
 */
function _validarPorcentajeGenerico(valor, etiqueta) {
  const str = String(valor ?? '').trim();
  if (!PORCENTAJE_RE.test(str)) {
    throw new BadRequestError(`${etiqueta} debe ser un número mayor a 0, con máximo 2 decimales.`);
  }
  const n = Number(str);
  if (n <= 0 || n > 100) {
    throw new BadRequestError(`${etiqueta} debe ser mayor a 0 y menor o igual a 100.`);
  }
  return str;
}

function _validarPorcentaje(nivel, valor) {
  return _validarPorcentajeGenerico(valor, `El % de ${_capitalizar(nivel)}`);
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

/**
 * `porcentaje` (2026-10-20): si viene definido, el motivo pasa a calcular sus
 * puntos automáticamente desde `montoBase` al registrar un AJUSTE (ver
 * loyalty-ledger.service.js#registrarAjuste) — un motivo así NUNCA puede
 * tener signoPermitido:'AMBOS', porque el signo de los puntos resultantes
 * depende directamente de él (POSITIVO → tal cual, NEGATIVO → se niega).
 */
async function crearReason({ clave, nombre, signoPermitido, porcentaje } = {}) {
  if (!SIGNOS_VALIDOS.includes(signoPermitido)) {
    throw new BadRequestError(`signoPermitido debe ser uno de: ${SIGNOS_VALIDOS.join(', ')}.`);
  }

  let porcentajeValidado = null;
  if (porcentaje !== undefined && porcentaje !== null) {
    porcentajeValidado = _validarPorcentajeGenerico(porcentaje, 'El % del motivo');
    if (signoPermitido === 'AMBOS') {
      throw new BadRequestError(
        'Un motivo con % de cálculo automático no puede tener signo AMBOS — definí si es POSITIVO o NEGATIVO.',
      );
    }
  }

  return LoyaltyAdjustmentReason.create({
    clave, nombre, signoPermitido, activo: true, porcentaje: porcentajeValidado,
  });
}

/**
 * Edita un motivo existente. Desactivar (`activo:false`) SOLO cambia el flag
 * — nunca se borra la fila, porque `loyalty_points_ledger.reasonId` sigue
 * referenciándola en entradas históricas (requisito del spec: "Historical
 * entries keep their original reason").
 */
async function editarReason(id, { nombre, signoPermitido, activo, porcentaje } = {}) {
  const reason = await LoyaltyAdjustmentReason.findByPk(id);
  if (!reason) throw new NotFoundError('Motivo de ajuste');

  if (signoPermitido !== undefined && !SIGNOS_VALIDOS.includes(signoPermitido)) {
    throw new BadRequestError(`signoPermitido debe ser uno de: ${SIGNOS_VALIDOS.join(', ')}.`);
  }

  // `porcentaje: null` explícito es válido (quita el % y vuelve el motivo
  // manual) — se distingue de `undefined` (campo no tocado). Cuando viene un
  // valor, se valida contra el signoPermitido RESULTANTE: el del payload si
  // vino, si no el que ya tiene guardado la fila (nunca asumir que viene).
  let porcentajeValidado;
  if (porcentaje !== undefined) {
    if (porcentaje === null) {
      porcentajeValidado = null;
    } else {
      porcentajeValidado = _validarPorcentajeGenerico(porcentaje, 'El % del motivo');
      const signoResultante = signoPermitido !== undefined ? signoPermitido : reason.signoPermitido;
      if (signoResultante === 'AMBOS') {
        throw new BadRequestError(
          'Un motivo con % de cálculo automático no puede tener signo AMBOS — definí si es POSITIVO o NEGATIVO.',
        );
      }
    }
  }

  const cambios = {};
  if (nombre !== undefined) cambios.nombre = nombre;
  if (signoPermitido !== undefined) cambios.signoPermitido = signoPermitido;
  if (activo !== undefined) cambios.activo = activo;
  if (porcentaje !== undefined) cambios.porcentaje = porcentajeValidado;

  await reason.update(cambios);
  return reason;
}

/**
 * Sucursales con sus dos flags de lealtad, para la pantalla de configuración.
 *
 * `serieFacturacion` (2026-10-19, pedido explícito del usuario): se expone para mostrar
 * "Sucursal → CEDIS (A0)" en la UI — no se usa todavía para ninguna regla de Lealtad, pero el
 * dato ya existe en `centros_costo` (serie de facturación real de esa sucursal en Kore) y es
 * valioso tenerlo visible de cara a una regla futura (ver centros-costo.service.js#mapaPorSerie).
 */
async function listarSucursales() {
  return CentroCosto.findAll({
    attributes: ['id', 'clave', 'sucursal', 'isActive', 'participaLealtad', 'permiteCanjeLealtad', 'serieFacturacion'],
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

/**
 * Catálogo de promociones (bono extra sobre la acumulación normal, por
 * periodo — ej. "Buen Fin"). `soloActivas` sigue el mismo criterio que
 * `listarReasons`: la pantalla de administración trae todo (incluye
 * inactivas, para histórico); un selector operativo solo las activas.
 */
async function listarPromociones({ soloActivas = false } = {}) {
  const where = soloActivas ? { activa: true } : {};
  return LoyaltyPromotion.findAll({ where, order: [['fechaInicio', 'DESC']] });
}

function _validarRangoFechas(fechaInicio, fechaFin) {
  const inicio = new Date(fechaInicio);
  const fin = new Date(fechaFin);
  if (Number.isNaN(inicio.getTime()) || Number.isNaN(fin.getTime())) {
    throw new BadRequestError('fechaInicio/fechaFin deben ser fechas válidas.');
  }
  if (fin < inicio) {
    throw new BadRequestError('fechaFin debe ser mayor o igual a fechaInicio.');
  }
}

async function crearPromocion({ nombre, fechaInicio, fechaFin, porcentajeBono } = {}) {
  const nombreTrim = String(nombre ?? '').trim();
  if (!nombreTrim) {
    throw new BadRequestError('nombre es requerido.');
  }
  _validarRangoFechas(fechaInicio, fechaFin);
  const porcentajeValidado = _validarPorcentajeGenerico(porcentajeBono, 'El % de bono de la promoción');

  return LoyaltyPromotion.create({
    nombre: nombreTrim, fechaInicio, fechaFin, porcentajeBono: porcentajeValidado, activa: true,
  });
}

/**
 * Edita una promoción existente. Patch parcial igual que `editarReason`: solo
 * pisa los campos presentes. Si solo cambia fechaInicio O fechaFin (no
 * ambas), el rango resultante se valida contra el valor YA GUARDADO del otro
 * campo (nunca asumir que ambos vienen juntos en el payload).
 */
async function editarPromocion(id, { nombre, fechaInicio, fechaFin, porcentajeBono, activa } = {}) {
  const promocion = await LoyaltyPromotion.findByPk(id);
  if (!promocion) throw new NotFoundError('Promoción');

  const cambios = {};

  if (nombre !== undefined) {
    const nombreTrim = String(nombre ?? '').trim();
    if (!nombreTrim) throw new BadRequestError('nombre es requerido.');
    cambios.nombre = nombreTrim;
  }

  if (porcentajeBono !== undefined) {
    cambios.porcentajeBono = _validarPorcentajeGenerico(porcentajeBono, 'El % de bono de la promoción');
  }

  if (fechaInicio !== undefined || fechaFin !== undefined) {
    const inicioResultante = fechaInicio !== undefined ? fechaInicio : promocion.fechaInicio;
    const finResultante = fechaFin !== undefined ? fechaFin : promocion.fechaFin;
    _validarRangoFechas(inicioResultante, finResultante);
    if (fechaInicio !== undefined) cambios.fechaInicio = fechaInicio;
    if (fechaFin !== undefined) cambios.fechaFin = fechaFin;
  }

  if (activa !== undefined) cambios.activa = !!activa;

  await promocion.update(cambios);
  return promocion;
}

/**
 * Promoción vigente en `fecha` (default: ahora), la de mayor
 * `porcentajeBono` si hay dos solapadas (confirmado con el usuario: gana la
 * mayor). `null` si ninguna aplica. Comparación por DÍA (fechaInicio/fechaFin
 * son DATEONLY) — se normaliza `fecha` a 'YYYY-MM-DD' en hora de México, NO
 * `toISOString()` (UTC): un server en UTC desfasaría hasta 6h el día real en
 * México, prendiendo o cortando la promo antes/después de lo esperado — mismo
 * bug de huso horario ya visto y corregido en erp-modal/Kore.
 */
function _hoyMexico(fecha) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Mexico_City', day: '2-digit', month: '2-digit', year: 'numeric',
  }).format(fecha instanceof Date ? fecha : new Date(fecha));
}

async function obtenerPromocionVigente(fecha = new Date()) {
  const fechaStr = _hoyMexico(fecha);
  return LoyaltyPromotion.findOne({
    where: {
      activa:      true,
      fechaInicio: { [Op.lte]: fechaStr },
      fechaFin:    { [Op.gte]: fechaStr },
    },
    order: [['porcentajeBono', 'DESC']],
  });
}

module.exports = {
  obtenerPorcentajes,
  guardarPorcentajes,
  listarReasons,
  crearReason,
  editarReason,
  listarSucursales,
  actualizarSucursal,
  listarPromociones,
  crearPromocion,
  editarPromocion,
  obtenerPromocionVigente,
};
