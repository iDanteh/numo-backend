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
const { sequelize } = require('../../../config/database.postgres');
const {
  LoyaltyAdjustmentReason, LoyaltyPromotion, CentroCosto,
  LoyaltyKoreConcepto, LoyaltyKoreFormaPago, LoyaltyKoreNivel,
  ConfigSection, GlobalConfig,
} = require('../../../shared/models/postgres');
const { BadRequestError, NotFoundError } = require('../../../shared/errors/AppError');
const globalConfigService = require('../../../shared/services/global-config.service');
const koreCatalogosClient = require('./loyalty-kore-catalogos.client');

const SECTION = 'lealtad';
const NIVELES = ['BRONCE', 'PLATA', 'ORO'];

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
 * CONGELAMIENTO_MESES (2026-11-03, pedido explícito del usuario) — "Congelamiento de puntos"
 * por inactividad: meses sin una ACUMULACIÓN real tras los cuales el job diario
 * (`loyalty/jobs/congelamiento-puntos.service.js`) resetea el saldo de una tarjeta a 0, vía un
 * AJUSTE real en el ledger (auditable con el historial existente — PR2, sin UI nueva). Vive en
 * `GlobalConfig` sección `lealtad` (mismo mecanismo que los % de nivel), `tipo:'numero'`.
 *
 * Deshabilitado = sin configurar (D11: nunca default inventado) O configurado EXPLÍCITAMENTE en
 * 0 — mismo criterio exacto en lectura y escritura, para que `obtenerCongelamientoMeses() ===
 * null` sea la única señal que el job necesita consultar antes de correr.
 */
function _validarCongelamientoMeses(valor) {
  if (valor === null || valor === undefined || valor === '') return 0;
  const n = Number(valor);
  if (!Number.isInteger(n) || n < 0) {
    throw new BadRequestError('Los meses de congelamiento deben ser un número entero positivo (o 0/vacío para deshabilitar).');
  }
  return n;
}

/** `null` cuando no está configurado o está en 0 (deshabilitado) — nunca se asume un valor. */
async function obtenerCongelamientoMeses() {
  const valor = await globalConfigService.getValue(SECTION, 'CONGELAMIENTO_MESES').catch(() => null);
  if (valor === null) return null;
  const n = Number(valor);
  return (Number.isInteger(n) && n > 0) ? n : null;
}

/** Guarda los meses de congelamiento. `meses` null/undefined/0 deshabilita la feature (se
 *  persiste '0', nunca se borra la fila — mismo criterio que guardarFotoFolderId con vacío). */
async function guardarCongelamientoMeses(meses, user) {
  const { usuarioId, usuarioNombre } = _userLabel(user);
  const n = _validarCongelamientoMeses(meses);
  await globalConfigService.setValue(SECTION, 'CONGELAMIENTO_MESES', String(n), {
    tipo: 'numero', usuarioId, usuarioNombre,
  });
  return n > 0 ? n : null;
}

/**
 * CONSULTA_CODIGO_MAX_MES (2026-11-09, pedido explícito del usuario) — blindaje adicional sobre
 * la consulta pública de puntos: tope de cuántos ciclos de código ("solicitar código" +
 * "confirmar código" exitoso) puede completar UNA tarjeta por mes calendario (zona
 * America/Mexico_City). Las vistas de sesión (3 extra sin código, ver loyalty-recovery.service.js)
 * NO cuentan aparte — son parte del mismo ciclo ya contado. Mismo mecanismo que
 * CONGELAMIENTO_MESES (`GlobalConfig` sección `lealtad`, `tipo:'numero'`), idéntico criterio
 * deshabilitado = sin configurar o en 0 (D11: nunca default inventado).
 */
function _validarConsultaCodigoMaxMes(valor) {
  if (valor === null || valor === undefined || valor === '') return 0;
  const n = Number(valor);
  if (!Number.isInteger(n) || n < 0) {
    throw new BadRequestError('El máximo de consultas con código por mes debe ser un número entero positivo (o 0/vacío para deshabilitar).');
  }
  return n;
}

/** `null` cuando no está configurado o está en 0 (sin límite) — nunca se asume un valor. */
async function obtenerConsultaCodigoMaxMes() {
  const valor = await globalConfigService.getValue(SECTION, 'CONSULTA_CODIGO_MAX_MES').catch(() => null);
  if (valor === null) return null;
  const n = Number(valor);
  return (Number.isInteger(n) && n > 0) ? n : null;
}

/** Guarda el máximo de consultas con código por mes. `max` null/undefined/0 deshabilita el límite
 *  (se persiste '0', nunca se borra la fila — mismo criterio que guardarCongelamientoMeses). */
async function guardarConsultaCodigoMaxMes(max, user) {
  const { usuarioId, usuarioNombre } = _userLabel(user);
  const n = _validarConsultaCodigoMaxMes(max);
  await globalConfigService.setValue(SECTION, 'CONSULTA_CODIGO_MAX_MES', String(n), {
    tipo: 'numero', usuarioId, usuarioNombre,
  });
  return n > 0 ? n : null;
}

/**
 * CANJE_CODIGO_VIGENCIA_MIN (2026-11-13, pedido explícito del usuario) — cuántos minutos dura
 * vigente el código de un solo uso que Numo genera y manda al cliente para autorizar un canje de
 * puntos en una venta de Kore (ver `loyalty-kore-canje.service.js`). Mismo mecanismo que
 * CONGELAMIENTO_MESES/CONSULTA_CODIGO_MAX_MES (`GlobalConfig` sección `lealtad`, `tipo:'numero'`),
 * pero a diferencia de esos dos, acá NO hay un significado especial en 0 — la vigencia siempre
 * tiene que ser un número positivo real (nunca "deshabilitado"), con **default 5** si todavía no
 * se configuró nada (pedido explícito del usuario, no D11: acá sí hace falta un default usable
 * desde el primer canje, no se puede dejar sin vigencia).
 */
function _validarCanjeCodigoVigenciaMin(valor) {
  const n = Number(valor);
  if (!Number.isInteger(n) || n <= 0) {
    throw new BadRequestError('La vigencia del código de canje debe ser un número entero mayor a 0 (minutos).');
  }
  return n;
}

const CANJE_CODIGO_VIGENCIA_MIN_DEFAULT = 5;

/** Minutos de vigencia vigentes — default 5 si nunca se configuró explícitamente. */
async function obtenerCanjeCodigoVigenciaMin() {
  const valor = await globalConfigService.getValue(SECTION, 'CANJE_CODIGO_VIGENCIA_MIN').catch(() => null);
  if (valor === null) return CANJE_CODIGO_VIGENCIA_MIN_DEFAULT;
  const n = Number(valor);
  return (Number.isInteger(n) && n > 0) ? n : CANJE_CODIGO_VIGENCIA_MIN_DEFAULT;
}

/** Guarda la vigencia (minutos). No admite 0/vacío — mismo criterio de _validarCanjeCodigoVigenciaMin. */
async function guardarCanjeCodigoVigenciaMin(minutos, user) {
  const { usuarioId, usuarioNombre } = _userLabel(user);
  const n = _validarCanjeCodigoVigenciaMin(minutos);
  await globalConfigService.setValue(SECTION, 'CANJE_CODIGO_VIGENCIA_MIN', String(n), {
    tipo: 'numero', usuarioId, usuarioNombre,
  });
  return n;
}

/**
 * ESPECIALIDADES (2026-10-23) — catálogo de referencia para altas futuras de clientes, vive en
 * `GlobalConfig` sección `lealtad` (mismo mecanismo que los %), `tipo:'lista'` (JSON array de
 * strings). Se gestiona DESDE acá (loyalty:config/loyalty:read), nunca desde el `config:manage`
 * genérico — fusión de UI pedida explícitamente por el usuario para no tener dos pantallas
 * "Lealtad" distintas (ver config-admin.component.ts, que filtra la sección 'lealtad' del riel
 * genérico). El valor del CSV de import NO se valida contra este catálogo (es informativo).
 */
async function obtenerEspecialidades() {
  const valor = await globalConfigService.getValue(SECTION, 'ESPECIALIDADES').catch(() => '[]');
  try {
    const parsed = JSON.parse(valor || '[]');
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

/** Reemplaza el catálogo completo (mismo criterio que el editor de chips de Configuraciones
 *  Globales: cada add/quitar manda la lista entera, no un delta). Limpia vacíos/duplicados. */
async function guardarEspecialidades(lista, user) {
  const { usuarioId, usuarioNombre } = _userLabel(user);
  const limpia = Array.isArray(lista)
    ? [...new Set(lista.map((v) => String(v ?? '').trim()).filter(Boolean))]
    : [];
  await globalConfigService.setValue(SECTION, 'ESPECIALIDADES', JSON.stringify(limpia), {
    tipo: 'lista', usuarioId, usuarioNombre,
  });
  return limpia;
}

/**
 * FOTO_CLIENTE_FOLDER_ID (2026-10-29) — Folder ID de Drive consumido por
 * `drive-loyalty-foto.service.js#folderId()`. Mismo mecanismo que los % y
 * ESPECIALIDADES (`GlobalConfig` sección `lealtad`), `tipo: 'texto'` (igual
 * que FICHAS_IMAGEN_FOLDER_ID en `seed-global-config-banks.js`) porque es un
 * string simple, no una lista. Gestionado DESDE acá (loyalty:config/
 * loyalty:read) en vez del `config:manage` genérico, mismo criterio que
 * ESPECIALIDADES arriba — la sección 'lealtad' está oculta del riel genérico
 * de Configuraciones Globales a propósito (ver config-admin.component.ts).
 */
async function obtenerFotoFolderId() {
  const valor = await globalConfigService.getValue(SECTION, 'FOTO_CLIENTE_FOLDER_ID').catch(() => null);
  return valor === null ? '' : String(valor).trim();
}

/** Puede quedar vacío ("no configurado") — `drive-loyalty-foto.service.js#folderId()` ya rechaza
 *  explícito si al momento de subir una foto el valor sigue vacío. Acá no se valida formato de
 *  Drive ID: el admin puede pegar la URL completa y recortarla después, no hay forma confiable de
 *  distinguir un ID válido de uno inventado sin llamar a la API de Drive. */
async function guardarFotoFolderId(valor, user) {
  const { usuarioId, usuarioNombre } = _userLabel(user);
  const limpio = String(valor ?? '').trim();
  await globalConfigService.setValue(SECTION, 'FOTO_CLIENTE_FOLDER_ID', limpio, {
    tipo: 'texto', usuarioId, usuarioNombre,
  });
  return limpio;
}

/**
 * Historial de cambios de UN valor de configuración de Lealtad (% por nivel,
 * ESPECIALIDADES, FOTO_CLIENTE_FOLDER_ID — cualquier `clave` de la sección
 * `lealtad`). El audit log YA existe para cada guardado (`ConfigAuditLog`, ver
 * `global-config.service.js#setValue`) — acá solo se resuelve `sectionClave +
 * clave` a la fila `GlobalConfig` (mismo criterio que `setValue` usa
 * internamente para encontrar/crear esa fila) y se delega el armado del
 * historial a `listAuditLog(configId)` TAL CUAL, sin duplicar su lógica.
 *
 * Gateada por `loyalty:config` en la ruta (PR4/8) — nunca por el `config:manage`
 * genérico, mismo criterio que el resto de esta pantalla (D10).
 *
 * Si la clave nunca se guardó (no existe fila todavía en `GlobalConfig`), no
 * hay nada que auditar: se devuelve `[]` en vez de tirar error (ej. un admin
 * nuevo abre "Ver historial" de FOTO_CLIENTE_FOLDER_ID antes de haberlo
 * configurado nunca).
 */
async function obtenerHistorial(clave) {
  const section = await ConfigSection.findOne({ where: { clave: SECTION } });
  if (!section) return [];

  const config = await GlobalConfig.findOne({ where: { sectionId: section.id, clave } });
  if (!config) return [];

  return globalConfigService.listAuditLog(config.id);
}

const CATEGORIAS_REASON = ['ajuste', 'bonificacion'];

function _validarCategoria(categoria) {
  if (!CATEGORIAS_REASON.includes(categoria)) {
    throw new BadRequestError(`categoria debe ser una de: ${CATEGORIAS_REASON.join(', ')}.`);
  }
  return categoria;
}

/**
 * Catálogo de motivos de ajuste. `soloActivos` es lo que debe consumir el
 * selector de un AJUSTE nuevo (un motivo desactivado no es seleccionable);
 * sin el flag trae todo, para la pantalla de administración del catálogo
 * (donde también se ven/reactivan los inactivos).
 *
 * `categoria` (split 2026-11-03, pedido explícito del usuario): filtra entre
 * "Motivos de ajuste" y "Catálogo de bonificaciones" — mismo catálogo físico
 * (ver LoyaltyAdjustmentReason.js), la UI pide cada sección por separado.
 * Sin el filtro trae ambas categorías juntas (ej. el selector de un AJUSTE
 * nuevo en movimiento-modal, que puede usar un motivo de cualquiera de las dos).
 */
async function listarReasons({ soloActivos = false, categoria } = {}) {
  const where = soloActivos ? { activo: true } : {};
  if (categoria !== undefined) where.categoria = _validarCategoria(categoria);
  return LoyaltyAdjustmentReason.findAll({ where, order: [['nombre', 'ASC']] });
}

// Entero, cualquier signo, distinto de 0 — null/undefined es válido aparte (modo manual).
function _validarPuntosFijos(valor) {
  const n = Number(valor);
  if (!Number.isInteger(n) || n === 0) {
    throw new BadRequestError('Puntos fijos debe ser un número entero distinto de 0 (o dejarse vacío para modo manual).');
  }
  return n;
}

/**
 * `puntosFijos` (rediseño 2026-10-23, reemplaza `signoPermitido`/`porcentaje`): si viene
 * definido (puede ser negativo), un AJUSTE con este motivo aplica EXACTAMENTE ese valor — ver
 * loyalty-ledger.service.js#registrarAjuste. Si no, el motivo queda en modo manual (quien aplica
 * el ajuste escribe los puntos a mano, cualquier signo).
 *
 * `categoria` (split 2026-11-03) es REQUERIDA — columna NOT NULL en `loyalty_adjustment_reasons`
 * (ver migración en shared/models/postgres/index.js). La UI nunca deja elegirla en el formulario:
 * cada sección de lealtad-config.component.ts la manda fija según en cuál está parado el usuario.
 */
async function crearReason({ clave, nombre, puntosFijos, categoria } = {}) {
  const nombreTrim = String(nombre ?? '').trim();
  if (!nombreTrim) {
    throw new BadRequestError('nombre es requerido.');
  }
  const categoriaValidada = _validarCategoria(categoria);

  const puntosFijosValidado = (puntosFijos === undefined || puntosFijos === null)
    ? null
    : _validarPuntosFijos(puntosFijos);

  return LoyaltyAdjustmentReason.create({
    clave, nombre: nombreTrim, activo: true, puntosFijos: puntosFijosValidado, categoria: categoriaValidada,
  });
}

/**
 * Edita un motivo existente. Desactivar (`activo:false`) SOLO cambia el flag
 * — nunca se borra la fila, porque `loyalty_points_ledger.reasonId` sigue
 * referenciándola en entradas históricas (requisito del spec: "Historical
 * entries keep their original reason").
 */
async function editarReason(id, { nombre, activo, puntosFijos, categoria } = {}) {
  const reason = await LoyaltyAdjustmentReason.findByPk(id);
  if (!reason) throw new NotFoundError('Motivo de ajuste');

  // `puntosFijos: null` explícito es válido (vuelve el motivo manual) — se distingue de
  // `undefined` (campo no tocado).
  let puntosFijosValidado;
  if (puntosFijos !== undefined) {
    puntosFijosValidado = puntosFijos === null ? null : _validarPuntosFijos(puntosFijos);
  }

  // `categoria` (split 2026-11-03): opcional en edición (undefined = no tocar) — la UI nunca la
  // manda (una sección no mueve un motivo a la otra), pero el servicio la soporta por completitud
  // del contrato CRUD, validada igual que en crearReason si viene presente.
  const categoriaValidada = categoria !== undefined ? _validarCategoria(categoria) : undefined;

  const cambios = {};
  if (nombre !== undefined) cambios.nombre = nombre;
  if (activo !== undefined) cambios.activo = activo;
  if (puntosFijos !== undefined) cambios.puntosFijos = puntosFijosValidado;
  if (categoria !== undefined) cambios.categoria = categoriaValidada;

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

/**
 * CATÁLOGOS DE KORE (2026-10-26) — conceptos (tipos de venta) y formas de
 * pago, espejados para configurar manualmente qué ventas acumulan puntos (el
 * webhook Kore→Numo que los va a consumir todavía NO existe, es un tema
 * aparte). Decisión confirmada con el usuario: `acumulaPuntos`/`esPuntos` son
 * SIEMPRE manuales — un sync nunca los pisa, solo trae nombre/abreviatura/tipo
 * (conceptos) o los hints informativos de Kore (formas de pago).
 */

/** Upsert por `koreId`: solo toca nombre/abreviatura/tipo en los existentes,
 *  nunca `acumulaPuntos`. Marca `activo:false` en lo que ya no vino de Kore. */
async function sincronizarConceptosKore() {
  const conceptosKore = await koreCatalogosClient.obtenerConceptosKore();
  const idsVistos = new Set();
  let nuevos = 0;
  let actualizados = 0;

  for (const c of conceptosKore) {
    const koreId = String(c.Id ?? '').trim();
    if (!koreId) continue;
    idsVistos.add(koreId);

    const datos = {
      nombre:       String(c.Nombre ?? '').trim(),
      abreviatura:  c.Abreviatura ?? null,
      tipo:         c.Tipo ?? null,
      activo:       true,
    };

    // eslint-disable-next-line no-await-in-loop -- universo chico (catálogo de conceptos de Kore,
    // decenas de filas), secuencial sin problema, mismo criterio que correrMatchKore.
    const [fila, creada] = await LoyaltyKoreConcepto.findOrCreate({
      where:    { koreId },
      defaults: { koreId, ...datos, acumulaPuntos: false },
    });
    if (creada) {
      nuevos += 1;
    } else {
      // eslint-disable-next-line no-await-in-loop
      await fila.update(datos);
      actualizados += 1;
    }
  }

  const existentes = await LoyaltyKoreConcepto.findAll({ where: { activo: true } });
  const aDesactivar = existentes.filter(f => !idsVistos.has(f.koreId));
  for (const f of aDesactivar) {
    // eslint-disable-next-line no-await-in-loop
    await f.update({ activo: false });
  }

  return { nuevos, actualizados, desactivados: aDesactivar.length };
}

/** Mismo criterio que sincronizarConceptosKore, pero además refresca los
 *  hints informativos de Kore (koreAcumulaPuntosHint/koreEsPuntosHint) en
 *  cada sync — esos sí se pisan siempre, nunca son la config real. */
async function sincronizarFormasPagoKore() {
  const formasPagoKore = await koreCatalogosClient.obtenerFormasPagoKore();
  const idsVistos = new Set();
  let nuevos = 0;
  let actualizados = 0;

  for (const f of formasPagoKore) {
    const koreId = String(f.ID ?? '').trim();
    if (!koreId) continue;
    idsVistos.add(koreId);

    const datos = {
      nombre:                 String(f.Nombre ?? '').trim(),
      koreAcumulaPuntosHint:  !!f.AcumulaPuntosTuberos,
      koreEsPuntosHint:       !!f.EsPuntos,
      activo:                 true,
    };

    // eslint-disable-next-line no-await-in-loop -- universo chico, mismo criterio de arriba.
    const [fila, creada] = await LoyaltyKoreFormaPago.findOrCreate({
      where:    { koreId },
      defaults: { koreId, ...datos, acumulaPuntos: false, esPuntos: false },
    });
    if (creada) {
      nuevos += 1;
    } else {
      // eslint-disable-next-line no-await-in-loop
      await fila.update(datos);
      actualizados += 1;
    }
  }

  const existentes = await LoyaltyKoreFormaPago.findAll({ where: { activo: true } });
  const aDesactivar = existentes.filter(f => !idsVistos.has(f.koreId));
  for (const f of aDesactivar) {
    // eslint-disable-next-line no-await-in-loop
    await f.update({ activo: false });
  }

  return { nuevos, actualizados, desactivados: aDesactivar.length };
}

async function listarConceptosKore({ soloActivos = false } = {}) {
  const where = soloActivos ? { activo: true } : {};
  return LoyaltyKoreConcepto.findAll({ where, order: [['nombre', 'ASC']] });
}

async function listarFormasPagoKore({ soloActivos = false } = {}) {
  const where = soloActivos ? { activo: true } : {};
  return LoyaltyKoreFormaPago.findAll({ where, order: [['nombre', 'ASC']] });
}

async function actualizarConceptoKore(id, { acumulaPuntos } = {}) {
  const fila = await LoyaltyKoreConcepto.findByPk(id);
  if (!fila) throw new NotFoundError('Concepto de Kore');
  if (acumulaPuntos !== undefined) await fila.update({ acumulaPuntos: !!acumulaPuntos });
  return fila;
}

/** Si `esPuntos:true`, primero limpia cualquier OTRA fila que ya lo tuviera —
 *  invariante "como mucho una forma de pago Puntos a la vez", manejada acá de
 *  forma explícita (en vez de dejar que el índice único parcial tire un error
 *  de constraint poco legible). */
async function actualizarFormaPagoKore(id, { acumulaPuntos, esPuntos } = {}) {
  const fila = await LoyaltyKoreFormaPago.findByPk(id);
  if (!fila) throw new NotFoundError('Forma de pago de Kore');

  await sequelize.transaction(async (t) => {
    if (esPuntos === true) {
      await LoyaltyKoreFormaPago.update(
        { esPuntos: false },
        { where: { esPuntos: true, id: { [Op.ne]: fila.id } }, transaction: t },
      );
    }
    const cambios = {};
    if (acumulaPuntos !== undefined) cambios.acumulaPuntos = !!acumulaPuntos;
    if (esPuntos !== undefined) cambios.esPuntos = !!esPuntos;
    await fila.update(cambios, { transaction: t });
  });

  return fila;
}

/**
 * CATÁLOGO DE NIVELES DE KORE (2026-11-06, pedido explícito del usuario) — a diferencia de
 * conceptos/formas de pago, acá `porcentaje` SÍ es autoritativo de Kore: Numo ya no configura
 * ningún % propio (reemplaza PORCENTAJE_BRONCE/PLATA/ORO de GlobalConfig como fuente viva — esas
 * claves quedan sin UI de edición desde esta pantalla, pero el mecanismo de GlobalConfig en sí no
 * se tocó, por si algo más lo sigue leyendo). `esInicial` es la ÚNICA columna manual de Numo.
 *
 * Bootstrap transicional (ver seed-lealtad.js): existen filas con `koreId:null` para Bronce/Plata/
 * Oro hasta que el primer sync/webhook real de Kore llegue. `_buscarOAdoptarNivelKore` busca
 * primero por `koreId` real; si no aparece, busca por `nombre` entre las filas transicionales
 * (`koreId IS NULL`) y la ADOPTA (le asigna el koreId real) en vez de crear una duplicada.
 */
async function _buscarOAdoptarNivelKore({ koreId, nombre }) {
  let fila = await LoyaltyKoreNivel.findOne({ where: { koreId } });
  if (!fila && nombre) {
    fila = await LoyaltyKoreNivel.findOne({ where: { koreId: null, nombre } });
  }
  return fila;
}

/** Upsert por `koreId` (con adopción de fila transicional por nombre, ver arriba). `porcentaje` SÍ
 *  se pisa en cada sync (a diferencia de `acumulaPuntos` en conceptos/formas de pago) porque acá
 *  es la fuente real, no un hint. `esInicial` nunca se toca por un sync. Marca `activo:false` en
 *  lo que ya no vino de Kore — las filas transicionales (`koreId` todavía null) se excluyen de esa
 *  desactivación, nunca se apagan solas por no aparecer en una corrida puntual. */
async function sincronizarNivelesKore() {
  const nivelesKore = await koreCatalogosClient.obtenerNivelesKore();
  const idsVistos = new Set();
  let nuevos = 0;
  let actualizados = 0;

  for (const n of nivelesKore) {
    const koreId = String(n.Id ?? n.ID ?? '').trim();
    if (!koreId) continue;
    idsVistos.add(koreId);

    const datos = {
      koreId,
      nombre:     String(n.Nombre ?? '').trim(),
      porcentaje: n.Porcentaje != null && n.Porcentaje !== '' ? Number(n.Porcentaje) : null,
      activo:     true,
    };

    // eslint-disable-next-line no-await-in-loop -- universo chico (catálogo de niveles de Kore,
    // un puñado de filas), mismo criterio secuencial que los otros 2 sync de este archivo.
    const fila = await _buscarOAdoptarNivelKore({ koreId, nombre: datos.nombre });
    if (!fila) {
      // eslint-disable-next-line no-await-in-loop
      await LoyaltyKoreNivel.create({ ...datos, esInicial: false });
      nuevos += 1;
    } else {
      // eslint-disable-next-line no-await-in-loop
      await fila.update(datos); // adopta el koreId real si la fila era transicional
      actualizados += 1;
    }
  }

  const existentes = await LoyaltyKoreNivel.findAll({ where: { activo: true } });
  const aDesactivar = existentes.filter(f => f.koreId && !idsVistos.has(f.koreId));
  for (const f of aDesactivar) {
    // eslint-disable-next-line no-await-in-loop
    await f.update({ activo: false });
  }

  return { nuevos, actualizados, desactivados: aDesactivar.length };
}

async function listarNivelesKore({ soloActivos = false } = {}) {
  const where = soloActivos ? { activo: true } : {};
  return LoyaltyKoreNivel.findAll({ where, order: [['porcentaje', 'ASC']] });
}

/** % de acumulación real de un nivel — fuente única `LoyaltyKoreNivel.porcentaje` (autoritativo de
 *  Kore, ya no `GlobalConfig.lealtad.PORCENTAJE_<NIVEL>`). Devuelve `null` si el nivel no existe,
 *  está inactivo, o no tiene `porcentaje` configurado — nunca lanza, el caller decide qué hacer
 *  (bloquear con un mensaje, registrar un proceso fallido, etc.). Reusada por
 *  `loyalty-ledger.service.js#registrarAcumulacion` (flujo manual, bloqueante) y
 *  `loyalty-kore-venta.service.js` (webhook: metadata informativa siempre, y cálculo de respaldo
 *  cuando Kore no manda `puntosAbonados`) para no duplicar la fuente de verdad. */
async function obtenerPorcentajeNivel(nombre) {
  if (!nombre) return null;
  const fila = await LoyaltyKoreNivel.findOne({ where: { nombre, activo: true } });
  if (!fila || fila.porcentaje == null) return null;
  return Number(fila.porcentaje);
}

/** Único campo editable por Numo. `esInicial:true` limpia cualquier otra fila que ya lo tuviera —
 *  mismo criterio explícito que `esPuntos` en actualizarFormaPagoKore (en vez de dejar que el
 *  índice único parcial tire un error de constraint poco legible). */
async function actualizarNivelKore(id, { esInicial } = {}) {
  const fila = await LoyaltyKoreNivel.findByPk(id);
  if (!fila) throw new NotFoundError('Nivel de Kore');
  if (esInicial === undefined) return fila;

  await sequelize.transaction(async (t) => {
    if (esInicial === true) {
      await LoyaltyKoreNivel.update(
        { esInicial: false },
        { where: { esInicial: true, id: { [Op.ne]: fila.id } }, transaction: t },
      );
    }
    await fila.update({ esInicial: !!esInicial }, { transaction: t });
  });

  return fila;
}

module.exports = {
  obtenerPorcentajes,
  guardarPorcentajes,
  obtenerCongelamientoMeses,
  guardarCongelamientoMeses,
  obtenerConsultaCodigoMaxMes,
  guardarConsultaCodigoMaxMes,
  obtenerCanjeCodigoVigenciaMin,
  guardarCanjeCodigoVigenciaMin,
  obtenerEspecialidades,
  guardarEspecialidades,
  obtenerFotoFolderId,
  guardarFotoFolderId,
  obtenerHistorial,
  listarReasons,
  crearReason,
  editarReason,
  listarSucursales,
  actualizarSucursal,
  listarPromociones,
  crearPromocion,
  editarPromocion,
  obtenerPromocionVigente,
  sincronizarConceptosKore,
  sincronizarFormasPagoKore,
  listarConceptosKore,
  listarFormasPagoKore,
  actualizarConceptoKore,
  actualizarFormaPagoKore,
  sincronizarNivelesKore,
  listarNivelesKore,
  actualizarNivelKore,
  obtenerPorcentajeNivel,
  // Reexportada (prefijo `_`, mismo criterio que loyalty-ledger.service.js#_resolverCentroCostoPorSerieFolio)
  // para que loyalty-kore-nivel.service.js (webhook) reuse la MISMA lógica de adopción de fila
  // transicional que el sync, sin duplicarla.
  _buscarOAdoptarNivelKore,
};
