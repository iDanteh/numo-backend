'use strict';

/**
 * domains/import/loyalty-import-apply.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — "aplicar" la migración de clientes legados ("Club
 * Tuberos") a partir del concentrado de `loyalty-import.service.js`, y la
 * re-aplicación posterior de las filas bloqueantes que el admin corrigió a
 * mano en un Excel aparte (ver `construirFilasDesdeCorrecciones`).
 *
 * Categorías (precedencia, primer match gana):
 *   1. duplicado_bloqueante     — misma Tarjeta → 2+ personas distintas en el
 *                                 set de entrada (ambiguo de verdad, nunca se
 *                                 elige una al azar).
 *   2. pendiente_sin_telefono   — ni Celular ni Telefono (advertencia
 *                                 `sin_contacto` del concentrado). NO bloquea:
 *                                 decisión de negocio 2026-10-19, ver
 *                                 LoyaltyPendingEnrollment.js.
 *   3. invalido_bloqueante      — `contacto` no normaliza a 10 dígitos.
 *   4. conflicto_bloqueante     — ya existe una LoyaltyCard con ese teléfono
 *                                 y NO fue creada por una migración previa
 *                                 (no se pisa una tarjeta real en uso).
 *   5. ya_migrada               — ya existe una LoyaltyCard con ese teléfono
 *                                 Y sí fue creada por una migración previa
 *                                 (re-correr `aplicar` es idempotente). Desde 2026-10-22 SÍ es
 *                                 accionable (ver _aplicarFila): si a esa tarjeta le falta
 *                                 `tarjetaLegada` y/o alguno de los 13 campos de enriquecimiento
 *                                 (apodo/dirección/etc., desde 2026-10-23, cada uno con su propio
 *                                 gate independiente — ver _aplicarFila), se completan desde esta
 *                                 fila, SIN tocar puntos/nivel/nombre ni crear un segundo
 *                                 movimiento de ledger. Es el mecanismo de "re-import repetible":
 *                                 volver a subir el MISMO CSV completa el dato faltante de las
 *                                 tarjetas ya migradas, sin un script de backfill aparte.
 *   6. ok                       — se crea la tarjeta + movimiento MIGRACION + `tarjetaLegada`.
 *
 * Decisión de negocio 2026-10-19 (reemplaza el criterio anterior de "abortar
 * con CERO escrituras si hay bloqueantes"): `aplicar`/`iniciarAplicarJob` YA
 * NUNCA abortan por filas bloqueantes — procesan todas las filas 'ok' y
 * 'pendiente_sin_telefono' de la corrida, y simplemente EXCLUYEN (no escriben,
 * no abortan nada) las filas duplicado/invalido/conflicto_bloqueante, cuyo
 * conteo se reporta en `excluidasBloqueantes`. El admin las corrige en el
 * Excel exportado por el frontend (`descargarBloqueantes()`) y las vuelve a
 * subir por separado vía `clasificarCorrecciones`/`iniciarAplicarCorreccionesJob`,
 * que reutilizan exactamente la misma clasificación/aplicación/batching.
 */

const { Transaction, Op } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');
const { LoyaltyCard, LoyaltyPointsLedger, LoyaltyPendingEnrollment } = require('../../../shared/models/postgres');
const { BadRequestError, NotFoundError } = require('../../../shared/errors/AppError');
const { normalizarTelefono } = require('../../shared/puntos.util');
const { construirConcentrado, _resolverNivel } = require('./loyalty-import.service');
const { normalizarTarjeta } = require('./loyalty-import.parser');
const cardService = require('../cards/loyalty-card.service');
const ledgerService = require('../ledger/loyalty-ledger.service');
// El servidor de Socket.IO es infraestructura de toda la app (salas `user:{id}` para
// notificaciones personales, ver su propio docstring: "roles, importación"), aunque hoy vive
// físicamente bajo banks/ por ser el primer dominio que lo necesitó.
const { emitToUser } = require('../../../banks/shared/socket');
const { logger } = require('../../../shared/utils/logger');

// Enriquecimiento del import (2026-10-23) — registro histórico simple, ver LoyaltyCard.js.
// '' → null (no tiene sentido persistir cadenas vacías en columnas opcionales).
// 'apellidos' sumado 2026-10-23 (enfoque aditivo: `nombre` combinado sigue intacto, ver
// LoyaltyCard.js#apellidos) — reusa EXACTAMENTE el mismo mecanismo de backfill campo-por-campo
// que el resto de columnas de enriquecimiento, sin lógica nueva.
const CAMPOS_ENRIQUECIMIENTO = [
  'apellidos', 'apodo', 'sexo', 'fechaNacimiento', 'especialidades', 'sucursal', 'calle',
  'numExterior', 'numInterior', 'colonia', 'municipio', 'estado', 'cp', 'comentarios',
];
function _datosEnriquecimiento(fila) {
  const datos = {};
  for (const campo of CAMPOS_ENRIQUECIMIENTO) {
    datos[campo] = fila[campo] ? fila[campo] : null;
  }
  return datos;
}

function _userLabel(user) {
  return {
    usuarioId:     user?.dbId != null ? String(user.dbId) : (user?._id != null ? String(user._id) : null),
    usuarioNombre: user?.nombre ?? null,
  };
}

// Campo del `clasificacionResumen` que corresponde a cada categoría.
const CAMPO_POR_CATEGORIA = {
  ok:                          'ok',
  pendiente_sin_telefono:      'pendienteSinTelefono',
  ya_migrada:                  'yaMigrada',
  duplicado_bloqueante:        'duplicadoBloqueante',
  invalido_bloqueante:         'invalidoBloqueante',
  conflicto_bloqueante:        'conflictoBloqueante',
  telefono_duplicado_bloqueante: 'telefonoDuplicadoBloqueante',
  // Descarte automático por duplicidad de teléfono (2026-10-29, pedido explícito del usuario):
  // dentro de un grupo de 2+ filas con el mismo teléfono, cuando EXACTAMENTE UNA trae saldo > 0,
  // esa gana su clasificación normal (ok/ya_migrada/conflicto) y las demás (saldo 0) se descartan
  // acá — nunca se escriben, nunca generan LoyaltyPendingEnrollment. Si hay 0 o 2+ con saldo > 0
  // (empate real o nadie tiene puntos), sigue el camino manual de siempre
  // (telefono_duplicado_bloqueante para todas, ver _clasificarFilasYaConstruidas).
  descartado_duplicado_sin_puntos: 'descartadoDuplicadoSinPuntos',
};

function _clasificacionResumenVacio() {
  return {
    ok: 0, pendienteSinTelefono: 0, yaMigrada: 0,
    duplicadoBloqueante: 0, invalidoBloqueante: 0, conflictoBloqueante: 0,
    telefonoDuplicadoBloqueante: 0, descartadoDuplicadoSinPuntos: 0,
  };
}

/** Shape de UNA fila descartada automáticamente por duplicidad de teléfono (saldo 0, perdió
 *  contra otra fila del mismo grupo con saldo > 0) — usado tanto en `aplicar()` como en el job en
 *  background, pensado para el Excel descargable del frontend (ver
 *  importar-clientes.component.ts). Incluye `telefonoConflicto` (el teléfono compartido) para que
 *  el admin pueda rastrear con quién compitió esta fila. */
function _descartadoDesdeFila(fila) {
  return {
    nombre: fila.nombre,
    telefono: fila.contacto,
    tarjetaLegada: fila.tarjetaRawClientes ?? null,
    saldo: fila.saldo,
    telefonoConflicto: fila.telefonoNormalizado ?? null,
  };
}

/**
 * Clasifica UNA fila del concentrado. Nunca escribe — solo lecturas
 * (LoyaltyCard.findOne/LoyaltyPointsLedger.findOne).
 *
 * `esTelefonoDuplicadoEnSet` (caso real 2026-10-19): dos filas de clientes.csv con Tarjeta
 * legada DISTINTA pueden compartir el MISMO teléfono (ej. error de captura, o el mismo cliente
 * registrado dos veces bajo tarjetas distintas) — eso NO lo detecta `tarjeta_duplicada_en_clientes`
 * (que agrupa por Tarjeta, no por teléfono). Sin este chequeo, `clasificar()` marcaba AMBAS filas
 * como 'ok' (ninguna tenía tarjeta previa en la DB al momento de clasificar, que es de solo
 * lectura) y la segunda reventaba el UNIQUE constraint de `loyalty_cards` recién al aplicar,
 * tumbando el lote en pleno vuelo. Se detecta ANTES de llegar acá (ver
 * `_clasificarFilasYaConstruidas`) y bloquea con el mismo criterio que una tarjeta duplicada.
 */
async function _clasificarFila(fila, esTelefonoDuplicadoEnSet) {
  if (fila.advertencias.includes('tarjeta_duplicada_en_clientes')) {
    return { categoria: 'duplicado_bloqueante', telefonoNormalizado: null };
  }
  if (fila.advertencias.includes('sin_contacto')) {
    return { categoria: 'pendiente_sin_telefono', telefonoNormalizado: null };
  }

  let telefonoNormalizado;
  try {
    telefonoNormalizado = normalizarTelefono(fila.contacto);
  } catch (err) {
    return { categoria: 'invalido_bloqueante', telefonoNormalizado: null };
  }

  if (esTelefonoDuplicadoEnSet) {
    return { categoria: 'telefono_duplicado_bloqueante', telefonoNormalizado };
  }

  const existente = await LoyaltyCard.findOne({ where: { telefono: telefonoNormalizado } });
  if (!existente) {
    return { categoria: 'ok', telefonoNormalizado };
  }

  const migracionPrevia = await LoyaltyPointsLedger.findOne({
    where: { cardId: existente.id, origen: 'migracion' },
  });

  return {
    categoria: migracionPrevia ? 'ya_migrada' : 'conflicto_bloqueante',
    telefonoNormalizado,
  };
}

/**
 * Clasifica un array de filas YA construidas (shape de `construirConcentrado`/
 * `construirFilasDesdeCorrecciones`) contra el estado REAL de la base de datos.
 * Extraído de `clasificar()` para que `clasificarCorrecciones()` pueda reusar
 * exactamente la misma lógica de clasificación sin pasar por `construirConcentrado`
 * (que exige el shape de dos archivos fuente). Función de solo lectura — nunca
 * crea/edita nada, puede llamarse tantas veces como se quiera (dry-run).
 *
 * Primera pasada (sin DB): normaliza el teléfono de cada fila candidata y agrupa los ÍNDICES por
 * teléfono repetido DENTRO de este mismo set — ver `_clasificarFila` para por qué (caso real
 * 2026-10-19, dos Tarjetas legadas distintas con el mismo teléfono).
 *
 * Segunda pasada, SOLO sobre los grupos duplicados (2026-10-29, pedido explícito del usuario):
 * si EXACTAMENTE UNA fila del grupo trae saldo > 0, esa es la "ganadora" — sigue su clasificación
 * normal (ok/ya_migrada/conflicto, con su lookup real a la DB) como si no hubiera conflicto; las
 * demás ("perdedoras", saldo 0) se descartan directo, sin lookup a la DB (nunca se van a escribir).
 * Cualquier otro caso (0 o 2+ con saldo > 0 — empate real o nadie tiene puntos) sigue el camino
 * manual de siempre: todas quedan `telefono_duplicado_bloqueante`.
 */
async function _clasificarFilasYaConstruidas(filas) {
  const telefonosNormalizados = filas.map((fila) => {
    if (fila.advertencias.includes('tarjeta_duplicada_en_clientes')) return null;
    if (fila.advertencias.includes('sin_contacto')) return null;
    try {
      return normalizarTelefono(fila.contacto);
    } catch (err) {
      return null;
    }
  });

  const indicesPorTelefono = new Map();
  for (let i = 0; i < telefonosNormalizados.length; i++) {
    const tel = telefonosNormalizados[i];
    if (!tel) continue;
    if (!indicesPorTelefono.has(tel)) indicesPorTelefono.set(tel, []);
    indicesPorTelefono.get(tel).push(i);
  }

  // decisionPorIndice[i] === undefined → sin conflicto (teléfono único en el set).
  // decisionPorIndice[i] === 'ambiguo'  → grupo con 0 o 2+ filas con saldo > 0, sigue bloqueante manual.
  // decisionPorIndice[i] === 'ganador'  → única fila del grupo con saldo > 0, sigue clasificación normal.
  // decisionPorIndice[i] === 'perdedor' → resto del grupo (saldo 0), se descarta sin lookup a la DB.
  const decisionPorIndice = new Map();
  for (const indices of indicesPorTelefono.values()) {
    if (indices.length <= 1) continue; // no es un duplicado real
    const conSaldo = indices.filter((i) => Number(filas[i].saldo) > 0);
    if (conSaldo.length === 1) {
      const ganadorIdx = conSaldo[0];
      for (const i of indices) decisionPorIndice.set(i, i === ganadorIdx ? 'ganador' : 'perdedor');
    } else {
      for (const i of indices) decisionPorIndice.set(i, 'ambiguo');
    }
  }

  const clasificacionResumen = _clasificacionResumenVacio();

  const filasClasificadas = [];
  for (let i = 0; i < filas.length; i++) {
    const fila = filas[i];
    const decision = decisionPorIndice.get(i);

    if (decision === 'perdedor') {
      clasificacionResumen.descartadoDuplicadoSinPuntos += 1;
      filasClasificadas.push({
        ...fila,
        categoria: 'descartado_duplicado_sin_puntos',
        telefonoNormalizado: telefonosNormalizados[i],
      });
      continue;
    }

    // 'ganador' → sin conflicto real (esTelefonoDuplicadoEnSet=false), sigue el camino normal.
    // 'ambiguo' → mismo criterio que antes (telefono_duplicado_bloqueante para todas).
    // undefined → sin duplicado, también camino normal.
    const esTelefonoDuplicadoEnSet = decision === 'ambiguo';
    // eslint-disable-next-line no-await-in-loop -- clasificación secuencial de solo lectura, cada
    // fila puede depender de una consulta a la DB (findOne por teléfono).
    const { categoria, telefonoNormalizado } = await _clasificarFila(fila, esTelefonoDuplicadoEnSet);
    clasificacionResumen[CAMPO_POR_CATEGORIA[categoria]] += 1;
    filasClasificadas.push({ ...fila, categoria, telefonoNormalizado });
  }

  return { filas: filasClasificadas, clasificacionResumen };
}

/** Suma las categorías realmente EXCLUIDAS (nunca escritas, nunca recuperables salvo por el Excel
 * de "Descargar bloqueantes" + Correcciones) — mismo criterio usado antes por el
 * `_errorBloqueantes` ya retirado (decisión de negocio 2026-10-19: ya no abortan,
 * solo se cuentan/excluyen, ver docstring del archivo).
 *
 * `telefonoDuplicadoBloqueante` NO entra acá desde 2026-10-23: dejó de ser una categoría excluida
 * — ahora `_aplicarFila` la procesa y crea un `LoyaltyPendingEnrollment` (motivo
 * 'telefono_duplicado'), visible y resoluble desde "Clientes pendientes", así que ya no se
 * "pierde" como las demás bloqueantes. */
function _totalBloqueantes(clasificacionResumen) {
  return clasificacionResumen.duplicadoBloqueante
    + clasificacionResumen.invalidoBloqueante
    + clasificacionResumen.conflictoBloqueante;
}

/**
 * Cruza clientesRows x puntosRows (`construirConcentrado`) y clasifica cada
 * fila contra el estado REAL de la base de datos. Función de solo lectura —
 * nunca crea/edita nada, puede llamarse tantas veces como se quiera (dry-run).
 */
async function clasificar({ clientesRows, puntosRows }) {
  const { resumen, filas } = construirConcentrado({ clientesRows, puntosRows });
  const { filas: filasClasificadas, clasificacionResumen } = await _clasificarFilasYaConstruidas(filas);
  return { resumen, filas: filasClasificadas, clasificacionResumen };
}

/**
 * Construye las filas (mismo shape que produce `construirConcentrado`) a partir de las filas
 * crudas ya parseadas del Excel de correcciones (`loyalty-import.parser.js#parseCorreccionesXlsx`).
 * Función PURA, sin I/O — detecta tarjetas duplicadas DENTRO de este set (mismo criterio que
 * `construirConcentrado`: 2+ filas con la misma `normalizarTarjeta(tarjetaRaw)` se marcan ambas),
 * resuelve `contacto`/`sin_contacto` igual que una fila de clientes.csv, y resuelve
 * `nivelSugerido`/`nivel_asignado_por_default` vía `_resolverNivel` (reusada de
 * loyalty-import.service.js, no duplicada).
 *
 * `tarjetaRawPuntos` siempre es `null` y `correo` siempre `''` — el Excel de correcciones no
 * trae ninguno de los dos (no son necesarios para re-aplicar una fila ya revisada a mano).
 */
function construirFilasDesdeCorrecciones(filasCrudas) {
  const crudas = Array.isArray(filasCrudas) ? filasCrudas : [];

  const porTarjeta = new Map();
  for (const f of crudas) {
    const key = normalizarTarjeta(f.tarjetaRaw);
    if (!porTarjeta.has(key)) porTarjeta.set(key, []);
    porTarjeta.get(key).push(f);
  }

  return crudas.map((f) => {
    const key = normalizarTarjeta(f.tarjetaRaw);
    const esDuplicado = (porTarjeta.get(key) ?? []).length > 1;

    const celular = f.celular ? String(f.celular).trim() : '';
    const telefono = f.telefono ? String(f.telefono).trim() : '';
    const contacto = celular || telefono;

    const advertencias = [];
    const info = [];
    if (!contacto) advertencias.push('sin_contacto');
    if (esDuplicado) advertencias.push('tarjeta_duplicada_en_clientes');

    const { nivelSugerido, asignadoPorDefault } = _resolverNivel(f.categoriaOriginal);
    if (asignadoPorDefault) info.push('nivel_asignado_por_default');

    return {
      tarjetaNormalizada: key,
      tarjetaRawClientes: f.tarjetaRaw ?? '',
      tarjetaRawPuntos: null,
      nombre: f.nombre ?? '',
      celular: f.celular ?? '',
      telefono: f.telefono ?? '',
      contacto,
      saldo: f.saldo,
      categoriaOriginal: f.categoriaOriginal ?? '',
      correo: '',
      nivelSugerido,
      advertencias,
      info,
      // El Excel de correcciones no trae ninguna de las columnas de enriquecimiento (solo
      // Tarjeta/Nombre/Celular/Telefono/Categoria/Saldo, ver CORRECCIONES_COLUMNAS en
      // loyalty-import.parser.js) — se dejan vacías por consistencia de shape.
      apellidos: '', apodo: '', sexo: '', fechaNacimiento: '', especialidades: '', sucursal: '', calle: '',
      numExterior: '', numInterior: '', colonia: '', municipio: '', estado: '', cp: '', comentarios: '',
    };
  });
}

/**
 * `construirFilasDesdeCorrecciones` + `_clasificarFilasYaConstruidas` — dry-run del Excel de
 * correcciones, mismo criterio de "solo lectura" que `clasificar()`. No hay un `resumen` de tipo
 * concentrado acá (ese concepto es específico del cruce de dos archivos fuente) — se omite.
 */
async function clasificarCorrecciones(filasCrudas) {
  const filas = construirFilasDesdeCorrecciones(filasCrudas);
  return _clasificarFilasYaConstruidas(filas);
}

/** Texto final pendiente del dueño del negocio — mismo mensaje que
 * `scripts/migrate-lealtad-csv.js#construirMensaje` (duplicado a propósito:
 * src/ nunca debe depender de scripts/, ver bug real de Dockerfile que no
 * copiaba scripts/ a producción). Mantener ambos en sync si el texto cambia. */
function _construirMensaje({ nombre, nip, url }) {
  const urlTexto = url ? ` Consulta tu saldo en ${url}.` : '';
  return `Hola ${nombre}, tu nuevo número de tarjeta de lealtad es tu propio celular. Tu NIP es ${nip}.${urlTexto}`;
}

/**
 * Aplica UNA fila 'ok', 'pendiente_sin_telefono' o 'ya_migrada' (esta última solo backfillea
 * `tarjetaLegada` si falta, ver rama de abajo) dentro de una transacción YA abierta — lógica
 * compartida entre `aplicar()` (todo en un solo lote, para batches chicos / otros callers) y
 * `_correrAplicarJob()` (un lote de `BATCH_SIZE` filas por transacción, para la migración
 * completa en background). Nunca se llama con una fila bloqueante — ambos call sites las
 * filtran antes de llegar acá.
 */
async function _aplicarFila(t, fila, user, { url }) {
  const { usuarioId, usuarioNombre } = _userLabel(user);

  if (fila.categoria === 'ok') {
    // 1) Crea la tarjeta vía el servicio real (PR2) — genera/hashea el NIP,
    //    nunca lo persiste en texto plano (D6).
    const { card, nip } = await cardService.crearTarjeta(
      { telefono: fila.telefonoNormalizado, nombre: fila.nombre, nivel: fila.nivelSugerido },
      user,
      { transaction: t },
    );

    // 2) Bloquea la fila recién creada antes de aplicar el movimiento.
    const tarjetaBloqueada = await LoyaltyCard.findByPk(card.id, { transaction: t, lock: Transaction.LOCK.UPDATE });

    // 2.1) Persiste `tarjetaLegada` (gap real corregido 2026-10-22: el camino feliz nunca
    //      guardaba el número de Tarjeta del CSV legado — se usaba solo en memoria durante la
    //      clasificación y se descartaba, a diferencia de 'pendiente_sin_telefono', que SÍ lo
    //      persiste. Necesario para el match de clientes con Kore, ver kore-match/). Menos
    //      invasivo que extender `cardService.crearTarjeta` (su firma/contrato no cambia, lo
    //      siguen llamando otros callers como `completarRegistroPendiente`): un UPDATE puntual
    //      por id, antes de la única escritura de ledger permitida (mismo estilo de `LoyaltyCard.
    //      update(...)` usado por la rama 'ya_migrada' de abajo).
    // 2.2) Enriquecimiento (2026-10-23): mismo UPDATE puntual, mismo criterio que tarjetaLegada
    //      arriba — se persiste siempre (incluso si todos los campos vienen vacíos, para que una
    //      tarjeta creada hoy nunca dependa de una corrida de re-import futura).
    // Activo/Inactivo (2026-10-29): a diferencia del resto de campos de enriquecimiento (que solo
    // rellenan si están vacíos), `activa` viene directo de `fila.activaSugerida` — en 'ok' la
    // tarjeta recién se crea (siempre `activa:true` por default en `crearTarjeta`), así que esto
    // es la primera escritura real del valor, no un overwrite.
    const activaPatch = fila.activaSugerida !== undefined ? { activa: fila.activaSugerida } : {};
    if (fila.tarjetaRawClientes) {
      await LoyaltyCard.update(
        { tarjetaLegada: normalizarTarjeta(fila.tarjetaRawClientes), ..._datosEnriquecimiento(fila), ...activaPatch },
        { where: { id: card.id }, transaction: t },
      );
    } else {
      await LoyaltyCard.update(
        { ..._datosEnriquecimiento(fila), ...activaPatch },
        { where: { id: card.id }, transaction: t },
      );
    }

    // 3) Única escritura del ledger (D4, `_aplicarMovimiento`).
    await ledgerService._aplicarMovimiento(t, tarjetaBloqueada, {
      tipo: 'MIGRACION',
      puntos: fila.saldo,
      origen: 'migracion',
      usuarioId,
      usuarioNombre,
    });

    return {
      creada: {
        telefono: fila.telefonoNormalizado,
        nip,
        mensaje: _construirMensaje({ nombre: fila.nombre, nip, url }),
      },
    };
  }

  if (fila.categoria === 'ya_migrada') {
    // Re-import repetible (pedido explícito 2026-10-22, sirve además como backfill único para las
    // ~9,633 tarjetas ya migradas antes del fix de `tarjetaLegada` de arriba): volver a subir el
    // MISMO CSV rellena `tarjetaLegada` para una tarjeta que ya existe, SIN tocar puntos/nivel/
    // nombre y SIN crear un segundo movimiento de ledger (nunca se llama a `ledgerService`/
    // `cardService.crearTarjeta` acá).
    //
    // FIX 2026-10-23 (hallazgo CRÍTICO de revisión: confiabilidad + resiliencia): la versión
    // anterior ataba el backfill de los 13 campos de enriquecimiento al MISMO gate que
    // `tarjetaLegada` (`WHERE tarjetaLegada IS NULL`), así que para las tarjetas que ya tenían
    // `tarjetaLegada` cargada (el caso real en producción, backfilleado el 2026-10-22) el
    // enriquecimiento NUNCA se aplicaba — en silencio, sin error. Ahora cada gate es independiente:
    // `tarjetaLegada` se backfillea solo si falta (igual que antes); cada uno de los 13 campos de
    // enriquecimiento se backfillea campo por campo, solo si ESE campo está vacío en la tarjeta Y
    // la fila trae un valor — nunca se pisa un dato ya cargado (semántica confirmada por el
    // usuario). `withDatosPersonales` es necesario acá: `LoyaltyCard` excluye estos campos por
    // defaultScope (ver LoyaltyCard.js), así que sin ese scope `card[campo]` siempre sería
    // `undefined` y el backfill pisaría datos reales creyendo que estaban vacíos.
    const card = await LoyaltyCard.scope('withDatosPersonales').findOne({
      where: { telefono: fila.telefonoNormalizado },
      transaction: t,
      lock: Transaction.LOCK.UPDATE,
    });

    if (!card) {
      // No debería pasar — esta fila se clasificó como 'ya_migrada' porque `_clasificarFila`
      // encontró una tarjeta con este teléfono. Red de seguridad para un caso raro (borrado
      // concurrente entre clasificar y aplicar): se loguea para que no quede como éxito silencioso.
      logger.warn(`[LealtadImport] fila 'ya_migrada' sin tarjeta real para teléfono ${fila.telefonoNormalizado} — se omite sin backfillear nada.`);
      return { yaMigradaBackfill: false };
    }

    const updates = {};
    if (!card.tarjetaLegada && fila.tarjetaRawClientes) {
      updates.tarjetaLegada = normalizarTarjeta(fila.tarjetaRawClientes);
    }
    const datosFila = _datosEnriquecimiento(fila);
    for (const campo of CAMPOS_ENRIQUECIMIENTO) {
      if (!card[campo] && datosFila[campo]) {
        updates[campo] = datosFila[campo];
      }
    }
    // Activo/Inactivo (2026-10-29, pedido explícito del usuario): a diferencia de TODOS los
    // campos de arriba (que solo backfillean si están vacíos), `activa` se SOBRESCRIBE siempre
    // que la fila traiga `activaSugerida` — reporteClientesPorVendedor.xlsx es la fuente de
    // verdad de estatus en cada corrida, incluso si la tarjeta ya tenía un valor distinto (puede
    // pisar un cambio manual anterior del staff; decisión confirmada explícitamente). Las filas
    // de `construirFilasDesdeCorrecciones` (re-subida de Excel de bloqueantes) NUNCA traen
    // `activaSugerida` (ese flujo no vuelve a cruzar contra el reporte de vendedor) — en ese caso
    // no se toca `activa` para nada, nunca se interpreta `undefined` como `false`.
    if (fila.activaSugerida !== undefined && card.activa !== fila.activaSugerida) {
      updates.activa = fila.activaSugerida;
    }

    if (Object.keys(updates).length === 0) {
      // Ni tarjetaLegada ni ningún campo de enriquecimiento tenían algo para completar — resultado
      // legítimo (la tarjeta ya estaba completa), no un fallo, pero se distingue explícitamente del
      // caso "sí se backfilleó algo" en el conteo que se reporta (antes era un UPDATE ciego sin
      // forma de saber si había tocado 0 filas).
      return { yaMigradaBackfill: false };
    }

    await card.update(updates, { transaction: t });
    return { yaMigradaBackfill: true };
  }

  if (fila.categoria === 'pendiente_sin_telefono') {
    // Dedup (2026-10-23): aplicar()/el job pueden correr más de una vez sobre el mismo CSV
    // (re-import idempotente, ver docstring del archivo) — sin esto, cada corrida duplicaría el
    // pendiente para la misma tarjeta legada.
    const yaExiste = fila.tarjetaRawClientes
      ? await LoyaltyPendingEnrollment.findOne({ where: { tarjetaLegada: fila.tarjetaRawClientes }, transaction: t })
      : null;
    if (yaExiste) {
      return { pendiente: false };
    }

    await LoyaltyPendingEnrollment.create({
      nombre:          fila.nombre,
      correo:          fila.correo || null,
      tarjetaLegada:   fila.tarjetaRawClientes,
      saldoLegado:     fila.saldo,
      nivelSugerido:   fila.nivelSugerido,
      estado:          'pendiente',
      motivo:          'sin_telefono',
      createdById:     usuarioId,
      createdByNombre: usuarioNombre,
    }, { transaction: t });
    return { pendiente: true };
  }

  if (fila.categoria === 'telefono_duplicado_bloqueante') {
    // Dedup: mismo criterio que la rama 'pendiente_sin_telefono' de arriba — aplicar()/el job
    // pueden correr más de una vez sobre el mismo CSV (re-import idempotente), sin esto cada
    // corrida duplicaría el pendiente.
    const tarjetaLegadaNorm = fila.tarjetaRawClientes ? normalizarTarjeta(fila.tarjetaRawClientes) : null;
    const yaExiste = tarjetaLegadaNorm
      ? await LoyaltyPendingEnrollment.findOne({ where: { tarjetaLegada: tarjetaLegadaNorm }, transaction: t })
      : null;
    if (yaExiste) {
      return { pendiente: false };
    }

    await LoyaltyPendingEnrollment.create({
      nombre:            fila.nombre,
      correo:            fila.correo || null,
      tarjetaLegada:     tarjetaLegadaNorm,
      saldoLegado:       fila.saldo,
      nivelSugerido:     fila.nivelSugerido,
      estado:            'pendiente',
      motivo:            'telefono_duplicado',
      grupoId:           fila.telefonoNormalizado,
      telefonoCandidato: fila.contacto,
      createdById:       usuarioId,
      createdByNombre:   usuarioNombre,
    }, { transaction: t });
    return { pendiente: true };
  }

  return null;
}

/**
 * Aplica la migración: clasifica primero y corre TODO en una sola transacción Postgres. Ya NO
 * aborta por filas bloqueantes (decisión de negocio 2026-10-19, ver docstring del archivo) —
 * procesa 'ok'/'pendiente_sin_telefono' igual que siempre y simplemente EXCLUYE (sin escribir)
 * las filas duplicado/invalido/conflicto_bloqueante; su conteo se reporta en
 * `excluidasBloqueantes` para que el admin las corrija aparte (ver `clasificarCorrecciones`).
 *
 * Mantenido tal cual (sin más cambios de comportamiento) para callers/tests que ya dependen de
 * una llamada 100% síncrona — p.ej. lotes chicos manuales. La migración completa real (miles de
 * filas) debe usar `iniciarAplicarJob` en su lugar (ver comentario sobre bcrypt/timeouts más abajo).
 */
async function aplicar({ clientesRows, puntosRows }, user, { url } = {}) {
  const { filas, clasificacionResumen } = await clasificar({ clientesRows, puntosRows });

  const dataset = [];
  let pendientesCreadas = 0;
  let omitidasYaMigradas = 0;
  let yaMigradaBackfillAplicado = 0;
  // Descarte automático por duplicidad de teléfono (2026-10-29): no necesita lookup/escritura en
  // DB (ver _clasificarFilasYaConstruidas), así que se arma directo acá, FUERA de la transacción
  // — no tiene sentido abrir una transacción Postgres para una fila que nunca se va a tocar.
  const descartadosPorDuplicidad = filas
    .filter((f) => f.categoria === 'descartado_duplicado_sin_puntos')
    .map(_descartadoDesdeFila);

  await sequelize.transaction(async (t) => {
    for (const fila of filas) {
      if (fila.categoria === 'ok' || fila.categoria === 'pendiente_sin_telefono' || fila.categoria === 'telefono_duplicado_bloqueante') {
        const r = await _aplicarFila(t, fila, user, { url });
        if (r?.creada) dataset.push(r.creada);
        if (r?.pendiente) pendientesCreadas += 1;
      } else if (fila.categoria === 'ya_migrada') {
        // Re-import repetible (ver _aplicarFila): backfillea tarjetaLegada/enriquecimiento si
        // falta, nunca crea ledger/pendiente ni toca puntos/nivel/nombre.
        const r = await _aplicarFila(t, fila, user, { url });
        if (r?.yaMigradaBackfill) yaMigradaBackfillAplicado += 1;
        omitidasYaMigradas += 1;
      }
      // duplicado_bloqueante / invalido_bloqueante / conflicto_bloqueante: excluidas a propósito,
      // ni se escriben ni abortan el resto de la corrida (ver excluidasBloqueantes abajo).
      // descartado_duplicado_sin_puntos: ya resuelta arriba, fuera de la transacción.
    }
  });

  return {
    clasificacionResumen,
    creadas: dataset,
    pendientesCreadas,
    omitidasYaMigradas,
    // Hallazgo de resiliencia 2026-10-23: antes el backfill de 'ya_migrada' era un UPDATE ciego
    // (nunca se sabía si había tocado alguna fila). Este contador distingue, de las
    // `omitidasYaMigradas`, cuántas tuvieron al menos un campo (tarjetaLegada o enriquecimiento)
    // realmente completado — si queda muy por debajo de lo esperado, es señal de un desajuste real
    // (ej. formato de teléfono) en vez de asumir éxito silencioso.
    yaMigradaBackfillAplicado,
    excluidasBloqueantes: _totalBloqueantes(clasificacionResumen),
    descartadosPorDuplicidad,
    erroresFilas: [], // aplicar() usa una sola transacción compartida (ver docstring) — una fila
                       // que falla aborta esa transacción entera, no hay nada que listar acá. Solo
                       // _correrAplicarJob (per-fila) puede producir entradas reales.
  };
}

// ── Aplicar en BACKGROUND, por lotes (pedido explícito del usuario, 2026-10-19) ─────────────
// Incidente evitado: generar UN NIP (nip.util.js#hashNip, bcryptjs cost 10) mide ~138ms. Un
// lote real de migración (~9,633 filas 'ok') son ~22 minutos de puro bcrypt, TODO adentro de
// una sola transacción Postgres y una sola request HTTP — eso revienta cualquier timeout
// (proxy/DB/cliente) y hace rollback de TODO el trabajo ya hecho.
//
// Mismo patrón YA establecido en este proyecto para trabajos largos (ver
// netpay-reporte.service.js#iniciarCargaReporteJob / Sync ERP-Kore en erp.routes.js): la ruta
// responde 202+jobId de inmediato (después de clasificar, que es rápido y de solo lectura — las
// filas bloqueantes quedan excluidas del job al instante, sin escribir nada por ellas, pero ya NO
// abortan el resto, ver docstring del archivo), el trabajo pesado corre después sin bloquear la
// respuesta, partido en lotes de `BATCH_SIZE` filas — cada lote es SU PROPIA transacción Postgres
// — y el progreso/resultado final se emite por socket a quien lo disparó (emitToUser, acción
// personal de quien corre la migración).
const LEALTAD_APPLY_JOBS = new Map(); // jobId -> { status, userId, procesados, total, loteActual, totalLotes, resultado?, error? }
const LEALTAD_APPLY_JOB_TTL = 2 * 60 * 60 * 1000; // 2h — mismo criterio que NETPAY_UPLOAD_JOB_TTL

// ~150-200ms/fila (dominado por el bcrypt del NIP, cost 10 ≈138ms medidos) → cada lote de 200
// filas tarda ~30-40s de transacción: lejos de cualquier timeout razonable de Postgres/proxy,
// y muy por debajo de los ~22min que tomaría el batch completo en una sola transacción.
const BATCH_SIZE = 200;

function _nuevoJobIdAplicar() {
  return `lealtad-aplicar-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function _chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

/** Recalcula el `clasificacionResumen` a partir de un array de filas YA clasificadas — usado por
 * `_iniciarJobDesdeFilasClasificadas` para no tener que recibir el resumen como parámetro
 * aparte (evita que caller y resumen se desincronicen si alguna vez se filtra `filas` a mano). */
function _resumenDesdeFilasClasificadas(filas) {
  const clasificacionResumen = _clasificacionResumenVacio();
  for (const fila of filas) {
    clasificacionResumen[CAMPO_POR_CATEGORIA[fila.categoria]] += 1;
  }
  return clasificacionResumen;
}

/**
 * Dado un array de filas YA clasificadas (de `clasificar()` o `clasificarCorrecciones()`), parte
 * las accionables ('ok' + 'pendiente_sin_telefono') en lotes de `BATCH_SIZE`, crea el job, dispara
 * `_correrAplicarJob` SIN esperarlo (a propósito — es justo lo que permite responder 202 de
 * inmediato) y devuelve `{jobId,total,totalLotes}`. Extraído de `iniciarAplicarJob` para que
 * `iniciarAplicarCorreccionesJob` reuse la MISMA máquina de batching/progreso/sockets.
 *
 * Las filas bloqueantes NO son accionables — nunca entran a un lote, solo se cuentan
 * (`excluidasBloqueantes` en el resultado final). 'ya_migrada' SÍ es accionable desde
 * 2026-10-22 (re-import repetible, ver _aplicarFila) — pasa por `_aplicarFila` para backfillear
 * `tarjetaLegada`/enriquecimiento si falta, aunque nunca crea nada; su conteo total sigue viniendo
 * de `clasificacionResumen.yaMigrada`, pero `yaMigradaBackfillAplicado` en el resultado final
 * distingue cuántas de esas realmente tuvieron algún campo completado.
 */
function _iniciarJobDesdeFilasClasificadas(filas, user, { url } = {}) {
  const clasificacionResumen = _resumenDesdeFilasClasificadas(filas);
  const actionable = filas.filter((f) => (
    f.categoria === 'ok' || f.categoria === 'pendiente_sin_telefono' || f.categoria === 'ya_migrada'
      || f.categoria === 'telefono_duplicado_bloqueante'
  ));
  // Descarte automático por duplicidad de teléfono (2026-10-29): nunca es accionable (no entra a
  // ningún lote, no necesita lookup/escritura en DB) — se calcula directo acá y viaja aparte hasta
  // el resultado final del job (ver _correrAplicarJob).
  const descartadosPorDuplicidad = filas
    .filter((f) => f.categoria === 'descartado_duplicado_sin_puntos')
    .map(_descartadoDesdeFila);
  const lotes = _chunk(actionable, BATCH_SIZE);

  const jobId = _nuevoJobIdAplicar();
  LEALTAD_APPLY_JOBS.set(jobId, {
    status: 'running',
    userId: user?._id ?? null,
    procesados: 0,
    total: actionable.length,
    loteActual: 0,
    totalLotes: lotes.length,
  });

  // Sin await a propósito — mismo patrón que netpay-reporte.service.js#iniciarCargaReporteJob:
  // es justo lo que permite responder 202 de inmediato sin bloquear la request mientras los
  // lotes corren.
  _correrAplicarJob(jobId, lotes, user, { url }, clasificacionResumen, descartadosPorDuplicidad);

  return { jobId, total: actionable.length, totalLotes: lotes.length };
}

/**
 * Arranca la migración completa en background: clasifica (síncrono, rápido) y delega en
 * `_iniciarJobDesdeFilasClasificadas`. Ya NO aborta por filas bloqueantes (decisión de negocio
 * 2026-10-19, ver docstring del archivo) — simplemente quedan excluidas del job.
 */
async function iniciarAplicarJob({ clientesRows, puntosRows }, user, { url } = {}) {
  const { filas } = await clasificar({ clientesRows, puntosRows });
  return _iniciarJobDesdeFilasClasificadas(filas, user, { url });
}

/**
 * Arranca en background la aplicación del Excel de correcciones ya subido/parseado: clasifica
 * (`clasificarCorrecciones`) y delega en la MISMA `_iniciarJobDesdeFilasClasificadas` que usa la
 * migración principal — mismo batching, mismo progreso, mismos eventos de socket
 * (`lealtad-import:apply:progress|done|error`, genéricos por `jobId`, el frontend ya los filtra).
 */
async function iniciarAplicarCorreccionesJob(filasCrudas, user, { url } = {}) {
  const { filas } = await clasificarCorrecciones(filasCrudas);
  return _iniciarJobDesdeFilasClasificadas(filas, user, { url });
}

/**
 * Corre el job de principio a fin SIN detenerse nunca por una fila individual que falle al
 * escribir (decisión de negocio 2026-10-19: "que siga con los demás lotes automáticamente, sin
 * esperar confirmación del usuario"). Cada fila corre en SU PROPIA transacción Postgres — no una
 * compartida por lote — porque si una sentencia falla dentro de una transacción, Postgres la
 * marca abortada por completo (ninguna sentencia más corre hasta el ROLLBACK): compartir la
 * transacción entre las 200 filas de un lote habría tumbado a sus vecinas sanas junto con la que
 * realmente falló.
 *
 * Una fila que falla se registra en `erroresFilas` (visible en el resultado final) y el job
 * sigue con la siguiente — nunca pasa a `status: 'error'` por esto. Ese estado queda reservado
 * para algo catastrófico fuera del try/catch por fila (no debería ocurrir en la práctica).
 */
async function _correrAplicarJob(jobId, lotes, user, { url }, clasificacionResumen, descartadosPorDuplicidad = []) {
  const total = lotes.reduce((acc, lote) => acc + lote.length, 0);
  const dataset = [];
  const erroresFilas = [];
  let pendientesCreadas = 0;
  let yaMigradaBackfillAplicado = 0;
  let procesados = 0;
  let loteActual = 0;

  try {
    for (const lote of lotes) {
      for (const fila of lote) {
        try {
          // eslint-disable-next-line no-await-in-loop -- secuencial a propósito (mismo criterio
          // que netpay-reporte.service.js); cada fila es su propia transacción independiente.
          await sequelize.transaction(async (t) => {
            const r = await _aplicarFila(t, fila, user, { url });
            if (r?.creada) dataset.push(r.creada);
            if (r?.pendiente) pendientesCreadas += 1;
            if (r?.yaMigradaBackfill) yaMigradaBackfillAplicado += 1;
          });
        } catch (err) {
          // Caso real conocido (2026-10-19): dos filas con Tarjeta legada distinta pero el MISMO
          // teléfono — `clasificar()` ya detecta esto de antemano como
          // 'telefono_duplicado_bloqueante' (ver _clasificarFilasYaConstruidas), así que esto es
          // una red de seguridad para cualquier otro error inesperado de escritura. NUNCA detiene
          // el job — se registra y se sigue con la próxima fila.
          erroresFilas.push({
            telefono: fila.telefonoNormalizado ?? fila.contacto ?? null,
            nombre: fila.nombre,
            error: err.message,
          });
          logger.error(`[LealtadImport] job ${jobId}: fila "${fila.nombre}" (${fila.telefonoNormalizado ?? 's/d'}) falló, se omite y se continúa: ${err.message}`);
        }

        procesados += 1;
      }

      loteActual += 1;
      const job = LEALTAD_APPLY_JOBS.get(jobId);
      if (job) { job.procesados = procesados; job.loteActual = loteActual; }
      emitToUser(user?._id, 'lealtad-import:apply:progress', {
        jobId, procesados, total, loteActual, totalLotes: lotes.length,
        pct: total > 0 ? Math.round((procesados / total) * 100) : 100,
      });
    }

    const resultado = {
      clasificacionResumen,
      creadas: dataset,
      pendientesCreadas,
      omitidasYaMigradas: clasificacionResumen.yaMigrada,
      yaMigradaBackfillAplicado,
      excluidasBloqueantes: _totalBloqueantes(clasificacionResumen),
      descartadosPorDuplicidad,
      erroresFilas,
    };
    LEALTAD_APPLY_JOBS.set(jobId, {
      status: 'done',
      userId: user?._id ?? null,
      procesados: total,
      total,
      loteActual: lotes.length,
      totalLotes: lotes.length,
      resultado,
    });
    emitToUser(user?._id, 'lealtad-import:apply:done', { jobId, ...resultado });
  } catch (err) {
    // Solo llega acá algo catastrófico FUERA del try/catch por fila de arriba (ej. un bug real
    // en el propio runner) — no debería pasar en la práctica, pero se deja como red de
    // seguridad final en vez de dejar el job colgado en 'running' para siempre.
    const mensaje = `${err.message} Progreso parcial conservado: ${procesados}/${total} fila(s) ya `
      + `procesadas en ${loteActual}/${lotes.length} lote(s). Es seguro volver a correr la `
      + 'importación completa — las filas ya aplicadas se detectarán como "ya_migrada" y se omitirán.';
    LEALTAD_APPLY_JOBS.set(jobId, {
      status: 'error',
      userId: user?._id ?? null,
      procesados,
      total,
      loteActual,
      totalLotes: lotes.length,
      error: mensaje,
    });
    emitToUser(user?._id, 'lealtad-import:apply:error', {
      jobId, procesados, total, loteActual, totalLotes: lotes.length, error: mensaje,
    });
    logger.error(`[LealtadImport] job de aplicar ${jobId} falló de forma inesperada en el lote ${loteActual + 1}/${lotes.length}: ${err.message}`);
  } finally {
    setTimeout(() => LEALTAD_APPLY_JOBS.delete(jobId), LEALTAD_APPLY_JOB_TTL);
  }
}

// Fallback de recuperación tras un reload de página — mismo criterio que
// netpay-reporte.service.js#obtenerEstadoJobCarga (GET /netpay/reporte/upload-job/:jobId): el
// socket es la vía normal de enterarse del progreso, esto es solo para cuando alguien recarga a
// mitad del job y pierde la suscripción en memoria. SÍ hay chequeo de dueño (userId).
function obtenerEstadoAplicarJob(jobId, userId) {
  const job = LEALTAD_APPLY_JOBS.get(jobId);
  if (!job || String(job.userId) !== String(userId)) return null;
  const { userId: _userId, ...estado } = job;
  return estado;
}

/**
 * Orden (2026-10-23, pedido explícito del usuario): `saldoLegado` DESC primero — un cliente
 * legado con puntos reales esperando es más urgente de resolver (conseguirle el teléfono) que uno
 * sin saldo, independientemente de cuándo quedó pendiente. `createdAt` ASC como criterio
 * secundario (el de siempre) para los que empatan en saldo, incluido el caso común de saldo 0.
 *
 * Agrupado por `grupoId` (2026-10-23, pedido explícito del usuario): 2+ filas de motivo
 * 'telefono_duplicado' que comparten el mismo teléfono se devuelven juntas en un mismo grupo, para
 * que el admin las resuelva viendo el conflicto completo en vez de encontrarlas sueltas en páginas
 * distintas. Las filas de motivo 'sin_telefono' (grupoId null) quedan cada una en su propio grupo
 * de 1 — se paginan por GRUPO, no por fila, así un par en conflicto nunca queda partido entre dos
 * páginas. El volumen real hoy (~1.800 filas pendientes) es perfectamente razonable de traer
 * completo a memoria para un panel admin (no es un hot path).
 */
async function listarPendientes({ page = 1, limit = 50, exportar = false, search } = {}) {
  const p = Math.max(1, Number(page) || 1);
  const l = Math.min(100, Number(limit) || 50);

  // Buscador (2026-10-29, pedido explícito del usuario) — mismo criterio que
  // loyalty-card.service.js#listarPendientesRevisionKore: prefijo de teléfono O ILIKE de nombre.
  // Este modelo no tiene columna `telefono` propia (ver LoyaltyPendingEnrollment.js) — el único
  // candidato disponible es `telefonoCandidato` (solo se llena para motivo=telefono_duplicado).
  const where = { estado: 'pendiente' };
  if (search) {
    const s = String(search).trim();
    where[Op.and] = [
      {
        [Op.or]: [
          { telefonoCandidato: { [Op.like]: `${s}%` } },
          { nombre:            { [Op.iLike]: `%${s}%` } },
        ],
      },
    ];
  }

  const rows = await LoyaltyPendingEnrollment.findAll({
    where,
    order: [['saldoLegado', 'DESC'], ['createdAt', 'ASC']],
  });

  const gruposMap = new Map();
  for (const row of rows) {
    const key = row.grupoId || `solo-${row.id}`;
    if (!gruposMap.has(key)) gruposMap.set(key, []);
    gruposMap.get(key).push(row);
  }

  const grupos = Array.from(gruposMap.values()).map((items) => ({
    grupoId: items[0].grupoId, // null para los grupos de 1 (motivo sin_telefono)
    saldoMax: Math.max(...items.map((i) => Number(i.saldoLegado))),
    items,
  }));
  grupos.sort((a, b) => b.saldoMax - a.saldoMax || a.items[0].createdAt - b.items[0].createdAt);

  const total = grupos.length;
  const totalRegistros = rows.length;

  // Exportación a Excel (2026-10-26, pedido explícito del usuario): trae TODOS los grupos de una
  // sola vez, sin el corte de página ni el tope de 100 por página — el admin necesita el padrón
  // completo para compartirlo y resolverlo a mano, no una página a la vez. Los `rows` ya están
  // completos en memoria desde el `findAll` de arriba (mismo criterio de volumen documentado en
  // el comentario original de esta función), así que no hace falta una query aparte.
  if (exportar) {
    return { total, totalRegistros, page: 1, limit: total, pages: 1, grupos };
  }

  const pageSlice = grupos.slice((p - 1) * l, (p - 1) * l + l);

  return { total, totalRegistros, page: p, limit: l, pages: Math.ceil(total / l) || 1, grupos: pageSlice };
}

/**
 * Completa un registro pendiente: crea la LoyaltyCard real con el teléfono
 * que el admin consiguió, acredita `saldoLegado` (MIGRACION) y marca el
 * pendiente como 'completado'. Todo en UNA transacción, con el pendiente
 * bloqueado (mismo patrón que `loyalty-card.service.js#cambiarNivel`).
 */
async function completarRegistroPendiente(pendienteId, { telefono }, user) {
  return sequelize.transaction(async (t) => {
    const pendiente = await LoyaltyPendingEnrollment.findByPk(pendienteId, {
      transaction: t,
      lock: Transaction.LOCK.UPDATE,
    });
    if (!pendiente) throw new NotFoundError('Registro pendiente');
    if (pendiente.estado !== 'pendiente') {
      throw new BadRequestError('Este registro pendiente ya fue completado.');
    }

    const { card, nip } = await cardService.crearTarjeta(
      { telefono, nombre: pendiente.nombre, nivel: pendiente.nivelSugerido },
      user,
      { transaction: t },
    );

    const tarjetaBloqueada = await LoyaltyCard.findByPk(card.id, { transaction: t, lock: Transaction.LOCK.UPDATE });
    const { usuarioId, usuarioNombre } = _userLabel(user);
    await ledgerService._aplicarMovimiento(t, tarjetaBloqueada, {
      tipo: 'MIGRACION',
      puntos: pendiente.saldoLegado,
      origen: 'migracion',
      usuarioId,
      usuarioNombre,
    });

    await pendiente.update({
      estado: 'completado',
      cardIdResultante: card.id,
      resueltoPorId: usuarioId,
      resueltoPorNombre: usuarioNombre,
      resueltoAt: new Date(),
    }, { transaction: t });

    return { card, nip };
  });
}

/**
 * Rechaza UN registro pendiente — decisión de negocio 2026-10-19: el admin puede decidir que un
 * cliente legado sin teléfono ya no se va a migrar (contactado sin éxito, ya no es cliente,
 * etc.). Nunca se borra la fila (trazabilidad vía resueltoPorId/resueltoPorNombre/resueltoAt) —
 * solo cambia `estado`, que `listarPendientes` ya filtra, así que desaparece solo de la lista.
 */
async function rechazarRegistroPendiente(pendienteId, user) {
  const pendiente = await LoyaltyPendingEnrollment.findByPk(pendienteId);
  if (!pendiente) throw new NotFoundError('Registro pendiente');
  if (pendiente.estado !== 'pendiente') {
    throw new BadRequestError('Este registro pendiente ya fue resuelto (completado o rechazado).');
  }

  const { usuarioId, usuarioNombre } = _userLabel(user);
  await pendiente.update({
    estado: 'rechazado',
    resueltoPorId: usuarioId,
    resueltoPorNombre: usuarioNombre,
    resueltoAt: new Date(),
  });

  return pendiente;
}

/**
 * Rechaza TODOS los registros actualmente 'pendiente' de una sola vez (botón "Rechazar todos") —
 * un solo UPDATE masivo, no un job: a diferencia de `aplicar`, acá no se crea ninguna tarjeta ni
 * se hashea ningún NIP (lo más caro de esta tabla), así que miles de filas se resuelven en una
 * sola consulta, sin necesidad de lotes ni progreso en background.
 */
async function rechazarTodosPendientes(user) {
  const { usuarioId, usuarioNombre } = _userLabel(user);
  const [rechazados] = await LoyaltyPendingEnrollment.update({
    estado: 'rechazado',
    resueltoPorId: usuarioId,
    resueltoPorNombre: usuarioNombre,
    resueltoAt: new Date(),
  }, { where: { estado: 'pendiente' } });

  return { rechazados };
}

module.exports = {
  clasificar,
  aplicar,
  iniciarAplicarJob,
  obtenerEstadoAplicarJob,
  _correrAplicarJob, // exportado solo para tests (mismo criterio que netpay-reporte.service.js#_correrCargaReporteJob)
  listarPendientes,
  completarRegistroPendiente,
  rechazarRegistroPendiente,
  rechazarTodosPendientes,
  // ── Correcciones (re-subida de filas bloqueantes corregidas a mano) ──────
  construirFilasDesdeCorrecciones,
  clasificarCorrecciones,
  iniciarAplicarCorreccionesJob,
};
