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
 *                                 (re-correr `aplicar` es idempotente).
 *   6. ok                       — se crea la tarjeta + movimiento MIGRACION.
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

const { Transaction } = require('sequelize');
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
};

function _clasificacionResumenVacio() {
  return {
    ok: 0, pendienteSinTelefono: 0, yaMigrada: 0,
    duplicadoBloqueante: 0, invalidoBloqueante: 0, conflictoBloqueante: 0,
    telefonoDuplicadoBloqueante: 0,
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
 * Primera pasada (sin DB): normaliza el teléfono de cada fila candidata y cuenta cuántas veces
 * se repite DENTRO de este mismo set — ver `_clasificarFila` para por qué (caso real 2026-10-19,
 * dos Tarjetas legadas distintas con el mismo teléfono).
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
  const conteoTelefonos = new Map();
  for (const tel of telefonosNormalizados) {
    if (!tel) continue;
    conteoTelefonos.set(tel, (conteoTelefonos.get(tel) ?? 0) + 1);
  }

  const clasificacionResumen = _clasificacionResumenVacio();

  const filasClasificadas = [];
  for (let i = 0; i < filas.length; i++) {
    const fila = filas[i];
    const esTelefonoDuplicadoEnSet = (conteoTelefonos.get(telefonosNormalizados[i]) ?? 0) > 1;
    // eslint-disable-next-line no-await-in-loop -- clasificación secuencial de solo lectura, cada
    // fila puede depender de una consulta a la DB (findOne por teléfono).
    const { categoria, telefonoNormalizado } = await _clasificarFila(fila, esTelefonoDuplicadoEnSet);
    clasificacionResumen[CAMPO_POR_CATEGORIA[categoria]] += 1;
    filasClasificadas.push({ ...fila, categoria, telefonoNormalizado });
  }

  return { filas: filasClasificadas, clasificacionResumen };
}

/** Suma las cuatro categorías bloqueantes — mismo criterio usado antes por el
 * `_errorBloqueantes` ya retirado (decisión de negocio 2026-10-19: ya no abortan,
 * solo se cuentan/excluyen, ver docstring del archivo). */
function _totalBloqueantes(clasificacionResumen) {
  return clasificacionResumen.duplicadoBloqueante
    + clasificacionResumen.invalidoBloqueante
    + clasificacionResumen.conflictoBloqueante
    + clasificacionResumen.telefonoDuplicadoBloqueante;
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
 * Aplica UNA fila 'ok' o 'pendiente_sin_telefono' dentro de una transacción YA
 * abierta — lógica compartida entre `aplicar()` (todo en un solo lote, para
 * batches chicos / otros callers) y `_correrAplicarJob()` (un lote de
 * `BATCH_SIZE` filas por transacción, para la migración completa en background).
 * Nunca se llama con una fila bloqueante ni 'ya_migrada' — ambos call sites las
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

  if (fila.categoria === 'pendiente_sin_telefono') {
    await LoyaltyPendingEnrollment.create({
      nombre:          fila.nombre,
      correo:          fila.correo || null,
      tarjetaLegada:   fila.tarjetaRawClientes,
      saldoLegado:     fila.saldo,
      nivelSugerido:   fila.nivelSugerido,
      estado:          'pendiente',
      createdById:     usuarioId,
      createdByNombre: usuarioNombre,
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

  await sequelize.transaction(async (t) => {
    for (const fila of filas) {
      if (fila.categoria === 'ok' || fila.categoria === 'pendiente_sin_telefono') {
        const r = await _aplicarFila(t, fila, user, { url });
        if (r?.creada) dataset.push(r.creada);
        if (r?.pendiente) pendientesCreadas += 1;
      } else if (fila.categoria === 'ya_migrada') {
        omitidasYaMigradas += 1;
      }
      // duplicado_bloqueante / invalido_bloqueante / conflicto_bloqueante: excluidas a propósito,
      // ni se escriben ni abortan el resto de la corrida (ver excluidasBloqueantes abajo).
    }
  });

  return {
    clasificacionResumen,
    creadas: dataset,
    pendientesCreadas,
    omitidasYaMigradas,
    excluidasBloqueantes: _totalBloqueantes(clasificacionResumen),
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
 * Las filas bloqueantes/'ya_migrada' NO son accionables — nunca entran a un lote, solo se
 * cuentan (`clasificacionResumen.yaMigrada`/`excluidasBloqueantes` en el resultado final).
 */
function _iniciarJobDesdeFilasClasificadas(filas, user, { url } = {}) {
  const clasificacionResumen = _resumenDesdeFilasClasificadas(filas);
  const actionable = filas.filter((f) => f.categoria === 'ok' || f.categoria === 'pendiente_sin_telefono');
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
  _correrAplicarJob(jobId, lotes, user, { url }, clasificacionResumen);

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
async function _correrAplicarJob(jobId, lotes, user, { url }, clasificacionResumen) {
  const total = lotes.reduce((acc, lote) => acc + lote.length, 0);
  const dataset = [];
  const erroresFilas = [];
  let pendientesCreadas = 0;
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
      excluidasBloqueantes: _totalBloqueantes(clasificacionResumen),
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

/** Mismo criterio de paginación que `loyalty-card.service.js#listar`. */
async function listarPendientes({ page = 1, limit = 50 } = {}) {
  const p = Math.max(1, Number(page) || 1);
  const l = Math.min(100, Number(limit) || 50);

  const { count, rows } = await LoyaltyPendingEnrollment.findAndCountAll({
    where:  { estado: 'pendiente' },
    order:  [['createdAt', 'ASC']],
    limit:  l,
    offset: (p - 1) * l,
  });

  return { total: count, page: p, limit: l, pages: Math.ceil(count / l), pendientes: rows };
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
