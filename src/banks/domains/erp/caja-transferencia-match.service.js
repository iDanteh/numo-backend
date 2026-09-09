'use strict';

// caja-transferencia-match.service.js — Fase C del proceso de matching de
// transferencias entre cajas (ver plan acordado con el usuario 2026-09-01).
// Encuentra candidatos (BankMovement) para una CajaTransferencia. NO escribe
// erpLinks ni marca nada como 'identificado' — confirmar un match (con
// rastreabilidad de quién lo autorizó) es Fase D (caja-transferencia-confirm.service.js).
//
// 2026-09-02: se eliminó detectarHuerfanas() (marcaba 'huerfana' una transferencia
// pendiente sin candidatos tras cerrar su ventana) — pedido explícito del usuario,
// van a reemplazar ese mecanismo por algo distinto todavía no definido.
//
// Candidato = BankMovement con categoria "Depósito en efectivo", sin erpLinks Y con
// status !== 'identificado' (nunca se identificó por otra vía). Sin acotar por banco
// (confirmado con el usuario: un mismo depósito puede caer en cualquiera de los 4
// bancos manejados — no hay forma confiable de acotar).
//
// CORRECCIÓN 2026-09-03 (bug real reportado por el usuario): filtrar solo por erpLinks
// vacío NO alcanza — un movimiento puede quedar 'identificado' sin ningún erpLink, vía
// `ficha` (folio físico que carga un contador, aplicarLogicaErp lo fuerza a 'identificado'
// aunque erpLinks siga vacío — ver bank.service.js). La bandeja estaba sugiriendo como
// candidatos depósitos que un contador YA había resuelto a mano. Se agrega el filtro de
// status explícito. (Reemplaza la decisión previa de "ignorar status" — esa hablaba de
// que una regla de categorización puede pisar el status mostrado, no de identificaciones
// reales ya hechas por un contador.)
//
// CORRECCIÓN 2026-09-08 (pedido explícito del usuario, caso real de producción): se
// quitó la búsqueda de combinaciones de 2 movimientos cuya suma matchea el monto. Caso
// real que lo motivó: el depósito exacto de una transferencia ya estaba 'identificado'
// (por eso quedaba excluido del pool), y el fallback de pares encontró 2 movimientos NO
// relacionados (de bancos y fechas distintos) cuya suma coincidía por pura casualidad
// numérica con el monto buscado — falso positivo, no correspondían a ninguna transferencia
// real. El matching por pares no tiene ninguna señal de correlación más allá de "la suma
// cierra dentro de tolerancia y ambos caen en la ventana de fechas", así que con
// suficientes movimientos elegibles las coincidencias son inevitables. Por ahora se
// limita a 1:1 exacto; retomar combinaciones (con más criterios de correlación) cuando
// se decida escalar este panel — no reabrir sin decisión explícita del usuario.
//
// CORRECCIÓN 2026-09-08 (bug real, mismo día, reportado por el usuario): buscarCandidatos()
// usaba `.find()` para el match 1:1 — con 2+ depósitos elegibles que empatan EXACTO en
// monto (caso real: 3 depósitos de $1,200 para una transferencia de $1,200), `.find()`
// devuelve SOLO el primero según el orden natural de Mongo (sin ningún `.sort()`, no hay
// ninguna señal real de que sea el correcto) — los otros 2 candidatos igual de válidos
// quedaban invisibles, silenciosamente. Se cambió a `.filter()`: TODAS las coincidencias
// exactas se devuelven, cada una como su propio grupo de 1 movimiento — el modelo
// `candidatos: BankMovement[][]` y el frontend YA estaban preparados para esto ("puede
// haber más de un grupo si hay ambigüedad"), el bug era que este código nunca llegaba a
// producir más de un grupo. Ahora un humano elige a mano cuál es el correcto (fecha/banco
// visibles en cada tarjeta), en vez de que el sistema adivine.
//
// Bug real 2026-09-01 (reportado por el usuario, TODAS las transferencias mostraban
// "Sin candidatos"): `categoria` es texto libre que define quien arma las reglas de
// categorización (Reglas, dentro de Bancos) — en el ambiente real la regla se llama
// "DEPOSITO EN EFECTIVO" (mayúsculas, sin acento), no "Depósito en efectivo" como se
// comparaba antes con `===` estricto. La comparación ahora normaliza (mayúsculas +
// sin acentos) ambos lados antes de comparar, para no volver a romperse si alguien
// retipea la regla distinto en otro ambiente.

const BankMovement       = require('../banks/BankMovement.model');
const { ERP_TOLERANCE }  = require('../banks/bank.service');
const CajaTransferencia  = require('./CajaTransferencia.model');
const globalConfigService = require('../../../shared/services/global-config.service');

// Configuraciones Globales, sección `bancos`, clave TRANSFERENCIAS_DATE_WINDOW_DAYS —
// distinta de bancos.DATE_WINDOW_DAYS (esa es del motor de coincidencia ERP↔CxC, un
// proceso conceptualmente distinto; acoplarlas haría que ajustar uno afecte al otro
// sin querer). Fallback interno 5 días si el ambiente todavía no la sembró — valor de
// arranque razonable (el depósito físico normalmente sigue a la transferencia interna
// en cuestión de días), pero ES AJUSTABLE desde la UI, no una verdad de negocio fija.
const VENTANA_DEFAULT_DIAS = 5;

async function _ventanaDias() {
  let valor;
  try {
    valor = await globalConfigService.getValue('bancos', 'TRANSFERENCIAS_DATE_WINDOW_DAYS');
  } catch (err) {
    if (err.message?.includes('No existe la configuración')) return VENTANA_DEFAULT_DIAS;
    throw err;
  }
  const dias = parseInt(valor, 10);
  return Number.isFinite(dias) && dias > 0 ? dias : VENTANA_DEFAULT_DIAS;
}

function _montosIguales(a, b) {
  return Math.abs((a ?? 0) - (b ?? 0)) <= ERP_TOLERANCE;
}

// Mayúsculas + sin acentos/diacríticos + sin espacios de sobra — `categoria` es texto
// libre configurado por quien arma las reglas (Reglas, dentro de Bancos), así que no se
// puede confiar en una sola forma exacta de escribirlo. Compara por código de punto
// (0x0300-0x036F es el bloque "Combining Diacritical Marks" que normalize('NFD') separa
// de una letra acentuada) en vez de un literal de rango unicode en el código fuente, para
// que la lógica no dependa de que ese carácter sobreviva intacto a cada herramienta.
function _normalizarCategoria(str) {
  const sinDiacriticos = Array.from((str ?? '').normalize('NFD'))
    .filter((ch) => {
      const code = ch.codePointAt(0);
      return code < 0x0300 || code > 0x036f;
    })
    .join('');
  return sinDiacriticos.trim().toUpperCase();
}

const CATEGORIA_DEPOSITO_EFECTIVO = _normalizarCategoria('Depósito en efectivo');

// Exportada para que caja-transferencia-confirm.service.js re-valide con el MISMO criterio
// al confirmar — bug real 2026-09-01: buscarCandidatos() ya normalizaba, pero la validación
// de confirmarMatch() seguía comparando con `===` exacto, así que un candidato SUGERIDO acá
// no pasaba al confirmarlo (ConflictError "no es Depósito en efectivo" con una categoría que
// buscarCandidatos() sí había aceptado segundos antes).
function esCategoriaDepositoEfectivo(categoria) {
  return _normalizarCategoria(categoria) === CATEGORIA_DEPOSITO_EFECTIVO;
}

// Grupos candidatos para una transferencia: TODOS los BankMovement elegibles cuyo
// `deposito` matchea `transferencia.monto` dentro de la tolerancia (1:1 exacto),
// cada uno como su propio grupo de 1 elemento — si hay 2+ que empatan en monto,
// se devuelven TODOS (ambigüedad real, la resuelve un humano). Deliberadamente NO
// se buscan combinaciones de 2+ movimientos que sumen el monto (ver corrección
// 2026-09-08 arriba) — cada grupo tiene siempre exactamente 1 movimiento.
async function buscarCandidatos(transferencia) {
  if (!transferencia.fechaRecepcion) return [];

  const ventanaDias = await _ventanaDias();
  const desde = new Date(transferencia.fechaRecepcion);
  desde.setDate(desde.getDate() - ventanaDias);
  const hasta = new Date(transferencia.fechaRecepcion);
  hasta.setDate(hasta.getDate() + ventanaDias);

  // categoria no se filtra en la query de Mongo (texto libre, no se puede comparar
  // normalizado ahí) — se trae todo lo elegible por erpLinks+fecha y se filtra por
  // categoria normalizada en JS, abajo.
  const elegibles = await BankMovement.find({
    erpLinks: { $size: 0 },
    status:   { $ne: 'identificado' },
    fecha:    { $gte: desde, $lte: hasta },
  }).lean();
  const candidatos = elegibles.filter(m => _normalizarCategoria(m.categoria) === CATEGORIA_DEPOSITO_EFECTIVO);

  const coincidencias = candidatos.filter(m => _montosIguales(m.deposito, transferencia.monto));
  return coincidencias.map(m => [m]);
}

// FECHA_CORTE_LOGICA_HISTORICA (2026-09-09, pedido explícito del usuario): las
// transferencias con fechaRecepcion >= este valor siguen el proceso normal de arriba
// (buscarCandidatos ya excluye depósitos identificados a propósito — si no aparece
// ninguno, es una huérfana real y debe seguir 'pendiente' en la bandeja). Antes de esta
// fecha, el sync trae transferencias de meses atrás cuyo depósito correspondiente ya fue
// identificado hace tiempo por OTRA vía (ficha, otro proceso ERP) — buscarCandidatos()
// las deja perpetuamente sin candidatos, llenando la bandeja de ruido histórico sin nada
// accionable. Ver reclasificarHistoricasDescartadas(). Constante fija en código (decisión
// explícita del usuario: no amerita vivir en Configuraciones Globales).
const FECHA_CORTE_LOGICA_HISTORICA = new Date('2026-09-07T00:00:00.000Z');

// Igual que buscarCandidatos() pero SIN excluir depósitos con erpLinks/status:'identificado'
// — usada solo para decidir si una transferencia histórica sin candidato ACCIONABLE tiene,
// de todos modos, al menos un depósito que calza exacto por monto/ventana (ya resuelto por
// otra vía). Nunca se usa para sugerir nada a un humano, solo para descartar ruido.
async function _buscarCoincidenciasHistoricas(transferencia) {
  if (!transferencia.fechaRecepcion) return [];

  const ventanaDias = await _ventanaDias();
  const desde = new Date(transferencia.fechaRecepcion);
  desde.setDate(desde.getDate() - ventanaDias);
  const hasta = new Date(transferencia.fechaRecepcion);
  hasta.setDate(hasta.getDate() + ventanaDias);

  const elegibles = await BankMovement.find({
    fecha: { $gte: desde, $lte: hasta },
  }).lean();
  const candidatos = elegibles.filter(m => _normalizarCategoria(m.categoria) === CATEGORIA_DEPOSITO_EFECTIVO);

  return candidatos.filter(m => _montosIguales(m.deposito, transferencia.monto));
}

// Corre encadenada al final del sync diario (ver cajaTransferenciaSyncCron.js). Reclasifica
// como 'descartada' las transferencias 'pendiente' anteriores a FECHA_CORTE_LOGICA_HISTORICA
// que:
//   1. NO tienen ningún candidato ACCIONABLE (buscarCandidatos vacío), Y
//   2. SÍ tienen al menos un depósito que calza exacto por monto/ventana ya resuelto por
//      otra vía (identificado o con erpLinks) — sin importar si hay 1 o varios "atados": si
//      ninguno queda pendiente, no hay nada accionable para un humano acá (decisión explícita
//      del usuario 2026-09-09: un empate histórico donde TODOS ya están identificados también
//      se descarta, igual que el caso 1:1).
// Transferencias sin NINGÚN match, ni siquiera histórico, quedan 'pendiente' tal cual —
// esas sí son huérfanas reales, no basura de otro proceso.
//
// `excluidaPorFiltro:{$ne:true}` (mismo criterio que la bandeja, erp.routes.js) es
// obligatorio acá: reaplicarFiltro() y la bandeja solo tocan/leen 'pendiente' — si esta
// función descartara una transferencia mientras el filtro de tipo/caja la tiene oculta,
// esa decisión queda CONGELADA para siempre (ya no es 'pendiente', así que un futuro
// ensanche del filtro nunca la vuelve a evaluar). Solo se reclasifican las que realmente
// serían visibles en la bandeja hoy.
async function reclasificarHistoricasDescartadas() {
  const historicas = await CajaTransferencia.find({
    estatusMatch:      'pendiente',
    excluidaPorFiltro: { $ne: true },
    fechaRecepcion:    { $lt: FECHA_CORTE_LOGICA_HISTORICA },
  }).lean();

  let descartadas = 0;
  for (const t of historicas) {
    // eslint-disable-next-line no-await-in-loop
    const accionables = await buscarCandidatos(t);
    if (accionables.length > 0) continue;

    // eslint-disable-next-line no-await-in-loop
    const coincidenciasHistoricas = await _buscarCoincidenciasHistoricas(t);
    if (coincidenciasHistoricas.length === 0) continue;

    // eslint-disable-next-line no-await-in-loop
    await CajaTransferencia.updateOne({ _id: t._id }, { $set: { estatusMatch: 'descartada' } });
    descartadas++;
  }

  if (descartadas > 0) {
    console.log(`[CajaTransferenciaMatch] ${descartadas} transferencias históricas reclasificadas como 'descartada'.`);
  }
  return { revisadas: historicas.length, descartadas };
}

module.exports = {
  buscarCandidatos,
  esCategoriaDepositoEfectivo,
  reclasificarHistoricasDescartadas,
  _ventanaDias,
  _normalizarCategoria,
  _buscarCoincidenciasHistoricas,
  FECHA_CORTE_LOGICA_HISTORICA,
  VENTANA_DEFAULT_DIAS,
};
