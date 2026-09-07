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

// Grupos candidatos para una transferencia: cada grupo es 1 o 2 BankMovement cuya
// suma de `deposito` matchea `transferencia.monto` dentro de la tolerancia. Prueba
// 1:1 primero (caso normal); si no hay match exacto de un solo movimiento, prueba
// pares (caso real conocido: límite de depósito por banco obliga a partir el
// efectivo en 2 depósitos). No prueba combinaciones de 3+ — fuera del caso real
// que motivó este proceso, y crece combinatoriamente sin necesidad.
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

  const unico = candidatos.find(m => _montosIguales(m.deposito, transferencia.monto));
  if (unico) return [[unico]];

  const pares = [];
  for (let i = 0; i < candidatos.length; i++) {
    for (let j = i + 1; j < candidatos.length; j++) {
      const suma = (candidatos[i].deposito ?? 0) + (candidatos[j].deposito ?? 0);
      if (_montosIguales(suma, transferencia.monto)) pares.push([candidatos[i], candidatos[j]]);
    }
  }
  return pares;
}

module.exports = {
  buscarCandidatos,
  esCategoriaDepositoEfectivo,
  _ventanaDias,
  _normalizarCategoria,
  VENTANA_DEFAULT_DIAS,
};
