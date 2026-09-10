'use strict';
const mongoose = require('mongoose');

// CajaTransferencia — persistencia local de las transferencias internas de
// efectivo entre cajas (sucursal → gerente) que reporta Kore en
// GET /transferencias/reportes/buscar (ver kore-caja.service.js#buscarTransferenciasCajas).
// Kore no guarda estado de matching de su lado — esta colección es la única
// fuente de verdad para saber qué transferencia ya se cruzó contra qué
// BankMovement. (2026-09-02: se eliminó el concepto de 'huerfana' — pedido
// explícito del usuario, va a reemplazarse por algo distinto todavía no definido.
// 2026-09-09: el reemplazo es 'descartada', ver
// caja-transferencia-match.service.js#reclasificarHistoricasDescartadas.)
const cajaTransferenciaSchema = new mongoose.Schema({
  // Id propio de Kore para esta transferencia — clave de upsert (ver
  // caja-transferencia-sync.service.js), evita duplicados si el job de sync
  // reconsulta un rango de fechas ya sincronizado antes (backfill/reintentos).
  koreId: { type: String, required: true, unique: true, index: true },

  monto: { type: Number, required: true },

  // Estatus tal cual lo reporta Kore (ej. "RECIBIDO") — NO confundir con
  // estatusMatch, de abajo (estado interno de Numo para el matching).
  estatusKore: { type: String, default: null },

  cajaOrigenId:      { type: String, default: null },
  nombreCajaOrigen:  { type: String, default: null },
  almacenCajaOrigen: { type: String, default: null },

  cajaDestinoId:      { type: String, default: null },
  nombreCajaDestino:  { type: String, default: null },
  almacenCajaDestino: { type: String, default: null },

  sessionOrigenId:  { type: String, default: null },
  sessionDestinoId: { type: String, default: null },

  formaPago:       { type: String, default: null },
  nombreFormaPago: { type: String, default: null },

  solicito:       { type: String, default: null },
  nombreSolicito: { type: String, default: null },
  recibio:        { type: String, default: null },
  nombreRecibio:  { type: String, default: null },
  autorizo:       { type: String, default: null },
  nombreAutorizo: { type: String, default: null },

  fechaSolicitud: { type: Date, default: null },
  fechaRecepcion: { type: Date, default: null, index: true },

  observacion: { type: String, default: null },

  idTipoTransferencia:     { type: String, default: null },
  nombreTipoTransferencia: { type: String, default: null },

  // Estado interno de Numo para el proceso de matching — nunca se toca desde el
  // sync, solo se setea al insertar (ver $setOnInsert en
  // caja-transferencia-sync.service.js) para que un re-sync no pise el resultado
  // de un matching ya resuelto.
  // 'descartada' (2026-09-09): transferencia anterior a FECHA_CORTE_LOGICA_HISTORICA
  // (caja-transferencia-match.service.js) sin ningún candidato ACCIONABLE, pero cuyo
  // depósito correspondiente ya fue identificado por otra vía (ficha, otro proceso ERP)
  // antes de que existiera este panel — no hay nada que un humano pueda hacer acá, así
  // que se saca de la bandeja para no llenarla de ruido histórico. Nunca se asigna a
  // transferencias nuevas (fechaRecepcion >= el corte) — esas, sin candidato, siguen
  // 'pendiente' (huérfanas reales).
  // 'descartada-manual' (2026-09-10): descarte MANUAL de un humano sobre una transferencia
  // 'pendiente' SIN candidatos, que sabe (por fuera de este panel) que ya fue identificada —
  // DISTINTO de 'descartada' (esa es exclusiva del job automático de arriba, nunca se
  // mezclan). Ver caja-transferencia-descartar-manual.service.js.
  estatusMatch: {
    type:    String,
    enum:    ['pendiente', 'matcheada', 'descartada', 'descartada-manual'],
    default: 'pendiente',
    index:   true,
  },

  // Trazabilidad de la confirmación humana (Fase D) — quién autorizó el match y contra
  // qué BankMovement(s) (1 o 2, ver caja-transferencia-match.service.js#buscarCandidatos).
  // El lado BankMovement.erpLinks/historialVinculacion ya trae su propia trazabilidad
  // (ver bank.service.js#setErpIds) — esto es la vista desde la transferencia.
  confirmadoPor: {
    type: {
      userId: { type: String, default: null },
      nombre: { type: String, default: null },
    },
    default: null,
  },
  confirmadoEn: { type: Date, default: null },
  movementIdsConfirmados: { type: [mongoose.Schema.Types.ObjectId], ref: 'BankMovement', default: [] },

  // Trazabilidad del descarte manual (ver caja-transferencia-descartar-manual.service.js) —
  // mismo patrón que confirmadoPor/confirmadoEn arriba, pero para 'descartada-manual'.
  descartadoManualmentePor: {
    type: {
      userId: { type: String, default: null },
      nombre: { type: String, default: null },
    },
    default: null,
  },
  descartadoManualmenteEn: { type: Date, default: null },

  // Exclusión por filtro de config (NOMBRE_TIPO_TRANSFERENCIA_PERMITIDOS/NOMBRE_CAJA_DESTINO_PERMITIDAS,
  // ver caja-transferencia-sync.service.js#reaplicarFiltro) — ortogonal a estatusMatch a propósito:
  // estatusMatch sigue significando SOLO el resultado del matching (pendiente/matcheada), esto
  // es una capa aparte que dice "el admin no quiere ver esta transferencia" y es reversible (si la config
  // vuelve a permitirla, una futura corrida de reaplicarFiltro la reincluye sin tocar estatusMatch).
  excluidaPorFiltro: { type: Boolean, default: false, index: true },
  excluidaEn:        { type: Date, default: null },
}, { timestamps: true, collection: 'caja_transferencias' });

module.exports = mongoose.model('CajaTransferencia', cajaTransferenciaSchema);
