'use strict';
const mongoose = require('mongoose');

// NetpayFolioRegistro — netpay-matching-v2 (design.md "Architecture Decisions"/"Idempotent
// folios"): un registro por folio ya visto en CUALQUIER reporte cargado, para detectar el
// mismo folio/orderID repetido entre dos reportes SIN doble-contar ni doble-vincular (ver
// spec.md "Idempotent Report Re-Upload"). `clave` es orderId cuando existe, o
// 'REF:'+referencia cuando el folio no trae Order ID — nunca ambos a la vez, siempre un
// único valor determinístico por folio.
//
// Por qué una colección nueva y NO un índice único multikey sobre folios.orderId dentro de
// NetpayReporte (rechazado en design.md): un índice multikey único no deduplica DENTRO de
// un mismo documento y choca con null (varios folios sin orderId en el mismo reporte). Un
// findOne() de pre-chequeo antes de insertar tiene ventana de carrera entre dos cargas
// simultáneas. Esta colección resuelve ambos problemas: cargarReporte hace
// insertMany(ordered:false) y deja que Mongo mismo rechace (E11000) cada fila duplicada,
// sin necesitar un pre-chequeo.
//
// NUNCA se borra — ni siquiera cuando su NetpayReporte de origen se oculta (eliminado:true,
// ver NetpayReporte.model.js). Un reporte oculto sigue "gastando" sus folios para que no
// puedan volver a cargarse en un tercer reporte por error.
const netpayFolioRegistroSchema = new mongoose.Schema({
  // Clave determinística del folio — orderId, o 'REF:'+referencia si orderId está ausente.
  // Se calcula en netpay-reporte.service.js#cargarReporte, no acá, para mantener el
  // esquema libre de lógica de negocio.
  clave: { type: String, required: true, unique: true },

  reporteId: { type: mongoose.Schema.Types.ObjectId, ref: 'NetpayReporte', required: true },

  // Copia de los campos originales del folio — puramente informativo/auditoría (para
  // poder inspeccionar un E11000 sin tener que ir a buscar el reporte original).
  orderId:    { type: String, default: null },
  referencia: { type: String, default: null },
}, { timestamps: true, collection: 'netpay_folio_registros' });

module.exports = mongoose.model('NetpayFolioRegistro', netpayFolioRegistroSchema);
