'use strict';
const mongoose = require('mongoose');

// CorteDescarga — historial de descargas del Excel de Cortes (2026-11-06, pedido explícito del
// usuario: quiere poder ver, descarga a descarga, si "depósitos no identificados" sube o baja).
// Un documento por cada vez que alguien pide GET /cortes/descargar — se crea DENTRO de
// buildReporteCortes() (bank-indicadores.service.js), nunca por separado: no hay forma de
// generar el Excel sin que quede su registro de historial.
//
// `depositosNoIdentificados` es el MISMO número que ya muestra el encabezado del panel ("Saldo
// por identificar al hoy") — el `total` del día más reciente de getLedgerDiario(). No es un
// snapshot de la vista Antigüedad (que es un subconjunto más estricto, "completamente vacíos")
// ni de ningún mes/día en particular de las hojas Mensual/Diario del propio Excel — es el
// backlog total vigente al momento exacto de la descarga, consistente sin importar qué tab
// estaba viendo quien exportó (acá hay un solo botón de exportar para todo Cortes, no uno por
// tab — decisión explícita del usuario).
const corteDescargaSchema = new mongoose.Schema({
  fecha: { type: Date, default: Date.now },

  // Mismo patrón {userId, nombre} que ya usa el resto del dominio (ver NetpayReporte.cargadoPor,
  // bank.routes.js#'/autorizaciones/match') — texto libre, no una FK a un usuario real.
  usuario: {
    userId: { type: String, default: null },
    nombre: { type: String, default: null },
  },

  // Filtro de banco activo al momento de exportar — `null` significa "Todos", igual que en el
  // resto de los endpoints de Cortes (ver buildBaseMatch({banco})). El historial compara cada
  // descarga SOLO contra la anterior con este MISMO valor (ver listarDescargasCorte) — nunca
  // mezcla un snapshot de "BBVA" contra uno de "Todos".
  banco: { type: String, default: null },

  depositosNoIdentificados: {
    count: { type: Number, required: true },
    saldo: { type: Number, required: true },
  },

  // ID del archivo en Drive (2026-11-06, pedido explícito del usuario: poder volver a bajar
  // cualquier reporte de un punto pasado de la historia, no solo ver el delta de backlog) — ver
  // drive-cortes.service.js. `null` si la subida a Drive falló en el momento de generarlo (la
  // descarga en curso del usuario NUNCA se bloquea por eso, ver buildReporteCortes); esa fila en
  // particular simplemente no será redescargable después.
  driveFileId: { type: String, default: null },
}, { timestamps: true, collection: 'corte_descargas' });

// Insumo de listarDescargasCorte: "traeme la descarga anterior con este mismo banco" — ordena
// por fecha descendente dentro de cada valor de banco.
corteDescargaSchema.index({ banco: 1, fecha: -1 });

module.exports = mongoose.model('CorteDescarga', corteDescargaSchema);
