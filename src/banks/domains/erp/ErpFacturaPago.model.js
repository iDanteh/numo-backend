'use strict';
const mongoose = require('mongoose');
const { UUID } = require('sequelize');

const relacionesSchema = new mongoose.Schema({
    tipoRelacion: { type: String, default: null },
    uuid: { type: String, default: null },
}, { _id: false });

const erpFacturaPagoSchema = new mongoose.Schema({

    // Clave natural del ERP — se usa como filtro en el upsert (idempotente)
    erpId: { type: String, required: true, unique: true, index: true },

    uuid: { type: String, default: null },
    tipoComprobante: { type: String, default: null },
    serie: { type: String, default: null },
    folio: { type: String, default: null },
    subtotal: { type: Number, default: null },
    totalIva: { type: Number, default: null },
    totalRetenciones: { type: Number, default: null },
    importe: { type: Number, default: null },
    metodoPago: { type: String, default: null },
    fechaPago: { type: Date, default: null },
    fechaTimbrado: { type: Date, default: null },
    estatus: { type: String, default: null },

    relaciones: { type: [relacionesSchema], default: [] },

    // Última vez que el ERP devolvió este registro
    lastSeenAt: { type: Date, default: null },
}, { timestamps: true, collection: 'erp_facturas_pago' });

module.exports = mongoose.model('ErpFacturaPago', erpFacturaPagoSchema);