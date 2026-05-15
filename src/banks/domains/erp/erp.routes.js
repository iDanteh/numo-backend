'use strict';

const express = require('express');
const axios   = require('axios');
const { authenticate, permit }           = require('../../shared/middleware/auth.real');
const { asyncHandler }                   = require('../../shared/middleware/error-handler');
const { sincronizarCuentasPendientes }   = require('./erp-sync.service');
const ErpFacturaPago                     = require('./ErpFacturaPago.model');
const ErpCuentaPendiente                 = require('./ErpCuentaPendiente.model');
const BankMovement                       = require('../banks/BankMovement.model');

const router = express.Router();

const ERP_FACT_BASE_URL = (process.env.ERP_FACT_BASE_URL || '').replace(/\/$/, '');
const ERP_TOKEN    = process.env.ERP_TOKEN || '';

const ERP_PAGE_SIZE = 50;

// GET /api/erp/cuentas-pendientes
// Parámetros: fechaDesde, fechaHasta, estadoCobro (opcional; 'pendiente' para solo pendientes), page
// La paginación se aplica localmente sobre la respuesta completa del ERP.
router.get('/cuentas-pendientes', authenticate, asyncHandler(async (req, res) => {
  const { fechaDesde, fechaHasta, estadoCobro, page, serieExterna, folioExterno, nombrePersona } = req.query;
  const pageNum = Math.max(1, parseInt(page ?? '1', 10));

  // sincronizarCuentasPendientes llama al ERP, upserta en el caché y devuelve los
  // datos crudos para que este endpoint pueda construir la respuesta paginada.
  let raw = [];
  try {
    ({ raw } = await sincronizarCuentasPendientes({
      fechaDesde, fechaHasta, estadoCobro, serieExterna, folioExterno, nombrePersona,
    }));
  } catch (err) {
    if (err.message?.includes('ERP no configurado')) {
      return res.status(503).json({ error: err.message });
    }
    throw err;
  }

  const allCuentas = raw.map(c => ({
    id:               c.id,
    serie:            c.serie            ?? null,
    folio:            c.folio            ?? null,
    serieExterna:     c.serieExterna     ?? null,
    folioExterno:     c.folioExterno     ?? null,
    folioFiscal:      c.folioFiscal      ?? null,
    tipoPago:         c.tipoPago         ?? null,
    subtotal:         c.subtotal,
    impuesto:         c.impuesto,
    total:            c.total,
    saldoActual:      c.saldoActual,
    fechaVencimiento: c.fechaVencimiento ?? null,
    nombrePersona:    c.nombrePersona    ?? null,
  }));

  // Local pagination (filtering is now handled server-side by the ERP via serieExterna/folioExterno)
  const total        = allCuentas.length;
  const totalPaginas = Math.max(1, Math.ceil(total / ERP_PAGE_SIZE));
  const safePage     = Math.min(pageNum, totalPaginas);
  const start        = (safePage - 1) * ERP_PAGE_SIZE;
  const cuentas      = allCuentas.slice(start, start + ERP_PAGE_SIZE);

  res.json({
    data: cuentas,
    pagination: { page: safePage, totalPaginas, total },
  });
}));

// GET /api/erp/facturas/reporte
// Parámetros: fechaDesde, fechaHasta, tipo_comprobante (opcional)
router.get('/reporte', authenticate, asyncHandler(async (req, res) => {
  if (!ERP_FACT_BASE_URL) {
    return res.status(503).json({ error: 'ERP no configurado (ERP_FACT_BASE_URL ausente)' });
  }

  const { fechaInicio, fechaFin, tipo_comprobante } = req.query;

  // El ERP externo usa snake_case: fecha_inicio / fecha_fin
  const params = { fecha_inicio: fechaInicio, fecha_fin: fechaFin };
  if (tipo_comprobante) params.tipo_comprobante = tipo_comprobante;

  const response = await axios.get(`${ERP_FACT_BASE_URL}/api/facturas/reporte`, {
    params,
    headers: { Authorization: `Bearer ${ERP_TOKEN}` },
    timeout: 15000,
  });

  // El ERP devuelve PascalCase; el array puede estar en Data[] o Data.facturas[]
  const dataPayload = response.data?.Data ?? response.data ?? [];
  const raw = Array.isArray(dataPayload)
    ? dataPayload
    : (dataPayload.facturas ?? dataPayload.Facturas ?? []);
  const now = new Date();

  // Upsert idempotente: cada factura se identifica por su ID del ERP (PascalCase)
  if (raw.length > 0) {
    await Promise.all(raw.map(f => ErpFacturaPago.updateOne(
      { erpId: f.ID },
      {
        $set: {
          erpId:            f.ID,
          uuid:             f.UUID             ?? null,
          tipoComprobante:  f.TipoComprobante  ?? null,
          serie:            f.Serie            ?? null,
          folio:            f.Folio            ?? null,
          subtotal:         f.Subtotal         ?? null,
          totalIva:         f.TotalIVA         ?? null,
          totalRetenciones: f.TotalRetenciones ?? null,
          importe:          f.Importe          ?? null,
          metodoPago:       f.MetodoPago       ?? null,
          fechaPago:        f.FechaPago        ?? null,
          fechaTimbrado:    f.FechaTimbrado    ?? null,
          estatus:          f.Estatus          ?? null,
          relaciones: (f.Relaciones ?? []).map(r => ({
            tipoRelacion: r.TipoRelacion ?? null,
            uuid:         r.UUID         ?? null,
          })),
          lastSeenAt: now,
        },
      },
      { upsert: true }
    )));
  }

  const facturas = raw.map(f => ({
    id:               f.ID,
    uuid:             f.UUID             ?? null,
    tipoComprobante:  f.TipoComprobante  ?? null,
    serie:            f.Serie            ?? null,
    folio:            f.Folio            ?? null,
    subtotal:         f.Subtotal         ?? null,
    totalIva:         f.TotalIVA         ?? null,
    totalRetenciones: f.TotalRetenciones ?? null,
    importe:          f.Importe          ?? null,
    metodoPago:       f.MetodoPago       ?? null,
    fechaPago:        f.FechaPago        ?? null,
    fechaTimbrado:    f.FechaTimbrado    ?? null,
    estatus:          f.Estatus          ?? null,
  }));

  res.json(facturas);
}));

// POST /api/erp/match/revert
// Deshace todas las asociaciones realizadas por el motor automático (userId: 'erp-auto').
// Restaura erpIds, erpLinks, saldoErp, uuidXML y status a su estado original.
router.post('/match/revert', authenticate, permit('erp:manage'), asyncHandler(async (_req, res) => {
  // Pipeline de agregación para proteger el trabajo de usuarios humanos:
  // · Siempre limpia los datos ERP (erpIds, erpLinks, saldoErp, uuidXML).
  // · Elimina SOLO la entrada 'erp-auto' de identificadoPor (preserva las humanas).
  // · Resetea status a 'no_identificado' únicamente cuando no quedan entradas humanas
  //   en identificadoPor; si un usuario ya confirmó el movimiento, respeta su status.
  const result = await BankMovement.updateMany(
    { 'identificadoPor.userId': 'erp-auto' },
    [
      {
        $set: {
          erpIds:   [],
          erpLinks: [],
          saldoErp: null,
          uuidXML:  null,
          identificadoPor: {
            $filter: {
              input: '$identificadoPor',
              as:    'entry',
              cond:  { $ne: ['$$entry.userId', 'erp-auto'] },
            },
          },
          status: {
            $cond: {
              if: {
                $eq: [
                  {
                    $size: {
                      $filter: {
                        input: '$identificadoPor',
                        as:    'e',
                        cond:  { $ne: ['$$e.userId', 'erp-auto'] },
                      },
                    },
                  },
                  0,
                ],
              },
              then: 'no_identificado',
              else: '$status',
            },
          },
        },
      },
    ],
  );

  res.json({
    reverted: result.modifiedCount,
    message:  `${result.modifiedCount} asociación(es) revertida(s)`,
  });
}));

module.exports = router;
