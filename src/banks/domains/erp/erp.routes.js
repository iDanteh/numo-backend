'use strict';

const express = require('express');
const axios   = require('axios');
const { authenticate, permit }    = require('../../shared/middleware/auth.real');
const { asyncHandler } = require('../../shared/middleware/error-handler');
const ErpCuentaPendiente = require('./ErpCuentaPendiente.model');
const ErpFacturaPago     = require('./ErpFacturaPago.model');
const BankMovement       = require('../banks/BankMovement.model');

const router = express.Router();

const ERP_CAJA_BASE_URL = (process.env.ERP_CAJA_BASE_URL || '').replace(/\/$/, '');
const ERP_FACT_BASE_URL = (process.env.ERP_FACT_BASE_URL || '').replace(/\/$/, '');
const ERP_TOKEN    = process.env.ERP_TOKEN || '';

// GET /api/erp/cuentas-pendientes
// Parámetros: fechaDesde, fechaHasta, estadoCobro (opcional; 'pendiente' para solo pendientes)
router.get('/cuentas-pendientes', authenticate, asyncHandler(async (req, res) => {
  if (!ERP_CAJA_BASE_URL) {
    return res.status(503).json({ error: 'ERP no configurado (ERP_CAJA_BASE_URL ausente)' });
  }

  const { fechaDesde, fechaHasta, estadoCobro, page } = req.query;

  const params = { fechaDesde, fechaHasta };
  if (estadoCobro) params.estadoCobro = estadoCobro;
  if (page)        params.page        = page;

  const response = await axios.get(`${ERP_CAJA_BASE_URL}/cuentas-pendientes`, {
    params,
    headers: { Authorization: `Bearer ${ERP_TOKEN}` },
    timeout: 15000,
  });

  const dataPayload = response.data?.Data ?? {};
  const raw         = dataPayload.cuentas || [];
  const now = new Date();

  // Upsert idempotente: cada cuenta se identifica por su id del ERP
  if (raw.length > 0) {
    await Promise.all(raw.map(c => ErpCuentaPendiente.updateOne(
      { erpId: c.id },
      {
        $set: {
          erpId:            c.id,
          serie:            c.serie            ?? null,
          folio:            c.folio            ?? null,
          serieExterna:     c.serieExterna     ?? null,
          folioExterno:     c.folioExterno     ?? null,
          folioFiscal:      c.folioFiscal      ?? null,
          tipoPago:         c.tipoPago         ?? null,
          subtotal:         c.subtotal         ?? null,
          impuesto:         c.impuesto         ?? null,
          total:            c.total            ?? null,
          saldoActual:      c.saldoActual      ?? null,
          fechaCreacion:    c.fechaCreacion    ?? null,
          fechaRealPago:    c.fechaRealPago    ?? null,
          fechaAfectacion:  c.fechaAfectacion  ?? null,
          fechaVencimiento: c.fechaVencimiento ?? null,
          fechaProgramada:  c.fechaProgramada  ?? null,
          concepto:         c.concepto         ?? null,
          conceptoCobroID:  c.conceptoCobroID  ?? null,
          almacen:          c.almacen          ?? null,
          personaId:        c.personaId        ?? null,
          claveImpuesto:    c.claveImpuesto    ?? null,
          factorImpuesto:   c.factorImpuesto   ?? null,
          anotacion:        c.anotacion        ?? null,
          plazo:            c.plazo            ?? null,
          tipoMovimiento:   c.tipoMovimiento   ?? null,
          movimientos:      c.movimientos      ?? [],
          lastSeenAt:       now,
        },
      },
      { upsert: true }
    )));
  }

  const cuentas = raw.map(c => ({
    id:               c.id,
    serie:            c.serie,
    folio:            c.folio,
    folioFiscal:      c.folioFiscal ?? null,
    tipoPago:         c.tipoPago   ?? null,
    subtotal:         c.subtotal,
    impuesto:         c.impuesto,
    total:            c.total,
    saldoActual:      c.saldoActual,
    fechaVencimiento: c.fechaVencimiento ?? null,
    folioFiscal:      c.folioFiscal ?? null,
  }));

  res.json({
    data: cuentas,
    pagination: {
      page:        dataPayload.paginaActual  ?? Number(page ?? 1),
      totalPaginas: dataPayload.totalPaginas ?? null,
    },
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
  const result = await BankMovement.updateMany(
    { 'identificadoPor.userId': 'erp-auto' },
    {
      $set: {
        erpIds:   [],
        erpLinks: [],
        saldoErp: null,
        uuidXML:  null,
        status:   'no_identificado',
        identificadoPor: [],
      },
    },
  );

  res.json({
    reverted: result.modifiedCount,
    message:  `${result.modifiedCount} asociación(es) revertida(s)`,
  });
}));

module.exports = router;
