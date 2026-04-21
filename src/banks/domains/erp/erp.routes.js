'use strict';

const express = require('express');
const axios   = require('axios');
const { authenticate, permit }    = require('../../shared/middleware/auth.real');
const { asyncHandler } = require('../../shared/middleware/error-handler');
const ErpCuentaPendiente = require('./ErpCuentaPendiente.model');
const BankMovement       = require('../banks/BankMovement.model');

const router = express.Router();

const ERP_BASE_URL = (process.env.ERP_BASE_URL || '').replace(/\/$/, '');
const ERP_TOKEN    = process.env.ERP_TOKEN || '';

// GET /api/erp/cuentas-pendientes
// Parámetros: fechaDesde, fechaHasta, estadoCobro (opcional; 'pendiente' para solo pendientes)
router.get('/cuentas-pendientes', authenticate, asyncHandler(async (req, res) => {
  if (!ERP_BASE_URL) {
    return res.status(503).json({ error: 'ERP no configurado (ERP_BASE_URL ausente)' });
  }

  const { fechaDesde, fechaHasta, estadoCobro } = req.query;

  const params = { fechaDesde, fechaHasta };
  if (estadoCobro) params.estadoCobro = estadoCobro;

  const response = await axios.get(`${ERP_BASE_URL}/cuentas-pendientes`, {
    params,
    headers: { Authorization: `Bearer ${ERP_TOKEN}` },
    timeout: 15000,
  });

  const raw = response.data?.Data?.cuentas || [];
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

  res.json(cuentas);
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
        identificadoPor: { userId: null, nombre: null, fechaId: null },
      },
    },
  );

  res.json({
    reverted: result.modifiedCount,
    message:  `${result.modifiedCount} asociación(es) revertida(s)`,
  });
}));

module.exports = router;
