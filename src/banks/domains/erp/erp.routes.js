'use strict';

const express = require('express');
const axios   = require('axios');
const { authenticate, authorize } = require('../../shared/middleware/auth.real');
const { asyncHandler } = require('../../shared/middleware/error-handler');
const ErpCuentaPendiente = require('./ErpCuentaPendiente.model');
const BankMovement       = require('../banks/BankMovement.model');
const { emitToAll }      = require('../../shared/socket');

const ERP_TOLERANCE = 1.00; // $1 MXN de tolerancia (misma que bank.service)

// ── Helpers de normalización (misma lógica que bank-autorizaciones.service) ──
// Extrae solo dígitos y elimina ceros iniciales para comparación robusta.
//   "00354198"  → "354198"
//   " 354198 "  → "354198"
//   "ABC"       → null
function normalizarAuth(val) {
  if (val == null || val === '') return null;
  const digits = String(val).trim().replace(/\D/g, '');
  if (!digits) return null;
  const n = parseInt(digits, 10);
  return isNaN(n) ? null : String(n);
}

// Busca authNorm dentro de los bloques numéricos del concepto.
// Compara contra cada bloque normalizado, evitando falsos positivos por substring.
function conceptoContainsAuth(concepto, authNorm) {
  if (!concepto || !authNorm) return false;
  const bloques = String(concepto).match(/\d+/g);
  if (!bloques) return false;
  return bloques.some(b => normalizarAuth(b) === authNorm);
}

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

// POST /api/erp/match
// Criterio de match (mismas dos estrategias que matchAutorizaciones):
//   Estrategia 1 — numeroAutorizacion normalizado coincide con autorizacion del ERP.
//   Estrategia 2 — la autorizacion del ERP aparece dentro del concepto del movimiento.
//   En ambos casos el monto debe diferir <= $1 MXN del total ERP.
// La normalización elimina ceros iniciales, espacios y no-dígitos para que
//   "00354198" y "354198" sean equivalentes.
router.post('/match', authenticate, authorize('admin', 'contador'), asyncHandler(async (_req, res) => {
  const erpCuentas = await ErpCuentaPendiente.find().lean();
  if (!erpCuentas.length) {
    return res.json({ matched: 0, message: 'No hay cuentas ERP almacenadas' });
  }

  const movements = await BankMovement.find({
    isActive: true,
    status:   'no_identificado',
    deposito: { $gt: 0 },
  }).select('_id numeroAutorizacion concepto deposito erpIds erpLinks').lean();

  if (!movements.length) {
    return res.json({ matched: 0, message: 'No hay movimientos no identificados' });
  }

  // ── Índice 1: auth normalizado → movimiento (para los que tienen numeroAutorizacion) ──
  const byAuthNorm = new Map();
  for (const mov of movements) {
    const n = normalizarAuth(mov.numeroAutorizacion);
    if (!n) continue;
    if (!byAuthNorm.has(n)) byAuthNorm.set(n, []);
    byAuthNorm.get(n).push(mov);
  }

  // ── Índice 2: movimientos sin auth explícita, para buscar en concepto ────────
  const sinAuth = movements.filter(m => !normalizarAuth(m.numeroAutorizacion) && m.concepto);

  const usedMovIds = new Set();
  const ops        = [];

  for (const erp of erpCuentas) {
    // Normalizar todas las autorizaciones de las formasPago de esta cuenta ERP
    const erpAuthsNorm = [
      ...new Set(
        (erp.movimientos || [])
          .flatMap(m => m.formasPago || [])
          .map(fp => normalizarAuth(fp.autorizacion))
          .filter(Boolean),
      ),
    ];

    if (!erpAuthsNorm.length) continue; // cuenta sin autorización → no aplica

    let matchedMov = null;

    // Estrategia 1 — match por numeroAutorizacion normalizado
    outer1:
    for (const authNorm of erpAuthsNorm) {
      const candidatos = byAuthNorm.get(authNorm) ?? [];
      for (const mov of candidatos) {
        if (usedMovIds.has(mov._id.toString())) continue;
        if ((mov.erpIds || []).includes(erp.erpId)) continue;
        if (Math.abs(mov.deposito - erp.total) > ERP_TOLERANCE) continue;
        matchedMov = mov;
        break outer1;
      }
    }

    // Estrategia 2 — buscar auth dentro del concepto
    if (!matchedMov) {
      outer2:
      for (const authNorm of erpAuthsNorm) {
        for (const mov of sinAuth) {
          if (usedMovIds.has(mov._id.toString())) continue;
          if ((mov.erpIds || []).includes(erp.erpId)) continue;
          if (Math.abs(mov.deposito - erp.total) > ERP_TOLERANCE) continue;
          if (!conceptoContainsAuth(mov.concepto, authNorm)) continue;
          matchedMov = mov;
          break outer2;
        }
      }
    }

    if (!matchedMov) continue;

    usedMovIds.add(matchedMov._id.toString());

    const link = {
      erpId:       erp.erpId,
      saldoActual: erp.saldoActual ?? null,
      folioFiscal: null,
      total:       erp.total ?? null,
    };
    const newLinks  = [...(matchedMov.erpLinks || []), link];
    const newIds    = [...(matchedMov.erpIds   || []), erp.erpId];
    const saldoErp  = newLinks.reduce((s, l) => s + (l.saldoActual || l.total || 0), 0);
    const newStatus = Math.abs(matchedMov.deposito - saldoErp) <= ERP_TOLERANCE
      ? 'identificado'
      : 'no_identificado';

    ops.push({
      updateOne: {
        filter: { _id: matchedMov._id, status: 'no_identificado' },
        update: {
          $set: {
            erpIds:   newIds,
            erpLinks: newLinks,
            saldoErp,
            status:   newStatus,
            ...(newStatus === 'identificado' && {
              identificadoPor: { userId: 'erp-auto', nombre: 'Motor ERP', fechaId: new Date() },
            }),
          },
        },
      },
    });
  }

  let matched = 0;
  if (ops.length > 0) {
    const result = await BankMovement.bulkWrite(ops, { ordered: false });
    matched = result.modifiedCount ?? ops.length;
  }

  const matchResult = { matched, message: `${matched} movimiento(s) vinculado(s) con cuentas ERP` };
  emitToAll('bank:erp:match:done', matchResult);
  res.json(matchResult);
}));

// POST /api/erp/match/revert
// Deshace todas las asociaciones realizadas por el motor automático (userId: 'erp-auto').
// Restaura erpIds, erpLinks, saldoErp, uuidXML y status a su estado original.
router.post('/match/revert', authenticate, authorize('admin', 'contador'), asyncHandler(async (_req, res) => {
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
