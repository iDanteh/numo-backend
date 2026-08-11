'use strict';

const express = require('express');
const { authenticate, permit }         = require('../../shared/middleware/auth.real');
const { asyncHandler }                 = require('../../shared/middleware/error-handler');
const { verifyKoreApiKey }             = require('../../../shared/middleware/kore-reversion-auth');
const { PERMISSIONS }                  = require('../../../shared/config/rbac');
const { logger }                       = require('../../../shared/utils/logger');
const {
  procesarReversionKore, revertirReversion, listarReversiones,
} = require('./erp-reversion.service');

const router = express.Router();

const REFERENCIA_REGEX = /^[0-9a-fA-F]{24}$/;

// POST /api/erp/cxc-reversiones — webhook server-to-server que consume Kore cuando revierte
// o cancela una CxC que ya tenemos vinculada a un depósito bancario. Autenticado con
// X-Api-Key (verifyKoreApiKey), NUNCA con authenticate/permit — Kore no tiene sesión Numo.
// No debe filtrar detalle interno a un llamador externo: cualquier excepción no esperada se
// atrapa aquí mismo y responde 500 genérico, con el detalle real solo en el log.
router.post('/', verifyKoreApiKey, asyncHandler(async (req, res) => {
  const { erpId, referencia, motivo, fecha, serieExterna, folioExterno } = req.body;

  if (!erpId || typeof erpId !== 'string' || !erpId.trim()) {
    return res.status(400).json({ error: 'Se requiere erpId.' });
  }
  // serieExterna/folioExterno pasaron a ser obligatorios (2026-08-10): combinados son por sí
  // solos el identificador fiscal de la CxC, y le dan contenido real al rastro de auditoría
  // (ErpReversion) además de habilitar el cruce local contra lo ya guardado en erpLinks
  // (serieFolioMismatch) en todos los casos, no solo cuando Kore decide mandarlos.
  if (!serieExterna || typeof serieExterna !== 'string' || !serieExterna.trim()) {
    return res.status(400).json({ error: 'Se requiere serieExterna.' });
  }
  if (!folioExterno || typeof folioExterno !== 'string' || !folioExterno.trim()) {
    return res.status(400).json({ error: 'Se requiere folioExterno.' });
  }
  if (referencia !== undefined && referencia !== null && !REFERENCIA_REGEX.test(String(referencia))) {
    return res.status(400).json({ error: 'referencia debe ser un ObjectId válido (24 caracteres hexadecimales).' });
  }

  try {
    const { movimientosAfectados, yaEstabaDesvinculada } = await procesarReversionKore({
      erpId: erpId.trim(), motivo, fecha, serieExterna, folioExterno, referencia, payloadOriginal: req.body,
    });
    res.json({ ok: true, movimientosAfectados, yaEstabaDesvinculada });
  } catch (err) {
    logger.error(`[erp-reversion] Error al procesar reversión de Kore (erpId=${erpId}): ${err.message}`, { stack: err.stack });
    res.status(500).json({ error: 'Error interno al procesar la reversión.' });
  }
}));

// GET /api/erp/cxc-reversiones — bandeja para la UI (auditoría de reversiones aplicadas por Kore).
router.get('/', authenticate, permit(PERMISSIONS.BANKS_ERP_REVERSIONES), asyncHandler(async (req, res) => {
  const { page, estado, q } = req.query;
  res.json(await listarReversiones({ page, estado, q }));
}));

// POST /api/erp/cxc-reversiones/:id/revertir — deshace una reversión aplicada por error.
// NO le avisa nada a Kore, es una corrección puramente de nuestro lado.
router.post('/:id/revertir', authenticate, permit(PERMISSIONS.BANKS_ERP_REVERSIONES), asyncHandler(async (req, res) => {
  res.json(await revertirReversion(req.params.id, req.user));
}));

module.exports = router;
