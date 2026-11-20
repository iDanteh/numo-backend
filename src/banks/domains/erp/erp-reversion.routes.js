'use strict';

const express = require('express');
const { authenticate, permit }         = require('../../shared/middleware/auth.real');
const { asyncHandler }                 = require('../../shared/middleware/error-handler');
const { verifyKoreApiKey }             = require('../../../shared/middleware/kore-api-key-auth');
const { PERMISSIONS }                  = require('../../../shared/config/rbac');
const { logger }                       = require('../../../shared/utils/logger');
const BankMovement                     = require('../banks/BankMovement.model');
const {
  procesarReversionKore, listarReversiones,
} = require('./erp-reversion.service');

const router = express.Router();

const REFERENCIA_REGEX = /^[0-9a-fA-F]{24}$/;

// POST /api/erp/cxc-reversiones — webhook server-to-server que consume Kore cuando revierte
// o cancela una CxC que ya tenemos vinculada a un depósito bancario. Autenticado con
// X-Api-Key (verifyKoreApiKey), NUNCA con authenticate/permit — Kore no tiene sesión Numo.
// No debe filtrar detalle interno a un llamador externo: cualquier excepción no esperada se
// atrapa aquí mismo y responde 500 genérico, con el detalle real solo en el log.
/**
 * @swagger
 * /api/erp/cxc-reversiones:
 *   post:
 *     tags: [ERP - Reversiones]
 *     summary: Avisa que Kore revirtió/canceló una CxC vinculada a un depósito bancario
 *     description: >
 *       Responde de inmediato con el conteo de movimientos afectados (ya se conoce sin
 *       reconsultar Kore) y sigue el procesamiento real (reconsulta + ajuste + auditoría) en
 *       segundo plano, sin mantener la conexión de Kore abierta.
 *     security:
 *       - apiKeyAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [erpId, serieExterna, folioExterno]
 *             properties:
 *               erpId:
 *                 type: string
 *                 description: ID de la CxC en Kore que se revirtió/canceló.
 *               serieExterna:
 *                 type: string
 *               folioExterno:
 *                 type: string
 *               referencia:
 *                 type: string
 *                 description: ObjectId (24 hex) del movimiento bancario, si Kore lo conoce.
 *               motivo:
 *                 type: string
 *               fecha:
 *                 type: string
 *           example:
 *             erpId: "5fbd63472ab0c60001c64692"
 *             serieExterna: "A0"
 *             folioExterno: "260605320"
 *             motivo: "Cancelación de factura"
 *             fecha: "2026-11-19T17:27:55.620246Z"
 *     responses:
 *       200:
 *         description: Reversión recibida — el procesamiento real continúa en segundo plano.
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *             example:
 *               ok: true
 *               movimientosAfectados: 1
 *               yaEstabaDesvinculada: false
 *       400:
 *         description: Falta erpId/serieExterna/folioExterno, o referencia no es un ObjectId válido.
 *       401:
 *         description: X-Api-Key inválida o faltante.
 */
router.post('/', verifyKoreApiKey, asyncHandler(async (req, res) => {
  // 2026-08-20 (pedido explícito del usuario): log crudo del body ANTES de cualquier
  // validación — sin esto no quedaba ningún rastro en consola de lo que Kore realmente
  // mandó cuando el request se rechaza (400) o cuando "yaEstabaDesvinculada" (200 pero
  // sin persistir ErpReversion, ver procesarReversionKore). El payload COMPLETO también
  // queda guardado en ErpReversion.payloadOriginal cuando SÍ hay movimientos afectados —
  // consultable vía GET /api/erp/cxc-reversiones (bandeja) o directo en Mongo
  // (`db.erpreversions.find({erpId:'...'}).sort({createdAt:-1})`).
  console.log('[erp-reversion] payload recibido de Kore →', JSON.stringify(req.body));

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

  // 2026-08-21 (fix real, reportado por el usuario): Kore aparentemente llama a este webhook
  // de forma SÍNCRONA como parte de su propia transacción de reversión — al agregar el retry
  // con backoff de erp-reversion.service.js (hasta 90s bloqueando la respuesta), el cliente
  // HTTP de Kore hacía timeout esperando, Kore abortaba SU PROPIA reversión y le devolvía al
  // usuario "No se pudo revertir el movimiento" — aunque nuestro procesamiento terminara bien
  // segundos/minutos después. Antes del retry (una sola consulta, respuesta casi instantánea)
  // esto nunca pasaba. Fix: responder de inmediato con el conteo (ya se conoce sin reconsultar
  // Kore) y seguir el procesamiento real (reconsulta+retry+ajuste+auditoría) en segundo plano,
  // sin que la conexión de Kore tenga que seguir abierta para eso.
  try {
    const movimientosAfectados = await BankMovement.countDocuments({ erpIds: erpId.trim() });
    res.json({ ok: true, movimientosAfectados, yaEstabaDesvinculada: movimientosAfectados === 0 });

    procesarReversionKore({
      erpId: erpId.trim(), motivo, fecha, serieExterna, folioExterno, referencia, payloadOriginal: req.body,
    }).catch(err => {
      logger.error(`[erp-reversion] Error al procesar reversión de Kore en segundo plano (erpId=${erpId}): ${err.message}`, { stack: err.stack });
    });
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

module.exports = router;
