'use strict';

const express      = require('express');
const { authenticate, permit } = require('../../shared/middleware/auth.real');
const { asyncHandler }         = require('../../shared/middleware/error-handler');
const svc                      = require('./cfdi-mapping.service');
const generator                = require('./cfdi-poliza-generator.service');
const balanza                  = require('./balanza-preliminar.service');
const balanceGeneral           = require('./balance-general.service');

const router = express.Router();

// ── CRUD reglas de mapeo ──────────────────────────────────────────────────────

// GET /api/cfdi-mapping/rules
router.get('/rules',
  authenticate,
  permit('polizas:write'),
  asyncHandler(async (_req, res) => res.json(await svc.list())),
);

// GET /api/cfdi-mapping/rules/:id
router.get('/rules/:id',
  authenticate,
  permit('polizas:write'),
  asyncHandler(async (req, res) => res.json(await svc.getById(req.params.id))),
);

// POST /api/cfdi-mapping/rules
router.post('/rules',
  authenticate,
  permit('polizas:admin'),
  asyncHandler(async (req, res) => res.status(201).json(await svc.create(req.body))),
);

// PATCH /api/cfdi-mapping/rules/:id
router.patch('/rules/:id',
  authenticate,
  permit('polizas:admin'),
  asyncHandler(async (req, res) => res.json(await svc.update(req.params.id, req.body))),
);

// DELETE /api/cfdi-mapping/rules/:id
router.delete('/rules/:id',
  authenticate,
  permit('polizas:admin'),
  asyncHandler(async (req, res) => { await svc.remove(req.params.id); res.status(204).end(); }),
);

// ── Generador de propuesta ────────────────────────────────────────────────────

// POST /api/cfdi-mapping/generar-propuesta
// Body: { rfc, ejercicio, periodo, tipoPropuesta?, tipoCfdi }
router.post('/generar-propuesta',
  authenticate,
  permit('polizas:write'),
  asyncHandler(async (req, res) => res.json(await generator.generarPropuesta(req.body))),
);

// POST /api/cfdi-mapping/generar-y-guardar
// Guarda la póliza directo como borrador (sin límite de CFDIs).
// Body: { rfc, ejercicio, periodo, tipoPropuesta?, tipoCfdi }
// Response: { polizaId, totalCfdis, sinRegla, advertencias }
router.post('/generar-y-guardar',
  authenticate,
  permit('polizas:write'),
  asyncHandler(async (req, res) => res.status(201).json(await generator.generarYGuardar(req.body))),
);

// GET /api/cfdi-mapping/balanza-preliminar
// Query: rfc, ejercicio, periodo, tipoCfdi? (I|E|P — omitir = todos)
router.get('/balanza-preliminar',
  authenticate,
  permit('polizas:write'),
  asyncHandler(async (req, res) => {
    const { rfc, ejercicio, periodo, tipoCfdi } = req.query;
    res.json(await balanza.generarBalanzaPreliminar({ rfc, ejercicio, periodo, tipoCfdi }));
  }),
);

// GET /api/cfdi-mapping/balance-general
// Query: rfc, ejercicio, periodo
router.get('/balance-general',
  authenticate,
  permit('polizas:write'),
  asyncHandler(async (req, res) => {
    const { rfc, ejercicio, periodo } = req.query;
    res.json(await balanceGeneral.generarBalanceGeneral({ rfc, ejercicio, periodo }));
  }),
);

module.exports = router;
