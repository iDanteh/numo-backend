'use strict';

const express = require('express');
const { authenticate, permit } = require('../../shared/middleware/auth.real');
const { asyncHandler }         = require('../../shared/middleware/error-handler');
const service                  = require('./poliza.service');

const router = express.Router();

// GET /api/polizas?rfc=&ejercicio=&periodo=&tipo=&estado=&page=&limit=
router.get('/',
  authenticate,
  permit('polizas:read'),
  asyncHandler(async (req, res) => {
    res.json(await service.list(req.query));
  }),
);

// GET /api/polizas/xml-sat?rfc=&ejercicio=&periodo=&tipoSolicitud=AF&numOrden=&numTramite=
router.get('/xml-sat',
  authenticate,
  permit('polizas:read'),
  asyncHandler(async (req, res) => {
    const { rfc, ejercicio, periodo, tipoSolicitud, numOrden, numTramite } = req.query;
    const xml = await service.generarXmlSat({ rfc, ejercicio, periodo, tipoSolicitud, numOrden, numTramite });
    const mes = String(Number(periodo)).padStart(2, '0');
    res.setHeader('Content-Type', 'application/xml; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="Polizas_${ejercicio}_${mes}_${rfc}.xml"`);
    res.send(xml);
  }),
);

// GET /api/polizas/:id
router.get('/:id',
  authenticate,
  permit('polizas:read'),
  asyncHandler(async (req, res) => {
    res.json(await service.getById(req.params.id));
  }),
);

// POST /api/polizas
router.post('/',
  authenticate,
  permit('polizas:write'),
  asyncHandler(async (req, res) => {
    res.status(201).json(await service.create(req.body, req.user));
  }),
);

// PATCH /api/polizas/:id
router.patch('/:id',
  authenticate,
  permit('polizas:write'),
  asyncHandler(async (req, res) => {
    res.json(await service.update(req.params.id, req.body, req.user));
  }),
);

// POST /api/polizas/:id/contabilizar
router.post('/:id/contabilizar',
  authenticate,
  permit('polizas:write'),
  asyncHandler(async (req, res) => {
    res.json(await service.contabilizar(req.params.id, req.user));
  }),
);

// POST /api/polizas/:id/cancelar
router.post('/:id/cancelar',
  authenticate,
  permit('polizas:write'),
  asyncHandler(async (req, res) => {
    res.json(await service.cancel(req.params.id, req.user, req.body?.motivo));
  }),
);

// POST /api/polizas/:id/revertir  (solo admin)
router.post('/:id/revertir',
  authenticate,
  permit('polizas:admin'),
  asyncHandler(async (req, res) => {
    res.json(await service.revertir(req.params.id, req.user, req.body?.motivo));
  }),
);

module.exports = router;
