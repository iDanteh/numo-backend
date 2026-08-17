'use strict';

const express      = require('express');
const { authenticate, permit } = require('../../shared/middleware/auth.real');
const { asyncHandler }         = require('../../shared/middleware/error-handler');
const svc                      = require('./cfdi-mapping.service');
const generator                = require('./cfdi-poliza-generator.service');
const balanza                  = require('./balanza-preliminar.service');
const balanceGeneral           = require('./balance-general.service');
const polizaExportZip          = require('../polizas/poliza-export-zip.service');

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

// GET /api/cfdi-mapping/rules/:id/polizas
router.get('/rules/:id/polizas',
  authenticate,
  permit('polizas:write'),
  asyncHandler(async (req, res) => res.json(await svc.getRulePolizas(req.params.id))),
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

// POST /api/cfdi-mapping/rules/migrar-ppd-descuento
// Aplica en BD la corrección de reglas PPD con descuento (idempotente).
router.post('/rules/migrar-ppd-descuento',
  authenticate,
  permit('polizas:admin'),
  asyncHandler(async (_req, res) => res.json(await svc.migrarPpdDescuento())),
);

// ── Generador de propuesta ────────────────────────────────────────────────────

// POST /api/cfdi-mapping/generar-propuesta
// Body: { rfc, ejercicio, periodo, tipoPropuesta?, tipoCfdi, fechaInicio?, fechaFin?, formaPagoFiltro? }
// fechaInicio/fechaFin: si se mandan (ambos), acotan el periodo a ese rango de
// días en vez de procesar el mes completo.
// formaPagoFiltro: solo aplica si tipoCfdi='P' (Cobranza) — 'EFECTIVO'|'TRANSFERENCIA'|
// 'TARJETA'|'CHEQUE', ver FORMA_PAGO_A_CATEGORIA en cfdi-poliza-generator.service.js.
router.post('/generar-propuesta',
  authenticate,
  permit('polizas:write'),
  asyncHandler(async (req, res) => res.json(await generator.generarPropuesta(req.body))),
);

// POST /api/cfdi-mapping/generar-y-guardar
// Guarda la póliza directo como borrador (sin límite de CFDIs).
// Body: { rfc, ejercicio, periodo, tipoPropuesta?, tipoCfdi, centroCostoId?, fechaInicio?, fechaFin?, formaPagoFiltro? }
// centroCostoId: si se manda, solo procesa CFDIs de esa sucursal. Si se omite,
// procesa todas las sucursales mezcladas en una sola póliza (comportamiento previo).
// fechaInicio/fechaFin/formaPagoFiltro: mismo filtro opcional que en generar-propuesta.
// Response: { polizaId, totalCfdis, sinRegla, advertencias }
router.post('/generar-y-guardar',
  authenticate,
  permit('polizas:write'),
  asyncHandler(async (req, res) => res.status(201).json(await generator.generarYGuardar(req.body))),
);

// POST /api/cfdi-mapping/generar-y-guardar-por-sucursal
// Genera una póliza SEPARADA por cada sucursal (centro de costo) que tenga
// CFDIs sin póliza en el periodo, en vez de una sola póliza con todo mezclado.
// Body: { rfc, ejercicio, periodo, tipoPropuesta?, tipoCfdi, fechaInicio?, fechaFin?, formaPagoFiltro? }
// Response: { resultados: [{ centroCosto, centroCostoId, polizaId?, totalCfdis?, sinRegla?, error? }] }
router.post('/generar-y-guardar-por-sucursal',
  authenticate,
  permit('polizas:write'),
  asyncHandler(async (req, res) => res.status(201).json(await generator.generarYGuardarPorSucursal(req.body))),
);

// POST /api/cfdi-mapping/generar-y-guardar-por-dia
// Genera una póliza SEPARADA por cada día del rango (o del mes completo si no
// se manda fechaInicio/fechaFin) que tenga CFDIs sin póliza.
// Body: { rfc, ejercicio, periodo, tipoPropuesta?, tipoCfdi, centroCostoId?, fechaInicio?, fechaFin?, formaPagoFiltro? }
// Response: { resultados: [{ fecha, polizaId?, totalCfdis?, sinRegla?, error? }] }
router.post('/generar-y-guardar-por-dia',
  authenticate,
  permit('polizas:write'),
  asyncHandler(async (req, res) => res.status(201).json(await generator.generarYGuardarPorDia(req.body))),
);

// POST /api/cfdi-mapping/exportar-contpaq-zip
// Genera (si hace falta) las pólizas del modo pedido y regresa un ZIP con el
// .xlsx de CONTPAQ de cada una — carpeta por sucursal cuando el modo incluye
// sucursal, un archivo por día cuando incluye día, más un _resumen.txt con
// éxitos/errores de cada combinación (no cabe un JSON aparte junto al binario).
// Body: { rfc, ejercicio, periodo, tipoCfdi, tipoPropuesta?, modo: 'porSucursal'|'porDia'|'porDiaYSucursal', fechaInicio?, fechaFin?, formaPagoFiltro? }
router.post('/exportar-contpaq-zip',
  authenticate,
  permit('polizas:write'),
  asyncHandler(async (req, res) => {
    const { buffer, nombreZip } = await polizaExportZip.exportarContpaqZip(req.body);
    res.setHeader('Content-Type', 'application/zip');
    res.setHeader('Content-Disposition', `attachment; filename="${nombreZip}"`);
    res.setHeader('Content-Length', buffer.length);
    res.send(buffer);
  }),
);

// GET /api/cfdi-mapping/balanza-preliminar
// Query: rfc, ejercicio, periodo, tipoCfdi? (I|E|P — omitir = todos), excluirPagosSustitutos? (true)
router.get('/balanza-preliminar',
  authenticate,
  permit('polizas:write'),
  asyncHandler(async (req, res) => {
    const { rfc, ejercicio, periodo, tipoCfdi, excluirPagosSustitutos, excluirAplicacionesAnticipos, excluirReclasificaciones, incluirFechaCruzada, excluirMesesPosteriores } = req.query;
    res.json(await balanza.generarBalanzaPreliminar({
      rfc, ejercicio, periodo, tipoCfdi,
      excluirPagosSustitutos:       excluirPagosSustitutos      === 'true',
      excluirAplicacionesAnticipos: excluirAplicacionesAnticipos === 'true',
      excluirReclasificaciones:     excluirReclasificaciones     === 'true',
      incluirFechaCruzada:          incluirFechaCruzada          === 'true',
      excluirMesesPosteriores:      excluirMesesPosteriores      === 'true',
    }));
  }),
);

// GET /api/cfdi-mapping/balanza-cuenta-cfdis
// Drill-down: devuelve los CFDIs que generan movimientos en una cuenta específica.
// Query: mismos params que /balanza-preliminar + cuentaCodigo (requerido)
router.get('/balanza-cuenta-cfdis',
  authenticate,
  permit('polizas:write'),
  asyncHandler(async (req, res) => {
    const { rfc, ejercicio, periodo, tipoCfdi, cuentaCodigo,
            excluirPagosSustitutos, excluirAplicacionesAnticipos, excluirReclasificaciones,
            incluirFechaCruzada, excluirMesesPosteriores } = req.query;
    res.json(await balanza.generarDetalleCuenta({
      rfc, ejercicio, periodo, tipoCfdi, cuentaCodigo,
      excluirPagosSustitutos:       excluirPagosSustitutos      === 'true',
      excluirAplicacionesAnticipos: excluirAplicacionesAnticipos === 'true',
      excluirReclasificaciones:     excluirReclasificaciones     === 'true',
      incluirFechaCruzada:          incluirFechaCruzada          === 'true',
      excluirMesesPosteriores:      excluirMesesPosteriores      === 'true',
    }));
  }),
);

// GET /api/cfdi-mapping/balanza-detalle-export
// Devuelve lista plana de todos los movimientos CFDI→cuenta del periodo para el export Excel.
router.get('/balanza-detalle-export',
  authenticate,
  permit('polizas:write'),
  asyncHandler(async (req, res) => {
    const { rfc, ejercicio, periodo, tipoCfdi,
            excluirPagosSustitutos, excluirAplicacionesAnticipos, excluirReclasificaciones,
            incluirFechaCruzada, excluirMesesPosteriores } = req.query;
    res.json(await balanza.generarDetalleExport({
      rfc, ejercicio, periodo, tipoCfdi,
      excluirPagosSustitutos:       excluirPagosSustitutos      === 'true',
      excluirAplicacionesAnticipos: excluirAplicacionesAnticipos === 'true',
      excluirReclasificaciones:     excluirReclasificaciones     === 'true',
      incluirFechaCruzada:          incluirFechaCruzada          === 'true',
      excluirMesesPosteriores:      excluirMesesPosteriores      === 'true',
    }));
  }),
);

// GET /api/cfdi-mapping/reporte-sustitutos
// Devuelve los CFDIs tipo I/E con tipoRelacion='04' (sustitutos) del periodo.
// Úsalo para identificar cuáles no están capturados en CONTPAQi.
router.get('/reporte-sustitutos',
  authenticate,
  permit('polizas:write'),
  asyncHandler(async (req, res) => {
    const { rfc, ejercicio, periodo } = req.query;
    if (!rfc || !ejercicio || !periodo) {
      return res.status(400).json({ error: 'rfc, ejercicio y periodo son requeridos' });
    }
    const CFDI = require('../../../visor/models/CFDI');

    // 1. Todos los CFDIs SAT del mes (I, E, P) — sin filtrar por tipoRelacion todavía
    const satCfdis = await CFDI.find({
      'emisor.rfc': rfc,
      ejercicio:    Number(ejercicio),
      satStatus:    'Vigente',
      source:       'SAT',
      isActive:     true,
      tipoDeComprobante: { $in: ['I', 'E', 'P'] },
      $expr: { $eq: [{ $month: '$fecha' }, Number(periodo)] },
    })
      .select('uuid serie folio tipoDeComprobante metodoPago formaPago fecha subTotal total emisor.rfc receptor.rfc receptor.nombre cfdiRelacionados')
      .lean();

    // 2. De los que no tienen tipoRelacion='04', buscar su homólogo ERP para enriquecer
    const sinRel04 = satCfdis
      .filter(c => !c.cfdiRelacionados?.some(r => r.tipoRelacion === '04'))
      .map(c => c.uuid);

    let erpMap = {};
    if (sinRel04.length) {
      const erpCfdis = await CFDI.find({
        uuid:     { $in: sinRel04 },
        source:   'ERP',
        isActive: true,
        'cfdiRelacionados.tipoRelacion': '04',
      }).select('uuid cfdiRelacionados').lean();
      erpMap = Object.fromEntries(erpCfdis.map(c => [c.uuid, c]));
    }

    // 3. Enriquecer SAT con la relación '04' del ERP si la SAT no la trajo
    const enriquecidos = satCfdis.map(c => {
      const erp = erpMap[c.uuid];
      if (!erp) return c;
      const rel04ERP = erp.cfdiRelacionados?.filter(r => r.tipoRelacion === '04') ?? [];
      if (!rel04ERP.length) return c;
      return {
        ...c,
        cfdiRelacionados: [...(c.cfdiRelacionados ?? []), ...rel04ERP],
        _enriquecido: true,
      };
    });

    // 4. Filtrar solo los que tienen tipoRelacion='04'
    const cfdis = enriquecidos
      .filter(c => c.cfdiRelacionados?.some(r => r.tipoRelacion === '04'))
      .sort((a, b) => a.tipoDeComprobante.localeCompare(b.tipoDeComprobante) ||
                      new Date(a.fecha).getTime() - new Date(b.fecha).getTime());

    const toRow = c => ({
      uuid:           c.uuid,
      serie:          c.serie ?? '',
      folio:          c.folio ?? '',
      tipoComprobante: c.tipoDeComprobante,
      metodoPago:     c.metodoPago ?? '',
      formaPago:      c.formaPago  ?? '',
      fecha:          c.fecha,
      subTotal:       c.subTotal,
      total:          c.total,
      rfcEmisor:      c.emisor?.rfc    ?? '',
      rfcReceptor:    c.receptor?.rfc  ?? '',
      nombreReceptor: c.receptor?.nombre ?? '',
      uuidOriginal:   c.cfdiRelacionados?.find(r => r.tipoRelacion === '04')?.uuids?.[0] ?? '',
      enriquecido:    c._enriquecido ?? false,
    });

    res.json({
      total:    cfdis.length,
      ingresos: cfdis.filter(c => c.tipoDeComprobante === 'I').map(toRow),
      egresos:  cfdis.filter(c => c.tipoDeComprobante === 'E').map(toRow),
      pagos:    cfdis.filter(c => c.tipoDeComprobante === 'P').map(toRow),
    });
  }),
);

// GET /api/cfdi-mapping/reporte-anticipos
// Devuelve los CFDIs tipo I/E con tipoRelacion='07' (aplicaciones de anticipo) del periodo.
router.get('/reporte-anticipos',
  authenticate,
  permit('polizas:write'),
  asyncHandler(async (req, res) => {
    const { rfc, ejercicio, periodo } = req.query;
    if (!rfc || !ejercicio || !periodo) {
      return res.status(400).json({ error: 'rfc, ejercicio y periodo son requeridos' });
    }
    const CFDI = require('../../../visor/models/CFDI');
    const cfdis = await CFDI.find({
      'emisor.rfc':              rfc,
      ejercicio:                 Number(ejercicio),
      periodo:                   Number(periodo),
      satStatus:                 'Vigente',
      source:                    'SAT',
      isActive:                  true,
      'cfdiRelacionados.tipoRelacion': '07',
      tipoDeComprobante:         { $in: ['I', 'E'] },
    })
      .select('uuid serie folio tipoDeComprobante metodoPago formaPago fecha subTotal total emisor.rfc receptor.rfc receptor.nombre cfdiRelacionados')
      .sort({ fecha: 1 })
      .lean();

    res.json({
      total: cfdis.length,
      anticipos: cfdis.map(c => ({
        uuid:           c.uuid,
        serie:          c.serie ?? '',
        folio:          c.folio ?? '',
        tipoComprobante: c.tipoDeComprobante,
        metodoPago:     c.metodoPago ?? '',
        formaPago:      c.formaPago ?? '',
        fecha:          c.fecha,
        subTotal:       c.subTotal,
        total:          c.total,
        rfcEmisor:      c.emisor?.rfc ?? '',
        rfcReceptor:    c.receptor?.rfc ?? '',
        nombreReceptor: c.receptor?.nombre ?? '',
        uuidRelacionado: c.cfdiRelacionados?.[0]?.uuids?.[0] ?? '',
        tipoRelacion:   c.cfdiRelacionados?.[0]?.tipoRelacion ?? '',
      })),
    });
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
