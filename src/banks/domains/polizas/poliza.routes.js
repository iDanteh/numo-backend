'use strict';

const express = require('express');
const AdmZip  = require('adm-zip');
const { authenticate, permit } = require('../../shared/middleware/auth.real');
const { asyncHandler }         = require('../../shared/middleware/error-handler');
const service                  = require('./poliza.service');

// Sin caracteres fuera de [A-Za-z0-9_-] en nombres de archivo dentro del ZIP —
// mismo criterio que poliza-export-zip.service.js.
const sanitize = (s) => String(s ?? '').replace(/[^\w-]+/g, '_');

const router = express.Router();

// GET /api/polizas?rfc=&ejercicio=&periodo=&tipo=&estado=&page=&limit=
router.get('/',
  authenticate,
  permit('polizas:read'),
  asyncHandler(async (req, res) => {
    res.json(await service.list(req.query));
  }),
);

// GET /api/polizas/reporte-descuadradas?rfc=&ejercicio=&periodo=&estado=&format=csv
router.get('/reporte-descuadradas',
  authenticate,
  permit('polizas:read'),
  asyncHandler(async (req, res) => {
    const { format, ...filters } = req.query;
    const rows = await service.reporteDescuadradas(filters);

    if (format === 'csv') {
      const csv = _toCsv(rows);
      const mes = filters.periodo  ? String(Number(filters.periodo)).padStart(2, '0') : 'XX';
      const ej  = filters.ejercicio ?? 'XXXX';
      res.setHeader('Content-Type', 'text/csv; charset=utf-8');
      res.setHeader('Content-Disposition', `attachment; filename="DescuadradosCFDI_${ej}_${mes}_${filters.rfc}.csv"`);
      return res.send('\uFEFF' + csv);  // BOM para Excel
    }

    res.json({ total: rows.length, rows });
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

// GET /api/polizas/:id/export-contpaq?fecha=&folioContado=&folioCredito=&conceptoContado=&conceptoCredito=&centroCostoIds=1,2,3
router.get('/:id/export-contpaq',
  authenticate,
  permit('polizas:read'),
  asyncHandler(async (req, res) => {
    const { fecha, folioContado, folioCredito, conceptoContado, conceptoCredito, centroCostoIds } = req.query;
    const overrides = {
      fecha:           fecha || undefined,
      folioContado:    folioContado    != null ? parseInt(folioContado, 10)    : undefined,
      folioCredito:    folioCredito    != null ? parseInt(folioCredito, 10)    : undefined,
      conceptoContado: conceptoContado || undefined,
      conceptoCredito: conceptoCredito || undefined,
      centroCostoIds:  centroCostoIds
        ? String(centroCostoIds).split(',').map(v => parseInt(v, 10)).filter(v => !Number.isNaN(v))
        : undefined,
    };
    const { workbooks, poliza } = await service.exportContpaqXlsx(req.params.id, overrides);
    const mes = String(poliza.periodo).padStart(2, '0');

    if (workbooks.length === 1) {
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.setHeader('Content-Disposition', `attachment; filename="Poliza_${poliza.tipo}${poliza.numero}_${poliza.ejercicio}${mes}_CONTPAQ.xlsx"`);
      await workbooks[0].workbook.xlsx.write(res);
      return;
    }

    // CEDIS: más de un bloque → un .xlsx por bloque, entregados juntos en un ZIP.
    const zip = new AdmZip();
    for (const { tipoVenta, workbook } of workbooks) {
      const buffer = await workbook.xlsx.writeBuffer();
      zip.addFile(`Poliza_${poliza.tipo}${poliza.numero}_${poliza.ejercicio}${mes}_${sanitize(tipoVenta)}_CONTPAQ.xlsx`, Buffer.from(buffer));
    }
    res.setHeader('Content-Type', 'application/zip');
    res.setHeader('Content-Disposition', `attachment; filename="Poliza_${poliza.tipo}${poliza.numero}_${poliza.ejercicio}${mes}_CONTPAQ.zip"`);
    res.send(zip.toBuffer());
  }),
);

// PATCH /api/polizas/:id/contpaq-folio
// Body: { folioContado, folioCredito }
router.patch('/:id/contpaq-folio',
  authenticate,
  permit('polizas:write'),
  asyncHandler(async (req, res) => {
    res.json(await service.asociarFolioContpaq(req.params.id, req.body, req.user));
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

// POST /api/polizas/cancelar-todas
// Cancela todas las pólizas en estado 'borrador' del rfc/ejercicio/periodo
// (las contabilizadas y ya canceladas quedan fuera — se cancelan una por una).
// Body: { rfc, ejercicio, periodo, motivo? }
// Response: { canceladas, total, errores: [{ polizaId, numero, tipo, error }] }
router.post('/cancelar-todas',
  authenticate,
  permit('polizas:write'),
  asyncHandler(async (req, res) => {
    const { rfc, ejercicio, periodo, motivo } = req.body;
    res.json(await service.cancelarTodas({ rfc, ejercicio, periodo }, req.user, motivo));
  }),
);

// POST /api/polizas/cierre-iva?rfc=&ejercicio=&periodo=
router.post('/cierre-iva',
  authenticate,
  permit('polizas:write'),
  asyncHandler(async (req, res) => {
    const { rfc, ejercicio, periodo } = req.query;
    res.status(201).json(await service.generarCierreIVA({ rfc, ejercicio, periodo, user: req.user }));
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

function _toCsv(rows) {
  const headers = [
    'PolizaId', 'Tipo', 'Numero', 'FechaPoliza', 'EstadoPoliza',
    'CfdiUuid', 'TipoCfdi', 'Serie', 'Folio', 'FechaCfdi',
    'EmisorRfc', 'EmisorNombre', 'ReceptorRfc', 'ReceptorNombre',
    'MetodoPago', 'FormaPago', 'Subtotal', 'Total',
    'TotalDebe', 'TotalHaber', 'Diferencia', 'SatStatus', 'Fuentes',
  ];
  const esc = v => {
    const s = String(v ?? '');
    return (s.includes(',') || s.includes('"') || s.includes('\n'))
      ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const lines = [headers.join(',')];
  for (const r of rows) {
    const c = r.cfdi;
    lines.push([
      r.polizaId, r.tipo, r.numero, r.fecha, r.estado,
      r.cfdiUuid,
      c?.tipoDeComprobante ?? '',
      c?.serie             ?? '',
      c?.folio             ?? '',
      c?.fecha ? new Date(c.fecha).toISOString().slice(0, 10) : '',
      c?.emisor?.rfc       ?? '',
      c?.emisor?.nombre    ?? '',
      c?.receptor?.rfc     ?? '',
      c?.receptor?.nombre  ?? '',
      c?.metodoPago        ?? '',
      c?.formaPago         ?? '',
      c?.subTotal          ?? '',
      c?.total             ?? '',
      r.totalDebe, r.totalHaber, r.diferencia,
      c?.satStatus         ?? '',
      (c?.sources ?? []).join('|'),
    ].map(esc).join(','));
  }
  return lines.join('\n');
}

module.exports = router;
