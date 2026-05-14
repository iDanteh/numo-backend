const express = require('express');
const { authenticate, permit } = require('../../shared/middleware/auth');
const { dashboard, exportExcel, discrepanciasMontos, satVigenteErpInactivo, discrepanciasCriticas, notInErp, pagosRelacionados, conciliacionExcel } = require('../controllers/report.controller');

const router = express.Router();

router.get('/dashboard', authenticate, dashboard);
router.get('/export/excel', authenticate, exportExcel);
router.get('/discrepancias-montos', authenticate, discrepanciasMontos);
router.get('/sat-vigente-erp-inactivo', authenticate, satVigenteErpInactivo);
router.get('/discrepancias-criticas', authenticate, discrepanciasCriticas);
router.get('/not-in-erp', authenticate, notInErp);
router.get('/pagos-relacionados', authenticate, pagosRelacionados);
router.get('/conciliacion-excel', authenticate, conciliacionExcel);

module.exports = router;
