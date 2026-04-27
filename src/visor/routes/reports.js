const express = require('express');
const { authenticate, permit } = require('../../shared/middleware/auth');
const { dashboard, exportExcel, debugMontos, discrepanciasMontos, debugDiscrepanciasMontos, satVigenteErpInactivo, discrepanciasCriticas, notInErp, pagosRelacionados } = require('../controllers/report.controller');

const router = express.Router();

router.get('/dashboard', authenticate, dashboard);
router.get('/export/excel', authenticate, exportExcel);
router.get('/debug-montos', authenticate, debugMontos);
router.get('/discrepancias-montos', authenticate, discrepanciasMontos);
router.get('/debug-discrepancias-montos', authenticate, debugDiscrepanciasMontos);
router.get('/sat-vigente-erp-inactivo', authenticate, satVigenteErpInactivo);
router.get('/discrepancias-criticas', authenticate, discrepanciasCriticas);
router.get('/not-in-erp', authenticate, notInErp);
router.get('/pagos-relacionados', authenticate, pagosRelacionados);

module.exports = router;
