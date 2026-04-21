const express = require('express');
const { authenticate } = require('../../shared/middleware/auth');
const { dashboard, exportExcel, debugMontos, discrepanciasMontos, debugDiscrepanciasMontos, satVigenteErpInactivo } = require('../controllers/report.controller');

const router = express.Router();

router.get('/dashboard', authenticate, dashboard);
router.get('/export/excel', authenticate, exportExcel);
router.get('/debug-montos', authenticate, debugMontos);
router.get('/discrepancias-montos', authenticate, discrepanciasMontos);
router.get('/debug-discrepancias-montos', authenticate, debugDiscrepanciasMontos);
router.get('/sat-vigente-erp-inactivo', authenticate, satVigenteErpInactivo);

module.exports = router;
