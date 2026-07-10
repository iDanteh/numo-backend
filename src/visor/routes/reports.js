const express = require('express');
const { authenticate, permit } = require('../../shared/middleware/auth');
const { dashboard, exportExcel, discrepanciasMontos, satVigenteErpInactivo, discrepanciasCriticas, notInErp, pagosRelacionados, conciliacionExcel, pagosBanco, pagosBancoDetalle, pagosBancoExport, pagosBancosDistintos, pagosBancoContextoBanco, depositosIngresos, depositosIngresosDetalle, depositosIngresosExport } = require('../controllers/report.controller');

const router = express.Router();

router.get('/dashboard', authenticate, dashboard);
router.get('/export/excel', authenticate, exportExcel);
router.get('/discrepancias-montos', authenticate, discrepanciasMontos);
router.get('/sat-vigente-erp-inactivo', authenticate, satVigenteErpInactivo);
router.get('/discrepancias-criticas', authenticate, discrepanciasCriticas);
router.get('/not-in-erp', authenticate, notInErp);
router.get('/pagos-relacionados', authenticate, pagosRelacionados);
router.get('/conciliacion-excel', authenticate, conciliacionExcel);
router.get('/pagos-banco', authenticate, pagosBanco);
router.get('/pagos-banco/detalle', authenticate, pagosBancoDetalle);
router.get('/pagos-banco/export', authenticate, pagosBancoExport);
router.get('/pagos-banco/contexto-banco', authenticate, pagosBancoContextoBanco);
router.get('/pagos-banco/bancos', authenticate, pagosBancosDistintos);
router.get('/depositos-ingresos', authenticate, depositosIngresos);
router.get('/depositos-ingresos/detalle', authenticate, depositosIngresosDetalle);
router.get('/depositos-ingresos/export', authenticate, depositosIngresosExport);

module.exports = router;
