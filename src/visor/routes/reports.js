const express = require('express');
const { authenticate, permit } = require('../../shared/middleware/auth');
const { PERMISSIONS } = require('../../shared/config/rbac');
const { dashboard, dashboardRecibidos, resumenCfdis, exportExcel, discrepanciasMontos, satVigenteErpInactivo, discrepanciasCriticas, notInErp, pagosRelacionados, conciliacionExcel, pagosBanco, pagosBancoDetalle, pagosBancoExport, pagosBancosDistintos, pagosBancoContextoBanco, depositosIngresos, depositosIngresosDetalle, depositosIngresosExport } = require('../controllers/report.controller');

const router = express.Router();

router.get('/dashboard', authenticate, dashboard);
router.get('/dashboard-recibidos', authenticate, dashboardRecibidos);
router.get('/resumen-cfdis', authenticate, resumenCfdis);
router.get('/export/excel', authenticate, exportExcel);
router.get('/discrepancias-montos', authenticate, discrepanciasMontos);
router.get('/sat-vigente-erp-inactivo', authenticate, satVigenteErpInactivo);
router.get('/discrepancias-criticas', authenticate, discrepanciasCriticas);
router.get('/not-in-erp', authenticate, notInErp);
router.get('/pagos-relacionados', authenticate, pagosRelacionados);
router.get('/conciliacion-excel', authenticate, conciliacionExcel);
router.get('/pagos-banco', authenticate, permit(PERMISSIONS.VISOR_REPORTS), pagosBanco);
router.get('/pagos-banco/detalle', authenticate, permit(PERMISSIONS.VISOR_REPORTS), pagosBancoDetalle);
router.get('/pagos-banco/export', authenticate, permit(PERMISSIONS.VISOR_REPORTS), pagosBancoExport);
router.get('/pagos-banco/contexto-banco', authenticate, permit(PERMISSIONS.VISOR_REPORTS), pagosBancoContextoBanco);
router.get('/pagos-banco/bancos', authenticate, permit(PERMISSIONS.VISOR_REPORTS), pagosBancosDistintos);
router.get('/depositos-ingresos', authenticate, permit(PERMISSIONS.VISOR_REPORTS), depositosIngresos);
router.get('/depositos-ingresos/detalle', authenticate, permit(PERMISSIONS.VISOR_REPORTS), depositosIngresosDetalle);
router.get('/depositos-ingresos/export', authenticate, permit(PERMISSIONS.VISOR_REPORTS), depositosIngresosExport);

module.exports = router;
