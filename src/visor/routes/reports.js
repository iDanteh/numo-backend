const express = require('express');
const { authenticate, permit } = require('../../shared/middleware/auth');
const { PERMISSIONS } = require('../../shared/config/rbac');
const { limitarAEmpresasDelUsuario: empresas, soloSinEmpresasAsignadas } = require('../../shared/middleware/empresas-usuario');
const { dashboard, dashboardRecibidos, resumenCfdis, exportExcel, discrepanciasMontos, satVigenteErpInactivo, discrepanciasCriticas, notInErp, pagosRelacionados, conciliacionExcel, pagosBanco, pagosBancoDetalle, pagosBancoExport, pagosBancosDistintos, pagosBancoContextoBanco, pagosBancoSugerencias, depositosIngresos, depositosIngresosDetalle, depositosIngresosExport } = require('../controllers/report.controller');

const router = express.Router();

router.get('/dashboard', authenticate, permit('visor:read'), empresas({ inyectar: ['rfcEmisor'] }), dashboard);
router.get('/dashboard-recibidos', authenticate, permit('visor:read'), empresas({ inyectar: ['rfcReceptor'] }), dashboardRecibidos);
router.get('/resumen-cfdis', authenticate, permit('visor:read'), empresas({ inyectar: ['rfcEmisor', 'rfcReceptor'] }), resumenCfdis);
router.get('/export/excel', authenticate, permit('visor:read'), soloSinEmpresasAsignadas, exportExcel);
router.get('/discrepancias-montos', authenticate, permit('visor:read'), empresas({ inyectar: ['rfcEmisor'] }), discrepanciasMontos);
router.get('/sat-vigente-erp-inactivo', authenticate, permit('visor:read'), empresas({ inyectar: ['rfcEmisor'] }), satVigenteErpInactivo);
router.get('/discrepancias-criticas', authenticate, permit('visor:read'), empresas({ inyectar: ['rfcEmisor'] }), discrepanciasCriticas);
router.get('/not-in-erp', authenticate, permit('visor:read'), empresas({ inyectar: ['rfcEmisor'] }), notInErp);
router.get('/pagos-relacionados', authenticate, permit('visor:read'), empresas({ inyectar: ['rfcEmisor'] }), pagosRelacionados);
router.get('/conciliacion-excel', authenticate, permit('visor:read'), empresas({ inyectar: ['rfcEmisor'] }), conciliacionExcel);
router.get('/pagos-banco', authenticate, permit(PERMISSIONS.VISOR_REPORTS), pagosBanco);
router.get('/pagos-banco/detalle', authenticate, permit(PERMISSIONS.VISOR_REPORTS), pagosBancoDetalle);
router.get('/pagos-banco/export', authenticate, permit(PERMISSIONS.VISOR_REPORTS), pagosBancoExport);
router.get('/pagos-banco/contexto-banco', authenticate, permit(PERMISSIONS.VISOR_REPORTS), pagosBancoContextoBanco);
router.get('/pagos-banco/bancos', authenticate, permit(PERMISSIONS.VISOR_REPORTS), pagosBancosDistintos);
router.get('/pagos-banco/sugerencias-conciliacion', authenticate, permit(PERMISSIONS.VISOR_REPORTS), pagosBancoSugerencias);
router.get('/depositos-ingresos', authenticate, permit(PERMISSIONS.VISOR_REPORTS), depositosIngresos);
router.get('/depositos-ingresos/detalle', authenticate, permit(PERMISSIONS.VISOR_REPORTS), depositosIngresosDetalle);
router.get('/depositos-ingresos/export', authenticate, permit(PERMISSIONS.VISOR_REPORTS), depositosIngresosExport);

module.exports = router;
