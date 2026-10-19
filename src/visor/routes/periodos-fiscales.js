const express = require('express');
const { body } = require('express-validator');
const { authenticate, permit } = require('../../shared/middleware/auth');
const { PERMISSIONS } = require('../../shared/config/rbac');
const { list, listSimple, create, remove, cerrar, revertirCierre, listCierres, descargarCierre } = require('../controllers/periodoFiscal.controller');

const router = express.Router();

router.get('/', authenticate, permit(PERMISSIONS.VISOR_READ), list);
router.get('/simple', authenticate, permit(PERMISSIONS.VISOR_READ), listSimple);

// Historial de Cierre de Mes (Reportes → Cierre de Mes) — antes de '/:id/...'
// para no chocar con esos patrones.
router.get('/cierres',                 authenticate, permit(PERMISSIONS.VISOR_REPORTS), listCierres);
router.get('/cierres/:id/descargar',   authenticate, permit(PERMISSIONS.VISOR_REPORTS), descargarCierre);

// Crear, cerrar y borrar un periodo son operaciones que modifican el estado
// fiscal: exigen visor:write (2026-10-19, auditoría — antes solo authenticate,
// cualquier usuario podía cerrar/borrar un mes). Revertir un cierre ya es más
// restringido (visor:cierre-mes:revertir).
router.post('/',
  authenticate,
  permit(PERMISSIONS.VISOR_WRITE),
  [
    body('ejercicio').isInt({ min: 2000, max: 2100 }).withMessage('Ejercicio inválido'),
    body('periodo').optional({ nullable: true }).isInt({ min: 1, max: 12 }).withMessage('Periodo debe ser 1-12'),
    body('label').optional().isString().trim(),
  ],
  create,
);

router.post('/:id/cerrar',           authenticate, permit(PERMISSIONS.VISOR_WRITE), cerrar);
router.post('/:id/revertir-cierre',  authenticate, permit(PERMISSIONS.VISOR_CIERRE_MES_REVERTIR), revertirCierre);

router.delete('/:id', authenticate, permit(PERMISSIONS.VISOR_WRITE), remove);

module.exports = router;
