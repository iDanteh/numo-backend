const express = require('express');
const { body } = require('express-validator');
const { authenticate } = require('../../shared/middleware/auth');
const { list, listSimple, create, remove } = require('../controllers/periodoFiscal.controller');

const router = express.Router();

router.get('/', authenticate, list);
router.get('/simple', authenticate, listSimple);

router.post('/',
  authenticate,
  [
    body('ejercicio').isInt({ min: 2000, max: 2100 }).withMessage('Ejercicio inválido'),
    body('periodo').optional({ nullable: true }).isInt({ min: 1, max: 12 }).withMessage('Periodo debe ser 1-12'),
    body('label').optional().isString().trim(),
  ],
  create,
);

router.delete('/:id', authenticate, remove);

module.exports = router;
