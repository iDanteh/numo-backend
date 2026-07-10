'use strict';

const express = require('express');
const { authenticate, permit } = require('../../shared/middleware/auth');
const { list, create, update, alertarCredencialesSat } = require('../controllers/entity.controller');

const router = express.Router();

router.get('/',     authenticate,                         list);
router.post('/',    authenticate, permit('entities:write'), create);
router.patch('/:id', authenticate, permit('entities:write'), update);
router.post('/:id/alertar-credenciales-sat', authenticate, permit('entities:write'), alertarCredencialesSat);

module.exports = router;
