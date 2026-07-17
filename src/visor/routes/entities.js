'use strict';

const express = require('express');
const { authenticate, permit } = require('../../shared/middleware/auth');
const { list, create, update, alertarCredencialesSat } = require('../controllers/entity.controller');

const router = express.Router();

router.get('/',      authenticate, permit('entities:read'),    list);
router.post('/',     authenticate, permit('entities:edit'),    create);
router.patch('/:id', authenticate, permit('entities:edit'),    update);
router.post('/:id/alertar-credenciales-sat', authenticate, permit('entities:message'), alertarCredencialesSat);

module.exports = router;
