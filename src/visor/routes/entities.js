'use strict';

const express = require('express');
const { authenticate, permit } = require('../../shared/middleware/auth');
const { list, create, update } = require('../controllers/entity.controller');

const router = express.Router();

router.get('/',     authenticate,                         list);
router.post('/',    authenticate, permit('entities:write'), create);
router.patch('/:id', authenticate, permit('entities:write'), update);

module.exports = router;
