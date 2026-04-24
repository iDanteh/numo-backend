'use strict';

const express = require('express');
const { authenticate, permit } = require('../../shared/middleware/auth');
const { getSchedule, updateSchedule } = require('../controllers/schedule.controller');

const router = express.Router();

router.get('/',  authenticate,                          getSchedule);
router.put('/',  authenticate, permit('entities:write'), updateSchedule);

module.exports = router;
