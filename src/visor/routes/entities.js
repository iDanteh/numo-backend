const express = require('express');
const { authenticate, authorize } = require('../middleware/auth');
const { list, create, update } = require('../controllers/entity.controller');

const router = express.Router();

router.get('/', authenticate, list);
router.post('/', authenticate, authorize('admin'), create);
router.patch('/:id', authenticate, authorize('admin'), update);

module.exports = router;
