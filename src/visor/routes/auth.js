const express = require('express');
const { body } = require('express-validator');
const { authenticate } = require('../middleware/auth');
const { login, register, me } = require('../controllers/auth.controller');

const router = express.Router();

router.post('/login',
  [body('email').isEmail().normalizeEmail(), body('password').notEmpty()],
  login,
);

router.post('/register',
  authenticate,
  [
    body('email').isEmail().normalizeEmail(),
    body('password').isLength({ min: 8 }),
    body('name').notEmpty().trim(),
    body('role').isIn(['admin', 'contador', 'auditor', 'viewer']),
  ],
  register,
);

router.get('/me', authenticate, me);

module.exports = router;
