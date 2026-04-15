const jwt    = require('jsonwebtoken');
const config = require('../../config/env');
const { validationResult } = require('express-validator');
const User = require('../models/User');
const { asyncHandler } = require('../middleware/errorHandler');

/**
 * POST /api/auth/login
 */
const login = asyncHandler(async (req, res) => {
  const errors = validationResult(req);
  if (!errors.isEmpty()) return res.status(400).json({ errors: errors.array() });

  const { email, password } = req.body;
  const user = await User.findOne({ email, isActive: true }).select('+password');
  if (!user || !(await user.comparePassword(password))) {
    return res.status(401).json({ error: 'Credenciales inválidas' });
  }

  user.lastLogin = new Date();
  await user.save({ validateBeforeSave: false });

  const token = jwt.sign(
    { id: user._id, role: user.role },
    config.jwt.secret,
    { expiresIn: config.jwt.expiresIn },
  );

  res.json({
    token,
    user: { id: user._id, name: user.name, email: user.email, role: user.role },
  });
});

/**
 * POST /api/auth/register  (solo admin)
 */
const register = asyncHandler(async (req, res) => {
  if (req.user.role !== 'admin') return res.status(403).json({ error: 'No autorizado' });

  const errors = validationResult(req);
  if (!errors.isEmpty()) return res.status(400).json({ errors: errors.array() });

  const user = await User.create(req.body);
  res.status(201).json({ id: user._id, name: user.name, email: user.email, role: user.role });
});

/**
 * GET /api/auth/me
 */
const me = asyncHandler(async (req, res) => {
  res.json(req.user);
});

module.exports = { login, register, me };
