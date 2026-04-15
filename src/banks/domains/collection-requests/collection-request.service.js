'use strict';

const CollectionRequest = require('./CollectionRequest.model');
const BankMovement      = require('../banks/BankMovement.model');
const { extractReceiptData, findMatchingMovements } = require('./receipt.service');
const { NotFoundError, BadRequestError } = require('../../shared/errors/AppError');

async function analyzeReceipt(fileBuffer, mimetype) {
  const extracted  = await extractReceiptData(fileBuffer, mimetype);
  const candidates = await findMatchingMovements(extracted);
  return { extracted, candidates, totalCandidatos: candidates.length };
}

async function list(filters) {
  const { page = 1, limit = 50, status } = filters;
  const filter = {};
  if (status) filter.status = status;

  const skip = (parseInt(page) - 1) * parseInt(limit);
  const [data, total] = await Promise.all([
    CollectionRequest.find(filter)
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(parseInt(limit))
      .populate('bankMovementId', 'banco fecha concepto deposito retiro')
      .populate('creadoPor',     'name email')
      .lean(),
    CollectionRequest.countDocuments(filter),
  ]);

  return {
    data,
    pagination: { total, page: parseInt(page), limit: parseInt(limit), pages: Math.ceil(total / parseInt(limit)) },
  };
}

async function getById(id) {
  const cr = await CollectionRequest.findById(id)
    .populate('bankMovementId', 'banco fecha concepto deposito retiro numeroAutorizacion referenciaNumerica')
    .populate('cfdiIds',        'uuid serie folio total fecha emisor receptor')
    .populate('creadoPor',      'name email')
    .populate('confirmadoPor',  'name email')
    .lean();
  if (!cr) throw new NotFoundError('Solicitud');
  return cr;
}

async function create(data, userId) {
  const { clienteNombre, clienteRFC, monto, concepto,
          bankMovementId, cfdiIds, comprobante, notas } = data;

  const cr = await CollectionRequest.create({
    clienteNombre:  clienteNombre  || null,
    clienteRFC:     clienteRFC     || null,
    monto:          monto          || null,
    concepto:       concepto       || null,
    bankMovementId: bankMovementId || null,
    cfdiIds:        cfdiIds        || [],
    comprobante: {
      montoExtraido:            comprobante?.monto                || null,
      fechaExtraida:            comprobante?.fecha ? new Date(comprobante.fecha) : null,
      horaExtraida:             comprobante?.hora                 || null,
      claveRastreo:             comprobante?.claveRastreo         || null,
      referencia:               comprobante?.referencia           || null,
      bancoOrigen:              comprobante?.bancoOrigen          || null,
      bancoDestino:             comprobante?.bancoDestino         || null,
      cuentaOrigenUltimos4:     comprobante?.cuentaOrigenUltimos4  || null,
      cuentaDestinoUltimos4:    comprobante?.cuentaDestinoUltimos4 || null,
      titularOrigen:            comprobante?.titularOrigen        || null,
      titularDestino:           comprobante?.titularDestino       || null,
      conceptoExtraido:         comprobante?.concepto             || null,
      confianzaExtraccion:      comprobante?.confianza            || 0,
    },
    notas:          notas          || null,
    status:         bankMovementId ? 'por_confirmar' : 'pendiente',
    creadoPor:      userId,
  });

  // El movimiento fue identificado mediante el comprobante — marcarlo de inmediato.
  // Solo si no tiene uuidXML (que bloquea el status).
  if (bankMovementId) {
    await BankMovement.findOneAndUpdate(
      { _id: bankMovementId, uuidXML: null },
      { status: 'identificado' },
    );
  }

  return cr;
}

async function confirm(id, data, userId) {
  const cr = await CollectionRequest.findById(id);
  if (!cr) throw new NotFoundError('Solicitud');
  if (cr.status === 'confirmado') throw new BadRequestError('La solicitud ya está confirmada');

  if (data.bankMovementId)      cr.bankMovementId = data.bankMovementId;
  if (data.cfdiIds)             cr.cfdiIds        = data.cfdiIds;
  if (data.notas !== undefined) cr.notas          = data.notas;

  cr.status        = 'confirmado';
  cr.confirmadoPor = userId;
  cr.confirmadoAt  = new Date();
  await cr.save();

  // Marcar el movimiento bancario como identificado.
  // Solo se actualiza si aún no tiene uuidXML (que bloquea el status).
  if (cr.bankMovementId) {
    await BankMovement.findOneAndUpdate(
      { _id: cr.bankMovementId, uuidXML: null },
      { status: 'identificado' },
    );
  }

  return cr;
}

async function reject(id, notas) {
  const cr = await CollectionRequest.findOneAndUpdate(
    { _id: id, status: { $ne: 'confirmado' } },
    { status: 'rechazado', notas: notas || null },
    { new: true },
  );
  if (!cr) throw new NotFoundError('Solicitud no encontrada o ya confirmada');
  return cr;
}

module.exports = { analyzeReceipt, list, getById, create, confirm, reject };
