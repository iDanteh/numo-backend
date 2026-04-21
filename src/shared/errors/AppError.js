'use strict';

class AppError extends Error {
  constructor(message, statusCode = 500) {
    super(message);
    this.name       = this.constructor.name;
    this.statusCode = statusCode;
  }
}

class NotFoundError extends AppError {
  constructor(resource = 'Recurso') {
    super(`${resource} no encontrado`, 404);
  }
}

class BadRequestError extends AppError {
  constructor(message = 'Solicitud inválida') {
    super(message, 400);
  }
}

class ConflictError extends AppError {
  constructor(message = 'Conflicto en la solicitud') {
    super(message, 409);
  }
}

class UnprocessableError extends AppError {
  constructor(message = 'No se pudo procesar la entidad') {
    super(message, 422);
  }
}

class ForbiddenError extends AppError {
  constructor(message = 'Acceso denegado') {
    super(message, 403);
  }
}

module.exports = {
  AppError,
  NotFoundError,
  BadRequestError,
  ConflictError,
  UnprocessableError,
  ForbiddenError,
};
