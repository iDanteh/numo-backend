'use strict';

const CollectionRequest = require('./CollectionRequest.model');
const BankMovement      = require('../banks/BankMovement.model');
const bankService       = require('../banks/bank.service'); // setErpIds — mismo mecanismo que usa el panel de cobros
// Objeto completo, NO desestructurado a propósito: las llamadas de abajo usan
// koreCaja.obtenerSesionCaja(...) en vez de una const local, para poder
// mockear estas funciones en pruebas (reasignando koreCaja.obtenerSesionCaja =
// ...) igual que ya se hacía con erpRoutes.obtenerSesionCaja antes de este
// refactor — desestructurar aquí rompería esa capacidad de prueba.
const koreCaja          = require('../erp/kore-caja.service');
const { parseCxcs, parseFormasPago }               = require('./collection-request.parsers');
const { buildPayloadSingle, buildPayloadMulti }    = require('./collection-request-kore-payload');
const { buildErpLinksParaCobro }                   = require('./collection-request-erp-links');
const { extractReceiptData, findMatchingMovements } = require('./receipt.service');
const { NotFoundError, BadRequestError } = require('../../shared/errors/AppError');

async function analyzeReceipt(fileBuffer, mimetype) {
  const extracted  = await extractReceiptData(fileBuffer, mimetype);
  const candidates = await findMatchingMovements(extracted);
  return { extracted, candidates, totalCandidatos: candidates.length };
}

// ── Auto-match: identificar y aplicar el cobro SIN intervención humana ────────
// cuando el comprobante adjunto coincide con muy alta confianza contra un
// movimiento bancario. Se dispara (a) justo después de crear la solicitud
// (fire-and-forget, ver create()) y (b) periódicamente por el cron
// (collectionRequestAutoMatchCron.js) para solicitudes que sigan pendientes —
// el depósito bancario muchas veces se importa a Numo DESPUÉS de que Kore
// avisa la solicitud, así que un solo intento al crear no basta.
//
// Se queda EN ESTE ARCHIVO (no se extrae a un módulo aparte) porque llama
// directamente a identificar() y analyzeStoredComprobante(), que también
// viven aquí — extraerla generaría un require() circular con este mismo
// archivo. Es la única pieza de la lógica de auto-match/Kore que no se separó.
const AUTO_MATCH_UMBRAL_PCT = 95;
const AUTO_MATCH_USER = Object.freeze({
  _id:    'sistema-auto-match',
  nombre: 'Conciliación automática (OCR)',
  // 'cobranza' y no 'admin' a propósito: setErpIds() usa el role para el
  // chequeo de "¿puede forzar un movimiento ya tomado por otro?" — el
  // auto-match NUNCA debe poder pisar un movimiento que un humano (u otro
  // proceso) ya identificó, solo tomar los que están genuinamente libres.
  role:   'cobranza',
});

// Exige, además del umbral de %, que el comprobante haya podido extraer al
// menos fecha Y alguna clave/referencia (no solo el monto) — de lo contrario
// un comprobante que solo trae el monto podría llegar a "100%" (40/40) sin
// ninguna señal corroborante, lo cual es demasiado riesgoso para aplicar un
// cobro real sin que lo revise una persona.
function _tieneSenalesSuficientes(ext) {
  return !!ext.fecha && !!(ext.claveRastreo || ext.referencia || ext.numeroAutorizacion);
}

// Intenta identificar+aplicar el cobro de una solicitud de forma automática.
// Nunca lanza — cualquier fallo (OCR, Kore, sin match suficiente) se traga y
// se loguea, dejando la solicitud tal cual para revisión manual o un
// reintento posterior del cron. Regresa un resumen solo para logging/tests.
async function intentarAutoMatch(id) {
  const cr = await CollectionRequest.findById(id);
  if (!cr)                     return { aplicado: false, motivo: 'Solicitud no encontrada' };
  if (cr.status !== 'pendiente') return { aplicado: false, motivo: `status=${cr.status}` };
  if (!cr.comprobante?.data)   return { aplicado: false, motivo: 'Sin comprobante' };
  if (!cr.conceptoId)          return { aplicado: false, motivo: 'Sin conceptoId' };

  let extracted, candidates;
  try {
    ({ extracted, candidates } = await analyzeStoredComprobante(id));
  } catch (err) {
    console.warn(`[auto-match] ${id}: falló el análisis del comprobante — ${err.message}`);
    return { aplicado: false, motivo: 'OCR falló', error: err.message };
  }

  const top = candidates[0];
  if (!top) return { aplicado: false, motivo: 'Sin candidatos' };

  if (!_tieneSenalesSuficientes(extracted)) {
    console.log(`[auto-match] ${id}: comprobante sin fecha/referencia extraída — no es candidato a auto-match (top=${top.porcentaje}%)`);
    return { aplicado: false, motivo: 'Comprobante sin señales suficientes', porcentaje: top.porcentaje };
  }

  const montoMov    = top.movement.deposito ?? top.movement.retiro ?? 0;
  const montoExacto = extracted.monto != null && Math.abs(montoMov - extracted.monto) < 0.01;

  if (top.porcentaje < AUTO_MATCH_UMBRAL_PCT || !montoExacto) {
    console.log(`[auto-match] ${id}: no alcanza el umbral (top=${top.porcentaje}%, montoExacto=${montoExacto}) — queda pendiente para revisión manual`);
    return { aplicado: false, motivo: 'Debajo del umbral', porcentaje: top.porcentaje, montoExacto };
  }

  try {
    await identificar(id, top.movement._id.toString(), AUTO_MATCH_USER, { automatico: true });
    console.log(`[auto-match] ${id}: identificado y cobro aplicado automáticamente (${top.porcentaje}%, movimiento ${top.movement._id})`);
    return { aplicado: true, porcentaje: top.porcentaje, bankMovementId: top.movement._id.toString() };
  } catch (err) {
    // Cubre p.ej. ConflictError (alguien más ya tomó el movimiento) o que Kore
    // rechace el cobro (sesión de caja cerrada, etc.) — se deja pendiente para
    // revisión manual o el siguiente reintento del cron.
    console.warn(`[auto-match] ${id}: match de ${top.porcentaje}% pero identificar() falló — ${err.message}`);
    return { aplicado: false, motivo: 'identificar() falló', error: err.message, porcentaje: top.porcentaje };
  }
}

// Crea una solicitud de cobro — llamada por el ERP (Kore), sin sesión Numo (ver
// middleware requireErpApiKey en routes.js). El "usuario que solicita" viaja en
// el body porque quien llama no es ese usuario, es el backend del ERP.
async function create(data, file) {
  const {
    cxcs, formasPago, descripcion, conceptoId, solicitudIdErp,
    usuarioSolicitanteId, usuarioSolicitanteNombre,
  } = data;

  if (!usuarioSolicitanteId) throw new BadRequestError('usuarioSolicitanteId es requerido');

  // undefined (no null) a propósito cuando no viene — ver comentario en el
  // modelo sobre por qué el campo no puede tener default: null.
  const solicitudIdErpTrim = solicitudIdErp ? String(solicitudIdErp).trim() : undefined;

  // Idempotencia: si el ERP reintenta el mismo POST (timeout de red, retry
  // automático, etc.) con el mismo solicitudIdErp, no se duplica — se regresa
  // la solicitud ya creada.
  if (solicitudIdErpTrim) {
    const existente = await CollectionRequest.findOne({ solicitudIdErp: solicitudIdErpTrim });
    if (existente) return _toSafeJSON(existente);
  }

  const cxcsParsed = parseCxcs(cxcs);
  const cxcInvalido = cxcsParsed.find(c => !c.erpId);
  if (cxcInvalido) throw new BadRequestError('Cada CxC requiere erpId');

  const formasPagoParsed = parseFormasPago(formasPago);
  const montoInvalido = formasPagoParsed.find(f => !f.formaPagoId || !f.formaPagoDescripcion || !(f.importe > 0));
  if (montoInvalido) {
    throw new BadRequestError('Cada forma de pago requiere formaPagoId, formaPagoDescripcion e importe > 0');
  }

  // Modo 2 (varias CxC): Kore/cobro-panel no soportan combinar N CxC con M
  // formas de pago — exactamente 1 forma de pago global, y cada CxC debe traer
  // ya resuelto cuánto le corresponde de esa transferencia.
  if (cxcsParsed.length > 1) {
    if (formasPagoParsed.length !== 1) {
      throw new BadRequestError('Una solicitud con varias CxC (Modo 2) solo admite exactamente una forma de pago');
    }
    const sinMonto = cxcsParsed.find(c => !(c.montoAsignado > 0));
    if (sinMonto) throw new BadRequestError('En Modo 2, cada CxC requiere montoAsignado > 0');
  }

  const monto = Math.round(formasPagoParsed.reduce((s, f) => s + f.importe, 0) * 100) / 100;

  try {
    const cr = await CollectionRequest.create({
      cxcs: cxcsParsed,
      formasPago: formasPagoParsed,
      monto,
      descripcion: descripcion
        ? String(descripcion).trim()
        : `Solicitud de cobro de ${usuarioSolicitanteNombre || usuarioSolicitanteId} con fecha ${new Date().toISOString().slice(0, 10)}`,
      conceptoId:       conceptoId ? String(conceptoId).trim() : null,
      solicitudIdErp:   solicitudIdErpTrim,
      comprobante: file
        ? { data: file.buffer, mimetype: file.mimetype, originalName: file.originalname }
        : undefined,
      solicitanteUserId: String(usuarioSolicitanteId).trim(),
      solicitanteNombre: usuarioSolicitanteNombre ? String(usuarioSolicitanteNombre).trim() : null,
    });

    // Auto-match en segundo plano — NO se espera (fire-and-forget) para no
    // demorar la respuesta a Kore con el OCR + la aplicación del cobro, que
    // puede tardar varios segundos. Si falla o no alcanza el umbral, la
    // solicitud simplemente queda "pendiente" como si esto no hubiera corrido
    // (el cron periódico la vuelve a intentar más tarde).
    if (file) {
      setImmediate(() => {
        intentarAutoMatch(cr._id.toString()).catch(err => {
          console.error(`[auto-match] ${cr._id}: error inesperado —`, err.message);
        });
      });
    }

    return _toSafeJSON(cr);
  } catch (err) {
    // Race condition: dos requests casi simultáneas con el mismo solicitudIdErp
    // pasaron el check de arriba antes de que la primera terminara de insertar
    // — el índice único absorbe la carrera, aquí solo se recupera el ganador.
    if (err.code === 11000 && solicitudIdErpTrim) {
      const existente = await CollectionRequest.findOne({ solicitudIdErp: solicitudIdErpTrim });
      if (existente) return _toSafeJSON(existente);
    }
    throw err;
  }
}

// Oculta el binario del comprobante en las respuestas de listado/detalle —
// se sirve aparte vía GET /:id/comprobante.
function _toSafeJSON(doc) {
  const obj = doc.toObject ? doc.toObject() : doc;
  if (obj.comprobante) {
    obj.comprobante = {
      tieneComprobante: !!obj.comprobante.data,
      mimetype:         obj.comprobante.mimetype ?? null,
      originalName:     obj.comprobante.originalName ?? null,
    };
  }
  return obj;
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
      .select('-comprobante.data')
      .populate('bankMovementId', 'banco fecha concepto deposito retiro')
      .lean(),
    CollectionRequest.countDocuments(filter),
  ]);

  return {
    data: data.map(d => ({ ...d, comprobante: { ...d.comprobante, tieneComprobante: !!d.comprobante?.mimetype } })),
    pagination: { total, page: parseInt(page), limit: parseInt(limit), pages: Math.ceil(total / parseInt(limit)) },
  };
}

// Solicitudes creadas por el propio usuario autenticado (rol tienda revisando su historial).
async function listMine(userId, filters) {
  const { page = 1, limit = 50, status } = filters;
  const filter = { solicitanteUserId: userId };
  if (status) filter.status = status;

  const skip = (parseInt(page) - 1) * parseInt(limit);
  const [data, total] = await Promise.all([
    CollectionRequest.find(filter)
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(parseInt(limit))
      .select('-comprobante.data')
      .populate('bankMovementId', 'banco fecha concepto deposito retiro')
      .lean(),
    CollectionRequest.countDocuments(filter),
  ]);

  return {
    data: data.map(d => ({ ...d, comprobante: { ...d.comprobante, tieneComprobante: !!d.comprobante?.mimetype } })),
    pagination: { total, page: parseInt(page), limit: parseInt(limit), pages: Math.ceil(total / parseInt(limit)) },
  };
}

async function getById(id) {
  const cr = await CollectionRequest.findById(id)
    .select('-comprobante.data')
    .populate('bankMovementId', 'banco fecha concepto deposito retiro numeroAutorizacion referenciaNumerica')
    .lean();
  if (!cr) throw new NotFoundError('Solicitud');
  return { ...cr, comprobante: { ...cr.comprobante, tieneComprobante: !!cr.comprobante?.mimetype } };
}

async function getComprobante(id) {
  // Sin .lean(): con lean() Mongoose no castea el campo Buffer y regresa el
  // tipo BSON crudo (Binary), que Express NO sabe enviar como binario (res.send
  // lo trata como objeto plano y lo serializa mal) — hay que dejar que Mongoose
  // haga el cast normal a Buffer real.
  const cr = await CollectionRequest.findById(id).select('comprobante');
  if (!cr?.comprobante?.data) throw new NotFoundError('Comprobante');
  return cr.comprobante; // { data: Buffer, mimetype, originalName }
}

// Corre el mismo motor de OCR + matching que ya usa OcrModalComponent
// (analyzeReceipt), pero sobre el comprobante YA guardado en la solicitud —
// el usuario no tiene que volver a subir el archivo. Ayuda a ubicar el
// depósito bancario correspondiente cuando el auto-match/búsqueda manual no
// lo encuentran a simple vista.
async function analyzeStoredComprobante(id) {
  const comprobante = await getComprobante(id); // ya valida que exista
  const extracted   = await extractReceiptData(comprobante.data, comprobante.mimetype);
  const candidates  = await findMatchingMovements(extracted);
  return { extracted, candidates, totalCandidatos: candidates.length };
}

// Concilia la solicitud contra un BankMovement Y aplica el cobro real en Kore,
// en un solo paso (todo o nada): si cualquier parte de la llamada a Kore falla,
// no se guarda nada — ni el status, ni el bankMovementId, ni la referencia —
// para no dejar la solicitud en un estado a medias. El usuario ve el error y
// puede corregir (p.ej. pedirle al cajero que abra su caja) y reintentar.
//
// La sesión de caja se resuelve con el CAJERO que generó la solicitud
// (cr.solicitanteUserId), NO con el usuario de cobranza/contabilidad que está
// identificando — es el cajero quien tiene una caja abierta en Kore.
async function identificar(id, bankMovementId, user, opts = {}) {
  const automatico = !!opts.automatico;
  if (!bankMovementId) throw new BadRequestError('bankMovementId es requerido');

  const cr = await CollectionRequest.findById(id);
  if (!cr) throw new NotFoundError('Solicitud');
  if (cr.status === 'identificada') throw new BadRequestError('La solicitud ya está identificada');

  if (!cr.conceptoId) {
    throw new BadRequestError(
      'La solicitud no trae conceptoId — no se puede aplicar el cobro automáticamente en Kore. ' +
      'Resuelve este cobro manualmente desde el panel de Bancos.',
    );
  }

  const mov = await BankMovement.findOne({ _id: bankMovementId, uuidXML: null });
  if (!mov) throw new NotFoundError('Movimiento bancario');

  // 1. Sesión de caja del cajero solicitante.
  let sesionId, koreToken;
  try {
    ({ sesionId, koreToken } = await koreCaja.obtenerSesionCaja(cr.solicitanteUserId));
  } catch (err) {
    if (err instanceof koreCaja.KoreCajaError) {
      throw new BadRequestError(`No se pudo obtener la sesión de caja del solicitante: ${err.message}`);
    }
    throw err;
  }

  // 2. Saldo EN VIVO de cada CxC (antes de aplicar este cobro) — necesario para
  // calcular erpLinks[].saldoActual con el mismo criterio que el panel de cobros
  // (no el `total` que Kore mandó al crear la solicitud, que puede estar
  // desactualizado si hubo pagos parciales de por medio).
  let cuentasKore;
  try {
    const ids = cr.cxcs.map(c => c.erpId).join(',');
    cuentasKore = await koreCaja.obtenerCuentasKore(koreToken, ids);
  } catch (err) {
    if (err instanceof koreCaja.KoreCajaError) {
      throw new BadRequestError(`No se pudo consultar el saldo de la(s) CxC en Kore: ${err.message}`);
    }
    throw err;
  }

  // 3. La referencia bancaria SIEMPRE la asigna Numo con el folio del
  // movimiento identificado — nunca la que (no) haya mandado el ERP.
  const referencia        = String(mov.folio ?? '');
  const formasPagoConRef  = cr.formasPago.map(f => ({ ...f.toObject(), referencia }));

  // 4. Armar payload y llamar a Kore — Modo 1 (1 CxC) o Modo 2 (N CxC).
  let koreResult;
  try {
    if (cr.cxcs.length === 1) {
      const payload = buildPayloadSingle(cr, formasPagoConRef, mov, sesionId);
      koreResult = await koreCaja.aplicarCobroOperacion(sesionId, koreToken, payload);
    } else {
      const payload = buildPayloadMulti(cr, formasPagoConRef, mov, sesionId);
      koreResult = await koreCaja.aplicarCobroOperacionMultiple(sesionId, koreToken, payload);
    }
  } catch (err) {
    if (err instanceof koreCaja.KoreCajaError) {
      // aplicarCobroOperacion(Multiple) ya loguea el payload y el rechazo crudo
      // de Kore por consola — este log adicional deja explícito con qué
      // solicitud/CxC se relaciona ese rechazo, para no tener que cruzar logs.
      console.warn(`[collection-requests] identificar ${id}: Kore rechazó el cobro (cxcs=${cr.cxcs.map(c => c.erpId).join(',')}, conceptoId=${cr.conceptoId}):`, err.message, err.koreBody ? JSON.stringify(err.koreBody) : '');
      throw new BadRequestError(`Kore rechazó el cobro: ${err.message}`);
    }
    throw err;
  }

  // 5. Kore aceptó el cobro — vincular la(s) CxC al movimiento con el MISMO
  // mecanismo que usa el panel de cobros (erpLinks/erpIds/identificadoPor/
  // saldoErp/status vía aplicarLogicaErp), no un simple status a mano. Puede
  // lanzar ConflictError si otro usuario ya tiene el movimiento tomado — se
  // deja propagar tal cual (mismo comportamiento que el panel).
  //
  // identificadoPor debe quedar a nombre del CAJERO que generó la solicitud
  // (cr.solicitanteUserId/Nombre, el mismo que viene en el body original) — no
  // el usuario de cobranza/contabilidad que dio clic en "Identificar" en Numo.
  // El `role` sí es el del usuario que ejecuta la acción: setErpIds lo usa
  // únicamente para el chequeo de permisos/conflicto (¿puede forzar un
  // movimiento ya tomado?), no para la identidad que se guarda.
  const erpLinks = buildErpLinksParaCobro(cr, cuentasKore, mov.erpLinks);
  const identidadCajero = { _id: cr.solicitanteUserId, nombre: cr.solicitanteNombre, role: user.role };
  await bankService.setErpIds(bankMovementId, erpLinks, identidadCajero);

  // 6. Solo si todo lo anterior salió bien: persistir la solicitud.
  cr.formasPago          = formasPagoConRef;
  cr.bankMovementId      = bankMovementId;
  cr.status              = 'identificada';
  cr.resueltoPorUserId   = user._id;
  cr.resueltoPorNombre   = user.nombre ?? null;
  cr.resueltoAt          = new Date();
  cr.cobroAplicado       = true;
  cr.cobroAplicadoAt     = new Date();
  cr.koreOperacionResult = koreResult;
  cr.autoIdentificado    = automatico;
  await cr.save();

  return _toSafeJSON(cr);
}

async function rechazar(id, motivo, user) {
  const cr = await CollectionRequest.findOneAndUpdate(
    { _id: id, status: { $ne: 'identificada' } },
    {
      status:            'rechazada',
      motivoRechazo:     motivo || null,
      resueltoPorUserId: user._id,
      resueltoPorNombre: user.nombre ?? null,
      resueltoAt:        new Date(),
    },
    { new: true },
  );
  if (!cr) throw new NotFoundError('Solicitud no encontrada o ya identificada');
  return _toSafeJSON(cr);
}

module.exports = {
  analyzeReceipt, create, list, listMine, getById, getComprobante, analyzeStoredComprobante,
  identificar, rechazar, intentarAutoMatch,
};
