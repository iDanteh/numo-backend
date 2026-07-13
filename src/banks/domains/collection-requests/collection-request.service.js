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
const { buildErpLinksParaCobro, tipoSaldoEspecial } = require('./collection-request-erp-links');
const { extractReceiptData, findMatchingMovements } = require('./receipt.service');
const driveComprobantes                  = require('./drive-comprobantes.service');
const { NotFoundError, BadRequestError } = require('../../shared/errors/AppError');
const { emitToAll }                      = require('../../shared/socket');

// Payload mínimo para 'collection-request:updated' — mismo criterio que
// bank:movement:updated en bank.service.js: solo lo que la bandeja necesita
// para actualizar la fila en el arreglo local sin volver a pedir todo el listado.
function _eventoActualizacion(cr, mov) {
  return {
    _id:               cr._id.toString(),
    status:            cr.status,
    motivoRechazo:     cr.motivoRechazo,
    resueltoPorUserId: cr.resueltoPorUserId,
    resueltoPorNombre: cr.resueltoPorNombre,
    resueltoAt:        cr.resueltoAt,
    cobroAplicado:     cr.cobroAplicado,
    cobroAplicadoAt:   cr.cobroAplicadoAt,
    solicitanteUserId: cr.solicitanteUserId,
    bankMovementId: mov ? {
      _id: mov._id.toString(), banco: mov.banco, fecha: mov.fecha,
      concepto: mov.concepto, deposito: mov.deposito, retiro: mov.retiro,
    } : null,
  };
}

// Combina el comprobante LEGACY (Mongo, un solo archivo, campo `comprobante`)
// con los nuevos (Drive, arreglo `comprobantes[]`) en una sola lista uniforme
// por índice — así todo lo que lee/analiza comprobantes no necesita bifurcar
// su lógica según de dónde viene cada uno. Los documentos nuevos solo llenan
// `comprobantes[]`; los viejos solo tienen `comprobante` — nunca ambos a la vez
// en la práctica, pero si algún día coexistieran, Drive gana (es lo vigente).
const _MIME_A_EXT = {
  'image/jpeg': 'jpg', 'image/png': 'png', 'image/webp': 'webp',
  'image/gif': 'gif', 'application/pdf': 'pdf',
};

function _extensionDe(originalName, mimetype) {
  const dePorNombre = /\.([a-z0-9]+)$/i.exec(originalName || '')?.[1];
  return (dePorNombre || _MIME_A_EXT[mimetype] || 'bin').toLowerCase();
}

// Nombre de archivo en Drive: serie-folio de la CxC cuando la solicitud es de una sola
// CxC (Modo 1) — identifica de un vistazo a qué cuenta corresponde el comprobante sin
// tener que abrirlo. Con varias CxC (Modo 2) no hay un solo serie-folio al que apuntar,
// así que se usa el solicitudIdErp. `originalName` (el nombre real que subió el usuario)
// se conserva aparte en la metadata del comprobante — esto solo renombra el archivo
// que vive en Drive, para que sea buscable/identificable ahí directamente.
function _nombreDriveComprobante(cxcsParsed, solicitudIdErp, index, total, originalName, mimetype) {
  const unaSolaCxc = cxcsParsed.length === 1 ? cxcsParsed[0] : null;
  const base = (unaSolaCxc?.serie && unaSolaCxc?.folioExterno)
    ? `${unaSolaCxc.serie}-${unaSolaCxc.folioExterno}`
    : solicitudIdErp;
  const sufijo = total > 1 ? `-${index + 1}` : '';
  const ext    = _extensionDe(originalName, mimetype);
  return `${base}${sufijo}.${ext}`;
}

function _comprobantesUnificados(cr) {
  if (cr.comprobantes?.length > 0) {
    return cr.comprobantes.map(c => ({
      storage: 'drive', driveFileId: c.driveFileId,
      mimetype: c.mimetype, originalName: c.originalName,
    }));
  }
  if (cr.comprobante?.data) {
    return [{
      storage: 'mongo', data: cr.comprobante.data,
      mimetype: cr.comprobante.mimetype, originalName: cr.comprobante.originalName,
    }];
  }
  return [];
}

async function analyzeReceipt(fileBuffer, mimetype) {
  const extracted  = await extractReceiptData(fileBuffer, mimetype);
  const candidates = await findMatchingMovements(extracted);
  return { extracted, candidates, totalCandidatos: candidates.length };
}

// Crea una solicitud de cobro — llamada por el ERP (Kore), sin sesión Numo (ver
// middleware requireErpApiKey en routes.js). El "usuario que solicita" viaja en
// el body porque quien llama no es ese usuario, es el backend del ERP.
// `files` es un arreglo (multer .array) — puede venir vacío, con 1 o con varios
// comprobantes; cada uno puede corresponder a un depósito bancario distinto
// (ej. mitad transferencia + mitad efectivo, cada uno con su propio comprobante).
async function create(data, files = []) {
  const {
    cxcs, formasPago, descripcion, conceptoId, solicitudIdErp,
    usuarioSolicitanteId, usuarioSolicitanteNombre,
  } = data;

  if (!usuarioSolicitanteId) throw new BadRequestError('usuarioSolicitanteId es requerido');

  const solicitudIdErpTrim = solicitudIdErp ? String(solicitudIdErp).trim() : '';
  if (!solicitudIdErpTrim) throw new BadRequestError('solicitudIdErp es requerido');

  // Idempotencia: si el ERP reintenta el mismo POST (timeout de red, retry
  // automático, etc.) con el mismo solicitudIdErp, no se duplica — se regresa
  // la solicitud ya creada.
  const existente = await CollectionRequest.findOne({ solicitudIdErp: solicitudIdErpTrim });
  if (existente) return _toSafeJSON(existente);

  const cxcsParsed = parseCxcs(cxcs);
  const cxcInvalido = cxcsParsed.find(c => !c.erpId);
  if (cxcInvalido) throw new BadRequestError('Cada CxC requiere erpId');

  const formasPagoParsed = parseFormasPago(formasPago);
  const montoInvalido = formasPagoParsed.find(f => !f.formaPagoId || !f.formaPagoDescripcion || !(f.importe > 0));
  if (montoInvalido) {
    throw new BadRequestError('Cada forma de pago requiere formaPagoId, formaPagoDescripcion e importe > 0');
  }

  // Saldo a favor / anticipo: sin el id + monto del registro específico usado,
  // Kore recibiría saldosAFavorAUsar/anticipos vacíos y no sabría de dónde
  // descontar (ver collection-request-kore-payload.js). El mismo chequeo vive
  // en el pre('validate') del modelo — se repite aquí para dar un error claro
  // antes de tocar Mongo, igual que ya se hace con la validación de Modo 2.
  const saldoEspecialInvalido = formasPagoParsed.find(f => {
    const tipo = tipoSaldoEspecial(f);
    if (!tipo) return false;
    if (!f.saldosAplicados.length) return true;
    if (f.saldosAplicados.some(s => !s.id || !(s.monto > 0))) return true;
    const suma = Math.round(f.saldosAplicados.reduce((s, x) => s + x.monto, 0) * 100) / 100;
    return Math.abs(suma - f.importe) > 0.01;
  });
  if (saldoEspecialInvalido) {
    throw new BadRequestError(
      `La forma de pago "${saldoEspecialInvalido.formaPagoDescripcion}" requiere saldosAplicados ` +
      '(id + monto de cada saldo a favor/anticipo usado) cuya suma sea igual al importe',
    );
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

  // Comprobantes nuevos van SIEMPRE a Drive (nunca a Mongo) — el campo legacy
  // `comprobante` ya no se escribe. Se suben ANTES de crear el documento: si
  // Kore reintenta con el mismo solicitudIdErp por un timeout de red real
  // (nada que ver con esto), el chequeo de idempotencia de arriba ya regresó
  // antes de llegar aquí, así que no se vuelve a subir nada en un retry normal.
  let comprobantesSubidos = [];
  if (files.length > 0) {
    try {
      comprobantesSubidos = await Promise.all(files.map(async (f, i) => {
        const driveName = _nombreDriveComprobante(
          cxcsParsed, solicitudIdErpTrim, i, files.length, f.originalname, f.mimetype,
        );
        const { driveFileId, driveWebViewLink } = await driveComprobantes.subirComprobante(f.buffer, f.mimetype, driveName);
        return { driveFileId, driveWebViewLink, mimetype: f.mimetype, originalName: f.originalname };
      }));
    } catch (err) {
      throw new BadRequestError(`No se pudieron subir los comprobantes: ${err.message}`);
    }
  }

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
      comprobantes:      comprobantesSubidos,
      solicitanteUserId: String(usuarioSolicitanteId).trim(),
      solicitanteNombre: usuarioSolicitanteNombre ? String(usuarioSolicitanteNombre).trim() : null,
    });

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

// Oculta el binario del comprobante legacy en las respuestas de listado/detalle
// — se sirve aparte vía GET /:id/comprobante(s). `comprobantes[]` (Drive) nunca
// trae binario en Mongo, así que no necesita ocultarse.
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

// tieneComprobante debe ser true si hay AL MENOS uno, sin importar la fuente
// (legacy en Mongo o nuevos en Drive) — usado por list/listMine/getById/getByErpId,
// que trabajan sobre documentos `.lean()` (por eso no reusan _toSafeJSON).
function _tieneAlgunComprobante(doc) {
  return !!doc.comprobante?.mimetype || (doc.comprobantes?.length ?? 0) > 0;
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
    data: data.map(d => ({ ...d, comprobante: { ...d.comprobante, tieneComprobante: _tieneAlgunComprobante(d) } })),
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
    data: data.map(d => ({ ...d, comprobante: { ...d.comprobante, tieneComprobante: _tieneAlgunComprobante(d) } })),
    pagination: { total, page: parseInt(page), limit: parseInt(limit), pages: Math.ceil(total / parseInt(limit)) },
  };
}

async function getById(id) {
  const cr = await CollectionRequest.findById(id)
    .select('-comprobante.data')
    .populate('bankMovementId', 'banco fecha concepto deposito retiro numeroAutorizacion referenciaNumerica')
    .lean();
  if (!cr) throw new NotFoundError('Solicitud');
  return { ...cr, comprobante: { ...cr.comprobante, tieneComprobante: _tieneAlgunComprobante(cr) } };
}

// Para que el ERP (Kore) consulte el estado de una solicitud que él mismo creó
// — busca por solicitudIdErp (lo único que Kore conoce de antemano, nunca el
// _id interno de Numo). Respuesta deliberadamente MÍNIMA (a diferencia de
// getById, que sí regresa el documento completo para la bandeja de Numo):
// Kore ya conoce cxcs/formasPago/comprobantes (los mandó él al crearla) y no
// necesita IDs internos de Mongo ni quién en Numo la resolvió — solo el
// estatus, el motivo si se rechazó, cuándo se resolvió, el movimiento bancario
// al que quedó vinculada, y (agregado a petición del usuario) qué CxC quedaron
// afectadas y con qué monto — antes había que adivinarlo cruzando con Kore.
async function getByErpId(solicitudIdErp) {
  const cr = await CollectionRequest.findOne({ solicitudIdErp: String(solicitudIdErp).trim() })
    .select('solicitudIdErp status motivoRechazo resueltoAt bankMovementId cxcs monto cobroAplicado cobroAplicadoAt')
    .populate('bankMovementId', 'folio fecha deposito')
    .lean();
  if (!cr) throw new NotFoundError('Solicitud');

  return {
    solicitudIdErp:  cr.solicitudIdErp,
    status:          cr.status,
    motivoRechazo:   cr.motivoRechazo ?? null,
    resueltoAt:      cr.resueltoAt ?? null,
    monto:           cr.monto,
    cobroAplicado:   cr.cobroAplicado ?? false,
    cobroAplicadoAt: cr.cobroAplicadoAt ?? null,
    // CxC afectadas por esta solicitud — montoAsignado solo viene lleno en
    // Modo 2 (varias CxC); en Modo 1 (una sola CxC) el monto cobrado es `monto`
    // de arriba, ya que ahí no se reparte entre cuentas.
    cxcs: (cr.cxcs || []).map(c => ({
      erpId:         c.erpId,
      serie:         c.serie ?? null,
      folioExterno:  c.folioExterno ?? null,
      folioFiscal:   c.folioFiscal ?? null,
      montoAsignado: c.montoAsignado ?? null,
    })),
    bankMovement: cr.bankMovementId ? {
      folio:    cr.bankMovementId.folio    ?? null,
      fecha:    cr.bankMovementId.fecha    ?? null,
      deposito: cr.bankMovementId.deposito ?? null,
    } : null,
  };
}

// `index` selecciona CUÁL comprobante de la lista unificada (legacy Mongo +
// Drive) — por default el primero, que sigue funcionando igual que antes para
// las solicitudes viejas de un solo comprobante.
async function getComprobante(id, index = 0) {
  // Sin .lean(): con lean() Mongoose no castea el campo Buffer legacy y regresa
  // el tipo BSON crudo (Binary), que Express NO sabe enviar como binario
  // (res.send lo trata como objeto plano y lo serializa mal) — hay que dejar
  // que Mongoose haga el cast normal a Buffer real.
  const cr = await CollectionRequest.findById(id).select('comprobante comprobantes');
  if (!cr) throw new NotFoundError('Solicitud');

  const item = _comprobantesUnificados(cr)[index];
  if (!item) throw new NotFoundError('Comprobante');

  const data = item.storage === 'drive'
    ? await driveComprobantes.descargarComprobante(item.driveFileId)
    : item.data;
  return { data, mimetype: item.mimetype, originalName: item.originalName };
}

// Corre el mismo motor de OCR + matching que ya usa OcrModalComponent
// (analyzeReceipt), pero sobre los comprobantes YA guardados en la solicitud —
// el usuario no tiene que volver a subirlos. Cada comprobante se analiza de
// forma INDEPENDIENTE (nunca se combinan candidatos entre archivos — cada uno
// puede corresponder a un depósito bancario distinto), y se regresa un
// resultado por comprobante para que la búsqueda ayude a ubicar cada depósito.
async function analyzeStoredComprobantes(id) {
  const cr = await CollectionRequest.findById(id).select('comprobante comprobantes cxcs');
  if (!cr) throw new NotFoundError('Solicitud');

  const lista = _comprobantesUnificados(cr);
  if (lista.length === 0) throw new NotFoundError('Comprobante');

  const ownErpIds = cr.cxcs.map(c => c.erpId);
  const resultados = [];
  for (let i = 0; i < lista.length; i++) {
    const item = lista[i];
    const data = item.storage === 'drive'
      ? await driveComprobantes.descargarComprobante(item.driveFileId)
      : item.data;
    const extracted  = await extractReceiptData(data, item.mimetype);
    const candidates  = await findMatchingMovements(extracted, ownErpIds);
    resultados.push({ comprobanteIndex: i, extracted, candidates, totalCandidatos: candidates.length });
  }
  return resultados;
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
async function identificar(id, bankMovementId, user) {
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
  await cr.save();

  // 7. Avisar en tiempo real a quien tenga la bandeja abierta (cobranza/contabilidad/
  // admin) y a la tienda que la solicitó — sin esto, cualquier otra sesión con la
  // vista abierta se queda con el estado viejo hasta que alguien recargue a mano.
  emitToAll('collection-request:updated', _eventoActualizacion(cr, mov));

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

  emitToAll('collection-request:updated', _eventoActualizacion(cr, null));

  return _toSafeJSON(cr);
}

module.exports = {
  analyzeReceipt, create, list, listMine, getById, getByErpId, getComprobante, analyzeStoredComprobantes,
  identificar, rechazar,
};
