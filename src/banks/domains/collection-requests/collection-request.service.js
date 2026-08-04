'use strict';

const ExcelJS           = require('exceljs'); // reporte descargable de Solicitudes de Cobro (buildReport/buildReportMine)
const CollectionRequest = require('./CollectionRequest.model');
const BankMovement      = require('../banks/BankMovement.model');
const bankService       = require('../banks/bank.service'); // setErpIds — mismo mecanismo que usa el panel de cobros
// Objeto completo, NO desestructurado a propósito: las llamadas de abajo usan
// koreCaja.obtenerSesionCaja(...) en vez de una const local, para poder
// mockear estas funciones en pruebas (reasignando koreCaja.obtenerSesionCaja =
// ...) igual que ya se hacía con erpRoutes.obtenerSesionCaja antes de este
// refactor — desestructurar aquí rompería esa capacidad de prueba.
const koreCaja          = require('../erp/kore-caja.service');
// erp.routes expone _sincronizarConRetry/_rangoDesdeFollo (mismo helper que ya
// usan los scripts de backfill de este mismo dominio, ver
// migrate-erp-movimientoskore-formaspago.js) — es la única consulta a Kore que
// SÍ trae folioFiscal (endpoint /cuentas-pendientes del ERP; koreCaja habla con
// /cuentas de caja, cuyo mapeo no expone ese campo). Kore exige rango de fecha
// máximo un mes en este endpoint — nunca se le pega sin _rangoDesdeFollo.
const erpRoutes         = require('../erp/erp.routes');
const { parseCxcs, parseFormasPago }               = require('./collection-request.parsers');
const { buildErpLinksParaCobro, tipoSaldoEspecial, matchBancoDefault } = require('./collection-request-erp-links');
const { extractReceiptData, findMatchingMovements } = require('./receipt.service');
const driveComprobantes                  = require('./drive-comprobantes.service');
const { NotFoundError, BadRequestError } = require('../../shared/errors/AppError');
const { emitToAll }                      = require('../../shared/socket');
const { logger }                         = require('../../shared/utils/logger');

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
    canceladoPorUserId: cr.canceladoPorUserId,
    canceladoPorNombre: cr.canceladoPorNombre,
    canceladoAt:        cr.canceladoAt,
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
  'image/jpeg': 'jpg', 'image/jpg': 'jpg', 'image/png': 'png', 'image/webp': 'webp',
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

async function analyzeReceipt(fileBuffer, mimetype, label = null) {
  const extracted  = await extractReceiptData(fileBuffer, mimetype, label);
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
  // no queda claro de qué registro se descontó cada saldo/anticipo al
  // guardar la solicitud. El mismo chequeo vive en el pre('validate') del
  // modelo — se repite aquí para dar un error claro antes de tocar Mongo,
  // igual que ya se hace con la validación de Modo 2.
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

    const safe = _toSafeJSON(cr);
    // Kore ya avisa en tiempo real con este mismo POST — lo único que faltaba era
    // propagarlo a quien tenga la bandeja abierta, sin tener que recargar a mano.
    emitToAll('collection-request:created', safe);
    return safe;
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

// Filtro compartido por list()/listMine() para búsqueda de texto + rango de fecha
// de creación — mismo criterio que listMovements() en bank.service.js (Bancos),
// pero sin la complejidad de aggregation/_score: esta bandeja es mucho más chica
// y un $or plano alcanza, no hace falta rankear resultados por relevancia.
function _buildBusquedaFilter({ search, fechaInicio, fechaFin }) {
  const filter = {};

  // Rango sobre createdAt (cuándo se creó la solicitud), no sobre una fecha de
  // movimiento bancario — esta bandeja no tiene equivalente a "fecha" de Bancos.
  if (fechaInicio || fechaFin) {
    filter.createdAt = {};
    if (fechaInicio) filter.createdAt.$gte = new Date(fechaInicio);
    // Inclusive de todo el día — mismo criterio que fechaFin en bank.service.js.
    if (fechaFin) filter.createdAt.$lte = new Date(`${fechaFin}T23:59:59.999Z`);
  }

  if (search) {
    const esc = search.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const re  = new RegExp(esc, 'i');
    const orClauses = [
      { solicitudIdErp: re },
      { solicitanteNombre: re },
      { 'cxcs.folioExterno': re },
      { 'cxcs.nombrePersona': re },
    ];

    // Folio compuesto de CxC ("D0-260705980") — reportado por el usuario: buscar solo el
    // folioExterno ("260705980", la única forma posible hasta ahora, ver 'cxcs.folioExterno'
    // arriba) es ambiguo, porque el mismo número puede repetirse bajo series distintas
    // (D0-260705980 vs A0-260705980 serían indistinguibles). Si el término trae el formato
    // "serie-folio", se agrega una cláusula PRECISA además de la anterior (no la reemplaza,
    // para no romper la búsqueda parcial por folio solo) — $elemMatch es obligatorio acá:
    // 'cxcs' es un arreglo, así que dos condiciones sueltas ({'cxcs.serie':X},
    // {'cxcs.folioExterno':Y}) matchearían aunque X y Y vinieran de DOS CxC distintas de la
    // misma solicitud, no de una sola — $elemMatch exige que ambas valgan en el MISMO elemento.
    const folioCompuesto = search.match(/^\s*([A-Za-z0-9]{1,6})[\s-]+(\d+)\s*$/);
    if (folioCompuesto) {
      const [, serieRaw, folioRaw] = folioCompuesto;
      const escSerie = serieRaw.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      orClauses.push({
        cxcs: {
          $elemMatch: {
            serie:        new RegExp(`^${escSerie}$`, 'i'),
            folioExterno: new RegExp(folioRaw, 'i'),
          },
        },
      });
    }

    // Búsqueda por monto — mismo criterio de tolerancia que bank.service.js:
    // sin decimales → ±1 peso; 1 decimal → ±0.05; 2+ decimales → ±0.005.
    const cleanNum = search.replace(/[$,\s]/g, '');
    const num       = parseFloat(cleanNum);
    if (!isNaN(num) && num > 0) {
      const decimalPlaces = (cleanNum.split('.')[1] || '').length;
      const tolerance = decimalPlaces === 0 ? 1 : decimalPlaces === 1 ? 0.05 : 0.005;
      const lo = decimalPlaces === 0 ? num : num - tolerance;
      const hi = decimalPlaces === 0 ? num + tolerance : num + tolerance;
      orClauses.push({ monto: { $gte: lo, $lte: hi } });
    }

    filter.$or = orClauses;
  }

  return filter;
}

async function list(filters) {
  const { page = 1, limit = 50, status, search, fechaInicio, fechaFin } = filters;
  const filter = _buildBusquedaFilter({ search, fechaInicio, fechaFin });
  if (status) filter.status = status;

  // Pendientes: más antigua primero — se atiende en el orden en que llegó (decisión
  // del usuario 2026-07-24). Identificadas/rechazadas/canceladas (2026-08-04, a pedido
  // del usuario): más reciente primero — son historial ya resuelto, lo último resuelto
  // es lo que interesa ver arriba. `status` siempre viaja como un único valor (el tab
  // activo, ver collection-request.component.ts#reload) — nunca una lista combinada acá.
  const ordenAscendente = !status || status === 'pendiente';
  const skip = (parseInt(page) - 1) * parseInt(limit);
  const [data, total] = await Promise.all([
    // NO tocar listMine() (abajo, historial personal del solicitante en /mias) — sigue
    // más reciente primero a propósito, sin depender de status.
    CollectionRequest.find(filter)
      .sort({ createdAt: ordenAscendente ? 1 : -1 })
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
  const { page = 1, limit = 50, status, search, fechaInicio, fechaFin } = filters;
  const filter = { ..._buildBusquedaFilter({ search, fechaInicio, fechaFin }), solicitanteUserId: userId };
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

// Inicio del día en curso en hora de MÉXICO (no la hora local del servidor,
// que puede correr en otra zona) — mismo patrón ya usado en
// cfdi-poliza-generator.service.js (_medianocheMx): México abolió el DST desde
// 2022, offset fijo UTC-6, así que no hace falta librería de zonas horarias.
// El "hoy" de este conteo debe coincidir con el "hoy" que ve el usuario en
// México, no con la medianoche UTC ni la del servidor.
function _inicioDeHoy() {
  const hoyMX = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Mexico_City' });
  return new Date(`${hoyMX}T06:00:00.000Z`);
}

// Conteos para las tarjetas de stats y los badges de las pestañas (Pendientes/
// Identificadas/Rechazadas, "hoy", monto pendiente). Antes se calculaban en el
// frontend sobre el arreglo de hasta 200 solicitudes que list()/listMine()
// devolvía SIN filtrar por status — con paginación real por status (arriba),
// ese arreglo ya no trae todos los estatus a la vez, así que estos conteos se
// calculan aparte, directo en Mongo, sobre el universo completo (no la página
// actual). `baseFilter` acota a las solicitudes propias en statsMine().
async function _stats(baseFilter) {
  const inicioHoy = _inicioDeHoy();
  const [porStatus, identificadasHoy, rechazadasHoy, montoPendienteAgg] = await Promise.all([
    CollectionRequest.aggregate([
      { $match: baseFilter },
      { $group: { _id: '$status', count: { $sum: 1 } } },
    ]),
    CollectionRequest.countDocuments({ ...baseFilter, status: 'identificada', resueltoAt: { $gte: inicioHoy } }),
    CollectionRequest.countDocuments({ ...baseFilter, status: 'rechazada', resueltoAt: { $gte: inicioHoy } }),
    CollectionRequest.aggregate([
      { $match: { ...baseFilter, status: 'pendiente' } },
      { $group: { _id: null, total: { $sum: '$monto' } } },
    ]),
  ]);

  const counts = { pendiente: 0, identificada: 0, rechazada: 0, cancelada: 0 };
  for (const r of porStatus) if (r._id in counts) counts[r._id] = r.count;

  return {
    counts,
    identificadasHoy,
    rechazadasHoy,
    montoPendienteTotal: montoPendienteAgg[0]?.total ?? 0,
  };
}

async function stats() {
  return _stats({});
}

async function statsMine(userId) {
  return _stats({ solicitanteUserId: userId });
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
  const cr = await CollectionRequest.findById(id).select('comprobante comprobantes');
  if (!cr) throw new NotFoundError('Solicitud');

  const lista = _comprobantesUnificados(cr);
  if (lista.length === 0) throw new NotFoundError('Comprobante');

  const resultados = [];
  for (let i = 0; i < lista.length; i++) {
    const item = lista[i];
    // Cada comprobante se procesa de forma AISLADA: una solicitud puede traer
    // varios (ej. mitad transferencia + mitad efectivo), y que uno esté
    // corrupto o protegido con contraseña no debe tumbar la lectura de los
    // demás, que sí pueden ser perfectamente legibles.
    try {
      const data = item.storage === 'drive'
        ? await driveComprobantes.descargarComprobante(item.driveFileId)
        : item.data;
      const label      = item.originalName || `comprobante#${i}`;
      const extracted  = await extractReceiptData(data, item.mimetype, label);
      const candidates  = await findMatchingMovements(extracted);
      resultados.push({ comprobanteIndex: i, extracted, candidates, totalCandidatos: candidates.length });
    } catch (err) {
      logger.warn(`[analyzeStoredComprobantes] Comprobante #${i} no se pudo leer:`, err.message);
      resultados.push({ comprobanteIndex: i, extracted: {}, candidates: [], totalCandidatos: 0, error: err.message });
    }
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

  // 2b. Best-effort: si alguna CxC llegó sin folioFiscal (foto de cr.cxcs tomada al
  // CREAR la solicitud — Kore puede timbrar el CFDI después, antes de autorizarla),
  // se reintenta contra /cuentas-pendientes del ERP, que sí lo trae. Nunca bloquea el
  // cobro: si el ERP no responde o no encuentra la cuenta, se sigue sin folioFiscal
  // (mismo criterio de "recuperar cuando sea posible, nunca a costa de trabar el
  // cobro" que ya usa _backfillFormasPagoYFolioFiscal en erp.routes.js).
  const cuentaKorePorId = new Map(cuentasKore.map(c => [String(c.id), c]));
  for (const cxc of cr.cxcs) {
    if (cxc.folioFiscal) continue;
    const rango = erpRoutes._rangoDesdeFollo(cxc.folioExterno);
    if (!rango) continue; // folioExterno con formato inesperado — no se puede acotar la fecha, se deja para el rescate manual
    try {
      const { raw } = await erpRoutes._sincronizarConRetry({
        serieExterna: cxc.serie, folioExterno: String(cxc.folioExterno),
        fechaDesde: rango.fechaDesde, fechaHasta: rango.fechaHasta,
      });
      const encontrada = raw.find(c => String(c.folioExterno) === String(cxc.folioExterno) && (!cxc.serie || String(c.serieExterna) === String(cxc.serie)));
      if (encontrada?.folioFiscal) {
        const cuenta = cuentaKorePorId.get(String(cxc.erpId));
        if (cuenta) cuenta.folioFiscal = encontrada.folioFiscal;
      }
    } catch (err) {
      logger.warn(`[collection-requests] identificar ${id}: no se pudo recuperar folioFiscal para erpId=${cxc.erpId} vía /cuentas-pendientes (se continúa sin él): ${err.message}`);
    }
  }

  // 3. La referencia bancaria SIEMPRE la asigna Numo con el folio del
  // movimiento identificado — nunca la que (no) haya mandado el ERP.
  const referencia        = String(mov.folio ?? '');
  const formasPagoConRef  = cr.formasPago.map(f => ({ ...f.toObject(), referencia }));

  // 4. Avisar a Kore el estatus de revisión contable de la solicitud (endpoint
  // distinto al de aplicar el cobro) — con el token del usuario de cobranza/
  // contabilidad que está identificando, NO el del cajero: esta acción es
  // "revisión contable", no aplicar un cobro, y el cajero puede no tener nada
  // que ver con quién revisa. Kore EXIGE este aviso (APROBADO) antes de
  // permitir aplicar el cobro — confirmado con Kore real: intentar aplicar el
  // cobro sin este paso responde 400 "no puede aplicar operaciones... hasta
  // resolver las solicitudes generadas anteriormente". Todo o nada: si Kore
  // rechaza este aviso, no se aplica el cobro (paso 5) ni se persiste nada en
  // Numo.
  try {
    const tokenRevisor = await koreCaja.obtenerTokenKore(user._id);
    const avisoResult = await koreCaja.actualizarEstatusSolicitud(tokenRevisor, cr.solicitudIdErp, 'APROBADO', 'Cobro conciliado y aplicado en Numo');
    console.log(`[collection-requests] identificar ${id}: Kore confirmó APROBADO para solicitudIdErp=${cr.solicitudIdErp} →`, JSON.stringify(avisoResult));
  } catch (err) {
    if (koreCaja.esErrorYaEnEstatus(err, 'APROBADO')) {
      // Reintento sobre una solicitud que un intento anterior ya dejó
      // APROBADO en Kore, pero que no se persistió en Numo porque el cobro
      // (paso 5) falló en ese intento previo — no es un error real, se sigue.
      console.warn(`[collection-requests] identificar ${id}: Kore ya tenía solicitudIdErp=${cr.solicitudIdErp} en APROBADO (reintento) — se continúa con el cobro.`);
    } else if (err instanceof koreCaja.KoreCajaError) {
      throw new BadRequestError(`No se pudo notificar el estatus a Kore: ${err.message}`);
    } else {
      throw err;
    }
  }

  // 5. BancoID — igual criterio que _matchBancoDefault() en cobro-panel.component.ts
  // (panel manual): solo las formas de pago con claveSAT '03' (transferencia) lo
  // necesitan. Acá no hay un humano confirmando el banco en pantalla antes de
  // aplicar (a diferencia del panel manual), pero el usuario confirmó (2026-07-28)
  // que igual quiere el mismo fallback: si `mov.banco` no matchea ningún banco del
  // catálogo de Kore, se manda bancos[0] (el primero del catálogo) en vez de
  // dejar el cobro sin BancoID. Todo o nada: si Kore rechaza cualquiera de los 2
  // catálogos, no se aplica el cobro (mismo criterio que el resto de la función).
  let bancoDefault = null;
  const formaPagoRequiereBanco = new Map();
  try {
    const formasPagoKore = await koreCaja.listarFormasPago(koreToken);
    for (const f of formasPagoKore) formaPagoRequiereBanco.set(String(f.id), f.claveSAT === '03');

    const algunaFormaRequiereBanco = cr.formasPago.some(f => formaPagoRequiereBanco.get(f.formaPagoId));
    if (algunaFormaRequiereBanco) {
      const bancosKore = await koreCaja.listarBancos(koreToken);
      bancoDefault = matchBancoDefault(bancosKore, mov.banco);
    }
  } catch (err) {
    if (err instanceof koreCaja.KoreCajaError) {
      throw new BadRequestError(`No se pudo resolver el banco para aplicar el cobro: ${err.message}`);
    }
    throw err;
  }

  // 6. Aplicar el cobro — ahora que Kore ya aprobó la solicitud, este endpoint
  // dedicado la aplica internamente con los datos que Kore ya tiene desde que
  // ÉL creó la solicitud; lo único que se manda, por cada forma de pago, es su
  // FormaPagoID, BancoID (solo si esa forma de pago lo requiere, ver arriba) y
  // DOS datos del movimiento identificado: "Aut" (el folio interno de Numo,
  // mismo folio que `referencia`, arriba) y "Numo" (el numeroAutorizacion
  // bancario real, extraído por OCR) — ambos por separado, ninguno reemplaza al
  // otro. Igual en Modo 1 y Modo 2 — un elemento del arreglo por cada forma de pago.
  // Fix 2026-08-04: el nombre correcto es "Aut", no "Autorizacion" — confirmado
  // contra el panel manual (cobro-panel.component.ts, ya funcionando en
  // producción), que es la única otra parte del sistema que manda este mismo
  // dato a Kore. El nombre equivocado provocaba errores del lado de Kore al
  // aplicar cobros vía Solicitudes de Cobro.
  // Fix 2026-08-04 (mismo día, después) — Kore actualizó su configuración: ahora
  // rechaza la solicitud completa si CUALQUIER forma de pago que no sea
  // transferencia (saldo a favor, cheque, depósito en efectivo) trae
  // DatosAdicionales. Antes se mandaba Aut/Numo sin importar el tipo; ahora se
  // exige el MISMO criterio que ya usa BancoID (`formaPagoRequiereBanco`,
  // claveSAT==='03') — nunca se manda DatosAdicionales fuera de transferencia.
  // Riesgo aceptado, no un descuido: una solicitud pagada 100% en efectivo/
  // cheque/saldo a favor ya no deja ningún tag Aut en Kore, así que
  // _montoSaldoLinkPorMovimiento (erp.routes.js) no podrá volver a matchearla
  // contra Kore más adelante — es una restricción nueva del lado de Kore, no
  // algo que Numo pueda evitar mientras la solicitud se siga aplicando.
  const datosAdicionalesPorFormaPago = cr.formasPago.map(f => {
    const esTransferencia = formaPagoRequiereBanco.get(f.formaPagoId) === true;
    return {
      ...(esTransferencia && bancoDefault ? { BancoID: bancoDefault.id } : {}),
      FormaPagoID: f.formaPagoId,
      ...(esTransferencia ? {
        DatosAdicionales: [
          { Nombre: 'Aut',  Valor: mov.folio || '' },
          { Nombre: 'Numo', Valor: mov.numeroAutorizacion || '' },
        ],
      } : {}),
    };
  });

  let koreResult;
  try {
    koreResult = await koreCaja.aplicarSolicitudOperacion(sesionId, cr.solicitudIdErp, koreToken, datosAdicionalesPorFormaPago);
  } catch (err) {
    if (err instanceof koreCaja.KoreCajaError) {
      // aplicarSolicitudOperacion ya loguea el payload y el rechazo crudo de
      // Kore por consola — este log adicional deja explícito con qué
      // solicitud/CxC se relaciona ese rechazo, para no tener que cruzar logs.
      console.warn(`[collection-requests] identificar ${id}: Kore rechazó el cobro (cxcs=${cr.cxcs.map(c => c.erpId).join(',')}, conceptoId=${cr.conceptoId}):`, err.message, err.koreBody ? JSON.stringify(err.koreBody) : '');
      throw new BadRequestError(`Kore rechazó el cobro: ${err.message}`);
    }
    throw err;
  }

  // 7. Kore aceptó el cobro — vincular la(s) CxC al movimiento con el MISMO
  // mecanismo que usa el panel de cobros (erpLinks/erpIds/identificadoPor/
  // saldoErp/status vía aplicarLogicaErp), no un simple status a mano. Puede
  // lanzar ConflictError si otro usuario ya tiene el movimiento tomado — se
  // deja propagar tal cual (mismo comportamiento que el panel).
  //
  // identificadoPor queda a nombre de quien AUTORIZA (el usuario de cobranza/
  // contabilidad que ejecuta esta acción) — cambio de criterio 2026-07-29,
  // revierte la decisión del 2026-07-07 que dejaba acá al cajero solicitante.
  // `cr.resueltoPorUserId`/`resueltoPorNombre` (más abajo) ya guardaban a este
  // mismo usuario desde el 07-07 — ahora esa misma identidad también queda en
  // el BankMovement, consistente con el resto del sistema (cobro-panel deja
  // en identificadoPor a quien aplica el cobro, no a un tercero).
  const erpLinks = buildErpLinksParaCobro(cr, cuentasKore, mov.erpLinks);
  await bankService.setErpIds(bankMovementId, erpLinks, user);

  // 8. Solo si todo lo anterior salió bien: persistir la solicitud.
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

  // 9. Avisar en tiempo real a quien tenga la bandeja abierta (cobranza/contabilidad/
  // admin) y a la tienda que la solicitó — sin esto, cualquier otra sesión con la
  // vista abierta se queda con el estado viejo hasta que alguien recargue a mano.
  emitToAll('collection-request:updated', _eventoActualizacion(cr, mov));

  return _toSafeJSON(cr);
}

// Antes de tocar Mongo, se avisa a Kore el estatus de revisión contable
// (RECHAZADO) — con el token del usuario de cobranza/contabilidad que ejecuta
// la acción (no el del cajero: ver el mismo criterio en identificar()). No hay
// paso de "aplicar" para un rechazo — solo existe para aprobar un cobro. Todo
// o nada: si Kore rechaza este aviso, no se marca "rechazada" en Numo tampoco.
// El `motivo` que ya captura el usuario es el mismo texto que viaja como
// Comentario a Kore — no hace falta pedir un campo aparte.
async function rechazar(id, motivo, user) {
  const cr = await CollectionRequest.findOne({ _id: id, status: { $ne: 'identificada' } });
  if (!cr) throw new NotFoundError('Solicitud no encontrada o ya identificada');

  try {
    const tokenRevisor = await koreCaja.obtenerTokenKore(user._id);
    await koreCaja.actualizarEstatusSolicitud(tokenRevisor, cr.solicitudIdErp, 'RECHAZADO', motivo || 'Solicitud rechazada en Numo');
  } catch (err) {
    if (koreCaja.esErrorYaEnEstatus(err, 'RECHAZADO')) {
      console.warn(`[collection-requests] rechazar ${id}: Kore ya tenía solicitudIdErp=${cr.solicitudIdErp} en RECHAZADO (reintento) — se continúa.`);
    } else if (err instanceof koreCaja.KoreCajaError) {
      throw new BadRequestError(`No se pudo notificar el estatus a Kore: ${err.message}`);
    } else {
      throw err;
    }
  }

  // Guard atómico repetido a propósito: entre el findOne de arriba y este
  // update pudo haberse identificado la solicitud desde otra sesión — el
  // filtro { status: { $ne: 'identificada' } } evita pisar ese resultado.
  const actualizada = await CollectionRequest.findOneAndUpdate(
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
  if (!actualizada) throw new NotFoundError('Solicitud no encontrada o ya identificada');

  emitToAll('collection-request:updated', _eventoActualizacion(actualizada, null));

  return _toSafeJSON(actualizada);
}

// Kore cancela la CxC de su lado (ej. CAC) mientras la solicitud sigue
// 'pendiente' en Numo — Kore avisa este endpoint DESPUÉS de que su propio
// usuario confirmó la cancelación, sabiendo que ya existía una solicitud
// previa en Numo (ver memoria "Solicitudes de Cobro ERP-Kore" para el caso
// real que originó esto). No hay aviso de vuelta a Kore (a diferencia de
// identificar/rechazar) — es Kore quien inicia esta acción, no Numo.
// canceladoPorUserId/canceladoPorNombre viajan tal cual los manda Kore en el
// body — no son necesariamente un usuario que Numo pueda resolver por su
// cuenta, se guardan para mostrar "Cancelado por el usuario X" en la bandeja.
async function cancelarPorErp(solicitudIdErp, { canceladoPorUserId, canceladoPorNombre } = {}) {
  const cr = await CollectionRequest.findOne({ solicitudIdErp: String(solicitudIdErp).trim() });
  if (!cr) throw new NotFoundError('Solicitud');
  if (cr.status !== 'pendiente') {
    throw new BadRequestError(`La solicitud ya fue resuelta (status: ${cr.status}), no se puede cancelar.`);
  }

  // Guard atómico repetido a propósito (mismo criterio que rechazar()): entre
  // el findOne de arriba y este update, un revisor pudo haber identificado o
  // rechazado la solicitud desde la bandeja — el filtro evita pisar ese resultado.
  const actualizada = await CollectionRequest.findOneAndUpdate(
    { _id: cr._id, status: 'pendiente' },
    {
      status:              'cancelada',
      canceladoPorUserId:  canceladoPorUserId || null,
      canceladoPorNombre:  canceladoPorNombre || null,
      canceladoAt:         new Date(),
    },
    { new: true },
  );
  if (!actualizada) throw new BadRequestError('La solicitud ya fue resuelta, no se puede cancelar.');

  // Tiempo real: si alguien tiene la bandeja abierta, la fila debe desaparecer
  // de "Pendientes" (o pasar a "Canceladas" si esa es la pestaña activa) sin
  // que nadie tenga que recargar — mismo mecanismo que identificar()/rechazar().
  emitToAll('collection-request:updated', _eventoActualizacion(actualizada, null));

  return _toSafeJSON(actualizada);
}

// ── Reporte Excel de Solicitudes de Cobro (2026-07-29) ──────────────────────────
// Solo cubre solicitudes RESUELTAS (Autorizadas = identificada, Rechazadas =
// rechazada) — nunca pendiente, sin importar qué pestaña esté activa en el
// frontend (el botón vive en la barra de filtros compartida, no dentro de una
// sola pestaña). El status va HARDCODEADO al armar el filtro: nunca se lee
// filters.status aquí, para que no haya forma de que un query param lo cambie.
const XLSX_HEADER_FILL = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF6D28D9' } };
const XLSX_HEADER_FONT = { bold: true, color: { argb: 'FFFFFFFF' }, size: 10 };

function _xlsxFormatFecha(d) {
  if (!d) return '';
  const dt = d instanceof Date ? d : new Date(d);
  if (isNaN(dt.getTime())) return '';
  return dt.toLocaleDateString('es-MX', { day: '2-digit', month: '2-digit', year: 'numeric' });
}

function _xlsxStyleHeader(ws) {
  ws.getRow(1).eachCell(cell => {
    cell.fill = XLSX_HEADER_FILL;
    cell.font = XLSX_HEADER_FONT;
    cell.alignment = { vertical: 'middle', horizontal: 'center' };
  });
  ws.getRow(1).height = 20;
}

// Mismo criterio de resumen que folioLabel() en collection-request.component.ts:
// una sola CxC se muestra directo (serie-folio), varias se resumen a "N CxC" —
// el detalle completo de cada una solo aparece en la columna "CxC detalle" del
// reporte rico (RICH).
function _cxcResumen(cxcs) {
  if (!cxcs || cxcs.length === 0) return '';
  if (cxcs.length === 1) {
    const c = cxcs[0];
    return c.serie && c.folioExterno ? `${c.serie}-${c.folioExterno}` : (c.folioExterno || c.erpId);
  }
  return `${cxcs.length} CxC`;
}

// Detalle por CxC (solo reporte rico) — Modo 2 (varias CxC) es el caso que más
// aporta acá, ya que el resumen de arriba las colapsa a "N CxC"; se incluye
// también en Modo 1 por consistencia de columna entre filas.
function _cxcDetalle(cxcs) {
  if (!cxcs || cxcs.length === 0) return '';
  return cxcs.map(c => {
    const folio = c.serie && c.folioExterno ? `${c.serie}-${c.folioExterno}` : (c.folioExterno || c.erpId);
    const monto = c.montoAsignado ?? c.total ?? 0;
    return `${c.nombrePersona || '—'} (${folio}, ${c.tipoPago || '—'}, ${monto.toFixed(2)})`;
  }).join('; ');
}

function _formaPagoDetalle(formasPago) {
  return (formasPago || [])
    .map(f => `${f.bancoDescripcion || ''} ${f.referencia || ''}`.trim())
    .filter(Boolean)
    .join('; ');
}

function _columnasReporte(rico) {
  const base = [
    { header: 'Folio de solicitud',     key: 'folio',            width: 16 },
    { header: 'Fecha de solicitud',      key: 'fechaSolicitud',    width: 14 },
    { header: 'Estatus',                 key: 'estatus',           width: 12 },
    { header: 'Monto',                   key: 'monto',             width: 14 },
    { header: 'CxC',                     key: 'cxc',               width: 20 },
    { header: 'Forma de pago',           key: 'formaPago',         width: 20 },
    { header: 'Fecha de resolución',     key: 'fechaResolucion',   width: 16 },
    { header: 'Motivo de rechazo',       key: 'motivoRechazo',     width: 30 },
  ];
  if (!rico) return base;
  return [
    ...base,
    { header: 'Solicitó',                key: 'solicito',            width: 20 },
    { header: 'Banco',                   key: 'banco',               width: 14 },
    { header: 'Fecha del depósito',      key: 'fechaDeposito',       width: 16 },
    { header: 'Concepto',                key: 'concepto',            width: 26 },
    { header: 'Depósito',                key: 'deposito',            width: 14 },
    { header: 'Retiro',                  key: 'retiro',              width: 14 },
    { header: 'Autorización bancaria',   key: 'autorizacionBancaria', width: 20 },
    { header: 'Autorizó/Rechazó',        key: 'resolvio',            width: 20 },
    { header: 'Cobro aplicado',          key: 'cobroAplicado',       width: 14 },
    { header: 'Fecha cobro aplicado',    key: 'fechaCobroAplicado',  width: 16 },
    { header: 'CxC detalle',             key: 'cxcDetalle',          width: 40 },
    { header: 'Forma de pago detalle',   key: 'formaPagoDetalle',    width: 30 },
  ];
}

function _filaReporte(cr, rico) {
  const fila = {
    folio:           cr.solicitudIdErp || '',
    fechaSolicitud:  _xlsxFormatFecha(cr.createdAt),
    estatus:         cr.status === 'identificada' ? 'Autorizada' : 'Rechazada',
    monto:           cr.monto ?? 0,
    cxc:             _cxcResumen(cr.cxcs),
    formaPago:       (cr.formasPago || []).map(f => f.formaPagoDescripcion).join(', '),
    fechaResolucion: _xlsxFormatFecha(cr.resueltoAt),
    motivoRechazo:   cr.motivoRechazo || '',
  };
  if (!rico) return fila;
  return {
    ...fila,
    solicito:             cr.solicitanteNombre || '',
    banco:                cr.bankMovementId?.banco || '',
    fechaDeposito:        _xlsxFormatFecha(cr.bankMovementId?.fecha),
    concepto:             cr.bankMovementId?.concepto || '',
    deposito:             cr.bankMovementId?.deposito ?? '',
    retiro:               cr.bankMovementId?.retiro ?? '',
    autorizacionBancaria: cr.bankMovementId?.numeroAutorizacion || '',
    resolvio:             cr.resueltoPorNombre || '',
    cobroAplicado:        cr.cobroAplicado ? 'Sí' : 'No',
    fechaCobroAplicado:   _xlsxFormatFecha(cr.cobroAplicadoAt),
    cxcDetalle:           _cxcDetalle(cr.cxcs),
    formaPagoDetalle:     _formaPagoDetalle(cr.formasPago),
  };
}

// Dos hojas fijas (Autorizadas/Rechazadas) sobre el mismo arreglo `data` (ya
// viene acotado a solo esos dos status desde buildReport/buildReportMine) — se
// reparte por status al llenar cada hoja, no con dos queries separadas.
async function _generarExcelSolicitudes(data, { rico }) {
  const wb = new ExcelJS.Workbook();
  wb.creator = 'Numo — Solicitudes de Cobro';
  wb.created = new Date();

  const columnas    = _columnasReporte(rico);
  const currencyKeys = rico ? ['monto', 'deposito', 'retiro'] : ['monto'];

  const wsAut = wb.addWorksheet('Autorizadas');
  wsAut.columns = columnas;
  _xlsxStyleHeader(wsAut);
  for (const cr of data.filter(d => d.status === 'identificada')) wsAut.addRow(_filaReporte(cr, rico));
  currencyKeys.forEach(k => { wsAut.getColumn(k).numFmt = '#,##0.00'; });
  if (wsAut.lastColumn) wsAut.autoFilter = { from: 'A1', to: wsAut.lastColumn.letter + '1' };

  const wsRec = wb.addWorksheet('Rechazadas');
  wsRec.columns = columnas;
  _xlsxStyleHeader(wsRec);
  for (const cr of data.filter(d => d.status === 'rechazada')) wsRec.addRow(_filaReporte(cr, rico));
  currencyKeys.forEach(k => { wsRec.getColumn(k).numFmt = '#,##0.00'; });
  if (wsRec.lastColumn) wsRec.autoFilter = { from: 'A1', to: wsRec.lastColumn.letter + '1' };

  return wb.xlsx.writeBuffer();
}

// Reporte completo (cobranza/contabilidad/admin, collections:write) — mismo
// filtro de búsqueda/fecha que list(), pero el status queda FIJO a resueltas
// (nunca se acepta filters.status: si se leyera, un query ?status=pendiente
// podría colar solicitudes pendientes al reporte). Columnas RICAS: incluye el
// movimiento bancario vinculado y quién resolvió — información que tienda no
// necesita ver de solicitudes ajenas.
async function buildReport(filters) {
  const { search, fechaInicio, fechaFin } = filters;
  const filter = {
    ..._buildBusquedaFilter({ search, fechaInicio, fechaFin }),
    status: { $in: ['identificada', 'rechazada'] },
  };
  const data = await CollectionRequest.find(filter)
    .sort({ createdAt: 1 })
    .select('-comprobante.data')
    .populate('bankMovementId', 'banco fecha concepto deposito retiro numeroAutorizacion referenciaNumerica')
    .lean();
  return _generarExcelSolicitudes(data, { rico: true });
}

// Reporte acotado (rol tienda, collections:read) — mismas solicitudes propias
// que listMine(), solo resueltas. Columnas MÍNIMAS: sin movimiento bancario ni
// quién resolvió — tienda no necesita ese detalle interno, solo el resultado.
async function buildReportMine(userId, filters) {
  const { search, fechaInicio, fechaFin } = filters;
  const filter = {
    ..._buildBusquedaFilter({ search, fechaInicio, fechaFin }),
    status: { $in: ['identificada', 'rechazada'] },
    solicitanteUserId: userId,
  };
  const data = await CollectionRequest.find(filter)
    .sort({ createdAt: 1 })
    .select('-comprobante.data')
    .lean();
  return _generarExcelSolicitudes(data, { rico: false });
}

module.exports = {
  analyzeReceipt, create, list, listMine, getById, getByErpId, getComprobante, analyzeStoredComprobantes,
  identificar, rechazar, cancelarPorErp, stats, statsMine, buildReport, buildReportMine,
  // Re-expuesta para pruebas standalone (mismo patrón que erp.routes.js) — filtro puro,
  // sin I/O, seguro de probar aislado.
  _buildBusquedaFilter,
};
