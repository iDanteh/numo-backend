'use strict';

const express = require('express');
const multer  = require('multer');
const axios   = require('axios');
const { authenticate, permit }           = require('../../shared/middleware/auth.real');
const { asyncHandler }                   = require('../../shared/middleware/error-handler');
const { sincronizarCuentasPendientes }   = require('./erp-sync.service');
const { procesarRefacturacionesCyc }     = require('./refacturaciones-cyc.service');
const { procesarMostradorCyc,
        generarExcelMostradorCyc }       = require('./mostrador-cyc.service');
const { procesarPagosCyc,
        generarExcelPagosCyc }           = require('./pagos-cyc.service');
const BankMovement                       = require('../banks/BankMovement.model');
const { emitToUser }                     = require('../../shared/socket');

const uploadCyc = multer({
  storage: multer.memoryStorage(),
  limits:  { fileSize: 20 * 1024 * 1024 },
  fileFilter: (_req, file, cb) => {
    const ok = /\.(xlsx|xls)$/i.test(file.originalname);
    cb(ok ? null : new Error('Solo se aceptan archivos Excel (.xlsx, .xls)'), ok);
  },
});

const router = express.Router();

// KORE_AUTH_URL apunta a producción (auth real de usuarios).
// El resto de las URLs Kore apuntan al ambiente de pruebas.
const KORE_AUTH_URL            = (process.env.KORE_AUTH_URL            || 'https://app.login.tubosyconexiones.mx/logink/tokenKore');
const KORE_SERVICIO            = process.env.KORE_SERVICIO             || '6491faf156358100016565e5';
const KORE_CAJA_URL            = (process.env.KORE_CAJA_URL            || 'https://test.cajas.koreingenieria.com/index');
const KORE_CAJA_BASE_URL       = (process.env.KORE_CAJA_BASE_URL       || 'https://test.cajas.koreingenieria.com');
const KORE_FORMASPAGO_BASE_URL = (process.env.KORE_FORMASPAGO_BASE_URL || 'https://test.formaspagos.koreingenieria.com');

// Token Kore por usuario — se guarda cuando verifica sesión de caja, se usa en proxy de cobros
const koreTokenCache = new Map(); // auth0Id → koreToken

const ERP_PAGE_SIZE = 50;

// GET /api/erp/cuentas-pendientes
// Parámetros: fechaDesde, fechaHasta, estadoCobro (opcional; 'pendiente' para solo pendientes), page
// La paginación se aplica localmente sobre la respuesta completa del ERP.
router.get('/cuentas-pendientes', authenticate, asyncHandler(async (req, res) => {
  const { fechaDesde, fechaHasta, estadoCobro, page, serieExterna, folioExterno, nombrePersona } = req.query;
  const pageNum = Math.max(1, parseInt(page ?? '1', 10));

  // sincronizarCuentasPendientes llama al ERP, upserta en el caché y devuelve los
  // datos crudos para que este endpoint pueda construir la respuesta paginada.
  let raw = [];
  try {
    ({ raw } = await sincronizarCuentasPendientes({
      fechaDesde, fechaHasta, estadoCobro, serieExterna, folioExterno, nombrePersona,
    }));
  } catch (err) {
    if (err.message?.includes('ERP no configurado')) {
      return res.status(503).json({ error: err.message });
    }
    throw err;
  }

  const allCuentas = raw.map(c => ({
    id:                   c.id,
    serie:                c.serie                ?? null,
    folio:                c.folio                ?? null,
    serieExterna:         c.serieExterna         ?? null,
    folioExterno:         c.folioExterno         ?? null,
    folioFiscal:          c.folioFiscal          ?? null,
    tipoPago:             c.tipoPago             ?? null,
    subtotal:             c.subtotal,
    impuesto:             c.impuesto,
    total:                c.total,
    saldoActual:          c.saldoActual,
    fechaVencimiento:     c.fechaVencimiento     ?? null,
    nombrePersona:        c.nombrePersona        ?? null,
    nombreTipoMovimiento: c.nombreTipoMovimiento ?? null,
    personaId:            c.personaId            ?? null,
  }));

  // Local pagination (filtering is now handled server-side by the ERP via serieExterna/folioExterno)
  const total        = allCuentas.length;
  const totalPaginas = Math.max(1, Math.ceil(total / ERP_PAGE_SIZE));
  const safePage     = Math.min(pageNum, totalPaginas);
  const start        = (safePage - 1) * ERP_PAGE_SIZE;
  const cuentas      = allCuentas.slice(start, start + ERP_PAGE_SIZE);

  res.json({
    data: cuentas,
    pagination: { page: safePage, totalPaginas, total },
  });
}));


// POST /api/erp/match/revert
// Deshace todas las asociaciones realizadas por el motor automático.
// Cubre DOS userIds del motor:
//   'erp-auto'  — Motor Match ERP (bank-autorizaciones.service.js → matchAutorizacionesDesdeErp)
//   'aut-match' — Motor Match Excel (bank-autorizaciones.service.js → ejecutarMatch)
// Protege el trabajo de usuarios humanos (cualquier userId distinto de los dos anteriores):
// · Limpia erpIds, erpLinks, saldoErp, uuidXML en todos los casos.
// · Elimina SOLO las entradas del motor de identificadoPor (preserva las humanas).
// · Resetea status a 'no_identificado' únicamente cuando no quedan entradas humanas.
const MOTOR_USER_IDS = ['erp-auto', 'aut-match'];

router.post('/match/revert', authenticate, permit('erp:manage'), asyncHandler(async (_req, res) => {
  const result = await BankMovement.updateMany(
    { 'identificadoPor.userId': { $in: MOTOR_USER_IDS } },
    [
      {
        $set: {
          erpIds:   [],
          erpLinks: [],
          saldoErp: null,
          uuidXML:  null,
          // Eliminar entradas de ambos motores; conservar las de usuarios humanos
          identificadoPor: {
            $filter: {
              input: '$identificadoPor',
              as:    'entry',
              cond:  { $not: { $in: ['$$entry.userId', MOTOR_USER_IDS] } },
            },
          },
          // Resetear status solo si ya no quedan entradas humanas
          status: {
            $cond: {
              if: {
                $eq: [
                  {
                    $size: {
                      $filter: {
                        input: '$identificadoPor',
                        as:    'e',
                        cond:  { $not: { $in: ['$$e.userId', MOTOR_USER_IDS] } },
                      },
                    },
                  },
                  0,
                ],
              },
              then: 'no_identificado',
              else: '$status',
            },
          },
        },
      },
    ],
  );

  res.json({
    reverted: result.modifiedCount,
    message:  `${result.modifiedCount} asociación(es) revertida(s)`,
  });
}));

// ── POST /api/erp/refacturaciones-cyc/upload ─────────────────────────────────
// Procesa el Excel "REFACTURACIONES CYC":
//   • Columna 1 CONCEPTO    → tokens numéricos para encontrar el BankMovement
//   • Columna 2 IMPORTE     → validación de monto (tolerancia ±$1 MXN)
//   • Columna 3 BANCO       → preferencia de banco en la búsqueda
//   • Columna 4 SERIE/FOLIO → lookup exacto de ErpCuentaPendiente (determinístico)
//
// Responde con { total, auto, review, escritos, errors, detalleNoMatcheados }
// Los "auto"   se vinculan en DB inmediatamente (Tier 1: auth + importe).
// Los "review" NO se escriben en DB; se retornan para revisión manual.
router.post('/refacturaciones-cyc/upload',
  authenticate,
  permit('banks:admin'),
  uploadCyc.single('excelFile'),
  asyncHandler(async (req, res) => {
    if (!req.file) return res.status(400).json({ error: 'No se envió ningún archivo Excel' });
    const result = await procesarRefacturacionesCyc(
      req.file.buffer,
      req.user._id,
      req.user.nombre,
    );
    res.json(result);
  }),
);

// ── POST /api/erp/mostrador-cyc/upload ───────────────────────────────────────
// Procesa el Excel "MOSTRADOR CYC":
//   • Col 1 FECHA        → informativo
//   • Col 2 DESCRIPCIÓN  → match exacto contra BankMovement.concepto
//   • Col 3 IMPORTE      → match exacto contra BankMovement.deposito
//   • Col 4 BANCO        → informativo
//   • Col 5 VENTAS       → folio(s) de ErpCuentaPendiente (serie-folio)
//   • Col 6 CLIENTE      → informativo
//
// Reglas:
//   · Filas sin VENTAS o importe inválido → ignoradas (se reportan)
//   · Match: BankMovement donde concepto===DESCRIPCIÓN Y deposito===IMPORTE
//   · No sobreescribe movimientos con status='identificado' o erpLinks existentes
//   · Marca el movimiento como 'identificado' al vincular
router.post('/mostrador-cyc/upload',
  authenticate,
  permit('banks:admin'),
  uploadCyc.single('excelFile'),
  asyncHandler(async (req, res) => {
    if (!req.file) return res.status(400).json({ error: 'No se envió ningún archivo Excel' });
    const result = await procesarMostradorCyc(
      req.file.buffer,
      req.user._id,
      req.user.nombre,
    );
    res.json(result);
  }),
);

// ── POST /api/erp/mostrador-cyc/export ───────────────────────────────────────
// Genera un Excel con 3 hojas a partir del resultado del upload:
//   · Hoja "Relacionados"    — movimientos vinculados exitosamente
//   · Hoja "No Relacionados" — con razón y detalle del fallo
//   · Hoja "Ignorados"       — registros sin columna VENTAS
router.post('/mostrador-cyc/export',
  authenticate,
  permit('banks:admin'),
  asyncHandler(async (req, res) => {
    const resultado = req.body;
    if (!resultado || typeof resultado !== 'object') {
      return res.status(400).json({ error: 'Se requiere el resultado del procesamiento en el cuerpo' });
    }

    const buffer = await generarExcelMostradorCyc(resultado);

    const fecha = new Date().toISOString().slice(0, 10);
    res.setHeader('Content-Type',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition',
      `attachment; filename="mostrador-cyc-${fecha}.xlsx"`);
    res.send(buffer);
  }),
);

// ── POST /api/erp/pagos-cyc/upload ───────────────────────────────────────────
// Procesa el Excel "PAGOS CYC":
//   • Col 1 FECHA        → informativo / Tier 3 de matching
//   • Col 2 DESCRIPCIÓN  → match contra BankMovement.concepto (normalizado)
//   • Col 3 MONTO        → match exacto contra BankMovement.deposito
//   • Col 4 BANCO        → preferencia de banco (con canonicalización Bancomer↔BBVA)
//   • Col 5 VENTAS       → folio(s) de ErpCuentaPendiente (serie-folio)
//
// Reglas:
//   · Filas sin VENTAS o monto inválido → ignoradas (se reportan)
//   · Matching 4 tiers: auth+concepto → auth solo → fecha → fallback
//   · NO sobreescribe movimientos con status='identificado' o erpLinks existentes
//   · Guard ACID en bulkWrite: protege explícitamente contra race conditions con
//     trabajo humano ($nor identificadoPor + erpLinks.0 $exists false)
router.post('/pagos-cyc/upload',
  authenticate,
  permit('banks:admin'),
  uploadCyc.single('excelFile'),
  asyncHandler(async (req, res) => {
    if (!req.file) return res.status(400).json({ error: 'No se envió ningún archivo Excel' });
    const result = await procesarPagosCyc(
      req.file.buffer,
      req.user._id,
      req.user.nombre,
    );
    res.json(result);
  }),
);

// ── POST /api/erp/pagos-cyc/export ───────────────────────────────────────────
// Genera un Excel con 3 hojas a partir del resultado del upload:
//   · Hoja "Relacionados"    — movimientos vinculados exitosamente (verde)
//   · Hoja "No Relacionados" — con razón y detalle del fallo (rojo/amarillo)
//   · Hoja "Ignorados"       — registros sin columna VENTAS válida (gris)
router.post('/pagos-cyc/export',
  authenticate,
  permit('banks:admin'),
  asyncHandler(async (req, res) => {
    const resultado = req.body;
    if (!resultado || typeof resultado !== 'object') {
      return res.status(400).json({ error: 'Se requiere el resultado del procesamiento en el cuerpo' });
    }

    const buffer = await generarExcelPagosCyc(resultado);

    const fecha = new Date().toISOString().slice(0, 10);
    res.setHeader('Content-Type',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition',
      `attachment; filename="pagos-cyc-${fecha}.xlsx"`);
    res.send(buffer);
  }),
);

// GET /api/erp/formas-pago — catálogo de formas de pago desde Kore (test)
router.get('/formas-pago', authenticate, asyncHandler(async (req, res) => {
  const koreToken = getKoreToken(req, res);
  if (!koreToken) return;

  const response = await axios.get(`${KORE_FORMASPAGO_BASE_URL}/api/formasdepago`, {
    headers: { Authorization: `Bearer ${koreToken}` },
    timeout: 10000,
  });

  const raw = response.data?.Data ?? [];
  const formas = raw
    .filter(f => f.Estatus === true)
    .map(f => ({
      id:             f.ID,
      nombre:         f.Nombre,
      claveSAT:       f.ClaveSAT,
      esBancarizada:  f.EsBancarizada  ?? false,
      reqNombreBanco: f.ReqNombreBanco ?? false,
    }));

  res.json(formas);
}));

// ── Helpers de cobros ────────────────────────────────────────────────────────

function getKoreToken(req, res) {
  const token = req.headers['x-kore-token'] || koreTokenCache.get(req.user._id);
  if (!token) {
    console.warn(`[cobros] koreToken no encontrado para usuario ${req.user._id}`);
    res.status(401).json({ error: 'Sesión de caja no iniciada. Regresa al panel y verifica tu sesión.' });
    return null;
  }
  return token;
}

// GET /api/erp/cobros/sesion-caja
// Obtiene el ID de sesión de caja activa en dos pasos:
//   1. Intercambia el sub de Auth0 por un token Kore
//   2. Consulta la sesión activa de caja con ese token
router.get('/cobros/sesion-caja', authenticate, asyncHandler(async (req, res) => {
  const auth0Id = req.user._id;  // e.g. "auth0|xxxx…"

  // Paso 1: obtener token Kore a partir del ID Auth0
  let koreToken;
  try {
    const tokenRes = await axios.get(KORE_AUTH_URL, {
      params:  { id: auth0Id, servicio: KORE_SERVICIO },
      timeout: 10000,
    });
    if (tokenRes.data?.Codigo !== 200 || !tokenRes.data?.Data) {
      return res.status(502).json({ error: 'No se pudo obtener el token de caja. Verifica tu acceso al sistema.' });
    }
    koreToken = tokenRes.data.Data;
    koreTokenCache.set(auth0Id, koreToken);  // disponible para proxies de cobros
  } catch {
    return res.status(502).json({ error: 'Error al conectar con el servidor de autenticación de caja.' });
  }

  // Paso 2: obtener sesión activa con el token Kore
  try {
    const sesionRes = await axios.get(KORE_CAJA_URL, {
      headers: { Authorization: `Bearer ${koreToken}` },
      timeout: 10000,
    });
    if (sesionRes.data?.Codigo !== 200 || !sesionRes.data?.Data?.sesion?.Id) {
      return res.status(502).json({ error: 'No se encontró sesión de caja activa. Inicia sesión en el sistema de caja primero.' });
    }
    console.log(`[cobros] sesion-caja OK para usuario ${auth0Id}, sesionId: ${sesionRes.data.Data.sesion.Id}`);
    return res.json({ sesionId: sesionRes.data.Data.sesion.Id, koreToken });
  } catch {
    return res.status(502).json({ error: 'Error al obtener la sesión de caja.' });
  }
}));

// GET /api/erp/cobros/bancos — catálogo de bancos Kore para aplicación de cobro
router.get('/cobros/bancos', authenticate, asyncHandler(async (req, res) => {
  const koreToken = getKoreToken(req, res);
  if (!koreToken) return;

  console.log(`[cobros/bancos] llamando a ${KORE_FORMASPAGO_BASE_URL}/api/bancos`);
  const r = await axios.get(`${KORE_FORMASPAGO_BASE_URL}/api/bancos`, {
    headers: { Authorization: `Bearer ${koreToken}` },
    timeout: 10000,
  });

  const bancos = (r.data?.Data ?? [])
    .filter(b => b.Activo !== false)
    .map(b => ({
      id:          b.ID,
      nombre:      b.Nombre       ?? '',
      claveBanco:  b.ClaveBanco   ?? '',
      descripcion: b.Descripcion  ?? '',
    }));

  res.json(bancos);
}));

// GET /api/erp/cobros/conceptos — conceptos de caja tipo ENTRADA
router.get('/cobros/conceptos', authenticate, asyncHandler(async (req, res) => {
  const koreToken = getKoreToken(req, res);
  if (!koreToken) return;

  console.log(`[cobros/conceptos] llamando a ${KORE_CAJA_BASE_URL}/conceptos`);
  const r = await axios.get(`${KORE_CAJA_BASE_URL}/conceptos`, {
    params:  { TYPE: 'ENTRADA' },
    headers: { Authorization: `Bearer ${koreToken}` },
    timeout: 10000,
  });

  const conceptos = (r.data?.Data ?? []).map(c => ({
    id:          c.Id,
    nombre:      c.Nombre      ?? '',
    abreviatura: c.Abreviatura ?? '',
  }));

  res.json(conceptos);
}));

// GET /api/erp/cobros/cuentas?ids=<id1>,<id2>
// Consulta el detalle de una o varias CxC en Kore, incluyendo políticas de descuento
// (pronto pago). Devuelve Descuentos[] y SaldoActualCalculado por cuenta.
router.get('/cobros/cuentas', authenticate, asyncHandler(async (req, res) => {
  const koreToken = getKoreToken(req, res);
  if (!koreToken) return;

  const { ids } = req.query;
  if (!ids) {
    return res.status(400).json({ error: 'Se requiere el parámetro ids.' });
  }

  let r;
  try {
    r = await axios.get(`${KORE_CAJA_BASE_URL}/cuentas`, {
      params:  { ids },
      headers: { Authorization: `Bearer ${koreToken}` },
      timeout: 10000,
    });
  } catch (axiosErr) {
    if (axiosErr.response) {
      const body = axiosErr.response.data ?? {};
      const msg  = body.Mensaje || body.message || body.error
        || `Error al consultar cuentas (${axiosErr.response.status})`;
      console.warn(`[cobros/cuentas] Kore rechazó con ${axiosErr.response.status}:`, body);
      return res.status(axiosErr.response.status).json({ error: msg });
    }
    throw axiosErr;
  }

  const cuentas = (r.data?.Data?.cuentas ?? []).map(c => ({
    id:                   c.Id,
    serie:                c.Serie            ?? null,
    folio:                c.Folio            ?? null,
    tipoPago:             c.TipoPago         ?? null,
    total:                c.Total,
    saldoActual:          c.SaldoActual,
    saldoActualCalculado: c.SaldoActualCalculado ?? c.SaldoActual,
    descuentos: (c.Descuentos ?? []).map(d => ({
      idPolitica:     d.IDPolitica,
      dias:           d.Dias,
      porcentaje:     d.Porcentaje,
      monto:          d.Monto,
      iniciado:       d.Iniciado      ?? false,
      diasTolerancia: d.DiasTolerancia ?? 0,
    })),
  }));

  console.log(`[cobros/cuentas] ids=${ids} → ${cuentas.length} cuentas, ${cuentas.filter(c => c.descuentos.length > 0).length} con descuento`);
  res.json(cuentas);
}));

// POST /api/erp/cobros/operacion/:sesionId — aplica cobro a una CxC en el sistema de caja
router.post('/cobros/operacion/:sesionId', authenticate, asyncHandler(async (req, res) => {
  const koreToken = getKoreToken(req, res);
  if (!koreToken) return;

  const { sesionId } = req.params;

  const body = req.body;
  console.log('[cobros/operacion] payload →', JSON.stringify({
    cuenta: body.cuenta,
    concepto: body.detalle?.concepto,
    formasPago: (body.detalle?.DetalleFormaPago ?? []).map(f => ({ id: f.FormaPagoID, nombre: f.FormaPagoNombre, monto: f.Monto })),
    anticipos: body.anticipos,
    saldosAFavorAUsar: body.saldosAFavorAUsar,
  }));

  let r;
  try {
    r = await axios.post(
      `${KORE_CAJA_BASE_URL}/sesiones/${sesionId}/operaciones`,
      body,
      {
        headers: {
          Authorization:  `Bearer ${koreToken}`,
          'Content-Type': 'application/json',
        },
        timeout: 15000,
      },
    );
  } catch (axiosErr) {
    if (axiosErr.response) {
      // Kore devolvió un error HTTP (4xx/5xx) — reenviar su cuerpo tal cual
      const koreBody = axiosErr.response.data ?? {};
      const msg = (typeof koreBody.Data === 'string' ? koreBody.Data : null)
        || koreBody.Mensaje || koreBody.message || koreBody.error
        || `Error al registrar el cobro en caja (${axiosErr.response.status})`;
      console.warn(`[cobros/operacion] Kore rechazó con ${axiosErr.response.status}:`, koreBody);
      return res.status(axiosErr.response.status).json({ error: msg, kore: koreBody });
    }
    throw axiosErr; // error de red o timeout — dejar que asyncHandler lo maneje
  }

  res.status(r.status).json(r.data);
}));

// POST /api/erp/cobros/operacion-multiple/:sesionId — aplica cobro a múltiples CxC en una sola operación
router.post('/cobros/operacion-multiple/:sesionId', authenticate, asyncHandler(async (req, res) => {
  const koreToken = getKoreToken(req, res);
  if (!koreToken) return;

  const { sesionId } = req.params;

  const body = req.body;
  console.log('[cobros/operacion-multiple] payload →', JSON.stringify({
    cuentas: (body.cuentas ?? []).map(c => ({ id: c.CuentaID, monto: c.Monto })),
    concepto: body.detalle?.concepto,
    formasPago: (body.detalle?.DetalleFormaPago ?? []).map(f => ({ id: f.FormaPagoID, nombre: f.FormaPagoNombre, monto: f.Monto })),
    anticipos: body.anticipos,
    saldosAFavorAUsar: body.saldosAFavorAUsar,
  }));

  let r;
  try {
    r = await axios.post(
      `${KORE_CAJA_BASE_URL}/sesiones/${sesionId}/operacionesmultiples`,
      body,
      {
        headers: {
          Authorization:  `Bearer ${koreToken}`,
          'Content-Type': 'application/json',
        },
        timeout: 15000,
      },
    );
  } catch (axiosErr) {
    if (axiosErr.response) {
      const koreBody = axiosErr.response.data ?? {};
      const msg = (typeof koreBody.Data === 'string' ? koreBody.Data : null)
        || koreBody.Mensaje || koreBody.message || koreBody.error
        || `Error al registrar el cobro múltiple en caja (${axiosErr.response.status})`;
      console.warn(`[cobros/operacion-multiple] Kore rechazó con ${axiosErr.response.status}:`, koreBody);
      return res.status(axiosErr.response.status).json({ error: msg, kore: koreBody });
    }
    throw axiosErr;
  }

  res.status(r.status).json(r.data);
}));

// GET /api/erp/cobros/saldos-favor/buscar?serie=A0&folio=260100035&esAnticipo=false
// Busca saldos a favor o anticipos en un movimiento específico por Serie-Folio.
// Usa personaId=0 en Kore para búsqueda por folio en lugar de por cliente.
// IMPORTANTE: debe ir antes de /:personaId para que Express no capture "buscar" como param.
router.get('/cobros/saldos-favor/buscar', authenticate, asyncHandler(async (req, res) => {
  const koreToken = getKoreToken(req, res);
  if (!koreToken) return;

  const { serie, folio, esAnticipo } = req.query;
  if (!serie || !folio) {
    return res.status(400).json({ error: 'Parámetros serie y folio son requeridos.' });
  }

  const buscarAnticipo = esAnticipo === 'true';

  let r;
  try {
    r = await axios.get(
      `${KORE_CAJA_BASE_URL}/anticipos/0`,
      {
        params:  { serie, folio, esAnticipo: buscarAnticipo },
        headers: { Authorization: `Bearer ${koreToken}` },
        timeout: 10000,
      },
    );
  } catch (axiosErr) {
    if (axiosErr.response) {
      const body = axiosErr.response.data ?? {};
      const msg  = body.Mensaje || body.message || body.error
        || `Error al buscar saldo (${axiosErr.response.status})`;
      console.warn(`[cobros/saldos-favor/buscar] Kore rechazó con ${axiosErr.response.status}:`, body);
      return res.status(axiosErr.response.status).json({ error: msg });
    }
    throw axiosErr;
  }

  const cuentas         = r.data?.Data?.cuentas      ?? [];
  const saldosAFavorRaw = r.data?.Data?.saldosAFavor ?? [];

  const anticipos = cuentas
    .filter(c => c.EsAnticipo === true && !c.Cancelado && typeof c.SaldoActual === 'number' && c.SaldoActual < 0)
    .map(c => ({
      id:          c.Id,
      descripcion: `${c.Serie}-${c.Folio}`,
      monto:       Math.abs(c.SaldoActual),
      fecha:       c.FechaCreacion ?? null,
      tipo:        'anticipo',
    }));

  const saldosFavor = saldosAFavorRaw
    .filter(s => typeof s.SaldoActual === 'number' && s.SaldoActual < 0)
    .flatMap(s => {
      const cuentaDescripcion = `${s.Serie}-${s.Folio}`;
      const origenes = Array.isArray(s.Origenes) ? s.Origenes : [];
      if (origenes.length === 0) {
        return [{ id: s.MovimientoID, descripcion: cuentaDescripcion, monto: Math.abs(s.SaldoActual), fecha: s.FechaCreacion ?? null, tipo: 'saldo_favor', cuentaDescripcion: null }];
      }
      return origenes
        .filter(o => typeof o.SaldoActual === 'number' && o.SaldoActual < 0)
        .map(o => ({ id: o.MovimientoID, descripcion: `${o.Serie}-${o.Folio}`, monto: Math.abs(o.SaldoActual), fecha: o.FechaCreacion ?? null, tipo: 'saldo_favor', cuentaDescripcion }));
    });

  const filtrado = buscarAnticipo ? anticipos : saldosFavor;
  console.log(`[cobros/saldos-favor/buscar] serie=${serie} folio=${folio} esAnticipo=${buscarAnticipo} → ${filtrado.length}`);
  res.json(filtrado);
}));

// GET /api/erp/cobros/saldos-favor/:personaId
// Lista anticipos y saldos a favor disponibles del cliente desde Kore.
// Kore devuelve registros mezclados: SerieExterna==='OPA' → anticipo, resto → saldo_favor.
// Params: tipo = 'saldo_favor' | 'compensacion' → filtra saldo_favor; 'anticipo' → filtra OPA; ausente → todos.
router.get('/cobros/saldos-favor/:personaId', authenticate, asyncHandler(async (req, res) => {
  const koreToken = getKoreToken(req, res);
  if (!koreToken) return;

  const { personaId } = req.params;
  const { tipo } = req.query;

  let r;
  try {
    r = await axios.get(
      `${KORE_CAJA_BASE_URL}/anticipos/${encodeURIComponent(personaId)}`,
      {
        headers: { Authorization: `Bearer ${koreToken}` },
        timeout: 10000,
      },
    );
  } catch (axiosErr) {
    if (axiosErr.response) {
      const body = axiosErr.response.data ?? {};
      const msg  = body.Mensaje || body.message || body.error
        || `Error al consultar saldos disponibles (${axiosErr.response.status})`;
      console.warn(`[cobros/saldos-favor] Kore rechazó con ${axiosErr.response.status}:`, body);
      return res.status(axiosErr.response.status).json({ error: msg });
    }
    throw axiosErr;
  }

  const cuentas         = r.data?.Data?.cuentas      ?? [];
  const saldosAFavorRaw = r.data?.Data?.saldosAFavor ?? [];

  // Anticipos: cuentas donde EsAnticipo === true con saldo disponible
  const anticipos = cuentas
    .filter(c => c.EsAnticipo === true && !c.Cancelado && typeof c.SaldoActual === 'number' && c.SaldoActual < 0)
    .map(c => ({
      id:          c.Id,
      descripcion: `${c.Serie}-${c.Folio}`,
      monto:       Math.abs(c.SaldoActual),
      fecha:       c.FechaCreacion ?? null,
      tipo:        'anticipo',
    }));

  // Saldos a favor: nodo saldosAFavor de la respuesta (estructura distinta a cuentas).
  // Cada entrada puede tener Origenes[] — los movimientos individuales disponibles.
  // Si hay Origenes, se expanden como ítems seleccionables con el padre como contexto.
  // Si no hay Origenes, se usa la propia entrada como ítem.
  const saldosFavor = saldosAFavorRaw
    .filter(s => typeof s.SaldoActual === 'number' && s.SaldoActual < 0)
    .flatMap(s => {
      const cuentaDescripcion = `${s.Serie}-${s.Folio}`;
      const origenes = Array.isArray(s.Origenes) ? s.Origenes : [];

      if (origenes.length === 0) {
        // Sin sub-movimientos — la cuenta es el ítem seleccionable
        return [{
          id:                s.MovimientoID,
          descripcion:       cuentaDescripcion,
          monto:             Math.abs(s.SaldoActual),
          fecha:             s.FechaCreacion ?? null,
          tipo:              'saldo_favor',
          cuentaDescripcion: null,
        }];
      }

      // Expandir Origenes: cada uno es un movimiento seleccionable
      return origenes
        .filter(o => typeof o.SaldoActual === 'number' && o.SaldoActual < 0)
        .map(o => ({
          id:                o.MovimientoID,
          descripcion:       `${o.Serie}-${o.Folio}`,
          monto:             Math.abs(o.SaldoActual),
          fecha:             o.FechaCreacion ?? null,
          tipo:              'saldo_favor',
          cuentaDescripcion, // referencia al padre para agrupar en UI
        }));
    });

  // Filtrar según el tipo solicitado
  const filtrado = tipo === 'anticipo'   ? anticipos
    : tipo === 'saldo_favor'             ? saldosFavor
    : [...anticipos, ...saldosFavor];    // compensacion o sin tipo → todos

  console.log(`[cobros/saldos-favor] personaId=${personaId} tipo=${tipo} → ${filtrado.length} (anticipos:${anticipos.length} saldos:${saldosFavor.length})`);
  res.json(filtrado);
}));

// ── POST /api/erp/sync-saldo-transferencia ────────────────────────────────────
// Backfill: itera todos los movimientos bancarios con CxC vinculada, consulta
// Kore por cada erpLink y actualiza saldoErp con la suma exacta de TRANSFERENCIA.
// Corre en background; emite progreso via Socket.IO al admin que lo disparó.

let saldoSyncRunning      = false;
let saldoSyncCurrentJobId = null;   // jobId del job activo (null si inactivo)
const saldoSyncControl    = { paused: false, stopped: false, pauseResolve: null };
const SALDO_SYNC_JOBS      = new Map(); // jobId → { status, auth0Sub, result?, error? }
const SALDO_SYNC_JOB_TTL   = 15 * 60 * 1000;
const SALDO_SYNC_DELAY_MS  = 500;        // pausa entre requests a Kore
const SALDO_SYNC_BACKOFF   = 25_000;     // fallback si Kore no indica cuánto esperar
const SALDO_SYNC_MAX_RETRIES = 4;        // intentos por link antes de contar como error

const _sleep = ms => new Promise(r => setTimeout(r, ms));

function _montoTransferenciaDeMovimientos(movimientos) {
  return (movimientos ?? [])
    .flatMap(m => m.formasPago ?? [])
    .filter(fp => fp.nombreFormaPago?.toUpperCase() === 'TRANSFERENCIA')
    .reduce((sum, fp) => sum + (fp.monto ?? 0), 0);
}

// Kore folios encode YYMMxxxxx (e.g. "260600250" → year=2026, month=06).
// Kore requires full ISO datetime and a max one-month window.
function _rangoDesdeFollo(folioExterno) {
  const str = String(folioExterno).trim();
  if (str.length < 4) return null;
  const yy = parseInt(str.slice(0, 2), 10);
  const mm = parseInt(str.slice(2, 4), 10);
  if (isNaN(yy) || isNaN(mm) || mm < 1 || mm > 12) return null;
  const year    = 2000 + yy;
  const mmStr   = String(mm).padStart(2, '0');
  const lastDay = new Date(Date.UTC(year, mm, 0)).getUTCDate();
  const lastStr = String(lastDay).padStart(2, '0');
  return {
    fechaDesde: `${year}-${mmStr}-01T00:00:00Z`,
    fechaHasta: `${year}-${mmStr}-${lastStr}T23:59:59Z`,
  };
}

// Extrae el tiempo de espera del mensaje de Kore: "retry after: 20.398s" → 20898 ms
function _parseRetryAfterMs(responseData) {
  const match = String(responseData?.Data ?? '').match(/retry after:\s*([\d.]+)s/i);
  return match ? Math.ceil(parseFloat(match[1]) * 1000) + 500 : null;
}

// Reintenta automáticamente en 429/503 respetando el tiempo que indica Kore.
// Lanza en el último intento fallido o ante cualquier error no recuperable.
async function _sincronizarConRetry(params) {
  for (let attempt = 0; attempt < SALDO_SYNC_MAX_RETRIES; attempt++) {
    try {
      return await sincronizarCuentasPendientes(params);
    } catch (err) {
      const status = err.response?.status;
      const retryable = status === 429 || status === 503;
      if (!retryable || attempt === SALDO_SYNC_MAX_RETRIES - 1) throw err;
      const wait = _parseRetryAfterMs(err.response?.data) ?? SALDO_SYNC_BACKOFF;
      await _sleep(wait);
    }
  }
}

// Verifica si el job debe continuar.
// Si está en pausa, aguarda hasta que llegue el resume.
// Devuelve false si fue detenido (el loop debe hacer break).
async function _checkSyncControl() {
  if (saldoSyncControl.stopped) return false;
  if (saldoSyncControl.paused) {
    await new Promise(resolve => { saldoSyncControl.pauseResolve = resolve; });
  }
  return !saldoSyncControl.stopped;
}

// Siempre salta movimientos ya marcados — cada ejecución continúa desde el checkpoint.
async function _syncSaldoJob(auth0Sub, jobId) {
  saldoSyncCurrentJobId = jobId;
  try {
    // null captura tanto el valor explícito null como documentos que aún no tienen el campo
    const filter = { 'erpIds.0': { $exists: true }, saldoErpSyncedAt: null };

    const movements = await BankMovement.find(filter)
      .select('_id erpLinks')
      .lean();

    let procesados = 0, actualizados = 0, sinTransferencia = 0, errores = 0;
    const total    = movements.length;
    let stopped    = false;

    emitToUser(auth0Sub, 'bank:erp:saldo:progress',
      { jobId, procesados, total, actualizados, sinTransferencia, errores, pct: 0 });

    for (const mov of movements) {
      if (!await _checkSyncControl()) { stopped = true; break; }

      const links = mov.erpLinks ?? [];
      let montoTotal = 0;

      for (const link of links) {
        if (!link.serie || !link.folioExterno) continue;
        const rango = _rangoDesdeFollo(link.folioExterno);
        if (!rango) continue;
        try {
          const { raw } = await _sincronizarConRetry({
            serieExterna: link.serie,
            folioExterno: String(link.folioExterno),
            fechaDesde:   rango.fechaDesde,
            fechaHasta:   rango.fechaHasta,
          });
          if (raw[0]) {
            montoTotal += _montoTransferenciaDeMovimientos(raw[0].movimientos);
          }
          await _sleep(SALDO_SYNC_DELAY_MS);
        } catch (err) {
          errores++;
        }
      }

      // Construir update: siempre marcar checkpoint; solo sobreescribir saldoErp si hubo monto
      const update = { saldoErpSyncedAt: new Date() };
      if (montoTotal > 0) { update.saldoErp = montoTotal; actualizados++; }
      else if (links.length > 0) sinTransferencia++;
      await BankMovement.findByIdAndUpdate(mov._id, update);

      procesados++;
      emitToUser(auth0Sub, 'bank:erp:saldo:progress', {
        jobId, procesados, total, actualizados, sinTransferencia, errores,
        pct: Math.round((procesados / total) * 100),
      });
    }

    if (stopped) {
      const result = { procesados, total, actualizados, sinTransferencia, errores };
      SALDO_SYNC_JOBS.set(jobId, { status: 'stopped', auth0Sub, result });
      emitToUser(auth0Sub, 'bank:erp:saldo:stopped', { jobId, ...result });
    } else {
      const result = { total, actualizados, sinTransferencia, errores };
      SALDO_SYNC_JOBS.set(jobId, { status: 'done', auth0Sub, result });
      emitToUser(auth0Sub, 'bank:erp:saldo:done', { jobId, ...result });
    }
  } catch (err) {
    const error = err.message || 'Error en sincronización de saldo ERP';
    SALDO_SYNC_JOBS.set(jobId, { status: 'error', auth0Sub, error });
    emitToUser(auth0Sub, 'bank:erp:saldo:error', { jobId, error });
  } finally {
    saldoSyncRunning      = false;
    saldoSyncCurrentJobId = null;
    setTimeout(() => SALDO_SYNC_JOBS.delete(jobId), SALDO_SYNC_JOB_TTL);
  }
}

router.post('/sync-saldo-transferencia', authenticate, permit('banks:admin'), asyncHandler(async (req, res) => {
  if (saldoSyncRunning) {
    return res.status(409).json({ error: 'Ya hay un proceso de sincronización en curso.' });
  }

  // Resetear control antes de cada job nuevo
  saldoSyncControl.paused       = false;
  saldoSyncControl.stopped      = false;
  saldoSyncControl.pauseResolve = null;

  saldoSyncRunning = true;
  const jobId    = `saldo-sync-${Date.now()}`;
  const auth0Sub = req.user._id;

  SALDO_SYNC_JOBS.set(jobId, { status: 'running', auth0Sub });
  res.status(202).json({ jobId });

  _syncSaldoJob(auth0Sub, jobId); // sin await — corre en background
}));

router.post('/sync-saldo-transferencia/pause', authenticate, permit('banks:admin'), asyncHandler(async (req, res) => {
  if (!saldoSyncRunning || saldoSyncControl.paused) {
    return res.status(409).json({ error: 'No hay sincronización activa para pausar.' });
  }
  saldoSyncControl.paused = true;
  if (saldoSyncCurrentJobId) {
    const job = SALDO_SYNC_JOBS.get(saldoSyncCurrentJobId);
    if (job) SALDO_SYNC_JOBS.set(saldoSyncCurrentJobId, { ...job, status: 'paused' });
    emitToUser(req.user._id, 'bank:erp:saldo:paused', { jobId: saldoSyncCurrentJobId });
  }
  res.json({ ok: true });
}));

router.post('/sync-saldo-transferencia/resume', authenticate, permit('banks:admin'), asyncHandler(async (req, res) => {
  if (!saldoSyncRunning || !saldoSyncControl.paused) {
    return res.status(409).json({ error: 'No hay sincronización en pausa para reanudar.' });
  }
  saldoSyncControl.paused = false;
  saldoSyncControl.pauseResolve?.();
  saldoSyncControl.pauseResolve = null;
  if (saldoSyncCurrentJobId) {
    const job = SALDO_SYNC_JOBS.get(saldoSyncCurrentJobId);
    if (job) SALDO_SYNC_JOBS.set(saldoSyncCurrentJobId, { ...job, status: 'running' });
    emitToUser(req.user._id, 'bank:erp:saldo:resumed', { jobId: saldoSyncCurrentJobId });
  }
  res.json({ ok: true });
}));

router.post('/sync-saldo-transferencia/stop', authenticate, permit('banks:admin'), asyncHandler(async (req, res) => {
  if (!saldoSyncRunning) {
    return res.status(409).json({ error: 'No hay sincronización en curso para detener.' });
  }
  saldoSyncControl.stopped = true;
  // Si estaba en pausa, destrabar el await para que el job detecte stopped
  if (saldoSyncControl.paused) {
    saldoSyncControl.paused = false;
    saldoSyncControl.pauseResolve?.();
    saldoSyncControl.pauseResolve = null;
  }
  res.json({ ok: true });
}));

module.exports = router;
