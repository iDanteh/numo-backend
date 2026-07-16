'use strict';

const express = require('express');
const multer  = require('multer');
const axios   = require('axios');
const ExcelJS = require('exceljs');
const { authenticate, permit }           = require('../../shared/middleware/auth.real');
const { asyncHandler }                   = require('../../shared/middleware/error-handler');
const { sincronizarCuentasPendientes }   = require('./erp-sync.service');
const { procesarRefacturacionesCyc }     = require('./refacturaciones-cyc.service');
const { procesarMostradorCyc,
        generarExcelMostradorCyc }       = require('./mostrador-cyc.service');
const { procesarPagosCyc,
        generarExcelPagosCyc }           = require('./pagos-cyc.service');
const ErpFacturaPago                     = require('./ErpFacturaPago.model');
const BankMovement                       = require('../banks/BankMovement.model');
const { emitToUser }                     = require('../../shared/socket');
const { ERP_TOLERANCE }                  = require('../banks/bank.service');
const {
  KoreCajaError, koreTokenCache, KORE_CAJA_BASE_URL,
  obtenerSesionCaja, obtenerCuentasKore, aplicarCobroOperacion, aplicarCobroOperacionMultiple,
}                                         = require('./kore-caja.service');

const uploadCyc = multer({
  storage: multer.memoryStorage(),
  limits:  { fileSize: 20 * 1024 * 1024 },
  fileFilter: (_req, file, cb) => {
    const ok = /\.(xlsx|xls)$/i.test(file.originalname);
    cb(ok ? null : new Error('Solo se aceptan archivos Excel (.xlsx, .xls)'), ok);
  },
});

const router = express.Router();

// KORE_FORMASPAGO_BASE_URL es exclusivo de este archivo (catálogo de bancos y
// formas de pago) — las demás constantes/funciones de Kore-caja viven en
// kore-caja.service.js (importado arriba) y se comparten desde ahí.
const KORE_FORMASPAGO_BASE_URL = (process.env.KORE_FORMASPAGO_BASE_URL || 'https://test.formaspagos.koreingenieria.com');

const ERP_PAGE_SIZE = 50;

// Exclusivos de GET /reporte (facturas para reporte de pagos-banco).
const ERP_FACT_BASE_URL = (process.env.ERP_FACT_BASE_URL || '').replace(/\/$/, '');
const ERP_TOKEN         = process.env.ERP_TOKEN || '';

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

// GET /api/erp/facturas/reporte
// Parámetros: fechaDesde, fechaHasta, tipo_comprobante (opcional)
router.get('/reporte', authenticate, asyncHandler(async (req, res) => {
  if (!ERP_FACT_BASE_URL) {
    return res.status(503).json({ error: 'ERP no configurado (ERP_FACT_BASE_URL ausente)' });
  }

  const { fechaInicio, fechaFin, tipo_comprobante } = req.query;

  // El ERP externo usa snake_case: fecha_inicio / fecha_fin
  const params = { fecha_inicio: fechaInicio, fecha_fin: fechaFin };
  if (tipo_comprobante) params.tipo_comprobante = tipo_comprobante;

  const response = await axios.get(`${ERP_FACT_BASE_URL}/api/facturas/reporte`, {
    params,
    headers: { Authorization: `Bearer ${ERP_TOKEN}` },
    timeout: 15000,
  });

  // El ERP devuelve PascalCase; el array puede estar en Data[] o Data.facturas[]
  const dataPayload = response.data?.Data ?? response.data ?? [];
  const raw = Array.isArray(dataPayload)
    ? dataPayload
    : (dataPayload.facturas ?? dataPayload.Facturas ?? []);
  const now = new Date();

  // Upsert idempotente: cada factura se identifica por su ID del ERP (PascalCase)
  if (raw.length > 0) {
    await Promise.all(raw.map(f => ErpFacturaPago.updateOne(
      { erpId: f.ID },
      {
        $set: {
          erpId:            f.ID,
          uuid:             f.UUID             ?? null,
          tipoComprobante:  f.TipoComprobante  ?? null,
          serie:            f.Serie            ?? null,
          folio:            f.Folio            ?? null,
          subtotal:         f.Subtotal         ?? null,
          descuento:        f.Descuento        ?? null,
          totalIva:         f.TotalIVA         ?? null,
          totalRetenciones: f.TotalRetenciones ?? null,
          importe:          f.Importe          ?? null,
          metodoPago:       f.MetodoPago       ?? null,
          fechaPago:        f.FechaPago        ?? null,
          fechaTimbrado:    f.FechaTimbrado    ?? null,
          estatus:          f.Estatus          ?? null,
          relaciones: (f.Relaciones ?? []).map(r => ({
            tipoRelacion: r.TipoRelacion ?? null,
            uuid:         r.UUID         ?? null,
          })),
          lastSeenAt: now,
        },
      },
      { upsert: true }
    )));
  }

  const facturas = raw.map(f => ({
    id:               f.ID,
    uuid:             f.UUID             ?? null,
    tipoComprobante:  f.TipoComprobante  ?? null,
    serie:            f.Serie            ?? null,
    folio:            f.Folio            ?? null,
    subtotal:         f.Subtotal         ?? null,
    descuento:        f.Descuento        ?? null,
    totalIva:         f.TotalIVA         ?? null,
    totalRetenciones: f.TotalRetenciones ?? null,
    importe:          f.Importe          ?? null,
    metodoPago:       f.MetodoPago       ?? null,
    fechaPago:        f.FechaPago        ?? null,
    fechaTimbrado:    f.FechaTimbrado    ?? null,
    estatus:          f.Estatus          ?? null,
  }));

  res.json(facturas);
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
// Obtiene el ID de sesión de caja activa del usuario logueado actual, en dos
// pasos (ver obtenerSesionCaja): 1) intercambia el sub de Auth0 por un token
// Kore, 2) consulta la sesión activa de caja con ese token.
router.get('/cobros/sesion-caja', authenticate, asyncHandler(async (req, res) => {
  try {
    const { sesionId, koreToken } = await obtenerSesionCaja(req.user._id);
    console.log(`[cobros] sesion-caja OK para usuario ${req.user._id}, sesionId: ${sesionId}`);
    return res.json({ sesionId, koreToken });
  } catch (err) {
    if (err instanceof KoreCajaError) return res.status(err.statusCode).json({ error: err.message });
    throw err;
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
router.get('/cobros/cuentas', authenticate, asyncHandler(async (req, res) => {
  const koreToken = getKoreToken(req, res);
  if (!koreToken) return;

  const { ids } = req.query;
  if (!ids) {
    return res.status(400).json({ error: 'Se requiere el parámetro ids.' });
  }

  try {
    const cuentas = await obtenerCuentasKore(koreToken, ids);
    console.log(`[cobros/cuentas] ids=${ids} → ${cuentas.length} cuentas, ${cuentas.filter(c => c.descuentos.length > 0).length} con descuento`);
    res.json(cuentas);
  } catch (err) {
    if (err instanceof KoreCajaError) {
      console.warn(`[cobros/cuentas] Kore rechazó con ${err.statusCode}:`, err.koreBody);
      return res.status(err.statusCode).json({ error: err.message });
    }
    throw err;
  }
}));

// POST /api/erp/cobros/operacion/:sesionId — aplica cobro a una CxC en el sistema de caja
// (proxy directo desde el panel de cobros — usa aplicarCobroOperacion, misma
// función que usa collection-request.service.js para el flujo automático).
router.post('/cobros/operacion/:sesionId', authenticate, asyncHandler(async (req, res) => {
  const koreToken = getKoreToken(req, res);
  if (!koreToken) return;

  const { sesionId } = req.params;
  const body = req.body;

  try {
    const data = await aplicarCobroOperacion(sesionId, koreToken, body);
    res.json(data);
  } catch (err) {
    if (err instanceof KoreCajaError) {
      return res.status(err.statusCode).json({ error: err.message, kore: err.koreBody });
    }
    throw err;
  }
}));

// POST /api/erp/cobros/operacion-multiple/:sesionId — aplica cobro a múltiples CxC en una sola operación
router.post('/cobros/operacion-multiple/:sesionId', authenticate, asyncHandler(async (req, res) => {
  const koreToken = getKoreToken(req, res);
  if (!koreToken) return;

  const { sesionId } = req.params;
  const body = req.body;

  try {
    const data = await aplicarCobroOperacionMultiple(sesionId, koreToken, body);
    res.json(data);
  } catch (err) {
    if (err instanceof KoreCajaError) {
      return res.status(err.statusCode).json({ error: err.message, kore: err.koreBody });
    }
    throw err;
  }
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

// ── POST /api/erp/sync-erp-kore ────────────────────────────────────────────────
// Job único de conciliación ERP-Kore. Reemplaza los antiguos "Sync Saldo ERP" y "Sync
// Histórico Kore" (fusionados el 2026-07-09 para dejar de consultar Kore dos veces por la
// misma CxC). Por cada erpLink con serie+folioExterno aún sin finalizar
// (conciliacionFinalizadaAt === null):
//   1. Siempre refresca el snapshot erpLinks[].movimientosKore con la respuesta actual de
//      Kore, sin importar si la CxC ya se saldó — permite rastrear/auditar una CxC en curso.
//   2. Si Kore confirma saldoActual <= 0 (CxC saldada, o con saldo A FAVOR — un saldo
//      negativo puede tardar mucho o nunca volver exactamente a 0, p.ej. por una retención
//      posterior al pago): calcula (solo si el vínculo es de un humano) el monto real
//      aportado a saldoErp — ver _montoSaldoLink — y marca conciliacionFinalizadaAt (con o
//      sin aporte; un vínculo de motor automático también se cierra, simplemente nunca
//      aporta). Ya no hay nada más que Kore pueda reportar para esa CxC, así que el link
//      deja de reconsultarse en corridas futuras.
//   3. Si saldoActual > 0 (aún pendiente de pago): el link se deja abierto
//      (conciliacionFinalizadaAt sigue null) — la siguiente corrida (manual, o el cron
//      diario, que no acota por fecha) lo vuelve a intentar sin límite de antigüedad, hasta
//      que la CxC se salde.
// saldoErp/status del movimiento se recalculan SOLO si al menos un link (de esta corrida o
// de una anterior) ya está finalizado; si ninguno lo está, se deja el valor existente
// intacto (puede provenir de otra fuente — ver aplicarLogicaErp en bank.service.js).
// Corre en background; emite progreso via Socket.IO al admin que lo disparó.

let syncRunning      = false;
let syncCurrentJobId = null;   // jobId del job activo (null si inactivo)
const syncControl    = { paused: false, stopped: false, pauseResolve: null };
const SYNC_JOBS      = new Map(); // jobId → { status, auth0Sub, result?, error?, detalles? }
const SYNC_JOB_TTL   = 2 * 60 * 60 * 1000; // el resultado (y el detalle para el reporte) vive en memoria este tiempo tras terminar
const SYNC_DELAY_MS    = 1000;       // pausa entre requests a Kore (subida de 500ms — Kore devolvía 429 con la anterior)
const SYNC_BACKOFF     = 25_000;     // fallback si Kore no indica cuánto esperar
const SYNC_MAX_RETRIES = 4;          // intentos por link antes de contar como error

const _sleep = ms => new Promise(r => setTimeout(r, ms));

// True si algún erpLink con este erpId fue identificado por una persona (no por un motor
// automático — MOTOR_USER_IDS, definida arriba: 'erp-auto' y 'aut-match').
function _erpIdIdentificadoPorHumano(identificadoPor, erpId) {
  return (identificadoPor ?? []).some(
    e => e.erpId === erpId && e.userId && !MOTOR_USER_IDS.includes(e.userId),
  );
}

// Nombres del catálogo de formas de pago de Kore que representan un DEPÓSITO BANCARIO real
// para efectos de saldoErp. Deliberadamente DISTINTO de _esFormaBancaria() (cobro-panel /
// collection-requests), que excluye cheque porque ese concepto solo alimenta el badge
// "CxC vinculadas" — aquí SÍ se incluye: un cheque cobrado también es dinero real que entró
// vía el banco. Coincidencia flexible (sin acentos, insensible a mayúsculas) porque no hay
// certeza de que "depósito en efectivo" sea siempre el nombre exacto en el catálogo de Kore.
function _esFormaPagoBancariaKore(nombreFormaPago) {
  const norm = String(nombreFormaPago ?? '')
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .trim().toUpperCase();
  return norm === 'TRANSFERENCIA'
      || norm === 'CHEQUE'
      || /DEPOSITO.*EFECTIVO/.test(norm);
}

// Monto realmente cobrado por el banco para una cuenta ya saldada (saldoActual 0) o con
// saldo A FAVOR (saldoActual negativo — p.ej. una retención aplicada después del pago
// completo, que puede tardar mucho o nunca en volver exactamente a 0): suma el 'total'
// (nunca formasPago[].monto, que puede ser un depósito compartido entre varias CxC, o —
// como en el caso de una aplicación de saldo a favor/anticipo— ni siquiera corresponder al
// total del movimiento que lo contiene) de cada movimiento cuya forma de pago sea bancaria
// real. Excluye a propósito aplicaciones de saldo a favor, efectivo de caja, tarjeta, etc.
// — esos movimientos también traen `formasPago` no vacío pero no son un depósito bancario.
// Movimientos de retención (serie RET) no traen `formasPago` en absoluto, así que quedan
// excluidos naturalmente — el monto recuperado es el que SALDÓ la CxC, no el saldo negativo
// resultante de un ajuste posterior.
function _montoSaldoLink(raw0) {
  if (!raw0 || raw0.saldoActual > 0) return 0;
  const conPagoBancario = (raw0.movimientos ?? []).filter(
    m => Array.isArray(m.formasPago)
      && m.formasPago.some(fp => _esFormaPagoBancariaKore(fp.nombreFormaPago)),
  );
  return conPagoBancario.reduce((sum, m) => sum + Math.abs(m.total ?? 0), 0);
}

// Deja solo dígitos y quita ceros a la izquierda — Kore antepone "REF " a algunas
// autorizaciones y puede traer ceros a la izquierda distintos a los del banco.
function _normalizarAutorizacion(v) {
  return String(v ?? '').replace(/\D/g, '').replace(/^0+/, '');
}

// Monto aportado por un vínculo hecho por un MOTOR automático (erp-auto/aut-match).
// A diferencia de _montoSaldoLink (usado para vínculos humanos, que suma TODO lo bancario
// de la CxC), aquí NO se puede sumar todo — una CxC puede recibir varios depósitos
// bancarios de movimientos bancarios distintos a lo largo del tiempo, y sumarlos todos
// atribuiría a ESTE movimiento dinero que en realidad entró por otro depósito. En vez de
// eso, se busca el movimiento de Kore cuya forma de pago trae la MISMA autorización
// bancaria (`adicionales` → "Aut") que este depósito (`mov.numeroAutorizacion`) — el mismo
// criterio que ya usa el motor de matching (bank-autorizaciones.service.js) para vincular
// en primer lugar. Regresa null (no 0) si no se encuentra ninguna coincidencia — "no se
// pudo determinar" es distinto de "el aporte es cero", y el llamador debe respetar esa
// diferencia para no pisar un saldoErp ya correcto con un cero falso.
function _montoSaldoLinkPorAutorizacion(raw0, numeroAutorizacion) {
  if (!raw0 || raw0.saldoActual > 0) return null;
  const autNorm = _normalizarAutorizacion(numeroAutorizacion);
  if (!autNorm) return null;
  const movimiento = (raw0.movimientos ?? []).find(m =>
    Array.isArray(m.formasPago) && m.formasPago.some(fp =>
      _esFormaPagoBancariaKore(fp.nombreFormaPago)
      && (fp.adicionales ?? []).some(a =>
        a.nombre === 'Aut' && _normalizarAutorizacion(a.valor) === autNorm,
      ),
    ),
  );
  return movimiento ? Math.abs(movimiento.total ?? 0) : null;
}

// Snapshot mínimo de movimientos Kore para rastreo/conciliación manual — se guarda en
// erpLinks[].movimientosKore. Se descarta el primero (el cargo original que crea la CxC,
// no un abono/ajuste) y solo se conservan los campos indispensables, nunca formasPago.
function _movimientosKoreDesde(raw0) {
  return (raw0?.movimientos ?? []).slice(1).map(m => ({
    serie:         m.serie         ?? null,
    folio:         m.folio         ?? null,
    serieOrigen:   m.serieOrigen   ?? null,
    folioOrigen:   m.folioOrigen   ?? null,
    fecha:         m.fecha ? new Date(m.fecha) : null,
    saldoAnterior: m.saldoAnterior ?? null,
    saldoActual:   m.saldoActual   ?? null,
    subtotal:      m.subtotal      ?? null,
    impuesto:      m.impuesto      ?? null,
    total:         m.total         ?? null,
  }));
}

// Kore folios encode YYMMxxxxx (e.g. "260600250" → year=2026, month=06).
// Kore exige un rango MÁXIMO de un mes — si se excede, responde 400:
// { "Codigo": 400, "Data": "parsing time \"...\": day out of range", "Mensaje": "Parámetros inválidos" }
// Por eso esta función SIEMPRE devuelve el mes calendario exacto (nunca más), sin ningún
// margen extra — ver _rangoSpilloverSiguienteMes más abajo para el caso de fin de mes.
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

// Ventana de "rescate" para el desfase de fin de mes: el reloj del servidor del ERP tiene un
// adelanto de ~6hrs sobre la hora local (Ciudad de México), así que una venta hecha cerca de
// medianoche el ÚLTIMO día del mes (hora local) puede quedar registrada en Kore con fecha ya
// del día 1 del mes SIGUIENTE — ej. venta real 31-03 19:33 hora local → Kore la guarda
// ~01:33 del 01-04. El folio sigue diciendo "03" (mes de negocio), así que `_rangoDesdeFollo`
// (marzo completo) nunca la encuentra.
//
// En vez de agrandar la ventana normal (arriesgando pasarse del límite de un mes que exige
// Kore — ver el 400 de arriba), esta función devuelve una ventana CORTA de un solo día: el
// día 1 del mes siguiente. Se usa como segundo intento SOLO cuando la consulta normal no
// encuentra nada — la gran mayoría de los links se resuelven en el primer intento y nunca
// disparan esta consulta extra.
function _rangoSpilloverSiguienteMes(folioExterno) {
  const str = String(folioExterno).trim();
  if (str.length < 4) return null;
  const yy = parseInt(str.slice(0, 2), 10);
  const mm = parseInt(str.slice(2, 4), 10);
  if (isNaN(yy) || isNaN(mm) || mm < 1 || mm > 12) return null;
  const year = 2000 + yy;
  // mm ya viene 1-indexado, así que equivale al índice 0-based del mes SIGUIENTE en Date.UTC.
  const desde = new Date(Date.UTC(year, mm, 1, 0, 0, 0));
  const hasta = new Date(Date.UTC(year, mm, 1, 23, 59, 59));
  return { fechaDesde: desde.toISOString(), fechaHasta: hasta.toISOString() };
}

// Extrae el tiempo de espera del mensaje de Kore: "retry after: 20.398s" → 20898 ms
function _parseRetryAfterMs(responseData) {
  const match = String(responseData?.Data ?? '').match(/retry after:\s*([\d.]+)s/i);
  return match ? Math.ceil(parseFloat(match[1]) * 1000) + 500 : null;
}

// Reintenta automáticamente en 429/503 respetando el tiempo que indica Kore.
// Lanza en el último intento fallido o ante cualquier error no recuperable.
async function _sincronizarConRetry(params) {
  for (let attempt = 0; attempt < SYNC_MAX_RETRIES; attempt++) {
    try {
      return await sincronizarCuentasPendientes(params);
    } catch (err) {
      const status = err.response?.status;
      const retryable = status === 429 || status === 503;
      if (!retryable || attempt === SYNC_MAX_RETRIES - 1) throw err;
      const wait = _parseRetryAfterMs(err.response?.data) ?? SYNC_BACKOFF;
      await _sleep(wait);
    }
  }
}

// Verifica si el job debe continuar. Si está en pausa, aguarda hasta que llegue el resume.
// Devuelve false si fue detenido (el loop debe hacer break).
async function _checkSyncControl() {
  if (syncControl.stopped) return false;
  if (syncControl.paused) {
    await new Promise(resolve => { syncControl.pauseResolve = resolve; });
  }
  return !syncControl.stopped;
}

// fechaInicio/fechaFin son ajustables por corrida manual (ver POST /sync-erp-kore); si
// vienen null (corrida automática del cron, o manual sin fechas) no se acota por fecha —
// se revisa TODO lo aún no finalizado, sin importar su antigüedad (ver punto 4 del diseño:
// nunca dejar CxC rezagadas fuera del alcance de la corrida automática diaria).
async function _syncErpKoreJob(auth0Sub, jobId, fechaInicio, fechaFin) {
  syncCurrentJobId = jobId;
  try {
    const filter = {
      erpLinks: {
        $elemMatch: {
          serie:                    { $ne: null },
          folioExterno:             { $ne: null },
          conciliacionFinalizadaAt: null,
        },
      },
    };
    if (fechaInicio && fechaFin) filter.fecha = { $gte: fechaInicio, $lte: fechaFin };

    const movements = await BankMovement.find(filter)
      .select('_id folio banco concepto deposito retiro fecha saldoErp status ficha erpLinks identificadoPor')
      .lean();

    let procesados = 0, actualizados = 0, pendientes = 0, errores = 0;
    const total    = movements.length;
    let stopped    = false;
    const detalles = []; // una entrada por movimiento — insumo del reporte Excel

    emitToUser(auth0Sub, 'bank:erp:sync:progress',
      { jobId, procesados, total, actualizados, pendientes, errores, pct: 0 });

    for (const mov of movements) {
      if (!await _checkSyncControl()) { stopped = true; break; }

      const links = mov.erpLinks ?? [];
      let huboErrorMov              = false;
      let huboLinkTocado            = false;
      let huboTipoPagoNuevo         = false;
      let huboFinalizacionEnCorrida = false;
      const linksDetalle    = [];
      const linksActualizados = links.map(l => ({ ...l }));

      for (let i = 0; i < links.length; i++) {
        const link = links[i];
        if (!link.serie || !link.folioExterno || link.conciliacionFinalizadaAt) continue;
        const rango = _rangoDesdeFollo(link.folioExterno);
        if (!rango) continue;
        try {
          let { raw } = await _sincronizarConRetry({
            serieExterna: link.serie,
            folioExterno: String(link.folioExterno),
            fechaDesde:   rango.fechaDesde,
            fechaHasta:   rango.fechaHasta,
          });

          // Ventana normal vacía → puede ser una venta de fin de mes que Kore registró ya
          // en el día 1 del mes siguiente (ver _rangoSpilloverSiguienteMes). Segundo intento
          // acotado a un solo día — no dispara en el caso común (ventana normal con datos).
          if (raw.length === 0) {
            const spillover = _rangoSpilloverSiguienteMes(link.folioExterno);
            if (spillover) {
              await _sleep(SYNC_DELAY_MS);
              const retryRes = await _sincronizarConRetry({
                serieExterna: link.serie,
                folioExterno: String(link.folioExterno),
                fechaDesde:   spillover.fechaDesde,
                fechaHasta:   spillover.fechaHasta,
              });
              if (retryRes.raw.length > 0) raw = retryRes.raw;
            }
          }

          const raw0 = raw[0];
          linksActualizados[i].movimientosKore = _movimientosKoreDesde(raw0);
          huboLinkTocado = true;

          if (raw0?.tipoPago) {
            const tipoPagoNorm = String(raw0.tipoPago).trim().toUpperCase();
            if (tipoPagoNorm !== (link.tipoPago ?? null)) {
              linksActualizados[i].tipoPago = tipoPagoNorm;
              huboTipoPagoNuevo = true;
            }
          }

          // tieneRetencion/montoRetenido se recalculan en CADA corrida (no solo al crear el
          // link) — una retención puede llegar después de que el link ya existía (ej. la CxC
          // se pagó por completo y semanas más tarde Kore aplicó una retención fiscal), y sin
          // este refresh el reporte "Retención" (bank.service.js) quedaría desactualizado para
          // siempre en esos casos. Mismo criterio que usa el motor automático al crear el
          // link (bank-autorizaciones.service.js): movimientos con serie 'RET'. montoRetenido
          // suma su `total` en valor absoluto — evita recorrer movimientosKore para saber
          // cuánto está retenido ahora mismo.
          const movsRetencion = (raw0?.movimientos ?? []).filter(m => m.serie === 'RET');
          const tieneRetencionAhora = movsRetencion.length > 0;
          const montoRetenidoAhora  = tieneRetencionAhora
            ? movsRetencion.reduce((s, m) => s + Math.abs(m.total ?? 0), 0)
            : null;
          if (tieneRetencionAhora !== (link.tieneRetencion ?? false)) {
            linksActualizados[i].tieneRetencion = tieneRetencionAhora;
          }
          if (montoRetenidoAhora !== (link.montoRetenido ?? null)) {
            linksActualizados[i].montoRetenido = montoRetenidoAhora;
          }

          if (raw0?.saldoActual <= 0) {
            // CxC saldada (0) o con saldo A FAVOR (negativo — p.ej. una retención posterior
            // al pago completo, que puede tardar mucho o nunca en volver exactamente a 0:
            // no tiene sentido dejar el link abierto esperando un 0 exacto que quizá nunca
            // llegue). Vínculo humano: suma TODO lo bancario de la CxC (un humano ya
            // confirmó visualmente que este depósito es el correcto). Vínculo de motor
            // automático: NUNCA se suma todo lo bancario (la CxC pudo recibir varios
            // depósitos de movimientos bancarios distintos a lo largo del tiempo) — se
            // busca específicamente el movimiento de Kore cuya autorización bancaria
            // coincide con la de ESTE depósito (mismo criterio que ya usa el motor de
            // matching para vincular). Si no hay coincidencia, el aporte queda `null`
            // ("no determinado", nunca "cero") para no pisar un saldoErp ya correcto.
            const esHumano = _erpIdIdentificadoPorHumano(mov.identificadoPor, link.erpId);
            const aporte   = esHumano
              ? _montoSaldoLink(raw0)
              : _montoSaldoLinkPorAutorizacion(raw0, mov.numeroAutorizacion);
            if (aporte != null) linksActualizados[i].saldoErpAportado = aporte;
            linksActualizados[i].conciliacionFinalizadaAt = new Date();
            linksActualizados[i].conciliacionRunId        = jobId;
            huboFinalizacionEnCorrida = true;
            linksDetalle.push({
              erpId: link.erpId, serie: link.serie, folioExterno: link.folioExterno,
              estado: 'finalizado', montoAportado: aporte,
            });
          } else {
            linksDetalle.push({
              erpId: link.erpId, serie: link.serie, folioExterno: link.folioExterno,
              estado: 'pendiente', saldoActual: raw0?.saldoActual ?? null,
            });
          }
          await _sleep(SYNC_DELAY_MS);
        } catch (err) {
          errores++;
          huboErrorMov = true;
          linksDetalle.push({
            erpId: link.erpId, serie: link.serie, folioExterno: link.folioExterno,
            estado: 'error', error: err.message || 'Error al consultar Kore',
          });
        }
      }

      const update = {};
      if (huboLinkTocado || huboTipoPagoNuevo) update.$set = { erpLinks: linksActualizados };

      // saldoErp/status se recalculan SOLO si al menos un link (de esta corrida o de una
      // anterior) tiene un aporte realmente determinado (saldoErpAportado != null) —
      // deliberadamente NO basta con que esté "finalizado": un vínculo de motor cerrado
      // sin autorización coincidente no aporta nada confiable, y dejarlo disparar el
      // recálculo pisaría con un cero falso un saldoErp que ya era correcto.
      const hayAlgunAporteDeterminado = linksActualizados.some(l => l.saldoErpAportado != null);
      if (hayAlgunAporteDeterminado) {
        const saldoErpNuevo = linksActualizados.reduce((s, l) => s + (l.saldoErpAportado ?? 0), 0);
        const bankAmount    = Math.abs(mov.deposito ?? mov.retiro ?? 0);
        let statusNuevo      = saldoErpNuevo >= bankAmount - ERP_TOLERANCE ? 'identificado' : 'no_identificado';
        if (mov.ficha && statusNuevo === 'no_identificado') statusNuevo = 'identificado';

        if (saldoErpNuevo !== (mov.saldoErp ?? null) || statusNuevo !== mov.status) {
          update.$set = { ...(update.$set ?? {}), saldoErp: saldoErpNuevo, status: statusNuevo };
          update.$push = {
            _changelog: {
              at: new Date(), via: 'erp-sync', campo: 'saldoErp+status',
              de: { saldoErp: mov.saldoErp ?? null, status: mov.status },
              a:  { saldoErp: saldoErpNuevo, status: statusNuevo },
              runId: jobId, revertedAt: null,
            },
          };
        }
      }

      let estado;
      if (huboErrorMov)                                   estado = 'error';
      else if (update.$push || huboFinalizacionEnCorrida)  estado = 'actualizado';
      else                                                  estado = 'pendiente';
      if (estado === 'actualizado') actualizados++;
      if (estado === 'pendiente')   pendientes++;

      if (update.$set || update.$push) {
        await BankMovement.findByIdAndUpdate(mov._id, update);
      }

      detalles.push({
        movementId: mov._id, folio: mov.folio, banco: mov.banco, concepto: mov.concepto,
        fecha: mov.fecha, deposito: mov.deposito,
        saldoErpAntes:   mov.saldoErp ?? null,
        saldoErpDespues: update.$set?.saldoErp ?? mov.saldoErp ?? null,
        estado, links: linksDetalle,
      });

      procesados++;
      emitToUser(auth0Sub, 'bank:erp:sync:progress', {
        jobId, procesados, total, actualizados, pendientes, errores,
        pct: Math.round((procesados / total) * 100),
      });
    }

    if (stopped) {
      const result = { procesados, total, actualizados, pendientes, errores };
      SYNC_JOBS.set(jobId, { status: 'stopped', auth0Sub, result, detalles });
      emitToUser(auth0Sub, 'bank:erp:sync:stopped', { jobId, ...result });
    } else {
      const result = { total, actualizados, pendientes, errores };
      SYNC_JOBS.set(jobId, { status: 'done', auth0Sub, result, detalles });
      emitToUser(auth0Sub, 'bank:erp:sync:done', { jobId, ...result });
    }
  } catch (err) {
    const error = err.message || 'Error en sincronización ERP-Kore';
    SYNC_JOBS.set(jobId, { status: 'error', auth0Sub, error });
    emitToUser(auth0Sub, 'bank:erp:sync:error', { jobId, error });
  } finally {
    syncRunning      = false;
    syncCurrentJobId = null;
    setTimeout(() => SYNC_JOBS.delete(jobId), SYNC_JOB_TTL);
  }
}

router.post('/sync-erp-kore', authenticate, permit('banks:admin'), asyncHandler(async (req, res) => {
  if (syncRunning) {
    return res.status(409).json({ error: 'Ya hay una sincronización ERP-Kore en curso.' });
  }

  // Rango de fechas OPCIONAL — sin acotar por defecto (procesa todo lo aún no finalizado,
  // igual que la corrida automática). El admin puede escribir un rango para acotar una
  // corrida puntual (ej. reprocesar solo un mes específico).
  let fechaInicio = null;
  let fechaFin    = null;
  if (req.body.fechaDesde) {
    fechaInicio = new Date(req.body.fechaDesde);
    if (isNaN(fechaInicio.getTime())) return res.status(400).json({ error: 'fechaDesde inválida' });
  }
  if (req.body.fechaHasta) {
    fechaFin = new Date(req.body.fechaHasta);
    if (isNaN(fechaFin.getTime())) return res.status(400).json({ error: 'fechaHasta inválida' });
  }
  if (fechaInicio && fechaFin && fechaInicio > fechaFin) {
    return res.status(400).json({ error: 'fechaDesde debe ser anterior o igual a fechaHasta' });
  }

  // Resetear control antes de cada job nuevo
  syncControl.paused       = false;
  syncControl.stopped      = false;
  syncControl.pauseResolve = null;

  syncRunning = true;
  const jobId    = `erp-sync-${Date.now()}`;
  const auth0Sub = req.user._id;

  SYNC_JOBS.set(jobId, { status: 'running', auth0Sub });
  res.status(202).json({ jobId });

  _syncErpKoreJob(auth0Sub, jobId, fechaInicio, fechaFin); // sin await — corre en background
}));

router.post('/sync-erp-kore/pause', authenticate, permit('banks:admin'), asyncHandler(async (req, res) => {
  if (!syncRunning || syncControl.paused) {
    return res.status(409).json({ error: 'No hay sincronización activa para pausar.' });
  }
  syncControl.paused = true;
  if (syncCurrentJobId) {
    const job = SYNC_JOBS.get(syncCurrentJobId);
    if (job) SYNC_JOBS.set(syncCurrentJobId, { ...job, status: 'paused' });
    emitToUser(req.user._id, 'bank:erp:sync:paused', { jobId: syncCurrentJobId });
  }
  res.json({ ok: true });
}));

router.post('/sync-erp-kore/resume', authenticate, permit('banks:admin'), asyncHandler(async (req, res) => {
  if (!syncRunning || !syncControl.paused) {
    return res.status(409).json({ error: 'No hay sincronización en pausa para reanudar.' });
  }
  syncControl.paused = false;
  syncControl.pauseResolve?.();
  syncControl.pauseResolve = null;
  if (syncCurrentJobId) {
    const job = SYNC_JOBS.get(syncCurrentJobId);
    if (job) SYNC_JOBS.set(syncCurrentJobId, { ...job, status: 'running' });
    emitToUser(req.user._id, 'bank:erp:sync:resumed', { jobId: syncCurrentJobId });
  }
  res.json({ ok: true });
}));

router.post('/sync-erp-kore/stop', authenticate, permit('banks:admin'), asyncHandler(async (req, res) => {
  if (!syncRunning) {
    return res.status(409).json({ error: 'No hay sincronización en curso para detener.' });
  }
  syncControl.stopped = true;
  // Si estaba en pausa, destrabar el await para que el job detecte stopped
  if (syncControl.paused) {
    syncControl.paused = false;
    syncControl.pauseResolve?.();
    syncControl.pauseResolve = null;
  }
  res.json({ ok: true });
}));

// GET polling de estado — permite recuperar el job tras un reload de página (fallback del socket).
// Sin chequeo de dueño: el job es global (una sola corrida a la vez para todo el sistema,
// igual que pause/resume/stop), así que cualquier admin puede consultarlo.
router.get('/sync-erp-kore/:jobId/status', authenticate, permit('banks:admin'), asyncHandler(async (req, res) => {
  const job = SYNC_JOBS.get(req.params.jobId);
  if (!job) return res.status(404).json({ error: 'Job no encontrado o expirado' });
  const { auth0Sub: _auth0Sub, detalles: _detalles, ...jobResponse } = job;
  res.json(jobResponse);
}));

// GET historial de corridas recientes (mientras sigan vivas en memoria, ver SYNC_JOB_TTL).
// Permite recuperar el reporte/revertir una corrida anterior, no solo la última.
router.get('/sync-erp-kore/jobs', authenticate, permit('banks:admin'), asyncHandler(async (req, res) => {
  const jobs = [...SYNC_JOBS.entries()]
    .map(([jobId, job]) => ({
      jobId,
      status:    job.status,
      result:    job.result ?? null,
      error:     job.error  ?? null,
      hasReport: Array.isArray(job.detalles) && job.detalles.length > 0,
    }))
    .sort((a, b) => (a.jobId < b.jobId ? 1 : -1)); // más reciente primero (jobId = erp-sync-<timestamp>)
  res.json(jobs);
}));

// ── Reporte Excel del job Sync ERP-Kore ───────────────────────────────────────
// 3 hojas: Actualizados (antes/después + diferencia vs. depósito real) · Pendientes
// (CxC aún no saldada en Kore) · Errores.
function _generarExcelSyncErpKore(detalles) {
  const wb = new ExcelJS.Workbook();
  wb.creator = 'Numo — Sync ERP-Kore';
  wb.created = new Date();

  const HEADER_FILL = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF6D28D9' } };
  const HEADER_FONT = { bold: true, color: { argb: 'FFFFFFFF' }, size: 10 };
  const OK_FILL     = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFD1FAE5' } };
  const WARN_FILL   = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFEF9C3' } };
  const ERR_FILL    = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFEE2E2' } };

  function formatFecha(d) {
    if (!d) return '';
    const dt = d instanceof Date ? d : new Date(d);
    if (isNaN(dt.getTime())) return '';
    return dt.toLocaleDateString('es-MX', { day: '2-digit', month: '2-digit', year: 'numeric' });
  }

  function styleHeader(ws) {
    ws.getRow(1).eachCell(cell => {
      cell.fill = HEADER_FILL;
      cell.font = HEADER_FONT;
      cell.alignment = { vertical: 'middle', horizontal: 'center' };
    });
    ws.getRow(1).height = 20;
  }

  const foliosCxc = d => d.links.map(l => `${l.serie ?? ''}${l.folioExterno ?? ''}`).join(', ');

  // ── Hoja 1: Actualizados ──────────────────────────────────────────────────
  const wsAct = wb.addWorksheet('Actualizados');
  wsAct.columns = [
    { header: 'Movimiento (folio)',            key: 'folio',    width: 14 },
    { header: 'Banco',                         key: 'banco',    width: 14 },
    { header: 'Fecha',                         key: 'fecha',    width: 12 },
    { header: 'Depósito',                      key: 'deposito', width: 14 },
    { header: 'Saldo ERP antes',                key: 'antes',    width: 16 },
    { header: 'Saldo ERP después',              key: 'despues',  width: 16 },
    { header: 'Diferencia',                     key: 'diff',     width: 14 },
    { header: 'Diferencia vs. depósito real',   key: 'diffDep',  width: 22 },
    { header: 'CxC vinculadas',                 key: 'cxc',      width: 30 },
    { header: 'Movimiento ID',                  key: 'id',       width: 28 },
  ];
  styleHeader(wsAct);
  for (const d of detalles.filter(d => d.estado === 'actualizado')) {
    const antes  = d.saldoErpAntes ?? 0;
    const despues = d.saldoErpDespues ?? 0;
    const row = wsAct.addRow({
      folio: d.folio ?? '', banco: d.banco ?? '', fecha: formatFecha(d.fecha),
      deposito: d.deposito, antes: d.saldoErpAntes, despues: d.saldoErpDespues,
      diff: despues - antes, diffDep: despues - Math.abs(d.deposito ?? 0),
      cxc: foliosCxc(d), id: String(d.movementId),
    });
    row.eachCell(cell => { cell.fill = OK_FILL; });
  }
  ['deposito', 'antes', 'despues', 'diff', 'diffDep'].forEach(k => { wsAct.getColumn(k).numFmt = '#,##0.00'; });
  if (wsAct.lastColumn) wsAct.autoFilter = { from: 'A1', to: wsAct.lastColumn.letter + '1' };

  // ── Hoja 2: Pendientes ────────────────────────────────────────────────────
  const wsPen = wb.addWorksheet('Pendientes');
  wsPen.columns = [
    { header: 'Movimiento (folio)', key: 'folio',    width: 14 },
    { header: 'Banco',              key: 'banco',    width: 14 },
    { header: 'Fecha',              key: 'fecha',    width: 12 },
    { header: 'Depósito',           key: 'deposito', width: 14 },
    { header: 'Saldo ERP actual',   key: 'saldo',    width: 16 },
    { header: 'Motivo',             key: 'motivo',   width: 26 },
    { header: 'CxC pendientes',     key: 'cxc',      width: 30 },
    { header: 'Movimiento ID',      key: 'id',       width: 28 },
  ];
  styleHeader(wsPen);
  for (const d of detalles.filter(d => d.estado === 'pendiente')) {
    const pendientesLinks = d.links.filter(l => l.estado === 'pendiente');
    const row = wsPen.addRow({
      folio: d.folio ?? '', banco: d.banco ?? '', fecha: formatFecha(d.fecha),
      deposito: d.deposito, saldo: d.saldoErpAntes,
      motivo: 'CxC aún no saldada en Kore',
      cxc: pendientesLinks.map(l => `${l.serie ?? ''}${l.folioExterno ?? ''}`).join(', '),
      id: String(d.movementId),
    });
    row.eachCell(cell => { cell.fill = WARN_FILL; });
  }
  ['deposito', 'saldo'].forEach(k => { wsPen.getColumn(k).numFmt = '#,##0.00'; });

  // ── Hoja 3: Errores ───────────────────────────────────────────────────────
  const wsErr = wb.addWorksheet('Errores');
  wsErr.columns = [
    { header: 'Movimiento (folio)', key: 'folio',   width: 14 },
    { header: 'Banco',              key: 'banco',   width: 14 },
    { header: 'Fecha',              key: 'fecha',   width: 12 },
    { header: 'CxC con error',      key: 'cxc',     width: 30 },
    { header: 'Detalle del error',  key: 'detalle', width: 50 },
    { header: 'Movimiento ID',      key: 'id',      width: 28 },
  ];
  styleHeader(wsErr);
  for (const d of detalles.filter(d => d.estado === 'error')) {
    const conError = d.links.filter(l => l.error);
    const row = wsErr.addRow({
      folio: d.folio ?? '', banco: d.banco ?? '', fecha: formatFecha(d.fecha),
      cxc:     conError.map(l => `${l.serie ?? ''}${l.folioExterno ?? ''}`).join(', '),
      detalle: conError.map(l => l.error).join(' | '),
      id:      String(d.movementId),
    });
    row.eachCell(cell => { cell.fill = ERR_FILL; });
  }

  return wb.xlsx.writeBuffer();
}

// GET reporte de una corrida — disponible solo mientras el job siga en memoria (SYNC_JOB_TTL).
router.get('/sync-erp-kore/:jobId/report', authenticate, permit('banks:admin'), asyncHandler(async (req, res) => {
  const job = SYNC_JOBS.get(req.params.jobId);
  if (!job || !job.detalles) {
    return res.status(404).json({ error: 'El reporte ya no está disponible (expiró o el jobId no existe).' });
  }

  const buffer = await _generarExcelSyncErpKore(job.detalles);
  const fecha  = new Date().toISOString().slice(0, 10);
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', `attachment; filename="sync-erp-kore-${fecha}.xlsx"`);
  res.send(buffer);
}));

// POST revierte una corrida (runId = jobId): restaura saldoErp/status al valor 'de' guardado
// en _changelog (sin pisar una corrida MÁS RECIENTE — mismo criterio que antes) Y limpia el
// checkpoint por CxC (conciliacionFinalizadaAt/saldoErpAportado/conciliacionRunId) de los
// links finalizados por esta corrida exacta, para que la próxima corrida los vuelva a tomar.
// A diferencia del changelog (que si puede quedar "protegido" por una corrida posterior), el
// checkpoint por link SIEMPRE se limpia si coincide el runId — una CxC finalizada no puede
// haber sido "re-finalizada" por otra corrida más nueva mientras siga marcada, así que no
// necesita la misma guardia de "más reciente".
router.post('/sync-erp-kore/:jobId/revert', authenticate, permit('banks:admin'), asyncHandler(async (req, res) => {
  const runId = req.params.jobId;

  const result = await BankMovement.updateMany(
    {
      $or: [
        { _changelog: { $elemMatch: { via: 'erp-sync', runId, revertedAt: null } } },
        { 'erpLinks.conciliacionRunId': runId },
      ],
    },
    [
      {
        $set: {
          _entradaRevert: {
            $first: {
              $filter: {
                input: '$_changelog', as: 'c',
                cond: {
                  $and: [
                    { $eq: ['$$c.via', 'erp-sync'] },
                    { $eq: ['$$c.runId', runId] },
                    { $eq: ['$$c.revertedAt', null] },
                  ],
                },
              },
            },
          },
        },
      },
      {
        $set: {
          _esLaMasReciente: {
            $eq: [
              {
                $size: {
                  $filter: {
                    input: '$_changelog', as: 'c',
                    cond: {
                      $and: [
                        { $eq: ['$$c.via', 'erp-sync'] },
                        { $gt: ['$$c.at', '$_entradaRevert.at'] },
                      ],
                    },
                  },
                },
              },
              0,
            ],
          },
        },
      },
      {
        $set: {
          saldoErp: {
            $cond: [
              { $and: ['$_entradaRevert', '$_esLaMasReciente'] },
              '$_entradaRevert.de.saldoErp', '$saldoErp',
            ],
          },
          status: {
            $cond: [
              { $and: ['$_entradaRevert', '$_esLaMasReciente'] },
              '$_entradaRevert.de.status', '$status',
            ],
          },
          _changelog: {
            $map: {
              input: '$_changelog', as: 'c',
              in: {
                $cond: [
                  {
                    $and: [
                      '$_esLaMasReciente',
                      { $eq: ['$$c.via', 'erp-sync'] },
                      { $eq: ['$$c.runId', runId] },
                      { $eq: ['$$c.revertedAt', null] },
                    ],
                  },
                  { $mergeObjects: ['$$c', { revertedAt: '$$NOW' }] },
                  '$$c',
                ],
              },
            },
          },
        },
      },
      {
        $set: {
          erpLinks: {
            $map: {
              input: '$erpLinks', as: 'l',
              in: {
                $cond: [
                  { $eq: ['$$l.conciliacionRunId', runId] },
                  {
                    $mergeObjects: ['$$l', {
                      saldoErpAportado: null, conciliacionFinalizadaAt: null, conciliacionRunId: null,
                    }],
                  },
                  '$$l',
                ],
              },
            },
          },
        },
      },
      { $unset: ['_entradaRevert', '_esLaMasReciente'] },
    ],
  );

  const omitidos = result.matchedCount - result.modifiedCount;
  res.json({
    ok:                            true,
    matched:                       result.matchedCount,
    revertidos:                    result.modifiedCount,
    omitidosPorCorridaMasReciente: omitidos > 0 ? omitidos : 0,
  });
}));

// ── Corrida automática diaria (cron) ──────────────────────────────────────────
// Se dispara vía node-cron desde numo-backend/src/banks/jobs/erpSyncCron.js (mismo
// patrón que los jobs de src/visor/jobs/satSyncJob.js), no desde HTTP — por eso corre
// sin usuario real (auth0Sub = null; emitToUser lo ignora en silencio si no hay auth0Sub).
// A propósito SIN rango de fecha (fechaInicio/fechaFin = null): revisa TODO lo aún no
// finalizado sin importar su antigüedad, para que ninguna CxC rezagada quede fuera del
// alcance de la corrida automática solo por ser más vieja que una ventana fija. Es seguro
// y barato de repetir cada día — un link ya finalizado nunca se vuelve a consultar (ver
// checkpoint conciliacionFinalizadaAt), así que el costo real solo lo aporta lo que
// sigue genuinamente pendiente. Queda registrada en SYNC_JOBS igual que una corrida
// manual — visible en "Historial Sync ERP-Kore" del panel Admin.
async function runErpSyncAutomatico() {
  if (syncRunning) {
    console.warn('[CronErpSync] Ya hay una sincronización ERP-Kore en curso — se omite la corrida automática de hoy.');
    return;
  }

  const auth0Sub = null;
  console.log('[CronErpSync] Iniciando Sync ERP-Kore automático (sin límite de fecha)...');
  syncControl.paused       = false;
  syncControl.stopped      = false;
  syncControl.pauseResolve = null;
  syncRunning = true;
  const jobId = `erp-sync-${Date.now()}`;
  SYNC_JOBS.set(jobId, { status: 'running', auth0Sub });
  await _syncErpKoreJob(auth0Sub, jobId, null, null);

  console.log('[CronErpSync] Sync ERP-Kore automático completado.');
}

router.runErpSyncAutomatico = runErpSyncAutomatico;

// obtenerSesionCaja/aplicarCobroOperacion(Multiple)/obtenerCuentasKore/KoreCajaError
// ya NO se re-exportan aquí — collection-request.service.js las importa
// directamente de ./kore-caja.service (ver kore-caja.service.js).

module.exports = router;
