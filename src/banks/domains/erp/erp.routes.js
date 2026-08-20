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
const { procesarFormasPagoCxc,
        generarExcelFormasPagoCxc }      = require('./formas-pago-cxc.service');
const ErpFacturaPago                     = require('./ErpFacturaPago.model');
const BankMovement                       = require('../banks/BankMovement.model');
const { resolvePrimeraIdentificacion }   = require('../banks/identificacion-timestamp.util');
const CFDI                               = require('../../../visor/models/CFDI');
const { emitToUser }                     = require('../../shared/socket');
const { ERP_TOLERANCE, updateErpIds }     = require('../banks/bank.service');
const { PERMISSIONS }                    = require('../../../shared/config/rbac');
const rbacStore                           = require('../../../shared/services/rbac-store');
const {
  KoreCajaError, koreTokenCache, KORE_CAJA_BASE_URL,
  obtenerSesionCaja, obtenerCuentasKore, aplicarCobroOperacion, aplicarCobroOperacionMultiple,
  listarBancos, listarFormasPago,
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

const ERP_PAGE_SIZE = 50;

// Exclusivos de GET /reporte (facturas para reporte de pagos-banco).
const ERP_FACT_BASE_URL = (process.env.ERP_FACT_BASE_URL || '').replace(/\/$/, '');
const ERP_TOKEN         = process.env.ERP_TOKEN || '';

// GET /api/erp/cuentas-pendientes
// Parámetros: fechaDesde, fechaHasta, estadoCobro (opcional; 'pendiente' para solo pendientes), page
// La paginación se aplica localmente sobre la respuesta completa del ERP.
router.get('/cuentas-pendientes', authenticate, permit(PERMISSIONS.BANKS_ERP_READ), asyncHandler(async (req, res) => {
  const { fechaDesde, fechaHasta, estadoCobro, page, serieExterna, folioExterno, nombrePersona, origen } = req.query;
  const pageNum = Math.max(1, parseInt(page ?? '1', 10));

  // origen=anticipo depende de un query param, así que no se puede exigir con permit()
  // (permit() solo conoce permisos fijos por ruta) — se verifica aquí adentro, además del
  // banks:erp:read ya exigido arriba para el resto de la consulta.
  // Nota: este permiso solo bloquea la petición explícita origen=anticipo (el switch
  // "Solo anticipos"); no oculta registros de anticipo del listado normal paginado —
  // no es un filtro de datos completo.
  if (origen === 'anticipo') {
    const puedeVerAnticipos = await rbacStore.hasPermission(
      req.user.role, PERMISSIONS.BANKS_ERP_ANTICIPOS, req.user.extraPermissions,
    );
    if (!puedeVerAnticipos) {
      return res.status(403).json({
        error:    'Permisos insuficientes para esta acción.',
        required: [PERMISSIONS.BANKS_ERP_ANTICIPOS],
      });
    }
  }

  // sincronizarCuentasPendientes llama al ERP, upserta en el caché y devuelve los
  // datos crudos para que este endpoint pueda construir la respuesta paginada.
  let raw = [];
  try {
    ({ raw } = await sincronizarCuentasPendientes({
      fechaDesde, fechaHasta, estadoCobro, serieExterna, folioExterno, nombrePersona, origen,
    }));
  } catch (err) {
    if (err.message?.includes('ERP no configurado')) {
      return res.status(503).json({ error: err.message });
    }
    throw err;
  }

  let allCuentas = raw.map(c => ({
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
    esAnticipo:           c.esAnticipo           ?? false,
    origen:               c.origen               ?? null,
  }));

  // El ERP a veces mezcla ventas normales dentro de origen=anticipo — se refuerza
  // el filtro con el campo esAnticipo, que sí es confiable (pedido del usuario 2026-08-05).
  // Además solo se muestran los que tienen saldo disponible REAL (> 0) — se excluyen
  // tanto los ya aplicados por completo (saldo 0) como los de saldo negativo (2026-08-05,
  // pedido explícito: un saldo negativo no es un anticipo disponible para usar).
  // Epsilon en vez de > 0 exacto, por el ruido de punto flotante típico en montos.
  if (origen === 'anticipo') {
    allCuentas = allCuentas.filter(c => c.esAnticipo === true && c.saldoActual > 0.01);
  }

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

// GET /api/erp/cuenta-por-serie-folio — resuelve UNA CxC puntual contra Kore por
// serie+folio exactos (segunda parte del buscador de CFDI del modal ERP, 2026-08-07):
// un CFDI (colección `cfdis`, dominio visor) trae `total` de la factura, NUNCA el saldo
// pendiente EN VIVO (puede haber pagos parciales) — para que "vincular" calcule bien
// diferencia/status hay que traer el dato fresco de Kore, no confiar en el CFDI.
// Reutiliza EXACTAMENTE el mismo patrón que /erp-links/:erpId/refrescar/_syncErpKoreJob
// (_rangoDesdeFollo + _sincronizarConRetry + reintento con _rangoSpilloverSiguienteMes si
// la ventana normal viene vacía) — deriva el rango de fecha del folio, sin que el usuario
// tenga que elegir un rango a mano. Mismo permiso que el buscador de CFDI
// (banks:cfdi:read): esto solo tiene sentido como continuación de ese flujo, no es un
// acceso genérico a Kore.
router.get('/cuenta-por-serie-folio', authenticate, permit(PERMISSIONS.BANKS_CFDI_READ), asyncHandler(async (req, res) => {
  const serieExterna = (req.query.serie ?? '').toString().trim();
  const folioExterno = (req.query.folio ?? '').toString().trim();
  if (!serieExterna || !folioExterno) {
    return res.status(400).json({ error: 'Se requiere serie y folio.' });
  }

  const rango = _rangoDesdeFollo(folioExterno);
  if (!rango) return res.status(400).json({ error: 'No se pudo determinar el rango de fecha para este folio.' });

  let raw;
  try {
    ({ raw } = await _sincronizarConRetry({
      serieExterna, folioExterno, fechaDesde: rango.fechaDesde, fechaHasta: rango.fechaHasta,
    }));

    if (raw.length === 0) {
      const spillover = _rangoSpilloverSiguienteMes(folioExterno);
      if (spillover) {
        await _sleep(SYNC_DELAY_MS);
        const retryRes = await _sincronizarConRetry({
          serieExterna, folioExterno, fechaDesde: spillover.fechaDesde, fechaHasta: spillover.fechaHasta,
        });
        if (retryRes.raw.length > 0) raw = retryRes.raw;
      }
    }
  } catch (err) {
    return res.status(502).json({ error: err.message || 'Error al consultar Kore.' });
  }

  // Sin fallback a raw[0]: si Kore devuelve cuentas pero ninguna calza EXACTO con la
  // serie/folio pedida, es preferible un 404 (el usuario puede reintentar) a vincular la
  // cuenta equivocada en silencio — bug real encontrado en revisión, el `?? raw[0]`
  // original podía devolver una CxC de otra serie-folio sin ningún aviso.
  const raw0 = raw.find(c => String(c.folioExterno) === folioExterno && String(c.serieExterna) === serieExterna);
  if (!raw0) {
    // Kore excluye por completo del endpoint /cuentas-pendientes cualquier factura YA
    // LIQUIDADA (pagada al 100%), sin importar el rango de fecha — no es que no exista,
    // es que Kore ya no la considera "pendiente". Caso real: CFDI H0-260100639, pago por
    // 488.73 == total 488.73. Pedido explícito del usuario (2026-08-10): permitir vincular
    // igual esa CxC — "es solo una relación simple", no hace falta verificarla en vivo
    // contra Kore, porque el propio CFDI ya prueba que no le queda saldo pendiente.
    // Fallback: buscar el CFDI local por serie+folio EXACTOS (no regex — a diferencia del
    // buscador de texto libre, acá ya sabemos serie/folio exactos porque vienen del
    // resultado que el usuario clickeó, mismo patrón cross-domain que /cfdis/buscar en
    // bank.routes.js).
    const cfdiLocal = await CFDI.findOne({ source: 'ERP', serie: serieExterna, folio: folioExterno }).lean();
    if (cfdiLocal) {
      const resuelto = _resolverCuentaDesdeCfdiLiquidado(cfdiLocal);
      if (resuelto.error) {
        return res.status(404).json({ error: resuelto.error });
      }
      return res.json(resuelto.cuenta);
    }
    return res.status(404).json({ error: 'No se encontró esta CxC en Kore — puede que ya no esté disponible para vincular.' });
  }

  res.json({
    id:                   raw0.id,
    serie:                raw0.serie                ?? null,
    folio:                raw0.folio                ?? null,
    serieExterna:         raw0.serieExterna         ?? null,
    folioExterno:         raw0.folioExterno         ?? null,
    folioFiscal:          raw0.folioFiscal          ?? null,
    tipoPago:             raw0.tipoPago             ?? null,
    subtotal:             raw0.subtotal,
    impuesto:             raw0.impuesto,
    total:                raw0.total,
    saldoActual:          raw0.saldoActual,
    fechaVencimiento:     raw0.fechaVencimiento     ?? null,
    nombrePersona:        raw0.nombrePersona        ?? null,
    nombreTipoMovimiento: raw0.nombreTipoMovimiento ?? null,
    personaId:            raw0.personaId            ?? null,
    esAnticipo:           raw0.esAnticipo           ?? false,
    origen:               raw0.origen               ?? null,
  });
}));

// Resuelve el shape de "cuenta" que espera el frontend a partir de un CFDI local, para el
// fallback de GET /cuenta-por-serie-folio cuando Kore ya no reporta la CxC como pendiente
// (factura liquidada al 100%). Función pura (sin I/O) para poder testearla aislada sin
// mockear Mongo — recibe el documento CFDI ya encontrado y devuelve { error } si no se
// puede vincular, o { cuenta } con el mismo shape que devuelve esta ruta en el camino
// normal (con Kore).
// saldoActual siempre 0: si llegamos a este fallback es PORQUE Kore ya no considera esta
// CxC pendiente, así que el saldo remanente es 0 por definición de este camino.
// origen: 'cfdi_liquidado' — marca de procedencia, para que quede auditable que esta
// cuenta NO se verificó en vivo contra Kore (a diferencia del camino normal, donde origen
// viene de Kore).
function _resolverCuentaDesdeCfdiLiquidado(cfdi) {
  if (!cfdi.erpId) {
    return { error: 'Esta factura no tiene un identificador de ERP asociado — no se puede vincular.' };
  }
  if (cfdi.erpStatus === 'Cancelado' || cfdi.satStatus === 'Cancelado') {
    return { error: 'Esta factura está cancelada — no se puede vincular.' };
  }

  return {
    cuenta: {
      id:                   cfdi.erpId,
      serie:                cfdi.serie ?? null,
      folio:                cfdi.folio ?? null,
      serieExterna:         cfdi.serie ?? null,
      folioExterno:         cfdi.folio ?? null,
      folioFiscal:          cfdi.uuid  ?? null,
      tipoPago:             cfdi.formaPago ?? null,
      subtotal:             cfdi.subTotal,
      impuesto:             cfdi.impuestos?.totalImpuestosTrasladados ?? 0,
      total:                cfdi.total,
      saldoActual:          0,
      fechaVencimiento:     null,
      nombrePersona:        cfdi.receptor?.nombre ?? null,
      nombreTipoMovimiento: null,
      personaId:            null,
      esAnticipo:           false,
      origen:               'cfdi_liquidado',
    },
  };
}

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

// ── POST /api/erp/formas-pago-cxc/upload ─────────────────────────────────────
// Procesa el Excel "Pagos Asociados" (21 columnas, un pago CFDI por fila que aún no tiene
// movimiento bancario identificado). Por cada fila:
//   1. Ubica la factura en `cfdis` por Serie+Folio (source ERP, tipoDeComprobante 'I').
//   2. Lee documentosRelacionados[0] → Serie/Folio del PEDIDO (no de la factura).
//   3. Consulta la CxC en Kore con esa Serie/Folio del pedido (serieExterna/folioExterno).
//   4. Lee las formas de pago reales de los abonos de esa CxC.
//   5. Clasifica: bancaria / no bancaria / sin resolver (sin_factura, sin_pedido,
//      sin_cxc_en_kore).
// SÍNCRONO a propósito (mismo patrón que pagos-cyc/mostrador-cyc): puede tardar varios
// minutos por la pausa de 1s entre llamados a Kore.
router.post('/formas-pago-cxc/upload',
  authenticate,
  permit('banks:admin'),
  uploadCyc.single('excelFile'),
  asyncHandler(async (req, res) => {
    if (!req.file) return res.status(400).json({ error: 'No se envió ningún archivo Excel' });
    const result = await procesarFormasPagoCxc(
      req.file.buffer,
      req.user._id,
      req.user.nombre,
    );
    res.json(result);
  }),
);

// ── POST /api/erp/formas-pago-cxc/export ─────────────────────────────────────
// Genera un Excel con 3 hojas a partir del resultado del upload:
//   · Hoja "Concentrado bancarias" — filas cuya forma de pago real es bancaria (verde)
//   · Hoja "No bancarias"          — filas cuya forma de pago real NO es bancaria (amarillo)
//   · Hoja "Sin resolver"          — factura/pedido/CxC no encontrados, con razón y detalle (rojo)
router.post('/formas-pago-cxc/export',
  authenticate,
  permit('banks:admin'),
  asyncHandler(async (req, res) => {
    const resultado = req.body;
    if (!resultado || typeof resultado !== 'object') {
      return res.status(400).json({ error: 'Se requiere el resultado del procesamiento en el cuerpo' });
    }

    const buffer = await generarExcelFormasPagoCxc(resultado);

    const fecha = new Date().toISOString().slice(0, 10);
    res.setHeader('Content-Type',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition',
      `attachment; filename="formas-pago-cxc-${fecha}.xlsx"`);
    res.send(buffer);
  }),
);

// GET /api/erp/formas-pago — catálogo de formas de pago desde Kore (test)
router.get('/formas-pago', authenticate, asyncHandler(async (req, res) => {
  const koreToken = getKoreToken(req, res);
  if (!koreToken) return;

  res.json(await listarFormasPago(koreToken));
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

  res.json(await listarBancos(koreToken));
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
// para efectos de saldoErp — mismo criterio de fondo que _esFormaBancaria() (cobro-panel /
// collection-requests): transferencia, cheque o depósito en efectivo. Coincidencia flexible
// (sin acentos, insensible a mayúsculas) porque no hay certeza de que "depósito en efectivo"
// sea siempre el nombre exacto en el catálogo de Kore.
function _esFormaPagoBancariaKore(nombreFormaPago) {
  const norm = String(nombreFormaPago ?? '')
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .trim().toUpperCase();
  return norm === 'TRANSFERENCIA'
      || norm === 'CHEQUE'
      || /DEPOSITO.*EFECTIVO/.test(norm);
}

// Monto realmente cobrado para una cuenta ya saldada (saldoActual 0) o con saldo A FAVOR
// (saldoActual negativo — p.ej. una retención aplicada después del pago completo, que puede
// tardar mucho o nunca en volver exactamente a 0): suma el 'total' (nunca formasPago[].monto,
// que puede ser un depósito compartido entre varias CxC, o —como en el caso de una aplicación
// de saldo a favor/anticipo— ni siquiera corresponder al total del movimiento que lo
// contiene) de CUALQUIER movimiento con `formasPago` no vacío, sin filtrar por tipo — mismo
// criterio que `saldoPagadoTotal` en el flujo manual de cobro (`aplicarLogicaErp`,
// bank.service.js). Movimientos de retención (serie RET) no traen `formasPago` en absoluto,
// así que quedan excluidos naturalmente.
//
// DECISIÓN EXPLÍCITA DEL USUARIO (2026-07-17): antes esto filtraba solo formas bancarias
// (`_esFormaPagoBancariaKore` — transferencia/cheque/depósito en efectivo), precisamente para
// corregir un bug del 2026-07-09 donde una "aplicación de saldo a favor" (pagada en
// EFECTIVO/TARJETA/SALDO A FAVOR de caja) se contaba igual que una transferencia real,
// inflando saldoErp con dinero que nunca entró al banco por ESE depósito. Se revirtió ese
// filtro a propósito porque casos reales de depósito bancario legítimo (ej. efectivo llevado
// físicamente al banco) llegaban con un nombre de forma de pago en Kore que no coincidía con
// el regex ("EFECTIVO" a secas, no "DEPOSITO EN EFECTIVO"), dejando `saldoErpAportado` en 0
// y el movimiento en `no_identificado` pese a ser un depósito real. Riesgo aceptado
// conscientemente: puede volver a inflar saldoErp si Kore trae una aplicación de saldo a
// favor/anticipo/tarjeta como parte de la misma cuenta — no "corregir" este comentario sin
// que el usuario lo pida de nuevo.
// NO se gatea por `raw0.saldoActual` aquí (fix 2026-07-28, folio 036789): este guard vivió
// aquí desde el diseño original, cuando el ÚNICO llamador (_syncErpKoreJob) ya garantizaba
// saldoActual<=0 antes de invocar la función — redundante ahí, pero cuando
// _recomputeErpKoreJob empezó a llamarla directamente sobre links YA finalizados (para
// backfillear con el Kore más fresco), el guard se volvió activo y destructivo: el saldo de
// una CxC en Kore NO es monótono (una retención/bonificación puede revertirse después con un
// ABO y reabrir saldoActual por encima de 0 aunque el pago bancario original siga intacto en
// el historial) — el guard descartaba a 0 un aporte real y ya confirmado solo porque el saldo
// ACTUAL (no el de cuando se cerró) volvió a ser positivo. Ambas funciones ahora solo suman lo
// que el historial de `movimientos` diga, sin importar el saldo vigente.
function _montoSaldoLink(raw0) {
  if (!raw0) return 0;
  const conFormaPago = (raw0.movimientos ?? []).filter(
    m => Array.isArray(m.formasPago) && m.formasPago.length > 0,
  );
  return conFormaPago.reduce((sum, m) => sum + Math.abs(m.total ?? 0), 0);
}

// Bancario-only: mismo criterio que tenía _montoSaldoLink ANTES del cambio del 2026-07-17
// (transferencia/cheque/depósito en efectivo, vía _esFormaPagoBancariaKore), aplicado ahora
// para backfillear `saldoPagado` (bancario-only) mientras saldoErpAportado/saldoPagadoTotal
// siguen sumando TODAS las formas de pago — ver _recomputeErpKoreJob. Ya no se usa desde
// _recomputeErpKoreJob/_syncErpKoreJob (ver _montoSaldoLinkPorMovimiento) — se deja solo
// para el script CLI deprecado (recompute-saldo-erp-todas-formas-pago.js).
function _montoSaldoLinkBancario(raw0) {
  if (!raw0) return 0;
  const conFormaBancaria = (raw0.movimientos ?? []).filter(
    m => Array.isArray(m.formasPago) && m.formasPago.some(fp => _esFormaPagoBancariaKore(fp.nombreFormaPago)),
  );
  return conFormaBancaria.reduce((sum, m) => sum + Math.abs(m.total ?? 0), 0);
}

// Compara el `Aut`/`Numo` de un forma-de-pago de Kore contra la identidad de ESTE
// movimiento bancario — dos señales redundantes porque Kore no las usa de forma
// consistente: "Numo" suele traer el numeroAutorizacion real del banco (cuando Numo aplicó
// el cobro), "Aut" suele traer el FOLIO de Numo, pero NO siempre a secas — quien autoriza en
// Kore a veces agrega texto alrededor (fix 2026-07-29, folio 037349: `Aut: "037349-CRISTIAN"`
// no matcheaba con igualdad exacta contra `folio: "037349"`; mismo patrón en 037075 ("037075
// C.P CRISTIAN") y 036472 ("AUTORIZA CRISTIAN 036472"), aunque estos últimos dos ya habían
// cerrado con el criterio viejo pre-Aut-matching, antes de que este bug pudiera manifestarse).
// La comparación ahora es "el folio aparece dentro del Aut" en vez de igualdad exacta — un
// folio de 6 dígitos casi no tiene riesgo real de aparecer como substring de un Aut ajeno.
// Se aceptan ambas coincidencias (Numo/Aut) porque una misma CxC puede traer movimientos
// tageados con una u otra según cómo Kore/Numo aplicó cada pago.
function _perteneceAEsteMovimiento(fp, mov) {
  const autNormMov = _normalizarAutorizacion(mov.numeroAutorizacion);
  const numoTag    = (fp.adicionales ?? []).find(a => a.nombre === 'Numo');
  if (numoTag && autNormMov && _normalizarAutorizacion(numoTag.valor) === autNormMov) return true;
  const autTag  = (fp.adicionales ?? []).find(a => a.nombre === 'Aut');
  const folioMov = String(mov.folio ?? '').trim();
  if (autTag && folioMov && String(autTag.valor ?? '').trim().includes(folioMov)) return true;
  return false;
}

// Aporte NETO (no suma de valores absolutos) que corresponde específicamente a ESTE
// movimiento bancario, para vínculos HUMANOS — reemplaza a _montoSaldoLink en
// _syncErpKoreJob/_recomputeErpKoreJob (fix 2026-07-28, folios 033439/033764/036170).
// _montoSaldoLink sumaba abs(total) de CUALQUIER movimiento con formasPago, sin importar
// (a) si un ciclo aplicar→revertir→reaplicar (series ABO→RAB→ABO, confirmado con Kore real)
// triplicaba el mismo pago, o (b) si la CxC recibió pagos de VARIOS depósitos bancarios
// distintos a lo largo del tiempo (caso real: folioExterno 260601153 recibió pagos de los
// movimientos 033439 Y 033764), atribuyendo a este movimiento dinero que entró por otro.
//
// Algoritmo: recorre los movimientos con formaPago en orden cronológico, sumando CON SIGNO
// (no absoluto) a un acumulador "mío" (su Aut/Numo coincide con ESTE movimiento, ver
// _perteneceAEsteMovimiento) o "de otro" (coincide con una autorización distinta — sabemos
// que no es nuestro). Las reversas (series RAB u otras) casi nunca traen Aut/Numo de
// vuelta — se atribuyen al acumulador cuyo neto actual coincide EXACTAMENTE en magnitud
// opuesta (una reversa, por definición, cancela algo que ya estaba ahí; si no cancela nada
// con exactitud, se ignora — no se inventa a qué autorización pertenece). Sumar con signo
// hace que un ciclo aplicar→revertir→reaplicar quede en el último valor vigente, sin
// triplicar. Devuelve null (nunca 0) si nunca hubo una coincidencia propia — "no
// determinado" evita pisar un saldoErp ya correcto con un cero falso.
function _montoSaldoLinkPorMovimiento(raw0, mov, incluirFormaPago = () => true) {
  if (!raw0) return null;
  const conFormaPago = (raw0.movimientos ?? []).filter(
    m => Array.isArray(m.formasPago) && m.formasPago.some(incluirFormaPago),
  );

  let miNeto = 0;
  let otroNeto = 0;
  let huboCoincidenciaPropia = false;

  for (const m of conFormaPago) {
    const esMio    = m.formasPago.some(fp => _perteneceAEsteMovimiento(fp, mov));
    const esDeOtro = !esMio && m.formasPago.some(fp =>
      (fp.adicionales ?? []).some(a => a.nombre === 'Numo' || a.nombre === 'Aut'),
    );
    const total = m.total ?? 0;

    if (esMio) {
      miNeto += total;
      huboCoincidenciaPropia = true;
    } else if (esDeOtro) {
      otroNeto += total;
    } else if (miNeto !== 0 && Math.abs(miNeto + total) < 0.01) {
      miNeto += total; // reversa sin tag — cancela lo que ya llevaba "mío"
    } else if (otroNeto !== 0 && Math.abs(otroNeto + total) < 0.01) {
      otroNeto += total; // reversa sin tag — cancela lo que ya llevaba "de otro"
    }
    // si una reversa sin tag no cancela ningún acumulador con magnitud exacta, se ignora:
    // no se puede atribuir con certeza y preferimos no adivinar.
  }

  return huboCoincidenciaPropia ? Math.abs(miNeto) : null;
}

// Piso de aporte para un depósito bancario ya vinculado por un HUMANO: nunca debe bajar en
// una corrida posterior de "Recalcular saldo ERP", sin importar la causa (retención,
// cancelación, devolución — CAC/DEV/RET, o cualquier otro ajuste que Kore aplique después).
// Decisión explícita del usuario (2026-08-06, folio 032686): "el objetivo es subir los
// movimientos, no bajarlos". Solo se permite subir si Kore trae un monto MAYOR atribuible a
// este movimiento (una bonificación real) — nunca bajar.
//
// El piso es, en orden: el aporte ya confirmado (saldoErpAportado), o si nunca se determinó
// el saldoPagadoTotal ya confirmado por un humano al vincular, o —último recurso, solo si
// AMBOS ya se perdieron a null por una corrida vieja anterior a este fix, y el movimiento
// tiene un ÚNICO erpLink— el monto real del depósito bancario: un solo link humano en un
// movimiento es, por definición, todo ese depósito.
function _aporteConRatchet(link, calculado, mov, totalLinksEnMovimiento) {
  const pisoConfirmado = link.saldoErpAportado
    ?? link.saldoPagadoTotal
    ?? (totalLinksEnMovimiento === 1 ? Math.abs(mov.deposito ?? mov.retiro ?? 0) : null);
  if (calculado == null) return pisoConfirmado;
  if (pisoConfirmado == null) return calculado;
  return Math.max(calculado, pisoConfirmado);
}

// Deriva saldoPagado/saldoPagadoTotal/folioFiscal de un erpLink a partir de la respuesta de
// Kore YA consultada (raw0) — compartido por _syncErpKoreJob (al finalizar un link por
// primera vez, sin pegarle a Kore una segunda vez) y _recomputeErpKoreJob (backfill
// histórico del botón "Recalcular saldo ERP"), para que ambos usen exactamente el mismo
// criterio y no se desincronicen con el tiempo.
// folioFiscalPendiente=true → el llamador NO debe avanzar recomputedFormasPagoAt (se
// reintenta en la próxima corrida). Decisión del usuario 2026-07-24 (revertida 2026-07-30):
// originalmente, si saldoActual===0 (CxC cerrada de forma limpia) se aceptaba folioFiscal
// null para siempre, asumiendo que Kore no tenía por qué facturar esa CxC nunca. Evidencia
// real (folios 036472/036917/036967/037095/037085/037076/037099, 2026-07-30): Kore SÍ
// termina timbrando el CFDI después del cierre limpio en el 100% de una muestra de 7 casos
// — la hipótesis no se sostenía. Se pasó a reintentar SIEMPRE mientras folioFiscal siga null,
// sin importar saldoActual/retención (mismo criterio ya usado para retención desde el
// 2026-07-28, folio 036827, extendido a todos los cierres).
// Acotado a 60 días (decisión del usuario 2026-08-03): el reintento indefinido no tenía techo
// de costo — reconsulta Kore para siempre incluso en CxC que legítimamente nunca van a
// facturarse. 60 días desde que Kore confirmó el cierre (conciliacionFinalizadaAt) da margen
// de sobra a un CFDI que tarda en timbrarse sin reprocesar ese link para siempre. Pasada la
// ventana, folioFiscal se acepta null (avanza el checkpoint) igual que la política vieja del
// 24-jul, pero ahora con una ventana de gracia real en vez de "nunca reintentar".
const DIAS_MAX_REINTENTO_FOLIO_FISCAL = 60;

// fechaAnclaAlterna (2026-08-04, folio 037600): los links creados por Solicitudes de
// Cobro/Aplicar cobro manual NUNCA llenan conciliacionFinalizadaAt (ese campo es exclusivo
// del flujo tradicional) — sin esto, `!conciliacionFinalizadaAt` siempre caía en "sigue
// dentro de ventana" para ellos, es decir, reintento indefinido de por vida (el mismo
// problema sin techo que esta ventana vino a resolver, solo que para otro flujo). El
// llamador resuelve la fecha real de cierre de ESE flujo (identificadoPor.fechaId) y la
// pasa acá como fallback cuando no hay conciliacionFinalizadaAt.
function _folioFiscalDentroDeVentanaReintento(conciliacionFinalizadaAt, fechaAnclaAlterna = null) {
  const ancla = conciliacionFinalizadaAt ?? fechaAnclaAlterna;
  // Sin ninguna fecha de cierre todavía (link recién finalizando en esta misma corrida) —
  // no hay "días transcurridos" que contar todavía, sigue dentro de ventana.
  if (!ancla) return true;
  const dias = (Date.now() - new Date(ancla).getTime()) / 86400000;
  return dias <= DIAS_MAX_REINTENTO_FOLIO_FISCAL;
}

function _backfillFormasPagoYFolioFiscal(link, raw0, mov, esHumano, aporteNuevo) {
  // aporteNuevo/saldoPagadoCalc ahora pueden venir en null (_montoSaldoLinkPorMovimiento no
  // encontró coincidencia propia) — se conserva el valor previo del link en vez de pisarlo
  // con un cero falso (mismo criterio de "no determinado" != "cero" del resto del archivo).
  const saldoPagadoTotal   = aporteNuevo ?? link.saldoPagadoTotal ?? null;
  const saldoPagadoCalc    = esHumano
    ? _montoSaldoLinkPorMovimiento(raw0, mov, fp => _esFormaPagoBancariaKore(fp.nombreFormaPago))
    : aporteNuevo;
  const saldoPagado      = saldoPagadoCalc ?? link.saldoPagado ?? null;
  // Fix 2026-07-28 (folio 034310): Kore puede devolver folioFiscal como '' (string vacío) en
  // vez de null/ausente cuando el CFDI todavía no existe. `??` no trata '' como "ausente" (no
  // es null/undefined) — antes, en cuanto raw0.folioFiscal llegaba como '', ese '' se guardaba
  // en el link para siempre y NUNCA se volvía a revisar Kore (link.folioFiscal ?? ... siempre
  // devolvía el '' ya guardado). El `||` sí normaliza '' a "ausente", igual que null/undefined
  // — seguro aquí porque folioFiscal es un UUID/string, nunca un 0/false legítimo.
  const folioFiscal = (link.folioFiscal || null) ?? (raw0.folioFiscal || null) ?? null;
  // Ver comentario arriba de la función (2026-07-30 + 2026-08-03): ya no importa si la CxC
  // cerró limpia, por retención o con saldo a favor — mientras folioFiscal siga sin resolver
  // Y todavía estemos dentro de los 60 días desde el cierre, se reintenta.
  // fechaAnclaAlterna (2026-08-04): para links sin conciliacionFinalizadaAt (Solicitudes de
  // Cobro/Aplicar cobro manual), la fecha de cierre real es cuándo se confirmó ESE cobro —
  // identificadoPor.fechaId del erpId de este link, ya guardado por identificar()/setErpIds().
  const fechaAnclaAlterna = mov.identificadoPor?.find(i => i.erpId === link.erpId)?.fechaId ?? null;
  const folioFiscalPendiente =
    folioFiscal == null &&
    _folioFiscalDentroDeVentanaReintento(link.conciliacionFinalizadaAt, fechaAnclaAlterna);
  return { saldoPagadoTotal, saldoPagado, folioFiscal, folioFiscalPendiente };
}

// Retención NETA vigente ahora mismo (para los campos persistidos link.tieneRetencion/
// link.montoRetenido, que alimentan el reporte "Retención" de bank.service.js) — fix
// 2026-07-28 (folio 036789): la implementación vieja sumaba abs(total) de movimientos con
// serie==='RET', pero una retención puede revertirse después vía un movimiento que NO trae
// serie 'RET' (caso real: un 'ABO' con el monto exacto en positivo) — filtrar por serie no
// alcanza para detectar la reversa, así que una retención ya cancelada se seguía contando
// como vigente para siempre. La señal robusta es la AUSENCIA de `formasPago`: tanto una
// retención como su reversa son ajustes contables internos (nunca dinero real de banco), a
// diferencia de un cobro/abono real que siempre trae formasPago. Sumar CON SIGNO (no abs)
// hace que una retención + su reversa exacta neteen a cero, dejando solo lo que sigue
// genuinamente retenido. Se descarta el primer movimiento (el cargo original, mismo criterio
// que _movimientosKoreDesde) porque nunca es un abono/ajuste y arruinaría el neto.
//
// OJO: distinto del `tieneRetencion` local de _backfillFormasPagoYFolioFiscal (arriba,
// "¿alguna vez hubo un RET?", más laxo a propósito) — ese sigue intacto porque gatea el
// reintento de folioFiscal y preferimos que sea conservador (seguir reintentando de más es
// seguro; no es lo que se pidió corregir acá).
function _retencionVigente(raw0) {
  const movsNoBancarios = (raw0?.movimientos ?? []).slice(1).filter(
    m => !Array.isArray(m.formasPago) || m.formasPago.length === 0,
  );
  const neto = movsNoBancarios.reduce((s, m) => s + (m.total ?? 0), 0);
  const tieneRetencion = Math.abs(neto) > 0.01;
  const montoRetenido  = tieneRetencion ? Math.abs(neto) : null;
  return { tieneRetencion, montoRetenido };
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

// Snapshot de movimientos Kore para rastreo/conciliación manual — se guarda en
// erpLinks[].movimientosKore. Se descarta el primero (el cargo original que crea la CxC,
// no un abono/ajuste). Incluye formasPago tal cual las manda Kore — información adicional
// para análisis futuro, nunca usada para calcular saldoErpAportado/tieneRetencion/
// montoRetenido (esos siguen basándose solo en `total`/`serie`, ver más abajo) — este campo
// se pisa completo en cada corrida junto con el resto de movimientosKore, así que no hay
// riesgo de duplicar ni corromper la bitácora desglosePorFormaPago (esa es acumulativa y
// la sigue llenando solo Numo al aplicar un cobro, el sync nunca la toca).
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
    formasPago: Array.isArray(m.formasPago)
      ? m.formasPago.map(fp => ({
          formaPagoId:          fp.formasPago      ?? null,
          formaPagoDescripcion: fp.nombreFormaPago ?? null,
          monto:                fp.monto           ?? null,
          adicionales: Array.isArray(fp.adicionales)
            ? fp.adicionales.map(a => ({ nombre: a.nombre ?? null, valor: a.valor ?? null }))
            : [],
        }))
      : [],
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
      .select('_id folio banco concepto deposito retiro fecha saldoErp status ficha erpLinks identificadoPor numeroAutorizacion primeraIdentificacionAt primeraIdentificacionPor')
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
          // siempre en esos casos. Ver _retencionVigente (neto, no suma de absolutos — una
          // retención revertida más tarde por Kore no debe seguir contando como vigente).
          const retencionAhora = _retencionVigente(raw0);
          if (retencionAhora.tieneRetencion !== (link.tieneRetencion ?? false)) {
            linksActualizados[i].tieneRetencion = retencionAhora.tieneRetencion;
          }
          if (retencionAhora.montoRetenido !== (link.montoRetenido ?? null)) {
            linksActualizados[i].montoRetenido = retencionAhora.montoRetenido;
          }

          // saldoActual también se refresca en CADA corrida — fix 2026-07-28 (folio 036789):
          // este campo antes SOLO se escribía una vez, en setErpIds() al vincular la CxC (ver
          // bank.service.js), y nunca se volvía a tocar. Si Kore reabre el saldo después (ej.
          // una bonificación aplicada se revierte vía ABO), el link quedaba con un `0`
          // congelado para siempre. cobro-panel.component.ts (_openCobroPanel) confía en este
          // campo por encima del caché de Kore cuando `saldoPagado != null` — con el `0`
          // obsoleto, la CxC parecía completamente saldada y el panel de "Aplicar cobro"
          // la excluía por completo, mostrando pantalla en blanco pese a que Kore sí tenía
          // saldo disponible para cobrar.
          if (raw0?.saldoActual != null && raw0.saldoActual !== (link.saldoActual ?? null)) {
            linksActualizados[i].saldoActual = raw0.saldoActual;
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
              ? _montoSaldoLinkPorMovimiento(raw0, mov)
              : _montoSaldoLinkPorAutorizacion(raw0, mov.numeroAutorizacion);
            if (aporte != null) linksActualizados[i].saldoErpAportado = aporte;

            // Backfill inmediato de saldoPagado/saldoPagadoTotal/folioFiscal AL FINALIZAR
            // (2026-07-24, mismo criterio que el botón "Recalcular saldo ERP" —
            // _backfillFormasPagoYFolioFiscal) — reutiliza el MISMO raw0 ya consultado, sin
            // pegarle a Kore otra vez. Así un link creado por el motor automático o vinculado
            // a mano sin cobro (que nunca pasa por setErpIds/buildErpLinksParaCobro) no
            // depende de que un admin corra el backfill manual para tener esta info desde el
            // día en que se cierra.
            const backfill = _backfillFormasPagoYFolioFiscal(link, raw0, mov, esHumano, aporte);
            linksActualizados[i].saldoPagadoTotal = backfill.saldoPagadoTotal;
            linksActualizados[i].saldoPagado      = backfill.saldoPagado;
            linksActualizados[i].folioFiscal      = backfill.folioFiscal;

            linksActualizados[i].conciliacionFinalizadaAt = new Date();
            linksActualizados[i].conciliacionRunId        = jobId;
            // Mismo checkpoint que usa el botón de backfill: si folioFiscal no se resolvió y
            // la CxC sigue "en movimiento" (saldoActual!==0), queda pendiente para que una
            // corrida futura (automática o el botón "Recalcular saldo ERP") lo reintente.
            linksActualizados[i].recomputedFormasPagoAt = backfill.folioFiscalPendiente ? null : new Date();
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
        // Fix 2026-07-28 (folio 036636, cobro múltiple): al sumar, un link con
        // saldoErpAportado:null usaba 0 — correcto SOLO si de verdad no se sabe nada de esa
        // CxC, pero incorrecto cuando esa CxC es enorme (se paga en varias exhibiciones a lo
        // largo de meses) y todavía no cierra por su cuenta (saldoActual!==0), aunque un
        // humano YA aplicó y confirmó un cobro real de ESTE depósito sobre ella
        // (saldoPagadoTotal, seteado por setErpIds()/aplicarLogicaErp al momento del cobro).
        // Tratar ese caso como "aportó 0" descartaba dinero real ya confirmado. Se prioriza
        // saldoErpAportado (el valor riguroso, re-verificado contra Kore) y solo se cae a
        // saldoPagadoTotal (la estimación del cobro humano) cuando el primero es null —
        // nunca 0 salvo que NINGUNO de los dos esté determinado.
        const saldoErpNuevo = linksActualizados.reduce((s, l) => s + (l.saldoErpAportado ?? l.saldoPagadoTotal ?? 0), 0);
        const bankAmount    = Math.abs(mov.deposito ?? mov.retiro ?? 0);
        let statusNuevo      = saldoErpNuevo >= bankAmount - ERP_TOLERANCE ? 'identificado' : 'no_identificado';
        if (mov.ficha && statusNuevo === 'no_identificado') statusNuevo = 'identificado';

        if (saldoErpNuevo !== (mov.saldoErp ?? null) || statusNuevo !== mov.status) {
          // Solo se toca primeraIdentificacionAt/Por en esta rama, donde el status
          // realmente cambia — no en cada corrida sin cambios de saldo/status.
          const { primeraIdentificacionAt, primeraIdentificacionPor } =
            resolvePrimeraIdentificacion(statusNuevo, mov, null);
          update.$set = {
            ...(update.$set ?? {}),
            saldoErp: saldoErpNuevo,
            status:   statusNuevo,
            primeraIdentificacionAt,
            primeraIdentificacionPor,
          };
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
      SYNC_JOBS.set(jobId, { status: 'stopped', auth0Sub, result, detalles, kind: 'sync' });
      emitToUser(auth0Sub, 'bank:erp:sync:stopped', { jobId, ...result });
    } else {
      const result = { total, actualizados, pendientes, errores };
      SYNC_JOBS.set(jobId, { status: 'done', auth0Sub, result, detalles, kind: 'sync' });
      emitToUser(auth0Sub, 'bank:erp:sync:done', { jobId, ...result });
    }
  } catch (err) {
    const error = err.message || 'Error en sincronización ERP-Kore';
    SYNC_JOBS.set(jobId, { status: 'error', auth0Sub, error, kind: 'sync' });
    emitToUser(auth0Sub, 'bank:erp:sync:error', { jobId, error });
  } finally {
    syncRunning      = false;
    syncCurrentJobId = null;
    setTimeout(() => SYNC_JOBS.delete(jobId), SYNC_JOB_TTL);
  }
}

// ── Job "Recalcular saldo ERP" (backfill unificado) ───────────────────────────────────────
// Fusiona en un solo paso por link lo que antes eran dos scripts separados
// (migrate-erp-movimientoskore-formaspago.js + recompute-saldo-erp-todas-formas-pago.js):
// por cada erpLink YA finalizado (conciliacionFinalizadaAt !== null) sin recomputar todavía
// (recomputedFormasPagoAt === null), UNA sola consulta a Kore por link — refresca siempre el
// snapshot movimientosKore, y si el vínculo es humano recalcula saldoErpAportado con el
// criterio de "todas las formas de pago" (2026-07-17). Los vínculos de motor solo reciben el
// backfill del snapshot, su cálculo por autorización no cambió.
//
// Reutiliza el MISMO SYNC_JOBS/syncControl/syncRunning que el job normal — mutuamente
// excluyentes entre sí (comparten el rate limit de Kore, nunca corren dos a la vez) — y el
// MISMO endpoint de revert (via:'erp-sync', runId=jobId), sin tooling nuevo.
//
// A diferencia de los scripts originales, SÍ tiene checkpoint (recomputedFormasPagoAt): una
// corrida repetida solo procesa lo que quedó pendiente de una corrida anterior (links
// finalizados nuevos desde entonces, o que no se pudieron consultar la vez pasada), nunca
// vuelve a pegarle a Kore por un link que ya se revisó.
// Bug real 2026-07-31 (folio 038141): este job originalmente exigía conciliacionFinalizadaAt
// no-nulo, un campo que SOLO marca el flujo tradicional de conciliación bancaria — los links
// creados por Solicitudes de Cobro, por "Aplicar Cobro" del panel manual, o por "Guardar"
// (vincular sin cobro) NUNCA tocan ese campo, así que quedaban invisibles para este job por
// diseño, aunque su saldoPagadoTotal/saldoErpAportado nunca se hubiera determinado (caso
// real: 3 CxC vinculadas manualmente, saldoPagado/saldoPagadoTotal/saldoErpAportado en null
// para siempre, saldoErp calculado con el fallback legacy — total/saldoActual — en vez del
// aporte bancario real). Ampliado: ahora también califica un link SIN conciliacionFinalizadaAt
// mientras su saldoPagadoTotal siga sin determinar (null) — deliberadamente acotado a ese
// caso, para no reprocesar de más los links de cobro-panel que YA tienen saldoPagadoTotal
// bien calculado por _buildCobroSaldosErp() del lado del frontend.
async function _recomputeErpKoreJob(auth0Sub, jobId, fechaInicio, fechaFin, dryRun = false) {
  syncCurrentJobId = jobId;
  try {
    const filter = {
      erpLinks: {
        $elemMatch: {
          serie:                  { $ne: null },
          folioExterno:           { $ne: null },
          recomputedFormasPagoAt: null,
          $or: [
            { conciliacionFinalizadaAt: { $ne: null } },
            { saldoPagadoTotal: null },
            // 2026-08-04 (folio 037600): sin esto, un link de Solicitud de Cobro con
            // saldoPagadoTotal YA correcto (confirmado por un humano) y
            // conciliacionFinalizadaAt que ese flujo nunca llena queda fuera de ESTE
            // filtro para siempre — el cron diario nunca lo vuelve a seleccionar aunque
            // folioFiscal siga null. La precisión real (¿sigue dentro de la ventana de
            // 60 días?) se decide por-link más abajo, con el ancla que corresponda.
            { folioFiscal: { $in: [null, ''] } },
          ],
        },
      },
    };
    if (fechaInicio && fechaFin) filter.fecha = { $gte: fechaInicio, $lte: fechaFin };

    const movements = await BankMovement.find(filter)
      .select('_id folio banco concepto deposito retiro fecha saldoErp status ficha erpLinks identificadoPor numeroAutorizacion primeraIdentificacionAt primeraIdentificacionPor')
      .lean();

    let procesados = 0, actualizados = 0, sinDatos = 0, errores = 0, pendientesFolioFiscal = 0;
    const total    = movements.length;
    let stopped    = false;
    const detalles = []; // una entrada por movimiento con cambios — insumo del reporte Excel

    emitToUser(auth0Sub, 'bank:erp:sync:progress',
      { jobId, procesados, total, actualizados, pendientes: sinDatos, pendientesFolioFiscal, errores, pct: 0 });

    for (const mov of movements) {
      if (!await _checkSyncControl()) { stopped = true; break; }

      const links = mov.erpLinks ?? [];
      let huboErrorMov             = false;
      let huboLinkTocado           = false; // avanzó el checkpoint (con o sin cambio de aporte)
      let huboCambioAporte         = false;
      let huboSinDatos             = false;
      let huboFolioFiscalPendiente = false; // algún link quedó sin folioFiscal y sin avanzar checkpoint
      let huboFolioFiscalRecuperado = false; // algún link pasó de folioFiscal null a un valor real esta corrida
      const linksDetalle      = [];
      const linksActualizados = links.map(l => ({ ...l }));

      for (let i = 0; i < links.length; i++) {
        const link = links[i];
        // Mismo criterio ampliado que el filtro de Mongo de arriba: califica si viene del
        // flujo tradicional (conciliacionFinalizadaAt) O si su aporte nunca se determinó
        // (saldoPagadoTotal null), sin importar el flujo que lo haya creado.
        // 2026-08-04 (folio 037600): además califica si folioFiscal sigue sin resolver Y
        // todavía estamos dentro de los 60 días desde el cierre real de ESTE link (mismo
        // ancla que usa _backfillFormasPagoYFolioFiscal — conciliacionFinalizadaAt si
        // existe, si no identificadoPor.fechaId del erpId). Pasada la ventana no hace
        // falta seguir entrando aquí: aunque el filtro de Mongo lo traiga, folioFiscal ya
        // se acepta null para siempre y no hay nada más que ganar reconsultando Kore.
        const fechaAnclaAlterna = mov.identificadoPor?.find(ip => ip.erpId === link.erpId)?.fechaId ?? null;
        const folioFiscalRecuperable = !link.folioFiscal
          && _folioFiscalDentroDeVentanaReintento(link.conciliacionFinalizadaAt, fechaAnclaAlterna);
        // `finalizadoManualmente` (2026-08-06): mismo rol que `conciliacionFinalizadaAt` pero
        // para Solicitudes de Cobro/Aplicar cobro manual — un link con aporte YA confirmado
        // (saldoPagadoTotal real) y una fecha de cierre propia (fechaAnclaAlterna) está tan
        // "finalizado" como uno tradicional. Sin esto, el rescate masivo/puntual
        // (`_FILTRO_LINK_ATRAPADO`/reset-recompute) resetea el checkpoint pero la SIGUIENTE
        // corrida vuelve a excluirlo aquí mismo por la ventana de 60 días — el reset queda sin
        // efecto. Igual que con conciliacionFinalizadaAt, la ventana sigue gobernando
        // ÚNICAMENTE si el checkpoint vuelve a avanzar después de consultar Kore
        // (folioFiscalPendiente, más abajo) — no si se consulta o no.
        const finalizadoManualmente = link.saldoPagadoTotal != null && fechaAnclaAlterna != null;
        const elegible = link.conciliacionFinalizadaAt != null || link.saldoPagadoTotal == null
          || finalizadoManualmente || folioFiscalRecuperable;
        if (!link.serie || !link.folioExterno || link.recomputedFormasPagoAt || !elegible) continue;

        const rango = _rangoDesdeFollo(link.folioExterno);
        if (!rango) continue;

        try {
          let { raw } = await _sincronizarConRetry({
            serieExterna: link.serie,
            folioExterno: String(link.folioExterno),
            fechaDesde:   rango.fechaDesde,
            fechaHasta:   rango.fechaHasta,
          });

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
          if (!raw0) {
            huboSinDatos = true;
            linksDetalle.push({
              erpId: link.erpId, serie: link.serie, folioExterno: link.folioExterno,
              estado: 'sin_datos',
            });
            await _sleep(SYNC_DELAY_MS);
            continue;
          }

          // Vínculo humano: recalcula el aporte con el criterio nuevo (todas las formas de
          // pago) — vínculo de motor: se deja intacto, solo recibe el backfill del snapshot.
          const esHumano  = _erpIdIdentificadoPorHumano(mov.identificadoPor, link.erpId);
          let aporteNuevo = link.saldoErpAportado ?? null;
          let cambioAporte = false;
          if (esHumano) {
            // calculado puede venir null (ninguna entrada de Kore trae el Aut/Numo de ESTE
            // movimiento) — _aporteConRatchet decide el piso a usar en ese caso. Generaliza
            // el fix 2026-07-29 (folio 036030, retención sin formasPago real) y agrega el
            // ratchet 2026-08-06 (folio 032686, DEV bajando un depósito ya identificado).
            const calculado   = _montoSaldoLinkPorMovimiento(raw0, mov);
            const mejorAporte = _aporteConRatchet(link, calculado, mov, links.length);
            if (mejorAporte != null && Math.abs(mejorAporte - (link.saldoErpAportado ?? 0)) > 0.01) {
              aporteNuevo  = mejorAporte;
              cambioAporte = true;
            }
          }

          const backfill = _backfillFormasPagoYFolioFiscal(link, raw0, mov, esHumano, aporteNuevo);
          // Fix 2026-07-28 (folio 036789): a diferencia de _syncErpKoreJob, este job antes
          // NUNCA refrescaba tieneRetencion/montoRetenido — una vez que el link finalizaba,
          // esos campos quedaban congelados para siempre aunque Kore aplicara/revirtiera una
          // retención después. _recomputeErpKoreJob es justo el que reconsulta links ya
          // finalizados con Kore fresco, así que es el lugar correcto para mantenerlos al día.
          const retencionAhora = _retencionVigente(raw0);

          linksActualizados[i] = {
            ...link,
            movimientosKore:        _movimientosKoreDesde(raw0),
            saldoErpAportado:       aporteNuevo,
            saldoPagadoTotal:       backfill.saldoPagadoTotal,
            saldoPagado:            backfill.saldoPagado,
            folioFiscal:            backfill.folioFiscal,
            tieneRetencion:         retencionAhora.tieneRetencion,
            montoRetenido:          retencionAhora.montoRetenido,
            // Fix 2026-07-28 (folio 036789): mismo motivo que tieneRetencion/montoRetenido
            // arriba — antes NUNCA se refrescaba tras finalizar el link, y cobro-panel
            // (_openCobroPanel) confía en este campo por encima del caché de Kore, así que un
            // `saldoActual` congelado en 0 excluía por completo una CxC que Kore sí tenía
            // disponible para cobrar, mostrando pantalla en blanco en "Aplicar cobro".
            saldoActual:            raw0.saldoActual ?? link.saldoActual ?? null,
            conciliacionRunId:      cambioAporte ? jobId : link.conciliacionRunId,
            recomputedFormasPagoAt: backfill.folioFiscalPendiente ? null : new Date(),
          };
          // Fix 2026-08-06 (lote de 16 folios en "Backfill sin cambio"): folioFiscal se
          // escribe SIEMPRE (línea de arriba, backfill.folioFiscal), sin relación con
          // cambioAporte — un link que recupera folioFiscal pero no mueve el aporte quedaba
          // reportado como "sin cambio" aunque el dato SÍ se haya actualizado en Mongo.
          const folioFiscalRecuperadoAhora = !link.folioFiscal && !!backfill.folioFiscal;

          huboLinkTocado = true;
          if (cambioAporte) huboCambioAporte = true;
          if (backfill.folioFiscalPendiente) huboFolioFiscalPendiente = true;
          if (folioFiscalRecuperadoAhora) huboFolioFiscalRecuperado = true;

          linksDetalle.push({
            erpId: link.erpId, serie: link.serie, folioExterno: link.folioExterno,
            estado: backfill.folioFiscalPendiente
              ? 'folio_fiscal_pendiente'
              : ((cambioAporte || folioFiscalRecuperadoAhora) ? 'actualizado' : 'sin_cambio'),
            aporteAntes: link.saldoErpAportado ?? null, aporteDespues: aporteNuevo,
          });
        } catch (err) {
          errores++;
          huboErrorMov = true;
          linksDetalle.push({
            erpId: link.erpId, serie: link.serie, folioExterno: link.folioExterno,
            estado: 'error', error: err.message || 'Error al consultar Kore',
          });
        }

        await _sleep(SYNC_DELAY_MS);
      }

      const update = {};
      if (huboLinkTocado) update.$set = { erpLinks: linksActualizados };

      if (huboCambioAporte) {
        const hayAlgunAporteDeterminado = linksActualizados.some(l => l.saldoErpAportado != null);
        if (hayAlgunAporteDeterminado) {
          // Fix 2026-07-28 (folio 036636, cobro múltiple) — mismo criterio que
          // _syncErpKoreJob: un link con saldoErpAportado:null (CxC grande, pagada en varias
          // exhibiciones, todavía no cierra por su cuenta) puede tener igual un cobro humano
          // ya confirmado de ESTE depósito (saldoPagadoTotal) — usar 0 ahí descartaba dinero
          // real. Se prioriza saldoErpAportado, se cae a saldoPagadoTotal si es null.
          const saldoErpNuevo = linksActualizados.reduce((s, l) => s + (l.saldoErpAportado ?? l.saldoPagadoTotal ?? 0), 0);
          const bankAmount    = Math.abs(mov.deposito ?? mov.retiro ?? 0);
          let statusNuevo     = saldoErpNuevo >= bankAmount - ERP_TOLERANCE ? 'identificado' : 'no_identificado';
          if (mov.ficha && statusNuevo === 'no_identificado') statusNuevo = 'identificado';

          if (saldoErpNuevo !== (mov.saldoErp ?? null) || statusNuevo !== mov.status) {
            // Solo se toca primeraIdentificacionAt/Por en esta rama, donde el status
            // realmente cambia — no en cada corrida sin cambios de saldo/status.
            const { primeraIdentificacionAt, primeraIdentificacionPor } =
              resolvePrimeraIdentificacion(statusNuevo, mov, null);
            update.$set = {
              ...(update.$set ?? {}),
              saldoErp: saldoErpNuevo,
              status:   statusNuevo,
              primeraIdentificacionAt,
              primeraIdentificacionPor,
            };
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
      }

      let estado;
      if (huboErrorMov)                                      estado = 'error';
      else if (huboFolioFiscalPendiente)                     estado = 'folio_fiscal_pendiente';
      else if (update.$push || huboFolioFiscalRecuperado)    estado = 'actualizado';
      else if (huboSinDatos)                                 estado = 'sin_datos';
      else if (huboLinkTocado)                               estado = 'backfill'; // checkpoint avanzó, sin cambio de saldo/folioFiscal
      else                                                    estado = null;      // nada elegible pudo procesarse (ej. folio inválido)
      if (estado === 'actualizado')            actualizados++;
      if (estado === 'sin_datos')              sinDatos++;
      if (estado === 'folio_fiscal_pendiente') pendientesFolioFiscal++;

      // dryRun: todo el cálculo/reporte de arriba corre igual — nada se escribe en Mongo,
      // así que el checkpoint recomputedFormasPagoAt tampoco avanza (una corrida real
      // posterior vuelve a ver estos mismos links como pendientes, sin nada perdido).
      if (!dryRun && (update.$set || update.$push)) {
        await BankMovement.findByIdAndUpdate(mov._id, update);
      }

      if (estado) {
        detalles.push({
          movementId: mov._id, folio: mov.folio, banco: mov.banco, concepto: mov.concepto,
          fecha: mov.fecha, deposito: mov.deposito,
          saldoErpAntes:   mov.saldoErp ?? null,
          saldoErpDespues: update.$set?.saldoErp ?? mov.saldoErp ?? null,
          estado, links: linksDetalle,
        });
      }

      procesados++;
      emitToUser(auth0Sub, 'bank:erp:sync:progress', {
        jobId, procesados, total, actualizados, pendientes: sinDatos, pendientesFolioFiscal, errores,
        pct: Math.round((procesados / total) * 100),
      });
    }

    if (stopped) {
      const result = { procesados, total, actualizados, pendientes: sinDatos, pendientesFolioFiscal, errores, dryRun };
      SYNC_JOBS.set(jobId, { status: 'stopped', auth0Sub, result, detalles, kind: 'recompute', dryRun });
      emitToUser(auth0Sub, 'bank:erp:sync:stopped', { jobId, ...result });
    } else {
      const result = { total, actualizados, pendientes: sinDatos, pendientesFolioFiscal, errores, dryRun };
      SYNC_JOBS.set(jobId, { status: 'done', auth0Sub, result, detalles, kind: 'recompute', dryRun });
      emitToUser(auth0Sub, 'bank:erp:sync:done', { jobId, ...result });
    }
  } catch (err) {
    const error = err.message || 'Error al recalcular saldo ERP';
    SYNC_JOBS.set(jobId, { status: 'error', auth0Sub, error, kind: 'recompute' });
    emitToUser(auth0Sub, 'bank:erp:sync:error', { jobId, error });
  } finally {
    syncRunning      = false;
    syncCurrentJobId = null;
    setTimeout(() => SYNC_JOBS.delete(jobId), SYNC_JOB_TTL);
  }
}

// ── Reporte Excel del job "Recalcular saldo ERP" ──────────────────────────────────────────
// 5 hojas: Saldo actualizado (cambió saldoErp/status) · Backfill sin cambio (checkpoint
// avanzó, snapshot refrescado, aporte igual o vínculo de motor) · Folio fiscal pendiente
// (checkpoint NO avanzó, se reintenta en la próxima corrida) · Sin datos en Kore (se
// reintenta en la próxima corrida) · Errores.
function _generarExcelRecomputeErpKore(detalles) {
  const wb = new ExcelJS.Workbook();
  wb.creator = 'Numo — Recalcular saldo ERP';
  wb.created = new Date();

  const formatFecha = _xlsxFormatFecha;
  const styleHeader = _xlsxStyleHeader;
  const foliosCxc   = d => d.links.map(l => `${l.serie ?? ''}${l.folioExterno ?? ''}`).join(', ');

  const baseCols = [
    { header: 'Movimiento (folio)', key: 'folio',    width: 14 },
    { header: 'Banco',              key: 'banco',    width: 14 },
    { header: 'Fecha',              key: 'fecha',    width: 12 },
    { header: 'Depósito',           key: 'deposito', width: 14 },
  ];

  // ── Hoja 1: Saldo actualizado ─────────────────────────────────────────────
  const wsAct = wb.addWorksheet('Saldo actualizado');
  wsAct.columns = [
    ...baseCols,
    { header: 'Saldo ERP antes',   key: 'antes',   width: 16 },
    { header: 'Saldo ERP después', key: 'despues', width: 16 },
    { header: 'Diferencia',        key: 'diff',    width: 14 },
    { header: 'CxC afectadas',     key: 'cxc',     width: 30 },
    { header: 'Movimiento ID',     key: 'id',      width: 28 },
  ];
  styleHeader(wsAct);
  for (const d of detalles.filter(d => d.estado === 'actualizado')) {
    const antes = d.saldoErpAntes ?? 0, despues = d.saldoErpDespues ?? 0;
    const row = wsAct.addRow({
      folio: d.folio ?? '', banco: d.banco ?? '', fecha: formatFecha(d.fecha), deposito: d.deposito,
      antes: d.saldoErpAntes, despues: d.saldoErpDespues, diff: despues - antes,
      cxc: foliosCxc(d), id: String(d.movementId),
    });
    row.eachCell(cell => { cell.fill = XLSX_OK_FILL; });
  }
  ['deposito', 'antes', 'despues', 'diff'].forEach(k => { wsAct.getColumn(k).numFmt = '#,##0.00'; });
  if (wsAct.lastColumn) wsAct.autoFilter = { from: 'A1', to: wsAct.lastColumn.letter + '1' };

  // ── Hoja 2: Backfill sin cambio de saldo ──────────────────────────────────
  const wsBk = wb.addWorksheet('Backfill sin cambio');
  wsBk.columns = [
    ...baseCols,
    { header: 'Saldo ERP',      key: 'saldo', width: 16 },
    { header: 'CxC refrescadas', key: 'cxc',  width: 30 },
    { header: 'Movimiento ID',  key: 'id',    width: 28 },
  ];
  styleHeader(wsBk);
  for (const d of detalles.filter(d => d.estado === 'backfill')) {
    const row = wsBk.addRow({
      folio: d.folio ?? '', banco: d.banco ?? '', fecha: formatFecha(d.fecha), deposito: d.deposito,
      saldo: d.saldoErpAntes, cxc: foliosCxc(d), id: String(d.movementId),
    });
    row.eachCell(cell => { cell.fill = XLSX_WARN_FILL; });
  }
  wsBk.getColumn('saldo').numFmt = '#,##0.00';

  // ── Hoja 3: Folio fiscal pendiente (checkpoint NO avanzó, se reintenta) ──
  const wsFf = wb.addWorksheet('Folio fiscal pendiente');
  wsFf.columns = [
    ...baseCols,
    { header: 'CxC sin folio fiscal', key: 'cxc', width: 30 },
    { header: 'Movimiento ID',        key: 'id',  width: 28 },
  ];
  styleHeader(wsFf);
  for (const d of detalles.filter(d => d.estado === 'folio_fiscal_pendiente')) {
    const row = wsFf.addRow({
      folio: d.folio ?? '', banco: d.banco ?? '', fecha: formatFecha(d.fecha), deposito: d.deposito,
      cxc: foliosCxc(d), id: String(d.movementId),
    });
    row.eachCell(cell => { cell.fill = XLSX_WARN_FILL; });
  }

  // ── Hoja 4: Sin datos en Kore (se reintenta en la próxima corrida) ────────
  const wsSd = wb.addWorksheet('Sin datos en Kore');
  wsSd.columns = [
    ...baseCols,
    { header: 'CxC sin respuesta', key: 'cxc', width: 30 },
    { header: 'Movimiento ID',     key: 'id',  width: 28 },
  ];
  styleHeader(wsSd);
  for (const d of detalles.filter(d => d.estado === 'sin_datos')) {
    const sinDatosLinks = d.links.filter(l => l.estado === 'sin_datos');
    const row = wsSd.addRow({
      folio: d.folio ?? '', banco: d.banco ?? '', fecha: formatFecha(d.fecha), deposito: d.deposito,
      cxc: sinDatosLinks.map(l => `${l.serie ?? ''}${l.folioExterno ?? ''}`).join(', '),
      id: String(d.movementId),
    });
    row.eachCell(cell => { cell.fill = XLSX_WARN_FILL; });
  }

  // ── Hoja 5: Errores ────────────────────────────────────────────────────────
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
    row.eachCell(cell => { cell.fill = XLSX_ERR_FILL; });
  }

  return wb.xlsx.writeBuffer();
}

// POST /api/erp/erp-links/:erpId/refrescar — refresca UNA sola CxC contra Kore bajo
// demanda (fuera del ciclo de sync/recompute), pensado para llamarse desde el frontend
// justo antes de abrir "Vincular CxC del ERP"/"Aplicar cobro" — así el usuario siempre ve
// el dato más fresco en el momento que lo necesita, sin esperar al cron de las 7am ni al
// botón masivo "Recalcular saldo ERP" (fix 2026-07-28, folio 036789: "Aplicar cobro"
// mostraba pantalla en blanco porque erpLinks[].saldoActual quedaba congelado desde la
// vinculación y nunca se refrescaba hasta la próxima corrida grande). Reutiliza EXACTAMENTE
// los mismos helpers que _syncErpKoreJob/_recomputeErpKoreJob sobre un solo link — no toca
// conciliacionFinalizadaAt/recomputedFormasPagoAt/conciliacionRunId, esos checkpoints siguen
// siendo exclusivos de los jobs grandes. Solo `authenticate` (sin `permit`), igual que
// /cuentas-pendientes — es una lectura/refresco, no una acción privilegiada por sí sola.
router.post('/erp-links/:erpId/refrescar', authenticate, asyncHandler(async (req, res) => {
  const { erpId }      = req.params;
  const { movementId } = req.body;
  if (!movementId) return res.status(400).json({ error: 'Se requiere movementId.' });

  const mov = await BankMovement.findOne({ _id: movementId, 'erpLinks.erpId': erpId });
  if (!mov) return res.status(404).json({ error: 'No se encontró el vínculo ERP indicado en este movimiento.' });

  const link = mov.erpLinks.find(l => l.erpId === erpId);
  if (!link.serie || !link.folioExterno) {
    return res.status(400).json({ error: 'Este vínculo no tiene serie/folioExterno para consultar Kore.' });
  }

  const rango = _rangoDesdeFollo(link.folioExterno);
  if (!rango) return res.status(400).json({ error: 'No se pudo determinar el rango de fecha para este folio.' });

  let raw;
  try {
    ({ raw } = await _sincronizarConRetry({
      serieExterna: link.serie, folioExterno: String(link.folioExterno),
      fechaDesde:   rango.fechaDesde, fechaHasta: rango.fechaHasta,
    }));
  } catch (err) {
    return res.status(502).json({ error: err.message || 'Error al consultar Kore.' });
  }

  const raw0 = raw[0];
  if (!raw0) return res.status(404).json({ error: 'Kore no devolvió datos para esta CxC en el rango esperado.' });

  const esHumano = _erpIdIdentificadoPorHumano(mov.identificadoPor, erpId);
  let aporte = link.saldoErpAportado ?? null;
  if (esHumano) {
    const calculado = _montoSaldoLinkPorMovimiento(raw0, mov);
    if (calculado != null) aporte = calculado;
  } else {
    const calculado = _montoSaldoLinkPorAutorizacion(raw0, mov.numeroAutorizacion);
    if (calculado != null) aporte = calculado;
  }

  const backfill  = _backfillFormasPagoYFolioFiscal(link, raw0, mov, esHumano, aporte);
  const retencion = _retencionVigente(raw0);

  link.movimientosKore  = _movimientosKoreDesde(raw0);
  link.saldoErpAportado = aporte;
  link.saldoPagadoTotal = backfill.saldoPagadoTotal;
  link.saldoPagado      = backfill.saldoPagado;
  link.folioFiscal      = backfill.folioFiscal;
  link.tieneRetencion   = retencion.tieneRetencion;
  link.montoRetenido    = retencion.montoRetenido;
  link.saldoActual      = raw0.saldoActual ?? link.saldoActual ?? null;

  await mov.save();

  res.json({
    ok: true,
    erpId,
    link: {
      erpId:                link.erpId,
      saldoActual:          link.saldoActual,
      saldoPagado:          link.saldoPagado,
      saldoPagadoTotal:     link.saldoPagadoTotal,
      total:                link.total,
      folioFiscal:          link.folioFiscal,
      serie:                link.serie,
      folioExterno:         link.folioExterno,
      tieneRetencion:       link.tieneRetencion,
      tipoPago:             link.tipoPago,
      desglosePorFormaPago: link.desglosePorFormaPago,
    },
  });
}));

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

  SYNC_JOBS.set(jobId, { status: 'running', auth0Sub, kind: 'sync' });
  res.status(202).json({ jobId });

  _syncErpKoreJob(auth0Sub, jobId, fechaInicio, fechaFin); // sin await — corre en background
}));

// POST recalcular saldo ERP (backfill unificado) — mismo guard/control que el sync normal,
// mutuamente excluyentes (comparten syncRunning/syncControl, ver _recomputeErpKoreJob).
router.post('/sync-erp-kore/recompute', authenticate, permit('banks:admin'), asyncHandler(async (req, res) => {
  if (syncRunning) {
    return res.status(409).json({ error: 'Ya hay una sincronización ERP-Kore en curso.' });
  }

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

  const dryRun = req.body.dryRun === true;

  syncControl.paused       = false;
  syncControl.stopped      = false;
  syncControl.pauseResolve = null;

  syncRunning = true;
  const jobId    = `erp-recompute-${Date.now()}`;
  const auth0Sub = req.user._id;

  SYNC_JOBS.set(jobId, { status: 'running', auth0Sub, kind: 'recompute', dryRun });
  res.status(202).json({ jobId });

  _recomputeErpKoreJob(auth0Sub, jobId, fechaInicio, fechaFin, dryRun); // sin await — corre en background
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
  // jobId trae el timestamp al final (erp-sync-<ts> / erp-recompute-<ts>) — se extrae para
  // ordenar por recencia real, ya que comparar el string completo ya no sirve desde que hay
  // dos prefijos distintos conviviendo en el mismo historial.
  const jobs = [...SYNC_JOBS.entries()]
    .map(([jobId, job]) => ({
      jobId,
      kind:      job.kind ?? 'sync',
      dryRun:    job.dryRun ?? job.result?.dryRun ?? false,
      status:    job.status,
      result:    job.result ?? null,
      error:     job.error  ?? null,
      hasReport: Array.isArray(job.detalles) && job.detalles.length > 0,
    }))
    .sort((a, b) => Number(b.jobId.match(/(\d+)$/)?.[1] ?? 0) - Number(a.jobId.match(/(\d+)$/)?.[1] ?? 0));
  res.json(jobs);
}));

// ── Reporte Excel del job Sync ERP-Kore ───────────────────────────────────────
// 3 hojas: Actualizados (antes/después + diferencia vs. depósito real) · Pendientes
// (CxC aún no saldada en Kore) · Errores.
// ── Estilos compartidos entre los reportes Excel de ERP-Kore (sync y recompute) ─────────────
const XLSX_HEADER_FILL = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF6D28D9' } };
const XLSX_HEADER_FONT = { bold: true, color: { argb: 'FFFFFFFF' }, size: 10 };
const XLSX_OK_FILL     = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFD1FAE5' } };
const XLSX_WARN_FILL   = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFEF9C3' } };
const XLSX_ERR_FILL    = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFEE2E2' } };

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

function _generarExcelSyncErpKore(detalles) {
  const wb = new ExcelJS.Workbook();
  wb.creator = 'Numo — Sync ERP-Kore';
  wb.created = new Date();

  const formatFecha  = _xlsxFormatFecha;
  const styleHeader  = _xlsxStyleHeader;
  const OK_FILL      = XLSX_OK_FILL;
  const WARN_FILL    = XLSX_WARN_FILL;
  const ERR_FILL     = XLSX_ERR_FILL;

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

  const esRecompute = job.kind === 'recompute';
  const buffer = esRecompute
    ? await _generarExcelRecomputeErpKore(job.detalles)
    : await _generarExcelSyncErpKore(job.detalles);
  const fecha  = new Date().toISOString().slice(0, 10);
  const nombre = esRecompute ? 'recalcular-saldo-erp' : 'sync-erp-kore';
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', `attachment; filename="${nombre}-${fecha}.xlsx"`);
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
                      recomputedFormasPagoAt: null,
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

// Patrón exacto del síntoma (folio 036827 y cualquier otro con el mismo bug): el checkpoint
// de "Recalcular saldo ERP" avanzó (recomputedFormasPagoAt no-null) con folioFiscal todavía
// sin resolver — la regla vieja de _backfillFormasPagoYFolioFiscal daba por perdido el
// folioFiscal apenas saldoActual llegaba a 0 (o, hasta el 2026-07-30, apenas cerraba limpio
// sin retención), sin considerar que Kore podía timbrar el CFDI después. Ya arreglado hacia
// adelante (ver el fix arriba); este filtro identifica solo lo que quedó atrapado con la
// regla vieja — nunca toca links con folioFiscal ya resuelto.
// `folioFiscal: { $in: [null, ''] }` (fix 2026-07-28, folio 034310): Kore puede devolver
// folioFiscal como '' en vez de null/ausente — Mongo NO trata '' como null (a diferencia de
// `??`/`==null` en JS, que además ya se corrigieron en _backfillFormasPagoYFolioFiscal), así
// que un link atrapado con '' quedaba invisible para este filtro y para el botón que lo usa.
// `tieneRetencion` (2026-07-30): se quitó del filtro — hasta ayer solo cubría cierres por
// retención; los cierres limpios (036472/036917/036967/037095/037085/037076/037099) también
// quedan atrapados con folioFiscal null y Kore SÍ termina facturándolos, así que también
// califican para el rescate manual.
// `conciliacionFinalizadaAt` (2026-08-06): se quitó como condición obligatoria — antes el
// rescate masivo solo veía el flujo tradicional. Los links de Solicitudes de Cobro/Aplicar
// cobro manual NUNCA llenan ese campo (ver fix 037600 más abajo) y quedaban invisibles para
// este filtro aunque su checkpoint hubiera avanzado con folioFiscal todavía null — el mismo
// patrón "atrapado", solo que en otro flujo. `recomputedFormasPagoAt != null` + `folioFiscal`
// sin resolver ya identifica el patrón sin importar de qué flujo vino el link.
const _FILTRO_LINK_ATRAPADO = {
  recomputedFormasPagoAt: { $ne: null },
  folioFiscal:            { $in: [null, ''] },
};
// Mismas condiciones, con el prefijo que exige `arrayFilters` (identificador posicional
// $[link] en vez de nombre de campo relativo al array, como pide $elemMatch).
const _ARRAY_FILTRO_LINK_ATRAPADO = Object.fromEntries(
  Object.entries(_FILTRO_LINK_ATRAPADO).map(([k, v]) => [`link.${k}`, v]),
);

// POST rescate manual — libera el checkpoint `recomputedFormasPagoAt` sin llamar a Kore.
// Existe para casos como el folio 036827 (ver _FILTRO_LINK_ATRAPADO arriba). Dos modos:
// - Con `folio` en el body: rescata solo ese movimiento (opcionalmente acotado a `erpId`),
//   sin exigir que cumpla el patrón — rescate dirigido, para un caso puntual ya diagnosticado.
// - Sin `folio`: modo masivo, un solo `updateMany` sobre TODOS los erpLinks que calzan
//   exactamente con el patrón del bug — pensado para correrse UNA VEZ como backfill
//   retroactivo (el fix de arriba ya evita que el patrón vuelva a producirse hacia adelante,
//   así que no hace falta un cron para esto).
// En ambos casos, la próxima corrida de "Recalcular saldo ERP" (o el cron diario) vuelve a
// tomar los links liberados normalmente — este endpoint nunca llama a Kore ni toca saldoErp.
router.post('/sync-erp-kore/reset-recompute', authenticate, permit('banks:admin'), asyncHandler(async (req, res) => {
  const { folio, erpId } = req.body;

  if (!folio) {
    // Rango de fechas OPCIONAL — sin acotar por defecto (mismo comportamiento que antes de
    // este cambio). Permite correr el rescate masivo en tandas por fecha desde el frontend.
    // Mismo patrón de validación que /sync-erp-kore y /sync-erp-kore/recompute.
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

    const filtroFecha = {};
    if (fechaInicio && fechaFin) filtroFecha.fecha = { $gte: fechaInicio, $lte: fechaFin };

    const result = await BankMovement.updateMany(
      { ...filtroFecha, erpLinks: { $elemMatch: _FILTRO_LINK_ATRAPADO } },
      { $set: { 'erpLinks.$[link].recomputedFormasPagoAt': null } },
      { arrayFilters: [_ARRAY_FILTRO_LINK_ATRAPADO] },
    );
    return res.json({
      ok:                     true,
      modo:                   'masivo',
      movimientosAfectados:   result.matchedCount,
      movimientosModificados: result.modifiedCount,
      fechaDesde:             req.body.fechaDesde ?? null,
      fechaHasta:             req.body.fechaHasta ?? null,
    });
  }

  const mov = await BankMovement.findOne({ folio: String(folio) });
  if (!mov) return res.status(404).json({ error: `No existe un movimiento con folio ${folio}` });

  const links = mov.erpLinks ?? [];
  if (erpId && !links.some(l => String(l.erpId) === String(erpId))) {
    return res.status(404).json({ error: 'erpId no encontrado en este movimiento' });
  }

  let reiniciados = 0;
  for (const link of links) {
    if (erpId && String(link.erpId) !== String(erpId)) continue;
    if (link.recomputedFormasPagoAt != null) {
      link.recomputedFormasPagoAt = null;
      reiniciados++;
    }
  }
  if (reiniciados > 0) await mov.save();

  res.json({ ok: true, modo: 'puntual', folio: mov.folio, reiniciados });
}));

// ── Barrido: desvincular CxC cerradas por CANCELACIÓN/DEVOLUCIÓN sin pago real ──
// Pedido del usuario (2026-07-29), tras confirmar con casos reales que estas CxC
// están correctamente en saldoErpAportado:0 pero SIGUEN ocupando el depósito
// bancario como si estuvieran vinculadas: la CxC se cerró en Kore por
// CANCELACIÓN (serieOrigen 'CAC', confirmado con folio 036030) o DEVOLUCIÓN
// (serieOrigen 'DEV', confirmado con folios 026829/028128) — en ambos casos el
// depósito nunca pagó esa CxC de verdad. El link queda "identificado" con una
// CxC que en realidad no cobró nada, mientras el dinero real de ese depósito
// probablemente pagó OTRA CxC que nadie vinculó todavía. Este barrido libera
// el depósito (quita erpId de erpLinks/erpIds/identificadoPor, recalcula
// saldoErp/status con lo que quede) para que el usuario correspondiente pueda
// buscar y vincular la CxC correcta.
//
// Deliberadamente ESTRICTO: "solo CAC y DEV" (pedido explícito — para
// cualquier otro origen no hacer nada todavía). Un link SOLO califica si
// TODOS sus movimientos RET son de origen CAC o DEV — si aparece cualquier
// OTRO origen mezclado en el mismo link (BON, BN, CES, etc.), se excluye por
// completo. Esto protege en particular a los folios YA conocidos del bug 2
// (033439/033764/036170, cierran por BON) — esos se recuperan con
// /reset-recompute + recompute, nunca con este barrido.
function _esLinkPuroCancelacionODevolucion(link) {
  const rets = (link.movimientosKore ?? []).filter(m => m.serie === 'RET');
  if (rets.length === 0) return false;
  return rets.every(m => m.serieOrigen === 'CAC' || m.serieOrigen === 'DEV');
}

// dryRun por defecto true — hay que pedir explícitamente `{dryRun:false}` para
// ejecutar de verdad (mismo criterio cauteloso que "Recalcular saldo ERP").
// Reusa updateErpIds() (el mismo mecanismo que ya usa el botón "Revertir" del
// motor de matching) en vez de reimplementar la desvinculación — identidad
// sintética con role:'admin' porque esto es una corrección administrativa,
// no la acción de un usuario puntual, y necesita poder forzar la
// desvinculación aunque el movimiento ya esté "identificado".
router.post('/sync-erp-kore/desvincular-cancelaciones', authenticate, permit('banks:admin'), asyncHandler(async (req, res) => {
  const dryRun = req.body?.dryRun !== false;

  const candidatos = await BankMovement.find({
    erpLinks: {
      $elemMatch: {
        conciliacionFinalizadaAt: { $ne: null },
        saldoErpAportado:         0,
        'movimientosKore.serie':  'RET',
      },
    },
  }).select('_id folio banco deposito retiro erpLinks');

  const detalle = [];
  for (const mov of candidatos) {
    for (const link of mov.erpLinks ?? []) {
      if (link.conciliacionFinalizadaAt == null) continue;
      if (link.saldoErpAportado !== 0) continue;
      if (!_esLinkPuroCancelacionODevolucion(link)) continue;

      const origenes = [...new Set(
        (link.movimientosKore ?? []).filter(m => m.serie === 'RET').map(m => m.serieOrigen),
      )];

      detalle.push({
        movimientoId: mov._id.toString(), folio: mov.folio, banco: mov.banco,
        deposito: mov.deposito, retiro: mov.retiro,
        erpId: link.erpId, folioExterno: link.folioExterno, origenes,
      });

      if (!dryRun) {
        await updateErpIds(mov._id, 'remove', link.erpId, { _id: 'erp-barrido-cac-dev', role: 'admin' });
      }
    }
  }

  res.json({
    ok:             true,
    dryRun,
    encontrados:    detalle.length,
    desvinculados:  dryRun ? 0 : detalle.length,
    detalle,
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
  const syncJobId = `erp-sync-${Date.now()}`;
  SYNC_JOBS.set(syncJobId, { status: 'running', auth0Sub, kind: 'sync' });
  await _syncErpKoreJob(auth0Sub, syncJobId, null, null);
  const syncJob = SYNC_JOBS.get(syncJobId);
  console.log(`[CronErpSync] Sync ERP-Kore automático completado — status=${syncJob?.status}.`);

  // Encadenar el backfill (saldoPagado/saldoPagadoTotal/folioFiscal, 2026-07-24) SOLO si el
  // sync principal terminó limpio — mismo criterio que el diseño de dos-jobs pre-2026-07-09
  // (comentario histórico arriba de este archivo): si el sync se detuvo o falló, no tiene
  // sentido encadenar un segundo paso sobre datos a medio procesar. Automatiza lo que antes
  // dependía de que un admin corriera a mano el botón "Recalcular saldo ERP".
  let recomputeJobId = null;
  let recomputeJob   = null;
  if (syncJob?.status === 'done') {
    console.log('[CronErpSync] Sync OK — iniciando backfill automático (Recalcular saldo ERP)...');
    syncControl.paused       = false;
    syncControl.stopped      = false;
    syncControl.pauseResolve = null;
    syncRunning = true;
    recomputeJobId = `erp-recompute-${Date.now()}`;
    SYNC_JOBS.set(recomputeJobId, { status: 'running', auth0Sub, kind: 'recompute', dryRun: false });
    await _recomputeErpKoreJob(auth0Sub, recomputeJobId, null, null, false);
    recomputeJob = SYNC_JOBS.get(recomputeJobId);
  } else {
    console.warn(`[CronErpSync] Sync terminó en status "${syncJob?.status}" — se omite el backfill automático de hoy.`);
  }

  // Resumen estructurado de una sola línea, con tag fijo greppable en logs/app.log — permite
  // confirmar que la corrida automática de hoy (sync + backfill encadenado) corrió bien sin
  // tener que rearmar el contexto de líneas sueltas. `grep '\[CronErpSync\]\[RESUMEN\]'`.
  const ok = syncJob?.status === 'done' && (recomputeJobId === null || recomputeJob?.status === 'done');
  const resumen = {
    fecha: new Date().toISOString(),
    sync:      { jobId: syncJobId, status: syncJob?.status ?? null, result: syncJob?.result ?? null },
    recompute: recomputeJobId
      ? { jobId: recomputeJobId, status: recomputeJob?.status ?? null, result: recomputeJob?.result ?? null }
      : null,
  };
  console.log(`[CronErpSync][RESUMEN][${ok ? 'OK' : 'REVISAR'}] ${JSON.stringify(resumen)}`);
}

router.runErpSyncAutomatico = runErpSyncAutomatico;

// obtenerSesionCaja/aplicarCobroOperacion(Multiple)/obtenerCuentasKore/KoreCajaError
// ya NO se re-exportan aquí — collection-request.service.js las importa
// directamente de ./kore-caja.service (ver kore-caja.service.js).

// Helpers re-expuestos para scripts de backfill one-off (ver banks/scripts/) que
// necesitan reconsultar Kore con el MISMO criterio de ventana/reintento que usa
// el job real, sin duplicar esa lógica y arriesgar que se desincronice con el tiempo.
router._rangoDesdeFollo             = _rangoDesdeFollo;
router._rangoSpilloverSiguienteMes  = _rangoSpilloverSiguienteMes;
router._sincronizarConRetry         = _sincronizarConRetry;
router._movimientosKoreDesde        = _movimientosKoreDesde;
router._montoSaldoLink              = _montoSaldoLink;
router._montoSaldoLinkPorMovimiento = _montoSaldoLinkPorMovimiento;
router._montoSaldoLinkPorAutorizacion = _montoSaldoLinkPorAutorizacion;
router._aporteConRatchet            = _aporteConRatchet;
router._FILTRO_LINK_ATRAPADO        = _FILTRO_LINK_ATRAPADO;
router._retencionVigente            = _retencionVigente;
router._erpIdIdentificadoPorHumano  = _erpIdIdentificadoPorHumano;
router._esLinkPuroCancelacionODevolucion = _esLinkPuroCancelacionODevolucion;
router._backfillFormasPagoYFolioFiscal   = _backfillFormasPagoYFolioFiscal;
router._folioFiscalDentroDeVentanaReintento = _folioFiscalDentroDeVentanaReintento;
router._resolverCuentaDesdeCfdiLiquidado    = _resolverCuentaDesdeCfdiLiquidado;
router.SYNC_DELAY_MS                = SYNC_DELAY_MS;

module.exports = router;
