'use strict';

const BankMovement = require('../banks/BankMovement.model');
const ErpReversion  = require('./ErpReversion.model');
const { aplicarLogicaErp }        = require('../banks/bank.service');
const { emitToBanco }             = require('../../shared/socket');
const { NotFoundError, ConflictError } = require('../../shared/errors/AppError');

// Mismo tamaño de página que ya usa el resto del módulo ERP (ver ERP_PAGE_SIZE en erp.routes.js).
const ERP_REVERSION_PAGE_SIZE = 50;

// Clona un subdocumento/entrada de Mongoose (o un objeto plano) para guardar un snapshot
// que no cambie si el original se sigue mutando después. `.toObject()` cuando es un
// subdocumento Mongoose real; JSON.parse(JSON.stringify(...)) para objetos planos (tests).
function _snapshot(value) {
  if (value == null) return null;
  return typeof value.toObject === 'function' ? value.toObject() : JSON.parse(JSON.stringify(value));
}

// Quita el erpId de UN movimiento bancario: guarda snapshots de lo que había (para poder
// revertir la operación después), limpia erpLinks/erpIds/identificadoPor, recalcula
// saldoErp/uuidXML/status con la misma lógica pura que usa el flujo manual (aplicarLogicaErp,
// bank.service.js) y emite el mismo shape que ya emite setErpIds.
//
// NO se usa setErpIds aquí: esa función asume un `user` de Auth0 para decidir permisos de
// ownership (quién puede desvincular qué) que no aplican a un webhook server-to-server sin
// sesión, y además su limpieza de `identificadoPor` SIEMPRE quita las entradas
// 'erp-auto' sin importar qué se esté agregando/quitando — correcto para el flujo humano
// (ceder ownership al humano que vincula a mano), pero no para esto: acá no hay ningún
// humano tomando posesión, así que la limpieza debe ser puntual, solo la entrada del erpId
// que se está removiendo.
async function _removerErpIdDeMovimiento(mov, erpId, { serieExterna, folioExterno } = {}) {
  const link = (mov.erpLinks ?? []).find(l => l.erpId === erpId) ?? null;
  const erpLinkRemovido = _snapshot(link);

  // Mismatch: Kore mandó su propia serie/folio Y el link vinculado tenía su propia
  // serie/folio guardados Y no coinciden — señal de que quizás se está desvinculando la
  // CxC equivocada (o que Kore reusó el erpId). Se reporta, no bloquea la reversión.
  let mismatch = false;
  if (link) {
    if (serieExterna != null && link.serie != null && String(link.serie) !== String(serieExterna)) {
      mismatch = true;
    }
    if (folioExterno != null && link.folioExterno != null && String(link.folioExterno) !== String(folioExterno)) {
      mismatch = true;
    }
  }

  const identificadoPorEntry = (mov.identificadoPor ?? []).find(e => e.erpId === erpId) ?? null;
  const identificadoPorRemovido = _snapshot(identificadoPorEntry);

  mov.erpLinks        = (mov.erpLinks ?? []).filter(l => l.erpId !== erpId);
  mov.erpIds          = (mov.erpIds ?? []).filter(id => id !== erpId);
  mov.identificadoPor = (mov.identificadoPor ?? []).filter(e => e.erpId !== erpId);

  const { saldoErp, uuidXML, status } = aplicarLogicaErp(mov);
  mov.saldoErp = saldoErp;
  mov.uuidXML  = uuidXML;
  mov.status   = status;

  await mov.save();

  emitToBanco(mov.banco, 'bank:movement:updated', {
    _id:             mov._id,
    banco:           mov.banco,
    erpIds:          mov.erpIds,
    erpLinks:        mov.erpLinks,
    saldoErp:        mov.saldoErp,
    uuidXML:         mov.uuidXML,
    status:          mov.status,
    identificadoPor: mov.identificadoPor,
  });

  return { movementId: mov._id, erpLinkRemovido, identificadoPorRemovido, mismatch };
}

// Punto de entrada del webhook: Kore avisó que revirtió/canceló una CxC. Busca todos los
// movimientos bancarios que la tengan vinculada (puede haber 0, 1 o varios), la desvincula
// de cada uno y deja un rastro de auditoría (ErpReversion) que permite deshacer la operación
// si fue un error.
async function procesarReversionKore({ erpId, motivo, fecha, serieExterna, folioExterno, referencia, payloadOriginal } = {}) {
  const movs = await BankMovement.find({ erpIds: erpId });

  if (movs.length === 0) {
    // Kore avisó una reversión que ya no encontramos vinculada a ningún movimiento — ya sea
    // porque nunca se vinculó de este lado, o porque este mismo evento (o un reintento suyo)
    // ya se procesó antes. No se persiste ningún ErpReversion: no hay nada que auditar ni que
    // el botón "Revertir" pudiera restaurar (movimientosAfectados quedaría vacío igual), y
    // guardar un registro por cada llamada repetida inflaba la bandeja con filas idénticas sin
    // información nueva (detectado 2026-08-10, probando el endpoint varias veces seguidas).
    // La respuesta sigue siendo 200/yaEstabaDesvinculada:true — el contrato con Kore no cambia.
    return { reversionId: null, movimientosAfectados: 0, yaEstabaDesvinculada: true };
  }

  const resultados = [];
  for (const mov of movs) {
    resultados.push(await _removerErpIdDeMovimiento(mov, erpId, { serieExterna, folioExterno }));
  }

  const serieFolioMismatch = resultados.some(r => r.mismatch);

  const reversion = await ErpReversion.create({
    erpId,
    motivo:             motivo ?? null,
    fechaKore:          fecha ?? null,
    serieExterna:       serieExterna ?? null,
    folioExterno:       folioExterno ?? null,
    referencia:         referencia ?? null,
    serieFolioMismatch,
    payloadOriginal:    payloadOriginal ?? null,
    movimientosAfectados: resultados.map(r => ({
      movementId:              r.movementId,
      erpLinkRemovido:         r.erpLinkRemovido,
      identificadoPorRemovido: r.identificadoPorRemovido,
    })),
  });

  return { reversionId: reversion._id, movimientosAfectados: movs.length, yaEstabaDesvinculada: false };
}

// Deshace una reversión aplicada por error: restaura el snapshot de cada movimiento
// afectado. NO le avisa nada a Kore — es una corrección puramente de nuestro lado.
// Si un movimiento ya no existe, o si alguien ya volvió a vincular manualmente la misma
// CxC mientras tanto, ese movimiento en particular se salta (no aborta toda la operación)
// y se reporta en `noRestaurados`.
async function revertirReversion(reversionId, user) {
  const reversion = await ErpReversion.findById(reversionId);
  if (!reversion) throw new NotFoundError('Reversión');
  if (reversion.estado === 'revertida') {
    throw new ConflictError('Esta reversión ya fue deshecha.');
  }

  const restaurados   = [];
  const noRestaurados = [];

  for (const entry of reversion.movimientosAfectados) {
    if (!entry.erpLinkRemovido) {
      noRestaurados.push({ movementId: entry.movementId, motivo: 'No había un snapshot que restaurar.' });
      continue;
    }

    const mov = await BankMovement.findById(entry.movementId);
    if (!mov) {
      noRestaurados.push({ movementId: entry.movementId, motivo: 'El movimiento ya no existe.' });
      continue;
    }

    const erpId = entry.erpLinkRemovido.erpId;
    const yaVinculada = (mov.erpLinks ?? []).some(l => l.erpId === erpId);
    if (yaVinculada) {
      noRestaurados.push({ movementId: entry.movementId, motivo: 'La CxC ya fue vinculada nuevamente de forma manual.' });
      continue;
    }

    mov.erpLinks = [...(mov.erpLinks ?? []), entry.erpLinkRemovido];
    mov.erpIds   = [...(mov.erpIds ?? []), erpId];
    if (entry.identificadoPorRemovido) {
      mov.identificadoPor = [...(mov.identificadoPor ?? []), entry.identificadoPorRemovido];
    }

    const { saldoErp, uuidXML, status } = aplicarLogicaErp(mov);
    mov.saldoErp = saldoErp;
    mov.uuidXML  = uuidXML;
    mov.status   = status;
    await mov.save();

    emitToBanco(mov.banco, 'bank:movement:updated', {
      _id:             mov._id,
      banco:           mov.banco,
      erpIds:          mov.erpIds,
      erpLinks:        mov.erpLinks,
      saldoErp:        mov.saldoErp,
      uuidXML:         mov.uuidXML,
      status:          mov.status,
      identificadoPor: mov.identificadoPor,
    });

    restaurados.push(entry.movementId);
  }

  reversion.estado       = 'revertida';
  reversion.revertidoPor = user?.email ?? user?._id ?? null;
  reversion.revertidoEn  = new Date();
  await reversion.save();

  return { reversion, restaurados, noRestaurados };
}

// Listado paginado para la UI (bandeja de reversiones aplicadas por Kore).
async function listarReversiones({ page = 1, estado, q } = {}) {
  const filtro = {};
  if (estado) filtro.estado = estado;

  const texto = (q ?? '').toString().trim();
  if (texto) {
    const escapeRegex = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const regex = new RegExp(escapeRegex(texto), 'i');
    filtro.$or = [
      { erpId:        regex },
      { serieExterna: regex },
      { folioExterno: regex },
    ];
  }

  const pageNum = Math.max(1, parseInt(page, 10) || 1);
  const skip    = (pageNum - 1) * ERP_REVERSION_PAGE_SIZE;

  const [data, total] = await Promise.all([
    ErpReversion.find(filtro).sort({ createdAt: -1 }).skip(skip).limit(ERP_REVERSION_PAGE_SIZE),
    ErpReversion.countDocuments(filtro),
  ]);

  return {
    data,
    pagination: {
      page:        pageNum,
      totalPaginas: Math.max(1, Math.ceil(total / ERP_REVERSION_PAGE_SIZE)),
      total,
    },
  };
}

module.exports = { procesarReversionKore, revertirReversion, listarReversiones };
