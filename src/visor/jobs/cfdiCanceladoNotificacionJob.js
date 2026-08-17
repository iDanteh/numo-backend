'use strict';

/**
 * visor/jobs/cfdiCanceladoNotificacionJob.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Bandeja de notificaciones: avisa cuando un CFDI que YA tiene una póliza
 * ACTIVA (estado != 'cancelada') aparece como Cancelado en el SAT — sin
 * importar si esa cancelación trajo un sustituto (tipoRelacion='04', ver
 * cfdi-poliza-generator.service.js) o no; cualquier cancelación posterior a
 * la contabilización es motivo de alerta para revisión manual.
 *
 * Corre periódico (no en vivo) — confirmado con el usuario 2026-08-13. No
 * depende de que el usuario tenga la app abierta: cada corrida cruza TODOS
 * los CFDIs de pólizas activas contra su satStatus actual en Mongo, e
 * inserta una Notificacion nueva por cada par (póliza, CFDI) recién
 * detectado como cancelado — el índice único (tipo, poliza_id, cfdi_uuid)
 * en Notificacion evita duplicar la misma alerta en corridas siguientes
 * mientras siga sin resolverse.
 */

const cron = require('node-cron');
const { logger } = require('../../shared/utils/logger');
const CFDI = require('../models/CFDI');
const { Poliza, Notificacion } = require('../../shared/models/postgres');
const { QueryTypes } = require('sequelize');

const TIPO = 'cfdi_cancelado_con_poliza';
const LOTE_MONGO = 1000;

/**
 * Cruza CFDIs de pólizas activas contra su satStatus actual y crea las
 * notificaciones nuevas. Devuelve cuántas creó (para el log/uso manual).
 */
async function detectarCfdisCanceladosConPoliza() {
  // 1. Pares (cfdiUuid, polizaId) distintos de TODAS las pólizas activas —
  // mismo cruce que ya hace poliza.repository.js para el chip visual de la
  // lista, pero sin el tope de 400 (esto corre en background, no bloquea
  // una request de usuario).
  const pares = await Poliza.sequelize.query(`
    SELECT DISTINCT pm.cfdi_uuid AS "cfdiUuid", pm.poliza_id AS "polizaId"
    FROM poliza_movimientos pm
    JOIN polizas p ON pm.poliza_id = p.id
    WHERE p.estado != 'cancelada' AND pm.cfdi_uuid IS NOT NULL
  `, { type: QueryTypes.SELECT });

  if (!pares.length) return 0;

  const uuidsUnicos = [...new Set(pares.map(p => p.cfdiUuid.toUpperCase()))];

  // 2. De esos, cuáles están Cancelado hoy en el SAT — en lotes (Mongo $in
  // soporta miles sin problema, el lote es solo para no armar un array gigante
  // de golpe si algún día esto crece mucho).
  const uuidsCancelados = new Map(); // uuid(upper) → { serie, folio }
  for (let i = 0; i < uuidsUnicos.length; i += LOTE_MONGO) {
    const lote = uuidsUnicos.slice(i, i + LOTE_MONGO);
    const cancelados = await CFDI.find({ uuid: { $in: lote }, satStatus: 'Cancelado' })
      .select('uuid serie folio tipoDeComprobante')
      .lean();
    for (const c of cancelados) {
      uuidsCancelados.set(c.uuid.toUpperCase(), { serie: c.serie, folio: c.folio, tipo: c.tipoDeComprobante });
    }
  }

  if (!uuidsCancelados.size) return 0;

  // 3. Info de póliza (folio/concepto) para el mensaje — solo de las que
  // realmente tienen algún CFDI cancelado, no todas las activas.
  const polizaIdsConCancelado = [...new Set(
    pares.filter(p => uuidsCancelados.has(p.cfdiUuid.toUpperCase())).map(p => p.polizaId),
  )];
  const polizas = await Poliza.findAll({
    where: { id: polizaIdsConCancelado },
    attributes: ['id', 'numero', 'tipo', 'concepto'],
    raw: true,
  });
  const polizaPorId = new Map(polizas.map(p => [p.id, p]));

  // 4. Crear notificación por cada par (poliza, cfdi cancelado) que no exista
  // todavía — findOrCreate respeta el índice único, no duplica en corridas futuras.
  let creadas = 0;
  for (const par of pares) {
    const uuidUpper = par.cfdiUuid.toUpperCase();
    const info = uuidsCancelados.get(uuidUpper);
    if (!info) continue;
    const poliza = polizaPorId.get(par.polizaId);
    if (!poliza) continue;

    const serieFolio = [info.serie, info.folio].filter(Boolean).join('-') || uuidUpper.slice(0, 8);
    const [, created] = await Notificacion.findOrCreate({
      where: { tipo: TIPO, polizaId: par.polizaId, cfdiUuid: par.cfdiUuid },
      defaults: {
        tipo:     TIPO,
        polizaId: par.polizaId,
        cfdiUuid: par.cfdiUuid,
        titulo:   `CFDI cancelado en el SAT — póliza ${poliza.tipo}-${poliza.numero}`,
        mensaje:  `El CFDI ${serieFolio} (${uuidUpper}) aparece como Cancelado en el SAT, pero ya está contabilizado en la póliza "${poliza.concepto}" (${poliza.tipo}-${poliza.numero}). Revisa si necesita reversión o ajuste manual.`,
      },
    });
    if (created) creadas++;
  }

  return creadas;
}

cron.schedule('30 * * * *', async () => {
  try {
    const creadas = await detectarCfdisCanceladosConPoliza();
    if (creadas > 0) logger.info(`[cfdiCanceladoNotificacionJob] ${creadas} notificación(es) nueva(s) de CFDI cancelado con póliza activa`);
  } catch (err) {
    logger.error(`[cfdiCanceladoNotificacionJob] Error: ${err.message}`);
  }
}, { timezone: 'America/Mexico_City' });

module.exports = { detectarCfdisCanceladosConPoliza };
