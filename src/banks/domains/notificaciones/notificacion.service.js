'use strict';

const { Notificacion } = require('../../../shared/models/postgres');

const LIMITE_DEFAULT = 30;

// Mismo criterio que poliza.service.js — nombre visible del usuario que
// marcó/resolvió la notificación.
function userLabel(user) {
  return user?.nombre || user?.email || String(user?.dbId ?? 'sistema');
}

/**
 * Lista las notificaciones NO RESUELTAS más recientes (no leídas primero) +
 * el conteo de no leídas — confirmado con el usuario 2026-08-13: `leida` y
 * `resuelta` son cosas distintas. Ver la notificación (clic) solo la marca
 * `leida` — sigue en la bandeja, solo deja de sumar al badge. Únicamente
 * desaparece de la bandeja cuando alguien la marca `resuelta` explícitamente
 * (el problema real ya se atendió).
 */
async function list({ limit } = {}) {
  const lim = Math.min(Number(limit) || LIMITE_DEFAULT, 100);
  const [items, noLeidas] = await Promise.all([
    Notificacion.findAll({
      where: { resuelta: false },
      order: [['leida', 'ASC'], ['createdAt', 'DESC']],
      limit: lim,
    }),
    Notificacion.count({ where: { leida: false, resuelta: false } }),
  ]);
  return { items, noLeidas };
}

async function marcarLeida(id, user) {
  const notif = await Notificacion.findByPk(id);
  if (!notif) return null;
  if (!notif.leida) {
    notif.leida = true;
    notif.leidaPor = userLabel(user);
    notif.leidaAt = new Date();
    await notif.save();
  }
  return notif;
}

async function marcarTodasLeidas(user) {
  const [count] = await Notificacion.update(
    { leida: true, leidaPor: userLabel(user), leidaAt: new Date() },
    { where: { leida: false, resuelta: false } },
  );
  return { actualizadas: count };
}

async function marcarResuelta(id, user) {
  const notif = await Notificacion.findByPk(id);
  if (!notif) return null;
  if (!notif.resuelta) {
    notif.resuelta = true;
    notif.resueltaPor = userLabel(user);
    notif.resueltaAt = new Date();
    // Resolver implica haberla visto — evita el caso raro de una
    // notificación "resuelta" pero que siga contando como no leída.
    if (!notif.leida) { notif.leida = true; notif.leidaPor = userLabel(user); notif.leidaAt = new Date(); }
    await notif.save();
  }
  return notif;
}

module.exports = { list, marcarLeida, marcarTodasLeidas, marcarResuelta };
