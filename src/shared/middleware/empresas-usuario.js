'use strict';

/**
 * Empresas fijas del usuario (User.empresaRfcs, pantalla Roles) aplicadas en el
 * backend del visor (2026-10-14). Antes solo se usaban para el selector de
 * entidades: cualquier usuario podía pedir otro RFC a mano y recibir los CFDIs
 * de una empresa que no tiene asignada. Sin empresas asignadas ([]) no cambia
 * nada — puede consultar cualquier empresa, igual que antes.
 */

const CAMPOS_RFC = ['rfc', 'rfcEmisor', 'rfcReceptor'];

/** RFCs a los que está limitado el usuario, o null si no tiene restricción. */
function empresasPermitidas(req) {
  const rfcs = (req.user?.empresaRfcs ?? []).map(r => String(r).trim().toUpperCase()).filter(Boolean);
  return rfcs.length ? rfcs : null;
}

/**
 * Para usuarios con empresas asignadas:
 *  - `campos`: los parámetros con los que la ruta filtra la empresa (ej.
 *    rfcEmisor en emitidos, rfcReceptor en recibidos). Cada uno que venga debe
 *    ser de sus empresas → si no, 403.
 *  - Sin `campos` (el controlador filtra por su cuenta): si trae RFC (rfc /
 *    rfcEmisor / rfcReceptor), al menos uno debe ser de sus empresas; el otro
 *    puede ser el filtro de proveedor/cliente.
 *  - `inyectar`: si no trae ninguno de esos campos, se llenan con su empresa
 *    (solo si tiene una; con varias → 400, debe elegir una), para que el
 *    reporte no caiga en "todas las entidades". Sin `campos`, son los mismos.
 * Deja `req.empresasPermitidas` (array o null) para los controladores.
 */
const limitarAEmpresasDelUsuario = ({ campos = null, inyectar = [] } = {}) => (req, res, next) => {
  const permitidas = empresasPermitidas(req);
  req.empresasPermitidas = permitidas;
  if (!permitidas) return next();

  const obligatorios = campos ?? (inyectar.length ? inyectar : null);
  const enviados = {};
  for (const campo of new Set([...CAMPOS_RFC, ...(obligatorios ?? [])])) {
    const valor = req.query[campo];
    if (valor === undefined || valor === '') continue;
    if (typeof valor !== 'string') return res.status(400).json({ error: `Parámetro ${campo} inválido` });
    enviados[campo] = valor.trim().toUpperCase();
  }
  const sinAcceso = () => res.status(403).json({ error: 'No tienes acceso a esa empresa.' });

  if (obligatorios) {
    const propios = obligatorios.filter(c => enviados[c]);
    if (propios.some(c => !permitidas.includes(enviados[c]))) return sinAcceso();
    if (propios.length) return next();
  } else {
    const valores = Object.values(enviados);
    if (valores.length && !valores.some(r => permitidas.includes(r))) return sinAcceso();
    return next();
  }

  if (inyectar.length) {
    if (permitidas.length !== 1) return res.status(400).json({ error: 'Selecciona una empresa.' });
    for (const campo of inyectar) req.query[campo] = permitidas[0];
  }
  return next();
};

/** Rutas que mezclan todas las empresas y no tienen filtro: fuera para usuarios con empresas asignadas. */
const soloSinEmpresasAsignadas = (req, res, next) => (
  empresasPermitidas(req)
    ? res.status(403).json({ error: 'No disponible para usuarios con empresas asignadas.' })
    : next()
);

/** true si el CFDI (emisor o receptor) es de alguna de las empresas permitidas; sin restricción siempre true. */
function cfdiDeEmpresasPermitidas(cfdi, permitidas) {
  if (!permitidas) return true;
  return [cfdi?.emisor?.rfc, cfdi?.receptor?.rfc]
    .some(r => r && permitidas.includes(String(r).toUpperCase()));
}

/** Filtro Mongo "emisor o receptor es de mis empresas" (para combinar con $and). */
function filtroCfdiEmpresas(permitidas) {
  return { $or: [{ 'emisor.rfc': { $in: permitidas } }, { 'receptor.rfc': { $in: permitidas } }] };
}

module.exports = {
  empresasPermitidas,
  limitarAEmpresasDelUsuario,
  soloSinEmpresasAsignadas,
  cfdiDeEmpresasPermitidas,
  filtroCfdiEmpresas,
};
