'use strict';

// netpay-comision-sync.service.js — sincroniza la comisión base detectada en cada reporte
// Netpay cargado hacia Configuraciones Globales (secciones 'netpay-comisiones-tarjetas' y
// 'netpay-comisiones-sucursales'), para tener ahí el valor VIGENTE por tipo de tarjeta y por
// sucursal, con historial de cambios real (pedido explícito del usuario, 2026-10-07: "tener
// dentro de la Central de Configuración, una nueva sección llamada Netpay"). Separado a
// propósito de netpay-comision.service.js (que es de solo-lectura/agregación sobre Mongo, para
// la pestaña "Comisiones" del panel Netpay): este archivo introduce una dependencia nueva a
// Postgres vía global-config.service.js, y mezclar ambas responsabilidades en un solo archivo
// haría menos obvio qué función toca qué base de datos. No reemplaza a ese archivo — son dos
// vistas complementarias del mismo dato crudo (folios[].comisionBasePct): una ad-hoc sobre todo
// el histórico, esta otra "oficial, con historial auditado" dentro de Configuraciones Globales.
//
// Fix de agrupación por sucursal (2026-10-07, pedido explícito del usuario — confirmado con
// datos reales de producción): la primera versión agrupaba sucursales por `storeId`, pero
// storeId identifica una TERMINAL/caja registradora, no la sucursal física — una misma sucursal
// puede tener varias terminales con storeId distinto (ej. storeId=1196184 y storeId=1650292
// ambos son "AV FERROCARRIL 802", con comisiones DISTINTAS 0.02 vs 0.01). Agrupar por storeId
// generaba "muchísimos registros" redundantes e ilegibles (un número sin nombre reconocible).
// Ahora se agrupa por el NOMBRE real de la sucursal — todas las terminales de un mismo local
// caen en una sola entrada; si dos terminales de la misma sucursal reportan comisiones
// distintas, eso queda reflejado como un cambio real en el historial (justo lo que este
// feature busca detectar), en vez de ser registros separados sin sentido.

const globalConfigService = require('../../../shared/services/global-config.service');
const { logger } = require('../../../shared/utils/logger');

const SECCION_TARJETAS = 'netpay-comisiones-tarjetas';
const SECCION_SUCURSALES = 'netpay-comisiones-sucursales';

// Redondeo a 2 decimales ANTES de comparar/guardar — mismo fix de precisión de float que ya
// existe en netpay-comision.service.js#_redondear (parseFloat del Excel puede traer
// 2.3600000000000003 en vez de 2.36). Se duplica la línea en vez de importarla de ese archivo
// a propósito: ese archivo es puramente de lectura/agregación Mongo, no vale la pena crear un
// acoplamiento entre los dos por una sola línea de redondeo.
function _redondear(pct) {
  return Math.round(pct * 100) / 100;
}

// _slug — normaliza texto libre (nombre de banco, tipo de tarjeta, nombre de sucursal) a una
// clave estable y legible en URL (ver PUT /sections/:sectionClave/configs/:clave en
// config.routes.js) — sin acentos, minúsculas, cualquier secuencia de caracteres no
// alfanuméricos colapsa a un solo guión, sin guiones al principio/final.
function _slug(texto) {
  return String(texto ?? '')
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

// _categoriasDeReporte — recorre los folios de UN NetpayReporte ya persistido/parseado y arma
// las categorías de comisión detectadas: por TARJETA (banco+tipoTarjeta) y por SUCURSAL
// (nombre real de la sucursal — ver fix de agrupación arriba). Un folio sin los campos
// necesarios para una categoría simplemente no aporta a esa categoría (nunca tira) — Netpay no
// siempre expone banco/tipoTarjeta/storeId/sucursal por folio. `storeId` sigue siendo parte de
// la condición de entrada (valida que el folio trae datos de sucursal completos) pero ya NO
// forma parte de la clave. Si 2+ folios del MISMO reporte caen en la misma clave con
// comisionBasePct distinto (ej. 2 terminales de la misma sucursal, o simplemente 2 folios del
// mismo banco+tarjeta), el último procesado gana (comportamiento natural de ir pisando el Map,
// sin lógica especial — no hay forma de saber cuál es "más correcta" dentro de un mismo
// reporte).
function _categoriasDeReporte(reporte) {
  const categorias = new Map();
  for (const folio of (reporte.folios ?? [])) {
    if (folio.comisionBasePct == null) continue;
    const pct = _redondear(folio.comisionBasePct);

    if (folio.banco && folio.tipoTarjeta) {
      const clave = `tarjeta-${_slug(folio.banco)}-${_slug(folio.tipoTarjeta)}`;
      categorias.set(clave, {
        pct,
        descripcion: `${folio.banco} · ${folio.tipoTarjeta} — comisión base detectada automáticamente desde reportes Netpay.`,
      });
    }

    if (folio.storeId && folio.sucursal) {
      const clave = `sucursal-${_slug(folio.sucursal)}`;
      categorias.set(clave, {
        pct,
        descripcion: `Sucursal ${folio.sucursal} — comisión base detectada automáticamente desde reportes Netpay.`,
      });
    }
  }
  return categorias;
}

// _seccionDeClave — cada clave ya trae el prefijo 'tarjeta-'/'sucursal-' (ver
// _categoriasDeReporte) — se usa para decidir a qué ConfigSection va cada valor.
function _seccionDeClave(clave) {
  return clave.startsWith('tarjeta-') ? SECCION_TARJETAS : SECCION_SUCURSALES;
}

// sincronizarComisiones — best-effort COMPLETO: nunca tira, mismo criterio que
// consultarFoliosPendientes dentro de cargarReporte. Que falle una categoría puntual (ej. la
// sección correspondiente todavía no existe porque no se corrió el seed en este ambiente) no
// debe abortar las demás categorías ni, mucho menos, la carga del reporte que la disparó.
async function sincronizarComisiones(reporte) {
  const categorias = _categoriasDeReporte(reporte);
  for (const [clave, { pct, descripcion }] of categorias) {
    const seccion = _seccionDeClave(clave);
    try {
      let actual = null;
      try {
        actual = await globalConfigService.getValue(seccion, clave);
      } catch {
        actual = null; // no existe todavía — setValue la crea como "creado", no "editado"
      }

      // CRÍTICO (hallazgo de diseño, 2026-10-07): setValue() SIEMPRE escribe una entrada en
      // ConfigAuditLog, sin comparar si el valor realmente cambió (ver
      // global-config.service.js#setValue) — sin este guard, cada carga de reporte
      // ensuciaría el historial con "cambios" falsos del mismo valor.
      if (actual != null && Number(actual) === pct) continue;

      await globalConfigService.setValue(seccion, clave, String(pct), {
        tipo: 'numero',
        descripcion,
        usuarioNombre: 'Motor de Sincronización de Comisiones Netpay (automático)',
      });
    } catch (err) {
      logger.warn(`[NetpayComisionSync] no se pudo sincronizar la clave '${clave}': ${err.message}`);
    }
  }
}

module.exports = {
  sincronizarComisiones, _categoriasDeReporte, _slug, _seccionDeClave,
  SECCION_TARJETAS, SECCION_SUCURSALES,
};
