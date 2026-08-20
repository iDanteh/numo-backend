'use strict';

const CFDI                 = require('../../../visor/models/CFDI');
const { PolizaMovimiento, AccountPlan, CfdiMappingRule, Poliza } = require('../../../shared/models/postgres');
const centrosSvc = require('../centros-costo/centros-costo.service');
const { Op, QueryTypes }   = require('sequelize');
const { sequelize }        = require('../../../config/database.postgres');
const mappingSvc           = require('./cfdi-mapping.service');
const { _getRulesActive, _enrichTasaIvaFromRelatedCfdis, _normalizarEgresoPue99, _normalizarEgresoCondonacion, _normalizarEgresoSegunFacturaRelacionada } = require('./balanza-preliminar.service');
const ErpCuentaPendiente   = require('../erp/ErpCuentaPendiente.model');
const BankMovement         = require('../banks/BankMovement.model');
const { construirMovimientosPuente, _extraerDocumentosRelacionados, _sincronizarCobroSucursalPendiente } = require('./cobros-sucursal-puente.service');
const { obtenerSaldosFavor, obtenerDesglosesCobroAlmacen, obtenerDesglosesCobroAlmacenPorCentro, obtenerSaldosFavorPorCentro } = require('../erp/erp-sync.service');
const { SERIES_CON_AUTH } = require('../erp/erp-auth.utils');
const { BadRequestError }          = require('../../shared/errors/AppError');
const { repararSubtotalDesdeXml }  = require('../../../visor/services/cfdiSubtotalRepair');
const {
  _splitUuids,
  _extraerSustitutos,
  _enriquecerSustitutosConPeriodoOriginal,
  _particionarSustitutosPorRiesgo,
} = require('./sustitutos-cfdi.util');

// Extrae los uuids de CFDIs relacionados de un CFDI — soporta tanto `uuids`
// (array, formato ERP) como `uuid` (singular, formato SAT), según el origen.
const _uuidsRelacionados = (cfdi) => (cfdi.cfdiRelacionados || []).flatMap(r => r.uuids ?? (r.uuid ? [r.uuid] : []));

// El pre-fetch de relMetodoPagoMap/relFacturaMetaMap (más abajo, `relTipoUuidsProp`/
// `relTipoUuidsGuard`) se calcula ANTES del merge con ERP, a partir del
// `cfdiRelacionados` crudo de SAT — pero algunas NC (Devolución/Bonificación)
// solo traen `cfdiRelacionados` poblado en la fuente ERP, no en SAT (confirmado
// con el usuario: NC 82AA95D2-A255-4441-9C99-BEE4D3E9B959, factura origen
// A0-260615344 PPD — su `cfdiRelacionados` en SAT llega vacío, solo aparece
// tras el merge ERP). Sin esto, esas NC nunca resuelven `metodoPagoRelacionado`
// (ni tampoco lo ven `_normalizarEgresoCondonacion`/`_normalizarEgresoSegunFacturaRelacionada`,
// que dependen del mismo mapa) y se clasifican con su propio metodoPago en vez
// del de la venta que ajustan. Se llama DESPUÉS del merge para completar los
// mapas con los uuids relacionados que solo aparecieron ahí (solo agrega, nunca
// sobrescribe lo que el pre-fetch original ya resolvió).
async function _completarRelacionadosPostMerge(cfdisFinal, relMetodoPagoMap, relFacturaMetaMap) {
  const faltantes = [...new Set(cfdisFinal.flatMap(c => _uuidsRelacionados(c)))]
    .filter(u => !(u in relMetodoPagoMap));
  if (!faltantes.length) return;
  const relCfdis = await CFDI.find({ uuid: { $in: faltantes } })
    .select('uuid metodoPago formaPago').lean();
  for (const c of relCfdis) {
    // Un mismo uuid trae DOS documentos (source SAT y ERP) — el de SAT suele
    // no tener metodoPago/formaPago propio (undefined). Sin este guard, si el
    // doc SAT se procesa después del ERP, pisa el valor bueno con `undefined`.
    if (c.metodoPago == null) continue;
    relMetodoPagoMap[c.uuid]  = c.metodoPago;
    relFacturaMetaMap[c.uuid] = { metodoPago: c.metodoPago, formaPago: c.formaPago };
  }
}

const SUCURSAL_DEFAULT = 'Cedis';

// Categorías de filtro por forma de pago para la póliza de Cobranza (Pagos) —
// mismas categorías/códigos que ya usa el export CONTPAQ
// (poliza.service.js: LABEL_FORMA_PAGO_CONSOLIDADO/FORMA_PAGO_TRANSFERENCIA/
// FORMA_PAGO_CHEQUE), duplicado a propósito aquí (mismo patrón que otras
// constantes pequeñas ya compartidas entre estos dos archivos). Solo aplica
// cuando `tipoCfdi === 'P'` — para Ingreso/Egreso se ignora (confirmado con
// el usuario 2026-08-11).
const FORMA_PAGO_A_CATEGORIA = { '01': 'EFECTIVO', '02': 'CHEQUE', '03': 'TRANSFERENCIA', '04': 'TARJETA', '28': 'TARJETA' };

// Para tipo P, `formaPago` casi nunca viene en el header del CFDI — vive en
// el complemento (mismo fallback que ya usa `findRuleInList`/`satMeta` en
// cfdi-mapping.service.js).
function _formaPagoResuelta(cfdi) {
  return cfdi.formaPago ?? cfdi.complementoPago?.pagos?.[0]?.formaDePagoP ?? null;
}

// Misma lista que TIPO_MARCADORES en cobros-sucursal-puente.service.js — el
// marcador de TIPO del documento relacionado (no la referencia real a la
// venta). Duplicado a propósito (archivo pequeño, mismo patrón que
// ETIQUETA_COBRO_SUCURSAL ya duplicado entre poliza.service.js y ese archivo).
const TIPO_MARCADORES_DEV = ['BON', 'BCT', 'DEV', 'CAC'];

// Etiqueta especial de columna C para el par generación+uso de un saldo a
// favor que se "lava" el mismo día en el mismo almacén (ver
// `_prefetchSaldosFavorGenerados`) — `_extraerCobrosSucursal`
// (poliza.service.js) las omite del export por completo (ni siquiera como
// "Cobro de otra sucursal"), pero SIGUEN existiendo en poliza_movimientos —
// mismo patrón que ya usa el sistema para el Abono real de una Devolución
// (se oculta del export, nunca se borra de la BD). Duplicado a propósito en
// cobros-sucursal-puente.service.js.
const ETIQUETA_SALDO_FAVOR_OCULTO = 'SF-OCULTO';

/**
 * Deduplica líneas de Saldo a Favor (SF/SF-OCULTO) que DOS mecanismos
 * independientes pueden detectar por separado para el MISMO saldo usado,
 * generando dos líneas idénticas en vez de una:
 *   1. `_prefetchAjustesFacturaPropia` (consulta "por centro", cubre TODO el
 *      uso de SF de esta factura sin importar forma de pago ni período).
 *   2. El bloque "SF usado por ventas de período anterior pagadas con APA"
 *      en `construirMovimientosPuente` (cobros-sucursal-puente.service.js,
 *      2026-08-17) — pensado para huecos que el (1) no cubría, pero ahora
 *      que (1) también cubre período anterior, se solapan para tickets que
 *      SÍ están en el batch actual.
 * Se identifican por compartir concepto+cuenta+debe+haber exactos — una
 * coincidencia casi imposible entre dos SF reales distintos (confirmado con
 * el usuario 2026-08-18, caso real Global 89CF6A7F: DEV-055991 aparecía dos
 * veces, $365.16+$58.42 cada vez, una como 'Cargo Especial' y otra como
 * 'Cobro Sucursal').
 */
function _deduplicarSFRedundante(movs) {
  const vistos = new Set();
  const resultado = [];
  for (const m of movs) {
    if (m.reglaNombre !== 'SF' && m.reglaNombre !== ETIQUETA_SALDO_FAVOR_OCULTO) { resultado.push(m); continue; }
    const key = `${m.concepto}|${m.cuentaId}|${Number(m.debe).toFixed(2)}|${Number(m.haber).toFixed(2)}`;
    if (vistos.has(key)) continue;
    vistos.add(key);
    resultado.push(m);
  }
  return resultado;
}

/**
 * Para cada Devolución (CFDI tipo E) del batch, consulta en lote
 * /desgloses-cobro/saldos-favor (vía `obtenerSaldosFavor`) usando el folioVenta
 * de la venta ORIGINAL que ajusta (su documento relacionado, excluyendo el
 * marcador de tipo) y devuelve `{ mapa, devsOcultos }`:
 *   - `mapa`: `DEV|folio` → { monto, ventaSerie, ventaFolio, oculto,
 *     centroProcesamiento } — para saber si ESA Devolución específica generó
 *     un saldo a favor real, y a qué VENTA se le atribuye (confirmado con el
 *     usuario 2026-08-04: DEV-055225 generó $136.22 registrados bajo la
 *     venta I0-260700186 — NO bajo el folio propio de la devolución).
 *   - `devsOcultos`: Set de `DEV|folio` cuyo saldo se generó Y se consumió
 *     POR COMPLETO el MISMO día en el MISMO almacén (una sola aplicación,
 *     sin sobrante) — confirmado con el usuario 2026-08-04: ese caso es un
 *     "lavado" interno del mismo almacén/día y no debe verse en el export
 *     (ni la línea de generación ni la de uso), aunque sí queden guardadas.
 *     Si se generó en OTRO almacén, se usó OTRO día, o el uso fue parcial
 *     (sobrante > 0, sea del mismo almacén o de otro), SÍ debe verse.
 *     `construirMovimientosPuente` (cobros-sucursal-puente.service.js) usa
 *     este set para marcar igual el lado de "uso".
 *
 * "MISMO almacén" (corregido 2026-08-05): se compara el `claveCentro` REAL
 * de `/desgloses-cobro/almacen` de ambos lados (dónde se cobró la venta que
 * generó el saldo vs. dónde se cobró la venta que lo usó), no la serie de
 * facturación — dos ventas de la MISMA sucursal facturadora (misma serie)
 * pueden haberse cobrado ambas en OTRA sucursal física (caso real:
 * MUNICIPIO DE SAN MIGUEL PERAS, generación I0-260700208 y uso I0-260700210,
 * ambas facturadas como PROMOTORIA/I0 pero cobradas físicamente en CEDIS —
 * comparar por serie sí las consideraba "mismo almacén" por coincidencia,
 * pero antes de este fix `centroProcesamiento` tampoco se resolvía bien, así
 * que el caso real a verificar es que ambas piezas ahora usen el mismo dato
 * (`claveCentro`) de forma consistente).
 *
 * `centroProcesamiento`: la Devolución puede haberse procesado físicamente
 * en una sucursal DISTINTA a la del CFDI (mismo patrón que un cobro cruzado
 * normal, confirmado con el usuario — caso real: DEV-055219, CFDI serie
 * I0/PROMOTORIA, pero el cobro se hizo en CEDIS). Se obtiene de
 * `/desgloses-cobro/almacen` consultado con la serie/folio de la VENTA que
 * se devuelve (NO el marcador "DEV"/"BON"/etc. de la propia devolución —
 * probado contra el ERP real 2026-08-05: ese marcador no es un folio real
 * del sistema de cajas, la consulta regresa vacía), emparejando por MONTO
 * contra `gen.monto` (ver `_claveCentroPorMonto` — una cuenta puede tener
 * varios cobros sin relación entre sí, ej. el pago original de la factura;
 * tomar "cualquiera con claveCentro" sin verificar el monto atribuye mal la
 * sucursal, caso real JONATAN/DEV-055225 confirmado con el usuario
 * 2026-08-05). `_inyectarSaldoFavorGenerado` decide con esto si debe encolar
 * el asiento para la sucursal real en vez de ponerlo en la del CFDI.
 */
// Dado un mapa `${serie}|${folio}` → cobros[] crudos de /desgloses-cobro/almacen
// y un monto a buscar, encuentra el cobro real (tipo reconocido, con
// claveCentro) cuyo monto coincide — una cuenta puede tener VARIOS cobros sin
// relación entre sí (el pago original de la factura, aplicaciones de saldo a
// favor de otras devoluciones, etc.), así que no basta con tomar "cualquiera
// con claveCentro": hay que emparejar por MONTO (confirmado con el usuario
// 2026-08-05 — caso real JONATAN/DEV-055225: el primer cobro con claveCentro
// de esa cuenta era una transferencia de $25,546.16 sin relación, ajena al
// saldo de $136.22, y encoló mal la generación a CEDIS cuando debía quedarse
// en PROMOTORIA).
// Forma de pago SAT dominante (por monto acumulado) de los cobros REALES de
// una venta — mismo criterio de filtrado que `_prefetchAjustesFacturaPropia`
// (solo SERIES_CON_AUTH, excluye Puntos/Saldo a favor del texto de la forma
// de pago), pero sin filtrar por día: aquí solo se usa para el caso "mismo
// folio" (ver `_prefetchSaldosFavorGenerados`), donde ya sabemos que
// generación y consumo son la MISMA venta, así que cualquier cobro real de
// esa cuenta es válido. Se usa UN solo valor dominante (no desglose) porque
// el usuario pidió restar directo del consolidado sin partir por forma de
// pago (confirmado 2026-08-13).
function _formaPagoDominante(cobros) {
  const acumuladoPorClave = new Map();
  for (const c of (cobros ?? [])) {
    if (!SERIES_CON_AUTH.includes((c.serieOrigen ?? '').toUpperCase())) continue;
    for (const fp of (c.formasPago ?? [])) {
      if (/puntos|saldo\s*a\s*favor/i.test(fp.nombre ?? '')) continue;
      const clave = (fp.claveSat ?? '').trim();
      if (!clave) continue;
      acumuladoPorClave.set(clave, (acumuladoPorClave.get(clave) ?? 0) + (Number(fp.monto) || 0));
    }
  }
  if (!acumuladoPorClave.size) return null;
  return [...acumuladoPorClave.entries()].sort((a, b) => b[1] - a[1])[0][0];
}

// 'APA' (2026-08-14, confirmado con el usuario — caso real LAZARO ROJAS
// RODRIGUEZ, ventas antiguas E0-260500658/659): cuando se USA un saldo a
// favor, el ERP registra en la cuenta que lo GENERÓ un cobro espejo
// serieOrigen='APA' con el mismo monto y la misma hora exacta que el cobro
// real del lado que lo usó, y sí trae `claveCentro` confiable. No es un pago
// real (no debe entrar al split de forma de pago ni a bank-autorizaciones),
// por eso NO se agrega a SERIES_CON_AUTH global — se acepta solo aquí, para
// resolver claveCentro del lado de generación de saldo a favor.
const SERIES_CLAVE_CENTRO_SF = [...SERIES_CON_AUTH, 'APA'];

function _claveCentroPorMonto(cobrosPorCuenta, serie, folio, monto, fechaExacta) {
  const cobros = cobrosPorCuenta.get(`${serie}|${folio}`) ?? [];
  const match = cobros.find(c => {
    if (!SERIES_CLAVE_CENTRO_SF.includes((c.serieOrigen ?? '').toUpperCase()) || !c.claveCentro) return false;
    // El cobro que generó/usó el saldo puede ser un pago MIXTO (ej. parte
    // efectivo + parte saldo a favor en un solo registro) — el monto TOTAL
    // del cobro no coincide con el monto del saldo en ese caso (confirmado
    // con el usuario 2026-08-05, caso real MUNICIPIO/I0-260700210: cobro de
    // $1,901.33 = $207.51 efectivo + $1,693.82 saldo a favor). Se acepta el
    // match si el TOTAL coincide (caso simple) O si el monto aparece exacto
    // dentro de alguna de sus formasPago (caso mixto).
    if (Math.abs(Math.abs(Number(c.monto) || 0) - Number(monto)) < 0.01) return true;
    if ((c.formasPago ?? []).some(fp =>
      /saldo\s*a\s*favor/i.test(fp.nombre ?? '')
      && Math.abs(Math.abs(Number(fp.monto) || 0) - Number(monto)) < 0.01,
    )) return true;
    // Varios saldos a favor DISTINTOS pueden aplicarse JUNTOS en un solo
    // cobro (caso real LAZARO ROJAS RODRIGUEZ, 2026-08-14: dos DEV de 252.82
    // y 402.17 se usaron en un solo pago combinado de 654.99 — ningún monto
    // individual coincide con esa formaPago sumada). En ese caso se acepta
    // el match por fecha EXACTA (mismo evento de aplicación, hasta el
    // microsegundo, confirmado idéntico entre `usos[].fecha` y `cobro.fecha`
    // en los datos reales del ERP).
    if (fechaExacta && c.fecha && new Date(c.fecha).getTime() === new Date(fechaExacta).getTime()
      && (c.formasPago ?? []).some(fp => /saldo\s*a\s*favor/i.test(fp.nombre ?? ''))) return true;
    return false;
  });
  return match?.claveCentro ?? null;
}

async function _prefetchSaldosFavorGenerados(cfdis, rfc, ccBySerieMap, opciones = {}) {
  const { centroPropioClave, fechaDesde, fechaHasta } = opciones;

  const generadosPorCuenta = []; // [{ cuenta, gen }]
  const cobrosPorVenta     = new Map();
  const cobrosPorVentaUso  = new Map();
  // Solo se popula en el camino fallback (sin centro+fecha), donde conocemos
  // los CFDIs del batch y podemos mapear venta → serie del CFDI de Devolución.
  const cfdiSeriePorVenta  = new Map();

  if (centroPropioClave && fechaDesde && fechaHasta) {
    // Camino principal: consulta directa por centro+fecha — fuente única para
    // saldos generados y cobros. Equivalente a lo que ya hace
    // `construirMovimientosPuente` para el lado de USO, pero para el lado de
    // GENERACIÓN. Encuentra todos los saldos del día en este centro sin
    // requerir conocer de antemano las ventas de cada Devolución — incluyendo
    // las que no tienen referencia de venta en `documentosRelacionados`.
    const [cuentasSF, cuentasAlm] = await Promise.all([
      obtenerSaldosFavorPorCentro({
        rfc, centro: centroPropioClave,
        fechaDesde: fechaDesde.toISOString(), fechaHasta: fechaHasta.toISOString(),
      }),
      obtenerDesglosesCobroAlmacenPorCentro({
        rfc, centro: centroPropioClave,
        fechaDesde: fechaDesde.toISOString(), fechaHasta: fechaHasta.toISOString(),
      }),
    ]);
    for (const cuenta of cuentasSF) {
      for (const gen of (cuenta.saldosFavorGenerados ?? [])) {
        generadosPorCuenta.push({ cuenta, gen });
      }
    }
    for (const cuenta of cuentasAlm) {
      const k = `${cuenta.serieVenta}|${cuenta.folioVenta}`;
      const cobros = cuenta.cobros ?? [];
      if (!cobros.length) continue;
      cobrosPorVenta.set(k, [...(cobrosPorVenta.get(k) ?? []), ...cobros]);
      cobrosPorVentaUso.set(k, [...(cobrosPorVentaUso.get(k) ?? []), ...cobros]);
    }
  } else {
    // Camino fallback: sin centro+fecha conocidos (ej. generación de periodo
    // completo sin filtro de sucursal). Usa las Devoluciones del batch y sus
    // ventas relacionadas para consultar el ERP por serie/folio.
    const devolucionesConVenta = cfdis
      .map(cfdi => {
        if (cfdi.tipoDeComprobante !== 'E') return null;
        const marcador = (cfdi.documentosRelacionados ?? [])
          .find(d => TIPO_MARCADORES_DEV.includes((d.Serie ?? '').toUpperCase()) && d.Folio);
        const venta = _extraerDocumentosRelacionados(cfdi)[0];
        return (marcador && venta) ? { marcador, venta, cfdiSerie: cfdi.serie ?? null } : null;
      })
      .filter(Boolean);
    if (!devolucionesConVenta.length) return { mapa: new Map(), devsOcultos: new Set() };
    devolucionesConVenta.forEach(d => cfdiSeriePorVenta.set(`${d.venta.serie}|${d.venta.folio}`, d.cfdiSerie));

    const LOTE = 150;
    for (let i = 0; i < devolucionesConVenta.length; i += LOTE) {
      const lote = devolucionesConVenta.slice(i, i + LOTE);
      const resultado = await obtenerSaldosFavor({
        rfc,
        series: lote.map(d => d.venta.serie),
        folios: lote.map(d => d.venta.folio),
      });
      for (const cuenta of resultado) {
        for (const gen of (cuenta.saldosFavorGenerados ?? [])) {
          generadosPorCuenta.push({ cuenta, gen });
        }
      }
    }
    if (!generadosPorCuenta.length) return { mapa: new Map(), devsOcultos: new Set() };

    const paresGeneracion = [...new Map(
      generadosPorCuenta.map(({ cuenta }) => [`${cuenta.serieVenta}|${cuenta.folioVenta}`, { serie: cuenta.serieVenta, folio: cuenta.folioVenta }]),
    ).values()];
    for (let i = 0; i < paresGeneracion.length; i += LOTE) {
      const lote = paresGeneracion.slice(i, i + LOTE);
      const resultadoAlmacen = await obtenerDesglosesCobroAlmacen({ rfc, series: lote.map(p => p.serie), folios: lote.map(p => p.folio) });
      for (const cuenta of resultadoAlmacen) cobrosPorVenta.set(`${cuenta.serieVenta}|${cuenta.folioVenta}`, cuenta.cobros ?? []);
    }

    const paresUso = [...new Map(
      generadosPorCuenta
        .map(({ gen }) => (gen.usos ?? []).length === 1 ? gen.usos[0] : null)
        .filter(Boolean)
        .map(u => [`${u.serieVenta}|${u.folioVenta}`, { serie: u.serieVenta, folio: u.folioVenta }]),
    ).values()];
    for (let i = 0; i < paresUso.length; i += LOTE) {
      const lote = paresUso.slice(i, i + LOTE);
      const resultadoAlmacen = await obtenerDesglosesCobroAlmacen({ rfc, series: lote.map(p => p.serie), folios: lote.map(p => p.folio) });
      for (const cuenta of resultadoAlmacen) cobrosPorVentaUso.set(`${cuenta.serieVenta}|${cuenta.folioVenta}`, cuenta.cobros ?? []);
    }
  }

  if (!generadosPorCuenta.length) return { mapa: new Map(), devsOcultos: new Set(), ajustesEfectivoRetiroSF: [] };

  // Armar `mapa`/`devsOcultos` ya con los dos lados resueltos.
  const mapa = new Map();
  const devsOcultos = new Set();
  // Retiros en EFECTIVO del saldo a favor (serieOrigen='ABO' dentro de
  // `usos`, confirmado con el usuario 2026-08-19: un "ABO" no es otra venta
  // que consume el saldo, es al cliente sacando su saldo en efectivo de
  // caja) — dinero real que salió, así que se resta del consolidado de
  // Efectivo (mismo patrón que `_ajusteConsolidadoSF`/"SF-MISMO-FOLIO":
  // Cargo NEGATIVO, sin fila propia), SIEMPRE — sin importar si el saldo
  // generado terminó en $0 ese día o le quedó un remanente pendiente.
  const ajustesEfectivoRetiroSF = [];
  for (const { cuenta, gen } of generadosPorCuenta) {
    const key = `${gen.serieOrigen}|${gen.folioOrigen}`;
    const usos     = gen.usos ?? [];
    const usoUnico = usos.length === 1 ? usos[0] : null;
    // _diaMx: hora México (UTC-6) — sin esto, eventos después de las 6pm local
    // cruzan al "día siguiente" en UTC y la comparación diaGen===diaUso falla.
    const diaGen = _diaMx(gen.fecha);
    const diaUso = _diaMx(usoUnico?.fecha ?? null);
    const usoCompleto = usoUnico && Math.abs(Number(usoUnico.montoSobrante) || 0) < 0.01;

    // Multi-uso el mismo día (2026-08-19, caso real CAC-077160: $97.36
    // aplicados a otra venta + $195.54 retirados en efectivo vía ABO, ambos
    // el mismo día de la generación): a diferencia del caso de un solo uso
    // (arriba), aquí se suman TODOS los usos del día y se compara contra lo
    // generado — si cierra en $0, se oculta igual que el caso normal; si NO
    // cierra, el remanente (`saldoRestanteSF`) se muestra como línea visible
    // de SF en vez de todo-o-nada. El retiro en efectivo se resta del
    // consolidado SIEMPRE, sin importar si cierra en $0 o no.
    const usosMismoDia = diaGen ? usos.filter(u => _diaMx(u.fecha) === diaGen) : [];
    const sumaUsosMismoDia = usosMismoDia.reduce((s, u) => s + (Math.abs(Number(u.montoUsado)) || 0), 0);
    const saldoRestanteSF = Math.round(((Number(gen.monto) || 0) - sumaUsosMismoDia) * 100) / 100;
    // Solo los usos tipo ABO (retiro en efectivo) se restan aquí del Abono de
    // generación — ver `montoAEmitir` abajo. Un uso normal (venta que pagó con
    // este saldo en OTRO ticket) ya se debita por separado en
    // `saldoFavorUsado` (ver `_prefetchAjustesFacturaPropia`, corrección
    // 2026-08-19 ticket C0-260800403/431) — restarlo TAMBIÉN aquí sería una
    // doble resta (el caso que este comentario original advertía, pero que
    // solo aplicaba de verdad al retiro ABO, no a cualquier uso).
    let sumaABOMismoDia = 0;
    for (const u of usosMismoDia) {
      if ((u.serieOrigen ?? u.serieVenta ?? '').toUpperCase() !== 'ABO') continue;
      const montoRetiro = Math.abs(Number(u.montoUsado)) || 0;
      if (montoRetiro <= 0) continue;
      sumaABOMismoDia += montoRetiro;
      ajustesEfectivoRetiroSF.push({
        monto: montoRetiro,
        centro: centroPropioClave ?? null,
        fecha: u.fecha ?? gen.fecha ?? null,
        ventaSerie: cuenta.serieVenta ?? null,
        ventaFolio: cuenta.folioVenta ?? null,
      });
    }
    const saldoRestanteSoloABO = Math.round(((Number(gen.monto) || 0) - sumaABOMismoDia) * 100) / 100;

    // En el camino principal (centro+fecha), `obtenerSaldosFavorPorCentro`
    // garantiza que TODOS los saldosFavorGenerados pertenecen al centro
    // consultado — la venta GEN puede ser de un día anterior y sus cobros no
    // estarán en `cobrosPorVenta` (que solo cubre el día del batch), así que
    // usar `_claveCentroPorMonto` devolvería null y rompería la detección de
    // `oculto`. En el camino fallback (por lote) sí usamos los cobros porque
    // no tenemos certeza del centro.
    const claveCentroGen = centroPropioClave
      ?? _claveCentroPorMonto(cobrosPorVenta, cuenta.serieVenta, cuenta.folioVenta, gen.monto);
    // En el camino principal, `cobrosPorVentaUso` se llena desde
    // `obtenerDesglosesCobroAlmacenPorCentro` — si la venta USO aparece en ese
    // mapa, ya sabemos que pertenece a `centroPropioClave`, aunque los cobros
    // individuales no tengan `serieOrigen` en SERIES_CLAVE_CENTRO_SF (que es el
    // requisito interno de `_claveCentroPorMonto` para extraer `claveCentro`).
    // Se usa `centroPropioClave` como fallback en lugar de dejar null y romper
    // la detección de SF-OCULTO (confirmado con el usuario 2026-08-17).
    const claveCentroUso = usoUnico
      ? (_claveCentroPorMonto(cobrosPorVentaUso, usoUnico.serieVenta, usoUnico.folioVenta, usoUnico.montoUsado, usoUnico.fecha)
         ?? (centroPropioClave && cobrosPorVentaUso.has(`${usoUnico.serieVenta}|${usoUnico.folioVenta}`) ? centroPropioClave : null))
      : null;
    const centro = (claveCentroGen && ccBySerieMap) ? (ccBySerieMap[claveCentroGen] ?? null) : null;

    // `cfdiSeriePorVenta` solo tiene datos en el camino fallback — en el
    // camino por centro+fecha cc=null → esCruzado=false (el cruce real ya lo
    // detecta `_inyectarSaldoFavorGenerado` vía `centroProcesamiento`).
    const cfdiSerie = cfdiSeriePorVenta.get(`${cuenta.serieVenta}|${cuenta.folioVenta}`);
    const cc        = (cfdiSerie && ccBySerieMap) ? (ccBySerieMap[cfdiSerie] ?? null) : null;
    const esCruzado = !!(centro && cc && String(centro.id) !== String(cc.id));

    // Multi-uso resuelto el mismo día (ver `saldoRestanteSF` arriba): oculta
    // igual que el caso de un solo uso cuando la SUMA de todos los usos de
    // ese día cierra el saldo en $0 — no se exige coincidencia de almacén
    // por uso individual (a diferencia del caso de un solo uso) porque un
    // retiro en efectivo (ABO) no necesariamente ocurre en el mismo almacén
    // que la generación, y eso no lo hace menos "resuelto el mismo día".
    const ocultoMultiUso = usosMismoDia.length > 1 && Math.abs(saldoRestanteSF) < 0.01;
    const oculto = ocultoMultiUso || (!esCruzado && !!(usoUnico && usoCompleto && diaGen && diaGen === diaUso
      && claveCentroGen && claveCentroUso && claveCentroGen === claveCentroUso));
    if (oculto) devsOcultos.add(key);

    // Caso "mismo folio" (confirmado con el usuario 2026-08-13): la MISMA
    // venta generó Y consumió el saldo a favor (uso completo, sin sobrante).
    // Guard: solo si ambas ventas tienen serie+folio no nulos, para evitar
    // que dos saldos sin venta asociada se igualen falsamente por `null===null`.
    const mismoFolio = !!(usoUnico && usoCompleto
      && cuenta.serieVenta && cuenta.folioVenta
      && String(cuenta.serieVenta) === String(usoUnico.serieVenta)
      && String(cuenta.folioVenta) === String(usoUnico.folioVenta));

    // Monto a EMITIR como Abono de Saldo a Favor:
    // - Si el día cierra en $0 (oculto, ver arriba) se usa `saldoRestanteSF`
    //   (resta TODOS los usos) — da ~0, sin cambios respecto al caso ya
    //   confirmado (CAC-077160: venta + ABO cierran juntos el saldo).
    // - Si NO cierra (remanente visible), solo se resta el retiro ABO
    //   (`saldoRestanteSoloABO`) — un retiro en efectivo no tiene otra fila
    //   que lo compense, así que se descuenta aquí. Un uso normal (venta en
    //   otro ticket) SÍ tiene su propia fila (débito vía `saldoFavorUsado`),
    //   así que el Abono de generación se emite en BRUTO respecto a ese uso
    //   — restarlo aquí también sería la doble resta que el comentario
    //   original de este archivo advertía (2026-08-19, caso real
    //   C0-260800403 $89.15 generado / C0-260800431 $85.36 usado: se
    //   emitía solo el sobrante de $3.79 en vez del generado completo,
    //   dejando el Abono de $85.36 sin su Cargo compensatorio).
    // - Si no hubo NINGÚN uso el mismo día, se emite el monto generado
    //   completo (comportamiento anterior, sin cambios).
    const montoAEmitir = oculto
      ? Math.max(saldoRestanteSF, 0)
      : (usosMismoDia.length > 0 ? Math.max(saldoRestanteSoloABO, 0) : (Number(gen.monto) || 0));

    const prev = mapa.get(key);
    mapa.set(key, {
      monto:      (prev?.monto ?? 0) + montoAEmitir,
      ventaSerie: cuenta.serieVenta,
      ventaFolio: cuenta.folioVenta,
      oculto,
      mismoFolio: mismoFolio || prev?.mismoFolio || false,
      formaPagoReal: mismoFolio
        ? (_formaPagoDominante(cobrosPorVenta.get(`${cuenta.serieVenta}|${cuenta.folioVenta}`)) ?? prev?.formaPagoReal ?? null)
        : (prev?.formaPagoReal ?? null),
      centroProcesamiento: centro ?? prev?.centroProcesamiento ?? null,
    });
  }

  return { mapa, devsOcultos, ajustesEfectivoRetiroSF };
}

/**
 * Para cada CFDI tipo I del batch, consulta en lote /desgloses-cobro/almacen
 * (vía `obtenerDesglosesCobroAlmacen`) usando la serie/folio PROPIA de la
 * factura (es su propia venta en cajas — a diferencia de una Devolución, que
 * usa la venta que ajusta) y devuelve `serie|folio → formasPago[]` real —
 * consumido por `cfdiToMovimientos` (`context.desglosePagoReal`) para partir
 * el Cargo por forma de pago real en vez del `formaPago` que declara el CFDI
 * (confirmado con el usuario 2026-08-06: ese dato casi siempre viene
 * mal/genérico).
 *
 * Alcance de esta primera versión (confirmado con el usuario): SOLO tipo I.
 * Egresos (E) y Pagos (P) quedan fuera por ahora — la venta a consultar en
 * cajas para esos tipos no es la serie/folio propia del CFDI (para E sería
 * la venta original que ajusta; para P, la factura que el complemento está
 * pagando), y merece diseñarse aparte con más cuidado.
 *
 * Solo se consultan las facturas cuya regla YA seleccionó una cuenta de
 * Caja/Bancos "por identificar" (`rule.cuentaCargo`) — cualquier otra regla
 * no depende de `formaPago` para elegir cuenta, así que no hay nada que
 * corregir y se evita tráfico innecesario al ERP.
 */
// Corrección 2026-08-06 (mismo día, tras revisar datos reales con el
// usuario): esta función devolvía TODAS las `formasPago` de los `cobros[]`
// con serieOrigen en SERIES_CON_AUTH (ABO/CBT/CPF/CFC) mezcladas en un solo
// arreglo — pero 'CBT' (Club Tuberos/Puntos) es un evento DISTINTO al pago
// original de la venta (que llega por ABO/CPF/CFC), y frecuentemente es el
// ÚNICO cobro que trae esta consulta para una venta dada (caso real
// verificado con el usuario: factura de $1,023.63 con SOLO un cobro CBT de
// $87.79 de Puntos — el pago real, el resto, no aparece aquí). El split
// proporcional de `cfdiToMovimientos` asumía que la suma de `formasPago`
// encontradas era el 100% de la factura, así que atribuía la factura
// COMPLETA a Puntos cuando solo era una fracción. Ahora los cobros CBT se
// separan aparte (`puntosUsado`, ver `montoPuntosUsado` en
// cfdi-mapping.service.js) y NUNCA entran al `desglosePagoReal` que alimenta
// el split de Efectivo/Tarjeta.
// Día calendario en México (UTC-6, sin DST desde 2022) de una fecha/hora ISO
// cualquiera. Usado para verificar que un cobro real (fecha/hora exacta del
// evento en cajas) ocurrió el MISMO día que el CFDI que se está procesando —
// sin esto, un cobro de una cuenta/ticket abierta días antes (factura emitida
// después, patrón común de facturación diferida/Global) se atribuye por error
// al día de la factura en vez del día real en que ocurrió (confirmado con el
// usuario 2026-08-06: Puntos de $87.79/$331.56/$108.16 que mi código sumó al
// consolidado del 10/07 en realidad ocurrieron el 04 y 06/07 — la factura de
// esa venta no se emitió hasta el 10/07, días después del ticket real).
function _diaMx(fechaIso) {
  if (!fechaIso) return null;
  return new Date(new Date(fechaIso).getTime() - 6 * 3600 * 1000).toISOString().slice(0, 10);
}

// Diferencia en días calendario (México) entre una fecha/hora ISO (ej. un
// cobro) y un día ya resuelto en formato 'YYYY-MM-DD' (ej. `_diaMx` de un
// CFDI) — usada para la tolerancia de ±1 día de facturación diferida (ver
// `TOLERANCIA_DIAS_FACTURACION_DIFERIDA` más abajo). null si falta cualquiera.
function _diferenciaDiasMx(fechaIso, diaYaResuelto) {
  const diaCobro = _diaMx(fechaIso);
  if (!diaCobro || !diaYaResuelto) return null;
  const msCobro = new Date(`${diaCobro}T00:00:00Z`).getTime();
  const msObjetivo = new Date(`${diaYaResuelto}T00:00:00Z`).getTime();
  return Math.round(Math.abs(msCobro - msObjetivo) / (24 * 3600 * 1000));
}

// Tolerancia de ±1 día calendario entre el cobro real y la fecha de la
// factura (2026-08-14, confirmado con el usuario — caso real Hidalgo
// B0-260701096/098/099/133/185/213, 09→10/07/2026): el cliente pagó el 9,
// pero la Factura Global que agrupa su ticket no se emitió hasta el 10 —
// un desfase de UN día entre cobro y facturación, distinto del caso de
// "días/semanas después" que el filtro exacto por día ya bloqueaba a
// propósito (ver comentario de `_diaMx` arriba, caso Puntos 04/06→10/07: ESE
// sigue bloqueado, la diferencia ahí es de 4-6 días, fuera de esta
// tolerancia). Los cobros de facturas viejas (varios días/semanas antes,
// típicamente cobranza de crédito) siguen excluidos — esos los maneja
// Cobranza, no Ingreso (confirmado con el usuario, mismo criterio ya
// aplicado a PPD en cobros-sucursal-puente.service.js).
const TOLERANCIA_DIAS_FACTURACION_DIFERIDA = 1;

/**
 * Fusión de `_prefetchDesglosePagoReal` + `_prefetchSaldoFavorUsadoPropio`
 * (2026-08-07, optimización): ambas funciones filtraban EXACTAMENTE el mismo
 * `candidatos` (tipo I, cuentaCargo Caja/Bancos) y calculaban el mismo
 * `diaCfdiPorClave` por separado — y, más importante, recorrían el MISMO
 * lote de facturas en bloques de 150 pero llamando cada una a SU propio
 * endpoint del ERP de forma secuencial (una función completa antes de
 * empezar la otra). Medido con datos reales: una póliza grande (Hidalgo,
 * ~30 lotes) hacía 66 llamadas ERP en 75s de pared, con boilerplate
 * (filtrado/mapa de fechas) duplicado.
 *
 * Aquí se calculan `candidatos`/`diaCfdiPorClave` UNA sola vez, y por cada
 * lote se llama a `/desgloses-cobro/almacen` y `/desgloses-cobro/saldos-favor`
 * EN PARALELO (`Promise.all`) — son endpoints DISTINTOS, así que no reintroduce
 * el problema de 429 que motivó quitar el paralelismo (ese fue por llamar el
 * MISMO endpoint demasiado rápido, no por llamar dos endpoints distintos a la
 * vez). Los lotes entre sí siguen siendo secuenciales (uno a la vez por
 * endpoint), preservando esa protección. Verificado que el resultado es
 * IDÉNTICO al de las dos funciones por separado (mismo póliza de prueba,
 * mismos montos por cuenta) antes de reemplazarlas.
 */
async function _prefetchAjustesFacturaPropia(cfdiConRegla, rfc, opciones = {}) {
  const candidatos = cfdiConRegla.filter(({ cfdi, rule }) =>
    cfdi.tipoDeComprobante === 'I' && cfdi.serie && cfdi.folio &&
    rule?.cuentaCargo && [CODIGO_CUENTA_CAJA, CODIGO_CUENTA_BANCOS].includes(rule.cuentaCargo),
  );
  const vacio = { desglosePagoReal: new Map(), puntosUsado: new Map(), saldoFavorUsado: new Map() };
  if (!candidatos.length) return vacio;

  // Día de CADA factura (México) — para filtrar cobros/usos que coinciden en
  // serie/folio pero son de un día distinto (ver `_diaMx`).
  const diaCfdiPorClave = new Map(candidatos.map(({ cfdi }) => [`${cfdi.serie}|${cfdi.folio}`, _diaMx(cfdi.fecha)]));

  const desglosePagoReal = new Map(); // `${serie}|${folio}` → [{ nombre, claveSat, monto }] (ABO/CPF/CFC — pago real de la venta)
  const puntosUsado = new Map();      // `${serie}|${folio}` → monto (solo CBT — redención de Club Tuberos, evento aparte)
  const saldoFavorUsado = new Map();  // `${serie}|${folio}` → { monto, detalle: [...] }

  // Fuente de los cobros reales — confirmado con el usuario 2026-08-14:
  // reemplaza POR COMPLETO la consulta por serie/folio propio cuando se
  // conoce el centro+rango de fechas de esta sucursal. La consulta por
  // serie/folio (bloque `else`) solo encuentra cobros de ventas que YA
  // sabíamos buscar (las del batch actual) — cualquier cobro real de esa
  // sucursal que no quedara ligado a esa venta específica (venta de
  // mostrador, ticket sin CFDI directo ese día, etc.) se perdía, y el split
  // de Cargo (`esCasoNormalParaSplit`/`esCasoAjusteSFPuntos` en
  // cfdi-mapping.service.js) caía al `formaPago` genérico declarado por el
  // CFDI en vez del real — caso real confirmado: Atzompa 09/07/2026, Tarjeta
  // de $16,067.38 contabilizada casi toda como Efectivo por falta de
  // desglose confiable. La consulta "por centro" trae TODOS los cobros de esa
  // sucursal en el rango sin depender de conocer de antemano cada serie/folio,
  // así que cubre ese hueco. Sigue filtrando por día de CADA factura
  // (`diaCfdiPorClave`/`_diaMx`) exactamente igual que antes.
  const { centroPropioClave, fechaDesde, fechaHasta } = opciones;
  let resultadosAlmacen = [];
  let resultadosSaldos  = [];
  let usoCaminoPorCentro = false;
  if (centroPropioClave && fechaDesde && fechaHasta) {
    try {
      // Ventana ampliada ±1 día SOLO para esta consulta (no para
      // `candidatos`/`diaCfdiPorClave`, que siguen siendo exactos al día de
      // esta generación) — ver `TOLERANCIA_DIAS_FACTURACION_DIFERIDA`: un
      // cobro de "ayer" puede pertenecer a una factura de "hoy" (facturación
      // diferida de un día), así que hay que poder VERLO antes de poder
      // decidir si aplica.
      const UN_DIA_MS = 24 * 3600 * 1000;
      const fechaDesdeAmpliada = new Date(fechaDesde.getTime() - TOLERANCIA_DIAS_FACTURACION_DIFERIDA * UN_DIA_MS);
      const fechaHastaAmpliada = new Date(fechaHasta.getTime() + TOLERANCIA_DIAS_FACTURACION_DIFERIDA * UN_DIA_MS);
      [resultadosAlmacen, resultadosSaldos] = await Promise.all([
        obtenerDesglosesCobroAlmacenPorCentro({ rfc, centro: centroPropioClave, fechaDesde: fechaDesdeAmpliada.toISOString(), fechaHasta: fechaHastaAmpliada.toISOString() }),
        obtenerSaldosFavorPorCentro({ rfc, centro: centroPropioClave, fechaDesde: fechaDesdeAmpliada.toISOString(), fechaHasta: fechaHastaAmpliada.toISOString() }),
      ]);
      usoCaminoPorCentro = true;
    } catch (err) {
      // Si el endpoint "por centro" falla (timeout, 5xx, lo que sea) esto NO
      // debe tumbar la generación completa de la póliza — confirmado con el
      // usuario 2026-08-14, caso real: timeout de 15s en
      // obtenerSaldosFavorPorCentro reventó generarYGuardar por completo
      // (la excepción se propagaba sin capturar). Se ignora esta fuente y se
      // cae al camino viejo por serie/folio (mismo patrón defensivo que ya
      // usa `construirMovimientosPuente` en cobros-sucursal-puente.service.js
      // para este mismo endpoint).
      const { logger } = require('../../../shared/utils/logger');
      logger.warn(`[AjustesFacturaPropia] Camino "por centro" falló (${err.message}), cae al camino viejo por serie/folio.`);
      resultadosAlmacen = [];
      resultadosSaldos  = [];
    }
  }
  if (!usoCaminoPorCentro) {
    // Fallback: sin centro/rango de fechas conocido (ej. generación sin
    // fechaInicio/fechaFin, o sin centroCostoId resuelto), o si el camino por
    // centro falló — vuelve al camino viejo, por serie/folio de cada factura
    // del batch.
    const LOTE = 150;
    for (let i = 0; i < candidatos.length; i += LOTE) {
      const lote = candidatos.slice(i, i + LOTE);
      const series = lote.map(({ cfdi }) => cfdi.serie);
      const folios = lote.map(({ cfdi }) => cfdi.folio);
      const [rA, rS] = await Promise.all([
        obtenerDesglosesCobroAlmacen({ rfc, series, folios }),
        obtenerSaldosFavor({ rfc, series, folios }),
      ]);
      resultadosAlmacen.push(...rA);
      resultadosSaldos.push(...rS);
    }
  }

  {
    for (const cuenta of resultadosAlmacen) {
      // `serieFactura`/`folioFactura` (2026-08-14, confirmado con datos
      // reales de producción): en una Factura Global, MUCHOS tickets de
      // mostrador individuales (cada uno con su propio `serieVenta`/
      // `folioVenta` en cajas) se facturan juntos bajo UN solo CFDI — el
      // ERP ya trae esa relación en cada cuenta. Si se agrupa por
      // `serieVenta/folioVenta` (el ticket), la clave nunca coincide con la
      // factura real que está procesando `cfdiToMovimientos` (que busca por
      // su propio serie/folio, el de la Factura Global) y el split nunca se
      // aplicaba — aunque el desglose ya estuviera disponible. Se usa
      // `serieFactura/folioFactura` cuando vienen (fallback a
      // serieVenta/folioVenta si no, para no romper el camino viejo por
      // serie/folio, donde ambos suelen coincidir). Como varios tickets caen
      // bajo la MISMA factura, hay que ACUMULAR (no sobreescribir).
      const key = `${cuenta.serieFactura || cuenta.serieVenta}|${cuenta.folioFactura || cuenta.folioVenta}`;
      const diaCfdi = diaCfdiPorClave.get(key);
      const formasPago = [];
      let montoPuntos = 0;
      for (const cobro of (cuenta.cobros ?? [])) {
        // El cobro debe haber ocurrido el MISMO día que la factura (con
        // tolerancia de ±1 día por facturación diferida — ver
        // `TOLERANCIA_DIAS_FACTURACION_DIFERIDA`/`_diferenciaDiasMx`). Sin
        // esto, una cuenta/ticket abierta DÍAS o SEMANAS antes (cobranza de
        // una factura vieja, no un simple desfase de frontera) mezclaría esa
        // actividad en el batch de hoy — la tolerancia de 1 día sigue
        // bloqueando ese caso (confirmado con el usuario 2026-08-14).
        const diffDias = _diferenciaDiasMx(cobro.fecha, diaCfdi);
        if (diaCfdi && (diffDias === null || diffDias > TOLERANCIA_DIAS_FACTURACION_DIFERIDA)) continue;
        // Cobro cruzado de sucursal (2026-08-14, caso real Ferrocarril
        // 09/07/2026): un ticket de la Factura Global de ESTA sucursal puede
        // haberse cobrado FÍSICAMENTE en otra (`cobro.claveCentro` distinto
        // de `centroPropioClave`) — ej. $9,159.56 en Tarjeta cobrados en C0
        // pero facturados en la Global de Ferrocarril (F0). Ese dinero no es
        // parte del Efectivo/Tarjeta de ESTA sucursal — el cruce ya lo
        // maneja aparte `cobros-sucursal-puente.service.js` (cuenta puente +
        // encolado para la sucursal cobradora). Sin este filtro, se sumaba
        // por partida doble: como Cargo normal aquí Y como cruce allá.
        if (centroPropioClave && cobro.claveCentro && cobro.claveCentro !== centroPropioClave) continue;
        const origen = (cobro.serieOrigen ?? '').toUpperCase();
        // 'CBT' NO es exclusivamente Puntos/Club Tuberos — confirmado con
        // datos reales 2026-08-06: un mismo cobro CBT puede traer
        // EFECTIVO/TARJETA/TRANSFERENCIA/SALDO A FAVOR mezclados (parece ser
        // un evento genérico de reclasificación/corrección de forma de pago,
        // no específico de monedero). Sumar TODO el cobro atribuía de más a
        // Puntos (caso real: $202,152 sumado vs $527.51 real filtrando por
        // nombre) — hay que filtrar por `nombre` cada `formaPago` individual.
        //
        // Corrección 2026-08-14 (caso real Atzompa E0-092, confirmado con el
        // usuario): este mismo filtro por texto (Puntos/Saldo a favor) SOLO
        // se aplicaba para origen CBT — un cobro `ABO` con un $654.99
        // etiquetado "SALDO A FAVOR" dentro de sus formasPago se colaba sin
        // filtrar, contándose como Tarjeta/Bancos real (no es Efectivo, cae
        // al bucket contrario) cuando ese monto YA se contabiliza aparte vía
        // `/saldos-favor` (`saldoFavorUsado`). El filtro ahora aplica igual
        // para CUALQUIER origen reconocido (CBT/ABO/CPF/CFC), no solo CBT.
        if (origen === 'CBT') {
          // Corrección 2026-08-07: las formasPago NO-Puntos dentro de un
          // cobro CBT (Efectivo/Tarjeta/Transferencia/SF) SÍ deben entrar al
          // `formasPago` general — antes se descartaban por completo, y una
          // factura cuyo ÚNICO cobro real viniera vía CBT (caso real:
          // transferencia BBVA $7,193.06, referencia Numo "034135") nunca
          // recibía su split, quedándose con el `formaPago` genérico
          // declarado por el CFDI en vez del real. El riesgo de origen (CBT
          // frecuentemente parcial, ver bug de Puntos de más arriba) ya no
          // se resuelve descartando el origen — se resuelve más abajo en
          // `cfdi-mapping.service.js` verificando que la suma de
          // `formasPago` encontradas coincida con el total del Cargo antes
          // de usarlas para partir (si no coincide, no se fuerza el split).
        } else if (origen === 'APS') {
          // 'APS' (2026-08-20, confirmado con el usuario contra datos reales
          // de Hidalgo/B0 — caso real folioOrigen 260800139/260800133): a
          // diferencia de 'APA' (mero espejo de atribución, sin dinero
          // nuevo, por eso NUNCA se agrega a SERIES_CON_AUTH), un cobro 'APS'
          // ES el pago real y primario del ticket cuando parte se cubrió con
          // dinero real y parte con saldo a favor ya existente (ej. $860.58
          // Tarjeta + $228.47 Saldo a Favor en un solo cobro APS, sin ningún
          // otro cobro ABO/CBT asociado a esa cuenta). Al no estar en
          // SERIES_CON_AUTH, el cobro completo — incluida su porción de
          // dinero real — se descartaba, faltando del corte de caja de
          // Efectivo/Tarjeta (parte de la brecha de $5,958.81 en Tarjeta de
          // Hidalgo 11-ago). La porción "saldo a favor" del texto de la
          // forma de pago se sigue filtrando abajo igual que en cualquier
          // otro origen — solo se deja pasar la porción de dinero real.
        } else if (!SERIES_CON_AUTH.includes(origen)) {
          continue;
        }
        // Bug del ERP (2026-08-14, confirmado con el usuario contra el
        // corte de caja real de Atzompa 09/07 y contra la consulta directa
        // por serie/folio): cuando UN pago real cierra varios tickets con
        // montos DESIGUALES (ej. real: $16.19 + $178.75 + $188.70 = $383.64
        // entre 3 tickets), este endpoint regresa `formasPago[].monto` con
        // el TOTAL del pago ($383.64) repetido en CADA ticket afectado, en
        // vez del monto real de cada uno — triplicando el monto real a
        // $1,150.92 (era ~86% del excedente de $892.03 encontrado en esa
        // Factura Global). El campo `cobro.monto` (nivel superior, no el de
        // `formasPago[]`) SÍ trae el monto real de ESE ticket específico
        // (confirmado: consulta directa por serie/folio del ticket
        // 260700883 devolvió `cobro.monto: -178.75`, exacto al corte de
        // caja, mientras `formasPago[0].monto` seguía en 383.64). Cuando el
        // cobro trae UN solo formaPago, `|cobro.monto|` y `fp.monto` deben
        // ser el mismo número en el caso normal (confirmado contra decenas
        // de cobros reales) — así que usar `cobro.monto` en ese caso corrige
        // el bug sin afectar los cobros normales. Con 2+ formasPago
        // mezclados no se puede hacer este reemplazo (ahí `cobro.monto` es
        // la suma de todos, no de uno solo), así que se deja `fp.monto` tal
        // cual para ese caso.
        const cobrosFormaPago = cobro.formasPago ?? [];
        for (const fp of cobrosFormaPago) {
          if (/puntos/i.test(fp.nombre ?? '')) { montoPuntos += Number(fp.monto) || 0; continue; }
          // "Saldo a favor" en el texto de la forma de pago se ignora aquí —
          // la fuente autoritativa para SF usado es `/saldos-favor`
          // (`saldoFavorUsado`, ver abajo), no este texto — mezclarlo en
          // `formasPago` lo trataría como Efectivo/Bancos real.
          if (/saldo\s*a\s*favor/i.test(fp.nombre ?? '')) continue;
          const monto = (cobrosFormaPago.length === 1 && cobro.monto != null)
            ? Math.abs(Number(cobro.monto) || 0)
            : (Number(fp.monto) || 0);
          // `serieVentaTicket`/`folioVentaTicket`: ticket real de cajas al que
          // pertenece ESTA porción del cobro (no la Factura Global que lo
          // agrupa) — confirmado con el usuario 2026-08-18 que cajas NO manda
          // número de autorización en este endpoint (`fp.autorizacion` no
          // existe); el dato real vive en `bank_movements.erpLinks` ligado
          // por serie+folioExterno DEL TICKET (verificado con datos reales:
          // Factura Global O0-260800164, 41 tickets, 6 BankMovements
          // distintos cada uno ligado a UN ticket específico vía erpLinks,
          // con su propio numeroAutorizacion). Permite que `consolidarCargos`
          // resuelva la autorización real POR TICKET (nunca por CFDI
          // completo, que fue justo el bug de Facturas Globales de Hidalgo
          // 2026-08-14) y agrupe Tarjeta por ella.
          formasPago.push({
            nombre: fp.nombre ?? null, claveSat: fp.claveSat ?? null, monto,
            serieVentaTicket: cuenta.serieVenta ?? null, folioVentaTicket: cuenta.folioVenta ?? null,
          });
        }
      }
      if (formasPago.length) {
        const prevFp = desglosePagoReal.get(key) ?? [];
        desglosePagoReal.set(key, [...prevFp, ...formasPago]);
      }
      if (montoPuntos > 0) puntosUsado.set(key, (puntosUsado.get(key) ?? 0) + montoPuntos);
    }

    // Saldo a favor generado y consumido DENTRO de la misma Factura Global
    // (2026-08-14, confirmado con el usuario, caso real Atzompa DEV-055175):
    // cuando la devolución ocurre ANTES de facturar la Global (el ticket
    // vendido ese mismo día aún no tiene CFDI al cual aplicársela), el ERP no
    // puede hacer una devolución normal — genera un saldo a favor temporal en
    // su lugar. Para cuando se factura la Global (al final del día), esa
    // devolución YA está reflejada: el ticket que la generó entra a la
    // Global con su monto NETO (post-devolución), no el original. Si ESE
    // MISMO ticket usa su propio saldo, restarla de nuevo aquí sería una
    // doble resta — el monto de la factura ya viene sin ese dinero contado.
    // OJO (2026-08-19, caso real confirmado: C0-260800403 generó DEV-056086
    // $89.15, usado en C0-260800431 $85.36 — TICKETS DISTINTOS dentro de la
    // misma Global de 200+ tickets): el neteo "monto NETO post-devolución"
    // solo aplica a la propia línea del ticket que generó el saldo — no a
    // cualquier OTRO ticket de la misma Global que después lo use. El cobro
    // real de ese otro ticket ya excluye ese monto (filtro "saldo a favor" en
    // formasPago, línea ~735) y nadie más lo resta, así que ese dinero
    // desaparecía del split de Cargo por completo. La clave para decidir
    // "ya viene neto" debe ser la VENTA (ticket) que generó el saldo, NO la
    // Factura Global completa — solo se excluye cuando el mismo ticket generó
    // Y usó su propio saldo. Para distinguir del caso normal (SF generado por
    // una Devolución de un día/factura ANTERIOR — ahí sí hay que restar,
    // confirmado con datos reales de la factura E0-092), se arma primero un
    // mapa `marcador → venta (ticket) que lo generó` recorriendo TODOS los
    // `saldosFavorGenerados` del batch.
    const _ventaGeneradoraPorMarcador = new Map();
    for (const cuenta of resultadosSaldos) {
      const ventaGenKey = `${cuenta.serieVenta}|${cuenta.folioVenta}`;
      for (const gen of (cuenta.saldosFavorGenerados ?? [])) {
        const marcador = `${(gen.serieOrigen ?? '').toUpperCase()}|${gen.folioOrigen ?? ''}`;
        _ventaGeneradoraPorMarcador.set(marcador, ventaGenKey);
      }
    }

    for (const cuenta of resultadosSaldos) {
      // Mismo criterio que arriba — agrupar por la factura real, no por el ticket.
      const key = `${cuenta.serieFactura || cuenta.serieVenta}|${cuenta.folioFactura || cuenta.folioVenta}`;
      const diaCfdi = diaCfdiPorClave.get(key);
      const ventaConsumidora = `${cuenta.serieVenta}|${cuenta.folioVenta}`;
      const usados = (cuenta.saldosFavorUsados ?? [])
        .filter(u => !diaCfdi || _diaMx(u.fecha) === diaCfdi)
        // Excluir SOLO el autoconsumo real (mismo ticket genera y usa su
        // propio saldo) — ver comentario arriba. Si el marcador no se
        // encuentra (Devolución con su propio CFDI, caso normal) o el ticket
        // que lo generó es DISTINTO al que lo usa, sí se resta.
        .filter(u => _ventaGeneradoraPorMarcador.get(`${(u.serieOrigen ?? '').toUpperCase()}|${u.folioOrigen ?? ''}`) !== ventaConsumidora);
      if (!usados.length) continue;
      const monto = usados.reduce((s, u) => s + (Math.abs(Number(u.montoUsado)) || 0), 0);
      if (monto <= 0) continue;
      const prevSF = saldoFavorUsado.get(key);
      saldoFavorUsado.set(key, {
        monto: (prevSF?.monto ?? 0) + monto,
        // `serieVenta`/`folioVenta`: la VENTA que generó el saldo (no el
        // marcador DEV/CAC) — para SF visible (periodo anterior), la columna
        // C debe mostrar esta referencia real de venta, no el marcador
        // (confirmado con el usuario 2026-08-18: DEV-055991/CAC-075406 no son
        // "serie y folio" auditables en cajas, la venta real sí lo es).
        detalle: [...(prevSF?.detalle ?? []), ...usados.map(u => ({
          serieOrigen: u.serieOrigen ?? null, folioOrigen: u.folioOrigen ?? null,
          monto: Math.abs(Number(u.montoUsado)) || 0,
          ventaSerie: u.serieVenta ?? null, ventaFolio: u.folioVenta ?? null,
        }))],
      });
    }
  }

  // Cobros de series ajenas cobrados FÍSICAMENTE en este centro (camino "por
  // centro", 2026-08-15): el endpoint devuelve cuentas cuya serieFactura/
  // folioFactura pertenece a otra sucursal (ej. D0|260800038) pero el cobro
  // fue físicamente en centroPropioClave. Esas entradas ya pasaron el filtro
  // de claveCentro (línea 588) — el cobro SÍ fue aquí — pero su clave no
  // coincide con ningún CFDI del batch, así que quedan en desglosePagoReal
  // sin usarse. Se recogen aquí como `cobrosCobradoraDirecta` para que el
  // generator pueda generar el Cargo+Abono de cobradora sin necesitar la cola
  // (`CobroSucursalPendiente`) ni esperar que la sucursal vendedora genere
  // primero. Solo aplica en el camino "por centro" — en el fallback por
  // serie/folio este caso nunca puede ocurrir (las cuentas foráneas no se
  // consultan).
  //
  // OJO: se itera `resultadosAlmacen` directamente (no `desglosePagoReal`)
  // para poder filtrar por fecha de cobro. La ventana ±1 día del endpoint
  // existe para facturas PROPIAS con facturación diferida — un cobro ajeno
  // debe ir en la póliza del día en que físicamente se cobró (cobro.fecha),
  // sin tolerancia adicional. Sin este filtro, cobros reales del día anterior
  // o siguiente (presentes en la ventana ampliada) aparecían en la póliza
  // incorrecta (caso real 2026-08-15: G1|260800010 del 6-ago en la póliza
  // del 5-ago, G1|260800005 del 4-ago idem, misma causa).
  let cobrosCobradoraDirecta = [];
  if (usoCaminoPorCentro) {
    // Derivar el prefijo de periodo del batch y el día exacto de la poliza.
    let folioPrefijoBatch = null;
    let diaPoliza = fechaDesde ? _diaMx(fechaDesde.toISOString()) : null;
    if (diaCfdiPorClave.size > 0) {
      const primeraClaveDelBatch = diaCfdiPorClave.keys().next().value;
      const folioDelBatch = primeraClaveDelBatch?.split('|')[1];
      folioPrefijoBatch = folioDelBatch?.substring(0, 4) ?? null; // "2608"
      if (!diaPoliza) diaPoliza = diaCfdiPorClave.get(primeraClaveDelBatch) ?? null;
    }

    for (const cuenta of resultadosAlmacen) {
      const serie = cuenta.serieFactura || cuenta.serieVenta;
      const folio = String(cuenta.folioFactura || cuenta.folioVenta || '');
      const k    = `${serie}|${folio}`;
      if (!serie || diaCfdiPorClave.has(k)) continue;                        // propio del batch
      if (serie === centroPropioClave) continue;                              // mismo centro
      if (folioPrefijoBatch && !folio.startsWith(folioPrefijoBatch)) continue; // periodo distinto

      for (const cobro of (cuenta.cobros ?? [])) {
        // Filtro de fecha estricto (diffDias > 0, sin tolerancia): el cobro
        // ajeno pertenece a la póliza del día en que se cobró físicamente, no
        // a la del CFDI de otra sucursal — la tolerancia ±1 no aplica aquí.
        if (diaPoliza) {
          const diffDias = _diferenciaDiasMx(cobro.fecha, diaPoliza);
          if (diffDias === null || diffDias > 0) continue;
        }
        if (centroPropioClave && cobro.claveCentro && cobro.claveCentro !== centroPropioClave) continue;
        const origen = (cobro.serieOrigen ?? '').toUpperCase();
        // 'APS' se acepta aquí igual que arriba (ver comentario 2026-08-20 en
        // el loop de `desglosePagoReal`) — es un cobro real mixto (dinero
        // real + saldo a favor), no un espejo como 'APA'.
        if (origen !== 'APS' && !SERIES_CON_AUTH.includes(origen)) continue;
        for (const fp of (cobro.formasPago ?? [])) {
          if (/puntos|saldo\s*a\s*favor/i.test(fp.nombre ?? '')) continue;
          const monto = (cobro.formasPago.length === 1 && cobro.monto != null)
            ? Math.abs(Number(cobro.monto) || 0)
            : (Number(fp.monto) || 0);
          if (monto <= 0) continue;
          // `claveFac` (serieFactura|folioFactura) es SOLO para resolver el CFDI
          // en Mongo (que indexa por folio SAT) — el texto que se le muestra al
          // contador (concepto) debe usar el documento relacionado real
          // (serieVenta/folioVenta, "serie y folio interno" auditable en cajas),
          // nunca el folio de la factura (confirmado con el usuario 2026-08-17:
          // mostrar el folio de factura llevaba a buscar un ticket equivocado en
          // Kore y comparar contra un saldo que no correspondía).
          const serFolTicket = `${cuenta.serieVenta || serie}-${cuenta.folioVenta ? String(cuenta.folioVenta) : folio}`;
          cobrosCobradoraDirecta.push({ claveSat: (fp.claveSat ?? '').trim() || null, monto, claveFac: k, serFolTicket, folioOrigen: cobro.folioOrigen ?? null });
        }
      }
    }

    // Enriquecer con nombre del receptor buscando en MongoDB por serie+folio
    if (cobrosCobradoraDirecta.length > 0) {
      const clavesUnicas = [...new Set(cobrosCobradoraDirecta.map(c => c.claveFac))];
      const serFolPairs = clavesUnicas.map(cv => { const [s, f] = cv.split('|'); return { serie: s, folio: f }; });
      const cfdisFac = await CFDI.find(
        { $or: serFolPairs },
        { serie: 1, folio: 1, 'receptor.nombre': 1, uuid: 1, metodoPago: 1 },
      ).lean();
      // OJO: la colección CFDI puede tener MÁS DE UN documento para el mismo
      // uuid/serie/folio (un "stub" incompleto sincronizado antes que el CFDI
      // completo, confirmado con el usuario 2026-08-17: caso real FILEMON
      // A0-260801889, dos documentos con el mismo uuid, uno sin `metodoPago`).
      // Un Map normal se queda con el ÚLTIMO valor visto sin importar cuál —
      // si el stub llega después, pisa el PPD real con `null` y el filtro de
      // abajo nunca lo detecta. Se recorre a mano prefiriendo SIEMPRE el
      // documento que sí trae el dato, sin importar el orden que regrese Mongo.
      const nombrePorClave     = new Map();
      const uuidPorClave       = new Map();
      const metodoPagoPorClave = new Map();
      for (const c of cfdisFac) {
        const key = `${c.serie}|${c.folio}`;
        if (c.receptor?.nombre) nombrePorClave.set(key, c.receptor.nombre);
        else if (!nombrePorClave.has(key)) nombrePorClave.set(key, null);
        if (c.uuid) uuidPorClave.set(key, c.uuid);
        else if (!uuidPorClave.has(key)) uuidPorClave.set(key, null);
        if (c.metodoPago) metodoPagoPorClave.set(key, c.metodoPago);
        else if (!metodoPagoPorClave.has(key)) metodoPagoPorClave.set(key, null);
      }
      for (const entry of cobrosCobradoraDirecta) {
        entry.nombre    = nombrePorClave.get(entry.claveFac) ?? null;
        entry.cfdiUuid  = uuidPorClave.get(entry.claveFac)   ?? null;
      }
      // PPD (Crédito): el cierre de esta CxC es responsabilidad exclusiva de
      // Cobranza, nunca de Ingreso — mismo criterio que `esPPD` en
      // cobros-sucursal-puente.service.js (confirmado con el usuario
      // 2026-08-05). Este camino ("por centro", 2026-08-15) no tenía el
      // filtro y dejaba pasar cobros de facturas PPD (confirmado con el
      // usuario 2026-08-17).
      cobrosCobradoraDirecta = cobrosCobradoraDirecta.filter(
        entry => metodoPagoPorClave.get(entry.claveFac) !== 'PPD',
      );
    }
  }

  return { desglosePagoReal, puntosUsado, saldoFavorUsado, cobrosCobradoraDirecta, usoCaminoPorCentro };
}

/**
 * Para cada CFDI tipo P (Complemento de Pago), extrae de
 * `complementoPago.pagos[].doctosRelacionados[]` la serie/folio/monto de
 * CADA factura que ese Pago liquida (`doctosPorUuid`, indexado por el UUID
 * del propio Pago) — consumido por `cfdiToMovimientos` (`context.doctosPago`)
 * para partir TANTO el Cargo como el Abono (columna H) en un asiento completo
 * POR FACTURA liquidada, con concepto "{cliente} / {serie}-{folio de esa
 * factura}" en vez de una sola línea agregada con el concepto genérico del
 * Pago completo (confirmado con el usuario 2026-08-11 — solo aplica a Pagos,
 * nunca a Ingreso/Egreso).
 *
 * Cada documento trae también su propio `montoSF` — lo consultado en
 * `/desgloses-cobro/saldos-favor` (mismo endpoint que
 * `_prefetchAjustesFacturaPropia` usa para tipo I) por la serie/folio de ESA
 * factura específica, para saber si esa liquidación puntual se pagó (parcial
 * o totalmente) con saldo a favor del cliente. A diferencia de tipo I, aquí
 * NO se fusiona en el mapa agregado de `esCasoAjusteSFPuntos` — cada factura
 * necesita su propio Cargo a Anticipos Otros individual, no uno agregado por
 * todo el Pago (confirmado con el usuario 2026-08-11, corrigiendo el diseño
 * inicial de esta función que sí agregaba por Pago).
 *
 * Simplificación consciente (confirmado con el usuario 2026-08-11): a
 * diferencia de `_prefetchAjustesFacturaPropia`, NO filtra por día ni
 * prorratea retenciones de IVA/ISR — un Pago PPD casi siempre ocurre días
 * después de la venta que liquida, así que filtrar por el día de la factura
 * descartaría el uso real; y este cliente no maneja Pagos con retención en la
 * práctica. Si eso cambia, hay que revisar ambos puntos.
 */
async function _prefetchDoctosPago(cfdiConRegla, rfc) {
  const pagos = cfdiConRegla.filter(({ cfdi }) => cfdi.tipoDeComprobante === 'P');
  const doctosPorUuid = new Map(); // uuid del Pago → [{ serie, folio, monto }]
  const paresVistos   = new Map(); // `${serie}|${folio}` → { serie, folio } (dedup entre Pagos)

  for (const { cfdi } of pagos) {
    const doctos = [];
    for (const pago of (cfdi.complementoPago?.pagos ?? [])) {
      for (const dr of (pago.doctosRelacionados ?? [])) {
        const serie = dr.serie ?? null;
        const folio = dr.folio ?? null;
        const monto = Number(dr.impPagado ?? 0);
        if (!serie || !folio || monto <= 0) continue;
        // IVA de ESTE doctoRelacionado — mismo cálculo que el agregado de
        // `iva` en cfdiToMovimientos (líneas ~558-565), pero por documento en
        // vez de sumado para todo el Pago, para poder partir el swap "IVA
        // cobrado" (Debe cuentaIvaPPD / Haber cuentaIva) por factura.
        const ivaDoc = (dr.trasladosDR ?? [])
          .filter(t => (t.impuesto || t.Impuesto || '') === '002' && Number(t.tasaOCuota ?? t.TasaOCuota ?? 0) > 0)
          .reduce((s, t) => s + Number(t.importe || t.importeDR || t.ImporteDR || 0), 0);
        doctos.push({ serie, folio, monto, montoSF: 0, ivaDoc });
        paresVistos.set(`${serie}|${folio}`, { serie, folio });
      }
    }
    if (doctos.length > 0 && cfdi.uuid) doctosPorUuid.set(cfdi.uuid, doctos);
  }

  if (paresVistos.size === 0) return { doctosPorUuid };

  const pares = [...paresVistos.values()];
  const LOTE  = 150;
  const saldoFavorPorFactura = new Map(); // `${serie}|${folio}` de la factura → monto usado
  for (let i = 0; i < pares.length; i += LOTE) {
    const lote = pares.slice(i, i + LOTE);
    const resultado = await obtenerSaldosFavor({ rfc, series: lote.map(p => p.serie), folios: lote.map(p => p.folio) });
    for (const cuenta of resultado) {
      const usados = cuenta.saldosFavorUsados ?? [];
      if (!usados.length) continue;
      const monto = usados.reduce((s, u) => s + (Math.abs(Number(u.montoUsado)) || 0), 0);
      if (monto > 0) saldoFavorPorFactura.set(`${cuenta.serieVenta}|${cuenta.folioVenta}`, monto);
    }
  }

  if (saldoFavorPorFactura.size > 0) {
    for (const doctos of doctosPorUuid.values()) {
      for (const d of doctos) {
        const sf = saldoFavorPorFactura.get(`${d.serie}|${d.folio}`);
        if (sf > 0) d.montoSF = sf;
      }
    }
  }

  return { doctosPorUuid };
}

/**
 * Arma las 2 líneas (Abono subtotal + Abono IVA) del saldo a favor generado
 * por una Devolución — ver `_prefetchSaldosFavorGenerados` (el monto viene
 * confirmado por el ERP, no es especulativo). Hasta 2026-08-10 estas 2 líneas
 * se dejaban deliberadamente sin ningún Cargo que las compensara (confirmado
 * con el usuario 2026-08-04: "el saldo a favor es un pasivo con saldo
 * corrido, no tiene que cuadrar el mismo día") — pero eso descuadraba el
 * asiento completo, porque el Abono normal de la Devolución (Caja por
 * identificar/Bancos/Clientes) ya contaba el mismo dinero como "salida de
 * efectivo" Y estas 2 líneas lo contaban OTRA VEZ como "pasivo nuevo". Ambas
 * cosas no pueden ser ciertas a la vez por el mismo monto. El caller (ver
 * `generarPropuesta`/`generarPolizaDesdeMovimientos`) ahora agrega una
 * tercera línea de Cargo que cierra exactamente ese Abono normal por el
 * monto que se convirtió en saldo a favor — el pasivo en sí (cuenta
 * Anticipos Otros) sigue con saldo corrido igual que antes; lo que cambia es
 * que ya no se duplica el mismo dinero dentro de ESTE asiento. Su consumo
 * futuro (cuando el saldo se aplica a otra venta) sigue sin tocarse aquí —
 * ver `usadosPorCuenta` en cobros-sucursal-puente.service.js.
 * Concepto/serie usan la VENTA que generó el saldo, no el folio propio de la
 * Devolución. Devuelve `[]` si esta Devolución no generó saldo a favor.
 *
 * Cruce de sucursal (2026-08-05): si `generado.centroProcesamiento` (dónde
 * se procesó la Devolución en sí, ver `_prefetchSaldosFavorGenerados`) existe
 * y es DISTINTO de `cc` (la sucursal dueña del CFDI), esta función NO puede
 * inyectar el asiento directo — cc es la única que puede ver este CFDI (su
 * propia serie), pero el asiento le corresponde a la otra sucursal. En vez de
 * eso, ENCOLA en `CobroSucursalPendiente` (mismo mecanismo que un cobro
 * cruzado normal) para que esa sucursal lo aplique al generar su propia
 * póliza, y regresa `[]` aquí (nada que inyectar en esta póliza — tampoco
 * hay Cargo de cierre que agregar en el caller, porque no hay líneas).
 */
async function _inyectarSaldoFavorGenerado({ cfdi, mapaGenerados, cuentaSaldoFavorId, cuentaIvaSaldoFavorId, cuentaCajaId, cuentaBancosId, cc, rfc }) {
  if (cfdi.tipoDeComprobante !== 'E' || !cuentaSaldoFavorId || !cuentaIvaSaldoFavorId) return [];
  const marcador = (cfdi.documentosRelacionados ?? [])
    .find(d => TIPO_MARCADORES_DEV.includes((d.Serie ?? '').toUpperCase()) && d.Folio);
  if (!marcador) return [];
  const generado = mapaGenerados.get(`${marcador.Serie}|${marcador.Folio}`);
  if (!generado?.monto) return [];

  const subtotal = Math.round((generado.monto / 1.16) * 100) / 100;
  const iva = Math.round((generado.monto - subtotal) * 100) / 100;
  const nombreCliente = cfdi.receptor?.nombre ?? 'CLIENTE NO IDENTIFICADO';
  const serieFolioVenta = [generado.ventaSerie, generado.ventaFolio].filter(Boolean).join('-') || null;
  const reglaSF = generado.oculto ? ETIQUETA_SALDO_FAVOR_OCULTO : 'SF';

  const centroReal = generado.centroProcesamiento;
  const esCruzado = !!(centroReal && (!cc || String(centroReal.id) !== String(cc.id)));
  // Siempre se sincroniza (no solo cuando hay cruce): si esta vez NO es
  // cruzado, limpia cualquier fila vieja de este folio en vez de dejarla
  // huérfana (ver `_sincronizarCobroSucursalPendiente`).
  await _sincronizarCobroSucursalPendiente({
    rfc,
    folioOrigen:          marcador.Folio,
    centroCostoIdOrigen:  cc?.id ?? null,
    centroCostoIdDestino: esCruzado ? centroReal.id : null,
    serieFolioTicket:     serieFolioVenta,
    cfdiUuid:             cfdi.uuid,
    nombreCliente,
    montoTotal:           subtotal + iva,
    lineas: esCruzado ? [
      { cuentaId: cuentaSaldoFavorId,    monto: subtotal, reglaNombre: reglaSF },
      { cuentaId: cuentaIvaSaldoFavorId, monto: iva,      reglaNombre: reglaSF },
    ] : [],
    tratamiento: 'SF_GENERADO',
    fechaCobro:  cfdi.fecha ?? null,
  });
  if (esCruzado) return [];

  const base = {
    concepto:      [nombreCliente, serieFolioVenta].filter(Boolean).join(' / '),
    serie:         serieFolioVenta,
    centroCosto:   cc?.clave ?? null,
    centroCostoId: cc?.id    ?? null,
    cfdiUuid:      cfdi.uuid,
    cuentaFaltante: false,
  };

  // Caso "mismo folio" (confirmado con el usuario 2026-08-13, ver
  // `_prefetchSaldosFavorGenerados`): la MISMA venta generó y consumió el
  // saldo — no es un pasivo real, es una salida de caja que ya volvió a
  // entrar. En vez de las 2 líneas de Anticipos Otros, se devuelve UNA sola
  // línea de Cargo NEGATIVO a Caja/Bancos (según la forma de pago real de esa
  // venta) — al llegar a `consolidarCargos` (poliza.service.js), que solo
  // agrupa por cuenta+centro+forma de pago y SUMA `debe`, este valor negativo
  // resta directo del total sin crear una fila propia (confirmado con el
  // usuario: "solo restarlo", sin desglose). `_ajusteConsolidadoSF` le indica
  // al caller (generarPropuesta/generarYGuardar) que trate esta línea como
  // el equivalente del Abono SF para efectos de cerrar/cancelar el Abono
  // normal de la Devolución (mismo mecanismo de cuadre por asiento que ya
  // existe para el caso normal, ver comentario junto al caller).
  // Alcance: solo si además NO hay cruce de sucursal (`esCruzado` ya
  // descartado arriba) — la combinación mismoFolio+cruzado no tiene caso real
  // confirmado con el usuario; si aparece, cae al comportamiento normal de
  // abajo (pasivo vía Anticipos Otros).
  if (generado.mismoFolio) {
    const esTarjeta      = generado.formaPagoReal === '04' || generado.formaPagoReal === '28';
    const cuentaAjusteId = esTarjeta ? cuentaBancosId : cuentaCajaId;
    if (cuentaAjusteId) {
      return [{
        ...base,
        cuentaId:    cuentaAjusteId,
        tipoOrigen:  'Ajuste Consolidado SF',
        reglaNombre: 'SF-MISMO-FOLIO',
        formaPago:   generado.formaPagoReal ?? null,
        debe:        -(subtotal + iva),
        haber:       0,
        _ajusteConsolidadoSF: true,
      }];
    }
    // Sin cuenta de Caja/Bancos disponible: no hay dónde ajustar el
    // consolidado — cae al comportamiento normal (pasivo) en vez de perder el
    // registro por completo.
  }

  return [
    // tipoOrigen='Cobro Sucursal' (NO un tipo propio) — a propósito: solo así
    // pasa por `_extraerCobrosSucursal` (poliza.service.js), que arma columna
    // C = "SF" (por `reglaNombre`) SIN reconstruir el concepto — a diferencia
    // de `bloquesAbonosNormales`+`enriquecerConceptoConCliente` (el camino que
    // toma cualquier otro tipoOrigen), que SÍ reconstruye el concepto a partir
    // de `serie` — poner serie="SF" ahí corrompía el concepto a "cliente / SF"
    // en vez de "cliente / I0-260700186" (confirmado con el usuario 2026-08-04).
    // 'SF-OCULTO' cuando se generó y se consumió por completo el mismo día
    // en el mismo almacén — ver `_prefetchSaldosFavorGenerados` — para que
    // `_extraerCobrosSucursal` la omita del export (sigue en poliza_movimientos).
    { ...base, cuentaId: cuentaSaldoFavorId,    tipoOrigen: 'Cobro Sucursal', reglaNombre: reglaSF, debe: 0, haber: subtotal },
    { ...base, cuentaId: cuentaIvaSaldoFavorId, tipoOrigen: 'Cobro Sucursal', reglaNombre: reglaSF, debe: 0, haber: iva },
  ];
}

// Cuentas fijas para los cobros cruzados de sucursales (ver
// cobros-sucursal-puente.service.js). PUE usa directamente Caja/Bancos por
// identificar en ambos lados; PPD usa la cuenta puente (2103040001) para el
// asiento adicional que cierra Clientes contra el Cargo/Abono cruzado.
// Saldo a favor (cualquiera de los dos) usa Anticipos Otros (2103090001) +
// IVA Trasladado - Anticipos (2104010002), con columna C = "SF".
const CODIGO_CUENTA_PUENTE_SUCURSALES = '2103040001';
const CODIGO_CUENTA_CAJA              = '1101010003';
const CODIGO_CUENTA_BANCOS            = '1102011005';
const CODIGO_CUENTA_SALDO_FAVOR       = '2103090001';
const CODIGO_CUENTA_IVA_SALDO_FAVOR   = '2104010002';
// Monedero electrónico Club Tuberos — "PUNTOS" como forma de pago (cliente
// paga/aplica su saldo del monedero) usa esta cuenta para el subtotal, con el
// mismo split 16% e IVA compartido (2104010002) que usa Saldo a Favor —
// confirmado con el usuario 2026-08-06.
const CODIGO_CUENTA_CLUB_TUBEROS      = '2103090002';
// Anticipos de clientes (2103010001) + IVA Trasladado Anticipos (2104010002,
// misma cuenta que CODIGO_CUENTA_IVA_SALDO_FAVOR) — usados cuando una factura
// tipo I trae `cfdiRelacionados` tipoRelacion='07' (aplicación de anticipo)
// pero la regla que le tocó NO es una regla de anticipo (sin `cuentaIvaAnticipo`,
// ver "Fix doble-contabilización anticipo PUE" más abajo): el ERP no emite una
// Nota de Crédito que cancele el anticipo, así que sin este ajuste el Cargo se
// iba completo a Clientes en vez de cancelar el pasivo de Anticipos (2026-08-19,
// confirmado con el usuario, caso real CONSTRUCTORA CARRASCO ORTEGA
// C0-260800064/065 contra el anticipo C0-260701665).
const CODIGO_CUENTA_ANTICIPOS_CLIENTES = '2103010001';
const CODIGO_CUENTA_IVA_ANTICIPO       = '2104010002';

// Referencia real del RECIBO del anticipo (ej. "OPA-00766") — el ERP la
// identifica con su propia serie/folio interno en `bank_movements.erpLinks`,
// SIN relación directa con el folio de la factura de anticipo ni con
// `folioFiscal` (confirmado con el usuario 2026-08-19, caso real:
// transferencia BBVA $689,000, `erpLinks: [{serie:'OPA', folioExterno:'00766',
// total: 689000}]`, encontrada solo por monto exacto — ni por folioFiscal ni
// por el folio de la factura/ticket). Se busca por el TOTAL exacto de la
// factura de anticipo dentro de una ventana de ±5 días de su fecha. Si no se
// encuentra (aún no sincronizado con bancos), el caller cae al placeholder de
// serie-folio de la factura.
async function _resolverReferenciaOpaPorMonto(anticiposCfdi) {
  const mapa = {};
  for (const c of anticiposCfdi) {
    const totalAnticipo = Number(c.total) || 0;
    if (totalAnticipo <= 0 || !c.fecha) continue;
    const fechaAnticipo = new Date(c.fecha);
    const VENTANA_MS = 5 * 24 * 3600 * 1000;
    const bm = await BankMovement.findOne({
      fecha: { $gte: new Date(fechaAnticipo.getTime() - VENTANA_MS), $lte: new Date(fechaAnticipo.getTime() + VENTANA_MS) },
      'erpLinks.total': { $gte: totalAnticipo - 0.01, $lte: totalAnticipo + 0.01 },
    }).select('erpLinks').lean();
    const link = (bm?.erpLinks ?? []).find(l => Math.abs((Number(l.total) || 0) - totalAnticipo) < 0.01);
    if (link?.serie && link?.folioExterno) {
      mapa[c.uuid.toUpperCase()] = `${link.serie}-${link.folioExterno}`;
    }
  }
  return mapa;
}
// Mismo texto que TIPO_ORIGEN_CARGO_ESPECIAL en cfdi-mapping.service.js/
// poliza.service.js (duplicado a propósito) — usado aquí solo para la línea
// consolidada de Puntos del batch (ver `puntosAcumuladosProp` más abajo).
const TIPO_ORIGEN_CARGO_ESPECIAL      = 'Cargo Especial';

// UUIDs cuyo Cargo YA está cubierto por una línea 'Cobro Sucursal' guardada
// en CUALQUIER póliza no cancelada (de cualquier día) — complementa el
// `facturasVendedorCubiertas` que devuelve `construirMovimientosPuente`, que
// solo detecta lo del MISMO día que se está generando. Necesario cuando una
// Factura Global se reconoce como Venta un día distinto al que se cobraron
// sus tickets (ej. Global emitida el 11/07, tickets cobrados el 10/07): sin
// esto, el día 11 nunca ve que esos tickets ya cubrieron su Cargo y termina
// duplicándolo contra el Abono de Ingresos/IVA (confirmado con el usuario
// 2026-08-04, Global I0-260700155).
// Devuelve uuid → monto Cargo YA cubierto (suma de `debe`) por líneas
// 'Cobro Sucursal' guardadas en CUALQUIER póliza no cancelada de días previos
// — corrección 2026-08-06: antes era un Set (solo sí/no), tratando cualquier
// cobertura previa como "cargo completo cubierto". Para una Factura Global,
// eso perdía el resto del cargo (tickets de la MISMA sucursal) — ver
// docstring de `facturasVendedorCubiertas` en cobros-sucursal-puente.service.js.
async function _uuidsConCargoCubiertoEnBD({ rfc }) {
  const rows = await PolizaMovimiento.findAll({
    where: { tipoOrigen: 'Cobro Sucursal', cfdiUuid: { [Op.ne]: null } },
    attributes: ['cfdiUuid', 'debe'],
    include: [{
      model:      Poliza,
      as:         'poliza',
      attributes: [],
      where:      { rfc, estado: { [Op.ne]: 'cancelada' } },
      required:   true,
    }],
    raw: true,
  });
  const mapa = new Map();
  for (const r of rows) {
    const uuid = r.cfdiUuid?.toUpperCase();
    if (!uuid) continue;
    mapa.set(uuid, (mapa.get(uuid) ?? 0) + (Number(r.debe) || 0));
  }
  return mapa;
}

async function _resolverCuentasPuenteSucursales() {
  const rows = await AccountPlan.findAll({
    where:      { codigo: [CODIGO_CUENTA_PUENTE_SUCURSALES, CODIGO_CUENTA_CAJA, CODIGO_CUENTA_BANCOS, CODIGO_CUENTA_SALDO_FAVOR, CODIGO_CUENTA_IVA_SALDO_FAVOR, CODIGO_CUENTA_CLUB_TUBEROS] },
    attributes: ['id', 'codigo'],
    raw:        true,
  });
  const byCodigo = Object.fromEntries(rows.map(r => [r.codigo, r.id]));
  return {
    cuentaPuenteId:        byCodigo[CODIGO_CUENTA_PUENTE_SUCURSALES] ?? null,
    cuentaCajaId:          byCodigo[CODIGO_CUENTA_CAJA] ?? null,
    cuentaBancosId:        byCodigo[CODIGO_CUENTA_BANCOS] ?? null,
    cuentaSaldoFavorId:    byCodigo[CODIGO_CUENTA_SALDO_FAVOR] ?? null,
    cuentaIvaSaldoFavorId: byCodigo[CODIGO_CUENTA_IVA_SALDO_FAVOR] ?? null,
    cuentaClubTuberosId:   byCodigo[CODIGO_CUENTA_CLUB_TUBEROS] ?? null,
  };
}

// Universo de CFDIs para que construirMovimientosPuente resuelva documentos
// relacionados (cliente/metodoPago de la venta original) SIN restringirlo al
// día que se está generando — a diferencia de `cfdisSinPolizaFinal`, que sí
// se acota al día (ver `uuidsPorFechaEfectiva` en generarPropuesta/
// generarYGuardar). Necesario porque el CFDI que trae el `documentoRelacionado`
// (ej. una Factura Global que agrupa varios tickets) puede fecharse un día
// DESPUÉS del día real en que se cobraron esos tickets — sin este universo
// amplio, esos cobros nunca se detectan al generar por día (confirmado con el
// usuario 2026-08-04: ticket I0-260700183/184/etc., cobrados el 10/07, solo
// referenciados por la Global I0-260700155 fechada el 11/07). El filtro por
// fecha REAL del cobro (no del CFDI) se aplica después, dentro de
// construirMovimientosPuente, vía `cobro.fecha` — ver fechaDesde/fechaHasta.
//
// Se acota a `serie` (la propia serie de facturación del centro que se está
// generando) — el "documento relacionado" de una venta SIEMPRE sale de un
// CFDI de la MISMA serie que vendió (nunca de quien solo cobró) — ampliar a
// las 11 sucursales saturaba el ERP (429 Too Many Requests) y, aun con
// caché+reintento, tardaba 5+ minutos en la primera sucursal generada de
// cada periodo (confirmado con el usuario 2026-08-05). Se abandonó ese
// enfoque: el lado COBRADOR de un cobro cruzado ya NO se resuelve buscando
// el CFDI de la vendedora desde aquí — se resuelve vía la cola
// `CobroSucursalPendiente` (ver `_encolarCobroSucursalPendiente`/
// `_aplicarCobrosSucursalPendientes` más abajo): cuando ESTA función corre
// para la sucursal VENDEDORA (con su propia serie, rápido de siempre) y
// `construirMovimientosPuente` detecta el cobro cruzado, encola lo que la
// sucursal cobradora necesita para su asiento — sin volver a tocar el ERP
// cuando esa sucursal genere su propia póliza.
async function _fetchCfdisParaPuenteAmplio({ rfc, ejercicio, periodo, tipoCfdi, serie }) {
  const satCfdis = await CFDI.find({
    'emisor.rfc':      rfc,
    ejercicio:         Number(ejercicio),
    periodo:           Number(periodo),
    tipoDeComprobante: tipoCfdi,
    source:            'SAT',
    satStatus:         'Vigente',
    isActive:          true,
    ...(serie ? { serie } : {}),
  }).select('uuid documentosRelacionados receptor metodoPago serie folio fecha').lean();

  const uuids = satCfdis.map(c => c.uuid);
  const erpCfdis = uuids.length
    ? await CFDI.find({ uuid: { $in: uuids }, source: 'ERP' }).select('uuid documentosRelacionados metodoPago').lean()
    : [];
  const erpMap = Object.fromEntries(erpCfdis.map(c => [c.uuid, c]));

  // Filtrar aquí (no dejárselo solo a construirMovimientosPuente) para no
  // acarrear miles de CFDIs sin documento relacionado del periodo completo —
  // la mayoría no trae ninguno.
  //
  // `metodoPago` también se fusiona con ERP (no solo `documentosRelacionados`):
  // SAT muchas veces NO trae metodoPago propio (undefined) — sin este merge,
  // `esPPD` en construirMovimientosPuente caía a `false` para esos CFDIs y el
  // cobro se procesaba como PUE (Cargo directo a Bancos, sin Abono a Clientes)
  // en vez de PPD (cuenta puente + Abono) — confirmado con el usuario
  // 2026-08-04, HERROZINC I0-260700082: quedó como asiento descuadrado
  // ($19,037.47 de Cargo sin ninguna contrapartida).
  return satCfdis
    .map(c => {
      const erp = erpMap[c.uuid];
      return {
        ...c,
        documentosRelacionados: erp?.documentosRelacionados ?? c.documentosRelacionados ?? [],
        metodoPago:             c.metodoPago ?? erp?.metodoPago ?? null,
      };
    })
    .filter(c => c.documentosRelacionados.length > 0);
}

// Rango de folios de CONTPAQ reservado por sucursal (confirmado con el
// usuario 2026-07-27) — cada póliza de Ingreso nueva de esa sucursal toma el
// siguiente folio libre DENTRO de su rango, nunca fuera de él. El rango se
// reinicia cada periodo (ejercicio+periodo): el primer folio de la sucursal
// en un mes nuevo siempre es `desde`. Nombres deben coincidir EXACTO con
// CentroCosto.sucursal (ver seed-account-plan.js) — comparación case-insensitive.
const RANGOS_FOLIO_POR_SUCURSAL = [
  { sucursales: ['CEDIS', 'PROMOTORIA'],          desde: 100,  hasta: 199 },
  { sucursales: ['HIDALGO', 'LICITACION HIDALGO'], desde: 300,  hasta: 399 },
  { sucursales: ['CONSTRUCASA'],                  desde: 400,  hasta: 499 },
  { sucursales: ['REFORMA'],                       desde: 500,  hasta: 599 },
  { sucursales: ['SIMBOLOS'],                      desde: 600,  hasta: 699 },
  { sucursales: ['FERROCARRIL'],                   desde: 700,  hasta: 799 },
  { sucursales: ['ATZOMPA'],                       desde: 800,  hasta: 899 },
  { sucursales: ['TEHUANTEPEC'],                   desde: 900,  hasta: 999 },
  { sucursales: ['SANTA ROSA'],                    desde: 1000, hasta: 1099 },
  { sucursales: ['VIGUERA'],                        desde: 1100, hasta: 1199 },
  { sucursales: ['PUERTO ESCONDIDO'],               desde: 1200, hasta: 1299 },
];

function _rangoFolioPorSucursal(sucursal) {
  if (!sucursal) return null;
  const norm = String(sucursal).trim().toUpperCase();
  return RANGOS_FOLIO_POR_SUCURSAL.find(r => r.sucursales.includes(norm)) ?? null;
}

// Folio siguiente disponible dentro del rango de la sucursal — a diferencia
// de un contador que solo sube, aquí una póliza CANCELADA libera su folio
// para que una futura generación lo reutilice (confirmado con el usuario
// 2026-07-27). Una póliza de Ingreso con Contado Y Crédito mezclados ocupa
// DOS folios consecutivos en el export (numero y numero+1, ver
// _conceptoConTipoVenta/bloques en poliza.service.js) aunque en Postgres solo
// exista una fila — por eso se deriva de sus propios movimientos (metodoPago)
// si cada póliza activa ocupó 1 o 2 folios, en vez de asumirlo.
// CEDIS es un caso aparte: además de Contado/Crédito separa Bonificaciones y
// Descuentos/Devoluciones en pólizas propias (ver rama `esCedis` en
// poliza.service.js), consumiendo hasta 6 folios consecutivos desde una sola
// fila de Postgres. Replicar aquí cuántos de esos 6 bloques tuvieron datos
// requeriría duplicar la categorización completa del export, así que para
// una póliza CEDIS (pasada o nueva) siempre se reservan/bloquean los 6 —
// conservador pero sin riesgo de colisión.
const FOLIOS_MAX_CEDIS = 6;

function _esCedisPorSucursal(sucursal) {
  return (sucursal || '').trim().toUpperCase() === 'CEDIS';
}

async function _folioSiguienteDisponible({ tipoPropuesta, rfc, ejercicio, periodo, rangoFolio, foliosNecesarios, ccBySerieMap, transaction }) {
  if (!rangoFolio) {
    const max = await Poliza.max('numero', {
      where: { tipo: tipoPropuesta, rfc, ejercicio: Number(ejercicio), periodo: Number(periodo) },
      transaction,
    });
    return { numero: (max || 0) + 1, agotado: false };
  }

  const activas = await Poliza.findAll({
    where: {
      tipo: tipoPropuesta, rfc, ejercicio: Number(ejercicio), periodo: Number(periodo),
      estado: { [Op.ne]: 'cancelada' },
      numero: { [Op.between]: [rangoFolio.desde, rangoFolio.hasta] },
    },
    attributes: ['id', 'numero'],
    include: [{ model: PolizaMovimiento, as: 'movimientos', attributes: ['metodoPago', 'centroCostoId'], required: false }],
    transaction,
  });

  const sucursalDeCentro = (id) => Object.values(ccBySerieMap ?? {}).find(c => String(c.id) === String(id))?.sucursal ?? null;

  const ocupados = new Set();
  for (const p of activas) {
    const movs = p.movimientos ?? [];
    const esCedisPast = movs.length > 0 && movs.every(m => _esCedisPorSucursal(sucursalDeCentro(m.centroCostoId)));
    if (esCedisPast) {
      for (let i = 0; i < FOLIOS_MAX_CEDIS; i++) ocupados.add(p.numero + i);
      continue;
    }
    const metodos = new Set(movs.map(m => m.metodoPago));
    const tieneContado = [...metodos].some(m => m !== 'PPD');
    const tieneCredito = metodos.has('PPD');
    ocupados.add(p.numero);
    if (tieneContado && tieneCredito) ocupados.add(p.numero + 1);
  }

  const necesarios = foliosNecesarios ?? 1;
  for (let n = rangoFolio.desde; n <= rangoFolio.hasta - necesarios + 1; n++) {
    let libre = true;
    for (let i = 0; i < necesarios; i++) {
      if (ocupados.has(n + i)) { libre = false; break; }
    }
    if (libre) return { numero: n, agotado: false };
  }
  return { numero: null, agotado: true };
}

// Series de documentosRelacionados (ERP) que identifican una NC como ajuste
// de una venta (bonificación/devolución/cancelación) — mismo catálogo que
// TIPO_MARCADORES de report.controller.js, más 'CANCELACION' (NCs de
// refacturación por cancelación, encontradas 2026-07-17: no traen
// cfdiRelacionados.tipoRelacion poblado, solo este indicador del ERP).
// 'DVE' = "Devolución Especial", variante real del ERP (10 casos, no un typo
// de 'DEV') que tampoco trae cfdiRelacionados.tipoRelacion a nivel SAT —
// sin este marcador la NC nunca se fusiona con ningún día/sucursal
// (encontrado 2026-07-23, UUID B576F5AE-EE3E-449A-B654-9C1A6F44141A).
// 'BEP'/'BXC'/'BN' = variantes de Bonificación (Especial / Por Cambio /
// Cliente Mostrador); 'ANN'/'CES' = variantes de Cancelación (de Anticipos /
// Especial) -- mismo problema de fusión, tipoOrigen ya viene correcto del
// ERP ("Bonificación"/"Cancelación") pero sin este marcador tampoco se
// fusionan (BEP: 5 casos, BN: 42 casos, CES: 1 caso confirmados en base;
// BXC/ANN sin casos aún pero se agregan igual, aportadas por el usuario
// 2026-07-23).
const SERIES_FUSION_NC = ['BCT', 'BON', 'DEV', 'DVE', 'CAC', 'CANCELACION', 'BEP', 'BXC', 'BN', 'ANN', 'CES'];

// Folios (del ERP) referenciados por NCs Serie=CANCELACION dentro del rango
// de fecha efectiva — usado para detectar la factura de REFACTURACIÓN que
// reemplaza la venta cancelada (misma serie que la NC, mismo Folio
// referenciado). Cruce exacto por Folio: NO basta con "documentosRelacionados
// trae algún Serie=propia con Folio distinto al mío" — ese patrón es normal
// en casi cualquier factura del ERP (dato interno sin relación con
// cancelaciones), confirmado con datos reales 2026-07-17 (8 de 9 facturas de
// un día cualquiera lo tenían, falso positivo masivo).
async function _foliosCancelacionDelDia({ rfc, ejercicio, periodo, fechaInicio, fechaFin }) {
  if (!fechaInicio || !fechaFin) return new Set();
  const uuidsE = await _uuidsPorFechaEfectiva({ rfc, ejercicio, periodo, tipoCfdi: 'E', fechaInicio, fechaFin });
  if (!uuidsE.size) return new Set();
  const docs = await CFDI.find({
    uuid:   { $in: [...uuidsE] },
    source: 'ERP',
    'documentosRelacionados.Serie': 'CANCELACION',
  }).select('documentosRelacionados').lean();
  const foliosRaw = new Set();
  for (const d of docs) {
    for (const dr of d.documentosRelacionados || []) {
      if (dr.Serie === 'CANCELACION' && dr.Folio) foliosRaw.add(dr.Folio);
    }
  }
  return foliosRaw;
}

function _fmtDMY(fechaISO) {
  const [y, m, d] = fechaISO.split('-');
  return `${d}/${m}/${y}`;
}

function _ultimoDiaDelMes(ejercicio, periodo) {
  return new Date(Date.UTC(Number(ejercicio), Number(periodo), 0)).toISOString().slice(0, 10);
}

// Concepto de pólizas de Ingreso (Contado/Crédito): base sin el calificativo de
// tipo de venta — poliza.service.js lo inserta al exportar según el bloque
// (Contado/Crédito), así la fecha/sucursal salen de esta única fuente y nunca
// se desincronizan entre el encabezado (columna B) y el concepto (columna G).
function _construirConceptoIngresoBase({ centroCostoId, ccBySerieMap, fechaInicio, fechaFin, ejercicio, periodo }) {
  const centro = centroCostoId
    ? Object.values(ccBySerieMap).find(c => String(c.id) === String(centroCostoId))
    : null;
  const sucursal = centro?.sucursal || SUCURSAL_DEFAULT;
  let rango;
  if (fechaInicio && fechaFin && fechaInicio !== fechaFin) {
    rango = `Día: ${_fmtDMY(fechaInicio)} al ${_fmtDMY(fechaFin)}`;
  } else if (fechaInicio) {
    rango = `Día: ${_fmtDMY(fechaInicio)}`;
  } else {
    rango = `Día: ${_fmtDMY(`${ejercicio}-${String(periodo).padStart(2, '0')}-01`)} al ${_fmtDMY(_ultimoDiaDelMes(ejercicio, periodo))}`;
  }
  return `Ingresos por Ventas Suc. ${sucursal} ${rango}`;
}

/**
 * Enriquece en memoria el campo `tasaIvaInferida` de CFDIs tipo P Metadata
 * cuyos UUIDs relacionados se encuentran en erp_cuentas_pendientes.
 * Replica la misma lógica del bloque ERP en balanza-preliminar.service.js.
 * Muta los objetos del array — NO escribe a MongoDB.
 */
async function _enrichTasaIvaErp(cfdis) {
  const sinTasa = cfdis.filter(c =>
    c.tasaIvaInferida == null &&
    !c.complementoPago?.pagos?.length &&
    c.cfdiRelacionados?.length,
  );
  if (!sinTasa.length) return;

  const uuidToIdxs = new Map();
  for (let i = 0; i < sinTasa.length; i++) {
    const uuids = (sinTasa[i].cfdiRelacionados ?? [])
      .flatMap(r => r.uuids ?? [])
      .flatMap(u => u.split(/\s*\|\s*/))
      .map(u => u.trim().toUpperCase())
      .filter(u => u.length >= 32);
    for (const uuid of uuids) {
      if (!uuidToIdxs.has(uuid)) uuidToIdxs.set(uuid, []);
      uuidToIdxs.get(uuid).push(i);
    }
  }
  if (!uuidToIdxs.size) return;

  const erpDocs = await ErpCuentaPendiente.find(
    { folioFiscal: { $in: [...uuidToIdxs.keys()] } },
    { folioFiscal: 1, factorImpuesto: 1, impuesto: 1, subtotal: 1 },
  ).lean();
  if (!erpDocs.length) return;

  const tasasPorIdx = new Map();
  for (const erp of erpDocs) {
    const uuidNorm = (erp.folioFiscal || '').trim().toUpperCase();
    const tasa = erp.factorImpuesto != null
      ? (erp.factorImpuesto > 0 ? '16' : '0')
      : (erp.subtotal > 0 && erp.impuesto != null
          ? (erp.impuesto > 0 ? '16' : '0') : null);
    if (!tasa) continue;
    for (const idx of (uuidToIdxs.get(uuidNorm) ?? [])) {
      if (!tasasPorIdx.has(idx)) tasasPorIdx.set(idx, []);
      tasasPorIdx.get(idx).push(tasa);
    }
  }
  for (const [idx, tasas] of tasasPorIdx) {
    const tiene16 = tasas.some(t => t === '16' || t === 'mixto');
    const tiene0  = tasas.some(t => t === '0'  || t === 'mixto');
    sinTasa[idx].tasaIvaInferida =
      (tiene16 && tiene0) ? 'mixto' : tiene16 ? '16' : tiene0 ? '0' : null;
  }
}

/**
 * Busca las Notas de Crédito (tipo E, tipoRelacion 01/03 — devolución,
 * descuento, bonificación, Club Tuberos) relacionadas a las facturas de
 * Ingreso de este batch, para FUSIONARLAS en la misma póliza de Ingreso en
 * vez de generarse como póliza de Egreso aparte. Confirmado con el usuario:
 * las NC deben vivir dentro de la póliza de la venta que ajustan, no en una
 * póliza de Egreso independiente — así el bloque Contado/Crédito de
 * `poliza.service.js` las puede agrupar y colorear junto a esa venta.
 *
 * Excluye NC que ya tengan movimiento en una póliza activa (mismo criterio
 * que `uuidsYaUsados`, ya resuelto por el caller).
 *
 * fechaInicio/fechaFin (opcionales, generación por día): cuando se generan
 * pólizas por día, la NC debe vivir en la póliza de SU PROPIO día (fecha
 * efectiva ERP/SAT — confirmado con el usuario), no en la del día de la
 * factura que ajusta si cae en un día distinto. Sin fechaInicio/fechaFin
 * (generación de todo el periodo) no se filtra por fecha — comportamiento
 * previo sin cambios.
 *
 * centroCostoId (opcional, generación por sucursal): en modo por día, la NC
 * solo se fusiona si SU PROPIA serie pertenece a esta sucursal — si no, se
 * excluye aquí y se recoge cuando le toque generarse la póliza de su propia
 * sucursal. Requiere `ccBySerieMap` (mapa serie→centro ya resuelto por el
 * caller, se reutiliza para no pagar una consulta extra a Postgres).
 *
 * @param {Array} facturasI - CFDIs tipo I ya cargados/enriquecidos de este batch
 * @param {string} rfc
 * @param {Set<string>} uuidsYaUsados - uuids (mayúsculas) con póliza activa
 * @param {{ejercicio?: number, periodo?: number, fechaInicio?: string, fechaFin?: string, centroCostoId?: string|number, ccBySerieMap?: object}} [opts]
 */
async function _fetchNotasCreditoParaFusion(facturasI, rfc, uuidsYaUsados, opts = {}) {
  const { ejercicio, periodo, fechaInicio, fechaFin, centroCostoId, ccBySerieMap } = opts;
  const facturaUuids = facturasI.map(c => c.uuid).filter(Boolean);

  const relUuidsDe = (c) => (c.cfdiRelacionados ?? [])
    .filter(r => r.tipoRelacion === '01' || r.tipoRelacion === '03')
    .flatMap(r => r.uuids ?? (r.uuid ? [r.uuid] : []));

  // Solo NCs EMITIDAS por esta entidad — una NC donde `rfc` es receptor es una
  // que le dieron a esta entidad como CLIENTE (compra), no una que emitió
  // sobre sus propias ventas. Mismo bug de fondo que ya se corrigió en
  // notInErp/discrepanciasMontos/discrepanciasCriticas (faltaba filtro
  // emisor.rfc) — confirmado con el usuario 2026-07-28 al encontrar un CFDI
  // recibido colado en una póliza de Ingresos vía este mismo patrón de $or.
  const filtroBaseNc = {
    'emisor.rfc':      rfc,
    tipoDeComprobante: 'E',
    source:            'SAT',
    satStatus:         'Vigente',
    isActive:          true,
  };
  const selectNc = 'uuid tipoDeComprobante metodoPago formaPago fecha folio serie emisor receptor subTotal total descuento impuestos complementoPago conceptos cfdiRelacionados tasaIvaInferida';

  let ncs;
  if (fechaInicio && fechaFin) {
    // Generación por día: la NC debe vivir en la póliza de SU PROPIO día
    // (fecha efectiva ERP/SAT — confirmado con el usuario), sin importar si
    // la factura que ajusta cayó en un día distinto y por tanto no está en
    // `facturasI` de este batch. Por eso se busca DIRECTO por la fecha
    // efectiva de la NC, no partiendo de la relación con las facturas del
    // lote (que nunca encontraría una NC cuya factura ya se generó en otro
    // día).
    const uuidsNcDelDia = await _uuidsPorFechaEfectiva({ rfc, ejercicio, periodo, tipoCfdi: 'E', fechaInicio, fechaFin });
    if (!uuidsNcDelDia.size) return [];

    // Algunas NCs (Club Tuberos, cancelaciones-refacturación) no traen
    // cfdiRelacionados.tipoRelacion poblado a nivel SAT — solo se identifican
    // por el indicador documentosRelacionados del ERP (hallazgo 2026-07-17,
    // UUIDs 102A4165.../79B7BB10... no se fusionaban por esto). Se calcula
    // el set de UUIDs con ese indicador para incluirlas también.
    const erpConIndicador = await CFDI.find({
      uuid:   { $in: [...uuidsNcDelDia] },
      source: 'ERP',
      'documentosRelacionados.Serie': { $in: SERIES_FUSION_NC },
    }).select('uuid').lean();
    const uuidsConIndicadorErp = erpConIndicador.map(d => d.uuid);

    const ncsRaw = await CFDI.find({
      ...filtroBaseNc,
      uuid: { $in: [...uuidsNcDelDia] },
      $or: [
        { 'cfdiRelacionados.tipoRelacion': { $in: ['01', '03'] } },
        { uuid: { $in: uuidsConIndicadorErp } },
      ],
    })
      .select(selectNc)
      .lean();
    ncs = ncsRaw.filter(nc => !uuidsYaUsados.has((nc.uuid || '').toUpperCase()));
    // Generación por sucursal (centroCostoId presente): la NC solo debe
    // fusionarse aquí si SU PROPIA serie pertenece a esta sucursal — si no,
    // se estaba colando en la póliza de una sucursal ajena (bug real
    // reportado: "seleccioné solo Atzompa y me manda de más sucursales").
    if (centroCostoId && ccBySerieMap) {
      ncs = ncs.filter(nc => String(ccBySerieMap[nc.serie]?.id ?? '') === String(centroCostoId));
    }
  } else {
    // Generación de todo el periodo: comportamiento original, por relación
    // con las facturas ya cargadas en este batch. NOTA: a diferencia del
    // branch por día, aquí NO se agrega el indicador documentosRelacionados
    // (BCT/CANCELACION/etc.) — esas NCs no tienen cfdiRelacionados.uuid, así
    // que el matching por relUuidsDe()/facturaSet nunca las encontraría de
    // todos modos; requeriría matching por serie+folio, no por UUID. Pendiente
    // si se necesita generación de periodo completo (no por día) con estas NCs.
    if (!facturaUuids.length) return [];
    const ncsRaw = await CFDI.find({ ...filtroBaseNc, 'cfdiRelacionados.tipoRelacion': { $in: ['01', '03'] } }).select(selectNc).lean();
    const facturaSet = new Set(facturaUuids.map(u => u.toUpperCase()));
    ncs = ncsRaw.filter(nc =>
      !uuidsYaUsados.has((nc.uuid || '').toUpperCase()) &&
      relUuidsDe(nc).some(u => facturaSet.has((u || '').toUpperCase())),
    );
  }
  if (!ncs.length) return [];

  await repararSubtotalDesdeXml(ncs);

  // Enriquecer con ERP — mismo patrón que el resto del pipeline.
  const uuidsSinMeta = ncs
    .filter(c => !c.formaPago || !c.metodoPago || !c.conceptos?.length)
    .map(c => c.uuid);
  let erpMetaMap = {};
  if (uuidsSinMeta.length) {
    const erpCfdis = await CFDI.find({ uuid: { $in: uuidsSinMeta }, source: 'ERP' })
      .select('uuid formaPago metodoPago conceptos impuestos tipoOrigen cfdiRelacionados documentosRelacionados').lean();
    erpMetaMap = Object.fromEntries(erpCfdis.map(c => [c.uuid, c]));
  }
  const ncsEnriquecidas = ncs.map(cfdi => {
    const erp = erpMetaMap[cfdi.uuid];
    if (!erp) return cfdi;
    const satHasTraslados = cfdi.conceptos?.some(con => con.impuestos?.traslados?.length);
    const relSAT     = cfdi.cfdiRelacionados ?? [];
    const tiposEnSAT = new Set(relSAT.map(r => r.tipoRelacion));
    const relERP     = (erp.cfdiRelacionados ?? []).filter(r => !tiposEnSAT.has(r.tipoRelacion));
    const esBCT = erp.documentosRelacionados?.some(d => d.Serie === 'BCT');
    const esBON = !esBCT && erp.documentosRelacionados?.some(d => (d.Serie ?? '').startsWith('BON'));
    return {
      ...cfdi,
      formaPago:              cfdi.formaPago  || erp.formaPago,
      metodoPago:             cfdi.metodoPago || erp.metodoPago,
      conceptos:              satHasTraslados ? cfdi.conceptos : (erp.conceptos?.length ? erp.conceptos : cfdi.conceptos ?? []),
      impuestos:              erp.impuestos ?? cfdi.impuestos,
      tipoOrigen:             esBCT ? 'Bonificación Club Tuberos' : esBON ? 'Bonificación' : (cfdi.tipoOrigen ?? erp.tipoOrigen ?? null),
      documentosRelacionados: erp.documentosRelacionados ?? cfdi.documentosRelacionados ?? [],
      cfdiRelacionados:       relERP.length ? [...relSAT, ...relERP] : relSAT,
    };
  });

  _normalizarEgresoPue99(ncsEnriquecidas);

  // metodoPago/formaPago reales de la factura relacionada: primero se busca
  // entre las ya cargadas en este mismo batch (sin costo extra de query); lo
  // que falte (factura de otro periodo) se resuelve con una consulta puntual.
  // metodoPagoPorFactura (solo metodoPago) alimenta _normalizarEgresoCondonacion
  // (formaPago=15); facturaRelacionadaMeta (metodoPago+formaPago) alimenta
  // _normalizarEgresoSegunFacturaRelacionada (medios de pago reales).
  const metodoPagoPorFactura   = Object.fromEntries(facturasI.map(c => [(c.uuid || '').toUpperCase(), c.metodoPago]));
  const facturaRelacionadaMeta = Object.fromEntries(facturasI.map(c => [(c.uuid || '').toUpperCase(), { metodoPago: c.metodoPago, formaPago: c.formaPago }]));
  const faltantes = [...new Set(ncsEnriquecidas.flatMap(relUuidsDe))]
    .map(u => (u || '').toUpperCase())
    .filter(u => !(u in metodoPagoPorFactura));
  if (faltantes.length) {
    const extra = await CFDI.find({ uuid: { $in: faltantes } }).select('uuid metodoPago formaPago').lean();
    for (const f of extra) {
      const uuidUp = (f.uuid || '').toUpperCase();
      metodoPagoPorFactura[uuidUp]   = f.metodoPago;
      facturaRelacionadaMeta[uuidUp] = { metodoPago: f.metodoPago, formaPago: f.formaPago };
    }
  }
  _normalizarEgresoCondonacion(ncsEnriquecidas, metodoPagoPorFactura);
  _normalizarEgresoSegunFacturaRelacionada(ncsEnriquecidas, facturaRelacionadaMeta);

  return ncsEnriquecidas;
}

/**
 * Genera una PROPUESTA de póliza a partir de los CFDIs vigentes del periodo
 * que aún no tienen movimiento contable registrado.
 *
 * No guarda nada en base de datos — devuelve el objeto para que el
 * frontend lo muestre en el modal de revisión.
 */
const LIMITE_CFDIS = 500;
const CHUNK_SIZE   = 200;

async function generarPropuesta({ rfc, ejercicio, periodo, tipoPropuesta = 'D', tipoCfdi, centroCostoId, fechaInicio, fechaFin, formaPagoFiltro }) {
  if (!rfc)       throw new BadRequestError('RFC requerido');
  if (!ejercicio) throw new BadRequestError('Ejercicio requerido');
  if (!periodo)   throw new BadRequestError('Periodo requerido');
  if (!tipoCfdi)  throw new BadRequestError('Debes seleccionar el tipo de CFDI a procesar (I, E o P)');

  // 1. UUIDs ya contabilizados — solo los del RFC solicitado (JOIN con polizas)
  const yaContabilizados = await PolizaMovimiento.findAll({
    // tipoOrigen != 'Cobro Sucursal': esas líneas solo REFERENCIAN el CFDI
    // (ej. una Factura Global citada por construirMovimientosPuente como
    // `cfdiOriginal` de varios tickets cruzados) — no registran su propia
    // venta. Sin esta exclusión, en cuanto la Global se cita una vez queda
    // "ya contabilizada" para siempre y su propia línea de Venta (Ingresos +
    // IVA) nunca se genera en ningún día (confirmado con el usuario
    // 2026-08-04: Global I0-260700155 nunca aparecía como Venta).
    // reglaNombre != 'COS': mismo problema, otra variante — el Cargo del
    // mecanismo "cobrosCobradoraDirecta" (línea ~2506) etiqueta su propia
    // mitad del par cruzado como tipoOrigen='Venta' (a propósito, para que
    // entre al consolidado de Depósitos de la sucursal COBRADORA), pero eso
    // la deja indistinguible de la Venta real de la Factura Global — vuelve a
    // marcarla "ya contabilizada" para siempre en la sucursal VENDEDORA (caso
    // real confirmado 2026-08-17: VIGUERA N0-260800019 nunca generaba su
    // línea de Venta porque el ticket 260800046 cobrado en otra sucursal ya
    // había insertado esta fila). 'COS' es el único reglaNombre que usa este
    // mecanismo (nunca lo usa una regla de mapeo real). `[Op.or]` con
    // `reglaNombre: null` porque `!=` en SQL no matchea NULL — sin esto,
    // cualquier Venta normal con reglaNombre NULL se excluiría por error de
    // "ya contabilizados" (nunca se marcaría como ya usada).
    where: {
      cfdiUuid:   { [Op.ne]: null },
      tipoOrigen: { [Op.ne]: 'Cobro Sucursal' },
      [Op.or]:    [{ reglaNombre: { [Op.ne]: 'COS' } }, { reglaNombre: null }],
    },
    attributes: ['cfdiUuid'],
    include: [{
      model:      Poliza,
      as:         'poliza',
      attributes: [],
      where:      { rfc, estado: { [Op.ne]: 'cancelada' } },
      required:   true,
    }],
    raw: true,
  });
  const uuidsYaUsados = new Set(yaContabilizados.map(m => m.cfdiUuid));

  // 2. CFDIs vigentes del periodo filtrados por tipo
  // Proyección mínima: solo los campos que necesita cfdiToMovimientos
  // fechaInicio/fechaFin (opcionales): acotan el periodo a un rango de días
  // específico — usado por `generarYGuardarPorDia` y por el filtro manual de
  // fecha en la UI. Sin ellos, el comportamiento es el de siempre (mes completo).
  // El filtro usa la fecha EFECTIVA (ERP cuando existe, SAT si no — ver
  // `_uuidsPorFechaEfectiva`), no el `fecha` crudo de SAT.
  const uuidsPorFechaProp = (fechaInicio && fechaFin)
    ? await _uuidsPorFechaEfectiva({ rfc, ejercicio, periodo, tipoCfdi, fechaInicio, fechaFin })
    : null;
  const foliosCancelacionProp = await _foliosCancelacionDelDia({ rfc, ejercicio, periodo, fechaInicio, fechaFin });
  // Solo CFDIs EMITIDOS por esta entidad — con receptor.rfc en el filtro se
  // colaban compras/gastos (CFDIs donde `rfc` es cliente de un tercero) dentro
  // de la póliza de Ingresos de sus propias ventas (confirmado con el usuario
  // 2026-07-28, mismo patrón de bug ya corregido en notInErp/discrepancias*).
  const filtroBase = {
    'emisor.rfc':      rfc,
    ejercicio:         Number(ejercicio),
    periodo:           Number(periodo),
    tipoDeComprobante: tipoCfdi,
    source:            'SAT',
    satStatus:         'Vigente',
    ...(uuidsPorFechaProp ? { uuid: { $in: [...uuidsPorFechaProp] } } : {}),
    isActive:          true,
  };

  const totalEncontrados = await CFDI.countDocuments(filtroBase);
  if (totalEncontrados > LIMITE_CFDIS) {
    throw new BadRequestError(
      `Se encontraron ${totalEncontrados} CFDIs tipo ${tipoCfdi} en este periodo. ` +
      `El límite por operación es ${LIMITE_CFDIS}. Divide el proceso por fuente (ERP/SAT) ` +
      `o contacta a soporte para procesamiento por lotes.`,
    );
  }

  const cfdis = await CFDI.find(filtroBase)
    .select('uuid tipoDeComprobante metodoPago formaPago fecha folio serie emisor receptor subTotal total descuento impuestos complementoPago conceptos cfdiRelacionados lastComparisonStatus tasaIvaInferida')
    .lean();

  await repararSubtotalDesdeXml(cfdis);

  // Filtro por forma de pago (solo Cobranza/Pagos) — ver `FORMA_PAGO_A_CATEGORIA`.
  const cfdisSinPoliza = cfdis.filter(c =>
    !uuidsYaUsados.has(c.uuid) &&
    (!formaPagoFiltro || tipoCfdi !== 'P' || FORMA_PAGO_A_CATEGORIA[_formaPagoResuelta(c)] === formaPagoFiltro),
  );

  // Antes ambos casos (cero CFDIs encontrados vs. todos ya poliza'dos) tiraban
  // el mismo mensaje "ya tienen póliza registrada" — confuso cuando en
  // realidad no se encontró ningún CFDI (ej. día sin facturas al generar "por
  // día"): no había nada que contabilizar, no que ya estuviera contabilizado.
  if (cfdis.length === 0) {
    const rango = (fechaInicio && fechaFin)
      ? (fechaInicio === fechaFin ? `el día ${fechaInicio}` : `el rango ${fechaInicio} a ${fechaFin}`)
      : `el periodo ${periodo}/${ejercicio}`;
    throw new BadRequestError(`No se encontró ningún CFDI tipo ${tipoCfdi} para ${rango}`);
  }
  if (cfdisSinPoliza.length === 0) {
    throw new BadRequestError('Todos los CFDIs vigentes del periodo ya tienen póliza registrada');
  }

  // 3. Cargar reglas activas (cacheadas 60s)
  const rules = await _getRulesActive();

  // 4. Pre-fetch tipoDeComprobante de CFDIs relacionados para discriminador relacionadoTipo
  // (r.uuid singular o r.uuids array — cfdiRelacionados usa ambas formas según el origen).
  const relTipoUuidsProp = [...new Set(
    cfdisSinPoliza
      .flatMap(c => (c.cfdiRelacionados || []).flatMap(r => r.uuids ?? (r.uuid ? [r.uuid] : []))),
  )];
  const relTipoCfdisArr = relTipoUuidsProp.length
    ? await CFDI.find({ uuid: { $in: relTipoUuidsProp } })
        .select('uuid tipoDeComprobante metodoPago formaPago').lean()
    : [];
  const relTipoMap = Object.fromEntries(relTipoCfdisArr.map(c => [c.uuid, c.tipoDeComprobante]));
  // uuid de factura → su metodoPago — usado por _normalizarEgresoCondonacion
  // para resolver el metodoPago real de NCs formaPago=15 (Condonación).
  const relMetodoPagoMap = Object.fromEntries(relTipoCfdisArr.map(c => [c.uuid, c.metodoPago]));
  // uuid de factura → metodoPago+formaPago — usado por
  // _normalizarEgresoSegunFacturaRelacionada (medios de pago reales).
  const relFacturaMetaMap = Object.fromEntries(relTipoCfdisArr.map(c => [c.uuid, { metodoPago: c.metodoPago, formaPago: c.formaPago }]));

  // Inyectar _relacionadoTipo en cada CFDI antes del matching
  const cfdisSinPolizaEnriquecidos = cfdisSinPoliza.map(cfdi => {
    const primerUuid = (cfdi.cfdiRelacionados || [])[0]?.uuid;
    return primerUuid && relTipoMap[primerUuid]
      ? { ...cfdi, _relacionadoTipo: relTipoMap[primerUuid] }
      : cfdi;
  });

  // Enriquecer CFDIs SAT con datos del homólogo ERP — misma lógica que balanza-preliminar
  // para que el matching de reglas produzca movimientos idénticos a la balanza.
  const uuidsSinMeta = new Set(
    cfdisSinPolizaEnriquecidos
      .filter(c => c.uuid && (
        !c.formaPago ||
        !c.metodoPago ||
        !c.conceptos?.length ||
        c.conceptos.every(con => !(con.impuestos?.traslados?.length)) ||
        (c.tipoDeComprobante === 'I' && c.metodoPago === 'PPD') ||
        // Enriquecer también sustitutos (tipoRelacion='04'): se conservan en
        // la póliza y necesitan formaPago/conceptos/tipoOrigen del ERP.
        (['E', 'P'].includes(c.tipoDeComprobante) && c.cfdiRelacionados?.length > 0)
      ))
      .map(c => c.uuid),
  );
  let erpMetaMap = {};
  if (uuidsSinMeta.size) {
    const erpCfdis = await CFDI.find({
      uuid:   { $in: [...uuidsSinMeta] },
      source: 'ERP',
    }).select('uuid formaPago metodoPago conceptos impuestos tipoOrigen cfdiRelacionados documentosRelacionados').lean();
    erpMetaMap = Object.fromEntries(erpCfdis.map(c => [c.uuid, c]));
  }
  const cfdisSinPolizaFinal = cfdisSinPolizaEnriquecidos.map(cfdi => {
    const erp = erpMetaMap[cfdi.uuid];
    if (!erp) return cfdi;
    const satHasTraslados     = cfdi.conceptos?.some(con => con.impuestos?.traslados?.length);
    const satHasBaseTraslados = (cfdi.impuestos?.traslados ?? []).some(t => (t.base ?? 0) > 0);
    const relSAT    = cfdi.cfdiRelacionados ?? [];
    const tiposEnSAT = new Set(relSAT.map(r => r.tipoRelacion));
    const relERP    = (erp.cfdiRelacionados ?? []).filter(r => !tiposEnSAT.has(r.tipoRelacion));
    const metodoPagoFinal = (cfdi.metodoPago === 'PPD' && erp.metodoPago === 'PUE')
      ? 'PUE' : (cfdi.metodoPago || erp.metodoPago);
    const esBCT = erp.documentosRelacionados?.some(d => d.Serie === 'BCT');
    const esBON = !esBCT && erp.documentosRelacionados?.some(d => (d.Serie ?? '').startsWith('BON'));
    // Refacturación (factura nueva que reemplaza una venta cancelada, misma
    // serie/sucursal, Folio referenciado coincide con el que referencia una
    // NC Serie=CANCELACION del mismo día) — confirmado con el usuario
    // 2026-07-17: su cargo (dinero en banco/caja) ya está contabilizado en el
    // asiento de la CANCELACION original, así que no debe consolidarse como
    // depósito nuevo (ver `esRefacturacion` en poliza.service.js). Cruce
    // exacto por Folio contra `foliosCancelacionProp` — ver comentario en
    // `_foliosCancelacionDelDia`. Prerequisito adicional: el CFDI debe declarar
    // `tipoRelacion='04'` (Sustitución) en sus `cfdiRelacionados` SAT — sin
    // esto, las devoluciones de tickets individuales dentro de un FG disparan
    // falsos positivos (caso real Atzompa 2026-08-03: tickets 260800116/135
    // devueltos ese día aparecen en `foliosCancelacion` Y en el docRel del FG,
    // marcando el FG entero como refacturación cuando en realidad es el FG
    // diario normal con tickets cancelados). Una refacturación real siempre
    // declara `tipoRelacion='04'` en el XML SAT; un FG con devoluciones no.
    const esRefacturacion = !esBCT && !esBON &&
      !!cfdi.cfdiRelacionados?.some(r => r.tipoRelacion === '04') &&
      erp.documentosRelacionados?.some(d => d.Serie === cfdi.serie && foliosCancelacionProp.has(d.Folio));
    return {
      ...cfdi,
      formaPago:              cfdi.formaPago  || erp.formaPago,
      metodoPago:             metodoPagoFinal,
      conceptos:              satHasTraslados     ? cfdi.conceptos : (erp.conceptos?.length ? erp.conceptos : cfdi.conceptos ?? []),
      impuestos:              satHasBaseTraslados  ? cfdi.impuestos : (erp.impuestos ?? cfdi.impuestos),
      tipoOrigen:             esBCT ? 'Bonificación Club Tuberos' : esBON ? 'Bonificación' : esRefacturacion ? 'Refacturación' : (cfdi.tipoOrigen ?? erp.tipoOrigen ?? null),
      documentosRelacionados: erp.documentosRelacionados ?? cfdi.documentosRelacionados ?? [],
      cfdiRelacionados:       relERP.length ? [...relSAT, ...relERP] : relSAT,
    };
  });

  // Completar relMetodoPagoMap/relFacturaMetaMap con relacionados que solo
  // aparecieron tras el merge ERP — ver `_completarRelacionadosPostMerge`.
  await _completarRelacionadosPostMerge(cfdisSinPolizaFinal, relMetodoPagoMap, relFacturaMetaMap);

  // Enriquecer tasaIvaInferida en memoria para CFDIs P Metadata.
  // Paso 1: facturas relacionadas en MongoDB SAT. Paso 2: fallback ERP.
  if (tipoCfdi === 'P') {
    await _enrichTasaIvaFromRelatedCfdis(cfdisSinPolizaFinal);
    await _enrichTasaIvaErp(cfdisSinPolizaFinal);
  }

  // Normalización: E PUE formaPago=99 → PPD (en memoria, antes de matching)
  _normalizarEgresoPue99(cfdisSinPolizaFinal);
  // Normalización: E formaPago=15 (Condonación) → metodoPago real de la factura relacionada
  _normalizarEgresoCondonacion(cfdisSinPolizaFinal, relMetodoPagoMap);
  // Normalización: E con medio de pago real (Efectivo/Cheque/Transferencia/Tarjeta)
  // que ajusta una factura PPD nunca cobrada → formaPago+metodoPago de esa factura.
  _normalizarEgresoSegunFacturaRelacionada(cfdisSinPolizaFinal, relFacturaMetaMap);

  // Excluir el CFDI cancelado cuando existe un sustituto (tipoRelacion='04').
  const _canceladosPorSustitutoProp = new Set(
    cfdisSinPolizaFinal
      .filter(c => c.cfdiRelacionados?.some(r => r.tipoRelacion === '04'))
      .flatMap(c => (c.cfdiRelacionados || [])
        .filter(r => r.tipoRelacion === '04')
        .flatMap(r => r.uuids ?? (r.uuid ? [r.uuid] : []))
        .flatMap(_splitUuids)
        .map(u => u.toUpperCase())
      )
  );
  // Sustitutos cuyo original es de un periodo ya cerrado (o no se pudo
  // determinar su periodo) se excluyen y se listan aparte (`sustitutos`) para
  // revisión manual — ver _particionarSustitutosPorRiesgo. Los del MISMO
  // periodo (`mismoPeriodo`) se contabilizan automático más abajo: el
  // sustituto entra normal al batch, y el original se agrega aparte con su
  // asiento + reversión (ver `cfdisOriginalesCanceladosProp`).
  const sustitutosEnriquecidosProp = await _enriquecerSustitutosConPeriodoOriginal(_extraerSustitutos(cfdisSinPolizaFinal));
  const { excluidos: sustitutosClasificadosProp } = _particionarSustitutosPorRiesgo(sustitutosEnriquecidosProp, { uuidsYaUsados, ejercicio, periodo });
  const sustitutosMismoPeriodoProp = sustitutosClasificadosProp.filter(s => s.mismoPeriodo);
  const sustitutosProp = sustitutosClasificadosProp.filter(s => !s.mismoPeriodo);
  const _uuidsSustitutosExcluidosProp = new Set(sustitutosProp.map(s => s.uuid?.toUpperCase()).filter(Boolean));

  const cfdisSinPolizaFinalFiltradoSustituto = (_canceladosPorSustitutoProp.size || _uuidsSustitutosExcluidosProp.size)
    ? cfdisSinPolizaFinal.filter(c =>
        !_canceladosPorSustitutoProp.has(c.uuid?.toUpperCase() ?? '') &&
        !_uuidsSustitutosExcluidosProp.has(c.uuid?.toUpperCase() ?? '')
      )
    : cfdisSinPolizaFinal;

  // Centro de costo por serie de facturación del CFDI (asignación automática).
  // Se resuelve aquí (antes del matching de reglas) para poder filtrar por
  // sucursal cuando se pide una sola, y se reutiliza más abajo para etiquetar
  // cada movimiento — evita una segunda consulta a Postgres.
  const ccBySerieMapProp = await centrosSvc.resolveBySerieMap();

  const cfdisSinPolizaFinalFiltrado = centroCostoId
    ? cfdisSinPolizaFinalFiltradoSustituto.filter(c =>
        String(ccBySerieMapProp[c.serie]?.id ?? '') === String(centroCostoId),
      )
    : cfdisSinPolizaFinalFiltradoSustituto;

  if (centroCostoId && cfdisSinPolizaFinalFiltrado.length === 0) {
    const totalSinPoliza = cfdisSinPolizaFinalFiltradoSustituto.length;
    const totalCfdis     = cfdis.length;
    const totalUsados    = cfdis.filter(c => uuidsYaUsados.has(c.uuid)).length;
    const seriesEncontradas = [...new Set(cfdisSinPolizaFinalFiltradoSustituto.map(c => c.serie).filter(Boolean))].join(', ') || '(ninguna)';
    throw new BadRequestError(
      `No hay CFDIs sin póliza para la sucursal seleccionada en este periodo. ` +
      `(Total CFDIs del periodo: ${totalCfdis}, ya en póliza: ${totalUsados}, sin póliza: ${totalSinPoliza}, series disponibles: ${seriesEncontradas})`,
    );
  }

  // Fusionar NC (tipo E) relacionadas a estas facturas en la MISMA póliza de
  // Ingreso — ver _fetchNotasCreditoParaFusion. Se agregan al final del batch
  // 'I'; el resto del pipeline (matching de reglas, cfdiToMovimientos) ya
  // maneja tipos mixtos genéricamente.
  const cfdisConNCSinReversionProp = tipoCfdi === 'I'
    ? [...cfdisSinPolizaFinalFiltrado, ...await _fetchNotasCreditoParaFusion(cfdisSinPolizaFinalFiltrado, rfc, uuidsYaUsados, { ejercicio, periodo, fechaInicio, fechaFin, centroCostoId, ccBySerieMap: ccBySerieMapProp })]
    : cfdisSinPolizaFinalFiltrado;

  // Originales cancelados-con-sustitución del MISMO periodo (ver
  // `sustitutosMismoPeriodoProp` arriba) — se agregan al batch para que
  // pasen por el mismo matching de reglas y `cfdiToMovimientos` que
  // cualquier CFDI normal (su asiento "como si estuviera vigente"); el
  // asiento de reversión que lo cancela se agrega después de generar todos
  // los movimientos (ver `sustitutosMismoPeriodoProp` más abajo, tras el
  // loop principal).
  const cfdisOriginalesCanceladosProp = sustitutosMismoPeriodoProp.length
    ? await CFDI.find({ uuid: { $in: sustitutosMismoPeriodoProp.flatMap(s => s.sustituyeA) } })
        .select('uuid tipoDeComprobante metodoPago formaPago fecha folio serie emisor receptor subTotal total descuento impuestos complementoPago conceptos cfdiRelacionados lastComparisonStatus tasaIvaInferida')
        .lean()
    : [];
  const cfdisConNCProp = [...cfdisConNCSinReversionProp, ...cfdisOriginalesCanceladosProp];

  // Póliza de Pagos (Cobranza): ordenar por folio del Pago ascendente — sin
  // esto el orden es el que regresa Mongo (esencialmente arbitrario), lo que
  // resultaba en saltos entre centros de costo sin ningún criterio visible
  // (confirmado con el usuario 2026-08-11). Solo tipo P — Ingreso/Egreso no
  // se tocan.
  if (tipoCfdi === 'P') {
    cfdisConNCProp.sort((a, b) => Number(a.folio) - Number(b.folio));
  }

  // Las NC fusionadas (_fetchNotasCreditoParaFusion) se agregan DESPUÉS del
  // primer `_completarRelacionadosPostMerge` — completar de nuevo (solo agrega
  // lo que aún falte) para que su `context.metodoPagoRelacionado` también se
  // resuelva más abajo.
  await _completarRelacionadosPostMerge(cfdisConNCProp, relMetodoPagoMap, relFacturaMetaMap);

  // Serie propia de esta sucursal — se calcula aquí (antes de lo que la
  // necesita: `_prefetchSaldosFavorGenerados` y, más abajo,
  // `_prefetchAjustesFacturaPropia`) como `centroPropioClave` para consultar
  // por centro+fecha en vez de por serie/folio propio.
  let serieDelCentroProp = centroCostoId
    ? Object.entries(ccBySerieMapProp).find(([, cc]) => String(cc.id) === String(centroCostoId))?.[0]
    : null;

  // Saldos a favor generados por las Devoluciones de este batch — ANTES de
  // construirMovimientosPuente porque `devsOcultos` (ver
  // `_prefetchSaldosFavorGenerados`) debe llegar a esa llamada, para que el
  // lado de "uso" también sepa marcar como oculto el mismo par
  // generación+uso "lavado" el mismo día en el mismo almacén.
  const { mapa: mapaSaldosFavorGeneradosProp, devsOcultos: devsOcultosSFProp, ajustesEfectivoRetiroSF: ajustesEfectivoRetiroSFProp } = await _prefetchSaldosFavorGenerados(cfdisConNCProp, rfc, ccBySerieMapProp, {
    centroPropioClave: serieDelCentroProp,
    fechaDesde: fechaInicio ? _medianocheMx(fechaInicio) : null,
    fechaHasta: fechaFin   ? new Date(_medianocheMx(_diaSiguiente(fechaFin)).getTime() - 1) : null,
  });

  // Cobros de sucursales (Caja/Bancos por identificar, ver
  // cobros-sucursal-puente.service.js) — solo aplica a pólizas de Ingreso.
  // Se calcula ANTES del loop de reglas (más abajo) porque
  // `facturasVendedorCubiertas` se usa ahí para omitir el Cargo normal de las
  // facturas cuyo Cargo ya cubre este flujo — si no se omite, la póliza queda
  // con doble Cargo (uno normal + uno de sucursal) contra un solo Abono.
  // Usa cfdisSinPolizaFinal (SIN filtrar por centro) para poder detectar el
  // cobro cruzado en cualquier dirección; el propio construirMovimientosPuente
  // filtra las líneas según centroCostoId.
  //
  // Al generar POR DÍA (fechaInicio/fechaFin), el universo de CFDIs para
  // resolver documentos relacionados se amplía a TODO el periodo (ver
  // `_fetchCfdisParaPuenteAmplio`) — el cobro se filtra por su fecha REAL
  // (fechaDesde/fechaHasta, dentro de construirMovimientosPuente), no por la
  // fecha del CFDI que lo referencia.
  let movsPuente = [];
  let facturasVendedorCubiertas = new Map(); // uuid → monto ya cubierto (ver docstring en cobros-sucursal-puente.service.js)
  let facturasPPDCubiertas = new Map();
  let pendientesPorFacturarProp = [];
  let cuentaSaldoFavorIdProp = null;
  let cuentaIvaSaldoFavorIdProp = null;
  if (tipoCfdi === 'I' && centroCostoId) {
    const { cuentaPuenteId, cuentaCajaId, cuentaBancosId, cuentaSaldoFavorId, cuentaIvaSaldoFavorId, cuentaClubTuberosId } = await _resolverCuentasPuenteSucursales();
    cuentaSaldoFavorIdProp = cuentaSaldoFavorId;
    cuentaIvaSaldoFavorIdProp = cuentaIvaSaldoFavorId;
    if (cuentaCajaId && cuentaBancosId) {
      // Acotado a la serie propia para ESTA consulta — el lado cobrador ya no
      // depende SOLO de ampliar esta consulta (ver `_fetchCfdisParaPuenteAmplio`
      // y la cola `CobroSucursalPendiente`): cuando se genera por día, además
      // se consulta directo por centro+fecha vía `centroPropioClave` (ver
      // comentario en `construirMovimientosPuente`).
      const serieDelCentro = serieDelCentroProp;
      const cfdisParaPuente = (fechaInicio && fechaFin)
        ? await _fetchCfdisParaPuenteAmplio({ rfc, ejercicio, periodo, tipoCfdi, serie: serieDelCentro })
        : cfdisSinPolizaFinal;
      const resultadoPuente = await construirMovimientosPuente({
        cfdis: cfdisParaPuente,
        centroCostoId,
        ccBySerieMap: ccBySerieMapProp,
        cuentaCajaId,
        cuentaBancosId,
        cuentaPuenteId,
        cuentaSaldoFavorId,
        cuentaIvaSaldoFavorId,
        cuentaClubTuberosId,
        rfc,
        fechaDesde: fechaInicio ? _medianocheMx(fechaInicio) : null,
        fechaHasta: fechaFin ? new Date(_medianocheMx(_diaSiguiente(fechaFin)).getTime() - 1) : null,
        devsOcultosSF: devsOcultosSFProp,
        centroPropioClave: serieDelCentro,
      });
      movsPuente = resultadoPuente.movimientos;
      facturasVendedorCubiertas = resultadoPuente.facturasVendedorCubiertas;
      facturasPPDCubiertas = resultadoPuente.facturasPPDCubiertas;
      pendientesPorFacturarProp = resultadoPuente.pendientesPorFacturar ?? [];
      // Ver comentario en `_uuidsConCargoCubiertoEnBD` — complementa lo
      // detectado hoy con lo ya cubierto en días previos.
      for (const [u, monto] of await _uuidsConCargoCubiertoEnBD({ rfc })) {
        facturasVendedorCubiertas.set(u, (facturasVendedorCubiertas.get(u) ?? 0) + monto);
      }
    }
  }

  // 5. Precalcular regla por CFDI y recolectar todos los códigos de cuenta necesarios
  const cfdiConRegla = cfdisConNCProp.map(cfdi => ({
    cfdi,
    rule: mappingSvc.findRuleInList(cfdi, rules),
  }));

  const codigosNecesarios = [...new Set(
    cfdiConRegla
      .filter(({ rule }) => rule)
      .flatMap(({ rule: r }) => [
        r.cuentaCargo, r.cuentaAbono, r.cuentaIva,
        r.cuentaIvaPPD, r.cuentaIvaRetenido, r.cuentaIsrRetenido,
        r.cuentaAbono2, r.cuentaDescuento, r.cuentaDescuento0,
        r.cuentaIvaAnticipo, r.cuentaDeltaAnticipo, r.cuentaCargo2,
        r.cuentaCargoMixto0, r.cuentaIvaAbono,
      ].filter(Boolean))
      // Caja/Bancos/Saldo a Favor/Club Tuberos SIEMPRE, aunque ninguna regla
      // de este batch las use — el split por forma de pago real (ver
      // `_prefetchDesglosePagoReal`) puede necesitar CUALQUIERA de estas
      // cuatro sin importar qué cuenta seleccionó la regla original (ej.
      // matcheó Efectivo/Caja, pero el desglose real trae una porción de
      // Tarjeta, o de Saldo a Favor/Puntos) — sin esto, `cuentaMap[...]`
      // saldría undefined y esa porción del split se saltaría en silencio.
      .concat([CODIGO_CUENTA_CAJA, CODIGO_CUENTA_BANCOS, CODIGO_CUENTA_SALDO_FAVOR, CODIGO_CUENTA_CLUB_TUBEROS, CODIGO_CUENTA_IVA_SALDO_FAVOR, CODIGO_CUENTA_ANTICIPOS_CLIENTES, CODIGO_CUENTA_IVA_ANTICIPO, CODIGO_CUENTA_PUENTE_SUCURSALES]),
  )];

  const cuentasRows = codigosNecesarios.length
    ? await AccountPlan.findAll({
        where:      { codigo: { [Op.in]: codigosNecesarios } },
        attributes: ['id', 'codigo'],
        raw:        true,
      })
    : [];
  const cuentaMap = Object.fromEntries(cuentasRows.map(c => [c.codigo, c.id]));

  // Desglose real de forma de pago (ver `_prefetchDesglosePagoReal`) — antes
  // de precalcular el `context` por CFDI, para que esté disponible ahí.
  // `centroPropioClave`/fechaDesde/fechaHasta (2026-08-14): consulta por
  // centro+rango de fechas en vez de por serie/folio propio — ver docstring
  // en `_prefetchAjustesFacturaPropia`.
  const { desglosePagoReal: desglosePagoRealMapProp, puntosUsado: puntosUsadoMapProp, saldoFavorUsado: saldoFavorUsadoMapProp, cobrosCobradoraDirecta: cobrosCobradoraDirectaProp = [], usoCaminoPorCentro: usoCaminoPorCentroProp = false } = await _prefetchAjustesFacturaPropia(cfdiConRegla, rfc, {
    centroPropioClave: serieDelCentroProp,
    fechaDesde: fechaInicio ? _medianocheMx(fechaInicio) : null,
    fechaHasta: fechaFin   ? new Date(_medianocheMx(_diaSiguiente(fechaFin)).getTime() - 1) : null,
  });
  // Documentos liquidados por cada Pago (asiento completo por factura,
  // incluyendo su propio saldo a favor si aplica) — ver `_prefetchDoctosPago`.
  const { doctosPorUuid: doctosPagoMapProp } = await _prefetchDoctosPago(cfdiConRegla, rfc);
  // Acumulador de Puntos usados en TODO el batch, por sucursal — Puntos va
  // consolidado en una sola línea genérica al final (a diferencia de Saldo a
  // Favor, que sí es individual), ver push al final de este flujo.
  const puntosAcumuladosProp = new Map(); // centroCostoId → { monto, centroCosto }

  // 6. Pre-fetch CFDIs relacionados (5° movimiento anticipo) y saldo a favor
  const relUuidsProp = [...new Set(
    cfdiConRegla
      .filter(({ rule, cfdi }) => rule?.cuentaDeltaAnticipo && cfdi.cfdiRelacionados?.length)
      .flatMap(({ cfdi }) => cfdi.cfdiRelacionados.map(r => r.uuid).filter(Boolean)),
  )];
  const relCfdiMapProp = relUuidsProp.length
    ? Object.fromEntries(
        (await CFDI.find({ uuid: { $in: relUuidsProp } }).select('uuid total impuestos.totalImpuestosTrasladados').lean())
          .map(c => [c.uuid, c]),
      )
    : {};

  // Aplicación de anticipo SIN Nota de Crédito SAT — ver comentario en
  // `CODIGO_CUENTA_ANTICIPOS_CLIENTES`. Solo se resuelve el folio del anticipo
  // para las facturas que califican (regla sin `cuentaIvaAnticipo`, para no
  // pisar el mecanismo ya existente cuando la regla SÍ es de anticipo).
  const _rel07UuidsSinReglaProp = [...new Set(
    cfdiConRegla
      .filter(({ rule, cfdi }) => cfdi.tipoDeComprobante === 'I' && !rule?.cuentaIvaAnticipo
        && cfdi.cfdiRelacionados?.some(r => r.tipoRelacion === '07'))
      .flatMap(({ cfdi }) => cfdi.cfdiRelacionados
        .filter(r => r.tipoRelacion === '07')
        .flatMap(r => r.uuids ?? (r.uuid ? [r.uuid] : []))),
  )];
  const anticipoCfdisProp = _rel07UuidsSinReglaProp.length
    ? await CFDI.find({ uuid: { $in: _rel07UuidsSinReglaProp } }).select('uuid serie folio total fecha').lean()
    : [];
  const anticipoFolioPorUuidProp = {
    ...Object.fromEntries(
      anticipoCfdisProp.map(c => [c.uuid.toUpperCase(), `OPA-${c.folio || c.serie || c.uuid}`]),
    ),
    ...(await _resolverReferenciaOpaPorMonto(anticipoCfdisProp)),
  };

  let saldoRestanteProp = 0;
  if (cfdiConRegla.some(({ rule }) => rule?.esAplicacionSaldo)) {
    const rows = await sequelize.query(
      `SELECT COALESCE(SUM(pm.debe) - SUM(pm.haber), 0) AS saldo
       FROM poliza_movimientos pm
       JOIN polizas p ON pm.poliza_id = p.id
       JOIN account_plans ap ON pm.cuenta_id = ap.id
       WHERE p.rfc = :rfc AND ap.codigo = '2103090001' AND p.estado != 'cancelada'`,
      { replacements: { rfc }, type: QueryTypes.SELECT },
    );
    saldoRestanteProp = Number(rows[0]?.saldo || 0);
  }

  // 6. Generar movimientos usando cuentaMap pre-cargado
  // (ccBySerieMapProp ya se resolvió arriba, antes del filtro por sucursal)

  // ── Fix doble-contabilización anticipo PUE ────────────────────────────────
  // Solo aplica cuando la factura final (formaPago=30) usa el modelo 2 asientos
  // (cuentaCargo=2103010001 Anticipos). En el modelo 3 asientos (cuentaCargo=1103010001
  // Clientes) la NC sí debe procesarse — cancela Anticipos vs Clientes en asiento 3.
  const anticosCubiertosPorReg22C = new Set();
  for (const { cfdi: c, rule: r } of cfdiConRegla) {
    if (c.tipoDeComprobante !== 'I' || c.formaPago !== '30') continue;
    if (r?.cuentaCargo !== '2103010001') continue;
    if (c.uuid) anticosCubiertosPorReg22C.add(c.uuid.toUpperCase());
  }

  // Fix 5: verificar también en BD — la NC y la factura final pueden venir en batches distintos.
  // Si el UUID relacionado tipo 07 de alguna NC ya tiene movimiento en una regla con cuentaIvaAnticipo
  // en una póliza no cancelada, la NC está cubierta aunque no esté en el batch actual.
  {
    const uuids07 = new Set(
      cfdiConRegla
        .filter(({ cfdi: c }) =>
          c.tipoDeComprobante === 'E' &&
          c.cfdiRelacionados?.some(r => r.tipoRelacion === '07'))
        .flatMap(({ cfdi: c }) =>
          (c.cfdiRelacionados || [])
            .filter(r => r.tipoRelacion === '07')
            .flatMap(r => r.uuids ?? (r.uuid ? [r.uuid] : []))
            .map(u => u.toUpperCase()),
        ),
    );
    if (uuids07.size > 0) {
      const reglasAnticipo = await CfdiMappingRule.findAll({
        where: { cuentaIvaAnticipo: { [Op.ne]: null } },
        attributes: ['id'], raw: true,
      });
      const idsAnticipo = reglasAnticipo.map(r => r.id);
      if (idsAnticipo.length > 0) {
        const yaEnBD = await PolizaMovimiento.findAll({
          where: { cfdiUuid: { [Op.in]: [...uuids07] }, reglaId: { [Op.in]: idsAnticipo } },
          attributes: ['cfdiUuid'],
          include: [{ model: Poliza, as: 'poliza', attributes: [], where: { rfc, estado: { [Op.ne]: 'cancelada' } }, required: true }],
        });
        for (const m of yaEnBD) anticosCubiertosPorReg22C.add(m.cfdiUuid.toUpperCase());
      }
    }
  }

  const movimientosResult = [];
  let sinRegla = 0;

  for (const { cfdi, rule } of cfdiConRegla) {
    // Omitir NC tipo E (tipoRelacion=07) cuyo anticipo original ya fue procesado
    // por una factura PUE formaPago=30 (Reg 22C) en este mismo batch.
    if (cfdi.tipoDeComprobante === 'E' &&
        cfdi.cfdiRelacionados?.some(r => r.tipoRelacion === '07')) {
      const _rel07 = (cfdi.cfdiRelacionados || []).find(r => r.tipoRelacion === '07');
      const uuid07 = (_rel07?.uuids?.[0] ?? _rel07?.uuid ?? '').toUpperCase() || undefined;
      if (uuid07 && anticosCubiertosPorReg22C.has(uuid07)) continue;
    }
    const context = {};
    if (rule?.cuentaDeltaAnticipo && cfdi.cfdiRelacionados?.length) {
      const uuidsProp = cfdi.cfdiRelacionados.map(r => r.uuid).filter(Boolean);
      const foundProp  = uuidsProp.some(u => relCfdiMapProp[u]);
      if (foundProp) {
        context.totalRelacionado = uuidsProp
          .reduce((s, u) => s + Number(relCfdiMapProp[u]?.total || 0), 0);
        context.ivaRelacionado = uuidsProp
          .reduce((s, u) => s + Number(relCfdiMapProp[u]?.impuestos?.totalImpuestosTrasladados || 0), 0);
      }
      // Si no se encontró el CFDI relacionado en MongoDB, omitir delta (sin context.totalRelacionado)
    }
    if (rule?.esAplicacionSaldo && saldoRestanteProp > 0) {
      context.saldoDisponible = saldoRestanteProp;
    }
    // Las NC (tipo E) deben tratarse como la VENTA ORIGINAL que ajustan, no
    // según su propio metodoPago declarado (puede no coincidir — confirmado
    // con el usuario: una NC "Efectivo/PUE" puede estar ajustando una
    // factura PPD nunca cobrada). Con esto el motor usa cuentaIvaPPD en vez
    // de cuentaIva cuando la factura relacionada era a crédito.
    if (cfdi.tipoDeComprobante === 'E') {
      const metodoPagoRel = _uuidsRelacionados(cfdi).map(u => relMetodoPagoMap[u]).find(Boolean);
      if (metodoPagoRel) context.metodoPagoRelacionado = metodoPagoRel;
    }
    if (cfdi.tipoDeComprobante === 'I' && cfdi.serie && cfdi.folio) {
      const desgloseReal = desglosePagoRealMapProp.get(`${cfdi.serie}|${cfdi.folio}`);
      // En camino por centro tenemos imagen completa del periodo: si no hay
      // cobro para esta factura en esta sucursal, desglosePagoReal = [] para
      // que cfdiToMovimientos detecte n=0 y marque el Cargo como 'Cobro Sucursal'
      // (registro aparte, no entra al consolidado de Efectivo/Tarjeta).
      if (desgloseReal || usoCaminoPorCentroProp) context.desglosePagoReal = desgloseReal ?? [];
      const sfUsado = saldoFavorUsadoMapProp.get(`${cfdi.serie}|${cfdi.folio}`);
      if (sfUsado) {
        // Split por origen (no todo-o-nada): una Factura Global puede combinar
        // SF de VARIAS Devoluciones — algunas "lavadas" el mismo día/almacén
        // (ocultables) y otras de un periodo anterior (NO ocultables). El
        // `.every()` anterior exigía que TODAS calificaran para ocultar
        // cualquier cosa, así que un solo origen no-ocultable escondía la
        // ocultación de TODO el monto combinado (caso real confirmado
        // 2026-08-17: Atzompa E0-260800025, DEV-056098 mismo día/almacén +
        // DEV-055729 de julio — el monto completo se quedaba visible en vez
        // de ocultar solo la porción de DEV-056098).
        const detalle = sfUsado.detalle ?? [];
        // `detalleVisible`: cada origen NO ocultable (periodo anterior) por
        // separado, con su propia referencia (serieOrigen-folioOrigen) — antes
        // se combinaban en un solo monto bajo el concepto del CFDI actual,
        // perdiendo de qué devolución/cancelación viene cada porción
        // (confirmado con el usuario 2026-08-18, caso real Global 89CF6A7F:
        // DEV-055991 de julio + CAC-075406 de junio, combinados como un solo
        // "$523.91 SF" sin poder rastrear el origen de cada uno).
        const detalleVisible = detalle.filter(d => !devsOcultosSFProp.has(`${d.serieOrigen}|${d.folioOrigen}`));
        const montoOculto = Math.round(detalle
          .filter(d => devsOcultosSFProp.has(`${d.serieOrigen}|${d.folioOrigen}`))
          .reduce((s, d) => s + (Number(d.monto) || 0), 0) * 100) / 100;
        context.saldoFavorUsadoPropio = {
          ...sfUsado,
          montoOculto,
          montoVisible: Math.round((sfUsado.monto - montoOculto) * 100) / 100,
          detalleVisible,
        };
      }
      const puntosUsadoCfdi = puntosUsadoMapProp.get(`${cfdi.serie}|${cfdi.folio}`);
      if (puntosUsadoCfdi > 0) context.montoPuntosUsado = puntosUsadoCfdi;
    }
    if (cfdi.tipoDeComprobante === 'P') {
      const doctosPago = doctosPagoMapProp.get(cfdi.uuid);
      if (doctosPago) context.doctosPago = doctosPago;
    }

    const movs = await mappingSvc.cfdiToMovimientos(cfdi, rule, cuentaMap, context);

    if (rule?.esAplicacionSaldo) {
      const usado = movs.find(m => m._saldoUsado != null)?._saldoUsado ?? 0;
      saldoRestanteProp = Math.max(0, saldoRestanteProp - usado);
    }

    const ccProp = cfdi.serie ? (ccBySerieMapProp[cfdi.serie] ?? null) : null;

    // Aplicación de anticipo SIN Nota de Crédito SAT — ver comentario en
    // `CODIGO_CUENTA_ANTICIPOS_CLIENTES`. Si esta factura califica, el Cargo
    // principal (normalmente a Clientes) se sustituye más abajo por Anticipos
    // + IVA-anticipo, referenciando el folio del anticipo con el marcador
    // "OPA" (confirmado con el usuario 2026-08-19: por lo mientras, sin dato
    // de marcador real del ERP para este caso).
    const rel07SinReglaProp = (cfdi.tipoDeComprobante === 'I' && !rule?.cuentaIvaAnticipo)
      ? cfdi.cfdiRelacionados?.find(r => r.tipoRelacion === '07')
      : null;
    const anticipoFolioRefProp = rel07SinReglaProp
      ? anticipoFolioPorUuidProp[(rel07SinReglaProp.uuids?.[0] ?? rel07SinReglaProp.uuid ?? '').toUpperCase()]
      : null;

    // Acumular Puntos usados por esta factura hacia el total de la sucursal
    // (ver `puntosAcumuladosProp` — Puntos va consolidado, no individual).
    const puntosUsadoEstaCfdi = movs.find(m => m._puntosUsado != null)?._puntosUsado ?? 0;
    if (puntosUsadoEstaCfdi > 0 && ccProp) {
      const prev = puntosAcumuladosProp.get(ccProp.id) ?? { monto: 0, centroCosto: ccProp };
      prev.monto = parseFloat((prev.monto + puntosUsadoEstaCfdi).toFixed(2));
      puntosAcumuladosProp.set(ccProp.id, prev);
    }

    // Si esta factura ya recibió (parte de) su Cargo vía
    // cobros-sucursal-puente.service.js (cobrada en otra sucursal), ese monto
    // se RESTA del Cargo normal de la regla — no se omite siempre por
    // completo (corrección 2026-08-06: para una Factura Global, un solo
    // ticket cruzado de cientos NO cubre el total de la factura — tratarlo
    // como sí/no perdía el resto del cargo real, ver docstring de
    // `facturasVendedorCubiertas` en cobros-sucursal-puente.service.js).
    const montoCubiertoPorSucursal = facturasVendedorCubiertas.get(cfdi.uuid?.toUpperCase() ?? '') ?? 0;
    let montoCubiertoRestante = montoCubiertoPorSucursal;
    // Facturas PPD cobradas en otra sucursal: el Cargo a Clientes de la venta
    // sigue normal (sin tocar); se agrega ABAJO un asiento adicional (Abono a
    // Clientes + la línea de Cargo a la cuenta puente que ya viene en
    // movsPuente) — ver `facturasPPDCubiertas` en construirMovimientosPuente.
    const ppdCubierta = facturasPPDCubiertas.get(cfdi.uuid?.toUpperCase() ?? '');

    for (const m of movs) {
      // `_esCargoPrincipal` (ver cfdiToMovimientos): señal explícita que
      // cubre TAMBIÉN el caso partido por forma de pago real (2026-08-06) —
      // sin esto, el split solo se reconocería por la cuenta ORIGINAL de la
      // regla, dejando sin omitir la(s) línea(s) que el split movió a otra
      // cuenta (ej. regla=Caja pero el desglose real trajo una porción de
      // Bancos), duplicando el cargo si la factura además resulta cubierta
      // por un cobro cruzado de sucursal. Se mantiene la reconstrucción
      // original como respaldo para líneas sin el tag (anticipo/aplicación
      // de saldo/tasa mixta, fuera del alcance del split).
      const esLineaCargoPrincipal = m._esCargoPrincipal === true || (!!rule?.cuentaCargo &&
        m.cuentaId === (cuentaMap[rule.cuentaCargo] ?? null) && m.debe > 0);
      // Anticipo sin NC (ver `anticipoFolioRefProp` arriba): el Cargo
      // principal NO se registra — se sustituye abajo por Anticipos/IVA-anticipo.
      if (anticipoFolioRefProp && esLineaCargoPrincipal) continue;
      // La reducción por cruce de sucursal SOLO aplica a Caja/Bancos — nunca
      // a las líneas de SF/Puntos (`_esCargoPrincipal` también las marca,
      // pero representan dinero que fue a Anticipos Otros/Club Tuberos, no
      // efectivo/tarjeta cobrado — un cruce de sucursal cubre esto último).
      const esLineaCajaOBancos = m.cuentaId === (cuentaMap[CODIGO_CUENTA_CAJA] ?? null) || m.cuentaId === (cuentaMap[CODIGO_CUENTA_BANCOS] ?? null);
      if (montoCubiertoRestante > 0 && esLineaCargoPrincipal && esLineaCajaOBancos && Number(m.debe) > 0) {
        const reduccion = Math.min(montoCubiertoRestante, Number(m.debe));
        montoCubiertoRestante = parseFloat((montoCubiertoRestante - reduccion).toFixed(2));
        const debeAjustado = parseFloat((Number(m.debe) - reduccion).toFixed(2));
        if (debeAjustado <= 0) continue; // esta línea quedó totalmente cubierta
        movimientosResult.push({
          ...m,
          debe:          debeAjustado,
          centroCosto:   ccProp?.clave   ?? m.centroCosto   ?? null,
          centroCostoId: ccProp?.id      ?? null,
          _cfdiInfo: {
            uuid:              cfdi.uuid,
            tipo:              cfdi.tipoDeComprobante,
            emisor:            cfdi.emisor?.rfc,
            total:             cfdi.total,
            fecha:             cfdi.fecha,
            sinRegla:          !!m._sinRegla,
            comparisonStatus:  cfdi.lastComparisonStatus ?? null,
          },
        });
        continue;
      }
      movimientosResult.push({
        ...m,
        centroCosto:   ccProp?.clave   ?? m.centroCosto   ?? null,
        centroCostoId: ccProp?.id      ?? null,
        _cfdiInfo: {
          uuid:              cfdi.uuid,
          tipo:              cfdi.tipoDeComprobante,
          emisor:            cfdi.emisor?.rfc,
          total:             cfdi.total,
          fecha:             cfdi.fecha,
          sinRegla:          !!m._sinRegla,
          comparisonStatus:  cfdi.lastComparisonStatus ?? null,
        },
      });
    }

    // Anticipo sin NC — sustituye el Cargo principal omitido arriba por
    // Anticipos + IVA-anticipo, con la misma referencia "OPA-{folio del
    // anticipo}" en ambas líneas (ver `CODIGO_CUENTA_ANTICIPOS_CLIENTES`).
    // Se reutilizan los montos YA calculados por la regla para Ventas/IVA
    // (en vez de recalcular desde el CFDI) para no duplicar esa lógica ni
    // arriesgar una descuadrada si difieren por descuentos/retenciones.
    if (anticipoFolioRefProp) {
      const montoVentasAnticipoProp = rule?.cuentaAbono
        ? (movs.find(m => m.cuentaId === (cuentaMap[rule.cuentaAbono] ?? null) && Number(m.haber) > 0)?.haber ?? 0)
        : 0;
      // Puede ser PPD (`rule.cuentaIvaPPD`, IVA por cobrar) o PUE (`rule.cuentaIva`) —
      // se prueban ambas cuentas, la que traiga un haber real gana.
      const montoIvaAnticipoProp = [rule?.cuentaIva, rule?.cuentaIvaPPD]
        .filter(Boolean)
        .map(cod => movs.find(m => m.cuentaId === (cuentaMap[cod] ?? null) && Number(m.haber) > 0)?.haber)
        .find(v => Number(v) > 0) ?? 0;
      const refOpaProp = anticipoFolioRefProp; // ya viene armado como "OPA-..." (real o placeholder)
      const baseAnticipoProp = {
        concepto: refOpaProp, serie: refOpaProp, centroCosto: ccProp?.clave ?? null, centroCostoId: ccProp?.id ?? null,
        cfdiUuid: cfdi.uuid, haber: 0, tipoOrigen: TIPO_ORIGEN_CARGO_ESPECIAL, reglaNombre: 'OPA',
        _cfdiInfo: {
          uuid: cfdi.uuid, tipo: cfdi.tipoDeComprobante, emisor: cfdi.emisor?.rfc,
          total: cfdi.total, fecha: cfdi.fecha, sinRegla: false,
          comparisonStatus: cfdi.lastComparisonStatus ?? null,
        },
      };
      if (Number(montoVentasAnticipoProp) > 0) {
        movimientosResult.push({ ...baseAnticipoProp, cuentaId: cuentaMap[CODIGO_CUENTA_ANTICIPOS_CLIENTES] ?? null, debe: montoVentasAnticipoProp });
      }
      if (Number(montoIvaAnticipoProp) > 0) {
        movimientosResult.push({ ...baseAnticipoProp, cuentaId: cuentaMap[CODIGO_CUENTA_IVA_ANTICIPO] ?? null, debe: montoIvaAnticipoProp });
      }
    }

    // Saldo a favor generado por esta Devolución (ver
    // `_prefetchSaldosFavorGenerados`/`_inyectarSaldoFavorGenerado`).
    const lineasSaldoFavorProp = await _inyectarSaldoFavorGenerado({
      cfdi, mapaGenerados: mapaSaldosFavorGeneradosProp,
      cuentaSaldoFavorId: cuentaSaldoFavorIdProp, cuentaIvaSaldoFavorId: cuentaIvaSaldoFavorIdProp,
      cuentaCajaId: cuentaMap[CODIGO_CUENTA_CAJA] ?? null, cuentaBancosId: cuentaMap[CODIGO_CUENTA_BANCOS] ?? null,
      cc: ccProp, rfc,
    });
    for (const linea of lineasSaldoFavorProp) {
      movimientosResult.push(linea);
    }
    // Cierra el Abono de Caja/Bancos/Clientes que dejó la Devolución (mismo
    // monto que se convirtió en Anticipos Otros) — sin esto el asiento queda
    // descuadrado: el mismo dinero se contaba dos veces (salida de
    // Caja/Bancos Y pasivo de saldo a favor). Revierte, para el cuadre por
    // asiento que exige el export CONTPAQ, la coexistencia deliberada
    // confirmada 2026-08-04 (ver docstring de `_inyectarSaldoFavorGenerado`)
    // — confirmado con el usuario 2026-08-10 que debe cuadrar por asiento.
    // `_ajusteConsolidadoSF` (caso "mismo folio", 2026-08-13): esa línea no
    // trae `haber` (es un Cargo negativo), así que se suma su `debe` en
    // valor absoluto para seguir cerrando el mismo Abono con el mismo monto.
    if (lineasSaldoFavorProp.length > 0) {
      const montoSaldoFavorProp = lineasSaldoFavorProp.reduce((s, l) =>
        s + (Number(l.haber) || 0) + (l._ajusteConsolidadoSF ? Math.abs(Number(l.debe) || 0) : 0), 0);
      const abonoDevolucionProp = movs.find(m => Number(m.haber) > 0);
      if (abonoDevolucionProp && montoSaldoFavorProp > 0) {
        // Si el SF que se está cerrando es TODO oculto (generado y usado el
        // mismo día/almacén, ver `_inyectarSaldoFavorGenerado`), esta línea de
        // cierre debe ocultarse igual que sus hermanas — sin esto queda
        // visible en la póliza principal como un Cargo "Cancelación" extra
        // sin contrapartida aparente (el abono que revierte SÍ existe, solo
        // que está oculto), confundiendo al contador (confirmado con el
        // usuario 2026-08-18, caso real NORBERTO VELAZQUEZ JUAREZ/CAC-077337).
        // `tipoOrigen` también se sobreescribe: `_extraerCobrosSucursal`
        // (poliza.service.js) solo revisa `reglaNombre` en líneas cuyo
        // tipoOrigen YA es 'Cobro Sucursal' — con el tipoOrigen original
        // ('Cancelación') la línea nunca llegaba a esa revisión.
        const todoOcultoProp = lineasSaldoFavorProp.every(l => l.reglaNombre === ETIQUETA_SALDO_FAVOR_OCULTO);
        movimientosResult.push({
          ...abonoDevolucionProp,
          debe:          montoSaldoFavorProp,
          haber:         0,
          centroCosto:   ccProp?.clave ?? abonoDevolucionProp.centroCosto ?? null,
          centroCostoId: ccProp?.id    ?? null,
          ...(todoOcultoProp ? { tipoOrigen: 'Cobro Sucursal', reglaNombre: ETIQUETA_SALDO_FAVOR_OCULTO } : {}),
          _cfdiInfo: {
            uuid:              cfdi.uuid,
            tipo:              cfdi.tipoDeComprobante,
            emisor:            cfdi.emisor?.rfc,
            total:             cfdi.total,
            fecha:             cfdi.fecha,
            sinRegla:          !!abonoDevolucionProp._sinRegla,
            comparisonStatus:  cfdi.lastComparisonStatus ?? null,
          },
        });
      }
    }

    // Asiento adicional PPD cobrada en otra sucursal: Abono a Clientes (misma
    // cuenta que usó el Cargo de la venta) — cierra la CxC por el monto
    // cobrado cruzado. Su contrapartida (Cargo a la cuenta puente) ya viene
    // en `movsPuente`, agregado más abajo.
    if (ppdCubierta && rule?.cuentaCargo && (cuentaMap[rule.cuentaCargo] ?? null)) {
      // Reutiliza concepto/serie de la línea de Cargo normal (misma factura)
      // en vez de recalcularlos — cfdiToMovimientos ya resuelve el marcador
      // de documentosRelacionados (ej. "I0-260700186") cuando aplica; armarlos
      // aquí desde cfdi.serie/cfdi.folio mostraba el folio propio de la
      // factura en vez del documento relacionado, inconsistente con sus
      // líneas hermanas.
      const cargoOriginal = movs.find(m => m._esCargoPrincipal === true)
        ?? movs.find(m => m.cuentaId === (cuentaMap[rule.cuentaCargo] ?? null) && m.debe > 0);
      const serieCfdi = cargoOriginal?.serie ?? ([cfdi.serie, cfdi.folio].filter(Boolean).join('-').slice(0, 25) || null);
      // `cargoOriginal.concepto` (armado por cfdiToMovimientos) NUNCA incluye
      // el nombre del cliente — eso se agrega después vía
      // enriquecerConceptoConCliente (poliza.service.js), un paso que solo
      // corre sobre las líneas normales. Esta línea va por el camino de
      // "Cobro Sucursal" (tipoOrigen abajo) y no pasa por ahí, así que hay
      // que anteponer el nombre aquí mismo — mismo patrón que `conceptoBase`
      // en cobros-sucursal-puente.service.js.
      const conceptoConCliente = [cfdi.receptor?.nombre, cargoOriginal?.concepto ?? serieCfdi].filter(Boolean).join(' / ');
      movimientosResult.push({
        cuentaId:      cuentaMap[rule.cuentaCargo],
        cuentaFaltante: false,
        concepto:      conceptoConCliente,
        debe:          0,
        haber:         ppdCubierta.monto,
        serie:         serieCfdi,
        centroCosto:   ccProp?.clave ?? null,
        centroCostoId: ccProp?.id    ?? null,
        // metodoPago='PPD' es OBLIGATORIO: poliza.service.js (exportContpaqXlsx,
        // ~línea 1356) separa Contado/Crédito únicamente por
        // `m.metodoPago === 'PPD'` — sin esto, esta línea cae por default en
        // Contado (huérfana, sin su Cargo original al lado) en vez de en
        // Crédito junto a sus hermanas.
        metodoPago:    'PPD',
        // tipoOrigen='Cobro Sucursal': confirmado con el usuario 2026-08-03 —
        // esta línea (y su contrapartida, el Cargo a la cuenta puente) debe
        // quedar al FINAL del apartado de Crédito, no junto a sus hermanas
        // por serie-folio. _extraerCobrosSucursal/_inyectarCobrosSucursal
        // (poliza.service.js) la sacan y reinyectan al final del bloque
        // correcto (Crédito, por `metodoPago==='PPD'`).
        tipoOrigen:    'Cobro Sucursal',
        reglaNombre:   ppdCubierta.reglaNombre,
        cfdiUuid:      cfdi.uuid,
        _cfdiInfo: {
          uuid:              cfdi.uuid,
          tipo:              cfdi.tipoDeComprobante,
          emisor:            cfdi.emisor?.rfc,
          total:             cfdi.total,
          fecha:             cfdi.fecha,
          sinRegla:          false,
          comparisonStatus:  cfdi.lastComparisonStatus ?? null,
        },
      });
    }
    if (!rule) sinRegla++;
  }

  // Saldos a favor GEN-huérfanos: entradas del mapa cuya Devolución no pasó
  // por el loop de CFDIs (ej. su CFDI tiene fecha distinta, serie de otra
  // sucursal o no tiene los marcadores SAT/ERP que exige
  // `_fetchNotasCreditoParaFusion` para incluirla en el batch). El mapa ya
  // las tiene porque `_prefetchSaldosFavorGenerados` usa
  // `obtenerSaldosFavorPorCentro` (por centro+fecha) como fuente principal —
  // `_inyectarSaldoFavorGenerado` nunca se llamó para ellas, así que las
  // líneas HABER no se crearon. Se inyectan aquí con el mismo formato que
  // produce `_inyectarSaldoFavorGenerado` (tipoOrigen='Cobro Sucursal',
  // reglaNombre='SF'/'SF-OCULTO') para que `_extraerCobrosSucursal`
  // (poliza.service.js) las trate exactamente igual.
  if (cuentaSaldoFavorIdProp && cuentaIvaSaldoFavorIdProp) {
    const ccOrfanos = serieDelCentroProp ? (ccBySerieMapProp[serieDelCentroProp] ?? null) : null;
    const clavesConsumidas = new Set(
      cfdisConNCProp
        .filter(c => c.tipoDeComprobante === 'E')
        .map(c => {
          const marcador = (c.documentosRelacionados ?? [])
            .find(d => TIPO_MARCADORES_DEV.includes((d.Serie ?? '').toUpperCase()) && d.Folio);
          return marcador ? `${marcador.Serie}|${marcador.Folio}` : null;
        })
        .filter(Boolean),
    );
    for (const [key, generado] of mapaSaldosFavorGeneradosProp) {
      if (clavesConsumidas.has(key)) continue;
      if (!generado?.monto) continue;
      const reglaSF = generado.oculto ? ETIQUETA_SALDO_FAVOR_OCULTO : 'SF';
      const subtotal = Math.round((generado.monto / 1.16) * 100) / 100;
      const iva      = Math.round((generado.monto - subtotal) * 100) / 100;
      const serieFolioVenta = [generado.ventaSerie, generado.ventaFolio].filter(Boolean).join('-') || null;
      const base = {
        concepto:       serieFolioVenta ?? key,
        serie:          serieFolioVenta,
        centroCosto:    ccOrfanos?.clave ?? null,
        centroCostoId:  ccOrfanos?.id    ?? null,
        cfdiUuid:       null,
        cuentaFaltante: false,
        tipoOrigen:     'Cobro Sucursal',
        reglaNombre:    reglaSF,
        debe:           0,
      };
      movimientosResult.push({ ...base, cuentaId: cuentaSaldoFavorIdProp,    haber: subtotal });
      movimientosResult.push({ ...base, cuentaId: cuentaIvaSaldoFavorIdProp, haber: iva     });
    }
  }

  // Retiros en EFECTIVO de saldo a favor (ABO) — ver
  // `_prefetchSaldosFavorGenerados`/`ajustesEfectivoRetiroSF`: dinero real
  // que salió de caja, se resta del consolidado de Efectivo con un Cargo
  // NEGATIVO sin fila propia (mismo patrón que "SF-MISMO-FOLIO"), sin
  // importar si el saldo generado terminó en $0 ese día o le quedó un
  // remanente pendiente (ese remanente, si existe, ya se muestra aparte
  // como línea de SF visible más arriba).
  if (cuentaMap[CODIGO_CUENTA_CAJA] && ajustesEfectivoRetiroSFProp.length) {
    const ccRetiroProp = serieDelCentroProp ? (ccBySerieMapProp[serieDelCentroProp] ?? null) : null;
    for (const ret of ajustesEfectivoRetiroSFProp) {
      const serieFolioRetiro = [ret.ventaSerie, ret.ventaFolio].filter(Boolean).join('-') || null;
      movimientosResult.push({
        concepto:       serieFolioRetiro ?? 'Retiro de saldo a favor',
        serie:          serieFolioRetiro,
        centroCosto:    ccRetiroProp?.clave ?? null,
        centroCostoId:  ccRetiroProp?.id    ?? null,
        cfdiUuid:       null,
        cuentaFaltante: false,
        tipoOrigen:     'Ajuste Consolidado SF',
        reglaNombre:    'SF-RETIRO-EFECTIVO',
        formaPago:      '01',
        cuentaId:       cuentaMap[CODIGO_CUENTA_CAJA],
        debe:           -ret.monto,
        haber:          0,
        _ajusteConsolidadoSF: true,
      });
    }
  }

  // Puntos/Club Tuberos usados en el batch: UNA sola línea consolidada por
  // sucursal (no individual, a diferencia de Saldo a Favor — confirmado con
  // el usuario 2026-08-06, revirtió su decisión anterior sobre Puntos),
  // etiqueta genérica "CLIENTE DE MOSTRADOR SUC. X". `tipoOrigen:
  // TIPO_ORIGEN_CARGO_ESPECIAL` (mismo valor que usa cfdi-mapping.service.js
  // para SF) para que `_extraerCobrosSucursal` (poliza.service.js) la saque
  // del pipeline normal de consolidación — si no, `consolidarCargos` la
  // agruparía con otras líneas sin `formaPago` bajo "Depósitos consolidados"
  // genérico, perdiendo la etiqueta "PAGO"/el nombre de la sucursal.
  for (const { monto, centroCosto: ccPuntos } of puntosAcumuladosProp.values()) {
    if (monto <= 0 || !cuentaMap[CODIGO_CUENTA_CLUB_TUBEROS] || !cuentaMap[CODIGO_CUENTA_IVA_SALDO_FAVOR]) continue;
    const subtotal = Math.round((monto / 1.16) * 100) / 100;
    const iva = Math.round((monto - subtotal) * 100) / 100;
    const conceptoConsolidado = `CLIENTE DE MOSTRADOR SUC. ${ccPuntos?.sucursal ?? ccPuntos?.clave ?? ''}`.trim();
    const baseConsolidado = {
      concepto: conceptoConsolidado, centroCosto: ccPuntos?.clave ?? null, centroCostoId: ccPuntos?.id ?? null,
      haber: 0, cfdiUuid: null, tipoOrigen: TIPO_ORIGEN_CARGO_ESPECIAL, reglaNombre: 'PAGO',
    };
    movimientosResult.push({ ...baseConsolidado, cuentaId: cuentaMap[CODIGO_CUENTA_CLUB_TUBEROS] ?? null, debe: subtotal });
    movimientosResult.push({ ...baseConsolidado, cuentaId: cuentaMap[CODIGO_CUENTA_IVA_SALDO_FAVOR] ?? null, debe: iva });
  }

  // Facturas PPD cobradas en otra sucursal cuya VENTA ORIGINAL no cae en el
  // lote de hoy (ej. factura del día 4, cobrada el día 10 — normal en PPD,
  // que por definición se cobra días/semanas después) — el Abono a Clientes
  // de arriba nunca corre para ellas porque ese loop solo recorre
  // `cfdiConRegla` (CFDIs del día que se está generando). Se resuelven aparte
  // (confirmado con el usuario 2026-08-04, HERROZINC I0-260700082: factura
  // del 04/07 cobrada en CEDIS el 10/07 — quedaba como asiento descuadrado,
  // Cargo a la cuenta puente sin su Abono a Clientes).
  const uuidsPPDManejadosProp = new Set(cfdiConRegla.map(({ cfdi: c }) => c.uuid?.toUpperCase()).filter(Boolean));
  const uuidsPPDOrfanosProp = [...facturasPPDCubiertas.keys()].filter(u => !uuidsPPDManejadosProp.has(u));
  if (uuidsPPDOrfanosProp.length) {
    const satOrfanosProp = await CFDI.find({ uuid: { $in: uuidsPPDOrfanosProp }, source: 'SAT' })
      .select('uuid tipoDeComprobante metodoPago formaPago fecha folio serie emisor receptor conceptos impuestos').lean();
    const erpOrfanosProp = await CFDI.find({ uuid: { $in: uuidsPPDOrfanosProp }, source: 'ERP' })
      .select('uuid formaPago metodoPago conceptos impuestos').lean();
    const erpOrfanosMapProp = Object.fromEntries(erpOrfanosProp.map(c => [c.uuid, c]));
    for (const cfdiOrfano of satOrfanosProp) {
      const erpOrf = erpOrfanosMapProp[cfdiOrfano.uuid];
      const cfdiFinalOrf = {
        ...cfdiOrfano,
        formaPago:  cfdiOrfano.formaPago  || erpOrf?.formaPago,
        metodoPago: cfdiOrfano.metodoPago || erpOrf?.metodoPago,
        conceptos:  erpOrf?.conceptos?.length ? erpOrf.conceptos : cfdiOrfano.conceptos,
        impuestos:  erpOrf?.impuestos ?? cfdiOrfano.impuestos,
      };
      const ruleOrfano = mappingSvc.findRuleInList(cfdiFinalOrf, rules);
      if (!ruleOrfano?.cuentaCargo) continue;
      let cuentaCargoIdOrfano = cuentaMap[ruleOrfano.cuentaCargo];
      if (!cuentaCargoIdOrfano) {
        const rowOrfano = await AccountPlan.findOne({ where: { codigo: ruleOrfano.cuentaCargo }, attributes: ['id'], raw: true });
        cuentaCargoIdOrfano = rowOrfano?.id ?? null;
        if (cuentaCargoIdOrfano) cuentaMap[ruleOrfano.cuentaCargo] = cuentaCargoIdOrfano;
      }
      if (!cuentaCargoIdOrfano) continue;
      const ppdCubiertaOrfano = facturasPPDCubiertas.get(cfdiOrfano.uuid.toUpperCase());
      const ccOrfano = cfdiFinalOrf.serie ? (ccBySerieMapProp[cfdiFinalOrf.serie] ?? null) : null;
      const serieCfdiOrfano = [cfdiFinalOrf.serie, cfdiFinalOrf.folio].filter(Boolean).join('-').slice(0, 25) || null;
      const conceptoOrfano = [cfdiFinalOrf.receptor?.nombre, serieCfdiOrfano].filter(Boolean).join(' / ');
      movimientosResult.push({
        cuentaId:      cuentaCargoIdOrfano,
        cuentaFaltante: false,
        concepto:      conceptoOrfano,
        debe:          0,
        haber:         ppdCubiertaOrfano.monto,
        serie:         serieCfdiOrfano,
        centroCosto:   ccOrfano?.clave ?? null,
        centroCostoId: ccOrfano?.id    ?? null,
        metodoPago:    'PPD',
        tipoOrigen:    'Cobro Sucursal',
        reglaNombre:   ppdCubiertaOrfano.reglaNombre,
        cfdiUuid:      cfdiOrfano.uuid,
      });
    }
  }

  movimientosResult.push(...movsPuente);

  // Cobros de series ajenas recibidos físicamente en ESTA sucursal (2026-08-15):
  // detectados directamente del endpoint ERP "por centro" — no requieren que la
  // sucursal vendedora genere primero y encole en CobroSucursalPendiente.
  // Para PUE: Cargo a Caja/Bancos (el ingreso real) + Abono a la misma cuenta
  // (contrapartida que cuadra contra la póliza de la sucursal vendedora).
  const _ccCobradora = serieDelCentroProp ? (ccBySerieMapProp[serieDelCentroProp] ?? null) : null;
  if (cobrosCobradoraDirectaProp.length > 0 && _ccCobradora) {
    const cuentaCajaIdDir   = cuentaMap[CODIGO_CUENTA_CAJA]   ?? null;
    const cuentaBancosIdDir = cuentaMap[CODIGO_CUENTA_BANCOS] ?? null;
    // Cobros que la cola CobroSucursalPendiente ya procesó (vendedora generó
    // primero y encoló el dato para la cobradora). Deduplica por UUID del CFDI:
    // el UUID es la clave SAT universal y está en la cola (cfdiUuid del vendedor)
    // y en MongoDB (consultado más arriba en _prefetchAjustesFacturaPropia).
    // El folioOrigen del ERP sirve de fallback por si el UUID no está disponible.
    // NOTE: series/folios NO son confiables como clave de dedup porque el queue
    // usa serieFolioTicket="serieVenta-folioVenta" (número interno de ticket) y
    // cobrosCobradoraDirecta usa claveFac="serieFactura|folioFactura" (folio SAT)
    // — son sistemas de numeración distintos (bug confirmado 2026-08-15).
    const _uuidsYaEnPuente = new Set(
      movsPuente
        .filter(m => m.tipoOrigen === 'Cobro Sucursal' && m.cfdiUuid)
        .map(m => m.cfdiUuid.toUpperCase()),
    );
    const _foliosYaEnPuente = new Set(
      movsPuente.filter(m => m.tipoOrigen === 'Cobro Sucursal' && m.folio != null).map(m => String(m.folio)),
    );
    for (const { claveSat, monto, claveFac, serFolTicket, nombre, folioOrigen, cfdiUuid } of cobrosCobradoraDirectaProp) {
      const esEfe     = claveSat === '01';
      const cuentaDir = esEfe ? cuentaCajaIdDir : cuentaBancosIdDir;
      if (!cuentaDir || monto <= 0) continue;
      // Concepto: SIEMPRE el documento relacionado (serieVenta-folioVenta,
      // "serie y folio interno" auditable en cajas) — nunca serieFactura/
      // folioFactura (`claveFac`, que solo sirve para resolver el CFDI en
      // Mongo). Mostrar el folio de factura llevaba a buscar un ticket
      // equivocado en Kore (confirmado con el usuario 2026-08-17).
      const _serFol = serFolTicket || (claveFac ?? '');
      // Saltar si la cola ya tiene este cobro — el DEBE+HABER de cobradora
      // ya lo generó `construirMovimientosPuente` (en movsPuente arriba).
      // Incluirlo aquí también inflaría el consolidado por partida doble
      // (bug real: Hidalgo EFECTIVO $215k vs $147k esperado, 2026-08-15).
      if (cfdiUuid && _uuidsYaEnPuente.has(cfdiUuid.toUpperCase())) continue;
      if (folioOrigen != null && _foliosYaEnPuente.has(String(folioOrigen))) continue;
      const _concepto = nombre ? `${nombre} / ${_serFol}` : _serFol;
      const baseDir = {
        concepto:      _concepto.slice(0, 255) || 'Cobro Suc. Ajena',
        centroCosto:   _ccCobradora.clave ?? null,
        centroCostoId: _ccCobradora.id    ?? null,
        reglaNombre:   'COS',
        formaPago:     claveSat || null,
      };
      // DEBE: Cargo a Caja/Bancos — cash físico recibido aquí de otra sucursal
      // → SIEMPRE va al consolidado ("Depósitos consolidados") para su depósito.
      // `cfdiUuid` solo se conserva para Efectivo (lo necesita el emparejado
      // de abajo) — en Tarjeta/Transferencia se omite a propósito: si ESE
      // MISMO ticket también tuviera una porción en Efectivo (mismo cfdiUuid),
      // el emparejamiento de esa porción marcaría el uuid como "ya cobrado en
      // otra sucursal" y arrastraría también esta línea de Tarjeta aunque su
      // Abono no comparta cuenta ni uuid con ella.
      movimientosResult.push({ ...baseDir, cuentaId: cuentaDir, cfdiUuid: esEfe ? (cfdiUuid ?? null) : null, tipoOrigen: 'Venta', debe: monto, haber: 0 });
      // HABER (contrapartida): Efectivo SÍ puede transferirse físicamente
      // entre sucursales, así que su Abono va a la MISMA cuenta (con el mismo
      // cfdiUuid, para que `_extraerCobrosSucursal` empareje y saque AMBAS
      // líneas del consolidado — el efectivo termina donde lo requiera la
      // sucursal vendedora, no se queda aquí). Tarjeta/Transferencia NUNCA se
      // pueden "mover" — el banco ya depositó en la cuenta de ESTA sucursal
      // sin importar quién facturó, así que su Abono va a la cuenta puente
      // (2103040001, deuda con la sucursal vendedora) SIN cfdiUuid — para que
      // el Cargo de arriba NO se empareje/extraiga y sí cuente en el
      // consolidado de Tarjeta (confirmado con el usuario 2026-08-19, caso
      // real CONSTRUCASA 13-ago: el corte de caja de Tarjeta cierra exacto
      // contra el bruto sin excluir cruces, a diferencia de Efectivo).
      if (esEfe) {
        movimientosResult.push({ ...baseDir, cuentaId: cuentaDir, cfdiUuid: cfdiUuid ?? null, tipoOrigen: 'Cobro Sucursal', debe: 0, haber: monto });
      } else {
        // Concepto deliberadamente DISTINTO al del Cargo (arriba): el
        // emparejador de `_extraerCobrosSucursal` también empareja pares sin
        // cfdiUuid por concepto+monto idénticos (caso "pendiente por
        // facturar") — si el concepto fuera el mismo, esta línea volvería a
        // arrastrar el Cargo de Tarjeta fuera del consolidado por esa otra vía.
        movimientosResult.push({
          ...baseDir, cuentaId: cuentaMap[CODIGO_CUENTA_PUENTE_SUCURSALES] ?? null,
          concepto: `${baseDir.concepto} (cruce sucursal)`.slice(0, 255),
          cfdiUuid: null, tipoOrigen: 'Cobro Sucursal', debe: 0, haber: monto,
        });
      }
    }
  }

  // Reversión de originales cancelados-con-sustitución del mismo periodo (ver
  // `sustitutosMismoPeriodoProp`/`cfdisOriginalesCanceladosProp` arriba): ya
  // se generó su asiento normal (como si estuviera vigente, junto con el
  // resto del batch); aquí se agrega el asiento contrario (debe/haber
  // invertidos) que lo cancela — confirmado con el usuario 2026-08-13.
  if (sustitutosMismoPeriodoProp.length) {
    const uuidsOriginalesMismoPeriodoProp = new Set(sustitutosMismoPeriodoProp.flatMap(s => s.sustituyeA));
    const reversionesProp = movimientosResult
      .filter(m => m.cfdiUuid && uuidsOriginalesMismoPeriodoProp.has(m.cfdiUuid.toUpperCase()))
      .map(m => ({
        ...m,
        debe:        m.haber,
        haber:       m.debe,
        tipoOrigen:  'Cancelación por Sustitución',
        reglaNombre: `Reversión — ${m.reglaNombre ?? ''}`.trim(),
      }));
    movimientosResult.push(...reversionesProp);
  }

  // 4. Construir propuesta (no guardada)
  // Si se generó para un día específico (fechaInicio), el encabezado debe
  // mostrar ESE día, no la fecha en la que se corrió la generación — si no,
  // una póliza del 1 de mayo mostraría en el encabezado la fecha de hoy.
  const fecha = fechaInicio ? new Date(`${fechaInicio}T12:00:00.000Z`) : new Date();
  const mesStr = String(periodo).padStart(2, '0');

  // ── Obs 4: detectar facturas PPD con tipoRelacion='07' que deberían ser PUE ──
  // Cuando el anticipo cubre el 100%, la factura final debe emitirse como PUE.
  // Si llega como PPD, el asiento queda incompleto (IVA en Por Trasladar en lugar de Trasladado).
  const ppd07 = cfdisSinPolizaFinal.filter(c =>
    c.tipoDeComprobante === 'I' &&
    c.metodoPago === 'PPD' &&
    c.cfdiRelacionados?.some(r => r.tipoRelacion === '07')
  );

  // Recopilar diagnóstico de CFDIs sin regla para incluir en advertencias
  const _sinReglaInfo = cfdiConRegla
    .filter(({ rule }) => !rule)
    .slice(0, 5)
    .map(({ cfdi: c }) => {
      const tasaDetectada = c.tipoDeComprobante === 'P' ? mappingSvc.detectTasaIva(c) : undefined;
      return (
        `${c.uuid?.slice(0, 8)}… tipo=${c.tipoDeComprobante} método=${c.metodoPago || '—'} ` +
        `forma=${c.formaPago || '—'} emisor=${c.emisor?.rfc || '—'}` +
        (tasaDetectada !== undefined ? ` tasaIva=${tasaDetectada ?? 'null (sin datos de tasa — descarga XML)'}` : '')
      );
    });

  const advertencias = [];
  const _ncFusionadasProp = cfdisConNCSinReversionProp.length - cfdisSinPolizaFinalFiltrado.length;
  if (_ncFusionadasProp > 0) {
    advertencias.push(`${_ncFusionadasProp} Nota(s) de Crédito fusionada(s) en esta póliza de Ingreso (devoluciones/descuentos/bonificaciones/anticipos relacionados)`);
  }
  if (sinRegla > 0) {
    advertencias.push(`${sinRegla} CFDI(s) sin regla de mapeo — las cuentas deben asignarse manualmente`);
    for (const info of _sinReglaInfo) advertencias.push(`  • ${info}`);
    if (sinRegla > 5) advertencias.push(`  … y ${sinRegla - 5} más`);
  }
  if (ppd07.length > 0) {
    advertencias.push(
      `⚠ ${ppd07.length} factura(s) PPD con tipoRelacion='07' (aplicación de anticipo): ` +
      `verificar si el anticipo cubre el 100% — en ese caso debió emitirse como PUE. ` +
      `Folios: ${ppd07.map(c => [c.serie, c.folio].filter(Boolean).join('-')).slice(0, 5).join(', ')}` +
      (ppd07.length > 5 ? ` y ${ppd07.length - 5} más` : ''),
    );
  }
  // Sustitutos excluidos automáticamente por riesgo de doble conteo — ver
  // _particionarSustitutosPorRiesgo. Los "normales" (sin riesgo detectado) no
  // generan advertencia: se contabilizan igual que cualquier otro CFDI.
  if (sustitutosProp.length) {
    advertencias.push(
      `⚠ ${sustitutosProp.length} CFDI(s) sustituto(s) excluido(s) automáticamente de esta póliza por riesgo de doble conteo — revisa la lista "sustitutos" antes de incorporarlos manualmente`,
    );
    for (const s of sustitutosProp.slice(0, 5)) {
      const motivoTxt = s.motivo === 'ya_contabilizado_en_numo'
        ? 'el original ya tiene póliza contabilizada en Numo'
        : `el original es de un periodo anterior (${s.originales.map(o => `${o.periodo ?? '?'}/${o.ejercicio ?? '?'}`).join(', ')})`;
      advertencias.push(`  • ${s.uuid?.slice(0, 8)}… sustituye a ${s.sustituyeA.map(u => u.slice(0, 8)).join(', ')}… — ${motivoTxt}`);
    }
    if (sustitutosProp.length > 5) advertencias.push(`  … y ${sustitutosProp.length - 5} más`);
  }
  // Sustitutos del MISMO periodo: se contabilizaron automático (original con
  // su asiento + reversión, sustituto normal) — solo informativo, no hace
  // falta revisión manual.
  if (sustitutosMismoPeriodoProp.length) {
    advertencias.push(
      `ℹ ${sustitutosMismoPeriodoProp.length} cancelación(es) con sustitución del mismo periodo contabilizada(s) automático (original + asiento de reversión, sustituto normal)`,
    );
  }
  // Tickets de cajas con cobro real pero sin ninguna factura ligada — hoja
  // aparte "por facturar" (ver `_detectarPendientesPorFacturar`), nunca se
  // suman a `movimientos` (confirmado con el usuario 2026-08-04).
  if (pendientesPorFacturarProp.length) {
    const totalPendiente = pendientesPorFacturarProp.reduce((s, p) => s + p.monto, 0);
    advertencias.push(
      `⚠ ${pendientesPorFacturarProp.length} ticket(s) de cajas cobrados este día por $${totalPendiente.toFixed(2)} ` +
      `SIN ninguna factura ligada — ver "pendientesPorFacturar" (no se incluyen en la póliza).`,
    );
  }

  return {
    tipo:       tipoPropuesta,
    fecha:      fecha.toISOString().slice(0, 10),
    // Mismo fix que totalCfdis: con centroCostoId, cfdisSinPoliza.length sigue
    // siendo el total del periodo completo (antes del filtro por sucursal).
    concepto:   tipoPropuesta === 'I'
      ? _construirConceptoIngresoBase({ centroCostoId, ccBySerieMap: ccBySerieMapProp, fechaInicio, fechaFin, ejercicio, periodo })
      : `CFDIs ${mesStr}/${ejercicio} — ${(centroCostoId ? cfdisSinPolizaFinalFiltrado.length : cfdisSinPoliza.length)} comprobante(s)`,
    ejercicio:  Number(ejercicio),
    periodo:    Number(periodo),
    rfc,
    movimientos: _deduplicarSFRedundante(movimientosResult),
    sustitutos: sustitutosProp,
    // Hoja aparte: tickets con cobro real sin factura ligada — ver comentario
    // arriba y `_detectarPendientesPorFacturar` en cobros-sucursal-puente.service.js.
    pendientesPorFacturar: pendientesPorFacturarProp,
    _meta: {
      totalCfdis:   cfdisSinPoliza.length,
      sinRegla,
      advertencias,
    },
  };
}

/**
 * Procesa los CFDIs vigentes del periodo y guarda la póliza directamente
 * como borrador en PostgreSQL. Útil cuando el volumen es demasiado grande
 * para devolver al frontend (>500 CFDIs).
 *
 * Devuelve: { polizaId, totalCfdis, sinRegla, advertencias }
 */
async function generarYGuardar({ rfc, ejercicio, periodo, tipoPropuesta = 'D', tipoCfdi, centroCostoId, fechaInicio, fechaFin, formaPagoFiltro }) {
  if (!rfc)       throw new BadRequestError('RFC requerido');
  if (!ejercicio) throw new BadRequestError('Ejercicio requerido');
  if (!periodo)   throw new BadRequestError('Periodo requerido');
  if (!tipoCfdi)  throw new BadRequestError('Debes seleccionar el tipo de CFDI a procesar (I, E o P)');

  // 1. UUIDs ya contabilizados (filtrado por RFC)
  const yaContabilizados = await PolizaMovimiento.findAll({
    // tipoOrigen != 'Cobro Sucursal': esas líneas solo REFERENCIAN el CFDI
    // (ej. una Factura Global citada por construirMovimientosPuente como
    // `cfdiOriginal` de varios tickets cruzados) — no registran su propia
    // venta. Sin esta exclusión, en cuanto la Global se cita una vez queda
    // "ya contabilizada" para siempre y su propia línea de Venta (Ingresos +
    // IVA) nunca se genera en ningún día (confirmado con el usuario
    // 2026-08-04: Global I0-260700155 nunca aparecía como Venta).
    // reglaNombre != 'COS': ver comentario equivalente en generarPropuesta —
    // mismo problema con el Cargo de "cobrosCobradoraDirecta" etiquetado
    // tipoOrigen='Venta' (caso real VIGUERA N0-260800019, 2026-08-17).
    // `[Op.or]` con reglaNombre: null porque `!=` en SQL no matchea NULL.
    where: {
      cfdiUuid:   { [Op.ne]: null },
      tipoOrigen: { [Op.ne]: 'Cobro Sucursal' },
      [Op.or]:    [{ reglaNombre: { [Op.ne]: 'COS' } }, { reglaNombre: null }],
    },
    attributes: ['cfdiUuid'],
    include: [{
      model:      Poliza,
      as:         'poliza',
      attributes: [],
      where:      { rfc, estado: { [Op.ne]: 'cancelada' } },
      required:   true,
    }],
    raw: true,
  });
  const uuidsYaUsados = new Set(yaContabilizados.map(m => m.cfdiUuid));

  // 2. CFDIs vigentes del periodo (sin límite)
  // fechaInicio/fechaFin (opcionales): ver misma nota en `generarPropuesta`
  // (usa la fecha EFECTIVA vía `_uuidsPorFechaEfectiva`, no el fecha crudo de SAT).
  const uuidsPorFechaGuard = (fechaInicio && fechaFin)
    ? await _uuidsPorFechaEfectiva({ rfc, ejercicio, periodo, tipoCfdi, fechaInicio, fechaFin })
    : null;
  const foliosCancelacionGuard = await _foliosCancelacionDelDia({ rfc, ejercicio, periodo, fechaInicio, fechaFin });
  // Solo CFDIs EMITIDOS por esta entidad — ver comentario equivalente en
  // generarPropuesta (mismo fix, mismo motivo: receptor.rfc colaba compras
  // ajenas dentro de la póliza de Ingresos).
  const filtroBase = {
    'emisor.rfc':      rfc,
    ejercicio:         Number(ejercicio),
    periodo:           Number(periodo),
    tipoDeComprobante: tipoCfdi,
    source:            'SAT',
    satStatus:         'Vigente',
    ...(uuidsPorFechaGuard ? { uuid: { $in: [...uuidsPorFechaGuard] } } : {}),
    isActive:          true,
  };

  const cfdis = await CFDI.find(filtroBase)
    .select('uuid tipoDeComprobante metodoPago formaPago fecha folio serie emisor receptor subTotal total descuento impuestos complementoPago conceptos cfdiRelacionados tasaIvaInferida')
    .lean();

  await repararSubtotalDesdeXml(cfdis);

  // Filtro por forma de pago (solo Cobranza/Pagos) — ver `FORMA_PAGO_A_CATEGORIA`.
  const cfdisSinPoliza = cfdis.filter(c =>
    !uuidsYaUsados.has(c.uuid) &&
    (!formaPagoFiltro || tipoCfdi !== 'P' || FORMA_PAGO_A_CATEGORIA[_formaPagoResuelta(c)] === formaPagoFiltro),
  );

  // Antes ambos casos (cero CFDIs encontrados vs. todos ya poliza'dos) tiraban
  // el mismo mensaje "ya tienen póliza registrada" — confuso cuando en
  // realidad no se encontró ningún CFDI (ej. día sin facturas al generar "por
  // día"): no había nada que contabilizar, no que ya estuviera contabilizado.
  if (cfdis.length === 0) {
    const rango = (fechaInicio && fechaFin)
      ? (fechaInicio === fechaFin ? `el día ${fechaInicio}` : `el rango ${fechaInicio} a ${fechaFin}`)
      : `el periodo ${periodo}/${ejercicio}`;
    throw new BadRequestError(`No se encontró ningún CFDI tipo ${tipoCfdi} para ${rango}`);
  }
  if (cfdisSinPoliza.length === 0) {
    throw new BadRequestError('Todos los CFDIs vigentes del periodo ya tienen póliza registrada');
  }

  // 3. Cargar reglas activas (cacheadas 60s)
  const rules = await _getRulesActive();

  // 4. Pre-fetch tipoDeComprobante de CFDIs relacionados para discriminador relacionadoTipo
  // (r.uuid singular o r.uuids array — cfdiRelacionados usa ambas formas según el origen).
  const relTipoUuidsGuard = [...new Set(
    cfdisSinPoliza
      .flatMap(c => (c.cfdiRelacionados || []).flatMap(r => r.uuids ?? (r.uuid ? [r.uuid] : []))),
  )];
  const relTipoCfdisGuard = relTipoUuidsGuard.length
    ? await CFDI.find({ uuid: { $in: relTipoUuidsGuard } })
        .select('uuid tipoDeComprobante metodoPago formaPago').lean()
    : [];
  const relTipoMapGuard = Object.fromEntries(
    relTipoCfdisGuard.map(c => [c.uuid, c.tipoDeComprobante]),
  );
  // uuid de factura → su metodoPago — usado por _normalizarEgresoCondonacion
  // para resolver el metodoPago real de NCs formaPago=15 (Condonación).
  const relMetodoPagoMapGuard = Object.fromEntries(relTipoCfdisGuard.map(c => [c.uuid, c.metodoPago]));
  // uuid de factura → metodoPago+formaPago — usado por
  // _normalizarEgresoSegunFacturaRelacionada (medios de pago reales).
  const relFacturaMetaMapGuard = Object.fromEntries(relTipoCfdisGuard.map(c => [c.uuid, { metodoPago: c.metodoPago, formaPago: c.formaPago }]));

  const cfdisSinPolizaEnriquecidosGuard = cfdisSinPoliza.map(cfdi => {
    const primerUuid = (cfdi.cfdiRelacionados || [])[0]?.uuid;
    return primerUuid && relTipoMapGuard[primerUuid]
      ? { ...cfdi, _relacionadoTipo: relTipoMapGuard[primerUuid] }
      : cfdi;
  });

  // Enriquecer CFDIs SAT con datos del homólogo ERP — misma lógica que balanza-preliminar.
  const uuidsSinMetaGuard = new Set(
    cfdisSinPolizaEnriquecidosGuard
      .filter(c => c.uuid && (
        !c.formaPago ||
        !c.metodoPago ||
        !c.conceptos?.length ||
        c.conceptos.every(con => !(con.impuestos?.traslados?.length)) ||
        (c.tipoDeComprobante === 'I' && c.metodoPago === 'PPD') ||
        // Enriquecer también sustitutos (tipoRelacion='04'): se conservan en
        // la póliza y necesitan formaPago/conceptos/tipoOrigen del ERP.
        (['E', 'P'].includes(c.tipoDeComprobante) && c.cfdiRelacionados?.length > 0)
      ))
      .map(c => c.uuid),
  );
  let erpMetaMapGuard = {};
  if (uuidsSinMetaGuard.size) {
    const erpCfdisGuard = await CFDI.find({
      uuid:   { $in: [...uuidsSinMetaGuard] },
      source: 'ERP',
    }).select('uuid formaPago metodoPago conceptos impuestos tipoOrigen cfdiRelacionados documentosRelacionados').lean();
    erpMetaMapGuard = Object.fromEntries(erpCfdisGuard.map(c => [c.uuid, c]));
  }
  const cfdisSinPolizaFinalGuard = cfdisSinPolizaEnriquecidosGuard.map(cfdi => {
    const erp = erpMetaMapGuard[cfdi.uuid];
    if (!erp) return cfdi;
    const satHasTraslados     = cfdi.conceptos?.some(con => con.impuestos?.traslados?.length);
    const satHasBaseTraslados = (cfdi.impuestos?.traslados ?? []).some(t => (t.base ?? 0) > 0);
    const relSAT    = cfdi.cfdiRelacionados ?? [];
    const tiposEnSAT = new Set(relSAT.map(r => r.tipoRelacion));
    const relERP    = (erp.cfdiRelacionados ?? []).filter(r => !tiposEnSAT.has(r.tipoRelacion));
    const metodoPagoFinal = (cfdi.metodoPago === 'PPD' && erp.metodoPago === 'PUE')
      ? 'PUE' : (cfdi.metodoPago || erp.metodoPago);
    const esBCT = erp.documentosRelacionados?.some(d => d.Serie === 'BCT');
    const esBON = !esBCT && erp.documentosRelacionados?.some(d => (d.Serie ?? '').startsWith('BON'));
    // Refacturación — ver comentario en generarPropuesta (misma detección,
    // cruce exacto por Folio contra `foliosCancelacionGuard`, más prerequisito
    // `tipoRelacion='04'` — ver comentario completo allá).
    const esRefacturacion = !esBCT && !esBON &&
      !!cfdi.cfdiRelacionados?.some(r => r.tipoRelacion === '04') &&
      erp.documentosRelacionados?.some(d => d.Serie === cfdi.serie && foliosCancelacionGuard.has(d.Folio));
    return {
      ...cfdi,
      formaPago:              cfdi.formaPago  || erp.formaPago,
      metodoPago:             metodoPagoFinal,
      conceptos:              satHasTraslados     ? cfdi.conceptos : (erp.conceptos?.length ? erp.conceptos : cfdi.conceptos ?? []),
      impuestos:              satHasBaseTraslados  ? cfdi.impuestos : (erp.impuestos ?? cfdi.impuestos),
      tipoOrigen:             esBCT ? 'Bonificación Club Tuberos' : esBON ? 'Bonificación' : esRefacturacion ? 'Refacturación' : (cfdi.tipoOrigen ?? erp.tipoOrigen ?? null),
      documentosRelacionados: erp.documentosRelacionados ?? cfdi.documentosRelacionados ?? [],
      cfdiRelacionados:       relERP.length ? [...relSAT, ...relERP] : relSAT,
    };
  });

  // Completar relMetodoPagoMapGuard/relFacturaMetaMapGuard con relacionados que
  // solo aparecieron tras el merge ERP — ver `_completarRelacionadosPostMerge`.
  await _completarRelacionadosPostMerge(cfdisSinPolizaFinalGuard, relMetodoPagoMapGuard, relFacturaMetaMapGuard);

  // Enriquecer tasaIvaInferida en memoria para CFDIs P Metadata.
  // Paso 1: facturas relacionadas en MongoDB SAT. Paso 2: fallback ERP.
  if (tipoCfdi === 'P') {
    await _enrichTasaIvaFromRelatedCfdis(cfdisSinPolizaFinalGuard);
    await _enrichTasaIvaErp(cfdisSinPolizaFinalGuard);
  }

  // Normalización: E PUE formaPago=99 → PPD (en memoria, antes de matching)
  _normalizarEgresoPue99(cfdisSinPolizaFinalGuard);
  // Normalización: E formaPago=15 (Condonación) → metodoPago real de la factura relacionada
  _normalizarEgresoCondonacion(cfdisSinPolizaFinalGuard, relMetodoPagoMapGuard);
  // Normalización: E con medio de pago real (Efectivo/Cheque/Transferencia/Tarjeta)
  // que ajusta una factura PPD nunca cobrada → formaPago+metodoPago de esa factura.
  _normalizarEgresoSegunFacturaRelacionada(cfdisSinPolizaFinalGuard, relFacturaMetaMapGuard);

  // Excluir el CFDI cancelado cuando existe un sustituto (tipoRelacion='04').
  // Genera póliza solo para el CFDI vigente final — espeja CONTPAQi.
  const _canceladosPorSustitutoGuard = new Set(
    cfdisSinPolizaFinalGuard
      .filter(c => c.cfdiRelacionados?.some(r => r.tipoRelacion === '04'))
      .flatMap(c => (c.cfdiRelacionados || [])
        .filter(r => r.tipoRelacion === '04')
        .flatMap(r => r.uuids ?? (r.uuid ? [r.uuid] : []))
        .flatMap(_splitUuids)
        .map(u => u.toUpperCase())
      )
  );
  // Sustitutos del mismo periodo se contabilizan automático (ver comentario
  // equivalente en generarPropuesta) — los demás se excluyen y se listan
  // aparte (`sustitutos`) para revisión manual, ver _particionarSustitutosPorRiesgo.
  const sustitutosEnriquecidosGuard = await _enriquecerSustitutosConPeriodoOriginal(_extraerSustitutos(cfdisSinPolizaFinalGuard));
  const { excluidos: sustitutosClasificadosGuard } = _particionarSustitutosPorRiesgo(sustitutosEnriquecidosGuard, { uuidsYaUsados, ejercicio, periodo });
  const sustitutosMismoPeriodoGuard = sustitutosClasificadosGuard.filter(s => s.mismoPeriodo);
  const sustitutosGuard = sustitutosClasificadosGuard.filter(s => !s.mismoPeriodo);
  const _uuidsSustitutosExcluidosGuard = new Set(sustitutosGuard.map(s => s.uuid?.toUpperCase()).filter(Boolean));

  const cfdisSinPolizaFinalGuardFiltradoSustituto = (_canceladosPorSustitutoGuard.size || _uuidsSustitutosExcluidosGuard.size)
    ? cfdisSinPolizaFinalGuard.filter(c =>
        !_canceladosPorSustitutoGuard.has(c.uuid?.toUpperCase() ?? '') &&
        !_uuidsSustitutosExcluidosGuard.has(c.uuid?.toUpperCase() ?? '')
      )
    : cfdisSinPolizaFinalGuard;

  // Centro de costo por serie de facturación (ver comentario en generarPropuesta).
  const ccBySerieMap = await centrosSvc.resolveBySerieMap();

  const cfdisSinPolizaFinalGuardFiltrado = centroCostoId
    ? cfdisSinPolizaFinalGuardFiltradoSustituto.filter(c =>
        String(ccBySerieMap[c.serie]?.id ?? '') === String(centroCostoId),
      )
    : cfdisSinPolizaFinalGuardFiltradoSustituto;

  if (centroCostoId && cfdisSinPolizaFinalGuardFiltrado.length === 0) {
    const totalSinPolizaGuard = cfdisSinPolizaFinalGuardFiltradoSustituto.length;
    const totalCfdisGuard     = cfdis.length;
    const totalUsadosGuard    = cfdis.filter(c => uuidsYaUsados.has(c.uuid)).length;
    const seriesGuard = [...new Set(cfdisSinPolizaFinalGuardFiltradoSustituto.map(c => c.serie).filter(Boolean))].join(', ') || '(ninguna)';
    throw new BadRequestError(
      `No hay CFDIs sin póliza para la sucursal seleccionada en este periodo. ` +
      `(Total CFDIs del periodo: ${totalCfdisGuard}, ya en póliza: ${totalUsadosGuard}, sin póliza: ${totalSinPolizaGuard}, series disponibles: ${seriesGuard})`,
    );
  }

  // Fusionar NC (tipo E) relacionadas a estas facturas en la MISMA póliza de
  // Ingreso — ver _fetchNotasCreditoParaFusion.
  const cfdisConNCSinReversionGuard = tipoCfdi === 'I'
    ? [...cfdisSinPolizaFinalGuardFiltrado, ...await _fetchNotasCreditoParaFusion(cfdisSinPolizaFinalGuardFiltrado, rfc, uuidsYaUsados, { ejercicio, periodo, fechaInicio, fechaFin, centroCostoId, ccBySerieMap })]
    : cfdisSinPolizaFinalGuardFiltrado;

  // Originales cancelados-con-sustitución del mismo periodo — ver comentario
  // equivalente en generarPropuesta.
  const cfdisOriginalesCanceladosGuard = sustitutosMismoPeriodoGuard.length
    ? await CFDI.find({ uuid: { $in: sustitutosMismoPeriodoGuard.flatMap(s => s.sustituyeA) } })
        .select('uuid tipoDeComprobante metodoPago formaPago fecha folio serie emisor receptor subTotal total descuento impuestos complementoPago conceptos cfdiRelacionados lastComparisonStatus tasaIvaInferida')
        .lean()
    : [];
  const cfdisConNCGuard = [...cfdisConNCSinReversionGuard, ...cfdisOriginalesCanceladosGuard];

  // Ver comentario equivalente en generarPropuesta.
  if (tipoCfdi === 'P') {
    cfdisConNCGuard.sort((a, b) => Number(a.folio) - Number(b.folio));
  }

  // Las NC fusionadas se agregan DESPUÉS del primer `_completarRelacionadosPostMerge`
  // — completar de nuevo (solo agrega lo que aún falte), ver comentario equivalente
  // en generarPropuesta.
  await _completarRelacionadosPostMerge(cfdisConNCGuard, relMetodoPagoMapGuard, relFacturaMetaMapGuard);

  // Serie propia de esta sucursal — ver comentario equivalente en generarPropuesta.
  let serieDelCentroGuard = centroCostoId
    ? Object.entries(ccBySerieMap).find(([, cc]) => String(cc.id) === String(centroCostoId))?.[0]
    : null;

  // Saldos a favor generados por las Devoluciones de este batch — ANTES de
  // construirMovimientosPuente, ver comentario equivalente en generarPropuesta.
  const { mapa: mapaSaldosFavorGeneradosGuard, devsOcultos: devsOcultosSFGuard, ajustesEfectivoRetiroSF: ajustesEfectivoRetiroSFGuard } = await _prefetchSaldosFavorGenerados(cfdisConNCGuard, rfc, ccBySerieMap, {
    centroPropioClave: serieDelCentroGuard,
    fechaDesde: fechaInicio ? _medianocheMx(fechaInicio) : null,
    fechaHasta: fechaFin   ? new Date(_medianocheMx(_diaSiguiente(fechaFin)).getTime() - 1) : null,
  });

  // Cobros de sucursales (Caja/Bancos por identificar) — ver comentario
  // equivalente en generarPropuesta. Se calcula ANTES del loop de reglas para
  // poder omitir, ahí, el Cargo normal de las facturas cuyo Cargo ya cubre
  // este flujo (si no, la póliza queda con doble Cargo contra un solo Abono).
  //
  // Universo ampliado + filtro por fecha real del cobro — ver comentario
  // equivalente en generarPropuesta y `_fetchCfdisParaPuenteAmplio`.
  let movsPuenteGuard = [];
  let facturasVendedorCubiertasGuard = new Map(); // uuid → monto ya cubierto
  let facturasPPDCubiertasGuard = new Map();
  let pendientesPorFacturarGuard = [];
  let cuentaSaldoFavorIdGuard = null;
  let cuentaIvaSaldoFavorIdGuard = null;
  if (tipoCfdi === 'I' && centroCostoId) {
    const { cuentaPuenteId, cuentaCajaId, cuentaBancosId, cuentaSaldoFavorId, cuentaIvaSaldoFavorId, cuentaClubTuberosId } = await _resolverCuentasPuenteSucursales();
    cuentaSaldoFavorIdGuard = cuentaSaldoFavorId;
    cuentaIvaSaldoFavorIdGuard = cuentaIvaSaldoFavorId;
    if (cuentaCajaId && cuentaBancosId) {
      // Acotado a la serie propia — ver comentario equivalente en generarPropuesta.
      const cfdisParaPuenteGuard = (fechaInicio && fechaFin)
        ? await _fetchCfdisParaPuenteAmplio({ rfc, ejercicio, periodo, tipoCfdi, serie: serieDelCentroGuard })
        : cfdisSinPolizaFinalGuard;
      const resultadoPuenteGuard = await construirMovimientosPuente({
        cfdis: cfdisParaPuenteGuard,
        centroCostoId,
        ccBySerieMap: ccBySerieMap,
        cuentaCajaId,
        cuentaBancosId,
        cuentaPuenteId,
        cuentaSaldoFavorId,
        cuentaIvaSaldoFavorId,
        cuentaClubTuberosId,
        fechaDesde: fechaInicio ? _medianocheMx(fechaInicio) : null,
        fechaHasta: fechaFin ? new Date(_medianocheMx(_diaSiguiente(fechaFin)).getTime() - 1) : null,
        rfc,
        devsOcultosSF: devsOcultosSFGuard,
        centroPropioClave: serieDelCentroGuard,
      });
      movsPuenteGuard = resultadoPuenteGuard.movimientos;
      facturasVendedorCubiertasGuard = resultadoPuenteGuard.facturasVendedorCubiertas;
      facturasPPDCubiertasGuard = resultadoPuenteGuard.facturasPPDCubiertas;
      pendientesPorFacturarGuard = resultadoPuenteGuard.pendientesPorFacturar ?? [];
      // Ver comentario en `_uuidsConCargoCubiertoEnBD` — complementa lo
      // detectado hoy con lo ya cubierto en días previos.
      for (const [u, monto] of await _uuidsConCargoCubiertoEnBD({ rfc })) {
        facturasVendedorCubiertasGuard.set(u, (facturasVendedorCubiertasGuard.get(u) ?? 0) + monto);
      }
    }
  }

  // 5. Precalcular regla por CFDI y resolver cuentaMap en un solo query
  const cfdiConRegla = cfdisConNCGuard.map(cfdi => ({
    cfdi,
    rule: mappingSvc.findRuleInList(cfdi, rules),
  }));

  const codigosNecesarios = [...new Set(
    cfdiConRegla
      .filter(({ rule }) => rule)
      .flatMap(({ rule: r }) => [
        r.cuentaCargo, r.cuentaAbono, r.cuentaIva,
        r.cuentaIvaPPD, r.cuentaIvaRetenido, r.cuentaIsrRetenido,
        r.cuentaAbono2, r.cuentaDescuento, r.cuentaDescuento0,
        r.cuentaIvaAnticipo, r.cuentaDeltaAnticipo, r.cuentaCargo2,
        r.cuentaCargoMixto0, r.cuentaIvaAbono,
      ].filter(Boolean))
      // Caja/Bancos/Saldo a Favor/Club Tuberos SIEMPRE — ver comentario
      // equivalente en generarPropuesta.
      .concat([CODIGO_CUENTA_CAJA, CODIGO_CUENTA_BANCOS, CODIGO_CUENTA_SALDO_FAVOR, CODIGO_CUENTA_CLUB_TUBEROS, CODIGO_CUENTA_IVA_SALDO_FAVOR, CODIGO_CUENTA_ANTICIPOS_CLIENTES, CODIGO_CUENTA_IVA_ANTICIPO, CODIGO_CUENTA_PUENTE_SUCURSALES]),
  )];

  const cuentasRows = codigosNecesarios.length
    ? await AccountPlan.findAll({
        where:      { codigo: { [Op.in]: codigosNecesarios } },
        attributes: ['id', 'codigo'],
        raw:        true,
      })
    : [];
  const cuentaMap = Object.fromEntries(cuentasRows.map(c => [c.codigo, c.id]));

  // Desglose real de forma de pago — ver `_prefetchDesglosePagoReal`.
  // Ver comentario equivalente en generarPropuesta sobre centroPropioClave/fechaDesde/fechaHasta.
  const { desglosePagoReal: desglosePagoRealMapGuard, puntosUsado: puntosUsadoMapGuard, saldoFavorUsado: saldoFavorUsadoMapGuard, cobrosCobradoraDirecta: cobrosCobradoraDirectaGuard = [], usoCaminoPorCentro: usoCaminoPorCentroGuard = false } = await _prefetchAjustesFacturaPropia(cfdiConRegla, rfc, {
    centroPropioClave: serieDelCentroGuard,
    fechaDesde: fechaInicio ? _medianocheMx(fechaInicio) : null,
    fechaHasta: fechaFin   ? new Date(_medianocheMx(_diaSiguiente(fechaFin)).getTime() - 1) : null,
  });
  // Ver comentario equivalente en generarPropuesta.
  const { doctosPorUuid: doctosPagoMapGuard } = await _prefetchDoctosPago(cfdiConRegla, rfc);
  const puntosAcumuladosGuard = new Map(); // centroCostoId → { monto, centroCosto }

  // 6. Pre-fetch CFDIs relacionados (5° movimiento anticipo) y saldo a favor
  const relUuidsGuard = [...new Set(
    cfdiConRegla
      .filter(({ rule, cfdi }) => rule?.cuentaDeltaAnticipo && cfdi.cfdiRelacionados?.length)
      .flatMap(({ cfdi }) => cfdi.cfdiRelacionados.map(r => r.uuid).filter(Boolean)),
  )];
  const relCfdiMapGuard = relUuidsGuard.length
    ? Object.fromEntries(
        (await CFDI.find({ uuid: { $in: relUuidsGuard } }).select('uuid total impuestos.totalImpuestosTrasladados').lean())
          .map(c => [c.uuid, c]),
      )
    : {};

  // Aplicación de anticipo SIN Nota de Crédito SAT — ver comentario en
  // `CODIGO_CUENTA_ANTICIPOS_CLIENTES` (misma lógica que en generarPropuesta).
  const _rel07UuidsSinReglaGuard = [...new Set(
    cfdiConRegla
      .filter(({ rule, cfdi }) => cfdi.tipoDeComprobante === 'I' && !rule?.cuentaIvaAnticipo
        && cfdi.cfdiRelacionados?.some(r => r.tipoRelacion === '07'))
      .flatMap(({ cfdi }) => cfdi.cfdiRelacionados
        .filter(r => r.tipoRelacion === '07')
        .flatMap(r => r.uuids ?? (r.uuid ? [r.uuid] : []))),
  )];
  const anticipoCfdisGuard = _rel07UuidsSinReglaGuard.length
    ? await CFDI.find({ uuid: { $in: _rel07UuidsSinReglaGuard } }).select('uuid serie folio total fecha').lean()
    : [];
  const anticipoFolioPorUuidGuard = {
    ...Object.fromEntries(
      anticipoCfdisGuard.map(c => [c.uuid.toUpperCase(), `OPA-${c.folio || c.serie || c.uuid}`]),
    ),
    ...(await _resolverReferenciaOpaPorMonto(anticipoCfdisGuard)),
  };

  let saldoRestanteGuard = 0;
  if (cfdiConRegla.some(({ rule }) => rule?.esAplicacionSaldo)) {
    const rows = await sequelize.query(
      `SELECT COALESCE(SUM(pm.debe) - SUM(pm.haber), 0) AS saldo
       FROM poliza_movimientos pm
       JOIN polizas p ON pm.poliza_id = p.id
       JOIN account_plans ap ON pm.cuenta_id = ap.id
       WHERE p.rfc = :rfc AND ap.codigo = '2103090001' AND p.estado != 'cancelada'`,
      { replacements: { rfc }, type: QueryTypes.SELECT },
    );
    saldoRestanteGuard = Number(rows[0]?.saldo || 0);
  }

  // 6. Generar movimientos en memoria
  // (ccBySerieMap ya se resolvió arriba, antes del filtro por sucursal)

  // ── Fix doble-contabilización anticipo PUE ────────────────────────────────
  // Misma lógica que en generarPropuesta: si hay una factura PUE formaPago=30
  // con tipoRelacion=07 en el batch, la NC tipo E del mismo anticipo se omite.
  const anticosCubiertosPorReg22CGuard = new Set();
  for (const { cfdi: c } of cfdiConRegla) {
    if (c.tipoDeComprobante !== 'I' || c.formaPago !== '30') continue;
    if (c.uuid) anticosCubiertosPorReg22CGuard.add(c.uuid.toUpperCase());
  }

  // Fix 5: verificar también en BD — la NC y la factura final pueden venir en batches distintos.
  {
    const uuids07g = new Set(
      cfdiConRegla
        .filter(({ cfdi: c }) =>
          c.tipoDeComprobante === 'E' &&
          c.cfdiRelacionados?.some(r => r.tipoRelacion === '07'))
        .flatMap(({ cfdi: c }) =>
          (c.cfdiRelacionados || [])
            .filter(r => r.tipoRelacion === '07')
            .flatMap(r => r.uuids ?? (r.uuid ? [r.uuid] : []))
            .map(u => u.toUpperCase()),
        ),
    );
    if (uuids07g.size > 0) {
      const reglasAnticipoG = await CfdiMappingRule.findAll({
        where: { cuentaIvaAnticipo: { [Op.ne]: null } },
        attributes: ['id'], raw: true,
      });
      const idsAnticipoG = reglasAnticipoG.map(r => r.id);
      if (idsAnticipoG.length > 0) {
        const yaEnBDG = await PolizaMovimiento.findAll({
          where: { cfdiUuid: { [Op.in]: [...uuids07g] }, reglaId: { [Op.in]: idsAnticipoG } },
          attributes: ['cfdiUuid'],
          include: [{ model: Poliza, as: 'poliza', attributes: [], where: { rfc, estado: { [Op.ne]: 'cancelada' } }, required: true }],
        });
        for (const m of yaEnBDG) anticosCubiertosPorReg22CGuard.add(m.cfdiUuid.toUpperCase());
      }
    }
  }

  const todosLosMovimientos = [];
  let sinRegla = 0;
  const advertencias = [];
  const ruleUsageCount = new Map();
  // Diagnóstico: acumular los primeros 5 CFDIs sin regla para dar info útil
  const muestrasSinRegla = [];

  for (const { cfdi, rule } of cfdiConRegla) {
    // Omitir NC tipo E (tipoRelacion=07) cuyo anticipo ya fue procesado por Reg 22C
    if (cfdi.tipoDeComprobante === 'E' &&
        cfdi.cfdiRelacionados?.some(r => r.tipoRelacion === '07')) {
      const _rel07g = (cfdi.cfdiRelacionados || []).find(r => r.tipoRelacion === '07');
      const uuid07 = (_rel07g?.uuids?.[0] ?? _rel07g?.uuid ?? '').toUpperCase() || undefined;
      if (uuid07 && anticosCubiertosPorReg22CGuard.has(uuid07)) continue;
    }

    if (!rule) {
      sinRegla++;
      if (muestrasSinRegla.length < 5) {
        const _tasaDet = cfdi.tipoDeComprobante === 'P' ? mappingSvc.detectTasaIva(cfdi) : undefined;
        muestrasSinRegla.push({
          uuid:    cfdi.uuid?.slice(0, 8),
          tipo:    cfdi.tipoDeComprobante,
          metodo:  cfdi.metodoPago,
          forma:   cfdi.formaPago,
          emisor:  cfdi.emisor?.rfc,
          ...(_tasaDet !== undefined ? { tasaIva: _tasaDet ?? 'null (sin datos — descarga XML)' } : {}),
        });
      }
      continue;
    }

    const context = {};
    if (rule?.cuentaDeltaAnticipo && cfdi.cfdiRelacionados?.length) {
      const uuidsGuard = cfdi.cfdiRelacionados.map(r => r.uuid).filter(Boolean);
      const foundGuard = uuidsGuard.some(u => relCfdiMapGuard[u]);
      if (foundGuard) {
        context.totalRelacionado = uuidsGuard
          .reduce((s, u) => s + Number(relCfdiMapGuard[u]?.total || 0), 0);
        context.ivaRelacionado = uuidsGuard
          .reduce((s, u) => s + Number(relCfdiMapGuard[u]?.impuestos?.totalImpuestosTrasladados || 0), 0);
      }
      // Si no se encontró el CFDI relacionado en MongoDB, omitir delta (sin context.totalRelacionado)
    }
    if (rule?.esAplicacionSaldo && saldoRestanteGuard > 0) {
      context.saldoDisponible = saldoRestanteGuard;
    }
    // Las NC (tipo E) deben tratarse como la VENTA ORIGINAL que ajustan, no
    // según su propio metodoPago declarado — ver comentario equivalente en
    // generarPropuesta.
    if (cfdi.tipoDeComprobante === 'E') {
      const metodoPagoRel = _uuidsRelacionados(cfdi).map(u => relMetodoPagoMapGuard[u]).find(Boolean);
      if (metodoPagoRel) context.metodoPagoRelacionado = metodoPagoRel;
    }
    if (cfdi.tipoDeComprobante === 'I' && cfdi.serie && cfdi.folio) {
      const desgloseReal = desglosePagoRealMapGuard.get(`${cfdi.serie}|${cfdi.folio}`);
      // En camino por centro tenemos imagen completa del periodo: si no hay
      // cobro para esta factura en esta sucursal, desglosePagoReal = [] para
      // que cfdiToMovimientos detecte n=0 y marque el Cargo como 'Cobro Sucursal'
      // (registro aparte, no entra al consolidado de Efectivo/Tarjeta).
      if (desgloseReal || usoCaminoPorCentroGuard) context.desglosePagoReal = desgloseReal ?? [];
      const sfUsado = saldoFavorUsadoMapGuard.get(`${cfdi.serie}|${cfdi.folio}`);
      if (sfUsado) {
        // Ver comentario equivalente en generarPropuesta sobre el split por
        // origen (no todo-o-nada) de una Factura Global con SF combinado.
        const detalle = sfUsado.detalle ?? [];
        // Ver comentario equivalente en generarPropuesta sobre `detalleVisible`.
        const detalleVisible = detalle.filter(d => !devsOcultosSFGuard.has(`${d.serieOrigen}|${d.folioOrigen}`));
        const montoOculto = Math.round(detalle
          .filter(d => devsOcultosSFGuard.has(`${d.serieOrigen}|${d.folioOrigen}`))
          .reduce((s, d) => s + (Number(d.monto) || 0), 0) * 100) / 100;
        context.saldoFavorUsadoPropio = {
          ...sfUsado,
          montoOculto,
          montoVisible: Math.round((sfUsado.monto - montoOculto) * 100) / 100,
          detalleVisible,
        };
      }
      const puntosUsadoCfdi = puntosUsadoMapGuard.get(`${cfdi.serie}|${cfdi.folio}`);
      if (puntosUsadoCfdi > 0) context.montoPuntosUsado = puntosUsadoCfdi;
    }
    if (cfdi.tipoDeComprobante === 'P') {
      const doctosPago = doctosPagoMapGuard.get(cfdi.uuid);
      if (doctosPago) context.doctosPago = doctosPago;
    }

    const movs = await mappingSvc.cfdiToMovimientos(cfdi, rule, cuentaMap, context);
    ruleUsageCount.set(rule.id, (ruleUsageCount.get(rule.id) || 0) + 1);

    if (rule?.esAplicacionSaldo) {
      const usado = movs.find(m => m._saldoUsado != null)?._saldoUsado ?? 0;
      saldoRestanteGuard = Math.max(0, saldoRestanteGuard - usado);
    }

    // Marcar movimientos cuya cuenta no existe en el catálogo (cuentaId queda null).
    // Se guardan igualmente para que el usuario los identifique y corrija manualmente.
    const tieneFaltante = movs.some(m => m.cuentaId == null);
    if (tieneFaltante) {
      advertencias.push(`CFDI ${cfdi.uuid?.slice(0, 8)} — una o más cuentas no encontradas en catálogo (regla: ${rule.nombre})`);
    }
    const cc = cfdi.serie ? (ccBySerieMap[cfdi.serie] ?? null) : null;

    // Anticipo sin NC — ver comentario equivalente en generarPropuesta.
    const rel07SinReglaGuard = (cfdi.tipoDeComprobante === 'I' && !rule?.cuentaIvaAnticipo)
      ? cfdi.cfdiRelacionados?.find(r => r.tipoRelacion === '07')
      : null;
    const anticipoFolioRefGuard = rel07SinReglaGuard
      ? anticipoFolioPorUuidGuard[(rel07SinReglaGuard.uuids?.[0] ?? rel07SinReglaGuard.uuid ?? '').toUpperCase()]
      : null;

    // Acumular Puntos usados por esta factura hacia el total de la sucursal
    // (ver comentario equivalente en generarPropuesta).
    const puntosUsadoEstaCfdiGuard = movs.find(m => m._puntosUsado != null)?._puntosUsado ?? 0;
    if (puntosUsadoEstaCfdiGuard > 0 && cc) {
      const prev = puntosAcumuladosGuard.get(cc.id) ?? { monto: 0, centroCosto: cc };
      prev.monto = parseFloat((prev.monto + puntosUsadoEstaCfdiGuard).toFixed(2));
      puntosAcumuladosGuard.set(cc.id, prev);
    }

    // Ver comentario equivalente en generarPropuesta: si esta factura ya
    // recibió (parte de) su Cargo vía cobros-sucursal-puente.service.js, ese
    // monto se RESTA del Cargo normal de la regla (no se omite siempre por
    // completo — corrección 2026-08-06, Facturas Globales).
    const montoCubiertoPorSucursalGuard = facturasVendedorCubiertasGuard.get(cfdi.uuid?.toUpperCase() ?? '') ?? 0;
    let montoCubiertoRestanteGuard = montoCubiertoPorSucursalGuard;
    // Ver comentario equivalente en generarPropuesta: PPD cobrada en otra
    // sucursal — el Cargo a Clientes normal no se toca; se agrega abajo un
    // asiento adicional (Abono a Clientes + Cargo puente, que ya viene en movsPuenteGuard).
    const ppdCubiertaGuard = facturasPPDCubiertasGuard.get(cfdi.uuid?.toUpperCase() ?? '');

    for (const m of movs) {
      // Ver comentario equivalente en generarPropuesta (`_esCargoPrincipal`).
      const esLineaCargoPrincipalGuard = m._esCargoPrincipal === true || (!!rule?.cuentaCargo &&
        m.cuentaId === (cuentaMap[rule.cuentaCargo] ?? null) && m.debe > 0);
      // Anticipo sin NC (ver `anticipoFolioRefGuard` arriba): el Cargo
      // principal NO se registra — se sustituye abajo por Anticipos/IVA-anticipo.
      if (anticipoFolioRefGuard && esLineaCargoPrincipalGuard) continue;
      // Ver comentario equivalente en generarPropuesta: la reducción solo
      // aplica a Caja/Bancos, nunca a SF/Puntos.
      const esLineaCajaOBancosGuard = m.cuentaId === (cuentaMap[CODIGO_CUENTA_CAJA] ?? null) || m.cuentaId === (cuentaMap[CODIGO_CUENTA_BANCOS] ?? null);
      // eslint-disable-next-line no-unused-vars
      const { _saldoUsado, ...cleanM } = m;
      if (montoCubiertoRestanteGuard > 0 && esLineaCargoPrincipalGuard && esLineaCajaOBancosGuard && Number(m.debe) > 0) {
        const reduccion = Math.min(montoCubiertoRestanteGuard, Number(m.debe));
        montoCubiertoRestanteGuard = parseFloat((montoCubiertoRestanteGuard - reduccion).toFixed(2));
        const debeAjustado = parseFloat((Number(m.debe) - reduccion).toFixed(2));
        if (debeAjustado <= 0) continue; // esta línea quedó totalmente cubierta
        todosLosMovimientos.push({
          ...cleanM,
          debe:           debeAjustado,
          cuentaFaltante: cleanM.cuentaId == null,
          centroCosto:    cc?.clave ?? cleanM.centroCosto ?? null,
          centroCostoId:  cc?.id    ?? null,
        });
        continue;
      }
      todosLosMovimientos.push({
        ...cleanM,
        cuentaFaltante: cleanM.cuentaId == null,
        centroCosto:    cc?.clave ?? cleanM.centroCosto ?? null,
        centroCostoId:  cc?.id    ?? null,
      });
    }

    // Anticipo sin NC — ver comentario equivalente en generarPropuesta.
    if (anticipoFolioRefGuard) {
      const montoVentasAnticipoGuard = rule?.cuentaAbono
        ? (movs.find(m => m.cuentaId === (cuentaMap[rule.cuentaAbono] ?? null) && Number(m.haber) > 0)?.haber ?? 0)
        : 0;
      // Puede ser PPD (`rule.cuentaIvaPPD`) o PUE (`rule.cuentaIva`) — ver
      // comentario equivalente en generarPropuesta.
      const montoIvaAnticipoGuard = [rule?.cuentaIva, rule?.cuentaIvaPPD]
        .filter(Boolean)
        .map(cod => movs.find(m => m.cuentaId === (cuentaMap[cod] ?? null) && Number(m.haber) > 0)?.haber)
        .find(v => Number(v) > 0) ?? 0;
      const refOpaGuard = anticipoFolioRefGuard; // ya viene armado como "OPA-..." (real o placeholder)
      const cuentaAnticiposIdGuard = cuentaMap[CODIGO_CUENTA_ANTICIPOS_CLIENTES] ?? null;
      const cuentaIvaAnticipoIdGuard = cuentaMap[CODIGO_CUENTA_IVA_ANTICIPO] ?? null;
      const baseAnticipoGuard = {
        concepto: refOpaGuard, serie: refOpaGuard, centroCosto: cc?.clave ?? null, centroCostoId: cc?.id ?? null,
        cfdiUuid: cfdi.uuid, haber: 0, tipoOrigen: TIPO_ORIGEN_CARGO_ESPECIAL, reglaNombre: 'OPA',
      };
      if (Number(montoVentasAnticipoGuard) > 0) {
        todosLosMovimientos.push({ ...baseAnticipoGuard, cuentaId: cuentaAnticiposIdGuard, debe: montoVentasAnticipoGuard, cuentaFaltante: cuentaAnticiposIdGuard == null });
      }
      if (Number(montoIvaAnticipoGuard) > 0) {
        todosLosMovimientos.push({ ...baseAnticipoGuard, cuentaId: cuentaIvaAnticipoIdGuard, debe: montoIvaAnticipoGuard, cuentaFaltante: cuentaIvaAnticipoIdGuard == null });
      }
    }

    // Saldo a favor generado por esta Devolución — ver comentario
    // equivalente en generarPropuesta.
    const lineasSaldoFavorGuard = await _inyectarSaldoFavorGenerado({
      cfdi, mapaGenerados: mapaSaldosFavorGeneradosGuard,
      cuentaSaldoFavorId: cuentaSaldoFavorIdGuard, cuentaIvaSaldoFavorId: cuentaIvaSaldoFavorIdGuard,
      cuentaCajaId: cuentaMap[CODIGO_CUENTA_CAJA] ?? null, cuentaBancosId: cuentaMap[CODIGO_CUENTA_BANCOS] ?? null,
      cc, rfc,
    });
    for (const linea of lineasSaldoFavorGuard) {
      todosLosMovimientos.push(linea);
    }
    // Cierra el Abono de Caja/Bancos/Clientes de la Devolución — ver
    // comentario equivalente en generarPropuesta.
    if (lineasSaldoFavorGuard.length > 0) {
      const montoSaldoFavorGuard = lineasSaldoFavorGuard.reduce((s, l) =>
        s + (Number(l.haber) || 0) + (l._ajusteConsolidadoSF ? Math.abs(Number(l.debe) || 0) : 0), 0);
      const abonoDevolucionGuard = movs.find(m => Number(m.haber) > 0);
      if (abonoDevolucionGuard && montoSaldoFavorGuard > 0) {
        // Ver comentario equivalente en generarPropuesta: si el SF que se
        // cierra es todo oculto, esta línea debe ocultarse igual que sus
        // hermanas (confirmado con el usuario 2026-08-18, caso real NORBERTO
        // VELAZQUEZ JUAREZ/CAC-077337).
        const todoOcultoGuard = lineasSaldoFavorGuard.every(l => l.reglaNombre === ETIQUETA_SALDO_FAVOR_OCULTO);
        todosLosMovimientos.push({
          ...abonoDevolucionGuard,
          debe:           montoSaldoFavorGuard,
          haber:          0,
          cuentaFaltante: abonoDevolucionGuard.cuentaId == null,
          centroCosto:    cc?.clave ?? abonoDevolucionGuard.centroCosto ?? null,
          centroCostoId:  cc?.id    ?? null,
          ...(todoOcultoGuard ? { tipoOrigen: 'Cobro Sucursal', reglaNombre: ETIQUETA_SALDO_FAVOR_OCULTO } : {}),
        });
      }
    }

    // Ver comentario equivalente en generarPropuesta.
    if (ppdCubiertaGuard && rule?.cuentaCargo && (cuentaMap[rule.cuentaCargo] ?? null)) {
      // Reutiliza concepto/serie de la línea de Cargo normal — ver comentario
      // equivalente en generarPropuesta.
      const cargoOriginalGuard = movs.find(m => m._esCargoPrincipal === true)
        ?? movs.find(m => m.cuentaId === (cuentaMap[rule.cuentaCargo] ?? null) && m.debe > 0);
      const serieCfdiGuard = cargoOriginalGuard?.serie ?? ([cfdi.serie, cfdi.folio].filter(Boolean).join('-').slice(0, 25) || null);
      // Ver comentario equivalente en generarPropuesta: hay que anteponer el
      // nombre del cliente aquí mismo (esta línea no pasa por
      // enriquecerConceptoConCliente).
      const conceptoConClienteGuard = [cfdi.receptor?.nombre, cargoOriginalGuard?.concepto ?? serieCfdiGuard].filter(Boolean).join(' / ');
      todosLosMovimientos.push({
        cuentaId:      cuentaMap[rule.cuentaCargo],
        cuentaFaltante: false,
        concepto:      conceptoConClienteGuard,
        debe:          0,
        haber:         ppdCubiertaGuard.monto,
        serie:         serieCfdiGuard,
        centroCosto:   cc?.clave ?? null,
        centroCostoId: cc?.id    ?? null,
        // Ver comentario equivalente en generarPropuesta: obligatorio para
        // que exportContpaqXlsx la clasifique en el bloque de Crédito.
        metodoPago:    'PPD',
        // Ver comentario equivalente en generarPropuesta: al final del
        // apartado de Crédito, no junto a sus hermanas.
        tipoOrigen:    'Cobro Sucursal',
        reglaNombre:   ppdCubiertaGuard.reglaNombre,
        cfdiUuid:      cfdi.uuid,
      });
    }
  }

  // SF GEN-huérfanos — ver comentario equivalente en generarPropuesta.
  if (cuentaSaldoFavorIdGuard && cuentaIvaSaldoFavorIdGuard) {
    const ccOrfanosGuard = serieDelCentroGuard ? (ccBySerieMap[serieDelCentroGuard] ?? null) : null;
    const clavesConsumidasGuard = new Set(
      cfdisConNCGuard
        .filter(c => c.tipoDeComprobante === 'E')
        .map(c => {
          const marcador = (c.documentosRelacionados ?? [])
            .find(d => TIPO_MARCADORES_DEV.includes((d.Serie ?? '').toUpperCase()) && d.Folio);
          return marcador ? `${marcador.Serie}|${marcador.Folio}` : null;
        })
        .filter(Boolean),
    );
    for (const [key, generado] of mapaSaldosFavorGeneradosGuard) {
      if (clavesConsumidasGuard.has(key)) continue;
      if (!generado?.monto) continue;
      const reglaSFG = generado.oculto ? ETIQUETA_SALDO_FAVOR_OCULTO : 'SF';
      const subtotalG = Math.round((generado.monto / 1.16) * 100) / 100;
      const ivaG      = Math.round((generado.monto - subtotalG) * 100) / 100;
      const serieFolioVentaG = [generado.ventaSerie, generado.ventaFolio].filter(Boolean).join('-') || null;
      const baseG = {
        concepto:       serieFolioVentaG ?? key,
        serie:          serieFolioVentaG,
        centroCosto:    ccOrfanosGuard?.clave ?? null,
        centroCostoId:  ccOrfanosGuard?.id    ?? null,
        cfdiUuid:       null,
        cuentaFaltante: false,
        tipoOrigen:     'Cobro Sucursal',
        reglaNombre:    reglaSFG,
        debe:           0,
      };
      todosLosMovimientos.push({ ...baseG, cuentaId: cuentaSaldoFavorIdGuard,    haber: subtotalG });
      todosLosMovimientos.push({ ...baseG, cuentaId: cuentaIvaSaldoFavorIdGuard, haber: ivaG     });
    }
  }

  // Retiros en EFECTIVO de saldo a favor (ABO) — ver comentario equivalente
  // en generarPropuesta.
  if (cuentaMap[CODIGO_CUENTA_CAJA] && ajustesEfectivoRetiroSFGuard.length) {
    const ccRetiroGuard = serieDelCentroGuard ? (ccBySerieMap[serieDelCentroGuard] ?? null) : null;
    for (const ret of ajustesEfectivoRetiroSFGuard) {
      const serieFolioRetiroG = [ret.ventaSerie, ret.ventaFolio].filter(Boolean).join('-') || null;
      todosLosMovimientos.push({
        concepto:       serieFolioRetiroG ?? 'Retiro de saldo a favor',
        serie:          serieFolioRetiroG,
        centroCosto:    ccRetiroGuard?.clave ?? null,
        centroCostoId:  ccRetiroGuard?.id    ?? null,
        cfdiUuid:       null,
        cuentaFaltante: false,
        tipoOrigen:     'Ajuste Consolidado SF',
        reglaNombre:    'SF-RETIRO-EFECTIVO',
        formaPago:      '01',
        cuentaId:       cuentaMap[CODIGO_CUENTA_CAJA],
        debe:           -ret.monto,
        haber:          0,
        _ajusteConsolidadoSF: true,
      });
    }
  }

  // Puntos/Club Tuberos consolidado del batch — ver comentario equivalente en
  // generarPropuesta.
  for (const { monto, centroCosto: ccPuntosGuard } of puntosAcumuladosGuard.values()) {
    if (monto <= 0 || !cuentaMap[CODIGO_CUENTA_CLUB_TUBEROS] || !cuentaMap[CODIGO_CUENTA_IVA_SALDO_FAVOR]) continue;
    const subtotalGuard = Math.round((monto / 1.16) * 100) / 100;
    const ivaGuard = Math.round((monto - subtotalGuard) * 100) / 100;
    const conceptoConsolidadoGuard = `CLIENTE DE MOSTRADOR SUC. ${ccPuntosGuard?.sucursal ?? ccPuntosGuard?.clave ?? ''}`.trim();
    const baseConsolidadoGuard = {
      concepto: conceptoConsolidadoGuard, centroCosto: ccPuntosGuard?.clave ?? null, centroCostoId: ccPuntosGuard?.id ?? null,
      haber: 0, cfdiUuid: null, tipoOrigen: TIPO_ORIGEN_CARGO_ESPECIAL, reglaNombre: 'PAGO', cuentaFaltante: false,
    };
    todosLosMovimientos.push({ ...baseConsolidadoGuard, cuentaId: cuentaMap[CODIGO_CUENTA_CLUB_TUBEROS] ?? null, debe: subtotalGuard });
    todosLosMovimientos.push({ ...baseConsolidadoGuard, cuentaId: cuentaMap[CODIGO_CUENTA_IVA_SALDO_FAVOR] ?? null, debe: ivaGuard });
  }

  // Facturas PPD cobradas en otra sucursal cuya VENTA ORIGINAL no cae en el
  // lote de hoy — ver comentario equivalente en generarPropuesta.
  const uuidsPPDManejadosGuard = new Set(cfdiConRegla.map(({ cfdi: c }) => c.uuid?.toUpperCase()).filter(Boolean));
  const uuidsPPDOrfanosGuard = [...facturasPPDCubiertasGuard.keys()].filter(u => !uuidsPPDManejadosGuard.has(u));
  if (uuidsPPDOrfanosGuard.length) {
    const satOrfanosGuard = await CFDI.find({ uuid: { $in: uuidsPPDOrfanosGuard }, source: 'SAT' })
      .select('uuid tipoDeComprobante metodoPago formaPago fecha folio serie emisor receptor conceptos impuestos').lean();
    const erpOrfanosGuard = await CFDI.find({ uuid: { $in: uuidsPPDOrfanosGuard }, source: 'ERP' })
      .select('uuid formaPago metodoPago conceptos impuestos').lean();
    const erpOrfanosMapGuard = Object.fromEntries(erpOrfanosGuard.map(c => [c.uuid, c]));
    for (const cfdiOrfanoGuard of satOrfanosGuard) {
      const erpOrfG = erpOrfanosMapGuard[cfdiOrfanoGuard.uuid];
      const cfdiFinalOrfG = {
        ...cfdiOrfanoGuard,
        formaPago:  cfdiOrfanoGuard.formaPago  || erpOrfG?.formaPago,
        metodoPago: cfdiOrfanoGuard.metodoPago || erpOrfG?.metodoPago,
        conceptos:  erpOrfG?.conceptos?.length ? erpOrfG.conceptos : cfdiOrfanoGuard.conceptos,
        impuestos:  erpOrfG?.impuestos ?? cfdiOrfanoGuard.impuestos,
      };
      const ruleOrfanoGuard = mappingSvc.findRuleInList(cfdiFinalOrfG, rules);
      if (!ruleOrfanoGuard?.cuentaCargo) continue;
      let cuentaCargoIdOrfanoGuard = cuentaMap[ruleOrfanoGuard.cuentaCargo];
      if (!cuentaCargoIdOrfanoGuard) {
        const rowOrfanoGuard = await AccountPlan.findOne({ where: { codigo: ruleOrfanoGuard.cuentaCargo }, attributes: ['id'], raw: true });
        cuentaCargoIdOrfanoGuard = rowOrfanoGuard?.id ?? null;
        if (cuentaCargoIdOrfanoGuard) cuentaMap[ruleOrfanoGuard.cuentaCargo] = cuentaCargoIdOrfanoGuard;
      }
      if (!cuentaCargoIdOrfanoGuard) continue;
      const ppdCubiertaOrfanoGuard = facturasPPDCubiertasGuard.get(cfdiOrfanoGuard.uuid.toUpperCase());
      const ccOrfanoGuard = cfdiFinalOrfG.serie ? (ccBySerieMap[cfdiFinalOrfG.serie] ?? null) : null;
      const serieCfdiOrfanoGuard = [cfdiFinalOrfG.serie, cfdiFinalOrfG.folio].filter(Boolean).join('-').slice(0, 25) || null;
      const conceptoOrfanoGuard = [cfdiFinalOrfG.receptor?.nombre, serieCfdiOrfanoGuard].filter(Boolean).join(' / ');
      todosLosMovimientos.push({
        cuentaId:      cuentaCargoIdOrfanoGuard,
        cuentaFaltante: false,
        concepto:      conceptoOrfanoGuard,
        debe:          0,
        haber:         ppdCubiertaOrfanoGuard.monto,
        serie:         serieCfdiOrfanoGuard,
        centroCosto:   ccOrfanoGuard?.clave ?? null,
        centroCostoId: ccOrfanoGuard?.id    ?? null,
        metodoPago:    'PPD',
        tipoOrigen:    'Cobro Sucursal',
        reglaNombre:   ppdCubiertaOrfanoGuard.reglaNombre,
        cfdiUuid:      cfdiOrfanoGuard.uuid,
      });
    }
  }

  // Sustitutos excluidos automáticamente por riesgo de doble conteo — ver
  // _particionarSustitutosPorRiesgo. Los "normales" (sin riesgo detectado) no
  // generan advertencia: ya están contabilizados igual que cualquier CFDI.
  if (sustitutosGuard.length) {
    advertencias.push(
      `⚠ ${sustitutosGuard.length} CFDI(s) sustituto(s) excluido(s) automáticamente de esta póliza por riesgo de doble conteo — revisa "sustitutosExcluidos" antes de incorporarlos manualmente`,
    );
    for (const s of sustitutosGuard.slice(0, 5)) {
      const motivoTxt = s.motivo === 'ya_contabilizado_en_numo'
        ? 'el original ya tiene póliza contabilizada en Numo'
        : `el original es de un periodo anterior (${s.originales.map(o => `${o.periodo ?? '?'}/${o.ejercicio ?? '?'}`).join(', ')})`;
      advertencias.push(`  • ${s.uuid?.slice(0, 8)}… sustituye a ${s.sustituyeA.map(u => u.slice(0, 8)).join(', ')}… — ${motivoTxt}`);
    }
    if (sustitutosGuard.length > 5) advertencias.push(`  … y ${sustitutosGuard.length - 5} más`);
  }
  // Sustitutos del MISMO periodo: se contabilizaron automático — ver
  // comentario equivalente en generarPropuesta.
  if (sustitutosMismoPeriodoGuard.length) {
    advertencias.push(
      `ℹ ${sustitutosMismoPeriodoGuard.length} cancelación(es) con sustitución del mismo periodo contabilizada(s) automático (original + asiento de reversión, sustituto normal)`,
    );
  }

  // Cobros de sucursales — ya calculado arriba (antes del loop de reglas),
  // ver `movsPuenteGuard`. Se agrega ANTES de calcular folios para que entre
  // en el mismo cálculo de rango/chunking que el resto.
  todosLosMovimientos.push(...movsPuenteGuard);

  // Cobros de series ajenas recibidos físicamente en ESTA sucursal —
  // ver comentario equivalente en generarPropuesta (lógica idéntica).
  const _ccCobradoraGuard = serieDelCentroGuard ? (ccBySerieMap[serieDelCentroGuard] ?? null) : null;
  if (cobrosCobradoraDirectaGuard.length > 0 && _ccCobradoraGuard) {
    const cuentaCajaIdCos   = cuentaMap[CODIGO_CUENTA_CAJA]   ?? null;
    const cuentaBancosIdCos = cuentaMap[CODIGO_CUENTA_BANCOS] ?? null;
    // Mismo criterio de dedup que en generarPropuesta — ver comentario allá.
    const _uuidsYaEnPuenteGuard = new Set(
      movsPuenteGuard
        .filter(m => m.tipoOrigen === 'Cobro Sucursal' && m.cfdiUuid)
        .map(m => m.cfdiUuid.toUpperCase()),
    );
    const _foliosYaEnPuenteGuard = new Set(
      movsPuenteGuard.filter(m => m.tipoOrigen === 'Cobro Sucursal' && m.folio != null).map(m => String(m.folio)),
    );
    for (const { claveSat, monto, claveFac, serFolTicket, nombre, folioOrigen, cfdiUuid } of cobrosCobradoraDirectaGuard) {
      const esEfe    = claveSat === '01';
      const cuentaCos = esEfe ? cuentaCajaIdCos : cuentaBancosIdCos;
      if (!cuentaCos || monto <= 0) continue;
      // Concepto: documento relacionado (serieVenta-folioVenta), nunca la
      // factura (`claveFac`) — ver comentario equivalente en generarPropuesta.
      const _serFolG = serFolTicket || (claveFac ?? '');
      if (cfdiUuid && _uuidsYaEnPuenteGuard.has(cfdiUuid.toUpperCase())) continue;
      if (folioOrigen != null && _foliosYaEnPuenteGuard.has(String(folioOrigen))) continue;
      const _conceptoG = nombre ? `${nombre} / ${_serFolG}` : _serFolG;
      const baseCos = {
        concepto:      _conceptoG.slice(0, 255) || 'Cobro Suc. Ajena',
        centroCosto:   _ccCobradoraGuard.clave ?? null,
        centroCostoId: _ccCobradoraGuard.id    ?? null,
        reglaNombre:   'COS',
        formaPago:     claveSat || null,
      };
      // Mismo criterio que en generarPropuesta (ver comentario allá): Efectivo
      // se puede mover entre sucursales (Abono a la misma cuenta, emparejado
      // por cfdiUuid, ambas líneas se sacan del consolidado). Tarjeta/
      // Transferencia NUNCA se mueven — el banco ya depositó aquí — así que su
      // Abono va a la cuenta puente SIN cfdiUuid, para que el Cargo sí cuente
      // en el consolidado.
      // `cfdiUuid` solo se conserva para Efectivo — ver comentario equivalente en generarPropuesta.
      todosLosMovimientos.push({ ...baseCos, cuentaId: cuentaCos, cfdiUuid: esEfe ? (cfdiUuid ?? null) : null, tipoOrigen: 'Venta', debe: monto, haber: 0 });
      if (esEfe) {
        todosLosMovimientos.push({ ...baseCos, cuentaId: cuentaCos, cfdiUuid: cfdiUuid ?? null, tipoOrigen: 'Cobro Sucursal', debe: 0, haber: monto });
      } else {
        // Concepto distinto al del Cargo — ver comentario equivalente en generarPropuesta.
        todosLosMovimientos.push({
          ...baseCos, cuentaId: cuentaMap[CODIGO_CUENTA_PUENTE_SUCURSALES] ?? null,
          concepto: `${baseCos.concepto} (cruce sucursal)`.slice(0, 255),
          cfdiUuid: null, tipoOrigen: 'Cobro Sucursal', debe: 0, haber: monto,
        });
      }
    }
  }

  // Reversión de originales cancelados-con-sustitución del mismo periodo —
  // ver comentario equivalente en generarPropuesta. Se agrega ANTES de la
  // transacción de guardado para que quede persistida junto con el resto.
  if (sustitutosMismoPeriodoGuard.length) {
    const uuidsOriginalesMismoPeriodoGuard = new Set(sustitutosMismoPeriodoGuard.flatMap(s => s.sustituyeA));
    const reversionesGuard = todosLosMovimientos
      .filter(m => m.cfdiUuid && uuidsOriginalesMismoPeriodoGuard.has(m.cfdiUuid.toUpperCase()))
      .map(m => ({
        ...m,
        debe:        m.haber,
        haber:       m.debe,
        tipoOrigen:  'Cancelación por Sustitución',
        reglaNombre: `Reversión — ${m.reglaNombre ?? ''}`.trim(),
      }));
    todosLosMovimientos.push(...reversionesGuard);
  }

  // 7. Guardar póliza + movimientos en una transacción con advisory lock
  // Si se generó para un día específico (fechaInicio), el encabezado debe
  // mostrar ESE día, no la fecha en la que se corrió la generación.
  const fecha    = fechaInicio ? new Date(`${fechaInicio}T12:00:00.000Z`) : new Date();
  const mesStr   = String(periodo).padStart(2, '0');
  // Mismo fix que totalCfdis: con centroCostoId, cfdisSinPoliza.length sigue
  // siendo el total del periodo completo (antes del filtro por sucursal) —
  // se guardaba un concepto con el conteo de TODAS las sucursales aunque la
  // póliza solo tuviera los CFDIs correctos de esta.
  const concepto = tipoPropuesta === 'I'
    ? _construirConceptoIngresoBase({ centroCostoId, ccBySerieMap, fechaInicio, fechaFin, ejercicio, periodo })
    : `CFDIs ${mesStr}/${ejercicio} — ${(centroCostoId ? cfdisSinPolizaFinalGuardFiltrado.length : cfdisSinPoliza.length)} comprobante(s)`;

  const poliza = await sequelize.transaction(async (t) => {
    await sequelize.query(
      'SELECT pg_advisory_xact_lock(hashtext(:key))',
      { replacements: { key: `poliza-${tipoPropuesta}-${rfc}-${ejercicio}-${periodo}` }, transaction: t },
    );

    // Rango de folios reservado por sucursal (ver RANGOS_FOLIO_POR_SUCURSAL):
    // el folio nunca sale de su rango y se reinicia en `desde` cada periodo.
    // Sin centroCostoId (o sucursal sin rango asignado) se conserva el
    // comportamiento anterior: contador simple por tipo/rfc/ejercicio/periodo.
    const centroFolio = centroCostoId
      ? Object.values(ccBySerieMap).find(c => String(c.id) === String(centroCostoId))
      : null;
    const rangoFolio = _rangoFolioPorSucursal(centroFolio?.sucursal);

    // ¿Esta póliza nueva va a mezclar Contado y Crédito? (solo aplica a
    // Ingreso) — si sí, necesita reservar 2 folios consecutivos, no 1.
    // CEDIS es un caso especial que puede necesitar hasta 6 (ver
    // _folioSiguienteDisponible arriba) — se reservan siempre los 6.
    const metodosPagoNuevos = new Set(todosLosMovimientos.map(m => m.metodoPago));
    const consumeDosFolios = tipoPropuesta === 'I'
      && [...metodosPagoNuevos].some(m => m !== 'PPD') && metodosPagoNuevos.has('PPD');
    const foliosNecesarios = _esCedisPorSucursal(centroFolio?.sucursal) ? FOLIOS_MAX_CEDIS : (consumeDosFolios ? 2 : 1);

    const { numero, agotado } = await _folioSiguienteDisponible({
      tipoPropuesta, rfc, ejercicio, periodo, rangoFolio, foliosNecesarios, ccBySerieMap, transaction: t,
    });

    if (agotado) {
      throw new BadRequestError(
        `Se agotó el rango de folios de ${centroFolio.sucursal} para este periodo (${rangoFolio.desde}-${rangoFolio.hasta}).`,
      );
    }

    const polizaHeader = await Poliza.create({
      tipo:      tipoPropuesta,
      numero,
      fecha:     fecha.toISOString().slice(0, 10),
      concepto,
      ejercicio: Number(ejercicio),
      periodo:   Number(periodo),
      rfc,
      estado:    'borrador',
      sustitutosExcluidos: sustitutosGuard.length ? sustitutosGuard : null,
      pendientesPorFacturar: pendientesPorFacturarGuard.length ? pendientesPorFacturarGuard : null,
    }, { transaction: t });

    const movimientosFinales = _deduplicarSFRedundante(todosLosMovimientos);
    for (let i = 0; i < movimientosFinales.length; i += CHUNK_SIZE) {
      const chunk = movimientosFinales.slice(i, i + CHUNK_SIZE);
      const rows  = chunk.map((m, j) => ({
        ...m,
        polizaId: polizaHeader.id,
        orden:    i + j + 1,
      }));
      await PolizaMovimiento.bulkCreate(rows, { transaction: t });
    }

    return polizaHeader;
  });

  // Incrementar contador de uso por regla (fuera de la transacción para no bloquearla)
  if (ruleUsageCount.size > 0) {
    await Promise.all(
      [...ruleUsageCount.entries()].map(([id, count]) =>
        CfdiMappingRule.increment('vecesUsada', { by: count, where: { id } }),
      ),
    );
  }

  const advertenciasFinal = [];
  const _ncFusionadasGuard = cfdisConNCSinReversionGuard.length - cfdisSinPolizaFinalGuardFiltrado.length;
  if (_ncFusionadasGuard > 0) {
    advertenciasFinal.push(`${_ncFusionadasGuard} Nota(s) de Crédito fusionada(s) en esta póliza de Ingreso (devoluciones/descuentos/bonificaciones/anticipos relacionados)`);
  }
  if (sinRegla > 0) {
    advertenciasFinal.push(`${sinRegla} CFDI(s) omitidos por no tener regla de mapeo`);
    // Muestra diagnóstico de los primeros 5 ignorados
    for (const m of muestrasSinRegla) {
      const tasaStr = m.tasaIva !== undefined ? ` tasaIva=${m.tasaIva}` : '';
      advertenciasFinal.push(
        `  Ej. ${m.uuid}… → tipo=${m.tipo} método=${m.metodo || '—'} forma=${m.forma || '—'} emisor=${m.emisor || '—'}${tasaStr}`,
      );
    }
    // Resumen de reglas activas para comparar
    if (rules.length === 0) {
      advertenciasFinal.push('  ⚠ No hay reglas activas en la base de datos');
    } else {
      advertenciasFinal.push(
        `  Reglas activas: ${rules.map(r => `"${r.nombre}" (tipo=${r.tipoComprobante || '*'} método=${r.metodoPago || '*'} forma=${r.formaPago || '*'} RFC=${r.rfcEmisor || '*'}) isActive=${r.isActive}`).join(', ')}`,
      );
    }
  }
  advertenciasFinal.push(...advertencias);
  // Tickets de cajas con cobro real pero sin ninguna factura ligada — hoja
  // aparte, ver comentario equivalente en generarPropuesta.
  if (pendientesPorFacturarGuard.length) {
    const totalPendienteGuard = pendientesPorFacturarGuard.reduce((s, p) => s + p.monto, 0);
    advertenciasFinal.push(
      `⚠ ${pendientesPorFacturarGuard.length} ticket(s) de cajas cobrados este día por $${totalPendienteGuard.toFixed(2)} ` +
      `SIN ninguna factura ligada — ver "pendientesPorFacturar" (no se incluyen en la póliza).`,
    );
  }

  return {
    polizaId:   poliza.id,
    // Bug corregido: con centroCostoId, cfdisSinPoliza.length sigue siendo el
    // total del día/periodo completo (antes del filtro por sucursal) — se
    // reportaba el conteo de TODAS las sucursales aunque la póliza guardada
    // sí contenía solo los CFDIs correctos de esa sucursal.
    totalCfdis:   centroCostoId ? cfdisSinPolizaFinalGuardFiltrado.length : cfdisSinPoliza.length,
    sinRegla,
    advertencias: advertenciasFinal,
    sustitutos:   sustitutosGuard,
    // Hoja aparte: tickets con cobro real sin factura ligada — ver
    // `_detectarPendientesPorFacturar` en cobros-sucursal-puente.service.js.
    pendientesPorFacturar: pendientesPorFacturarGuard,
  };
}

/**
 * Genera una póliza POR CADA sucursal (centro de costo) que tenga CFDIs sin
 * póliza en el periodo, en vez de una sola póliza con todo mezclado.
 * Reutiliza generarYGuardar por sucursal — no duplica lógica de mapeo.
 *
 * Devuelve: { resultados: [{ centroCosto, centroCostoId, polizaId?, totalCfdis?, sinRegla?, error? }] }
 */
async function generarYGuardarPorSucursal({ rfc, ejercicio, periodo, tipoPropuesta = 'D', tipoCfdi, formaPagoFiltro }) {
  const centros = await centrosSvc.list();
  const centrosConSerie = centros.filter(c => c.serieFacturacion);

  if (!centrosConSerie.length) {
    throw new BadRequestError('No hay centros de costo con serie de facturación configurada');
  }

  const resultados = await _conLimite(centrosConSerie, CONCURRENCIA_GENERACION, async (cc) => {
    try {
      const r = await generarYGuardar({ rfc, ejercicio, periodo, tipoPropuesta, tipoCfdi, centroCostoId: cc.id, formaPagoFiltro });
      return { centroCosto: cc.sucursal, centroCostoId: cc.id, ...r };
    } catch (err) {
      // "No hay CFDIs para esta sucursal" es esperado (no toda sucursal tiene
      // movimientos en cada periodo) — se reporta sin detener a las demás.
      return { centroCosto: cc.sucursal, centroCostoId: cc.id, error: err.message };
    }
  });

  return { resultados };
}

function _fmtDia(d) {
  const y  = d.getFullYear();
  const m  = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${dd}`;
}

/**
 * Lista de días (strings 'YYYY-MM-DD') entre fechaInicio/fechaFin, o del mes
 * calendario completo de ejercicio/periodo si no se especifica rango (mismo
 * supuesto "periodo fiscal = mes calendario" que usa el resto del sistema).
 * Construido con componentes y/m/d en vez de toISOString() para no depender
 * de la zona horaria del proceso Node.
 */
function _diasDelRango({ ejercicio, periodo, fechaInicio, fechaFin }) {
  const inicio = fechaInicio ? new Date(`${fechaInicio}T00:00:00`) : new Date(Number(ejercicio), Number(periodo) - 1, 1);
  const fin    = fechaFin    ? new Date(`${fechaFin}T00:00:00`)    : new Date(Number(ejercicio), Number(periodo), 0);

  const dias = [];
  for (const d = new Date(inicio); d <= fin; d.setDate(d.getDate() + 1)) {
    dias.push(_fmtDia(d));
  }
  return dias;
}

// Medianoche de `fechaYMD` en America/Mexico_City, como instante UTC real.
// México abolió el horario de verano (DST) desde 2022 — el offset es fijo
// UTC-6 todo el año, así que sumar 6 horas basta (no hace falta librería de
// zonas horarias).
function _medianocheMx(fechaYMD) {
  return new Date(`${fechaYMD}T06:00:00.000Z`);
}

function _diaSiguiente(fechaYMD) {
  // OJO: no reutilizar _fmtDia aquí — usa getters LOCALES, pero `d` se
  // construye y manipula en términos UTC (mismo tipo de bug de zona horaria
  // ya encontrado antes). Formatear con getters UTC para que sea consistente.
  const d = new Date(`${fechaYMD}T00:00:00.000Z`);
  d.setUTCDate(d.getUTCDate() + 1);
  const y  = d.getUTCFullYear();
  const m  = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${dd}`;
}

/**
 * Resuelve qué UUIDs de CFDI (tipo `tipoCfdi`, del RFC dado) tienen su fecha
 * EFECTIVA dentro de [fechaInicio, fechaFin] — usado para separar pólizas por
 * día. La fecha "efectiva" es la del documento ERP homólogo (mismo uuid,
 * source='ERP') cuando existe: el ERP entrega su fecha con hora real ya
 * resuelta a UTC (`FechaGeneracion`), mientras que la fecha del CFDI/SAT NO
 * trae zona horaria fiable — la mayoría son solo "fecha sin hora" (medianoche
 * UTC ingenua) y el resto es la hora de CDMX mal etiquetada como UTC. Por eso
 * NO se puede simplemente comparar `fecha` de SAT contra límites de huso
 * horario reales: quedaría corrida para las facturas emitidas por la tarde/
 * noche (~16% de los casos verificados). Cuando el CFDI no tiene homólogo ERP
 * (~34% de los casos), se usa su propio fecha de SAT con los mismos límites
 * "ingenuos" (UTC sin ajuste) que ya usaba el sistema — ese fecha, aunque no
 * es UTC real, ya está alineado por casualidad al día calendario de CDMX.
 */
async function _uuidsPorFechaEfectiva({ rfc, ejercicio, periodo, tipoCfdi, fechaInicio, fechaFin }) {
  const naiveInicio = new Date(`${fechaInicio}T00:00:00.000Z`);
  const naiveFin     = new Date(`${fechaFin}T23:59:59.999Z`);
  const mxInicio     = _medianocheMx(fechaInicio);
  const mxFin        = new Date(_medianocheMx(_diaSiguiente(fechaFin)).getTime() - 1);

  // Mismo fix que filtroBase en generarPropuesta/generarYGuardar — este
  // universo de UUIDs por fecha debe coincidir exactamente con el de esas
  // funciones (emisor.rfc únicamente) o el filtro por rango de fechas
  // seleccionaría un conjunto distinto de CFDIs al de la generación real.
  const filtroComun = {
    tipoDeComprobante: tipoCfdi,
    'emisor.rfc': rfc,
  };

  // 1. SAT cuyo fecha "ingenuo" cae en el rango — mismo universo que el
  //    filtro viejo, acotado por periodo (rápido, es el caso de siempre).
  const satNaive = await CFDI.find({
    ...filtroComun, source: 'SAT', ejercicio: Number(ejercicio), periodo: Number(periodo),
    fecha: { $gte: naiveInicio, $lte: naiveFin },
  }).select('uuid').lean();
  const uuidsSatNaive = satNaive.map(c => c.uuid.toUpperCase());

  // 2. De esos (no de TODO el histórico ERP del rfc), cuáles tienen homólogo
  //    ERP — para saber a cuáles no aplicarles el fallback de su fecha SAT.
  const erpDeEsosSat = uuidsSatNaive.length
    ? await CFDI.find({ uuid: { $in: uuidsSatNaive }, source: 'ERP' }).select('uuid').lean()
    : [];
  const uuidsConErp = new Set(erpDeEsosSat.map(c => c.uuid.toUpperCase()));

  // 3. UUIDs cuyo homólogo ERP cae en el rango (huso horario real de México)
  //    — acotado al rango de días, no a todo el histórico. Esto también
  //    reclasifica hacia este día CFDIs cuyo fecha SAT ingenuo cayó en OTRO
  //    día pero cuya fecha ERP real sí es este.
  const erpEnRango = await CFDI.find({ ...filtroComun, source: 'ERP', fecha: { $gte: mxInicio, $lte: mxFin } })
    .select('uuid').lean();
  const resultado = new Set(erpEnRango.map(c => c.uuid.toUpperCase()));

  // 4. SAT sin homólogo ERP → fallback a su propio fecha (ya está en rango,
  //    viene del paso 1). Los que SÍ tienen homólogo se descartan aquí: su
  //    inclusión/exclusión ya la decidió el paso 3 según su fecha ERP real.
  for (const uuid of uuidsSatNaive) {
    if (!uuidsConErp.has(uuid)) resultado.add(uuid);
  }

  return resultado;
}

// Corre `fn` sobre `items` con como máximo `limite` llamadas en vuelo a la
// vez, preservando el orden de `items` en el arreglo devuelto. Usado para que
// generar N pólizas (por día/sucursal) no espere una por una en serie —
// generarYGuardar ya serializa la parte crítica (asignar `numero`) con
// advisory lock por rfc/ejercicio/periodo, así que correr el resto en
// paralelo (fetch/enriquecimiento de CFDIs) es seguro.
async function _conLimite(items, limite, fn) {
  const resultado = new Array(items.length);
  let siguiente = 0;
  async function trabajador() {
    while (siguiente < items.length) {
      const idx = siguiente++;
      resultado[idx] = await fn(items[idx], idx);
    }
  }
  await Promise.all(Array.from({ length: Math.min(limite, items.length) }, trabajador));
  return resultado;
}

const CONCURRENCIA_GENERACION = 4;

/**
 * Genera una póliza POR CADA DÍA del rango indicado (o del mes calendario
 * completo de ejercicio/periodo si no se especifica fechaInicio/fechaFin)
 * que tenga CFDIs sin póliza. Mismo patrón que `generarYGuardarPorSucursal`:
 * reutiliza generarYGuardar por día, no duplica lógica de mapeo.
 *
 * Devuelve: { resultados: [{ fecha, polizaId?, totalCfdis?, sinRegla?, error? }] }
 */
async function generarYGuardarPorDia({ rfc, ejercicio, periodo, tipoPropuesta = 'D', tipoCfdi, centroCostoId, fechaInicio, fechaFin, formaPagoFiltro }) {
  if (!ejercicio) throw new BadRequestError('Ejercicio requerido');
  if (!periodo)   throw new BadRequestError('Periodo requerido');

  const dias = _diasDelRango({ ejercicio, periodo, fechaInicio, fechaFin });
  if (!dias.length) throw new BadRequestError('Rango de fechas inválido');

  const resultados = await _conLimite(dias, CONCURRENCIA_GENERACION, async (dia) => {
    try {
      const r = await generarYGuardar({ rfc, ejercicio, periodo, tipoPropuesta, tipoCfdi, centroCostoId, fechaInicio: dia, fechaFin: dia, formaPagoFiltro });
      return { fecha: dia, ...r };
    } catch (err) {
      // "No hay CFDIs para este día" es esperado (no todos los días tienen
      // movimientos) — se reporta sin detener a los demás.
      return { fecha: dia, error: err.message };
    }
  });

  return { resultados };
}

/**
 * Genera una póliza POR CADA COMBINACIÓN sucursal × día — el cruce de
 * `generarYGuardarPorSucursal` y `generarYGuardarPorDia`. Útil para el
 * export a CONTPAQ en ZIP con una carpeta por sucursal y un archivo por día
 * dentro de cada una.
 *
 * Devuelve: { resultados: [{ centroCosto, centroCostoId, fecha, polizaId?, totalCfdis?, sinRegla?, error? }] }
 */
async function generarYGuardarPorSucursalYDia({ rfc, ejercicio, periodo, tipoPropuesta = 'D', tipoCfdi, fechaInicio, fechaFin, formaPagoFiltro }) {
  if (!ejercicio) throw new BadRequestError('Ejercicio requerido');
  if (!periodo)   throw new BadRequestError('Periodo requerido');

  const centros = await centrosSvc.list();
  const centrosConSerie = centros.filter(c => c.serieFacturacion);
  if (!centrosConSerie.length) {
    throw new BadRequestError('No hay centros de costo con serie de facturación configurada');
  }

  const dias = _diasDelRango({ ejercicio, periodo, fechaInicio, fechaFin });
  if (!dias.length) throw new BadRequestError('Rango de fechas inválido');

  // Aplanar sucursal × día en una sola lista de combinaciones para que el
  // límite de concurrencia aplique sobre el total, no por sucursal.
  const combinaciones = centrosConSerie.flatMap(cc => dias.map(dia => ({ cc, dia })));

  const resultados = await _conLimite(combinaciones, CONCURRENCIA_GENERACION, async ({ cc, dia }) => {
    try {
      const r = await generarYGuardar({ rfc, ejercicio, periodo, tipoPropuesta, tipoCfdi, centroCostoId: cc.id, fechaInicio: dia, fechaFin: dia, formaPagoFiltro });
      return { centroCosto: cc.sucursal, centroCostoId: cc.id, fecha: dia, ...r };
    } catch (err) {
      // "No hay CFDIs para esta sucursal/día" es esperado — se reporta sin
      // detener las demás combinaciones.
      return { centroCosto: cc.sucursal, centroCostoId: cc.id, fecha: dia, error: err.message };
    }
  });

  return { resultados };
}

module.exports = {
  generarPropuesta, generarYGuardar, generarYGuardarPorSucursal,
  generarYGuardarPorDia, generarYGuardarPorSucursalYDia,
  _uuidsPorFechaEfectiva,
  _prefetchSaldosFavorGenerados, _inyectarSaldoFavorGenerado, _formaPagoDominante,
  _prefetchAjustesFacturaPropia,
};
