'use strict';

/**
 * cobranza-poliza-generator.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Generación de pólizas de Cobranza (Pagos, CFDI tipoDeComprobante='P') —
 * COMPLETAMENTE INDEPENDIENTE de cfdi-poliza-generator.service.js/
 * cfdi-mapping.service.js (Ingreso/Egreso): no comparte `cfdiToMovimientos`
 * ni el pipeline de `generarPropuesta`/`generarYGuardar` de esos archivos,
 * cada uno tiene su propia copia de la lógica que necesita (mismo criterio ya
 * usado en este código para constantes pequeñas — "duplicado a propósito,
 * archivos independientes"). Decisión del usuario 2026-09-01: lo que se haga
 * aquí NO debe poder afectar la generación de pólizas de Ingreso.
 *
 * Lo único que se REUTILIZA (vía require, nunca copiando lógica de negocio)
 * es utilería genérica sin conocimiento de Ingreso/Egreso/Cobranza:
 *   - `findRuleInList` (cfdi-mapping.service.js) — empareja CFDI↔regla, es
 *     genérico para cualquier tipoComprobante.
 *   - `_extraerSustitutos`/`_enriquecerSustitutosConPeriodoOriginal`/
 *     `_particionarSustitutosPorRiesgo` (sustitutos-cfdi.util.js) — ya vive en
 *     su propio módulo justo para evitar acoplarse a los generadores.
 *   - `centrosSvc.resolveBySerieMap` — catálogo de sucursales.
 *   - `_getRulesActive` (balanza-preliminar.service.js) — catálogo de reglas
 *     activas, cacheado.
 *   - `obtenerSaldosFavor`/`obtenerDesglosesCobroAlmacen` (erp-sync.service.js)
 *     — cliente ERP genérico.
 *   - Numeración de folio (`_folioSiguienteDisponible` y utilidades de fecha)
 *     — se REQUIERE (perezosamente, ver abajo) desde
 *     cfdi-poliza-generator.service.js porque Ingreso y Cobranza comparten el
 *     MISMO contador/rango de folio por sucursal — duplicarla arriesgaría
 *     folios chocados entre ambos tipos de póliza.
 */

const { Op } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');
const { Poliza, PolizaMovimiento, AccountPlan, CfdiMappingRule, CobroSucursalPendienteCobranza } = require('../../../shared/models/postgres');
const CFDI = require('../../../visor/models/CFDI');
const { BadRequestError } = require('../../shared/errors/AppError');
const centrosSvc = require('../centros-costo/centros-costo.service');
const mappingSvc = require('./cfdi-mapping.service');
const { _getRulesActive } = require('./balanza-preliminar.service');
const { obtenerSaldosFavor, obtenerDesglosesCobroAlmacen } = require('../erp/erp-sync.service');
const {
  _extraerSustitutos, _enriquecerSustitutosConPeriodoOriginal, _particionarSustitutosPorRiesgo,
} = require('./sustitutos-cfdi.util');

// ── Constantes propias (duplicadas a propósito — ver docstring de arriba) ──
const CODIGO_CUENTA_CAJA            = '1101010003';
const CODIGO_CUENTA_BANCOS          = '1102011005';
const CODIGO_CUENTA_SALDO_FAVOR     = '2103090001';
const CODIGO_CUENTA_IVA_SALDO_FAVOR = '2104010002';
// "Cobros De Sucursales Por Identificar" — mismo código que usa Ingreso
// (cobros-sucursal-puente.service.js), duplicado a propósito. Es una cuenta
// puente genérica del catálogo, no lógica de negocio de Ingreso: no hay
// riesgo real en reutilizar el mismo código de cuenta contable aquí.
const CODIGO_CUENTA_PUENTE_SUCURSALES = '2103040001';
// Cuentas estándar de tasa 16% usadas para cerrar un cobro de otra sucursal
// (ver `CobroSucursalPendienteCobranza`) — el registro encolado no conserva
// qué regla/tasa usó la factura original, así que el cierre siempre usa las
// cuentas estándar (estas mismas en TODOS los ejemplos reales vistos hasta
// ahora). Si una factura a tasa 0%/mixta llega a cobrarse cruzada, quedaría
// mal clasificada aquí — no hay caso real confirmado todavía de esa
// combinación, se deja como limitación conocida.
const CODIGO_CUENTA_IVA_POR_TRASLADAR = '2105010001';
const CODIGO_CUENTA_IVA_TRASLADADO    = '2104010001';
const CODIGO_CUENTA_CLIENTES          = '1103010001';
const TASA_IVA_SALDO_FAVOR          = 0.16;
const TIPO_ORIGEN_CARGO_ESPECIAL    = 'Cargo Especial';
const CHUNK_SIZE                    = 200;

function _fmtDMY(fechaISO) {
  const [y, m, d] = fechaISO.split('-');
  return `${d}/${m}/${y}`;
}

// Mismo criterio que `_diaMx`/`_diferenciaDiasMx` en cfdi-poliza-generator.
// service.js (duplicado a propósito) — día calendario en México (UTC-6 fijo,
// sin horario de verano desde 2022).
function _diaMx(fechaIso) {
  if (!fechaIso) return null;
  return new Date(new Date(fechaIso).getTime() - 6 * 3600 * 1000).toISOString().slice(0, 10);
}
function _diferenciaDiasMx(fechaIso, diaYaResuelto) {
  const diaCobro = _diaMx(fechaIso);
  if (!diaCobro || !diaYaResuelto) return null;
  const msCobro     = new Date(`${diaCobro}T00:00:00Z`).getTime();
  const msObjetivo  = new Date(`${diaYaResuelto}T00:00:00Z`).getTime();
  return Math.round(Math.abs(msCobro - msObjetivo) / (24 * 3600 * 1000));
}

// Mismo criterio que `_referenciaDocRelacionado` en cfdi-mapping.service.js
// (duplicado a propósito): un CFDI con `documentosRelacionados` (ajuste del
// ERP) muestra esa referencia en el concepto en vez de la descripción.
function _referenciaDocRelacionado(documentosRelacionados, serieCfdiPropia) {
  const lista = documentosRelacionados || [];
  const esAmbiguo = lista.length > 1;
  for (const d of lista) {
    if (!d.Serie) continue;
    const mismaSerie = (d.Serie ?? '').toUpperCase() === (serieCfdiPropia ?? '').toUpperCase();
    if (mismaSerie && esAmbiguo) continue;
    return d.Folio ? `${d.Serie}-${d.Folio}`.slice(0, 25) : d.Serie;
  }
  return null;
}

/**
 * Para cada CFDI tipo P (Complemento de Pago), extrae de
 * `complementoPago.pagos[].doctosRelacionados[]` la serie/folio/monto de CADA
 * factura que ese Pago liquida (`doctosPorUuid`, indexado por el UUID del
 * propio Pago) — usado por `cfdiToMovimientosCobranza` para armar un asiento
 * completo POR FACTURA liquidada (Cargo + IVA + Abono), en vez de una sola
 * línea agregada con el concepto genérico del Pago completo.
 *
 * También adjunta a cada factura `montoSF` (saldo a favor usado en ESA
 * liquidación puntual, vía `/desgloses-cobro/saldos-favor`) y
 * `desglosePagoReal` (forma de pago real de ESA liquidación — Efectivo/
 * Tarjeta/Transferencia —, vía `/desgloses-cobro/almacen`, filtrado por el
 * día calendario del Pago: ese endpoint responde por serie/folio de la
 * FACTURA, así que sin este filtro se arrastraría el historial COMPLETO de
 * cobros de la factura — incluidos pagos parciales previos — en vez de solo
 * el de hoy, desbalanceando el asiento).
 */
async function _prefetchDoctosPago(cfdiConRegla, rfc) {
  const pagos = cfdiConRegla.filter(({ cfdi }) => cfdi.tipoDeComprobante === 'P');
  const doctosPorUuid    = new Map(); // uuid del Pago → [{ serie, folio, monto, montoSF, ivaDoc, desglosePagoReal }]
  const paresVistos      = new Map(); // `${serie}|${folio}` → { serie, folio } (dedup entre Pagos)
  const fechaPagoPorUuid = new Map(); // uuid del Pago → cfdi.fecha

  for (const { cfdi } of pagos) {
    fechaPagoPorUuid.set(cfdi.uuid, cfdi.fecha ?? null);
    const doctos = [];
    for (const pago of (cfdi.complementoPago?.pagos ?? [])) {
      for (const dr of (pago.doctosRelacionados ?? [])) {
        const serie = dr.serie ?? null;
        const folio = dr.folio ?? null;
        const monto = Number(dr.impPagado ?? 0);
        if (!serie || !folio || monto <= 0) continue;
        const ivaDoc = (dr.trasladosDR ?? [])
          .filter(t => (t.impuesto || t.Impuesto || '') === '002' && Number(t.tasaOCuota ?? t.TasaOCuota ?? 0) > 0)
          .reduce((s, t) => s + Number(t.importe || t.importeDR || t.ImporteDR || 0), 0);
        doctos.push({ serie, folio, monto, montoSF: 0, ivaDoc, desglosePagoReal: [] });
        paresVistos.set(`${serie}|${folio}`, { serie, folio });
      }
    }
    if (doctos.length > 0 && cfdi.uuid) doctosPorUuid.set(cfdi.uuid, doctos);
  }

  if (paresVistos.size === 0) return { doctosPorUuid };

  const pares = [...paresVistos.values()];
  const LOTE  = 150;
  const saldoFavorPorFactura      = new Map(); // `${serie}|${folio}` → monto usado
  const cobrosCrudosPorFactura    = new Map(); // `${serie}|${folio}` → cobro[] crudos (con su `fecha`)
  for (let i = 0; i < pares.length; i += LOTE) {
    const lote = pares.slice(i, i + LOTE);
    const [resultadoSF, resultadoAlmacen] = await Promise.all([
      obtenerSaldosFavor({ rfc, series: lote.map(p => p.serie), folios: lote.map(p => p.folio) }),
      obtenerDesglosesCobroAlmacen({ rfc, series: lote.map(p => p.serie), folios: lote.map(p => p.folio) }),
    ]);
    for (const cuenta of resultadoSF) {
      const usados = cuenta.saldosFavorUsados ?? [];
      if (!usados.length) continue;
      const monto = usados.reduce((s, u) => s + (Math.abs(Number(u.montoUsado)) || 0), 0);
      if (monto > 0) saldoFavorPorFactura.set(`${cuenta.serieVenta}|${cuenta.folioVenta}`, monto);
    }
    for (const cuenta of resultadoAlmacen) {
      const key = `${cuenta.serieVenta}|${cuenta.folioVenta}`;
      const cobros = cuenta.cobros ?? [];
      if (!cobros.length) continue;
      cobrosCrudosPorFactura.set(key, [...(cobrosCrudosPorFactura.get(key) ?? []), ...cobros]);
    }
  }

  const extraerFormasPagoDelDia = (cobros, fechaPago) => {
    const diaPago = _diaMx(fechaPago);
    const formasPago = [];
    for (const cobro of cobros) {
      if (diaPago && _diferenciaDiasMx(cobro.fecha, diaPago) !== 0) continue;
      const cobrosFormaPago = cobro.formasPago ?? [];
      for (const fp of cobrosFormaPago) {
        if (/puntos/i.test(fp.nombre ?? '')) continue;
        if (/saldo\s*a\s*favor/i.test(fp.nombre ?? '')) continue;
        if (/anticipo/i.test(fp.nombre ?? '')) continue;
        const monto = (cobrosFormaPago.length === 1 && cobro.monto != null)
          ? Math.abs(Number(cobro.monto) || 0)
          : (Number(fp.monto) || 0);
        if (monto > 0) formasPago.push({ monto, claveSat: (fp.claveSat ?? '').trim() || null });
      }
    }
    return formasPago;
  };

  for (const [uuid, doctos] of doctosPorUuid.entries()) {
    const fechaPago = fechaPagoPorUuid.get(uuid);
    for (const d of doctos) {
      const sf = saldoFavorPorFactura.get(`${d.serie}|${d.folio}`);
      if (sf > 0) d.montoSF = sf;
      const cobrosCrudos = cobrosCrudosPorFactura.get(`${d.serie}|${d.folio}`) ?? [];
      d.desglosePagoReal = extraerFormasPagoDelDia(cobrosCrudos, fechaPago);
    }
  }

  return { doctosPorUuid };
}

/**
 * Convierte un CFDI tipo P (Complemento de Pago) en sus movimientos
 * contables — equivalente independiente de `cfdiToMovimientos` (cfdi-mapping.
 * service.js) pero SOLO para Pagos: sin las ramas de Ingreso/Egreso (NC,
 * anticipo Reg 22C completo, aplicación de saldo a nueva venta, tasa mixta
 * genérica, desglose real por ticket de Factura Global) que no aplican aquí.
 *
 * `context.doctosPago` (ver `_prefetchDoctosPago`) es lo único que este
 * generador inyecta — cuando viene poblado, arma un asiento completo POR
 * FACTURA liquidada (`esSplitPagoPorFactura`); si no, cae al camino genérico
 * (un solo Cargo + IVA + Abono para todo el Pago).
 */
function cfdiToMovimientosCobranza(cfdi, rule, cuentaMap, context = {}) {
  const cpTotales = cfdi.complementoPago?.totales;
  const cpPagos   = cfdi.complementoPago?.pagos ?? [];

  const total = cpTotales
    ? Number(cpTotales.montoTotalPagos || 0)
    : cpPagos.length > 0
      ? cpPagos.reduce((s, p) => s + Number(p.monto || 0), 0)
      : Number(cfdi.total || 0); // fallback: Descarga Metadata sin complemento

  const iva = cpTotales
    ? Number(cpTotales.totalTrasladosImpuestoIVA16 || 0) + Number(cpTotales.totalTrasladosImpuestoIVA8 || 0)
    : cpPagos.reduce((sum, pago) => sum + (pago.doctosRelacionados ?? []).reduce((s2, dr) =>
        s2 + (dr.trasladosDR ?? [])
          .filter(t => (t.impuesto || t.Impuesto || '') === '002' && Number(t.tasaOCuota ?? t.TasaOCuota ?? 0) > 0)
          .reduce((s3, t) => s3 + Number(t.importe || t.importeDR || t.ImporteDR || 0), 0)
      , 0), 0);

  const ivaRet = Number(cpTotales?.totalRetencionesImpuestoIVA || 0);
  const isrRet = Number(cpTotales?.totalRetencionesImpuestoISR || 0);

  const rfcTercero = cfdi.emisor?.rfc === cfdi.receptor?.rfc ? null : cfdi.receptor?.rfc;

  const descRaw = cfdi.conceptos?.[0]?.descripcion || cfdi.conceptos?.[0]?.Descripcion || '';
  const marcadorAjuste = _referenciaDocRelacionado(cfdi.documentosRelacionados, cfdi.serie);
  const concepto = marcadorAjuste ?? (descRaw.trim() ? descRaw.trim().slice(0, 200) : `CFDI P ${cfdi.uuid?.slice(0, 8)}`);
  const centroCosto = rule?.centroCosto ?? '';
  const ventaFecha  = cfdi.fecha ? new Date(cfdi.fecha).toISOString().slice(0, 10) : null;
  const serieCfdi   = [cfdi.serie, cfdi.folio].filter(Boolean).join('-').slice(0, 25) || null;

  if (!rule) {
    return {
      movs: [
        { cuentaId: null, concepto, centroCosto: '', debe: total, haber: 0,     cfdiUuid: cfdi.uuid, rfcTercero, _sinRegla: true },
        { cuentaId: null, concepto, centroCosto: '', debe: 0,     haber: total, cfdiUuid: cfdi.uuid, rfcTercero, _sinRegla: true },
      ],
      pendientesCruzados: [],
    };
  }

  const movs = [];

  // Anticipo aplicado vía Pago (rarísimo — ninguna regla real de Cobranza lo
  // usa hasta donde se ha confirmado, pero se soporta por si acaso: si la
  // regla trae `cuentaIvaAnticipo`, el swap de IVA se hace igual que en
  // Ingreso, y el split por factura se desactiva, como en el motor original).
  const esAnticipo = !!(rule.cuentaIvaAnticipo && iva > 0);

  // montoCargo/montoAbono para Pago SIEMPRE son estas dos fórmulas — sin
  // importar esAnticipo, ver `cfdiToMovimientos` (cfdi-mapping.service.js):
  // ambas ramas del `if(esAnticipo)` colapsan al mismo valor cuando esPago.
  const montoCargo = total;
  const montoAbono = total - ivaRet - isrRet;

  const esSplitPagoPorFactura = !esAnticipo && rule.tasaIva !== 'mixto'
    && Array.isArray(context.doctosPago) && context.doctosPago.length > 0;

  if (esAnticipo) {
    movs.push({ cuentaId: cuentaMap[rule.cuentaIvaAnticipo] ?? null, concepto: `IVA ant. - ${concepto}`, centroCosto, ventaFecha, serie: serieCfdi, debe: iva, haber: 0, cfdiUuid: cfdi.uuid, rfcTercero });
    if (rule.cuentaIva) {
      movs.push({ cuentaId: cuentaMap[rule.cuentaIva] ?? null, concepto: `IVA ant. - ${concepto}`, centroCosto, ventaFecha, serie: serieCfdi, debe: 0, haber: iva, cfdiUuid: cfdi.uuid, rfcTercero });
    }
  }

  // Motor de tasa mixta (16%/0%) para Cobranza — divide el Abono de CxC entre
  // ambas tasas usando los totales del propio Complemento de Pago. Solo
  // aplica cuando NO se hace split por factura (esSplitPagoPorFactura ya
  // excluye tasaIva='mixto' arriba, igual que en el motor original).
  let montoAbonoFinal = montoAbono;
  if (rule.tasaIva === 'mixto' && rule.cuentaAbono2 && cpTotales) {
    const base16CP = Number(cpTotales.totalTrasladosBaseIVA16 || 0);
    const iva16CP  = Number(cpTotales.totalTrasladosImpuestoIVA16 || 0);
    const monto16  = parseFloat((base16CP + iva16CP).toFixed(2));
    const monto0   = parseFloat((montoAbonoFinal - monto16).toFixed(2));
    if (monto0 > 0.01) {
      montoAbonoFinal = monto16;
      movs.push({ cuentaId: cuentaMap[rule.cuentaAbono2] ?? null, concepto: `${concepto} (0%)`, centroCosto, ventaFecha, serie: serieCfdi, debe: 0, haber: monto0, cfdiUuid: cfdi.uuid, rfcTercero });
    }
  }

  // IVA cobrado sin split por factura (fallback cuando no hay doctosPago).
  if (!esAnticipo && iva > 0 && rule.cuentaIvaPPD && rule.cuentaIva && !esSplitPagoPorFactura) {
    movs.push({ cuentaId: cuentaMap[rule.cuentaIvaPPD] ?? null, concepto: `IVA cobrado - ${concepto}`, centroCosto, ventaFecha, serie: serieCfdi, debe: iva, haber: 0, cfdiUuid: cfdi.uuid, rfcTercero });
    movs.push({ cuentaId: cuentaMap[rule.cuentaIva] ?? null, concepto: `IVA cobrado - ${concepto}`, centroCosto, ventaFecha, serie: serieCfdi, debe: 0, haber: iva, cfdiUuid: cfdi.uuid, rfcTercero });
  }

  if (rule.cuentaIvaRetenido && ivaRet > 0) {
    movs.push({ cuentaId: cuentaMap[rule.cuentaIvaRetenido] ?? null, concepto: `IVA ret. - ${concepto}`, centroCosto, ventaFecha, serie: serieCfdi, debe: 0, haber: ivaRet, cfdiUuid: cfdi.uuid, rfcTercero });
  }
  if (rule.cuentaIsrRetenido && isrRet > 0) {
    movs.push({ cuentaId: cuentaMap[rule.cuentaIsrRetenido] ?? null, concepto: `ISR ret. - ${concepto}`, centroCosto, ventaFecha, serie: serieCfdi, debe: 0, haber: isrRet, cfdiUuid: cfdi.uuid, rfcTercero });
  }

  const pendientesCruzados = [];
  if (esSplitPagoPorFactura) {
    const nombreCliente  = cfdi.receptor?.nombre ?? 'CLIENTE NO IDENTIFICADO';
    const totalDoctos    = context.doctosPago.reduce((s, d) => s + d.monto, 0);
    const aplicaIvaCobrado = iva > 0 && rule.cuentaIvaPPD && rule.cuentaIva;
    let acumuladoCargo = 0;
    let acumuladoAbono = 0;
    context.doctosPago.forEach((d, idx) => {
      const esUltimo = idx === context.doctosPago.length - 1;
      const share = totalDoctos > 0 ? d.monto / totalDoctos : 1 / context.doctosPago.length;
      const conceptoFactura = [nombreCliente, `${d.serie}-${d.folio}`].filter(Boolean).join(' / ');
      const baseFactura = { concepto: conceptoFactura, centroCosto, ventaFecha, serie: serieCfdi, cfdiUuid: cfdi.uuid, rfcTercero };

      // Cobro de otra sucursal (2026-09-01): la factura que este Pago liquida
      // puede haber sido emitida por una sucursal DISTINTA a la que procesó
      // el cobro (`context.ccActual`, resuelta por la serie del propio Pago
      // en `_procesarCobranza`) — mismo problema que ya resuelve Ingreso para
      // ventas, pero aquí a nivel factura liquidada, no ticket. Cuando hay
      // cruce: el Cargo real (banco/efectivo/SF) se queda en ESTA póliza (el
      // dinero sí entró aquí), pero el cierre de la CxC (Abono Clientes + IVA
      // reclasificado) NO — esa es responsabilidad de la sucursal vendedora,
      // así que se reemplaza por un Abono a la cuenta puente (2103040001) y
      // se encola en `pendientesCruzados` para que `_procesarCobranza` lo
      // guarde en CobroSucursalPendienteCobranza; la vendedora lo consume al
      // generar su propia póliza.
      const ccVendedora = context.ccBySerieMap?.[d.serie] ?? null;
      const esCruzada = !!(ccVendedora && context.ccActual && String(ccVendedora.id) !== String(context.ccActual.id));

      // 1. Cargo (+SF si esta factura se pagó con saldo a favor).
      const montoLineaCargo = esUltimo
        ? parseFloat((montoCargo - acumuladoCargo).toFixed(2))
        : parseFloat((montoCargo * share).toFixed(2));
      acumuladoCargo += montoLineaCargo;
      if (montoLineaCargo > 0) {
        const montoSFLinea = cuentaMap[CODIGO_CUENTA_SALDO_FAVOR] && cuentaMap[CODIGO_CUENTA_IVA_SALDO_FAVOR]
          ? Math.min(Number(d.montoSF) || 0, montoLineaCargo)
          : 0;
        let restanteLinea = montoLineaCargo;
        if (montoSFLinea > 0) {
          const subtotalSF = Math.round((montoSFLinea / (1 + TASA_IVA_SALDO_FAVOR)) * 100) / 100;
          const ivaSF      = Math.round((montoSFLinea - subtotalSF) * 100) / 100;
          movs.push({ ...baseFactura, cuentaId: cuentaMap[CODIGO_CUENTA_SALDO_FAVOR],    debe: subtotalSF, haber: 0, tipoOrigen: TIPO_ORIGEN_CARGO_ESPECIAL, reglaNombre: 'SF' });
          movs.push({ ...baseFactura, cuentaId: cuentaMap[CODIGO_CUENTA_IVA_SALDO_FAVOR], debe: ivaSF,      haber: 0, tipoOrigen: TIPO_ORIGEN_CARGO_ESPECIAL, reglaNombre: 'SF' });
          restanteLinea = parseFloat((restanteLinea - montoSFLinea).toFixed(2));
        }
        if (restanteLinea > 0) {
          // Split del Cargo por forma de pago REAL — ver `_prefetchDoctosPago`.
          // Efectivo real → Caja; cualquier otra → la cuenta genérica de la
          // regla (la consolidación por depósito real/sucursal ocurre después,
          // en poliza.service.js). Sin desglose: una sola línea, sin adivinar.
          const desgloseFactura = Array.isArray(d.desglosePagoReal) ? d.desglosePagoReal : [];
          const puedeSplitReal = desgloseFactura.length > 0 && !!cuentaMap[CODIGO_CUENTA_CAJA] && !!cuentaMap[CODIGO_CUENTA_BANCOS];
          if (puedeSplitReal) {
            desgloseFactura.forEach(fp => {
              const montoLinea = Math.round((Number(fp.monto) || 0) * 100) / 100;
              if (montoLinea <= 0) return;
              const esEfectivo = fp.claveSat === '01';
              movs.push({
                ...baseFactura,
                cuentaId: esEfectivo ? cuentaMap[CODIGO_CUENTA_CAJA] : (cuentaMap[rule.cuentaCargo] ?? null),
                debe: montoLinea, haber: 0, _esCargoPrincipal: true,
                _formaPagoReal: fp.claveSat ?? null,
              });
            });
          } else {
            movs.push({ ...baseFactura, cuentaId: cuentaMap[rule.cuentaCargo] ?? null, debe: restanteLinea, haber: 0, _esCargoPrincipal: true });
          }
        }
      }

      const ivaFactura = Number(d.ivaDoc) || 0;

      // Se calcula SIEMPRE (cruzada o no) para que `acumuladoAbono` — y por
      // tanto el residuo de la última factura — quede correcto sin importar
      // si esta línea en particular termina como Abono local o como
      // pendiente cruzado (ver debajo).
      const montoLineaAbono = esUltimo
        ? parseFloat((montoAbonoFinal - acumuladoAbono).toFixed(2))
        : parseFloat((montoAbonoFinal * share).toFixed(2));
      acumuladoAbono += montoLineaAbono;

      if (esCruzada) {
        // Cierra LOCALMENTE contra el puente (por el monto completo de esta
        // factura: incluye tanto la porción de SF como la de banco/efectivo
        // ya cargadas arriba) en vez del Abono a Clientes normal.
        if (montoLineaCargo > 0) {
          movs.push({ ...baseFactura, cuentaId: cuentaMap[CODIGO_CUENTA_PUENTE_SUCURSALES] ?? null, debe: 0, haber: montoLineaCargo, tipoOrigen: 'Cobro Sucursal', reglaNombre: 'COS-COBRANZA' });
        }
        pendientesCruzados.push({
          centroCostoIdDestino: ccVendedora.id,
          centroCostoIdOrigen:  context.ccActual?.id ?? null,
          serieFolioFactura:    `${d.serie}-${d.folio}`,
          nombreCliente,
          montoSubtotal:        parseFloat((montoLineaCargo - ivaFactura).toFixed(2)),
          montoIva:             ivaFactura,
          fechaCobro:           cfdi.fecha ?? null,
          cfdiUuidPago:         cfdi.uuid,
          folioOrigen:          `${cfdi.uuid}|${d.serie}|${d.folio}`,
        });
      } else {
        // 2. IVA cobrado (swap cuentaIvaPPD → cuentaIva) — IVA real de ESTA factura.
        if (aplicaIvaCobrado && ivaFactura > 0) {
          movs.push({ ...baseFactura, cuentaId: cuentaMap[rule.cuentaIvaPPD] ?? null, debe: ivaFactura, haber: 0 });
          movs.push({ ...baseFactura, cuentaId: cuentaMap[rule.cuentaIva]    ?? null, debe: 0, haber: ivaFactura });
        }

        // 3. Abono que cierra la CxC de esta factura.
        if (montoLineaAbono > 0) {
          movs.push({ ...baseFactura, cuentaId: cuentaMap[rule.cuentaAbono] ?? null, debe: 0, haber: montoLineaAbono });
        }
      }
    });
  } else {
    // Sin doctosPago (CFDI sin detalle de complemento, ej. Descarga Metadata):
    // una sola línea de Cargo + una sola línea de Abono para todo el Pago.
    const esEfectivoCfdi = (cfdi.formaPago ?? '') === '01';
    const cuentaCargoFallback = (esEfectivoCfdi ? cuentaMap[CODIGO_CUENTA_CAJA] : cuentaMap[CODIGO_CUENTA_BANCOS]) ?? cuentaMap[rule.cuentaCargo] ?? null;
    movs.push({ cuentaId: cuentaCargoFallback, concepto, centroCosto, ventaFecha, serie: serieCfdi, debe: montoCargo, haber: 0, cfdiUuid: cfdi.uuid, rfcTercero, _esCargoPrincipal: true });
    movs.push({ cuentaId: cuentaMap[rule.cuentaAbono] ?? null, concepto, centroCosto, ventaFecha, serie: serieCfdi, debe: 0, haber: montoAbonoFinal, cfdiUuid: cfdi.uuid, rfcTercero });
  }

  if (!esSplitPagoPorFactura) {
    movs.sort((a, b) => ((a.debe || 0) > 0 ? 0 : 1) - ((b.debe || 0) > 0 ? 0 : 1));
  }

  const _sumDebe  = movs.reduce((s, m) => s + (m.debe  || 0), 0);
  const _sumHaber = movs.reduce((s, m) => s + (m.haber || 0), 0);
  if (Math.abs(_sumDebe - _sumHaber) > 0.01) {
    const { logger } = require('../../../shared/utils/logger');
    logger.warn(`[cfdiToMovimientosCobranza] ASIENTO DESBALANCEADO uuid=${cfdi.uuid} regla="${rule?.nombre}" `
      + `debe=${_sumDebe.toFixed(2)} haber=${_sumHaber.toFixed(2)} diff=${(_sumDebe - _sumHaber).toFixed(2)}`);
    movs.forEach(m => { m._desbalanceado = true; });
  }

  const satMeta = {
    tipoComprobante: 'P',
    metodoPago:      cfdi.metodoPago ?? null,
    formaPago:       cfdi.formaPago ?? cfdi.complementoPago?.pagos?.[0]?.formaDePagoP ?? null,
    folio:           cfdi.folio ?? null,
    rfcEmisor:       cfdi.emisor?.rfc ?? null,
    rfcReceptor:     cfdi.receptor?.rfc ?? null,
    reglaId:         rule?.id ?? null,
    reglaNombre:     rule?.nombre ?? null,
    tipoOrigen:      cfdi.tipoOrigen ?? 'Pago',
  };

  return {
    movs: movs.map(m => ({
      ...m,
      ...satMeta,
      ...(m._formaPagoReal != null ? { formaPago: m._formaPagoReal } : {}),
      ...((m.tipoOrigen === TIPO_ORIGEN_CARGO_ESPECIAL || m.tipoOrigen === 'Cobro Sucursal')
        ? { tipoOrigen: m.tipoOrigen, reglaNombre: m.reglaNombre } : {}),
    })),
    pendientesCruzados,
  };
}

/**
 * Concepto del encabezado de la póliza de Cobranza — "Ingresos por Cobranza
 * del Día: DD/MM/YYYY" (o rango/mes completo), independiente del de Ingreso
 * (`_construirConceptoIngresoBase`, que dice "Ventas").
 */
function _construirConceptoCobranza({ fechaInicio, fechaFin, ejercicio, periodo }) {
  let rango;
  if (fechaInicio && fechaFin && fechaInicio !== fechaFin) {
    rango = `Día: ${_fmtDMY(fechaInicio)} al ${_fmtDMY(fechaFin)}`;
  } else if (fechaInicio) {
    rango = `Día: ${_fmtDMY(fechaInicio)}`;
  } else {
    const ultimoDia = new Date(Date.UTC(Number(ejercicio), Number(periodo), 0)).toISOString().slice(0, 10);
    rango = `Día: ${_fmtDMY(`${ejercicio}-${String(periodo).padStart(2, '0')}-01`)} al ${_fmtDMY(ultimoDia)}`;
  }
  return `Ingresos por Cobranza del ${rango}`;
}

/**
 * Núcleo compartido entre `generarPropuestaCobranza` y `generarYGuardarCobranza`
 * — busca los CFDIs tipo P sin póliza, arma sus movimientos y devuelve todo
 * lo necesario para previsualizar o persistir, sin escribir nada en BD.
 */
async function _procesarCobranza({ rfc, ejercicio, periodo, centroCostoId, fechaInicio, fechaFin, formaPagoFiltro }) {
  // 1. UUIDs ya contabilizados.
  const yaContabilizados = await PolizaMovimiento.findAll({
    where: { cfdiUuid: { [Op.ne]: null } },
    attributes: ['cfdiUuid'],
    include: [{ model: Poliza, as: 'poliza', attributes: [], where: { rfc, estado: { [Op.ne]: 'cancelada' } }, required: true }],
    raw: true,
  });
  const uuidsYaUsados = new Set(yaContabilizados.map(m => m.cfdiUuid));

  // 2. CFDIs tipo P vigentes del periodo.
  const uuidsPorFecha = (fechaInicio && fechaFin)
    ? await require('./cfdi-poliza-generator.service')._uuidsPorFechaEfectiva({ rfc, ejercicio, periodo, tipoCfdi: 'P', fechaInicio, fechaFin })
    : null;
  const filtroBase = {
    'emisor.rfc':      rfc,
    ejercicio:         Number(ejercicio),
    periodo:           Number(periodo),
    tipoDeComprobante: 'P',
    source:            'SAT',
    satStatus:         'Vigente',
    ...(uuidsPorFecha ? { uuid: { $in: [...uuidsPorFecha] } } : {}),
    isActive:          true,
  };
  const cfdis = await CFDI.find(filtroBase)
    .select('uuid tipoDeComprobante metodoPago formaPago fecha folio serie emisor receptor subTotal total descuento impuestos complementoPago conceptos cfdiRelacionados tasaIvaInferida')
    .lean();

  const cfdisSinPoliza = cfdis.filter(c =>
    !uuidsYaUsados.has(c.uuid) &&
    (!formaPagoFiltro || FORMA_PAGO_A_CATEGORIA[_formaPagoResuelta(c)] === formaPagoFiltro),
  );

  if (cfdis.length === 0) {
    const rango = (fechaInicio && fechaFin)
      ? (fechaInicio === fechaFin ? `el día ${fechaInicio}` : `el rango ${fechaInicio} a ${fechaFin}`)
      : `el periodo ${periodo}/${ejercicio}`;
    throw new BadRequestError(`No se encontró ningún CFDI tipo P para ${rango}`);
  }
  if (cfdisSinPoliza.length === 0) {
    throw new BadRequestError('Todos los CFDIs vigentes del periodo ya tienen póliza registrada');
  }

  // 3. Sustitutos (cancelado+reemplazado, tipoRelacion='04') — excluidos por
  // riesgo de doble conteo, mismo criterio genérico que usa Ingreso.
  const sustitutosEnriquecidos = await _enriquecerSustitutosConPeriodoOriginal(_extraerSustitutos(cfdisSinPoliza));
  const { excluidos: sustitutosClasificados } = _particionarSustitutosPorRiesgo(sustitutosEnriquecidos, { uuidsYaUsados, ejercicio, periodo });
  // Sustitutos del MISMO periodo se contabilizan automático (mismo criterio
  // que Ingreso) — solo se excluyen de verdad los que sustituyen un original
  // de OTRO periodo (o ya contabilizado), donde sí hay riesgo real de doble
  // conteo sin revisión manual.
  const sustitutosMismoPeriodo = sustitutosClasificados.filter(s => s.mismoPeriodo);
  const sustitutosExcluidos    = sustitutosClasificados.filter(s => !s.mismoPeriodo);
  const uuidsSustitutosExcluidos = new Set(sustitutosExcluidos.map(s => s.uuid?.toUpperCase()).filter(Boolean));
  const cfdisFinal = uuidsSustitutosExcluidos.size
    ? cfdisSinPoliza.filter(c => !uuidsSustitutosExcluidos.has(c.uuid?.toUpperCase() ?? ''))
    : cfdisSinPoliza;

  // 4. Centro de costo por serie de facturación + filtro por sucursal.
  const ccBySerieMap = await centrosSvc.resolveBySerieMap();
  const cfdisFiltrado = centroCostoId
    ? cfdisFinal.filter(c => String(ccBySerieMap[c.serie]?.id ?? '') === String(centroCostoId))
    : cfdisFinal;
  if (centroCostoId && cfdisFiltrado.length === 0) {
    throw new BadRequestError('No hay CFDIs de Cobranza sin póliza para la sucursal seleccionada en este periodo.');
  }
  const serieDelCentro = centroCostoId
    ? Object.entries(ccBySerieMap).find(([, cc]) => String(cc.id) === String(centroCostoId))?.[0]
    : null;

  // 5. Reglas + cuentas necesarias.
  const rules = await _getRulesActive();
  const cfdiConRegla = cfdisFiltrado.map(cfdi => ({ cfdi, rule: mappingSvc.findRuleInList(cfdi, rules) }));

  const codigosNecesarios = [...new Set(
    cfdiConRegla
      .filter(({ rule }) => rule)
      .flatMap(({ rule: r }) => [r.cuentaCargo, r.cuentaAbono, r.cuentaAbono2, r.cuentaIva, r.cuentaIvaPPD, r.cuentaIvaRetenido, r.cuentaIsrRetenido, r.cuentaIvaAnticipo].filter(Boolean))
      .concat([
        CODIGO_CUENTA_CAJA, CODIGO_CUENTA_BANCOS, CODIGO_CUENTA_SALDO_FAVOR, CODIGO_CUENTA_IVA_SALDO_FAVOR,
        CODIGO_CUENTA_PUENTE_SUCURSALES, CODIGO_CUENTA_IVA_POR_TRASLADAR, CODIGO_CUENTA_IVA_TRASLADADO, CODIGO_CUENTA_CLIENTES,
      ]),
  )];
  const cuentasRows = codigosNecesarios.length
    ? await AccountPlan.findAll({ where: { codigo: { [Op.in]: codigosNecesarios } }, attributes: ['id', 'codigo'], raw: true })
    : [];
  const cuentaMap = Object.fromEntries(cuentasRows.map(c => [c.codigo, c.id]));

  // 6. Desglose real de cobro por factura liquidada.
  const { doctosPorUuid } = await _prefetchDoctosPago(cfdiConRegla, rfc);

  // 7. Generar movimientos.
  const todosLosMovimientos = [];
  let sinRegla = 0;
  const advertencias = [];
  const ruleUsageCount = new Map();
  const muestrasSinRegla = [];
  const pendientesCruzadosParaEncolar = [];

  for (const { cfdi, rule } of cfdiConRegla) {
    if (!rule) {
      sinRegla++;
      if (muestrasSinRegla.length < 5) {
        muestrasSinRegla.push({ uuid: cfdi.uuid?.slice(0, 8), tipo: cfdi.tipoDeComprobante, metodo: cfdi.metodoPago, forma: cfdi.formaPago, emisor: cfdi.emisor?.rfc });
      }
      continue;
    }
    // Sucursal COBRADORA (dueña de la serie del propio Pago) — se resuelve
    // ANTES de armar los movimientos para que `cfdiToMovimientosCobranza`
    // pueda comparar contra la sucursal VENDEDORA de cada factura liquidada
    // y detectar un cobro cruzado (ver `context.ccActual`/`ccBySerieMap`).
    const cc = cfdi.serie ? (ccBySerieMap[cfdi.serie] ?? null) : null;
    const context = { ccActual: cc, ccBySerieMap };
    const doctosPago = doctosPorUuid.get(cfdi.uuid);
    if (doctosPago) context.doctosPago = doctosPago;

    const { movs, pendientesCruzados } = cfdiToMovimientosCobranza(cfdi, rule, cuentaMap, context);
    ruleUsageCount.set(rule.id, (ruleUsageCount.get(rule.id) || 0) + 1);
    pendientesCruzadosParaEncolar.push(...pendientesCruzados.map(p => ({ ...p, rfc })));

    const tieneFaltante = movs.some(m => m.cuentaId == null);
    if (tieneFaltante) {
      advertencias.push(`CFDI ${cfdi.uuid?.slice(0, 8)} — una o más cuentas no encontradas en catálogo (regla: ${rule.nombre})`);
    }
    for (const m of movs) {
      todosLosMovimientos.push({
        ...m,
        cuentaFaltante: m.cuentaId == null,
        centroCosto:    cc?.clave ?? m.centroCosto ?? null,
        centroCostoId:  cc?.id    ?? null,
      });
    }
  }

  // 8. Cobros de otra sucursal PENDIENTES DE CONSUMIR — facturas de ESTA
  // sucursal (centroCostoId, si se especificó una) que se cobraron en otra y
  // ya están encoladas en CobroSucursalPendienteCobranza. Solo aplica cuando
  // se genera acotado a una sucursal (sin ella no hay a quién atribuírselas).
  const pendientesConsumidos = [];
  if (centroCostoId) {
    const pendientesRows = await CobroSucursalPendienteCobranza.findAll({
      where: { rfc, centroCostoIdDestino: centroCostoId, consumido: false },
      raw: true,
    });
    const ccDestino = Object.values(ccBySerieMap).find(c => String(c.id) === String(centroCostoId)) ?? null;
    for (const p of pendientesRows) {
      const baseFactura = {
        concepto:      [p.nombreCliente, p.serieFolioFactura].filter(Boolean).join(' / '),
        centroCosto:   ccDestino?.clave ?? null,
        centroCostoId: Number(centroCostoId),
        serie:         p.serieFolioFactura,
        cfdiUuid:       p.cfdiUuidPago,
        ventaFecha:    p.fechaCobro ? new Date(p.fechaCobro).toISOString().slice(0, 10) : null,
        tipoComprobante: 'P', tipoOrigen: 'Cobro Sucursal', reglaNombre: 'COS-COBRANZA',
      };
      const montoSubtotal = Number(p.montoSubtotal) || 0;
      const montoIva      = Number(p.montoIva) || 0;
      const montoTotal    = parseFloat((montoSubtotal + montoIva).toFixed(2));
      const cuentaPuenteId = cuentaMap[CODIGO_CUENTA_PUENTE_SUCURSALES]   ?? null;
      const cuentaIvaPPDId = cuentaMap[CODIGO_CUENTA_IVA_POR_TRASLADAR]   ?? null;
      const cuentaIvaId    = cuentaMap[CODIGO_CUENTA_IVA_TRASLADADO]      ?? null;
      const cuentaAbonoId  = cuentaMap[CODIGO_CUENTA_CLIENTES]            ?? null;
      if (montoTotal > 0) {
        todosLosMovimientos.push({ ...baseFactura, cuentaId: cuentaPuenteId, cuentaFaltante: cuentaPuenteId == null, debe: montoTotal, haber: 0 });
      }
      if (montoIva > 0 && cuentaIvaPPDId && cuentaIvaId) {
        todosLosMovimientos.push({ ...baseFactura, cuentaId: cuentaIvaPPDId, cuentaFaltante: false, debe: montoIva, haber: 0, tipoOrigen: 'Pago', reglaNombre: null });
        todosLosMovimientos.push({ ...baseFactura, cuentaId: cuentaIvaId,    cuentaFaltante: false, debe: 0, haber: montoIva, tipoOrigen: 'Pago', reglaNombre: null });
      }
      if (montoTotal > 0) {
        todosLosMovimientos.push({ ...baseFactura, cuentaId: cuentaAbonoId, cuentaFaltante: cuentaAbonoId == null, debe: 0, haber: montoTotal, tipoOrigen: 'Pago', reglaNombre: null });
      }
      pendientesConsumidos.push(p.id);
    }
    if (pendientesRows.length) {
      advertencias.push(`ℹ ${pendientesRows.length} factura(s) cobrada(s) en otra sucursal, cerradas aquí contra la cuenta puente`);
    }
  }

  if (sinRegla > 0) {
    advertencias.push(`${sinRegla} CFDI(s) omitidos por no tener regla de mapeo`);
    for (const m of muestrasSinRegla) {
      advertencias.push(`  Ej. ${m.uuid}… → tipo=${m.tipo} método=${m.metodo || '—'} forma=${m.forma || '—'} emisor=${m.emisor || '—'}`);
    }
  }
  if (sustitutosExcluidos.length) {
    advertencias.push(`⚠ ${sustitutosExcluidos.length} CFDI(s) sustituto(s) excluido(s) automáticamente de esta póliza por riesgo de doble conteo — revisa la lista "sustitutos" antes de incorporarlos manualmente`);
  }
  if (sustitutosMismoPeriodo.length) {
    advertencias.push(`ℹ ${sustitutosMismoPeriodo.length} cancelación(es) con sustitución del mismo periodo contabilizada(s) automático`);
  }
  if (pendientesCruzadosParaEncolar.length) {
    advertencias.push(`⚠ ${pendientesCruzadosParaEncolar.length} factura(s) de otra(s) sucursal(es) cobrada(s) aquí — se encolarán para que su sucursal cierre su CxC al generar su propia póliza`);
  }

  return {
    cfdisSinPoliza: cfdisFiltrado, todosLosMovimientos, sinRegla, advertencias, ruleUsageCount,
    sustitutos: sustitutosExcluidos, ccBySerieMap, serieDelCentro,
    pendientesCruzadosParaEncolar, pendientesConsumidos,
  };
}

// Mismas categorías/códigos que usa el export CONTPAQ (poliza.service.js) —
// duplicado a propósito. Solo aplica cuando se filtra por forma de pago.
const FORMA_PAGO_A_CATEGORIA = { '01': 'efectivo', '03': 'transferencia', '02': 'transferencia', '04': 'tarjeta', '28': 'tarjeta' };
function _formaPagoResuelta(cfdi) {
  return cfdi.formaPago ?? cfdi.complementoPago?.pagos?.[0]?.formaDePagoP ?? null;
}

/**
 * Preview (sin persistir) de una póliza de Cobranza — mismo contrato de
 * salida que `generarPropuesta` (Ingreso), pero generado por un pipeline
 * totalmente independiente.
 */
async function generarPropuestaCobranza({ rfc, ejercicio, periodo, tipoPropuesta = 'D', centroCostoId, fechaInicio, fechaFin, formaPagoFiltro }) {
  const { todosLosMovimientos, sinRegla, advertencias, sustitutos, cfdisSinPoliza } =
    await _procesarCobranza({ rfc, ejercicio, periodo, centroCostoId, fechaInicio, fechaFin, formaPagoFiltro });

  const fecha = fechaInicio ? new Date(`${fechaInicio}T12:00:00.000Z`) : new Date();

  return {
    tipo:        tipoPropuesta,
    fecha:       fecha.toISOString().slice(0, 10),
    concepto:    _construirConceptoCobranza({ fechaInicio, fechaFin, ejercicio, periodo }),
    ejercicio:   Number(ejercicio),
    periodo:     Number(periodo),
    rfc,
    movimientos: todosLosMovimientos,
    sustitutos,
    pendientesPorFacturar: [],
    _meta: { totalCfdis: cfdisSinPoliza.length, sinRegla, advertencias },
  };
}

/**
 * Genera y persiste (como póliza en estado 'borrador') los CFDIs tipo P
 * (Cobranza) sin póliza del periodo/sucursal indicados.
 */
async function generarYGuardarCobranza({ rfc, ejercicio, periodo, tipoPropuesta = 'D', centroCostoId, fechaInicio, fechaFin, formaPagoFiltro }) {
  const {
    todosLosMovimientos, sinRegla, advertencias, sustitutos, ccBySerieMap, cfdisSinPoliza, ruleUsageCount,
    pendientesCruzadosParaEncolar, pendientesConsumidos,
  } = await _procesarCobranza({ rfc, ejercicio, periodo, centroCostoId, fechaInicio, fechaFin, formaPagoFiltro });

  // Numeración de folio: se reutiliza la utilería de cfdi-poliza-generator.
  // service.js (require perezoso — ver docstring del archivo) porque Ingreso
  // y Cobranza comparten el mismo contador/rango por sucursal.
  const generatorUtils = require('./cfdi-poliza-generator.service');
  const fecha = fechaInicio ? new Date(`${fechaInicio}T12:00:00.000Z`) : new Date();
  const concepto = _construirConceptoCobranza({ fechaInicio, fechaFin, ejercicio, periodo });

  const poliza = await sequelize.transaction(async (t) => {
    await sequelize.query(
      'SELECT pg_advisory_xact_lock(hashtext(:key))',
      { replacements: { key: `poliza-${tipoPropuesta}-${rfc}-${ejercicio}-${periodo}` }, transaction: t },
    );

    const centroFolio = centroCostoId
      ? Object.values(ccBySerieMap).find(c => String(c.id) === String(centroCostoId))
      : null;
    const rangoFolio = generatorUtils._rangoFolioPorSucursal(centroFolio?.sucursal);
    const foliosNecesarios = generatorUtils._esCedisPorSucursal(centroFolio?.sucursal) ? generatorUtils.FOLIOS_MAX_CEDIS : 1;

    const { numero, agotado } = await generatorUtils._folioSiguienteDisponible({
      tipoPropuesta, rfc, ejercicio, periodo, rangoFolio, foliosNecesarios, ccBySerieMap, transaction: t,
    });
    if (agotado) {
      throw new BadRequestError(`Se agotó el rango de folios de ${centroFolio.sucursal} para este periodo (${rangoFolio.desde}-${rangoFolio.hasta}).`);
    }

    const polizaHeader = await Poliza.create({
      tipo: tipoPropuesta, numero, fecha: fecha.toISOString().slice(0, 10), concepto,
      ejercicio: Number(ejercicio), periodo: Number(periodo), rfc, estado: 'borrador',
      sustitutosExcluidos: sustitutos.length ? sustitutos : null,
      pendientesPorFacturar: null,
    }, { transaction: t });

    for (let i = 0; i < todosLosMovimientos.length; i += CHUNK_SIZE) {
      const chunk = todosLosMovimientos.slice(i, i + CHUNK_SIZE);
      const rows  = chunk.map((m, j) => ({ ...m, polizaId: polizaHeader.id, orden: i + j + 1 }));
      await PolizaMovimiento.bulkCreate(rows, { transaction: t });
    }

    // Cobro de otra sucursal: encola lo que la sucursal vendedora necesita
    // (upsert por rfc+centroCostoIdDestino+folioOrigen — regenerar esta
    // póliza no debe duplicar filas en la cola) y marca como consumidas las
    // que esta misma póliza ya cerró contra el puente.
    for (const p of pendientesCruzadosParaEncolar) {
      await CobroSucursalPendienteCobranza.upsert({ ...p, consumido: false }, { transaction: t });
    }
    if (pendientesConsumidos.length) {
      await CobroSucursalPendienteCobranza.update(
        { consumido: true },
        { where: { id: { [Op.in]: pendientesConsumidos } }, transaction: t },
      );
    }

    return polizaHeader;
  });

  if (ruleUsageCount.size > 0) {
    await Promise.all(
      [...ruleUsageCount.entries()].map(([id, count]) => CfdiMappingRule.increment('vecesUsada', { by: count, where: { id } })),
    );
  }

  const advertenciasFinal = [...advertencias];
  return {
    polizaId:    poliza.id,
    totalCfdis:  cfdisSinPoliza.length,
    sinRegla,
    advertencias: advertenciasFinal,
    sustitutos,
    pendientesPorFacturar: [],
  };
}

module.exports = {
  generarPropuestaCobranza, generarYGuardarCobranza,
  cfdiToMovimientosCobranza, _prefetchDoctosPago,
};
