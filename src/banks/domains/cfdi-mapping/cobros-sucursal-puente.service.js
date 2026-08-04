'use strict';

/**
 * cobros-sucursal-puente.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Cobros hechos en una sucursal distinta a la que facturó (ej. cliente paga
 * en CEDIS una factura emitida en Santa Rosa). El sistema de cajas
 * (app.cajas.tubosyconexiones.mx, vía erp-sync.service.js) expone el desglose
 * de estos cobros por documento relacionado.
 *
 * Reemplaza el registro anterior contra "Anticipos Otros" + "IVA Trasladado -
 * Anticipos" (trataba el cobro como un anticipo nuevo, con IVA que no
 * corresponde). Dos tratamientos distintos según el CFDI original sea PUE o PPD
 * (confirmado con el usuario 2026-08-03):
 *
 * ── PUE (Contado): sin cuenta puente, ambos lados usan directamente Caja o
 *    Bancos por identificar (según claveSat):
 *   - Sucursal VENDEDORA: Cargo a Caja/Bancos por identificar por cada forma
 *     de pago. Sin contrapartida en esa misma póliza — se compensa con el
 *     Abono de la sucursal COBRADORA al consolidar (cada póliza de sucursal
 *     no necesita cuadrar sola contra este movimiento, solo el consolidado).
 *   - Sucursal COBRADORA: Cargo a Caja/Bancos por identificar (el cobro real)
 *     + Abono a esa MISMA cuenta, ambos por forma de pago — el neto de esta
 *     póliza en esas cuentas es cero, pero cada línea cuadra contra su
 *     contraparte de la póliza vendedora al consolidar.
 *
 * ── PPD (Crédito): el Cargo a Clientes normal de la venta se conserva
 *    intacto (lo genera cfdiToMovimientos como siempre) — este flujo agrega
 *    un asiento ADICIONAL que usa la cuenta puente "Cobros De Sucursales Por
 *    Identificar" (2103040001) para cerrar esa cuenta por cobrar:
 *   - Sucursal VENDEDORA: Abono a Clientes (misma cuenta que usó el Cargo
 *     original de la venta — la resuelve cfdi-poliza-generator.service.js vía
 *     `facturasPPDCubiertas`) + Cargo a la cuenta puente (este archivo).
 *   - Sucursal COBRADORA: Cargo a Caja/Bancos por identificar (el cobro real,
 *     por forma de pago) + Abono a la cuenta puente.
 *
 * ── SALDO A FAVOR: cuando una forma de pago del cobro es "saldo a favor"
 *    (aplicación de un saldo existente del cliente, no dinero nuevo), NO usa
 *    Caja/Bancos ni la cuenta puente — usa directamente "Anticipos Otros"
 *    (2103090001, subtotal) + "IVA Trasladado - Anticipos" (2104010002, IVA
 *    16%), con columna C = "SF" (confirmado con el usuario 2026-08-03).
 *    Mismo patrón vendedor/cobrador que Efectivo/Transferencia, pero el
 *    Abono del lado cobrador va a esas MISMAS cuentas (no a la puente,
 *    aunque la factura sea PPD) — no es un movimiento de banco, es puramente
 *    la aplicación del saldo, así que no necesita pasar por la puente.
 */

const { Op } = require('sequelize');
const { PolizaMovimiento, Poliza, AccountPlan } = require('../../../shared/models/postgres');
const BankMovement = require('../banks/BankMovement.model');
const { obtenerDesglosesCobroAlmacen, obtenerSaldosFavor } = require('../erp/erp-sync.service');
const { SERIES_CON_AUTH } = require('../erp/erp-auth.utils');

// `BankMovement.banco` → código de cuenta bancaria real del catálogo — mismo
// mapeo que `BANCO_A_CODIGO_CUENTA` en poliza.service.js (duplicado a
// propósito, igual que ETIQUETA_COBRO_SUCURSAL — archivos pequeños,
// independientes). BBVA usa la cuenta general de depósitos de clientes.
const BANCO_A_CODIGO_CUENTA = {
  'Banamex':    '1102012001',
  'BBVA':       '1102011001',
  'Santander':  '1102013001',
  'Banorte':    '1102014001',
  'Scotiabank': '1102015001',
  'Azteca':     '1102016001',
};

// Serie del CFDI que marca el TIPO de documento relacionado (Bonificación/
// Devolución/Cargo a Cliente), no una referencia a documento — misma
// convención que usa el motor de balanza del Visor (ver
// visor/controllers/report.controller.js TIPO_MARCADORES).
const TIPO_MARCADORES = ['BON', 'BCT', 'DEV', 'CAC'];

// Clasificación Caja/Bancos por clave SAT de forma de pago — mismo criterio
// que ya usan las reglas de CfdiMappingRule (01→Caja, todo lo demás→Bancos).
const CLAVE_SAT_EFECTIVO = '01';

// "Saldo a favor" no es dinero cobrado en la otra sucursal — es aplicación de
// un saldo existente del cliente. Se detecta por texto porque la API de cajas
// no trae un campo dedicado; mismo criterio que `tipoSaldoEspecial` en
// collection-request-erp-links.js. Nota (2026-08-03, factura I0-260700127):
// la API a veces liga montos de "saldo a favor" a una cuenta ajena/confusa —
// no afecta el monto que aquí se registra (viene de `cobro.monto`, ya
// prorrateado), solo hay que tener cuidado si se usa esto para otra cosa.
function _esSaldoAFavor(fp) {
  return /saldo\s*a\s*favor/i.test(fp?.nombre ?? '');
}

// Tasa usada para partir el monto de "saldo a favor" en subtotal/IVA — todos
// los casos reales vistos hasta ahora son tasa 16%.
const TASA_IVA_SALDO_FAVOR = 0.16;

// Mismo texto que ETIQUETA_COBRO_SUCURSAL en poliza.service.js (columna C).
const ETIQUETA_COBRO_SUCURSAL = 'Cobro de otra sucursal';
// Columna C para líneas de saldo a favor — literal "SF", sin el prefijo
// "Cobro de otra sucursal -" (confirmado con el usuario 2026-08-03).
const ETIQUETA_SALDO_FAVOR = 'SF';
// Marca un par generación+uso de saldo a favor que NO debe mostrarse en el
// export (pero SÍ debe seguir guardado en BD) — regla confirmada con el
// usuario 2026-08-04: se oculta cuando el saldo se genera y se usa por
// completo (sin sobrante) el MISMO día, en el MISMO almacén. La decisión
// (`devsOcultos`, ver `_prefetchSaldosFavorGenerados` en
// cfdi-poliza-generator.service.js) ya viene calculada desde fuera; aquí solo
// se aplica al lado de "uso" de cada par.
const ETIQUETA_SALDO_FAVOR_OCULTO = 'SF-OCULTO';

/**
 * Extrae TODOS los documentos relacionados (serie/folio interno) de un CFDI —
 * las referencias reales a documentos de venta, sin contar el marcador de
 * tipo. Devuelve un array (vacío si el CFDI no trae ninguno relevante).
 *
 * Antes devolvía solo el PRIMERO (`.find`) — asumía que un CFDI referencia
 * como máximo una venta (cierto para una NC normal), pero una Factura Global
 * puede agrupar VARIOS tickets (uno por cada `documentoRelacionado`) — con
 * `.find()` solo se procesaba el primero y los demás se descartaban en
 * silencio (confirmado con el usuario 2026-08-04: Global I0-260700155 agrupa
 * 8 tickets — 184/187/188/194/195/211/217/218 — y solo 184 llegaba a
 * construirMovimientosPuente).
 *
 * NOTA: este campo está verificado para Notas de Crédito (E); para facturas
 * de Ingreso (I) aún no se ha confirmado con un caso real — validar con la
 * tarea de verificación antes de confiar en esto en producción.
 */
function _extraerDocumentosRelacionados(cfdi) {
  return (cfdi.documentosRelacionados ?? [])
    .filter(d => !TIPO_MARCADORES.includes((d.Serie ?? '').toUpperCase()) && d.Folio)
    .map(d => ({ serie: d.Serie ?? null, folio: d.Folio ?? null }));
}

// Padding del rango de folioVenta a escanear alrededor de los ya conocidos
// del día — heurístico: el numerador de cajas es aproximadamente secuencial
// por almacén, así que los folios de un mismo día quedan cerca entre sí.
const PADDING_ESCANEO_POR_FACTURAR = 15;

/**
 * Detecta tickets de cajas con cobro real (mismo día, mismas series que
 * SERIES_CON_AUTH) que NO tienen ninguna factura ligada (`serieFactura`/
 * `folioFactura` ausentes) — ej. I0-260700183 (CLIENTE MOSTRADOR, $184.89,
 * cobrado el 10/07 pero nunca facturado, confirmado con el usuario
 * 2026-08-04). El motor normal nunca los ve porque solo consulta folios que
 * ya conoce vía `documentosRelacionados` de algún CFDI — un ticket sin
 * factura no está referenciado por nada.
 *
 * No hay endpoint en cajas para "listar" tickets por día/almacén — la única
 * vía es adivinar el folioVenta y consultar uno por uno (confirmado con el
 * usuario 2026-08-04). Se infiere el rango a partir de los folios YA
 * conocidos de este mismo día (`foliosConocidos`) ± `PADDING_ESCANEO_POR_FACTURAR`
 * — es un heurístico, no garantiza encontrar el 100% (ej. si todos los
 * tickets del día cayeran fuera del padding).
 *
 * Devuelve una lista informativa (NO movimientos contables) para que se
 * muestre aparte como "pendientes por facturar" — nunca se mezcla con
 * `candidatas`.
 */
async function _detectarPendientesPorFacturar({ foliosDelDiaNumericos, serieDelDia, foliosConocidos, fechaDesde, fechaHasta }) {
  if (!fechaDesde || !fechaHasta || !serieDelDia || !foliosDelDiaNumericos.length) return [];

  // Descartar outliers antes de sacar min/max: un solo folioVenta de otra
  // época (ej. un "documento relacionado" viejo de junio colándose entre los
  // de julio) dispara un rango de miles de folios y satura el ERP (429 Too
  // Many Requests, confirmado con el usuario 2026-08-04). Se usa la mediana
  // como ancla en vez del min/max crudo.
  const sorted = [...foliosDelDiaNumericos].sort((a, b) => a - b);
  const mediana = sorted[Math.floor(sorted.length / 2)];
  const cercanosALaMediana = foliosDelDiaNumericos.filter(f => Math.abs(f - mediana) <= 300);
  if (!cercanosALaMediana.length) return [];

  const min = Math.min(...cercanosALaMediana) - PADDING_ESCANEO_POR_FACTURAR;
  const max = Math.max(...cercanosALaMediana) + PADDING_ESCANEO_POR_FACTURAR;
  // Límite de seguridad adicional — si aun así el rango es enorme, prefiere
  // no escanear nada a arriesgar otro 429.
  if (max - min > 500) return [];
  const candidatosFolios = [];
  for (let f = min; f <= max; f++) {
    const folioStr = String(f);
    if (!foliosConocidos.has(`${serieDelDia}|${folioStr}`)) candidatosFolios.push(folioStr);
  }
  if (!candidatosFolios.length) return [];

  const LOTE = 150;
  const cuentasEscaneadas = [];
  for (let i = 0; i < candidatosFolios.length; i += LOTE) {
    const lote = candidatosFolios.slice(i, i + LOTE);
    const resultado = await obtenerDesglosesCobroAlmacen({
      series: lote.map(() => serieDelDia),
      folios: lote,
    });
    cuentasEscaneadas.push(...resultado);
  }

  const pendientes = [];
  for (const cuenta of cuentasEscaneadas) {
    if (cuenta.serieFactura && cuenta.folioFactura) continue; // ya tiene factura — no es un pendiente
    for (const cobro of (cuenta.cobros ?? [])) {
      if (!SERIES_CON_AUTH.includes((cobro.serieOrigen ?? '').toUpperCase())) continue;
      const fechaCobro = cobro.fecha ? new Date(cobro.fecha) : null;
      if (!fechaCobro || fechaCobro < fechaDesde || fechaCobro > fechaHasta) continue;
      pendientes.push({
        serie:       cuenta.serieVenta ?? serieDelDia,
        folio:       cuenta.folioVenta,
        monto:       Math.abs(Number(cobro.monto) || 0),
        formasPago:  (cobro.formasPago ?? []).map(fp => ({ nombre: fp.nombre ?? fp.claveSat ?? null, monto: Number(fp.monto) || 0 })),
        fecha:       cobro.fecha,
        folioOrigen: cobro.folioOrigen ?? null,
        // Cajas no trae nombre de cliente para cuentas sin factura (por algo
        // no tienen documentoRelacionado que permita resolverlo) — mismo
        // fallback que usa `nombreCliente` arriba para cuentas normales.
        nombreCliente: 'CLIENTE NO IDENTIFICADO',
      });
    }
  }
  return pendientes;
}

/**
 * Devuelve, para un centroCostoId dado, las líneas de movimiento puente que
 * deben incluirse en SU póliza de Ingreso (ya sea como vendedor o como
 * cobrador de cobros ajenos).
 *
 * @param {Array} cfdis - CFDIs tipoCfdi='I' del periodo COMPLETO (todas las
 *   sucursales, sin filtrar por centro — necesario para detectar cobros
 *   cruzados en cualquier dirección).
 * @param {string|number} centroCostoId - centro para el que se genera la póliza actual.
 * @param {object} ccBySerieMap - serieFacturacion → {id, clave, sucursal}. Se
 *   usa para resolver TANTO al vendedor (cuenta.serieFactura) COMO al
 *   cobrador (cobro.claveCentro) — verificado con datos reales: `claveCentro`
 *   que regresa la API de cajas es la serie de facturación de la sucursal
 *   (ej. "A0" para CEDIS), NO el campo `clave` del catálogo CentroCosto (ej.
 *   "300"). Antes esto se resolvía con un mapa por `clave` separado
 *   (`ccByClaveMap`/`resolveByClaveMap`) que nunca coincidía con nada —
 *   corregido 2026-07-31: el lado cobrador nunca se generaba por este bug.
 * @param {number} cuentaCajaId - AccountPlan.id de 1101010003 (Caja por identificar).
 * @param {number} cuentaBancosId - AccountPlan.id de 1102011005 (Bancos por identificar).
 * @param {number} cuentaPuenteId - AccountPlan.id de 2103040001 (Cobros De
 *   Sucursales Por Identificar) — solo se usa para el asiento adicional de
 *   facturas PPD (ver encabezado del archivo); PUE no la toca.
 * @param {number} [cuentaSaldoFavorId] - AccountPlan.id de 2103090001
 *   (Anticipos Otros) — subtotal de las porciones "saldo a favor".
 * @param {number} [cuentaIvaSaldoFavorId] - AccountPlan.id de 2104010002
 *   (IVA Trasladado - Anticipos) — IVA de las porciones "saldo a favor".
 * @param {string} rfc
 * @returns {Promise<{movimientos: Array, facturasVendedorCubiertas: Set<string>, facturasPPDCubiertas: Map<string,{monto:number, reglaNombre:string}>}>}
 *   `movimientos`: líneas listas para concatenar a movimientosResult/todosLosMovimientos.
 *   `facturasVendedorCubiertas`: UUIDs (mayúsculas) de las facturas PUE de
 *   ESTE centroCostoId (como vendedora) cuyo Cargo normal (el que arma
 *   cfdiToMovimientos según formaPago del propio CFDI) debe OMITIRSE, porque
 *   ya lo cubre la línea Cargo a Caja/Bancos por identificar de este flujo —
 *   sin esto, la póliza queda con doble Cargo contra un solo Abono
 *   (ver cfdi-poliza-generator.service.js, donde se usa para filtrar).
 *   `facturasPPDCubiertas`: UUID (mayúsculas) → { monto, reglaNombre } para
 *   facturas PPD de ESTE centroCostoId (como vendedora) cobradas en otra
 *   sucursal — cfdi-poliza-generator.service.js usa esto para agregar el
 *   Abono a Clientes (contrapartida del Cargo a la cuenta puente que ya viene
 *   en `movimientos`) y la etiqueta de columna C. El Cargo a Clientes normal
 *   de la venta NO se toca.
 */
async function construirMovimientosPuente({
  cfdis,
  centroCostoId,
  ccBySerieMap,
  cuentaCajaId,
  cuentaBancosId,
  cuentaPuenteId,
  cuentaSaldoFavorId,
  cuentaIvaSaldoFavorId,
  rfc,
  // Set de claves `${serieOrigen}|${folioOrigen}` (la Devolución que generó
  // el saldo) que deben ocultarse del export — ver `ETIQUETA_SALDO_FAVOR_OCULTO`.
  devsOcultosSF,
  // Rango [fechaDesde, fechaHasta] (Date, límites reales del día en CDMX) —
  // cuando se generan pólizas POR DÍA, `cfdis` puede traer documentos
  // relacionados de CUALQUIER día del periodo (ver `_fetchCfdisParaPuenteAmplio`
  // en cfdi-poliza-generator.service.js), así que el día se decide aquí por la
  // fecha REAL del cobro (`cobro.fecha`, que viene de cajas) — no por la fecha
  // del CFDI que lo referencia. Sin esto, un cobro hecho el día X pero solo
  // referenciado por una Factura Global emitida el día X+1 nunca se registraba
  // en la póliza de ningún día (confirmado con el usuario 2026-08-04).
  // Si vienen null (generación sin acotar por día), no se filtra nada.
  fechaDesde,
  fechaHasta,
}) {
  const vacio = { movimientos: [], facturasVendedorCubiertas: new Set(), facturasPPDCubiertas: new Map(), pendientesPorFacturar: [] };
  if (!centroCostoId || !cuentaCajaId || !cuentaBancosId) return vacio;

  // 1. Documentos relacionados de todos los CFDIs del periodo → batch de consulta.
  // Un mismo CFDI (ej. una Factura Global) puede traer VARIOS documentos
  // relacionados — flatMap, no map, para no quedarnos solo con el primero.
  const docsPorCfdi = cfdis
    .flatMap(cfdi => _extraerDocumentosRelacionados(cfdi).map(doc => ({ cfdi, doc })));

  if (!docsPorCfdi.length) return vacio;

  // Mapa serie|folio del documento relacionado → CFDI original, para poder
  // recuperar el nombre del cliente (receptor) de cada cuenta que regrese la
  // API — la API solo nos devuelve claves/series, no el nombre.
  const cfdiPorDoc = new Map(docsPorCfdi.map(x => [`${x.doc.serie}|${x.doc.folio}`, x.cfdi]));

  // Deduplicar por serie|folio (varios CFDIs — ej. una factura y su NC —
  // pueden referenciar el mismo documento) y consultar en lotes: desde que
  // `cfdis` puede traer el periodo COMPLETO (ver `_fetchCfdisParaPuenteAmplio`
  // en cfdi-poliza-generator.service.js), una sola llamada con todos los
  // series/folios del mes revienta el límite de largo de URL del ERP (414
  // Request-URI Too Large, confirmado con el usuario 2026-08-04).
  const docsUnicos = [...cfdiPorDoc.keys()].map(k => {
    const [serie, folio] = k.split('|');
    return { serie, folio };
  });
  const LOTE = 150;
  const cuentas = [];
  for (let i = 0; i < docsUnicos.length; i += LOTE) {
    const lote = docsUnicos.slice(i, i + LOTE);
    const resultado = await obtenerDesglosesCobroAlmacen({
      series: lote.map(d => d.serie),
      folios: lote.map(d => d.folio),
    });
    cuentas.push(...resultado);
  }
  if (!cuentas.length) return vacio;

  // 1b. Saldos a favor USADOS por estas mismas ventas — mismo lote de
  // series/folios que /desgloses-cobro/almacen. Reemplaza la detección
  // heurística anterior (buscar "SALDO A FAVOR" en `formasPago[].nombre`,
  // con el monto tal cual lo trae la API) por el dato explícito de
  // saldos-favor: monto exacto (`montoUsado`) y origen real (qué Devolución/
  // venta generó el saldo) — confirmado con el usuario 2026-08-04.
  const usadosPorCuenta = new Map(); // `${serie}|${folioVenta}` → [{serieOrigen, folioOrigen, montoUsado, fecha}]
  for (let i = 0; i < docsUnicos.length; i += LOTE) {
    const lote = docsUnicos.slice(i, i + LOTE);
    const resultado = await obtenerSaldosFavor({
      series: lote.map(d => d.serie),
      folios: lote.map(d => d.folio),
    });
    for (const cuenta of resultado) {
      const key = `${cuenta.serieVenta}|${cuenta.folioVenta}`;
      if (cuenta.saldosFavorUsados?.length) {
        usadosPorCuenta.set(key, [...(usadosPorCuenta.get(key) ?? []), ...cuenta.saldosFavorUsados]);
      }
    }
  }

  // 1c. Ocultar también saldos a favor generados por una Devolución SIN CFDI
  // (ej. DEV-055199, venta I0-260700183: una devolución de caja que nunca se
  // facturó) — `devsOcultosSF` (parámetro, calculado en
  // `_prefetchSaldosFavorGenerados` a partir de CFDIs tipo E) nunca detecta
  // estos casos porque no hay ningún CFDI que los marque. Cada entrada de
  // `saldosFavorUsados` ya trae `serieVenta`/`folioVenta` DEL ORIGEN del
  // saldo (no de la cuenta consultada) — se usa esa referencia para consultar
  // directamente la cuenta de origen y aplicar el mismo criterio (uso único,
  // sin sobrante, mismo día, mismo almacén) — confirmado con el usuario
  // 2026-08-04.
  const devsOcultosHuerfanos = new Set();
  const origenesUnicos = [...new Map(
    [...usadosPorCuenta.values()].flat()
      .filter(u => u.serieVenta && u.folioVenta)
      .map(u => [`${u.serieVenta}|${u.folioVenta}`, { serie: u.serieVenta, folio: u.folioVenta }]),
  ).values()];
  for (let i = 0; i < origenesUnicos.length; i += LOTE) {
    const lote = origenesUnicos.slice(i, i + LOTE);
    const resultado = await obtenerSaldosFavor({
      series: lote.map(d => d.serie),
      folios: lote.map(d => d.folio),
    });
    for (const cuentaOrigen of resultado) {
      for (const gen of (cuentaOrigen.saldosFavorGenerados ?? [])) {
        const usos = gen.usos ?? [];
        const usoUnico = usos.length === 1 ? usos[0] : null;
        if (!usoUnico) continue;
        const diaGen = gen.fecha ? new Date(gen.fecha).toISOString().slice(0, 10) : null;
        const diaUso = usoUnico.fecha ? new Date(usoUnico.fecha).toISOString().slice(0, 10) : null;
        const usoCompleto = Math.abs(Number(usoUnico.montoSobrante) || 0) < 0.01;
        const mismoAlmacen = usoUnico.serieVenta === cuentaOrigen.serieVenta;
        if (usoCompleto && diaGen && diaGen === diaUso && mismoAlmacen) {
          devsOcultosHuerfanos.add(`${gen.serieOrigen}|${gen.folioOrigen}`);
        }
      }
    }
  }
  // Unión con lo ya detectado vía CFDI — cualquiera de los dos criterios oculta el par.
  const devsOcultosCombinado = new Set([...(devsOcultosSF ?? []), ...devsOcultosHuerfanos]);

  // 1c. Depósito bancario real de estas mismas ventas — para cobros cruzados
  // de sucursal, bank_movements NO se vincula por `erpLinks.folioFiscal`
  // (viene null) sino por `erpLinks.serie`+`erpLinks.folioExterno` = el
  // mismo par serie/folioVenta que ya usamos contra cajas — confirmado con
  // el usuario 2026-08-04 con datos reales (RENIT/GRUPO CUBOOAX, banco
  // Banamex). Sin esto, estas líneas se quedan en la cuenta genérica
  // "Bancos por identificar" aunque sí haya un depósito real identificado.
  const bancoPorVenta = new Map(); // `${serie}|${folioVenta}` → { banco, referencia }
  if (docsUnicos.length) {
    const orCondiciones = docsUnicos.map(d => ({ 'erpLinks.serie': d.serie, 'erpLinks.folioExterno': d.folio }));
    for (let i = 0; i < orCondiciones.length; i += LOTE) {
      const lote = orCondiciones.slice(i, i + LOTE);
      const movsBanco = await BankMovement.find({ $or: lote }, {
        banco: 1, folio: 1, erpLinks: 1,
      }).lean();
      for (const mb of movsBanco) {
        // `folio` (el auto-incremental propio de Numo, ej. "034287") — NO
        // `numeroAutorizacion`/`referenciaNumerica` (esos son del banco, no
        // coinciden con la referencia que se espera en columna C — confirmado
        // con el usuario 2026-08-04 contra el Excel de referencia real).
        const referencia = mb.folio || null;
        for (const link of (mb.erpLinks ?? [])) {
          if (!link.serie || !link.folioExterno) continue;
          const key = `${link.serie}|${link.folioExterno}`;
          if (!docsUnicos.some(d => `${d.serie}|${d.folio}` === key)) continue;
          bancoPorVenta.set(key, { banco: mb.banco, referencia });
        }
      }
    }
  }
  // Cuentas reales de banco (ver BANCO_A_CODIGO_CUENTA) — un solo query.
  const codigosBancoReal = Object.values(BANCO_A_CODIGO_CUENTA);
  const cuentasBancoRealRows = await AccountPlan.findAll({
    where:      { codigo: { [Op.in]: codigosBancoReal } },
    attributes: ['id', 'codigo'],
    raw:        true,
  });
  const idCuentaBancoPorCodigo = new Map(cuentasBancoRealRows.map(r => [r.codigo, r.id]));

  // 2. Armar líneas candidatas (antes de filtrar por idempotencia).
  const candidatas = [];
  const facturasVendedorCubiertas = new Set();
  // Acumulador interno (Set de nombres de forma de pago, para la etiqueta
  // combinada) — se convierte a `facturasPPDCubiertas` (monto + reglaNombre
  // string) justo antes de retornar.
  const facturasPPDAcumulado = new Map();
  // folioVenta (numérico) de cobros REALES del día — usado abajo para acotar
  // el rango de folios a escanear en busca de tickets "por facturar" (ver
  // `_detectarPendientesPorFacturar`). Solo se llenan con folioVenta ya
  // conocidos por `docsUnicos` — el propio ticket huérfano (sin factura,
  // como I0-260700183) nunca llega a este loop porque nada lo referencia.
  const foliosDelDiaNumericos = [];
  let serieDelDia = null;

  for (const cuenta of cuentas) {
    const centroVendedor = cuenta.serieFactura ? ccBySerieMap[cuenta.serieFactura] : null;
    const cfdiOriginal = cfdiPorDoc.get(`${cuenta.serieVenta}|${cuenta.folioVenta}`) ?? null;
    const nombreCliente = cfdiOriginal?.receptor?.nombre ?? 'CLIENTE NO IDENTIFICADO';
    const esPPD = cfdiOriginal?.metodoPago === 'PPD';

    // Serie-folio del DOCUMENTO RELACIONADO (cuenta.serieVenta/folioVenta) —
    // NO el de "la factura" (cuenta.serieFactura/folioFactura). Confirmado
    // con el usuario 2026-08-03 con un caso real: al consultar la API con la
    // referencia "I0-260700127", la cuenta que regresa es la de OTRA factura
    // (FC3F33E9, folio 260700067) — es decir, `folioFactura` es una
    // resolución de la API que puede apuntar a una cuenta ajena/confusa. La
    // referencia que SÍ identifica la cuenta real consultada (y que se puede
    // buscar tal cual en el sistema de cajas para auditar) es
    // serieVenta-folioVenta ("Serie y folio interno" en su UI).
    const serieFolioFactura = `${cuenta.serieVenta ?? '?'}-${cuenta.folioVenta ?? '?'}`;

    for (const cobro of (cuenta.cobros ?? [])) {
      // Solo cobros reales — mismas series que SERIES_CON_AUTH (ABO/CBT/CPF/CFC,
      // ver erp-auth.utils.js y Movimientos CxC) — 'APA' (aplicación de
      // anticipo/saldo a favor) puede pertenecer a OTRA factura que consumió
      // el saldo generado aquí (verificado con datos reales 2026-08-03,
      // I0-260700142: su APA trae exactamente el desglose de pago de
      // I0-260700143, no el propio) y 'RET' es cierre/retiro, no un cobro.
      // Puede haber depósitos/pagos de más (sobrepagos). Antes solo se aceptaba
      // 'ABO' — descartaba cobros reales tipo 'CPF' (confirmado con el usuario
      // 2026-08-03, I0-260700183: su cobro CPF-260701517 nunca entraba a la
      // póliza).
      if (!SERIES_CON_AUTH.includes((cobro.serieOrigen ?? '').toUpperCase())) continue;

      // Filtro por día real del cobro (ver comentario en la firma de la función).
      if (fechaDesde && fechaHasta) {
        const fechaCobro = cobro.fecha ? new Date(cobro.fecha) : null;
        if (!fechaCobro || fechaCobro < fechaDesde || fechaCobro > fechaHasta) continue;
      }

      // Folio de venta de un cobro real de HOY — ancla para inferir el rango
      // de folios del día al escanear "por facturar" más abajo.
      const folioVentaNum = parseInt(cuenta.folioVenta, 10);
      if (!isNaN(folioVentaNum)) {
        foliosDelDiaNumericos.push(folioVentaNum);
        serieDelDia = serieDelDia ?? cuenta.serieVenta ?? null;
      }

      const centroCobrador = cobro.claveCentro ? ccBySerieMap[cobro.claveCentro] : null;

      // La API regresa TODOS los cobros de la venta, no solo los cruzados —
      // verificado con datos reales (2026-07-31): ~70% de los "cobros" de
      // CEDIS resultaron ser de la MISMA sucursal que vendió (el cliente
      // compró y pagó en el mismo lugar). Eso NO es un cobro de otra
      // sucursal — ya lo contabiliza el flujo normal de CFDI/pago — así que
      // se omite por completo (nada de líneas puente) para no duplicarlo.
      if (centroVendedor && centroCobrador && String(centroVendedor.id) === String(centroCobrador.id)) {
        continue;
      }

      const montoCobro = Math.abs(Number(cobro.monto) || 0);
      // Cada cobro se desglosa en su propio renglón (nunca se consolida con
      // otros — ver `_extraerCobrosSucursal` en poliza.service.js, que los
      // saca del pipeline de consolidación antes de armar el export). Columna
      // H (concepto) del export CONTPAQ: "Nombre del cliente / Serie-Folio",
      // mismo patrón que devoluciones/descuentos/bonificaciones.
      const conceptoBase = [nombreCliente, serieFolioFactura].filter(Boolean).join(' / ');

      // Saldo a favor USADO por este cobro específico — dato explícito de
      // /desgloses-cobro/saldos-favor (ver `usadosPorCuenta` arriba), no
      // heurístico. Se correlaciona por fecha exacta: verificado con datos
      // reales que `saldosFavorUsados[].fecha` coincide exacto con
      // `cobro.fecha` de /desgloses-cobro/almacen para el mismo evento
      // (confirmado con el usuario 2026-08-04).
      const usadosDeEsteCobro = (usadosPorCuenta.get(`${cuenta.serieVenta}|${cuenta.folioVenta}`) ?? [])
        .filter(u => u.fecha && cobro.fecha && new Date(u.fecha).getTime() === new Date(cobro.fecha).getTime());
      const montoSFReal = usadosDeEsteCobro.length
        ? Math.round(usadosDeEsteCobro.reduce((s, u) => s + (Math.abs(Number(u.montoUsado)) || 0), 0) * 100) / 100
        : null;
      // Columna H para la porción de SF: el documento relacionado de la
      // VENTA que USA el saldo (mismo `serieFolioFactura` que las líneas
      // normales, ej. "I0-260700210") — NO el origen del saldo ("DEV-055219",
      // la Devolución que lo generó) — confirmado con el usuario 2026-08-04:
      // el origen ya se puede rastrear en la hoja de "SF Generado", esta
      // línea es sobre la venta que lo está usando ahora.
      const conceptoSF = conceptoBase;
      // Oculto si TODO lo usado por este cobro viene de una generación marcada
      // como oculta (mismo día/mismo almacén/uso único) — ver
      // `ETIQUETA_SALDO_FAVOR_OCULTO`.
      const usoOculto = usadosDeEsteCobro.length > 0 && usadosDeEsteCobro.every(
        u => devsOcultosCombinado.has(`${u.serieOrigen}|${u.folioOrigen}`)
      );

      // ── Desglose por forma de pago (Caja vs Bancos), compartido por los dos
      // lados — se saca de `cobro.formasPago` (mismo endpoint de cajas).
      // OJO: verificado con datos reales (2026-07-31) que `formasPago[].monto`
      // puede traer el TOTAL de la operación de pago completa (sin prorratear)
      // cuando un mismo pago cubre varias facturas/cuentas — dos "cuenta"
      // distintas llegaron con el MISMO total de formasPago aunque su
      // `cobro.monto` (la porción real de cada una) era distinto. Por eso el
      // monto que se contabiliza SIEMPRE sale de `cobro.monto` (prorrateado
      // entre formasPago si hay más de una), nunca de `fp.monto` directo.
      // Sin formasPago no hay forma de saber Caja vs Bancos — se cae a
      // Bancos por defecto para que la póliza no quede sin su línea, y se
      // marca en el concepto para revisión manual.
      const formasPago = (cobro.formasPago ?? []).length
        ? cobro.formasPago
        : [{ claveSat: null, nombre: 'SIN FORMA DE PAGO — REVISAR', monto: montoCobro }];
      const totalFormasPago = formasPago.reduce((s, fp) => s + (Number(fp.monto) || 0), 0);
      let acumulado = 0;
      // `lineas`: cada forma de pago se convierte en 1 línea contable, EXCEPTO
      // "saldo a favor", que se parte en 2 (subtotal a cuentaSaldoFavorId, IVA
      // a cuentaIvaSaldoFavorId) — ver encabezado del archivo. `esSF: true`
      // marca ambas para el manejo especial (columna C = "SF", nunca se
      // acumula en la línea combinada de la puente).
      const lineas = [];
      formasPago.forEach((fp, idx) => {
        const esUltimo = idx === formasPago.length - 1;
        const share = totalFormasPago > 0 ? (Number(fp.monto) || 0) / totalFormasPago : 1 / formasPago.length;
        // El último absorbe el residuo de redondeo para que la suma cuadre exacto con montoCobro.
        let montoAsignado = esUltimo
          ? Math.round((montoCobro - acumulado) * 100) / 100
          : Math.round(montoCobro * share * 100) / 100;

        // Si saldos-favor confirma el monto REAL usado, se usa ese en vez del
        // heurístico de arriba (ver `montoSFReal`) — mismo criterio de
        // "el último absorbe el residuo" sigue aplicando sobre el monto ya
        // corregido, para que la suma total siga cuadrando con `montoCobro`.
        if (_esSaldoAFavor(fp) && montoSFReal != null) montoAsignado = montoSFReal;
        acumulado += montoAsignado;

        if (_esSaldoAFavor(fp)) {
          if (!cuentaSaldoFavorId || !cuentaIvaSaldoFavorId || montoAsignado <= 0) return;
          const subtotal = Math.round((montoAsignado / (1 + TASA_IVA_SALDO_FAVOR)) * 100) / 100;
          const iva = Math.round((montoAsignado - subtotal) * 100) / 100;
          const reglaSF = usoOculto ? ETIQUETA_SALDO_FAVOR_OCULTO : ETIQUETA_SALDO_FAVOR;
          lineas.push({ cuentaId: cuentaSaldoFavorId, montoAsignado: subtotal, reglaNombre: reglaSF, esSF: true, concepto: conceptoSF });
          lineas.push({ cuentaId: cuentaIvaSaldoFavorId, montoAsignado: iva, reglaNombre: reglaSF, esSF: true, concepto: conceptoSF });
          return;
        }
        const esEfectivo = (fp.claveSat ?? '').trim() === CLAVE_SAT_EFECTIVO;
        // Depósito bancario real identificado (ver `bancoPorVenta` arriba) —
        // solo aplica a Transferencia/Tarjeta (nunca Efectivo, que no pasa
        // por banco). Cuenta real + número de depósito en vez de la genérica
        // "Bancos por identificar" + etiqueta "TRANSFERENCIA"/"TARJETA".
        const bancoReal = !esEfectivo
          ? bancoPorVenta.get(`${cuenta.serieVenta}|${cuenta.folioVenta}`)
          : null;
        const idCuentaBancoReal = bancoReal ? idCuentaBancoPorCodigo.get(BANCO_A_CODIGO_CUENTA[bancoReal.banco]) : null;
        lineas.push({
          cuentaId:    esEfectivo ? cuentaCajaId : (idCuentaBancoReal ?? cuentaBancosId),
          montoAsignado,
          reglaNombre: (idCuentaBancoReal && bancoReal?.referencia) ? bancoReal.referencia : (fp.nombre || fp.claveSat || null),
          esSF: false,
          concepto: conceptoBase,
        });
      });
      if (!lineas.length) continue;

      const lineasNormales = lineas.filter(l => !l.esSF);
      const lineasSF       = lineas.filter(l => l.esSF);
      const formaPagoLabelCombinado = lineasNormales.map(l => l.reglaNombre).filter(Boolean).join('/') || null;

      if (esPPD) {
        // Cierre mismo día: la CxC de una venta PPD solo se cierra en ESTA
        // póliza de Ingreso cuando la factura ORIGINAL es del MISMO día que
        // se está generando — si es de un día distinto (ej. factura del
        // 04/07 cobrada el 10/07 en otra sucursal), ese cierre le
        // corresponde a Cobranza, no a Ingreso (confirmado con el usuario
        // 2026-08-04, HERROZINC I0-260700082: quedaba inyectado aquí sin
        // deber estarlo). Si no se puede determinar la fecha de la factura,
        // se excluye por seguridad (mismo criterio).
        if (fechaDesde && fechaHasta) {
          const fechaFactura = cfdiOriginal?.fecha ? new Date(cfdiOriginal.fecha) : null;
          if (!fechaFactura || fechaFactura < fechaDesde || fechaFactura > fechaHasta) continue;
        }
        // ── PPD lado VENDEDOR: NO se toca el Cargo a Clientes normal de la
        // venta (lo sigue armando cfdiToMovimientos) — aquí se agrega el
        // Cargo a la cuenta puente (solo con las formas de pago normales) y,
        // aparte, una línea de Cargo por cada mitad de "saldo a favor" (no se
        // mezcla con la puente). El Abono a Clientes que compensa TODO esto
        // (puente + SF) lo agrega cfdi-poliza-generator.service.js usando
        // `facturasPPDCubiertas`.
        if (centroVendedor && String(centroVendedor.id) === String(centroCostoId) && cuentaPuenteId) {
          const montoNeto = lineasNormales.reduce((s, l) => s + l.montoAsignado, 0);
          let montoTotalCubierto = 0;
          const nombresFormaPagoUsados = new Set();

          if (cfdiOriginal?.uuid && montoNeto > 0) {
            candidatas.push({
              cuentaId:      cuentaPuenteId,
              cuentaFaltante: false,
              concepto:      conceptoBase,
              debe:          montoNeto,
              haber:         0,
              serie:         serieFolioFactura,
              folio:         cobro.folioOrigen ?? null,
              centroCosto:   centroVendedor.clave,
              centroCostoId: centroVendedor.id,
              tipoOrigen:    'Cobro Sucursal',
              reglaNombre:   formaPagoLabelCombinado,
              cfdiUuid:      cfdiOriginal.uuid,
              // Discrimina PPD/Crédito vs PUE/Contado para _inyectarCobrosSucursal.
              metodoPago:    'PPD',
            });
            montoTotalCubierto += montoNeto;
            lineasNormales.forEach(l => { if (l.reglaNombre) nombresFormaPagoUsados.add(l.reglaNombre); });
          }

          if (cfdiOriginal?.uuid) {
            lineasSF.forEach(l => {
              if (l.montoAsignado <= 0) return;
              candidatas.push({
                cuentaId:      l.cuentaId,
                cuentaFaltante: false,
                concepto:      l.concepto,
                debe:          l.montoAsignado,
                haber:         0,
                serie:         serieFolioFactura,
                folio:         cobro.folioOrigen ?? null,
                centroCosto:   centroVendedor.clave,
                centroCostoId: centroVendedor.id,
                tipoOrigen:    'Cobro Sucursal',
                reglaNombre:   l.reglaNombre,
                cfdiUuid:      cfdiOriginal.uuid,
                metodoPago:    'PPD',
              });
              montoTotalCubierto += l.montoAsignado;
              nombresFormaPagoUsados.add(l.reglaNombre);
            });
          }

          if (cfdiOriginal?.uuid && montoTotalCubierto > 0) {
            const uuidUpper = cfdiOriginal.uuid.toUpperCase();
            const prev = facturasPPDAcumulado.get(uuidUpper) ?? { monto: 0, nombresFormaPago: new Set() };
            prev.monto += montoTotalCubierto;
            nombresFormaPagoUsados.forEach(n => prev.nombresFormaPago.add(n));
            facturasPPDAcumulado.set(uuidUpper, prev);
          }
        }

        // ── PPD lado COBRADOR: igual que PUE (Cargo Caja/Bancos real o SF),
        // pero el Abono de las formas de pago NORMALES va a la cuenta puente
        // (la contrapartida vive en la póliza vendedora) — el Abono de "saldo
        // a favor" va a esa MISMA cuenta SF (no a la puente: no es un
        // movimiento de banco entre sucursales, es solo aplicar el saldo).
        if (centroCobrador && String(centroCobrador.id) === String(centroCostoId) && cuentaPuenteId) {
          lineas.forEach(l => {
            candidatas.push({
              cuentaId:      l.cuentaId,
              cuentaFaltante: false,
              concepto:      l.concepto,
              debe:          l.montoAsignado,
              haber:         0,
              serie:         serieFolioFactura,
              folio:         cobro.folioOrigen ?? null,
              centroCosto:   centroCobrador.clave,
              centroCostoId: centroCobrador.id,
              tipoOrigen:    'Cobro Sucursal',
              reglaNombre:   l.reglaNombre,
              cfdiUuid:      cfdiOriginal?.uuid ?? null,
              metodoPago:    'PPD',
            });
            candidatas.push({
              cuentaId:      l.esSF ? l.cuentaId : cuentaPuenteId,
              cuentaFaltante: false,
              concepto:      l.concepto,
              debe:          0,
              haber:         l.montoAsignado,
              serie:         serieFolioFactura,
              folio:         cobro.folioOrigen ?? null,
              centroCosto:   centroCobrador.clave,
              centroCostoId: centroCobrador.id,
              tipoOrigen:    'Cobro Sucursal',
              reglaNombre:   l.reglaNombre,
              cfdiUuid:      cfdiOriginal?.uuid ?? null,
              metodoPago:    'PPD',
            });
          });
        }
        continue;
      }

      // ── PUE lado VENDEDOR: cargo a Caja/Bancos por identificar (o a las
      // cuentas de saldo a favor, si la forma de pago es SF), una línea por
      // forma de pago (misma cuenta que usa el lado cobrador para su cargo,
      // según claveSat) ──────────────────────────────────────────────────
      if (centroVendedor && String(centroVendedor.id) === String(centroCostoId)) {
        if (cfdiOriginal?.uuid) facturasVendedorCubiertas.add(cfdiOriginal.uuid.toUpperCase());
        lineas.forEach(l => {
          candidatas.push({
            cuentaId:      l.cuentaId,
            cuentaFaltante: false,
            concepto:      l.concepto,
            debe:          l.montoAsignado,
            haber:         0,
            serie:         serieFolioFactura,
            folio:         cobro.folioOrigen ?? null,
            centroCosto:   centroVendedor.clave,
            centroCostoId: centroVendedor.id,
            tipoOrigen:    'Cobro Sucursal',
            reglaNombre:   l.reglaNombre,
            // Sin esto, el diagnóstico de "asientos descuadrados" (que agrupa
            // por cfdiUuid) nunca encuentra este Cargo bajo la factura que
            // generó el Abono, y la marca como descuadrada aunque la póliza
            // sí cuadre en total.
            cfdiUuid:      cfdiOriginal?.uuid ?? null,
          });
        });
      }

      // ── PUE lado COBRADOR: cargo Caja/Bancos (o SF) por forma de pago
      // (cobro real) + abono a la MISMA cuenta por forma de pago — sin
      // cuenta puente; el neto de esta póliza en esas cuentas es cero, pero
      // cada línea cuadra contra su contraparte de la póliza vendedora al
      // consolidar. ─────────────────────────────────────────────────────
      if (centroCobrador && String(centroCobrador.id) === String(centroCostoId)) {
        lineas.forEach(l => {
          candidatas.push({
            cuentaId:      l.cuentaId,
            cuentaFaltante: false,
            concepto:      l.concepto,
            debe:          l.montoAsignado,
            haber:         0,
            serie:         serieFolioFactura,
            folio:         cobro.folioOrigen ?? null,
            centroCosto:   centroCobrador.clave,
            centroCostoId: centroCobrador.id,
            tipoOrigen:    'Cobro Sucursal',
            reglaNombre:   l.reglaNombre,
            cfdiUuid:      cfdiOriginal?.uuid ?? null,
          });
          candidatas.push({
            cuentaId:      l.cuentaId,
            cuentaFaltante: false,
            concepto:      l.concepto,
            debe:          0,
            haber:         l.montoAsignado,
            serie:         serieFolioFactura,
            folio:         cobro.folioOrigen ?? null,
            centroCosto:   centroCobrador.clave,
            centroCostoId: centroCobrador.id,
            tipoOrigen:    'Cobro Sucursal',
            reglaNombre:   l.reglaNombre,
            cfdiUuid:      cfdiOriginal?.uuid ?? null,
          });
        });
      }
    }
  }

  // reglaNombre SIN el prefijo "Cobro de otra sucursal -": la línea de Abono
  // a Clientes (cfdi-poliza-generator.service.js) lleva tipoOrigen='Cobro
  // Sucursal', así que _extraerCobrosSucursal (poliza.service.js) ya le
  // agrega el prefijo — ponerlo aquí también lo duplicaba ("Cobro de otra
  // sucursal - Cobro de otra sucursal - X", verificado con datos reales
  // 2026-08-03).
  const facturasPPDCubiertas = new Map(
    [...facturasPPDAcumulado.entries()].map(([uuid, v]) => [
      uuid,
      {
        monto: Math.round(v.monto * 100) / 100,
        reglaNombre: v.nombresFormaPago.size ? [...v.nombresFormaPago].join('/') : null,
      },
    ]),
  );

  // Tickets con cobro real pero sin ninguna factura ligada (ver
  // `_detectarPendientesPorFacturar`) — informativo, nunca se mezcla con
  // `candidatas`/`movimientos`.
  const foliosConocidos = new Set(docsUnicos.map(d => `${d.serie}|${d.folio}`));
  const centroDelDia = serieDelDia ? (ccBySerieMap[serieDelDia] ?? null) : null;
  const pendientesPorFacturar = (await _detectarPendientesPorFacturar({
    foliosDelDiaNumericos, serieDelDia, foliosConocidos, fechaDesde, fechaHasta,
  })).map(p => ({ ...p, centroCosto: centroDelDia?.clave ?? null, centroCostoId: centroDelDia?.id ?? null, sucursal: centroDelDia?.sucursal ?? null }));

  if (!candidatas.length) return { movimientos: [], facturasVendedorCubiertas, facturasPPDCubiertas, pendientesPorFacturar };

  // 3. Idempotencia: solo contra movimientos YA existentes en la cuenta
  // puente para ESTE centroCostoId — la otra pierna de la misma venta vive
  // en el centro contrario y no cuenta como duplicado.
  // Se filtra por tipoOrigen (no por cuentaId): Caja/Bancos por identificar
  // son cuentas compartidas con otros flujos (TO-CAN, anticipos, etc.), así
  // que el account code ya no sirve para aislar solo estos movimientos.
  const foliosOrigen = [...new Set(candidatas.map(c => c.folio).filter(Boolean))];
  const yaRegistrados = await PolizaMovimiento.findAll({
    where: {
      tipoOrigen:    'Cobro Sucursal',
      centroCostoId,
      folio:         { [Op.in]: foliosOrigen },
    },
    attributes: ['folio'],
    include: [{
      model:      Poliza,
      as:         'poliza',
      attributes: [],
      where:      { rfc, estado: { [Op.ne]: 'cancelada' } },
      required:   true,
    }],
  });
  const foliosYaRegistrados = new Set(yaRegistrados.map(m => m.folio));

  return {
    movimientos: candidatas.filter(c => !foliosYaRegistrados.has(c.folio)),
    facturasVendedorCubiertas,
    facturasPPDCubiertas,
    pendientesPorFacturar,
  };
}

module.exports = { construirMovimientosPuente, _extraerDocumentosRelacionados };
