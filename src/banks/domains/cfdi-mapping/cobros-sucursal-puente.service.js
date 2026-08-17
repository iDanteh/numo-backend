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
 * ── PPD (Crédito): NO genera ningún movimiento aquí. Se probó un asiento
 *    adicional vía la cuenta puente "Cobros De Sucursales Por Identificar"
 *    (2103040001) para cerrar la CxC desde Ingreso, pero el usuario confirmó
 *    (2026-08-05) que ese cierre es responsabilidad exclusiva de Cobranza —
 *    la póliza de Ingreso nunca debe mostrar cobros de otra sucursal para
 *    ventas a crédito. `esPPD` corta el procesamiento del cobro apenas se
 *    detecta (ver `if (esPPD) continue;` más abajo).
 *
 * ── SALDO A FAVOR: cuando una forma de pago del cobro es "saldo a favor"
 *    (aplicación de un saldo existente del cliente, no dinero nuevo), NO usa
 *    Caja/Bancos ni la cuenta puente — usa directamente "Anticipos Otros"
 *    (2103090001, subtotal) + "IVA Trasladado - Anticipos" (2104010002, IVA
 *    16%), con columna C = "SF" (confirmado con el usuario 2026-08-03).
 *    Mismo patrón vendedor/cobrador que Efectivo/Transferencia — no es un
 *    movimiento de banco, es puramente la aplicación del saldo. Solo aplica a
 *    PUE: si la factura original es PPD, `esPPD` corta antes de llegar aquí
 *    (ver nota de PPD arriba).
 *
 * ── CÓMO SE ENTERA LA COBRADORA (2026-08-05): esta función recibe `cfdis`
 *    acotado a la propia serie de facturación de la sucursal que se está
 *    generando (ver `_fetchCfdisParaPuenteAmplio` en
 *    cfdi-poliza-generator.service.js) — el "documento relacionado" que
 *    revela un cobro cruzado SIEMPRE sale de un CFDI de la serie de quien
 *    VENDIÓ, nunca de quien solo cobró. Por diseño, entonces, SOLO la
 *    sucursal vendedora puede descubrir este cobro en su propia generación.
 *    Se probó ampliar la búsqueda a las 11 sucursales para que la cobradora
 *    también lo viera directo, pero saturaba el ERP con 429 y tardaba 5+
 *    minutos aun con caché+reintento. En vez de eso: cuando la vendedora
 *    detecta el cobro cruzado, además de su propio asiento, ENCOLA en
 *    `CobroSucursalPendiente` lo que la cobradora necesita — cuando la
 *    cobradora genera su propia póliza (con su propia serie, sin tocar el
 *    ERP para esto), `_aplicarCobrosSucursalPendientes` lee esa cola y arma
 *    su asiento directo. Si la cobradora genera ANTES que la vendedora
 *    (orden no garantizado dentro de una misma corrida), simplemente no
 *    encuentra nada todavía — al regenerar después de que la vendedora ya
 *    corrió, sí lo toma (mismo comportamiento que cualquier otro ajuste
 *    pendiente, visible en el reporte de "asientos descuadrados" mientras
 *    tanto).
 */

const { Op } = require('sequelize');
const { PolizaMovimiento, Poliza, AccountPlan, CobroSucursalPendiente } = require('../../../shared/models/postgres');
const BankMovement = require('../banks/BankMovement.model');
const CFDI = require('../../../visor/models/CFDI');
const {
  obtenerDesglosesCobroAlmacen, obtenerSaldosFavor,
  obtenerDesglosesCobroAlmacenPorCentro, obtenerSaldosFavorPorCentro,
} = require('../erp/erp-sync.service');
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

// "PUNTOS" = monedero electrónico Club Tuberos aplicado como forma de pago —
// mismo principio que Saldo a Favor (reduce un pasivo existente, no es
// dinero nuevo), pero cuenta distinta (2103090002, Anticipos Otros Club
// Tuberos) — confirmado con el usuario 2026-08-06. Cajas reutiliza
// claveSat="01" (Efectivo) para este tipo, así que SOLO el texto de `nombre`
// distingue el caso — sin este check, "PUNTOS" se clasificaría como Efectivo
// normal.
function _esPuntos(fp) {
  return /puntos/i.test(fp?.nombre ?? '');
}

// c_FormaPago SAT — Cheque y Tarjeta, para el orden fijo de abajo (Efectivo
// ya tiene su propia constante arriba). Mismos códigos que
// FORMA_PAGO_CHEQUE/LABEL_FORMA_PAGO_CONSOLIDADO en poliza.service.js
// (duplicado a propósito, archivos pequeños, independientes).
const CLAVE_SAT_CHEQUE           = '02';
const CLAVES_SAT_TARJETA         = ['04', '28'];

// Orden fijo de las líneas de un mismo cobro cuando tiene varias formas de
// pago (ej. parte efectivo, parte tarjeta) — confirmado con el usuario
// 2026-08-05: Efectivo, Transferencia, Saldo a favor, Cheque, Tarjeta (en
// ese orden). Cualquier forma de pago no reconocida (o sin claveSat, ej.
// "SIN FORMA DE PAGO — REVISAR") queda al final.
function _ordenFormaPago(fp) {
  if ((fp.claveSat ?? '').trim() === CLAVE_SAT_EFECTIVO) return 0;
  if ((fp.claveSat ?? '').trim() === '03') return 1;
  if (_esSaldoAFavor(fp)) return 2;
  if (_esPuntos(fp)) return 2.5;
  if ((fp.claveSat ?? '').trim() === CLAVE_SAT_CHEQUE) return 3;
  if (CLAVES_SAT_TARJETA.includes((fp.claveSat ?? '').trim())) return 4;
  return 5;
}
function _ordenarFormasPago(formasPago) {
  return [...formasPago].sort((a, b) => _ordenFormaPago(a) - _ordenFormaPago(b));
}

// Tasa usada para partir el monto de "saldo a favor" en subtotal/IVA — todos
// los casos reales vistos hasta ahora son tasa 16%.
const TASA_IVA_SALDO_FAVOR = 0.16;

// Mismo texto que ETIQUETA_COBRO_SUCURSAL en poliza.service.js (columna C) —
// formato "COS-FORMADEPAGO" (confirmado con el usuario 2026-08-07).
const ETIQUETA_COBRO_SUCURSAL = 'COS';
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
// Columna C para líneas de monedero Club Tuberos ("PUNTOS") — literal "PAGO",
// sin el prefijo "Cobro de otra sucursal -", mismo criterio que SF
// (confirmado con el usuario 2026-08-06).
const ETIQUETA_PUNTOS = 'PAGO';
// tipoOrigen distinto de 'Cobro Sucursal' para los tickets sin factura de la
// PROPIA sucursal (sin cruce real) — necesario para que la columna C en el
// export NO lleve el prefijo "Cobro de otra sucursal -" (ver
// `_extraerCobrosSucursal` en poliza.service.js, que trata ambos tipoOrigen
// pero solo agrega el prefijo a 'Cobro Sucursal'). Confirmado con el usuario
// 2026-08-06: antes estos tickets se saltaban por completo (mismo almacén,
// no es cruzado) — ahora también generan su asiento (Cargo flotante sin
// contrapartida, se resuelve al facturar), solo que sin cruce de sucursal.
const TIPO_ORIGEN_PENDIENTE_PROPIO = 'Pendiente Propio';

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
 * Devuelve una lista informativa para mostrar aparte como "pendientes por
 * facturar" (esta función en sí NO arma movimientos contables). Si además el
 * cobro resulta cruzado de sucursal, el CALLER (`construirMovimientosPuente`,
 * justo después de invocar esta función) SÍ le genera su asiento de "Cobro
 * de otra sucursal" (Cargo Caja/Bancos + Abono a la cuenta puente) aparte —
 * confirmado con el usuario 2026-08-05: el ticket debe aparecer en AMBOS
 * lados (pendiente por facturar + asiento real), el dinero ya se cobró de
 * verdad aunque falte la factura.
 */
async function _detectarPendientesPorFacturar({ rfc, foliosDelDiaNumericos, serieDelDia, foliosConocidos, fechaDesde, fechaHasta }) {
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
      rfc,
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
        formasPago:  (cobro.formasPago ?? []).map(fp => ({ nombre: fp.nombre ?? fp.claveSat ?? null, claveSat: fp.claveSat ?? null, monto: Number(fp.monto) || 0 })),
        fecha:       cobro.fecha,
        folioOrigen: cobro.folioOrigen ?? null,
        // Centro que hizo el cobro (puede ser distinto al que vendió el
        // ticket) — necesario para generar el asiento de "Cobro de otra
        // sucursal" cuando aplica (ver uso en construirMovimientosPuente).
        claveCentro: cobro.claveCentro ?? null,
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
 * Encola (upsert) lo que la sucursal COBRADORA necesita para su propio
 * asiento de "Cobro de otra sucursal" — ver nota de arquitectura en el
 * encabezado del archivo. `lineas` ya viene resuelta (cuentaId real,
 * incluyendo depósito bancario real o saldo a favor si aplica) por el
 * caller — esta función solo persiste, no decide nada de contabilidad.
 *
 * Upsert por (rfc, centroCostoIdDestino, folioOrigen): si la vendedora se
 * regenera (mismo folio), actualiza la fila existente en vez de duplicarla.
 */
async function _encolarCobroSucursalPendiente({
  rfc, centroCostoIdOrigen, centroCostoIdDestino, serieFolioTicket, folioOrigen,
  cfdiUuid, nombreCliente, montoTotal, lineas, tratamiento, fechaCobro,
}) {
  if (!folioOrigen || !centroCostoIdDestino || !lineas.length) return;
  await CobroSucursalPendiente.sequelize.query(`
    INSERT INTO cobros_sucursal_pendientes
      (rfc, centro_costo_id_origen, centro_costo_id_destino, serie_folio_ticket,
       folio_origen, cfdi_uuid, nombre_cliente, monto_total, lineas, tratamiento,
       fecha_cobro, created_at, updated_at)
    VALUES (:rfc, :centroCostoIdOrigen, :centroCostoIdDestino, :serieFolioTicket,
            :folioOrigen, :cfdiUuid, :nombreCliente, :montoTotal, :lineas, :tratamiento,
            :fechaCobro, NOW(), NOW())
    ON CONFLICT (rfc, centro_costo_id_destino, folio_origen)
    DO UPDATE SET
      centro_costo_id_origen = EXCLUDED.centro_costo_id_origen,
      serie_folio_ticket     = EXCLUDED.serie_folio_ticket,
      cfdi_uuid              = EXCLUDED.cfdi_uuid,
      nombre_cliente         = EXCLUDED.nombre_cliente,
      monto_total            = EXCLUDED.monto_total,
      lineas                 = EXCLUDED.lineas,
      tratamiento            = EXCLUDED.tratamiento,
      fecha_cobro            = EXCLUDED.fecha_cobro,
      updated_at             = NOW()
  `, {
    replacements: {
      rfc,
      centroCostoIdOrigen:   centroCostoIdOrigen ?? null,
      centroCostoIdDestino,
      serieFolioTicket:      serieFolioTicket ?? null,
      folioOrigen,
      cfdiUuid:              cfdiUuid ?? null,
      nombreCliente:         nombreCliente ?? null,
      montoTotal,
      lineas:                JSON.stringify(lineas),
      tratamiento,
      fechaCobro:            fechaCobro ?? null,
    },
  });
}

/**
 * Punto de entrada único para mantener la cola al día — llamar SIEMPRE que
 * se evalúe un folio candidato a cruce de sucursal (haya resultado cruzado o
 * no esta vez), nunca solo cuando SÍ hay cruce. Sin esto, si un caso deja de
 * ser cruzado entre una regeneración y otra (por un fix de código, un dato
 * corregido en el ERP, etc.), la fila vieja queda huérfana para siempre —
 * confirmado con el usuario 2026-08-05: pasó justo así con JONATAN/DEV-055225
 * durante las pruebas de hoy, tuvo que borrarse a mano.
 *
 * `centroCostoIdDestino: null` (o `lineas` vacío) → ya NO es cruzado: borra
 * cualquier fila vieja de este folio, sin importar a qué sucursal apuntaba.
 * `centroCostoIdDestino` presente → upsert normal, y de paso limpia cualquier
 * fila vieja de este MISMO folio que apuntara a una sucursal DISTINTA (caso
 * borde: el cruce cambió de sucursal destino entre una corrida y otra).
 */
async function _sincronizarCobroSucursalPendiente(datos) {
  const { rfc, folioOrigen, centroCostoIdDestino, lineas } = datos;
  if (!rfc || !folioOrigen) return;
  if (!centroCostoIdDestino || !lineas?.length) {
    await CobroSucursalPendiente.destroy({ where: { rfc, folioOrigen } });
    return;
  }
  await CobroSucursalPendiente.destroy({
    where: { rfc, folioOrigen, centroCostoIdDestino: { [Op.ne]: centroCostoIdDestino } },
  });
  await _encolarCobroSucursalPendiente(datos);
}

/**
 * Lee la cola de `_encolarCobroSucursalPendiente` para ESTE centroCostoId
 * (como cobradora) y arma sus candidatas — sin tocar el ERP. `tratamiento`
 * decide el patrón contable de cada línea (ver comentario en
 * CobroSucursalPendiente.js):
 *   - 'PUE': Cargo + Abono a la MISMA cuenta (self-balancing, cuadra contra
 *     el Cargo sin contrapartida que la vendedora ya dejó en su póliza).
 *   - 'HUERFANO': Cargo a Caja/Bancos + Abono a la cuenta puente (cuadra
 *     contra el Cargo puente que la vendedora ya dejó).
 *   - 'SF_GENERADO': Abono solo (sin Cargo que lo compense, igual que
 *     `_inyectarSaldoFavorGenerado` en cfdi-poliza-generator.service.js) —
 *     una Devolución generó un saldo a favor pero se procesó físicamente en
 *     ESTA sucursal, no en la del CFDI (ver `_prefetchSaldosFavorGenerados`).
 */
async function _aplicarCobrosSucursalPendientes({ rfc, centroCostoId, centroCobradorClave, cuentaPuenteId, fechaDesde, fechaHasta }) {
  const where = { rfc, centroCostoIdDestino: centroCostoId };
  if (fechaDesde && fechaHasta) where.fechaCobro = { [Op.gte]: fechaDesde, [Op.lte]: fechaHasta };
  const pendientes = await CobroSucursalPendiente.findAll({ where, raw: true });

  const candidatas = [];
  for (const p of pendientes) {
    const lineas = p.lineas ?? [];
    const concepto = [p.nombreCliente, p.serieFolioTicket].filter(Boolean).join(' / ');
    const base = {
      cuentaFaltante: false,
      concepto,
      serie:          p.serieFolioTicket,
      folio:          p.folioOrigen,
      centroCosto:    centroCobradorClave ?? null,
      centroCostoId,
      tipoOrigen:     'Cobro Sucursal',
      cfdiUuid:       p.cfdiUuid ?? null,
    };
    if (p.tratamiento === 'PUE') {
      lineas.forEach(l => {
        candidatas.push({ ...base, cuentaId: l.cuentaId, debe: l.monto, haber: 0, reglaNombre: l.reglaNombre });
        candidatas.push({ ...base, cuentaId: l.cuentaId, debe: 0, haber: l.monto, reglaNombre: l.reglaNombre });
      });
    } else if (p.tratamiento === 'HUERFANO' && cuentaPuenteId) {
      lineas.forEach(l => {
        candidatas.push({ ...base, cuentaId: l.cuentaId, debe: l.monto, haber: 0, reglaNombre: l.reglaNombre });
        candidatas.push({ ...base, cuentaId: cuentaPuenteId, debe: 0, haber: l.monto, reglaNombre: l.reglaNombre });
      });
    } else if (p.tratamiento === 'SF_GENERADO') {
      lineas.forEach(l => {
        candidatas.push({ ...base, cuentaId: l.cuentaId, debe: 0, haber: l.monto, reglaNombre: l.reglaNombre });
      });
    }
  }
  return candidatas;
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
 * @returns {Promise<{movimientos: Array, facturasVendedorCubiertas: Map<string,number>, facturasPPDCubiertas: Map<string,{monto:number, reglaNombre:string}>}>}
 *   `movimientos`: líneas listas para concatenar a movimientosResult/todosLosMovimientos.
 *   `facturasVendedorCubiertas`: UUID (mayúsculas) → monto YA cubierto por
 *   líneas de Cargo a Caja/Bancos por identificar de este flujo, para ESTE
 *   centroCostoId (como vendedora). El Cargo normal que arma cfdiToMovimientos
 *   según formaPago del propio CFDI debe reducirse por este monto (no
 *   omitirse siempre por completo) — corrección 2026-08-06: para una Factura
 *   Global (un solo CFDI que agrupa cientos de tickets), basta con que UN
 *   ticket se haya cobrado en otra sucursal para que el monto acumulado aquí
 *   sea MENOR al total de la factura — el resto (tickets cobrados en la
 *   MISMA sucursal) necesita su propio Cargo normal, que antes se omitía por
 *   completo tratando esto como un booleano sí/no (caso real: Global de
 *   $206,937.70 con 3 tickets cruzados por $9,773.35 — el código omitía LOS
 *   $206,937.70 completos, perdiendo ~$197,164 de cargo real). Para una
 *   factura normal (no Global), el monto acumulado es simplemente el total de
 *   la factura y el efecto es el mismo que antes (Cargo completo omitido).
 *   Ver cfdi-poliza-generator.service.js, donde se usa para calcular el
 *   remanente en vez de solo filtrar.
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
  cuentaClubTuberosId,
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
  // Serie de facturación de ESTA sucursal (ej. "E0" para Atzompa) — cuando
  // viene junto con fechaDesde/fechaHasta, permite que la sucursal COBRADORA
  // descubra DIRECTO (sin depender de que la vendedora ya se haya generado)
  // lo que cobró de otras sucursales ese día, vía el endpoint nuevo
  // `/desgloses-cobro/almacen` y `/saldos-favor` filtrado por centro+fecha
  // (ver `obtenerDesglosesCobroAlmacenPorCentro`/`obtenerSaldosFavorPorCentro`
  // en erp-sync.service.js). Complementa (no reemplaza) la cola
  // `CobroSucursalPendiente`: si algo no llega por aquí (fuera del rango de
  // fechas, endpoint caído, etc.), la cola sigue siendo la red de seguridad.
  centroPropioClave,
}) {
  const vacio = { movimientos: [], facturasVendedorCubiertas: new Map(), facturasPPDCubiertas: new Map(), pendientesPorFacturar: [] };
  if (!centroCostoId || !cuentaCajaId || !cuentaBancosId) return vacio;

  // 1. Documentos relacionados de todos los CFDIs del periodo → batch de consulta.
  // Un mismo CFDI (ej. una Factura Global) puede traer VARIOS documentos
  // relacionados — flatMap, no map, para no quedarnos solo con el primero.
  const docsPorCfdi = cfdis
    .flatMap(cfdi => _extraerDocumentosRelacionados(cfdi).map(doc => ({ cfdi, doc })));

  // Puede seguir sin ningún documento relacionado conocido y AÚN encontrar
  // cobros cruzados vía la consulta directa por centro+fecha (más abajo) —
  // solo se corta aquí si tampoco hay esa vía disponible.
  if (!docsPorCfdi.length && !(centroPropioClave && fechaDesde && fechaHasta)) return vacio;

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
      rfc,
      series: lote.map(d => d.serie),
      folios: lote.map(d => d.folio),
    });
    cuentas.push(...resultado);
  }

  // 1a-bis. Consulta DIRECTA por centro+fecha (2026-08-13) — a diferencia del
  // bloque anterior (que solo conoce los documentos referenciados por `cfdis`,
  // acotado a la serie PROPIA), este endpoint regresa TODO lo cobrado/vendido
  // en ESTE centro ese día, sin importar de qué serie sea la venta. Es lo que
  // permite que la sucursal COBRADORA descubra un cruce por su cuenta, sin
  // esperar a que la VENDEDORA se haya generado primero y encolado el dato en
  // `CobroSucursalPendiente`. Solo aplica generando por día (necesita
  // fechaDesde/fechaHasta acotados a un único día — no tiene sentido pedirle
  // al ERP "todo lo cobrado en este centro en el mes completo").
  if (centroPropioClave && fechaDesde && fechaHasta) {
    // Ya liberado en producción (confirmado con el usuario 2026-08-14) — el
    // try/catch se deja de todos modos: si falla por cualquier otra razón
    // (ERP caído, timeout, etc.), esto NO debe tumbar la generación completa
    // de la póliza: se ignora esta fuente y se sigue solo con lo que ya
    // encontró la vía de siempre (documentos relacionados + cola
    // `CobroSucursalPendiente`).
    try {
      const fechaDesdeIso = fechaDesde.toISOString();
      const fechaHastaIso = fechaHasta.toISOString();
      const cuentasIdsConocidas = new Set(cuentas.map(c => c.cuentaId).filter(Boolean));

      // Folios de CFDIs propios de este centro en el batch actual — ya los
      // procesa cfdiToMovimientos, incluirlos aquí duplicaría sus cobros.
      // CFDIs de períodos ANTERIORES (ej. factura de julio con un SF usado
      // hoy en agosto) NO están en el batch y nadie más los registra, así
      // que el puente debe hacerlo (política: si el SF se usó ese día, va
      // en esa póliza, sin importar cuándo se generó, confirmado 2026-08-17).
      const ownBatchFolios = new Set(
        cfdis.filter(c => c.serie === centroPropioClave && c.folio).map(c => String(c.folio))
      );

      const cuentasDirecto = await obtenerDesglosesCobroAlmacenPorCentro({
        rfc, centro: centroPropioClave, fechaDesde: fechaDesdeIso, fechaHasta: fechaHastaIso,
      });
      for (const c of cuentasDirecto) {
        if (c.cuentaId && cuentasIdsConocidas.has(c.cuentaId)) continue;
        // Omitir entradas cuya sucursal VENDEDORA es esta misma sucursal Y
        // cuyo CFDI SÍ está en el batch actual (cfdiToMovimientos lo cubre).
        // Si el folio es de un período anterior, el CFDI no está en el batch
        // y el puente es el único que puede registrar el SF usado ese día.
        // — Si serieFactura es null no se sabe quién vendió → SE INCLUYE.
        if (centroPropioClave && c.serieFactura === centroPropioClave
            && ownBatchFolios.has(String(c.folioFactura || c.folioVenta || ''))) continue;
        // Filtrar a solo los cobros que ocurrieron físicamente en ESTE centro.
        // El endpoint puede devolver el historial COMPLETO de un ticket,
        // incluyendo cobros en otros centros (ej. un ticket de Hidalgo que
        // también tuvo un cobro en CEDIS). Esos cobros-en-otro-centro para
        // CFDIs PROPIOS ya los detecta el camino serie/folio y los procesa
        // en el bloque vendedor → `facturasVendedorCubiertas`. Procesarlos
        // aquí TAMBIÉN los contaría por duplicado, reduciendo el `debe` de
        // `cfdiToMovimientos` a cero para esas facturas y haciéndolas
        // desaparecer del consolidado (bug real: Hidalgo TARJETA $37k vs
        // $150k esperado, confirmado 2026-08-15).
        const cobrosFiltrados = (c.cobros ?? []).filter(
          cobro => !centroPropioClave || !cobro.claveCentro || cobro.claveCentro === centroPropioClave,
        );
        if (!cobrosFiltrados.length) continue;
        cuentas.push({ ...c, cobros: cobrosFiltrados });
        if (c.cuentaId) cuentasIdsConocidas.add(c.cuentaId);

      }

      // Resolver nombreCliente/metodoPago (vía Mongo, NO el ERP) de las cuentas
      // que la consulta directa trajo y que `cfdiPorDoc` todavía no conoce —
      // mismo criterio que usa `cfdiPorDoc` para las demás, solo que la fuente
      // del serie/folio es `cuenta.serieVenta`/`folioVenta` en vez de
      // `documentosRelacionados` de un CFDI ya cargado.
      // MongoDB almacena CFDIs indexados por serie/folio SAT (serieFactura/
      // folioFactura del ERP), NO por el número de ticket interno
      // (serieVenta/folioVenta). Se consulta por serieFactura|folioFactura y
      // se guarda bajo serieVenta|folioVenta (la clave que usa cfdiPorDoc en el
      // loop de más abajo) para que cfdiOriginal.uuid quede poblado y el dedup
      // por UUID en cfdi-poliza-generator funcione correctamente.
      const factKeyAVentaKey = new Map();
      for (const c of cuentasDirecto) {
        if (!c.serieVenta || !c.folioVenta) continue;
        const ventaKey = `${c.serieVenta}|${c.folioVenta}`;
        if (cfdiPorDoc.has(ventaKey)) continue;
        if (c.serieFactura && c.folioFactura) {
          const factKey = `${c.serieFactura}|${c.folioFactura}`;
          if (!factKeyAVentaKey.has(factKey)) factKeyAVentaKey.set(factKey, ventaKey);
        }
      }
      if (factKeyAVentaKey.size) {
        const queryPairs = [...factKeyAVentaKey.keys()].map(k => {
          const [s, f] = k.split('|');
          return { serie: s, folio: f };
        });
        const cfdisEncontrados = await CFDI.find({
          'emisor.rfc': rfc,
          $or: queryPairs,
        }).select('serie folio receptor metodoPago uuid').lean();
        for (const cf of cfdisEncontrados) {
          const factKey  = `${cf.serie}|${cf.folio}`;
          const ventaKey = factKeyAVentaKey.get(factKey) ?? factKey;
          if (!cfdiPorDoc.has(ventaKey)) cfdiPorDoc.set(ventaKey, cf);
        }
      }
    } catch (err) {
      const { logger } = require('../../../shared/utils/logger');
      logger.warn(`[CobrosSucursalPuente] Consulta directa por centro (${centroPropioClave}) falló, se ignora: ${err.message}`);
    }
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
      rfc,
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
  // Misma consulta, pero por centro+fecha (ver 1a-bis) — cubre las cuentas
  // que la sucursal cobradora descubrió directo y que `docsUnicos` no traía.
  if (centroPropioClave && fechaDesde && fechaHasta) {
    // Mismo criterio que la consulta directa de arriba: endpoint aún no
    // liberado en producción — un fallo aquí no debe tumbar la póliza.
    try {
      const resultadoDirecto = await obtenerSaldosFavorPorCentro({
        rfc, centro: centroPropioClave, fechaDesde: fechaDesde.toISOString(), fechaHasta: fechaHasta.toISOString(),
      });
      for (const cuenta of resultadoDirecto) {
        const key = `${cuenta.serieVenta}|${cuenta.folioVenta}`;
        // En la consulta por centro+fecha el ERP devuelve el dato desde la
        // perspectiva de la venta GEN: cuenta.serieVenta|folioVenta = GEN venta,
        // y saldosFavorUsados[].serieVenta|folioVenta = USE venta (la que aplicó
        // el saldo). Se indexa por USE venta para que el SF-APA fallback y el
        // loop de cobros la encuentren (distinto al path por-folio, donde la
        // cuenta misma ya es la USE venta y se indexa directamente por su clave).
        for (const uso of (cuenta.saldosFavorUsados ?? [])) {
          const usoKey = `${uso.serieVenta}|${uso.folioVenta}`;
          const existentesUso = usadosPorCuenta.get(usoKey) ?? [];
          if (!existentesUso.some(e => e.serieOrigen === uso.serieOrigen && String(e.folioOrigen) === String(uso.folioOrigen))) {
            usadosPorCuenta.set(usoKey, [...existentesUso, uso]);
          }
        }
        // El ERP indexa los SF por la venta GEN (no por la venta USO), así
        // que no hay un `saldosFavorUsados` de nivel superior — los usos
        // vienen anidados en `saldosFavorGenerados[].usos[]`. Se extraen aquí
        // para que ventas de períodos anteriores (ej. factura de julio con
        // SF aplicado hoy en agosto) también aparezcan en `usadosPorCuenta`
        // y el puente pueda registrar sus líneas de SF (2026-08-17).
        for (const gen of (cuenta.saldosFavorGenerados ?? [])) {
          for (const uso of (gen.usos ?? [])) {
            const usoKey = `${uso.serieVenta}|${uso.folioVenta}`;
            const existentes = usadosPorCuenta.get(usoKey) ?? [];
            const yaExiste = existentes.some(
              e => e.serieOrigen === gen.serieOrigen && String(e.folioOrigen) === String(gen.folioOrigen)
            );
            if (!yaExiste) {
              usadosPorCuenta.set(usoKey, [...existentes, {
                serieOrigen:   gen.serieOrigen,
                folioOrigen:   gen.folioOrigen,
                montoUsado:    uso.montoUsado,
                fecha:         uso.fecha,
                montoSobrante: uso.montoSobrante,
                serieVenta:    uso.serieVenta,
                folioVenta:    uso.folioVenta,
              }]);
            }
          }
        }
      }
    } catch (err) {
      const { logger } = require('../../../shared/utils/logger');
      logger.warn(`[CobrosSucursalPuente] Consulta directa de saldos a favor por centro (${centroPropioClave}) falló, se ignora: ${err.message}`);
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
      rfc,
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
  const facturasVendedorCubiertas = new Map(); // uuid → monto acumulado cubierto (ver docstring)
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
      const formasPago = _ordenarFormasPago((cobro.formasPago ?? []).length
        ? cobro.formasPago
        : [{ claveSat: null, nombre: 'SIN FORMA DE PAGO — REVISAR', monto: montoCobro }]);
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

        if (_esPuntos(fp)) {
          if (!cuentaClubTuberosId || !cuentaIvaSaldoFavorId || montoAsignado <= 0) return;
          const subtotal = Math.round((montoAsignado / (1 + TASA_IVA_SALDO_FAVOR)) * 100) / 100;
          const iva = Math.round((montoAsignado - subtotal) * 100) / 100;
          lineas.push({ cuentaId: cuentaClubTuberosId, montoAsignado: subtotal, reglaNombre: ETIQUETA_PUNTOS, esSF: true, concepto: conceptoBase });
          lineas.push({ cuentaId: cuentaIvaSaldoFavorId, montoAsignado: iva, reglaNombre: ETIQUETA_PUNTOS, esSF: true, concepto: conceptoBase });
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
          reglaNombre: (idCuentaBancoReal && bancoReal?.referencia) ? bancoReal.referencia : (fp.autorizacion || fp.nombre || fp.claveSat || null),
          esSF: false,
          concepto: conceptoBase,
        });
      });
      if (!lineas.length) continue;

      // PPD (Crédito): el cierre de la CxC cobrada en otra sucursal es
      // responsabilidad exclusiva de Cobranza, nunca de Ingreso — antes este
      // bloque generaba aquí un Cargo a la cuenta puente (vendedor) + Abono a
      // Clientes y, del lado cobrador, Cargo Caja/Bancos + Abono puente, pero
      // el usuario confirmó que esas líneas NO deben aparecer en la póliza de
      // Ingreso bajo ningún caso (revertido 2026-08-05, ver
      // `facturasPPDCubiertas` en cfdi-poliza-generator.service.js: al quedar
      // siempre vacío, los bloques que dependían de él quedan inertes sin
      // más cambios ahí).
      if (esPPD) continue;

      // ── PUE lado VENDEDOR: cargo a Caja/Bancos por identificar (o a las
      // cuentas de saldo a favor, si la forma de pago es SF), una línea por
      // forma de pago (misma cuenta que usará la cobradora para su cargo,
      // según claveSat) ──────────────────────────────────────────────────
      if (centroVendedor && String(centroVendedor.id) === String(centroCostoId)) {
        if (cfdiOriginal?.uuid) {
          const uuidUpper = cfdiOriginal.uuid.toUpperCase();
          facturasVendedorCubiertas.set(uuidUpper, (facturasVendedorCubiertas.get(uuidUpper) ?? 0) + montoCobro);
        }
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

        // Lado COBRADOR: esta póliza (la vendedora) es la ÚNICA que puede
        // ver este cobro cruzado (ver nota de arquitectura en el encabezado
        // del archivo) — en vez de intentar generar aquí mismo el asiento de
        // la cobradora (imposible sin su serie), se encola para que ella lo
        // aplique al generar su propia póliza, sin tocar el ERP. SIEMPRE se
        // llama (no solo cuando hay cruce): si esta vez NO es cruzado,
        // `_sincronizarCobroSucursalPendiente` limpia cualquier fila vieja de
        // este folio en vez de dejarla huérfana (confirmado con el usuario
        // 2026-08-05, caso real JONATAN/DEV-055225).
        const esCruzado = centroCobrador && String(centroCobrador.id) !== String(centroCostoId);
        if (cobro.folioOrigen) {
          await _sincronizarCobroSucursalPendiente({
            rfc,
            folioOrigen:          cobro.folioOrigen,
            centroCostoIdOrigen:  centroVendedor.id,
            centroCostoIdDestino: esCruzado ? centroCobrador.id : null,
            serieFolioTicket:     serieFolioFactura,
            cfdiUuid:             cfdiOriginal?.uuid ?? null,
            nombreCliente,
            montoTotal:           lineas.reduce((s, l) => s + l.montoAsignado, 0),
            lineas:               esCruzado ? lineas.map(l => ({ cuentaId: l.cuentaId, monto: l.montoAsignado, reglaNombre: l.reglaNombre })) : [],
            tratamiento:          'PUE',
            fechaCobro:           cobro.fecha ?? null,
          });
        }
      }
    }

    // SF usado por ventas de períodos anteriores pagadas puramente con APA
    // (aplicación de saldo a favor): la serie 'APA' no pasa el filtro
    // SERIES_CON_AUTH del loop de arriba (ver nota ahí — ese filtro es
    // correcto para ventas propias del batch). Para ventas de otro período
    // que solo tienen cobros APA, los datos ya están en usadosPorCuenta
    // (poblado desde saldosFavorGenerados[].usos[]). Política confirmada
    // 2026-08-17: si el SF se usó ese día, va en esa póliza sin importar
    // cuándo se generó ni el período de la venta original.
    if (!esPPD && cuentaSaldoFavorId && cuentaIvaSaldoFavorId) {
      const sfUsadosVenta = (usadosPorCuenta.get(`${cuenta.serieVenta}|${cuenta.folioVenta}`) ?? [])
        .filter(u => {
          if (!fechaDesde || !fechaHasta) return true;
          const f = u.fecha ? new Date(u.fecha) : null;
          return f && f >= fechaDesde && f <= fechaHasta;
        });
      const soloCobrosAPA = (cuenta.cobros ?? []).length > 0
        && (cuenta.cobros ?? []).every(cb => (cb.serieOrigen ?? '').toUpperCase() === 'APA');
      if (sfUsadosVenta.length > 0 && soloCobrosAPA
          && centroVendedor && String(centroVendedor.id) === String(centroCostoId)) {
        const montoSF = Math.round(
          sfUsadosVenta.reduce((s, u) => s + (Math.abs(Number(u.montoUsado)) || 0), 0) * 100
        ) / 100;
        if (montoSF > 0) {
          const usoOcultoAPA = sfUsadosVenta.every(
            u => devsOcultosCombinado.has(`${u.serieOrigen}|${u.folioOrigen}`)
          );
          const reglaSF    = usoOcultoAPA ? ETIQUETA_SALDO_FAVOR_OCULTO : ETIQUETA_SALDO_FAVOR;
          const subtotal   = Math.round((montoSF / (1 + TASA_IVA_SALDO_FAVOR)) * 100) / 100;
          const iva        = Math.round((montoSF - subtotal) * 100) / 100;
          const conceptoSF = [nombreCliente, serieFolioFactura].filter(Boolean).join(' / ');
          candidatas.push({
            cuentaId: cuentaSaldoFavorId, cuentaFaltante: false, concepto: conceptoSF,
            debe: subtotal, haber: 0, serie: serieFolioFactura, folio: null,
            centroCosto: centroVendedor.clave, centroCostoId: centroVendedor.id,
            tipoOrigen: 'Cobro Sucursal', reglaNombre: reglaSF, cfdiUuid: cfdiOriginal?.uuid ?? null,
          });
          candidatas.push({
            cuentaId: cuentaIvaSaldoFavorId, cuentaFaltante: false, concepto: conceptoSF,
            debe: iva, haber: 0, serie: serieFolioFactura, folio: null,
            centroCosto: centroVendedor.clave, centroCostoId: centroVendedor.id,
            tipoOrigen: 'Cobro Sucursal', reglaNombre: reglaSF, cfdiUuid: cfdiOriginal?.uuid ?? null,
          });
        }
      }
    }
  }

  // PPD ya no genera nada aquí (ver el `if (esPPD) continue;` arriba) — se
  // mantiene el Map vacío en el retorno porque cfdi-poliza-generator.service.js
  // sigue consultándolo (`facturasPPDCubiertas.get(...)`); con el Map siempre
  // vacío esos bloques quedan inertes sin tocar ese archivo.
  const facturasPPDCubiertas = new Map();

  // Tickets con cobro real pero sin ninguna factura ligada (ver
  // `_detectarPendientesPorFacturar`) — la lista que se regresa abajo sigue
  // siendo informativa (se muestra aparte, en la hoja de "Pendientes por
  // facturar"), pero SI el cobro fue cruzado de sucursal (cobrado en un
  // almacén distinto al que vendió el ticket) también se genera su asiento
  // de "Cobro de otra sucursal" en AMBAS pólizas — confirmado con el usuario
  // 2026-08-05: el dinero ya se cobró de verdad, no debe quedar sin
  // registrar en ninguna cuenta solo porque el ticket todavía no tiene
  // factura. Mismo principio que PUE normal (Cargo vendedor sin
  // contrapartida en su propia póliza / Cargo+Abono cobrador), pero como no
  // hay factura con qué cuadrar el lado vendedor, ambos lados usan la cuenta
  // puente (Cargo vendedor / Abono cobrador) en vez de Caja/Bancos directo
  // en el vendedor — se resuelve solo cuando el ticket se facture y el flujo
  // normal de arriba lo tome como cualquier otro documento relacionado.
  // Ancla adicional para el escaneo heurístico: `foliosDelDiaNumericos` arriba
  // solo se llena con folios YA conocidos por algún cruce real (un ticket
  // referenciado por otra Devolución/Factura Global) — si esta sucursal no
  // tuvo NINGÚN cruce ese día, el escaneo nunca arrancaba (`serieDelDia`
  // quedaba null), aunque sí tuviera sus propias facturas normales ese mismo
  // día que hubieran servido de ancla igual de bien. Se agregan los folios de
  // las facturas NORMALES de la propia sucursal (mismo día, misma serie) como
  // ancla de respaldo — confirmado con el usuario 2026-08-05, caso real
  // GUADALUPE RUIZ RAMIREZ / A0-260700940 (CEDIS): un ticket 100% propio sin
  // ningún cruce que nunca aparecía ni en "Pendientes por facturar" porque no
  // había ningún otro ticket cruzado ese día que sirviera de ancla.
  const serieDelCentro = Object.entries(ccBySerieMap).find(([, cc]) => String(cc.id) === String(centroCostoId))?.[0] ?? null;
  if (serieDelCentro) {
    for (const cfdi of cfdis) {
      if (cfdi.serie !== serieDelCentro) continue;
      if (fechaDesde && fechaHasta) {
        const fechaCfdi = cfdi.fecha ? new Date(cfdi.fecha) : null;
        if (!fechaCfdi || fechaCfdi < fechaDesde || fechaCfdi > fechaHasta) continue;
      }
      const folioNum = parseInt(cfdi.folio, 10);
      if (!isNaN(folioNum)) {
        foliosDelDiaNumericos.push(folioNum);
        serieDelDia = serieDelDia ?? serieDelCentro;
      }
    }
  }

  const foliosConocidos = new Set(docsUnicos.map(d => `${d.serie}|${d.folio}`));
  const centroDelDia = serieDelDia ? (ccBySerieMap[serieDelDia] ?? null) : null;
  const pendientesDetectados = await _detectarPendientesPorFacturar({
    rfc, foliosDelDiaNumericos, serieDelDia, foliosConocidos, fechaDesde, fechaHasta,
  });

  if (cuentaPuenteId) {
    for (const p of pendientesDetectados) {
      const centroVendedor = p.serie ? (ccBySerieMap[p.serie] ?? null) : null;
      const centroCobrador = p.claveCentro ? (ccBySerieMap[p.claveCentro] ?? null) : null;
      if (!centroCobrador) continue;

      const serieFolioTicket = `${p.serie ?? '?'}-${p.folio ?? '?'}`;
      const conceptoTicket = [p.nombreCliente, serieFolioTicket].filter(Boolean).join(' / ');
      const formasPagoTicket = _ordenarFormasPago(p.formasPago.length ? p.formasPago : [{ claveSat: null, nombre: 'SIN FORMA DE PAGO — REVISAR', monto: p.monto }]);

      const mismaSucursal = centroVendedor && String(centroVendedor.id) === String(centroCobrador.id);

      if (mismaSucursal) {
        // Ticket sin factura cobrado en SU PROPIA sucursal (sin cruce) —
        // confirmado con el usuario 2026-08-06: antes se saltaba por
        // completo (solo aparecía en la lista informativa de "Pendientes por
        // facturar"); ahora también genera su asiento — Cargo directo por
        // forma de pago (o split Club Tuberos si aplica, ver `_esPuntos`),
        // SIN cuenta puente (no hay otra sucursal con la que cuadrar):
        // floating debit hasta que se facture, mismo principio que el lado
        // vendedor de un cruce. tipoOrigen='Pendiente Propio' (no 'Cobro
        // Sucursal') para que la columna C NO lleve el prefijo "Cobro de
        // otra sucursal -" (ver `_extraerCobrosSucursal`, poliza.service.js:
        // no es un cruce real, sería una etiqueta falsa).
        if (String(centroVendedor.id) !== String(centroCostoId) || !p.folioOrigen) continue;

        // Limpieza defensiva: si este folio había quedado mal encolado como
        // cruzado en una corrida anterior (antes de este fix, o por un dato
        // que cambió de lado), se borra esa fila vieja de la cola.
        await _sincronizarCobroSucursalPendiente({
          rfc, folioOrigen: p.folioOrigen, centroCostoIdOrigen: centroVendedor.id,
          centroCostoIdDestino: null, serieFolioTicket, cfdiUuid: null,
          nombreCliente: p.nombreCliente, montoTotal: 0, lineas: [],
          tratamiento: 'HUERFANO', fechaCobro: p.fecha ?? null,
        });

        const totalFormasPagoTicket = formasPagoTicket.reduce((s, fp) => s + (Number(fp.monto) || 0), 0);
        let acumuladoTicket = 0;
        formasPagoTicket.forEach((fp, idx) => {
          const esUltimo = idx === formasPagoTicket.length - 1;
          const share = totalFormasPagoTicket > 0 ? (Number(fp.monto) || 0) / totalFormasPagoTicket : 1 / formasPagoTicket.length;
          const montoAsignado = esUltimo
            ? Math.round((p.monto - acumuladoTicket) * 100) / 100
            : Math.round(p.monto * share * 100) / 100;
          acumuladoTicket += montoAsignado;
          if (montoAsignado <= 0) return;

          if (_esPuntos(fp) && cuentaClubTuberosId && cuentaIvaSaldoFavorId) {
            const subtotal = Math.round((montoAsignado / (1 + TASA_IVA_SALDO_FAVOR)) * 100) / 100;
            const iva = Math.round((montoAsignado - subtotal) * 100) / 100;
            candidatas.push({
              cuentaId: cuentaClubTuberosId, cuentaFaltante: false, concepto: conceptoTicket,
              debe: subtotal, haber: 0, serie: serieFolioTicket, folio: p.folioOrigen,
              centroCosto: centroVendedor.clave, centroCostoId: centroVendedor.id,
              tipoOrigen: TIPO_ORIGEN_PENDIENTE_PROPIO, reglaNombre: ETIQUETA_PUNTOS, cfdiUuid: null,
            });
            candidatas.push({
              cuentaId: cuentaIvaSaldoFavorId, cuentaFaltante: false, concepto: conceptoTicket,
              debe: iva, haber: 0, serie: serieFolioTicket, folio: p.folioOrigen,
              centroCosto: centroVendedor.clave, centroCostoId: centroVendedor.id,
              tipoOrigen: TIPO_ORIGEN_PENDIENTE_PROPIO, reglaNombre: ETIQUETA_PUNTOS, cfdiUuid: null,
            });
            return;
          }

          const esEfectivo = (fp.claveSat ?? '').trim() === CLAVE_SAT_EFECTIVO;
          candidatas.push({
            cuentaId: esEfectivo ? cuentaCajaId : cuentaBancosId, cuentaFaltante: false,
            concepto: conceptoTicket, debe: montoAsignado, haber: 0, serie: serieFolioTicket,
            folio: p.folioOrigen, centroCosto: centroVendedor.clave, centroCostoId: centroVendedor.id,
            tipoOrigen: TIPO_ORIGEN_PENDIENTE_PROPIO, reglaNombre: fp.nombre || null, cfdiUuid: null,
          });
        });
        continue;
      }

      // ── Lado VENDEDOR: Cargo a la cuenta puente por el total cobrado —
      // mismo principio que el lado vendedor PUE normal (Cargo Caja/Bancos
      // sin contrapartida en esta póliza, se compensa al consolidar), solo
      // que aquí no hay factura con la que cuadrar todavía, así que se usa
      // la cuenta puente en vez de Caja/Bancos directo. Cuando el ticket se
      // facture, el flujo normal de arriba tomará este mismo folio como
      // cualquier otro documento relacionado.
      if (centroVendedor && String(centroVendedor.id) === String(centroCostoId) && p.monto > 0) {
        candidatas.push({
          cuentaId:      cuentaPuenteId,
          cuentaFaltante: false,
          concepto:      conceptoTicket,
          debe:          p.monto,
          haber:         0,
          serie:         serieFolioTicket,
          folio:         p.folioOrigen,
          centroCosto:   centroVendedor.clave,
          centroCostoId: centroVendedor.id,
          tipoOrigen:    'Cobro Sucursal',
          reglaNombre:   formasPagoTicket.map(fp => fp.nombre).filter(Boolean).join('/') || null,
          cfdiUuid:      null,
        });

        // Lado COBRADOR: se encola (ver nota de arquitectura en el
        // encabezado) — Cargo Caja/Bancos por forma de pago + Abono a la
        // cuenta puente, para que cuadre contra el Cargo puente de arriba
        // cuando la cobradora genere su propia póliza. SIEMPRE se llama (no
        // solo cuando hay cruce) para que la cola se limpie sola si deja de
        // serlo (ver `_sincronizarCobroSucursalPendiente`).
        const esCruzadoHuerfano = String(centroCobrador.id) !== String(centroVendedor.id);
        if (p.folioOrigen) {
          const totalFormasPagoTicket = formasPagoTicket.reduce((s, fp) => s + (Number(fp.monto) || 0), 0);
          let acumuladoTicket = 0;
          const lineasCobrador = [];
          formasPagoTicket.forEach((fp, idx) => {
            const esUltimo = idx === formasPagoTicket.length - 1;
            const share = totalFormasPagoTicket > 0 ? (Number(fp.monto) || 0) / totalFormasPagoTicket : 1 / formasPagoTicket.length;
            const montoAsignado = esUltimo
              ? Math.round((p.monto - acumuladoTicket) * 100) / 100
              : Math.round(p.monto * share * 100) / 100;
            acumuladoTicket += montoAsignado;
            if (montoAsignado <= 0) return;

            if (_esPuntos(fp) && cuentaClubTuberosId && cuentaIvaSaldoFavorId) {
              const subtotal = Math.round((montoAsignado / (1 + TASA_IVA_SALDO_FAVOR)) * 100) / 100;
              const iva = Math.round((montoAsignado - subtotal) * 100) / 100;
              lineasCobrador.push({ cuentaId: cuentaClubTuberosId, monto: subtotal, reglaNombre: ETIQUETA_PUNTOS });
              lineasCobrador.push({ cuentaId: cuentaIvaSaldoFavorId, monto: iva, reglaNombre: ETIQUETA_PUNTOS });
              return;
            }

            const esEfectivo = (fp.claveSat ?? '').trim() === CLAVE_SAT_EFECTIVO;
            lineasCobrador.push({ cuentaId: esEfectivo ? cuentaCajaId : cuentaBancosId, monto: montoAsignado, reglaNombre: fp.nombre || null });
          });
          await _sincronizarCobroSucursalPendiente({
            rfc,
            folioOrigen:          p.folioOrigen,
            centroCostoIdOrigen:  centroVendedor.id,
            centroCostoIdDestino: esCruzadoHuerfano ? centroCobrador.id : null,
            serieFolioTicket,
            cfdiUuid:             null,
            nombreCliente:        p.nombreCliente,
            montoTotal:           lineasCobrador.reduce((s, l) => s + l.monto, 0),
            lineas:               esCruzadoHuerfano ? lineasCobrador : [],
            tratamiento:          'HUERFANO',
            fechaCobro:           p.fecha ?? null,
          });
        }
      }
    }
  }

  const pendientesPorFacturar = pendientesDetectados.map(p => ({ ...p, centroCosto: centroDelDia?.clave ?? null, centroCostoId: centroDelDia?.id ?? null, sucursal: centroDelDia?.sucursal ?? null }));

  // Cola de cobros cruzados encolados por OTRA sucursal (cuando ESTA fue la
  // vendedora) — ver nota de arquitectura en el encabezado del archivo. Se
  // aplica sin importar si el loop de arriba encontró algo o no: esta
  // póliza puede ser puramente cobradora de tickets vendidos en otro lado.
  const centroPropio = Object.values(ccBySerieMap).find(cc => String(cc.id) === String(centroCostoId));
  const candidatasPendientes = await _aplicarCobrosSucursalPendientes({
    rfc, centroCostoId, centroCobradorClave: centroPropio?.clave ?? null, cuentaPuenteId, fechaDesde, fechaHasta,
  });
  candidatas.push(...candidatasPendientes);

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

  // TIPO_ORIGEN_PENDIENTE_PROPIO (ticket sin factura cobrado en SU PROPIA
  // sucursal, sin cruce real — ver bloque `mismaSucursal` arriba) es
  // puramente informativo: es un Cargo flotante sin ninguna contrapartida en
  // esta póliza (no hay otra sucursal con la que cuadrar). Confirmado con el
  // usuario 2026-08-10: estas líneas ("CLIENTE NO IDENTIFICADO / <folio>"
  // contra Caja/Bancos por identificar) NO deben aparecer en "Movimientos
  // contables" — solo en la hoja/lista separada `pendientesPorFacturar`
  // (que se arma aparte, arriba, a partir de `pendientesDetectados`, así que
  // excluirlas de aquí no le quita nada a esa lista). Las líneas de
  // 'Cobro Sucursal' (incluidas las que nacen del mismo ticket huérfano
  // cuando SÍ hay cruce, líneas ~1035 y ~849) siguen intactas — esas cuadran
  // contra la cuenta puente 2103040001 en la póliza cobradora y deben seguir
  // contabilizándose de verdad.
  const movimientos = candidatas.filter(c =>
    !foliosYaRegistrados.has(c.folio) && c.tipoOrigen !== TIPO_ORIGEN_PENDIENTE_PROPIO
  );

  return {
    movimientos,
    facturasVendedorCubiertas,
    facturasPPDCubiertas,
    pendientesPorFacturar,
  };
}

module.exports = { construirMovimientosPuente, _extraerDocumentosRelacionados, _encolarCobroSucursalPendiente, _sincronizarCobroSucursalPendiente };
