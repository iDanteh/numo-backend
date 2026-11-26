'use strict';

/**
 * reportes-por-facturar.fichas.js
 * ─────────────────────────────────────────────────────────────────────────────
 * "Fichas de efectivo" (Reportes, 2026-11-04, pedido del usuario con la foto
 * de su ficha de entrega de efectivo a traslado de valores). 2026-11-05: ya
 * no se eligen facturas globales — solo SUCURSAL y RANGO de fechas (más banco
 * y denominaciones, que captura la pantalla). La ficha suma el efectivo de la
 * CAJA de la sucursal en cada día del rango (corte de caja de Kore — lo que
 * ampara la global no incluye la cobranza y no cuadraba):
 *
 *   cobros con claveCentro = serie de la sucursal y fecha (día MX) = ese día
 *   (`/desgloses-cobro/almacen?centro=`), solo la parte en EFECTIVO:
 *   2026-11-25 (pedido del usuario) — renglones según el TIPO de venta en Kore
 *   (nombreTipoMovimiento / tipoPago de /cuentas-pendientes, memoria kore_remisiones):
 *     • Efectivo facturado:      venta de contado PUE ya facturada que quedó
 *       cobrada en su totalidad (sin saldo pendiente). Ticket (CBT) y contra
 *       entrega (CCE) facturados entran aquí sin preguntar a Kore.
 *     • Consignación facturada (consignacionFacturada): cobros de una venta de
 *       CONSIGNACIÓN (PPD o PUE) que ya está facturada, liquide o no.
 *     • Cobranza facturado (clave cobranzaEfectivo): venta PPD facturada — el
 *       cobro genera complemento de pago —, liquide o no.
 *     • Abonos en efectivo:      cualquier otro cobro que deja SALDO PENDIENTE
 *       (el saldo después del cobro y de la bonificación que Kore aplica en el
 *       mismo momento, p. ej. BON 4%).
 *     • Efectivo consignación SIN factura (clave efectivoPorFacturar): ya NO
 *       suma al total a entregar; va al final del anexo con su desglose.
 *       Incluye cualquier venta sin factura que no quedó con saldo pendiente.
 *     • Venta miscelánea:        cobros MIS ("TIENDITA TYC"), facturados o no.
 *     • Cobros de otras sucursales (2026-11-06, pedido del usuario): cualquier
 *       cobro o abono en efectivo de una remisión de OTRA sucursal hecho en esta
 *       caja (Puerto 31-oct: remisiones y abonos de CEDIS $10,785.42). Va antes
 *       que las demás clasificaciones; la póliza los lleva a la otra sucursal o a Cobranza.
 *     • APA = aplicación de saldo a favor: no es dinero nuevo → fuera.
 *   menos salidas de la caja ese día (`/desgloses-salidas/caja`):
 *     • Devoluciones en efectivo (RETD), retiro de efectivo (RETEF, 2026-11-06:
 *       Reforma 31-oct $2,601.03 — la póliza ya lo resta) y faltante en el cierre (RPF).
 *       "Salida por Transferencia" (ST) y "CIERRE CAJA" (CIC) NO restan.
 *   Informativos (no suman): depósito en efectivo (no pasa por la caja) y
 *   saldo a favor aplicado.
 *
 * Verificado contra la ficha física de CONSTRUCASA (C0): 31-oct $83,405.41 −
 * RETD $215.47 − RPF $0.55 = $83,189.39 (ficha $83,189.00); 1-nov $17,039.26 −
 * RPF $0.01 = $17,039.25 (ficha $17,039).
 */

const CFDI       = require('../../../visor/models/CFDI');
const centrosSvc = require('../centros-costo/centros-costo.service');
const { obtenerDesglosesCobroAlmacenPorCentro, obtenerDesglosesSalidasCajaPorAlmacen, obtenerDesglosesCobroPorFactura, obtenerCuentaTicket } = require('../erp/erp-sync.service');
const { traerFacturasRecientes } = require('./reportes-por-facturar.facturas-kore');
const { infoRemisiones } = require('../erp/kore-remisiones.service');
const { BadRequestError } = require('../../shared/errors/AppError');

const MAX_DIAS_RANGO = 31;
// Ficha (2026-11-05, decisión del usuario): ~3 s por día en Kore, consultados uno por uno — con
// más días la petición se acerca al tiempo de espera de nginx (60 s).
const MAX_DIAS_FICHA = 10;
const FECHA_RE = /^\d{4}-\d{2}-\d{2}$/;
const RFC_PUBLICO_GENERAL = 'XAXX010101000';
// Suman al total a entregar (las salidas restan).
// miscelaneaEfectivo (2026-11-05): ventas MISCELÁNEA (MIS, 'TIENDITA TYC') en su propio renglón — antes caían en por facturar.
// Orden (2026-11-25, pedido del usuario): efectivo facturado, consignación facturada, cobranza
// facturado, abonos, venta miscelánea, otras sucursales; luego lo que se resta.
const ENTRADAS = ['efectivoFacturado', 'consignacionFacturada', 'cobranzaEfectivo', 'abonosEfectivo', 'miscelaneaEfectivo', 'cobroOtraSucursal'];
const SALIDAS = ['devoluciones', 'retiroEfectivo', 'faltante'];
// bonificacionGlobal (2026-11-26): BN de las globales — lo cobrado que no se factura ni se deposita.
const INFORMATIVOS = ['depositoEfectivo', 'saldoFavor', 'bonificacionGlobal'];
// Efectivo de consignación SIN factura (2026-11-25): no suma al total; va al final del anexo.
// cobroOtraSucursalSinFactura (2026-11-26, pedido del usuario; Atzompa E0-261102072 cobrada en
// CEDIS 24-nov sin factura): no suma hasta que se facture; ese día suma en ESTA caja.
const SIN_FACTURA = ['efectivoPorFacturar', 'cobroOtraSucursalSinFactura'];
const CONCEPTOS = [...ENTRADAS, ...SALIDAS, ...INFORMATIVOS, ...SIN_FACTURA];
// Cobros que no hace falta preguntar a Kore: ticket y contra entrega son de contado.
const ORIGEN_CONTADO = new Set(['CBT', 'CCE']);
// Ajustes sin dinero (bonificación BON/BN) que Kore aplica justo después del cobro.
const MS_AJUSTE_MISMO_COBRO = 2 * 60 * 1000;

const _r2 = n => Math.round((Number(n) || 0) * 100) / 100;
const _serie = v => String(v ?? '').trim().toUpperCase();
const _diaMx = f => new Date(new Date(f).getTime() - 6 * 3600000).toISOString().slice(0, 10);
const _dias = (a, b) => Math.round((new Date(`${b}T00:00:00Z`) - new Date(`${a}T00:00:00Z`)) / 86400000) + 1;

// Misma clasificación de formas que el reporte "Por facturar" (mostrador.service._formaPago).
function _forma(fp) {
  const nombre = fp?.nombre ?? fp?.nombreFormaPago ?? '';
  if (/saldo\s*a\s*favor|anticipo/i.test(nombre)) return 'saldoFavor';
  // Puntos de lealtad: Kore a veces los manda con clave 01, pero no son efectivo
  // (C0 1-nov: $50.78 de PUNTOS hacían que no cuadrara con la ficha).
  if (/puntos/i.test(nombre)) return null;
  if (/dep[oó]sito/i.test(nombre)) return 'depositoEfectivo';
  if (String(fp?.claveSat ?? '').trim() === '01' || /efectivo/i.test(nombre)) return 'efectivo';
  return null; // tarjeta, transferencia, cheque, puntos… no van en la ficha
}

/** Facturas globales vigentes (una por UUID; manda la copia ERP). */
function _vigentesPorUuid(docs) {
  const porUuid = new Map();
  for (const c of docs) {
    const u = String(c.uuid ?? '').toUpperCase();
    if (!u) continue;
    if (!porUuid.has(u) || c.source === 'ERP') porUuid.set(u, c);
  }
  return [...porUuid.entries()]
    .filter(([, c]) => c.satStatus !== 'Cancelado' && !['Cancelado', 'Cancelacion Pendiente'].includes(c.erpStatus));
}

const _orden = (a, b) => a.dia.localeCompare(b.dia) || a.serie.localeCompare(b.serie) || String(a.folio ?? '').localeCompare(String(b.folio ?? ''), undefined, { numeric: true });

/**
 * Facturas globales (receptor público en general) vigentes de la sucursal en
 * el periodo — día en hora de México. Hoy / ayer se traen antes de Kore (la
 * descarga nocturna aún no las tiene), igual que el reporte "Por facturar".
 */
async function listarGlobales({ rfc, centroCostoId, fechaInicio, fechaFin }) {
  fechaFin = fechaFin || fechaInicio;
  if (!rfc) throw new BadRequestError('RFC requerido');
  if (!FECHA_RE.test(fechaInicio ?? '') || !FECHA_RE.test(fechaFin ?? '')) throw new BadRequestError('Fechas inválidas (formato YYYY-MM-DD)');
  if (fechaFin < fechaInicio) throw new BadRequestError('La fecha final no puede ser anterior a la inicial');
  if (_dias(fechaInicio, fechaFin) > MAX_DIAS_RANGO) throw new BadRequestError(`El rango no puede pasar de ${MAX_DIAS_RANGO} días`);

  const ccBySerieMap = await centrosSvc.resolveBySerieMap();
  const entradas = Object.entries(ccBySerieMap).filter(([, c]) => !centroCostoId || String(c.id) === String(centroCostoId));
  if (!entradas.length) throw new BadRequestError('La sucursal no tiene serie de facturación configurada');
  const series = entradas.map(([s]) => _serie(s));
  const sucursalDe = new Map(Object.entries(ccBySerieMap).map(([s, c]) => [_serie(s), c?.sucursal ?? null]));

  const advertencias = [];
  const recientes = await traerFacturasRecientes({ series, fechaInicio, fechaFin });
  if (recientes.error) {
    advertencias.push(`No se pudieron traer de Kore las facturas del ${recientes.dias.join(' y ')} (${recientes.error}) — pueden faltar las de ese día hasta la descarga de la noche.`);
  }

  const docs = await CFDI.find({
    'emisor.rfc': rfc,
    tipoDeComprobante: 'I',
    'receptor.rfc': RFC_PUBLICO_GENERAL,
    serie: { $in: series },
    fecha: { $gte: new Date(`${fechaInicio}T00:00:00.000-06:00`), $lte: new Date(`${fechaFin}T23:59:59.999-06:00`) },
  }).select('uuid serie folio fecha total satStatus erpStatus source').lean();

  const globales = _vigentesPorUuid(docs)
    .map(([uuid, c]) => ({
      uuid, serie: _serie(c.serie), folio: String(c.folio ?? '').trim(), fecha: c.fecha, dia: _diaMx(c.fecha),
      total: _r2(c.total), sucursal: sucursalDe.get(_serie(c.serie)) ?? null,
    }))
    .sort(_orden);

  return { sucursal: centroCostoId ? entradas[0][1]?.sucursal ?? null : 'Todas', fechaInicio, fechaFin, globales, advertencias: advertencias.length ? advertencias : null };
}

/**
 * Efectivo de la caja de una sucursal (serie) en un día, con el DETALLE de
 * cada cobro / salida que lo forma (2026-11-05: el usuario quiere el rastro
 * venta → factura / global → ficha).
 */
async function _corteDeCaja({ rfc, serie, dia, seriesSucursal = [serie], asegurarRecientes = async () => {} }) {
  const propias = new Set(seriesSucursal.map(_serie));
  const montos = Object.fromEntries(CONCEPTOS.map(k => [k, 0]));
  const detalle = [];
  const fechaDesde = new Date(`${dia}T00:00:00.000-06:00`).toISOString();
  const fechaHasta = new Date(`${dia}T23:59:59.999-06:00`).toISOString();
  const [cuentas, salidas] = await Promise.all([
    obtenerDesglosesCobroAlmacenPorCentro({ rfc, centro: serie, fechaDesde, fechaHasta }),
    obtenerDesglosesSalidasCajaPorAlmacen({ rfc, almacen: serie, fechaDesde, fechaHasta }),
  ]);

  // 2026-11-26 (pedido del usuario; CONCEPCION ESPINOSA y JUAN ALBERTO AQUINO, cobrados el
  // 23-nov y amparados por la global A0-261112740 del 24): lo que se cobró SIN factura es
  // "sin factura" en la ficha del día del cobro aunque hoy Kore ya lo muestre en una global
  // de un día POSTERIOR, y la ficha del día de esa global lo suma (consignación facturada…).
  // Las facturas de hoy / ayer aún no están en NUMO (descarga nocturna): se traen de Kore
  // solo si la ficha es de esos días o si un cobro tiene una factura que NUMO no conoce.
  const { hoy, ayer } = _hoyAyer();
  if (dia >= ayer) await asegurarRecientes();
  // Contra entrega sin factura ligada en Kore (2026-11-26, Atzompa E0-261102072 → E0-261100257).
  const cuentasDia = await _ligarContraEntrega(rfc, cuentas, serie, dia);
  const facturasCobros = cuentasDia.filter(c => c.serieFactura)
    .map(c => `${_serie(c.serieFactura)}-${String(c.folioFactura ?? '').trim()}`);
  let { dias: diaGlobal, faltan } = await _diaDeGlobales(rfc, facturasCobros);
  if (faltan && dia < hoy && await asegurarRecientes()) ({ dias: diaGlobal } = await _diaDeGlobales(rfc, facturasCobros));
  const anteriores = [];
  const globalesHoy = await _globalesDelDia(rfc, serie, dia);
  for (const g of globalesHoy) {
    const cs = await obtenerDesglosesCobroPorFactura({ rfc, serieFactura: g.serie, folioFactura: g.folio });
    for (const c of cs ?? []) anteriores.push({ ...c, serieFactura: g.serie, folioFactura: g.folio });
  }

  // 1) Cobros en efectivo; los que dependen del tipo de venta se clasifican después.
  const pendientes = [];
  const lotes = [{ lista: cuentasDia, previos: false }, { lista: anteriores, previos: true }];
  for (const { lista, previos } of lotes) for (const c of lista ?? []) {
    const facturaKore = c.serieFactura ? `${_serie(c.serieFactura)}-${String(c.folioFactura ?? '').trim()}` : null;
    const remision = c.serieVenta && c.folioVenta ? `${_serie(c.serieVenta)}-${String(c.folioVenta).trim()}` : null;
    // Remisión de otra sucursal cobrada en esta caja (2026-11-06).
    const deOtraSucursal = !!c.serieVenta && !propias.has(_serie(c.serieVenta));
    for (const k of c.cobros ?? []) {
      if (_serie(k.claveCentro) !== serie || !k.fecha) continue;
      const diaCobro = _diaMx(k.fecha);
      if (previos ? !(diaCobro < dia) : diaCobro !== dia) continue;
      // Global de un día posterior al cobro: ese día el cobro estaba sin factura.
      // (Otras sucursales suman siempre: se quedan con su factura.)
      const posterior = !previos && !deOtraSucursal && facturaKore && diaGlobal.get(facturaKore) > dia ? facturaKore : null;
      // Los de días anteriores se evalúan como estaban ese día (sin factura); al sumarlos, facturados.
      const facturada = !previos && !!facturaKore && !posterior;
      const factura = facturada ? facturaKore : null;
      const extra = previos ? { cobradoEl: diaCobro }
        : posterior ? { facturaPosterior: posterior, facturaPosteriorDia: diaGlobal.get(posterior) } : {};
      const origen = _serie(k.serieOrigen);
      if (origen === 'APA') continue; // aplicación de saldo a favor: no es dinero nuevo
      const fps = k.formasPago ?? [];
      // Kore repite en cada ticket el total de un pago que cerró varios: con
      // una forma se usa |monto| del cobro; con varias que lo exceden, se
      // escalan (salvo saldo a favor, donde el monto ya es solo esa parte).
      const montoReal = k.monto != null ? Math.abs(Number(k.monto) || 0) : null;
      const suma = fps.reduce((s, f) => s + (Number(f.monto) || 0), 0);
      const conSaldoFavor = fps.some(f => _forma(f) === 'saldoFavor');
      const factor = (!conSaldoFavor && fps.length > 1 && montoReal != null && suma > montoReal + 0.05) ? montoReal / suma : 1;
      const cobro = `${origen}-${String(k.folioOrigen ?? '').trim()}`;
      for (const fp of fps) {
        const forma = _forma(fp);
        if (!forma) continue;
        const monto = (fps.length === 1 && montoReal != null) ? montoReal : (Number(fp.monto) || 0) * factor;
        // Cobros de días anteriores (global de hoy): solo el efectivo que ese día quedó sin
        // factura; otras sucursales y misceláneas ya sumaron en su día.
        if (previos && (forma !== 'efectivo' || deOtraSucursal || origen === 'MIS')) continue;
        let concepto = null;
        if (forma !== 'efectivo') concepto = forma;
        else if (deOtraSucursal) concepto = facturada ? 'cobroOtraSucursal' : 'cobroOtraSucursalSinFactura';
        else if (origen === 'MIS') concepto = 'miscelaneaEfectivo';
        else if (ORIGEN_CONTADO.has(origen)) concepto = (facturada || previos) ? 'efectivoFacturado' : 'efectivoPorFacturar';
        const mov = { dia, serie, concepto, remision, factura: previos ? facturaKore : factura, cobro, fecha: k.fecha, importe: _r2(monto), _monto: monto, ...extra,
          fechaCreacion: c.fechaCreacion ?? k.fecha };
        if (concepto) _sumar(montos, detalle, mov);
        else pendientes.push({ ...mov, facturada, previo: previos, serieVenta: c.serieVenta, folioVenta: c.folioVenta });
      }
    }
  }

  // 2) Tipo de venta (consignación / PPD / contado) y saldo después del cobro (2026-11-25).
  const avisos = [];
  if (pendientes.length) {
    const remisiones = pendientes.filter(p => p.remision)
      .map(p => ({ serie: _serie(p.serieVenta), folio: String(p.folioVenta).trim(), fechaCreacion: p.fechaCreacion }));
    const { info } = await infoRemisiones({ rfc, remisiones });
    // Saldo fresco: las de contado (abono o no) y la consignación SIN factura (2026-11-26: el
    // anexo la separa en "con saldo pendiente" y "saldo 0"). Consignación facturada y PPD
    // facturada no dependen del saldo.
    const conSaldo = new Set(pendientes.filter(p => {
      const i = info.get(_claveRem(p.remision));
      if (!i?.nombreTipoMovimiento) return false;
      if (_esConsignacion(i)) return !p.facturada;
      return !(p.facturada && _esPpd(i));
    }).map(p => _claveRem(p.remision)));
    if (conSaldo.size) {
      const { info: frescas } = await infoRemisiones({
        rfc, remisiones: remisiones.filter(r => conSaldo.has(`${r.serie}|${r.folio}`)), necesitaSaldo: () => true,
      });
      for (const [k, v] of frescas) info.set(k, v);
    }
    let sinTipo = 0;
    for (const p of pendientes) {
      const i = p.remision ? info.get(_claveRem(p.remision)) : null;
      if (!i?.nombreTipoMovimiento) sinTipo++;
      if (p.previo) {
        // Cobro de un día anterior que ampara la global de hoy: entra solo si ese día quedó
        // sin factura (en una venta de contado que dejó saldo ya sumó como abono).
        if (_clasificar({ ...p, facturada: false, info: i }).concepto !== 'efectivoPorFacturar') continue;
        _sumar(montos, detalle, { ...p, ..._clasificar({ ...p, facturada: true, info: i }) });
        continue;
      }
      _sumar(montos, detalle, { ...p, ..._clasificar({ ...p, info: i }) });
    }
    if (sinTipo) avisos.push(`Kore no dio el tipo de venta de ${sinTipo} cobro(s) de la caja ${serie} del ${dia}; vuelve a generar la ficha.`);
  }

  // 3) Lo facturado de cada cobro amparado por una global (2026-11-26, pedido del usuario:
  // "solo se debe sumar sin las BON porque eso se deposita").
  const globales = new Set([...diaGlobal.keys(), ...globalesHoy.map(g => `${g.serie}-${g.folio}`)]);
  await _aplicarFacturado({ rfc, detalle, montos, globales, avisos, caja: `${serie} del ${dia}` });

  // 4) Cobros de otras sucursales hechos en esta caja SIN factura en días anteriores (fichas
  // guardadas) que se facturaron HOY: suman hoy, por lo facturado (2026-11-26).
  for (const d of await _otrasSucursalesFacturadasHoy({ rfc, serie, dia, asegurarRecientes })) {
    detalle.push(d);
    montos.cobroOtraSucursal += d.facturado;
    montos.bonificacionGlobal += d.importe - d.facturado;
  }

  for (const s of salidas ?? []) {
    if (!s.fecha || _diaMx(s.fecha) !== dia) continue;
    const tipo = _serie(s.tipoMovimiento?.abreviatura);
    const concepto = tipo === 'RETD' ? 'devoluciones'
      : (tipo === 'RETEF' || /^RETIRO DE EFECTIVO$/i.test(String(s.tipoMovimiento?.nombre ?? '').trim())) ? 'retiroEfectivo'
      : tipo === 'RPF' ? 'faltante' : null;
    if (!concepto) continue;
    const monto = Number(s.montoRetirado) || 0;
    montos[concepto] += monto;
    detalle.push({
      dia, serie, concepto, remision: null, factura: null,
      cobro: s.serieAfectacion ? `${_serie(s.serieAfectacion)}-${String(s.folioAfectacion ?? '').trim()}` : tipo,
      origen: s.serieOrigen ? `${_serie(s.serieOrigen)}-${String(s.folioOrigen ?? '').trim()}` : null,
      fecha: s.fecha, importe: _r2(-monto), usuario: s.nombreUsuario ?? null,
    });
  }
  for (const k of CONCEPTOS) montos[k] = _r2(montos[k]);
  return { montos, detalle, avisos };
}

const _esBn = m => /^BN-/i.test(String(m?.origen ?? '').trim());

/**
 * Parte de un pago que se llevó la BN de la global (2026-11-26). Kore, al timbrar la
 * global, aplica a la remisión una bonificación (origen BN-…, p. ej. BN-088622 $13,000 =
 * 20% de VIRIDIANA A0-261002349) y la factura sale por lo cobrado menos esa BN. Cada BN se
 * reparte entre los pagos (con forma de pago) hechos desde la BN anterior, en proporción.
 * @returns {number|null} BN que corresponde al pago `cobro`; null si Kore no trae el movimiento.
 */
function _bnDelCobro(abonos, cobro) {
  const movs = Array.isArray(abonos) ? abonos : [];
  if (!movs.some(m => _serie(m.movimiento) === _serie(cobro))) return null;
  let pagos = [];
  let bn = 0;
  for (const m of movs) {
    if (_esBn(m)) {
      const base = pagos.reduce((s, p) => s + p.importe, 0);
      const mio = pagos.find(p => _serie(p.movimiento) === _serie(cobro));
      if (mio && base > 0) bn += (Number(m.importe) || 0) * mio.importe / base;
      pagos = [];
    } else if (m.forma && Number(m.importe) > 0) {
      pagos.push({ movimiento: m.movimiento, importe: Number(m.importe) });
    } else if (m.forma && Number(m.importe) < 0) {
      // Cobro cancelado (RAB, p. ej. GLADIS GOMEZ A0-261101623: CPF de $18,853.88 revertido):
      // ya no cuenta para repartir la BN.
      const i = pagos.findIndex(p => Math.abs(p.importe + Number(m.importe)) < 0.005);
      if (i >= 0) pagos.splice(i, 1);
    }
  }
  const pago = movs.find(m => _serie(m.movimiento) === _serie(cobro));
  return pago && Number(pago.importe) > 0 ? bn / Number(pago.importe) : 0;
}

/**
 * `facturado` de cada renglón que suma: lo cobrado menos la BN de la global (solo cobros
 * amparados por una GLOBAL; lo demás se factura por lo cobrado). Ajusta los montos de cada
 * renglón y acumula la diferencia en `bonificacionGlobal` (informativo).
 */
async function _aplicarFacturado({ rfc, detalle, montos, globales, avisos, caja }) {
  const filas = detalle.filter(d => ENTRADAS.includes(d.concepto));
  for (const d of filas) d.facturado = d.importe;
  // Incluye los cobros de otras sucursales (2026-11-26, pedido del usuario: "debería sumar por lo facturado").
  const conGlobal = filas.filter(d => d.remision && d.factura && globales.has(d.factura));
  if (!conGlobal.length) return;
  const remisiones = conGlobal.map(d => { const i = d.remision.indexOf('-'); return { serie: d.remision.slice(0, i), folio: d.remision.slice(i + 1), fechaCreacion: d._fechaCreacion }; });
  // Movimientos frescos salvo que la memoria ya traiga la BN (se aplica una vez, al timbrar).
  const { info } = await infoRemisiones({ rfc, remisiones, necesitaSaldo: (k, g) => !(g?.abonos ?? []).some(_esBn) });
  let sinMovimientos = 0;
  for (const d of conGlobal) {
    const proporcion = _bnDelCobro(info.get(_claveRem(d.remision))?.abonos, d.cobro);
    if (proporcion == null) { sinMovimientos++; continue; }
    const facturado = _r2(d.importe * (1 - proporcion));
    montos[d.concepto] += facturado - d.importe;
    montos.bonificacionGlobal += d.importe - facturado;
    d.facturado = facturado;
  }
  if (sinMovimientos) avisos.push(`Kore no dio los movimientos de ${sinMovimientos} cobro(s) de la caja ${caja}: van con lo cobrado como facturado; vuelve a generar la ficha.`);
}

/** Día (México) de cada factura vigente de la lista (cualquier receptor): Map 'SERIE-FOLIO' → día. */
async function _diaDeFacturas(rfc, facturas) {
  const pares = [...new Set(facturas)].map(f => { const i = f.indexOf('-'); return { serie: f.slice(0, i), folio: f.slice(i + 1) }; });
  const dias = new Map();
  if (!pares.length) return dias;
  const docs = await CFDI.find({ 'emisor.rfc': rfc, tipoDeComprobante: 'I', $or: pares })
    .select('uuid serie folio fecha receptor satStatus erpStatus source').lean();
  for (const [, c] of _vigentesPorUuid(docs ?? [])) if (c.fecha) dias.set(`${_serie(c.serie)}-${String(c.folio ?? '').trim()}`, _diaMx(c.fecha));
  return dias;
}

/**
 * Cobros de remisiones de OTRAS sucursales hechos en la caja `serie` sin factura (renglón
 * cobroOtraSucursalSinFactura de fichas guardadas vigentes, últimos 45 días) cuya factura es
 * de `dia`: suman en la ficha de hoy por lo facturado (cobrado − BN), con el día del cobro.
 * La ficha de la otra sucursal nunca los trae: el dinero entró a esta caja.
 */
async function _otrasSucursalesFacturadasHoy({ rfc, serie, dia, asegurarRecientes }) {
  const FichaEfectivo = require('./FichaEfectivo.model');
  const desde = new Date(new Date(`${dia}T00:00:00Z`).getTime() - 45 * 86400000).toISOString().slice(0, 10);
  const docs = await FichaEfectivo.find({ rfc, estado: 'vigente', 'detalle.concepto': 'cobroOtraSucursalSinFactura' }).select('detalle').lean();
  const pendientes = new Map();
  for (const f of docs ?? []) for (const x of f.detalle ?? []) {
    if (x.concepto === 'cobroOtraSucursalSinFactura' && x.serie === serie && x.dia < dia && x.dia >= desde && x.remision && x.cobro) pendientes.set(`${x.remision}|${x.cobro}`, x);
  }
  const salida = [];
  for (const x of pendientes.values()) {
    const i = x.remision.indexOf('-');
    const c = await obtenerCuentaTicket({ rfc, serie: x.remision.slice(0, i), folio: x.remision.slice(i + 1), fechaCreacion: x.remisionCreada ?? x.fecha }).catch(() => null);
    if (!c?.factura) continue;
    const factura = _serie(c.factura);
    let dias = await _diaDeFacturas(rfc, [factura]);
    if (!dias.has(factura) && dia >= _hoyAyer().ayer && await asegurarRecientes()) dias = await _diaDeFacturas(rfc, [factura]);
    if (dias.get(factura) !== dia) continue;
    const proporcion = _bnDelCobro(c.abonos, x.cobro) ?? 0;
    const importe = _r2(x.importe);
    salida.push({
      dia, serie, concepto: 'cobroOtraSucursal', remision: x.remision, factura, cobro: x.cobro, fecha: x.fecha,
      importe, facturado: _r2(importe * (1 - proporcion)), cobradoEl: x.dia,
    });
  }
  return salida;
}

/**
 * Pago contra entrega (CCE) sin factura ligada en Kore (2026-11-26, pedido del usuario): Kore
 * timbra la factura en el momento del cobro pero no la liga a la remisión (Atzompa
 * E0-261102072, ABRAHAM PACHECO, CCE-261100130 $2,033.99 → E0-261100257, UUID 4B006F47…).
 * Se toma la factura vigente de la serie de la remisión con el mismo total timbrada junto al
 * cobro — solo si hay exactamente una (Kore guarda la fecha de la factura en hora local con
 * "Z": 6 h antes que la del cobro, por eso la ventana). Regresa copias de las cuentas ligadas.
 */
async function _ligarContraEntrega(rfc, cuentas, serie, dia) {
  const lista = cuentas ?? [];
  const usadas = new Set();
  const ligadas = new Map();
  for (const c of lista) {
    if (c.serieFactura || !c.serieVenta) continue;
    const k = (c.cobros ?? []).find(x => _serie(x.serieOrigen) === 'CCE' && _serie(x.claveCentro) === serie && x.fecha && _diaMx(x.fecha) === dia);
    if (!k) continue;
    const monto = Math.abs(Number(k.monto) || 0);
    const t = new Date(k.fecha).getTime();
    const docs = await CFDI.find({
      'emisor.rfc': rfc, tipoDeComprobante: 'I', serie: _serie(c.serieVenta),
      total: { $gte: monto - 0.01, $lte: monto + 0.01 },
      fecha: { $gte: new Date(t - 7 * 3600000), $lte: new Date(t + 3600000) },
    }).select('uuid serie folio fecha total receptor satStatus erpStatus source').lean();
    const vigentes = _vigentesPorUuid(docs ?? []).filter(([u]) => !usadas.has(u));
    if (vigentes.length !== 1) continue;
    const [uuid, f] = vigentes[0];
    usadas.add(uuid);
    ligadas.set(c, { ...c, serieFactura: _serie(f.serie), folioFactura: String(f.folio ?? '').trim() });
  }
  return lista.map(c => ligadas.get(c) ?? c);
}

const _hoyAyer = (ahora = new Date()) => {
  const hoy = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Mexico_City' }).format(ahora);
  return { hoy, ayer: new Date(new Date(`${hoy}T00:00:00Z`).getTime() - 86400000).toISOString().slice(0, 10) };
};

/**
 * Día (México) de las facturas GLOBALES de la lista: `dias` Map 'SERIE-FOLIO' → 'YYYY-MM-DD';
 * `faltan` = alguna factura (global o no) que todavía no está en NUMO.
 */
async function _diaDeGlobales(rfc, facturas) {
  const unicas = [...new Set(facturas)];
  const dias = new Map();
  if (!unicas.length) return { dias, faltan: false };
  const pares = unicas.map(f => { const i = f.indexOf('-'); return { serie: f.slice(0, i), folio: f.slice(i + 1) }; });
  const docs = await CFDI.find({ 'emisor.rfc': rfc, tipoDeComprobante: 'I', $or: pares })
    .select('uuid serie folio fecha receptor satStatus erpStatus source').lean();
  // Conocida = está en NUMO aunque esté cancelada (no hace falta bajarla de Kore).
  const conocidas = new Set((docs ?? []).map(c => `${_serie(c.serie)}-${String(c.folio ?? '').trim()}`));
  for (const [, c] of _vigentesPorUuid(docs ?? [])) {
    const clave = `${_serie(c.serie)}-${String(c.folio ?? '').trim()}`;
    if (c.fecha && _serie(c.receptor?.rfc) === RFC_PUBLICO_GENERAL) dias.set(clave, _diaMx(c.fecha));
  }
  return { dias, faltan: unicas.some(f => !conocidas.has(f)) };
}

/** Facturas globales vigentes de una serie timbradas ese día (México). */
async function _globalesDelDia(rfc, serie, dia) {
  const docs = await CFDI.find({
    'emisor.rfc': rfc, tipoDeComprobante: 'I', 'receptor.rfc': RFC_PUBLICO_GENERAL, serie,
    fecha: { $gte: new Date(`${dia}T00:00:00.000-06:00`), $lte: new Date(`${dia}T23:59:59.999-06:00`) },
  }).select('uuid serie folio fecha receptor satStatus erpStatus source').lean();
  return _vigentesPorUuid(docs ?? [])
    .filter(([, c]) => c.fecha && _diaMx(c.fecha) === dia && _serie(c.serie) === _serie(serie) && _serie(c.receptor?.rfc) === RFC_PUBLICO_GENERAL)
    .map(([, c]) => ({ serie: _serie(c.serie), folio: String(c.folio ?? '').trim() }));
}

const _claveRem = remision => String(remision ?? '').replace('-', '|');
const _esConsignacion = i => /CONSIGNACI/i.test(String(i?.nombreTipoMovimiento ?? ''));
const _esPpd = i => String(i?.tipoPago ?? '').toUpperCase().includes('PPD');

function _sumar(montos, detalle, mov) {
  montos[mov.concepto] += mov._monto;
  if (!ENTRADAS.includes(mov.concepto) && !SIN_FACTURA.includes(mov.concepto)) return;
  const d = { dia: mov.dia, serie: mov.serie, concepto: mov.concepto, remision: mov.remision, factura: mov.factura, cobro: mov.cobro, fecha: mov.fecha, importe: mov.importe };
  if (mov.tipoVenta) d.tipoVenta = mov.tipoVenta;
  if (mov.saldoDespues != null) d.saldoDespues = mov.saldoDespues;
  if (mov.cobradoEl) d.cobradoEl = mov.cobradoEl;
  if (mov.fechaCreacion) Object.defineProperty(d, '_fechaCreacion', { value: mov.fechaCreacion, enumerable: false });
  if (mov.concepto === 'cobroOtraSucursalSinFactura' && mov.fechaCreacion) d.remisionCreada = mov.fechaCreacion;
  if (mov.facturaPosterior) { d.facturaPosterior = mov.facturaPosterior; d.facturaPosteriorDia = mov.facturaPosteriorDia; }
  detalle.push(d);
}

/**
 * Saldo de la cuenta después del cobro (2026-11-25): el del movimiento del
 * cobro y, si Kore aplicó justo después un ajuste sin dinero (bonificación
 * BON/BN), el que dejó ese ajuste. Sin el movimiento, el saldo actual.
 */
function _saldoDespuesDe(info, cobro) {
  const movs = Array.isArray(info?.abonos) ? info.abonos : [];
  const i = movs.findIndex(m => _serie(m.movimiento) === _serie(cobro));
  if (i < 0) return info?.saldoActual != null ? _r2(info.saldoActual) : null;
  let saldo = movs[i].saldo;
  const t0 = new Date(movs[i].fecha).getTime();
  for (let j = i + 1; j < movs.length; j++) {
    const m = movs[j];
    if (m.forma || !(new Date(m.fecha).getTime() - t0 <= MS_AJUSTE_MISMO_COBRO)) break;
    saldo = m.saldo;
  }
  return _r2(saldo);
}

/**
 * Renglón de un cobro en efectivo según el tipo de venta (2026-11-25, pedido del usuario):
 * consignación → facturada o sin factura (anexo, no suma); PPD facturada → cobranza
 * (complemento); lo demás → abono si deja saldo pendiente, si no efectivo facturado / sin factura.
 * Sin datos de Kore: como antes (abono si es ABO, cobranza si está facturada).
 */
function _clasificar({ facturada, cobro, info }) {
  if (!info?.nombreTipoMovimiento) {
    const origen = String(cobro ?? '').split('-')[0];
    return { concepto: origen === 'ABO' ? 'abonosEfectivo' : facturada ? 'cobranzaEfectivo' : 'efectivoPorFacturar', tipoVenta: null, saldoDespues: null };
  }
  const tipoVenta = `${String(info.tipoPago ?? '').trim()} ${String(info.nombreTipoMovimiento).trim()}`.trim();
  if (_esConsignacion(info)) {
    if (facturada) return { concepto: 'consignacionFacturada', tipoVenta, saldoDespues: null };
    // Sin factura: el saldo después del cobro separa el anexo en "con saldo" y "saldo 0" (2026-11-26).
    return { concepto: 'efectivoPorFacturar', tipoVenta, saldoDespues: _saldoDespuesDe(info, cobro) };
  }
  if (facturada && _esPpd(info)) return { concepto: 'cobranzaEfectivo', tipoVenta, saldoDespues: null };
  const saldoDespues = _saldoDespuesDe(info, cobro);
  if (saldoDespues != null && saldoDespues >= 0.01) return { concepto: 'abonosEfectivo', tipoVenta, saldoDespues };
  return { concepto: facturada ? 'efectivoFacturado' : 'efectivoPorFacturar', tipoVenta, saldoDespues };
}

const _total = m => _r2(ENTRADAS.reduce((s, k) => s + m[k], 0) - SALIDAS.reduce((s, k) => s + m[k], 0));

/**
 * Cliente y si la factura es GLOBAL para cada renglón del detalle: de la
 * factura (CFDI en NUMO) y, sin factura, de la memoria kore_remisiones (sin
 * preguntar a Kore: con miles de remisiones daría 429).
 */
async function _completarDetalle({ rfc, detalle }) {
  const facturas = [...new Set(detalle.map(d => d.factura).filter(Boolean))];
  const porFactura = new Map();
  if (facturas.length) {
    const pares = facturas.map(f => { const i = f.indexOf('-'); return { serie: f.slice(0, i), folio: f.slice(i + 1) }; });
    const docs = await CFDI.find({ 'emisor.rfc': rfc, tipoDeComprobante: 'I', $or: pares })
      .select('uuid serie folio receptor satStatus erpStatus source').lean();
    for (const [uuid, c] of _vigentesPorUuid(docs)) {
      porFactura.set(`${_serie(c.serie)}-${String(c.folio ?? '').trim()}`, {
        uuid, esGlobal: _serie(c.receptor?.rfc) === RFC_PUBLICO_GENERAL, cliente: c.receptor?.nombre ?? null,
      });
    }
  }
  const remisiones = [...new Set(detalle.filter(d => d.remision && (!d.factura || porFactura.get(d.factura)?.esGlobal !== false)).map(d => d.remision))];
  const clienteDe = new Map();
  if (remisiones.length) {
    try {
      const KoreRemision = require('../erp/KoreRemision.model');
      const pares = remisiones.map(r => { const i = r.indexOf('-'); return { serie: r.slice(0, i), folio: r.slice(i + 1) }; });
      for (let i = 0; i < pares.length; i += 500) {
        const docs = await KoreRemision.find({ rfc, $or: pares.slice(i, i + 500) }).select('serie folio nombre').lean();
        for (const g of docs) clienteDe.set(`${_serie(g.serie)}-${String(g.folio).trim()}`, g.nombre ?? null);
      }
    } catch { /* sin la memoria el cliente queda vacío */ }
  }
  for (const d of detalle) {
    const f = d.factura ? porFactura.get(d.factura) : null;
    d.facturaUuid = f?.uuid ?? null;
    d.esGlobal = f ? f.esGlobal : null;
    d.cliente = (f && !f.esGlobal ? f.cliente : null) ?? clienteDe.get(d.remision) ?? (f?.esGlobal ? 'PÚBLICO EN GENERAL' : null);
  }
  return detalle;
}

/**
 * Ficha de una sucursal en un rango de fechas (2026-11-05: el usuario ya no
 * quiere elegir globales — solo sucursal, rango, banco y denominaciones): el
 * efectivo de la caja de cada serie de la sucursal en cada día del rango, y la
 * suma, con el detalle de cada venta / cobro / salida (rastro venta → factura
 * o global). Los días sin ningún movimiento de efectivo no salen. Las cajas se
 * consultan una por una para no provocar 429 en Kore. `yaEnFicha`: cajas
 * (serie + día) que ya están en una ficha guardada vigente.
 */
async function calcularFicha({ rfc, centroCostoId, fechaInicio, fechaFin }) {
  fechaFin = fechaFin || fechaInicio;
  if (!rfc) throw new BadRequestError('RFC requerido');
  if (centroCostoId == null || centroCostoId === '') throw new BadRequestError('Elige la sucursal');
  if (!FECHA_RE.test(fechaInicio ?? '') || !FECHA_RE.test(fechaFin ?? '')) throw new BadRequestError('Fechas inválidas (formato YYYY-MM-DD)');
  if (fechaFin < fechaInicio) throw new BadRequestError('La fecha final no puede ser anterior a la inicial');
  if (_dias(fechaInicio, fechaFin) > MAX_DIAS_FICHA) throw new BadRequestError(`El rango no puede pasar de ${MAX_DIAS_FICHA} días`);

  const ccBySerieMap = await centrosSvc.resolveBySerieMap();
  const entradas = Object.entries(ccBySerieMap).filter(([, c]) => String(c?.id) === String(centroCostoId));
  if (!entradas.length) throw new BadRequestError('La sucursal no tiene serie de facturación configurada');
  const series = [...new Set(entradas.map(([s]) => _serie(s)))].sort();
  const sucursal = entradas[0][1]?.sucursal ?? null;

  const dias = [];
  for (let d = fechaInicio; d <= fechaFin; d = new Date(new Date(`${d}T00:00:00Z`).getTime() + 86400000).toISOString().slice(0, 10)) dias.push(d);

  const advertencias = [];
  // Facturas de hoy / ayer que la descarga nocturna aún no trae (2026-11-26): una sola vez por
  // ficha y solo si hace falta (bajarlas tarda ~20 s). true = se trajeron sin error.
  let traidas = null;
  const asegurarRecientes = async () => {
    if (!traidas) {
      const { hoy, ayer } = _hoyAyer();
      traidas = traerFacturasRecientes({ series, fechaInicio: ayer, fechaFin: hoy }).then(r => {
        if (r?.error) advertencias.push(`No se pudieron traer de Kore las facturas del ${r.dias.join(' y ')} (${r.error}); pueden faltar globales de esos días.`);
        return !r?.error;
      });
    }
    return traidas;
  };
  const fallidas = [];
  const filas = [];
  let detalle = [];
  for (const dia of dias) {
    for (const serie of series) {
      try {
        const { montos, detalle: det, avisos = [] } = await _corteDeCaja({ rfc, serie, dia, seriesSucursal: series, asegurarRecientes });
        if (avisos.length) { fallidas.push(`${serie}|${dia}`); advertencias.push(...avisos); }
        if (!CONCEPTOS.some(k => montos[k])) continue; // día sin movimiento de efectivo
        filas.push({ serie, dia, sucursal, ...montos, totalEntregar: _total(montos) });
        detalle = detalle.concat(det);
      } catch (err) {
        fallidas.push(`${serie}|${dia}`);
        advertencias.push(`Kore no respondió la caja ${serie} del ${dia} (${err.message}); no se incluyó en la ficha — vuelve a generarla más tarde.`);
      }
    }
  }
  await _completarDetalle({ rfc, detalle });
  // Lo sin factura (no suma) va al final del anexo, con su desglose (2026-11-25).
  const _alFinal = d => (SIN_FACTURA.includes(d.concepto) ? 1 : 0);
  detalle.sort((a, b) => _alFinal(a) - _alFinal(b) || a.dia.localeCompare(b.dia) || a.serie.localeCompare(b.serie)
    || CONCEPTOS.indexOf(a.concepto) - CONCEPTOS.indexOf(b.concepto) || String(a.fecha).localeCompare(String(b.fecha)));

  const totales = Object.fromEntries([...CONCEPTOS, 'totalEntregar'].map(k => [k, _r2(filas.reduce((s, f) => s + f[k], 0))]));
  const guardadas = require('./reportes-por-facturar.fichas-guardadas');
  const yaEnFicha = await guardadas.cajasYaGuardadas({ rfc, cajas: filas.map(f => `${f.serie}|${f.dia}`) });
  return {
    generadoEn: new Date(),
    centroCostoId: Number(centroCostoId),
    sucursal,
    series,
    fechaInicio,
    fechaFin,
    cortes: filas,
    totales,
    detalle,
    yaEnFicha,
    incompleta: fallidas.length > 0,
    advertencias: advertencias.length ? advertencias : null,
  };
}

/**
 * Anexo › Actualizar (2026-11-06, pedido del usuario): ¿las ventas que estaban
 * sin factura (consignación, miscelánea…) ya se facturaron? Vuelve a pedir a
 * Kore, SIN caché, el corte de las cajas (serie + día) que tienen renglones
 * sin factura y les pone la factura que tengan ahora (`facturadaDespues`).
 * Los importes y conceptos NO cambian: la ficha ya se entregó así. Muta y
 * regresa `detalle`. Las cajas se consultan una por una (429 de Kore).
 */
async function actualizarFacturasDetalle({ rfc, detalle }) {
  if (!rfc) throw new BadRequestError('RFC requerido');
  const lista = Array.isArray(detalle) ? detalle : [];
  const sinFactura = lista.filter(d => d?.remision && !d.factura && (ENTRADAS.includes(d.concepto) || SIN_FACTURA.includes(d.concepto)));
  const cajas = [...new Set(sinFactura.map(d => `${_serie(d.serie)}|${d.dia}`))]
    .filter(c => FECHA_RE.test(c.split('|')[1] ?? ''));
  const facturaDe = new Map(); // remisión → factura actual
  const advertencias = [];
  for (const caja of cajas) {
    const [serie, dia] = caja.split('|');
    try {
      const cuentas = await obtenerDesglosesCobroAlmacenPorCentro({
        rfc, centro: serie, sinCache: true,
        fechaDesde: new Date(`${dia}T00:00:00.000-06:00`).toISOString(),
        fechaHasta: new Date(`${dia}T23:59:59.999-06:00`).toISOString(),
      });
      for (const c of cuentas ?? []) {
        if (!c.serieFactura || !c.serieVenta || !c.folioVenta) continue;
        facturaDe.set(`${_serie(c.serieVenta)}-${String(c.folioVenta).trim()}`, `${_serie(c.serieFactura)}-${String(c.folioFactura ?? '').trim()}`);
      }
    } catch (err) {
      advertencias.push(`Kore no respondió la caja ${serie} del ${dia} (${err.message}); vuelve a intentarlo.`);
    }
  }
  let nuevas = 0;
  for (const d of sinFactura) {
    const f = facturaDe.get(d.remision);
    if (f) { d.factura = f; d.facturadaDespues = true; nuevas++; }
  }
  if (nuevas) await _completarDetalle({ rfc, detalle: lista });
  return {
    detalle: lista,
    nuevas,
    siguenSinFactura: sinFactura.filter(d => !d.factura).length,
    actualizadoEn: new Date(),
    advertencias: advertencias.length ? advertencias : null,
  };
}

/**
 * Bancos dados de alta en Bancos (los que tienen movimientos o configuración),
 * con su número de cuenta para la ficha.
 */
async function listarBancos() {
  const BankMovement = require('../banks/BankMovement.model');
  const bankConfigRepo = require('../banks/repositories/bank-config.repository');
  const [nombres, configs] = await Promise.all([
    BankMovement.distinct('banco'),
    bankConfigRepo.findAllAsMap().catch(() => new Map()),
  ]);
  const todos = [...new Set([...nombres, ...configs.keys()].filter(Boolean))].sort((a, b) => a.localeCompare(b));
  return todos.map(banco => ({ banco, numeroCuenta: configs.get(banco)?.numeroCuenta ?? null }));
}

module.exports = { listarGlobales, calcularFicha, listarBancos, actualizarFacturasDetalle, _corteDeCaja, _clasificar, ENTRADAS, SALIDAS, SIN_FACTURA, CONCEPTOS };
