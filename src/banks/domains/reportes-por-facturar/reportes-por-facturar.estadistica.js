'use strict';

/**
 * reportes-por-facturar.estadistica.js
 * ─────────────────────────────────────────────────────────────────────────────
 * "Estadística por facturar" (Reportes, 2026-10-29, pedido del usuario: "cuántos
 * había y ahora cuántos hay, los que se facturaron y los que faltan").
 *
 * Se recalcula por periodo (no se usan los reportes guardados: ↻ Actualizar
 * los sobrescribe). Universo ("había") = depósitos de Bancos que pagan
 * remisiones de la sucursal que Kore tiene como PPD "POR FACTURAR" (Kore no
 * cambia ese estatus al facturar), que entraron en el rango — por fecha del
 * depósito o porque se identificaron/vincularon dentro del rango (mismo
 * criterio que el reporte "Por facturar"). Hoy, cada uno está:
 *   • facturado: todas sus remisiones ya tienen factura en Kore (global o a
 *     nombre del cliente); fecha = la de la factura más reciente (CFDI);
 *   • falta: alguna sin factura (con o sin saldo pendiente).
 * Con la fecha de entrada y la de factura se arma la evolución día por día.
 *
 * Consulta Kore remisión por remisión (puede tardar minutos) → corre como
 * trabajo en segundo plano; la pantalla consulta su avance.
 */

const crypto       = require('crypto');
const ExcelJS      = require('exceljs');
const BankMovement = require('../banks/BankMovement.model');
const CFDI         = require('../../../visor/models/CFDI');
const centrosSvc   = require('../centros-costo/centros-costo.service');
const { obtenerDesglosesCobroAlmacen } = require('../erp/erp-sync.service');
const { infoRemisiones } = require('../erp/kore-remisiones.service');
const { _identificadoEl } = require('../cfdi-mapping/mostrador.service');
const { BadRequestError, NotFoundError } = require('../../shared/errors/AppError');
const Foto = require('./EstadisticaPorFacturarFoto.model');
const { logger } = require('../../../shared/utils/logger');

const MAX_DIAS_RANGO = 31;
const FECHA_RE = /^\d{4}-\d{2}-\d{2}$/;
const ESTATUS_POR_FACTURAR = 'POR FACTURAR';
const RFC_PUBLICO_GENERAL = 'XAXX010101000';
const LOTE_DESGLOSES = 50;

const _r2 = n => Math.round((Number(n) || 0) * 100) / 100;
const _diaMx = f => (f ? new Date(new Date(f).getTime() - 6 * 3600000).toISOString().slice(0, 10) : null);
// `fecha` de Bancos = día del estado de cuenta a medianoche UTC ("ingenua").
const _diaDeposito = f => (f ? new Date(f).toISOString().slice(0, 10) : null);
const _dias = (desde, hasta) => Math.max(0, Math.round((new Date(`${hasta}T00:00:00Z`) - new Date(`${desde}T00:00:00Z`)) / 86400000));

function _diasDelRango(fechaInicio, fechaFin) {
  const dias = [];
  for (let d = new Date(`${fechaInicio}T00:00:00Z`); d <= new Date(`${fechaFin}T00:00:00Z`); d = new Date(d.getTime() + 86400000)) {
    dias.push(d.toISOString().slice(0, 10));
  }
  return dias;
}

// Fecha y tipo (global / cliente) de cada factura `serie-folio` según el CFDI.
async function _facturasCfdi(claves) {
  const pares = [...claves].map(k => { const i = k.indexOf('-'); return { serie: k.slice(0, i), folio: k.slice(i + 1) }; });
  const porClave = new Map();
  for (let i = 0; i < pares.length; i += 200) {
    const docs = await CFDI.find({ tipoDeComprobante: 'I', $or: pares.slice(i, i + 200) })
      .select('serie folio fecha receptor satStatus source').lean();
    for (const c of docs) {
      const k = `${String(c.serie ?? '').trim().toUpperCase()}-${String(c.folio ?? '').trim()}`;
      // La copia ERP manda (trae la fecha real); una cancelada no gana a una vigente.
      const prev = porClave.get(k);
      if (prev && prev.satStatus !== 'Cancelado' && (c.satStatus === 'Cancelado' || prev.source === 'ERP')) continue;
      porClave.set(k, c);
    }
  }
  return porClave;
}

/**
 * Calcula la estadística (sin guardar).
 * @param {(avance: string) => void} [onAvance]
 */
async function calcular({ rfc, centroCostoId, fechaInicio, fechaFin }, onAvance = () => {}) {
  fechaFin = fechaFin || fechaInicio;
  if (!rfc) throw new BadRequestError('RFC requerido');
  if (!FECHA_RE.test(fechaInicio ?? '') || !FECHA_RE.test(fechaFin ?? '')) throw new BadRequestError('Fechas inválidas (formato YYYY-MM-DD)');
  if (fechaFin < fechaInicio) throw new BadRequestError('La fecha final no puede ser anterior a la inicial');
  if (_diasDelRango(fechaInicio, fechaFin).length > MAX_DIAS_RANGO) throw new BadRequestError(`El rango no puede pasar de ${MAX_DIAS_RANGO} días`);

  const ccBySerieMap = await centrosSvc.resolveBySerieMap();
  const sucursalDe = new Map(Object.entries(ccBySerieMap).map(([s, c]) => [s.trim().toUpperCase(), c?.sucursal ?? null]));
  const seriesSet = new Set(Object.entries(ccBySerieMap)
    .filter(([, c]) => !centroCostoId || String(c.id) === String(centroCostoId))
    .map(([serie]) => serie.trim().toUpperCase()));
  if (!seriesSet.size) throw new BadRequestError('La sucursal no tiene serie de facturación configurada');
  const sucursal = centroCostoId ? sucursalDe.get([...seriesSet][0]) ?? null : 'Todas';

  // ── Depósitos que entraron en el rango (fecha o identificación) ─────────────
  onAvance('Buscando depósitos en Bancos…');
  const enRangoId = { $gte: new Date(`${fechaInicio}T00:00:00.000-06:00`), $lte: new Date(`${fechaFin}T23:59:59.999-06:00`) };
  const bancos = await BankMovement.find({
    isActive: { $ne: false },
    deposito: { $gt: 0 },
    $or: [
      { fecha: { $gte: new Date(`${fechaInicio}T00:00:00.000Z`), $lte: new Date(`${fechaFin}T23:59:59.999Z`) } },
      { primeraIdentificacionAt: enRangoId },
      { 'identificadoPor.fechaId': enRangoId },
      { historialVinculacion: { $elemMatch: { accion: 'vinculado', at: enRangoId } } },
    ],
    'erpLinks.serie': { $in: [...seriesSet] },
  }).select('fecha concepto deposito banco folio erpLinks primeraIdentificacionAt identificadoPor historialVinculacion').lean();

  const _serie = l => String(l.serie ?? '').trim().toUpperCase();
  const _clave = l => `${_serie(l)}|${String(l.folioExterno ?? '').trim()}`;
  // Solo PPD (o sin tipo en el vínculo): las PUE nunca son "por facturar" y
  // consultarlas una por una en Kore tardaría demasiado.
  const linksDe = b => (b.erpLinks ?? []).filter(l => seriesSet.has(_serie(l)) && l.folioExterno && (!l.tipoPago || l.tipoPago === 'PPD'));
  const remisiones = [...new Map(bancos.flatMap(linksDe).map(l => [_clave(l), { serie: _serie(l), folio: String(l.folioExterno).trim() }])).values()];

  // ── Kore: factura de cada remisión + tipo de pago / estatus / saldo ─────────
  const desglose = new Map();
  for (let i = 0; i < remisiones.length; i += LOTE_DESGLOSES) {
    onAvance(`Consultando Kore: facturas ${Math.min(i + LOTE_DESGLOSES, remisiones.length)} de ${remisiones.length} remisiones…`);
    const lote = remisiones.slice(i, i + LOTE_DESGLOSES);
    const cuentas = await obtenerDesglosesCobroAlmacen({ rfc, series: lote.map(r => r.serie), folios: lote.map(r => r.folio) });
    for (const c of cuentas ?? []) desglose.set(`${c.serieVenta}|${c.folioVenta}`, c);
  }
  // Cliente / tipo de pago / estatus de la memoria de NUMO (kore_remisiones);
  // a Kore solo las nuevas y, por su saldo, las que todavía no tienen factura.
  onAvance('Consultando Kore: clientes y estatus de las remisiones nuevas…');
  const conDesglose = remisiones.filter(r => desglose.has(`${r.serie}|${r.folio}`));
  const { info: cuentaKore, sinRespuesta } = await infoRemisiones({
    rfc,
    remisiones: conDesglose.map(r => ({ ...r, fechaCreacion: desglose.get(`${r.serie}|${r.folio}`).fechaCreacion })),
    necesitaSaldo: k => !desglose.get(k)?.serieFactura,
  });

  const esPorFacturar = k => { const c = cuentaKore.get(k); return c?.tipoPago === 'PPD' && c?.nombreTipoMovimiento === ESTATUS_POR_FACTURAR; };
  const facturaDe = k => {
    const d = desglose.get(k);
    if (d?.serieFactura) return `${String(d.serieFactura).trim().toUpperCase()}-${String(d.folioFactura ?? '').trim()}`;
    return cuentaKore.get(k)?.factura ?? null;
  };

  // ── Clasificación por depósito ──────────────────────────────────────────────
  onAvance('Armando la estadística…');
  const candidatos = [];
  for (const b of bancos) {
    const links = linksDe(b).filter(l => esPorFacturar(_clave(l)));
    if (!links.length) continue;
    const entradaId = _identificadoEl(b);
    const entrada = [_diaDeposito(b.fecha), entradaId ? _diaMx(entradaId) : null].filter(Boolean).sort().pop();
    if (!entrada || entrada < fechaInicio || entrada > fechaFin) continue;
    candidatos.push({ b, links, entrada });
  }
  const facturasCfdi = await _facturasCfdi(new Set(candidatos.flatMap(c => c.links.map(l => facturaDe(_clave(l)))).filter(Boolean)));

  const hoy = _diaMx(new Date());
  const depositos = candidatos.map(({ b, links, entrada }) => {
    const unicas = [...new Map(links.map(l => [_clave(l), l])).values()];
    const facturas = unicas.map(l => facturaDe(_clave(l)));
    const facturado = facturas.every(Boolean);
    const cfdis = [...new Set(facturas.filter(Boolean))].map(f => ({ folio: f, cfdi: facturasCfdi.get(f) }));
    const fechaFactura = facturado
      ? cfdis.map(f => (f.cfdi?.fecha ? _diaMx(f.cfdi.fecha) : null)).filter(Boolean).sort().pop() ?? null
      : null;
    const tipos = new Set(cfdis.map(f => ((f.cfdi?.receptor?.rfc ?? '').trim().toUpperCase() === RFC_PUBLICO_GENERAL ? 'global' : 'cliente')));
    const saldo = _r2(unicas.reduce((s, l) => s + (Number(cuentaKore.get(_clave(l))?.saldoActual) || 0), 0));
    const serie = _serie(unicas[0]);
    return {
      idNumo:      b.folio ?? '',
      fecha:       b.fecha ?? null,
      entrada,
      descripcion: b.concepto ?? '',
      banco:       b.banco ?? '',
      importe:     _r2(b.deposito),
      aplicado:    _r2(links.reduce((s, l) => s + (Number(l.saldoPagado ?? l.saldoPagadoTotal) || 0), 0)),
      serie,
      sucursal:    sucursalDe.get(serie) ?? null,
      remisiones:  unicas.map(l => `${_serie(l)}-${String(l.folioExterno).trim()}`),
      cliente:     [...new Set(unicas.map(l => cuentaKore.get(_clave(l))?.nombre).filter(Boolean))].join(' / '),
      estado:      facturado ? 'facturado' : 'falta',
      facturas:    cfdis.map(f => f.folio),
      tipoFactura: facturado ? (tipos.size > 1 ? 'mixto' : [...tipos][0] ?? null) : null,
      fechaFactura,
      // Factura anterior a la entrada (se facturó antes de identificar el depósito) → 0.
      diasParaFacturar: facturado && fechaFactura ? _dias(entrada, fechaFactura) : null,
      diasSinFacturar:  facturado ? null : _dias(entrada, hoy),
      saldo:       facturado ? 0 : saldo,
    };
  }).sort((a, b) => a.entrada.localeCompare(b.entrada) || String(a.idNumo).localeCompare(String(b.idNumo)));

  // ── Totales, por almacén y día por día ──────────────────────────────────────
  const resumir = ds => {
    const fact = ds.filter(d => d.estado === 'facturado');
    const faltan = ds.filter(d => d.estado === 'falta');
    const suma = xs => _r2(xs.reduce((s, d) => s + d.importe, 0));
    const conDias = fact.filter(d => d.diasParaFacturar != null);
    return {
      habia:      { depositos: ds.length, importe: suma(ds) },
      facturados: { depositos: fact.length, importe: suma(fact),
        global: fact.filter(d => d.tipoFactura === 'global').length, cliente: fact.filter(d => d.tipoFactura !== 'global').length },
      faltan:     { depositos: faltan.length, importe: suma(faltan),
        conSaldo: faltan.filter(d => d.saldo > 0.005).length, saldo: _r2(faltan.reduce((s, d) => s + d.saldo, 0)) },
      avance:     ds.length ? _r2((fact.length / ds.length) * 100) : null,
      promedioDiasParaFacturar: conDias.length ? _r2(conDias.reduce((s, d) => s + d.diasParaFacturar, 0) / conDias.length) : null,
    };
  };
  const porSerie = new Map();
  for (const d of depositos) {
    if (!porSerie.has(d.serie)) porSerie.set(d.serie, []);
    porSerie.get(d.serie).push(d);
  }
  const porAlmacen = [...porSerie.entries()].sort(([a], [b]) => a.localeCompare(b))
    .map(([serie, ds]) => ({ serie, sucursal: sucursalDe.get(serie) ?? null, ...resumir(ds) }));

  const diaADia = _diasDelRango(fechaInicio, fechaFin).map(dia => {
    const nuevos = depositos.filter(d => d.entrada === dia);
    const facturadosDia = depositos.filter(d => d.fechaFactura === dia);
    const pendientes = depositos.filter(d => d.entrada <= dia && !(d.fechaFactura && d.fechaFactura <= dia));
    const suma = xs => _r2(xs.reduce((s, d) => s + d.importe, 0));
    return {
      dia,
      nuevos:     { depositos: nuevos.length, importe: suma(nuevos) },
      facturados: { depositos: facturadosDia.length, importe: suma(facturadosDia) },
      pendientes: { depositos: pendientes.length, importe: suma(pendientes) },
    };
  });

  return {
    sucursal, centroCostoId: centroCostoId ?? null, fechaInicio, fechaFin, calculadoEn: new Date(),
    totales: resumir(depositos),
    porAlmacen,
    diaADia,
    // Facturados después del rango (hasta hoy): no salen en el día a día.
    facturadosDespues: depositos.filter(d => d.fechaFactura && d.fechaFactura > fechaFin).length,
    depositos,
    advertencias: sinRespuesta ? [`Kore no respondió ${sinRespuesta} remisión(es); sus depósitos no se pudieron clasificar — vuelve a calcular más tarde.`] : null,
  };
}

// ── Trabajos en segundo plano (en memoria; se pierden al reiniciar) ──────────
const TTL_TRABAJO_MS = 2 * 3600000;
const _trabajos = new Map();

function _limpiar() {
  const limite = Date.now() - TTL_TRABAJO_MS;
  for (const [id, t] of _trabajos) if (t.creado < limite) _trabajos.delete(id);
}

/** Arranca el cálculo y regresa su id; la pantalla consulta `estado(id)`. */
function iniciar(params, user) {
  _limpiar();
  const id = crypto.randomUUID();
  const t = { id, estado: 'calculando', avance: 'Iniciando…', creado: Date.now(), usuario: user?.nombre ?? null, resultado: null, error: null };
  _trabajos.set(id, t);
  calcular(params, avance => { t.avance = avance; })
    .then(r => { t.estado = 'listo'; t.resultado = r; t.avance = null; })
    .catch(err => { t.estado = 'error'; t.error = err?.message ?? String(err); });
  return { id, estado: t.estado, avance: t.avance };
}

function estado(id) {
  const t = _trabajos.get(id);
  if (!t) throw new NotFoundError('Cálculo');
  return { id, estado: t.estado, avance: t.avance, error: t.error, resultado: t.resultado };
}

/** Valida antes de arrancar (errores de captura → 400 inmediato, no al consultar). */
function validar({ rfc, fechaInicio, fechaFin }) {
  fechaFin = fechaFin || fechaInicio;
  if (!rfc) throw new BadRequestError('RFC requerido');
  if (!FECHA_RE.test(fechaInicio ?? '') || !FECHA_RE.test(fechaFin ?? '')) throw new BadRequestError('Fechas inválidas (formato YYYY-MM-DD)');
  if (fechaFin < fechaInicio) throw new BadRequestError('La fecha final no puede ser anterior a la inicial');
  if (_diasDelRango(fechaInicio, fechaFin).length > MAX_DIAS_RANGO) throw new BadRequestError(`El rango no puede pasar de ${MAX_DIAS_RANGO} días`);
}

/** Excel de un cálculo terminado: RESUMEN, DÍA A DÍA, FACTURADOS y FALTAN. */
async function excel(id) {
  const t = _trabajos.get(id);
  if (!t || t.estado !== 'listo') throw new NotFoundError('Cálculo');
  return _excelDe(t.resultado);
}

async function _excelDe(r) {
  const wb = new ExcelJS.Workbook();
  const dinero = '$#,##0.00';
  const encabezado = row => row.eachCell(c => {
    c.font = { bold: true, color: { argb: 'FFFFFFFF' } };
    c.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF5B4A9E' } };
    c.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true };
  });
  const hoja = (nombre, columnas, filas) => {
    const ws = wb.addWorksheet(nombre);
    ws.columns = columnas.map(([key, , width]) => ({ key, width }));
    encabezado(ws.addRow(columnas.map(([, header]) => header)));
    for (const f of filas) {
      const row = ws.addRow(f);
      for (const [key, , , fmt] of columnas) if (fmt) row.getCell(key).numFmt = fmt;
    }
    return ws;
  };

  const res = wb.addWorksheet('RESUMEN');
  res.columns = [{ width: 34 }, { width: 14 }, { width: 18 }, { width: 14 }, { width: 18 }, { width: 14 }, { width: 18 }, { width: 10 }];
  res.addRow([`Estadística por facturar — ${r.sucursal ?? 'Todas'} — ${r.fechaInicio} a ${r.fechaFin}`]).font = { bold: true, size: 13 };
  res.addRow([`Calculado el ${new Date(r.calculadoEn).toLocaleString('es-MX', { timeZone: 'America/Mexico_City' })}`]);
  res.addRow([]);
  encabezado(res.addRow(['ALMACÉN', 'HABÍA', 'IMPORTE', 'FACTURADOS', 'IMPORTE', 'FALTAN', 'IMPORTE', 'AVANCE']));
  const filaRes = (etiqueta, x, negritas) => {
    const row = res.addRow([etiqueta, x.habia.depositos, x.habia.importe, x.facturados.depositos, x.facturados.importe,
      x.faltan.depositos, x.faltan.importe, x.avance != null ? x.avance / 100 : null]);
    [3, 5, 7].forEach(c => { row.getCell(c).numFmt = dinero; });
    row.getCell(8).numFmt = '0.0%';
    if (negritas) row.font = { bold: true };
  };
  for (const a of r.porAlmacen) filaRes([a.serie, a.sucursal].filter(Boolean).join(' - '), a);
  filaRes('TOTAL', r.totales, true);

  hoja('DÍA A DÍA', [
    ['dia', 'DÍA', 12], ['nuevosN', 'NUEVOS', 10], ['nuevosI', 'IMPORTE NUEVOS', 16, dinero],
    ['factN', 'FACTURADOS', 12], ['factI', 'IMPORTE FACTURADOS', 18, dinero],
    ['pendN', 'PENDIENTES AL CIERRE', 14], ['pendI', 'IMPORTE PENDIENTE', 18, dinero],
  ], r.diaADia.map(d => ({
    dia: d.dia, nuevosN: d.nuevos.depositos, nuevosI: d.nuevos.importe, factN: d.facturados.depositos,
    factI: d.facturados.importe, pendN: d.pendientes.depositos, pendI: d.pendientes.importe,
  })));

  const comunes = [
    ['entrada', 'ENTRÓ', 11], ['idNumo', 'ID NUMO', 10], ['descripcion', 'DESCRIPCIÓN', 40], ['importe', 'IMPORTE', 14, dinero],
    ['almacen', 'ALMACÉN', 16], ['rem', 'REMISIONES', 18], ['cliente', 'CLIENTE', 30],
  ];
  const base = d => ({ ...d, almacen: [d.serie, d.sucursal].filter(Boolean).join(' - '), rem: d.remisiones.join('\n'), cliente: d.cliente || 'CLIENTE NO IDENTIFICADO' });
  hoja('FACTURADOS', [...comunes, ['factura', 'FACTURA', 18], ['tipo', 'TIPO', 10], ['fechaFactura', 'FECHA FACTURA', 13], ['diasParaFacturar', 'DÍAS QUE TARDÓ', 10]],
    r.depositos.filter(d => d.estado === 'facturado').map(d => ({
      ...base(d), factura: d.facturas.join('\n'), tipo: d.tipoFactura === 'global' ? 'GLOBAL' : d.tipoFactura === 'mixto' ? 'MIXTO' : 'CLIENTE',
    })));
  hoja('FALTAN', [...comunes, ['saldo', 'SALDO PENDIENTE', 14, dinero], ['diasSinFacturar', 'DÍAS SIN FACTURAR', 10]],
    r.depositos.filter(d => d.estado === 'falta').map(base));

  const rango = r.fechaInicio === r.fechaFin ? r.fechaInicio : `${r.fechaInicio}_${r.fechaFin}`;
  const sucursal = String(r.sucursal ?? 'Todas').replace(/[^\w-]+/g, '_');
  return { buffer: await wb.xlsx.writeBuffer(), filename: `Estadistica_por_facturar_${sucursal}_${rango}.xlsx` };
}

// ── Mes en curso, ya calculado (2026-10-30) ──────────────────────────────────
// "Ya debe verse en cuanto se entra al módulo": el mes en curso de cada
// empresa / sucursal se guarda (EstadisticaPorFacturarFoto) y se recalcula
// solo — cada hora (cron) y al abrirla si tiene más de FOTO_VIGENTE_MS. La
// pantalla muestra al instante la última guardada mientras tanto.
const FOTO_VIGENTE_MS = 60 * 60000;
const _fotoEnCurso = new Map(); // `${rfc}|${centroCostoId}` → { avance, promesa }

const _ccId = v => (v === null || v === undefined || v === '' ? null : Number(v));
const _claveFoto = (rfc, centroCostoId) => `${rfc}|${_ccId(centroCostoId) ?? 'todas'}`;
const _mesEnCurso = (ahora = new Date()) => {
  const hoy = _diaMx(ahora);
  return { fechaInicio: `${hoy.slice(0, 8)}01`, fechaFin: hoy };
};

/** Recalcula (en segundo plano) la foto del mes en curso; si ya corre, no hace nada. */
function recalcularFoto(rfc, centroCostoId = null) {
  const clave = _claveFoto(rfc, centroCostoId);
  if (_fotoEnCurso.has(clave)) return _fotoEnCurso.get(clave).promesa;
  const cc = _ccId(centroCostoId);
  const trabajo = { avance: 'Iniciando…', promesa: null };
  const periodo = _mesEnCurso();
  trabajo.promesa = (async () => {
    try {
      const resultado = await calcular({ rfc, centroCostoId: cc, ...periodo }, avance => { trabajo.avance = avance; });
      await Foto.updateOne({ rfc, centroCostoId: cc },
        { $set: { ...periodo, resultado, calculadoEn: resultado.calculadoEn, error: null } }, { upsert: true });
    } catch (err) {
      logger.warn(`[EstadisticaPorFacturar] No se pudo recalcular ${clave}: ${err?.message ?? err}`);
      await Foto.updateOne({ rfc, centroCostoId: cc }, { $set: { error: err?.message ?? String(err) } }, { upsert: true }).catch(() => {});
    } finally {
      _fotoEnCurso.delete(clave);
    }
  })();
  _fotoEnCurso.set(clave, trabajo);
  return trabajo.promesa;
}

/**
 * Lo que ve la pantalla al entrar: la última foto guardada del mes en curso
 * (al instante) y si se está recalculando. Si no hay, es de otro mes o ya es
 * vieja, arranca el recálculo en segundo plano.
 */
async function fotoActual(rfc, centroCostoId = null) {
  if (!rfc) throw new BadRequestError('RFC requerido');
  const cc = _ccId(centroCostoId);
  const foto = await Foto.findOneAndUpdate({ rfc, centroCostoId: cc }, { $set: { consultadaEn: new Date() } }, { upsert: true, new: true }).lean();
  const { fechaInicio } = _mesEnCurso();
  const vigente = foto?.resultado && foto.fechaInicio === fechaInicio
    && foto.calculadoEn && Date.now() - new Date(foto.calculadoEn).getTime() < FOTO_VIGENTE_MS;
  if (!vigente) recalcularFoto(rfc, cc);
  const enCurso = _fotoEnCurso.get(_claveFoto(rfc, cc));
  return {
    // Una foto de un mes anterior no se muestra como si fuera la de este.
    resultado:   foto?.fechaInicio === fechaInicio ? foto.resultado ?? null : null,
    calculadoEn: foto?.fechaInicio === fechaInicio ? foto.calculadoEn ?? null : null,
    calculando:  !!enCurso,
    avance:      enCurso?.avance ?? null,
    error:       enCurso ? null : foto?.error ?? null,
  };
}

/** Excel de la foto guardada del mes en curso. */
async function excelFoto(rfc, centroCostoId = null) {
  const foto = await Foto.findOne({ rfc, centroCostoId: _ccId(centroCostoId) }).lean();
  if (!foto?.resultado) throw new NotFoundError('Estadística');
  return _excelDe(foto.resultado);
}

/** Cron: recalcula las fotos que alguien abrió en los últimos 7 días (una a la vez). */
async function recalcularFotosEnUso() {
  const fotos = await Foto.find({ consultadaEn: { $gte: new Date(Date.now() - 7 * 86400000) } }).select('rfc centroCostoId').lean();
  for (const f of fotos) await recalcularFoto(f.rfc, f.centroCostoId);
  return fotos.length;
}

/** Espera los recálculos en curso (pruebas). */
const _esperarFotos = () => Promise.all([..._fotoEnCurso.values()].map(t => t.promesa));

module.exports = { calcular, iniciar, estado, validar, excel, fotoActual, recalcularFoto, excelFoto, recalcularFotosEnUso, _esperarFotos };
