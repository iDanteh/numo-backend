'use strict';

const { CfdiMappingRule, AccountPlan, PolizaMovimiento, Poliza } = require('../../../shared/models/postgres');
const { Op, fn, col, literal } = require('sequelize');
const { NotFoundError, BadRequestError } = require('../../shared/errors/AppError');

// DEBUG TEMPORAL (2026-08-20, quitar después de investigar el gap de
// Efectivo de Hidalgo/B0 11-ago) — activar con DEBUG_SPLIT_PAGO=1.
const { logger: _debugLogger } = require('../../../shared/utils/logger');
const _DEBUG_SPLIT_PAGO = process.env.DEBUG_SPLIT_PAGO === '1';

// tipoOrigen para las porciones de Saldo a Favor/Puntos DENTRO del split de
// una factura NORMAL — deliberadamente DISTINTO de 'Cobro Sucursal' (el que
// usa el mecanismo de cruces reales entre sucursales, cobros-sucursal-puente
// .service.js): `_uuidsConCargoCubiertoEnBD` (cfdi-poliza-generator.service.js)
// consulta PolizaMovimiento por `tipoOrigen: 'Cobro Sucursal'` para saber
// qué facturas ya tienen su Cargo cubierto por un cruce real — si estas
// líneas (que NO son un cruce, son solo una forma de pago del split normal)
// compartieran ese mismo valor, una regeneración futura las confundiría con
// un cruce real y omitiría el Cargo de Efectivo/Tarjeta de la MISMA factura
// por error. `_extraerCobrosSucursal` (poliza.service.js) sí las reconoce
// para el mismo tratamiento de display (individual, nunca consolidado, sin
// prefijo en la columna C) — mismo patrón que TIPO_ORIGEN_PENDIENTE_PROPIO
// en cobros-sucursal-puente.service.js.
const TIPO_ORIGEN_CARGO_ESPECIAL = 'Cargo Especial';

// Mismo patrón de ocultamiento que ETIQUETA_SALDO_FAVOR_OCULTO
// (poliza.service.js/cobros-sucursal-puente.service.js: tipoOrigen='Cobro
// Sucursal' + este reglaNombre → `_extraerCobrosSucursal` lo saca a "Otros
// Ingresos oculto"), pero para un caso distinto (2026-08-25): un Cargo de
// Efectivo/Tarjeta cuyo cobro real ya se contabilizó otro día vía
// `_cobrosSinFacturaPorCentro` (ver `yaContabilizadoOtroDia` en
// `_prefetchAjustesFacturaPropia`, cfdi-poliza-generator.service.js).
const ETIQUETA_COBRO_YA_CONTABILIZADO = 'COBRO-DIA-REAL';

// Caja/Bancos "por identificar" — mismos códigos que
// cfdi-poliza-generator.service.js/cobros-sucursal-puente.service.js
// (duplicado a propósito, archivos pequeños, independientes). Son las ÚNICAS
// cuentas cuya selección depende realmente de `formaPago` — ver
// `esCasoNormalParaSplit` en `cfdiToMovimientos`.
const CODIGO_CUENTA_CAJA   = '1101010003';
const CODIGO_CUENTA_BANCOS = '1102011005';
// Cuentas bancarias específicas (igual que `BANCO_A_CODIGO_CUENTA` en
// poliza.service.js — duplicado a propósito). Una regla que apunte a
// cualquiera de estas cuentas se trata igual que si apuntara a la cuenta
// genérica CAJA/BANCOS para el split de `splitPorFormaPagoReal`: Efectivo →
// CAJA, todo lo demás → BANCOS genérico. El mapping banco-real se hace
// DESPUÉS (en poliza.service.js, vía verdadBancaria) solo para Transferencias,
// no para Tarjeta/Efectivo (confirmado con el usuario 2026-08-15).
const CODIGOS_CUENTAS_CAJA_O_BANCO = new Set([
  '1101010003', // CAJA
  '1102011005', // BANCOS (genérico, "por identificar")
  '1102011001', // BBVA
  '1102012001', // Banamex
  '1102013001', // Santander
  '1102014001', // Banorte
  '1102015001', // Scotiabank
  '1102016001', // Azteca
]);
// Saldo a Favor / monedero Club Tuberos — mismos códigos y mismo split 16%
// (subtotal + IVA) que ya usa cobros-sucursal-puente.service.js. Necesarios
// aquí porque el desglose REAL de cajas puede traer estas formas de pago
// también en facturas normales (no solo en cobros cruzados de sucursal) —
// confirmado con datos reales 2026-08-06: "SALDO A FAVOR" usa claveSat='30'
// (cuenta propia, no colisiona) pero "PUNTOS" reutiliza claveSat='01', el
// MISMO que Efectivo — sin distinguir por `nombre`, un pago con monedero
// Club Tuberos se clasificaría como Efectivo real (a Caja), y un Saldo a
// Favor (claveSat='30', no reconocido) caería al bucket genérico de Bancos
// sin ninguna etiqueta válida (visible en el export como "Depósitos
// consolidados" sin sufijo Efectivo/Tarjeta).
const CODIGO_CUENTA_SALDO_FAVOR     = '2103090001';
const CODIGO_CUENTA_CLUB_TUBEROS    = '2103090002';
const CODIGO_CUENTA_IVA_SALDO_FAVOR = '2104010002';
const TASA_IVA_SALDO_FAVOR = 0.16;
// NOTA (2026-08-06, corrección del mismo día): las funciones `_esSaldoAFavorReal`/
// `_esPuntosReal` que escaneaban `formasPago` de /desgloses-cobro/almacen para
// detectar SF/Puntos DENTRO del desglose de Efectivo/Tarjeta quedaron
// eliminadas — confirmado con datos reales del usuario que ese desglose casi
// nunca trae el pago principal junto con el ajuste de SF/Puntos (solo trae
// eventos de series ABO/CBT/CPF/CFC, ver SERIES_CON_AUTH), así que "encontrar
// una sola formaPago ahí" NO significa "eso es el 100% de la factura" — mi
// split proporcional anterior asumía eso y atribuía el CARGO COMPLETO a SF/
// Puntos cuando solo era una fracción (caso real: factura de $1,023.63 con
// $87.79 de Puntos redimidos, terminaba con $1,023.63 completos en la cuenta
// de Puntos). Ahora SF y Puntos llegan ya resueltos y CONFIABLES vía
// `context.saldoFavorUsadoPropio`/`context.montoPuntosUsado`
// (`_prefetchSaldoFavorUsadoPropio`/`_prefetchDesglosePagoReal` en
// cfdi-poliza-generator.service.js) — ver `esCasoAjusteSFPuntos` más abajo.

// ── CRUD de reglas ────────────────────────────────────────────────────────────

async function list() {
  return CfdiMappingRule.findAll({
    attributes: {
      include: [[
        literal(`(
          SELECT COUNT(pm.id)
          FROM poliza_movimientos pm
          JOIN polizas p ON p.id = pm.poliza_id
          WHERE pm.regla_id = "CfdiMappingRule"."id"
            AND p.estado IN ('borrador', 'contabilizada')
        )`),
        'vecesUsadaActiva',
      ]],
    },
    order: [['prioridad', 'ASC'], ['nombre', 'ASC']],
  });
}

async function getById(id) {
  const rule = await CfdiMappingRule.findByPk(id);
  if (!rule) throw new NotFoundError('Regla de mapeo');
  return rule;
}

async function create(data) {
  _validate(data);
  return CfdiMappingRule.create(data);
}

async function update(id, data) {
  const rule = await CfdiMappingRule.findByPk(id);
  if (!rule) throw new NotFoundError('Regla de mapeo');
  _validate({ ...rule.toJSON(), ...data });

  const usada = await PolizaMovimiento.count({ where: { reglaId: id } });
  const snapshotAntes = usada > 0 ? rule.toJSON() : null;

  await rule.update(data);

  const result = rule.toJSON();
  if (snapshotAntes) {
    result._advertencia = `Esta regla ya fue usada en ${usada} movimiento(s). Las pólizas anteriores no se modifican — cada movimiento guarda el nombre de la regla que se usó al generarlo.`;
    result._reglaAlMomentoDeUso = snapshotAntes;
  }
  return result;
}

async function remove(id) {
  const rule = await CfdiMappingRule.findByPk(id);
  if (!rule) throw new NotFoundError('Regla de mapeo');

  const usada = await PolizaMovimiento.count({ where: { reglaId: id } });
  if (usada > 0) {
    throw new BadRequestError(
      `No se puede eliminar la regla "${rule.nombre}" porque ya fue usada en ${usada} movimiento(s). ` +
      `Puedes deshabilitarla (isActive=false) o actualizarla sin afectar las pólizas anteriores.`,
    );
  }

  await rule.destroy();
}

// ── Matching ──────────────────────────────────────────────────────────────────

/**
 * Busca la regla más específica que aplica a un CFDI.
 * Orden de prioridad: menor número primero, y dentro del mismo número
 * gana la que tiene más restricciones (rfcEmisor específico antes que vacío).
 */
async function findRuleForCfdi(cfdi) {
  const rules = await CfdiMappingRule.findAll({
    where: { isActive: true },
    order: [['prioridad', 'ASC']],
  });
  return findRuleInList(cfdi, rules);
}

/**
 * Versión síncrona de findRuleForCfdi que opera sobre una lista ya cargada.
 * Usar cuando se procesan múltiples CFDIs para evitar una query por iteración.
 *
 * Una regla aplica si todos sus filtros no-nulos coinciden con el CFDI.
 * Entre las reglas que aplican, gana la de menor prioridad numérica;
 * en empate, la más específica (más filtros con valor).
 */
function findRuleInList(cfdi, rules) {
  const cfdiClaveProdServ  = cfdi.conceptos?.[0]?.claveProdServ ?? null;
  // Prioridad de tipoRelacion: '04' y '07' son más específicos que '01'.
  // Si el CFDI tiene múltiples relaciones (ej. SAT concatenó '01' y ERP tiene '04'),
  // se prefiere '04' (sustitución) > '07' (anticipo) > cualquier otro > primer elemento.
  const _RELACION_PRIORIDAD = ['04', '07'];
  const cfdiTipoRelacion = cfdi.cfdiRelacionados?.find(r => _RELACION_PRIORIDAD.includes(r.tipoRelacion))?.tipoRelacion
    ?? cfdi.cfdiRelacionados?.[0]?.tipoRelacion
    ?? null;
  const cfdiTasaIva        = _detectTasaIva(cfdi);
  const cfdiTieneDescuento = _detectTieneDescuento(cfdi);

  // _relacionadoTipo es un campo virtual inyectado por el generator antes del matching.
  // Contiene el tipoDeComprobante del primer CFDI relacionado (pre-fetched de MongoDB).
  const cfdiRelacionadoTipo = cfdi._relacionadoTipo ?? null;

  // Para tipo P, formaPago va en el complemento (formaDePagoP), no en el header.
  const cfdiFormaPago = cfdi.formaPago
    ?? (cfdi.tipoDeComprobante === 'P'
      ? cfdi.complementoPago?.pagos?.[0]?.formaDePagoP ?? null
      : null);

  const cfdiDescripcion = (cfdi.conceptos ?? [])
    .map(c => c.descripcion ?? c.Descripcion ?? '')
    .join(' ')
    .toLowerCase();
  const cfdiTipoOrigen  = cfdi.tipoOrigen ?? _derivarTipoOrigen(cfdi) ?? null;

  const matching = rules.filter(r =>
    (!r.tipoComprobante    || r.tipoComprobante  === cfdi.tipoDeComprobante) &&
    (!r.rfcEmisor          || r.rfcEmisor        === cfdi.emisor?.rfc) &&
    (!r.rfcReceptor        || r.rfcReceptor      === cfdi.receptor?.rfc) &&
    (!r.metodoPago         || r.metodoPago       === cfdi.metodoPago) &&
    (!r.formaPago          || r.formaPago         === cfdiFormaPago) &&
    (!r.claveProdServ      || r.claveProdServ     === cfdiClaveProdServ) &&
    (!r.tipoRelacion       || r.tipoRelacion      === cfdiTipoRelacion) &&
    (!r.relacionadoTipo    || r.relacionadoTipo   === cfdiRelacionadoTipo) &&
    (!r.conceptoContiene   || cfdiDescripcion.includes(r.conceptoContiene.toLowerCase())) &&
    (!r.tipoOrigen         || r.tipoOrigen        === cfdiTipoOrigen) &&
    (r.tasaIva        == null || r.tasaIva        === cfdiTasaIva) &&
    (r.tieneDescuento == null || r.tieneDescuento === cfdiTieneDescuento),
  );
  if (!matching.length) return null;

  const spec = r => [
    r.tipoComprobante, r.rfcEmisor, r.rfcReceptor, r.metodoPago, r.formaPago,
    r.claveProdServ, r.tipoRelacion, r.relacionadoTipo, r.tasaIva,
    r.tieneDescuento  != null ? String(r.tieneDescuento)  : null,
    r.conceptoContiene != null ? r.conceptoContiene       : null,
    r.tipoOrigen      != null ? r.tipoOrigen              : null,
  ].filter(v => v != null).length;

  return matching.sort((a, b) => {
    if (a.prioridad !== b.prioridad) return a.prioridad - b.prioridad;
    return spec(b) - spec(a);
  })[0];
}

// ── Helpers de detección de propiedades del CFDI ──────────────────────────────

/** Detecta la tasa IVA dominante en los conceptos del CFDI.
 *  Tipo P (complemento de pago) siempre devuelve null — no tiene conceptos con tasa.
 *  Fallback: si los conceptos no tienen desglose de tasa, lee del header (cfdi.impuestos). */
function _detectTasaIva(cfdi) {
  if (cfdi.tipoDeComprobante === 'P') {
    // El complemento de pago no tiene conceptos propios; detectar tasa desde los
    // totales SAT 4.0 del complemento. Si hay IVA16 o IVA8 (fronterizo) → '16'.
    // Si hay monto pagado pero cero IVA → '0' (exento o tasa cero).
    // Si no hay totales (CFDI incompleto) → null (fallback a reglas sin tasaIva).
    const totales = cfdi.complementoPago?.totales;
    if (totales) {
      const iva16 = Number(totales.totalTrasladosImpuestoIVA16 || 0);
      const iva8  = Number(totales.totalTrasladosImpuestoIVA8  || 0);
      if (iva16 > 0 || iva8 > 0) {
        // Si el monto total pagado excede la suma base16+iva16 (+iva8), hay porción 0%.
        const base16  = Number(totales.totalTrasladosBaseIVA16 || 0);
        const monto   = Number(totales.montoTotalPagos || 0);
        const monto16 = base16 + iva16 + Number(totales.totalTrasladosImpuestoIVA8 || 0);
        if (monto > monto16 + 0.01) return 'mixto';
        return '16';
      }
      const montoTotal = Number(totales.montoTotalPagos || 0);
      if (montoTotal > 0) return '0';
    } else {
      // CP 1.0 (CFDI 3.3): no hay <Totales>, pero cada DoctoRelacionado puede
      // tener <ImpuestosDR><TrasladosDR> con la tasa por documento.
      let drTiene16 = false;
      let drTiene0  = false;
      for (const pago of (cfdi.complementoPago?.pagos ?? [])) {
        for (const dr of (pago.doctosRelacionados ?? [])) {
          for (const t of (dr.trasladosDR ?? [])) {
            if ((t.impuesto || '') !== '002') continue;
            if ((t.tasaOCuota || 0) > 0) drTiene16 = true;
            else drTiene0 = true;
          }
        }
      }
      if (drTiene16 && drTiene0) return 'mixto';
      if (drTiene16) return '16';
      if (drTiene0)  return '0';
      // Último fallback: tasa pre-computada por migración (CP 1.0 sin xmlContent)
      if (cfdi.tasaIvaInferida != null) return cfdi.tasaIvaInferida;
    }
    return null;
  }
  let tiene16 = false;
  let tiene0  = false;
  for (const c of (cfdi.conceptos || [])) {
    for (const t of (c.impuestos?.traslados || [])) {
      // Solo IVA (código SAT '002') — excluye IEPS y otros impuestos
      const impuesto = t.impuesto || t.Impuesto || '';
      if (impuesto !== '002') continue;
      const tasa = Number(t.tasaOCuota ?? t.TasaOCuota ?? 0);
      if (tasa > 0)  tiene16 = true;
      else           tiene0  = true;
    }
  }
  // Fallback 1: IVA a nivel header (cfdi.impuestos.traslados)
  if (!tiene16 && !tiene0) {
    for (const t of (cfdi.impuestos?.traslados || [])) {
      const impuesto = t.impuesto || t.Impuesto || '';
      if (impuesto !== '002') continue;
      const tasa = Number(t.tasaOCuota ?? t.TasaOCuota ?? 0);
      if (tasa > 0)  tiene16 = true;
      else           tiene0  = true;
    }
  }
  // Fallback 2: traslados vacío pero totalImpuestosTrasladados disponible
  // (CFDIs Metadata — sin XML completo pero con totales SAT en el header)
  if (!tiene16 && !tiene0) {
    const rawTotal = cfdi.impuestos?.totalImpuestosTrasladados;
    // Solo actuar si el campo está EXPLÍCITAMENTE presente en el documento.
    // Si es undefined/null no sabemos la tasa → devolvemos null (no inferir 0%).
    if (rawTotal != null) {
      const totalImptos = Number(rawTotal);
      if (totalImptos <= 0) {
        tiene0 = true;
      } else {
        // Determinar si es puro 16% o mixto (parte 16% + parte exenta/0%).
        // Si el IVA real difiere del esperado a 16% en más de $0.50, FORZOSAMENTE
        // hay productos con tasa diferente (0% o exento) → MIXTO.
        // Tolerancia de $0.50 absorbe redondeos por concepto sin falsos positivos.
        const base = Number(cfdi.subTotal || 0) - Number(cfdi.descuento || 0);
        if (base > 0 && Math.abs(totalImptos - base * 0.16) > 0.50) {
          // Antes de marcar mixto: descartar artefacto SAT metadata donde
          // totalImpuestosRetenidos ≈ ivaTras y retenciones:[] — en ese caso la
          // porción aparente 0% es un descuento implícito, no producto a tasa 0%.
          const ivaRetChk = Number(cfdi.impuestos?.totalImpuestosRetenidos || 0);
          const esArtRet  = (cfdi.impuestos?.retenciones || []).length === 0 &&
                            ivaRetChk > 0 && Math.abs(ivaRetChk - totalImptos) < 1.0;
          tiene16 = true;
          if (!esArtRet) tiene0 = true; // mixto real; artefacto → solo 16%
        } else {
          tiene16 = true;   // puro 16% (diferencia ≤ $0.50 → solo redondeo)
        }
      }
    }
  }
  if (tiene16 && tiene0) return 'mixto';
  if (tiene16)           return '16';
  if (tiene0)            return '0';
  return null; // sin información de tasa
}

/**
 * Deriva el TipoOrigen de negocio a partir de los campos del CFDI cuando el ERP
 * no lo proporcionó. Se usa como fallback para CFDIs SAT sin homólogo ERP.
 */
function _derivarTipoOrigen(cfdi) {
  const tipo = cfdi.tipoDeComprobante;
  if (tipo === 'I') return 'Venta';
  if (tipo === 'E') {
    const rel        = cfdi.cfdiRelacionados?.[0]?.tipoRelacion;
    const concepto   = (cfdi.conceptos?.[0]?.descripcion ?? cfdi.conceptos?.[0]?.Descripcion ?? '').toUpperCase();
    // CANCELAC en el concepto siempre es Cancelación (devolución contable),
    // sin importar el tipoRelacion — evita que queden en Descuentos.
    if (concepto.includes('CANCELAC')) return 'Cancelación';
    if (rel === '03' || concepto.includes('DEVOLUCI')) return 'Devolución';
    if (rel === '01') return 'Nota de Crédito';
    return 'Bonificación';
  }
  if (tipo === 'P') return 'Pago';
  if (tipo === 'N') return 'Nómina';
  if (tipo === 'T') return 'Traslado';
  return null;
}

/** Detecta si el CFDI tiene descuento > 0 (header o en algún concepto). */
function _detectTieneDescuento(cfdi) {
  if (Number(cfdi.descuento || 0) > 0) return true;
  if ((cfdi.conceptos || []).some(c => Number(c.descuento || c.Descuento || 0) > 0)) return true;
  // Metadata SAT: ivaRet ≈ ivaTras y retenciones:[] → hay descuento implícito en conceptos.
  const iva0    = Number(cfdi.impuestos?.totalImpuestosTrasladados || 0);
  const ivaRet0 = Number(cfdi.impuestos?.totalImpuestosRetenidos  || 0);
  return (cfdi.conceptos || []).length === 0 &&
    (cfdi.impuestos?.retenciones || []).length === 0 &&
    ivaRet0 > 0 && Math.abs(ivaRet0 - iva0) < 1.0 &&
    (Number(cfdi.subTotal || 0) + iva0 - Number(cfdi.total || 0)) > 0.5;
}

/** Desglosa importes por tasa (para reglas mixtas y con descuento). */
function _calcCfdiMontos(cfdi) {
  let subTotal16 = 0, subTotal0 = 0, desc16 = 0, desc0 = 0;
  for (const c of (cfdi.conceptos || [])) {
    const importe   = Number(c.importe   || c.Importe   || 0);
    const descuento = Number(c.descuento || c.Descuento || 0);
    const traslados = c.impuestos?.traslados || [];
    const esTasa16  = traslados.some(t => {
      const imp = t.impuesto || t.Impuesto || '';
      return imp === '002' && Number(t.tasaOCuota ?? t.TasaOCuota ?? 0) > 0;
    });
    if (esTasa16) { subTotal16 += importe; desc16 += descuento; }
    else          { subTotal0  += importe; desc0  += descuento; }
  }
  // Fallback traslados header con 0% explícito (ERP mixto sin conceptos).
  // Los CFDIs ERP mixtos almacenan base_16 = subTotal completo (incluye porción 0%),
  // por lo que el fallback matemático (IVA/0.16) devuelve subTotal0 ≈ 0.
  // En su lugar: leer base_0 directo del traslado 0% del header y derivar subTotal16
  // como (subTotal - descuento - base_0), que sí es el neto 16% correcto.
  // Guardia: si base0Ht >= baseTotal el dato ERP está malformado → dejar caer al fallback matemático.
  if (subTotal16 === 0 && subTotal0 === 0) {
    const ht = cfdi.impuestos?.traslados || [];
    const base0Ht = ht
      .filter(t => (t.impuesto || t.Impuesto || '') === '002' &&
                   Number(t.tasaOCuota ?? t.TasaOCuota ?? 0) <= 0)
      .reduce((s, t) => s + Number(t.base ?? t.Base ?? 0), 0);
    if (base0Ht > 0) {
      const baseTotal = Number(cfdi.subTotal || 0) - Number(cfdi.descuento || 0);
      if (base0Ht < baseTotal) {
        subTotal0  = base0Ht;
        subTotal16 = parseFloat((baseTotal - subTotal0).toFixed(6));
        // Distribuir descuento header proporcionalmente (igual que el fallback matemático).
        // Necesario para que reglas tieneDescuento=true generen la línea de descuento correcta.
        const totalDesc = Number(cfdi.descuento || 0);
        if (totalDesc > 0) {
          const sumSub = subTotal16 + subTotal0;
          desc16 = sumSub > 0 ? parseFloat((totalDesc * subTotal16 / sumSub).toFixed(6)) : 0;
          desc0  = parseFloat(Math.max(0, totalDesc - desc16).toFixed(6));
        }
      }
    }
  }
  // Fallback 1b: leer base_16 y base_0 directamente de traslados header cuando Fallback 1 no disparó.
  // Cubre CFDIs ERP puro-16% (base_0Ht=0) y puro-0% (ivaHeader=0).
  // Solo aplica cuando ningún concepto tiene traslados (subTotal16+subTotal0 aún es 0).
  if (subTotal16 === 0 && subTotal0 === 0) {
    const ht = cfdi.impuestos?.traslados || [];
    const base16Ht = ht
      .filter(t => (t.impuesto || t.Impuesto || '') === '002' &&
                   Number(t.tasaOCuota ?? t.TasaOCuota ?? 0) > 0)
      .reduce((s, t) => s + Number(t.base ?? t.Base ?? 0), 0);
    const base0Ht2 = ht
      .filter(t => (t.impuesto || t.Impuesto || '') === '002' &&
                   Number(t.tasaOCuota ?? t.TasaOCuota ?? 0) <= 0)
      .reduce((s, t) => s + Number(t.base ?? t.Base ?? 0), 0);
    if (base16Ht > 0 || base0Ht2 > 0) {
      subTotal16 = base16Ht;
      subTotal0  = base0Ht2;
      const totalDesc = Number(cfdi.descuento || 0);
      if (totalDesc > 0 && (subTotal16 + subTotal0) > 0) {
        desc16 = parseFloat((totalDesc * subTotal16 / (subTotal16 + subTotal0)).toFixed(6));
        desc0  = parseFloat(Math.max(0, totalDesc - desc16).toFixed(6));
      }
    }
  }
  // Fallback Metadata: no hay traslados en conceptos → estimar split desde el encabezado.
  // Se activa en dos casos:
  //   a) conceptos vacíos (subTotal16 + subTotal0 === 0)
  //   b) conceptos presentes pero SIN traslados → todos los importes cayeron en subTotal0
  //      aunque el header indique IVA > 0 (SAT Metadata o CFDIs sin desglose por concepto).
  const ivaHeader0 = Number(cfdi.impuestos?.totalImpuestosTrasladados || 0);
  if (subTotal16 === 0 && ivaHeader0 > 0) {
    const base = Number(cfdi.subTotal || 0) - Number(cfdi.descuento || 0);
    if (base > 0) {
      subTotal16 = parseFloat((ivaHeader0 / 0.16).toFixed(6));
      const rawSub0 = parseFloat(Math.max(0, base - subTotal16).toFixed(6));
      // Artefacto metadata: retenciones:[] con ivaRet ≈ ivaTras → rawSub0 es descuento implícito
      const ivaRetC = Number(cfdi.impuestos?.totalImpuestosRetenidos || 0);
      const esArtC  = (cfdi.conceptos || []).length === 0 &&
                      (cfdi.impuestos?.retenciones || []).length === 0 &&
                      ivaRetC > 0 && Math.abs(ivaRetC - ivaHeader0) < 1.0 &&
                      rawSub0 > 0.5;
      if (esArtC) {
        desc16    = rawSub0;  // descuento implícito inferido de los importes del CFDI
        subTotal0 = 0;
      } else {
        subTotal0 = rawSub0;
        const totalDesc = Number(cfdi.descuento || 0);
        if (totalDesc > 0 && (subTotal16 + subTotal0) > 0) {
          desc16 = parseFloat((totalDesc * subTotal16 / (subTotal16 + subTotal0)).toFixed(6));
          desc0  = parseFloat(Math.max(0, totalDesc - desc16).toFixed(6));
        }
      }
    }
  }
  return { subTotal16, subTotal0, desc16, desc0 };
}

// Para CFDIs con `documentosRelacionados` (Bonificación/Devolución/
// Cancelación en cualquiera de sus variantes -- BON/BCT/DEV/CAC/etc. -- pero
// también facturas normales relacionadas a otro documento del ERP, ej.
// {Serie:"M0", Folio:"260701736"}), el CONCEPTO de la póliza (columna H) debe
// mostrar la serie-folio de esa primera referencia relacionada, sin importar
// cuál sea su Serie -- la columna C (serie) siempre lleva la serie-folio de
// la factura/CFDI propia, nunca la del documento relacionado (corregido
// 2026-07-24: el commit del 2026-07-23 que introdujo esta función lo aplicaba
// por error a la columna C en vez de la H, y además limitaba el marcador a
// una lista fija de Series -- confirmado con el usuario que cualquier
// documentosRelacionados debe reflejarse en H, no solo esa lista).
//
// Excepción agregada 2026-08-26 (caso real Hidalgo B0-260801157/B0-260801256):
// cuando la Serie de la referencia es la MISMA que la de la propia factura
// (ej. B0→B0) Y HAY MÁS DE UNA referencia (Factura Global, puede traer 1 o
// cientos), NO es un documento relacionado real -- son tickets internos de
// cajas ligados por el mecanismo de "factura PUE cobrada días después"
// (`ticketsPropioPorClave`, cfdi-poliza-generator.service.js). Tomar el
// PRIMERO al azar como si fuera "el" documento relacionado sobreescribía el
// concepto de TODA la factura con un ticket arbitrario, perdiendo el nombre
// del cliente en el proceso. Una referencia real a otro documento (ajuste,
// fusión) SIEMPRE trae una Serie DISTINTA a la propia (BON/DEV/CAC/M0/etc.)
// -- ese caso sigue mostrándose igual que antes.
//
// Corrección 2026-08-26 (mismo día, caso real Atzompa E0-260800126, ticket
// E0-260801137): cuando hay EXACTAMENTE UNA referencia y es de la misma
// Serie, NO es ruido de Factura Global -- es el ÚNICO ticket real de esta
// factura normal (mismo mecanismo `ticketsPropioPorClave`, pero sin
// ambigüedad posible al haber un solo candidato) -- sí debe mostrarse.
// Devuelve null si el CFDI no tiene documentosRelacionados (o solo trae
// VARIOS tickets internos de su propia serie sin poder distinguir cuál es
// el correcto), para que el llamador use la descripción del CFDI en el
// concepto como siempre.
function _referenciaDocRelacionado(documentosRelacionados, serieCfdiPropia) {
  const lista = documentosRelacionados || [];
  const esAmbiguo = lista.length > 1;
  for (const d of lista) {
    if (!d.Serie) continue;
    const mismaSerie = (d.Serie ?? '').toUpperCase() === (serieCfdiPropia ?? '').toUpperCase();
    if (mismaSerie && esAmbiguo) continue;
    // Folio vacío ("") es falsy -- sin este fallback, `!d.Folio` saltaba la
    // entrada completa y el concepto caía en la serie-folio de la factura
    // propia en vez de mostrar la referencia (encontrado 2026-07-23, folios
    // B0-260700408 y B0-260700785, ambos Serie='BCT' con Folio='').
    return d.Folio ? `${d.Serie}-${d.Folio}`.slice(0, 25) : d.Serie;
  }
  return null;
}

// Detecta si un `concepto` ya persistido en poliza_movimientos ES (o
// CONTIENE, con alguno de los prefijos/sufijos que cfdiToMovimientos le
// agrega a las líneas de IVA/ISR/Saldo -- "IVA - ", "IVA cobrado - ",
// "IVA ant. - ", "IVA ret. - ", "ISR ret. - ", "Saldo - ", "... (0%)") una
// referencia de documentosRelacionados (lo que `_referenciaDocRelacionado`
// devuelve, ej. "DEV-054861" o "M0-260701736") en vez de una descripción de
// producto -- usado en poliza.service.js (`enriquecerConceptoConCliente`)
// para saber si debe preservar la referencia en la columna H al reconstruir
// el concepto como "Cliente / ...", ya que a esa altura ya no se tiene
// `documentosRelacionados` a la mano (el movimiento viene de Postgres, no del
// CFDI en Mongo). Una referencia siempre tiene forma "Serie" o "Serie-Folio"
// (alfanumérico corto, sin espacios); una descripción de producto real
// siempre trae espacios -- esa es la señal que las distingue.
//
// Los prefijos se quitan por lista exacta (no con un recorte genérico de
// "... - ") porque una descripción de producto real puede traer un guion
// propio (ej. "Tubo galvanizado - 3/4"), y un recorte genérico hasta el
// último " - " confundiría la última palabra con un código.
const _PREFIJOS_CONCEPTO = ['IVA cobrado - ', 'IVA ant. - ', 'IVA ret. - ', 'ISR ret. - ', 'Saldo - ', 'IVA - '];
// `(-\d+)*-?` (en vez de `(-\d+)?`): acepta VARIOS anticipos concatenados
// ("OPA-00763-00665", ver `anticipoFolioRefGuard` en cfdi-poliza-generator.
// service.js) y un guion colgante final cuando alguno no resolvió folio
// todavía ("OPA-00763-") — sin esto, ese guion colgante rompía el match y
// `enriquecerConceptoConCliente` caía a mostrar la serie-folio de la venta
// en vez de la referencia OPA en la columna H (confirmado con el usuario
// 2026-08-25, caso real MONSAN B0-260801098).
const _REFERENCIA_REGEX  = /^[A-Z0-9]+(-\d+)*-?$/i;
function esConceptoMarcadorAjuste(concepto) {
  let nucleo = String(concepto || '');
  for (const prefijo of _PREFIJOS_CONCEPTO) {
    if (nucleo.startsWith(prefijo)) { nucleo = nucleo.slice(prefijo.length); break; }
  }
  if (nucleo.endsWith(' (0%)')) nucleo = nucleo.slice(0, -' (0%)'.length);
  return _REFERENCIA_REGEX.test(nucleo);
}

/**
 * Convierte un CFDI en movimientos contables usando la regla encontrada.
 * Si no hay regla, devuelve movimientos con cuentaId null (requieren revisión manual).
 */
async function cfdiToMovimientos(cfdi, rule, cuentaMapExterno = null, context = {}) {
  const tipo      = cfdi.tipoDeComprobante;
  const esIngreso = tipo === 'I';
  const esPago    = tipo === 'P';
  // Para NC (tipo E): el criterio PPD/PUE es el de la VENTA ORIGINAL que
  // ajusta (context.metodoPagoRelacionado, pre-fetch del generator), no el
  // metodoPago propio de la NC — pueden no coincidir (confirmado con el
  // usuario: una NC "Efectivo/PUE" puede estar ajustando una factura PPD
  // nunca cobrada; en ese caso el IVA debe cancelarse contra cuentaIvaPPD,
  // no contra cuentaIva, Y el movimiento debe clasificarse como PPD/Crédito
  // en la póliza — no solo la cuenta de IVA, ver `satMeta.metodoPago` abajo).
  // Sin ese dato (I/P, o E sin relación resuelta), cae al metodoPago propio
  // del CFDI, comportamiento previo sin cambios.
  const metodoPagoResuelto = (tipo === 'E' && context.metodoPagoRelacionado)
    ? context.metodoPagoRelacionado
    : cfdi.metodoPago;
  const esPPD     = metodoPagoResuelto === 'PPD';

  // Para CFDI tipo P, total y subTotal son siempre 0 por diseño SAT.
  // Los montos reales viven en el complemento de pago.
  //
  // Complemento de Pago 2.0 (CFDI 4.0): usa el nodo <Totales> con MontoTotalPagos.
  // Complemento de Pago 1.0 (CFDI 3.3): no tiene <Totales> → suma pagos[].monto.
  // Descarga Metadata (>5 días): no tiene complementoPago → usar cfdi.total (viene del SAT metadata).
  const cpTotales = cfdi.complementoPago?.totales;
  const cpPagos   = cfdi.complementoPago?.pagos ?? [];

  const _cpTotal = esPago
    ? (cpTotales
        ? Number(cpTotales.montoTotalPagos || 0)
        : cpPagos.length > 0
          ? cpPagos.reduce((s, p) => s + Number(p.monto || 0), 0)
          : Number(cfdi.total || 0))   // fallback: total del SAT Metadata
    : Number(cfdi.total || 0);

  const subtotal = esPago
    ? (cpTotales
        ? Number(cpTotales.totalTrasladosBaseIVA16 || 0) +
          Number(cpTotales.totalTrasladosBaseIVA8  || 0)
        : _cpTotal)   // sin desglose de base: usar monto completo
    : (() => {
        const raw = Number(cfdi.subTotal || 0);
        if (raw > 0) return raw;
        // Metadata SAT: subTotal no viene en la descarga; derivar de total menos IVA conocido.
        const ivaKnown = Number(cfdi.impuestos?.totalImpuestosTrasladados || 0);
        return parseFloat((Number(cfdi.total || 0) - ivaKnown).toFixed(2));
      })();

  const total = _cpTotal;

  // Para tipo P el SAT exige Total=0 y SubTotal=0 en el header; los importes reales
  // están en complementoPago.totales (CP 2.0), pagos[].monto (CP 1.0), o cfdi.total (Metadata).
  const iva = esPago
    ? (cpTotales
        ? Number(cpTotales.totalTrasladosImpuestoIVA16 || 0) +
          Number(cpTotales.totalTrasladosImpuestoIVA8  || 0)
        : cpPagos.reduce((sum, pago) =>              // CP 1.0: IVA por DoctoRelacionado en trasladosDR
            sum + (pago.doctosRelacionados ?? []).reduce((s2, dr) =>
              s2 + (dr.trasladosDR ?? [])
                .filter(t => (t.impuesto || t.Impuesto || '') === '002' &&
                             Number(t.tasaOCuota ?? t.TasaOCuota ?? 0) > 0)
                .reduce((s3, t) => s3 + Number(t.importe || t.importeDR || t.ImporteDR || 0), 0)
            , 0)
          , 0))
    : Number(cfdi.impuestos?.totalImpuestosTrasladados || 0);

  const ivaRet = esPago
    ? Number(cpTotales?.totalRetencionesImpuestoIVA || 0)
    : Number(cfdi.impuestos?.totalImpuestosRetenidos || 0);

  // Artefacto metadata SAT: ivaRet ≈ ivaTras pero retenciones:[] → no hay retención real.
  // El gap (subTotal + iva − total) es un descuento implícito de conceptos que NUMO
  // infiere sin necesidad del XML. Permite generar el asiento bruto igual que el ERP.
  const esMetadataConDescuento = !esPago &&
    (cfdi.conceptos || []).length === 0 &&
    (cfdi.impuestos?.retenciones || []).length === 0 &&
    ivaRet > 0 && Math.abs(ivaRet - iva) < 1.0 &&
    (subtotal + iva - total) > 0.5;

  // ISR retenido: tipo P lo reporta en complementoPago.totales; tipo I/E en retenciones del header.
  // cfdi.impuestos está vacío para tipo P por especificación SAT CFDI 4.0.
  const isrRet = esPago
    ? Number(cpTotales?.totalRetencionesImpuestoISR || 0)
    : Number(
        (cfdi.impuestos?.retenciones ?? [])
          .filter(r => r.impuesto === '001')
          .reduce((s, r) => s + Number(r.importe || 0), 0),
      );

  // Para CFDIs emitidos (I=ingreso, E=nota crédito, P=cobro) la empresa es siempre
  // el emisor → el tercero para DIOT es el receptor (cliente).
  // Para CFDIs recibidos (gastos) la empresa es el receptor → el tercero es el emisor.
  const esEmitido = ['I', 'E', 'P'].includes(tipo);
  const rfcTercero = cfdi.emisor?.rfc === cfdi.receptor?.rfc
    ? null
    : esEmitido
      ? cfdi.receptor?.rfc
      : cfdi.emisor?.rfc;

  // Descripcion: algunos documentos tienen el campo en minúsculas (schema Mongoose)
  // y otros con mayúscula inicial (como viene del XML del SAT)
  const descRaw        = cfdi.conceptos?.[0]?.descripcion || cfdi.conceptos?.[0]?.Descripcion || '';
  // CFDI con documentosRelacionados (ajuste -- Bonificación/Devolución/
  // Cancelación en cualquiera de sus variantes -- o cualquier otra referencia
  // del ERP a otro documento): el concepto (columna H) muestra esa referencia
  // en vez de la descripción de producto -- ver `_referenciaDocRelacionado`.
  const marcadorAjuste = _referenciaDocRelacionado(cfdi.documentosRelacionados, cfdi.serie);
  const concepto    = marcadorAjuste
    ?? (descRaw.trim() ? descRaw.trim().slice(0, 200) : `CFDI ${tipo} ${cfdi.uuid?.slice(0, 8)}`);
  // Ticket real único (mismo criterio que `_referenciaDocRelacionado`: un solo
  // `documentosRelacionados` de la misma Serie no es ruido de Factura Global,
  // es el ticket real de esta factura normal cobrada días después) -- se
  // expone como `serieVentaTicket`/`folioVentaTicket` en `satMeta` (ver
  // abajo) para que `armarIndividual` (consolidarCargos, poliza.service.js)
  // muestre en columna H "cliente / ticket interno" en vez de "cliente /
  // factura propia" en los renglones de Anticipo/SF (confirmado con el
  // usuario 2026-08-26, caso real E0-260800126/E0-260801137).
  const _docsRelPropios = cfdi.documentosRelacionados || [];
  const ticketRealUnico = (_docsRelPropios.length === 1 && _docsRelPropios[0]?.Serie
    && (_docsRelPropios[0].Serie ?? '').toUpperCase() === (cfdi.serie ?? '').toUpperCase())
    ? _docsRelPropios[0] : null;
  const centroCosto = rule?.centroCosto ?? '';
  // Fecha del CFDI como fecha de venta en formato YYYY-MM-DD
  const ventaFecha  = cfdi.fecha ? new Date(cfdi.fecha).toISOString().slice(0, 10) : null;
  // Serie del CFDI como referencia (serie+folio si existen) -- SIEMPRE la
  // propia serie-folio de la factura/CFDI (columna C), nunca el marcador de
  // ajuste (ese va en `concepto`, columna H -- ver arriba).
  const serieCfdi   = [cfdi.serie, cfdi.folio].filter(Boolean).join('-').slice(0, 25) || null;
  // Concepto específico para la línea "Venta Sin Cobro" (columna H): cuando
  // `_prefetchAjustesFacturaPropia` encontró que el dinero de este ticket
  // está atribuido en el ERP a OTRA factura (`context.atribuidoOtraFactura`,
  // ej. "B0-260801321"), se muestra esa factura real en vez del concepto
  // genérico de esta factura -- confirmado con el usuario 2026-08-26, caso
  // real B0-260801256 ($6,207.20, ticket B0-260802904 atribuido a B0-260801321)
  // -- mucho más rastreable para cajas/facturación que el ticket o el folio
  // propio. Si no hay atribución conocida, se usa el concepto normal.
  const conceptoVentaSinCobro = context.atribuidoOtraFactura ?? concepto;

  if (!rule) {
    return [
      { cuentaId: null, concepto, centroCosto: '', debe: total,  haber: 0,    cfdiUuid: cfdi.uuid, rfcTercero, _sinRegla: true },
      { cuentaId: null, concepto, centroCosto: '', debe: 0,      haber: total, cfdiUuid: cfdi.uuid, rfcTercero, _sinRegla: true },
    ];
  }

  // Resolver cuentaId a partir del código
  const codigos = [
    rule.cuentaCargo,
    rule.cuentaAbono,
    rule.cuentaAbono2,
    rule.cuentaIva,
    rule.cuentaIvaPPD,
    rule.cuentaIvaRetenido,
    rule.cuentaIsrRetenido,
    rule.cuentaIvaAnticipo,
    rule.cuentaDeltaAnticipo,
    rule.cuentaDescuento,
    rule.cuentaDescuento0,
    rule.cuentaCargo2,
  ].filter(Boolean);

  let cuentaMap = cuentaMapExterno;
  if (!cuentaMap) {
    const cuentas = await AccountPlan.findAll({
      where: { codigo: { [Op.in]: codigos } },
      attributes: ['id', 'codigo'],
    });
    cuentaMap = Object.fromEntries(cuentas.map(c => [c.codigo, c.id]));
  }

  const movs = [];

  // ── Flags para lógica especializada ───────────────────────────────────────
  // esAnticipo (Reg 22C, Reg 23): el cargo cancela un pasivo → usa subtotal;
  //   el IVA hace swap: DEBE cuentaIvaAnticipo + HABER cuentaIva.
  // esIvaHaber (Reg 19): NC correctiva que actúa como ingreso →
  //   montoCargo=total, IVA va al HABER, montoAbono=subtotal.
  // esAplicacionSaldo (Reg 24B/25B): el cargo se divide entre saldo a favor
  //   (cuentaCargo) y efectivo/banco (cuentaCargo2).
  const esAnticipo        = !!(rule.cuentaIvaAnticipo && iva > 0);
  const esIvaHaber        = !!(rule.ivaHaber === true && tipo === 'E');
  const esAplicacionSaldo = !!(rule.esAplicacionSaldo && context.saldoDisponible != null);

  // Split de Pago (Cargo Y Abono) por FACTURA liquidada — solo Pagos (nunca
  // Ingreso/Egreso), confirmado con el usuario 2026-08-11. `context.doctosPago`
  // viene de `_prefetchDoctosPago` (cfdi-poliza-generator.service.js): cada
  // factura que este Complemento de Pago liquida forma su propio asiento
  // completo (Cargo + Abono), en vez de un solo Cargo agregado para todo el
  // Pago. No aplica junto con `tasaIva==='mixto'` (ese motor ya parte el
  // abono por tasa) ni con `esAnticipo`/`esAplicacionSaldo` (casos distintos,
  // fuera de alcance por ahora).
  const esSplitPagoPorFactura = esPago && !esAnticipo && !esAplicacionSaldo && rule.tasaIva !== 'mixto'
    && Array.isArray(context.doctosPago) && context.doctosPago.length > 0;

  // H1: para Reg 22C cuando factura final > anticipo, el cargo debe cancelar SOLO el pasivo del
  // anticipo (no el subtotal completo de la factura). El IVA diferido a cancelar también es solo
  // el del anticipo. El exceso va a Bancos via el bloque delta.
  // context.ivaRelacionado y context.totalRelacionado son pre-fetched por el generator.
  // Si no están disponibles (balanza, o anticipo 1:1), cae al comportamiento original.
  const ivaCancelado     = (esAnticipo && context.ivaRelacionado     != null) ? context.ivaRelacionado                          : iva;
  const subtotalAnticipo = (esAnticipo && context.totalRelacionado   != null) ? context.totalRelacionado - ivaCancelado          : subtotal;

  // Línea principal cargo
  // esAnticipo + tipo I/E: cargo cancela el pasivo del anticipo → subtotalAnticipo
  // esAnticipo + tipo P: cargo aplica el saldo completo del monedero → total
  // esAplicacionSaldo: cargo = min(saldoDisponible, total) — el resto en cuentaCargo2
  // Egreso normal: cargo = subtotal − descuento (netear igual que el abono de un ingreso)
  let montoCargo;
  if (esAnticipo)        montoCargo = esPago ? total : subtotalAnticipo;
  else if (esIvaHaber)   montoCargo = total;
  else                   montoCargo = (esIngreso || esPago) ? total : parseFloat((subtotal - Number(cfdi.descuento || 0)).toFixed(2));

  if (esAplicacionSaldo) {
    const saldoAplicado = parseFloat(Math.min(context.saldoDisponible, montoCargo).toFixed(2));
    const cashPago      = parseFloat((montoCargo - saldoAplicado).toFixed(2));
    montoCargo = saldoAplicado;
    // Cargo secundario: efectivo/banco por el monto no cubierto por el saldo
    if (cashPago > 0 && rule.cuentaCargo2) {
      movs.push({
        cuentaId:    cuentaMap[rule.cuentaCargo2] ?? null,
        concepto,
        centroCosto,
        ventaFecha,
        serie:       serieCfdi,
        debe:        cashPago,
        haber:       0,
        cfdiUuid:    cfdi.uuid,
        rfcTercero,
        _saldoUsado: saldoAplicado,   // metadata para que el generator actualice saldoRestante
      });
    }
  }

  // Split del cargo por forma de pago REAL (2026-08-06): el `formaPago` que
  // declara el CFDI ante el SAT con frecuencia viene mal/genérico — el
  // desglose real de cobro en cajas (`context.desglosePagoReal`, ver
  // `_prefetchDesglosePagoReal` en cfdi-poliza-generator.service.js) es la
  // fuente confiable. Solo aplica cuando la cuenta que ya seleccionó la
  // regla es una de las genéricas "por identificar" (Caja/Bancos) — las
  // ÚNICAS cuya selección depende realmente de `formaPago`; cualquier otra
  // cuenta (Devoluciones, Anticipos, un banco real ya específico, etc.) no
  // se toca. Alcance de esta primera versión (confirmado con el usuario):
  // SOLO tipo I, SOLO el caso normal — `esAnticipo`/`esAplicacionSaldo`
  // ya tocan el cargo por su cuenta y colisionarían con un split aquí (esos
  // mecanismos usan `movs.find(...)` para localizar "la" línea de cargo,
  // asumiendo que hay una sola — ver `_esCargoPrincipal` más abajo, que le
  // da al caller una señal explícita en vez de esa reconstrucción frágil).
  //
  // Corrección 2026-08-21 (caso real Hidalgo B0-260800846, Factura Global
  // $194,749.74, regla "Reg 12A — Venta Mixta PUE Efectivo"): antes
  // `tasaIva==='mixto'` bloqueaba el split por completo — el cargo entero se
  // iba a Efectivo (el `formaPago` genérico que declara toda Factura Global)
  // aunque el desglose real de cobro mostrara Efectivo/Tarjeta/Transferencia
  // mezclados, escondiendo ~$113,000 de Tarjeta+Transferencia. El split por
  // tasa (16%/0%, líneas 1263+ más abajo) SOLO toca el ABONO (ingreso
  // reconocido) — es ortogonal al split por forma de pago del CARGO — así
  // que ambos pueden convivir. La única colisión real es con el ajuste
  // OPCIONAL de cargo `rule.cuentaCargoMixto0` (línea ~1284), que asume una
  // sola línea de cargo vía `movs.find(...)` — se excluye ESE caso
  // específico (`!rule.cuentaCargoMixto0`) para no romperlo; ninguna regla
  // real usa ambos mecanismos a la vez hasta donde se ha confirmado.
  const gateBase = !esAnticipo && !esAplicacionSaldo
    && (rule.tasaIva !== 'mixto' || (esIngreso && !rule.cuentaCargoMixto0))
    && CODIGOS_CUENTAS_CAJA_O_BANCO.has(rule.cuentaCargo);

  // Ajuste por Saldo a Favor/Puntos REALMENTE usados en esta factura —
  // corrección del mismo día (2026-08-06): ya NO se detectan escaneando
  // `formasPago` de /desgloses-cobro/almacen (ver nota junto a las
  // constantes, arriba). `montoSFUsado` viene de `/saldos-favor`
  // (`saldosFavorUsados[].montoUsado`, autoritativo) y `montoPuntosUsado` de
  // los `cobros[]` con `serieOrigen: 'CBT'` de /desgloses-cobro/almacen
  // (ambos ya resueltos por el generator antes de llamar esta función — ver
  // `_prefetchSaldoFavorUsadoPropio`/`_prefetchDesglosePagoReal` en
  // cfdi-poliza-generator.service.js).
  const montoSFUsado     = Number(context.saldoFavorUsadoPropio?.monto) || 0;
  const montoSFOculto    = Number(context.saldoFavorUsadoPropio?.montoOculto) || 0;
  const montoSFVisible   = Number(context.saldoFavorUsadoPropio?.montoVisible) || 0;
  // Cada origen NO ocultable (periodo anterior) por separado, con su propia
  // referencia (serieOrigen-folioOrigen) — ver comentario en `emitirLineaSF`.
  const detalleSFVisible = context.saldoFavorUsadoPropio?.detalleVisible ?? [];
  const montoPuntosUsado = Number(context.montoPuntosUsado) || 0;
  // Monto REAL de anticipo aplicado (ver `_prefetchAjustesFacturaPropia`,
  // `context.montoAnticipoUsado`) — mismo dato que usa el cierre OPA en
  // `cfdi-poliza-generator.service.js`, aquí se usa para separar el
  // remanente real (ver `esCasoCargoAnticipoConRemanenteReal` abajo).
  const montoAnticipoUsado = Number(context.montoAnticipoUsado) || 0;

  const esCasoAjusteSFPuntos = gateBase && (montoSFUsado > 0 || montoPuntosUsado > 0)
    && cuentaMap[CODIGO_CUENTA_SALDO_FAVOR] && cuentaMap[CODIGO_CUENTA_IVA_SALDO_FAVOR] && cuentaMap[CODIGO_CUENTA_CLUB_TUBEROS];

  // Caso espejo de `esCasoAjusteSFPuntos` (2026-08-26, confirmado con el
  // usuario, caso real Atzompa/E0 11-ago, Reg 22C id 406, factura
  // E0-260800110 $1,355.92 = $1,308.61 saldo a favor + $47.31 efectivo real):
  // cuando `rule.cuentaCargo` YA es una cuenta de Saldo a Favor/pasivo (no
  // Caja/Bancos — reglas tipo "formaPago 30", fuera de `gateBase`) pero el
  // ticket se pagó SOLO PARCIALMENTE con ese saldo, el resto (real, según
  // `desglosePagoReal`) nunca llegaba a Caja/Bancos — el Cargo completo se
  // iba a la cuenta de SF. Aquí el cargo "por defecto" ya es la cuenta de SF
  // (al revés de `esCasoAjusteSFPuntos`, donde el default es Caja/Bancos), así
  // que se separa el remanente REAL (no cubierto por SF) hacia Caja/Bancos vía
  // `splitPorFormaPagoReal`, dejando en `cuentaCargo` solo lo realmente
  // cubierto por saldo a favor.
  const esCasoCargoSFConRemanenteReal = !esAnticipo && !esAplicacionSaldo && !esPago
    && !CODIGOS_CUENTAS_CAJA_O_BANCO.has(rule.cuentaCargo)
    && montoSFUsado > 0.01 && montoSFUsado < montoCargo - 0.01
    && Array.isArray(context.desglosePagoReal) && context.desglosePagoReal.length > 0
    && cuentaMap[CODIGO_CUENTA_CAJA] && cuentaMap[CODIGO_CUENTA_BANCOS];

  // Caso espejo de `esCasoCargoSFConRemanenteReal`, pero para Anticipo en vez
  // de Saldo a Favor (2026-08-31, confirmado con el usuario, caso real
  // ESCUELA PRIMARIA VESPERTINA CARLOS A. CARRILLO H0-260800539: $177.97 =
  // $135.98 anticipo + $41.99 efectivo real). Reg 22C — "Factura Final
  // Anticipo" (formaPago=30) tiene `cuentaCargo` apuntando a Anticipos de
  // Clientes (pasivo, fuera de `gateBase`), igual que el caso de SF — cuando
  // el ticket se cubrió SOLO PARCIALMENTE con el anticipo, el remanente real
  // (según `desglosePagoReal`) se separa hacia Caja/Bancos vía
  // `splitPorFormaPagoReal`, dejando en `cuentaCargo` solo lo realmente
  // aplicado del anticipo (el cierre OPA en `cfdi-poliza-generator.service.js`
  // luego reduce esa porción a 0 y la reemplaza por sus propias líneas de
  // Cargo Anticipos/IVA-Anticipo — ver `esLineaCargoDeLaReglaGuard/Prop` ahí).
  const esCasoCargoAnticipoConRemanenteReal = !esAnticipo && !esAplicacionSaldo && !esPago
    && !CODIGOS_CUENTAS_CAJA_O_BANCO.has(rule.cuentaCargo)
    && montoAnticipoUsado > 0.01 && montoAnticipoUsado < montoCargo - 0.01
    && Array.isArray(context.desglosePagoReal) && context.desglosePagoReal.length > 0
    && cuentaMap[CODIGO_CUENTA_CAJA] && cuentaMap[CODIGO_CUENTA_BANCOS];

  // Suma de las formasPago reales encontradas — calculada ANTES del gate
  // porque el gate mismo la necesita (ver comentario abajo).
  const totalFormasPagoReal = Array.isArray(context.desglosePagoReal)
    ? context.desglosePagoReal.reduce((s, fp) => s + (Number(fp.monto) || 0), 0)
    : 0;
  // Verificación de suma exacta (2026-08-07, QUITADA 2026-08-14): existió acá
  // porque el desglose real de cajas puede venir PARCIAL — caso real donde un
  // cobro CBT de $87.79 (Puntos) era el ÚNICO encontrado para una factura de
  // $1,023.63, y el split proporcional le atribuía el Cargo COMPLETO a
  // Puntos. Pero exigir cuadre exacto (<0.02) resultó DEMASIADO estricto para
  // Facturas Globales grandes (caso real Hidalgo B0-260701074, 09/07/2026:
  // factura de $201,995.71 con desglose real de $170,386.71 — solo 15.6% de
  // "ruido" de reclasificación del ERP, mismo fenómeno ya documentado para
  // Atzompa — caía al fallback de una sola línea, escondiendo $31,609 del
  // consolidado real de Efectivo/Tarjeta). Se quita la verificación (mismo
  // criterio ya usado en el remanente de `esCasoAjusteSFPuntos`, confirmado
  // con el usuario 2026-08-14): partir proporcional con lo que se encontró es
  // preferible a mandar la factura completa a una sola forma de pago.
  const esCasoNormalParaSplit = !esCasoAjusteSFPuntos && gateBase
    && Array.isArray(context.desglosePagoReal) && context.desglosePagoReal.length > 0
    && cuentaMap[CODIGO_CUENTA_CAJA] && cuentaMap[CODIGO_CUENTA_BANCOS];

  if (_DEBUG_SPLIT_PAGO) {
    _debugLogger.warn(`[DEBUG_SPLIT] ${serieCfdi}-${cfdi.folio} uuid=${cfdi.uuid} total=${cfdi.total} formaPagoCFDI=${cfdi.formaPago} `
      + `gateBase=${gateBase} esAnticipo=${!!esAnticipo} esAplicacionSaldo=${!!esAplicacionSaldo} tasaIvaMixto=${rule.tasaIva === 'mixto'} `
      + `cuentaCargoRegla=${rule.cuentaCargo} montoCargo=${montoCargo} `
      + `montoSFUsado=${montoSFUsado} montoPuntosUsado=${montoPuntosUsado} `
      + `desglosePagoRealLen=${Array.isArray(context.desglosePagoReal) ? context.desglosePagoReal.length : 'N/A'} `
      + `totalFormasPagoReal=${totalFormasPagoReal} esCasoAjusteSFPuntos=${esCasoAjusteSFPuntos} esCasoNormalParaSplit=${esCasoNormalParaSplit}`);
  }

  // Reparte `montoAPartir` entre Efectivo/Tarjeta usando `context.desglosePagoReal`
  // — extraído del bloque `esCasoNormalParaSplit` (2026-08-14) para poder
  // reutilizarlo también en el remanente de `esCasoAjusteSFPuntos` (ver ahí).
  // `extraEnUltima` se adjunta SOLO a la última línea empujada (mismo lugar
  // que ya absorbe el residuo de redondeo) — usado para no perder el
  // marcador `_puntosUsado` cuando el remanente también se parte.
  // `reglaNombreOverride` (2026-08-26, ver `esCasoCargoSFConRemanenteReal`):
  // por defecto estas líneas heredan el nombre de LA REGLA completa (via
  // `satMeta`, al final de esta función) — correcto cuando la regla ya es una
  // Venta normal, pero para Reg 22C ("...Anticipo...") el remanente real NO
  // es un anticipo, y `consolidarCargos`/`esReglaAnticipo` (poliza.service.js)
  // lo sacaría del consolidado de Efectivo/Tarjeta solo por el texto
  // "Anticipo" en el nombre de la regla. Se fuerza un nombre neutro que
  // sobrevive al spread de `satMeta` (mismo mecanismo que `_formaPagoReal`).
  const splitPorFormaPagoReal = (montoAPartir, extraEnUltima = {}, reglaNombreOverride = null) => {
    const formasPagoReal = context.desglosePagoReal;
    formasPagoReal.forEach((fp, idx) => {
      const esUltimo = idx === formasPagoReal.length - 1;
      // Monto REAL de cada ticket, tal cual, SIN ajustar ni reescalar ninguna
      // línea para forzar el cierre contra `montoAPartir` (confirmado con el
      // usuario 2026-08-19: si la Factura Global no cierra 1 a 1 por "ruido"
      // de reclasificación del ERP, se acepta desbalanceada en vez de restarle
      // a un ticket puntual — la línea anterior que absorbía el residuo en la
      // última línea podía dejarla en negativo y perderse por completo, caso
      // real CONSTRUCASA 13-ago, ticket C0-260802371).
      const montoLinea = Math.round((Number(fp.monto) || 0) * 100) / 100;
      if (montoLinea <= 0) return;

      const esEfectivo = (fp.claveSat ?? '').trim() === '01';
      movs.push({
        cuentaId:    esEfectivo ? cuentaMap[CODIGO_CUENTA_CAJA] : cuentaMap[CODIGO_CUENTA_BANCOS],
        concepto, centroCosto, ventaFecha, serie: serieCfdi,
        debe:        montoLinea,
        haber:       0,
        cfdiUuid:    cfdi.uuid,
        rfcTercero,
        _esCargoPrincipal: true,
        // Ver comentario más abajo (bloque `esCasoNormalParaSplit`) sobre por
        // qué esta línea concreta debe llevar el claveSat REAL, no el del CFDI.
        _formaPagoReal: (fp.claveSat ?? '').trim() || null,
        ...(reglaNombreOverride ? { _reglaNombreReal: reglaNombreOverride } : {}),
        // Ticket real (cajas) al que pertenece esta porción — ver comentario
        // en `_prefetchAjustesFacturaPropia`. `consolidarCargos` lo usa para
        // resolver la autorización real de Tarjeta POR TICKET vía
        // bank_movements.erpLinks, nunca por CFDI completo (evita el bug de
        // Facturas Globales de Hidalgo 2026-08-14).
        _serieVentaTicket: fp.serieVentaTicket ?? null,
        _folioVentaTicket: fp.folioVentaTicket ?? null,
        // `yaContabilizadoOtroDia` (ver `_prefetchAjustesFacturaPropia`,
        // 2026-08-25): este cobro real ya se contabilizó en su día REAL vía
        // "Cobros sin factura" (`_cobrosSinFacturaPorCentro`) — se oculta
        // este Cargo (día de la factura) de "Depósitos consolidados" para no
        // duplicar el dinero entre los dos días. El Abono Ingresos/IVA de
        // esta misma factura queda visible sin cambios (la venta sí ocurrió
        // y se facturó aquí, solo el lado de caja ya se contó antes).
        ...(fp.yaContabilizadoOtroDia ? { tipoOrigen: 'Cobro Sucursal', reglaNombre: ETIQUETA_COBRO_YA_CONTABILIZADO } : {}),
        ...(esUltimo ? extraEnUltima : {}),
      });
    });
  };

  if (esSplitPagoPorFactura) {
    // Asiento completo (Cargo + IVA cobrado + Abono) POR FACTURA liquidada —
    // para que las líneas de una misma factura queden CONSECUTIVAS en vez de
    // en tres pasadas separadas (todos los Cargos, luego todo el IVA cobrado,
    // luego todos los Abonos), se difiere todo a un solo bucle unificado más
    // abajo, junto al Abono (confirmado con el usuario 2026-08-11: los
    // renglones de cada factura deben verse juntos). Ver ese bloque para el
    // detalle de Cargo/SF/IVA — aquí no se empuja nada.
  } else if (esCasoAjusteSFPuntos) {
    let restante = montoCargo;
    // Saldo a Favor: individual por cliente/factura (confirmado con el
    // usuario 2026-08-06) — mismo patrón que un cruce real de sucursal, pero
    // con `tipoOrigen: TIPO_ORIGEN_CARGO_ESPECIAL` (distinto a propósito, ver
    // comentario en la constante) para no confundirse con
    // `_uuidsConCargoCubiertoEnBD`.
    if (montoSFUsado > 0) {
      // Split por origen (2026-08-17): una Factura Global puede combinar SF
      // ocultable (generado/usado mismo día-almacén) con SF no-ocultable (de
      // un periodo anterior) — cada porción se emite como su propia línea
      // con su propio reglaNombre, en vez de una sola línea todo-o-nada (ver
      // comentario en `_prefetchAjustesFacturaPropia`/generarPropuesta,
      // cfdi-poliza-generator.service.js).
      const nombreCliente = cfdi.receptor?.nombre ?? 'CLIENTE NO IDENTIFICADO';
      const conceptoCliente = [nombreCliente, serieCfdi].filter(Boolean).join(' / ');
      const baseSF = { centroCosto, ventaFecha, serie: serieCfdi, haber: 0, cfdiUuid: cfdi.uuid, rfcTercero, concepto: conceptoCliente, tipoOrigen: TIPO_ORIGEN_CARGO_ESPECIAL, _esCargoPrincipal: true };
      const emitirLineaSF = (montoBruto, reglaNombre, overrides = {}) => {
        if (montoBruto <= 0 || restante <= 0) return;
        const monto = Math.min(montoBruto, restante);
        const subtotal = Math.round((monto / (1 + TASA_IVA_SALDO_FAVOR)) * 100) / 100;
        const iva = Math.round((monto - subtotal) * 100) / 100;
        movs.push({ ...baseSF, ...overrides, reglaNombre, cuentaId: cuentaMap[CODIGO_CUENTA_SALDO_FAVOR], debe: subtotal });
        movs.push({ ...baseSF, ...overrides, reglaNombre, cuentaId: cuentaMap[CODIGO_CUENTA_IVA_SALDO_FAVOR], debe: iva });
        restante = parseFloat((restante - monto).toFixed(2));
      };
      emitirLineaSF(montoSFOculto, 'SF-OCULTO');
      // Visible (de periodo anterior): una línea POR ORIGEN, con su propia
      // referencia (serieOrigen-folioOrigen, ej. "DEV-055991") en vez de
      // combinar todo bajo el concepto del CFDI actual — sin esto, dos
      // devoluciones de meses distintos (ej. julio y junio) se veían como un
      // solo monto sin poder rastrear de cuál venía cada porción (confirmado
      // con el usuario 2026-08-18, caso real Global 89CF6A7F: DEV-055991 +
      // CAC-075406). Si por lo que sea no hay detalle disponible (dato viejo
      // sin `detalleVisible`), cae al comportamiento anterior (una sola línea
      // con el concepto del CFDI actual).
      if (detalleSFVisible.length > 0) {
        for (const d of detalleSFVisible) {
          // Serie y folio de la VENTA que generó el saldo (auditable en
          // cajas) — nunca el marcador DEV/CAC (ese no es un documento que se
          // pueda buscar como "serie y folio interno", solo identifica el
          // TIPO de ajuste). Si por lo que sea no viene la venta, cae al
          // marcador como último recurso (mejor que dejarlo vacío).
          const referenciaVenta = [d.ventaSerie, d.ventaFolio].filter(Boolean).join('-')
            || [d.serieOrigen, d.folioOrigen].filter(Boolean).join('-') || null;
          // Nota informativa del sobrante (confirmado con el usuario
          // 2026-09-01): NO cambia el `debe` real de la línea (eso sigue
          // siendo lo realmente usado, ver `emitirLineaSF` arriba) — solo se
          // anota en el concepto cuánto le queda al cliente de este origen
          // después de este uso, si es que queda algo (`saldoSobrante` viene
          // del ERP, ver `_prefetchAjustesFacturaPropia`). Si el dato no vino
          // (registro viejo) o ya cerró en $0, no se anota nada.
          const notaSobrante = (Number.isFinite(d.saldoSobrante) && d.saldoSobrante > 0.01)
            ? ` (saldo disponible: $${d.saldoSobrante.toFixed(2)})`
            : '';
          emitirLineaSF(Math.abs(Number(d.monto) || 0), 'SF', {
            serie: referenciaVenta,
            concepto: [nombreCliente, referenciaVenta].filter(Boolean).join(' / ') + notaSobrante,
          });
        }
      } else {
        emitirLineaSF(montoSFVisible, 'SF');
      }
    }
    // Puntos/Club Tuberos: a diferencia de SF, va CONSOLIDADO en una sola
    // línea genérica por sucursal/día ("CLIENTE DE MOSTRADOR SUC. X"), no
    // individual (confirmado con el usuario 2026-08-06 — revirtió la
    // decisión anterior sobre Puntos, Saldo a Favor sigue individual). Esta
    // función no arma esa línea consolidada (no tiene visibilidad del resto
    // del batch) — solo marca `_puntosUsado` en la línea de remanente para
    // que el generator la acumule y agregue una sola vez al final del batch
    // (ver `puntosAcumulados` en cfdi-poliza-generator.service.js).
    const montoPuntosAplicado = Math.min(montoPuntosUsado, restante);
    if (montoPuntosAplicado > 0) {
      restante = parseFloat((restante - montoPuntosAplicado).toFixed(2));
    }
    // Remanente: lo que sigue después de SF/Puntos. Corrección 2026-08-14
    // (caso real Atzompa, Factura Global de mostrador con Puntos usados):
    // antes esto SIEMPRE se mandaba completo a la cuenta por defecto de la
    // regla (Efectivo/Caja), aunque `context.desglosePagoReal` sí trajera un
    // desglose real de Efectivo/Tarjeta.
    // A diferencia de `esCasoNormalParaSplit` (que exige que la suma cuadre
    // casi exacto contra `montoCargo`, por el caso real de un desglose
    // gravemente INCOMPLETO que le atribuía el Cargo completo a Puntos), aquí
    // NO se exige ese cuadre exacto (confirmado con el usuario 2026-08-14,
    // caso real Atzompa: la consulta "por centro" del ERP trae cobros CBT que
    // parecen incluir eventos de reclasificación/corrección, dejando un
    // sobrante ~1.7% sin explicar frente a `restante` — no se pudo aislar la
    // causa exacta, pero partir proporcional sigue siendo preferible a mandar
    // todo el remanente a una sola forma de pago). El monto total del
    // remanente NUNCA cambia (el prorrateo ancla en `restante`, no en la suma
    // del desglose — ver `splitPorFormaPagoReal`), así que esto solo afecta
    // la PROPORCIÓN Efectivo/Tarjeta, nunca el cuadre del asiento.
    //
    // Corrección 2026-08-16 (caso real Ferrocarril F0|260800117):
    // cuando `restante > totalFormasPagoReal`, prorratear `restante` directo
    // infla cada renglón dp por encima de su monto real — la diferencia es
    // la cobranza de otra sucursal (incluida en montoCargo vía
    // facturasVendedorCubiertas pero AUSENTE del desglosePagoReal de este
    // centro). Se empuja primero el exceso como línea CAJA aparte para que
    // la reducción greedy de montoCubierto la consuma antes que los renglones
    // dp; luego se distribuyen esos renglones con sus montos ABSOLUTOS
    // (splitPorFormaPagoReal anclado en totalFormasPagoReal, no en restante).
    // El total del asiento no cambia.
    const remanenteConfiableParaSplit = Array.isArray(context.desglosePagoReal) && context.desglosePagoReal.length > 0
      && cuentaMap[CODIGO_CUENTA_CAJA] && cuentaMap[CODIGO_CUENTA_BANCOS];
    const extraRemanente = montoPuntosAplicado > 0 ? { _puntosUsado: montoPuntosAplicado } : {};
    if (remanenteConfiableParaSplit && restante > 0) {
      const excesoCubrir = totalFormasPagoReal > 0
        ? parseFloat((restante - totalFormasPagoReal).toFixed(2))
        : 0;
      if (_DEBUG_SPLIT_PAGO) {
        _debugLogger.warn(`[DEBUG_SPLIT_SFPUNTOS] ${serieCfdi}-${cfdi.folio} restante=${restante} totalFormasPagoReal=${totalFormasPagoReal} `
          + `excesoCubrir=${excesoCubrir} -> ${excesoCubrir > 0.01 && totalFormasPagoReal > 0 ? 'EXCESO_VENTA_SIN_COBRO_EXCLUIDO' : 'SPLIT_DIRECTO_RESTANTE'}`);
      }
      if (excesoCubrir > 0.01 && totalFormasPagoReal > 0) {
        // Antes este exceso se mandaba SIEMPRE a Caja sin excluir (a
        // diferencia del caso normal, ver `esCasoNormalParaSplit` abajo, que
        // sí lo etiqueta "Venta Sin Cobro"). Investigación exhaustiva
        // 2026-08-20 (caso real Global Hidalgo B0-260801256, $5,087.28):
        // se agotaron 4 fuentes independientes (API de cobros ±15 días, API
        // por factura sin restricción de fecha, reporte oficial de
        // Movimientos en Cajas completo, y /cuentas-pendientes) y NINGUNA
        // mostró ticket real que respalde este remanente — no es "ruido de
        // reclasificación" recuperable, es dinero sin ticket detrás. Se
        // alinea con el caso normal: se excluye del consolidado de
        // Efectivo/Tarjeta en vez de sumarse a ciegas.
        movs.push({
          cuentaId:    cuentaMap[CODIGO_CUENTA_CAJA] ?? null,
          concepto: conceptoVentaSinCobro, centroCosto, ventaFecha, serie: serieCfdi,
          debe:        excesoCubrir,
          haber:       0,
          cfdiUuid:    cfdi.uuid,
          rfcTercero,
          _esCargoPrincipal: true,
          tipoOrigen:  'Venta Sin Cobro',
        });
        splitPorFormaPagoReal(totalFormasPagoReal, extraRemanente);
      } else {
        splitPorFormaPagoReal(restante, extraRemanente);
      }
    } else {
      // Se empuja SIEMPRE, aunque sea $0 (factura pagada 100% con SF/Puntos)
      // para que el marcador `_puntosUsado` tenga dónde ir — no rompe el
      // cuadre, el otro lado de esa porción ya quedó representado arriba
      // (SF) o en la línea consolidada que arma el generator (Puntos).
      movs.push({
        cuentaId:    cuentaMap[rule.cuentaCargo] ?? null,
        concepto, centroCosto, ventaFecha, serie: serieCfdi,
        debe:        restante,
        haber:       0,
        cfdiUuid:    cfdi.uuid,
        rfcTercero,
        _esCargoPrincipal: true,
        ...extraRemanente,
      });
    }
  } else if (esCasoNormalParaSplit) {
    // El `formaPago` que se persiste (satMeta, al final de esta función) por
    // defecto es el que declara el CFDI — para una línea partida por el
    // desglose real, ESA línea concreta debe llevar el claveSat REAL
    // (`_formaPagoReal`, ver `splitPorFormaPagoReal`), no el del CFDI, o el
    // export (`consolidarCargos` en poliza.service.js, que agrupa "Depósitos
    // consolidados (Efectivo/Tarjeta)" por
    // `LABEL_FORMA_PAGO_CONSOLIDADO[m.formaPago]`) etiqueta TODAS las líneas
    // de esta factura igual que el CFDI original, sin importar a qué cuenta
    // fue cada una — resultando en "Efectivo" apareciendo también en la
    // cuenta de Bancos y viceversa (confirmado con el usuario 2026-08-06,
    // caso real VENTAS SUC.HIDALGO). Idéntico al split del remanente de
    // `esCasoAjusteSFPuntos` (ancla en `montoCargo`, no en la suma
    // encontrada) — se reutiliza el mismo helper.
    //
    // Corrección 2026-08-16 (caso real Ferrocarril F0|260800107/109):
    // cuando `montoCargo > totalFormasPagoReal`, prorratear directo infla
    // los renglones dp (igual que en `esCasoAjusteSFPuntos`). La diferencia
    // representa cobros no registrados en desglosePagoReal de esta sucursal
    // (otra sucursal, complemento de pago aún no procesado, discrepancia ERP).
    // Se empuja el exceso como línea CAJA con `tipoOrigen: 'Venta Sin Cobro'`
    // para que `_extraerCobrosSucursal` (poliza.service.js) la saque del
    // pipeline de consolidación Efectivo/Tarjeta; los renglones dp se
    // distribuyen con sus montos ABSOLUTOS (anclado en `totalFormasPagoReal`).
    // El total del asiento no cambia.
    const excesoCasoNormal = totalFormasPagoReal > 0
      ? parseFloat((montoCargo - totalFormasPagoReal).toFixed(2))
      : 0;
    if (_DEBUG_SPLIT_PAGO) {
      _debugLogger.warn(`[DEBUG_SPLIT_NORMAL] ${serieCfdi}-${cfdi.folio} montoCargo=${montoCargo} totalFormasPagoReal=${totalFormasPagoReal} `
        + `excesoCasoNormal=${excesoCasoNormal} -> ${excesoCasoNormal > 0.01 && totalFormasPagoReal > 0 ? 'EXCESO_EXCLUIDO_VENTA_SIN_COBRO' : 'SPLIT_DIRECTO_MONTOCARGO'}`);
    }
    if (excesoCasoNormal > 0.01 && totalFormasPagoReal > 0) {
      movs.push({
        cuentaId:    cuentaMap[CODIGO_CUENTA_CAJA] ?? null,
        concepto: conceptoVentaSinCobro, centroCosto, ventaFecha, serie: serieCfdi,
        debe:        excesoCasoNormal,
        haber:       0,
        cfdiUuid:    cfdi.uuid,
        rfcTercero,
        _esCargoPrincipal: true,
        tipoOrigen:  'Venta Sin Cobro',
      });
      splitPorFormaPagoReal(totalFormasPagoReal);
    } else {
      splitPorFormaPagoReal(montoCargo);
    }
  } else if (esCasoCargoSFConRemanenteReal) {
    const remanenteReal = parseFloat((montoCargo - montoSFUsado).toFixed(2));
    movs.push({
      cuentaId:    cuentaMap[rule.cuentaCargo] ?? null,
      concepto, centroCosto, ventaFecha, serie: serieCfdi,
      debe:        montoSFUsado,
      haber:       0,
      cfdiUuid:    cfdi.uuid,
      rfcTercero,
      _esCargoPrincipal: true,
    });
    // Nombre neutro (no "Anticipo") para que `consolidarCargos` sume esta
    // porción al consolidado de Efectivo/Tarjeta en vez de sacarla como
    // ajuste individual — ver comentario en `splitPorFormaPagoReal`.
    splitPorFormaPagoReal(remanenteReal, {}, 'Venta — remanente real (no cubierto por saldo a favor)');
  } else if (esCasoCargoAnticipoConRemanenteReal) {
    // Ver comentario en `esCasoCargoAnticipoConRemanenteReal` — mismo patrón
    // que el bloque de SF de arriba, con `montoAnticipoUsado` en vez de
    // `montoSFUsado`. La porción que queda en `cuentaCargo` (Anticipos) la
    // reduce/reemplaza después el cierre OPA en `cfdi-poliza-generator.service.js`.
    const remanenteRealAnticipo = parseFloat((montoCargo - montoAnticipoUsado).toFixed(2));
    movs.push({
      cuentaId:    cuentaMap[rule.cuentaCargo] ?? null,
      concepto, centroCosto, ventaFecha, serie: serieCfdi,
      debe:        montoAnticipoUsado,
      haber:       0,
      cfdiUuid:    cfdi.uuid,
      rfcTercero,
      _esCargoPrincipal: true,
    });
    // Nombre neutro — mismo motivo que en el bloque de SF: `esReglaAnticipo`
    // (poliza.service.js) es un match de texto simple (`/anticipo/i`), así
    // que el override NUNCA debe contener la palabra "anticipo" o esta línea
    // se saldría del consolidado de Efectivo/Tarjeta hacia el bloque de
    // ajustes (bug real encontrado 2026-08-31 probando este mismo fix: el
    // primer texto usado, "...no cubierto por anticipo", disparaba
    // exactamente ese problema).
    splitPorFormaPagoReal(remanenteRealAnticipo, {}, 'Venta — remanente real (no cubierto por OPA)');
  } else {
    // Cuando la regla apunta a Caja/Bancos puente (gateBase) pero no hay
    // cobros de esta sucursal en el ERP (desglosePagoReal vacío), la venta
    // se cobró en OTRA sucursal. El cargo a la cuenta puente se marca como
    // 'Cobro Sucursal' para que _extraerCobrosSucursal (poliza.service.js)
    // lo extraiga como "registro aparte" y NO llegue a consolidarCargos
    // (que armaría el consolidado de Efectivo/Tarjeta con dinero que no
    // se recibió físicamente aquí). La otra sucursal cobrador incluirá
    // el depósito real en su propio poliza vía cobrosCobradoraDirecta.
    const sinCobrosEnSucursal = gateBase
      && Array.isArray(context.desglosePagoReal)
      && context.desglosePagoReal.length === 0;
    // Cuando gateBase=true (regla apunta a Caja/Bancos, genérica o específica
    // como Banamex/BBVA), el cargo de fallback debe ir a CAJA (Efectivo) o
    // BANCOS genérico (cualquier otra formaPago), no a la cuenta específica.
    // Esto cubre el caso donde desglosePagoReal está vacío/null (sin cobros
    // ERP para este CFDI) pero la regla apuntaba a un banco real
    // (confirmado con el usuario 2026-08-15: FG con cuentaCargo=1102012001
    // aparecía como "Depósitos consolidados (Efectivo/Tarjeta)" en Banamex
    // en vez de en CAJA/BANCOS por identificar).
    const _esEfectivoCfdi = (cfdi.formaPago ?? '') === '01';
    const _cuentaIdFallback = gateBase
      ? ((_esEfectivoCfdi ? cuentaMap[CODIGO_CUENTA_CAJA] : cuentaMap[CODIGO_CUENTA_BANCOS])
          ?? cuentaMap[rule.cuentaCargo] ?? null)
      : (cuentaMap[rule.cuentaCargo] ?? null);
    if (_DEBUG_SPLIT_PAGO) {
      _debugLogger.warn(`[DEBUG_SPLIT_FALLBACK] ${serieCfdi}-${cfdi.folio} montoCargo=${montoCargo} formaPagoCFDI=${cfdi.formaPago} `
        + `sinCobrosEnSucursal=${sinCobrosEnSucursal} tipoOrigenAplicado=${sinCobrosEnSucursal ? 'Venta Sin Cobro' : 'NINGUNO (SI CONSOLIDA)'}`);
    }
    movs.push({
      cuentaId:    _cuentaIdFallback,
      concepto:    sinCobrosEnSucursal ? conceptoVentaSinCobro : concepto,
      centroCosto,
      ventaFecha,
      serie:       serieCfdi,
      debe:        montoCargo,
      haber:       0,
      cfdiUuid:    cfdi.uuid,
      rfcTercero,
      _esCargoPrincipal: true,
      ...(esAplicacionSaldo && !rule.cuentaCargo2 ? { _saldoUsado: montoCargo } : {}),
      ...(sinCobrosEnSucursal ? { tipoOrigen: 'Venta Sin Cobro' } : {}),
    });
  }

  // IVA en facturas (tipo I y E)
  // PPD → cuenta "por cobrar/por pagar" (cuentaIvaPPD); PUE → cuenta final (cuentaIva)
  // Ingreso: HABER | Egreso: DEBE | esIvaHaber (Reg 19): HABER aunque sea E
  // esAnticipo: el IVA se maneja en el bloque dedicado de más abajo → omitir aquí
  if (!esPago && !esAnticipo && iva > 0) {
    const cuentaIvaAplicable = (esPPD && rule.cuentaIvaPPD) ? rule.cuentaIvaPPD : rule.cuentaIva;
    if (cuentaIvaAplicable) {
      const ivaEsHaber = esIngreso || esIvaHaber;
      // Fórmula SAT: total = subtotal − descuento + IVA → IVA = total − subtotal + descuento.
      // Tanto ingreso (abono neteado) como egreso (cargo neteado) llevan descuento aquí.
      const _descuentoEnIva = !esPago ? Number(cfdi.descuento || 0) : 0;
      const ivaR = (ivaRet === 0 && isrRet === 0)
        ? parseFloat((total - subtotal + _descuentoEnIva).toFixed(2))
        : iva;
      movs.push({
        cuentaId:    cuentaMap[cuentaIvaAplicable] ?? null,
        // Sin prefijo "IVA - " en la columna H (confirmado con el usuario
        // 2026-08-25) — la cuenta contable ya identifica que es IVA, el
        // prefijo era redundante con la referencia real (serie-folio/OPA-...).
        concepto,
        centroCosto,
        ventaFecha,
        serie:       serieCfdi,
        debe:        ivaEsHaber ? 0    : ivaR,
        haber:       ivaEsHaber ? ivaR : 0,
        cfdiUuid:    cfdi.uuid,
        rfcTercero,
      });
    }
  }

  // Reconocimiento de IVA al cobro (solo tipo P con PPD configurado)
  // Cancela el saldo de cuentaIvaPPD y lo traslada a cuentaIva.
  // esAnticipo usa su propio bloque de swap IVA → se omite aquí.
  // Split por factura: diferido al bucle unificado junto al Abono (ver
  // comentario en el bloque de Cargo) — aquí solo se arma para el caso NO
  // partido.
  if (esPago && !esAnticipo && iva > 0 && rule.cuentaIvaPPD && rule.cuentaIva && !esSplitPagoPorFactura) {
    movs.push({
      cuentaId:    cuentaMap[rule.cuentaIvaPPD] ?? null,
      concepto:    `IVA cobrado - ${concepto}`,
      centroCosto,
      ventaFecha,
      serie:       serieCfdi,
      debe:        iva,
      haber:       0,
      cfdiUuid:    cfdi.uuid,
      rfcTercero,
    });
    movs.push({
      cuentaId:    cuentaMap[rule.cuentaIva] ?? null,
      concepto:    `IVA cobrado - ${concepto}`,
      centroCosto,
      ventaFecha,
      serie:       serieCfdi,
      debe:        0,
      haber:       iva,
      cfdiUuid:    cfdi.uuid,
      rfcTercero,
    });
  }

  // ── Motor anticipo: swap IVA-diferido → IVA-definitivo ───────────────────
  // Aplica a Reg 22C (I + formaPago=30) y Reg 23 (E + tipoRelacion=07).
  // DEBE cuentaIvaAnticipo (2104010002) cancela el IVA diferido del anticipo.
  // HABER cuentaIva (2104010001) reconoce el IVA causado definitivo.
  if (esAnticipo) {
    // DEBE: cancela el IVA diferido del anticipo (solo su monto, no el de la factura completa)
    movs.push({
      cuentaId:    cuentaMap[rule.cuentaIvaAnticipo] ?? null,
      concepto:    `IVA ant. - ${concepto}`,
      centroCosto, ventaFecha, serie: serieCfdi,
      debe:        ivaCancelado,
      haber:       0,
      cfdiUuid:    cfdi.uuid,
      rfcTercero,
    });
    if (rule.cuentaIva) {
      // HABER: reconoce el IVA definitivo completo de la factura final
      movs.push({
        cuentaId:    cuentaMap[rule.cuentaIva] ?? null,
        concepto:    `IVA ant. - ${concepto}`,
        centroCosto, ventaFecha, serie: serieCfdi,
        debe:        0,
        haber:       iva,
        cfdiUuid:    cfdi.uuid,
        rfcTercero,
      });
    }
  }

  // ── 5° movimiento: delta anticipo (total_factura - total_anticipo_relacionado) ─
  // Solo para Reg 22C (esAnticipo + tipo I/E) cuando la factura supera el anticipo.
  // context.totalRelacionado = suma de totales de los CFDIs relacionados (pre-fetch externo).
  if (esAnticipo && rule.cuentaDeltaAnticipo && context.totalRelacionado != null) {
    const delta = parseFloat((total - context.totalRelacionado).toFixed(2));
    if (delta > 0) {
      movs.push({
        cuentaId:    cuentaMap[rule.cuentaDeltaAnticipo] ?? null,
        concepto:    `Saldo - ${concepto}`,
        centroCosto, ventaFecha, serie: serieCfdi,
        debe:        delta,
        haber:       0,
        cfdiUuid:    cfdi.uuid,
        rfcTercero,
      });
    }
  }

  // IVA retenido (siempre HABER)
  if (rule.cuentaIvaRetenido && ivaRet > 0) {
    movs.push({
      cuentaId:    cuentaMap[rule.cuentaIvaRetenido] ?? null,
      concepto:    `IVA ret. - ${concepto}`,
      centroCosto,
      ventaFecha,
      serie:       serieCfdi,
      debe:        0,
      haber:       ivaRet,
      cfdiUuid:    cfdi.uuid,
      rfcTercero,
    });
  }

  // ISR retenido (siempre HABER)
  if (rule.cuentaIsrRetenido && isrRet > 0) {
    movs.push({
      cuentaId:    cuentaMap[rule.cuentaIsrRetenido] ?? null,
      concepto:    `ISR ret. - ${concepto}`,
      centroCosto,
      ventaFecha,
      serie:       serieCfdi,
      debe:        0,
      haber:       isrRet,
      cfdiUuid:    cfdi.uuid,
      rfcTercero,
    });
  }

  // Línea principal abono
  // esAnticipo (Reg 22C/23): abono = subtotal (monto sin IVA — el IVA ya está en bloque anticipo)
  // esIvaHaber (Reg 19):     abono = subtotal - descuento (IVA ya en HABER; descuento reduce base)
  // Ingreso normal:          HABER = subtotal
  // Egreso/Pago normal:      HABER = total neto (descontando retenciones)
  // cuentaIvaAbono (ej. Club Tuberos monedero): abono = subtotal; IVA va a cuenta separada
  // esAnticipo + tipo P: abono cierra la CxC por el total pagado
  // esAnticipo + tipo I/E / esIvaHaber: abono = subtotal - descuento (IVA ya manejado aparte)
  let montoAbono;
  const tieneIvaAbonoSplit = !esPago && !esAnticipo && !esIvaHaber && rule.cuentaIvaAbono && iva > 0;
  if (esAnticipo)      montoAbono = esPago ? (total - ivaRet - isrRet) : subtotal;
  else if (esIvaHaber) montoAbono = subtotal - Number(cfdi.descuento || 0);
  else if (tieneIvaAbonoSplit) {
    // Split IVA abono: cuentaAbono recibe solo subtotal, cuentaIvaAbono recibe IVA
    montoAbono = subtotal;
  } else if (esIngreso) {
    // El descuento se netea directo en el ingreso: montoAbono = subtotal − descuento.
    // Fórmula D=H: cargo(total) = abono(subtotal−desc) + IVA(total−subtotal+desc).
    // Con retenciones se usa total−iva que ya equivale al neto cuando hay retenciones.
    const descuento = Number(cfdi.descuento || 0);
    montoAbono = (ivaRet === 0 && isrRet === 0) || esMetadataConDescuento
      ? parseFloat((subtotal - descuento).toFixed(2))
      : parseFloat((total - iva).toFixed(2));
  } else {
    montoAbono = total - ivaRet - isrRet;
  }

  // ── Motor extendido: reglas MIXTAS (tasaIva=mixto, cuentaAbono2) ──────────
  // Divide el abono/cargo entre porción 16% y porción 0%.
  // Para tipo I: cuentaAbono=Ingresos16% (HABER subtotal16), cuentaAbono2=Ingresos0% (HABER subtotal0).
  // Para tipo E: cuentaAbono2=Devoluciones0% (DEBE subtotal0), ajusta cuentaCargo a subtotal16.
  if (!esPago && rule.tasaIva === 'mixto' && rule.cuentaAbono2) {
    const { subTotal16, subTotal0 } = _calcCfdiMontos(cfdi);
    if (subTotal16 + subTotal0 > 0) {
      if (esIngreso) {
        // Usamos total−iva como neto garantizado (= subtotal−descuento siempre).
        // subTotal16/subTotal0 pueden ser brutos o netos según el fallback usado,
        // pero su PROPORCIÓN es correcta para repartir el neto entre tasas.
        const netTotal = parseFloat((total - iva).toFixed(2));
        const sumSub   = subTotal16 + subTotal0;
        const haber16R = parseFloat((netTotal * subTotal16 / sumSub).toFixed(2));
        const haber0R  = parseFloat((netTotal - haber16R).toFixed(2));
        montoAbono = haber16R;
        if (haber0R > 0) {
          movs.push({
            cuentaId:    cuentaMap[rule.cuentaAbono2] ?? null,
            concepto:    `${concepto} (0%)`,
            centroCosto, ventaFecha, serie: serieCfdi,
            debe: 0, haber: haber0R,
            cfdiUuid: cfdi.uuid, rfcTercero,
          });
          // Split cargo 0%: cuentaCargoMixto0 recibe el neto 0% (sin IVA).
          if (rule.cuentaCargoMixto0 && haber0R > 0) {
            const cargoLine = movs.find(m =>
              m.cuentaId === (cuentaMap[rule.cuentaCargo] ?? null) && m.debe > 0
            );
            if (cargoLine) {
              cargoLine.debe = parseFloat((cargoLine.debe - haber0R).toFixed(2));
            }
            movs.push({
              cuentaId:    cuentaMap[rule.cuentaCargoMixto0] ?? null,
              concepto:    `${concepto} (0%)`,
              centroCosto, ventaFecha, serie: serieCfdi,
              debe: haber0R, haber: 0,
              cfdiUuid: cfdi.uuid, rfcTercero,
            });
          }
        }
      } else {
        // Tipo E (NC mixta): cargo principal = subTotal16, cargo secundario = subTotal0.
        // Pre-redondear a 2 decimales antes de usar: el fallback (_calcCfdiMontos) puede
        // devolver valores con 4 decimales (iva/0.16). PostgreSQL los redondea al insertar
        // en DECIMAL(15,2), creando un descuadre de $0.01. Al redondear aquí primero y
        // calcular ivaR desde los valores ya redondeados se garantiza balance exacto.
        const s16R = parseFloat(subTotal16.toFixed(2));
        const s0R  = parseFloat(subTotal0.toFixed(2));
        const cargoLine = movs.find(m => m.cuentaId === (cuentaMap[rule.cuentaCargo] ?? null) && m.debe > 0);
        if (cargoLine) cargoLine.debe = s16R;
        // Localiza la línea de IVA por CUENTA (misma que usó el push original,
        // línea ~1122), no por el prefijo "IVA - " del concepto — ese prefijo
        // se quitó de la columna H (confirmado con el usuario 2026-08-25), así
        // que ya no sirve como identificador aquí.
        const cuentaIvaAplicableMixto = (esPPD && rule.cuentaIvaPPD) ? rule.cuentaIvaPPD : rule.cuentaIva;
        const ivaLineMixto = movs.find(m => m.cuentaId === (cuentaMap[cuentaIvaAplicableMixto] ?? null) && m.debe > 0);
        if (ivaLineMixto) {
          ivaLineMixto.debe = parseFloat((total - s16R - s0R).toFixed(2));
        }
        if (s0R > 0) {
          movs.push({
            cuentaId:    cuentaMap[rule.cuentaAbono2] ?? null,
            concepto:    `${concepto} (0%)`,
            centroCosto, ventaFecha, serie: serieCfdi,
            debe: s0R, haber: 0,
            cfdiUuid: cfdi.uuid, rfcTercero,
          });
        }
      }
    }
  }

  // ── Motor extendido: cobros MIXTOS tipo P (tasaIva=mixto, cuentaAbono2) ──────
  // Divide el abono CxC entre porción 16% (cuentaAbono) y porción 0% (cuentaAbono2).
  // monto16 = totalTrasladosBaseIVA16 + totalTrasladosImpuestoIVA16 del CP <Totales>.
  // monto0  = montoTotalPagos - monto16 (facturas 0% pagadas en el mismo complemento).
  if (esPago && rule.tasaIva === 'mixto' && rule.cuentaAbono2) {
    const totalesCP = cfdi.complementoPago?.totales;
    if (totalesCP) {
      const base16CP = Number(totalesCP.totalTrasladosBaseIVA16 || 0);
      const iva16CP  = Number(totalesCP.totalTrasladosImpuestoIVA16 || 0);
      const monto16  = parseFloat((base16CP + iva16CP).toFixed(2));
      const monto0   = parseFloat((montoAbono - monto16).toFixed(2));
      if (monto0 > 0.01) {
        montoAbono = monto16;
        movs.push({
          cuentaId:  cuentaMap[rule.cuentaAbono2] ?? null,
          concepto:  `${concepto} (0%)`,
          centroCosto, ventaFecha, serie: serieCfdi,
          debe: 0, haber: monto0,
          cfdiUuid: cfdi.uuid, rfcTercero,
        });
      }
    }
  }

  // El descuento se netea directamente en montoAbono (subtotal − descuento).
  // No se genera línea separada de Descuentos s/Ventas.

  // Asiento completo POR FACTURA liquidada — Cargo (+SF si aplica) + IVA
  // cobrado + Abono, todo consecutivo por factura (confirmado con el usuario
  // 2026-08-11: los renglones de una misma factura deben verse juntos, no en
  // tres pasadas separadas). Reemplaza los bloques de Cargo/IVA cobrado de
  // arriba para este caso (ver sus comentarios) y el Abono agregado del
  // `else` de aquí abajo.
  if (esSplitPagoPorFactura) {
    const nombreCliente = cfdi.receptor?.nombre ?? 'CLIENTE NO IDENTIFICADO';
    const totalDoctos    = context.doctosPago.reduce((s, d) => s + d.monto, 0);
    const aplicaIvaCobrado = iva > 0 && rule.cuentaIvaPPD && rule.cuentaIva;
    let acumuladoCargo = 0;
    let acumuladoAbono = 0;
    context.doctosPago.forEach((d, idx) => {
      const esUltimo = idx === context.doctosPago.length - 1;
      const share = totalDoctos > 0 ? d.monto / totalDoctos : 1 / context.doctosPago.length;
      const conceptoFactura = [nombreCliente, `${d.serie}-${d.folio}`].filter(Boolean).join(' / ');
      const baseFactura = { concepto: conceptoFactura, centroCosto, ventaFecha, serie: serieCfdi, cfdiUuid: cfdi.uuid, rfcTercero };

      // 1. Cargo (+SF si esta factura se pagó con saldo a favor) — mismo
      // prorrateo con residuo que `esCasoNormalParaSplit`.
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
          const ivaSF       = Math.round((montoSFLinea - subtotalSF) * 100) / 100;
          movs.push({ ...baseFactura, cuentaId: cuentaMap[CODIGO_CUENTA_SALDO_FAVOR],    debe: subtotalSF, haber: 0, tipoOrigen: TIPO_ORIGEN_CARGO_ESPECIAL, reglaNombre: 'SF' });
          movs.push({ ...baseFactura, cuentaId: cuentaMap[CODIGO_CUENTA_IVA_SALDO_FAVOR], debe: ivaSF,      haber: 0, tipoOrigen: TIPO_ORIGEN_CARGO_ESPECIAL, reglaNombre: 'SF' });
          restanteLinea = parseFloat((restanteLinea - montoSFLinea).toFixed(2));
        }
        if (restanteLinea > 0) {
          movs.push({ ...baseFactura, cuentaId: cuentaMap[rule.cuentaCargo] ?? null, debe: restanteLinea, haber: 0, _esCargoPrincipal: true });
        }
      }

      // 2. IVA cobrado (swap cuentaIvaPPD → cuentaIva) — IVA real de ESTA
      // factura (`d.ivaDoc`), no prorrateado.
      if (aplicaIvaCobrado) {
        const ivaFactura = Number(d.ivaDoc) || 0;
        if (ivaFactura > 0) {
          movs.push({ ...baseFactura, cuentaId: cuentaMap[rule.cuentaIvaPPD] ?? null, debe: ivaFactura, haber: 0 });
          movs.push({ ...baseFactura, cuentaId: cuentaMap[rule.cuentaIva]    ?? null, debe: 0, haber: ivaFactura });
        }
      }

      // 3. Abono que cierra la CxC de esta factura.
      const montoLineaAbono = esUltimo
        ? parseFloat((montoAbono - acumuladoAbono).toFixed(2))
        : parseFloat((montoAbono * share).toFixed(2));
      acumuladoAbono += montoLineaAbono;
      if (montoLineaAbono > 0) {
        movs.push({ ...baseFactura, cuentaId: cuentaMap[rule.cuentaAbono] ?? null, debe: 0, haber: montoLineaAbono });
      }
    });
  } else {
    movs.push({
      cuentaId:    cuentaMap[rule.cuentaAbono] ?? null,
      concepto,
      centroCosto,
      ventaFecha,
      serie:       serieCfdi,
      debe:        0,
      haber:       montoAbono,
      cfdiUuid:    cfdi.uuid,
      rfcTercero,
    });
  }

  // Split IVA abono: cuando cuentaIvaAbono está definida, el IVA va a cuenta separada
  // HABER cuentaIvaAbono = IVA (ej. 2104010002 IVA Trasladado Anticipos para Club Tuberos)
  // Esto espeja el patrón CONTPAQI: HABER Monedero=subtotal + HABER IVAAnticipo=IVA
  if (tieneIvaAbonoSplit && rule.cuentaIvaAbono) {
    const ivaR = parseFloat((total - subtotal + Number(cfdi.descuento || 0)).toFixed(2));
    if (ivaR > 0) {
      movs.push({
        cuentaId:    cuentaMap[rule.cuentaIvaAbono] ?? null,
        // Ver comentario equivalente arriba: sin prefijo "IVA - " en columna H.
        concepto,
        centroCosto,
        ventaFecha,
        serie:       serieCfdi,
        debe:        0,
        haber:       ivaR,
        cfdiUuid:    cfdi.uuid,
        rfcTercero,
      });
    }
  }

  // Dentro del asiento de cada CFDI: cargos (debe > 0) primero, abonos
  // después — EXCEPTO en el split de Pago por factura (`esSplitPagoPorFactura`),
  // donde el orden ya se armó a propósito para que Cargo+IVA+Abono de una
  // misma factura queden consecutivos; este sort global los separaría de
  // vuelta en "todos los cargos" / "todos los abonos" (confirmado con el
  // usuario 2026-08-11).
  if (!esSplitPagoPorFactura) {
    movs.sort((a, b) => {
      const ao = (a.debe || 0) > 0 ? 0 : 1;
      const bo = (b.debe || 0) > 0 ? 0 : 1;
      return ao - bo;
    });
  }

  // Validar cuadre contable: ∑DEBE debe igualar ∑HABER dentro de $0.01 (tolerancia SAT Anexo 24)
  const _sumDebe  = movs.reduce((s, m) => s + (m.debe  || 0), 0);
  const _sumHaber = movs.reduce((s, m) => s + (m.haber || 0), 0);
  if (Math.abs(_sumDebe - _sumHaber) > 0.01) {
    console.warn(
      `[cfdiToMovimientos] ASIENTO DESBALANCEADO uuid=${cfdi.uuid} regla="${rule?.nombre}" ` +
      `debe=${_sumDebe.toFixed(2)} haber=${_sumHaber.toFixed(2)} ` +
      `diff=${(_sumDebe - _sumHaber).toFixed(2)}`
    );
    movs.forEach(m => { m._desbalanceado = true; });
  }

  // Enriquecer cada movimiento con los campos SAT del CFDI origen y la regla usada.
  // Para tipo P, formaPago va en el complemento (formaDePagoP), no en el header
  // (igual que en findRuleInList) — sin esto, el subcódigo CONTPAQi 20 (cobros
  // PPD por transferencia) nunca se detecta porque formaPago siempre llega null.
  const satMeta = {
    tipoComprobante: cfdi.tipoDeComprobante                      ?? null,
    // metodoPago ya resuelto arriba (factura origen para NC tipo E, ver
    // `metodoPagoResuelto`) — no el metodoPago crudo de la NC. Esto es lo que
    // luego decide el bloque Contado/Crédito en el export CONTPAQ
    // (`poliza.service.js` → `exportContpaqXlsx`).
    metodoPago:      metodoPagoResuelto                         ?? null,
    formaPago:       cfdi.formaPago
      ?? (cfdi.tipoDeComprobante === 'P'
        ? cfdi.complementoPago?.pagos?.[0]?.formaDePagoP ?? null
        : null),
    folio:           cfdi.folio                                 ?? null,
    rfcEmisor:       cfdi.emisor?.rfc                           ?? null,
    rfcReceptor:     cfdi.receptor?.rfc                         ?? null,
    reglaId:         rule?.id                                   ?? null,
    reglaNombre:     rule?.nombre                               ?? null,
    tipoOrigen:      cfdi.tipoOrigen ?? _derivarTipoOrigen(cfdi) ?? null,
    // Ver `ticketRealUnico` arriba -- default para TODAS las líneas de este
    // CFDI (incluida la de Abono, que no pasa por `splitPorFormaPagoReal` y
    // por eso nunca trae su propio `_serieVentaTicket`). Una línea de Cargo
    // partida por forma de pago real SÍ trae el suyo (`_serieVentaTicket`,
    // por ticket) y lo sobrescribe más abajo -- esto es solo el default.
    ...(ticketRealUnico ? { serieVentaTicket: ticketRealUnico.Serie, folioVentaTicket: ticketRealUnico.Folio } : {}),
  };

  // `_formaPagoReal` (ver split del cargo más arriba): si esta línea puntual
  // fue partida por el desglose real de cobro, su `formaPago` correcto es el
  // claveSat REAL de esa porción, no el que declara el CFDI completo — debe
  // sobrevivir al spread de `satMeta` (que de otro modo lo pisaría con el
  // mismo valor para TODAS las líneas de la factura, rompiendo el bucket
  // Efectivo/Tarjeta del export — ver comentario en el split).
  //
  // Mismo problema con las líneas de Saldo a Favor/Puntos (`tipoOrigen:
  // TIPO_ORIGEN_CARGO_ESPECIAL`, ver arriba): `satMeta.tipoOrigen`/
  // `satMeta.reglaNombre` son del CFDI/regla completos y pisarían el
  // tipoOrigen especial que las saca del pipeline normal de consolidación,
  // dejándolas otra vez como 'Venta' — deben sobrevivir igual que
  // `_formaPagoReal`.
  return movs.map(m => ({
    ...m,
    ...satMeta,
    ...(m._formaPagoReal != null ? { formaPago: m._formaPagoReal } : {}),
    ...(m._reglaNombreReal != null ? { reglaNombre: m._reglaNombreReal } : {}),
    // Sobrevive al spread de `satMeta` por la misma razón que `_formaPagoReal`
    // — ver comentario ahí. `consolidarCargos` los usa para resolver la
    // autorización real de Tarjeta por ticket (bank_movements.erpLinks).
    ...(m._serieVentaTicket != null ? { serieVentaTicket: m._serieVentaTicket } : {}),
    ...(m._folioVentaTicket != null ? { folioVentaTicket: m._folioVentaTicket } : {}),
    ...(
      m.tipoOrigen === TIPO_ORIGEN_CARGO_ESPECIAL
        ? { tipoOrigen: m.tipoOrigen, reglaNombre: m.reglaNombre }
        : m.tipoOrigen === 'Venta Sin Cobro'
          ? { tipoOrigen: 'Venta Sin Cobro' }
          // `yaContabilizadoOtroDia` (ver arriba, `splitPorFormaPagoReal`):
          // mismo problema — debe sobrevivir al spread de `satMeta` o se
          // pierde el ocultamiento y el Cargo vuelve a verse como 'Venta'
          // normal, duplicando el dinero entre los dos días.
          : m.reglaNombre === ETIQUETA_COBRO_YA_CONTABILIZADO
            ? { tipoOrigen: m.tipoOrigen, reglaNombre: m.reglaNombre }
            : {}
    ),
  }));
}

/**
 * Aplica la migración de descuentos PPD directamente en la BD:
 *   - Reg 6, 11, 13 → tieneDescuento=false
 *   - Inserta Reg 6B (PPD 16% con descuento) y Reg 6C (PPD 0% con descuento)
 * Es idempotente.
 */
async function migrarPpdDescuento() {
  const resultado = { actualizadas: [], insertadas: [], yaExistian: [] };

  // 1. Actualizar Reg 6, 11, 13
  const nombresActualizar = [
    'Reg 6 — Venta PPD 16% (Factura a Crédito)',
    'Reg 11 — Venta PPD Tasa 0%',
    'Reg 13 — Venta Mixta PPD (0%+16%)',
  ];
  for (const nombre of nombresActualizar) {
    const regla = await CfdiMappingRule.findOne({ where: { nombre } });
    if (!regla) continue;
    if (regla.tieneDescuento === false) { resultado.yaExistian.push(nombre); continue; }
    await regla.update({ tieneDescuento: false });
    resultado.actualizadas.push(nombre);
  }

  // 2. Insertar Reg 6B
  const nuevas = [
    {
      nombre:          'Reg 6B — Venta con Descuento PPD 16%',
      tipoComprobante: 'I',
      metodoPago:      'PPD',
      formaPago:       '99',
      tasaIva:         '16',
      tieneDescuento:  true,
      cuentaCargo:     '1103010001',
      cuentaAbono:     '4100020001',
      cuentaIvaPPD:    '2105010001',
      cuentaDescuento: '4200020001',
      prioridad:       59,
      isActive:        true,
    },
    {
      nombre:          'Reg 6C — Venta con Descuento PPD 0%',
      tipoComprobante: 'I',
      metodoPago:      'PPD',
      formaPago:       '99',
      tasaIva:         '0',
      tieneDescuento:  true,
      cuentaCargo:     '1103010002',
      cuentaAbono:     '4100020002',   // Ingresos Crédito 0% (PPD = crédito, no contado)
      cuentaIvaPPD:    null,
      cuentaDescuento: '4200020002',
      prioridad:       64,
      isActive:        true,
    },
  ];
  for (const datos of nuevas) {
    const existe = await CfdiMappingRule.findOne({ where: { nombre: datos.nombre } });
    if (existe) { resultado.yaExistian.push(datos.nombre); continue; }
    await CfdiMappingRule.create(datos);
    resultado.insertadas.push(datos.nombre);
  }

  return resultado;
}

// ── Privado ───────────────────────────────────────────────────────────────────

function _validate(data) {
  if (!data.nombre?.trim())    throw new BadRequestError('El nombre es requerido');
  if (!data.cuentaCargo?.trim()) throw new BadRequestError('La cuenta de cargo es requerida');
  if (!data.cuentaAbono?.trim()) throw new BadRequestError('La cuenta de abono es requerida');
}

async function getRulePolizas(ruleId) {
  const rule = await CfdiMappingRule.findByPk(ruleId);
  if (!rule) throw new NotFoundError('Regla de mapeo');

  const counts = await PolizaMovimiento.findAll({
    where:      { reglaId: ruleId },
    attributes: ['polizaId', [fn('COUNT', col('id')), 'cnt']],
    group:      ['polizaId'],
    raw:        true,
  });
  if (!counts.length) return [];

  const idSet    = counts.map(c => c.polizaId);
  const countMap = Object.fromEntries(counts.map(c => [c.polizaId, parseInt(c.cnt, 10)]));

  const polizas = await Poliza.findAll({
    where:      { id: { [Op.in]: idSet }, estado: { [Op.in]: ['borrador', 'contabilizada'] } },
    attributes: ['id', 'tipo', 'numero', 'fecha', 'concepto', 'ejercicio', 'periodo', 'rfc', 'estado'],
    order:      [['fecha', 'DESC']],
    raw:        true,
  });

  return polizas.map(p => ({ ...p, movimientosConRegla: countMap[p.id] ?? 0 }));
}

module.exports = { list, getById, create, update, remove, getRulePolizas, findRuleForCfdi, findRuleInList, cfdiToMovimientos, migrarPpdDescuento, esConceptoMarcadorAjuste, _detectTasaIvaPublic: _detectTasaIva, _derivarTipoOrigenPublic: _derivarTipoOrigen, _calcCfdiMontosPublic: _calcCfdiMontos, detectTasaIva: _detectTasaIva };
