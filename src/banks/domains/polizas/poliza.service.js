'use strict';

const ExcelJS = require('exceljs');
const repo = require('./repositories/poliza.repository');
const { NotFoundError, BadRequestError: ValidationError, ForbiddenError } = require('../../shared/errors/AppError');
const { AccountPlan, CfdiMappingRule, PolizaMovimiento, Poliza } = require('../../../shared/models/postgres');
const { Op } = require('sequelize');
const BankMovement = require('../banks/BankMovement.model');
const CFDI = require('../../../visor/models/CFDI');

// Categorías de bank_movements que representan una transferencia electrónica real.
const CATEGORIAS_TRANSFERENCIA_BANCO = ['SPEI', 'TRASPASO'];

/**
 * Cruza los CFDIs de la póliza contra sus movimientos bancarios reales
 * (bank_movements.erpLinks.folioFiscal) para saber, con el dato real del
 * banco, si el cobro fue transferencia o no — más confiable que el
 * `formaPago` que el CFDI declara, que a veces no coincide con lo que
 * realmente pasó en el banco (ej. CFDI dice "03-Transferencia" pero el banco
 * registró un depósito en efectivo).
 *
 * Antes cruzaba por `uuidXML` (campo legado, solo 13% de cobertura en
 * bank_movements). Se cambió a `erpLinks.folioFiscal` — el mismo campo que ya
 * usan los reportes de Pagos Asociados / Depósitos Ingresos, con ~59% de
 * cobertura. erpLinks.folioFiscal tiene case inconsistente en los datos
 * (confirmado al corregir el mismo problema en report.controller.js) — se
 * busca con regex case-insensitive, no igualdad exacta.
 *
 * También trae la referencia bancaria real (numeroAutorizacion o, si no hay,
 * referenciaNumerica) — usada como "serie" de las líneas de cargo por
 * Transferencia (siempre desglosadas) y de Tarjeta cuando SÍ tienen un
 * depósito ligado en Bancos (ver `consolidarCargos`) — confirmado con el
 * usuario. No filtra por `categoria` (puede venir null en movimientos de
 * tarjeta ligados): lo que importa aquí es si HAY un depósito ligado, no de
 * qué categoría es.
 *
 * @param {string[]} cfdiUuids
 * @returns {Promise<Map<string, {esTransferencia: boolean, referencia: string|null}>>}
 *   uuid (mayúsculas) → info bancaria
 */
async function construirVerdadBancaria(cfdiUuids) {
  const mapa = new Map();
  const uuidsUnicos = [...new Set(cfdiUuids.filter(Boolean).map(u => u.toUpperCase()))];
  if (uuidsUnicos.length === 0) return mapa;

  const uuidsSet = new Set(uuidsUnicos);
  const movs = await BankMovement.find(
    { 'erpLinks.folioFiscal': { $in: uuidsUnicos.map(u => new RegExp(`^${u}$`, 'i')) } },
    { erpLinks: 1, categoria: 1, numeroAutorizacion: 1, referenciaNumerica: 1 },
  ).lean();

  for (const m of movs) {
    const cat = (m.categoria || '').toUpperCase();
    const esTransferencia = CATEGORIAS_TRANSFERENCIA_BANCO.some(c => cat.includes(c));
    const referencia = m.numeroAutorizacion || m.referenciaNumerica || null;

    for (const link of (m.erpLinks ?? [])) {
      const folioFiscalUpper = (link.folioFiscal || '').toUpperCase();
      if (!uuidsSet.has(folioFiscalUpper)) continue;
      // Un mismo CFDI puede tener varios movimientos ligados (varias
      // parcialidades) — si alguno confirma transferencia, esa gana.
      const actual = mapa.get(folioFiscalUpper);
      if (!actual || (!actual.esTransferencia && esTransferencia)) {
        mapa.set(folioFiscalUpper, { esTransferencia, referencia });
      }
    }
  }
  return mapa;
}

// Poliza.tipo interno (A,I,E,D,N,C,P) → TipoPol de CONTPAQi (1=Ingreso 2=Egreso 3=Diario)
const TIPO_POL_MAP = { I: '1', E: '2' };
const tipoPolContpaq = (tipo) => TIPO_POL_MAP[tipo] ?? '3';

function userLabel(user) {
  return user?.nombre || user?.email || String(user?.dbId ?? 'sistema');
}

function validateBalance(movimientos) {
  if (!movimientos || movimientos.length === 0) return;
  let debe  = 0;
  let haber = 0;
  for (const m of movimientos) {
    debe  = Math.round((debe  + (Number(m.debe)  || 0)) * 100) / 100;
    haber = Math.round((haber + (Number(m.haber) || 0)) * 100) / 100;
  }
  if (debe === 0 && haber === 0) {
    throw new ValidationError('La póliza debe tener importes mayores a cero');
  }
  const diff = Math.abs(debe - haber);
  if (diff > 0.01) {
    throw new ValidationError(`La póliza no está balanceada. Debe: ${debe.toFixed(2)}, Haber: ${haber.toFixed(2)}, Diferencia: ${diff.toFixed(2)}`);
  }
}

async function list(filters) {
  return repo.findAll(filters);
}

async function getById(id) {
  const poliza = await repo.findById(id);
  if (!poliza) throw new NotFoundError('Póliza');
  return poliza;
}

async function create(data, user) {
  if (!data.tipo)      throw new ValidationError('El tipo de póliza es requerido (A, I, E, D, N, C, P)');
  if (!data.fecha)     throw new ValidationError('La fecha es requerida');
  if (!data.concepto)  throw new ValidationError('El concepto es requerido');
  if (!data.ejercicio) throw new ValidationError('El ejercicio es requerido');
  if (!data.periodo)   throw new ValidationError('El periodo es requerido');
  if (!data.rfc)       throw new ValidationError('El RFC es requerido');

  const d = new Date(data.fecha);
  if (d.getFullYear() !== Number(data.ejercicio) || d.getMonth() + 1 !== Number(data.periodo)) {
    throw new ValidationError(
      `La fecha ${data.fecha} no corresponde al ejercicio ${data.ejercicio} periodo ${data.periodo}`,
    );
  }

  validateBalance(data.movimientos);

  try {
    return await repo.create({ ...data, creadoPor: userLabel(user) });
  } catch (e) {
    if (e.name === 'SequelizeUniqueConstraintError') {
      throw new ValidationError('Ya existe una póliza con ese número para este tipo/período. Intenta de nuevo.');
    }
    throw e;
  }
}

async function update(id, data, user) {
  // La validación de estado ocurre DENTRO de la transacción (con lock) en el repo
  // para evitar race condition TOCTOU. Aquí solo validamos la lógica de negocio.
  const poliza = await repo.findById(id);
  if (!poliza) throw new NotFoundError('Póliza');
  if (poliza.estado !== 'borrador') throw new ValidationError('Solo se pueden editar pólizas en estado borrador');

  if (data.movimientos !== undefined) validateBalance(data.movimientos);

  const fechaCheck = data.fecha ?? poliza.fecha;
  const ejCheck    = data.ejercicio ?? poliza.ejercicio;
  const perCheck   = data.periodo   ?? poliza.periodo;
  if (fechaCheck && ejCheck && perCheck) {
    const d = new Date(fechaCheck);
    if (d.getFullYear() !== Number(ejCheck) || d.getMonth() + 1 !== Number(perCheck)) {
      throw new ValidationError(
        `La fecha ${fechaCheck} no corresponde al ejercicio ${ejCheck} periodo ${perCheck}`,
      );
    }
  }

  const updated = await repo.update(id, data);
  if (!updated) throw new NotFoundError('Póliza');
  return updated;
}

async function cancel(id, user, motivo) {
  const poliza = await repo.findByIdLight(id);
  if (!poliza)                        throw new NotFoundError('Póliza');
  if (poliza.estado === 'cancelada')  throw new ValidationError('La póliza ya está cancelada');
  if (poliza.estado === 'contabilizada' && user?.role !== 'admin') {
    throw new ForbiddenError('Solo un administrador puede cancelar pólizas contabilizadas');
  }

  const result = await repo.cancel(id, {
    canceladoPor:       userLabel(user),
    canceladaAt:        new Date(),
    motivoCancelacion:  motivo || null,
  });
  if (!result) throw new NotFoundError('Póliza');

  // Advertir si la póliza tenía movimientos de IVA PPD (IVA por cobrar/pagar).
  // Cancelar la póliza deja saldo fantasma en esa cuenta — se requiere asiento de reversa.
  let advertenciaIvaPpd = null;
  if (poliza.estado === 'contabilizada' && poliza.movimientos?.length > 0) {
    try {
      const reglasConPpd = await CfdiMappingRule.findAll({
        where: { cuentaIvaPPD: { [Op.ne]: null } },
        attributes: ['cuentaIvaPPD'],
        raw: true,
      });
      const codigosPpd = [...new Set(reglasConPpd.map(r => r.cuentaIvaPPD).filter(Boolean))];
      if (codigosPpd.length > 0) {
        const cuentasPpdRows = await AccountPlan.findAll({
          where: { codigo: { [Op.in]: codigosPpd } },
          attributes: ['id'],
          raw: true,
        });
        const idsPpd = new Set(cuentasPpdRows.map(c => c.id));
        const tieneIvaPpd = poliza.movimientos.some(m => idsPpd.has(m.cuentaId));
        if (tieneIvaPpd) {
          advertenciaIvaPpd =
            'Esta póliza contenía movimientos de IVA PPD (IVA por cobrar/pagar pendiente de reconocer). ' +
            'Debes crear un asiento de reversa manual para limpiar el saldo de esa cuenta y evitar ' +
            'diferencias en la DIOT y la balanza de comprobación.';
        }
      }
    } catch (_) { /* no bloquear la cancelación por error en advertencia */ }
  }

  const resultPlain = typeof result?.toJSON === 'function' ? result.toJSON() : result;
  return { ...resultPlain, advertenciaIvaPpd };
}

/**
 * Cancela TODAS las pólizas en estado 'borrador' del rfc/ejercicio/periodo
 * indicado (mismo alcance que usa el resto de la pantalla de Pólizas para
 * generar/exportar). Deliberadamente excluye 'contabilizada' y 'cancelada' —
 * las contabilizadas requieren el permiso de admin y se cancelan una por una
 * desde su propio modal, no en bulk.
 *
 * Reutiliza `cancel()` por cada póliza (misma validación, mismo aviso de IVA
 * PPD) en vez de duplicar la lógica — un error en una póliza no detiene las
 * demás.
 *
 * Devuelve: { canceladas: number, errores: [{ polizaId, numero, tipo, error }] }
 */
async function cancelarTodas({ rfc, ejercicio, periodo }, user, motivo) {
  if (!rfc)       throw new ValidationError('RFC requerido');
  if (!ejercicio) throw new ValidationError('Ejercicio requerido');
  if (!periodo)   throw new ValidationError('Periodo requerido');

  const polizas = await Poliza.findAll({
    where: { rfc, ejercicio: Number(ejercicio), periodo: Number(periodo), estado: 'borrador' },
    attributes: ['id', 'numero', 'tipo'],
  });

  let canceladas = 0;
  const errores = [];
  for (const p of polizas) {
    try {
      await cancel(p.id, user, motivo);
      canceladas++;
    } catch (err) {
      errores.push({ polizaId: p.id, numero: p.numero, tipo: p.tipo, error: err.message });
    }
  }

  return { canceladas, errores, total: polizas.length };
}

async function contabilizar(id, user) {
  // findByIdLight: sólo PostgreSQL, sin consulta cruzada a MongoDB
  const poliza = await repo.findByIdLight(id);
  if (!poliza)                      throw new NotFoundError('Póliza');
  if (poliza.estado !== 'borrador') throw new ValidationError('Solo se pueden contabilizar pólizas en borrador');
  if (!poliza.movimientos?.length)  throw new ValidationError('La póliza no tiene movimientos');

  const _cuentasFaltantes = poliza.movimientos.filter(m => m.cuentaFaltante || m.cuentaId == null).length;
  if (_cuentasFaltantes > 0) {
    throw new ValidationError(
      `No se puede contabilizar: ${_cuentasFaltantes} movimiento(s) sin cuenta contable asignada. ` +
      `Edita la póliza y asigna las cuentas faltantes antes de contabilizar.`,
    );
  }

  validateBalance(poliza.movimientos.map(m => ({ debe: m.debe, haber: m.haber })));

  const updated = await repo.setEstado(id, 'contabilizada', {
    contabilizadoPor: userLabel(user),
    contabilizadaAt:  new Date(),
  });
  return updated;
}

async function revertir(id, user, motivo) {
  const poliza = await repo.findByIdLight(id);
  if (!poliza)                           throw new NotFoundError('Póliza');
  if (poliza.estado !== 'contabilizada') throw new ValidationError('Solo se pueden revertir pólizas contabilizadas');

  const updated = await repo.setEstado(id, 'borrador', {
    revertidoPor:    userLabel(user),
    revertidaAt:     new Date(),
    motivoReversion: motivo || null,
  });
  return updated;
}

async function reporteDescuadradas(filters) {
  if (!filters.rfc) throw new ValidationError('RFC requerido');
  return repo.findDescuadradas(filters);
}

/**
 * Genera el XML de Pólizas para el SAT (PolizasPeriodo_v1_3.xsd).
 * Solo incluye pólizas con estado 'contabilizada'.
 */
async function generarXmlSat({ rfc, ejercicio, periodo, tipoSolicitud = 'AF', numOrden, numTramite }) {
  if (!rfc)       throw new ValidationError('RFC requerido');
  if (!ejercicio) throw new ValidationError('Ejercicio requerido');
  if (!periodo)   throw new ValidationError('Periodo requerido');

  const polizas = await repo.findAllContabilizadas({ rfc, ejercicio: Number(ejercicio), periodo: Number(periodo) });

  const mes = String(Number(periodo)).padStart(2, '0');

  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  const fmt2 = (n) => Number(n || 0).toFixed(2);

  const polizasXml = polizas.map(p => {
    const transacciones = (p.movimientos || []).map(m => {
      const numCta = m.cuenta?.codigo ?? String(m.cuentaId ?? '');
      const desCta = m.cuenta?.nombre ?? '';
      let transpXml = `      <BCE:Transaccion NumCta="${esc(numCta)}" DesCta="${esc(desCta)}" Concepto="${esc(m.concepto)}" Debe="${fmt2(m.debe)}" Haber="${fmt2(m.haber)}">`;
      if (m.cfdiUuid && m.rfcTercero) {
        transpXml += `\n        <BCE:CompNal UUID_CFDI="${esc(m.cfdiUuid)}" RFC="${esc(m.rfcTercero)}" MontoTotal="${fmt2(Number(m.debe) || Number(m.haber))}" Moneda="MXN"/>`;
        transpXml += '\n      </BCE:Transaccion>';
      } else {
        transpXml += '</BCE:Transaccion>';
      }
      return transpXml;
    }).join('\n');

    const numPol  = p.folio || String(p.numero);
    // Tipo A (Apertura) es interno — el SAT solo acepta I,E,D,N,C → se mapea a D
    const tipoSat = p.tipo === 'A' ? 'D' : p.tipo;
    return `    <BCE:Poliza NumUnIdenPol="${esc(numPol)}" Fecha="${p.fecha}" Concepto="${esc(p.concepto)}" Tipo="${esc(tipoSat)}">\n${transacciones}\n    </BCE:Poliza>`;
  }).join('\n');

  const attrs = [
    `xmlns:BCE="http://www.sat.gob.mx/esquemas/ContabilidadE/1_3/PolizasPeriodo"`,
    `xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"`,
    `xsi:schemaLocation="http://www.sat.gob.mx/esquemas/ContabilidadE/1_3/PolizasPeriodo http://www.sat.gob.mx/esquemas/ContabilidadE/1_3/PolizasPeriodo/PolizasPeriodo_1_3.xsd"`,
    `Version="1.3"`,
    `TipoSolicitud="${esc(tipoSolicitud)}"`,
    ...(numOrden  ? [`NumOrden="${esc(numOrden)}"`]  : []),
    ...(numTramite ? [`NumTramite="${esc(numTramite)}"`] : []),
    `Mes="${mes}"`,
    `Anio="${Number(ejercicio)}"`,
    `RFC="${esc(rfc)}"`,
  ].join(' ');

  return `<?xml version="1.0" encoding="UTF-8"?>\n<BCE:Polizas ${attrs}>\n${polizasXml}\n</BCE:Polizas>`;
}

/**
 * Genera y contabiliza la póliza de cierre de IVA Trasladado para el período.
 * Mueve el saldo acreedor neto de 2104010001 (IVA Trasladado) a 2106020001 (IVA Por Pagar).
 */
async function generarCierreIVA({ rfc, ejercicio, periodo, user }) {
  if (!rfc)       throw new ValidationError('RFC requerido');
  if (!ejercicio) throw new ValidationError('Ejercicio requerido');
  if (!periodo)   throw new ValidationError('Periodo requerido');

  const ej = Number(ejercicio);
  const pe = Number(periodo);

  // Verificar que no exista ya una póliza de cierre IVA para el período
  const existente = await Poliza.findOne({
    where: { rfc, ejercicio: ej, periodo: pe, tipo: 'D', concepto: { [Op.like]: '%Cierre IVA%' } },
  });
  if (existente) {
    throw new ValidationError(`Ya existe una póliza de cierre IVA para este período (ID: ${existente.id}). Reviértela antes de generar una nueva.`);
  }

  // Cuentas contables requeridas
  const [ctaIVA, ctaPagar] = await Promise.all([
    AccountPlan.findOne({ where: { codigo: '2104010001' }, attributes: ['id'], raw: true }),
    AccountPlan.findOne({ where: { codigo: '2106020001' }, attributes: ['id'], raw: true }),
  ]);
  if (!ctaIVA)    throw new ValidationError('Cuenta 2104010001 (IVA Trasladado) no encontrada en el catálogo');
  if (!ctaPagar)  throw new ValidationError('Cuenta 2106020001 (IVA Por Pagar) no encontrada en el catálogo');

  // Sumar movimientos de IVA Trasladado de todas las pólizas contabilizadas del período
  const polizasPeriodo = await Poliza.findAll({
    where: { rfc, ejercicio: ej, periodo: pe, estado: 'contabilizada' },
    attributes: ['id'],
    raw: true,
  });
  if (!polizasPeriodo.length) {
    throw new ValidationError('No hay pólizas contabilizadas en el período — contabiliza primero los asientos del período');
  }

  const ids = polizasPeriodo.map(p => p.id);
  const movs = await PolizaMovimiento.findAll({
    where:      { polizaId: { [Op.in]: ids }, cuentaId: ctaIVA.id },
    attributes: ['debe', 'haber'],
    raw:        true,
  });

  const totalDebe  = movs.reduce((s, m) => Math.round((s + Number(m.debe  || 0)) * 100) / 100, 0);
  const totalHaber = movs.reduce((s, m) => Math.round((s + Number(m.haber || 0)) * 100) / 100, 0);
  const netIVA     = Math.round((totalHaber - totalDebe) * 100) / 100;

  if (Math.abs(netIVA) <= 0.01) {
    throw new ValidationError(
      `IVA Trasladado con saldo cero (${netIVA.toFixed(2)}) — no hay cierre que generar. ` +
      `(Debe: ${totalDebe.toFixed(2)}, Haber: ${totalHaber.toFixed(2)})`,
    );
  }
  if (netIVA < 0) {
    throw new ValidationError(
      `IVA Trasladado con saldo DEUDOR (${netIVA.toFixed(2)}): hay más cancelaciones/débitos ` +
      `que IVA trasladado cobrado en el período. Revisa los movimientos de la cuenta 2104010001 ` +
      `antes de generar el cierre. (Debe: ${totalDebe.toFixed(2)}, Haber: ${totalHaber.toFixed(2)})`,
    );
  }

  // Último día del período como fecha de la póliza
  const lastDay = new Date(ej, pe, 0).toISOString().slice(0, 10);
  const mesStr  = String(pe).padStart(2, '0');

  const poliza = await create({
    tipo:      'D',
    fecha:     lastDay,
    concepto:  `Cierre IVA Trasladado ${ej}-${mesStr}`,
    ejercicio: ej,
    periodo:   pe,
    rfc,
    movimientos: [
      { orden: 1, cuentaId: ctaIVA.id,   concepto: `Cierre IVA Trasladado ${ej}-${mesStr}`, debe: netIVA,  haber: 0       },
      { orden: 2, cuentaId: ctaPagar.id, concepto: `IVA Por Pagar ${ej}-${mesStr}`,         debe: 0,       haber: netIVA  },
    ],
  }, user);

  // Contabilizar automáticamente
  await contabilizar(poliza.id, user);
  const polizaFinal = await repo.findById(poliza.id);

  return { poliza: polizaFinal, netIVA, totalDebe, totalHaber };
}

// c_FormaPago del SAT que corresponde a "Transferencia electrónica de fondos".
// Es el caso del subcódigo CONTPAQi 21 (COBROS DE VENTAS PUE CON FORMA DE PAGO
// TRANSFERENCIA) — el resto de formas de pago (efectivo, cheque, tarjeta) usan
// la MISMA cuenta de Bancos (1102011005) pero no llevan ese subcódigo, por eso
// la agrupación también separa por formaPago y no solo por cuenta.
const FORMA_PAGO_TRANSFERENCIA = '03';

// c_FormaPago SAT → etiqueta para las líneas consolidadas de Efectivo/Tarjeta
// (Transferencia nunca llega aquí — se desglosa individual, ver
// `consolidarCargos`). formaPago sin mapear (cheque, etc.) cae al bucket
// genérico de siempre (sin etiqueta), confirmado con el usuario contra un
// export real donde Efectivo y Tarjeta salen en cuentas/líneas separadas.
const LABEL_FORMA_PAGO_CONSOLIDADO = { '01': 'EFECTIVO', '04': 'TARJETA', '28': 'TARJETA' };

// Cuentas cuyo abono en una Devolución/Cancelación SÍ debe mostrarse — a
// diferencia de un reembolso real en efectivo/banco (que se oculta, ver
// `esAjusteContadoMov`/`bloquesAjustesContado`), estas dos representan algo
// que el cliente sigue "debiendo o teniendo a favor", no dinero que salió:
//   - Saldo a favor (Anticipos Otros / monedero Club Tuberos) o su IVA
//     diferido — se crea un pasivo que el cliente puede usar después.
//   - Clientes (CxC) — la NC en realidad ajusta una venta A CRÉDITO (nunca
//     cobrada): el "abono" es la reducción de esa cuenta por cobrar, no un
//     reembolso — confirmado con el usuario: "cuando sean DEV a crédito debe
//     estar el asiento completo no solo los cargos".
const CUENTAS_SALDO_FAVOR = new Set(['2103090001', '2103090002', '2104010002']);
const CUENTAS_CLIENTES    = new Set(['1103010001', '1103010002']);
const esAbonoSaldoFavor = (m) => CUENTAS_SALDO_FAVOR.has(m.cuenta?.codigo) || CUENTAS_CLIENTES.has(m.cuenta?.codigo);

// "OPA" = anticipo (confirmado por el usuario). Aplica a CUALQUIER regla de
// anticipo — recepción (Reg 22, 22A, 22-0, 22C-DESC, "Recepción Anticipo por
// Descripción", etc.) y también aplicación/cierre ("Factura Final Anticipo",
// Reg 22C/22C-0 con formaPago 30) — el usuario confirmó que el 22 va en todo
// movimiento de anticipo aunque ya se haya aplicado/usado, no solo al recibirlo.
const esReglaAnticipo = (reglaNombre) => /anticipo/i.test(reglaNombre || '');

// Distingue "Recepción" (aún no se ha usado — Reg 22, 22A, 22-0, 22C-DESC,
// "Recepción Anticipo por Descripción") de "Factura Final"/aplicación/cierre
// (Reg 22B, 22C, 22C-0 — el anticipo ya se ocupó). Confirmado con el usuario:
// solo los de Recepción deben salir individuales al final; los ya aplicados
// siguen consolidados como cualquier otro cargo normal.
const esRecepcionAnticipo = (reglaNombre) => /recepci[oó]n/i.test(reglaNombre || '');

// Ventas con descuento (Reg 14/14A/15/15A/16/16A/6B/6C/6D, etc.) — únicas
// reglas de Ingreso donde "descuento" aparece en el nombre; su cargo usa la
// MISMA cuenta de Caja/Bancos que una venta normal, así que hoy se mezclan
// indistinguibles dentro de "Depósitos consolidados".
const esVentaConDescuento = (reglaNombre) => /descuento/i.test(reglaNombre || '');

// Club Tuberos identificado por texto literal en la descripción/concepto del
// CFDI (confirmado por el usuario) — independiente de la detección por
// `documentosRelacionados.Serie==='BCT'` que alimenta `tipoOrigen` (esa nunca
// ha disparado en Ingresos hasta ahora; esta es la señal que sí aplica aquí).
const esClubTuberosPorDescripcion = (concepto) => /club\s*tuberos/i.test(concepto || '');

// Devolución (incluye "Cancelación" — el seed ya trata cancelación de precio
// como devolución, confirmado con el usuario) y Bonificación genérica (no
// Club Tuberos) — señal por `tipoOrigen` (más confiable) o por el concepto
// del CFDI, mismo patrón que `esClubTuberosPorDescripcion`.
const esDevolucionOCancelacion = (m) =>
  /devoluci[oó]n|cancelaci[oó]n/i.test(m.tipoOrigen || '') || /devoluci[oó]n/i.test(m.concepto || '');
const esBonificacionGenerica = (m) =>
  /bonificaci[oó]n/i.test(m.tipoOrigen || '') || /bonificaci[oó]n/i.test(m.concepto || '');

// Colores fijos por categoría en el export a CONTPAQi (confirmado con el
// usuario) — reemplazan el alternado blanco/gris por factura para que cada
// bloque (Devolución, Descuento, Bonificación, Club Tuberos, Anticipo) se
// pueda distinguir a simple vista. ARGB de ExcelJS.
const COLOR_CATEGORIA = {
  devolucion:   'FFD9E8FB', // azul claro
  descuento:    'FFD9F2D9', // verde claro
  bonificacion: 'FFFCE4D6', // naranja claro
  clubTuberos:  'FFE8D9F2', // morado claro
  anticipo:     'FFFFF2CC', // amarillo claro (ya existía para "Anticipo sin aplicar")
};
const ETIQUETA_CATEGORIA = {
  clubTuberos:  'Bonificación Club Tuberos',
  descuento:    'Venta con Descuento',
  devolucion:   'Devolución',
  bonificacion: 'Bonificación',
};

/**
 * Clasifica un movimiento de cargo (Contado) en una de las 4 categorías de
 * ajuste — orden de prioridad confirmado con el usuario cuando una señal
 * podría matchear más de una: Club Tuberos > Descuento > Devolución
 * (incluye Cancelación) > Bonificación genérica. `null` = cargo normal
 * (venta de contado real), se consolida como siempre.
 */
function categorizarAjusteContado(m) {
  if (m.tipoOrigen === TIPO_ORIGEN_BCT || esClubTuberosPorDescripcion(m.concepto)) return 'clubTuberos';
  if (esVentaConDescuento(m.reglaNombre)) return 'descuento';
  if (esDevolucionOCancelacion(m)) return 'devolucion';
  if (esBonificacionGenerica(m)) return 'bonificacion';
  return null;
}

/**
 * Consolida los movimientos de cargo (debe > 0) de ventas normales de
 * Contado, agrupados por cuenta + centro de costo + forma de pago SAT en una
 * sola línea — replica el depósito real de caja/banco del periodo.
 *
 * Los CFDI de ajuste (Devolución, Descuento, Bonificación, Bonificación Club
 * Tuberos, Anticipo) ya se excluyen del `movs` de entrada por `armarBloqueContado`
 * — se arman aparte en `armarAjustesContado` (cargo+abono juntos por CFDI,
 * coloreados, agrupados por categoría) para no separar el cargo de su abono.
 *
 * El subcódigo CONTPAQi (columna F) sale de esa forma de pago (`subcodigoTransferencia`
 * cuando es `03`), salvo que sea un anticipo (OPA, solo aplica a Ingresos), que
 * siempre lleva 22 sin importar cómo se cobró.
 *
 * @param {number} subcodigoTransferencia — 21 para cobros PUE (Ingresos-Contado),
 *   20 para cobros PPD (Pagos) — misma cuenta de banco, distinto subcódigo según
 *   si la venta que se está cobrando era de contado o de crédito.
 * @param {boolean} [detectarAnticipo=false] — solo aplica en Ingresos; los
 *   movimientos de Pago nunca son "Recepción de Anticipo".
 * @param {Map<string, {esTransferencia: boolean, referencia: string|null}>} [verdadBancaria]
 *   uuid CFDI → info bancaria real (ver `construirVerdadBancaria`). Cuando el
 *   CFDI está en el mapa, su dato manda sobre el `formaPago` autodeclarado.
 *
 * Transferencia (formaPago SAT declarado = 03) NO se consolida: cada CFDI
 * queda como línea individual, con la referencia bancaria real
 * (numeroAutorizacion/referenciaNumerica) como serie cuando está disponible —
 * cada transferencia es un depósito bancario propio, rastreable por
 * separado. Tarjeta se comporta igual, pero SOLO cuando tiene un depósito
 * ligado en Bancos (`verdadBancaria`) — sin ligar, sigue consolidándose.
 * Efectivo siempre se consolida. Efectivo y Tarjeta consolidados van cada
 * uno en su PROPIA línea/cuenta (ya no juntos en un solo "Depósitos
 * consolidados") — confirmado con el usuario contra un export real de
 * ContPaQi.
 */
function consolidarCargos(movs, subcodigoTransferencia, detectarAnticipo = false, verdadBancaria = null, nombresClientes = null) {
  const grupos = new Map();
  const porCategoria = { devolucion: [], descuento: [], bonificacion: [], clubTuberos: [] };
  const anticipos      = [];  // Recepción Y Aplicación — después de las 4 categorías, antes del resto
  const transferencias = [];  // formaPago=03 (siempre) y Tarjeta con depósito ligado — desglosado

  for (const m of movs) {
    if (!(Number(m.debe) > 0)) continue;
    const centroCosto = m.centroCostoObj?.clave ?? m.centroCosto ?? '';
    const bancario    = verdadBancaria?.get((m.cfdiUuid || '').toUpperCase());
    const esTransferenciaVerificada = bancario ? bancario.esTransferencia : (m.formaPago === FORMA_PAGO_TRANSFERENCIA);
    const esAnticipo        = detectarAnticipo && esReglaAnticipo(m.reglaNombre);
    const esAnticipoSinUsar = esAnticipo && esRecepcionAnticipo(m.reglaNombre);

    const armarIndividual = (etiqueta, subcodigo, categoria, serieOverride) => {
      const nombre   = nombresClientes?.get((m.cfdiUuid || '').toUpperCase()) || '';
      const concepto = [nombre, m.serie].filter(Boolean).join(' / ') || etiqueta;
      return {
        cuenta: m.cuenta, serie: serieOverride ?? (m.serie || ''), concepto, centroCosto,
        debe: Number(m.debe), haber: 0, cfdiUuid: m.cfdiUuid, _subcodigo: subcodigo,
        _categoria: categoria,
      };
    };

    // Cualquier anticipo (Recepción sin usar O Factura Final ya aplicada) —
    // nunca se suma al total agregado, siempre línea individual por CFDI con
    // su serie real, igual que las otras 4 categorías de ajuste. Subcódigo
    // siempre 22, sin importar cómo se cobró (confirmado con el usuario).
    if (esAnticipo) {
      anticipos.push(armarIndividual(esAnticipoSinUsar ? 'Anticipo sin aplicar' : 'Anticipo Aplicado', 22, 'anticipo'));
      continue;
    }

    // Devolución, Descuento, Bonificación y Bonificación Club Tuberos — no se
    // meten al total agregado, quedan como su propia línea (con su serie
    // real), después de los abonos de la venta pero antes de los anticipos.
    const categoria = categorizarAjusteContado(m);
    if (categoria) {
      porCategoria[categoria].push(
        armarIndividual(ETIQUETA_CATEGORIA[categoria], esTransferenciaVerificada ? subcodigoTransferencia : 0, categoria),
      );
      continue;
    }

    // Transferencia confirmada (20 o 21, según el bloque) SIEMPRE se desglosa
    // individual, con o sin número de autorización real del banco — a
    // diferencia de Efectivo, que siempre se consolida. Sin referencia
    // bancaria, se usa la serie/folio propio del CFDI (armarIndividual cae a
    // `m.serie` cuando no se pasa `serieOverride`). Confirmado con el usuario.
    if (esTransferenciaVerificada) {
      transferencias.push(armarIndividual(
        'Transferencia',
        subcodigoTransferencia,
        null,
        bancario?.referencia,
      ));
      continue;
    }

    // Tarjeta (crédito '04' o débito '28') SIEMPRE se desglosa individual,
    // igual que Transferencia — antes solo se desglosaba cuando había un
    // depósito bancario identificado; sin él, se consolidaba en "Depósitos
    // consolidados (Tarjeta)". Confirmado con el usuario: Tarjeta también
    // debe salir completa, no agrupada. Subcódigo 0 (no tiene código propio
    // como Transferencia/Anticipo — confirmado con el usuario).
    if (LABEL_FORMA_PAGO_CONSOLIDADO[m.formaPago] === 'TARJETA') {
      transferencias.push(armarIndividual('Tarjeta', 0, null, bancario?.referencia));
      continue;
    }

    // CON depósito ligado en Bancos (identificado contra un movimiento real,
    // con su propio número de autorización/referencia) se desglosa
    // individual — sin importar la forma de pago que declare el CFDI
    // (cheque, "99 Por definir", etc. — transferencia y tarjeta ya se
    // manejaron arriba). SIN un depósito real que mostrar, no aplica el
    // desglose: una línea individual repitiendo solo la serie del propio CFDI
    // (que ya aparece en el concepto) no aporta nada — se consolida como
    // cualquier otro cargo, igual que antes. Confirmado con el usuario contra
    // un export real.
    if (bancario?.referencia) {
      transferencias.push(armarIndividual('Depósito identificado', 0, null, bancario.referencia));
      continue;
    }

    const label = LABEL_FORMA_PAGO_CONSOLIDADO[m.formaPago] ?? null;

    const key = `${m.cuenta?.codigo}|${centroCosto}|${label ?? ''}`;
    if (!grupos.has(key)) {
      grupos.set(key, { cuenta: m.cuenta, centroCosto, label, algunaTransferenciaVerificada: false, debe: 0, detalle: [] });
    }
    const g = grupos.get(key);
    g.debe += Number(m.debe);
    if (esTransferenciaVerificada) g.algunaTransferenciaVerificada = true;
    // Se guarda qué CFDI aportó cada monto — no va en la póliza de CONTPAQ
    // (esa línea sigue sin serie/folio, sigue siendo un total agregado), pero
    // permite armar la hoja de desglose para poder rastrear el detalle.
    g.detalle.push({ cfdiUuid: m.cfdiUuid, serie: m.serie, monto: Number(m.debe), formaPago: label ?? m.formaPago ?? null });
  }

  const consolidados = [...grupos.values()].map(g => ({
    cuenta:      g.cuenta,
    serie:       g.label ?? '',
    concepto:    g.label === 'EFECTIVO' ? 'Depósitos consolidados (Efectivo)'
               : g.label === 'TARJETA'  ? 'Depósitos consolidados (Tarjeta)'
               : 'Depósitos consolidados',
    centroCosto: g.centroCosto,
    debe:        g.debe,
    haber:       0,
    cfdiUuid:    null,
    _subcodigo:  g.algunaTransferenciaVerificada ? subcodigoTransferencia : 0,
    _detalle:    g.detalle,
    _esTransferencia: g.algunaTransferenciaVerificada,
    _esResto:    true,
  }));

  return [
    ...porCategoria.devolucion, ...porCategoria.descuento, ...porCategoria.bonificacion,
    ...porCategoria.clubTuberos, ...anticipos, ...transferencias, ...consolidados,
  ];
}

/**
 * Nombre del receptor por CFDI — para identificar a qué cliente pertenece cada
 * factura en el bloque de Ventas de Crédito, donde no se consolida por CFDI
 * (a diferencia de Contado) y por eso el concepto necesita decir de quién es.
 *
 * @param {string[]} cfdiUuids
 * @returns {Promise<Map<string, string>>} uuid (mayúsculas) → nombre del receptor
 */
async function construirNombresClientes(cfdiUuids) {
  const mapa = new Map();
  const uuidsUnicos = [...new Set(cfdiUuids.filter(Boolean).map(u => u.toUpperCase()))];
  if (uuidsUnicos.length === 0) return mapa;

  const cfdis = await CFDI.find(
    { uuid: { $in: uuidsUnicos } },
    { uuid: 1, 'receptor.nombre': 1 },
  ).lean();

  for (const c of cfdis) {
    if (c.receptor?.nombre) mapa.set(c.uuid.toUpperCase(), c.receptor.nombre);
  }
  return mapa;
}

// Marca que `cfdi-poliza-generator.service.js` pone en `tipoOrigen` cuando
// detecta un documento relacionado con Serie='BCT' — se copia a cada
// PolizaMovimiento vía satMeta en cfdiToMovimientos.
const TIPO_ORIGEN_BCT = 'Bonificación Club Tuberos';

/**
 * Reordena las líneas de cada CFDI para que los cargos (debe > 0) queden
 * antes que los abonos (haber > 0) — usado en pólizas de Egreso (NC), donde
 * `movimientos` llega tal cual se guardó (sin pasar por `consolidarCargos`).
 * Algunas NCs viejas (ej. Bonificación Club Tuberos) se generaron con una
 * versión del motor que empujaba el abono antes que el cargo; esto corrige
 * el orden de despliegue en el export sin tocar el `orden` guardado en BD.
 * Solo reordena DENTRO de cada CFDI, nunca entre CFDIs distintos.
 */
function ordenarCargoAntesDeAbono(movs) {
  const porCfdi   = new Map();
  const ordenCfdi = [];
  for (const m of movs) {
    const key = m.cfdiUuid ?? Symbol('sin-cfdi');
    if (!porCfdi.has(key)) { porCfdi.set(key, []); ordenCfdi.push(key); }
    porCfdi.get(key).push(m);
  }

  const resultado = [];
  for (const key of ordenCfdi) {
    const grupo  = porCfdi.get(key);
    const cargos = grupo.filter(m => Number(m.debe) > 0);
    const abonos = grupo.filter(m => !(Number(m.debe) > 0));
    resultado.push(...cargos, ...abonos);
  }
  return resultado;
}

// Detecta si un movimiento de Contado pertenece a un CFDI de ajuste
// (Devolución, Descuento, Bonificación, Club Tuberos o Anticipo) — mismo
// criterio que usa `consolidarCargos` para el lado del cargo, aplicado aquí
// a cualquier lado (cargo o abono) para poder separar el CFDI completo.
function esAjusteContadoMov(m) {
  const plano = m.get ? m.get({ plain: true }) : m;
  return !!(esReglaAnticipo(plano.reglaNombre) || categorizarAjusteContado(plano));
}

// Identifica la línea de IVA/ISR dentro de un grupo de movimientos de un
// mismo CFDI por el NOMBRE de la cuenta (no por código: evita hardcodear
// catálogos distintos entre clientes) — usado para que, dentro del mismo
// lado (cargo o abono), la cuenta de negocio (Ingresos/Devoluciones) quede
// SIEMPRE antes que la de impuestos, confirmado con el usuario contra un
// export real donde aparecían al revés.
const esCuentaImpuesto = (m) => /iva|isr/i.test(m.cuenta?.nombre || '');
const conImpuestoAlFinal = (arr) => [...arr].sort((a, b) => (esCuentaImpuesto(a) ? 1 : 0) - (esCuentaImpuesto(b) ? 1 : 0));

// Serie+folio ("E0-260500003") → { prefijo, folio } para poder ordenar por
// folio NUMÉRICO (no lexicográfico: "9" debe ir antes que "10").
function parseSerieFolio(serie) {
  const m = /^(.*)-(\d+)$/.exec(serie || '');
  return m ? { prefijo: m[1], folio: Number(m[2]) } : { prefijo: serie || '', folio: 0 };
}
function compararSerieFolio(a, b) {
  const pa = parseSerieFolio(a.serie), pb = parseSerieFolio(b.serie);
  if (pa.prefijo !== pb.prefijo) return pa.prefijo < pb.prefijo ? -1 : 1;
  return pa.folio - pb.folio;
}

/**
 * Bloques (uno por CFDI) de los abonos normales de venta (Ingreso+IVA,
 * Contado) — dentro de cada bloque, Ingresos antes de IVA
 * (`conImpuestoAlFinal`). No ordena entre bloques: eso lo hace el caller
 * junto con los bloques de ajuste, para una sola secuencia por serie/folio.
 */
function bloquesAbonosNormales(movs) {
  const porCfdi   = new Map();
  const ordenCfdi = [];
  for (const m of movs) {
    const key = m.cfdiUuid ?? Symbol('sin-cfdi');
    if (!porCfdi.has(key)) { porCfdi.set(key, []); ordenCfdi.push(key); }
    porCfdi.get(key).push(m);
  }
  return ordenCfdi.map(key => conImpuestoAlFinal(porCfdi.get(key)));
}

/**
 * Bloques (uno por CFDI) de ajuste de Contado (Devolución/Cancelación,
 * Descuento, Bonificación, Club Tuberos, Anticipo) — cargo primero, y dentro
 * del mismo lado la cuenta de Ingresos/Devoluciones antes que la de IVA
 * (`conImpuestoAlFinal`). Devolución/Cancelación normalmente NO muestra su
 * abono (reembolso a Clientes/Bancos) — confirmado con el usuario: solo debe
 * verse el cargo (la reversión de Ingresos+IVA). EXCEPCIÓN: cuando el abono
 * es la CREACIÓN de un saldo a favor del cliente (`esAbonoSaldoFavor` —
 * Anticipos Otros / monedero Club Tuberos / su IVA diferido) sí se muestra,
 * porque no es un reembolso sino un nuevo pasivo que el cliente puede usar
 * después — confirmado con el usuario contra un export real.
 *
 * No ordena entre bloques ni entre categorías: el caller (`armarBloqueContado`)
 * los mezcla con los de venta normal y los ordena TODOS juntos por serie/folio
 * — confirmado con el usuario: sin separar por categoría, una sola secuencia
 * continua por folio (el color de fila sigue distinguiendo cada categoría).
 *
 * Reemplaza el esquema anterior donde el cargo de un ajuste vivía en el
 * bloque de "Depósitos consolidados" y su abono en el bloque de abonos
 * normales — lejos uno del otro en el archivo (confirmado contra un export
 * real: el cargo y el abono de una misma NC de Club Tuberos aparecían a
 * miles de filas de distancia).
 */
function bloquesAjustesContado(movs) {
  const porCfdi   = new Map();
  const ordenCfdi = [];

  for (const m of movs) {
    const plano     = m.get ? m.get({ plain: true }) : m;
    const categoria = esReglaAnticipo(plano.reglaNombre) ? 'anticipo' : categorizarAjusteContado(plano);
    if (!categoria) continue;
    const key = plano.cfdiUuid ?? Symbol('sin-cfdi');
    if (!porCfdi.has(key)) { porCfdi.set(key, { categoria, movs: [] }); ordenCfdi.push(key); }
    porCfdi.get(key).movs.push(plano);
  }

  return ordenCfdi.map(key => {
    const { categoria, movs: grupo } = porCfdi.get(key);
    // Anticipo (recepción o aplicación/cierre) siempre lleva subcódigo 22,
    // sin importar cómo se cobró — confirmado con el usuario. Antes esta
    // función solo etiquetaba `_categoria: 'anticipo'` sin asignar el
    // subcódigo, cayendo al 0 por defecto en la hoja de CONTPAQ.
    const extra = categoria === 'anticipo' ? { _subcodigo: 22 } : {};
    const cargos = conImpuestoAlFinal(grupo.filter(m => Number(m.debe) > 0)).map(m => ({ ...m, _categoria: categoria, ...extra }));
    const abonosCandidatos = categoria === 'devolucion'
      ? grupo.filter(m => !(Number(m.debe) > 0) && esAbonoSaldoFavor(m))
      : grupo.filter(m => !(Number(m.debe) > 0));
    return [...cargos, ...conImpuestoAlFinal(abonosCandidatos).map(m => ({ ...m, _categoria: categoria, ...extra }))];
  });
}

/**
 * Arma el bloque completo de Contado: ventas normales y ajustes (Devolución,
 * Descuento, Bonificación, Club Tuberos, Anticipo) mezclados en UNA sola
 * secuencia continua por serie/folio ascendente — confirmado con el usuario:
 * ya no van agrupados por categoría al final, cada CFDI aparece en su
 * posición de folio, con su color de categoría cuando aplica. Al final va el
 * cargo normal consolidado por cuenta/centro de costo/forma de pago. Los CFDI
 * de ajuste se excluyen de `consolidarCargos` para que no se dupliquen ni
 * queden separados de su abono.
 */
function armarBloqueContado(contado, verdadBancaria, nombresClientes) {
  const contadoAjuste = contado.filter(esAjusteContadoMov);
  const contadoNormal = contado.filter(m => !esAjusteContadoMov(m));

  const bloques = [
    ...bloquesAbonosNormales(contadoNormal.filter(m => Number(m.haber) > 0)),
    ...bloquesAjustesContado(contadoAjuste),
  ];
  bloques.sort((b1, b2) => compararSerieFolio(b1[0], b2[0]));

  const abonosYAjustes = enriquecerConceptoConCliente(bloques.flat(), nombresClientes);
  const consolidados   = consolidarCargos(contadoNormal, 21, false, verdadBancaria, nombresClientes);

  return [...abonosYAjustes, ...consolidados];
}

// Igual que `categorizarAjusteContado`, pero también reconoce Anticipo — en
// Crédito no hay bloque de cargo consolidado aparte donde detectarlo, así que
// se agrega aquí como quinta categoría (confirmado con el usuario: mismas 5
// categorías y mismo orden que en Contado).
function categoriaDeGrupoCredito(movs) {
  if (movs.some(m => m.tipoOrigen === TIPO_ORIGEN_BCT || esClubTuberosPorDescripcion(m.concepto))) return 'clubTuberos';
  if (movs.some(m => esVentaConDescuento(m.reglaNombre))) return 'descuento';
  if (movs.some(esDevolucionOCancelacion)) return 'devolucion';
  if (movs.some(esBonificacionGenerica)) return 'bonificacion';
  if (movs.some(m => esReglaAnticipo(m.reglaNombre))) return 'anticipo';
  return null;
}

/**
 * Para Crédito (donde cada CFDI conserva sus movimientos completos, sin
 * consolidar): agrupa por CFDI y arma UNA sola secuencia continua por serie/
 * folio ascendente, mezclando ventas normales y ajustes (Devolución,
 * Descuento, Bonificación, Bonificación Club Tuberos, Anticipo) — mismo
 * criterio que `armarBloqueContado` para Contado, confirmado con el usuario:
 * ya no van agrupados por categoría al final, cada CFDI aparece en su
 * posición de folio (el color de fila sigue distinguiendo cada categoría).
 * Reemplaza a `moverBCTAlFinal` para los bloques de Crédito.
 */
function moverAjustesAlFinal(movs) {
  // Aplanar primero (mismo motivo que `enriquecerConceptoConCliente`): `m`
  // puede ser una instancia de Sequelize, y un spread directo más adelante
  // perdería todos sus campos reales.
  const planos = movs.map(m => (m.get ? m.get({ plain: true }) : m));

  const porCfdi = new Map();
  const ordenCfdi = [];
  for (const m of planos) {
    const key = m.cfdiUuid ?? Symbol('sin-cfdi');
    if (!porCfdi.has(key)) { porCfdi.set(key, []); ordenCfdi.push(key); }
    porCfdi.get(key).push(m);
  }

  const bloques = [];
  for (const key of ordenCfdi) {
    const grupo    = porCfdi.get(key);
    const categoria = categoriaDeGrupoCredito(grupo);
    if (!categoria) { bloques.push(grupo); continue; }
    // Mismo criterio que en Contado: Devolución/Cancelación solo muestra el
    // cargo (salvo que el abono sea la creación de un saldo a favor —
    // `esAbonoSaldoFavor` — que sí se muestra), y dentro de cada lado la
    // cuenta de Ingresos/Devoluciones va antes que la de IVA.
    // Anticipo siempre lleva subcódigo 22, sin importar cómo se cobró —
    // mismo fix que en `bloquesAjustesContado` (Contado); antes solo se
    // etiquetaba `_categoria: 'anticipo'` sin asignar el subcódigo.
    const extra = categoria === 'anticipo' ? { _subcodigo: 22 } : {};
    const cargos = conImpuestoAlFinal(grupo.filter(m => Number(m.debe) > 0)).map(m => ({ ...m, _categoria: categoria, ...extra }));
    const abonosCandidatos = categoria === 'devolucion'
      ? grupo.filter(m => !(Number(m.debe) > 0) && esAbonoSaldoFavor(m))
      : grupo.filter(m => !(Number(m.debe) > 0));
    const bloque = [...cargos, ...conImpuestoAlFinal(abonosCandidatos).map(m => ({ ...m, _categoria: categoria, ...extra }))];
    bloques.push(bloque);
  }

  bloques.sort((b1, b2) => compararSerieFolio(b1[0], b2[0]));
  return bloques.flat();
}

/**
 * Para las líneas que quedan una por CFDI (el abono de Contado y toda la
 * venta de Crédito — no aplica a los renglones ya consolidados de cargo/
 * depósito): reemplaza el concepto original (descripción de productos, a
 * veces muy larga) por "Nombre del cliente / Serie-Folio interno".
 */
function enriquecerConceptoConCliente(movs, nombresClientes) {
  return movs.map(m => {
    // `m` es una instancia de Sequelize — sus campos reales viven detrás de
    // getters, no como propiedades propias. Un spread directo ({...m}) los
    // pierde todos (quedan `undefined`, y luego `Number(undefined)` = NaN al
    // escribir la celda, lo que corrompe el .xlsx). Hay que aplanar primero.
    const plano  = m.get ? m.get({ plain: true }) : m;
    const nombre = nombresClientes.get((plano.cfdiUuid || '').toUpperCase()) || '';
    // `serie` ya viene como "SERIE-FOLIO" completo (ej. "I0-251100402").
    const partes = [nombre, plano.serie].filter(Boolean);
    return { ...plano, concepto: partes.join(' / ') };
  });
}

/**
 * Genera el archivo .xlsx en el layout de importación de pólizas de CONTPAQi
 * (filas P=encabezado, M1=movimiento, AD=UUID de CFDI asociado), calcado de
 * un archivo real ya importado con éxito por el cliente.
 *
 * @param {object} [overrides] — valores editables desde el formulario previo al
 *   export (todo opcional, cae al cálculo por default si no viene):
 *   { fecha, folioContado, folioCredito, conceptoContado, conceptoCredito }
 */
async function exportContpaqXlsx(id, overrides = {}) {
  const poliza = await repo.findByIdLight(id);
  if (!poliza) throw new NotFoundError('Póliza');

  let movimientos = poliza.movimientos ?? [];

  // Filtro opcional por sucursal (centro de costo). Cuando `centroCostoIds` viene
  // definido, sólo se exportan los movimientos de esas sucursales; de lo contrario
  // se exporta la póliza completa (todas las sucursales) como siempre.
  if (overrides.centroCostoIds != null && overrides.centroCostoIds.length > 0) {
    const ids = overrides.centroCostoIds.map(Number);
    movimientos = movimientos.filter(m => ids.includes(Number(m.centroCostoId)));
    if (movimientos.length === 0) {
      throw new ValidationError('Las sucursales seleccionadas no tienen movimientos en esta póliza.');
    }
  }

  const sinCuenta = movimientos.filter(m => m.cuentaFaltante || m.cuentaId == null);
  if (sinCuenta.length > 0) {
    throw new ValidationError(
      `Hay ${sinCuenta.length} movimiento(s) con cuenta faltante en el catálogo — asígnalas antes de exportar a CONTPAQ.`,
    );
  }

  const fechaFinal = overrides.fecha ? new Date(overrides.fecha) : new Date(poliza.fecha);

  // Verdad bancaria: para saber si un cobro fue realmente por transferencia,
  // se prefiere el movimiento bancario real (bank_movements) sobre el
  // `formaPago` que el CFDI declara — ver `construirVerdadBancaria`.
  const verdadBancaria = await construirVerdadBancaria(movimientos.map(m => m.cfdiUuid));

  // Nombres de cliente — para el bloque de Crédito (cada CFDI es su propia
  // línea) y también para la hoja de desglose de los consolidados de Contado
  // (ahí cada CFDI se resume en un total, pero el desglose sí lista cada uno).
  const nombresClientes = await construirNombresClientes(movimientos.map(m => m.cfdiUuid));

  // Las pólizas de Ingresos con movimientos de Contado y de Crédito mezclados se
  // exportan como DOS pólizas de CONTPAQi (folios consecutivos) dentro del mismo
  // archivo/hoja — Numo por dentro sigue manejando una sola póliza combinada.
  // `metodoPago` ya viene poblado desde cfdiToMovimientos → satMeta (cfdi-mapping.service.js).
  let bloques;
  if (poliza.tipo === 'I') {
    const contado = movimientos.filter(m => m.metodoPago !== 'PPD');
    const credito = movimientos.filter(m => m.metodoPago === 'PPD');
    bloques = (contado.length > 0 && credito.length > 0)
      ? [
          // Contado: la práctica contable real solo registra el abono (Ingreso+IVA)
          // por CFDI, y el cargo va consolidado por cuenta/centro de costo (no por
          // factura) — refleja el depósito real de caja/banco del periodo.
          {
            tipoVenta: 'Contado',
            movs:      armarBloqueContado(contado, verdadBancaria, nombresClientes),
            folio:     overrides.folioContado   ?? poliza.numero,
            concepto:  overrides.conceptoContado ?? `${poliza.concepto} - Ventas de Contado`,
          },
          {
            tipoVenta: 'Credito',
            movs:      enriquecerConceptoConCliente(moverAjustesAlFinal(credito), nombresClientes),
            folio:     overrides.folioCredito   ?? (poliza.numero + 1),
            concepto:  overrides.conceptoCredito ?? `${poliza.concepto} - Ventas de Crédito`,
          },
        ]
      // Un solo tipo de venta presente: no hace falta un segundo folio, pero si
      // es Contado igual se consolida el cargo (no se deja uno por factura);
      // en ambos casos (Contado o Crédito) se enriquece el concepto con cliente y serie-folio.
      : [{
          tipoVenta: null,
          movs:      contado.length > 0
            ? armarBloqueContado(contado, verdadBancaria, nombresClientes)
            : enriquecerConceptoConCliente(moverAjustesAlFinal(movimientos), nombresClientes),
          folio:     overrides.folioContado   ?? poliza.numero,
          concepto:  overrides.conceptoContado ?? poliza.concepto,
        }];
  } else {
    // Pólizas de Pago (cobros de facturas PPD): internamente son tipo 'D',
    // identificables porque sus movimientos traen tipoComprobante='P'. El cargo
    // (dinero recibido en Caja/Bancos) se consolida igual que en Contado, pero
    // con subcódigo 20 (PPD) en vez de 21 (PUE) — no aplica el caso OPA aquí,
    // los cobros de PPD nunca son "Recepción de Anticipo".
    const esPagos = movimientos.some(m => m.tipoComprobante === 'P');
    bloques = [{
      tipoVenta: null,
      movs:      esPagos
        ? [...movimientos.filter(m => Number(m.haber) > 0), ...consolidarCargos(movimientos, 20, false, verdadBancaria, nombresClientes)]
        : ordenarCargoAntesDeAbono(movimientos),
      folio:     overrides.folioContado   ?? poliza.numero,
      concepto:  overrides.conceptoContado ?? poliza.concepto,
    }];
  }

  const workbook = new ExcelJS.Workbook();
  const sheet = workbook.addWorksheet('poliza');

  // Detalle de qué CFDIs componen cada línea consolidada (Depósitos/Anticipos) —
  // esas líneas de la póliza no llevan serie/folio propio por ser un total
  // agregado; este arreglo alimenta la hoja "Desglose Consolidado" para poder
  // rastrear después qué facturas conforman cada monto.
  const desgloseConsolidado = [];

  for (const bloque of bloques) {
    // La columna Fecha del encabezado en el archivo real es una celda de fecha
    // genuina (ctype XL_CELL_DATE, formato "m/d/yy"), no un número plano.
    const headerRow = sheet.addRow([
      'P',
      fechaFinal,
      tipoPolContpaq(poliza.tipo),
      bloque.folio,
      '1',
      '0',
      bloque.concepto,
      '11',
      '0',
      '0',
    ]);
    headerRow.getCell(2).numFmt = 'm/d/yy';
    headerRow.eachCell({ includeEmpty: true }, (cell) => {
      cell.font = { color: { argb: 'FFFFFFFF' }, bold: true };
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF000000' } };
    });

    const uuidsVistos = new Set();
    const uuidsOrdenados = [];

    // Alternar un color de relleno por factura (grupo de movimientos con el mismo
    // cfdiUuid) para diferenciarlas a simple vista. `movimientos` viene ordenado
    // por `orden` (repo.findByIdLight), así que cada CFDI ya llega agrupado.
    const FILL_ALTERNADO = ['FFFFFFFF', 'FFF2F2F2'];
    let colorIdx = -1;
    let cfdiAnterior;

    for (const m of bloque.movs) {
      if (m.cfdiUuid !== cfdiAnterior) {
        colorIdx = (colorIdx + 1) % FILL_ALTERNADO.length;
        cfdiAnterior = m.cfdiUuid;
      }

      const esCargo = Number(m.debe) > 0;
      const row = sheet.addRow([
        'M1',
        Number(m.cuenta?.codigo),
        m.serie || '',
        esCargo ? 0 : 1,
        esCargo ? Number(m.debe) : Number(m.haber),
        m._subcodigo ?? 0,
        0,
        m.concepto || '',
        m.centroCostoObj?.clave ?? m.centroCosto ?? '',
      ]);
      // Cada categoría de ajuste (Devolución, Descuento, Bonificación, Club
      // Tuberos, Anticipo) lleva su propio color fijo — tanto en Contado
      // (`consolidarCargos`) como en Crédito (`moverAjustesAlFinal`) — para
      // distinguirlas a simple vista del resto de los movimientos del bloque.
      const colorFila = m._categoria ? COLOR_CATEGORIA[m._categoria]
        : m._esResto ? 'FFF2F2F2'
        : FILL_ALTERNADO[colorIdx];
      row.eachCell({ includeEmpty: true }, (cell) => {
        cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: colorFila } };
      });

      if (m._detalle) {
        for (const d of m._detalle) {
          desgloseConsolidado.push({
            cuenta:           m.cuenta?.codigo,
            centroCosto:      m.centroCostoObj?.clave ?? m.centroCosto ?? '',
            tipo:             m._esAnticipo ? 'Anticipo' : 'Depósito',
            transferencia:    m._esTransferencia ? 'Sí' : 'No',
            formaPago:        d.formaPago || '',
            cfdiSerie:        d.serie || '',
            cliente:          nombresClientes.get((d.cfdiUuid || '').toUpperCase()) || '',
            monto:            d.monto,
          });
        }
      }

      if (m.cfdiUuid && !uuidsVistos.has(m.cfdiUuid)) {
        uuidsVistos.add(m.cfdiUuid);
        uuidsOrdenados.push(m.cfdiUuid);
      }
    }

    for (const uuid of uuidsOrdenados) {
      sheet.addRow(['AD', uuid]);
    }
  }

  // Hoja de desglose: qué CFDIs componen cada línea consolidada de Depósitos/
  // Anticipos (esas líneas en la póliza no llevan serie/folio por ser un total
  // agregado — aquí se puede rastrear el detalle real detrás de cada monto).
  if (desgloseConsolidado.length > 0) {
    const wsDesglose = workbook.addWorksheet('Desglose Consolidado');
    wsDesglose.columns = [
      { header: 'Cuenta',        key: 'cuenta',        width: 14 },
      { header: 'Sucursal',      key: 'centroCosto',   width: 12 },
      { header: 'Tipo',          key: 'tipo',           width: 12 },
      { header: 'Transferencia', key: 'transferencia',  width: 14 },
      { header: 'Forma de pago', key: 'formaPago',      width: 14 },
      { header: 'CFDI (Serie-Folio)', key: 'cfdiSerie', width: 20 },
      { header: 'Cliente',       key: 'cliente',        width: 34 },
      { header: 'Monto',         key: 'monto',          width: 16 },
    ];
    wsDesglose.getRow(1).font = { bold: true };
    wsDesglose.getRow(1).eachCell(cell => {
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFD9D9D9' } };
    });
    desgloseConsolidado
      .sort((a, b) => (a.cuenta - b.cuenta) || a.centroCosto.localeCompare(b.centroCosto))
      .forEach(d => {
        const row = wsDesglose.addRow(d);
        row.getCell('monto').numFmt = '#,##0.00';
      });
    wsDesglose.autoFilter = { from: 'A1', to: 'G1' };
  }

  return { workbook, poliza };
}

/**
 * Registra con qué folio(s) de CONTPAQi quedó asociada la póliza tras importar
 * el archivo exportado — es solo trazabilidad, no vuelve a tocar el archivo.
 */
async function asociarFolioContpaq(id, { folioContado, folioCredito }, user) {
  const poliza = await repo.findByIdLight(id);
  if (!poliza) throw new NotFoundError('Póliza');

  await Poliza.update(
    {
      contpaqFolioContado: folioContado ?? null,
      contpaqFolioCredito: folioCredito ?? null,
      contpaqAsociadoPor:  userLabel(user),
      contpaqAsociadoEn:   new Date(),
    },
    { where: { id } },
  );

  return repo.findByIdLight(id);
}

module.exports = {
  list, getById, create, update, cancel, cancelarTodas, contabilizar, revertir, generarXmlSat,
  reporteDescuadradas, generarCierreIVA, exportContpaqXlsx, asociarFolioContpaq,
  _consolidarCargos: consolidarCargos, _moverAjustesAlFinal: moverAjustesAlFinal,
  _categorizarAjusteContado: categorizarAjusteContado, _categoriaDeGrupoCredito: categoriaDeGrupoCredito,
};
