'use strict';

const ExcelJS = require('exceljs');
const repo = require('./repositories/poliza.repository');
const { NotFoundError, BadRequestError: ValidationError, ForbiddenError } = require('../../shared/errors/AppError');
const { AccountPlan, CfdiMappingRule, PolizaMovimiento, Poliza } = require('../../../shared/models/postgres');
const { Op } = require('sequelize');
const BankMovement = require('../banks/BankMovement.model');
const CFDI = require('../../../visor/models/CFDI');
const { esConceptoMarcadorAjuste } = require('../cfdi-mapping/cfdi-mapping.service');

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
    // La mayoría de los movimientos ligados NO traen `categoria` (viene null)
    // — confirmado contra datos reales: ~18,000 de ~18,650 movimientos con
    // erpLinks no tienen categoria. Sin esto, `esTransferencia` (abajo)
    // siempre da `false` para ellos, y el caller (consolidarCargos) lo
    // tomaba como "confirmado que NO es transferencia", perdiendo el
    // subcódigo 21 en transferencias reales solo por falta de categoría.
    const categoriaConocida = m.categoria != null;

    for (const link of (m.erpLinks ?? [])) {
      const folioFiscalUpper = (link.folioFiscal || '').toUpperCase();
      if (!uuidsSet.has(folioFiscalUpper)) continue;
      // Un mismo CFDI puede tener varios movimientos ligados (varias
      // parcialidades) — si alguno confirma transferencia, esa gana.
      const actual = mapa.get(folioFiscalUpper);
      if (!actual || (!actual.esTransferencia && esTransferencia)) {
        mapa.set(folioFiscalUpper, { esTransferencia, referencia, categoriaConocida });
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
 * Lista TODAS las pólizas en borrador del rfc/ejercicio/periodo — sin el tope
 * de 100 que aplica `list()` (paginado, para la tabla) — para alimentar el
 * modal de selección de "Cancelar todas". Mismo alcance/where que usa
 * `cancelarTodas` para poder cancelar exactamente lo que aquí se muestra.
 */
async function listBorradorCandidatas({ rfc, ejercicio, periodo }) {
  if (!rfc)       throw new ValidationError('RFC requerido');
  if (!ejercicio) throw new ValidationError('Ejercicio requerido');
  if (!periodo)   throw new ValidationError('Periodo requerido');

  const polizas = await Poliza.findAll({
    where: { rfc, ejercicio: Number(ejercicio), periodo: Number(periodo), estado: 'borrador' },
    attributes: ['id', 'tipo', 'numero', 'concepto', 'fecha'],
    order: [['fecha', 'DESC'], ['tipo', 'ASC'], ['numero', 'DESC']],
  });
  return polizas;
}

/**
 * Cancela las pólizas en estado 'borrador' del rfc/ejercicio/periodo indicado
 * (mismo alcance que usa el resto de la pantalla de Pólizas para
 * generar/exportar). Deliberadamente excluye 'contabilizada' y 'cancelada' —
 * las contabilizadas requieren el permiso de admin y se cancelan una por una
 * desde su propio modal, no en bulk.
 *
 * Si polizaIds viene con elementos, solo cancela esas (selección manual desde
 * el modal de "Cancelar todas"); si no, cancela todas las de borrador del
 * periodo (comportamiento previo).
 *
 * Reutiliza `cancel()` por cada póliza (misma validación, mismo aviso de IVA
 * PPD) en vez de duplicar la lógica — un error en una póliza no detiene las
 * demás.
 *
 * Devuelve: { canceladas: number, errores: [{ polizaId, numero, tipo, error }] }
 */
async function cancelarTodas({ rfc, ejercicio, periodo, polizaIds }, user, motivo) {
  if (!rfc)       throw new ValidationError('RFC requerido');
  if (!ejercicio) throw new ValidationError('Ejercicio requerido');
  if (!periodo)   throw new ValidationError('Periodo requerido');

  const where = { rfc, ejercicio: Number(ejercicio), periodo: Number(periodo), estado: 'borrador' };
  // Selección manual desde el modal — si no viene, se cancelan todas las de borrador.
  if (Array.isArray(polizaIds) && polizaIds.length) {
    where.id = polizaIds;
  }

  const polizas = await Poliza.findAll({
    where,
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
// c_FormaPago SAT para "Cheque nominativo" — igual que Transferencia, se
// desglosa siempre por CFDI y solo se agrupa cuando comparte el mismo número
// de autorización/referencia real ligado en Bancos (confirmado con el usuario
// 2026-07-24; ver bloque de detalle en `consolidarCargos`).
const FORMA_PAGO_CHEQUE = '02';

// c_FormaPago SAT → etiqueta para las líneas consolidadas de Efectivo/Tarjeta
// (Transferencia y Cheque nunca llegan aquí — se desglosan individual, ver
// `consolidarCargos`). formaPago sin mapear (distinto de estos cuatro) cae al
// bucket genérico de siempre (sin etiqueta), confirmado con el usuario contra
// un export real donde Efectivo y Tarjeta salen en cuentas/líneas separadas.
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
// Caja por identificar (1101010003): cuenta puente para NCs "CANCELACION-
// refacturación" (Serie=CANCELACION en documentosRelacionados) — no es un
// reembolso real en efectivo/banco, es una cancelación de facturación sin
// movimiento de dinero real. Confirmado con el usuario 2026-07-17: su abono
// SÍ debe mostrarse (a diferencia de un reembolso real, que se oculta).
// NOTA 2026-07-17: Caja por identificar (1101010003, cuenta puente de las NCs
// "CANCELACION-refacturación") se probó primero incluida aquí para que su
// abono SÍ se mostrara — el usuario revirtió esa decisión: en el reporte
// CONTPAQ, una CANCELACION debe mostrar SIEMPRE solo sus 2 cargos
// (Devoluciones+IVA), nunca su abono, sin importar si tiene o no una
// refacturación pareja. Por eso NO se agrega 1101010003 a este set.
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
// Excluye las reglas "CC-CAN-D-*" (ej. "NC Cancelación Descuento 16%
// Efectivo") -- son Notas de Crédito de Cancelación cuyo nombre también
// menciona "descuento" (describe la tasa/mecánica fiscal, no el tipo de
// ajuste), y sin esta exclusión ganaban la prioridad sobre Devolución/
// Cancelación, cayendo siempre en la categoría "descuento" genérica y
// perdiendo el sufijo CANCELACION-REFACTURACION/CANCELACION-DEV/CAC
// (encontrado 2026-07-23 al validar el export real de una CC-CAN-D-16-EF).
const esVentaConDescuento = (reglaNombre) => /descuento/i.test(reglaNombre || '') && !/cancelaci[oó]n/i.test(reglaNombre || '');

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
 * Efectivo y Tarjeta se consolidan CADA UNO en su propia línea/cuenta
 * ("Depósitos consolidados (Efectivo/Tarjeta)") — pero SOLO cuando NO tienen
 * un depósito bancario real identificado.
 *
 * En cuanto un movimiento de Efectivo o Tarjeta SÍ tiene un número de
 * autorización/referencia real ligado en Bancos (`verdadBancaria`), se SACA
 * del consolidado y se muestra como línea individual con esa referencia como
 * serie — confirmado con el usuario con un ejemplo concreto: 3 CFDIs de
 * Tarjeta por $1,000, dos sin match bancario y uno con match, deben verse
 * como "Tarjeta" consolidada ($2,000) + 1 línea individual ($1,000) con su
 * número de autorización, no los 3 juntos. Esta línea individual ("Depósito
 * identificado") se devuelve aparte para que el caller la coloque al final
 * del export.
 *
 * Transferencia y Cheque NUNCA se consolidan en un bucket genérico (a
 * diferencia de Efectivo/Tarjeta): cada una se muestra en su propia línea con
 * su serie-folio real, salvo que dos o más (del mismo tipo) compartan el
 * MISMO número de autorización/referencia real ligado en Bancos — en ese
 * caso sí se juntan en una sola línea, porque es literalmente el mismo
 * depósito bancario aplicado a varias facturas (confirmado con el usuario
 * 2026-07-24). Estas líneas también se devuelven en `depositosIdentificados`,
 * junto con Tarjeta/Depósito identificado.
 *
 * @returns {{
 *   porCategoria: { devolucion: object[], descuento: object[], bonificacion: object[], clubTuberos: object[] },
 *   anticipos: object[], consolidados: object[], depositosIdentificados: object[],
 * }} — cada arreglo ya viene ordenado por serie/folio (salvo `consolidados`,
 *   ordenado Efectivo → Tarjeta). El caller decide en qué secuencia los
 *   concatena (ver `aplanarCargosConsolidados` y `armarBloqueContado`).
 */
function consolidarCargos(movs, subcodigoTransferencia, detectarAnticipo = false, verdadBancaria = null, nombresClientes = null) {
  const grupos = new Map();
  const gruposDetallados = new Map(); // Transferencia y Cheque: agrupan SOLO por mismo número de autorización real
  const porCategoria = { devolucion: [], descuento: [], bonificacion: [], clubTuberos: [] };
  const anticipos = [];              // Recepción Y Aplicación
  const depositosIdentificados = []; // forma de pago sin mapear + depósito real ligado en Bancos — va al final

  for (const m of movs) {
    if (!(Number(m.debe) > 0)) continue;
    // Refacturación (factura que reemplaza una venta cancelada, ver
    // `esRefacturacion` en cfdi-poliza-generator.service.js): su cargo
    // (dinero en banco/caja) ya quedó contabilizado en el asiento de la
    // CANCELACION original — mostrarlo aquí (individual o consolidado)
    // duplicaría el depósito. Se omite por completo; su abono (Ingreso+IVA)
    // sigue su camino normal (bloquesAbonosNormales), sin tocar.
    if (/refacturaci[oó]n/i.test(m.tipoOrigen || '')) continue;
    const centroCosto = m.centroCostoObj?.clave ?? m.centroCosto ?? '';
    const bancario    = verdadBancaria?.get((m.cfdiUuid || '').toUpperCase());
    // Solo se confía en `bancario.esTransferencia` cuando el movimiento
    // bancario SÍ trae `categoria` (SPEI/TRASPASO/otra) — si el match existe
    // pero la categoría nunca se llenó (`categoriaConocida: false`, el caso
    // más común), no hay evidencia real de que NO sea transferencia, así que
    // se usa el formaPago que el propio CFDI declaró. Confirmado con el
    // usuario: sin esto se perdía el subcódigo 21 en transferencias reales
    // solo por falta de categoría en bank_movements.
    const esTransferenciaVerificada = bancario?.categoriaConocida
      ? bancario.esTransferencia
      : (m.formaPago === FORMA_PAGO_TRANSFERENCIA);
    const esAnticipo        = detectarAnticipo && esReglaAnticipo(m.reglaNombre);
    const esAnticipoSinUsar = esAnticipo && esRecepcionAnticipo(m.reglaNombre);

    const armarIndividual = (etiqueta, subcodigo, categoria, serieOverride) => {
      const nombre = nombresClientes?.get((m.cfdiUuid || '').toUpperCase()) || '';
      // Devolución (no Cancelación): el concepto debe terminar en "DEV" —
      // mismo criterio que `enriquecerConceptoConCliente`, confirmado con el
      // usuario 2026-07-22.
      const esCancelacionAqui = categoria === 'devolucion' && /cancelaci[oó]n/i.test(m.tipoOrigen || '');
      const serieSufijo = (categoria === 'devolucion' && !esCancelacionAqui && m.serie)
        ? `${m.serie} DEV`
        : m.serie;
      const concepto = [nombre, serieSufijo].filter(Boolean).join(' / ') || etiqueta;
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

    // Transferencia y Cheque: SIEMPRE se detallan (nunca caen al bucket
    // genérico de "Depósitos consolidados") — solo se agrupan entre sí las
    // que comparten el MISMO número de autorización/referencia real
    // (bancario.referencia), porque eso significa que son literalmente el
    // mismo depósito bancario aplicado a varias facturas. Sin ese match, cada
    // una queda en su propia línea (con su serie-folio propio), nunca
    // mezclada con otra solo por compartir forma de pago (corregido
    // 2026-07-24: antes, cualquier transferencia sin depósito bancario ligado
    // caía al bucket genérico junto con otras transferencias no relacionadas
    // entre sí, perdiendo el detalle por CFDI; extendido a Cheque el mismo
    // día, mismo criterio).
    const esChequeDeclarado = m.formaPago === FORMA_PAGO_CHEQUE;
    if (esTransferenciaVerificada || esChequeDeclarado) {
      const tipoDetalle = esTransferenciaVerificada ? 'TRANSFERENCIA' : 'CHEQUE';
      const subcodigoDetalle = esTransferenciaVerificada ? subcodigoTransferencia : 0;
      const referencia = bancario?.referencia ?? null;
      const key = `${m.cuenta?.codigo}|${centroCosto}|${tipoDetalle}|${referencia ?? `__cfdi_${m.cfdiUuid}`}`;
      if (!gruposDetallados.has(key)) {
        gruposDetallados.set(key, {
          cuenta: m.cuenta, centroCosto, referencia, tipoDetalle, subcodigo: subcodigoDetalle,
          debe: 0, detalle: [], primerMov: m,
        });
      }
      const gt = gruposDetallados.get(key);
      gt.debe += Number(m.debe);
      gt.detalle.push({ cfdiUuid: m.cfdiUuid, serie: m.serie, monto: Number(m.debe), formaPago: tipoDetalle });
      continue;
    }

    // Depósito real identificado en Bancos (Tarjeta u otra forma de pago con
    // número de autorización/referencia real ligado; Transferencia y Cheque
    // ya se manejaron arriba) — SIEMPRE se saca del consolidado. La etiqueta
    // conserva la forma de pago real cuando se conoce, para que la línea
    // individual siga siendo legible aunque ya no vaya agrupada.
    if (bancario?.referencia) {
      const etiquetaIdentificado = LABEL_FORMA_PAGO_CONSOLIDADO[m.formaPago] === 'TARJETA' ? 'Tarjeta'
        : 'Depósito identificado';
      depositosIdentificados.push(armarIndividual(etiquetaIdentificado, 0, null, bancario.referencia));
      continue;
    }

    // Sin depósito real que mostrar: se consolida por la forma de pago
    // declarada (Efectivo o Tarjeta — cada una en su propia línea/cuenta;
    // Transferencia y Cheque ya se manejaron arriba, nunca llegan aquí).
    const label = LABEL_FORMA_PAGO_CONSOLIDADO[m.formaPago] === 'TARJETA' ? 'TARJETA'
      : LABEL_FORMA_PAGO_CONSOLIDADO[m.formaPago] ?? null;

    const key = `${m.cuenta?.codigo}|${centroCosto}|${label ?? ''}`;
    if (!grupos.has(key)) {
      grupos.set(key, { cuenta: m.cuenta, centroCosto, label, debe: 0, detalle: [] });
    }
    const g = grupos.get(key);
    g.debe += Number(m.debe);
    // Se guarda qué CFDI aportó cada monto — no va en la póliza de CONTPAQ
    // (esa línea sigue sin serie/folio, sigue siendo un total agregado), pero
    // permite armar la hoja de desglose para poder rastrear el detalle.
    g.detalle.push({ cfdiUuid: m.cfdiUuid, serie: m.serie, monto: Number(m.debe), formaPago: label ?? m.formaPago ?? null });
  }

  // Efectivo → Tarjeta → resto, siempre en ese orden dentro de los cargos
  // consolidados (confirmado con el usuario). Transferencia y Cheque nunca
  // llegan a este bucket (ver arriba) — se arman aparte más abajo, dentro de
  // `depositosIdentificados` (bucle sobre `gruposDetallados`).
  const ORDEN_LABEL_CONSOLIDADO = { EFECTIVO: 0, TARJETA: 1 };
  const consolidados = [...grupos.values()]
    .map(g => ({
      cuenta:      g.cuenta,
      serie:       g.label ?? '',
      concepto:    g.label === 'EFECTIVO' ? 'Depósitos consolidados (Efectivo)'
                 : g.label === 'TARJETA'  ? 'Depósitos consolidados (Tarjeta)'
                 : 'Depósitos consolidados',
      centroCosto: g.centroCosto,
      debe:        g.debe,
      haber:       0,
      cfdiUuid:    null,
      _subcodigo:  0,
      _detalle:    g.detalle,
      _esTransferencia: false,
      _esResto:    true,
    }))
    .sort((a, b) => (ORDEN_LABEL_CONSOLIDADO[a.serie] ?? 2) - (ORDEN_LABEL_CONSOLIDADO[b.serie] ?? 2));

  // Transferencia y Cheque detallados: una línea por CFDI, salvo cuando dos o
  // más comparten el mismo número de autorización/referencia real
  // (bancario.referencia) — en ese caso sí se consolidan en una sola línea,
  // porque es literalmente el mismo depósito bancario aplicado a varias
  // facturas. Se agregan a `depositosIdentificados` para conservar el mismo
  // lugar en el export (al final, junto con Tarjeta/Depósito identificado)
  // que ya tenían las transferencias con match bancario antes de este cambio.
  // Individual: "Cliente / Serie-Folio Transferencia" (o "Cheque") — mismo
  // patrón que Devolución, que agrega "DEV" al final de la serie (confirmado
  // con el usuario 2026-07-24). Agrupada (mismo número de autorización real
  // en 2+ CFDIs): sin cliente único que mostrar, solo la etiqueta.
  const ETIQUETA_TIPO_DETALLE = { TRANSFERENCIA: 'TRANSFERENCIA', CHEQUE: 'CHEQUE' };
  for (const gt of gruposDetallados.values()) {
    const m = gt.primerMov;
    const etiqueta = ETIQUETA_TIPO_DETALLE[gt.tipoDetalle];
    const esGrupo = gt.detalle.length > 1;
    const nombre = nombresClientes?.get((m.cfdiUuid || '').toUpperCase()) || '';
    const serieFinal = gt.referencia ?? (m.serie || '');
    // Individual (un solo CFDI): la columna C (serie) pasa a mostrar el TIPO
    // ("Transferencia"/"Cheque") en vez del serie-folio — el serie-folio se
    // conserva en el concepto (columna H) junto al cliente, sin repetir la
    // etiqueta ahí. Confirmado con el usuario 2026-07-28. Agrupada (mismo
    // número de autorización real en 2+ CFDIs): sin cambios — la columna C
    // sigue mostrando la referencia bancaria real (gt.referencia), dato que
    // se perdería si también se reemplazara por la etiqueta.
    const concepto = esGrupo ? etiqueta : ([nombre, serieFinal].filter(Boolean).join(' / ') || etiqueta);
    const serieColumnaC = esGrupo ? serieFinal : etiqueta;
    depositosIdentificados.push({
      cuenta: gt.cuenta, serie: serieColumnaC, concepto,
      centroCosto: gt.centroCosto, debe: gt.debe, haber: 0,
      cfdiUuid: esGrupo ? null : m.cfdiUuid, _subcodigo: gt.subcodigo,
      _categoria: null,
      ...(esGrupo ? { _detalle: gt.detalle, _esTransferencia: gt.tipoDetalle === 'TRANSFERENCIA', _esResto: true } : {}),
    });
  }

  // Cada arreglo se ordena internamente por serie/folio ascendente — antes
  // quedaban en el orden en que llegaron los CFDIs de entrada (arbitrario).
  const porSerieFolio = (arr) => [...arr].sort(compararSerieFolio);

  return {
    porCategoria: {
      devolucion:   porSerieFolio(porCategoria.devolucion),
      descuento:    porSerieFolio(porCategoria.descuento),
      bonificacion: porSerieFolio(porCategoria.bonificacion),
      clubTuberos:  porSerieFolio(porCategoria.clubTuberos),
    },
    anticipos: porSerieFolio(anticipos),
    consolidados,
    depositosIdentificados: porSerieFolio(depositosIdentificados),
  };
}

/**
 * Aplana el resultado de `consolidarCargos` en el orden legado (usado por el
 * bloque de Pagos/PPD, que no tiene el reordenamiento especial de Contado):
 * Devolución, Descuento, Bonificación, Club Tuberos, Anticipos, cargos
 * consolidados (Efectivo/Tarjeta) y, al final, Transferencia/Depósito
 * identificado.
 */
function aplanarCargosConsolidados(resultado) {
  return [
    ...resultado.porCategoria.devolucion, ...resultado.porCategoria.descuento, ...resultado.porCategoria.bonificacion,
    ...resultado.porCategoria.clubTuberos, ...resultado.anticipos, ...resultado.consolidados, ...resultado.depositosIdentificados,
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
 * No ordena entre bloques: el caller (`armarBloqueContado`) agrupa cada
 * categoría en su sección correspondiente del export y ordena por
 * serie/folio DENTRO de cada sección (ver esa función para el orden completo).
 *
 * Reemplaza el esquema anterior donde el cargo de un ajuste vivía en el
 * bloque de "Depósitos consolidados" y su abono en el bloque de abonos
 * normales — lejos uno del otro en el archivo (confirmado contra un export
 * real: el cargo y el abono de una misma NC de Club Tuberos aparecían a
 * miles de filas de distancia).
 *
 * @returns {{categoria: string, bloque: object[]}[]} — bloque = movimientos
 *   (cargo+abono) de un mismo CFDI, `categoria` para que el caller decida en
 *   qué sección va (devolucion/descuento/bonificacion → ventas normales;
 *   clubTuberos → su propia sección; anticipo → Anticipos y saldo a favor).
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
    const bloque = [...cargos, ...conImpuestoAlFinal(abonosCandidatos).map(m => ({ ...m, _categoria: categoria, ...extra }))];
    return { categoria, bloque };
  });
}

/**
 * Arma el bloque completo de Contado en 5 secciones, en este orden fijo
 * (confirmado con el usuario):
 *   1. Ventas normales — incluye Devolución/Cancelación y Bonificación
 *      genérica (Descuento igual) — una sola secuencia por serie/folio.
 *   2. Bonificación Club Tuberos — su propia sección, por serie/folio.
 *   3. Cargo consolidado por forma de pago: Efectivo, Tarjeta.
 *   4. Anticipos y saldo a favor (Recepción y Aplicación), por serie/folio.
 *   5. Transferencia (siempre detallada por CFDI, o agrupada cuando comparte
 *      número de autorización real) y Depósito identificado (forma de pago
 *      sin mapear con depósito bancario real ligado) — al final del export.
 */
// `separarCategorias` (usado solo para la sucursal CEDIS — ver exportContpaqXlsx):
// cuando es `true`, Bonificación (genérica + Club Tuberos) y Descuento/Devolución
// (incluye Cancelación) NO se meten a `ventas` — se devuelven aparte para que el
// caller las arme como sus propias pólizas. Anticipos y depósitos identificados
// no cambian, siguen dentro de `ventas` igual que siempre.
function armarBloqueContado(contado, verdadBancaria, nombresClientes, { separarCategorias = false } = {}) {
  const contadoAjuste = contado.filter(esAjusteContadoMov);
  const contadoNormal = contado.filter(m => !esAjusteContadoMov(m));

  const ajustes = bloquesAjustesContado(contadoAjuste);
  const bloquesDeCategorias = (...categorias) =>
    ajustes.filter(a => categorias.includes(a.categoria)).map(a => a.bloque);

  const bloquesVentas = [
    ...bloquesAbonosNormales(contadoNormal.filter(m => Number(m.haber) > 0)),
    ...(separarCategorias ? [] : bloquesDeCategorias('devolucion', 'descuento', 'bonificacion')),
  ];
  bloquesVentas.sort((b1, b2) => compararSerieFolio(b1[0], b2[0]));

  const bloquesClubTuberos = separarCategorias ? [] : bloquesDeCategorias('clubTuberos');
  bloquesClubTuberos.sort((b1, b2) => compararSerieFolio(b1[0], b2[0]));

  const bloquesAnticipos = bloquesDeCategorias('anticipo');
  bloquesAnticipos.sort((b1, b2) => compararSerieFolio(b1[0], b2[0]));

  const { consolidados, depositosIdentificados } =
    consolidarCargos(contadoNormal, 21, false, verdadBancaria, nombresClientes);

  const ventasYClubTuberos = enriquecerConceptoConCliente(
    [...bloquesVentas.flat(), ...bloquesClubTuberos.flat()],
    nombresClientes,
  );
  const anticiposEnriquecidos = enriquecerConceptoConCliente(bloquesAnticipos.flat(), nombresClientes);

  const ventas = [...ventasYClubTuberos, ...consolidados, ...anticiposEnriquecidos, ...depositosIdentificados];

  if (!separarCategorias) return ventas;

  const bloquesBonificaciones = [...bloquesDeCategorias('bonificacion'), ...bloquesDeCategorias('clubTuberos')];
  bloquesBonificaciones.sort((b1, b2) => compararSerieFolio(b1[0], b2[0]));

  const bloquesDescuentoDevolucion = [...bloquesDeCategorias('descuento'), ...bloquesDeCategorias('devolucion')];
  bloquesDescuentoDevolucion.sort((b1, b2) => compararSerieFolio(b1[0], b2[0]));

  return {
    ventas,
    bonificaciones:         enriquecerConceptoConCliente(bloquesBonificaciones.flat(), nombresClientes),
    descuentosDevoluciones: enriquecerConceptoConCliente(bloquesDescuentoDevolucion.flat(), nombresClientes),
  };
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
// `separarCategorias` (usado solo para la sucursal CEDIS — ver exportContpaqXlsx):
// mismo criterio que `armarBloqueContado` — Bonificación (genérica + Club
// Tuberos) y Descuento/Devolución (incluye Cancelación) se devuelven aparte en
// vez de mezclarse en la secuencia normal. Anticipo (y venta normal, categoría
// `null`) se quedan en `ventas`, igual que siempre.
function moverAjustesAlFinal(movs, { separarCategorias = false } = {}) {
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

  const bloques = []; // { categoria: string|null, bloque: object[] }
  for (const key of ordenCfdi) {
    const grupo    = porCfdi.get(key);
    const categoria = categoriaDeGrupoCredito(grupo);
    if (!categoria) { bloques.push({ categoria: null, bloque: grupo }); continue; }
    // A diferencia de Contado, en Crédito SIEMPRE se muestran los 3 registros
    // de una Devolución/Cancelación (los 2 cargos + su abono, sea reembolso
    // real en banco o saldo a favor) — confirmado con el usuario 2026-07-23.
    // Dentro de cada lado, la cuenta de Ingresos/Devoluciones va antes que la
    // de IVA (`conImpuestoAlFinal`).
    // Anticipo siempre lleva subcódigo 22, sin importar cómo se cobró —
    // mismo fix que en `bloquesAjustesContado` (Contado); antes solo se
    // etiquetaba `_categoria: 'anticipo'` sin asignar el subcódigo.
    const extra = categoria === 'anticipo' ? { _subcodigo: 22 } : {};
    const cargos = conImpuestoAlFinal(grupo.filter(m => Number(m.debe) > 0)).map(m => ({ ...m, _categoria: categoria, ...extra }));
    const abonosCandidatos = grupo.filter(m => !(Number(m.debe) > 0));
    const bloque = [...cargos, ...conImpuestoAlFinal(abonosCandidatos).map(m => ({ ...m, _categoria: categoria, ...extra }))];
    bloques.push({ categoria, bloque });
  }

  bloques.sort((b1, b2) => compararSerieFolio(b1.bloque[0], b2.bloque[0]));

  if (!separarCategorias) return bloques.map(b => b.bloque).flat();

  const deCategorias = (...categorias) =>
    bloques.filter(b => categorias.includes(b.categoria)).map(b => b.bloque).flat();

  return {
    ventas:                 deCategorias(null, 'anticipo'),
    bonificaciones:         deCategorias('bonificacion', 'clubTuberos'),
    descuentosDevoluciones: deCategorias('descuento', 'devolucion'),
  };
}

/**
 * Para las líneas que quedan una por CFDI (el abono de Contado y toda la
 * venta de Crédito — no aplica a los renglones ya consolidados de cargo/
 * depósito): reemplaza el concepto original (descripción de productos, a
 * veces muy larga) por "Nombre del cliente / Serie-Folio".
 *
 * Para movimientos de ajuste (Club Tuberos/Bonificación/Devolución/
 * Cancelación en cualquiera de sus variantes: BON, BEP, BXC, BN, DEV, DVE,
 * CANCELACION, CAC, ANN, CES...), `plano.serie` es la serie-folio de la
 * factura/CFDI (columna C, nunca el marcador -- corregido 2026-07-24) y
 * `plano.concepto` YA trae el marcador del ajuste (ej. "DEV-054861"), puesto
 * por cfdi-mapping.service.js (`_serieMarcadorAjuste`, ver `cfdiToMovimientos`)
 * al momento de generar la póliza -- aquí se detecta con
 * `esConceptoMarcadorAjuste` y se preserva en vez de usar la serie-folio de
 * la factura, para que la columna H siga mostrando el marcador del ajuste.
 */
function enriquecerConceptoConCliente(movs, nombresClientes) {
  return movs.map(m => {
    // `m` es una instancia de Sequelize — sus campos reales viven detrás de
    // getters, no como propiedades propias. Un spread directo ({...m}) los
    // pierde todos (quedan `undefined`, y luego `Number(undefined)` = NaN al
    // escribir la celda, lo que corrompe el .xlsx). Hay que aplanar primero.
    const plano  = m.get ? m.get({ plain: true }) : m;
    const nombre = nombresClientes.get((plano.cfdiUuid || '').toUpperCase()) || '';
    const refSerieOMarcador = esConceptoMarcadorAjuste(plano.concepto) ? plano.concepto : plano.serie;
    const partes = [nombre, refSerieOMarcador].filter(Boolean);
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

  // CEDIS es la única sucursal donde, además de Contado/Crédito, se piden
  // Bonificaciones y Descuentos/Devoluciones/Cancelaciones como pólizas propias
  // (ver rama `esCedis` más abajo) — el resto de sucursales sigue igual.
  const esCedis = movimientos.length > 0 &&
    movimientos.every(m => (m.centroCostoObj?.sucursal || '').trim().toUpperCase() === 'CEDIS');

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
  //
  // poliza.concepto es la única fuente de la fecha/sucursal (ver
  // _construirConceptoIngresoBase en cfdi-poliza-generator.service.js) — aquí
  // solo se le inserta el calificativo Contado/Credito para columna G del
  // Excel, nunca se recalcula la fecha para evitar que se desincronice del
  // encabezado (columna B, `fechaFinal`). Pólizas viejas (concepto formato
  // "CFDIs MM/YYYY...") conservan su comportamiento anterior.
  const _conceptoConTipoVenta = (tipoVenta, sufijoLegacy) => poliza.concepto?.startsWith('Ingresos por Ventas ')
    ? poliza.concepto.replace('Ingresos por Ventas ', `Ingresos por Ventas de ${tipoVenta} `)
    : sufijoLegacy;

  let bloques;
  if (poliza.tipo === 'I') {
    const contado = movimientos.filter(m => m.metodoPago !== 'PPD');
    const credito = movimientos.filter(m => m.metodoPago === 'PPD');

    if (esCedis) {
      // CEDIS: hasta 6 pólizas — Contado, Crédito, Bonificaciones de Contado,
      // Bonificaciones de Crédito, Descuentos/Devoluciones/Cancelaciones de
      // Contado y de Crédito. Solo se genera la que tenga movimientos —
      // mismo principio que ya usa el resto de sucursales cuando falta
      // Contado o Crédito (folios consecutivos, sin huecos).
      const cSplit = contado.length > 0
        ? armarBloqueContado(contado, verdadBancaria, nombresClientes, { separarCategorias: true })
        : { ventas: [], bonificaciones: [], descuentosDevoluciones: [] };
      const rSplit = credito.length > 0
        ? moverAjustesAlFinal(credito, { separarCategorias: true })
        : { ventas: [], bonificaciones: [], descuentosDevoluciones: [] };

      let folio = overrides.folioContado ?? poliza.numero;
      bloques = [];
      const push = (tipoVenta, movs, folioOverride) => {
        if (!movs.length) return;
        bloques.push({
          tipoVenta,
          movs,
          folio:    folioOverride ?? folio++,
          concepto: _conceptoConTipoVenta(tipoVenta, `${poliza.concepto} - ${tipoVenta}`),
        });
      };
      // `armarBloqueContado` (Contado) ya enriquece el concepto internamente;
      // `moverAjustesAlFinal` (Crédito) no lo hace — mismo patrón que el
      // camino legado (línea de abajo: `enriquecerConceptoConCliente(moverAjustesAlFinal(credito), ...)`).
      push('Contado',                              cSplit.ventas,                                                         overrides.folioContado);
      push('Credito',                              enriquecerConceptoConCliente(rSplit.ventas, nombresClientes),          overrides.folioCredito);
      push('Bonificaciones de Contado',             cSplit.bonificaciones);
      push('Bonificaciones de Crédito',             enriquecerConceptoConCliente(rSplit.bonificaciones, nombresClientes));
      push('Descuentos y Devoluciones de Contado',  cSplit.descuentosDevoluciones);
      push('Descuentos y Devoluciones de Crédito',  enriquecerConceptoConCliente(rSplit.descuentosDevoluciones, nombresClientes));

      if (bloques.length === 0) {
        throw new ValidationError('No hay movimientos para generar la póliza de CEDIS.');
      }
    } else

    bloques = (contado.length > 0 && credito.length > 0)
      ? [
          // Contado: la práctica contable real solo registra el abono (Ingreso+IVA)
          // por CFDI, y el cargo va consolidado por cuenta/centro de costo (no por
          // factura) — refleja el depósito real de caja/banco del periodo.
          {
            tipoVenta: 'Contado',
            movs:      armarBloqueContado(contado, verdadBancaria, nombresClientes),
            folio:     overrides.folioContado   ?? poliza.numero,
            concepto:  overrides.conceptoContado ?? _conceptoConTipoVenta('Contado', `${poliza.concepto} - Ventas de Contado`),
          },
          {
            tipoVenta: 'Credito',
            movs:      enriquecerConceptoConCliente(moverAjustesAlFinal(credito), nombresClientes),
            folio:     overrides.folioCredito   ?? (poliza.numero + 1),
            concepto:  overrides.conceptoCredito ?? _conceptoConTipoVenta('Credito', `${poliza.concepto} - Ventas de Crédito`),
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
          concepto:  overrides.conceptoContado ?? _conceptoConTipoVenta(contado.length > 0 ? 'Contado' : 'Credito', poliza.concepto),
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
        ? [...movimientos.filter(m => Number(m.haber) > 0), ...aplanarCargosConsolidados(consolidarCargos(movimientos, 20, false, verdadBancaria, nombresClientes))]
        : ordenarCargoAntesDeAbono(movimientos),
      folio:     overrides.folioContado   ?? poliza.numero,
      concepto:  overrides.conceptoContado ?? poliza.concepto,
    }];
  }

  if (esCedis) {
    // CEDIS: 3 archivos — Ventas (Contado+Crédito), Bonificaciones (Contado+
    // Crédito) y Descuentos y Devoluciones (Contado+Crédito). Cada archivo
    // trae 1 o 2 encabezados 'P' — mismo patrón que ya usa el resto de
    // sucursales para Contado/Crédito (un solo folio si falta un lado, dos
    // folios consecutivos compartiendo archivo si están ambos).
    const GRUPO_POR_TIPO_VENTA = {
      'Contado': 'Ventas', 'Credito': 'Ventas',
      'Bonificaciones de Contado': 'Bonificaciones', 'Bonificaciones de Crédito': 'Bonificaciones',
      'Descuentos y Devoluciones de Contado': 'Descuentos y Devoluciones',
      'Descuentos y Devoluciones de Crédito': 'Descuentos y Devoluciones',
    };
    const gruposOrdenados = ['Ventas', 'Bonificaciones', 'Descuentos y Devoluciones'];
    const bloquesPorGrupo = new Map(gruposOrdenados.map(g => [g, []]));
    for (const bloque of bloques) {
      bloquesPorGrupo.get(GRUPO_POR_TIPO_VENTA[bloque.tipoVenta]).push(bloque);
    }

    const workbooks = [];
    for (const grupo of gruposOrdenados) {
      const bloquesGrupo = bloquesPorGrupo.get(grupo);
      if (bloquesGrupo.length === 0) continue;
      workbooks.push({
        tipoVenta: grupo,
        folio:     bloquesGrupo[0].folio,
        workbook:  _construirWorkbookPoliza(poliza, bloquesGrupo, fechaFinal, nombresClientes),
      });
    }
    return { poliza, workbooks };
  }

  const workbook = _construirWorkbookPoliza(poliza, bloques, fechaFinal, nombresClientes);
  return { poliza, workbooks: [{ tipoVenta: null, folio: bloques[0]?.folio, workbook }] };
}

/**
 * Arma el workbook de CONTPAQ (hoja `poliza` con filas P/M1/AD, más las hojas
 * informativas de Desglose Consolidado y CFDIs Sustitutos si aplican) para el
 * subconjunto de `bloques` recibido — 1 sola llamada para el caso normal
 * (todos los bloques en un archivo) o 1 llamada POR bloque para CEDIS (cada
 * bloque en su propio archivo).
 */
function _construirWorkbookPoliza(poliza, bloques, fechaFinal, nombresClientes) {
  const workbook = new ExcelJS.Workbook();
  const sheet = workbook.addWorksheet('poliza');

  // Sin anchos de columna, Excel muestra "#######" en celdas numéricas/fecha
  // que no entran en el ancho default (~8.4) — ej. columna B con "dd/mm/yyyy"
  // (10 caracteres). Solo afecta la vista en Excel, nunca el valor real de la
  // celda que lee CONTPAQ. Anchos cubren tanto la fila 'P' (encabezado, 10
  // columnas) como 'M1' (detalle, 9 columnas, desplazadas — concepto cae en
  // la columna H, no G) y 'AD' (2 columnas).
  sheet.columns = [
    { width: 6 },   // A: marcador P/M1/AD
    { width: 14 },  // B: fecha (P) / cuenta contable (M1)
    { width: 10 },  // C: tipo póliza (P) / serie (M1)
    { width: 10 },  // D: folio (P) / cargo-abono (M1)
    { width: 16 },  // E: '1' (P) / monto (M1)
    { width: 10 },  // F: '0' (P) / subcódigo (M1)
    { width: 65 },  // G: concepto del encabezado (P)
    { width: 50 },  // H: '11' (P) / concepto del movimiento (M1)
    { width: 14 },  // I: '0' (P) / centro de costo (M1)
    { width: 10 },  // J: '0' (P)
  ];

  // Detalle de qué CFDIs componen cada línea consolidada (Depósitos/Anticipos) —
  // esas líneas de la póliza no llevan serie/folio propio por ser un total
  // agregado; este arreglo alimenta la hoja "Desglose Consolidado" para poder
  // rastrear después qué facturas conforman cada monto.
  const desgloseConsolidado = [];

  for (const bloque of bloques) {
    // La columna Fecha del encabezado es una celda de fecha genuina (ctype
    // XL_CELL_DATE), no un número plano ni texto — CONTPAQ lee el valor real
    // de la celda, no el formato de despliegue, así que cambiar `numFmt` es
    // seguro para la importación. Formato "dd/mm/yyyy" (ej. "09/07/2026", con
    // cero a la izquierda en día/mes) confirmado con el usuario 2026-07-24;
    // antes usaba "m/d/yy" (formato de EE.UU., sin ceros a la izquierda).
    // "- DEV" solo aplica al encabezado cuando el bloque completo es de
    // Descuentos/Devoluciones (CEDIS) — el resto de bloques (Ventas, Crédito,
    // Pagos, Bonificaciones) no debe llevarlo (confirmado con el usuario
    // 2026-07-22; antes se pegaba a todos los bloques por error).
    const esBloqueDevoluciones = /devoluci[oó]n/i.test(bloque.tipoVenta || '');
    const conceptoHeader = esBloqueDevoluciones ? `${bloque.concepto} - DEV` : bloque.concepto;
    const headerRow = sheet.addRow([
      'P',
      fechaFinal,
      tipoPolContpaq(poliza.tipo),
      bloque.folio,
      '1',
      '0',
      conceptoHeader,
      '11',
      '0',
      '0',
    ]);
    headerRow.getCell(2).numFmt = 'dd/mm/yyyy';
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

    // Efectivo y Tarjeta primero (mismo orden que en la póliza), agrupados
    // TODOS juntos sin importar cuenta/sucursal — el usuario pidió que no
    // queden mezclados, con un renglón de encabezado divisorio cada vez que
    // cambia la forma de pago. Transferencia/Cheque agrupados (cuando
    // comparten referencia bancaria real) y cualquier forma de pago sin
    // mapear caen después, en el orden en que ya llegan.
    const ORDEN_FORMA_PAGO_DESGLOSE = { EFECTIVO: 0, TARJETA: 1 };
    const FILL_HEADER_FORMA_PAGO = { EFECTIVO: 'FFD9E8FB', TARJETA: 'FFFCE4D6' };
    desgloseConsolidado.sort((a, b) =>
      (ORDEN_FORMA_PAGO_DESGLOSE[a.formaPago] ?? 2) - (ORDEN_FORMA_PAGO_DESGLOSE[b.formaPago] ?? 2)
      || (a.cuenta - b.cuenta) || a.centroCosto.localeCompare(b.centroCosto),
    );

    let formaPagoAnterior = null;
    for (const d of desgloseConsolidado) {
      if (d.formaPago !== formaPagoAnterior) {
        const rowHeader = wsDesglose.addRow([d.formaPago || 'OTRA FORMA DE PAGO']);
        wsDesglose.mergeCells(`A${rowHeader.number}:H${rowHeader.number}`);
        rowHeader.font = { bold: true };
        rowHeader.eachCell({ includeEmpty: true }, cell => {
          cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: FILL_HEADER_FORMA_PAGO[d.formaPago] ?? 'FFE2E2E2' } };
        });
        formaPagoAnterior = d.formaPago;
      }
      const row = wsDesglose.addRow(d);
      row.getCell('monto').numFmt = '#,##0.00';
    }
    wsDesglose.autoFilter = { from: 'A1', to: 'G1' };
  }

  // Hoja de CFDIs sustitutos (tipoRelacion='04') excluidos automáticamente al
  // generar esta póliza por riesgo de doble conteo — ver
  // _particionarSustitutosPorRiesgo en cfdi-poliza-generator.service.js. No se
  // contabilizaron; quedan aquí para que el contador decida caso por caso.
  if (poliza.sustitutosExcluidos?.length > 0) {
    const wsSustitutos = workbook.addWorksheet('CFDIs Sustitutos');
    wsSustitutos.columns = [
      { header: 'UUID Sustituto', key: 'uuid',        width: 38 },
      { header: 'Serie-Folio',    key: 'serieFolio',   width: 16 },
      { header: 'Fecha',          key: 'fecha',        width: 14 },
      { header: 'Tipo',           key: 'tipo',         width: 8 },
      { header: 'Total',          key: 'total',        width: 14 },
      { header: 'Sustituye a (UUID)', key: 'sustituyeA', width: 60 },
      { header: 'Periodo original',   key: 'periodoOriginal', width: 18 },
      { header: 'Motivo exclusión',   key: 'motivo',    width: 26 },
    ];
    wsSustitutos.getRow(1).font = { bold: true };
    wsSustitutos.getRow(1).eachCell(cell => {
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFD9D9D9' } };
    });
    const motivoLabel = {
      ya_contabilizado_en_numo: 'Sustituto — original ya contabilizado en Numo',
      periodo_anterior:         'Sustituto — original de periodo anterior',
      sin_riesgo_detectado:     'Sustituto — sin riesgo detectado, revisar manualmente',
    };
    for (const s of poliza.sustitutosExcluidos) {
      const row = wsSustitutos.addRow({
        uuid:            s.uuid,
        serieFolio:      [s.serie, s.folio].filter(Boolean).join('-') || null,
        fecha:           s.fecha ? new Date(s.fecha) : null,
        tipo:            s.tipoDeComprobante,
        total:           s.total,
        sustituyeA:      (s.sustituyeA || []).join(', '),
        periodoOriginal: (s.originales || [])
          .map(o => (o.periodo != null ? `${o.periodo}/${o.ejercicio}` : '—'))
          .join(', '),
        motivo: motivoLabel[s.motivo] ?? s.motivo,
      });
      if (row.getCell('fecha').value) row.getCell('fecha').numFmt = 'm/d/yy';
      row.getCell('total').numFmt = '#,##0.00';
    }
    wsSustitutos.autoFilter = { from: 'A1', to: 'H1' };
  }

  return workbook;
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
  list, getById, create, update, cancel, cancelarTodas, listBorradorCandidatas, contabilizar, revertir, generarXmlSat,
  reporteDescuadradas, generarCierreIVA, exportContpaqXlsx, asociarFolioContpaq,
  _consolidarCargos: consolidarCargos, _moverAjustesAlFinal: moverAjustesAlFinal,
  _categorizarAjusteContado: categorizarAjusteContado, _categoriaDeGrupoCredito: categoriaDeGrupoCredito,
};
