'use strict';

const ExcelJS = require('exceljs');
const repo = require('./repositories/poliza.repository');
const { NotFoundError, BadRequestError: ValidationError, ForbiddenError } = require('../../shared/errors/AppError');
const { AccountPlan, CfdiMappingRule, PolizaMovimiento, Poliza } = require('../../../shared/models/postgres');
const { Op } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');
const BankMovement = require('../banks/BankMovement.model');
const CFDI = require('../../../visor/models/CFDI');
const { esConceptoMarcadorAjuste } = require('../cfdi-mapping/cfdi-mapping.service');

// Categorías de bank_movements que representan una transferencia electrónica real.
const CATEGORIAS_TRANSFERENCIA_BANCO = ['SPEI', 'TRASPASO'];

// `BankMovement.banco` (enum de conciliación) → código de cuenta bancaria
// real del catálogo — confirmado con el usuario 2026-08-04. BBVA usa la
// cuenta general (0109031014) para depósitos/transferencias de clientes; las
// otras 3 variantes BBVA (Nómina, Tarjeta Versátil, Tarjeta Periférica) son
// para otros flujos, no para cobros de venta. Bancos sin cuenta dedicada en
// el catálogo (HSBC, Inbursa, BanBajío, Afirme, Intercam, Nu, Spin, Hey
// Banco, Albo) quedan fuera del mapa a propósito — sin entrada, se usa la
// cuenta genérica ("Bancos por identificar") que ya traía la línea.
const BANCO_A_CODIGO_CUENTA = {
  'Banamex':    '1102012001',
  'BBVA':       '1102011001',
  'Santander':  '1102013001',
  'Banorte':    '1102014001',
  'Scotiabank': '1102015001',
  'Azteca':     '1102016001',
};

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
 * Fallback por serie+folioExterno (2026-08-07): `erpLinks.folioFiscal` solo
 * cubre ~59% de los movimientos (ver arriba) — el resto tiene
 * `folioFiscal: null` aunque SÍ traiga `erpLinks.serie`/`erpLinks.folioExterno`
 * correctos (caso real confirmado: transferencia BBVA folio Numo "034135",
 * $7,193.06, ligada a B0-260702455 vía serie+folioExterno pero con
 * folioFiscal null — nunca aparecía en la póliza). Mismo criterio que ya usa
 * `bancoPorVenta` en cobros-sucursal-puente.service.js para el caso cruzado.
 *
 * Segundo fallback vía ERP (2026-08-07): el `erpLinks.serie`+`folioExterno`
 * de BankMovement es el folioVENTA (referencia interna del ticket en cajas),
 * NO el folioFACTURA del CFDI — pueden ser números completamente distintos
 * (facturación diferida: el ticket se cobró un día, la factura se emitió
 * después con su propio folio). El primer fallback (arriba, comparar
 * directo contra `m.serie` del CFDI) solo cubre el caso en que ambos
 * folios coinciden. Para el resto, se consulta `/desgloses-cobro/almacen`
 * con el folioVenta (eso SÍ lo acepta el endpoint) para obtener el
 * `folioFactura` real y healthcheck contra nuestros CFDIs conocidos — casos
 * reales confirmados: transferencias BBVA folio Numo "034135" ($7,193.06,
 * folioVenta 260702455 → folioFactura 260701106) y "034315" ($5,462.21,
 * folioVenta 260702612 → folioFactura 260701171).
 *
 * @param {{cfdiUuid: string, serie: string}[]} movimientos
 * @param {string} rfc
 * @returns {Promise<Map<string, {esTransferencia: boolean, referencia: string|null, cuentaBanco: {id:number,codigo:string,nombre:string}|null}>>}
 *   uuid (mayúsculas) → info bancaria. `cuentaBanco`: cuenta real del banco
 *   donde cayó el depósito (ver `BANCO_A_CODIGO_CUENTA`) — null cuando el
 *   banco no tiene cuenta dedicada en el catálogo, o no se pudo determinar.
 */
async function construirVerdadBancaria(movimientos, rfc) {
  const mapa = new Map();
  const uuidsUnicos = [...new Set(movimientos.map(m => m.cfdiUuid).filter(Boolean).map(u => u.toUpperCase()))];
  if (uuidsUnicos.length === 0) return mapa;

  const uuidsSet = new Set(uuidsUnicos);

  // serie-folio propio del CFDI (ej. "B0-260702455") → uuid — para el
  // fallback por erpLinks.serie+folioExterno cuando folioFiscal viene null.
  const uuidPorSerieFolio = new Map();
  const paresSerieFolio = [];
  for (const m of movimientos) {
    if (!m.cfdiUuid || !m.serie) continue;
    const match = /^(.+)-(\d+)$/.exec(m.serie);
    if (!match) continue;
    const [, serie, folio] = match;
    const key = `${serie}|${folio}`;
    if (!uuidPorSerieFolio.has(key)) {
      uuidPorSerieFolio.set(key, m.cfdiUuid.toUpperCase());
      paresSerieFolio.push({ serie, folio });
    }
  }

  // Batching de las condiciones serie+folioExterno (una póliza grande puede
  // tener cientos) — mismo LOTE que `bancoPorVenta`, para no armar un $or
  // gigantesco en un solo query.
  const LOTE = 150;
  const movs = [];
  const condicionFolioFiscal = { 'erpLinks.folioFiscal': { $in: uuidsUnicos.map(u => new RegExp(`^${u}$`, 'i')) } };
  movs.push(...await BankMovement.find(
    condicionFolioFiscal,
    { erpLinks: 1, categoria: 1, folio: 1, banco: 1, numeroAutorizacion: 1 },
  ).lean());
  for (let i = 0; i < paresSerieFolio.length; i += LOTE) {
    const lote = paresSerieFolio.slice(i, i + LOTE);
    movs.push(...await BankMovement.find(
      { $or: lote.map(p => ({ 'erpLinks.serie': p.serie, 'erpLinks.folioExterno': p.folio })) },
      { erpLinks: 1, categoria: 1, folio: 1, banco: 1, numeroAutorizacion: 1 },
    ).lean());
  }

  // Cuentas reales de banco (ver BANCO_A_CODIGO_CUENTA) — un solo query para
  // las 6, reutilizado por todos los movimientos de esta llamada.
  const codigosBanco = Object.values(BANCO_A_CODIGO_CUENTA);
  const cuentasBancoRows = await AccountPlan.findAll({
    where:      { codigo: { [Op.in]: codigosBanco } },
    attributes: ['id', 'codigo', 'nombre'],
    raw:        true,
  });
  const cuentaPorCodigo = new Map(cuentasBancoRows.map(r => [r.codigo, { id: r.id, codigo: r.codigo, nombre: r.nombre }]));

  for (const m of movs) {
    const cat = (m.categoria || '').toUpperCase();
    const esTransferencia = CATEGORIAS_TRANSFERENCIA_BANCO.some(c => cat.includes(c));
    // Folio propio de Numo (ej. "034186") — NO `numeroAutorizacion`/
    // `referenciaNumerica` (esos son del banco, no coinciden con la
    // referencia esperada en columna C, mismo criterio ya aplicado en
    // `bancoPorVenta` de cobros-sucursal-puente.service.js — confirmado con
    // el usuario 2026-08-04 que esta función, al resolver por `folioFiscal`,
    // seguía devolviendo el número de autorización del banco y pisaba el
    // folio correcto que ya había resuelto la puente para cobros cruzados).
    const referencia = m.folio || null;
    // La mayoría de los movimientos ligados NO traen `categoria` (viene null)
    // — confirmado contra datos reales: ~18,000 de ~18,650 movimientos con
    // erpLinks no tienen categoria. Sin esto, `esTransferencia` (abajo)
    // siempre da `false` para ellos, y el caller (consolidarCargos) lo
    // tomaba como "confirmado que NO es transferencia", perdiendo el
    // subcódigo 21 en transferencias reales solo por falta de categoría.
    const categoriaConocida = m.categoria != null;
    // Cuenta real del banco al que llegó el depósito — null si el banco no
    // tiene cuenta dedicada en el catálogo (queda en la genérica que ya
    // traía la línea, ver armarBloqueContado) — confirmado con el usuario
    // 2026-08-04.
    const codigoCuentaBanco = BANCO_A_CODIGO_CUENTA[m.banco];
    const cuentaBanco = codigoCuentaBanco ? (cuentaPorCodigo.get(codigoCuentaBanco) ?? null) : null;
    // Número de autorización REAL de la tarjeta (del banco, ej. terminal
    // punto de venta) — a diferencia de `referencia` (folio propio de Numo,
    // usado para Transferencia/Cheque porque ahí representa el depósito
    // bancario), este es un concepto distinto: identifica el lote/swipe de
    // la TARJETA, para agrupar ventas que comparten la misma autorización
    // (confirmado con el usuario 2026-08-07).
    const numeroAutorizacion = m.numeroAutorizacion || null;

    for (const link of (m.erpLinks ?? [])) {
      const folioFiscalUpper = (link.folioFiscal || '').toUpperCase();
      // Resuelve el uuid por folioFiscal si es válido; si no (null o no es
      // uno de los que buscamos), cae al fallback por serie+folioExterno —
      // ver docstring de la función.
      const uuidResuelto = uuidsSet.has(folioFiscalUpper)
        ? folioFiscalUpper
        : (link.serie && link.folioExterno ? uuidPorSerieFolio.get(`${link.serie}|${link.folioExterno}`) : null);
      if (!uuidResuelto) continue;
      // Un mismo CFDI puede tener varios movimientos ligados (varias
      // parcialidades) — si alguno confirma transferencia, esa gana.
      const actual = mapa.get(uuidResuelto);
      if (!actual || (!actual.esTransferencia && esTransferencia)) {
        mapa.set(uuidResuelto, { esTransferencia, referencia, categoriaConocida, cuentaBanco, numeroAutorizacion });
      }
    }
  }
  return mapa;
}

// Categorías de bank_movements que representan una transferencia electrónica
// real — mismo criterio que `construirVerdadBancaria` (CATEGORIAS_TRANSFERENCIA_BANCO).
const _CATEGORIAS_TRANSFERENCIA = ['SPEI', 'TRASPASO'];

/**
 * Resuelve el depósito bancario REAL cruzando bank_movements por TICKET
 * individual (`erpLinks.serie`+`erpLinks.folioExterno`, el mismo criterio ya
 * usado por `bancoPorVenta` en cobros-sucursal-puente.service.js para el caso
 * cruzado) — a propósito NO usa `cfdiUuid`/`folioFiscal` como
 * `construirVerdadBancaria`: ese match es por CFDI COMPLETO, y en una Factura
 * Global (un solo CFDI que agrupa decenas de tickets) mezclaría el depósito
 * de un ticket cualquiera con el resto de la factura — confirmado con datos
 * reales dos veces (2026-08-18): Tarjeta (bug original, Hidalgo
 * B0-260701074, 2026-08-14) Y Transferencia (Factura Global O0-260800164,
 * 41 documentos relacionados, 6 `BankMovement` distintos cada uno ligado a
 * un ticket específico — un ticket pagado por Banamex quedaba con la cuenta
 * de BBVA de OTRO ticket de la misma factura). Usado tanto para Tarjeta
 * (agrupar por `numeroAutorizacion`) como para Transferencia/Cheque
 * (resolver el banco/referencia real en vez del de `construirVerdadBancaria`
 * cuando hay dato por ticket disponible).
 *
 * @param {{serieVentaTicket: string, folioVentaTicket: string}[]} movimientos
 * @returns {Promise<Map<string, {esTransferencia: boolean, referencia: string|null, numeroAutorizacion: string|null, banco: string|null, cuentaBanco: object|null}>>}
 *   `serieVentaTicket|folioVentaTicket` → info bancaria del ticket.
 *   `referencia`: folio propio de Numo (para Transferencia/Cheque).
 *   `numeroAutorizacion`: número de autorización real de terminal (para Tarjeta).
 */
async function construirBancoRealPorTicket(movimientos) {
  const mapa = new Map();
  const pares = [...new Map(
    movimientos
      .filter(m => m.serieVentaTicket && m.folioVentaTicket)
      .map(m => [`${m.serieVentaTicket}|${m.folioVentaTicket}`, { serie: m.serieVentaTicket, folio: m.folioVentaTicket }]),
  ).values()];
  if (!pares.length) return mapa;

  const codigosBanco = Object.values(BANCO_A_CODIGO_CUENTA);
  const cuentasBancoRows = await AccountPlan.findAll({
    where:      { codigo: { [Op.in]: codigosBanco } },
    attributes: ['id', 'codigo', 'nombre'],
    raw:        true,
  });
  const cuentaPorCodigo = new Map(cuentasBancoRows.map(r => [r.codigo, { id: r.id, codigo: r.codigo, nombre: r.nombre }]));
  const paresSet = new Set(pares.map(p => `${p.serie}|${p.folio}`));

  const LOTE = 150;
  for (let i = 0; i < pares.length; i += LOTE) {
    const lote = pares.slice(i, i + LOTE);
    const movs = await BankMovement.find(
      { $or: lote.map(p => ({ 'erpLinks.serie': p.serie, 'erpLinks.folioExterno': p.folio })) },
      { erpLinks: 1, banco: 1, categoria: 1, folio: 1, numeroAutorizacion: 1, referenciaNumerica: 1 },
    ).lean();
    for (const m of movs) {
      const codigoCuentaBanco = BANCO_A_CODIGO_CUENTA[m.banco];
      const cuentaBanco = codigoCuentaBanco ? (cuentaPorCodigo.get(codigoCuentaBanco) ?? null) : null;
      const cat = (m.categoria || '').toUpperCase();
      const esTransferencia = _CATEGORIAS_TRANSFERENCIA.some(c => cat.includes(c));
      // Folio propio de Numo — referencia para Transferencia/Cheque (mismo
      // criterio que `construirVerdadBancaria`, ver comentario ahí).
      const referencia = m.folio || null;
      const numeroAutorizacion = m.numeroAutorizacion || m.referenciaNumerica || null;
      if (!referencia && !numeroAutorizacion) continue;
      for (const link of (m.erpLinks ?? [])) {
        if (!link.serie || !link.folioExterno) continue;
        const key = `${link.serie}|${link.folioExterno}`;
        if (!paresSet.has(key)) continue;
        if (!mapa.has(key)) mapa.set(key, { esTransferencia, referencia, numeroAutorizacion, banco: m.banco ?? null, cuentaBanco });
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
 *
 * `soloCobranza` separa estrictamente Ingreso de Cobranza (a diferencia del
 * filtro homónimo de `list()`, que solo incluye cuando es true y no excluye
 * nada cuando es false/undefined): true = solo pólizas con algún movimiento
 * de Pago (tipo_comprobante='P'); false = solo pólizas SIN ninguno (para que
 * el modal de "Cancelar todas" en Pólizas de Ingreso no se mezcle con las de
 * Cobranza, y viceversa); undefined = sin filtro (todas).
 */
async function listBorradorCandidatas({ rfc, ejercicio, periodo, soloCobranza }) {
  if (!rfc)       throw new ValidationError('RFC requerido');
  if (!ejercicio) throw new ValidationError('Ejercicio requerido');
  if (!periodo)   throw new ValidationError('Periodo requerido');

  const where = { rfc, ejercicio: Number(ejercicio), periodo: Number(periodo), estado: 'borrador' };
  const SUBQUERY_POLIZAS_PAGO = `(SELECT DISTINCT poliza_id FROM poliza_movimientos WHERE tipo_comprobante = 'P')`;
  if (soloCobranza === true || soloCobranza === 'true') {
    where.id = { [Op.in]: sequelize.literal(SUBQUERY_POLIZAS_PAGO) };
  } else if (soloCobranza === false || soloCobranza === 'false') {
    where.id = { [Op.notIn]: sequelize.literal(SUBQUERY_POLIZAS_PAGO) };
  }

  const polizas = await Poliza.findAll({
    where,
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

// Códigos de cuenta bancaria real (destino del reemplazo) — un movimiento que
// YA quedó en una de estas cuentas no se vuelve a tocar.
const CODIGOS_CUENTA_BANCO_REAL = new Set(Object.values(BANCO_A_CODIGO_CUENTA));

// Cuentas genéricas/placeholder (ver seed-account-plan.js) — un movimiento que
// siga en alguna de estas después del cruce automático se reporta como
// "pendiente" para que el usuario lo resuelva a mano (ver `resolverCuentasBanco`).
const CODIGOS_CUENTA_PUENTE = new Set(['1101010003', '1102010004', '1102011005']);

/**
 * Reemplaza, en los movimientos de la póliza, la cuenta genérica ("Bancos por
 * identificar") por la cuenta bancaria real — usando el mismo cruce
 * (`construirVerdadBancaria`) que ya usa el export CONTPAQ, pero persistido
 * en `poliza_movimientos.cuenta_id` en vez de calculado solo al exportar.
 * Cobertura parcial (~59%, ver docstring de `construirVerdadBancaria`): los
 * movimientos sin cruce posible (sin `cfdiUuid`, o sin `erpLinks` en su
 * `BankMovement`) se quedan con la cuenta que ya tenían — no es un error,
 * quedan disponibles para el reemplazo manual (`reemplazarCuenta`).
 */
async function _resolverCuentasBancoReal(poliza) {
  const verdadBancaria = await construirVerdadBancaria(
    poliza.movimientos.map(m => ({ cfdiUuid: m.cfdiUuid, serie: m.serie })),
    poliza.rfc,
  );
  if (verdadBancaria.size === 0) return [];

  const actualizaciones = [];
  for (const m of poliza.movimientos) {
    if (!m.cfdiUuid) continue;
    if (CODIGOS_CUENTA_BANCO_REAL.has(m.cuenta?.codigo)) continue; // ya es cuenta real
    // Solo debe tocar cuentas GENÉRICAS/puente ("Bancos por identificar" y
    // similares, ver docstring arriba) — sin este guard, cualquier movimiento
    // con el mismo cfdiUuid que SÍ tuviera un depósito ligado se reasignaba,
    // incluyendo Clientes/IVA-PPD/Ingresos de la Venta a Crédito original
    // (comparten cfdiUuid con el Pago que la liquida después). Caso real
    // confirmado 2026-08-17: Puerto Escondido, PAZCUAL HERNANDEZ CORTES
    // O0-260800130 (Reg 6 — Venta PPD) — sus 3 líneas (Cargo Clientes, Abono
    // IVA-PPD, Abono Ingresos) terminaron las 3 en la cuenta de BBVA Bancomer
    // solo porque esa factura eventualmente se cobró por transferencia.
    if (!CODIGOS_CUENTA_PUENTE.has(m.cuenta?.codigo)) continue;
    // Refuerzo explícito (2026-08-17, a petición del usuario): las líneas de
    // Crédito (PPD) NUNCA deben pasar por este cruce — la Venta a Crédito se
    // liquida con un Pago (P) aparte más adelante, que es el que de verdad
    // tiene el depósito bancario; la Venta original no debe tocarse aunque
    // comparta cfdiUuid con ese Pago.
    if (m.metodoPago === 'PPD') continue;
    // Efectivo (01) y Tarjeta (04/28) NUNCA se reasignan al banco real —
    // una Factura Global agrupa ~150 tickets con formas de pago mixtas bajo
    // un solo cfdiUuid; si la Transferencia de esa FG está ligada a Banamex,
    // sin este filtro todos los movimientos (Efectivo y Tarjeta incluidos)
    // se irían a Banamex en lugar de quedar en CAJA/BANCOS genérico.
    if (['01', '04', '28'].includes(m.formaPago)) continue;
    const info = verdadBancaria.get(m.cfdiUuid.toUpperCase());
    if (info?.cuentaBanco?.id) {
      actualizaciones.push({ movimientoId: m.id, cuentaId: info.cuentaBanco.id });
    }
  }
  return actualizaciones;
}

/**
 * Corre el cruce automático de cuentas de banco (`_resolverCuentasBancoReal`)
 * y lo persiste, sin cambiar el estado de la póliza. Pensado como paso previo
 * a `contabilizar` desde el frontend: primero resuelve lo que se puede
 * automáticamente, y devuelve agrupado lo que quedó en cuenta puente para que
 * el usuario decida manualmente (modal) antes de confirmar la contabilización.
 */
/**
 * Corre `_resolverCuentasBancoReal` y persiste — compartido por
 * `resolverCuentasBanco` (modal al contabilizar) y `exportContpaqXlsx`
 * (para que el Excel y lo guardado en la póliza siempre coincidan). No
 * revalida estado — quien llama decide si aplica (cualquier estado excepto
 * 'cancelada' es seguro: cambiar la cuenta no altera debe/haber).
 */
async function _resolverYPersistirCuentasBanco(poliza) {
  const actualizacionesCuenta = await _resolverCuentasBancoReal(poliza);
  if (actualizacionesCuenta.length === 0) return { poliza, actualizados: 0 };
  await repo.actualizarCuentasMovimientos(poliza.id, actualizacionesCuenta);
  return { poliza: await repo.findByIdLight(poliza.id), actualizados: actualizacionesCuenta.length };
}

async function resolverCuentasBanco(id) {
  const poliza = await repo.findByIdLight(id);
  if (!poliza)                      throw new NotFoundError('Póliza');
  if (poliza.estado !== 'borrador') throw new ValidationError('Solo se pueden resolver cuentas en pólizas en borrador');

  const { poliza: actualizada, actualizados } = await _resolverYPersistirCuentasBanco(poliza);

  const pendientesMap = new Map();
  for (const m of actualizada.movimientos) {
    const codigo = m.cuenta?.codigo;
    if (!codigo || !CODIGOS_CUENTA_PUENTE.has(codigo)) continue;
    const prev = pendientesMap.get(m.cuentaId) ?? {
      cuentaId: m.cuentaId, codigo, nombre: m.cuenta.nombre, cantidadLineas: 0, monto: 0,
    };
    prev.cantidadLineas += 1;
    prev.monto = parseFloat((prev.monto + Number(m.debe || 0) + Number(m.haber || 0)).toFixed(2));
    pendientesMap.set(m.cuentaId, prev);
  }

  return { actualizados, pendientes: [...pendientesMap.values()] };
}

/**
 * Resuelve, para los CFDIs cuyo movimiento bancario se acaba de identificar
 * (ver `setErpIds` en bank.service.js), cualquier línea de póliza que siga en
 * cuenta puente para ese mismo `cfdiUuid` — sin importar si la póliza ya está
 * en borrador o contabilizada (cambiar la cuenta no altera el cuadre, solo
 * corrige la clasificación). No toca pólizas canceladas.
 *
 * Llamado desde el módulo de bancos justo después de guardar `erpLinks`, para
 * que la cuenta puente se resuelva sola en el momento en que se concilia el
 * banco, sin que el usuario tenga que volver a la póliza.
 *
 * @param {string[]} uuids       — folioFiscal de los CFDIs recién vinculados
 * @param {string}   bancoNombre — `BankMovement.banco` (ver BANCO_A_CODIGO_CUENTA)
 * @returns {Promise<number>} cantidad de líneas actualizadas
 */
async function resolverCuentasPorCfdisIdentificados(uuids, bancoNombre) {
  const codigoCuentaBanco = BANCO_A_CODIGO_CUENTA[bancoNombre];
  if (!codigoCuentaBanco || !uuids?.length) return 0;

  const cuentaBanco = await AccountPlan.findOne({ where: { codigo: codigoCuentaBanco }, attributes: ['id'] });
  if (!cuentaBanco) return 0;

  const uuidsUpper = [...new Set(uuids.filter(Boolean).map(u => u.toUpperCase()))];
  if (uuidsUpper.length === 0) return 0;

  const movimientos = await PolizaMovimiento.findAll({
    where: { cfdiUuid: { [Op.in]: uuidsUpper } },
    include: [
      { model: AccountPlan, as: 'cuenta', attributes: ['codigo'] },
      { model: Poliza, as: 'poliza', attributes: ['id', 'estado'] },
    ],
  });

  const porPoliza = new Map();
  for (const m of movimientos) {
    if (m.poliza?.estado === 'cancelada') continue;
    if (!CODIGOS_CUENTA_PUENTE.has(m.cuenta?.codigo)) continue;
    // Efectivo (01) y Tarjeta (04/28) no se remapean al banco real — igual
    // que se quitó el override de verdadBancaria en el export (2026-08-14).
    // Una FG agrupa ~150 tickets con formas de pago mixtas bajo un solo UUID;
    // remapear por UUID completo pierde esa distinción y manda Efectivo a
    // Bancos y Tarjeta al banco específico cuando deben quedar en CAJA/BANCOS
    // genérico. Solo Transferencias (y formas de pago sin código específico)
    // se remapean al banco real confirmado (confirmado con el usuario 2026-08-15).
    if (['01', '04', '28'].includes(m.formaPago)) continue;
    if (!porPoliza.has(m.polizaId)) porPoliza.set(m.polizaId, []);
    porPoliza.get(m.polizaId).push({ movimientoId: m.id, cuentaId: cuentaBanco.id });
  }

  let total = 0;
  for (const [polizaId, actualizaciones] of porPoliza) {
    await repo.actualizarCuentasMovimientos(polizaId, actualizaciones);
    total += actualizaciones.length;
  }
  return total;
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

  // Antes de contabilizar: reemplazar cuenta genérica de banco por la cuenta
  // real donde se identificó el depósito (ver `_resolverCuentasBancoReal`).
  // No afecta el cuadre (mismo movimiento, solo cambia la cuenta) — se hace
  // antes de `validateBalance` conceptualmente, pero como no altera debe/haber
  // no hace falta revalidar.
  const actualizacionesCuenta = await _resolverCuentasBancoReal(poliza);
  if (actualizacionesCuenta.length > 0) {
    await repo.actualizarCuentasMovimientos(id, actualizacionesCuenta);
  }

  const updated = await repo.setEstado(id, 'contabilizada', {
    contabilizadoPor: userLabel(user),
    contabilizadaAt:  new Date(),
  });
  return updated;
}

/**
 * Reemplazo manual: cambia todas las líneas de la póliza que usan
 * `cuentaPuenteId` por `cuentaDestinoId`. Pensado para resolver a mano el
 * resto de los casos que `_resolverCuentasBancoReal` no pudo cruzar
 * automáticamente al contabilizar.
 */
async function reemplazarCuenta(id, { cuentaPuenteId, cuentaDestinoId }, user) {
  const poliza = await repo.findByIdLight(id);
  if (!poliza) throw new NotFoundError('Póliza');
  if (poliza.estado === 'cancelada') throw new ValidationError('No se puede modificar una póliza cancelada');
  if (!cuentaPuenteId || !cuentaDestinoId) {
    throw new ValidationError('cuentaPuenteId y cuentaDestinoId son requeridos');
  }
  if (Number(cuentaPuenteId) === Number(cuentaDestinoId)) {
    throw new ValidationError('La cuenta destino debe ser distinta de la cuenta puente');
  }

  const destino = await AccountPlan.findByPk(cuentaDestinoId);
  if (!destino) throw new ValidationError('Cuenta destino no encontrada en el catálogo');

  const afectados = await repo.reemplazarCuentaEnPoliza(id, cuentaPuenteId, cuentaDestinoId);
  return { afectados, poliza: await repo.findByIdLight(id) };
}

async function revertir(id, user, motivo, revertirCuentas = true) {
  const poliza = await repo.findByIdLight(id);
  if (!poliza)                           throw new NotFoundError('Póliza');
  if (poliza.estado !== 'contabilizada') throw new ValidationError('Solo se pueden revertir pólizas contabilizadas');

  // Deshace el cruce banco-real (automático o manual) que se hizo al
  // contabilizar — la póliza vuelve a quedar exactamente como estaba antes.
  // Opcional (default true): el usuario puede optar por conservar el cruce
  // ya resuelto si solo revierte para corregir algo distinto.
  if (revertirCuentas) {
    await repo.restaurarCuentasAnteriores(id);
  }

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
// 'SF'/'PTS': sentinels cortos (no un claveSat SAT real, esos son siempre
// numéricos de 2 dígitos) que cfdi-mapping.service.js pone en `formaPago`
// cuando el split del Cargo por forma de pago real (2026-08-06) detecta que
// una porción del cobro es Saldo a Favor o Puntos/Club Tuberos — sin esto,
// esa línea (que ya va a su cuenta dedicada, 2103090001/2103090002, nunca a
// Caja/Bancos) se etiquetaría con el formaPago ORIGINAL del CFDI completo,
// mostrando p.ej. "Depósitos consolidados (Efectivo)" en una cuenta que en
// realidad es de Saldo a Favor.
const LABEL_FORMA_PAGO_CONSOLIDADO = { '01': 'EFECTIVO', '04': 'TARJETA', '28': 'TARJETA', 'SF': 'SF', 'PTS': 'PUNTOS' };

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
  // OPA (ver `REGLAS_MEZCLADAS_CON_VENTAS`): mismo tratamiento de categoría
  // que `esReglaAnticipo` (se mezcla con Ventas normales por folio), pero con
  // `reglaNombre` propio en vez del nombre de la regla, para poder distinguir
  // el orden cargo/abono (ver `bloquesAjustesContado`/`moverAjustesAlFinal`)
  // sin afectar el mecanismo estándar de anticipo (Reg 22C/23 con NC).
  if (REGLAS_MEZCLADAS_CON_VENTAS.has(m.reglaNombre)) return 'anticipo';
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
/**
 * Variante de `consolidarCargos` para la póliza de Cobranza (Pagos) SIN
 * agrupar/mezclar líneas — anota cada Cargo (debe>0) con su cuenta bancaria
 * real y subcódigo (transferencia o no), pero conserva exactamente una línea
 * de salida por cada línea de entrada, en el mismo orden. A diferencia de
 * Contado/Crédito, aquí NO se quiere consolidar varias facturas del mismo
 * Pago en una sola línea: `cfdiToMovimientos` (cfdi-mapping.service.js,
 * `esSplitPagoPorFactura`) ya arma Cargo+IVA+Abono agrupados por factura
 * liquidada, y `consolidarCargos` los volvía a fusionar en una sola línea
 * porque agrupa por `cfdiUuid` cuando no hay referencia bancaria — todas las
 * líneas de un mismo Pago comparten el mismo `cfdiUuid` (confirmado con el
 * usuario 2026-08-11). Los Abono (haber>0) tampoco se separan aparte — se
 * dejan intercalados como ya vienen.
 */
function anotarCargosPorFacturaSinAgrupar(movs, subcodigoTransferencia, verdadBancaria, nombresClientes = null) {
  return movs.map(m => {
    if (!(Number(m.debe) > 0)) return m;
    // Las líneas de Saldo a Favor (Anticipos Otros) de una factura pagada con
    // SF no representan un depósito bancario — no se les debe cambiar la
    // cuenta a un banco real ni asignarles el subcódigo de transferencia
    // (confirmado con el usuario 2026-08-11). Se dejan pasar tal cual, solo
    // con subcódigo 0.
    if (m.tipoOrigen === TIPO_ORIGEN_CARGO_ESPECIAL) {
      return {
        cuenta: m.cuenta, cuentaId: m.cuentaId, serie: m.serie, concepto: m.concepto,
        centroCosto: m.centroCosto, centroCostoObj: m.centroCostoObj,
        debe: Number(m.debe), haber: Number(m.haber), cfdiUuid: m.cfdiUuid,
        rfcTercero: m.rfcTercero, formaPago: m.formaPago, reglaNombre: m.reglaNombre,
        tipoOrigen: m.tipoOrigen, _subcodigo: 0,
      };
    }
    const bancario = verdadBancaria?.get((m.cfdiUuid || '').toUpperCase());
    const esTransferenciaVerificada = bancario?.categoriaConocida
      ? bancario.esTransferencia
      : (m.formaPago === FORMA_PAGO_TRANSFERENCIA);
    // El concepto por-factura que arma `cfdiToMovimientos` ("cliente /
    // serie-folio") ya viene completo — se deja tal cual. Si no lo tiene
    // (caso viejo/`tasaIva==='mixto'` sin split), se enriquece aquí con el
    // nombre del cliente, mismo criterio que antes hacía `consolidarCargos`.
    const nombre = nombresClientes?.get((m.cfdiUuid || '').toUpperCase()) || '';
    const yaEnriquecido = !nombre || m.concepto?.includes(nombre);
    // NUNCA copiar `m` con spread (`{...m}`) — `m` es una instancia de
    // Sequelize y el spread no copia bien `debe`/`haber` (salían NaN en el
    // Excel, confirmado con datos reales 2026-08-11). Por eso, igual que
    // `armarIndividual` en `consolidarCargos`, se listan los campos a mano.
    return {
      cuenta:         bancario?.cuentaBanco ?? m.cuenta,
      cuentaId:       m.cuentaId,
      serie:          m.serie,
      concepto:       yaEnriquecido ? m.concepto : [nombre, m.serie || ''].filter(Boolean).join(' / '),
      centroCosto:    m.centroCosto,
      centroCostoObj: m.centroCostoObj,
      debe:           Number(m.debe),
      haber:          Number(m.haber),
      cfdiUuid:       m.cfdiUuid,
      rfcTercero:     m.rfcTercero,
      formaPago:      m.formaPago,
      reglaNombre:    m.reglaNombre,
      tipoOrigen:     m.tipoOrigen,
      _subcodigo:     esTransferenciaVerificada ? subcodigoTransferencia : 0,
    };
  });
}

// Mismo literal que usa `_inyectarSaldoFavorGenerado` (cfdi-poliza-generator.
// service.js) para el caso "mismo folio" (confirmado con el usuario
// 2026-08-13): una Devolución cuyo saldo a favor generado se consumió por
// completo contra la MISMA venta que lo generó — no es un pasivo real, es
// caja que salió y volvió a entrar. Esa línea llega como un Cargo NEGATIVO a
// Caja/Bancos; `consolidarCargos` necesita dejarla pasar su filtro inicial
// (que de otro modo descarta cualquier `debe` que no sea > 0) para que reste
// del total en vez de perderse — sin crear una fila propia en el export
// (confirmado: "solo restarlo", sin desglose).
const TIPO_ORIGEN_AJUSTE_CONSOLIDADO_SF = 'Ajuste Consolidado SF';

// Movimientos de cobro real sin CFDI "normal" detrás — inyectados directo al
// consolidado de Efectivo/Tarjeta por cfdi-poliza-generator.service.js (ver
// `_cfdisCanceladasSinCompensar`/`_cobrosSinFacturaPorCentro`) — marcados por
// `reglaNombre` para poder anotarlos en la hoja "Desglose Consolidado".
const NOTA_AJUSTE_SIN_CFDI = {
  'FACTURA-CANCELADA-COBRO-REAL': 'CANCELADA (cobro real, sin efecto fiscal)',
  'COBRO-SIN-FACTURA':            'SIN FACTURA (cobro real, sin CFDI asociado)',
};

function consolidarCargos(movs, subcodigoTransferencia, detectarAnticipo = false, verdadBancaria = null, nombresClientes = null, bancoRealPorTicket = null) {
  const grupos = new Map();
  const gruposDetallados = new Map(); // Transferencia y Cheque: agrupan SOLO por mismo número de autorización real
  const porCategoria = { devolucion: [], descuento: [], bonificacion: [], clubTuberos: [] };
  const anticipos = [];              // Recepción Y Aplicación
  const depositosIdentificados = []; // forma de pago sin mapear + depósito real ligado en Bancos — va al final

  for (const m of movs) {
    const esAjusteConsolidadoSF = m.tipoOrigen === TIPO_ORIGEN_AJUSTE_CONSOLIDADO_SF;
    if (!(Number(m.debe) > 0) && !esAjusteConsolidadoSF) continue;
    // Refacturación (factura que reemplaza una venta cancelada, ver
    // `esRefacturacion` en cfdi-poliza-generator.service.js): su cargo
    // (dinero en banco/caja) ya quedó contabilizado en el asiento de la
    // CANCELACION original — mostrarlo aquí (individual o consolidado)
    // duplicaría el depósito. Se omite por completo; su abono (Ingreso+IVA)
    // sigue su camino normal (bloquesAbonosNormales), sin tocar.
    if (/refacturaci[oó]n/i.test(m.tipoOrigen || '')) continue;
    // Ajustes de cobro real sin CFDI "normal" detrás (factura cancelada sin
    // NC/sustituto, o cobro sin ninguna factura — ver `_cfdisCanceladasSinCompensar`/
    // `_cobrosSinFacturaPorCentro` en cfdi-poliza-generator.service.js): se
    // funden en el mismo consolidado de Efectivo/Tarjeta, pero deben quedar
    // marcados en la hoja "Desglose Consolidado" (confirmado con el usuario
    // 2026-08-20, casos reales B0-260801159 y B0 11-ago $759.59 sin factura).
    const notaAjusteSinCfdi = NOTA_AJUSTE_SIN_CFDI[m.reglaNombre] ?? null;
    const centroCosto = m.centroCostoObj?.clave ?? m.centroCosto ?? '';
    // Ticket real (serieVentaTicket/folioVentaTicket) en vez de `m.serie`
    // (la Factura, que en una Global es la misma para decenas de tickets) —
    // usado para el detalle de la hoja "Desglose Consolidado" más abajo.
    const serieParaDetalle = (m.serieVentaTicket && m.folioVentaTicket)
      ? `${m.serieVentaTicket}-${m.folioVentaTicket}`
      : (m.serie || '');
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

    const armarIndividual = (etiqueta, subcodigo, categoria, serieOverride, cuentaOverride) => {
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
        // Cuenta real del banco donde cayó el depósito (ver
        // `BANCO_A_CODIGO_CUENTA`) cuando aplica — si no, la de la línea.
        cuenta: cuentaOverride ?? m.cuenta, serie: serieOverride ?? (m.serie || ''), concepto, centroCosto,
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
    // Remanentes menores a $10 (ej. sobrante de $0.01-$9.99 cuando SF/Puntos
    // cubre casi toda la factura y no se encontró desglose real completo,
    // ver `esCasoAjusteSFPuntos` en cfdi-mapping.service.js): no vale la pena
    // mostrarlos como su propia línea de Transferencia/Cheque/Tarjeta — caen
    // al bucket genérico de abajo en vez de individualizarse (confirmado con
    // el usuario 2026-08-20).
    const esRemanenteMenor = Number(m.debe) < 10;
    if ((esTransferenciaVerificada || esChequeDeclarado) && !esRemanenteMenor) {
      const tipoDetalle = esTransferenciaVerificada ? 'TRANSFERENCIA' : 'CHEQUE';
      const subcodigoDetalle = esTransferenciaVerificada ? subcodigoTransferencia : 0;
      // Info por TICKET (`bancoRealPorTicket`, ver docstring) tiene prioridad
      // sobre `bancario` (`construirVerdadBancaria`, por CFDI completo): en
      // una Factura Global, `bancario` puede traer el depósito de OTRO
      // ticket de la misma factura — confirmado con datos reales 2026-08-18
      // (Global O0-260800164, ticket 260800269 pagado por Banamex quedaba
      // con la cuenta BBVA de otro ticket de la misma factura). Sin dato por
      // ticket (venta normal, no partida), `bancario` sigue siendo correcto
      // (un solo ticket por CFDI, ambos coinciden).
      const infoTicketTransfCheque = (m.serieVentaTicket && m.folioVentaTicket)
        ? bancoRealPorTicket?.get(`${m.serieVentaTicket}|${m.folioVentaTicket}`)
        : null;
      const referencia = infoTicketTransfCheque?.referencia ?? bancario?.referencia ?? null;
      // Cuenta real del banco donde cayó el depósito (ver
      // `BANCO_A_CODIGO_CUENTA`/`construirVerdadBancaria`) en vez de la
      // genérica "Bancos por identificar" que ya traía la línea — solo
      // cuando el banco tiene cuenta dedicada en el catálogo.
      const cuentaLinea = infoTicketTransfCheque?.cuentaBanco ?? bancario?.cuentaBanco ?? m.cuenta;
      const key = `${cuentaLinea?.codigo}|${centroCosto}|${tipoDetalle}|${referencia ?? `__cfdi_${m.cfdiUuid}`}`;
      if (!gruposDetallados.has(key)) {
        gruposDetallados.set(key, {
          cuenta: cuentaLinea, centroCosto, referencia, tipoDetalle, subcodigo: subcodigoDetalle,
          debe: 0, detalle: [], primerMov: m,
        });
      }
      const gt = gruposDetallados.get(key);
      gt.debe += Number(m.debe);
      gt.detalle.push({ cfdiUuid: m.cfdiUuid, serie: serieParaDetalle, monto: Number(m.debe), formaPago: tipoDetalle, nota: notaAjusteSinCfdi });
      continue;
    }

    // Tarjeta/Efectivo NUNCA se sacan del consolidado por un match de
    // `verdadBancaria` (quitado 2026-08-14, confirmado con el usuario —
    // caso real Hidalgo B0-260701074): `construirVerdadBancaria` resuelve el
    // banco real POR CFDI completo (una sola entrada por `cfdiUuid`), no por
    // línea individual. Una Factura Global agrupa ~150 tickets, cada uno con
    // su propio posible depósito — un solo match parcial (ej. un lote de
    // tarjeta de $1,720.93 de un ticket cualquiera) hacía que las 175 líneas
    // completas de la factura ($201,995.71) se sacaran del consolidado hacia
    // esa cuenta bancaria, ocultando casi todo el Efectivo/Tarjeta real del
    // día. `verdadBancaria` sigue siendo confiable para Transferencia (única
    // forma de pago que ya se maneja arriba antes de llegar aquí, vía
    // `esTransferenciaVerificada`) — para Tarjeta/Efectivo/cualquier otra,
    // siempre se consolida por forma de pago declarada, sin importar si hay
    // match bancario.

    // Tarjeta con número de autorización REAL por TICKET (`bancoRealPorTicket`
    // — ver `construirBancoRealPorTicket`, cruza bank_movements por
    // `erpLinks.serie`+`folioExterno` del ticket real de cajas, NUNCA por
    // `cfdiUuid`/CFDI completo como `verdadBancaria` — no repite el problema
    // de Facturas Globales de arriba, porque el match es por ticket
    // individual, nunca por la factura completa que los agrupa): mismo
    // criterio que Transferencia/Cheque, se agrupan SOLO las que comparten el
    // mismo número de autorización (mismo depósito/lote real de terminal);
    // sin ese dato, cae al bucket genérico de abajo, sin cambios (confirmado
    // con el usuario 2026-08-18, caso real Puerto Escondido — $41,572.15 de
    // Tarjeta consolidados a ciegas cuando en realidad correspondían a
    // depósitos con autorización distinta, ej. Factura Global O0-260800164).
    const esTarjetaDeclarada = LABEL_FORMA_PAGO_CONSOLIDADO[m.formaPago] === 'TARJETA';
    const infoTarjetaTicket = (esTarjetaDeclarada && m.serieVentaTicket && m.folioVentaTicket)
      ? bancoRealPorTicket?.get(`${m.serieVentaTicket}|${m.folioVentaTicket}`)
      : null;
    if (esTarjetaDeclarada && infoTarjetaTicket?.numeroAutorizacion && !esRemanenteMenor) {
      const cuentaLineaTarjeta = infoTarjetaTicket.cuentaBanco ?? m.cuenta;
      const key = `${cuentaLineaTarjeta?.codigo}|${centroCosto}|TARJETA|${infoTarjetaTicket.numeroAutorizacion}`;
      if (!gruposDetallados.has(key)) {
        gruposDetallados.set(key, {
          cuenta: cuentaLineaTarjeta, centroCosto, referencia: infoTarjetaTicket.numeroAutorizacion, tipoDetalle: 'TARJETA', subcodigo: 0,
          debe: 0, detalle: [], primerMov: m,
        });
      }
      const gt = gruposDetallados.get(key);
      gt.debe += Number(m.debe);
      gt.detalle.push({ cfdiUuid: m.cfdiUuid, serie: serieParaDetalle, monto: Number(m.debe), formaPago: 'TARJETA', nota: notaAjusteSinCfdi });
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
    g.detalle.push({ cfdiUuid: m.cfdiUuid, serie: m.serie, monto: Number(m.debe), formaPago: label ?? m.formaPago ?? null, nota: notaAjusteSinCfdi });
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
                 : g.label === 'SF'       ? 'Depósitos consolidados (SF)'
                 : g.label === 'PUNTOS'   ? 'Depósitos consolidados (Puntos)'
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
  const ETIQUETA_TIPO_DETALLE = { TRANSFERENCIA: 'TRANSFERENCIA', CHEQUE: 'CHEQUE', TARJETA: 'TARJETA' };
  for (const gt of gruposDetallados.values()) {
    const m = gt.primerMov;
    const etiqueta = ETIQUETA_TIPO_DETALLE[gt.tipoDetalle];
    const esGrupo = gt.detalle.length > 1;
    const nombre = nombresClientes?.get((m.cfdiUuid || '').toUpperCase()) || '';
    // Individual (un solo CFDI): columna C (serie) muestra el número de
    // autorización/referencia bancaria REAL cuando existe (gt.referencia,
    // ej. "034135") — solo cae al tipo genérico ("Transferencia"/"Cheque")
    // si no hay depósito bancario real ligado. Columna H (concepto) SIEMPRE
    // usa el serie-folio INTERNO real, nunca la referencia bancaria —
    // invertido 2026-08-06 (antes: columna C mostraba el tipo genérico y
    // columna H la referencia, confirmado 2026-07-28; el usuario pidió el
    // orden contrario para poder conciliar la referencia bancaria directo
    // desde la columna C). Agrupada (mismo número de autorización real en
    // 2+ CFDIs): sin cambios — la columna C sigue mostrando la referencia
    // bancaria real (gt.referencia), que siempre existe en este caso (ver
    // `key` más arriba, agrupar solo ocurre cuando hay referencia).
    //
    // El "serie-folio interno real" es el TICKET (serieVentaTicket/
    // folioVentaTicket), no `m.serie` (la Factura, que en una Global es la
    // misma para decenas de tickets distintos) — sin esto, todas las líneas
    // resueltas por ticket de una misma Global mostraban el folio de la
    // factura repetido en vez del ticket real (confirmado con el usuario
    // 2026-08-18, caso real Factura Global O0-260800164).
    const serFolReal = (m.serieVentaTicket && m.folioVentaTicket)
      ? `${m.serieVentaTicket}-${m.folioVentaTicket}`
      : (m.serie || '');
    const serieFinal = gt.referencia ?? serFolReal;
    const concepto = esGrupo ? etiqueta : ([nombre, serFolReal].filter(Boolean).join(' / ') || etiqueta);
    const serieColumnaC = esGrupo ? serieFinal : (gt.referencia ?? etiqueta);
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

// Cobros de sucursal (Caja/Bancos por identificar, ver
// cobros-sucursal-puente.service.js): cada uno ya trae su concepto
// completo armado ("Nombre cliente / Serie-Folio") — NUNCA deben pasar por
// consolidarCargos/armarBloqueContado (se perderían dentro de "Depósitos
// consolidados" o del bucket de Transferencia/Cheque, sin cliente ni
// serie/folio visibles). Se sacan del pipeline normal ANTES de procesar y se
// reinyectan después, ya armados, como línea individual — mismo principio que
// Devolución/Descuento/Bonificación/Anticipo (nunca se consolidan).
// Formato "COS-FORMADEPAGO" (ej. "COS-EFECTIVO") — confirmado con el usuario
// 2026-08-07, reemplaza el formato anterior "Cobro de otra sucursal - X".
const ETIQUETA_COBRO_SUCURSAL = 'COS';
// Mismo texto que ETIQUETA_SALDO_FAVOR en cobros-sucursal-puente.service.js.
const ETIQUETA_SALDO_FAVOR = 'SF';
// Mismo texto que ETIQUETA_SALDO_FAVOR_OCULTO en cobros-sucursal-puente.service.js
// — par generación+uso de saldo a favor generado y consumido por completo el
// mismo día en el mismo almacén: se omite del export (queda en BD intacto)
// confirmado con el usuario 2026-08-04.
const ETIQUETA_SALDO_FAVOR_OCULTO = 'SF-OCULTO';
// Mismo texto que ETIQUETA_COBRO_YA_CONTABILIZADO en cfdi-mapping.service.js
// (2026-08-25) — Cargo de Efectivo/Tarjeta de una factura cuyo cobro real ya
// se contabilizó otro día vía "Cobros sin factura" (facturación diferida
// fuera de tolerancia, ver `yaContabilizadoOtroDia`): se oculta del export
// igual que SF-OCULTO, para no duplicar el dinero entre los dos días.
const ETIQUETA_COBRO_YA_CONTABILIZADO = 'COBRO-DIA-REAL';
// Mismo texto que ETIQUETA_PUNTOS en cobros-sucursal-puente.service.js —
// monedero electrónico Club Tuberos aplicado como forma de pago, columna C =
// "PAGO" sin prefijo, mismo criterio que SF (confirmado con el usuario
// 2026-08-06).
const ETIQUETA_PUNTOS = 'PAGO';
// Mismo texto que TIPO_ORIGEN_PENDIENTE_PROPIO en
// cobros-sucursal-puente.service.js — tickets sin factura de la PROPIA
// sucursal (sin cruce real): necesitan el mismo tratamiento especial que
// 'Cobro Sucursal' (nunca se consolidan, concepto propio) pero la columna C
// NUNCA lleva el prefijo "Cobro de otra sucursal -" (no es un cruce real).
const TIPO_ORIGEN_PENDIENTE_PROPIO = 'Pendiente Propio';
// Mismo texto que TIPO_ORIGEN_CARGO_ESPECIAL en cfdi-mapping.service.js —
// porciones de Saldo a Favor/Puntos dentro del split del Cargo de una
// factura NORMAL (2026-08-06): a diferencia de Efectivo/Tarjeta (que sí se
// consolidan en un total anónimo), estas deben verse desglosadas por
// cliente/factura — mismo tratamiento de display que 'Cobro Sucursal'
// (nunca se consolidan, columna C sin prefijo para SF/PAGO), pero con un
// tipoOrigen DISTINTO para no confundirse con un cruce real de sucursal en
// `_uuidsConCargoCubiertoEnBD` (cfdi-poliza-generator.service.js).
const TIPO_ORIGEN_CARGO_ESPECIAL = 'Cargo Especial';
// Etiqueta para `tipoOrigen: 'Venta Sin Cobro'` (excedente de una factura sin
// forma de pago real que lo respalde, ver `esCasoAjusteSFPuntos`/
// `esCasoNormalParaSplit` en cfdi-mapping.service.js) — bug encontrado
// 2026-08-20 (caso real Hidalgo/B0, factura E48070D3, $618.81): esta línea se
// trataba igual que 'Cobro Sucursal' y salía con el prefijo "COS-" (Cobro de
// OTRA sucursal), una etiqueta falsa — no hay cruce de sucursal aquí, solo un
// cobro real que el desglose de cajas no pudo emparejar dentro de su ventana
// de búsqueda (puede ser de días anteriores en la MISMA sucursal, o
// directamente sin ticket real detrás, ver comentario en cfdi-mapping.service.js).
// Se mantiene aparte del consolidado (mismo tratamiento que COS), pero con
// etiqueta propia — confirmado con el usuario 2026-08-21, opción (b) de las
// 3 presentadas.
const ETIQUETA_VENTA_SIN_COBRO = 'SIN-COBRO';

// Reglas ('Cargo Especial') que se mezclan con Ventas normales por su propio
// serie/folio en vez de ir a una sección aparte — OPA (anticipo sin NC,
// 2026-08-19): es una línea complementaria de una factura normal, no un
// cruce de sucursal ni un ajuste de venta real.
const REGLAS_MEZCLADAS_CON_VENTAS = new Set(['OPA']);

// Orden fijo del bloque de "Cobro de otra sucursal": Efectivo, Transferencia,
// Saldo a favor, Cheque, Tarjeta (confirmado con el usuario 2026-08-05).
// Las líneas con depósito bancario real identificado (`_referenciaBancoReal`
// — muestran el folio del banco en vez de "TRANSFERENCIA"/"TARJETA", ver
// `_extraerCobrosSucursal` más abajo) se tratan como Transferencia: en la
// práctica casi nunca hay match 1 a 1 de Tarjeta contra un depósito bancario
// real (las liquidaciones de terminal llegan en lote, no por venta),
// confirmado con el usuario.
function _categoriaCobroSucursal(f) {
  // 'Venta Sin Cobro' NO trae una forma de pago real en `_formaPagoLabel`
  // (hereda el nombre completo de la regla fiscal, ej. "Reg 1C — Venta PUE
  // Cheque 16%") — sin este corte temprano, el `.includes('CHEQUE')` de abajo
  // hace match por accidente contra ese texto y la ordena como si fuera un
  // cobro real en Cheque. Va siempre al final, categoría propia.
  if (f._esVentaSinCobro) return 6;
  if (f._referenciaBancoReal) return 1;
  const label = (f._formaPagoLabel ?? '').toUpperCase();
  if (label === ETIQUETA_SALDO_FAVOR)   return 2;
  if (label.includes('EFECTIVO'))       return 0;
  if (label.includes('TRANSFERENCIA'))  return 1;
  if (label.includes('CHEQUE'))         return 3;
  if (label.includes('TARJETA'))        return 4;
  return 5;
}

function _extraerCobrosSucursal(movimientos) {
  const resto = [];
  const filas = [];
  // SF-OCULTO: generados y usados el mismo día en la misma sucursal — no van
  // a la póliza principal, pero SÍ deben aparecer en la hoja "Otros Ingresos"
  // (antes se descartaban con `continue`, ahora se reencaminan).
  const filasOtrosIngresosOcultos = [];
  // Pre-calcular los cfdiUuids de las entradas 'Cobro Sucursal' HABER para
  // poder identificar y también extraer el DEBE correspondiente ('Venta') del
  // mismo par. Sin esto, el DEBE queda en `resto` → `consolidarCargos` →
  // "Depósitos consolidados", inflando el total con montos que se cancelan
  // contra el haber del cobro-sucursal (el par neto es 0 en CAJA, pero solo
  // el haber se extraía, dejando el debe suelto en el consolidado).
  // Se usa cfdiUuid (no concepto) para identificar el par: más robusto que
  // texto y no puede colisionar con entradas de otras facturas.
  // IMPORTANTE: solo HABER (no DEBE) — las HABER de la cola cobrador tienen
  // cfdiUuid del vendedor; las DEBE del camino vendedor también tienen cfdiUuid
  // pero corresponden a cobros parciales cuyo 'Venta' DEBE restante SÍ debe
  // quedarse en consolidado (el monto fue cobrado aquí) — usarlas extraería
  // incorrectamente entradas que deben estar en "Depósitos consolidados"
  // (bug real: $127k de EFECTIVO/TARJETA removidos de más, 2026-08-15).
  const _uuidsCobradosPorSucursal = new Set(
    movimientos
      .filter(m => m.tipoOrigen === 'Cobro Sucursal' && Number(m.haber) > 0 && !(Number(m.debe) > 0)
                && m.cfdiUuid != null)
      .map(m => m.cfdiUuid)
      .filter(Boolean),
  );
  // Fallback SOLO para pares sin cfdiUuid: un ticket "pendiente por facturar"
  // (sin factura todavía) cobrado en otra sucursal llega con AMBAS líneas del
  // par (Venta debe / Cobro Sucursal haber) con cfdiUuid null — ver
  // `cobrosCobradoraDirecta` en cfdi-poliza-generator.service.js. El
  // emparejamiento por uuid de arriba nunca las conecta, así que el DEBE
  // quedaba suelto en "Depósitos consolidados" duplicando el monto contra el
  // HABER que sí se extrae al bloque "Cobro de otra sucursal" (confirmado con
  // el usuario 2026-08-18, caso real A0-260800476, $5,308.69 sumándose de más
  // a Efectivo). Se empareja por concepto+monto SOLO cuando cfdiUuid es null
  // en ambos lados — nunca puede colisionar con el caso de cobro PARCIAL de
  // una factura real (ese siempre trae uuid, ver comentario de más arriba).
  const _conceptosCobradosPorSucursalSinUuid = new Set(
    movimientos
      .filter(m => m.tipoOrigen === 'Cobro Sucursal' && Number(m.haber) > 0 && !(Number(m.debe) > 0)
                && m.cfdiUuid == null)
      .map(m => `${m.concepto || ''}|${Number(m.haber).toFixed(2)}`),
  );
  for (const m of movimientos) {
    // Las líneas de Saldo a Favor de un Pago (tipoComprobante='P',
    // `esSplitPagoPorFactura` en cfdi-mapping.service.js) también llevan
    // `tipoOrigen: 'Cargo Especial'` para reusar la misma etiqueta 'SF' —
    // pero a diferencia del caso de Ingreso (SF/Puntos en una venta normal,
    // donde SÍ deben salir a este bloque aparte), aquí deben quedarse en su
    // lugar dentro de `movimientos`, junto a las otras 3 líneas de su misma
    // factura (confirmado con el usuario 2026-08-11 — si se extraen, se ven
    // sueltas en vez de agrupadas por factura).
    const esCargoEspecialDePago = m.tipoOrigen === TIPO_ORIGEN_CARGO_ESPECIAL && m.tipoComprobante === 'P';
    if (esCargoEspecialDePago) { resto.push(m); continue; }
    // OPA (ver `REGLAS_MEZCLADAS_CON_VENTAS`, cfdi-mapping.service.js
    // 2026-08-19): también usa `tipoOrigen: 'Cargo Especial'` (mismo patrón
    // que SF/Puntos), pero a diferencia de esos, aquí NO hay "otra sucursal"
    // ni forma de pago que mostrar — es una línea complementaria de ESA
    // factura normal. Debe quedarse junto a sus líneas hermanas, no
    // desglosarse en el bloque de "Cobro de otra sucursal" (confirmado con el
    // usuario: aparecía como "COS-Anticipo" separado de su factura).
    if (m.tipoOrigen === TIPO_ORIGEN_CARGO_ESPECIAL && REGLAS_MEZCLADAS_CON_VENTAS.has(m.reglaNombre)) { resto.push(m); continue; }
    // El DEBE del par cobro-sucursal (tipoOrigen='Venta') se extrae aquí para
    // que no llegue a consolidarCargos y no infle "Depósitos consolidados".
    // Usa cfdiUuid para identificar el par de forma determinista: el mismo
    // UUID que tiene la entrada 'Cobro Sucursal' DEBE de cobros-sucursal-puente
    // identifica sin ambigüedad la 'Venta' DEBE de cfdiToMovimientos.
    if (m.tipoOrigen === 'Venta' && Number(m.debe) > 0 && !(Number(m.haber) > 0)
        && (
          (m.cfdiUuid != null && _uuidsCobradosPorSucursal.has(m.cfdiUuid))
          || (m.cfdiUuid == null && _conceptosCobradosPorSucursalSinUuid.has(`${m.concepto || ''}|${Number(m.debe).toFixed(2)}`))
        )) {
      filas.push({
        cuenta:             m.cuenta,
        serie:              m.serie || '',
        concepto:           m.concepto || '',
        centroCosto:        m.centroCostoObj?.clave ?? m.centroCosto ?? '',
        debe:               Number(m.debe),
        haber:              0,
        cfdiUuid:           null,
        metodoPago:         m.metodoPago ?? null,
        _subcodigo:         0,
        _categoria:         null,
        _formaPagoLabel:    m.reglaNombre || null,
        _referenciaBancoReal: null,
        _esPendientePropio: false,
      });
      continue;
    }
    if (m.tipoOrigen !== 'Cobro Sucursal' && m.tipoOrigen !== 'Venta Sin Cobro' && m.tipoOrigen !== TIPO_ORIGEN_PENDIENTE_PROPIO && m.tipoOrigen !== TIPO_ORIGEN_CARGO_ESPECIAL) { resto.push(m); continue; }
    if (m.reglaNombre === ETIQUETA_SALDO_FAVOR_OCULTO || m.reglaNombre === ETIQUETA_COBRO_YA_CONTABILIZADO) {
      filasOtrosIngresosOcultos.push({
        cuenta:      m.cuenta,
        centroCosto: m.centroCostoObj?.clave ?? m.centroCosto ?? '',
        concepto:    m.concepto || '',
        debe:        Number(m.debe),
        haber:       Number(m.haber),
        // Columna "Motivo" en la hoja "Otros Ingresos" (2026-08-17, confirmado
        // con el usuario) — distingue este caso del de SF ≤ $50 más abajo, que
        // antes se mezclaban sin forma de saber cuál era cuál en el Excel.
        motivo:      m.reglaNombre === ETIQUETA_COBRO_YA_CONTABILIZADO
          ? 'Oculto — cobro real ya contabilizado el día real del cobro'
          : 'Oculto — generado y usado el mismo día/almacén',
      });
      continue;
    }
    // OJO: NO usar `verdadBancaria`/`construirVerdadBancaria` aquí (busca por
    // `cfdiUuid`, sin distinguir vendedor/cobrador) — para una factura PPD,
    // el lado VENDEDOR (Abono Clientes + Cargo a la cuenta puente) comparte
    // el mismo `cfdiUuid` que el lado COBRADOR (Cargo Bancos real + Abono
    // puente), así que `verdadBancaria` pisaba TAMBIÉN la cuenta de Clientes
    // y la cuenta puente del lado vendedor con la cuenta de banco real,
    // creando un Cargo+Abono ficticio en la misma cuenta de banco dentro de
    // ESTA póliza (confirmado con el usuario 2026-08-04, JONATAN I0-260700186/
    // 185: aparecían 4 líneas en "1102011001" en vez de 2). El depósito
    // bancario real ya se resuelve correctamente en generación — solo en el
    // lado cobrador — vía `bancoPorVenta` en cobros-sucursal-puente.service.js
    // (más preciso: matchea por `erpLinks.serie+folioExterno`, no por
    // `folioFiscal`), así que la cuenta y el `reglaNombre` de estas líneas ya
    // vienen correctos desde ahí — solo hace falta detectarlo por el código
    // de cuenta (mismo mapeo que `BANCO_A_CODIGO_CUENTA`) para no mostrarlo
    // con el prefijo "Cobro de otra sucursal -".
    // REVERTIDO 2026-08-20: se intentó sumar "Cobro Sucursal" en Efectivo al
    // consolidado, pero al cruzar contra el Reporte de Movimientos en Cajas
    // real (Hidalgo/B0 11-ago) no aparece NINGÚN cobro de otra sucursal ese
    // día (columna "Serie y Folio" siempre B0) — sin respaldo real, se revierte
    // al tratamiento original: línea individual en "Cobro de otra sucursal",
    // fuera del consolidado, sin importar la forma de pago.
    const esBancoReal = Object.values(BANCO_A_CODIGO_CUENTA).includes(m.cuenta?.codigo);
    filas.push({
      cuenta:      m.cuenta,
      serie:       m.serie || '', // serie-folio real de la factura — solo para ordenar, se sobreescribe abajo
      concepto:    m.concepto || '',
      centroCosto: m.centroCostoObj?.clave ?? m.centroCosto ?? '',
      debe:        Number(m.debe),
      haber:       Number(m.haber),
      cfdiUuid:    null,
      // Discrimina PPD (bloque Crédito) de PUE (bloque Contado) en
      // _inyectarCobrosSucursal — ver `metodoPago` en cobros-sucursal-puente.service.js.
      metodoPago:  m.metodoPago ?? null,
      _subcodigo:  0,
      _categoria:  null,
      // Nombre(s) de forma de pago ya armado por cobros-sucursal-puente.service.js
      // (viene en reglaNombre, no en formaPago — ese es varchar(3) y no cabe
      // un nombre combinado como "EFECTIVO/TRANSFERENCIA"). Solo para la
      // etiqueta de la columna C, no se exporta tal cual.
      _formaPagoLabel: m.reglaNombre || null,
      _referenciaBancoReal: esBancoReal ? (m.reglaNombre || null) : null,
      _esPendientePropio: m.tipoOrigen === TIPO_ORIGEN_PENDIENTE_PROPIO,
      // Ver `ETIQUETA_VENTA_SIN_COBRO` — no es un cruce de sucursal, así que
      // NUNCA debe llevar el prefijo "COS-" ni mostrar `reglaNombre` (que en
      // este caso es el nombre completo de la regla fiscal, no una forma de
      // pago, ej. "Reg 1C — Venta PUE Cheque 16%" — mostrarlo confundiría más).
      _esVentaSinCobro: m.tipoOrigen === 'Venta Sin Cobro',
    });
  }
  // Primero por categoría de forma de pago (Efectivo → Transferencia → SF →
  // Cheque → Tarjeta), y dentro de cada categoría por serie-folio — antes
  // solo ordenaba por serie-folio, mezclando todos los tipos de cobro en el
  // orden en que llegaban los tickets (confirmado con el usuario 2026-08-05).
  filas.sort((a, b) => _categoriaCobroSucursal(a) - _categoriaCobroSucursal(b) || compararSerieFolio(a, b));

  // Saldo a Favor por debajo de $50 va a una pestaña aparte "Otros Ingresos"
  // en vez de la póliza (confirmado con el usuario 2026-08-07) — un SF de
  // subtotal + IVA son 2 filas (2103090001 + 2104010002) con el MISMO
  // `concepto` (cliente/serie-folio); se agrupan por ahí para decidir sobre
  // el monto TOTAL de esa factura, no cada línea por separado (partirlas
  // arbitrariamente entre las dos pestañas no tendría sentido).
  const UMBRAL_SF_OTROS_INGRESOS = 50;
  const totalPorConceptoSF = new Map();
  for (const f of filas) {
    if (f._formaPagoLabel !== ETIQUETA_SALDO_FAVOR) continue;
    const monto = Number(f.debe) + Number(f.haber);
    totalPorConceptoSF.set(f.concepto, (totalPorConceptoSF.get(f.concepto) ?? 0) + monto);
  }
  const filasOtrosIngresos = filas.filter(f =>
    f._formaPagoLabel === ETIQUETA_SALDO_FAVOR && (totalPorConceptoSF.get(f.concepto) ?? 0) <= UMBRAL_SF_OTROS_INGRESOS,
  );
  // Columna "Motivo" — ver comentario equivalente en filasOtrosIngresosOcultos.
  for (const f of filasOtrosIngresos) f.motivo = `Monto ≤ $${UMBRAL_SF_OTROS_INGRESOS}`;
  if (filasOtrosIngresos.length) {
    const idsOtrosIngresos = new Set(filasOtrosIngresos);
    for (let i = filas.length - 1; i >= 0; i--) {
      if (idsOtrosIngresos.has(filas[i])) filas.splice(i, 1);
    }
  }

  // Columna C debe decir "Cobro de otra sucursal" en vez del serie-folio —
  // mismo patrón que Transferencia/Cheque individual (línea ~825): el
  // serie-folio real se conserva en el concepto (columna H) junto al
  // cliente, la columna C pasa a mostrar la etiqueta, con la forma de pago
  // al final cuando aplica (ej. "Cobro de otra sucursal - TRANSFERENCIA").
  // Excepción: "saldo a favor" (cobros-sucursal-puente.service.js le pone
  // reglaNombre="SF" literal) muestra solo "SF" en columna C, sin el prefijo
  // — confirmado con el usuario 2026-08-03. Excepción 2: con depósito real
  // identificado, columna C es la referencia bancaria real, no la etiqueta.
  // Excepción 3: "PUNTOS" (monedero Club Tuberos, reglaNombre="PAGO") mismo
  // criterio que SF, sin prefijo. Excepción 4: cualquier línea de
  // 'Pendiente Propio' (ticket sin factura de la PROPIA sucursal, sin cruce)
  // tampoco lleva el prefijo — confirmado con el usuario 2026-08-06, no es un
  // cruce real, decirlo sería una etiqueta falsa.
  for (const f of filas) {
    f.serie = f._esVentaSinCobro
      ? ETIQUETA_VENTA_SIN_COBRO
      : f._referenciaBancoReal
        ? f._referenciaBancoReal
        : (f._formaPagoLabel === ETIQUETA_SALDO_FAVOR || f._formaPagoLabel === ETIQUETA_PUNTOS || f._esPendientePropio)
          ? (f._formaPagoLabel || ETIQUETA_COBRO_SUCURSAL)
          : (f._formaPagoLabel ? `${ETIQUETA_COBRO_SUCURSAL}-${f._formaPagoLabel}` : ETIQUETA_COBRO_SUCURSAL);
    delete f._formaPagoLabel;
    delete f._referenciaBancoReal;
    delete f._esPendientePropio;
    delete f._esVentaSinCobro;
  }
  // Agregar los SF-OCULTO (mismo día + misma sucursal) a la hoja "Otros Ingresos" —
  // nunca suman a depósitos consolidados de efectivo/tarjeta.
  if (filasOtrosIngresosOcultos.length) filasOtrosIngresos.push(...filasOtrosIngresosOcultos);

  return { resto, filas, filasOtrosIngresos };
}

// Inyecta las filas de cobro-sucursal en el bloque correspondiente — PUE en
// Contado (Contado > sin tipoVenta > Crédito, orden de preferencia), PPD en
// Crédito (Crédito > sin tipoVenta > Contado) — confirmado con el usuario
// 2026-08-03: una factura PPD cobrada en otra sucursal debe quedar en el
// apartado de Crédito (AL FINAL de ese apartado, tanto el Cargo a la cuenta
// puente como el Abono a Clientes), no mezclada en Contado ni junto a sus
// líneas hermanas por serie-folio. Nunca en Bonificaciones/Descuentos/
// Devoluciones, que son categorías ajenas. Muta `bloques` in-place después de
// que ya se calcularon todos sus `movs`.
function _inyectarCobrosSucursal(bloques, filas) {
  if (!filas.length || !bloques.length) return;
  const esBonificacionODescuento = (t) => /^(Bonificaciones|Descuentos y Devoluciones) de/.test(t || '');
  // Discrimina por metodoPago (no por cuenta): el Cargo a la cuenta puente
  // (2103040001) Y el Abono a Clientes (misma cuenta que la venta) ambos
  // llevan metodoPago='PPD' cuando la factura original es de Crédito.
  const esPPD = (f) => f.metodoPago === 'PPD';

  const filasPPD = filas.filter(esPPD);
  const filasPUE = filas.filter(f => !esPPD(f));

  if (filasPUE.length) {
    const candidatoContado =
      bloques.find(b => b.tipoVenta === 'Contado') ??
      bloques.find(b => b.tipoVenta == null) ??
      bloques.find(b => b.tipoVenta === 'Credito') ??
      bloques.find(b => !esBonificacionODescuento(b.tipoVenta)) ??
      bloques[0];
    candidatoContado.movs.push(...filasPUE);
  }

  if (filasPPD.length) {
    const candidatoCredito =
      bloques.find(b => b.tipoVenta === 'Credito') ??
      bloques.find(b => b.tipoVenta == null) ??
      bloques.find(b => b.tipoVenta === 'Contado') ??
      bloques.find(b => !esBonificacionODescuento(b.tipoVenta)) ??
      bloques[0];
    candidatoCredito.movs.push(...filasPPD);
  }
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
    const abonos = conImpuestoAlFinal(abonosCandidatos).map(m => ({ ...m, _categoria: categoria, ...extra }));
    // OPA (anticipo sin NC, ver `categorizarAjusteContado`): a diferencia del
    // anticipo estándar (Reg 22C/23, cargo primero), aquí el Ingreso (Abono)
    // debe verse PRIMERO y la aplicación del anticipo (Cargo OPA) después,
    // para trazabilidad — confirmado con el usuario 2026-08-19.
    const esOpa = categoria === 'anticipo' && grupo.some(m => m.reglaNombre === 'OPA');
    const bloque = esOpa ? [...abonos, ...cargos] : [...cargos, ...abonos];
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
 *   4. Saldo a favor (Recepción y Aplicación), por serie/folio — Anticipo
 *      (estándar con NC del SAT, y OPA sin NC) ya NO va aquí, se mezcla con
 *      Ventas normales (punto 1), ordenado por su propio serie/folio
 *      (confirmado con el usuario 2026-08-19).
 *   5. Transferencia (siempre detallada por CFDI, o agrupada cuando comparte
 *      número de autorización real) y Depósito identificado (forma de pago
 *      sin mapear con depósito bancario real ligado) — al final del export.
 */
// `separarCategorias` (usado solo para la sucursal CEDIS — ver exportContpaqXlsx):
// cuando es `true`, Bonificación (genérica + Club Tuberos) y Descuento/Devolución
// (incluye Cancelación) NO se meten a `ventas` — se devuelven aparte para que el
// caller las arme como sus propias pólizas. Anticipos y depósitos identificados
// no cambian, siguen dentro de `ventas` igual que siempre.
function armarBloqueContado(contado, verdadBancaria, nombresClientes, { separarCategorias = false, bancoRealPorTicket = null } = {}) {
  // REVERTIDO 2026-08-20: se intentó sumar Cancelación/Devolución en Efectivo
  // al consolidado, pero al cruzar contra el "Reporte de Movimientos en
  // Cajas" real (Hidalgo/B0 11-ago) se confirmó que NO hay ningún movimiento
  // de caja real detrás de esos montos (~$24,136.86 netos) — el reporte solo
  // trae UN retiro real por cancelación ($132.59, ya cubierto por
  // SF-RETIRO-EFECTIVO). "TO-CAN-16"/"TO-DEV-16-EF" son reclasificaciones
  // contables de CFDI cancelado, no efectivo físico — sumarlas inflaba el
  // consolidado sin respaldo real. Cancelación/Devolución vuelven a su
  // tratamiento original: línea individual, fuera del consolidado, sin
  // importar la forma de pago.
  const contadoAjuste = contado.filter(esAjusteContadoMov);
  const contadoNormal = contado.filter(m => !esAjusteContadoMov(m));

  const ajustes = bloquesAjustesContado(contadoAjuste);
  const bloquesDeCategorias = (...categorias) =>
    ajustes.filter(a => categorias.includes(a.categoria)).map(a => a.bloque);

  // Anticipo (estándar con NC del SAT, y OPA sin NC): a diferencia de Saldo a
  // Favor (que sigue en su propio flujo/sección, sin tocar), el Anticipo ya
  // NO va en una sección aparte después de Efectivo/Tarjeta — se mezcla con
  // Ventas normales, ordenado por su propio serie/folio (confirmado con el
  // usuario 2026-08-19: aplica a ambos mecanismos, no solo a OPA).
  const bloquesAnticipoTodos = bloquesDeCategorias('anticipo');

  const bloquesVentas = [
    ...bloquesAbonosNormales(contadoNormal.filter(m => Number(m.haber) > 0)),
    ...(separarCategorias ? [] : bloquesDeCategorias('devolucion', 'descuento', 'bonificacion')),
    // Anticipo siempre va en `bloquesVentas` sin importar `separarCategorias`
    // — a diferencia de devolucion/descuento/bonificacion, no tiene una salida
    // aparte en el modo CEDIS, así que excluirlo aquí lo perdería por completo.
    ...bloquesAnticipoTodos,
  ];
  bloquesVentas.sort((b1, b2) => compararSerieFolio(b1[0], b2[0]));

  const bloquesClubTuberos = separarCategorias ? [] : bloquesDeCategorias('clubTuberos');
  bloquesClubTuberos.sort((b1, b2) => compararSerieFolio(b1[0], b2[0]));

  // Ya no queda ningún bloque de anticipo aparte — todos se mezclaron arriba.
  const bloquesAnticipos = [];

  const { consolidados, depositosIdentificados } =
    consolidarCargos(contadoNormal, 21, false, verdadBancaria, nombresClientes, bancoRealPorTicket);

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
  if (movs.some(m => esReglaAnticipo(m.reglaNombre) || REGLAS_MEZCLADAS_CON_VENTAS.has(m.reglaNombre))) return 'anticipo';
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
    if (!categoria) {
      // Venta normal a Crédito (sin categoría de ajuste): los Abonos
      // (Ingresos, luego IVA-PPD) van ANTES que el Cargo a Clientes —
      // orden invertido respecto a Devolución/Descuento/Bonificación/
      // Anticipo (esos SÍ van cargo-primero, confirmado 2026-07-23, ver
      // abajo) — confirmado con el usuario 2026-08-18, caso real PAZCUAL
      // HERNANDEZ CORTES/O0-260800214.
      const cargosVenta  = grupo.filter(m => Number(m.debe) > 0);
      const abonosVenta   = conImpuestoAlFinal(grupo.filter(m => !(Number(m.debe) > 0)));
      bloques.push({ categoria: null, bloque: [...abonosVenta, ...cargosVenta] });
      continue;
    }
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
    const abonos = conImpuestoAlFinal(abonosCandidatos).map(m => ({ ...m, _categoria: categoria, ...extra }));
    // OPA (anticipo sin NC) — ver comentario equivalente en `bloquesAjustesContado`.
    const esOpa = categoria === 'anticipo' && grupo.some(m => m.reglaNombre === 'OPA');
    const bloque = esOpa ? [...abonos, ...cargos] : [...cargos, ...abonos];
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
  let poliza = await repo.findByIdLight(id);
  if (!poliza) throw new NotFoundError('Póliza');

  // Persistir cualquier cruce banco-real pendiente ANTES de armar el Excel —
  // así lo que se exporta y lo que queda guardado en poliza_movimientos
  // siempre coinciden, en vez de que el export lo calcule aparte cada vez
  // (ver `_resolverYPersistirCuentasBanco`). Cambiar la cuenta no altera
  // debe/haber, así que es seguro para cualquier estado excepto 'cancelada'.
  if (poliza.estado !== 'cancelada') {
    ({ poliza } = await _resolverYPersistirCuentasBanco(poliza));
  }

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
  // `formaPago` que el CFDI declara — ver `construirVerdadBancaria`. Solo
  // para el flujo NORMAL (misma sucursal, vía `consolidarCargos`/
  // `armarBloqueContado`) — los "Cobro Sucursal" (`_extraerCobrosSucursal`)
  // ya resuelven su propio depósito real en generación (`bancoPorVenta` en
  // cobros-sucursal-puente.service.js, más preciso: por `erpLinks.serie`+
  // `folioExterno`, no por `folioFiscal`). Usar `construirVerdadBancaria`
  // (por `folioFiscal`) para esas filas también pisaba la cuenta del lado
  // VENDEDOR de una factura PPD (Clientes/cuenta puente), que comparte el
  // mismo `cfdiUuid` que el lado cobrador — confirmado con el usuario
  // 2026-08-04.
  const verdadBancaria = await construirVerdadBancaria(movimientos.map(m => ({ cfdiUuid: m.cfdiUuid, serie: m.serie })));
  // Autorización real de Tarjeta por TICKET (no por CFDI completo, ver
  // `construirBancoRealPorTicket`) — solo las líneas partidas por
  // el desglose real de cobro traen `serieVentaTicket`/`folioVentaTicket`.
  const bancoRealPorTicket = await construirBancoRealPorTicket(
    movimientos.map(m => ({ serieVentaTicket: m.serieVentaTicket, folioVentaTicket: m.folioVentaTicket })),
  );

  // Cobros de sucursal: se sacan ANTES del pipeline de Contado/Crédito (nunca
  // deben pasar por consolidarCargos) y se reinyectan ya armados una vez que
  // `bloques` está listo (ver _inyectarCobrosSucursal más abajo).
  const { resto: movimientosSinCobroSucursal, filas: filasCobroSucursal, filasOtrosIngresos } = _extraerCobrosSucursal(movimientos);
  movimientos = movimientosSinCobroSucursal;

  // MEDIDA TEMPORAL (2026-08-25, pedida por el usuario, caso real ELECTRICA
  // MEXICANA DE ANTEQUERA B0-260801134): un Cargo de Venta normal por un
  // monto chico (< $10) suele ser un residuo de redondeo entre el Saldo a
  // Favor usado y el total real de la factura (el SF no cubre el 100% por un
  // par de centavos, y el faltante cae a la forma de pago que declaró el CFDI
  // aunque no hubo depósito/transferencia real) — se oculta igual que el SF
  // chico (mismo umbral $50 de `UMBRAL_SF_OTROS_INGRESOS`, pero aquí a
  // propósito más chico, $10, para no esconder ventas reales pequeñas por
  // error). PENDIENTE (ver memoria): la solución correcta es cruzar esto con
  // el monto de SF usado por la MISMA factura y absorber el residuo ahí en
  // vez de esconderlo a ciegas por monto.
  const UMBRAL_RESIDUO_VENTA_OTROS_INGRESOS = 10;
  const residuosVenta = [];
  movimientos = movimientos.filter((m) => {
    const esResiduoChico = m.tipoOrigen === 'Venta' && Number(m.debe) > 0 && Number(m.debe) < UMBRAL_RESIDUO_VENTA_OTROS_INGRESOS && Number(m.haber) === 0;
    if (!esResiduoChico) return true;
    residuosVenta.push({
      cuenta:      m.cuenta,
      centroCosto: m.centroCostoObj?.clave ?? m.centroCosto ?? '',
      concepto:    m.concepto || '',
      debe:        Number(m.debe),
      haber:       Number(m.haber),
      motivo:      `Monto < $${UMBRAL_RESIDUO_VENTA_OTROS_INGRESOS} (posible residuo de redondeo SF)`,
    });
    return false;
  });
  if (residuosVenta.length) filasOtrosIngresos.push(...residuosVenta);

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
        ? armarBloqueContado(contado, verdadBancaria, nombresClientes, { separarCategorias: true, bancoRealPorTicket })
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
            movs:      armarBloqueContado(contado, verdadBancaria, nombresClientes, { bancoRealPorTicket }),
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
            ? armarBloqueContado(contado, verdadBancaria, nombresClientes, { bancoRealPorTicket })
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
        ? anotarCargosPorFacturaSinAgrupar(movimientos, 20, verdadBancaria, nombresClientes)
        : ordenarCargoAntesDeAbono(movimientos),
      folio:     overrides.folioContado   ?? poliza.numero,
      concepto:  overrides.conceptoContado ?? poliza.concepto,
    }];
  }

  _inyectarCobrosSucursal(bloques, filasCobroSucursal);

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
      // "Otros Ingresos" (SF ≤ $50) va solo en el archivo de Ventas — ahí es
      // donde se inyectan los cobros de sucursal (_inyectarCobrosSucursal).
      const otrosIngresosGrupo = grupo === 'Ventas' ? filasOtrosIngresos : [];
      workbooks.push({
        tipoVenta: grupo,
        folio:     bloquesGrupo[0].folio,
        workbook:  _construirWorkbookPoliza(poliza, bloquesGrupo, fechaFinal, nombresClientes, otrosIngresosGrupo),
      });
    }
    return { poliza, workbooks };
  }

  const workbook = _construirWorkbookPoliza(poliza, bloques, fechaFinal, nombresClientes, filasOtrosIngresos);
  return { poliza, workbooks: [{ tipoVenta: null, folio: bloques[0]?.folio, workbook }] };
}

/**
 * Arma el workbook de CONTPAQ (hoja `poliza` con filas P/M1/AD, más las hojas
 * informativas de Desglose Consolidado y CFDIs Sustitutos si aplican) para el
 * subconjunto de `bloques` recibido — 1 sola llamada para el caso normal
 * (todos los bloques en un archivo) o 1 llamada POR bloque para CEDIS (cada
 * bloque en su propio archivo).
 */
function _construirWorkbookPoliza(poliza, bloques, fechaFinal, nombresClientes, filasOtrosIngresos = []) {
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
      // Facturas PPD cobradas en otra sucursal (cobros-sucursal-puente.service.js):
      // la etiqueta va en reglaNombre, no en serie -- `serie` es varchar(25) en
      // Postgres y no le cabe "COS-TRANSFERENCIA".
      const columnaC = /^COS\b/.test(m.reglaNombre || '') ? m.reglaNombre : (m.serie || '');
      const row = sheet.addRow([
        'M1',
        Number(m.cuenta?.codigo),
        columnaC,
        esCargo ? 0 : 1,
        esCargo ? Number(m.debe) : Number(m.haber),
        m._subcodigo ?? 0,
        0,
        m.concepto || '',
        m.centroCostoObj?.clave ?? m.centroCosto ?? '',
      ]);
      // Monto (columna E) siempre con 2 decimales — sin esto, $199.90 se ve
      // como "199.9" en Excel (igual que ya se fuerza `numFmt` en la fecha
      // del encabezado). No cambia el valor real de la celda que lee
      // CONTPAQ, solo cómo se despliega (confirmado con el usuario 2026-08-07).
      row.getCell(5).numFmt = '#,##0.00';
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
            nota:             d.nota || '',
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
      { header: 'Nota',          key: 'nota',           width: 34 },
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

  // Hoja "Otros Ingresos": Saldo a Favor de $50 o menos — no se contabilizan
  // en la póliza (ver `_extraerCobrosSucursal`/UMBRAL_SF_OTROS_INGRESOS),
  // solo quedan aquí como informativo (confirmado con el usuario 2026-08-07).
  if (filasOtrosIngresos.length > 0) {
    const wsOtrosIngresos = workbook.addWorksheet('Otros Ingresos');
    wsOtrosIngresos.columns = [
      { header: 'Cuenta',        key: 'cuenta',      width: 14 },
      { header: 'Sucursal',      key: 'centroCosto', width: 12 },
      { header: 'Cliente / Serie-Folio', key: 'concepto', width: 40 },
      { header: 'Monto',         key: 'monto',       width: 16 },
      { header: 'Motivo',        key: 'motivo',      width: 42 },
    ];
    wsOtrosIngresos.getRow(1).font = { bold: true };
    wsOtrosIngresos.getRow(1).eachCell(cell => {
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFD9D9D9' } };
    });
    for (const f of filasOtrosIngresos) {
      const row = wsOtrosIngresos.addRow({
        cuenta:      f.cuenta?.codigo ?? '',
        centroCosto: f.centroCosto ?? '',
        concepto:    f.concepto ?? '',
        monto:       Number(f.debe) || Number(f.haber) || 0,
        motivo:      f.motivo ?? '',
      });
      row.getCell('monto').numFmt = '#,##0.00';
    }
    wsOtrosIngresos.autoFilter = { from: 'A1', to: 'E1' };
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

  // Hoja de tickets de cajas con cobro real (mismo día) pero SIN ninguna
  // factura ligada — ej. venta de mostrador que nunca se globalizó. Solo
  // informativo: NUNCA se contabilizaron en esta póliza — ver
  // `_detectarPendientesPorFacturar` en cobros-sucursal-puente.service.js.
  if (poliza.pendientesPorFacturar?.length > 0) {
    const wsPorFacturar = workbook.addWorksheet('Pendientes Por Facturar');
    wsPorFacturar.columns = [
      { header: 'Centro de costo',  key: 'centroCosto',  width: 14 },
      { header: 'Sucursal',         key: 'sucursal',     width: 18 },
      { header: 'Cliente',          key: 'nombreCliente', width: 28 },
      { header: 'Serie',            key: 'serie',        width: 8 },
      { header: 'Folio venta',      key: 'folio',        width: 16 },
      { header: 'Fecha del cobro',  key: 'fecha',        width: 16 },
      { header: 'Monto cobrado',    key: 'monto',        width: 16 },
      { header: 'Formas de pago',   key: 'formasPago',   width: 32 },
      { header: 'Folio del cobro (cajas)', key: 'folioOrigen', width: 20 },
    ];
    wsPorFacturar.getRow(1).font = { bold: true };
    wsPorFacturar.getRow(1).eachCell(cell => {
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFE699' } };
    });
    for (const p of poliza.pendientesPorFacturar) {
      const row = wsPorFacturar.addRow({
        centroCosto:   p.centroCosto,
        sucursal:      p.sucursal,
        nombreCliente: p.nombreCliente ?? 'CLIENTE NO IDENTIFICADO',
        serie:         p.serie,
        folio:         p.folio,
        fecha:         p.fecha ? new Date(p.fecha) : null,
        monto:         p.monto,
        formasPago:    (p.formasPago || []).map(fp => `${fp.nombre ?? '?'}: ${fp.monto}`).join(', '),
        folioOrigen:   p.folioOrigen,
      });
      if (row.getCell('fecha').value) row.getCell('fecha').numFmt = 'm/d/yy hh:mm';
      row.getCell('monto').numFmt = '#,##0.00';
    }
    wsPorFacturar.autoFilter = { from: 'A1', to: 'I1' };
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
  reporteDescuadradas, generarCierreIVA, exportContpaqXlsx, asociarFolioContpaq, reemplazarCuenta, resolverCuentasBanco,
  resolverCuentasPorCfdisIdentificados,
  _consolidarCargos: consolidarCargos, _moverAjustesAlFinal: moverAjustesAlFinal,
  _categorizarAjusteContado: categorizarAjusteContado, _categoriaDeGrupoCredito: categoriaDeGrupoCredito,
};
