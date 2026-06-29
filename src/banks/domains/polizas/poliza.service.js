'use strict';

const repo = require('./repositories/poliza.repository');
const { NotFoundError, BadRequestError: ValidationError, ForbiddenError } = require('../../shared/errors/AppError');
const { AccountPlan, CfdiMappingRule, PolizaMovimiento, Poliza } = require('../../../shared/models/postgres');
const { Op } = require('sequelize');

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

module.exports = { list, getById, create, update, cancel, contabilizar, revertir, generarXmlSat, reporteDescuadradas, generarCierreIVA };
