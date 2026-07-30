'use strict';

/**
 * Motor de sugerencias de conciliación (fallback #3) -- capa de servicio para el
 * endpoint GET /api/reports/pagos-banco/sugerencias-conciliacion (visor).
 *
 * Este motor complementa a matchAutorizacionesDesdeErp (bank-autorizaciones.service.js),
 * que solo cruza contra `erp_cuentas_pendientes` -- feed que el ERP expone SOLO con
 * cuentas ABIERTAS. En cuanto una factura se salda, desaparece de ese feed, así que
 * el motor real nunca tiene con qué cruzarla si el depósito bancario se importa
 * después de que la CxC ya cerró en el ERP.
 *
 * Este motor usa dos señales que NUNCA expiran:
 *   1. `cfdis` (CFDI tipo P, complemento de pago) conserva para siempre impPagado +
 *      fechaPago por cada factura pagada, sin depender de erp_cuentas_pendientes.
 *   2. El propio historial de `bank_movements` YA identificados (tampoco se purga) --
 *      permite construir un catálogo firma-bancaria -> cliente real (confirmado
 *      contra el receptor del CFDI, no un nombre libre cualquiera).
 *
 * Solo GENERA sugerencias -- no escribe erpLinks. La escritura, al aceptar una
 * sugerencia desde el visor, reutiliza el mismo endpoint que usa la vinculación
 * manual (PUT /movements/:id/erp-ids -> service.setErpIds), para no duplicar esa
 * lógica de escritura/estado/candado.
 */

const CFDI = require('../models/CFDI');
const BankMovement = require('../../banks/domains/banks/BankMovement.model');
const {
  construirCatalogoFirmas,
  sugerirClientePorFirma,
  construirCandidatosPago,
  calcularHits,
} = require('./conciliacion-fallback.util');

const TOLERANCIA_MONTO       = 2;   // pesos
const TOLERANCIA_DIAS_GLOBAL = 5;   // ventana para el cruce SIN cliente confirmado
const TOLERANCIA_DIAS_FIRMA  = 15;  // ventana mas laxa cuando ya sabemos el cliente por firma
const MONTO_MINIMO           = 10;  // por debajo de esto son residuos/centavos
const RFC_EMPRESA            = process.env.RFC_EMPRESA_PROPIA || 'CCO011113663';

const ESTADOS_ORDEN = ['CONFIRMADO_FIRMA_CFDI', 'MATCH_UNICO_MONTO_FECHA', 'SOLO_FIRMA', 'AMBIGUO'];

/**
 * @param {object} params
 * @param {string} params.fechaInicio - 'YYYY-MM-DD'
 * @param {string} params.fechaFin    - 'YYYY-MM-DD'
 * @param {string} [params.banco]
 * @returns {Promise<{ resumen: object, sugerencias: object[] }>}
 */
async function generarSugerencias({ fechaInicio, fechaFin, banco }) {
  if (!fechaInicio || !fechaFin) {
    const err = new Error('fechaInicio y fechaFin son requeridos');
    err.statusCode = 400;
    throw err;
  }

  const desde = new Date(fechaInicio + 'T00:00:00Z');
  const hasta = new Date(fechaFin + 'T23:59:59Z');

  const movimientos = await BankMovement.find({
    isActive: true,
    status:   'no_identificado',
    deposito: { $gt: MONTO_MINIMO },
    fecha:    { $gte: desde, $lte: hasta },
    // Un movimiento con erpLinks ya tiene una decisión tomada (por un humano o por el
    // motor real) aunque `status` siga en 'no_identificado' -- eso pasa, por ejemplo,
    // cuando de las N CxC vinculadas alguna todavía tiene saldo abierto (PPD sin
    // terminar de pagar), lo cual es un estado normal del ERP y no un caso de "no se
    // pudo cruzar". Este motor solo debe proponer para movimientos SIN ningún vínculo.
    erpLinks: { $size: 0 },
    ...(banco ? { banco } : {}),
  }).select('_id banco fecha deposito folio concepto').sort({ fecha: 1 }).lean();

  const conteo = { CONFIRMADO_FIRMA_CFDI: 0, MATCH_UNICO_MONTO_FECHA: 0, SOLO_FIRMA: 0, AMBIGUO: 0, SIN_SUGERENCIA: 0 };
  if (!movimientos.length) {
    return { resumen: { totalMovimientos: 0, ...conteo }, sugerencias: [] };
  }

  const { catalogo: catalogoFirmas } = await construirCatalogoFirmas(BankMovement, CFDI);

  const yaVinculadas = new Set(
    (await BankMovement.distinct('erpLinks.folioFiscal', { isActive: true })).filter(Boolean).map(f => f.toUpperCase()),
  );

  const ventanaInicio = new Date(desde.getTime() - TOLERANCIA_DIAS_FIRMA * 86400000);
  const ventanaFin    = new Date(hasta.getTime() + TOLERANCIA_DIAS_FIRMA * 86400000);
  const { candidatosFactura, candidatosPagoCompleto } = await construirCandidatosPago(CFDI, {
    ventanaInicio, ventanaFin, rfcEmpresa: RFC_EMPRESA,
  });
  const candidatosFacturaLibres = candidatosFactura.filter(c => !yaVinculadas.has(c.idDocumento.toUpperCase()));
  // Mismo criterio para "pago completo": si TODAS las facturas que cubre ya tienen
  // un movimiento vinculado (ya aparecen en Depósitos Ingresos), no hay nada nuevo
  // que ofrecer -- se descarta el candidato completo. Si solo cubre una factura
  // libre entre varias, se conserva (sigue habiendo algo por conciliar).
  const candidatosPagoCompletoLibres = candidatosPagoCompleto.filter(c =>
    c.facturas.length === 0 || !c.facturas.every(f => yaVinculadas.has(f.toUpperCase())),
  );

  const sugerencias = [];

  for (const mov of movimientos) {
    const sugerencia = sugerirClientePorFirma(mov.concepto, catalogoFirmas);

    const hitsGlobal = calcularHits(mov, candidatosFacturaLibres, candidatosPagoCompletoLibres, {
      toleranciaMonto: TOLERANCIA_MONTO, toleranciaDias: TOLERANCIA_DIAS_GLOBAL,
    });

    let estado, candidatos, extra = {};

    if (sugerencia) {
      const hitsCliente = calcularHits(mov, candidatosFacturaLibres, candidatosPagoCompletoLibres, {
        toleranciaMonto: TOLERANCIA_MONTO, toleranciaDias: TOLERANCIA_DIAS_FIRMA, rfcFiltro: sugerencia.cliente.rfc,
      });

      const clienteSugerido = {
        rfc: sugerencia.cliente.rfc,
        nombre: sugerencia.cliente.nombre,
        vecesVistoEnHistorico: sugerencia.cliente.count,
        firma: sugerencia.firma,
      };

      if (hitsCliente.length === 1) {
        estado = 'CONFIRMADO_FIRMA_CFDI';
        candidatos = hitsCliente;
        extra.clienteSugerido = clienteSugerido;
        if (hitsGlobal.length > 1) extra.ambiguedadResueltaPorFirma = true;
      } else if (hitsCliente.length === 0) {
        // Sabemos quién es (firma bancaria) pero ninguna de sus facturas/pagos
        // calza en monto/fecha. Igual mostramos sus candidatos SIN tolerancia
        // (dentro de la ventana ya acotada por fecha) para que el usuario tenga
        // un uuid con el que trabajar en vez de tener que buscarlo por fuera --
        // el diffMonto/diffDias real queda visible en cada candidato para que
        // se note que no es un match automático.
        estado = 'SOLO_FIRMA';
        candidatos = calcularHits(mov, candidatosFacturaLibres, candidatosPagoCompletoLibres, {
          toleranciaMonto: Infinity, toleranciaDias: Infinity, rfcFiltro: sugerencia.cliente.rfc,
        });
        extra.clienteSugerido = clienteSugerido;
        if (candidatos.length > 0) {
          extra.nota = 'Sin match exacto en monto/fecha para este cliente -- revisa el diff antes de aceptar.';
        }
      } else {
        estado = 'AMBIGUO';
        candidatos = hitsCliente;
        extra.clienteSugerido = clienteSugerido;
        extra.nota = 'Ambiguo incluso dentro del mismo cliente (varias facturas suyas calzan en monto/fecha)';
      }
    } else if (hitsGlobal.length === 1) {
      estado = 'MATCH_UNICO_MONTO_FECHA';
      candidatos = hitsGlobal;
    } else if (hitsGlobal.length > 1) {
      estado = 'AMBIGUO';
      candidatos = hitsGlobal;
    } else {
      estado = 'SIN_SUGERENCIA';
      candidatos = [];
    }

    conteo[estado]++;
    if (estado !== 'SIN_SUGERENCIA') {
      sugerencias.push({
        movimiento: { _id: mov._id, banco: mov.banco, fecha: mov.fecha, deposito: mov.deposito, folio: mov.folio, concepto: mov.concepto },
        estado,
        ...extra,
        candidatos: candidatos.slice(0, 5),
      });
    }
  }

  sugerencias.sort((a, b) => ESTADOS_ORDEN.indexOf(a.estado) - ESTADOS_ORDEN.indexOf(b.estado));

  return {
    resumen: { totalMovimientos: movimientos.length, ...conteo },
    sugerencias,
  };
}

module.exports = { generarSugerencias };
