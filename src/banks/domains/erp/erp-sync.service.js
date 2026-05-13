'use strict';

const axios              = require('axios');
const ErpCuentaPendiente = require('./ErpCuentaPendiente.model');

const ERP_CAJA_BASE_URL = (process.env.ERP_CAJA_BASE_URL || '').replace(/\/$/, '');
const ERP_TOKEN         = process.env.ERP_TOKEN || '';

/**
 * Descarga cuentas pendientes del ERP y las upserta en el caché local.
 * Idempotente: puede llamarse N veces sin duplicar datos (clave: erpId).
 *
 * @param {{ fechaDesde?, fechaHasta?, estadoCobro?, serieExterna?, folioExterno?, nombrePersona? }} params
 * @returns {Promise<{ synced: number, lastSeenAt: Date }>}
 * @throws {Error} si ERP_CAJA_BASE_URL no está configurado o la petición falla
 */
async function sincronizarCuentasPendientes(params = {}) {
  if (!ERP_CAJA_BASE_URL) {
    throw new Error('ERP no configurado (ERP_CAJA_BASE_URL ausente)');
  }

  const queryParams = {};
  if (params.fechaDesde)    queryParams.fechaDesde    = params.fechaDesde;
  if (params.fechaHasta)    queryParams.fechaHasta    = params.fechaHasta;
  if (params.estadoCobro)   queryParams.estadoCobro   = params.estadoCobro;
  if (params.serieExterna)  queryParams.serieExterna  = String(params.serieExterna).trim();
  if (params.folioExterno)  queryParams.folioExterno  = String(params.folioExterno).trim();
  if (params.nombrePersona) queryParams.nombrePersona = String(params.nombrePersona).trim();

  const response = await axios.get(`${ERP_CAJA_BASE_URL}/cuentas-pendientes`, {
    params:  queryParams,
    headers: { Authorization: `Bearer ${ERP_TOKEN}` },
    timeout: 15000,
  });

  const raw = response.data?.Data?.cuentas || [];
  const now = new Date();

  if (raw.length > 0) {
    await Promise.all(raw.map(c => ErpCuentaPendiente.updateOne(
      { erpId: c.id },
      {
        $set: {
          erpId:            c.id,
          serie:            c.serie            ?? null,
          folio:            c.folio            ?? null,
          serieExterna:     c.serieExterna     ?? null,
          folioExterno:     c.folioExterno     ?? null,
          folioFiscal:      c.folioFiscal      ?? null,
          tipoPago:         c.tipoPago         ?? null,
          subtotal:         c.subtotal         ?? null,
          impuesto:         c.impuesto         ?? null,
          total:            c.total            ?? null,
          saldoActual:      c.saldoActual      ?? null,
          fechaCreacion:    c.fechaCreacion    ?? null,
          fechaRealPago:    c.fechaRealPago    ?? null,
          fechaAfectacion:  c.fechaAfectacion  ?? null,
          fechaVencimiento: c.fechaVencimiento ?? null,
          fechaProgramada:  c.fechaProgramada  ?? null,
          concepto:         c.concepto         ?? null,
          conceptoCobroID:  c.conceptoCobroID  ?? null,
          almacen:          c.almacen          ?? null,
          personaId:        c.personaId        ?? null,
          claveImpuesto:    c.claveImpuesto    ?? null,
          factorImpuesto:   c.factorImpuesto   ?? null,
          anotacion:        c.anotacion        ?? null,
          plazo:            c.plazo            ?? null,
          tipoMovimiento:   c.tipoMovimiento   ?? null,
          movimientos:      c.movimientos      ?? [],
          lastSeenAt:       now,
        },
      },
      { upsert: true },
    )));
  }

  // Se devuelve `raw` para que el caller (ruta HTTP) pueda construir la respuesta
  // paginada sin hacer una segunda consulta a la BD.
  return { synced: raw.length, lastSeenAt: now, raw };
}

module.exports = { sincronizarCuentasPendientes };
