'use strict';

/**
 * Diagnóstico de solo lectura — investiga el bug reportado por el usuario en el folio
 * 038309 (erpId 6a7e6386f1a6ec0001f912ab, $196,431.71): cada corrida de _syncErpKoreJob
 * resetea el movimiento a status:'no_identificado'/saldoErp:0 pese a que fue vinculado
 * manualmente y el pago es real.
 *
 * Mecanismo confirmado a mano contra los datos reales de ese folio (ver conversación):
 * el kardex de Kore para esa CxC trae, además del abono real tageado (Aut=038309), una
 * línea "SALDO A FAVOR" SIN ningún tag de identidad (Numo/Aut/Num Recibo) — parte de la
 * resolución de una retención (RET/APA) — cuyo monto coincide EXACTO con el abono real.
 * Tanto _montoSaldoLinkPorMovimiento (la rutina que usa _syncErpKoreJob hoy) como
 * _aportesPorErpIdCronologico (la rutina "corregida", pero nunca conectada a los jobs
 * diarios) tratan esa coincidencia de magnitud como una reversa sin tag que cancela el
 * abono real — ninguna de las 2 sabe que "SALDO A FAVOR" es la resolución de una
 * retención, no una reversión genuina del pago.
 *
 * Qué hace este script:
 *   1) Re-simula ambas funciones contra el snapshot YA GUARDADO en Mongo
 *      (erpLinks[].movimientosKore) del caso ancla (038309), sin pegarle a Kore — para
 *      confirmar con código real, no a mano, que las 2 producen el mismo resultado falso.
 *   2) Escanea TODOS los BankMovement con la misma firma (vínculo humano, un abono
 *      tageado real, y una línea sin tag de magnitud exactamente igual en el mismo
 *      erpLink) para saber cuántos más están sufriendo el mismo problema — usa el
 *      snapshot guardado, no requiere Kore.
 *
 * No escribe nada en la base de datos.
 *
 * Uso: node src/banks/scripts/diag-retencion-cancela-aporte.js
 */

require('dotenv').config();

const mongoose     = require('mongoose');
const BankMovement = require('../domains/banks/BankMovement.model');
const erpRoutes    = require('../domains/erp/erp.routes');

const ERP_ID_ANCLA = '6a7e6386f1a6ec0001f912ab'; // D0-260802730, folio 038309

// _perteneceAEsteMovimiento / _tieneTagIdentidadPropia no están exportadas de erp.routes.js
// (solo las funciones de más alto nivel) — se replican acá TAL CUAL (erp.routes.js:1222-1260)
// para no tocar el módulo real solo por un diagnóstico de solo lectura.
function _normalizarAutorizacion(v) {
  return String(v ?? '').replace(/\D/g, '').replace(/^0+/, '');
}
function _tieneTagIdentidadPropia(fp) {
  return (fp.adicionales ?? []).some(a => a.nombre === 'Numo' || a.nombre === 'Aut' || a.nombre === 'Num Recibo');
}
function _perteneceAEsteMovimiento(fp, mov) {
  const autNormMov = _normalizarAutorizacion(mov.numeroAutorizacion);
  const folioMov   = String(mov.folio ?? '').trim();
  const ads = fp.adicionales ?? [];
  const numoTag = ads.find(a => a.nombre === 'Numo');
  if (numoTag) {
    const valRaw = String(numoTag.valor ?? '').trim();
    if (autNormMov && _normalizarAutorizacion(valRaw) === autNormMov) return true;
    if (folioMov && valRaw === folioMov) return true;
  }
  const autTag = ads.find(a => a.nombre === 'Aut');
  if (autTag) {
    const valRaw = String(autTag.valor ?? '').trim();
    if (folioMov && valRaw.includes(folioMov)) return true;
    if (autNormMov && _normalizarAutorizacion(valRaw) === autNormMov) return true;
  }
  const numReciboTag = ads.find(a => a.nombre === 'Num Recibo');
  if (numReciboTag && folioMov && String(numReciboTag.valor ?? '').trim() === folioMov) return true;
  return false;
}

// Reconstruye la forma `raw0.movimientos[]` que esperan _montoSaldoLinkPorMovimiento /
// _aportesPorErpIdCronologico a partir del snapshot guardado (erpLinks[].movimientosKore)
// — mismos campos que ambas funciones realmente leen (formasPago[].adicionales, total),
// no se necesita reconsultar Kore.
function raw0DesdeSnapshot(movimientosKore) {
  return {
    movimientos: (movimientosKore ?? []).map(m => ({
      total: m.total,
      formasPago: (m.formasPago ?? []).map(fp => ({
        adicionales: fp.adicionales ?? [],
        monto: fp.monto,
      })),
    })),
  };
}

function tieneAbonoTageadoYReversaSinTagDeIgualMagnitud(mov, link) {
  const movimientos = link.movimientosKore ?? [];
  const tageados = [];
  const sinTag = [];
  for (const m of movimientos) {
    for (const fp of m.formasPago ?? []) {
      if (_perteneceAEsteMovimiento(fp, mov)) tageados.push(Math.abs(m.total ?? 0));
      else if (!_tieneTagIdentidadPropia(fp)) sinTag.push(Math.abs(m.total ?? 0));
    }
  }
  if (!tageados.length || !sinTag.length) return null;
  for (const t of tageados) {
    for (const s of sinTag) {
      if (Math.abs(t - s) < 0.01) return t;
    }
  }
  return null;
}

// Hipótesis refinada: la línea sin tag que cancela el abono real, ¿coincide en magnitud con
// una línea de RETENCIÓN genuina (formasPago vacío — mismo criterio que _retencionVigente)
// del MISMO kardex? Si sí, es evidencia de que es la resolución de ESA retención, no una
// reversión real del pago — señal segura para excluirla sin afectar el uso legítimo de
// "SALDO A FAVOR" que el usuario confirmó que existe.
function lineaSinTagCoincideConRetencionGenuina(link, montoLineaSinTag) {
  const movimientos = link.movimientosKore ?? [];
  const lineasRetencion = movimientos.filter(m => !Array.isArray(m.formasPago) || m.formasPago.length === 0);
  return lineasRetencion.some(m => Math.abs(Math.abs(m.total ?? 0) - montoLineaSinTag) < 0.01);
}

async function run() {
  await mongoose.connect(process.env.MONGODB_URI);
  console.log('Conectado a MongoDB.\n');

  // ── 1) Caso ancla: replay real de ambas funciones ──────────────────────────
  const movAncla = await BankMovement.findOne({ erpIds: ERP_ID_ANCLA }).lean();
  if (!movAncla) {
    console.log(`No se encontró el BankMovement ancla (erpId=${ERP_ID_ANCLA}).`);
  } else {
    const linkAncla = movAncla.erpLinks.find(l => l.erpId === ERP_ID_ANCLA);
    const raw0 = raw0DesdeSnapshot(linkAncla.movimientosKore);

    console.log('═══ CASO ANCLA — folio', movAncla.folio, '═══');
    console.log('Líneas del kardex guardado:');
    for (const m of linkAncla.movimientosKore ?? []) {
      for (const fp of m.formasPago ?? []) {
        const tags = (fp.adicionales ?? []).map(a => `${a.nombre}=${a.valor}`).join(', ') || '(sin tag)';
        console.log(`  · total=${m.total}  formaPago="${fp.formaPagoDescripcion}"  [${tags}]`);
      }
    }

    const viejo = erpRoutes._montoSaldoLinkPorMovimiento(raw0, movAncla);
    console.log(`\n_montoSaldoLinkPorMovimiento (la que usa _syncErpKoreJob hoy): ${viejo}`);

    const cronologico = erpRoutes._aportesPorErpIdCronologico(raw0, [movAncla]);
    const idxAncla = 0;
    const nuevo = cronologico.has(idxAncla) ? cronologico.get(idxAncla) : null;
    console.log(`_aportesPorErpIdCronologico (la "corregida", sin conectar a los jobs):  ${nuevo === null ? 'null (no determinado)' : nuevo}`);

    console.log('\nConclusión caso ancla:');
    if (viejo === 0 && nuevo === null) {
      console.log('  Las 2 fallan igual — ninguna reconoce el pago real de', movAncla.deposito);
      console.log('  Diferencia práctica: la vieja escribe saldoErpAportado=0 (dispara el reseteo a');
      console.log('  no_identificado); la "corregida" devuelve null (no determinado), que el job NO');
      console.log('  escribe — dejaría de romperlo de nuevo, pero tampoco lo repara si ya quedó en 0.');
    } else {
      console.log(`  Resultados: vieja=${viejo}, cronológica=${nuevo} — revisar a mano, no coincide con lo esperado.`);
    }
  }

  // ── 2) Escaneo de otros movimientos con la misma firma ──────────────────────
  console.log('\n\n═══ ESCANEO — otros movimientos con la misma firma (solo lectura) ═══');
  const candidatos = await BankMovement.find({
    isActive: true,
    status: 'no_identificado',
    saldoErp: 0,
    erpLinks: { $exists: true, $ne: [] },
  }).lean();

  console.log(`Movimientos no_identificado/saldoErp=0 con al menos un erpLink: ${candidatos.length}`);

  const afectados = [];
  for (const mov of candidatos) {
    for (const link of mov.erpLinks ?? []) {
      const esHumano = (mov.identificadoPor ?? []).some(ip => ip.erpId === link.erpId && ip.userId);
      if (!esHumano) continue;
      if (!link.movimientosKore?.length) continue;
      const montoCancelado = tieneAbonoTageadoYReversaSinTagDeIgualMagnitud(mov, link);
      if (montoCancelado != null) {
        const coincideConRetencion = lineaSinTagCoincideConRetencionGenuina(link, montoCancelado);
        afectados.push({
          folio: mov.folio, banco: mov.banco, deposito: mov.deposito,
          erpId: link.erpId, folioExterno: link.folioExterno, montoCancelado,
          tieneRetencion: link.tieneRetencion, coincideConRetencion,
        });
        break; // un match por movimiento alcanza para listarlo
      }
    }
  }

  console.log(`\nAfectados por el patrón "abono tageado + reversa sin tag de igual magnitud": ${afectados.length}\n`);
  for (const a of afectados) {
    console.log(`  folio=${a.folio}  depósito=${a.deposito}  monto cancelado=${a.montoCancelado}  tieneRetencion=${a.tieneRetencion}  ¿coincide con línea de retención genuina?=${a.coincideConRetencion}`);
  }
  const conCoincidencia = afectados.filter(a => a.coincideConRetencion).length;
  console.log(`\nDe los ${afectados.length} afectados, ${conCoincidencia} tienen la línea canceladora coincidiendo en monto con una retención genuina del mismo kardex.`);
  if (conCoincidencia < afectados.length) {
    console.log(`${afectados.length - conCoincidencia} NO coinciden — la hipótesis "excluir solo cuando coincide con retención" no los cubriría, revisar a mano.\n`);
    for (const a of afectados.filter(x => !x.coincideConRetencion)) {
      const movFull = candidatos.find(c => c.folio === a.folio);
      const linkFull = movFull.erpLinks.find(l => l.erpId === a.erpId);
      console.log(`\n--- folio=${a.folio} (no coincide) — kardex completo guardado ---`);
      for (const m of linkFull.movimientosKore ?? []) {
        console.log(`  total=${m.total}  serie=${m.serie ?? '(sin serie)'}`);
        for (const fp of m.formasPago ?? []) {
          const tags = (fp.adicionales ?? []).map(x => `${x.nombre}=${x.valor}`).join(', ') || '(sin tag)';
          console.log(`      formaPago="${fp.formaPagoDescripcion}" monto=${fp.monto} [${tags}]`);
        }
      }
    }
  }

  await mongoose.connection.close();
}

run().catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
