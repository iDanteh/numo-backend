const mongoose = require('mongoose');

const bankMovementSchema = new mongoose.Schema({
  banco: {
    type: String,
    enum: [
      'Banamex', 'BBVA', 'Santander', 'Azteca',
      'Banorte', 'HSBC', 'Inbursa', 'Scotiabank',
      'BanBajío', 'Afirme', 'Intercam', 'Nu',
      'Spin', 'Hey Banco', 'Albo',
    ],
    required: true,
    index: true,
  },

  fecha: { type: Date, required: true, index: true },

  // Concepto completo (concatenación de todas las sub-filas donde aplique)
  concepto: { type: String, trim: true },

  // Montos
  deposito: { type: Number, default: null },
  retiro:   { type: Number, default: null },
  saldo:    { type: Number, default: null },

  // Identificadores del movimiento
  numeroAutorizacion: { type: String, trim: true, default: null },
  referenciaNumerica: { type: String, trim: true, default: null },

  // Estado de conciliación del movimiento
  status: {
    type:    String,
    enum:    ['no_identificado', 'identificado', 'otros', 'reclasificado'],
    default: 'no_identificado',
    index:   true,
  },

  // Categoría inferida del concepto
  categoria: {
    type:    String,
    default: null,
    index:   true,
  },

  // Folio auto-incremental de 6 dígitos (000001, 000002…)
  folio: { type: String, default: null },

  // UUID del CFDI; sólo se gestiona automáticamente vía folioFiscal del ERP
  uuidXML: { type: String, default: null },

  // IDs de CxC provenientes del ERP externo (N por movimiento)
  erpIds: { type: [String], default: [] },

  // Snapshot por cada CxC vinculada: saldoActual, folioFiscal, serie y folioExterno al momento de la vinculación
  erpLinks: {
    type: [{
      erpId:            { type: String, required: true },
      saldoActual:      { type: Number, default: 0 },
      // Acumulado cobrado por transferencia/depósito en efectivo (forma "bancaria") —
      // alimenta el badge/dropdown "CxC vinculadas" en la tabla de movimientos.
      saldoPagado:      { type: Number, default: null },
      // Acumulado cobrado por TODAS las formas de pago (transferencia, efectivo, cheque,
      // tarjeta, etc.) — es la fuente de saldoErp/status en aplicarLogicaErp. Ver
      // bank.service.js. Deliberadamente separado de saldoPagado: ese sigue siendo
      // bancario-only para no inflar el dropdown de CxC vinculadas con formas no bancarias.
      saldoPagadoTotal: { type: Number, default: null },
      folioFiscal:    { type: String, default: null },
      total:          { type: Number, default: null },
      serie:          { type: String, default: null },
      folioExterno:   { type: String, default: null },
      tieneRetencion: { type: Boolean, default: false },
      // Suma de movimientosKore[] con serie 'RET' (valor absoluto) — cuánto tiene retenido
      // esta CxC ahora mismo, sin tener que recorrer movimientosKore para calcularlo. Lo
      // refresca el sync junto con tieneRetencion; null cuando no hay ninguna retención.
      montoRetenido:  { type: Number, default: null },
      tipoPago:       { type: String, default: null },
      // Marca de procedencia de la CxC: 'cfdi_liquidado' cuando se resolvió vía el buscador
      // de CFDI sin verificación en vivo contra Kore (ver erp.routes.js,
      // _resolverCuentaDesdeCfdiLiquidado). Persistido para que la regla de negocio "una CxC
      // de origen CFDI nunca es cobrable" sobreviva a cerrar y reabrir el modal ERP.
      origen:         { type: String, default: null },
      // Snapshot de movimientos Kore para esta CxC (todos menos el primero — el primero
      // es el cargo original, no aporta al rastreo de conciliación). Lo llena y refresca
      // el job "Sync ERP-Kore" en cada corrida mientras la CxC siga abierta.
      movimientosKore: {
        type: [{
          serie:         { type: String, default: null },
          folio:         { type: String, default: null },
          serieOrigen:   { type: String, default: null },
          folioOrigen:   { type: String, default: null },
          fecha:         { type: Date,   default: null },
          saldoAnterior: { type: Number, default: null },
          saldoActual:   { type: Number, default: null },
          subtotal:      { type: Number, default: null },
          impuesto:      { type: Number, default: null },
          total:         { type: Number, default: null },
          // Desglose por forma de pago que Kore reporta para ESTE movimiento — información
          // adicional para análisis futuro (nunca se usa hoy para calcular saldoErpAportado
          // ni ningún otro campo; ver _montoSaldoLink en erp.routes.js, que sigue basándose
          // solo en `total`). Se pisa completo en cada corrida junto con el resto de
          // movimientosKore — no es una bitácora acumulativa como desglosePorFormaPago.
          formasPago: {
            type: [{
              formaPagoId:          { type: String, default: null },
              formaPagoDescripcion: { type: String, default: null },
              monto:                { type: Number, default: null },
              adicionales: {
                type: [{
                  nombre: { type: String, default: null },
                  valor:  { type: String, default: null },
                }],
                default: [],
              },
            }],
            default: [],
          },
        }],
        default: [],
      },
      // Monto real aportado por esta CxC a saldoErp — solo se calcula cuando Kore confirma
      // saldoActual===0 (CxC saldada) Y el vínculo fue hecho por un humano (los vínculos de
      // motores automáticos se cierran igual, ver conciliacionFinalizadaAt, pero nunca aportan
      // aquí). Suma solo movimientos con forma de pago bancaria real — transferencia, cheque,
      // depósito en efectivo — nunca formasPago[].monto (ver _montoSaldoLink en erp.routes.js).
      saldoErpAportado: { type: Number, default: null },
      // Checkpoint de conciliación por CxC (job "Sync ERP-Kore"): se marca SOLO cuando Kore
      // confirma que la CxC ya está saldada (saldoActual===0) — ya no hay nada más que Kore
      // pueda reportar para ella, así que deja de reconsultarse. Mientras sea null (CxC
      // todavía abierta), toda corrida futura —manual o el cron diario, sin límite de
      // antigüedad— vuelve a intentarla.
      conciliacionFinalizadaAt: { type: Date, default: null },
      // jobId de la corrida que finalizó este link — permite revertir selectivamente.
      conciliacionRunId: { type: String, default: null },
      // Checkpoint del job "Recalcular saldo ERP" (backfill+recompute unificado, ver
      // erp.routes.js#_recomputeErpKoreJob): se marca cuando este link YA finalizado
      // recibió el backfill de movimientosKore Y (si es humano) la revisión de
      // saldoErpAportado con el criterio de todas las formas de pago — sin importar si
      // hubo cambios. Evita volver a pegarle a Kore por este link en corridas futuras
      // del mismo job. Se limpia al revertir la corrida que lo puso (mismo criterio que
      // conciliacionRunId), para que un link reabierto por el revert vuelva a ser
      // candidato cuando el sync normal lo re-finalice.
      recomputedFormasPagoAt: { type: Date, default: null },
      // Bitácora de auditoría: una entrada por cada forma de pago usada en cada cobro
      // aplicado a esta CxC. Se ACUMULA a través de varios cobros parciales (PPD) sobre
      // la misma CxC — nunca se sobreescribe, solo crece. saldoPagado/saldoPagadoTotal
      // siguen siendo los acumulados rápidos; esto es el detalle que los respalda (de
      // dónde salió cada peso: cuánto en efectivo, transferencia, cheque, etc.).
      desglosePorFormaPago: {
        type: [{
          formaPagoId:          { type: String, default: null },
          formaPagoDescripcion: { type: String, default: null },
          monto:                { type: Number, required: true },
          fecha:                { type: Date, default: Date.now },
        }],
        default: [],
      },
    }],
    default: [],
  },

  // Suma de erpLinks[].saldoErpAportado de los links ya finalizados; null cuando no hay vínculos
  saldoErp: { type: Number, default: null },

  // Última vez que saldoErp se sincronizó contra el ERP externo
  saldoErpSyncedAt: { type: Date, default: null },

  // Roles a los que este movimiento se les oculta (visibilidad selectiva),
  // independiente del flag general `oculto`.
  ocultoRoles: { type: [String], default: [] },

  // Nombre del cliente identificado mediante el catálogo auxiliar
  auxNombre: { type: String, default: null, index: true },

  // Hash de deduplicación: SHA-256 de campos clave, evita duplicados al
  // volver a cargar el mismo archivo.
  hash: { type: String },

  // Historial de usuarios que han relacionado una CxC a este movimiento.
  // Cada entrada representa una asociación (userId + CxC específica).
  // Se añade al vincular una CxC nueva y se elimina al desvincularla.
  identificadoPor: {
    type: [{
      userId:  { type: String, default: null },
      nombre:  { type: String, default: null },
      fechaId: { type: Date,   default: null },
      erpId:   { type: String, default: null },  // CxC que este usuario asoció
      source:  { type: String, default: null },  // 'conciliacion-import' para bulk imports por Excel
      runId:   { type: String, default: null },  // ID único de la operación bulk (permite revert selectivo)
    }],
    default: [],
  },

  // Ficha física (ticket de depósito): folio del comprobante físico
  ficha:       { type: String, default: null },
  fichaBy:     { type: String, default: null },   // userId que registró la ficha
  fichaNombre: { type: String, default: null },   // nombre display del usuario
  fichaAt:     { type: Date,   default: null },

  // Oculto por regla — el movimiento existe pero no aparece en vistas normales
  oculto: { type: Boolean, default: false, index: true },

  // Oculto solo para estos roles (regla 'ocultar' con ocultarRoles). Independiente de `oculto`.
  ocultoRoles: { type: [String], default: [] },

  // Auditoría
  uploadedBy: { type: String, default: null },
  isActive:   { type: Boolean, default: true, index: true },

  // Registro de cambios automáticos durante importación/enriquecimiento.
  // Cada entrada es inmutable: documenta qué se modificó, por qué capa y desde qué archivo.
  _changelog: {
    type: [{
      at:         { type: Date,   required: true },
      via:        { type: String, required: true },  // 'capa1e' | 'enrich-capa1a' | …
      campo:      { type: String, default: null },   // campo modificado, o null si son varios
      campos:     { type: [String], default: [] },   // lista de campos (para enriquecimientos)
      de:         { type: mongoose.Schema.Types.Mixed, default: null },
      a:          { type: mongoose.Schema.Types.Mixed, default: null },
      importFile: { type: String, default: null },
      runId:      { type: String, default: null },   // ID de la corrida bulk que generó este cambio (permite revert selectivo)
      revertedAt: { type: Date,   default: null },    // se marca al revertir, la entrada nunca se borra (rastro de auditoría)
    }],
    default: [],
  },
}, {
  timestamps: true,
  collection: 'bank_movements',
});

// Índice único sobre el hash — garantiza que el mismo movimiento no se duplique
bankMovementSchema.index({ hash:  1 }, { unique: true, sparse: true });
bankMovementSchema.index({ folio: 1 }, { unique: true, sparse: true });

// Índices compuestos para consultas comunes
bankMovementSchema.index({ banco: 1, fecha: -1 });
bankMovementSchema.index({ fecha: -1, banco: 1 });
bankMovementSchema.index({ numeroAutorizacion: 1, banco: 1 });
bankMovementSchema.index({ banco: 1, status: 1 });
bankMovementSchema.index({ banco: 1, categoria: 1 });
// Soporta la agregación de porCategoria en getCards() (bank.service.js), que agrupa por
// banco+categoria SIN acotar por banco en el $match — a diferencia del índice de arriba,
// aquí categoria va primero porque es el campo que el $match sí filtra (excluye null/'').
bankMovementSchema.index({ categoria: 1, isActive: 1 });
// Índices para el motor Match ERP
bankMovementSchema.index({ isActive: 1, status: 1, deposito: 1 });
bankMovementSchema.index({ 'identificadoPor.userId': 1 });
bankMovementSchema.index({ erpIds: 1, isActive: 1 });

// Índice de texto para el buscador
bankMovementSchema.index({
  concepto:           'text',
  numeroAutorizacion: 'text',
  referenciaNumerica: 'text',
}, { default_language: 'spanish' });

module.exports = mongoose.model('BankMovement', bankMovementSchema);
