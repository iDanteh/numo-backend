'use strict';

/**
 * domains/ledger/loyalty-kore-reversion-abono.service.test.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — reversión de puntos de UN abono puntual, disparada desde
 * `cxc-reversiones` (ver design.md del cambio `cxc-reversiones-puntos-abono`).
 *
 * `_aplicarMovimiento` (el único escritor del ledger, `loyalty-ledger.service.js`)
 * se mockea como un LÍMITE — ya tiene su propia cobertura completa en
 * `loyalty-ledger.service.test.js` (escritura real, lock de fila, actualización de
 * saldo). Lo que se prueba ACÁ es la lógica nueva: el agregado del `serieFolio`
 * completo (D2), el clamp del guard, la idempotencia y los no-op silenciosos — no
 * la mecánica de escritura en sí, que ya está cubierta.
 */

const { Op } = require('sequelize');

jest.mock('../../../shared/models/postgres', () => ({
  LoyaltyCard:         { findByPk: jest.fn() },
  LoyaltyPointsLedger: { findAll: jest.fn() },
}));
jest.mock('../../../config/database.postgres', () => ({
  sequelize: { transaction: jest.fn() },
}));
jest.mock('./loyalty-ledger.service', () => ({
  _aplicarMovimiento: jest.fn(),
}));
jest.mock('../../../banks/shared/socket', () => ({
  emitToLoyaltyCard: jest.fn(),
}));

const { LoyaltyCard, LoyaltyPointsLedger } = require('../../../shared/models/postgres');
const { sequelize } = require('../../../config/database.postgres');
const { _aplicarMovimiento } = require('./loyalty-ledger.service');
const { emitToLoyaltyCard } = require('../../../banks/shared/socket');
const { revertirPuntosPorAbono } = require('./loyalty-kore-reversion-abono.service');

const SERIE_FOLIO = 'A0-260605320';
const ID_ABONO = 'abono-1';

/** Fila cruda del ledger (plain object — mismo criterio que loyalty-ledger.service.test.js). */
function fila({ referenciaExterna, puntos, cardId = 1 }) {
  return { origen: 'kore', referenciaExterna, puntos, cardId };
}

beforeEach(() => {
  jest.clearAllMocks();
  // Mismo patrón que loyalty-ledger.service.test.js: sequelize.transaction invoca el
  // callback con una transacción falsa.
  sequelize.transaction.mockImplementation(async (cb) => cb({ fake: true }));
  LoyaltyCard.findByPk.mockResolvedValue({ id: 1, activa: true });
  _aplicarMovimiento.mockImplementation(async (t, card, mov) => ({ ...mov, id: 999 }));
});

describe('revertirPuntosPorAbono', () => {
  test('abono con solo ACUMULACION revierte exactamente ese monto', async () => {
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      fila({ referenciaExterna: `${SERIE_FOLIO}:${ID_ABONO}:ACUMULACION`, puntos: '50.00' }),
    ]);

    const resultado = await revertirPuntosPorAbono({ serieFolio: SERIE_FOLIO, idAbono: ID_ABONO, motivo: 'Primer reversión' });

    expect(resultado).toEqual({ reverted: true, puntosRevertidos: '-50.00', cardId: 1 });
    expect(_aplicarMovimiento).toHaveBeenCalledTimes(1);
    const [, , mov] = _aplicarMovimiento.mock.calls[0];
    expect(mov).toMatchObject({
      tipo: 'AJUSTE',
      puntos: '-50.00',
      origen: 'kore',
      referenciaExterna: `${SERIE_FOLIO}:${ID_ABONO}:REVERSION`,
    });
    expect(mov.justificacion).toContain(ID_ABONO);
    expect(mov.justificacion).toContain('Primer reversión');
    expect(emitToLoyaltyCard).toHaveBeenCalledWith(1, 'lealtad-tarjeta:actualizada', expect.objectContaining({ cardId: 1 }));
  });

  test('abono con ACUMULACION+CANJE+PROMOCION (3 filas) revierte el NETO en una sola fila AJUSTE', async () => {
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      fila({ referenciaExterna: `${SERIE_FOLIO}:${ID_ABONO}:ACUMULACION`, puntos: '50.00' }),
      fila({ referenciaExterna: `${SERIE_FOLIO}:${ID_ABONO}:PROMOCION`, puntos: '10.00' }),
      fila({ referenciaExterna: `${SERIE_FOLIO}:${ID_ABONO}:CANJE`, puntos: '-20.00' }),
    ]);

    const resultado = await revertirPuntosPorAbono({ serieFolio: SERIE_FOLIO, idAbono: ID_ABONO });

    // Neto: 50 + 10 - 20 = 40 -> se revierte -40.00 en UNA sola llamada.
    expect(resultado).toEqual({ reverted: true, puntosRevertidos: '-40.00', cardId: 1 });
    expect(_aplicarMovimiento).toHaveBeenCalledTimes(1);
  });

  test('abono sin NINGUNA fila de lealtad asociada -> no-op, cero llamadas a _aplicarMovimiento', async () => {
    LoyaltyPointsLedger.findAll.mockResolvedValue([]);

    const resultado = await revertirPuntosPorAbono({ serieFolio: SERIE_FOLIO, idAbono: ID_ABONO });

    expect(resultado).toEqual({ reverted: false });
    expect(_aplicarMovimiento).not.toHaveBeenCalled();
  });

  test('abono ya revertido antes (fila :REVERSION ya existe) -> no-op, idempotente', async () => {
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      fila({ referenciaExterna: `${SERIE_FOLIO}:${ID_ABONO}:ACUMULACION`, puntos: '50.00' }),
      fila({ referenciaExterna: `${SERIE_FOLIO}:${ID_ABONO}:REVERSION`, puntos: '-50.00' }),
    ]);

    const resultado = await revertirPuntosPorAbono({ serieFolio: SERIE_FOLIO, idAbono: ID_ABONO });

    expect(resultado).toEqual({ reverted: false });
    expect(_aplicarMovimiento).not.toHaveBeenCalled();
  });

  test('guard D2: un retiro TOTAL previo de venta-retiro ya cubrió el abono -> no-op (magnitud=0)', async () => {
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      fila({ referenciaExterna: `${SERIE_FOLIO}:${ID_ABONO}:ACUMULACION`, puntos: '50.00' }),
      // Reversión de venta-retiro: SIN idAbono (invisible para cualquier query filtrada por
      // idAbono) — cubre el neto completo del serieFolio.
      fila({ referenciaExterna: `${SERIE_FOLIO}:REVERSION:folio-retiro-9`, puntos: '-50.00' }),
    ]);

    const resultado = await revertirPuntosPorAbono({ serieFolio: SERIE_FOLIO, idAbono: ID_ABONO });

    expect(resultado).toEqual({ reverted: false });
    expect(_aplicarMovimiento).not.toHaveBeenCalled();
  });

  test('guard D2 parcial: un retiro PARCIAL previo deja remanente -> reversión clamped al remanente', async () => {
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      fila({ referenciaExterna: `${SERIE_FOLIO}:${ID_ABONO}:ACUMULACION`, puntos: '50.00' }),
      // venta-retiro ya revirtió 30 de los 50 totales del serieFolio (otro abono, o un
      // ajuste parcial) -> solo quedan 20 de remanente real.
      fila({ referenciaExterna: `${SERIE_FOLIO}:REVERSION:folio-retiro-9`, puntos: '-30.00' }),
    ]);

    const resultado = await revertirPuntosPorAbono({ serieFolio: SERIE_FOLIO, idAbono: ID_ABONO });

    expect(resultado).toEqual({ reverted: true, puntosRevertidos: '-20.00', cardId: 1 });
    const [, , mov] = _aplicarMovimiento.mock.calls[0];
    expect(mov.puntos).toBe('-20.00');
  });

  test('carrera concurrente (SequelizeUniqueConstraintError) -> se traga el error, {reverted:false}', async () => {
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      fila({ referenciaExterna: `${SERIE_FOLIO}:${ID_ABONO}:ACUMULACION`, puntos: '50.00' }),
    ]);
    const dupErr = new Error('duplicate');
    dupErr.name = 'SequelizeUniqueConstraintError';
    sequelize.transaction.mockRejectedValueOnce(dupErr);

    const resultado = await revertirPuntosPorAbono({ serieFolio: SERIE_FOLIO, idAbono: ID_ABONO });

    expect(resultado).toEqual({ reverted: false });
  });

  test('tarjeta no encontrada dentro de la transacción -> {reverted:false}, nunca lanza', async () => {
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      fila({ referenciaExterna: `${SERIE_FOLIO}:${ID_ABONO}:ACUMULACION`, puntos: '50.00' }),
    ]);
    LoyaltyCard.findByPk.mockResolvedValue(null);

    const resultado = await revertirPuntosPorAbono({ serieFolio: SERIE_FOLIO, idAbono: ID_ABONO });

    expect(resultado).toEqual({ reverted: false });
    expect(_aplicarMovimiento).not.toHaveBeenCalled();
  });

  test('error inesperado (no de carrera) dentro de la transacción SÍ se propaga — lo maneja el caller (D6)', async () => {
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      fila({ referenciaExterna: `${SERIE_FOLIO}:${ID_ABONO}:ACUMULACION`, puntos: '50.00' }),
    ]);
    sequelize.transaction.mockRejectedValueOnce(new Error('DB caída'));

    await expect(
      revertirPuntosPorAbono({ serieFolio: SERIE_FOLIO, idAbono: ID_ABONO }),
    ).rejects.toThrow('DB caída');
  });
});

// Confirma que la búsqueda usa el prefijo LIKE correcto (origen:'kore', serieFolio completo) —
// sin esto, un test que mockea findAll sin verificar el WHERE podría pasar aunque la query real
// buscara en el lugar equivocado.
test('consulta LoyaltyPointsLedger con origen=kore y referenciaExterna LIKE "{serieFolio}:%"', async () => {
  LoyaltyPointsLedger.findAll.mockResolvedValue([]);

  await revertirPuntosPorAbono({ serieFolio: SERIE_FOLIO, idAbono: ID_ABONO });

  expect(LoyaltyPointsLedger.findAll).toHaveBeenCalledWith({
    where: { origen: 'kore', referenciaExterna: { [Op.like]: `${SERIE_FOLIO}:%` } },
  });
});
