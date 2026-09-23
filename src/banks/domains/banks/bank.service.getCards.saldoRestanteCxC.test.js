'use strict';

// bank.service.getCards.saldoRestanteCxC.test.js — getCards() (dashboard "Estatus", 2026-09-23):
// los $ de "Identificados"/"Otros"/"Por conciliar" no deben restar retiro (comentario existente,
// bank.service.js línea ~205: estas 4 categorías son solo depósitos), y "Otros"/"Por conciliar"
// deben restar el saldo ya cubierto por una CxC vinculada (saldoErp) igual que "No identificados"
// ya lo hacía — nunca un valor negativo ni el excedente cuando la CxC cubre de más.
//
// Mismo patrón que bank.service.getCards.rangoExplicito.test.js: se mockea BankMovement.aggregate
// (no hay mongodb-memory-server en este proyecto) y se inspecciona la FORMA del pipeline armado —
// getCards() no ejecuta la agregación acá, solo la arma. Los 3 escenarios de negocio concretos
// (parcial/excedente/sin CxC) se prueban aparte con una réplica pura en JS de la expresión, al
// final de este archivo.
jest.mock('./BankMovement.model');
jest.mock('../../shared/socket');
jest.mock('./drive-fichas.service');
jest.mock('./repositories/bank-config.repository');

const BankMovement   = require('./BankMovement.model');
const bankConfigRepo = require('./repositories/bank-config.repository');
const { getCards }   = require('./bank.service');

describe('getCards() — saldoIdentificado/saldoOtrosSolo/saldoReclasificado', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    BankMovement.aggregate.mockResolvedValue([]);
    bankConfigRepo.findAllAsMap.mockResolvedValue(new Map());
  });

  function gruposDeAmbasAgregaciones() {
    const calls = BankMovement.aggregate.mock.calls;
    // Llamada 0 = agregación por banco, llamada 1 = agregación por banco+categoría (Promise.all
    // en getCards()) — ambas traen un $group entre sus stages, en distinta posición cada una.
    const grupoPorBanco     = calls[0][0].find(stage => stage.$group).$group;
    const grupoPorCategoria = calls[1][0].find(stage => stage.$group).$group;
    return { grupoPorBanco, grupoPorCategoria };
  }

  test('saldoIdentificado ya no resta retiro, en ninguna de las 2 agregaciones', async () => {
    await getCards(null);
    const { grupoPorBanco, grupoPorCategoria } = gruposDeAmbasAgregaciones();

    for (const grupo of [grupoPorBanco, grupoPorCategoria]) {
      const [, montoSiIdentificado] = grupo.saldoIdentificado.$sum.$cond;
      expect(montoSiIdentificado).toEqual({ $ifNull: ['$deposito', 0] });
    }
  });

  test('saldoOtrosSolo y saldoReclasificado reusan la MISMA expresión que saldoPendiente (DRY + mismo comportamiento), en las 2 agregaciones', async () => {
    await getCards(null);
    const { grupoPorBanco, grupoPorCategoria } = gruposDeAmbasAgregaciones();

    for (const grupo of [grupoPorBanco, grupoPorCategoria]) {
      const [, saldoRestanteCxC]      = grupo.saldoPendiente.$sum.$cond;
      const [, montoSiOtros]          = grupo.saldoOtrosSolo.$sum.$cond;
      const [, montoSiReclasificado]  = grupo.saldoReclasificado.$sum.$cond;

      // Misma referencia de objeto (la constante se define una sola vez en getCards()) —
      // garantiza que los 3 buckets están matemáticamente sincronizados sin repetir la fórmula.
      expect(montoSiOtros).toBe(saldoRestanteCxC);
      expect(montoSiReclasificado).toBe(saldoRestanteCxC);

      // Forma esperada de la expresión — con saldoErp vinculado, max(0, deposito - saldoErp);
      // sin saldoErp, el depósito completo. Nunca resta retiro.
      expect(saldoRestanteCxC).toEqual({
        $cond: [
          { $ne: ['$saldoErp', null] },
          { $max: [0, { $subtract: [{ $ifNull: ['$deposito', 0] }, '$saldoErp'] }] },
          { $ifNull: ['$deposito', 0] },
        ],
      });
    }
  });

  test('regresión: saldoOtros combinado (otros+reclasificado) sigue intacto, sin tocar', async () => {
    await getCards(null);
    const { grupoPorBanco } = gruposDeAmbasAgregaciones();
    // Solo existe en la agregación por banco — código muerto en el frontend hoy (no se toca en
    // este fix), esto solo confirma que no se eliminó/alteró por error.
    expect(grupoPorBanco.saldoOtros.$sum.$cond[1]).toEqual({
      $subtract: [{ $ifNull: ['$deposito', 0] }, { $ifNull: ['$retiro', 0] }],
    });
  });
});

// ── Réplica pura en JS de saldoRestanteCxC — prueba los 3 escenarios de negocio concretos ──
// (los tests de arriba no ejecutan la agregación contra datos reales, así que esto es lo que
// verifica los NÚMEROS: $1000 con CxC de $900 → $100 restante; $1000 con CxC de $1500 → $0
// (nunca negativo ni el excedente); $1000 sin CxC vinculada → $1000 completos.)
function saldoRestanteCxCReplica(deposito, saldoErp) {
  const dep = deposito ?? 0;
  if (saldoErp != null) return Math.max(0, dep - saldoErp);
  return dep;
}

describe('saldoRestanteCxC — semántica de negocio (réplica pura, sin Mongo)', () => {
  test('CxC parcial: solo cuenta la diferencia restante (depósito 1000, CxC 900 → 100)', () => {
    expect(saldoRestanteCxCReplica(1000, 900)).toBe(100);
  });

  test('CxC igual o mayor al depósito: cuenta 0, nunca negativo ni el excedente (depósito 1000, CxC 1500 → 0)', () => {
    expect(saldoRestanteCxCReplica(1000, 1500)).toBe(0);
    expect(saldoRestanteCxCReplica(1000, 1000)).toBe(0);
  });

  test('sin CxC vinculada (saldoErp null): cuenta el depósito completo', () => {
    expect(saldoRestanteCxCReplica(1000, null)).toBe(1000);
  });
});
