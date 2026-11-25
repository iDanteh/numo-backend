'use strict';

jest.mock('./ClienteCodigo.model');
jest.mock('./BankMovementClienteCodigoMatch.model');
jest.mock('../banks/BankMovement.model');

const ClienteCodigo = require('./ClienteCodigo.model');
const BankMovementClienteCodigoMatch = require('./BankMovementClienteCodigoMatch.model');
const BankMovement = require('../banks/BankMovement.model');
const {
  importarCatalogo,
  actualizarMatches,
  listarMatchesPersistidos,
  buscarMatches,
  _textoAisladoPorBanco,
  _candidatosConfiables,
  _clasificarConfianza,
} = require('./clientes-codigo.service');
const { construirCodigoConDigito } = require('../../shared/utils/codigo-cliente-digito-verificador');

function mockBankMovementFind(docs) {
  const lean = jest.fn().mockResolvedValue(docs);
  const select = jest.fn().mockReturnValue({ lean });
  BankMovement.find.mockReturnValue({ select });
}

function mockClienteCodigoFind(docs) {
  ClienteCodigo.find.mockReturnValue({ lean: jest.fn().mockResolvedValue(docs) });
}

// Default "sin nada que hacer" para los mocks que importarCatalogo() cascadea hacia
// actualizarMatches() — cada test de actualizarMatches/listarMatchesPersistidos pisa lo que
// necesite con valores concretos.
beforeEach(() => {
  jest.clearAllMocks();
  BankMovementClienteCodigoMatch.distinct.mockResolvedValue([]);
  mockBankMovementFind([]);
  mockClienteCodigoFind([]);
});

describe('_textoAisladoPorBanco', () => {
  test('Banamex: aisla el texto entre "Concepto del Pago:" y el siguiente "|"', () => {
    const concepto = 'ABONO INTERBANCARIO | Concepto del Pago: pago cliente 24788 | Referencia numérica: D INT 0010203';
    expect(_textoAisladoPorBanco('Banamex', concepto)).toBe('pago cliente 24788');
  });

  test('Banamex: sin "Concepto del Pago:" no es determinable (ej. depósito de venta con tarjeta)', () => {
    const concepto = 'DEPOSITO VENTAS NETAS D TAR POR EVOPAYMX | Referencia numérica: 0004100739';
    expect(_textoAisladoPorBanco('Banamex', concepto)).toBeNull();
  });

  test('Santander: aisla el texto después del "|" y recorta el numero de ruta pegado al final', () => {
    const concepto = 'ABONO TRANSFERENCIA SPEI | PAGO AMAURY CIRIGO SANCHEZ               072614011428705510';
    expect(_textoAisladoPorBanco('Santander', concepto)).toBe('PAGO AMAURY CIRIGO SANCHEZ');
  });

  test('Santander: sin numero de ruta pegado, no recorta nada', () => {
    const concepto = 'ABONO TRANSFERENCIA ENLACE | A0251204380 pavel Garcia Orozco';
    expect(_textoAisladoPorBanco('Santander', concepto)).toBe('A0251204380 pavel Garcia Orozco');
  });

  test('BBVA y Azteca nunca son determinables (texto libre pegado sin delimitador)', () => {
    expect(_textoAisladoPorBanco('BBVA', 'SPEI RECIBIDOBANORTE / 0114237087 072 0020126PAGO')).toBeNull();
    expect(_textoAisladoPorBanco('Azteca', 'TRASPASO 01801301199558')).toBeNull();
  });

  test('concepto vacío/null nunca es determinable', () => {
    expect(_textoAisladoPorBanco('Banamex', null)).toBeNull();
    expect(_textoAisladoPorBanco('Banamex', '')).toBeNull();
  });
});

describe('_clasificarConfianza', () => {
  test('código de 5+ dígitos sin dígito verificador siguiente: confianza media', () => {
    // "pago cliente 24788" — 24788 está al final, no hay un dígito más después, no puede
    // validar dígito verificador.
    expect(_clasificarConfianza(24788, 'pago cliente 24788', 13)).toBe('media');
  });

  test('código de 4 dígitos o menos: confianza baja', () => {
    expect(_clasificarConfianza(40, 'pago 40', 5)).toBe('baja');
    expect(_clasificarConfianza(1232, 'ref 1232', 4)).toBe('baja');
  });

  test('código seguido de un dígito que SÍ valida como dígito verificador mod-11: confianza alta', () => {
    const codigo = 40;
    const conDigito = construirCodigoConDigito(codigo); // ej. "406"
    const texto = `pago ${conDigito} ok`;
    expect(_clasificarConfianza(codigo, texto, 5)).toBe('alta');
  });

  test('código seguido de un dígito que NO valida como dígito verificador: no sube a alta', () => {
    const codigo = 40;
    const real = construirCodigoConDigito(codigo);
    const digitoReal = Number(real.slice(-1));
    const digitoFalso = (digitoReal + 1) % 10;
    const texto = `pago ${codigo}${digitoFalso} ok`;
    expect(_clasificarConfianza(codigo, texto, 5)).toBe('baja'); // 40 tiene 2 dígitos → baja, no alta
  });
});

describe('_candidatosConfiables — filtros de exclusión validados sobre el Excel real', () => {
  const codigosSet = new Set([24788, 2979, 12171, 40]);

  test('caso limpio: código de 5 dígitos sin contexto de ruido sobrevive', () => {
    const candidatos = _candidatosConfiables('pago cliente 24788', codigosSet);
    expect(candidatos).toEqual([{ codigo: 24788, confianza: 'media' }]);
  });

  test('excluido por palabra de contexto FOLIO (regla 3)', () => {
    // mismo caso que se encontró como falso positivo en el Excel real: "folio 2979"
    expect(_candidatosConfiables('folio 2979', codigosSet)).toEqual([]);
  });

  test('excluido por prefijo tipo serie-folio ERP pegado antes del número (regla 1)', () => {
    // "O0-" es un prefijo real de Serie_Folio (ver Serie_Folio del concentrado de depósitos),
    // sin ninguna palabra de OC/cotización para no solapar con la regla 3.
    expect(_candidatosConfiables('venta O0-12171', codigosSet)).toEqual([]);
  });

  test('excluido por palabra de cantidad/unidad pegada después del número (regla 2)', () => {
    expect(_candidatosConfiables('pago 40 acrilicos', codigosSet)).toEqual([]);
  });

  test('ambiguo: 2 códigos distintos en el mismo texto (no es responsabilidad de esta función resolverlo, buscarMatches lo descarta)', () => {
    const candidatos = _candidatosConfiables('pago cliente 24788 ref 2979', codigosSet);
    expect(candidatos.map((c) => c.codigo).sort((a, b) => a - b)).toEqual([2979, 24788]);
  });

  // LIMITACIÓN ACEPTADA, documentada a propósito (no es un bug): las 3 reglas de exclusión
  // no cubren abreviaturas de unidad pegadas sin espacio ("40M" por "40 metros") ni nombres
  // de producto/marca que casualmente contienen el código ("WD-40"). Por diseño, un código de
  // 4 dígitos o menos queda clasificado 'baja' precisamente porque sigue teniendo ruido
  // residual — la mitigación real es el dígito verificador (ver mod-11), no esta regex. Reportado
  // al usuario: estos 2 ejemplos reales del Excel SIGUEN matcheando (confianza 'baja'), las 3
  // reglas no los excluyen.
  test('limitación conocida: "CORDON USO RUDO 40M" y "Niasabond y aceite DW 40" NO quedan excluidos por las 3 reglas (quedan en confianza baja)', () => {
    expect(_candidatosConfiables('CORDON USO RUDO 40M', codigosSet)).toEqual([{ codigo: 40, confianza: 'baja' }]);
    expect(_candidatosConfiables('Niasabond y aceite DW 40', codigosSet)).toEqual([{ codigo: 40, confianza: 'baja' }]);
  });

  test('número que no está en el catálogo nunca es un candidato', () => {
    expect(_candidatosConfiables('pago 99999999', codigosSet)).toEqual([]);
  });
});

describe('importarCatalogo', () => {
  test('arma un bulkWrite de upserts por codigo, acumulativo (nunca borra), y dispara actualizarMatches', async () => {
    ClienteCodigo.bulkWrite.mockResolvedValue({ upsertedCount: 2, modifiedCount: 1 });
    // sin movimientos nuevos que matchear (mocks por defecto del beforeEach) -> matchesNuevos: 0

    const resultado = await importarCatalogo([
      { codigo: 1, nombre: 'Cliente Uno', rfc: null },
      { codigo: 2, nombre: 'Cliente Dos', rfc: 'ABC123' },
    ]);

    expect(ClienteCodigo.bulkWrite).toHaveBeenCalledTimes(1);
    const ops = ClienteCodigo.bulkWrite.mock.calls[0][0];
    expect(ops).toHaveLength(2);
    expect(ops[0].updateOne.filter).toEqual({ codigo: 1 });
    expect(ops[0].updateOne.upsert).toBe(true);
    expect(resultado).toEqual({ importados: 2, creados: 2, actualizados: 1, matchesNuevos: 0 });
  });

  test('lista vacía no llama a bulkWrite ni a actualizarMatches', async () => {
    const resultado = await importarCatalogo([]);
    expect(ClienteCodigo.bulkWrite).not.toHaveBeenCalled();
    expect(BankMovementClienteCodigoMatch.distinct).not.toHaveBeenCalled();
    expect(resultado).toEqual({ importados: 0, creados: 0, actualizados: 0, matchesNuevos: 0 });
  });
});

describe('actualizarMatches — incremental, nunca reevalúa lo ya persistido', () => {
  test('solo busca BankMovement sin match persistido ($nin sobre bankMovementId ya distintos)', async () => {
    BankMovementClienteCodigoMatch.distinct.mockResolvedValue(['mov-ya-matcheado-1']);
    mockClienteCodigoFind([{ codigo: 24788, nombre: 'ELSA MANUEL BASILIO' }]);
    mockBankMovementFind([
      { _id: 'mov-nuevo-1', banco: 'Banamex', fecha: new Date('2026-01-05'), concepto: 'X | Concepto del Pago: pago cliente 24788 | Referencia numérica: D INT 1', deposito: 500 },
    ]);
    BankMovementClienteCodigoMatch.insertMany.mockResolvedValue([]);

    const resultado = await actualizarMatches();

    expect(BankMovementClienteCodigoMatch.distinct).toHaveBeenCalledWith('bankMovementId');
    const [filtro] = BankMovement.find.mock.calls[0];
    expect(filtro._id.$nin).toEqual(['mov-ya-matcheado-1']);
    expect(filtro.banco.$in).toEqual(['Banamex', 'Santander']);

    expect(BankMovementClienteCodigoMatch.insertMany).toHaveBeenCalledWith([
      { bankMovementId: 'mov-nuevo-1', codigo: 24788, confianza: 'media', banco: 'Banamex', fecha: new Date('2026-01-05'), deposito: 500 },
    ]);
    expect(resultado).toEqual({ matchesNuevos: 1 });
  });

  test('descarta sin persistir nada un movimiento ambiguo (2+ codigos) o no determinable', async () => {
    mockClienteCodigoFind([{ codigo: 24788 }, { codigo: 2979 }]);
    mockBankMovementFind([
      { _id: 'mov-ambiguo', banco: 'Banamex', fecha: new Date(), concepto: 'X | Concepto del Pago: pago cliente 24788 ref 2979 | Referencia numérica: D INT 1', deposito: 100 },
      { _id: 'mov-bbva', banco: 'BBVA', fecha: new Date(), concepto: 'SPEI RECIBIDOBANORTE / 0114237087 072 0020126PAGO', deposito: 200 },
    ]);

    const resultado = await actualizarMatches();

    expect(BankMovementClienteCodigoMatch.insertMany).not.toHaveBeenCalled();
    expect(resultado).toEqual({ matchesNuevos: 0 });
  });

  test('sin movimientos nuevos: no llama insertMany', async () => {
    const resultado = await actualizarMatches();
    expect(BankMovementClienteCodigoMatch.insertMany).not.toHaveBeenCalled();
    expect(resultado).toEqual({ matchesNuevos: 0 });
  });
});

describe('listarMatchesPersistidos — lee lo ya guardado, resuelve nombre/concepto al momento de leer', () => {
  test('junta BankMovementClienteCodigoMatch + concepto de BankMovement + nombre ACTUAL del catálogo', async () => {
    BankMovementClienteCodigoMatch.find.mockReturnValue({
      select: jest.fn().mockReturnValue({ lean: jest.fn().mockResolvedValue([
        { bankMovementId: 'mov-1', codigo: 24788, confianza: 'media', banco: 'Banamex', fecha: new Date('2026-01-05'), deposito: 500 },
      ]) }),
    });
    mockBankMovementFind([
      { _id: 'mov-1', concepto: 'X | Concepto del Pago: pago cliente 24788 | Referencia numérica: D INT 1' },
    ]);
    mockClienteCodigoFind([{ codigo: 24788, nombre: 'ELSA MANUEL BASILIO (nombre actualizado)' }]);

    const matches = await listarMatchesPersistidos({ banco: null });

    expect(matches).toEqual([{
      fecha: new Date('2026-01-05'),
      banco: 'Banamex',
      concepto: 'X | Concepto del Pago: pago cliente 24788 | Referencia numérica: D INT 1',
      textoAislado: 'pago cliente 24788',
      codigo: 24788,
      nombreCliente: 'ELSA MANUEL BASILIO (nombre actualizado)', // viene del catálogo HOY, no denormalizado
      deposito: 500,
      confianza: 'media',
    }]);
  });

  test('sin matches persistidos: devuelve array vacío sin consultar BankMovement/ClienteCodigo', async () => {
    BankMovementClienteCodigoMatch.find.mockReturnValue({ select: jest.fn().mockReturnValue({ lean: jest.fn().mockResolvedValue([]) }) });

    const matches = await listarMatchesPersistidos({});

    expect(matches).toEqual([]);
    expect(BankMovement.find).not.toHaveBeenCalled();
  });

  test('aplica filtro de banco/fechas sobre la colección de matches', async () => {
    BankMovementClienteCodigoMatch.find.mockReturnValue({ select: jest.fn().mockReturnValue({ lean: jest.fn().mockResolvedValue([]) }) });

    await listarMatchesPersistidos({ banco: 'Santander', fechaInicio: '2026-01-01', fechaFin: '2026-01-31' });

    const [filtro] = BankMovementClienteCodigoMatch.find.mock.calls[0];
    expect(filtro.banco).toBe('Santander');
    expect(filtro.fecha.$gte).toEqual(new Date('2026-01-01'));
    expect(filtro.fecha.$lte).toEqual(new Date('2026-01-31'));
  });
});

describe('buscarMatches', () => {
  test('descarta BBVA/Azteca (no determinables), mantiene Banamex/Santander, resuelve nombre del catálogo', async () => {
    ClienteCodigo.find.mockReturnValue({ lean: jest.fn().mockResolvedValue([{ codigo: 24788, nombre: 'ELSA MANUEL BASILIO' }]) });
    mockBankMovementFind([
      { banco: 'Banamex', fecha: new Date('2026-01-05'), concepto: 'X | Concepto del Pago: pago cliente 24788 | Referencia numérica: D INT 1', deposito: 500 },
    ]);

    const matches = await buscarMatches({ banco: null });

    expect(matches).toHaveLength(1);
    expect(matches[0]).toMatchObject({ codigo: 24788, nombreCliente: 'ELSA MANUEL BASILIO', confianza: 'media', deposito: 500 });
  });

  test('descarta un depósito ambiguo (matchea 2 códigos distintos)', async () => {
    ClienteCodigo.find.mockReturnValue({ lean: jest.fn().mockResolvedValue([{ codigo: 24788 }, { codigo: 2979 }]) });
    mockBankMovementFind([
      { banco: 'Banamex', fecha: new Date(), concepto: 'X | Concepto del Pago: pago cliente 24788 ref 2979 | Referencia numérica: D INT 1', deposito: 100 },
    ]);

    const matches = await buscarMatches({});
    expect(matches).toEqual([]);
  });

  test('banco=BBVA explícito: no determinable, devuelve vacío sin consultar BankMovement', async () => {
    ClienteCodigo.find.mockReturnValue({ lean: jest.fn().mockResolvedValue([]) });
    const matches = await buscarMatches({ banco: 'BBVA' });
    expect(matches).toEqual([]);
    expect(BankMovement.find).not.toHaveBeenCalled();
  });
});
