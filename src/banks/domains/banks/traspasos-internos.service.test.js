'use strict';

// traspasos-internos.service.test.js — mismo patrón de mock que
// bank-autorizaciones.service.matchDesdeErp.test.js: se mockea BankMovement.model
// completo (no hay mongodb-memory-server en este proyecto — confirmado por grep) y se
// inspeccionan/simulan los pipeline updates.
//
// Para los casos que requieren verificar el ESTADO FINAL del documento tras un pipeline
// update ($set con $ifNull/$concatArrays/$filter/$cond — el mismo patrón que
// importarConciliacion/revertirConciliacion en bank.service.js), se usa un motor "fake
// mongo" mínimo (solo los operadores que este servicio realmente usa) que aplica esos
// pipelines contra un array de documentos en memoria. Esto da cobertura real de la
// lógica de negocio (no solo la forma del objeto pasado a bulkWrite/updateMany).
//
// El banco contraparte ya NO es fijo a Banamex (corrección 2026-08-14) — se extrae del
// concepto BBVA con /RECIBIDO\s*([A-Za-zÁÉÍÓÚÑáéíóúñ]+)/i. Por eso `BankMovement.schema`
// se mockea a mano con los enumValues reales de `banco` (jest.mock automock no reproduce
// de forma confiable los métodos de instancia de un mongoose.Schema real).
jest.mock('./BankMovement.model');

const BankMovement = require('./BankMovement.model');
const { TODOS_MOTORES_HISTORICO } = require('./bank.service');
const {
  matchTraspasosInternos, revertirTraspasosInternos,
  generarPolizaContpaqTraspasos, generarPolizasContpaqTraspasosPorRango,
} = require('./traspasos-internos.service');
const ExcelJS = require('exceljs');
const AdmZip  = require('adm-zip');

const BANCO_ENUM = [
  'Banamex', 'BBVA', 'Santander', 'Azteca',
  'Banorte', 'HSBC', 'Inbursa', 'Scotiabank',
  'BanBajío', 'Afirme', 'Intercam', 'Nu',
  'Spin', 'Hey Banco', 'Albo',
];
BankMovement.schema = { path: () => ({ enumValues: BANCO_ENUM }) };

const CAT = 'Traspaso entre cuentas propias';
const USER = { _id: 'user-1', nombre: 'Usuario Uno' };

// ── Fake Mongo — solo los operadores realmente usados por traspasos-internos.service.js ──
function getPath(obj, path) {
  return path.split('.').reduce((o, k) => (o == null ? undefined : o[k]), obj);
}

function matchFilter(doc, filter) {
  return Object.entries(filter).every(([key, cond]) => matchField(getPath(doc, key), cond));
}

function matchField(val, cond) {
  // RegExp como filtro (ej. RE_CATEGORIA_TRASPASO_INTERNO) — mismo comportamiento que
  // Mongo/Mongoose real (find({campo: /regex/i})), sin esto Object.entries(regex) da []
  // y el filtro matchearía cualquier cosa por error.
  if (cond instanceof RegExp) return typeof val === 'string' && cond.test(val);
  if (cond !== null && typeof cond === 'object' && !Array.isArray(cond) && !(cond instanceof Date)) {
    return Object.entries(cond).every(([op, arg]) => {
      if (op === '$in') return arg.includes(val);
      if (op === '$gt') return (val ?? 0) > arg;
      if (op === '$size') return Array.isArray(val) && val.length === arg;
      if (op === '$elemMatch') return Array.isArray(val) && val.some(item => matchFilter(item, arg));
      throw new Error(`op no soportado en fake mongo (filter): ${op}`);
    });
  }
  return val === cond;
}

function resolveExpr(val, doc, vars) {
  if (val === null || val === undefined || val instanceof Date) return val;
  if (typeof val === 'string') {
    if (val.startsWith('$$')) {
      const v = getPath(vars, val.slice(2));
      return v === undefined ? null : v;
    }
    if (val.startsWith('$')) {
      const v = getPath(doc, val.slice(1));
      return v === undefined ? null : v;
    }
    return val;
  }
  if (Array.isArray(val)) return val.map(v => resolveExpr(v, doc, vars));
  if (typeof val === 'object') {
    const keys = Object.keys(val);
    if (keys.length === 1 && keys[0].startsWith('$')) {
      const op = keys[0];
      const arg = val[op];
      switch (op) {
        case '$ifNull': {
          const a = resolveExpr(arg[0], doc, vars);
          return (a === null || a === undefined) ? resolveExpr(arg[1], doc, vars) : a;
        }
        case '$concatArrays':
          return arg.reduce((acc, e) => acc.concat(resolveExpr(e, doc, vars)), []);
        case '$filter': {
          const input = resolveExpr(arg.input, doc, vars) || [];
          return input.filter(item => !!resolveExpr(arg.cond, doc, { ...vars, [arg.as]: item }));
        }
        case '$size':
          return (resolveExpr(arg, doc, vars) || []).length;
        case '$in': {
          const [needle, haystack] = arg.map(a => resolveExpr(a, doc, vars));
          return (haystack || []).some(h => JSON.stringify(h) === JSON.stringify(needle));
        }
        case '$not': {
          const inner = Array.isArray(arg) ? arg[0] : arg;
          return !resolveExpr(inner, doc, vars);
        }
        case '$and':
          return arg.every(e => !!resolveExpr(e, doc, vars));
        case '$eq': {
          const [a, b] = arg.map(e => resolveExpr(e, doc, vars));
          return JSON.stringify(a) === JSON.stringify(b);
        }
        case '$cond': {
          const condVal = resolveExpr(arg.if, doc, vars);
          return condVal ? resolveExpr(arg.then, doc, vars) : resolveExpr(arg.else, doc, vars);
        }
        default:
          throw new Error(`operador no soportado en fake mongo (expr): ${op}`);
      }
    }
    const out = {};
    for (const k of keys) out[k] = resolveExpr(val[k], doc, vars);
    return out;
  }
  return val;
}

// Las expresiones de un mismo $set leen el doc PRE-stage (no los valores hermanos ya
// actualizados en la misma pasada) — mismo comportamiento real de Mongo, documentado
// explícitamente en revertirConciliacion (bank.service.js).
function applyPipeline(doc, pipeline) {
  let current = { ...doc };
  for (const stage of pipeline) {
    if (stage.$set) {
      const base = current;
      const updates = {};
      for (const [k, expr] of Object.entries(stage.$set)) {
        updates[k] = resolveExpr(expr, base, {});
      }
      current = { ...current, ...updates };
    }
  }
  return current;
}

class FakeCollection {
  constructor(docs) { this.docs = docs; }

  find(filter) {
    const results = this.docs.filter(d => matchFilter(d, filter));
    const q = {};
    q.select = jest.fn(() => q);
    q.lean   = jest.fn().mockResolvedValue(results);
    return q;
  }

  async bulkWrite(ops) {
    let modifiedCount = 0;
    for (const { updateOne } of ops) {
      const idx = this.docs.findIndex(d => matchFilter(d, updateOne.filter));
      if (idx === -1) continue;
      this.docs[idx] = applyPipeline(this.docs[idx], updateOne.update);
      modifiedCount++;
    }
    return { modifiedCount };
  }

  async updateMany(filter, pipeline) {
    let modifiedCount = 0;
    this.docs = this.docs.map(d => {
      if (!matchFilter(d, filter)) return d;
      modifiedCount++;
      return applyPipeline(d, pipeline);
    });
    return { modifiedCount };
  }
}

let collection;
function setupDb(docs) {
  collection = new FakeCollection(docs);
  BankMovement.find.mockImplementation(filter => collection.find(filter));
  BankMovement.bulkWrite.mockImplementation(ops => collection.bulkWrite(ops));
  BankMovement.updateMany.mockImplementation((filter, pipeline) => collection.updateMany(filter, pipeline));
}

// concepto BBVA con el banco contraparte pegado justo después de "RECIBIDO" — formato
// real confirmado contra Mongo del usuario (2026-08-14).
function conceptoRecibido(banco) {
  return `SPEI RECIBIDO${banco.toUpperCase()} / 0192794826 014 8351574TRASPASO ENTRE CUENTAS PROPIAS`;
}

function bbva(over = {}) {
  return {
    _id: 'bbva-1', banco: 'BBVA', categoria: CAT, status: 'no_identificado',
    isActive: true, deposito: 1000, retiro: null, erpIds: [], folio: 'B001',
    fecha: new Date('2026-08-01T12:00:00.000Z'), concepto: conceptoRecibido('BANAMEX'), identificadoPor: [],
    ...over,
  };
}
function contraparte(over = {}) {
  return {
    _id: 'bnmx-1', banco: 'Banamex', status: 'no_identificado',
    isActive: true, deposito: null, retiro: 1000, erpIds: [], folio: 'N001',
    fecha: new Date('2026-08-01T09:00:00.000Z'), concepto: 'RETIRO', identificadoPor: [],
    ...over,
  };
}

beforeEach(() => {
  jest.clearAllMocks();
});

describe('matchTraspasosInternos — clasificación', () => {
  test('caso 1:1 simple → va a relacionados', async () => {
    setupDb([bbva(), contraparte()]);
    const resultado = await matchTraspasosInternos({ categoriaBbva: CAT, dryRun: true }, USER);

    expect(resultado.relacionados).toHaveLength(1);
    expect(resultado.relacionados[0].bbva._id).toBe('bbva-1');
    expect(resultado.relacionados[0].contraparte._id).toBe('bnmx-1');
    expect(resultado.ambiguos).toHaveLength(0);
    expect(resultado.sinContraparteBbva).toHaveLength(0);
    expect(resultado.sinContraparteOtros).toHaveLength(0);
    expect(resultado.sinBancoDetectado).toHaveLength(0);
    expect(resultado.runId).toBeNull();
  });

  test('caso ambiguo (2 BBVA + 2 contraparte mismo banco/día/monto) → los 4 van a ambiguos, cero escritura aunque dryRun:false', async () => {
    const fecha = new Date('2026-08-02T10:00:00.000Z');
    const docs = [
      bbva({ _id: 'bbva-1', status: 'no_identificado', deposito: 500, fecha, folio: 'B1', concepto: conceptoRecibido('Banamex') }),
      bbva({ _id: 'bbva-2', status: 'otros',            deposito: 500, fecha, folio: 'B2', concepto: conceptoRecibido('Banamex') }),
      contraparte({ _id: 'bnmx-1', deposito: null, retiro: 500, fecha, folio: 'N1' }),
      contraparte({ _id: 'bnmx-2', deposito: null, retiro: 500, fecha, folio: 'N2' }),
    ];
    setupDb(docs);
    const resultado = await matchTraspasosInternos({ categoriaBbva: CAT, dryRun: false }, USER);

    expect(resultado.relacionados).toHaveLength(0);
    expect(resultado.ambiguos).toHaveLength(4);
    expect(resultado.sinContraparteBbva).toHaveLength(0);
    expect(resultado.sinContraparteOtros).toHaveLength(0);
    expect(BankMovement.bulkWrite).not.toHaveBeenCalled();

    // Ningún documento fue tocado
    for (const d of collection.docs) {
      expect(['no_identificado', 'otros']).toContain(d.status);
      expect(d.identificadoPor).toHaveLength(0);
    }
  });

  test('dos pares mismo día/monto pero con bancos contraparte DISTINTOS → ambos van a relacionados con SU banco real, sin mezclarse ni marcarse ambiguos', async () => {
    const fecha = new Date('2026-08-06T10:00:00.000Z');
    const docs = [
      bbva({ _id: 'bbva-banamex', deposito: 5000, fecha, folio: 'B10', concepto: conceptoRecibido('Banamex') }),
      contraparte({ _id: 'bnmx-1', banco: 'Banamex', retiro: 5000, fecha, folio: 'N10' }),
      bbva({ _id: 'bbva-santander', deposito: 5000, fecha, folio: 'B11', concepto: conceptoRecibido('Santander') }),
      contraparte({ _id: 'sant-1', banco: 'Santander', retiro: 5000, fecha, folio: 'S10' }),
    ];
    setupDb(docs);
    const resultado = await matchTraspasosInternos({ categoriaBbva: CAT, dryRun: true }, USER);

    expect(resultado.ambiguos).toHaveLength(0);
    expect(resultado.relacionados).toHaveLength(2);

    const parBanamex   = resultado.relacionados.find(p => p.bbva._id === 'bbva-banamex');
    const parSantander = resultado.relacionados.find(p => p.bbva._id === 'bbva-santander');
    expect(parBanamex.contraparte._id).toBe('bnmx-1');
    expect(parBanamex.contraparte.banco).toBe('Banamex');
    expect(parSantander.contraparte._id).toBe('sant-1');
    expect(parSantander.contraparte.banco).toBe('Santander');
  });

  test('concepto sin ningún banco reconocible → va a sinBancoDetectado, no rompe el resto del matching', async () => {
    // Concepto real encontrado en los datos del usuario, sin "RECIBIDO<banco>".
    const conceptoSinBanco = 'TRASPASO ENTRE CUENTAS / REFBNTC00500224 TRASPASO ENTRE CUENTAS PROPIASBMRCASH';
    const fecha = new Date('2026-08-07T00:00:00Z');
    setupDb([
      bbva({ _id: 'bbva-sin-banco', deposito: 800, fecha, concepto: conceptoSinBanco }),
      bbva({ _id: 'bbva-ok', deposito: 900, fecha, concepto: conceptoRecibido('Azteca') }),
      contraparte({ _id: 'azteca-1', banco: 'Azteca', retiro: 900, fecha }),
    ]);
    const resultado = await matchTraspasosInternos({ categoriaBbva: CAT, dryRun: true }, USER);

    expect(resultado.sinBancoDetectado).toHaveLength(1);
    expect(resultado.sinBancoDetectado[0]._id).toBe('bbva-sin-banco');
    expect(resultado.relacionados).toHaveLength(1);
    expect(resultado.relacionados[0].bbva._id).toBe('bbva-ok');
    expect(resultado.ambiguos).toHaveLength(0);
  });

  test('caso sin contraparte de un lado → va a sinContraparteBbva o sinContraparteOtros', async () => {
    setupDb([bbva({ _id: 'bbva-solo', deposito: 700, fecha: new Date('2026-08-03T00:00:00Z') })]);
    let resultado = await matchTraspasosInternos({ categoriaBbva: CAT, dryRun: true }, USER);
    expect(resultado.sinContraparteBbva).toHaveLength(1);
    expect(resultado.sinContraparteBbva[0]._id).toBe('bbva-solo');
    expect(resultado.relacionados).toHaveLength(0);
    expect(resultado.ambiguos).toHaveLength(0);

    // El banco contraparte solo se consulta si algún BBVA lo necesita (evita consultar los
    // 15 bancos del enum siempre) — se agrega un BBVA con Banamex detectado en otro
    // monto/día para disparar esa query, sin que matchee con el retiro huérfano.
    const fecha = new Date('2026-08-03T00:00:00Z');
    setupDb([
      bbva({ _id: 'bbva-otro', deposito: 111, fecha, concepto: conceptoRecibido('Banamex') }),
      contraparte({ _id: 'bnmx-solo', retiro: 700, fecha }),
    ]);
    resultado = await matchTraspasosInternos({ categoriaBbva: CAT, dryRun: true }, USER);
    expect(resultado.sinContraparteOtros).toHaveLength(1);
    expect(resultado.sinContraparteOtros[0]._id).toBe('bnmx-solo');
    expect(resultado.sinContraparteBbva.map(m => m._id)).toContain('bbva-otro');
    expect(resultado.relacionados).toHaveLength(0);
    expect(resultado.ambiguos).toHaveLength(0);
  });

  test('caso con erpIds no vacío → excluido de candidatos (no aparece en ningún bucket)', async () => {
    const fecha = new Date('2026-08-04T00:00:00Z');
    setupDb([
      bbva({ _id: 'bbva-con-erp', deposito: 900, fecha, erpIds: ['CXC-1'] }),
      // BBVA adicional (mismo banco contraparte, otro monto) para disparar la query
      // Banamex — sin él, bnmx-libre nunca se consultaría y el test no probaría nada.
      bbva({ _id: 'bbva-otro', deposito: 111, fecha }),
      contraparte({ _id: 'bnmx-libre', retiro: 900, fecha }),
    ]);
    const resultado = await matchTraspasosInternos({ categoriaBbva: CAT, dryRun: true }, USER);

    const idsEnResultado = [
      ...resultado.relacionados.flatMap(r => [r.bbva._id, r.contraparte._id]),
      ...resultado.ambiguos.map(m => m._id),
      ...resultado.sinContraparteBbva.map(m => m._id),
      ...resultado.sinContraparteOtros.map(m => m._id),
      ...resultado.sinBancoDetectado.map(m => m._id),
    ];
    expect(idsEnResultado).not.toContain('bbva-con-erp');
    expect(resultado.sinContraparteOtros.map(m => m._id)).toContain('bnmx-libre');
  });

  test("status:'reclasificado' → excluido de candidatos BBVA", async () => {
    const fecha = new Date('2026-08-05T00:00:00Z');
    setupDb([
      bbva({ _id: 'bbva-reclas', status: 'reclasificado', deposito: 300, fecha }),
      // BBVA adicional (mismo banco contraparte, otro monto) para disparar la query Banamex.
      bbva({ _id: 'bbva-otro', deposito: 111, fecha }),
      contraparte({ _id: 'bnmx-solo2', retiro: 300, fecha }),
    ]);
    const resultado = await matchTraspasosInternos({ categoriaBbva: CAT, dryRun: true }, USER);

    const idsEnResultado = [
      ...resultado.relacionados.flatMap(r => [r.bbva._id, r.contraparte._id]),
      ...resultado.ambiguos.map(m => m._id),
      ...resultado.sinContraparteBbva.map(m => m._id),
      ...resultado.sinContraparteOtros.map(m => m._id),
      ...resultado.sinBancoDetectado.map(m => m._id),
    ];
    expect(idsEnResultado).not.toContain('bbva-reclas');
    expect(resultado.sinContraparteOtros.map(m => m._id)).toContain('bnmx-solo2');
  });
});

describe('matchTraspasosInternos — dryRun:false (escritura)', () => {
  test('par 1:1 → ambos quedan identificado con traspasoInterno cruzado, identificadoPor e primeraIdentificacionAt', async () => {
    setupDb([bbva(), contraparte()]);
    const resultado = await matchTraspasosInternos({ categoriaBbva: CAT, dryRun: false }, USER);

    expect(resultado.runId).toEqual(expect.any(String));
    expect(BankMovement.bulkWrite).toHaveBeenCalledTimes(1);

    const docBbva        = collection.docs.find(d => d._id === 'bbva-1');
    const docContraparte = collection.docs.find(d => d._id === 'bnmx-1');

    expect(docBbva.status).toBe('identificado');
    expect(docContraparte.status).toBe('identificado');

    expect(docBbva.traspasoInterno).toEqual({
      movimientoId: 'bnmx-1', banco: 'Banamex', folio: 'N001',
      fecha: contraparte().fecha, monto: 1000, runId: resultado.runId,
    });
    expect(docContraparte.traspasoInterno).toEqual({
      movimientoId: 'bbva-1', banco: 'BBVA', folio: 'B001',
      fecha: bbva().fecha, monto: 1000, runId: resultado.runId,
    });

    for (const d of [docBbva, docContraparte]) {
      expect(d.identificadoPor).toHaveLength(1);
      expect(d.identificadoPor[0]).toMatchObject({
        userId: 'user-1', nombre: 'Usuario Uno', source: 'traspaso-interno', runId: resultado.runId,
      });
      expect(d.primeraIdentificacionAt).toBeInstanceOf(Date);
      expect(d.primeraIdentificacionPor).toEqual({ userId: 'user-1', nombre: 'Usuario Uno' });
    }
  });
});

describe('revertirTraspasosInternos', () => {
  async function identificarPar() {
    setupDb([bbva(), contraparte()]);
    const resultado = await matchTraspasosInternos({ categoriaBbva: CAT, dryRun: false }, USER);
    return resultado.runId;
  }

  test('revert completo → ambos vuelven a no_identificado, traspasoInterno limpio, entrada removida', async () => {
    const runId = await identificarPar();

    const revertResult = await revertirTraspasosInternos(runId, 'user-1');
    expect(revertResult.revertidos).toBe(2);

    const docBbva        = collection.docs.find(d => d._id === 'bbva-1');
    const docContraparte = collection.docs.find(d => d._id === 'bnmx-1');

    for (const d of [docBbva, docContraparte]) {
      expect(d.status).toBe('no_identificado');
      expect(d.traspasoInterno).toBeNull();
      expect(d.identificadoPor.find(e => e.source === 'traspaso-interno')).toBeUndefined();
    }
  });

  test('revert cuando alguien identificó manualmente después → quita solo la entrada traspaso-interno, status NO vuelve a no_identificado', async () => {
    const runId = await identificarPar();

    // Identificación humana posterior con un source fuera de TODOS_MOTORES_HISTORICO
    expect(TODOS_MOTORES_HISTORICO).not.toContain('otro-humano');
    const docBbva = collection.docs.find(d => d._id === 'bbva-1');
    docBbva.identificadoPor.push({
      userId: 'user-2', nombre: 'Usuario Dos', fechaId: new Date(),
      erpId: null, source: 'otro-humano', runId: null,
    });

    const revertResult = await revertirTraspasosInternos(runId, 'user-1');
    expect(revertResult.revertidos).toBe(2);

    const docBbvaPost        = collection.docs.find(d => d._id === 'bbva-1');
    const docContrapartePost = collection.docs.find(d => d._id === 'bnmx-1');

    // BBVA: queda identificado por la identificación manual posterior
    expect(docBbvaPost.status).toBe('identificado');
    expect(docBbvaPost.identificadoPor.find(e => e.source === 'traspaso-interno')).toBeUndefined();
    expect(docBbvaPost.identificadoPor.find(e => e.source === 'otro-humano')).toBeDefined();
    expect(docBbvaPost.traspasoInterno).toBeNull();

    // Contraparte: sin identificaciones humanas restantes → sí vuelve a no_identificado
    expect(docContrapartePost.status).toBe('no_identificado');
    expect(docContrapartePost.traspasoInterno).toBeNull();
  });
});

describe('generarPolizaContpaqTraspasos', () => {
  async function leerHojaPoliza(buffer) {
    const wb = new ExcelJS.Workbook();
    await wb.xlsx.load(buffer);
    return wb.getWorksheet('poliza');
  }

  test('2 pares (Banamex + Santander) → 1 fila P + 4 filas M1 con cuenta/cargo-abono/monto/subcódigo correctos', async () => {
    const relacionados = [
      { bbva: bbva({ _id: 'bbva-1', deposito: 720000 }), contraparte: contraparte({ _id: 'bnmx-1', banco: 'Banamex', retiro: 720000 }) },
      { bbva: bbva({ _id: 'bbva-2', deposito: 110000 }), contraparte: contraparte({ _id: 'sant-1', banco: 'Santander', retiro: 110000 }) },
    ];

    const buffer = await generarPolizaContpaqTraspasos(relacionados, { folio: 46246, fecha: new Date('2026-08-08T00:00:00Z') });
    const sheet  = await leerHojaPoliza(buffer);

    expect(sheet.rowCount).toBe(5); // 1 'P' + 4 'M1'

    const headerRow = sheet.getRow(1);
    expect(headerRow.getCell(1).value).toBe('P');
    expect(headerRow.getCell(3).value).toBe('3');
    expect(headerRow.getCell(4).value).toBe(46246);
    expect(headerRow.getCell(7).value).toBe('Traspaso Entre Cuentas Propias');
    expect(headerRow.getCell(9).value).toBe(1); // col I — verificado contra el .xls real

    const rowCargoBanamex = sheet.getRow(2);
    expect(rowCargoBanamex.getCell(1).value).toBe('M1');
    expect(rowCargoBanamex.getCell(2).value).toBe('1102011001'); // BBVA
    expect(rowCargoBanamex.getCell(4).value).toBe(0);            // cargo
    expect(rowCargoBanamex.getCell(5).value).toBe(720000);
    expect(rowCargoBanamex.getCell(6).value).toBe(26);           // Ingr traspasos
    expect(rowCargoBanamex.getCell(8).value).toBe('TRASPASO ENTRE CUENTAS PROPIAS');
    expect(rowCargoBanamex.getCell(9).value).toBe(1);            // col I — verificado contra el .xls real

    const rowAbonoBanamex = sheet.getRow(3);
    expect(rowAbonoBanamex.getCell(2).value).toBe('1102012001'); // Banamex
    expect(rowAbonoBanamex.getCell(4).value).toBe(1);            // abono
    expect(rowAbonoBanamex.getCell(5).value).toBe(720000);
    expect(rowAbonoBanamex.getCell(6).value).toBe(46);           // Egr traspasos

    const rowCargoSantander = sheet.getRow(4);
    expect(rowCargoSantander.getCell(2).value).toBe('1102011001');
    expect(rowCargoSantander.getCell(5).value).toBe(110000);

    const rowAbonoSantander = sheet.getRow(5);
    expect(rowAbonoSantander.getCell(2).value).toBe('1102013001'); // Santander
    expect(rowAbonoSantander.getCell(5).value).toBe(110000);
  });

  test('banco sin cuenta contable mapeada (ej. HSBC) → lanza error identificando el banco', async () => {
    const relacionados = [
      { bbva: bbva(), contraparte: contraparte({ banco: 'HSBC' }) },
    ];
    await expect(generarPolizaContpaqTraspasos(relacionados, { folio: 1 }))
      .rejects.toThrow(/HSBC/);
  });

  test('relacionados vacío → lanza error', async () => {
    await expect(generarPolizaContpaqTraspasos([], { folio: 1 })).rejects.toThrow();
    await expect(generarPolizaContpaqTraspasos(null, { folio: 1 })).rejects.toThrow();
  });

  test('sin folio → lanza error', async () => {
    const relacionados = [{ bbva: bbva(), contraparte: contraparte() }];
    await expect(generarPolizaContpaqTraspasos(relacionados, {})).rejects.toThrow();
  });
});

describe('generarPolizasContpaqTraspasosPorRango', () => {
  test('rango de 2 días con pares en ambos → 2 archivos en el zip, folio provisional secuencial (1, 2...)', async () => {
    const fechaDia1 = new Date('2026-08-08T12:00:00.000Z');
    const fechaDia2 = new Date('2026-08-09T12:00:00.000Z');
    setupDb([
      bbva({ _id: 'bbva-d1', deposito: 720000, fecha: fechaDia1, concepto: conceptoRecibido('Banamex') }),
      contraparte({ _id: 'bnmx-d1', banco: 'Banamex', retiro: 720000, fecha: fechaDia1 }),
      bbva({ _id: 'bbva-d2', deposito: 110000, fecha: fechaDia2, concepto: conceptoRecibido('Santander') }),
      contraparte({ _id: 'sant-d2', banco: 'Santander', retiro: 110000, fecha: fechaDia2 }),
    ]);

    const { buffer, nombreZip } = await generarPolizasContpaqTraspasosPorRango(
      { fechaInicio: '2026-08-08', fechaFin: '2026-08-09' }, USER,
    );
    expect(nombreZip).toBe('Traspasos_CONTPAQ_2026-08-08_a_2026-08-09.zip');

    const zip     = new AdmZip(buffer);
    const nombres = zip.getEntries().map(e => e.entryName).sort();
    expect(nombres).toEqual([
      'Poliza_Traspasos_20260808_CONTPAQ.xlsx',
      'Poliza_Traspasos_20260809_CONTPAQ.xlsx',
    ]);

    const wb1 = new ExcelJS.Workbook();
    await wb1.xlsx.load(zip.getEntry('Poliza_Traspasos_20260808_CONTPAQ.xlsx').getData());
    const sheet1 = wb1.getWorksheet('poliza');
    expect(sheet1.getRow(1).getCell(4).value).toBe(1); // folio provisional secuencial
    expect(sheet1.getRow(2).getCell(5).value).toBe(720000);

    const wb2 = new ExcelJS.Workbook();
    await wb2.xlsx.load(zip.getEntry('Poliza_Traspasos_20260809_CONTPAQ.xlsx').getData());
    const sheet2 = wb2.getWorksheet('poliza');
    expect(sheet2.getRow(1).getCell(4).value).toBe(2);
    expect(sheet2.getRow(2).getCell(5).value).toBe(110000);
  });

  test('pares fuera del rango quedan excluidos', async () => {
    const fechaDentro = new Date('2026-08-08T12:00:00.000Z');
    const fechaFuera  = new Date('2026-08-15T12:00:00.000Z');
    setupDb([
      bbva({ _id: 'bbva-dentro', deposito: 500, fecha: fechaDentro, concepto: conceptoRecibido('Banamex') }),
      contraparte({ _id: 'bnmx-dentro', banco: 'Banamex', retiro: 500, fecha: fechaDentro }),
      bbva({ _id: 'bbva-fuera', deposito: 999, fecha: fechaFuera, concepto: conceptoRecibido('Banamex') }),
      contraparte({ _id: 'bnmx-fuera', banco: 'Banamex', retiro: 999, fecha: fechaFuera }),
    ]);

    const { buffer } = await generarPolizasContpaqTraspasosPorRango(
      { fechaInicio: '2026-08-08', fechaFin: '2026-08-09' }, USER,
    );

    const zip = new AdmZip(buffer);
    expect(zip.getEntries()).toHaveLength(1);
    expect(zip.getEntries()[0].entryName).toBe('Poliza_Traspasos_20260808_CONTPAQ.xlsx');
  });

  test('sin ningún par relacionado en el rango → lanza error', async () => {
    setupDb([]);
    await expect(generarPolizasContpaqTraspasosPorRango(
      { fechaInicio: '2026-08-08', fechaFin: '2026-08-09' }, USER,
    )).rejects.toThrow(/rango de fechas/);
  });

  test('faltan fechas → lanza error', async () => {
    await expect(generarPolizasContpaqTraspasosPorRango(
      { fechaFin: '2026-08-09' }, USER,
    )).rejects.toThrow();
    await expect(generarPolizasContpaqTraspasosPorRango(
      { fechaInicio: '2026-08-08' }, USER,
    )).rejects.toThrow();
  });

  test('categoría con mayúsculas/redacción distinta a la del fixture → igual matchea (case-insensitive, tolerante a variaciones)', async () => {
    const fecha = new Date('2026-08-08T12:00:00.000Z');
    setupDb([
      bbva({ _id: 'bbva-var', deposito: 300, fecha, categoria: 'TRASPASOS ENTRE CUENTAS PROPIAS', concepto: conceptoRecibido('Banamex') }),
      contraparte({ _id: 'bnmx-var', banco: 'Banamex', retiro: 300, fecha }),
    ]);

    const { buffer } = await generarPolizasContpaqTraspasosPorRango(
      { fechaInicio: '2026-08-08', fechaFin: '2026-08-08' }, USER,
    );
    const zip = new AdmZip(buffer);
    expect(zip.getEntries()).toHaveLength(1);
  });

  test('categoría que NO es de traspaso interno → no matchea, lanza error', async () => {
    const fecha = new Date('2026-08-08T12:00:00.000Z');
    setupDb([
      bbva({ _id: 'bbva-otra', deposito: 300, fecha, categoria: 'Pago de nómina', concepto: conceptoRecibido('Banamex') }),
      contraparte({ _id: 'bnmx-otra', banco: 'Banamex', retiro: 300, fecha }),
    ]);

    await expect(generarPolizasContpaqTraspasosPorRango(
      { fechaInicio: '2026-08-08', fechaFin: '2026-08-08' }, USER,
    )).rejects.toThrow(/rango de fechas/);
  });
});
