'use strict';

// caja-transferencia-sync.service.test.js — Fase A del proceso de matching de
// transferencias entre cajas: _rangoSync() (cálculo de ventana con backfill
// acotado a 1 mes) y sincronizarTransferenciasCajas() (upsert por koreId, sin
// pisar estatusMatch en un re-sync).
jest.mock('./CajaTransferencia.model');
jest.mock('./kore-caja.service', () => ({
  buscarTransferenciasCajas: jest.fn(),
}));
jest.mock('../../../shared/services/global-config.service');

const CajaTransferencia = require('./CajaTransferencia.model');
const { buscarTransferenciasCajas } = require('./kore-caja.service');
const globalConfigService = require('../../../shared/services/global-config.service');
const {
  sincronizarTransferenciasCajas, sincronizarTransferenciasCajasManual, reaplicarFiltro, init, _rangoSync, _pasaFiltro,
} = require('./caja-transferencia-sync.service');

const SIN_CONFIGURAR = new Error('No existe la configuración bancos.X para este ambiente.');

// 2026-09-02 09:00 hora de México (UTC-6, sin horario de verano) — "ayer" en
// México es 2026-09-01, "hace 31 días desde ayer" es 2026-08-01 (Agosto tiene
// 31 días, números redondos a propósito para que el test sea legible).
const AHORA = new Date('2026-09-02T15:00:00Z');

describe('_rangoSync', () => {
  test('primera corrida (sin transferencias guardadas): ventana completa de 31 días hasta ayer', () => {
    const rango = _rangoSync(null, AHORA);
    expect(rango).toEqual({ fechaDesde: '2026-08-01T00:00:00Z', fechaHasta: '2026-09-01T23:59:59Z' });
  });

  test('corrida normal (última guardada = anteayer): solo el día de ayer', () => {
    const rango = _rangoSync(new Date('2026-08-30T10:00:00Z'), AHORA);
    expect(rango).toEqual({ fechaDesde: '2026-08-31T00:00:00Z', fechaHasta: '2026-09-01T23:59:59Z' });
  });

  test('ya está al día (última guardada = ayer): no hay nada nuevo que sincronizar', () => {
    const rango = _rangoSync(new Date('2026-09-01T10:00:00Z'), AHORA);
    expect(rango).toBeNull();
  });

  test('catch-up de más de un mes: se acota a los 31 días, no se va más atrás', () => {
    const rango = _rangoSync(new Date('2026-01-01T00:00:00Z'), AHORA);
    expect(rango).toEqual({ fechaDesde: '2026-08-01T00:00:00Z', fechaHasta: '2026-09-01T23:59:59Z' });
  });
});

function fakeFindOneChain(result) {
  const q = { sort: jest.fn(() => q), select: jest.fn(() => q), lean: jest.fn().mockResolvedValue(result) };
  return q;
}

describe('_pasaFiltro', () => {
  const t = { estatus: 'RECIBIDO', nombreTipoTransferencia: 'CIERRE DE CAJA', nombreCajaDestino: 'CAJA SILVA' };

  test('sin ninguna lista configurada (null en ambas): pasa todo', () => {
    expect(_pasaFiltro(t, null, null)).toBe(true);
  });

  test('tipo permitido pero caja destino NO está en la lista: no pasa', () => {
    expect(_pasaFiltro(t, ['CIERRE DE CAJA'], ['OTRA CAJA'])).toBe(false);
  });

  test('caja destino permitida pero tipo NO está en la lista: no pasa', () => {
    expect(_pasaFiltro(t, ['INICIO DE SESIÓN'], ['CAJA SILVA'])).toBe(false);
  });

  test('ambas listas configuradas y la transferencia matchea las dos: pasa', () => {
    expect(_pasaFiltro(t, ['CIERRE DE CAJA'], ['CAJA SILVA'])).toBe(true);
  });

  test('estatus CANCELADO (crudo de Kore, campo `estatus`): no pasa aunque tipo/caja matcheen', () => {
    expect(_pasaFiltro({ ...t, estatus: 'CANCELADO' }, null, null)).toBe(false);
  });

  test('estatus CANCELADO (documento persistido, campo `estatusKore`): no pasa', () => {
    const persistido = { estatusKore: 'CANCELADO', nombreTipoTransferencia: 'CIERRE DE CAJA', nombreCajaDestino: 'CAJA SILVA' };
    expect(_pasaFiltro(persistido, null, null)).toBe(false);
  });

  test('sin estatus en absoluto (ni estatus ni estatusKore): no pasa', () => {
    expect(_pasaFiltro({ nombreTipoTransferencia: 'CIERRE DE CAJA', nombreCajaDestino: 'CAJA SILVA' }, null, null)).toBe(false);
  });

  test('documento persistido con estatusKore RECIBIDO: pasa igual que el crudo', () => {
    const persistido = { estatusKore: 'RECIBIDO', nombreTipoTransferencia: 'CIERRE DE CAJA', nombreCajaDestino: 'CAJA SILVA' };
    expect(_pasaFiltro(persistido, null, null)).toBe(true);
  });
});

describe('sincronizarTransferenciasCajas', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    CajaTransferencia.updateOne = jest.fn().mockResolvedValue({});
    // Default: ninguna de las 2 claves de filtro está sembrada todavía en este ambiente —
    // mismo comportamiento que un environment recién actualizado a Fase B, antes de correr
    // el seed. Los tests que sí quieren probar el filtro sobreescriben esto puntualmente.
    globalConfigService.getValue.mockRejectedValue(SIN_CONFIGURAR);
  });

  test('ya al día: no llama a Kore ni hace ningún upsert', async () => {
    CajaTransferencia.findOne = jest.fn(() => fakeFindOneChain({ fechaRecepcion: new Date() }));

    const res = await sincronizarTransferenciasCajas();

    expect(buscarTransferenciasCajas).not.toHaveBeenCalled();
    expect(CajaTransferencia.updateOne).not.toHaveBeenCalled();
    expect(res).toEqual({ sincronizadas: 0, descartadas: 0 });
  });

  test('upserta por koreId, sin pisar estatusMatch (solo va en $setOnInsert)', async () => {
    CajaTransferencia.findOne = jest.fn(() => fakeFindOneChain(null));
    buscarTransferenciasCajas.mockResolvedValue({
      raw: [{
        id: '6a97291ab6007400011db828', monto: 1500, estatus: 'RECIBIDO',
        cajaOrigenId: 'c1', nombreCajaOrigen: 'CAJA SILVA', almacenCajaOrigen: 'A0',
        cajaDestinoId: 'c2', nombreCajaDestino: 'CAJA - HECTOR', almacenCajaDestino: 'A0',
        sessionOrigenId: 's1', sessionDestinoId: 's2',
        formaPago: 'fp1', nombreFormaPago: 'EFECTIVO',
        solicito: 'u1', nombreSolicito: 'CARLOS', recibio: 'u2', nombreRecibio: 'ROBERTO',
        autorizo: '', nombreAutorizo: '',
        fechaSolicitud: '2026-09-01T19:35:54.606037Z', fechaRecepcion: '2026-09-01T19:36:32.057614Z',
        observacion: 'FONDO INICIAL', idTipoTransferencia: 't1', nombreTipoTransferencia: 'INICIO DE SESIÓN',
      }],
    });

    const res = await sincronizarTransferenciasCajas();

    expect(res).toMatchObject({ sincronizadas: 1, descartadas: 0 });
    expect(CajaTransferencia.updateOne).toHaveBeenCalledTimes(1);
    const [filtro, update, opts] = CajaTransferencia.updateOne.mock.calls[0];
    expect(filtro).toEqual({ koreId: '6a97291ab6007400011db828' });
    expect(opts).toEqual({ upsert: true });
    expect(update.$set.monto).toBe(1500);
    expect(update.$set.estatusMatch).toBeUndefined(); // nunca en $set
    expect(update.$setOnInsert).toEqual({ koreId: '6a97291ab6007400011db828', estatusMatch: 'pendiente' });
  });

  test('descarta entradas sin id de Kore (no se puede deduplicar)', async () => {
    CajaTransferencia.findOne = jest.fn(() => fakeFindOneChain(null));
    buscarTransferenciasCajas.mockResolvedValue({ raw: [{ monto: 100, estatus: 'RECIBIDO' }] });

    const res = await sincronizarTransferenciasCajas();

    expect(res).toMatchObject({ sincronizadas: 0, descartadas: 0 });
    expect(CajaTransferencia.updateOne).not.toHaveBeenCalled();
  });

  test('con NOMBRE_TIPO_TRANSFERENCIA_PERMITIDOS configurado: descarta lo que no matchea, sincroniza lo que sí', async () => {
    CajaTransferencia.findOne = jest.fn(() => fakeFindOneChain(null));
    globalConfigService.getValue.mockImplementation((seccion, clave) => {
      if (clave === 'NOMBRE_TIPO_TRANSFERENCIA_PERMITIDOS') return Promise.resolve('["CIERRE DE CAJA"]');
      return Promise.reject(SIN_CONFIGURAR); // NOMBRE_CAJA_DESTINO_PERMITIDAS sin configurar
    });
    buscarTransferenciasCajas.mockResolvedValue({
      raw: [
        { id: 't-1', monto: 100, estatus: 'RECIBIDO', nombreTipoTransferencia: 'CIERRE DE CAJA', nombreCajaDestino: 'CAJA SILVA' },
        { id: 't-2', monto: 200, estatus: 'RECIBIDO', nombreTipoTransferencia: 'INICIO DE SESIÓN', nombreCajaDestino: 'CAJA SILVA' },
      ],
    });

    const res = await sincronizarTransferenciasCajas();

    expect(res).toMatchObject({ sincronizadas: 1, descartadas: 1 });
    expect(CajaTransferencia.updateOne).toHaveBeenCalledTimes(1);
    expect(CajaTransferencia.updateOne.mock.calls[0][0]).toEqual({ koreId: 't-1' });
  });

  test('filtro configurado como [] (default del seed): se trata igual que sin configurar, no bloquea nada', async () => {
    CajaTransferencia.findOne = jest.fn(() => fakeFindOneChain(null));
    globalConfigService.getValue.mockResolvedValue('[]');
    buscarTransferenciasCajas.mockResolvedValue({ raw: [{ id: 't-1', monto: 100, estatus: 'RECIBIDO' }] });

    const res = await sincronizarTransferenciasCajas();

    expect(res).toMatchObject({ sincronizadas: 1, descartadas: 0 });
  });
});

describe('sincronizarTransferenciasCajasManual', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    CajaTransferencia.updateOne = jest.fn().mockResolvedValue({});
    globalConfigService.getValue.mockRejectedValue(SIN_CONFIGURAR);
  });

  test('requiere fechaDesde y fechaHasta explícitas', async () => {
    await expect(sincronizarTransferenciasCajasManual({})).rejects.toThrow('Se requieren fechaDesde y fechaHasta');
    await expect(sincronizarTransferenciasCajasManual({ fechaDesde: '2026-01-01T00:00:00Z' })).rejects.toThrow('Se requieren fechaDesde y fechaHasta');
    expect(buscarTransferenciasCajas).not.toHaveBeenCalled();
  });

  test('usa el rango EXACTO que se le pasa, sin pasar por _rangoSync ni el tope de VENTANA_MAX_DIAS', async () => {
    buscarTransferenciasCajas.mockResolvedValue({ raw: [{ id: 't-1', monto: 100, estatus: 'RECIBIDO' }] });

    const res = await sincronizarTransferenciasCajasManual({
      fechaDesde: '2020-01-01T00:00:00Z', fechaHasta: '2020-01-31T23:59:59Z',
    });

    expect(CajaTransferencia.findOne).not.toHaveBeenCalled(); // no busca "última guardada" — el rango ya vino dado
    expect(buscarTransferenciasCajas).toHaveBeenCalledWith({
      fechaDesde: '2020-01-01T00:00:00Z', fechaHasta: '2020-01-31T23:59:59Z',
    });
    expect(res).toMatchObject({ sincronizadas: 1, descartadas: 0 });
  });

  test('rechaza una segunda corrida manual mientras la primera sigue en curso', async () => {
    let resolverPrimera;
    buscarTransferenciasCajas.mockReturnValueOnce(new Promise((resolve) => { resolverPrimera = resolve; }));

    const primera  = sincronizarTransferenciasCajasManual({ fechaDesde: '2020-01-01T00:00:00Z', fechaHasta: '2020-01-02T00:00:00Z' });
    const segunda  = sincronizarTransferenciasCajasManual({ fechaDesde: '2020-02-01T00:00:00Z', fechaHasta: '2020-02-02T00:00:00Z' });

    await expect(segunda).rejects.toThrow('Ya hay una sincronización manual');
    resolverPrimera({ raw: [] });
    await primera;
  });
});

function fakeFindChain(result) {
  const q = { skip: jest.fn(() => q), limit: jest.fn(() => q), lean: jest.fn().mockResolvedValue(result) };
  return q;
}

describe('reaplicarFiltro', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    CajaTransferencia.bulkWrite = jest.fn().mockResolvedValue({});
  });

  test('dry-run: detecta exclusiones y reinclusiones sin escribir nada', async () => {
    globalConfigService.getValue.mockImplementation((seccion, clave) => {
      if (clave === 'NOMBRE_TIPO_TRANSFERENCIA_PERMITIDOS') return Promise.resolve('["CIERRE DE CAJA"]');
      return Promise.reject(SIN_CONFIGURAR);
    });
    CajaTransferencia.find = jest.fn(() => fakeFindChain([
      { _id: 'a', koreId: 'k-a', monto: 100, estatusKore: 'RECIBIDO', nombreTipoTransferencia: 'INICIO DE SESIÓN', excluidaPorFiltro: false },
      { _id: 'b', koreId: 'k-b', monto: 200, estatusKore: 'RECIBIDO', nombreTipoTransferencia: 'CIERRE DE CAJA', excluidaPorFiltro: true },
      { _id: 'c', koreId: 'k-c', monto: 300, estatusKore: 'RECIBIDO', nombreTipoTransferencia: 'CIERRE DE CAJA', excluidaPorFiltro: false },
    ]));

    const res = await reaplicarFiltro({ dryRun: true });

    expect(res.dryRun).toBe(true);
    expect(res.aplicados).toBe(0);
    expect(res.encontrados).toBe(2);
    expect(res.detalle).toEqual(expect.arrayContaining([
      expect.objectContaining({ _id: 'a', accion: 'excluir' }),
      expect.objectContaining({ _id: 'b', accion: 'reincluir' }),
    ]));
    expect(CajaTransferencia.bulkWrite).not.toHaveBeenCalled();
    expect(CajaTransferencia.find.mock.calls[0][0]).toMatchObject({ estatusMatch: 'pendiente' });
  });

  test('ejecuta de verdad: bulkWrite setea excluidaPorFiltro/excluidaEn según la acción', async () => {
    globalConfigService.getValue.mockImplementation((seccion, clave) => {
      if (clave === 'NOMBRE_TIPO_TRANSFERENCIA_PERMITIDOS') return Promise.resolve('["CIERRE DE CAJA"]');
      return Promise.reject(SIN_CONFIGURAR);
    });
    CajaTransferencia.find = jest.fn(() => fakeFindChain([
      { _id: 'a', koreId: 'k-a', monto: 100, estatusKore: 'RECIBIDO', nombreTipoTransferencia: 'INICIO DE SESIÓN', excluidaPorFiltro: false },
      { _id: 'b', koreId: 'k-b', monto: 200, estatusKore: 'RECIBIDO', nombreTipoTransferencia: 'CIERRE DE CAJA', excluidaPorFiltro: true },
    ]));

    const res = await reaplicarFiltro({ dryRun: false });

    expect(res.aplicados).toBe(2);
    expect(CajaTransferencia.bulkWrite).toHaveBeenCalledTimes(1);
    const ops = CajaTransferencia.bulkWrite.mock.calls[0][0];
    const opA = ops.find(o => o.updateOne.filter._id === 'a');
    const opB = ops.find(o => o.updateOne.filter._id === 'b');
    expect(opA.updateOne.update.$set).toMatchObject({ excluidaPorFiltro: true });
    expect(opA.updateOne.update.$set.excluidaEn).toBeInstanceOf(Date);
    expect(opB.updateOne.update.$set).toEqual({ excluidaPorFiltro: false, excluidaEn: null });
  });

  test('nada que cambiar: no llama bulkWrite ni siquiera en modo ejecución', async () => {
    globalConfigService.getValue.mockRejectedValue(SIN_CONFIGURAR); // sin filtro configurado, todo pasa
    CajaTransferencia.find = jest.fn(() => fakeFindChain([
      { _id: 'a', koreId: 'k-a', monto: 100, estatusKore: 'RECIBIDO', nombreTipoTransferencia: 'X', excluidaPorFiltro: false },
    ]));

    const res = await reaplicarFiltro({ dryRun: false });

    expect(res).toMatchObject({ encontrados: 0, aplicados: 0 });
    expect(CajaTransferencia.bulkWrite).not.toHaveBeenCalled();
  });

  test('caso real 2026-09-02: transferencia CANCELADO (fechaRecepcion null, nunca alcanzada por detectarHuerfanas) queda excluida', async () => {
    globalConfigService.getValue.mockRejectedValue(SIN_CONFIGURAR); // sin filtro de tipo/caja configurado
    CajaTransferencia.find = jest.fn(() => fakeFindChain([
      {
        _id: 'huerfano-real', koreId: 'k-cancelado', monto: 5500,
        estatusKore: 'CANCELADO', fechaRecepcion: null,
        nombreTipoTransferencia: 'NORMAL', nombreCajaDestino: 'CAJA SILVA',
        excluidaPorFiltro: false,
      },
    ]));

    const res = await reaplicarFiltro({ dryRun: false });

    expect(res.aplicados).toBe(1);
    const ops = CajaTransferencia.bulkWrite.mock.calls[0][0];
    expect(ops[0].updateOne.filter).toEqual({ _id: 'huerfano-real' });
    expect(ops[0].updateOne.update.$set).toMatchObject({ excluidaPorFiltro: true });
  });

  test('la query nunca pide matcheada/huerfana (solo estatusMatch:pendiente)', async () => {
    globalConfigService.getValue.mockRejectedValue(SIN_CONFIGURAR);
    CajaTransferencia.find = jest.fn(() => fakeFindChain([]));

    await reaplicarFiltro({ dryRun: true });

    expect(CajaTransferencia.find).toHaveBeenCalledWith({ estatusMatch: 'pendiente' });
  });
});

// init() (2026-09-02) — registra el hook que reaplica el filtro automáticamente al cambiar
// config, sin control manual del usuario ("pueden causar un desastre"). Se testea el efecto
// real (CajaTransferencia.find/bulkWrite mockeados, mismo patrón que el resto del archivo)
// en vez de mockear reaplicarFiltro, porque init() lo llama por referencia interna al módulo.
describe('init', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    CajaTransferencia.bulkWrite = jest.fn().mockResolvedValue({});
  });

  test('registra exactamente un hook en globalConfigService', () => {
    init();
    expect(globalConfigService.registerConfigChangeHook).toHaveBeenCalledTimes(1);
    expect(globalConfigService.registerConfigChangeHook).toHaveBeenCalledWith(expect.any(Function));
  });

  test('el hook corre reaplicarFiltro(dryRun:false) cuando cambia NOMBRE_TIPO_TRANSFERENCIA_PERMITIDOS', async () => {
    globalConfigService.getValue.mockImplementation((seccion, clave) => {
      if (clave === 'NOMBRE_TIPO_TRANSFERENCIA_PERMITIDOS') return Promise.resolve('["NORMAL"]');
      return Promise.reject(SIN_CONFIGURAR);
    });
    CajaTransferencia.find = jest.fn(() => fakeFindChain([
      { _id: 'a', koreId: 'k-a', monto: 100, estatusKore: 'RECIBIDO', nombreTipoTransferencia: 'OTRO', excluidaPorFiltro: false },
    ]));

    init();
    const hook = globalConfigService.registerConfigChangeHook.mock.calls[0][0];
    await hook({ sectionClave: 'bancos', clave: 'NOMBRE_TIPO_TRANSFERENCIA_PERMITIDOS', valor: '["NORMAL"]' });

    expect(CajaTransferencia.bulkWrite).toHaveBeenCalledTimes(1); // corrió de verdad, no dry-run
  });

  test('el hook corre reaplicarFiltro(dryRun:false) cuando cambia NOMBRE_CAJA_DESTINO_PERMITIDAS', async () => {
    globalConfigService.getValue.mockImplementation((seccion, clave) => {
      if (clave === 'NOMBRE_CAJA_DESTINO_PERMITIDAS') return Promise.resolve('["CAJA SILVA"]');
      return Promise.reject(SIN_CONFIGURAR);
    });
    CajaTransferencia.find = jest.fn(() => fakeFindChain([
      { _id: 'a', koreId: 'k-a', monto: 100, estatusKore: 'RECIBIDO', nombreCajaDestino: 'OTRA CAJA', excluidaPorFiltro: false },
    ]));

    init();
    const hook = globalConfigService.registerConfigChangeHook.mock.calls[0][0];
    await hook({ sectionClave: 'bancos', clave: 'NOMBRE_CAJA_DESTINO_PERMITIDAS', valor: '["CAJA SILVA"]' });

    expect(CajaTransferencia.bulkWrite).toHaveBeenCalledTimes(1);
  });

  test('el hook no hace nada si la sección o la clave no son ninguna de las 2 relevantes', async () => {
    init();
    const hook = globalConfigService.registerConfigChangeHook.mock.calls[0][0];

    await hook({ sectionClave: 'kore', clave: 'AUTH_URL', valor: 'x' });
    await hook({ sectionClave: 'bancos', clave: 'TOKEN', valor: 'x' });

    expect(CajaTransferencia.find).not.toHaveBeenCalled();
    expect(CajaTransferencia.bulkWrite).not.toHaveBeenCalled();
  });
});
