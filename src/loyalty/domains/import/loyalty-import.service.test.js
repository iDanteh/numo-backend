'use strict';

const { construirConcentrado } = require('./loyalty-import.service');

function cliente(overrides = {}) {
  return {
    tarjetaRaw: '9005',
    tarjetaNormalizada: '9005',
    nombre: 'CRISTINA DIAZ GUTIERREZ',
    celular: '5559876543',
    telefono: '',
    correo: 'cristina@test.com',
    categoriaOriginal: 'Oro',
    ...overrides,
  };
}

function punto(overrides = {}) {
  return {
    tarjetaRaw: '9005',
    tarjetaNormalizada: '9005',
    nombre: 'CRISTINA DIAZ GUTIERREZ',
    saldo: 5134,
    ...overrides,
  };
}

describe('construirConcentrado', () => {
  test('lanza un error de validación cuando ambos arrays vienen vacíos', () => {
    expect(() => construirConcentrado({ clientesRows: [], puntosRows: [] })).toThrow();
    expect(() => construirConcentrado({})).toThrow();
  });

  test('funciona solo con clientesRows (puntosRows ausente)', () => {
    const resultado = construirConcentrado({ clientesRows: [cliente()] });

    expect(resultado.resumen.totalClientes).toBe(1);
    expect(resultado.filas).toHaveLength(1);
    expect(resultado.filas[0].saldo).toBe(0);
    expect(resultado.filas[0].info).toContain('sin_registro_de_saldo');
  });

  test('funciona solo con puntosRows (clientesRows ausente) — todo queda huérfano', () => {
    const resultado = construirConcentrado({ puntosRows: [punto()] });

    expect(resultado.resumen.totalClientes).toBe(0);
    expect(resultado.resumen.totalHuerfanosEnPuntos).toBe(1);
    expect(resultado.filas).toHaveLength(1);
    expect(resultado.filas[0].advertencias).toContain('tarjeta_sin_match_en_puntos');
  });

  test('cliente con match en puntos: saldo correcto, sin advertencia de saldo', () => {
    const resultado = construirConcentrado({
      clientesRows: [cliente()],
      puntosRows: [punto()],
    });

    const [fila] = resultado.filas;
    expect(fila.saldo).toBe(5134);
    expect(fila.info).not.toContain('sin_registro_de_saldo');
    expect(resultado.resumen.totalConSaldo).toBe(1);
    expect(resultado.resumen.totalSinSaldo).toBe(0);
  });

  test('cliente sin match en puntos: saldo 0 e info sin_registro_de_saldo (NO es advertencia)', () => {
    const resultado = construirConcentrado({
      clientesRows: [cliente({ tarjetaRaw: '7000', tarjetaNormalizada: '7000' })],
      puntosRows: [punto()], // tarjeta distinta, no matchea
    });

    const filaCliente = resultado.filas.find((f) => f.tarjetaNormalizada === '7000');
    expect(filaCliente.saldo).toBe(0);
    expect(filaCliente.info).toEqual(['sin_registro_de_saldo']);
    expect(filaCliente.advertencias).not.toContain('sin_registro_de_saldo');
    expect(resultado.resumen.totalSinSaldo).toBe(1);
  });

  test('puntos row huérfano (sin match en clientes): tarjeta_sin_match_en_puntos y sin_contacto', () => {
    const resultado = construirConcentrado({
      clientesRows: [cliente({ tarjetaRaw: '7000', tarjetaNormalizada: '7000' })],
      puntosRows: [punto()], // tarjeta '9005', ningún cliente la tiene
    });

    const huerfano = resultado.filas.find((f) => f.tarjetaRawPuntos === '9005' && f.tarjetaRawClientes === null);
    expect(huerfano).toBeDefined();
    expect(huerfano.advertencias).toContain('tarjeta_sin_match_en_puntos');
    expect(huerfano.advertencias).toContain('sin_contacto');
    expect(resultado.resumen.totalHuerfanosEnPuntos).toBe(1);
  });

  test('tarjeta duplicada en clientes.csv: ambas filas quedan marcadas (caso real "3569")', () => {
    const resultado = construirConcentrado({
      clientesRows: [
        cliente({ tarjetaRaw: '3569', tarjetaNormalizada: '3569', nombre: 'PERSONA UNO', celular: '5551111111' }),
        cliente({ tarjetaRaw: '3569', tarjetaNormalizada: '3569', nombre: 'PERSONA DOS', celular: '5552222222' }),
      ],
    });

    expect(resultado.filas).toHaveLength(2);
    expect(resultado.filas.every((f) => f.advertencias.includes('tarjeta_duplicada_en_clientes'))).toBe(true);
    expect(resultado.resumen.totalDuplicadosEnClientes).toBe(2);
  });

  test('tarjeta duplicada vacía ("") también se marca en ambas filas', () => {
    const resultado = construirConcentrado({
      clientesRows: [
        cliente({ tarjetaRaw: '', tarjetaNormalizada: '', nombre: 'PERSONA A', celular: '5550000001' }),
        cliente({ tarjetaRaw: '', tarjetaNormalizada: '', nombre: 'PERSONA B', celular: '5550000002' }),
      ],
    });

    expect(resultado.filas.every((f) => f.advertencias.includes('tarjeta_duplicada_en_clientes'))).toBe(true);
  });

  test('Categoria no reconocida (ej. "Profesionales"): nivelSugerido Bronce por default, NO bloquea (es info, no advertencia)', () => {
    const resultado = construirConcentrado({
      clientesRows: [cliente({ categoriaOriginal: 'Profesionales' })],
    });

    const [fila] = resultado.filas;
    expect(fila.nivelSugerido).toBe('Bronce');
    expect(fila.info).toContain('nivel_asignado_por_default');
    expect(fila.advertencias).not.toContain('nivel_asignado_por_default');
    expect(resultado.resumen.totalNivelPorDefault).toBe(1);
  });

  test.each([
    ['Oro', 'Oro'], ['ORO', 'Oro'], ['oro', 'Oro'],
    ['Plata', 'Plata'], ['PLATA', 'Plata'],
    ['Bronce', 'Bronce'], [' bronce ', 'Bronce'],
  ])('Categoria "%s" se respeta como nivelSugerido "%s", sin asignación por default', (categoriaOriginal, nivelEsperado) => {
    const resultado = construirConcentrado({
      clientesRows: [cliente({ categoriaOriginal })],
    });

    expect(resultado.filas[0].nivelSugerido).toBe(nivelEsperado);
    expect(resultado.filas[0].info).not.toContain('nivel_asignado_por_default');
  });

  test('"CLIENTE PRUEBA2 no borrar" con saldo 238,620 produce posible_registro_prueba', () => {
    const resultado = construirConcentrado({
      clientesRows: [cliente({
        tarjetaRaw: '99999900',
        tarjetaNormalizada: '99999900',
        nombre: 'CLIENTE PRUEBA2 no borrar',
      })],
      puntosRows: [punto({ tarjetaRaw: '99999900', tarjetaNormalizada: '99999900', saldo: 238620 })],
    });

    const fila = resultado.filas.find((f) => f.tarjetaNormalizada === '99999900');
    expect(fila.advertencias).toContain('posible_registro_prueba');
    expect(resultado.resumen.totalPosiblesPrueba).toBe(1);
  });

  test('cliente sin celular NI telefono produce sin_contacto (queda pendiente, no bloquea)', () => {
    const resultado = construirConcentrado({
      clientesRows: [cliente({ celular: '', telefono: '' })],
    });

    expect(resultado.filas[0].advertencias).toContain('sin_contacto');
    expect(resultado.filas[0].contacto).toBe('');
    expect(resultado.resumen.totalSinContacto).toBe(1);
  });

  test('cliente sin celular PERO con telefono: NO produce sin_contacto, usa telefono como contacto', () => {
    const resultado = construirConcentrado({
      clientesRows: [cliente({ celular: '', telefono: '9511122205' })],
    });

    expect(resultado.filas[0].advertencias).not.toContain('sin_contacto');
    expect(resultado.filas[0].contacto).toBe('9511122205');
    expect(resultado.resumen.totalSinContacto).toBe(0);
  });

  test('cliente con celular: contacto usa celular, no telefono', () => {
    const resultado = construirConcentrado({
      clientesRows: [cliente({ celular: '5559876543', telefono: '5551112222' })],
    });

    expect(resultado.filas[0].contacto).toBe('5559876543');
  });

  test('correo se propaga de clientesRows a la fila', () => {
    const resultado = construirConcentrado({ clientesRows: [cliente({ correo: 'ana@test.com' })] });

    expect(resultado.filas[0].correo).toBe('ana@test.com');
  });

  test('fila huérfana de puntos.pdf (sin cliente) trae correo vacío', () => {
    const resultado = construirConcentrado({ puntosRows: [punto()] });

    expect(resultado.filas[0].correo).toBe('');
  });
});
