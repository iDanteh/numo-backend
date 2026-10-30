'use strict';

const ExcelJS = require('exceljs');
const {
  normalizarTarjeta,
  parseClientesCsv,
  parseVendedorXlsx,
  parseCorreccionesXlsx,
} = require('./loyalty-import.parser');

describe('normalizarTarjeta', () => {
  test('quita ceros a la izquierda de tarjetas puramente numéricas', () => {
    expect(normalizarTarjeta('044')).toBe('44');
    expect(normalizarTarjeta('099')).toBe('99');
  });

  test('deja sin cambios una tarjeta numérica normal (sin ceros a la izquierda)', () => {
    expect(normalizarTarjeta('9005')).toBe('9005');
  });

  test('normaliza códigos CF- con separador inconsistente al mismo valor', () => {
    expect(normalizarTarjeta('CF-3615')).toBe('CF-3615');
    expect(normalizarTarjeta('CF 2221')).toBe('CF-2221');
    expect(normalizarTarjeta('cf-2221')).toBe('CF-2221');
  });

  test('tarjeta vacía normaliza a cadena vacía', () => {
    expect(normalizarTarjeta('')).toBe('');
    expect(normalizarTarjeta(undefined)).toBe('');
    expect(normalizarTarjeta(null)).toBe('');
  });
});

describe('parseClientesCsv', () => {
  const HEADER = 'Tarjeta,Nombre,Apellidos,Apodo,Categoria,Sexo,Fecha nacimiento,Correo,Celular,Telefono,Especialidades,Vendedor,Sucursal,Calle,Num exterior,Num Interior,Colonia,Municipio,Estado,CP,Comentarios';

  function csvBuffer(rows) {
    const texto = [HEADER, ...rows].join('\r\n');
    // Codificado como Windows-1252/ISO-8859-1 (latin1), igual que el archivo real.
    return Buffer.from(texto, 'latin1');
  }

  test('parsea un campo entre comillas con salto de línea embebido sin partir la fila', async () => {
    const direccionMultilinea = '"Calle Hidalgo 123\nEntre Juárez y Morelos"';
    const rows = [
      `044,RAYMUNDO,JAVIER RICARDEZ MORALES,,BRONCE,M,01/01/1980,,5551234567,,,,,,,,,,,${direccionMultilinea}`,
      '9005,CRISTINA,DIAZ GUTIERREZ,,ORO,F,,,5559876543,,,,,,,,,,,',
    ];
    const buffer = csvBuffer(rows);

    const resultado = await parseClientesCsv(buffer);

    // Dos filas de datos reales, NO tres: si el parser partiera por '\n' a mano,
    // el salto de línea dentro de las comillas generaría una fila extra espuria.
    expect(resultado).toHaveLength(2);
    expect(resultado[0].tarjetaRaw).toBe('044');
    expect(resultado[0].tarjetaNormalizada).toBe('44');
    expect(resultado[0].nombre).toBe('RAYMUNDO JAVIER RICARDEZ MORALES');
    expect(resultado[0].celular).toBe('5551234567');
    expect(resultado[1].tarjetaRaw).toBe('9005');
    expect(resultado[1].categoriaOriginal).toBe('ORO');
  });

  test('extrae la columna Correo', async () => {
    const rows = ['9005,CRISTINA,DIAZ GUTIERREZ,,ORO,F,,cristina@test.com,5559876543,,,,,,,,,,,'];
    const buffer = csvBuffer(rows);

    const [fila] = await parseClientesCsv(buffer);

    expect(fila.correo).toBe('cristina@test.com');
  });

  test('Correo vacío se parsea como cadena vacía, no undefined', async () => {
    const rows = ['9005,CRISTINA,DIAZ GUTIERREZ,,ORO,F,,,5559876543,,,,,,,,,,,'];
    const buffer = csvBuffer(rows);

    const [fila] = await parseClientesCsv(buffer);

    expect(fila.correo).toBe('');
  });

  test('decodifica acentos/ñ (Windows-1252/ISO-8859-1) correctamente', async () => {
    const rows = ['CF-1420,JOSÉ,MUÑOZ PEÑA,,Plata,M,,,5550001111,,,,,,,,,,,'];
    const buffer = csvBuffer(rows);

    const [fila] = await parseClientesCsv(buffer);

    expect(fila.nombre).toBe('JOSÉ MUÑOZ PEÑA');
  });

  test('conserva filas con Tarjeta vacía (no las descarta)', async () => {
    const rows = [',ANA,LOPEZ,,Bronce,F,,,5552223333,,,,,,,,,,,'];
    const buffer = csvBuffer(rows);

    const resultado = await parseClientesCsv(buffer);

    expect(resultado).toHaveLength(1);
    expect(resultado[0].tarjetaRaw).toBe('');
    expect(resultado[0].tarjetaNormalizada).toBe('');
  });

  test('descarta únicamente las líneas completamente vacías', async () => {
    const rows = [
      '9005,CRISTINA,DIAZ,,ORO,F,,,5559876543,,,,,,,,,,,',
      ',,,,,,,,,,,,,,,,,,,,',
    ];
    const buffer = csvBuffer(rows);

    const resultado = await parseClientesCsv(buffer);

    expect(resultado).toHaveLength(1);
  });

  // Enriquecimiento (2026-10-23) — columnas opcionales, nunca llave de identidad.
  test('extrae las columnas de enriquecimiento (Apodo, Sexo, Fecha nacimiento, Especialidades, Sucursal, dirección, Comentarios)', async () => {
    const rows = [
      '9005,CRISTINA,DIAZ GUTIERREZ,Cris,ORO,F,01/02/1990,,5559876543,,Comercial,JUAN PEREZ,A0,'
      + 'Hidalgo,123,4B,Centro,Oaxaca,Oaxaca,68000,Cliente frecuente',
    ];
    const buffer = csvBuffer(rows);

    const [fila] = await parseClientesCsv(buffer);

    expect(fila.apellidos).toBe('DIAZ GUTIERREZ');
    expect(fila.nombre).toBe('CRISTINA DIAZ GUTIERREZ');
    expect(fila.apodo).toBe('Cris');
    expect(fila.sexo).toBe('F');
    expect(fila.fechaNacimiento).toBe('01/02/1990');
    expect(fila.especialidades).toBe('Comercial');
    expect(fila.sucursal).toBe('A0');
    expect(fila.calle).toBe('Hidalgo');
    expect(fila.numExterior).toBe('123');
    expect(fila.numInterior).toBe('4B');
    expect(fila.colonia).toBe('Centro');
    expect(fila.municipio).toBe('Oaxaca');
    expect(fila.estado).toBe('Oaxaca');
    expect(fila.cp).toBe('68000');
    expect(fila.comentarios).toBe('Cliente frecuente');
  });

  test('columnas de enriquecimiento ausentes del header: no rompen el parseo, quedan vacías (opcionales)', async () => {
    const headerViejo = 'Tarjeta,Nombre,Apellidos,Categoria,Celular,Telefono';
    const texto = [headerViejo, '9005,CRISTINA,DIAZ,ORO,5559876543,'].join('\r\n');
    const buffer = Buffer.from(texto, 'latin1');

    const [fila] = await parseClientesCsv(buffer);

    expect(fila.nombre).toBe('CRISTINA DIAZ');
    expect(fila.especialidades).toBe('');
    expect(fila.comentarios).toBe('');
  });
});

describe('parseVendedorXlsx', () => {
  // Fila 1 = título del reporte, fila 2 = vacía, fila 3 = encabezados reales —
  // calcado de la estructura real de reporteClientesPorVendedor.xlsx.
  async function vendedorBuffer(rows, { headers = ['Tarjeta', 'Cliente', 'Vendedor', 'Fecha de registro', 'Saldo'] } = {}) {
    const workbook = new ExcelJS.Workbook();
    const ws = workbook.addWorksheet('Clientes por Vendedor');
    ws.addRow(['Clientes por vendedor registrados del 1999-12-31 al 2026-10-29']);
    ws.addRow([]);
    ws.addRow(headers);
    rows.forEach((r) => ws.addRow(r));
    return workbook.xlsx.writeBuffer();
  }

  test('extrae Tarjeta/Cliente/Saldo saltando el título y la fila vacía', async () => {
    const buffer = await vendedorBuffer([
      ['016', 'PORFIRIO ANTONIO TRUJILLO MORALES', 'WANDA VIRGINIA ZUNIGA', '2015-02-18', 14.31],
      [9277, 'ARMANDO FAUSTINO LOPEZCUEVAS', 'LUZ ELENA', '2015-02-18', 89.15],
    ]);

    const filas = await parseVendedorXlsx(buffer);

    expect(filas).toHaveLength(2);
    expect(filas[0]).toEqual({ tarjetaRaw: '016', tarjetaNormalizada: '16', nombre: 'PORFIRIO ANTONIO TRUJILLO MORALES', saldo: 14.31 });
    expect(filas[1]).toEqual({ tarjetaRaw: '9277', tarjetaNormalizada: '9277', nombre: 'ARMANDO FAUSTINO LOPEZCUEVAS', saldo: 89.15 });
  });

  test('Tarjeta con tipo mixto (string con ceros a la izquierda vs número puro) normaliza igual', async () => {
    const buffer = await vendedorBuffer([
      ['044', 'UNO', 'V', '2020-01-01', 10],
      [44, 'DOS', 'V', '2020-01-01', 20],
    ]);

    const filas = await parseVendedorXlsx(buffer);

    expect(filas[0].tarjetaNormalizada).toBe('44');
    expect(filas[1].tarjetaNormalizada).toBe('44');
  });

  test('saldo con decimales reales no se trunca (a diferencia del viejo parser de puntos.pdf)', async () => {
    const buffer = await vendedorBuffer([['7152', 'INES GERARDO GARCIA HERNANDEZ', 'IVAN NUNEZ', '2015-02-18', 0]]);

    const [fila] = await parseVendedorXlsx(buffer);

    expect(fila.saldo).toBe(0);
  });

  test('descarta únicamente las filas completamente vacías', async () => {
    const buffer = await vendedorBuffer([
      ['016', 'UNO', 'V', '2020-01-01', 10],
      [],
    ]);

    const filas = await parseVendedorXlsx(buffer);

    expect(filas).toHaveLength(1);
  });

  test('match de encabezado case-insensitive', async () => {
    const buffer = await vendedorBuffer(
      [['044', 'ANA', 'V', '2020-01-01', 100]],
      { headers: ['tarjeta', 'cliente', 'vendedor', 'fecha de registro', 'SALDO'] },
    );

    const [fila] = await parseVendedorXlsx(buffer);

    expect(fila).toEqual({ tarjetaRaw: '044', tarjetaNormalizada: '44', nombre: 'ANA', saldo: 100 });
  });

  test('columnas faltantes: lanza un error 400 nombrándolas', async () => {
    const buffer = await vendedorBuffer([['016', 'UNO', 'V', '2020-01-01', 10]], { headers: ['Tarjeta', 'Cliente'] });

    await expect(parseVendedorXlsx(buffer)).rejects.toMatchObject({ statusCode: 400 });
  });
});

describe('parseCorreccionesXlsx', () => {
  async function xlsxBuffer(headers, rows) {
    const workbook = new ExcelJS.Workbook();
    const ws = workbook.addWorksheet('Correcciones');
    ws.addRow(headers);
    rows.forEach((r) => ws.addRow(r));
    return workbook.xlsx.writeBuffer();
  }

  const HEADERS = ['Tarjeta', 'Nombre', 'Celular', 'Telefono', 'Categoria', 'Saldo', 'Motivo'];

  test('parsea las filas esperadas, ignorando la columna Motivo (informativa)', async () => {
    const buffer = await xlsxBuffer(HEADERS, [
      ['9005', 'CRISTINA DIAZ GUTIERREZ', '5559876543', '', 'Oro', 5134, 'Bloqueante: duplicada'],
      ['9006', 'OTRO CLIENTE', '', '9511122205', 'Bronce', 0, ''],
    ]);

    const filas = await parseCorreccionesXlsx(buffer);

    expect(filas).toHaveLength(2);
    expect(filas[0]).toEqual({
      tarjetaRaw: '9005', nombre: 'CRISTINA DIAZ GUTIERREZ', celular: '5559876543',
      telefono: '', categoriaOriginal: 'Oro', saldo: 5134,
    });
    expect(filas[1]).toEqual({
      tarjetaRaw: '9006', nombre: 'OTRO CLIENTE', celular: '', telefono: '9511122205',
      categoriaOriginal: 'Bronce', saldo: 0,
    });
  });

  test('match de encabezado case-insensitive y sin exigir que exista la columna Motivo', async () => {
    const buffer = await xlsxBuffer(
      ['tarjeta', 'NOMBRE', 'celular', 'TELEFONO', 'categoria', 'SALDO'], // sin Motivo
      [['044', 'ANA', '5551112222', '', 'Plata', 100]],
    );

    const [fila] = await parseCorreccionesXlsx(buffer);

    expect(fila).toEqual({
      tarjetaRaw: '044', nombre: 'ANA', celular: '5551112222', telefono: '', categoriaOriginal: 'Plata', saldo: 100,
    });
  });

  test('Saldo que no parsea a un número ≥0 degrada a 0 en vez de tronar el archivo', async () => {
    const buffer = await xlsxBuffer(HEADERS, [
      ['9005', 'UNO', '5551111111', '', 'Oro', 'no-es-numero', ''],
      ['9006', 'DOS', '5552222222', '', 'Oro', -50, ''],
      ['9007', 'TRES', '5553333333', '', 'Oro', '', ''],
    ]);

    const filas = await parseCorreccionesXlsx(buffer);

    expect(filas.map((f) => f.saldo)).toEqual([0, 0, 0]);
  });

  test('descarta únicamente las filas completamente vacías (sin Tarjeta ni Nombre)', async () => {
    const buffer = await xlsxBuffer(HEADERS, [
      ['9005', 'UNO', '5551111111', '', 'Oro', 100, ''],
      [],
    ]);

    const filas = await parseCorreccionesXlsx(buffer);

    expect(filas).toHaveLength(1);
  });

  test('columnas faltantes: lanza un error 400 nombrándolas', async () => {
    const buffer = await xlsxBuffer(['Tarjeta', 'Nombre'], [['9005', 'UNO']]);

    await expect(parseCorreccionesXlsx(buffer)).rejects.toMatchObject({ statusCode: 400 });
  });
});
