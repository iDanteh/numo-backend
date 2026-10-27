'use strict';

const ExcelJS = require('exceljs');
const {
  normalizarTarjeta,
  parseClientesCsv,
  parsePuntosLines,
  parsePuntosPdf,
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

describe('parsePuntosLines', () => {
  // Formato REAL extraído con pdf-parse (verificado a mano contra puntos.pdf,
  // 2026-10-19): a diferencia de pdfplumber, pdf-parse NO preserva espacios
  // entre columnas — todo queda concatenado: <rowIndex><tarjeta><nombre><saldo>.
  // Estas líneas son copia literal de `data.text` para las filas 1, 12 y 19
  // del reporte real, con el encabezado/pie de página real intercalado (sin
  // espacios tampoco, p. ej. "TarjetaNombreSaldo" y "1 de 3919/10/2026...").
  const PAGINA_EJEMPLO = [
    'Firefoxhttps://www.clubtuberos.com/app/clubtuberos.php',
    'Club Tuberos',
    'Hidalgo 1120 Centro',
    'Oaxaca Oaxaca MEXICO',
    'Teléfono (951)5021340',
    'Saldos Totales',
    'TarjetaNombreSaldo',
    '199999900CLIENTE PRUEBA2 no borrar238,620',
    '29005DIEGO OCTAVIO CRUZ BAUTISTA11,802',
    '35919MAXIMILIANO OJEDA CARRILLO11,560',
    '48606JONATAN JUAREZ MENDOZA10,786',
    '5448EMILIO HERNANDEZ JOSE10,219',
    '68102ARELI MENDEZ HERNANDEZ9,715',
    '71188CAYETANO GARCIA SANTIAGO9,376',
    '87186ADOLFO JULIO SANTIAGO JIMENEZ9,053',
    '910110ALIER SANTILLAN SANPEDRO8,941',
    '106820ORIOLDO VELASCO VARGAS7,861',
    '117471DIEGO ENRICO PLA ACEVEDO7,748',
    '12044RAYMUNDO JAVIER RICARDEZ MORALES7,703',
    '135779JOSE LUIS CRISOSTOMO PEDRO7,432',
    '149171ALBINO MARTINEZ CORTES7,096',
    '158609ISRAEL RODOLFO MARTINEZ RUIZ6,838',
    '169477DANIELA DIAZ GONZALEZ6,611',
    '177923ALBERTO RAFAEL CORTES PEREZ5,961',
    '186430ALEJANDRO VASQUEZ GARCIA5,162',
    '19CF-3615CRISTINA DIAZ GUTIERREZ5,134',
    'Firefoxhttps://www.clubtuberos.com/app/clubtuberos.php',
    '1 de 3919/10/2026, 09:40 a.m.',
  ].join('\n');

  test('extrae solo las filas de datos, descartando encabezado/pie de página', () => {
    const resultado = parsePuntosLines(PAGINA_EJEMPLO);

    expect(resultado).toHaveLength(19);
  });

  test('extrae tarjeta/nombre/saldo correctamente pese a la falta de espacios entre columnas', () => {
    const resultado = parsePuntosLines(PAGINA_EJEMPLO);
    const prueba = resultado[0];
    const raymundo = resultado.find((f) => f.nombre.includes('RICARDEZ MORALES'));
    const cristina = resultado.find((f) => f.nombre.includes('GUTIERREZ'));

    expect(prueba).toEqual(expect.objectContaining({
      tarjetaRaw: '99999900',
      tarjetaNormalizada: '99999900',
      nombre: 'CLIENTE PRUEBA2 no borrar',
      saldo: 238620,
    }));

    expect(raymundo).toEqual(expect.objectContaining({
      tarjetaRaw: '044',
      tarjetaNormalizada: '44',
      nombre: 'RAYMUNDO JAVIER RICARDEZ MORALES',
      saldo: 7703,
    }));

    expect(cristina).toEqual(expect.objectContaining({
      tarjetaRaw: 'CF-3615',
      tarjetaNormalizada: 'CF-3615',
      nombre: 'CRISTINA DIAZ GUTIERREZ',
      saldo: 5134,
    }));
  });

  test('saldo cero (sin coma de miles) se parsea como número, no se descarta', () => {
    // Confirmado real al final del reporte: "...JIMENEZ0" (fila con saldo 0,
    // sin coma de miles porque es menor a 1000).
    const texto = '159145WILLIAMS RAMIREZ JIMENEZ0';

    const resultado = parsePuntosLines(texto);

    expect(resultado).toEqual([
      expect.objectContaining({ tarjetaRaw: '59145', nombre: 'WILLIAMS RAMIREZ JIMENEZ', saldo: 0 }),
    ]);
  });

  test('se resincroniza solo: una línea ajena entre filas no rompe el conteo secuencial', () => {
    const texto = [
      '199999900CLIENTE PRUEBA2 no borrar238,620',
      'Firefoxhttps://www.clubtuberos.com/app/clubtuberos.php', // no empieza con "2" → se descarta sin avanzar
      '29005DIEGO OCTAVIO CRUZ BAUTISTA11,802',
    ].join('\n');

    const resultado = parsePuntosLines(texto);

    expect(resultado).toHaveLength(2);
    expect(resultado[1].nombre).toBe('DIEGO OCTAVIO CRUZ BAUTISTA');
  });

  test('ignora líneas en blanco', () => {
    const texto = `\n\n${PAGINA_EJEMPLO}\n\n`;
    const resultado = parsePuntosLines(texto);

    expect(resultado).toHaveLength(19);
  });
});

// NOTA DE ORDEN: este describe debe ir ANTES de 'parsePuntosPdf' — ese test hace
// jest.resetModules() + require('./loyalty-import.parser') a mano para mockear 'pdf-parse',
// lo que fuerza una SEGUNDA carga de 'exceljs' (vía el nuevo require del módulo) en el mismo
// proceso de Jest. 'exceljs' usa 'readable-stream' internamente, y dos instancias del mismo
// paquete de streams cargadas en el mismo proceso rompen `instanceof`/estado interno de los
// streams ("Cannot read properties of undefined (reading 'objectMode')") para CUALQUIER uso
// posterior de ExcelJS en este archivo de test. Verificado a mano: corriendo este describe antes
// no hay colisión; después sí.
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

describe('parsePuntosPdf', () => {
  test('delega en parsePuntosLines sobre el texto extraído por pdf-parse', async () => {
    jest.resetModules();
    jest.doMock('pdf-parse', () => jest.fn().mockResolvedValue({
      text: '1CF-3615CRISTINA DIAZ GUTIERREZ5,134',
    }), { virtual: true });

    const { parsePuntosPdf: parsePuntosPdfMockeado } = require('./loyalty-import.parser');
    const resultado = await parsePuntosPdfMockeado(Buffer.from('contenido-no-usado'));

    expect(resultado).toHaveLength(1);
    expect(resultado[0]).toEqual(expect.objectContaining({
      tarjetaNormalizada: 'CF-3615',
      nombre: 'CRISTINA DIAZ GUTIERREZ',
      saldo: 5134,
    }));

    jest.dontMock('pdf-parse');
    jest.resetModules();
  });
});
