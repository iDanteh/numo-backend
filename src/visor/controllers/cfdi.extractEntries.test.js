'use strict';

// Carga de XML en ZIP (2026-10-14): un ZIP armado para pesar poco comprimido y
// muchísimo al abrirse ("bomba ZIP") tumbaba el proceso, porque se descomprimía
// todo en memoria sin revisar tamaños.
const AdmZip = require('adm-zip');
const { _extractEntries } = require('./cfdi.controller');

const zipCon = (archivos) => {
  const zip = new AdmZip();
  for (const [nombre, contenido] of archivos) zip.addFile(nombre, contenido);
  return { originalname: 'lote.zip', buffer: zip.toBuffer() };
};

test('ZIP normal: cada XML sale con su contenido, como antes', () => {
  const r = [..._extractEntries(zipCon([['a.xml', Buffer.from('<cfdi/>')], ['b.txt', Buffer.from('x')]]))];
  expect(r).toEqual([{ name: 'a.xml', buffer: Buffer.from('<cfdi/>') }]);
});

test('XML suelto pasa tal cual', () => {
  const file = { originalname: 'a.xml', buffer: Buffer.from('<cfdi/>') };
  expect([..._extractEntries(file)]).toEqual([{ name: 'a.xml', buffer: file.buffer }]);
});

test('XML que se abre en más de 50 MB no se descomprime', () => {
  const bomba = zipCon([['bomba.xml', Buffer.alloc(51 * 1024 * 1024)], ['ok.xml', Buffer.from('<cfdi/>')]]);
  expect(bomba.buffer.length).toBeLessThan(1024 * 1024); // pesa poco comprimido
  const r = [..._extractEntries(bomba)];
  expect(r[0]).toMatchObject({ name: 'bomba.xml', buffer: null });
  expect(r[0].error).toMatch(/demasiado grande/);
  expect(r[1]).toEqual({ name: 'ok.xml', buffer: Buffer.from('<cfdi/>') });
});

test('XML vacío se rechaza (sin tamaño declarado no hay tope al abrirlo)', () => {
  const r = [..._extractEntries(zipCon([['vacio.xml', Buffer.alloc(0)]]))];
  expect(r).toEqual([{ name: 'vacio.xml', buffer: null, error: 'XML vacío' }]);
});
