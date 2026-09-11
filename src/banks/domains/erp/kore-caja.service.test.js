'use strict';

// kore-caja.service.test.js — no existía ningún test para este service.
// Cobertura enfocada SOLO en estatusActualDeErrorKore/esErrorYaEnEstatus (lógica
// pura, sin I/O) — el resto del archivo son wrappers finos de axios contra Kore,
// sin valor real en un test unitario sin Kore real disponible.

const { KoreCajaError, esErrorYaEnEstatus, estatusActualDeErrorKore } = require('./kore-caja.service');

function koreError(mensaje, koreBody = { Mensaje: mensaje }) {
  return new KoreCajaError(mensaje, 400, koreBody);
}

describe('estatusActualDeErrorKore()', () => {
  test('extrae el estatus real del mensaje de rechazo de Kore (Mensaje)', () => {
    expect(estatusActualDeErrorKore(koreError('No puede cambiar el estatus de la solicitud con estatus: APROBADO')))
      .toBe('APROBADO');
  });

  test('extrae el estatus cuando Kore lo manda en el campo Data en vez de Mensaje', () => {
    const err = koreError('irrelevante', { Data: 'No puede cambiar el estatus de la solicitud con estatus: APLICADO' });
    expect(estatusActualDeErrorKore(err)).toBe('APLICADO');
  });

  test('reconoce el caso que motivó este fix: estatus APLICADO (Kore ya avanzó más allá de lo pedido)', () => {
    expect(estatusActualDeErrorKore(koreError('No puede cambiar el estatus de la solicitud con estatus: APLICADO')))
      .toBe('APLICADO');
  });

  test('null si el error no tiene la forma "no puede cambiar el estatus..."', () => {
    expect(estatusActualDeErrorKore(koreError('Solicitud no encontrada'))).toBeNull();
  });

  test('null si el error no es un KoreCajaError', () => {
    expect(estatusActualDeErrorKore(new Error('cualquier otra cosa'))).toBeNull();
  });
});

describe('esErrorYaEnEstatus() — reintento idempotente exacto (comportamiento previo, sin cambios)', () => {
  test('true cuando Kore ya está EXACTAMENTE en el estatus pedido', () => {
    const err = koreError('No puede cambiar el estatus de la solicitud con estatus: RECHAZADO');
    expect(esErrorYaEnEstatus(err, 'RECHAZADO')).toBe(true);
  });

  test('false cuando Kore ya avanzó a un estatus DISTINTO del pedido (caso APLICADO vs APROBADO)', () => {
    const err = koreError('No puede cambiar el estatus de la solicitud con estatus: APLICADO');
    expect(esErrorYaEnEstatus(err, 'APROBADO')).toBe(false);
  });
});
