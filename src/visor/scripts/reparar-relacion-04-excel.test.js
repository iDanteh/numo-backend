'use strict';

// Relación '04' supuesta por la carga de Excel en copias SAT sin XML: toma el
// tipo del ERP; sin relación en el ERP queda sin tipo. Caso real 12-oct
// C0-261000642 → C0-261000641 (ERP: 07, aplicación de anticipo).
const { relacionesCorregidas } = require('./reparar-relacion-04-excel');

const A = 'C42E0C8A-C940-428D-B6CB-99F3E9C8D2A3';

test('ERP dice 07 → 07', () => {
  const r = relacionesCorregidas([{ tipoRelacion: '04', uuids: [A] }], [{ tipoRelacion: '07', uuids: [A] }]);
  expect(r).toEqual({ cambio: true, nuevas: [{ tipoRelacion: '07', uuids: [A] }] });
});

test('ERP sin esa relación (o sin copia ERP) → sin tipo', () => {
  expect(relacionesCorregidas([{ tipoRelacion: '04', uuids: [A] }], undefined).nuevas[0].tipoRelacion).toBeNull();
  expect(relacionesCorregidas([{ tipoRelacion: '04', uuids: [A] }], [{ tipoRelacion: '01', uuids: ['OTRO'] }]).nuevas[0].tipoRelacion).toBeNull();
});

test('ERP también dice 04 → se queda igual', () => {
  expect(relacionesCorregidas([{ tipoRelacion: '04', uuids: [A] }], [{ tipoRelacion: '04', uuids: [A.toLowerCase()] }])).toEqual({ cambio: false, nuevas: [{ tipoRelacion: '04', uuids: [A] }] });
});
