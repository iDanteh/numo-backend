'use strict';

// VENTAS SHOPIFY (2026-11-05): la Tarjeta de clientes Shopify / ecommerce sale
// del total de Tarjeta en un renglón por almacén, sin comisión.
jest.mock('../../../shared/models/postgres', () => ({
  AccountPlan: {}, CfdiMappingRule: {}, PolizaMovimiento: {}, Poliza: {}, CentroCosto: {}, Terminal: {},
}));

const { _construirVentasShopifyInfo, _lineaVentasShopify, _esClienteShopify } = require('./poliza.service');

const B0 = { id: 2, clave: 'B0-CC', serieFacturacion: 'B0', sucursal: 'FERROCARRIL' };
const D0 = { id: 4, clave: 'D0-CC', serieFacturacion: 'D0', sucursal: 'HIDALGO' };
const mov = (id, ticket, debe, extra = {}) => {
  const [serieVentaTicket, folioVentaTicket] = ticket.split('-');
  return { id, tipoOrigen: 'Venta', formaPago: '04', debe, haber: 0, serieVentaTicket, folioVentaTicket, centroCostoObj: B0, ...extra };
};
const dia = new Date('2026-10-28T12:00:00Z');

test('solo la Tarjeta de los tickets de clientes Shopify, acumulada por almacén', async () => {
  const tickets = new Map([['B0-261000001', 'CLIENTE MOSTRADOR ROTOPLAS'], ['B0-261007885', 'CLIENTE MOSTRADOR SHOPIFY'], ['B0-261007380', 'MOSTRADOR ECOMMERCE CLIENTE'], ['D0-261005264', 'MOSTRADOR ECOMMERCE CLIENTE']]);
  const movs = [
    mov(1, 'B0-261007885', 473.05),
    mov(2, 'B0-261007380', 1314.74),                       // mixta: su línea de tarjeta
    mov(3, 'B0-261007380', 3500, { formaPago: '01' }),     // y su efectivo NO entra
    mov(4, 'B0-261009999', 800),                           // otro cliente
    mov(5, 'D0-261005264', 98.46, { formaPago: '28', centroCostoObj: D0 }),
    mov(6, 'B0-261007885', 100, { metodoPago: 'PPD' }),    // crédito: no
    mov(7, 'B0-261000001', 250),                           // ROTOPLAS: fuera (2026-11-06)
  ];
  let diaPedido = null;
  const r = await _construirVentasShopifyInfo(movs, dia, async d => { diaPedido = d; return tickets; });
  expect(diaPedido).toBe('2026-10-28');
  expect([...r.ids].sort()).toEqual([1, 2, 5]);
  // 2026-11-06: un renglón por almacén Y cliente — SHOPIFY → STRIPE, ECOMMERCE → SHOPIFY PAYMENTS.
  expect(r.porCentro.get('2|STRIPE').gross).toBe(473.05);
  expect(r.porCentro.get('2|SHOPIFY PAYMENTS').gross).toBe(1314.74);
  expect(r.porCentro.get('4|SHOPIFY PAYMENTS').gross).toBe(98.46);
  expect(_lineaVentasShopify(r.porCentro.get('2|STRIPE'), { codigo: '1102011001' })).toMatchObject({
    cuenta: { codigo: '1102011001' }, serie: 'STRIPE', concepto: 'CLIENTE MOSTRADOR SHOPIFY', centroCosto: 'B0-CC', debe: 473.05, haber: 0,
  });
  expect(_lineaVentasShopify(r.porCentro.get('2|SHOPIFY PAYMENTS'), { codigo: '1102011001' })).toMatchObject({
    serie: 'SHOPIFY PAYMENTS', concepto: 'MOSTRADOR ECOMMERCE CLIENTE', centroCosto: 'B0-CC', debe: 1314.74,
  });
});

test('si Kore falla, la Tarjeta se queda en el total (sin separar)', async () => {
  const r = await _construirVentasShopifyInfo([mov(1, 'B0-261007885', 473.05)], dia, async () => { throw new Error('429'); });
  expect(r.ids.size).toBe(0);
  expect(r.porCentro.size).toBe(0);
});

test('sin Tarjeta no consulta a Kore', async () => {
  const consulta = jest.fn();
  const r = await _construirVentasShopifyInfo([mov(1, 'B0-261007885', 50, { formaPago: '01' })], dia, consulta);
  expect(consulta).not.toHaveBeenCalled();
  expect(r.ids.size).toBe(0);
});

test('solo los dos clientes exactos (la palabra ECOMMERCE sola no basta)', () => {
  expect(_esClienteShopify('MOSTRADOR ECOMMERCE CLIENTE')).toBe(true);
  expect(_esClienteShopify(' cliente mostrador  shopify ')).toBe(true);
  expect(_esClienteShopify('CLIENTE MOSTRADOR ROTOPLAS')).toBe(false); // fuera (2026-11-06)
  expect(_esClienteShopify('DANIEL LOPEZ GARCIA ECOMMERCE')).toBe(false);
  expect(_esClienteShopify('CLIENTE MOSTRADOR')).toBe(false);
});
