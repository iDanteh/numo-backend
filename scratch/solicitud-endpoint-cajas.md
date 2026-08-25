# Solicitud a equipo de cajas — endpoint por fecha/sucursal

## Problema común (4 casos distintos, misma causa raíz)

Todos los endpoints actuales de `/desgloses-cobro/*` requieren conocer **de
antemano** la serie/folio de una venta específica para poder consultar algo.
Numo (el sistema de conciliación/pólizas) solo conoce las ventas que **ya
tienen un CFDI generado** — pero varias situaciones reales de negocio generan
actividad en cajas que nunca llega a tener (o tarda días en tener) un CFDI
propio:

1. **Pendientes por facturar**: tickets cobrados el mismo día que nunca se
   facturan individualmente (quedan sueltos, sin ninguna factura ligada).
2. **Saldo a favor generado**: una Devolución/Cancelación en cajas (ej. serie
   `CAC`) puede generar saldo a favor para un ticket que nunca tuvo CFDI
   propio — no hay forma de encontrar esa devolución sin conocer ya su
   referencia exacta.
3. **Puntos (Club Tuberos) redimidos**: la mayoría de las redenciones reales
   ocurren en tickets sueltos ("TICKET SENCILLO") sin CFDI propio.
4. **Facturas Globales**: agrupan cientos de tickets individuales bajo un solo
   CFDI. Cuando ALGUNOS de esos tickets se cobraron en otra sucursal, Numo
   necesita saber el desglose Efectivo/Tarjeta/Puntos/SF del RESTO (los
   cobrados en la misma sucursal) — pero la Global no es "una venta" en cajas,
   no tiene su propio folio de venta que consultar.

## Lo que se necesita

Un endpoint que acepte **fecha + sucursal** (no folio) y regrese todos los
tickets/ventas de ese día en esa sucursal, con su desglose real de cobro
(mismo formato que ya trae `/desgloses-cobro/almacen` y
`/desgloses-cobro/saldos-favor`, solo que filtrable por fecha/sucursal en vez
de por folio conocido).

Ejemplo de forma sugerida (mismo estilo que los endpoints actuales):

```
GET /desgloses-cobro/almacen?serie=B0&fecha=2026-07-10
GET /desgloses-cobro/saldos-favor?serie=B0&fecha=2026-07-10
```

Con esto, Numo podría:
- Encontrar tickets sin CFDI (pendientes por facturar) directamente.
- Encontrar saldos a favor generados/usados sin depender de que la Devolución
  tenga CFDI propio.
- Encontrar redenciones de Puntos reales del día, no solo las que coinciden
  con una factura ya conocida.
- Partir correctamente el remanente de una Factura Global en Efectivo/
  Tarjeta/Puntos/SF, y saber qué parte de sus tickets pertenece realmente a
  ese día (una Global puede agrupar tickets de días distintos).

## Estado actual (mientras no exista el endpoint)

Los 4 casos se manejan de forma parcial/best-effort usando los endpoints
existentes (por folio conocido) — correcto para el subconjunto que sí
podemos encontrar, pero sabidamente incompleto para el resto. Documentado en
el proyecto Numo, memoria de sesión 2026-08-06/07.
