'use strict';

// diferenciasContraXml: una copia intacta no cambia; una pisada por el Excel
// del SAT (conceptos sin impuestos, relación 04 supuesta, fecha sin hora)
// se restaura solo en esos campos.

const CFDI = require('../models/CFDI');
const { parseCFDI } = require('../services/cfdiParser');
const { diferenciasContraXml } = require('./reparar-sat-desde-xml');

const XML = `<?xml version="1.0" encoding="UTF-8"?>
<cfdi:Comprobante xmlns:cfdi="http://www.sat.gob.mx/cfd/4" xmlns:tfd="http://www.sat.gob.mx/TimbreFiscalDigital" Version="4.0" Serie="H0" Folio="261000130" Fecha="2026-10-07T15:19:54" FormaPago="01" MetodoPago="PUE" TipoDeComprobante="E" SubTotal="1864.29" Descuento="0.00" Total="2162.58" Moneda="MXN" LugarExpedicion="70760">
  <cfdi:CfdiRelacionados TipoRelacion="03"><cfdi:CfdiRelacionado UUID="8FD4E688-608A-46B9-87FD-DC70D1940883"/></cfdi:CfdiRelacionados>
  <cfdi:Emisor Rfc="CCO011113663" Nombre="EMISOR" RegimenFiscal="601"/>
  <cfdi:Receptor Rfc="XAXX010101000" Nombre="PUBLICO EN GENERAL" UsoCFDI="S01" DomicilioFiscalReceptor="70760" RegimenFiscalReceptor="616"/>
  <cfdi:Conceptos>
    <cfdi:Concepto ClaveProdServ="84111506" Cantidad="1" ClaveUnidad="ACT" Descripcion="CANCELACION DE CLIENTE" ValorUnitario="1864.29" Importe="1864.29" ObjetoImp="02">
      <cfdi:Impuestos><cfdi:Traslados><cfdi:Traslado Base="1864.29" Impuesto="002" TipoFactor="Tasa" TasaOCuota="0.160000" Importe="298.29"/></cfdi:Traslados></cfdi:Impuestos>
    </cfdi:Concepto>
  </cfdi:Conceptos>
  <cfdi:Impuestos TotalImpuestosTrasladados="298.29"><cfdi:Traslados><cfdi:Traslado Base="1864.29" Impuesto="002" TipoFactor="Tasa" TasaOCuota="0.160000" Importe="298.29"/></cfdi:Traslados></cfdi:Impuestos>
  <cfdi:Complemento><tfd:TimbreFiscalDigital Version="1.1" UUID="E587E3C2-29BE-4A87-80AB-64A22213E5AD" FechaTimbrado="2026-10-07T15:19:56" RfcProvCertif="AAA010101AAA" SelloCFD="abc" NoCertificadoSAT="0001" SelloSAT="xyz"/></cfdi:Complemento>
</cfdi:Comprobante>`;

let parsed;
beforeAll(async () => { parsed = await parseCFDI(XML); });

// Como quedó guardada por la descarga SAT (pasando por el schema).
const intacta = () => new CFDI({ ...parsed, source: 'SAT' }).toObject();

test('copia intacta: sin diferencias', () => {
  expect(diferenciasContraXml(intacta(), parsed).campos).toEqual([]);
});

test('copia pisada por el Excel: restaura conceptos, impuestos, relación, fecha y timbre', () => {
  const pisada = {
    ...intacta(),
    fecha: new Date('2026-10-07T00:00:00Z'),
    conceptos: [{ descripcion: 'CANCELACION DE CLIENTE' }],
    impuestos: { totalImpuestosTrasladados: 298.29, totalImpuestosRetenidos: 0, traslados: [{ impuesto: '002', tipoFactor: 'Tasa', tasaOCuota: 0.16, importe: 298.29 }], retenciones: [] },
    cfdiRelacionados: [{ tipoRelacion: '04', uuids: ['8FD4E688-608A-46B9-87FD-DC70D1940883'] }],
    timbreFiscalDigital: { fechaTimbrado: new Date('2026-10-07T15:19:56Z') },
  };
  const { set, campos } = diferenciasContraXml(pisada, parsed);
  expect(campos.sort()).toEqual(['cfdiRelacionados', 'conceptos', 'fecha', 'impuestos', 'timbreFiscalDigital']);
  expect(set.cfdiRelacionados[0].tipoRelacion).toBe('03');
  expect(set.conceptos[0]).toMatchObject({ claveProdServ: '84111506', importe: 1864.29 });
  expect(set.impuestos.traslados ?? []).toEqual([]);
  expect(set.timbreFiscalDigital.selloSAT).toBe('xyz');
  expect(set).not.toHaveProperty('satStatus');
});
