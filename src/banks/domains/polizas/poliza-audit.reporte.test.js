'use strict';

// Reporte .xlsx del Monitor de Pólizas (2026-10-20): verifica que
// construirReporteXlsx arma un libro válido con las 5 hojas a partir de los
// datos de las consultas (mockeadas), sin tocar DB.
const ExcelJS = require('exceljs');

// Mock de la capa de datos: cada consulta del reporte devuelve datos canónicos
// según el SQL que recibe.
jest.mock('../../../config/database.postgres', () => ({
  sequelize: {
    query: jest.fn(async (sql) => {
      if (/GROUP BY 1, accion/.test(sql) && /MAX\(fecha\) AS ultima/.test(sql)) {
        // resumenPorUsuario
        return [
          { usuario: 'ana', rol: 'admin', accion: 'creada', total: 3, ultima: '2026-10-19T10:00:00Z' },
          { usuario: 'ana', rol: 'admin', accion: 'cancelada', total: 1, ultima: '2026-10-19T12:00:00Z' },
        ];
      }
      if (/AT TIME ZONE 'America\/Mexico_City'\)::date/.test(sql)) {
        // actividadDiaria
        return [{ dia: '2026-10-19', accion: 'creada', total: 3 }];
      }
      if (/LEFT JOIN LATERAL/.test(sql)) {
        // cancelaciones
        return [{
          id: 1, polizaId: 9, usuarioNombre: 'ana', usuarioRol: 'admin', rfc: 'XAXX010101000',
          polizaTipo: 'D', polizaNumero: 12, polizaFecha: '2026-10-19', polizaConcepto: 'Ingresos por Cobranza',
          estadoAnterior: 'contabilizada', motivo: 'error', origen: 'manual', fecha: '2026-10-19T12:00:00Z',
          regeneradaNumero: 13, regeneradaEstado: 'contabilizada', regeneradaPor: 'ana', regeneradaEn: '2026-10-19T13:00:00Z',
        }];
      }
      // _bitacoraFull
      return [
        { id: 1, accion: 'creada', usuarioNombre: 'ana', usuarioRol: 'admin', rfc: 'XAXX010101000', polizaTipo: 'D', polizaNumero: 12, polizaConcepto: 'Ingresos por Cobranza', estadoAnterior: null, estadoNuevo: 'borrador', motivo: null, detalle: null, origen: 'hook', fecha: '2026-10-19T10:00:00Z' },
        { id: 2, accion: 'exportada', usuarioNombre: 'ana', usuarioRol: 'admin', rfc: 'XAXX010101000', polizaTipo: 'D', polizaNumero: 12, polizaConcepto: 'Ingresos por Cobranza', estadoAnterior: null, estadoNuevo: null, motivo: null, detalle: 'CONTPAQ', origen: 'export', fecha: '2026-10-19T11:00:00Z' },
      ];
    }),
  },
}));
jest.mock('../../../shared/models/postgres/PolizaAuditLog', () => ({ create: jest.fn() }));

const audit = require('./poliza-audit.service');

test('construirReporteXlsx arma un .xlsx con las 5 hojas y datos', async () => {
  const buffer = await audit.construirReporteXlsx({ desde: '2026-10-01', hasta: '2026-10-31' });
  expect(Buffer.isBuffer(Buffer.from(buffer))).toBe(true);
  expect(buffer.byteLength).toBeGreaterThan(1000);

  // Re-leer el libro y verificar hojas + algún contenido.
  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load(buffer);
  const hojas = wb.worksheets.map(w => w.name);
  expect(hojas).toEqual(['Resumen por usuario', 'Actividad diaria', 'Bitácora', 'Cancelaciones', 'Exportaciones']);

  // Resumen: ana con total 4 (3 creada + 1 cancelada).
  const wsR = wb.getWorksheet('Resumen por usuario');
  expect(wsR.getRow(2).getCell(1).value).toBe('ana');
  expect(wsR.getRow(2).getCell(3).value).toBe(4);

  // Exportaciones: solo la fila 'exportada'.
  const wsE = wb.getWorksheet('Exportaciones');
  expect(wsE.actualRowCount).toBe(2); // encabezado + 1
  expect(wsE.getRow(2).getCell(3).value).toBe('exportada');
});
