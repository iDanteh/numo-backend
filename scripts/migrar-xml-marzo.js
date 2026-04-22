'use strict';

/**
 * Script de migración: importa XMLs de una carpeta local a MongoDB
 * como CFDIs de source=SAT, ejercicio=2025, periodo=3 (Marzo).
 *
 * Uso:
 *   node scripts/migrar-xml-marzo.js <ruta-a-carpeta>
 *
 * Ejemplo:
 *   node scripts/migrar-xml-marzo.js "C:/Users/Daniel/Downloads/XMLsMarzo"
 *
 * - Solo procesa archivos .xml (ignora .pdf y otros)
 * - Si el UUID ya existe en MongoDB lo actualiza (upsert)
 * - Si el XML tiene InformacionGlobal.Mes distinto a 3, se respeta
 *   (la reclasificación automática lo corregirá después)
 * - Al finalizar muestra un resumen: nuevos / actualizados / errores
 */

require('../src/config/env');          // carga .env
const path    = require('path');
const fs      = require('fs');
const mongoose = require('mongoose');
const { parseCFDI } = require('../src/visor/services/cfdiParser');
const CFDI    = require('../src/visor/models/CFDI');
const { aplicarReclasificacion } = require('../src/visor/services/reclasificacionGlobal.service');
const config  = require('../src/config/env');

// ── Parámetros ────────────────────────────────────────────────────────────────
const CARPETA  = process.argv[2];
const SOURCE   = 'SAT';
const EJERCICIO = 2025;
const PERIODO   = 3;          // Marzo — se aplica solo si el XML no tiene InformacionGlobal

if (!CARPETA) {
  console.error('\n  Uso: node scripts/migrar-xml-marzo.js <ruta-a-carpeta>\n');
  process.exit(1);
}

if (!fs.existsSync(CARPETA)) {
  console.error(`\n  Carpeta no encontrada: ${CARPETA}\n`);
  process.exit(1);
}

// ── Helpers ───────────────────────────────────────────────────────────────────

const leerXMLsDeDir = (dir) => {
  const archivos = fs.readdirSync(dir).filter(f => f.toLowerCase().endsWith('.xml'));
  return archivos.map(f => ({ nombre: f, ruta: path.join(dir, f) }));
};

// ── Proceso principal ─────────────────────────────────────────────────────────

(async () => {
  await mongoose.connect(config.db.uri);
  console.log('\n  Conectado a MongoDB.');

  const archivos = leerXMLsDeDir(CARPETA);
  console.log(`  Archivos XML encontrados: ${archivos.length}\n`);

  if (archivos.length === 0) {
    console.log('  No hay XMLs que procesar. Saliendo.');
    await mongoose.disconnect();
    return;
  }

  let nuevos = 0, actualizados = 0;
  const errores = [];

  for (const { nombre, ruta } of archivos) {
    try {
      const xml = fs.readFileSync(ruta, 'utf8');
      const cfdiData = await parseCFDI(xml);

      if (!cfdiData.uuid) {
        errores.push({ nombre, error: 'UUID no encontrado en el XML' });
        continue;
      }

      // Período base: usar el del XML solo si el parser ya lo asignó,
      // de lo contrario usar los parámetros del script.
      // La reclasificación global corregirá facturas globales automáticamente.
      if (!cfdiData.ejercicio) cfdiData.ejercicio = EJERCICIO;
      if (!cfdiData.periodo)   cfdiData.periodo   = PERIODO;

      // Marcar como Vigente hasta verificación formal SAT
      if (!cfdiData.satStatus) cfdiData.satStatus = 'Vigente';

      // Guardar el XML original para búsquedas de InformacionGlobal
      cfdiData.xmlContent = xml;

      const prev = await CFDI.findOneAndUpdate(
        { uuid: cfdiData.uuid, source: SOURCE },
        { ...cfdiData, source: SOURCE, isActive: true },
        { upsert: true, new: false, setDefaultsOnInsert: true },
      );

      if (prev === null) {
        nuevos++;
        console.log(`  [NUEVO]        ${cfdiData.uuid}  →  ${nombre}`);
      } else {
        actualizados++;
        console.log(`  [ACTUALIZADO]  ${cfdiData.uuid}  →  ${nombre}`);
      }
    } catch (err) {
      errores.push({ nombre, error: err.message });
      console.error(`  [ERROR]        ${nombre}  →  ${err.message}`);
    }
  }

  // ── Reclasificación automática ─────────────────────────────────────────────
  const procesados = nuevos + actualizados;
  if (procesados > 0) {
    console.log('\n  Ejecutando reclasificación de facturas globales...');
    try {
      const reclas = await aplicarReclasificacion({ ejercicio: EJERCICIO, source: SOURCE });
      if (reclas.totalModificados > 0) {
        console.log(`  ✓ Reclasificadas ${reclas.totalModificados} facturas globales al periodo correcto.`);
      } else {
        console.log('  ✓ Sin facturas globales que reclasificar.');
      }
    } catch (err) {
      console.warn(`  ⚠ Reclasificación falló: ${err.message}`);
    }
  }

  // ── Resumen ────────────────────────────────────────────────────────────────
  console.log('\n  ══════════════════════════════════════════');
  console.log(`  Total archivos procesados : ${procesados}`);
  console.log(`  ✓ Nuevos                  : ${nuevos}`);
  console.log(`  ↺ Actualizados            : ${actualizados}`);
  console.log(`  ✗ Con error               : ${errores.length}`);
  if (errores.length) {
    console.log('\n  Errores:');
    errores.forEach(e => console.log(`    • ${e.nombre}: ${e.error}`));
  }
  console.log('  ══════════════════════════════════════════\n');

  await mongoose.disconnect();
})();
