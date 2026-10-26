'use strict';

/**
 * shared/models/postgres/LoyaltyCard.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — tarjeta de puntos identificada por teléfono (D1: el
 * teléfono es la única llave de identidad; la UI la sigue llamando "tarjeta").
 * Ver openspec/changes/sistema-lealtad/design.md "Data Model".
 */

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const LoyaltyCard = sequelize.define('LoyaltyCard', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  telefono: {
    type:      DataTypes.STRING(10),
    allowNull: false,
    unique:    true,
    validate:  { is: /^\d{10}$/ },
    comment:   'Única llave de identidad de la tarjeta (D1). 10 dígitos, sin lada ni símbolos.',
  },
  nipHash: {
    type:      DataTypes.STRING(100),
    allowNull: false,
    comment:   'Hash bcrypt del NIP (D6). El texto plano nunca se persiste.',
  },
  nombre: {
    type:      DataTypes.STRING(200),
    allowNull: false,
    comment:   'Nombre del cliente dueño de la tarjeta.',
  },
  nivel: {
    type:         DataTypes.STRING(10),
    allowNull:    false,
    defaultValue: 'Bronce',
    validate:     { isIn: [['Bronce', 'Plata', 'Oro']] },
    comment:      'Nivel actual de la tarjeta (D2: STRING + isIn, no ENUM — ver rationale en design.md).',
  },
  saldoPuntos: {
    type:         DataTypes.DECIMAL(12, 2),
    allowNull:    false,
    defaultValue: 0,
    comment:      'Saldo denormalizado de puntos. Respaldado por CHECK saldo_puntos >= 0 (D4).',
  },
  activa: {
    type:         DataTypes.BOOLEAN,
    allowNull:    false,
    defaultValue: true,
  },
  nipIntentosFallidos: {
    type:         DataTypes.INTEGER,
    allowNull:    false,
    defaultValue: 0,
    comment:      'Contador de intentos fallidos consecutivos del NIP en la consulta pública (D7).',
  },
  nipBloqueadoHasta: {
    type:      DataTypes.DATE,
    allowNull: true,
    comment:   'Bloqueo temporal tras 5 intentos fallidos (D7): 15 min desde el 5to fallo.',
  },
  recuperacionCodigoHash: {
    type:      DataTypes.STRING(100),
    allowNull: true,
    comment:   'Hash bcrypt del código de verificación de 6 dígitos (recuperación de NIP en 2 pasos). El texto plano nunca se persiste. Null cuando no hay recuperación en curso.',
  },
  recuperacionExpiraEn: {
    type:      DataTypes.DATE,
    allowNull: true,
    comment:   'Vencimiento del código de recuperación (10 min desde que se solicitó).',
  },
  recuperacionIntentosFallidos: {
    type:         DataTypes.INTEGER,
    allowNull:    false,
    defaultValue: 0,
    comment:      'Contador de intentos fallidos consecutivos del código de recuperación. Independiente del contador de NIP.',
  },
  recuperacionBloqueadaHasta: {
    type:      DataTypes.DATE,
    allowNull: true,
    comment:   'Bloqueo temporal tras 5 intentos fallidos del código de recuperación: 15 min desde el 5to fallo.',
  },
  createdById: {
    type:      DataTypes.STRING(200),
    allowNull: true,
  },
  createdByNombre: {
    type:      DataTypes.STRING(200),
    allowNull: true,
  },
  updatedById: {
    type:      DataTypes.STRING(200),
    allowNull: true,
    comment:   'Quién hizo la última edición de teléfono/nombre/activa. Alcanza con quién/cuándo (decisión confirmada, ver design.md), sin tabla de historial completo.',
  },
  updatedByNombre: {
    type:      DataTypes.STRING(200),
    allowNull: true,
  },
  // ── Match de clientes con Kore (monedero externo), ver kore-match/ ─────────
  tarjetaLegada: {
    // STRING(50), no 20: `normalizarTarjeta()` devuelve valores cortos (numéricos o "CF-####")
    // para el formato esperado, pero su propio fallback documentado preserva tal cual cualquier
    // Tarjeta que no matchee ninguno de los dos patrones ("nunca se descartan") — un valor de
    // origen inesperadamente largo no debe tronar el import. Ensanchado 2026-10-23 junto con el
    // resto de columnas de enriquecimiento, mismo incidente real (ver numExterior más abajo).
    type:      DataTypes.STRING(50),
    allowNull: true,
    comment:   'Número de Tarjeta del CSV legado ("Club Tuberos"), normalizado con normalizarTarjeta() — llave de cruce contra el `monedero` de Kore (GET /clientes/monedero). Null para tarjetas creadas sin ese dato (gap histórico ya corregido en el import).',
  },
  koreClienteId: {
    type:      DataTypes.STRING(50),
    allowNull: true,
    comment:   'Id interno de Kore (GET /clientes/monedero → Data.clientes[].id), tal cual, nunca reconstruido — mismo criterio que erpId en Solicitudes de Cobro. Null hasta que el match de kore-match/ encuentra una coincidencia.',
  },
  koreMonedero: {
    // STRING(50), no 20 — mismo margen de seguridad que tarjetaLegada (ensanchado 2026-10-23),
    // aunque el `monedero` real de Kore observado hasta ahora siempre es corto.
    type:      DataTypes.STRING(50),
    allowNull: true,
    comment:   'monedero de Kore tal cual (solo auditoría/trazabilidad del match — nunca se usa para lógica de negocio).',
  },
  // Apellidos (2026-10-23) — enfoque ADITIVO: `nombre` sigue siendo el combinado "Nombre
  // Apellidos" que usa TODO el sistema (listados/NIP/WhatsApp/búsqueda/Excel/kore-match), esta
  // columna es solo para granularidad estructurada (ej. reportes, alta/edición manual). Visible
  // por defaultScope (no PII más sensible que `nombre`, que ya es visible).
  apellidos: {
    type:      DataTypes.STRING(200),
    allowNull: true,
  },
  // ── Enriquecimiento del import de clientes.csv (2026-10-23) — registro histórico simple, nunca
  // usado para lógica de negocio (nivel/puntos/identidad siguen siendo teléfono+categoría). Ver
  // loyalty-import.parser.js para las columnas reales del CSV.
  apodo: {
    type:      DataTypes.STRING(200),
    allowNull: true,
  },
  sexo: {
    // STRING(50), no 20 — ensanchado 2026-10-23 (ver numExterior más abajo), por si el CSV trae
    // un valor largo ("Prefiero no decir", etc.) en vez de M/F.
    type:      DataTypes.STRING(50),
    allowNull: true,
  },
  // STRING y no DATEONLY a propósito: el formato real de "Fecha nacimiento" en clientes.csv no
  // está confirmado consistente (ver loyalty-import.parser.js, que tampoco la parsea) — persistir
  // el texto crudo evita introducir un bug de parseo de fecha sobre un dato que hoy es solo
  // historial, nunca usado para lógica de negocio.
  fechaNacimiento: {
    // STRING(50), no 20 — mismo criterio: texto crudo sin formato garantizado, ensanchado
    // 2026-10-23 junto con numExterior tras el incidente real de import.
    type:      DataTypes.STRING(50),
    allowNull: true,
  },
  // Catálogo configurable (Configuraciones Globales, sección `lealtad`, clave ESPECIALIDADES) —
  // el valor del CSV se persiste tal cual, sin validar contra el catálogo (ver
  // loyalty-import.service.js). El catálogo es de referencia, no bloqueante.
  especialidades: {
    type:      DataTypes.STRING(300),
    allowNull: true,
  },
  sucursal: {
    type:      DataTypes.STRING(100),
    allowNull: true,
  },
  calle: {
    type:      DataTypes.STRING(200),
    allowNull: true,
  },
  // STRING(100), no 20 — INCIDENTE REAL 2026-10-23: cliente con Num exterior "CASA 13
  // CONDOMINIO 26" (21 caracteres) reventó el import con VARCHAR(20) ("el valor es demasiado
  // largo para el tipo character varying(20)"). Las direcciones reales de este CSV mezclan
  // casa/condominio/manzana en este campo, no son solo un número de puerta.
  numExterior: {
    type:      DataTypes.STRING(100),
    allowNull: true,
  },
  numInterior: {
    // Mismo criterio que numExterior — ensanchado preventivamente, mismo patrón de datos.
    type:      DataTypes.STRING(100),
    allowNull: true,
  },
  colonia: {
    type:      DataTypes.STRING(150),
    allowNull: true,
  },
  municipio: {
    type:      DataTypes.STRING(150),
    allowNull: true,
  },
  estado: {
    type:      DataTypes.STRING(100),
    allowNull: true,
  },
  cp: {
    type:      DataTypes.STRING(10),
    allowNull: true,
  },
  comentarios: {
    type:      DataTypes.TEXT,
    allowNull: true,
  },
}, {
  tableName:   'loyalty_cards',
  underscored: true,
  defaultScope: {
    // CRÍTICO (hallazgo de revisión 2026-10-23): `loyalty-card.service.js#listar/obtener` —
    // detrás de `GET /cards`/`GET /cards/:id`, permiso `loyalty:read` que tienen contabilidad y
    // cobranza como trabajo rutinario — no aplicaba NINGÚN whitelist de columnas. Antes del
    // enriquecimiento del import (2026-10-23) eso exponía solo teléfono/nombre/saldo/nivel; con
    // las 13 columnas nuevas hubiera expuesto fecha de nacimiento y domicilio completo a esos
    // roles sin querer. Se excluyen acá, a nivel de modelo, para que CUALQUIER lectura futura que
    // no pase explícitamente por un scope quede protegida por default (fail-safe), no solo las dos
    // rutas ya conocidas. `especialidades`/`sucursal` quedan visibles — son dato de catálogo/
    // negocio, no PII sensible.
    attributes: {
      exclude: [
        'nipHash', 'recuperacionCodigoHash',
        'apodo', 'sexo', 'fechaNacimiento', 'calle', 'numExterior', 'numInterior',
        'colonia', 'municipio', 'estado', 'cp', 'comentarios',
      ],
    },
  },
  scopes: {
    withNipHash: {
      attributes: { include: ['nipHash'] },
    },
    withRecuperacionCodigoHash: {
      attributes: { include: ['recuperacionCodigoHash'] },
    },
    // Uso interno del propio dominio loyalty (ej. backfill de enriquecimiento en
    // loyalty-import-apply.service.js) — nunca para servir una respuesta HTTP directa.
    withDatosPersonales: {
      attributes: {
        include: [
          'apodo', 'sexo', 'fechaNacimiento', 'calle', 'numExterior', 'numInterior',
          'colonia', 'municipio', 'estado', 'cp', 'comentarios',
        ],
      },
    },
  },
});

module.exports = LoyaltyCard;
