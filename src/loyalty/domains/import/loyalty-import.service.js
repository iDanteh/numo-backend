'use strict';

/**
 * domains/import/loyalty-import.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — cruce de clientes.csv x puntos.pdf (migración desde
 * "Club Tuberos"). `construirConcentrado` es una función PURA: no hace I/O,
 * no toca la base de datos, no crea tarjetas ni movimientos — solo arma un
 * reporte revisable para la pantalla de importación (fuera de alcance de
 * este cambio: aplicar la migración, y el mapeo Categoria→Nivel, pendiente
 * de decisión del negocio).
 */

const { BadRequestError } = require('../../../shared/errors/AppError');

// Decisión de negocio 2026-10-19: Categoria no reconocida (ej. "Profesionales",
// "Público en general") NO bloquea la migración — se asigna el nivel más bajo
// por default. Es una asignación provisional, pendiente de revisión posterior
// por parte del negocio (no es un mapeo real, es "no tenemos el dato, arranca
// en el piso"). Se marca con `info: ['nivel_asignado_por_default']`, nunca
// como advertencia, porque ya no requiere intervención para poder migrar.
const NIVELES_VALIDOS_MAP = { bronce: 'Bronce', plata: 'Plata', oro: 'Oro' };
const NIVEL_DEFAULT = 'Bronce';

function _resolverNivel(categoriaOriginal) {
  const trimmed = String(categoriaOriginal ?? '').trim().toLowerCase();
  const nivelReconocido = NIVELES_VALIDOS_MAP[trimmed];
  if (nivelReconocido) {
    return { nivelSugerido: nivelReconocido, asignadoPorDefault: false };
  }
  return { nivelSugerido: NIVEL_DEFAULT, asignadoPorDefault: true };
}

// Umbral conservador para el heurístico de "posible registro de prueba" —
// además del match por nombre ("no borrar"/"prueba"), un saldo anormalmente
// alto es una segunda señal débil (ej. el registro real "CLIENTE PRUEBA2 no
// borrar" trae 238,620, muy por encima de los saldos reales observados en el
// reporte). Deliberadamente NO se hardcodea el nombre — es un heurístico, no
// una exclusión: la fila se marca para que un humano decida, nunca se
// descarta sola.
const SALDO_SOSPECHOSO_UMBRAL = 100000;
const NOMBRE_PRUEBA_RE = /no\s*borrar|\bprueba\b/i;

function _esPosibleRegistroPrueba(nombre, saldo) {
  if (nombre && NOMBRE_PRUEBA_RE.test(nombre)) return true;
  if (typeof saldo === 'number' && saldo >= SALDO_SOSPECHOSO_UMBRAL) return true;
  return false;
}

/**
 * Cruza clientesRows (clientes.csv ya parseado) x puntosRows (puntos.pdf ya
 * parseado) por `tarjetaNormalizada` y arma el concentrado de revisión.
 *
 * @param {object} params
 * @param {Array}  [params.clientesRows]
 * @param {Array}  [params.puntosRows]
 */
function construirConcentrado({ clientesRows, puntosRows } = {}) {
  const clientes = Array.isArray(clientesRows) ? clientesRows : [];
  const puntos = Array.isArray(puntosRows) ? puntosRows : [];

  if (clientes.length === 0 && puntos.length === 0) {
    throw new BadRequestError('Debes subir al menos un archivo (clientes.csv o reporteClientesPorVendedor.xlsx).');
  }

  // ── Agrupar clientes por tarjetaNormalizada (detecta duplicados) ─────────
  const clientesPorTarjeta = new Map();
  for (const c of clientes) {
    const key = c.tarjetaNormalizada ?? '';
    if (!clientesPorTarjeta.has(key)) clientesPorTarjeta.set(key, []);
    clientesPorTarjeta.get(key).push(c);
  }

  // ── Agrupar puntos por tarjetaNormalizada (acceso a saldo + detectar huérfanos) ──
  const puntosPorTarjeta = new Map();
  for (const p of puntos) {
    const key = p.tarjetaNormalizada ?? '';
    if (!puntosPorTarjeta.has(key)) puntosPorTarjeta.set(key, []);
    puntosPorTarjeta.get(key).push(p);
  }

  const filas = [];

  let totalConSaldo = 0;
  let totalSinSaldo = 0;
  let totalDuplicadosEnClientes = 0;
  let totalPosiblesPrueba = 0;
  let totalNivelPorDefault = 0;
  let totalSinContacto = 0;
  let totalNombreProvisional = 0;

  // ── Filas con origen en clientes.csv ─────────────────────────────────────
  for (const c of clientes) {
    const key = c.tarjetaNormalizada ?? '';
    const grupo = clientesPorTarjeta.get(key) ?? [];
    const esDuplicado = grupo.length > 1;

    const matchesPuntos = puntosPorTarjeta.get(key) ?? [];
    const tienePuntos = matchesPuntos.length > 0;
    const saldo = tienePuntos ? matchesPuntos[0].saldo : 0;
    // Activo/Inactivo controlado por reporteClientesPorVendedor.xlsx (2026-10-29, pedido
    // explícito del usuario): la sola PRESENCIA de la Tarjeta en ese reporte determina
    // `activa`, sin importar el saldo (incluso 0) — reusa `tienePuntos`, que es exactamente esa
    // señal de presencia. A diferencia de TODOS los demás campos de enriquecimiento (que solo
    // rellenan si están vacíos, ver CAMPOS_ENRIQUECIMIENTO en loyalty-import-apply.service.js),
    // este SÍ sobrescribe en cada corrida — incluidas tarjetas 'ya_migrada' (ver _aplicarFila).
    const activaSugerida = tienePuntos;

    const advertencias = [];
    const info = [];

    // Decisión de negocio 2026-10-19: un cliente sin NINGÚN teléfono (ni
    // Celular ni Telefono) no bloquea la migración — queda pendiente, con
    // aviso, para que el negocio le actualice el dato. Si trae Telefono
    // (aunque Celular esté vacío), ese es el contacto a usar — el sistema
    // nuevo exige *un* teléfono para identificar la tarjeta, no específicamente
    // el de celular.
    const celular = c.celular ? String(c.celular).trim() : '';
    const telefono = c.telefono ? String(c.telefono).trim() : '';
    const contacto = celular || telefono;
    if (!contacto) {
      advertencias.push('sin_contacto');
      totalSinContacto += 1;
    }

    if (esDuplicado) {
      advertencias.push('tarjeta_duplicada_en_clientes');
      totalDuplicadosEnClientes += 1;
    }

    const { nivelSugerido, asignadoPorDefault } = _resolverNivel(c.categoriaOriginal);
    if (asignadoPorDefault) {
      info.push('nivel_asignado_por_default');
      totalNivelPorDefault += 1;
    }

    if (_esPosibleRegistroPrueba(c.nombre, saldo)) {
      advertencias.push('posible_registro_prueba');
      totalPosiblesPrueba += 1;
    }

    if (tienePuntos) {
      totalConSaldo += 1;
    } else {
      totalSinSaldo += 1;
      info.push('sin_registro_de_saldo');
    }

    // Decisión de negocio 2026-10-23 (pedido explícito del usuario, "maximizar la cantidad de
    // clientes importados"): Nombre+Apellidos vacíos en clientes.csv ya no bloquea la migración
    // (antes reventaba en `cardService.crearTarjeta` con "nombre es requerido", excluyendo del
    // todo a un cliente real que SÍ tiene monedero en Kore). Se arma un nombre provisional a
    // partir del contacto — el mismo criterio que `nivelSugerido`/`nivel_asignado_por_default`
    // arriba: dato no disponible, se rellena con un default visible y auditable (`info`, nunca
    // `advertencias`, porque ya no requiere intervención para poder migrar) en vez de bloquear.
    // Si tampoco hay contacto, esta fila ya cayó en 'sin_contacto' arriba y nunca llega a crearse
    // como tarjeta (queda 'pendiente_sin_telefono') — acá siempre hay un contacto real disponible.
    let nombreResuelto = c.nombre ?? '';
    if (!nombreResuelto.trim()) {
      nombreResuelto = contacto ? `Cliente ${contacto}` : '';
      if (nombreResuelto) {
        info.push('nombre_provisional');
        totalNombreProvisional += 1;
      }
    }

    filas.push({
      tarjetaNormalizada: key,
      tarjetaRawClientes: c.tarjetaRaw ?? '',
      tarjetaRawPuntos: tienePuntos ? matchesPuntos[0].tarjetaRaw : null,
      nombre: nombreResuelto,
      apellidos: c.apellidos ?? '',
      celular: c.celular ?? '',
      telefono: c.telefono ?? '',
      correo: c.correo ?? '',
      contacto,
      saldo,
      activaSugerida,
      categoriaOriginal: c.categoriaOriginal ?? '',
      nivelSugerido,
      advertencias,
      info,
      // Enriquecimiento (2026-10-23) — registro histórico simple, ver loyalty-import.parser.js.
      apodo: c.apodo ?? '',
      sexo: c.sexo ?? '',
      fechaNacimiento: c.fechaNacimiento ?? '',
      especialidades: c.especialidades ?? '',
      sucursal: c.sucursal ?? '',
      calle: c.calle ?? '',
      numExterior: c.numExterior ?? '',
      numInterior: c.numInterior ?? '',
      colonia: c.colonia ?? '',
      municipio: c.municipio ?? '',
      estado: c.estado ?? '',
      cp: c.cp ?? '',
      comentarios: c.comentarios ?? '',
    });
  }

  // ── Filas huérfanas: puntos.pdf sin match en clientes.csv ────────────────
  // Usa el mismo agrupador por tarjeta (no solo tarjetas con clientes.length
  // === 0) para no perder huérfanos cuando clientesRows viene vacío/ausente.
  let totalHuerfanosEnPuntos = 0;
  for (const p of puntos) {
    const key = p.tarjetaNormalizada ?? '';
    const tieneCliente = (clientesPorTarjeta.get(key) ?? []).length > 0;
    if (tieneCliente) continue;

    totalHuerfanosEnPuntos += 1;
    totalSinContacto += 1; // sin fila en clientes.csv → no hay Celular ni Telefono posibles

    const advertencias = ['tarjeta_sin_match_en_puntos', 'sin_contacto'];
    if (_esPosibleRegistroPrueba(p.nombre, p.saldo)) {
      advertencias.push('posible_registro_prueba');
      totalPosiblesPrueba += 1;
    }

    filas.push({
      tarjetaNormalizada: key,
      tarjetaRawClientes: null,
      tarjetaRawPuntos: p.tarjetaRaw ?? '',
      nombre: p.nombre ?? '',
      celular: '',
      telefono: '',
      correo: '',
      contacto: '',
      saldo: p.saldo,
      // Huérfana de reporteClientesPorVendedor.xlsx: está en el reporte (es justo por eso que es
      // huérfana, no tiene match en clientes.csv) → activa por definición.
      activaSugerida: true,
      categoriaOriginal: null,
      nivelSugerido: NIVEL_DEFAULT,
      advertencias,
      info: ['nivel_asignado_por_default'],
      // Huérfano de puntos.pdf: ninguno de estos campos existe en ese archivo (solo clientes.csv
      // los trae) — se dejan vacíos por consistencia de shape con las filas de arriba.
      apellidos: '', apodo: '', sexo: '', fechaNacimiento: '', especialidades: '', sucursal: '', calle: '',
      numExterior: '', numInterior: '', colonia: '', municipio: '', estado: '', cp: '', comentarios: '',
    });
  }

  return {
    resumen: {
      totalClientes: clientes.length,
      totalConSaldo,
      totalSinSaldo,
      totalHuerfanosEnPuntos,
      totalDuplicadosEnClientes,
      totalPosiblesPrueba,
      totalNivelPorDefault,
      totalSinContacto,
      totalNombreProvisional,
    },
    filas,
  };
}

module.exports = {
  construirConcentrado,
  // Exportado para loyalty-import-apply.service.js#construirFilasDesdeCorrecciones — reutiliza
  // la misma resolución Categoria→Nivel que usa construirConcentrado, en vez de duplicarla.
  _resolverNivel,
};
