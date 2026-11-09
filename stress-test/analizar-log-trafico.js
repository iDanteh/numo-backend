'use strict';

/**
 * stress-test/analizar-log-trafico.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Convierte un extracto real de logs (logs/app.log O `docker logs`, producción)
 * en números concretos para calibrar k6-otp-flow.js con tráfico de fondo FIEL —
 * distribución real de endpoints, pico real de requests/minuto, y mezcla real
 * de status codes — en vez de los valores inventados que usamos de arranque.
 *
 * Formato esperado por línea (winston envolviendo morgan 'combined', ver
 * shared/utils/logger.js) — el prefijo de winston varía según el transport
 * (archivo: "YYYY-MM-DD HH:mm:ss", consola/docker logs: solo "HH:mm:ss"), por
 * eso la fecha/hora real que se usa para agrupar por minuto es la del PROPIO
 * log de Apache entre corchetes, no el prefijo de winston:
 *   HH:mm:ss [info]: <ip> - - [DD/Mon/YYYY:HH:mm:ss +0000] "MET /ruta HTTP/1.1" 200 1532 "-" "UA"
 * El status puede ser "-" (request abortada del lado del cliente, sin
 * res.statusCode) en vez de un número — se cuenta aparte, no se descarta.
 *
 * Uso: node stress-test/analizar-log-trafico.js <archivo-con-extracto-de-log.txt>
 *      (o por stdin: cat extracto.txt | node stress-test/analizar-log-trafico.js)
 *
 * Nunca requiere ni toca la base de datos ni el backend — es un script de
 * solo lectura sobre texto, para correr con cualquier extracto que te pasen.
 */

const fs = require('fs');

// Captura: 1=fecha Apache (DD/Mon/YYYY), 2=hora Apache (HH:mm:ss), 3=metodo, 4=ruta, 5=status (o "-"), 6=bytes
const LINEA_RE = /^(?:\d{4}-\d{2}-\d{2} )?\d{2}:\d{2}:\d{2} \[info\]: \S+ \S+ \S+ \[(\d{2}\/\w{3}\/\d{4}):(\d{2}:\d{2}:\d{2}) [^\]]*\] "(\S+) (\S+)[^"]*" (\d+|-) (\S+)/;

/** Agrupa rutas con un id/uuid/número al final bajo un mismo patrón — si no, cada
 *  tarjeta/transacción distinta cuenta como "endpoint" aparte y el top-N queda inútil. */
function normalizarRuta(ruta) {
  const sinQuery = ruta.split('?')[0];
  return sinQuery
    .replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi, ':uuid')
    .replace(/\/\d+(?=\/|$)/g, '/:id');
}

function main() {
  const archivo = process.argv[2];
  const texto = archivo ? fs.readFileSync(archivo, 'utf8') : fs.readFileSync(0, 'utf8');
  const lineas = texto.split('\n');

  const porRuta = new Map();   // "METODO ruta" -> count
  const porStatus = new Map(); // "2xx/3xx/4xx/5xx" -> count
  const porMinuto = new Map(); // "YYYY-MM-DD HH:mm" -> count
  let total = 0;
  let descartadas = 0;

  for (const linea of lineas) {
    const m = LINEA_RE.exec(linea);
    if (!m) { if (linea.trim()) descartadas += 1; continue; }
    const [, fechaApache, horaApache, metodo, ruta, status] = m;
    total += 1;

    const clave = `${metodo} ${normalizarRuta(ruta)}`;
    porRuta.set(clave, (porRuta.get(clave) || 0) + 1);

    // "-" = request abortada del lado del cliente (sin res.statusCode) — se cuenta aparte,
    // nunca se mezcla con una familia 2xx/3xx/4xx/5xx real.
    const grupoStatus = status === '-' ? 'abortada (-)' : `${status[0]}xx`;
    porStatus.set(grupoStatus, (porStatus.get(grupoStatus) || 0) + 1);

    // Minuto real de la request (log de Apache, confiable sin importar si vino de
    // logs/app.log o de `docker logs` — el prefijo de winston no siempre trae fecha).
    const minuto = `${fechaApache} ${horaApache.slice(0, 5)}`; // "DD/Mon/YYYY HH:mm"
    porMinuto.set(minuto, (porMinuto.get(minuto) || 0) + 1);
  }

  if (!total) {
    console.error('No se reconoció ninguna línea con el formato esperado (ver LINEA_RE). ¿Es realmente un extracto de logs/app.log?');
    process.exit(1);
  }

  const topRutas = [...porRuta.entries()].sort((a, b) => b[1] - a[1]).slice(0, 20);
  const picoMinuto = [...porMinuto.entries()].sort((a, b) => b[1] - a[1])[0];
  const minutos = porMinuto.size || 1;

  console.log(`\n=== Resumen de ${total} requests (${descartadas} líneas descartadas, no matchearon el formato) ===\n`);

  console.log(`Ventana cubierta: ${minutos} minutos distintos con tráfico`);
  console.log(`Promedio: ${(total / minutos).toFixed(1)} req/min`);
  console.log(`Pico real: ${picoMinuto[1]} req/min (${picoMinuto[0]}) → ${(picoMinuto[1] / 60).toFixed(2)} req/seg sostenido ese minuto\n`);

  console.log('Status codes:');
  for (const [grupo, n] of [...porStatus.entries()].sort()) {
    console.log(`  ${grupo}: ${n} (${((n / total) * 100).toFixed(1)}%)`);
  }

  console.log('\nTop 20 endpoints (METODO ruta normalizada — count, % del total):');
  for (const [ruta, n] of topRutas) {
    console.log(`  ${String(n).padStart(6)}  ${((n / total) * 100).toFixed(1).padStart(5)}%  ${ruta}`);
  }

  console.log('\nSugerencia para staff_background en k6-otp-flow.js: repartir las VUs/sleep() de ese\nescenario proporcional al % de cada endpoint de arriba, y usar el "pico real" como el\ntarget de requests/seg a sostener durante la etapa "sostenido" de otp_flow.');
}

main();
