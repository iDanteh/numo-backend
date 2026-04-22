# ══════════════════════════════════════════════════════════════════════════════
# numo-backend — Dockerfile de producción
# ══════════════════════════════════════════════════════════════════════════════
#
# ARQUITECTURA MULTI-STAGE:
#   Stage 1 (deps)    — instala solo dependencias de producción
#   Stage 2 (runtime) — imagen final mínima, sin herramientas de build
#
# Por qué node:18-slim y NO node:18-alpine:
#   • 'sharp' (procesamiento de imágenes) usa libvips compilado contra glibc.
#     Alpine usa musl libc — incompatible sin recompilar. slim es Debian-based
#     y evita este problema por completo.
#   • 'tesseract.js' y 'node-forge' también tienen bindings nativos que se
#     benefician de las librerías estándar de Debian.
# ══════════════════════════════════════════════════════════════════════════════


# ── Stage 1: Instalación de dependencias ──────────────────────────────────────
FROM node:18-slim AS deps

WORKDIR /app

# Instalar librerías del sistema requeridas por dependencias nativas:
#   • libvips-dev  → requerida por 'sharp' para procesamiento de imágenes
#   • python3      → requerida por node-gyp para compilar módulos nativos
#   • build-essential → compilador C++ para bindings nativos (node-gyp)
#   Limpiamos el cache de apt en el mismo RUN para no inflar el layer.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libvips-dev \
    python3 \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copiamos SOLO los archivos de manifiesto de dependencias primero.
# Razón: Docker cachea cada layer. Si solo cambias código fuente (src/),
# pero no package.json, este layer se reutiliza → builds mucho más rápidos.
COPY package*.json ./

# npm ci vs npm install:
#   • npm ci lee package-lock.json y garantiza versiones exactas (reproducible)
#   • --only=production excluye jest, nodemon y supertest (no necesarios en prod)
#   • --ignore-scripts evita scripts postinstall arbitrarios por seguridad
#     EXCEPCIÓN: sharp necesita su script install → no usamos --ignore-scripts aquí
RUN npm ci --only=production


# ── Stage 2: Imagen de runtime ────────────────────────────────────────────────
FROM node:18-slim AS runtime

WORKDIR /app

# Reinstalar solo las librerías de runtime (sin herramientas de build).
# libvips es necesaria en runtime para que sharp funcione.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libvips42 \
    && rm -rf /var/lib/apt/lists/*

# Copiamos node_modules ya instalados desde el stage anterior.
# Esto evita tener python3 y build-essential en la imagen final.
COPY --from=deps /app/node_modules ./node_modules

# Copiamos el código fuente de la aplicación.
# Se copia DESPUÉS de node_modules para aprovechar el cache de Docker:
# cambios en src/ no invalidan el layer de dependencias.
COPY src/ ./src/
COPY package.json ./

# Crear directorios necesarios en runtime con los permisos correctos
# antes de cambiar al usuario no-root.
RUN mkdir -p logs uploads tmp

# ── Seguridad: usuario no-root ─────────────────────────────────────────────────
# Nunca ejecutes aplicaciones como root en contenedores.
# node:18-slim incluye el usuario 'node' (uid 1000) listo para usar.
# Le damos ownership de los directorios que la app necesita escribir.
RUN chown -R node:node /app
USER node

# Puerto que expone el servidor Express.
# Este valor DEBE coincidir con PORT en .env (default: 3000).
EXPOSE 3000

# ── Health check ──────────────────────────────────────────────────────────────
# Docker verifica el estado del contenedor cada 30s.
# Si el /health falla 3 veces → el contenedor se marca "unhealthy".
# docker-compose.yml usa esto para no enrutar tráfico a contenedores no listos.
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD node -e "require('http').get('http://localhost:3000/health', (r) => { process.exit(r.statusCode === 200 ? 0 : 1) }).on('error', () => process.exit(1))"

# Comando de inicio.
# Usamos 'node' directamente (no npm start) porque:
#   • npm añade un proceso intermediario que intercepta señales (SIGTERM)
#   • Con 'node' directo, Docker puede enviar SIGTERM directamente al proceso
#     para un graceful shutdown correcto.
CMD ["node", "src/app.js"]
