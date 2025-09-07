FROM python:3.11-slim

# Metadata
LABEL maintainer="GridMR Team"
LABEL version="1.0.0"
LABEL description="GridMR Worker Node - Production Ready"

# Variables de entorno
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    GRIDMR_WORKER_WORKER_ID="" \
    GRIDMR_WORKER_MASTER_HOST="master" \
    GRIDMR_WORKER_MASTER_PORT="8000" \
    GRIDMR_WORKER_WORKER_PORT="8001" \
    GRIDMR_WORKER_WORKER_TYPE="generic" \
    GRIDMR_WORKER_MAX_CONCURRENT_TASKS="4" \
    GRIDMR_WORKER_LOG_LEVEL="INFO"

# Crear usuario no-root para seguridad
RUN groupadd -r gridmr && \
    useradd -r -g gridmr -d /app -s /bin/bash gridmr

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    procps \
    htop \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Establecer directorio de trabajo
WORKDIR /app

# Copiar requirements primero para mejor cache de Docker
COPY requirements.txt .

# Instalar dependencias Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY --chown=gridmr:gridmr . .

# Crear directorios con permisos correctos
RUN mkdir -p /app/data/input \
             /app/data/output \
             /app/data/temp \
             /app/data/cache \
             /app/logs && \
    chown -R gridmr:gridmr /app && \
    chmod -R 755 /app



# Cambiar a usuario no-root
USER gridmr

# Health check mejorado
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:${GRIDMR_WORKER_WORKER_PORT}/health || exit 1

# Exponer puerto
EXPOSE ${GRIDMR_WORKER_WORKER_PORT}

# Volúmenes para datos persistentes
VOLUME ["/app/data", "/app/logs"]

# Punto de entrada por defecto
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]