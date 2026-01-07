# Dockerfile completo del scheduler con dependencias de scrapers

FROM python:3.11-slim

LABEL maintainer="CDA Búsqueda Team"
LABEL description="Scheduler autónomo para scrapers de personas desaparecidas"
LABEL version="1.0"

# =============================================================================
# VARIABLES DE ENTORNO
# =============================================================================
# Evitar que Python genere archivos .pyc (optimización para containers)
ENV PYTHONDONTWRITEBYTECODE=1

# Forzar output sin buffer (logs en tiempo real)
ENV PYTHONUNBUFFERED=1

# Directorio de trabajo
ENV APP_HOME=/app
WORKDIR $APP_HOME

# =============================================================================
# INSTALACIÓN DE DEPENDENCIAS DEL SISTEMA
# =============================================================================
# Necesarias para compilar algunas dependencias Python (lxml principalmente)
# También incluye wget para health checks
RUN apt-get update && apt-get install -y --no-install-recommends \
    libxml2-dev \
    libxslt-dev \
    gcc \
    wget \
    procps \
    && rm -rf /var/lib/apt/lists/*

# =============================================================================
# INSTALACIÓN DE DEPENDENCIAS PYTHON
# =============================================================================
#edittt - Copiar requirements.txt primero (para aprovechar cache de Docker)
# Si requirements.txt no cambia, Docker reutiliza esta capa
COPY scheduler/requirements.txt ${APP_HOME}/requirements.txt

#edittt - Instalar dependencias Python
# --no-cache-dir: No guardar cache de pip (reduce tamaño de imagen)
# --upgrade pip: Asegurar versión reciente de pip
RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir -r /app/requirements.txt

# =============================================================================
# COPIAR CÓDIGO DEL SCHEDULER
# =============================================================================
#edittt - Copiar archivos Python del scheduler
COPY scheduler/scheduler_config.py ${APP_HOME}/
COPY scheduler/jitter_calculator.py ${APP_HOME}/
COPY scheduler/scraper_executor.py ${APP_HOME}/
COPY scheduler/scheduler.py ${APP_HOME}/
COPY scheduler/config.yaml ${APP_HOME}/config.yaml

# =============================================================================
# COPIAR SCRIPTS DE SCRAPING
# =============================================================================
#edittt - Crear directorio para scripts
RUN mkdir -p ${APP_HOME}/scripts/paralelizado

#edittt - Los scripts se montarán como volumen desde docker-compose.yml
# Pero creamos el directorio aquí para que exista incluso sin volumen
# Si quisieras incluir los scripts en la imagen (no recomendado):
# COPY ../scripts/ ${APP_HOME}/scripts/

# =============================================================================
# HEALTH CHECK
# =============================================================================
#edittt - Health check simple: verificar que el proceso Python existe
# No podemos hacer HTTP health check porque no hay endpoint
# Verificamos que scheduler.py está corriendo
HEALTHCHECK --interval=60s --timeout=5s --start-period=60s --retries=3 \
    CMD pgrep -f "python.*scheduler.py" > /dev/null || exit 1

# =============================================================================
# USUARIO NO-ROOT (SEGURIDAD)   
# =============================================================================
#edittt - Crear usuario no-privilegiado para ejecutar el scheduler
# Mejor práctica de seguridad: no correr como root
RUN useradd -m -u 1000 scheduler && \
    chown -R scheduler:scheduler ${APP_HOME}

#edittt - Cambiar a usuario no-root
USER scheduler

# =============================================================================
# COMANDO DE INICIO
# =============================================================================
#edittt - Ejecutar scheduler.py como comando principal
# El scheduler se ejecuta en modo foreground (BlockingScheduler)
CMD ["python", "scheduler.py"]

# =============================================================================
# NOTAS DE USO
# =============================================================================
# Build:
#   docker build -t cda-scheduler:latest -f scheduler/Dockerfile .
#
# Run standalone (sin compose):
#   docker run -it --rm \
#     -v $(pwd)/scripts:/app/scripts:ro \
#     -v $(pwd)/scheduler/config.yaml:/app/config.yaml:ro \
#     --network app-network \
#     cda-scheduler:latest
#
# Run con docker-compose (recomendado):
#   docker-compose up scheduler
#
# Logs:
#   docker logs -f scheduler
#
# Stop gracefully:
#   docker stop scheduler  (envía SIGTERM, esperado por signal_handler)
#
# =============================================================================
# TROUBLESHOOTING
# =============================================================================
# 
# Problema: "ModuleNotFoundError: No module named 'X'"
# Solución: Verificar que requirements.txt tiene todas las dependencias
#   docker exec -it scheduler pip list
#
# Problema: "FileNotFoundError: config.yaml"
# Solución: Montar config.yaml como volumen en docker-compose.yml
#   volumes:
#     - ./scheduler/config.yaml:/app/config.yaml:ro
#
# Problema: "FileNotFoundError: paralelo_*.py"
# Solución: Montar scripts como volumen
#   volumes:
#     - ./scripts:/app/scripts:ro
#
# Problema: "Permission denied" al escribir logs
# Solución: El scheduler solo escribe a stdout (Docker captura logs)
#   No necesita permisos de escritura en disco
#
# Problema: Health check falla
# Solución: Verificar que scheduler.py está corriendo
#   docker exec scheduler ps aux | grep scheduler.py
#
# =============================================================================