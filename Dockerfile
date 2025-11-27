# Usa una imagen base de Python más estable
FROM python:3.11-slim

# Establece el directorio de trabajo en /app
WORKDIR /app

# Instala dependencias de sistema necesarias para compilar paquetes
RUN apt-get update && apt-get install -y \
    gcc \
    make \
    build-essential \
    libpq-dev \
    libxml2-dev \
    libxslt-dev \
    libffi-dev \
    libjpeg-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Instalar boto3
RUN pip install boto3 psycopg2-binary requests lxml pandas

# Copia el archivo de requisitos al contenedor
COPY requirements.txt .

# Instala las dependencias de Python
RUN python3 -m pip install --no-cache-dir -r requirements.txt

# Copia tu código Python
# COPY ./scripts/paralelizado/paralelo_havistoa_chiapas.py .
# COPY ./scripts/paralelizado/paralelo_amber_chiapas.py .
# COPY ./scripts/paralelizado/paralelo_amber_nacional.py .
# COPY ./scripts/serial/serial_hasvistoa_michoacan.py .

# Comando por defecto
# CMD python -u paralelo_havistoa_chiapas.py && python -u paralelo_amber_chiapas.py && python -u paralelo_amber_nacional.py && python -u serial_hasvistoa_michoacan.py
