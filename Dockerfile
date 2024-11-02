# Usa una imagen base de Python
FROM python:3.12.0-slim

# Establece el directorio de trabajo en /app
WORKDIR /app

# Copia el archivo de requisitos al contenedor
COPY requirements.txt .

# Instala las dependencias
RUN python3 -m pip install --no-cache-dir -r requirements.txt

# Copia el codigo de python al contenedor
COPY ./scripts/scrapper_havistoa_chiapas.py .

# Define el comando por defecto para ejecutar la aplicación
CMD python -u scrapper_havistoa_chiapas.py