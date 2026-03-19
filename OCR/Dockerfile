FROM python:3.7.12-slim

WORKDIR /app

# Instala librerías para OpenCV y Keras OCR
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential git \
    libglib2.0-0 libsm6 libxext6 libxrender-dev libgl1-mesa-glx libgl1-mesa-dev \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements.txt
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install --no-cache-dir -r /app/requirements.txt

# Copiar notebook
COPY keras-ocr-text-recognition-4.ipynb /app/keras-ocr-text-recognition-4.ipynb

EXPOSE 8888

# Arrancar Jupyter no token ni password
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=", "--NotebookApp.password="]
