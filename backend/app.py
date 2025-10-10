from fastapi import FastAPI, UploadFile, Form, File
from fastapi.middleware.cors import CORSMiddleware
import boto3
import requests
import os
import json
from dotenv import load_dotenv
from typing import Optional

#EDIT: imports mínimos para disparar scrapers sin bloquear la API
import sys  #EDIT
import subprocess  #EDIT
import threading  #EDIT
from pathlib import Path  #EDIT

app = FastAPI()

# Permitir CORS para que tu front pueda llamar a este API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Cargar variables de entorno
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')  # usa us-east-1 por default si no hay

API_ENDPOINT = "http://postgrest:3000/desaparecidos"
SIMILARITY_THRESHOLD = 80

@app.post("/busqueda-avanzada")
async def busqueda_avanzada(
    nombre: str = Form(...),
    apellidos: Optional[str] = Form(None),
    foto: UploadFile = File(...)
):
    # Leer imagen subida
    source_bytes = await foto.read()

    # Armar nombre completo, limpiando espacios extra
    nombre_completo = nombre.strip()
    if apellidos:
        nombre_completo += ' ' + apellidos.strip()
    nombre_completo = nombre_completo.upper()
    nombre_completo = ' '.join(nombre_completo.split())  # Normaliza múltiples espacios
    nombre_completo_encoded = nombre_completo.replace(' ', '%')

    # Buscar personas con nombre similar
    url = f"{API_ENDPOINT}?datos->>nombre=ilike.*{nombre_completo_encoded}*"
    response = requests.get(url)
    if response.status_code != 200:
        return {"error": "Error al buscar en la base de datos"}

    personas = response.json()
    if not personas:
        return {"resultados": []}

    # Conexión directa a Rekognition usando claves de AWS
    client = boto3.client(
        'rekognition',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    resultados = []

    for persona in personas:
        imagen_url = persona['datos'].get('imagen_url')
        if not imagen_url:
            continue

        target_response = requests.get(imagen_url)
        if target_response.status_code != 200:
            continue

        try:
            rekognition_response = client.compare_faces(
                SourceImage={'Bytes': source_bytes},
                TargetImage={'Bytes': target_response.content},
                SimilarityThreshold=SIMILARITY_THRESHOLD
            )
            face_matches = rekognition_response['FaceMatches']
            if face_matches:
                resultados.append(persona)
        except Exception as e:
            print(f"Error al comparar imágenes: {e}")
            continue

    return {"resultados": resultados}


#EDIT: endpoint de salud simple para healthchecks
@app.get("/healthz")  #EDIT
def healthz():  #EDIT
    return {"ok": True}  #EDIT



#EDIT: función interna que lanza los scrapers sin bloquear la petición
def _launch_scrapers_background():  #EDIT
    """Lanza uno o varios scrapers en procesos independientes."""  #EDIT
    # Ajusta las rutas a tus scripts reales si difieren:  #EDIT
    candidate_cmds = [  #EDIT
        [sys.executable, "scripts/paralelizado/paralelo_amber_cdmx.py"],  #EDIT
        # Agrega aquí otros scrapers si los quieres disparar también:        #EDIT
        # [sys.executable, "scripts/paralelizado/paralelo_estado_X.py"],     #EDIT
        # [sys.executable, "scripts/serial/amber_cdmx.py"],                  #EDIT
    ]  #EDIT

    cwd = Path(".").resolve()  #EDIT
    for cmd in candidate_cmds:  #EDIT
        script_path = (cwd / cmd[-1]).resolve() if len(cmd) >= 2 else None  #EDIT
        if script_path and script_path.exists():  #EDIT
            try:  #EDIT
                subprocess.Popen(cmd, cwd=str(cwd))  #EDIT
                print(f"[backend] Lanzado scraper: {' '.join(cmd)}")  #EDIT
            except Exception as e:  #EDIT
                print(f"[backend] Error lanzando {' '.join(cmd)} :: {e}")  #EDIT
        else:  #EDIT
            print(f"[backend] No encontrado: {' '.join(cmd)}")  #EDIT


#EDIT: endpoint que el scheduler invoca cada X minutos
@app.post("/run-scrapers")  #EDIT
def run_scrapers_endpoint():  #EDIT
    t = threading.Thread(target=_launch_scrapers_background, daemon=True)  #EDIT
    t.start()  #EDIT
    return {"status": "accepted"}  #EDIT
