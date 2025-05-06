from fastapi import FastAPI, UploadFile, Form, File
from fastapi.middleware.cors import CORSMiddleware
import boto3
import requests
import os
import json
from dotenv import load_dotenv
from typing import Optional

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
