'''
 - [ ] Personas Desaparecidas: [link](https://sg.guanajuato.gob.mx/personas-desaparecidas/)
    - Incluye Alerta Amber, Protocolo Alba, "Te estamos buscando" y no se si otro
    - De Amber y Alba, al dar click en "Ver Detalles", salen JPG con las fichas
    - Datos obtenidos via API: https://boletines.guanajuato.gob.mx/desaparecidos/apiPersonas.php?tipo=2
    - hashid: 1302_
'''

import os
import time
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
from urllib.parse import urljoin
import urllib3

import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from utils.db_utils import build_record, insert_records
from utils.s3_utils import upload_url_if_enabled

S3_FOLDER = "jpg/"
HASH_ID = "1302_"
TEST_LIMIT = None  # Set to 1 for testing, None for full run

API_URL = "https://boletines.guanajuato.gob.mx/desaparecidos/apiPersonas.php?tipo=2"
IMG_BASE = "https://boletines.guanajuato.gob.mx/desaparecidos/"


def get_existing_files(bucket, prefix):
    return set()

def download_image(url, existing_files):
    try:
        url = requests.utils.requote_uri(url)
        file_name = os.path.basename(url) or "imagen.jpg"
        file_name = f"{HASH_ID}{file_name}"
        s3_key = f"{S3_FOLDER}{file_name}"
        if file_name in existing_files:
            print(f"La imagen ya existe en S3: {s3_key}")
            return s3_key
        s3_url = upload_url_if_enabled(url, s3_key, timeout=20)
        if s3_url:
            print(f"Imagen subida a S3: {s3_url}")
            return s3_url
    except Exception as e:
        print(f"Error al descargar {url}: {e}")
    return None


def first_value(record, *keys):
    for key in keys:
        value = record.get(key)
        if value not in (None, ""):
            return value
    return None


def build_nombre(record):
    direct = first_value(record, "nombre", "Nombre", "pNombre", "sNombre")
    if direct:
        return str(direct).strip()
    parts = [
        first_value(record, "pNombre", "nombre"),
        first_value(record, "pApellidoPaterno", "apellidoPaterno", "primerApellido"),
        first_value(record, "pApellidoMaterno", "apellidoMaterno", "segundoApellido"),
    ]
    return " ".join(str(part).strip() for part in parts if part).strip() or None


def build_datos(record, imagen_url):
    folio = first_value(record, "folio", "pFolio", "idPersona", "id", "Id")
    descripcion = first_value(record, "descripcion_hechos", "pDescripcion", "hechos")
    senas = first_value(record, "senas", "pSenas", "senas_particulares")
    return {
        "nombre": build_nombre(record),
        "folio": str(folio) if folio is not None else imagen_url,
        "estado_alerta": "Alerta Amber Guanajuato",
        "imagen_url": imagen_url,
        "descripcion_hechos": descripcion,
        "senas": senas,
        "localizado": False,
        "raw": record,
    }


def insert_records_to_db(records, image_urls):
    db_records = []
    for record, imagen_url in zip(records, image_urls):
        datos = build_datos(record, imagen_url)
        db_records.append(build_record(HASH_ID, datos, imagen_url, localizado=False))
    inserted = insert_records(db_records)
    print(f"Insertados en DB: {inserted} nuevos de {len(db_records)} registros Amber Guanajuato")
    return inserted

def main():
    start_time = time.time()

    response = requests.get(API_URL, verify=False, timeout=20)
    if response.status_code != 200:
        print(f"Error al acceder a la API: {response.status_code}")
        return

    records = response.json()
    print(f"Se encontraron {len(records)} registros en la API.")

    image_urls = []
    for record in records:
        foto = record.get("pFoto", "")
        if foto:
            full_url = urljoin(IMG_BASE, foto)
            image_urls.append(full_url)

    print(f"Total de URLs a descargar: {len(image_urls)}")

    existing_files = get_existing_files(None, S3_FOLDER)
    print(f"Archivos existentes en S3: {len(existing_files)}")

    if TEST_LIMIT:
        records = records[:TEST_LIMIT]
        image_urls = image_urls[:TEST_LIMIT]
        print(f"Modo de prueba: procesando solo {TEST_LIMIT} imagen(es).")

    insert_records_to_db(records, image_urls)

    workers = min(24, multiprocessing.cpu_count() * 2)
    print(f"Usando {workers} workers para la descarga de imágenes.")

    with ThreadPoolExecutor(max_workers=workers) as executor:
        for url in image_urls:
            executor.submit(download_image, url, existing_files)

    end_time = time.time()
    print(f"Tiempo total de scrappeo: {end_time - start_time:.2f} segundos.")

if __name__ == "__main__":
    main()
