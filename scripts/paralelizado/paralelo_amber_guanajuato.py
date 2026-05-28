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
from urllib.parse import urljoin
import urllib3

import requests
import boto3
from botocore.exceptions import ClientError

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

s3 = boto3.client("s3")
BUCKET_NAME = "cdas-2025-alertas-amber"
S3_FOLDER = "jpg/"
HASH_ID = "1302_"
TEST_LIMIT = None  # Set to 1 for testing, None for full run

API_URL = "https://boletines.guanajuato.gob.mx/desaparecidos/apiPersonas.php?tipo=2"
IMG_BASE = "https://boletines.guanajuato.gob.mx/desaparecidos/"


def get_existing_files(bucket, prefix):
    existing = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if "Contents" in page:
            for obj in page["Contents"]:
                existing.add(os.path.basename(obj["Key"]))
    return existing

def download_image(url, existing_files):
    try:
        url = requests.utils.requote_uri(url)
        response = requests.get(url, timeout=20, verify=False)
        if response.status_code == 200:
            file_name = os.path.basename(url)
            file_name = f"{HASH_ID}{file_name}"
            s3_key = f"{S3_FOLDER}{file_name}"
            if file_name in existing_files:
                print(f"La imagen ya existe en S3: {s3_key}")
                return s3_key
            s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=response.content)
            print(f"Imagen subida a S3: {s3_key}")
            return s3_key
        else:
            print(f"Error {response.status_code} al descargar {url}")
    except Exception as e:
        print(f"Error al descargar {url}: {e}")
    return None

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

    existing_files = get_existing_files(BUCKET_NAME, S3_FOLDER)
    print(f"Archivos existentes en S3: {len(existing_files)}")

    if TEST_LIMIT:
        image_urls = image_urls[:TEST_LIMIT]
        print(f"Modo de prueba: procesando solo {TEST_LIMIT} imagen(es).")

    workers = min(24, multiprocessing.cpu_count() * 2)
    print(f"Usando {workers} workers para la descarga de imágenes.")

    with ThreadPoolExecutor(max_workers=workers) as executor:
        for url in image_urls:
            executor.submit(download_image, url, existing_files)

    end_time = time.time()
    print(f"Tiempo total de scrappeo: {end_time - start_time:.2f} segundos.")

if __name__ == "__main__":
    main()
