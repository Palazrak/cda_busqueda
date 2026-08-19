'''
  - [X] Fiscalia Especial en Personas Desaparecidas: [link](https://fiscaliaenpersonasdesaparecidas.jalisco.gob.mx/?f1=&f2=&f3=&f4=&wpcfs=preset-1)
    - Grid de fotografias de fichas
    - Al dar click a cada ficha, se abre una nueva pagina con la ficha en JPG
    - Incluye Amber, Alba y Fiscalia. Fichas en wp-content/uploads, accesibles sin JS
    - hashid: 1602_
'''

import os
import time
import multiprocessing
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin
import urllib3
import boto3
from botocore.exceptions import ClientError

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

s3 = boto3.client("s3")
BUCKET_NAME = "cdas-2025-alertas-amber"
S3_FOLDER = "jpg/"
HASH_ID = "1602_"
TEST_LIMIT = None  # Set to 1 for testing, None for full run

BASE_URL = "https://fiscaliaenpersonasdesaparecidas.jalisco.gob.mx/alerta-amber-jalisco/"
SKIP_KEYWORDS = ['cabeza', 'boton', 'banner', 'logo', 'thumb', 'icon', 'footer', 'header']


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

    response = requests.get(BASE_URL, timeout=20)
    if response.status_code != 200:
        print(f"Error al acceder a la página: {response.status_code}")
        return

    soup = BeautifulSoup(response.content, "html.parser")

    image_urls = []
    for img in soup.find_all("img"):
        src = img.get("src", "")
        if "wp-content/uploads" not in src:
            continue
        src_lower = src.lower()
        if any(kw in src_lower for kw in SKIP_KEYWORDS):
            continue
        if not any(ext in src_lower for ext in ['.jpg', '.jpeg', '.png']):
            continue
        image_urls.append(src if src.startswith("http") else urljoin(BASE_URL, src))

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
