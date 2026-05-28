'''
  - [X] Amber: [link](https://www.fgjcdmx.gob.mx/nuestros-servicios/servicios-la-ciudadania/alerta-amber-df)
    - Hay una seccion de "activacion" que contiene fotos de fichas de Alertas Amber con un formato distinto al general
    - Las fichas estan pegadas secuencialmente y son JPG
    - hash_id: 0802_
'''

import os
import time
import requests
import multiprocessing
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
HASH_ID = "0802_"
TEST_LIMIT = None  # Set to 1 for testing, None for full run

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

    base_url = "https://www.fgjcdmx.gob.mx/nuestros-servicios/servicios-la-ciudadania/alerta-amber-df"
    response = requests.get(base_url, timeout=20)
    if response.status_code != 200:
        print("Error al acceder a la página.")
        return

    soup = BeautifulSoup(response.content, "html.parser")

    collapse_ids = [a["href"].lstrip("#") for a in soup.find_all("a", href=lambda h: h and h.startswith("#collapse"))]
    if not collapse_ids:
        print("No se encontraron secciones collapse.")
        return

    image_urls = []
    for collapse_id in collapse_ids:
        container = soup.find(id=collapse_id)
        if not container:
            continue
        for img_tag in container.find_all("img"):
            src = img_tag.get("src")
            if src:
                image_urls.append(urljoin(base_url, src))
        print(f"Sección {collapse_id}: {len(container.find_all('img'))} imágenes.")

    print(f"Total de URLs de imágenes a descargar: {len(image_urls)}")

    existing_files = get_existing_files(BUCKET_NAME, S3_FOLDER)
    print(f"Archivos existentes en S3: {len(existing_files)}")

    if TEST_LIMIT:
        image_urls = image_urls[:TEST_LIMIT]
        print(f"Modo de prueba: procesando solo {TEST_LIMIT} imagen(es).")

    cores_virtuales = multiprocessing.cpu_count() * 2
    workers = min(24, cores_virtuales)
    print(f"Usando {workers} workers para la descarga de imágenes.")

    with ThreadPoolExecutor(max_workers=workers) as executor:
        for url in image_urls:
            executor.submit(download_image, url, existing_files)

    end_time = time.time()
    print(f"Tiempo total de scrappeo: {end_time - start_time:.2f} segundos.")

if __name__ == "__main__":
    main()
