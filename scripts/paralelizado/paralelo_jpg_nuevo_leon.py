'''
  - [ ] Busqueda de Personas no Localizadas o Desaparecidas: [link](https://www.hcnl.gob.mx/desaparecidos/)
    - Hay un Wrapper de desaparecidos que contiene imagen, nombre y mas info
    - Al darle click a la imagen, sale una ventana con la ficha en JPG
    - Parece que hay un TXT llamado "desaparecidos" que incluye el nombre y el url de la imagen de desaparicion (el URL aparece hasta con whatsappdownloads)
Prefijo hashid: 2001_
'''

import os
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
import urllib3
import boto3
from botocore.exceptions import ClientError

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

s3 = boto3.client("s3")
BUCKET_NAME = "cdas-2025-alertas-amber"
S3_FOLDER = "jpg/"
HASH_ID = "2001_"
TEST_LIMIT = None  # Set to 1 for testing, None for full run

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
BASE_URL = "https://www.hcnl.gob.mx/desaparecidos/"
DATA_URL = "https://www.hcnl.gob.mx/desaparecidos/desaparecidos.txt"
MAX_WORKERS = 8


def get_existing_files(bucket, prefix):
    existing = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if "Contents" in page:
            for obj in page["Contents"]:
                existing.add(os.path.basename(obj["Key"]))
    return existing

def upload_image_to_s3(url, existing_files):
    try:
        url = requests.utils.requote_uri(url)
        response = requests.get(url, headers=HEADERS, timeout=20, verify=False)
        if response.status_code == 200:
            file_name = os.path.basename(url.split("?")[0]) or "imagen.jpg"
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


def get_all_cards_data():
    cards_data = []
    try:
        print(f"Descargando datos desde {DATA_URL}...")
        response = requests.get(DATA_URL, headers=HEADERS, timeout=20, verify=False)
        if response.status_code != 200:
            print(f"Error al obtener datos: {response.status_code}")
            return []

        data_json = json.loads(response.text)
        records = data_json.get("data", [])
        print(f"Se encontraron {len(records)} registros en el archivo JSON.")

        for record in records:
            if not record or len(record) < 2:
                continue

            image_html = record[0] if len(record) > 0 else ""
            name_html = record[1] if len(record) > 1 else ""

            soup_image = BeautifulSoup(image_html, "html.parser")
            soup_name = BeautifulSoup(name_html, "html.parser")

            imagen_url = None
            nombre = None

            image_link = soup_image.find("a", class_="shadow")
            if image_link:
                imagen_url = image_link.get("href", "")
                nombre = image_link.get("title", "")

            if not nombre:
                name_link = soup_name.find("a")
                if name_link:
                    h5 = name_link.find("h5")
                    if h5:
                        nombre = h5.get_text(strip=True).replace("  ", " ").strip()

            if nombre and imagen_url:
                cards_data.append({"imagen_url": imagen_url, "nombre": nombre})

        print(f"Se encontraron {len(cards_data)} tarjetas válidas.")
        return cards_data

    except Exception as e:
        print(f"Error al obtener datos de las tarjetas: {e}")
        return []


def main() -> None:
    overall_start = time.time()

    cards = get_all_cards_data()
    if not cards:
        print("No se encontraron tarjetas.")
        return

    existing_files = get_existing_files(BUCKET_NAME, S3_FOLDER)
    print(f"Archivos existentes en S3: {len(existing_files)}")

    image_urls = [c["imagen_url"] for c in cards if c.get("imagen_url")]

    if TEST_LIMIT:
        image_urls = image_urls[:TEST_LIMIT]
        print(f"Modo de prueba: procesando solo {TEST_LIMIT} imagen(es).")

    print(f"Subiendo {len(image_urls)} imágenes a S3...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(upload_image_to_s3, url, existing_files) for url in image_urls]
        for f in as_completed(futures):
            f.result()

    print(f"Tiempo total: {time.time() - overall_start:.2f} segundos")


if __name__ == "__main__":
    main()
