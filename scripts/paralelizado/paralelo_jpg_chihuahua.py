'''
  - [ ] Fiscalia General del Estado de Chihuahua: [link](https://fiscalia.chihuahua.gob.mx/desaparecidos/)
    - Pagina que incluye nombres y fichas en JPG. Al dar click a cada una, se abre una nueva pagina que contiene solo el JPG.
    - Es super pesada de cargar por tantos elementos

Prefijo hashid: 0702_
'''

import os
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
from urllib.parse import urljoin
import urllib3

import requests
from bs4 import BeautifulSoup
import boto3
from botocore.exceptions import ClientError

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

s3 = boto3.client("s3")
BUCKET_NAME = "cdas-2025-alertas-amber"
S3_FOLDER = "jpg/"
HASH_ID = "0702_"
TEST_LIMIT = None  # Set to 1 for testing, None for full run

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
BASE_URL = "https://fiscalia.chihuahua.gob.mx/desaparecidos/"
MAX_WORKERS = min(24, multiprocessing.cpu_count() * 2)


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
        response = requests.get(BASE_URL, headers=HEADERS, timeout=20, verify=False)
        if response.status_code != 200:
            print(f"Error al obtener página: {response.status_code}")
            return []

        soup = BeautifulSoup(response.content, "html.parser")
        thumbnail_divs = soup.find_all("div", class_="anwp-pg-post-teaser__thumbnail-img")

        for div in thumbnail_divs:
            thumbnail_parent = div.find_parent("div", class_=re.compile(r"anwp-pg-post-teaser__thumbnail"))
            card_container = None
            if thumbnail_parent:
                for parent in thumbnail_parent.find_parents("div"):
                    classes = parent.get("class", [])
                    class_str = " ".join(classes) if classes else ""
                    if "anwp-pg-post-teaser" in class_str and "__thumbnail" not in class_str:
                        card_container = parent
                        break

            if not card_container:
                for parent in div.find_parents("div"):
                    classes = parent.get("class", [])
                    class_str = " ".join(classes) if classes else ""
                    if "anwp-pg-post-teaser" in class_str and "__thumbnail" not in class_str:
                        card_container = parent
                        break

            if not card_container:
                card_container = div.find_parent("div")

            imagen_url = None
            nombre = None
            detalle_url = None

            style = div.get("style", "")
            if style:
                match = re.search(r'background-image:\s*url\(["\']?([^"\']+)["\']?\)', style)
                if match:
                    image_url = match.group(1).strip()
                    imagen_url = urljoin(BASE_URL, image_url)

            if card_container:
                title_div = card_container.find("div", class_="anwp-pg-post-teaser__title")
                if title_div:
                    name_link = title_div.find("a", class_="anwp-link-without-effects")
                    if name_link:
                        nombre = name_link.get_text(strip=True)
                        detalle_url = name_link.get("href", "")
                        if detalle_url and not detalle_url.startswith("http"):
                            detalle_url = urljoin(BASE_URL, detalle_url)

            if imagen_url:
                cards_data.append({
                    "imagen_url": imagen_url,
                    "nombre": nombre,
                    "detalle_url": detalle_url or imagen_url,
                })

        print(f"Se encontraron {len(cards_data)} tarjetas en la página.")
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
