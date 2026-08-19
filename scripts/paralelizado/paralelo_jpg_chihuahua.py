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
from pathlib import Path
import sys
from urllib.parse import urljoin
import urllib3

import requests
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from utils.db_utils import build_record, insert_records
from utils.s3_utils import upload_url_if_enabled

S3_FOLDER = "jpg/"
HASH_ID = "0702_"
TEST_LIMIT = None  # Set to 1 for testing, None for full run

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
BASE_URL = "https://fiscalia.chihuahua.gob.mx/desaparecidos/"
MAX_WORKERS = min(24, multiprocessing.cpu_count() * 2)


def get_existing_files(bucket, prefix):
    return set()

def upload_image_to_s3(url, existing_files):
    try:
        url = requests.utils.requote_uri(url)
        file_name = os.path.basename(url.split("?")[0]) or "imagen.jpg"
        file_name = f"{HASH_ID}{file_name}"
        s3_key = f"{S3_FOLDER}{file_name}"
        if file_name in existing_files:
            print(f"La imagen ya existe en S3: {s3_key}")
            return s3_key
        s3_url = upload_url_if_enabled(url, s3_key, headers=HEADERS, timeout=20)
        if s3_url:
            print(f"Imagen subida a S3: {s3_url}")
            return s3_url
    except Exception as e:
        print(f"Error al descargar {url}: {e}")
    return None


def insert_cards_to_db(cards):
    records = []
    for card in cards:
        imagen_url = card.get("imagen_url")
        if not imagen_url:
            continue
        url_origen = card.get("detalle_url") or imagen_url
        datos = {
            "nombre": card.get("nombre"),
            "folio": url_origen,
            "estado_alerta": "Desaparecidos Chihuahua",
            "imagen_url": imagen_url,
            "descripcion_hechos": None,
            "senas": None,
            "localizado": False,
        }
        records.append(build_record(HASH_ID, datos, url_origen, localizado=False))
    inserted = insert_records(records)
    print(f"Insertados en DB: {inserted} nuevos de {len(records)} registros Chihuahua")
    return inserted


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

    existing_files = get_existing_files(None, S3_FOLDER)
    print(f"Archivos existentes en S3: {len(existing_files)}")

    if TEST_LIMIT:
        cards = cards[:TEST_LIMIT]
        print(f"Modo de prueba: procesando solo {TEST_LIMIT} imagen(es).")

    insert_cards_to_db(cards)
    image_urls = [c["imagen_url"] for c in cards if c.get("imagen_url")]
    print(f"Subiendo {len(image_urls)} imágenes a S3...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(upload_image_to_s3, url, existing_files) for url in image_urls]
        for f in as_completed(futures):
            f.result()

    print(f"Tiempo total: {time.time() - overall_start:.2f} segundos")


if __name__ == "__main__":
    main()
