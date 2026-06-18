'''
  - [ ] Comision de Busqueda de Personas del Estado de Mexico: [link](http://cobupem.edomex.gob.mx/boletines-personas-desaparecidas)
    - Muchas pestañas segun mes y año
    - Cada pestaña incluye fotos en JGP de las fichas
    - Tienen calidad variable

Prefijo hashid: 1201_
'''

import os
import time
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
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
HASH_ID = "1201_"
TEST_LIMIT = None  # Set to 1 for testing, None for full run

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
BASE_URL = "https://cobupem.edomex.gob.mx/boletines-personas-desaparecidas"
MAX_WORKERS = 8


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
        datos = {
            "nombre": card.get("nombre"),
            "folio": imagen_url,
            "estado_alerta": "Boletines Estado de México",
            "imagen_url": imagen_url,
            "descripcion_hechos": None,
            "senas": None,
            "localizado": False,
        }
        records.append(build_record(HASH_ID, datos, imagen_url, localizado=False))
    inserted = insert_records(records)
    print(f"Insertados en DB: {inserted} nuevos de {len(records)} registros Edomex")
    return inserted


def get_all_cards_data():
    cards_data = []
    try:
        response = requests.get(BASE_URL, headers=HEADERS, timeout=20, verify=False)
        if response.status_code != 200:
            print(f"Error al obtener página: {response.status_code}")
            return []

        soup = BeautifulSoup(response.content, "html.parser")
        paragraphs = soup.find_all("p", class_="text-align-center")

        for p in paragraphs:
            link = p.find("a")
            if not link:
                continue

            href = link.get("href", "")
            if not href or not any(ext in href.lower() for ext in ['.jpg', '.jpeg', '.png']):
                continue

            if href.startswith("//"):
                imagen_url = "https:" + href
            elif href.startswith("/"):
                imagen_url = urljoin(BASE_URL, href)
            elif not href.startswith("http"):
                imagen_url = urljoin(BASE_URL, href)
            else:
                imagen_url = href

            strong = link.find("strong")
            nombre = strong.get_text(strip=True) if strong else link.get_text(strip=True)

            if nombre and imagen_url:
                cards_data.append({"imagen_url": imagen_url, "nombre": nombre})

        print(f"Se encontraron {len(cards_data)} tarjetas en la página.")
        return cards_data

    except Exception as e:
        print(f"Error al obtener datos de las tarjetas: {e}")
        return []


def select_cards_for_shard(cards, shard_index=None, shard_count=None):
    """Selecciona la porción de tarjetas que corresponde a este shard."""
    if shard_index is None or not shard_count or shard_count <= 1:
        return cards
    if shard_index < 0 or shard_index >= shard_count:
        raise ValueError(
            f"shard_index debe estar entre 0 y {shard_count - 1} (recibido {shard_index})"
        )
    return cards[shard_index::shard_count]


def main() -> None:
    parser = argparse.ArgumentParser(description="Scraper JPG Estado de México")
    parser.add_argument("--shard-index", type=int, default=None, help="Índice 0-based del shard a procesar")
    parser.add_argument("--shard-count", type=int, default=None, help="Cantidad total de shards")
    parser.add_argument("--max-records", type=int, default=None, help="Limitar tarjetas procesadas")
    args = parser.parse_args()

    overall_start = time.time()

    cards = get_all_cards_data()
    if not cards:
        print("No se encontraron tarjetas.")
        return

    cards = select_cards_for_shard(cards, shard_index=args.shard_index, shard_count=args.shard_count)
    if args.shard_index is not None and args.shard_count and args.shard_count > 1:
        print(f"🔀 Shard {args.shard_index}/{args.shard_count}: {len(cards)} tarjetas asignadas.")

    existing_files = get_existing_files(None, S3_FOLDER)
    print(f"Archivos existentes en S3: {len(existing_files)}")

    if TEST_LIMIT:
        cards = cards[:TEST_LIMIT]
        print(f"Modo de prueba: procesando solo {TEST_LIMIT} imagen(es).")
    if args.max_records:
        cards = cards[:args.max_records]
        print(f"Limite CLI: procesando solo {args.max_records} tarjeta(s).")

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
