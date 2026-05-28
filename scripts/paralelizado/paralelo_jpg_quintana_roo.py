'''
  - [ ] Fiscalia General del Estado de Quintana Roo: [link](https://www.fgeqroo.gob.mx/servicio-social/Extraviado)
    - Ventanas con elementos en un grid
    - Al darle click a cada foto, aparece una ventana mas grande con un JPG
    - Hay para seleccionar siguiente y fin, se actualiza el page en el URL
Hashid: 2401_
'''

import os
import time
from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
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
HASH_ID = "2401_"
TEST_LIMIT = None  # Set to 1 for testing, None for full run

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
BASE_URL = "https://www.fgeqroo.gob.mx/servicio-social/Extraviado"
MAX_WORKERS = 12


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


def extract_cards_from_soup(soup: BeautifulSoup, detail_url: str) -> list:
    cards = soup.select("div.detalle-com")
    page_records = []

    for card in cards:
        img_tag = card.find("img")
        imagen_url = None
        if img_tag and img_tag.get("src"):
            imagen_url = img_tag["src"]
            if not imagen_url.startswith("http"):
                imagen_url = urljoin(BASE_URL, imagen_url)

        if not imagen_url:
            continue

        page_records.append({"imagen_url": imagen_url})

    return page_records


def get_total_pages(soup: BeautifulSoup) -> int:
    page_numbers = []
    for link in soup.select("ul.pagination a"):
        text = link.get_text(strip=True)
        if text.isdigit():
            page_numbers.append(int(text))
    return max(page_numbers) if page_numbers else 1


def get_all_cards_data() -> list:
    print(f"Descargando datos desde {BASE_URL}...")
    try:
        response = requests.get(BASE_URL, headers=HEADERS, timeout=30, verify=False)
        response.raise_for_status()
    except Exception as exc:
        print(f"Error al obtener la página inicial: {exc}")
        return []

    soup = BeautifulSoup(response.content, "html.parser")
    total_pages = get_total_pages(soup)
    print(f"Total estimado de páginas: {total_pages}")

    records = extract_cards_from_soup(soup, BASE_URL)
    print(f"Página 1: {len(records)} tarjetas.")

    remaining_pages = list(range(2, total_pages + 1))
    if not remaining_pages:
        return records

    max_workers = min(MAX_WORKERS, len(remaining_pages))

    def fetch_page(page_num: int) -> Tuple[int, list]:
        page_url = f"{BASE_URL}?page={page_num}"
        try:
            resp = requests.get(page_url, headers=HEADERS, timeout=30, verify=False)
            resp.raise_for_status()
            page_soup = BeautifulSoup(resp.content, "html.parser")
            return page_num, extract_cards_from_soup(page_soup, page_url)
        except Exception as exc:
            print(f"Error al obtener la página {page_num}: {exc}")
            return page_num, []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_page = {executor.submit(fetch_page, page): page for page in remaining_pages}
        for future in as_completed(future_to_page):
            page_num, page_records = future.result()
            print(f"Página {page_num}: {len(page_records)} tarjetas.")
            records.extend(page_records)

    print(f"Se extrajeron {len(records)} tarjetas válidas en total.")
    return records


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
