'''
 - [ ] Fiscalia General del Estado: [link](https://fiscaliaslp.gob.mx/vi/busqueda-de-personas-no-localizadas/)
    - Elementos en un div con nombre, foto
    - Al darles click, abre una nueva ventana con un JPG
Hashid: 2501_
'''

import os
import time
import re
from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
import urllib3
from urllib.parse import urljoin
import boto3
from botocore.exceptions import ClientError

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

s3 = boto3.client("s3")
BUCKET_NAME = "cdas-2025-alertas-amber"
S3_FOLDER = "jpg/"
HASH_ID = "2501_"
TEST_LIMIT = None  # Set to 1 for testing, None for full run

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
BASE_DATA_URL = "https://fiscaliaslp.gob.mx/DESAPARECIDOSFGE/"
LANDING_URL = urljoin(BASE_DATA_URL, "portada_pesquisa.php")
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


def get_available_years() -> List[int]:
    response = requests.get(LANDING_URL, headers=HEADERS, timeout=30, verify=False)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
    years = []
    for option in soup.select("select#anio option"):
        value = (option.get("value") or "").strip()
        if not value or value == "0":
            continue
        if value.isdigit():
            years.append(int(value))
    unique_years = sorted(set(years), reverse=True)
    print(f"Años detectados: {unique_years}")
    return unique_years


def parse_detail_href(href: str) -> str | None:
    if not href:
        return None
    match = re.search(r'popUp\("([^"]+)"\)', href)
    if not match:
        return None
    relative = match.group(1)
    return urljoin(BASE_DATA_URL, relative)


def extract_cards_from_soup(soup: BeautifulSoup, page_url: str, year: int) -> List[dict]:
    records = []
    for foto in soup.select("div.foto"):
        text_link = foto.find("a", class_="textofoto")
        img_tag = foto.find("img", class_="foto2")
        detail_link = foto.find("a")

        if not text_link or not img_tag:
            continue

        text_raw = text_link.get_text("|", strip=True)
        if "|" in text_raw:
            nombre, edad = [part.strip() for part in text_raw.split("|", 1)]
        else:
            nombre, edad = text_raw.strip(), None

        imagen_url = img_tag.get("src")
        if imagen_url:
            imagen_url = urljoin(page_url, imagen_url)

        detalle_url = parse_detail_href(detail_link.get("href") if detail_link else "") or page_url

        if not nombre or not imagen_url:
            continue

        records.append({
            "nombre": nombre,
            "edad": edad,
            "imagen_url": imagen_url,
            "detalle_url": detalle_url,
            "anio": year,
        })
    return records


def fetch_year_cards(year: int) -> Tuple[int, List[dict]]:
    page_url = urljoin(BASE_DATA_URL, f"busqueda2.php?tipo=pesquisas&year={year}")
    try:
        response = requests.get(page_url, headers=HEADERS, timeout=30, verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        cards = extract_cards_from_soup(soup, page_url, year)
        return year, cards
    except Exception as exc:
        print(f"Error al procesar el año {year}: {exc}")
        return year, []


def get_all_cards_data() -> List[dict]:
    years = get_available_years()
    if not years:
        print("No se pudieron detectar años disponibles.")
        return []

    records: List[dict] = []
    max_workers = min(MAX_WORKERS, len(years))
    print(f"Descargando datos de {len(years)} años con {max_workers} hilos...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_year_cards, year): year for year in years}
        for future in as_completed(futures):
            year, year_cards = future.result()
            print(f"Año {year}: {len(year_cards)} tarjetas.")
            records.extend(year_cards)

    print(f"Se extrajeron {len(records)} tarjetas en total.")
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
