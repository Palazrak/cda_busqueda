import json
import datetime
import time
import re
from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
import requests
from bs4 import BeautifulSoup
import urllib3
from urllib.parse import urljoin

# Deshabilitar advertencias SSL (el sitio usa certificados legacy)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DB_NAME = "cda_busqueda"
DB_USER = "postgres"
DB_PASSWORD = "mysecretpassword"
DB_HOST = "postgres"  # Cambiar a "postgres" si se ejecuta en Docker
DB_PORT = "5432"

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
BASE_DATA_URL = "https://fiscaliaslp.gob.mx/DESAPARECIDOSFGE/"
LANDING_URL = urljoin(BASE_DATA_URL, "portada_pesquisa.php")
MAX_WORKERS = 8


def insert_many_to_db(data_list: List[Tuple[str, dict]], extraction_date: datetime.date) -> None:
    """Inserta registros en la base de datos."""
    if not data_list:
        print("⚠️  No hay datos para insertar.")
        return

    print(f"💾 Preparando inserción de {len(data_list)} registros...")
    start_time = time.time()

    conn = None
    cur = None
    try:
        print(f"🔌 Conectando a la base de datos en {DB_HOST}:{DB_PORT}...")
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        cur = conn.cursor()
        print("✅ Conexión exitosa a la base de datos.")

        insert_query = """
            INSERT INTO public.desaparecidos (fecha_extraccion, url_origen, datos)
            VALUES (%s, %s, %s)
        """
        rows = [
            (extraction_date, url, json.dumps(payload, ensure_ascii=False))
            for url, payload in data_list
        ]
        print(f"📝 Insertando {len(rows)} registros...")
        cur.executemany(insert_query, rows)
        conn.commit()
        print("✅ Inserción completada.")
    except Exception as exc:
        print(f"❌ Error al insertar en la base de datos: {exc}")
        import traceback

        traceback.print_exc()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    print(f"⏳ Tiempo de escritura en BD: {time.time() - start_time:.2f} segundos")


def get_available_years() -> List[int]:
    """Obtiene la lista de años desde la página de portada."""
    print(f"📥 Obteniendo lista de años desde {LANDING_URL}...")
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
    print(f"📅 Años detectados: {unique_years}")
    return unique_years


def parse_detail_href(href: str) -> str | None:
    """Convierte el javascript:popUp(...) a una URL absoluta."""
    if not href:
        return None
    match = re.search(r'popUp\("([^"]+)"\)', href)
    if not match:
        return None
    relative = match.group(1)
    return urljoin(BASE_DATA_URL, relative)


def extract_cards_from_soup(soup: BeautifulSoup, page_url: str, year: int) -> List[dict]:
    """Extrae tarjetas del HTML."""
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

        records.append(
            {
                "nombre": nombre,
                "edad": edad,
                "imagen_url": imagen_url,
                "detalle_url": detalle_url,
                "anio": year,
            }
        )

    return records


def fetch_year_cards(year: int) -> Tuple[int, List[dict]]:
    """Descarga y procesa una página de año específico."""
    page_url = urljoin(BASE_DATA_URL, f"busqueda2.php?tipo=pesquisas&year={year}")
    try:
        response = requests.get(page_url, headers=HEADERS, timeout=30, verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        cards = extract_cards_from_soup(soup, page_url, year)
        return year, cards
    except Exception as exc:
        print(f"❌ Error al procesar el año {year}: {exc}")
        return year, []


def get_all_cards_data() -> List[dict]:
    """Obtiene todas las tarjetas disponibles para cada año."""
    years = get_available_years()
    if not years:
        print("⚠️  No se pudieron detectar años disponibles.")
        return []

    records: List[dict] = []

    max_workers = min(MAX_WORKERS, len(years))
    print(f"⚙️  Descargando datos de {len(years)} años con {max_workers} hilos...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_year_cards, year): year for year in years}
        for future in as_completed(futures):
            year, year_cards = future.result()
            print(f"🔎 Año {year}: {len(year_cards)} tarjetas.")
            records.extend(year_cards)

    print(f"✅ Se extrajeron {len(records)} tarjetas en total.")
    return records


def scrape_all_cards() -> List[Tuple[str, dict]]:
    """Estructura los datos para la inserción."""
    start_time = time.time()

    cards = get_all_cards_data()
    if not cards:
        return []

    data = []
    for card in cards:
        payload = {
            "imagen_url": card["imagen_url"],
            "nombre": card["nombre"],
            "edad": card.get("edad"),
            "anio": card.get("anio"),
            "estado_alerta": "Fiscalía SLP Desaparecidos",
        }
        url_origen = card.get("detalle_url") or card["imagen_url"]
        data.append((url_origen, payload))

    print(f"⏳ Tiempo de scraping: {time.time() - start_time:.2f} segundos")
    return data


def main() -> None:
    overall_start = time.time()
    extraction_date = datetime.date.today()

    scraped = scrape_all_cards()
    insert_many_to_db(scraped, extraction_date)

    print(f"⏱️  Tiempo total: {time.time() - overall_start:.2f} segundos")


if __name__ == "__main__":
    main()

