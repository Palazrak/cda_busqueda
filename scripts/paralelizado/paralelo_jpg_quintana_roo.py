
import json
import datetime
import time
from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
import requests
from bs4 import BeautifulSoup
import urllib3
from urllib.parse import urljoin

# Deshabilitamos advertencias SSL (el sitio usa certificados intermedios)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DB_NAME = "cda_busqueda"
DB_USER = "postgres"
DB_PASSWORD = "mysecretpassword"
DB_HOST = "postgres"  # Cambiar a "postgres" si se ejecuta en Docker
DB_PORT = "5432"

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
BASE_URL = "https://www.fgeqroo.gob.mx/servicio-social/Extraviado"
MAX_WORKERS = 12


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


def extract_cards_from_soup(soup: BeautifulSoup, detail_url: str) -> list:
    """Extrae tarjetas desde el HTML ya parseado."""
    cards = soup.select("div.detalle-com")
    page_records = []

    for card in cards:
        name_tag = card.find("h3")
        img_tag = card.find("img")
        date_tag = card.find("small")

        nombre = name_tag.get_text(" ", strip=True) if name_tag else None
        imagen_url = None
        if img_tag and img_tag.get("src"):
            imagen_url = img_tag["src"]
            if not imagen_url.startswith("http"):
                imagen_url = urljoin(BASE_URL, imagen_url)

        fecha_reporte = date_tag.get_text(strip=True) if date_tag else None

        if not nombre or not imagen_url:
            continue

        page_records.append(
            {
                "nombre": nombre,
                "imagen_url": imagen_url,
                "fecha_reporte": fecha_reporte,
                "detalle_url": detail_url,
            }
        )

    return page_records


def get_total_pages(soup: BeautifulSoup) -> int:
    """Detecta el número máximo de páginas desde la paginación."""
    page_numbers = []
    for link in soup.select("ul.pagination a"):
        text = link.get_text(strip=True)
        if text.isdigit():
            page_numbers.append(int(text))
    return max(page_numbers) if page_numbers else 1


def get_all_cards_data() -> list:
    """Recorre todas las páginas y obtiene los registros."""
    print(f"📥 Descargando datos desde {BASE_URL}...")
    try:
        response = requests.get(BASE_URL, headers=HEADERS, timeout=30, verify=False)
        response.raise_for_status()
    except Exception as exc:
        print(f"❌ Error al obtener la página inicial: {exc}")
        return []

    soup = BeautifulSoup(response.content, "html.parser")
    total_pages = get_total_pages(soup)
    print(f"📄 Total estimado de páginas: {total_pages}")

    records = []

    # Procesar la primera página con el HTML ya descargado
    first_page_records = extract_cards_from_soup(soup, BASE_URL)
    print(f"🔎 Página 1: {len(first_page_records)} tarjetas.")
    records.extend(first_page_records)

    remaining_pages = list(range(2, total_pages + 1))
    if not remaining_pages:
        print(f"✅ Se extrajeron {len(records)} tarjetas válidas en total.")
        return records

    max_workers = min(MAX_WORKERS, len(remaining_pages))
    print(f"⚙️  Descargando {len(remaining_pages)} páginas adicionales con {max_workers} hilos...")

    def fetch_page(page_num: int) -> Tuple[int, list]:
        page_url = f"{BASE_URL}?page={page_num}"
        try:
            resp = requests.get(page_url, headers=HEADERS, timeout=30, verify=False)
            resp.raise_for_status()
            page_soup = BeautifulSoup(resp.content, "html.parser")
            page_records = extract_cards_from_soup(page_soup, page_url)
            return page_num, page_records
        except Exception as exc:
            print(f"❌ Error al obtener la página {page_num}: {exc}")
            return page_num, []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_page = {executor.submit(fetch_page, page): page for page in remaining_pages}
        for future in as_completed(future_to_page):
            page_num, page_records = future.result()
            print(f"🔎 Página {page_num}: {len(page_records)} tarjetas.")
            records.extend(page_records)

    print(f"✅ Se extrajeron {len(records)} tarjetas válidas en total.")
    return records


def scrape_all_cards() -> List[Tuple[str, dict]]:
    """Procesa las tarjetas y prepara la carga para la BD."""
    start_time = time.time()

    cards = get_all_cards_data()
    if not cards:
        print("⚠️  No se encontraron tarjetas para procesar.")
        return []

    data = []
    for card in cards:
        payload = {
            "imagen_url": card["imagen_url"],
            "nombre": card["nombre"],
            "fecha_reporte": card.get("fecha_reporte"),
            "estado_alerta": "Servicio Social Quintana Roo",
        }
        data.append((card["detalle_url"], payload))

    print(f"✅ {len(data)} tarjetas procesadas.")
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

