# ESTE SCRAPPER SÓLO SACA IMÁGENES DE LAS PERSONAS (SIN INFO)


import json
import datetime
import time

import psycopg2
import requests
from bs4 import BeautifulSoup
import urllib3

# Deshabilitar advertencias de SSL (el sitio no siempre entrega certificados completos)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DB_NAME = "cda_busqueda"
DB_USER = "postgres"
DB_PASSWORD = "mysecretpassword"
DB_HOST = "postgres"  # Cambiar a "postgres" si se ejecuta en Docker
DB_PORT = "5432"

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
BASE_URL = "https://portal.fgeguanajuato.gob.mx/PortalWebEstatal/AlertaAmber/Formularios/frmAAEstado.aspx"


def insert_many_to_db(data_list: list, extraction_date: datetime.date) -> None:
    """Inserta todos los registros procesados en la base de datos."""
    if not data_list:
        print("⚠️  No hay datos para insertar.")
        return

    print(f"💾 Preparando inserción de {len(data_list)} registros...")
    start = time.time()

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

    print(f"⏳ Tiempo de escritura en BD: {time.time() - start:.2f} segundos")


def get_all_cards_data() -> list:
    """Obtiene imagen y nombre de cada tarjeta en la página."""
    print(f"📥 Descargando datos desde {BASE_URL}...")
    try:
        response = requests.get(BASE_URL, headers=HEADERS, timeout=30, verify=False)
        response.raise_for_status()
    except Exception as exc:
        print(f"❌ No se pudo obtener la página: {exc}")
        return []

    soup = BeautifulSoup(response.content, "html.parser")
    figures = soup.select("figure.snip1527")
    print(f"🔎 Se detectaron {len(figures)} figuras en el HTML.")

    cards = []
    for fig in figures:
        name_tag = fig.select_one("p.tituloNoticia")
        img_tag = fig.select_one("div.image img")

        nombre = name_tag.get_text(" ", strip=True) if name_tag else None
        imagen_url = img_tag.get("src") if img_tag else None

        if not nombre or not imagen_url:
            continue

        cards.append(
            {
                "nombre": nombre,
                "imagen_url": imagen_url,
                "detalle_url": BASE_URL,
            }
        )

    print(f"✅ Se extrajeron {len(cards)} tarjetas válidas.")
    return cards


def scrape_all_cards() -> list:
    """Procesa todas las tarjetas y estructura los datos para la BD."""
    start = time.time()

    cards = get_all_cards_data()
    if not cards:
        print("⚠️  No se encontraron tarjetas para procesar.")
        return []

    result = []
    for card in cards:
        payload = {
            "imagen_url": card["imagen_url"],
            "nombre": card["nombre"],
            "estado_alerta": "Alerta Amber Guanajuato",
        }
        result.append((card["detalle_url"], payload))

    print(f"✅ {len(result)} tarjetas procesadas.")
    print(f"⏳ Tiempo de scraping: {time.time() - start:.2f} segundos")
    return result


def main() -> None:
    """Punto de entrada del script."""
    overall_start = time.time()
    extraction_date = datetime.date.today()

    scraped = scrape_all_cards()
    insert_many_to_db(scraped, extraction_date)

    print(f"⏱️  Tiempo total: {time.time() - overall_start:.2f} segundos")


if __name__ == "__main__":
    main()

