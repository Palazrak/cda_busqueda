import requests
from bs4 import BeautifulSoup
import json
import time
import datetime
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
from urllib.parse import urljoin
import urllib3
import re

# Deshabilitar advertencias de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuración de la base de datos
DB_NAME = "cda_busqueda"
DB_USER = "postgres"
DB_PASSWORD = "mysecretpassword"
DB_HOST = "postgres"  # Cambiar a "postgres" si se ejecuta en Docker
DB_PORT = "5432"

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

BASE_URL = "https://fiscalia.chihuahua.gob.mx/desaparecidos/"


def insert_many_to_db(data_list: list, extraction_date: datetime.date):
    """Inserta registros en la base de datos."""
    if not data_list:
        return

    start_time = time.time()

    conn = None
    cur = None
    
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()

        insert_query = """
            INSERT INTO public.desaparecidos (fecha_extraccion, url_origen, datos)
            VALUES (%s, %s, %s)
        """
        cur.executemany(insert_query, [
            (extraction_date, url, json.dumps(data, ensure_ascii=False)) for url, data in data_list
        ])
        conn.commit()
        print(f"✅ Insertados {len(data_list)} registros en la BD.")

    except Exception as e:
        print(f"❌ Error al conectar a la base de datos: {e}")

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    end_time = time.time()
    print(f"⏳ Tiempo de escritura en BD: {end_time - start_time:.2f} segundos")


def get_all_cards_data():
    """Obtiene todas las tarjetas con imagen y nombre desde la página."""
    cards_data = []
    
    try:
        response = requests.get(BASE_URL, headers=HEADERS, timeout=20, verify=False)
        if response.status_code != 200:
            print(f"❌ Error al obtener página: {response.status_code}")
            return []

        soup = BeautifulSoup(response.content, "html.parser")
        
        # Buscar divs con la clase específica que contienen background-image
        thumbnail_divs = soup.find_all("div", class_="anwp-pg-post-teaser__thumbnail-img")
        
        for div in thumbnail_divs:
            # Buscar el contenedor padre que contiene tanto la imagen como el nombre
            # El thumbnail está en anwp-pg-post-teaser__thumbnail, pero el título está en anwp-pg-post-teaser
            # Necesitamos subir dos niveles: thumbnail -> anwp-pg-post-teaser__thumbnail -> anwp-pg-post-teaser
            thumbnail_parent = div.find_parent("div", class_=re.compile(r"anwp-pg-post-teaser__thumbnail"))
            card_container = None
            if thumbnail_parent:
                # Buscar el contenedor principal (padre del thumbnail_parent)
                # Buscar un div que tenga "anwp-pg-post-teaser" pero no "__thumbnail"
                for parent in thumbnail_parent.find_parents("div"):
                    classes = parent.get("class", [])
                    class_str = " ".join(classes) if classes else ""
                    if "anwp-pg-post-teaser" in class_str and "__thumbnail" not in class_str:
                        card_container = parent
                        break
            
            # Si no encontramos, buscar directamente desde el div
            if not card_container:
                for parent in div.find_parents("div"):
                    classes = parent.get("class", [])
                    class_str = " ".join(classes) if classes else ""
                    if "anwp-pg-post-teaser" in class_str and "__thumbnail" not in class_str:
                        card_container = parent
                        break
            
            # Fallback: buscar cualquier contenedor padre
            if not card_container:
                card_container = div.find_parent("div")
            
            imagen_url = None
            nombre = None
            detalle_url = None
            
            # Extraer imagen del background-image
            style = div.get("style", "")
            if style:
                match = re.search(r'background-image:\s*url\(["\']?([^"\']+)["\']?\)', style)
                if match:
                    image_url = match.group(1).strip()
                    imagen_url = urljoin(BASE_URL, image_url)
            
            # Buscar el nombre en el div de título dentro del mismo contenedor
            if card_container:
                title_div = card_container.find("div", class_="anwp-pg-post-teaser__title")
                if title_div:
                    name_link = title_div.find("a", class_="anwp-link-without-effects")
                    if name_link:
                        nombre = name_link.get_text(strip=True)
                        detalle_url = name_link.get("href", "")
                        if detalle_url and not detalle_url.startswith("http"):
                            detalle_url = urljoin(BASE_URL, detalle_url)
            
            # Solo agregar si tenemos al menos la imagen
            if imagen_url:
                cards_data.append({
                    "imagen_url": imagen_url,
                    "nombre": nombre,
                    "detalle_url": detalle_url or imagen_url  # Usar imagen_url como fallback
                })
        
        print(f"✅ Se encontraron {len(cards_data)} tarjetas en la página.")
        return cards_data

    except Exception as e:
        print(f"❌ Error al obtener datos de las tarjetas: {e}")
        return []


def process_card(session, card_data):
    """Procesa una tarjeta individual."""
    try:
        imagen_url = card_data["imagen_url"]
        # Verificar que la URL de imagen es válida
        response = session.head(imagen_url, timeout=10, allow_redirects=True, verify=False)
        if response.status_code == 200:
            # Crear JSON con imagen_url y nombre
            data = {
                "imagen_url": imagen_url,
                "nombre": card_data.get("nombre"),
                "estado_alerta": "Desaparecidos Chihuahua"
            }
            # Usar detalle_url como url_origen, o imagen_url como fallback
            url_origen = card_data.get("detalle_url") or imagen_url
            return (url_origen, data)
    except Exception as e:
        pass  # Silenciar errores individuales
    
    return None


def scrape_all_cards():
    """Scrapea todas las tarjetas en paralelo."""
    start_time = time.time()

    cards_data = get_all_cards_data()
    if not cards_data:
        return []

    num_cores = multiprocessing.cpu_count()
    max_workers = min(24, num_cores * 2)

    worker_data = {i: [] for i in range(max_workers)}

    with requests.Session() as session:
        session.headers.update(HEADERS)
        session.verify = False  # Deshabilitar verificación SSL
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_worker = {
                executor.submit(process_card, session, card): i % max_workers
                for i, card in enumerate(cards_data)
            }

            for future in as_completed(future_to_worker):
                worker_id = future_to_worker[future]
                result = future.result()

                if result:
                    worker_data[worker_id].append(result)

    # Fusionar todas las listas
    all_data = []
    for worker_list in worker_data.values():
        all_data.extend(worker_list)

    end_time = time.time()
    print(f"⏳ Tiempo total de scraping: {end_time - start_time:.2f} segundos")

    return all_data


def main():
    """Ejecuta el scraping."""
    global_start_time = time.time()

    extraction_date = datetime.date.today()
    
    # SCRAPING
    scraped_data = scrape_all_cards()
    
    # INSERCIÓN EN BD
    insert_many_to_db(scraped_data, extraction_date)

    global_end_time = time.time()
    print(f"⏳ Tiempo total de ejecución: {global_end_time - global_start_time:.2f} segundos")


if __name__ == '__main__':
    main()

