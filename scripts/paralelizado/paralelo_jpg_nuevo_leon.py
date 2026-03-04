import requests
from bs4 import BeautifulSoup
import json
import time
import datetime
import psycopg2
from urllib.parse import urljoin
import urllib3

# Deshabilitar advertencias de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuración de la base de datos
DB_NAME = "cda_busqueda"
DB_USER = "postgres"
DB_PASSWORD = "mysecretpassword"
DB_HOST = "postgres"  # Cambiar a "postgres" si se ejecuta en Docker
DB_PORT = "5432"

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

BASE_URL = "https://www.hcnl.gob.mx/desaparecidos/"
DATA_URL = "https://www.hcnl.gob.mx/desaparecidos/desaparecidos.txt"


def insert_many_to_db(data_list: list, extraction_date: datetime.date):
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
            port=DB_PORT
        )
        cur = conn.cursor()
        print("✅ Conexión exitosa a la base de datos.")

        insert_query = """
            INSERT INTO public.desaparecidos (fecha_extraccion, url_origen, datos)
            VALUES (%s, %s, %s)
        """
        
        records = [
            (extraction_date, url, json.dumps(data, ensure_ascii=False)) 
            for url, data in data_list
        ]
        
        print(f"📝 Insertando {len(records)} registros...")
        cur.executemany(insert_query, records)
        conn.commit()
        print(f"✅ Insertados {len(data_list)} registros en la BD correctamente.")

    except Exception as e:
        print(f"❌ Error al insertar en la base de datos: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    end_time = time.time()
    print(f"⏳ Tiempo de escritura en BD: {end_time - start_time:.2f} segundos")


def get_all_cards_data():
    """Obtiene todas las tarjetas con imagen y nombre desde el archivo JSON."""
    cards_data = []
    
    try:
        print(f"📥 Descargando datos desde {DATA_URL}...")
        response = requests.get(DATA_URL, headers=HEADERS, timeout=20, verify=False)
        if response.status_code != 200:
            print(f"❌ Error al obtener datos: {response.status_code}")
            return []

        # Parsear el JSON
        data_json = json.loads(response.text)
        records = data_json.get("data", [])
        print(f"📊 Se encontraron {len(records)} registros en el archivo JSON.")
        
        for record in records:
            if not record or len(record) < 2:
                continue
            
            # El primer elemento contiene el HTML con la imagen y el nombre
            image_html = record[0] if len(record) > 0 else ""
            name_html = record[1] if len(record) > 1 else ""
            fecha_desaparicion = record[2] if len(record) > 2 else ""
            estatus = record[3] if len(record) > 3 else ""
            
            # Parsear el HTML para extraer imagen y nombre
            soup_image = BeautifulSoup(image_html, "html.parser")
            soup_name = BeautifulSoup(name_html, "html.parser")
            
            imagen_url = None
            nombre = None
            
            # Buscar el link con la imagen
            image_link = soup_image.find("a", class_="shadow")
            if image_link:
                # La imagen original está en el href
                imagen_url = image_link.get("href", "")
                # El nombre está en el atributo title
                nombre = image_link.get("title", "")
            
            # Si no encontramos el nombre en el image_link, buscarlo en name_html
            if not nombre:
                name_link = soup_name.find("a")
                if name_link:
                    # Buscar el h5 dentro del link
                    h5 = name_link.find("h5")
                    if h5:
                        # Extraer el texto sin el icono
                        nombre = h5.get_text(strip=True)
                        # Limpiar el texto (remover el icono de external link si está)
                        nombre = nombre.replace("  ", " ").strip()
            
            # Solo agregar si tenemos nombre e imagen
            if nombre and imagen_url:
                cards_data.append({
                    "imagen_url": imagen_url,
                    "nombre": nombre,
                    "fecha_desaparicion": fecha_desaparicion,
                    "estatus": estatus,
                    "detalle_url": BASE_URL
                })
        
        print(f"✅ Se encontraron {len(cards_data)} tarjetas válidas.")
        return cards_data

    except Exception as e:
        print(f"❌ Error al obtener datos de las tarjetas: {e}")
        import traceback
        traceback.print_exc()
        return []


def scrape_all_cards():
    """Scrapea todas las tarjetas."""
    start_time = time.time()

    cards_data = get_all_cards_data()
    if not cards_data:
        print("⚠️  No se encontraron tarjetas para procesar.")
        return []

    print(f"📊 Procesando {len(cards_data)} tarjetas...")
    
    # Procesar directamente sin verificar imágenes (más rápido)
    all_data = []
    for card in cards_data:
        try:
            data = {
                "imagen_url": card["imagen_url"],
                "nombre": card.get("nombre"),
                "fecha_desaparicion": card.get("fecha_desaparicion"),
                "estatus": card.get("estatus"),
                "estado_alerta": "Desaparecidos Nuevo León"
            }
            url_origen = card.get("detalle_url") or card["imagen_url"]
            all_data.append((url_origen, data))
        except Exception:
            continue

    end_time = time.time()
    print(f"⏳ Tiempo total de scraping: {end_time - start_time:.2f} segundos")
    print(f"✅ {len(all_data)} tarjetas procesadas correctamente.")

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

