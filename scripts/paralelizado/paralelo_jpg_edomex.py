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

BASE_URL = "https://cobupem.edomex.gob.mx/boletines-personas-desaparecidas"


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
    """Obtiene todas las tarjetas con imagen y nombre desde la página."""
    cards_data = []
    
    try:
        response = requests.get(BASE_URL, headers=HEADERS, timeout=20, verify=False)
        if response.status_code != 200:
            print(f"❌ Error al obtener página: {response.status_code}")
            return []

        soup = BeautifulSoup(response.content, "html.parser")
        
        # Buscar todos los párrafos con class text-align-center que contienen los links
        paragraphs = soup.find_all("p", class_="text-align-center")
        
        for p in paragraphs:
            link = p.find("a")
            if not link:
                continue
            
            href = link.get("href", "")
            # Verificar que el href apunta a una imagen
            if not href or not any(ext in href.lower() for ext in ['.jpg', '.jpeg', '.png']):
                continue
            
            # Convertir URL protocol-relative (//) a absoluta
            if href.startswith("//"):
                imagen_url = "https:" + href
            elif href.startswith("/"):
                imagen_url = urljoin(BASE_URL, href)
            elif not href.startswith("http"):
                imagen_url = urljoin(BASE_URL, href)
            else:
                imagen_url = href
            
            # Extraer el nombre del <strong> dentro del <a> o del texto del <a>
            strong = link.find("strong")
            if strong:
                nombre = strong.get_text(strip=True)
            else:
                nombre = link.get_text(strip=True)
            
            # Solo agregar si tenemos nombre e imagen
            if nombre and imagen_url:
                cards_data.append({
                    "imagen_url": imagen_url,
                    "nombre": nombre,
                    "detalle_url": BASE_URL  # La página de origen
                })
        
        print(f"✅ Se encontraron {len(cards_data)} tarjetas en la página.")
        return cards_data

    except Exception as e:
        print(f"❌ Error al obtener datos de las tarjetas: {e}")
        return []


def scrape_all_cards():
    """Scrapea todas las tarjetas en paralelo."""
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
                "estado_alerta": "Desaparecidos Estado de México"
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

