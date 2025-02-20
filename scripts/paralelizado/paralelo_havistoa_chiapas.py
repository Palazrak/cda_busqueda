import requests
from bs4 import BeautifulSoup
import json
import time
import datetime
import random
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing

# Configuración de la base de datos
DB_NAME = "cda_busqueda"
DB_USER = "postgres"
DB_PASSWORD = "mysecretpassword"
DB_HOST = "postgres"
DB_PORT = "5432"

def insert_many_to_db(data_list: list, extraction_date: datetime.date):
    """Inserta registros en la base de datos."""
    if not data_list:
        return  

    start_time = time.time()  

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
            (extraction_date, url, json.dumps(data)) for url,data in data_list
        ])
        conn.commit()
        
    except Exception as e:
        print(f"❌ Error al conectar a la base de datos: {e}")
    
    finally:
        cur.close()
        conn.close()

    end_time = time.time()
    print(f"⏳ Tiempo de escritura en BD: {end_time - start_time:.2f} segundos")

# Extraer los URLs de cada ficha desde el JSON
def get_all_ficha_urls():
    """Obtiene todas las URLs de las fichas desde el JSON central."""
    json_url = "https://www.fge.chiapas.gob.mx/Servicios/Hasvistoa/JsonPersonasBusqueda"

    try:
        response = requests.post(json_url, timeout=10)
        if response.status_code != 200:
            print(f"❌ Error al obtener JSON: {response.status_code}")
            return []

        json_data = response.json()
        urls = [
            f"https://www.fge.chiapas.gob.mx/Servicios/Hasvistoa/HASVISTOA/{persona['id_persona']}"
            for persona in json_data
        ]
        print(f"✅ Se encontraron {len(urls)} fichas en el JSON.")
        return urls

    except Exception as e:
        print(f"❌ Error al obtener URLs desde el JSON: {e}")
        return []

# Función para extraer datos de una ficha
def extract_data(data_section: BeautifulSoup):
    """Extrae la información de una ficha desde una sección HTML."""
    try:
        return {
            "nombre": data_section.find('h3').get_text(strip=True),
            "imagen_url": data_section.find('img')['src'],
            "registro": data_section.find('p', class_='proile-rating').find('span').get_text(strip=True) if data_section.find('p', class_='proile-rating') else None,
            "sexo": data_section.find('label', string='Sexo:').find_next('p').get_text(strip=True),
            "estatura": data_section.find('label', string='Estatura:').find_next('p').get_text(strip=True),
            "tez": data_section.find('label', string='Tez:').find_next('p').get_text(strip=True),
            "ojos": data_section.find('label', string='Ojos:').find_next('p').get_text(strip=True),
            "cabello": data_section.find('label', string='Cabello:').find_next('p').get_text(strip=True),
            "peso": data_section.find('label', string='Peso:').find_next('p').get_text(strip=True),
            "fecha_desaparicion": data_section.find('label', string='Fecha desaparición:').find_next('p').get_text(strip=True),
            "complexion": data_section.find('label', string='Complexion:').find_next('p').get_text(strip=True),
            "boca": data_section.find('label', string='Boca:').find_next('p').get_text(strip=True),
            "tamano_nariz": data_section.find('label', string='Tamaño de nariz').find_next('p').get_text(strip=True),
            "tipo_nariz": data_section.find('label', string='Tipo de nariz:').find_next('p').get_text(strip=True),
            "escolaridad": data_section.find('label', string='Escolaridad:').find_next('p').get_text(strip=True),
            "originario_de": data_section.find('label', string='Originario de:').find_next('p').get_text(strip=True),
            "fecha_nacimiento": data_section.find('b', string='Fecha de nacimiento:').find_next('p').get_text(strip=True),
            "senas_particulares": data_section.find('strong', string='Señas Particulares:').find_next('p').get_text(strip=True),
            "circunstancia": data_section.find('strong', string='Circunstancia:').find_next('p').get_text(strip=True)
        }
    except:
        return None  

# Función para procesar cada ficha
def process_ficha(session, ficha_url):
    """Scrapea una ficha individual en segundo plano usando una sesión HTTP."""
    try:
        ficha_response = session.get(ficha_url)
        ficha_soup = BeautifulSoup(ficha_response.text, 'html.parser')

        if ficha_soup.select_one('h1.display-4'):
            return None  

        data_section = ficha_soup.select_one('div.emp-profile-wrap')
        ficha_data = extract_data(data_section) if data_section else None
        return (ficha_url, ficha_data) if ficha_data else None
    except:
        return None  


# Scraping en paralelo con listas independientes por worker
def scrape_all_fichas():
    """Scrapea todas las fichas en paralelo usando listas separadas por worker."""
    start_time = time.time()

    urls = get_all_ficha_urls()
    if not urls:
        return []

    num_cores = multiprocessing.cpu_count()
    max_workers = min(24, num_cores * 2)  # Usamos 24 threads

    worker_data = {i: [] for i in range(max_workers)}  # Diccionario con listas separadas

    with requests.Session() as session, ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_worker = {executor.submit(process_ficha, session, url): i % max_workers for i, url in enumerate(urls)}

        for future in as_completed(future_to_worker):
            worker_id = future_to_worker[future]
            result = future.result()
            
            if result:
                worker_data[worker_id].append(result)  # Cada worker escribe en su propia lista

    # 🔹 Fusionar todas las listas en una sola lista final
    all_data = []
    for worker_list in worker_data.values():
        all_data.extend(worker_list)

    end_time = time.time()
    print(f"⏳ Tiempo total de scraping: {end_time - start_time:.2f} segundos")

    return all_data

def main():
    """Ejecuta el scraping desde el JSON sin paginación."""
    global_start_time = time.time()

    extraction_date = datetime.date.today()
    # SCRAPING
    scraped_data = scrape_all_fichas()
    # Insercion en BD
    insert_many_to_db(scraped_data, extraction_date)

    global_end_time = time.time()
    print(f"⏳ Tiempo total de ejecución: {global_end_time - global_start_time:.2f} segundos")

if __name__ == '__main__':
    main()
