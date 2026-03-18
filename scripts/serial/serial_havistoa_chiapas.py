import requests
from bs4 import BeautifulSoup
import json
import time
import datetime
import random
import hashlib
import re
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing

# Configuración de la base de datos
DB_NAME = "cda_busqueda"
DB_USER = "postgres"
DB_PASSWORD = "mysecretpassword"
DB_HOST = "postgres"
DB_PORT = "5432"

# ------------------ Helpers: hash y S3 (S3 comentado) ------------------

# s3 = boto3.client("s3")
# S3_BUCKET = "cdas-2025-alertas-amber"
# S3_PREFIX = "pdf"

def normalize_for_hash(value):
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip().lower()

def make_hashid(parsed_data):
    """
    Genera el hash a partir de:
    folio, localizado, nombre, edad, descripcion_hechos, senas
    Prefijo: 0501_
    """
    parts = [
        normalize_for_hash(parsed_data.get("folio")),
        normalize_for_hash(parsed_data.get("localizado")),
        normalize_for_hash(parsed_data.get("nombre")),
        normalize_for_hash(parsed_data.get("edad")),
        normalize_for_hash(parsed_data.get("descripcion_hechos")),
        normalize_for_hash(parsed_data.get("senas")),
    ]
    joined = "||".join(parts)
    h = hashlib.sha256(joined.encode("utf-8")).hexdigest()[:10]
    hashid = f"0501_{h}"
    filename = f"{hashid}.pdf"
    # s3_key = f"{S3_PREFIX}/{filename}"
    s3_key = None  # S3 deshabilitado por ahora
    return hashid, filename, s3_key


def insert_into_db(data: dict, detalle_url: str, hashid: str):
    extraction_date = datetime.date.today()
    localizado = data.get("localizado", False)

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

        # Verificar existencia exactamente para (hashid, localizado)
        cur.execute(
            "SELECT 1 FROM public.desaparecidos WHERE hashid = %s AND localizado = %s LIMIT 1",
            (hashid, localizado),
        )
        exists = cur.fetchone() is not None
        if exists:
            print(f"✔ No hay que insertar: ya existe hashid={hashid} y localizado={localizado}")
            return False

        query = """
            INSERT INTO public.desaparecidos (fecha_extraccion, url_origen, localizado, hashid, datos)
            VALUES (%s, %s, %s, %s, %s)
        """
        cur.execute(query, (extraction_date, detalle_url, localizado, hashid, json.dumps(data)))
        conn.commit()
        print(f"✅ Insertado en DB: hashid={hashid}")
        return True

    except Exception as e:
        print(f"❌ Error al insertar en la base de datos: {e}")
        return False

    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

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
        text_all = data_section.get_text(" ", strip=True).lower() if data_section else ""
        if re.search(r"\bno\s*localizad", text_all, flags=re.IGNORECASE):
            localizado = False
        elif re.search(r"\blocalizad", text_all, flags=re.IGNORECASE):
            localizado = True
        else:
            localizado = False

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
            "circunstancia": data_section.find('strong', string='Circunstancia:').find_next('p').get_text(strip=True),
            "localizado": localizado,
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
    start_time = time.time()
    urls = get_all_ficha_urls()
    if not urls:
        return []
    
    all_data = []
    with requests.Session() as session:
        for url in urls:
            result = process_ficha(session, url)
            if result:
                all_data.append(result)
    
    end_time = time.time()
    print(f"⏳ Tiempo total de scraping (serial): {end_time - start_time:.2f} segundos")
    return all_data

def main():
    """Ejecuta el scraping desde el JSON sin paginación."""
    global_start_time = time.time()

    # SCRAPING
    scraped_data = scrape_all_fichas()

    # Insercion en BD (hash + deduplicación por hash/localizado)
    for detalle_url, ficha_data in scraped_data:
        if not ficha_data:
            continue

        # Normalizar campos para el hash (mismo esquema que Michoacán)
        ficha_data["folio"] = ficha_data.get("registro")
        ficha_data["senas"] = ficha_data.get("senas_particulares")
        ficha_data["descripcion_hechos"] = ficha_data.get("circunstancia")
        ficha_data["estado_alerta"] = "Desaparecidos Hasvistoa Chiapas"

        # Edad calculada con (fecha_desaparicion - fecha_nacimiento)
        fecha_nacimiento = ficha_data.get("fecha_nacimiento")
        fecha_desaparicion = ficha_data.get("fecha_desaparicion")
        edad = None
        if fecha_nacimiento and fecha_desaparicion:
            try:
                birth = datetime.datetime.strptime(fecha_nacimiento, "%d/%m/%Y")
                disp = datetime.datetime.strptime(fecha_desaparicion, "%d/%m/%Y")
                edad = disp.year - birth.year - ((disp.month, disp.day) < (birth.month, birth.day))
            except Exception:
                edad = None
        ficha_data["edad"] = edad

        hashid, filename, s3_key = make_hashid(ficha_data)

        # ---------------- S3 (COMENTADO) ----------------
        # Bloque intencionalmente comentado: todavía no tenemos PDF bytes en este scraper.
        # try:
        #     if not s3_object_exists(s3, S3_BUCKET, s3_key):
        #         uploaded = upload_pdf_to_s3_if_not_exists(pdf_bytes, S3_BUCKET, s3_key, s3)
        #     else:
        #         print(f"☑️ Ya existe en S3: s3://{S3_BUCKET}/{s3_key}")
        #         uploaded = False
        # except Exception as e:
        #     print(f"❌ Error verificando/guardando en S3: {e}")
        #     uploaded = False
        # ---------------- Fin S3 ----------------

        insert_into_db(ficha_data, detalle_url, hashid)

    global_end_time = time.time()
    print(f"⏳ Tiempo total de ejecución: {global_end_time - global_start_time:.2f} segundos")

if __name__ == '__main__':
    main()
