import requests
from bs4 import BeautifulSoup
import json
import time
import datetime
import hashlib
import re
import psycopg2

# Configuración de la base de datos
DB_NAME = "cda_busqueda"
DB_USER = "postgres"
DB_PASSWORD = "mysecretpassword"
DB_HOST = "postgres"
DB_PORT = "5432"

#
# ------------------ Helpers: hash y S3 (S3 comentado) ------------------
#

# s3 = boto3.client("s3")
# S3_BUCKET = "cdas-2025-alertas-amber"
# S3_PREFIX = "pdf"

def normalize_for_hash(value):
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip().lower()

def make_hashid(parsed_data):
    """
    Genera hash a partir de:
    folio, localizado, nombre, edad, descripcion_hechos, senas
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
    hashid = f"0502_{h}"
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
            port=DB_PORT,
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

# Obtener todas las URLs de fichas desde el JSON
def get_all_ficha_urls():
    json_url = "https://www.amberchiapas.org.mx/WebService.asmx/JsonAlertasGeneral"
    
    try:
        response = requests.get(json_url, timeout=10)
        if response.status_code != 200:
            print(f"❌ Error al obtener JSON: {response.status_code}")
            return []
        
        json_data = response.json()
        urls = [
            f"https://www.amberchiapas.org.mx/Detalle.aspx?carpeta={persona['id_persona']}&tipoAlerta={persona['tipo']}"
            for persona in json_data
        ]
        print(f"✅ Se encontraron {len(urls)} fichas en el JSON.")
        return urls
    
    except Exception as e:
        print(f"❌ Error al obtener URLs desde el JSON: {e}")
        return []

# Extraer los datos de una ficha
def extract_data(html_content):
    """Extrae la información de una ficha de una manera más eficiente."""
    soup = BeautifulSoup(html_content, 'html.parser')
    base_url = "https://www.amberchiapas.org.mx"

    # Buscar el contenedor de los datos
    data_section = soup.find('div', id='contenedorLocalesWrap') or soup.find('div', id='contenedorColaboracionesWrap')
    if not data_section:
        print("❌ No se encontró la sección de datos.")
        return None

    # Extraer nombre concatenando los h1
    nombre = " ".join(h.get_text(strip=True) for h in data_section.find_all('h1'))

    # Extraer la URL de la imagen y limpiarla
    imagen_tag = data_section.find('img')
    imagen_url = f"{base_url}/{imagen_tag['src'].lstrip('/')}" if imagen_tag else None
    imagen_url = imagen_url.replace("../", "")

    # Determinar localizado (heurística basada en texto)
    full_text = html_content.lower()
    if re.search(r"\bno\s*localizad", full_text, flags=re.IGNORECASE):
        localizado = False
    elif re.search(r"\blocalizad", full_text, flags=re.IGNORECASE):
        localizado = True
    else:
        localizado = False

    # Extraer datos generales
    data = {
        "reporte_num": (soup.find('p', class_='pNoAlerta') or {}).get_text(strip=True),
        "nombre": nombre,
        "imagen_url": imagen_url,
        "senas_particulares": (soup.find('p', class_='pSenText') or {}).get_text(strip=True),
        "descripcion_hechos": (soup.find('p', class_='pDesText') or {}).get_text(strip=True)
    }

    # Extraer información de los párrafos con clase 'p1'
    mapping = {
        "Fecha de nacimiento": "fecha_nacimiento",
        "Edad": "edad",
        "Fecha de Hechos": "fecha_hechos",
        "Lugar de Hechos": "lugar_hechos",
        "Cabello": "cabello",
        "Color de Ojos": "color_ojos",
        "Estatura": "estatura",
        "Peso": "peso"
    }

    for p in data_section.find_all('p', class_='p1'):
        text = p.get_text(strip=True)
        for key, value in mapping.items():
            if key in text:
                data[value] = (p.find('b', class_='p2') or {}).get_text(strip=True)

    # Normalizar nombres de campos para el hash (igual que Michoacán)
    data["folio"] = data.get("reporte_num")
    data["senas"] = data.get("senas_particulares")
    data["localizado"] = localizado
    data["estado_alerta"] = "Desaparecidos Amber Chiapas"

    return data if any(data.values()) else None  # Retorna None si no hay datos útiles

# Scraping de fichas en forma **SECUENCIAL**
def scrape_all_fichas():
    start_time = time.time()
    urls = get_all_ficha_urls()
    if not urls:
        return []
    
    all_data = []
    
    for url in urls:  # 🔹 **Ejecuta el scraping secuencialmente (SIN PARALELIZACIÓN)**
        try:
            response = requests.get(url)
            if response.status_code != 200:
                continue

            ficha_data = extract_data(response.text)
            if ficha_data:
                all_data.append((url, ficha_data))

        except Exception as e:
            print(f"❌ Error al procesar ficha {url}: {e}")

    end_time = time.time()
    print(f"⏳ Tiempo total de scraping (serial): {end_time - start_time:.2f} segundos")
    return all_data

def main():
    global_start_time = time.time()
    
    # 🔄 Ejecutando scraping en **modo serial**
    scraped_data = scrape_all_fichas()

    # 🔄 Insertar datos en BD (con hash + deduplicación por hashid/localizado)
    for detalle_url, ficha_data in scraped_data:
        if not ficha_data:
            continue

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
