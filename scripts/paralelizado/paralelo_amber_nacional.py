import requests
from bs4 import BeautifulSoup
import re
import json
import datetime
import psycopg2
import fitz  # PyMuPDF
import base64
import time
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuración de la base de datos
DB_NAME = "cda_busqueda"
DB_USER = "postgres"
DB_PASSWORD = "mysecretpassword"
DB_HOST = "postgres"
DB_PORT = "5432"

# Diccionario para mapear el id_estado con el estado correspondiente (orden alfabético)
estado_mapping = {
    0: "NACIONAL",
    2: "AGUASCALIENTES",
    3: "BAJA CALIFORNIA",
    4: "BAJA CALIFORNIA SUR",
    5: "CAMPECHE",
    6: "CHIAPAS",
    7: "CHIHUAHUA",
    8: "CIUDAD DE MÉXICO",
    9: "COAHUILA",
    10: "COLIMA",
    11: "DURANGO",
    12: "ESTADO DE MÉXICO",
    13: "GUANAJUATO",
    14: "GUERRERO",
    15: "HIDALGO",
    16: "JALISCO",
    17: "MICHOACÁN",
    18: "MORELOS",
    19: "NAYARIT",
    20: "NUEVO LEÓN",
    21: "OAXACA",
    22: "PUEBLA",
    23: "QUERÉTARO",
    24: "QUINTANA ROO",
    25: "SAN LUIS POTOSÍ",
    26: "SINALOA",
    27: "SONORA",
    28: "TABASCO",
    29: "TAMAULIPAS",
    30: "TLAXCALA",
    31: "VERACRUZ",
    32: "YUCATÁN",
    33: "ZACATECAS"
}

def insert_many_to_db(data_list, extraction_date, source_url):
    if not data_list:
        return
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
        records = [
            (extraction_date, source_url, json.dumps(data, ensure_ascii=False))
            for data in data_list
        ]
        cur.executemany(insert_query, records)
        conn.commit()
        print(f"✅ Insertados {len(records)} registros en la BD.")
    except Exception as e:
        print("Error insertando en BD:", e)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def parse_pdf_text(text):
    data = {}
    match = re.search(r'REPORTE NÚM\.\s*:\s*([\w\d]+)', text)
    data['reporte_num'] = match.group(1).strip() if match else None

    match = re.search(r'FECHA DE ACTIVACIÓN\s*:\s*([0-9/]+)', text)
    data['fecha_activacion'] = match.group(1).strip() if match else None

    lines = text.splitlines()
    nombre = None
    for i, line in enumerate(lines):
        if "FECHA DE ACTIVACIÓN" in line:
            for j in range(i+1, len(lines)):
                candidate = lines[j].strip()
                if candidate and not re.search(
                    r'^(REPORTE NÚM\.|FECHA DE ACTIVACIÓN|FECHA DE NACIMIENTO|EDAD|GÉNERO|FECHA DE LOS HECHOS|LUGAR DE LOS HECHOS|NACIONALIDAD|CABELLO|COLOR\s*:|COLOR DE OJOS|ESTATURA|PESO|SEÑAS PARTICULARES|RESUMEN)',
                    candidate, re.IGNORECASE):
                    nombre = candidate
                    break
            break
    data['nombre'] = nombre

    match = re.search(r'FECHA DE NACIMIENTO\s*:\s*([0-9/]+)', text)
    data['fecha_nacimiento'] = match.group(1).strip() if match else None

    match = re.search(r'EDAD\s*:\s*(\d+)', text, re.IGNORECASE)
    data['edad'] = match.group(1).strip() if match else None

    match = re.search(r'GÉNERO\s*:\s*([\w]+)', text)
    data['genero'] = match.group(1).strip() if match else None

    match = re.search(r'FECHA DE LOS\s+HECHOS\s*:\s*([0-9/]+)', text, re.IGNORECASE)
    data['fecha_hechos'] = match.group(1).strip() if match else None

    match = re.search(r'LUGAR DE LOS\s+HECHOS\s*:\s*([^\n]+)', text, re.IGNORECASE)
    data['lugar_hechos'] = match.group(1).strip() if match else None

    match = re.search(r'NACIONALIDAD\s*:\s*([\w]+)', text)
    data['nacionalidad'] = match.group(1).strip() if match else None

    match = re.search(r'CABELLO\s*:\s*([\w]+)', text)
    data['cabello'] = match.group(1).strip() if match else None

    match = re.search(r'COLOR\s*:\s*([\w]+)', text)
    data['color'] = match.group(1).strip() if match else None

    match = re.search(r'COLOR DE OJOS\s*:\s*([\w_]+)', text, re.IGNORECASE)
    if match:
        data['color_ojos'] = match.group(1).strip().replace("_", " ").lower()
    else:
        data['color_ojos'] = None

    match = re.search(r'ESTATURA\s*:\s*([\d\.]+\s*m)', text, re.IGNORECASE)
    data['estatura'] = match.group(1).strip() if match else None

    match = re.search(r'PESO\s*:\s*([\d]+\s*kg)', text, re.IGNORECASE)
    data['peso'] = match.group(1).strip() if match else None

    match = re.search(r'SEÑAS PARTICULARES\s*:\s*(.*?)\s*(?=RESUMEN\s+DE LOS HECHOS)', text, re.DOTALL | re.IGNORECASE)
    data['senas_particulares'] = " ".join(match.group(1).split()) if match else None

    match = re.search(r'RESUMEN (?:DE LOS|DE) HECHOS\s*:\s*(.*?)(?:LADA|$)', text, re.DOTALL | re.IGNORECASE)
    data['resumen_hechos'] = " ".join(match.group(1).split()) if match else None

    return data

def extract_pdf_text_from_bytes(pdf_bytes):
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        text = ""
        for page in doc:
            text += page.get_text() + "\n"
        return text
    except Exception:
        return ""

def extract_pdf_data(pdf_bytes):
    text = extract_pdf_text_from_bytes(pdf_bytes)
    if text:
        return parse_pdf_text(text)
    return {}

def scrape_page(id_estado):
    """
    Procesa la página correspondiente a un id_estado y retorna una lista de registros extraídos.
    Se asigna el campo "estado" usando el mapeo del diccionario.
    """
    # Actualizamos el base_url al nuevo dominio que funciona:
    base_url = "https://appalertaamber1.fgr.org.mx"
    headers = {"User-Agent": "Mozilla/5.0"}
    results = []
    page_url = f"{base_url}/Alerta/CarruselGB?id_estado={id_estado}"
    try:
        response = requests.get(page_url, headers=headers, timeout=10)
        if response.status_code != 200:
            return results
        soup = BeautifulSoup(response.text, "html.parser")
        pdf_links = soup.find_all("a", href=re.compile(r'/Alerta/CreaAlertaPDFPublico\?numero_reporte=\d+'))
        for a_tag in pdf_links:
            pdf_href = a_tag.get("href")
            pdf_link = base_url + pdf_href
            img_tag = a_tag.find("img", class_="carousel-image")
            if img_tag:
                img_src = img_tag.get("src")
                foto_url = base_url + img_src
            else:
                foto_url = None
            try:
                pdf_response = requests.get(pdf_link, headers=headers, timeout=10)
                if pdf_response.status_code != 200:
                    continue
                pdf_data = extract_pdf_data(pdf_response.content)
                pdf_data['imagen_url'] = foto_url
                pdf_data['pdf_link'] = pdf_link
                # Asignar "estado" usando el mapeo del diccionario
                pdf_data['estado'] = estado_mapping.get(id_estado, None)
                results.append(pdf_data)
            except Exception:
                pass
            time.sleep(0.5)
    except Exception:
        pass
    return results

def scrape_macro_parallel():
    """Procesa las páginas en paralelo. Cada thread obtiene su propia lista de resultados y, al finalizar,
    se unen todos en una única lista."""
    # Iteramos sobre los estados: 0 y del 2 al 33
    estados_ids = [0] + list(range(2, 34))
    all_data = []
    max_workers = max(24, multiprocessing.cpu_count() * 2)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(scrape_page, id_estado): id_estado for id_estado in estados_ids}
        for future in as_completed(futures):
            data = future.result()
            if data:
                all_data.extend(data)
    return all_data

def main():
    start_time = time.time()
    data_list = scrape_macro_parallel()
    extraction_date = datetime.date.today()
    source_url = "https://appalertaamber1.fgr.org.mx/Alerta/CarruselGB"
    insert_many_to_db(data_list, extraction_date, source_url)
    end_time = time.time()
    print(f"⏳ Tiempo total de ejecución: {end_time - start_time:.2f} segundos")

if __name__ == '__main__':
    main()
