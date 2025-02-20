import requests
from bs4 import BeautifulSoup
import json
import time
import datetime
import psycopg2

# Configuración de la base de datos
DB_NAME = "cda_busqueda"
DB_USER = "postgres"
DB_PASSWORD = "mysecretpassword"
DB_HOST = "postgres"
DB_PORT = "5432"

# Función para insertar múltiples registros en la base de datos
def insert_many_to_db(data_list: list, extraction_date: datetime.date):
    if not data_list:
        print("⚠️ No hay datos para insertar en la base de datos.")
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
            (extraction_date, url, json.dumps(data)) for url, data in data_list
        ])
        conn.commit()
        print(f"✅ {len(data_list)} registros insertados correctamente.")
        
    except Exception as e:
        print(f"❌ Error al conectar a la base de datos: {e}")
    
    finally:
        cur.close()
        conn.close()

    end_time = time.time()
    print(f"⏳ Tiempo de escritura en BD: {end_time - start_time:.2f} segundos")

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
    extraction_date = datetime.date.today()
    
    # 🔄 Ejecutando scraping en **modo serial**
    scraped_data = scrape_all_fichas()
    
    # 🔄 Insertar datos en BD
    insert_many_to_db(scraped_data, extraction_date)
    
    global_end_time = time.time()
    print(f"⏳ Tiempo total de ejecución: {global_end_time - global_start_time:.2f} segundos")

if __name__ == '__main__':
    main()
