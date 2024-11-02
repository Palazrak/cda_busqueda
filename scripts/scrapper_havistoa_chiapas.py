import requests
from bs4 import BeautifulSoup
import json
import time
import datetime
import random
import psycopg2
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Extraer las credenciales de la base de datos desde variables de entorno
DB_NAME = "cda_busqueda"
DB_USER = "postgres"
DB_PASSWORD = "mysecretpassword"
DB_HOST = "postgres"
DB_PORT = "5432"

# Función para insertar múltiples registros en la base de datos
def insert_many_to_db(data_list: list,
                      extraction_date:datetime, 
                      source_url:str)->None:
    """
    Método void para insertar múltiples registros en la base de datos. Utiliza las variables de entorno para hacer la conexión
    y recibe una lista de diccionarios con los datos a insertar, la fecha de extracción y la URL de origen.
    """
    # Conexión a la base de datos
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()
        
        # Crear tabla si no existe
        cur.execute('''
            CREATE TABLE IF NOT EXISTS desaparecidos (
                id SERIAL PRIMARY KEY,
                fecha_extraccion DATE NOT NULL,
                url_origen TEXT NOT NULL,
                fecha_modificacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                localizado BOOLEAN DEFAULT FALSE,
                datos JSONB
            );
        ''')

        # Asegurarse de que extraction_date esté en formato de cadena adecuado para PostgreSQL
        extraction_date_str = extraction_date.strftime('%Y-%m-%d')
        
        # Preparar consulta de inserción con los nuevos campos
        insert_query = """
            INSERT INTO desaparecidos (fecha_extraccion, url_origen, datos)
            VALUES (%s, %s, %s)
        """
        # Ejecutar inserción masiva
        cur.executemany(insert_query, [
            (extraction_date, source_url, json.dumps(data)) for data in data_list
        ])
        conn.commit()
        
        # Imprime cuantos registros se insertaron
        print(f"{len(data_list)} registros insertados exitosamente.")
        
    except Exception as e: # Capturar cualquier error
        print(f"Error al conectar a la base de datos: {e}")
    
    finally: # Cerrar la conexión a la base de datos
        cur.close()
        conn.close()

def extract_data(data_section: BeautifulSoup)->dict:
    """
    # Función para extraer la información de una ficha específica a partir de una sección de HTML obtenida con BeautifulSoup.
    Extrae todos los campos de la ficha y los devuelve en un diccionario.
    """
    
    # Extraer los datos
    nombre = data_section.find('h3').get_text(strip=True)
    imagen_url = data_section.find('img')['src']
    
    registro_p = data_section.find('p', class_='proile-rating')
    registro = registro_p.find('span').get_text(strip=True) if registro_p and 'Registro' in registro_p.get_text() else None
    
    sexo = data_section.find('label', string='Sexo:').find_next('p').get_text(strip=True)
    estatura = data_section.find('label', string='Estatura:').find_next('p').get_text(strip=True)
    tez = data_section.find('label', string='Tez:').find_next('p').get_text(strip=True)
    ojos = data_section.find('label', string='Ojos:').find_next('p').get_text(strip=True)
    cabello = data_section.find('label', string='Cabello:').find_next('p').get_text(strip=True)
    peso = data_section.find('label', string='Peso:').find_next('p').get_text(strip=True)
    fecha_desaparicion = data_section.find('label', string='Fecha desaparición:').find_next('p').get_text(strip=True)
    complexion = data_section.find('label', string='Complexion:').find_next('p').get_text(strip=True)
    boca = data_section.find('label', string='Boca:').find_next('p').get_text(strip=True)
    tamano_nariz = data_section.find('label', string='Tamaño de nariz').find_next('p').get_text(strip=True)
    tipo_nariz = data_section.find('label', string='Tipo de nariz:').find_next('p').get_text(strip=True)
    escolaridad = data_section.find('label', string='Escolaridad:').find_next('p').get_text(strip=True)
    originario_de = data_section.find('label', string='Originario de:').find_next('p').get_text(strip=True)
    fecha_nacimiento = data_section.find('b', string='Fecha de nacimiento:').find_next('p').get_text(strip=True)
    senas_particulares = data_section.find('strong', string='Señas Particulares:').find_next('p').get_text(strip=True)
    circunstancia = data_section.find('strong', string='Circunstancia:').find_next('p').get_text(strip=True)

    # Crear un diccionario con los datos
    desaparecido_data = {
        "nombre": nombre,
        "imagen_url": imagen_url,
        "registro": registro,
        "sexo": sexo,
        "estatura": estatura,
        "tez": tez,
        "ojos": ojos,
        "cabello": cabello,
        "peso": peso,
        "fecha_desaparicion": fecha_desaparicion,
        "complexion": complexion,
        "boca": boca,
        "tamano_nariz": tamano_nariz,
        "tipo_nariz": tipo_nariz,
        "escolaridad": escolaridad,
        "originario_de": originario_de,
        "fecha_nacimiento": fecha_nacimiento,
        "senas_particulares": senas_particulares,
        "circunstancia": circunstancia
    }
    
    return desaparecido_data

def scrape_all_pages(source_url: str, 
                     extraction_date: datetime)->None:
    """
    Método para scrapeo de todas las páginas de la fuente de datos. 
    Recibe la URL de la primera página y la fecha de extracción, e iterativamente manda a llamar a la función extract_data para cada ficha.
    Regresa la información de todas las fichas en una lista de diccionarios.
    """
    next_page = source_url
    all_data = []
    num = 0
    
    while next_page:
        print(f"Scrapeando página {num}")
        response = requests.get(next_page)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Encontrar todas las fichas en la página
        fichas_links = [f"https://www.fge.chiapas.gob.mx{link['href']}" for link in soup.select('a[role="button"].btn.btn-secondary.btn-sm.mr-1.bg-theme1.border-theme1') if link.get_text(strip=True) == "Ver Ficha"] # Obtener directamente los href de cada ficha en una sola línea
        
        for link in fichas_links:
            ficha_response = requests.get(link)
            ficha_soup = BeautifulSoup(ficha_response.text, 'html.parser')
            
            unavailable_message = ficha_soup.select_one('h1.display-4')
            if unavailable_message and "Lo sentimos esta ficha ya no esta disponible" in unavailable_message.get_text(strip=True):
                print(f"Ficha en {link} no está disponible. Saltando...")
                continue

            data_section = ficha_soup.select_one('div.emp-profile-wrap')
            if data_section:
                data = extract_data(data_section)
                all_data.append(data)
        
        next_page = None
        
        for link in soup.find_all('a', class_='page-link'):
            if "Página Siguiente" in link.get_text(strip=True):
                next_page = f"https://www.fge.chiapas.gob.mx{link['href']}"
                break
        
        # Pausa aleatoria para evitar sobrecargar el servidor
        time.sleep(random.uniform(0.5, 2))
        num += 1
    
    return all_data
    
        
def main():
    # Iniciar scraping desde la primera página
    extraction_date = datetime.date.today()
    source_url = "https://www.fge.chiapas.gob.mx/Servicios/Hasvistoa"
    informacion = scrape_all_pages(source_url, extraction_date)
    insert_many_to_db(informacion, extraction_date, source_url)
    
if __name__ == '__main__':
    main()
    
"""
Son 95 paginas en total con 1127 personas en total. Falta:

-) Considerar cómo limpiar la base de datos a partir de la imágen de la persona. (Aparece un texto de LOCALIZADA)
-) Optimizar el código. Buscar qué comparaciones se pueden evitar para hacerlo más eficiente.

En Alerta Amber Chiapas son 30 paginas. Falta hacer el scrappeo de Amber.

"""