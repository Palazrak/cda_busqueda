import requests
from bs4 import BeautifulSoup
import re
import fitz  # PyMuPDF
import time
import psycopg2
import json
import datetime

# Configuración de la base de datos
DB_NAME = "cda_busqueda"
DB_USER = "postgres"
DB_PASSWORD = "mysecretpassword"
DB_HOST = "postgres"
DB_PORT = "5432"

BASE_URL = "https://www.fiscaliatabasco.gob.mx"
PAGE_URL = f"{BASE_URL}/AtencionVictimas/AlertaAmber"
HEADERS = {"User-Agent": "Mozilla/5.0"}

def get_pdf_links():
    response = requests.get(PAGE_URL, headers=HEADERS)
    if response.status_code != 200:
        print("❌ Error al obtener la página.")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    figures = soup.find_all("figure")

    pdf_links = []
    for figure in figures:
        a_tag = figure.find("a", href=True)
        img_tag = figure.find("img")
        figcaption = figure.find("figcaption")

        if a_tag and img_tag and figcaption:
            nombre_edad = figcaption.text.strip()
            match = re.search(r"(\d+) años", nombre_edad)
            edad = match.group(1) if match else None
            nombre = nombre_edad.replace(f"{edad} años", "").strip() if edad else nombre_edad
            pdf_url = a_tag["href"]
            if not pdf_url.startswith("http"):
                pdf_url = BASE_URL + pdf_url
            img_url = img_tag["src"]
            if not img_url.startswith("http"):
                img_url = BASE_URL + img_url

            pdf_links.append({
                "nombre": nombre,
                "edad": edad,
                "pdf_url": pdf_url,
                "imagen_url": img_url
            })
    return pdf_links

def extract_text_and_image(pdf_url):
    try:
        response = requests.get(pdf_url, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            print(f"❌ Error al descargar el PDF: {pdf_url}")
            return "", None

        doc = fitz.open(stream=response.content, filetype="pdf")
        text = "\n".join([page.get_text("text") for page in doc])
        return text, None
    except Exception as e:
        print(f"❌ Error al procesar PDF: {e}")
        return "", None

def insert_into_db(data, url_origen):
    extraction_date = datetime.date.today()
    conn, cur = None, None

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
            INSERT INTO desaparecidos (fecha_extraccion, url_origen, datos)
            VALUES (%s, %s, %s)
        """
        cur.execute(insert_query, (extraction_date, url_origen, json.dumps(data)))
        conn.commit()
        print("✅ Datos insertados correctamente.")
    except Exception as e:
        print(f"❌ Error al insertar en la base de datos: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()

def limpiar_texto(texto):
    texto = texto.replace('\n', ' ')
    texto = re.sub(r'\s+', ' ', texto)
    return texto.strip()

def truncar_resumen(resumen):
    patrones_fin = [
        r'comunicarse al (teléfono|siguiente número)',
        r'proporcionarla a los siguientes números',
        r'favor de llamar al',
        r'Cualquier información proporcionarla',
        r'FGE TABASCO',
        r'Correo[s]* Electr[oó]nico',
        r'Mail:',
    ]
    for patron in patrones_fin:
        if re.search(patron, resumen, flags=re.IGNORECASE):
            resumen = re.split(patron, resumen, flags=re.IGNORECASE)[0]
            break
    return resumen.strip()

def estandarizar_estatura(estatura):
    estatura = estatura.lower()
    match = re.search(r'(\d+\.?\d*)\s*(m|mts?|metros?)', estatura)
    if match:
        return f"{match.group(1)} m"
    return estatura

def estandarizar_peso(peso):
    peso = peso.replace('aprox.', '').replace('kg.', 'kg').strip()
    return peso

def extraer_acompanante(texto):
    acomp = {}
    seccion = re.search(r'ACOMPAÑANTE:\s*(.+?)RESUMEN DE LOS HECHOS:', texto, re.DOTALL | re.IGNORECASE)
    if not seccion:
        return None
    contenido = limpiar_texto(seccion.group(1))

    acomp['nombre'] = re.search(r'Nombre:\s*(.+?)(Complexión|Sexo|Peso|Edad|Cabello|$)', contenido)
    acomp['nombre'] = acomp['nombre'].group(1).strip() if acomp['nombre'] else None

    acomp['edad'] = re.search(r'Edad:\s*(\d+)', contenido)
    acomp['edad'] = acomp['edad'].group(1) if acomp['edad'] else None

    acomp['sexo'] = re.search(r'Sexo:\s*(Masculino|Femenino)', contenido)
    acomp['sexo'] = acomp['sexo'].group(1) if acomp['sexo'] else None

    acomp['estatura'] = re.search(r'Estatura:\s*(\d+\.?\d*\s*(m|mts?))', contenido)
    acomp['estatura'] = estandarizar_estatura(acomp['estatura'].group(1)) if acomp['estatura'] else None

    acomp['peso'] = re.search(r'Peso:\s*(\d+)\s*kg', contenido, re.IGNORECASE)
    acomp['peso'] = f"{acomp['peso'].group(1)} kg" if acomp['peso'] else None

    acomp['cabello'] = re.search(r'Cabello:\s*(.+?)(\.|$)', contenido)
    acomp['cabello'] = acomp['cabello'].group(1).strip() if acomp['cabello'] else None

    return acomp

def parsear_pdf(texto_extraido):
    datos = {}
    texto = limpiar_texto(texto_extraido)

    # Extraer datos del acompañante si existen
    acompanante = extraer_acompanante(texto)
    if acompanante:
        datos['acompanante'] = acompanante
        # Eliminar esta sección del texto para evitar confusión con datos principales
        texto = re.sub(r'ACOMPAÑANTE:.*?RESUMEN DE LOS HECHOS:', 'RESUMEN DE LOS HECHOS:', texto, flags=re.DOTALL | re.IGNORECASE)

    # Extracción robusta de datos principales
    campos = {
        'fecha_nacimiento': r'Fecha de Nacimiento:\s*(.+?)Fecha',
        'fecha_hechos': r'Fecha de los hechos:\s*(.+?)Edad',
        'edad': r'Edad:\s*(\d+)',
        'nacionalidad': r'Nacionalidad:\s*(.+?)Originaria',
        'originaria': r'Originaria:\s*(.+?)Género',
        'genero': r'Género:\s*(.+?)Estatura',
        'estatura': r'Estatura:\s*(.+?)Peso',
        'peso': r'Peso:\s*(.+?)Cabello',
        'cabello': r'Cabello:\s*(.+?)Ojos',
        'ojos': r'Ojos:\s*(.+?)Señas particulares',
        'senas_particulares': r'Señas particulares:\s*(.+?)Lugar de los hechos',
        'lugar_hechos': r'Lugar de los hechos:\s*(.+?)RESUMEN',
        'reporte_num': r'Reporte núm\.\s*:\s*([A-Z0-9/]+)',
        'resumen_hechos': r'RESUMEN DE LOS HECHOS:\s*(.+?)((Dirección de Correos Electr[oó]nico)|(Correo[s]* Electr[oó]nico)|ALERTA AMBER|Reporte núm|Mail:)',
    }

    for campo, patron in campos.items():
        match = re.search(patron, texto, flags=re.IGNORECASE | re.DOTALL)
        if match:
            valor = limpiar_texto(match.group(1))
            if campo == 'estatura':
                valor = estandarizar_estatura(valor)
            elif campo == 'peso':
                valor = estandarizar_peso(valor)
            elif campo == 'resumen_hechos':
                valor = truncar_resumen(valor)
            datos[campo] = valor
        else:
            datos[campo] = None

    return datos



def process_pdfs(pdf_links):
    for entry in pdf_links:
        pdf_text, _ = extract_text_and_image(entry["pdf_url"])
        if pdf_text:
            info_adicional = parsear_pdf(pdf_text)
            data = {
                "nombre": entry["nombre"],
                "edad": entry["edad"],
                "pdf_url": entry["pdf_url"],
                "imagen_url": entry["imagen_url"],
                **info_adicional
            }
            # Mostrar en consola lo que se va a insertar
            print("\n📄 Datos a insertar:")
            print(json.dumps(data, indent=2, ensure_ascii=False))
            
            # Luego insertarlo en la base
            insert_into_db(data, entry["pdf_url"])
        time.sleep(0.5)


def main():
    pdf_links = get_pdf_links()
    if not pdf_links:
        print("❌ No se encontraron PDFs.")
        return

    process_pdfs(pdf_links)
    print(f"✅ Se procesaron {len(pdf_links)} registros.")

if __name__ == "__main__":
    main()
