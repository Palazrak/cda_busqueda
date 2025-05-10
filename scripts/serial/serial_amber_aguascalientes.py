import requests
from bs4 import BeautifulSoup
import re
import fitz  # PyMuPDF
import time
import psycopg2
import json
import datetime

DB_NAME = "cda_busqueda"
DB_USER = "postgres"
DB_PASSWORD = "mysecretpassword"
DB_HOST = "postgres"
DB_PORT = "5432"

BASE_URL = "https://www.fiscalia-aguascalientes.gob.mx"
PAGE_URL = BASE_URL + "/"
HEADERS = {"User-Agent": "Mozilla/5.0"}

def get_all_pdf_links():
    try:
        response = requests.get(PAGE_URL, headers=HEADERS)
        response.raise_for_status()
    except Exception as e:
        print(f"❌ Error al cargar página principal: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    secciones = ["Personas no Localizadas", "Alerta Amber", "Alerta Alba"]
    resultados = []

    for seccion_nombre in secciones:
        seccion = soup.find("h3", string=re.compile(seccion_nombre, re.IGNORECASE))
        if not seccion:
            print(f"⚠️ No se encontró la sección {seccion_nombre}")
            continue

        container = seccion.find_parent("div", class_="row")
        if not container:
            print(f"⚠️ No se encontró el contenedor de {seccion_nombre}")
            continue

        anchors = container.find_all("a", href=True)
        for a in anchors:
            href = a["href"]
            if not href.lower().endswith(".pdf") and "genera_boletin" not in href:
                continue

            nombre_tag = a.find("p", style=True)
            estado_tag = a.find("p", class_="no-localizada")
            img_tag = a.find("img")

            nombre = nombre_tag.text.strip() if nombre_tag else None
            estado_alerta = seccion_nombre
            pdf_url = BASE_URL + "/" + href.lstrip("/")
            imagen_url = img_tag["src"] if img_tag else None
            if imagen_url and not imagen_url.startswith("http"):
                imagen_url = BASE_URL + "/" + imagen_url.lstrip("/")

            resultados.append({
                "nombre": nombre,
                "estado_alerta": estado_alerta,
                "pdf_url": pdf_url,
                "imagen_url": imagen_url
            })
    return resultados


def extract_text_from_pdf_url(pdf_url):
    try:
        response = requests.get(pdf_url, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            print(f"❌ Error al descargar el PDF: {pdf_url}")
            return ""
        doc = fitz.open(stream=response.content, filetype="pdf")
        return "\n".join([page.get_text("text") for page in doc])
    except Exception as e:
        print(f"❌ Error al procesar PDF: {e}")
        return ""

def parse_pdf_data_general(text):
    # Normalize line endings and unify whitespace
    text = re.sub(r"\r\n|\r", "\n", text)
    # Remove leading whitespace/newlines so name pattern matches
    text = text.lstrip()
    # Patterns for extraction
    patrones = {
        # Nombre: two uppercase lines or labeled
        "nombre": r"^\s*(?:Nombre[:\-]?\s*)?([A-ZÑÁÉÍÓÚ]+(?: [A-ZÑÁÉÍÓÚ]+)*)(?:\n)([A-ZÑÁÉÍÓÚ]+(?: [A-ZÑÁÉÍÓÚ]+)*)(?=\n)",
        # Fecha de nacimiento
        "fecha_nacimiento": r"(?:Fecha de Nacimiento|Fecha de nacimiento)[:\-]?\s*(\d{2}/\d{2}/\d{4})",
        # Fecha de desaparición o hechos
        "fecha_desaparicion": r"(?:Fecha de los hechos|Fecha y lugar de ausencia|Fecha de desaparici[oó]n)[:\-]?\s*(\d{2}/\d{2}/\d{4})",
        # Lugar tras fecha y lugar de ausencia o etiqueta general
        "lugar": r"(?:(?:Fecha y lugar de ausencia)[:\-]?\s*\d{2}/\d{2}/\d{4}\s*|(?:Lugar de los hechos|Lugar de desaparici[oó]n)[:\-]?\s*)([^\.\n]*)",
        # Sexo o Género
        "sexo": r"(?:Sexo|G[eé]nero)[:\-]?\s*(Masculino|Femenino|Hombre|Mujer)",
        # Edad
        "edad": r"Edad[:\-]?\s*(\d{1,3})",
        # Estatura en metros
        "estatura": r"Estatura[:\-]?\s*([\d\.,]+)\s?m",
        # Peso en kg
        "peso": r"Peso[:\-]?\s*(\d{1,3})\s?kg",
        # Complexión
        "complexion": r"Complexi[oó]n[:\-]?\s*(.*?)(?=\n)",
        # Tez
        "tez": r"Tez[:\-]?\s*(.*?)(?=\n)",
        # Cara
        "cara": r"Cara[:\-]?\s*(.*?)(?=\n)",
        # Frente
        "frente": r"Frente[:\-]?\s*(.*?)(?=\n)",
        # Cejas
        "cejas": r"Cejas[:\-]?\s*(.*?)(?=\n)",
        # Cabello: tipo y color
        "cabello": r"(?:Tipo y color cabello|Cabello)[:\-]?\s*([\s\S]*?)(?=\n(?:Cejas|Ojos|Se[nñ]as|Vestimenta))",
        # Ojos: tipo y color multilínea
        "ojos": r"(?:Tipo y color ojos|Ojos)[:\-]?\s*([\s\S]*?)(?=\n(?:Nariz|Boca|Ment[oó]n|Se[nñ]as|Vestimenta|$))",
        # Nariz
        "nariz": r"Nariz[:\-]?\s*(.*?)(?=\n)",
        # Boca
        "boca": r"Boca[:\-]?\s*(.*?)(?=\n)",
        # Mentón
        "menton": r"Ment[oó]n[:\-]?\s*(.*?)(?=\n)",
        # Señas particulares
        "senas_particulares": r"Se[nñ]as particulares[:\-]?\s*([\s\S]*?)(?=\n(?:Vestimenta|Observaci[oó]n|RESUMEN|$))",
        # Vestimenta
        "vestimenta": r"Vestimenta[:\-]?\s*(.*?)(?=\n)",
        # Observación
        "observacion": r"Observaci[oó]n[:\-]?\s*(.*?)(?=\n)",
        # Resumen de los hechos
        "resumen_hechos": r"RESUMEN DE LOS HECHOS[:\-]?\s*([\s\S]*)"
    }

    datos = {}
    for campo, patron in patrones.items():
        match = re.search(patron, text, re.IGNORECASE | re.MULTILINE)
        if match:
            # For patterns with a single group, group(1) is the data; for multiple, pick first non-empty
            if campo in ("lugar",):
                raw = match.group(1) or ''
            else:
                raw = next((g for g in match.groups() if g), '')
            # Collapse internal newlines
            value = re.sub(r"\s*\n\s*", " ", raw).strip()
            # Join name parts
            if campo == "nombre":
                parts = [g.strip() for g in match.groups() if g]
                value = " ".join(parts)
            datos[campo] = value
        else:
            datos[campo] = None
    return datos




def insert_into_db(data, url_origen):
    extraction_date = datetime.date.today()
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
            host=DB_HOST, port=DB_PORT
        )
        cur = conn.cursor()
        query = "INSERT INTO desaparecidos (fecha_extraccion, url_origen, datos) VALUES (%s, %s, %s)"
        cur.execute(query, (extraction_date, url_origen, json.dumps(data)))
        conn.commit()
        print("✅ Datos insertados correctamente.")
    except Exception as e:
        print(f"❌ Error al insertar en la base de datos: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()

def process_pdfs(pdf_links):
    for entry in pdf_links:
        pdf_text = extract_text_from_pdf_url(entry["pdf_url"])
        if not pdf_text:
            continue

        parsed_data = parse_pdf_data_general(pdf_text)

        if not entry.get("nombre"):
            entry["nombre"] = parsed_data.get("nombre")

        data = {
            "nombre": entry["nombre"],
            "estado_alerta": entry.get("estado_alerta"),
            "pdf_url": entry["pdf_url"],
            "imagen_url": entry.get("imagen_url"),
            **parsed_data
        }

        print(f"\n🔎 Datos extraídos:\n{json.dumps(data, indent=2, ensure_ascii=False)}")
        insert_into_db(data, entry["pdf_url"])
        time.sleep(0.5)


def main():
    pdf_links = get_all_pdf_links()
    if not pdf_links:
        print("❌ No se encontraron PDFs.")
        return
    process_pdfs(pdf_links)
    print(f"✅ Se procesaron {len(pdf_links)} registros.")

if __name__ == "__main__":
    main()
