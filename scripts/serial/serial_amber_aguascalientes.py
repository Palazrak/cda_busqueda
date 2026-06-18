import requests
from bs4 import BeautifulSoup
import argparse
import re
import fitz  # PyMuPDF
import time
import json
from pathlib import Path
import sys
from dotenv import load_dotenv

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from utils.db_utils import insert_payload, make_hashid as make_shared_hashid
from utils.s3_utils import upload_bytes_if_enabled

# Cargar variables de entorno
load_dotenv()

S3_PREFIX = "pdf"

TEST_LIMIT = None

BASE_URL = "https://www.fiscalia-aguascalientes.gob.mx"
PAGE_URL = BASE_URL + "/"
HEADERS = {"User-Agent": "Mozilla/5.0"}

# ------------------ Helpers: hash ------------------
def normalize_for_hash(value):
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip().lower()


def make_hashid(parsed_data):
    """Genera el hash a partir de: fecha_desaparicion, localizado, nombre, edad, resumen_hechos, senas_particulares.
    Devuelve el hashid con prefijo 0202_ (sin extensión) y el nombre de archivo 0202_<hash>.pdf"""
    hashid = make_shared_hashid("0202_", parsed_data)
    filename = f"{hashid}.pdf"
    s3_key = f"{S3_PREFIX}/{filename}"
    return hashid, filename, s3_key

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

        # Buscar el contenedor padre que tiene class="row"
        container = seccion.find_parent("div", class_="row")
        if not container:
            # Si no encuentra con class="row", buscar cualquier div padre
            container = seccion.find_parent("div")
        
        if not container:
            print(f"⚠️ No se encontró el contenedor de {seccion_nombre}")
            continue

        # Buscar todos los enlaces dentro del contenedor, no solo los directos
        anchors = container.find_all("a", href=True)
        print(f"📋 Sección '{seccion_nombre}': {len(anchors)} enlaces encontrados en el contenedor")
        
        valid_count = 0
        for a in anchors:
            href = a["href"]
            # Incluir enlaces que generan boletines/alertas: genera_boletin, genera_alerta_amber, genera_alerta_alba
            # o que son PDFs directos
            if not (href.lower().endswith(".pdf") or 
                    "genera_boletin" in href.lower() or 
                    "genera_alerta" in href.lower()):
                continue
            
            valid_count += 1

            # Buscar el nombre en el HTML - está en un <p> con style="color:black;"
            nombre = None
            nombre_tag = a.find("p", style=re.compile(r"color:\s*black", re.IGNORECASE))
            if nombre_tag:
                nombre = nombre_tag.get_text(strip=True)
            
            # Si no se encontró, buscar cualquier <p> con style que no sea el de estado
            if not nombre:
                all_p_tags = a.find_all("p")
                for p_tag in all_p_tags:
                    style_attr = p_tag.get("style", "")
                    if "color:black" in style_attr.lower():
                        nombre = p_tag.get_text(strip=True)
                        break
            
            # Buscar tags de estado
            estado_tag = a.find("p", class_="no-localizada")
            # Also check for p tags with class "localizada" or that might contain status text
            estado_tags = a.find_all("p")
            img_tag = a.find("img")

            estado_alerta = seccion_nombre
            pdf_url = BASE_URL + "/" + href.lstrip("/")
            imagen_url = img_tag["src"] if img_tag else None
            if imagen_url and not imagen_url.startswith("http"):
                imagen_url = BASE_URL + "/" + imagen_url.lstrip("/")

            # Detectar si está localizada desde el HTML
            localizado = False  # Default
            for tag in estado_tags:
                # Verificar por clase CSS
                tag_classes = tag.get("class", [])
                if "localizada" in tag_classes:
                    localizado = True
                    break
                
                # Verificar por texto
                text = tag.get_text(strip=True)
                if "Localizada" in text:
                    localizado = True
                    break
                elif "Desactivada" in text:
                    localizado = True  # Desactivada significa que fue localizada
                    break
                elif "No Localizado" in text or "Activa" in text:
                    localizado = False
                    break

            resultados.append({
                "nombre": nombre,
                "estado_alerta": estado_alerta,
                "pdf_url": pdf_url,
                "imagen_url": imagen_url,
                "localizado": localizado
            })
        
        print(f"✅ Sección '{seccion_nombre}': {valid_count} enlaces válidos procesados")
    
    print(f"📊 Total de registros encontrados: {len(resultados)}")
    return resultados


def s3_object_exists(s3_client, bucket, key):
    return False


def upload_pdf_to_s3_if_not_exists(pdf_bytes, key):
    s3_url = upload_bytes_if_enabled(pdf_bytes, key, content_type="application/pdf")
    if s3_url:
        print(f"✅ PDF uploaded to {s3_url}")
        return True
    print("☑️ S3 deshabilitado; se conserva pdf_url de origen")
    return False


def extract_text_from_pdf_url(pdf_url):
    try:
        response = requests.get(pdf_url, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            print(f"❌ Error al descargar el PDF: {pdf_url}")
            return "", None
        pdf_bytes = response.content
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        text = "\n".join([page.get_text("text") for page in doc])
        doc.close()
        return text, pdf_bytes
    except Exception as e:
        print(f"❌ Error al procesar PDF: {e}")
        return "", None

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




def insert_into_db(data, url_origen, hashid):
    localizado = data.get("localizado", False)
    try:
        inserted = insert_payload(
            "0202_",
            data,
            url_origen,
            localizado=localizado,
            hashid=hashid,
        )
        print(f"✅ Insertados en DB: {inserted} hashid={hashid}")
        return bool(inserted)
    except Exception as e:
        print(f"❌ Error al insertar en la base de datos: {e}")
        return False

def process_pdfs(pdf_links, max_records=None):
    count = 0
    for entry in pdf_links:
        pdf_text, pdf_bytes = extract_text_from_pdf_url(entry["pdf_url"])
        if not pdf_text or not pdf_bytes:
            continue

        parsed_data = parse_pdf_data_general(pdf_text)

        # Priorizar el nombre del HTML sobre el del PDF - SIEMPRE usar el del HTML si existe
        nombre_html = entry.get("nombre")
        nombre_pdf = parsed_data.get("nombre")
        
        # Limpiar el nombre del HTML (remover espacios en blanco)
        if nombre_html:
            nombre_html = nombre_html.strip()
        
        # SIEMPRE priorizar el nombre del HTML si existe (incluso si está vacío después de strip)
        # Solo usar el del PDF si el HTML no tiene nombre
        if nombre_html:
            nombre_final = nombre_html
            print(f"📝 Usando nombre del HTML: '{nombre_final}'")
        else:
            nombre_final = nombre_pdf
            if nombre_pdf:
                print(f"⚠️  Nombre del HTML no encontrado, usando del PDF: '{nombre_pdf}'")
        
        # Priorizar el estado de localizado del HTML sobre cualquier otro
        localizado = entry.get("localizado", False)

        # Construir el diccionario de datos
        # Primero copiar datos del PDF, pero excluir "nombre" para evitar conflictos
        data = {k: v for k, v in parsed_data.items() if k != "nombre"}
        
        # Ahora agregar todos los campos, con el nombre del HTML teniendo prioridad absoluta
        data.update({
            "nombre": nombre_final,  # Nombre del HTML (si existe) o PDF
            "estado_alerta": entry.get("estado_alerta"),
            "pdf_url": entry["pdf_url"],
            "imagen_url": entry.get("imagen_url"),
            "localizado": localizado,
        })

        # Generar hashid y nombre de archivo
        hashid, filename, s3_key = make_hashid(data)

        # Subir a S3 si no existe
        try:
            uploaded = upload_pdf_to_s3_if_not_exists(pdf_bytes, s3_key)
        except Exception as e:
            print(f"❌ Error verificando/guardando en S3: {e}")
            uploaded = False

        print(f"\n🔎 Datos extraídos:\n{json.dumps(data, indent=2, ensure_ascii=False)}")
        print(f"🔑 HashID: {hashid}")
        insert_into_db(data, entry["pdf_url"], hashid)
        time.sleep(0.5)
        count += 1
        limit = max_records or TEST_LIMIT
        if limit and count >= limit:
            break


def main():
    parser = argparse.ArgumentParser(description="Scraper Amber/boletines Aguascalientes")
    parser.add_argument("--max-records", type=int, default=None, help="Limita registros procesados en esta corrida")
    args = parser.parse_args()

    pdf_links = get_all_pdf_links()
    if not pdf_links:
        print("❌ No se encontraron PDFs.")
        return
    process_pdfs(pdf_links, max_records=args.max_records)
    print(f"✅ Se procesaron {len(pdf_links)} registros.")

if __name__ == "__main__":
    main()
