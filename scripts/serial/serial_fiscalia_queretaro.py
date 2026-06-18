'''
- [x] Fiscalia General del Estado: https://www.fiscaliageneralqro.gob.mx/NoLocalizados/NoLocalizados_Mme.html
    - Se separa en mujeres y hombres mayores y menores
    - Todos los elementos estan en HTML extraible
    - Prefijo hashid: 2301_
'''
import requests
from bs4 import BeautifulSoup
import re
import time
import json
import os
from pathlib import Path
import sys

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from utils.db_utils import insert_payload, make_hashid as make_shared_hashid
from utils.s3_utils import upload_url_if_enabled

S3_FOLDER = "html/"
TEST_LIMIT = None

BASE_URL = "https://www.fiscaliageneralqro.gob.mx"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "es-MX,es;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
}

# (url, genero, grupo_edad)
PAGES = [
    ("/NoLocalizados/NoLocalizados_Mme.html", "Femenino", "Menor"),
    ("/NoLocalizados/NoLocalizados_Mmy.html", "Femenino", "Mayor"),
    ("/NoLocalizados/NoLocalizados_Hme.html", "Masculino", "Menor"),
    ("/NoLocalizados/NoLocalizados_Hmy.html", "Masculino", "Mayor"),
]


# ------------------ Helpers: hash ------------------
def normalize_for_hash(value):
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip().lower()


def make_hashid(parsed_data):
    hashid = make_shared_hashid("2301_", parsed_data)
    filename = f"{hashid}.pdf"
    return hashid, filename


# -------------------------------------------------------------------
# 1) Parsear registros desde HTML de una página
# -------------------------------------------------------------------
def parse_records_from_html(html, page_url, genero, grupo_edad):
    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Cada registro tiene una foto con src que empieza en "Fotos/"
    foto_imgs = soup.find_all("img", src=re.compile(r"^Fotos/", re.IGNORECASE))

    for img in foto_imgs:
        # Subir hasta el <tr> contenedor del registro
        record_row = img.find_parent("tr")
        if not record_row:
            continue

        # La foto y los datos están en el mismo <tr>
        imagen_src = img["src"]
        imagen_url = BASE_URL + "/NoLocalizados/" + imagen_src

        # El <td colspan="2"> contiene los datos del registro
        data_td = record_row.find("td", attrs={"colspan": "2"})
        if not data_td:
            continue

        data = _parse_record_td(data_td)
        data["imagen_url"] = imagen_url
        data["genero"] = genero
        data["grupo_edad"] = grupo_edad
        data["localizado"] = False
        data["url_origen"] = page_url
        records.append(data)

    return records


def _parse_record_td(td):
    """Extrae campos del <td colspan='2'> de cada registro.

    El sitio tiene tres variantes de HTML; se usa regex sobre texto plano
    para los campos simples (robusto ante cualquier variante de etiquetas),
    y búsqueda en árbol solo donde hay estructura consistente.
    """
    # Texto plano con saltos de línea como separador
    text = td.get_text(separator="\n")
    data = {}

    # --- Campos extraídos por regex sobre texto plano ---
    def first_line_after(label_re, src=text):
        m = re.search(label_re + r"[:\s]*([^\n]+)", src, re.IGNORECASE)
        return m.group(1).strip() if m else None

    nombre = first_line_after(r"NOMBRE")
    if nombre:
        # Limpiar espacios extras y posibles artefactos de etiquetas vacías
        nombre = re.sub(r"\s+", " ", nombre).strip()
        data["nombre"] = nombre if nombre else None

    fecha = first_line_after(r"FECHA DESAPAR[A-ZIÓC]*")
    if fecha:
        data["fecha_desaparicion"] = fecha.strip()

    expediente = first_line_after(r"EXPEDIENTE")
    if expediente:
        data["folio"] = expediente.rstrip(".").strip()

    edad_match = re.search(r"EDAD[:\s]*([0-9]+)\s*años?", text, re.IGNORECASE)
    if edad_match:
        data["edad"] = edad_match.group(1)

    estatura_match = re.search(r"ESTATURA[:\s]*([0-9.,]+)\s*mts", text, re.IGNORECASE)
    if estatura_match:
        data["estatura"] = estatura_match.group(1)

    # --- SEÑAS PARTICULARES y ROPAS: buscar el span Verd12Ngro dentro del elemento ---
    # que contiene la etiqueta (presente en todas las variantes HTML del sitio).
    for element in td.find_all(True):
        if re.search(r"SEÑAS PARTICULARES", element.get_text(separator=" "), re.IGNORECASE):
            span = element.find("span", class_="Verd12Ngro")
            if span:
                data["senas"] = re.sub(r"\s+", " ", span.get_text()).strip()
                break

    for element in td.find_all(True):
        if re.search(r"ROPAS QUE VEST", element.get_text(separator=" "), re.IGNORECASE):
            span = element.find("span", class_="Verd12Ngro")
            if span:
                data["vestimenta"] = re.sub(r"\s+", " ", span.get_text()).strip()
                break

    data["descripcion_hechos"] = None
    return data


# -------------------------------------------------------------------
# 2) Obtener HTML de una página (o leer archivo local para tests)
# -------------------------------------------------------------------
def fetch_html(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=25)
        response.raise_for_status()
        response.encoding = "utf-8"
        return response.text
    except Exception as e:
        print(f"❌ Error al obtener {url}: {e}")
        return None


# -------------------------------------------------------------------
# 3) Insertar en la base de datos
# -------------------------------------------------------------------
def insert_into_db(data, hashid):
    localizado = data.get("localizado")
    url_origen = data.get("url_origen")
    try:
        inserted = insert_payload(
            "2301_",
            data,
            url_origen,
            localizado=localizado,
            hashid=hashid,
        )
        print(f"✅ Insertados en DB: {inserted} hashid={hashid}")
        return bool(inserted)
    except Exception as e:
        print(f"❌ Error DB: {e}")
        return False


def upload_image_to_s3(url, hashid):
    if not url or url.startswith("data:"):
        return None
    try:
        if not url.startswith("http"):
            url = BASE_URL + "/" + url.lstrip("/")
        ext = os.path.splitext(url.split("?")[0])[-1]
        if not ext or len(ext) > 5:
            ext = ".jpg"
        filename = f"{hashid}{ext}"
        s3_key = f"{S3_FOLDER}{filename}"
        s3_url = upload_url_if_enabled(url, s3_key, headers=HEADERS, timeout=20)
        if s3_url:
            print(f"✅ Imagen subida: {s3_url}")
            return s3_url
        print("☑️ S3 deshabilitado; se conserva imagen_url de origen")
        return None
    except Exception as e:
        print(f"❌ Error S3: {e}")
        return None


# -------------------------------------------------------------------
# 4) Flujo principal
# -------------------------------------------------------------------
def process_all():
    total_insertados = 0
    total_count = 0
    for path, genero, grupo_edad in PAGES:
        url = BASE_URL + path
        print(f"\n{'='*70}")
        print(f"Procesando: {url} ({genero} {grupo_edad})")
        print(f"{'='*70}")

        html = fetch_html(url)
        if not html:
            continue

        records = parse_records_from_html(html, url, genero, grupo_edad)
        print(f"  📋 Registros encontrados: {len(records)}")

        for record in records:
            hashid, _ = make_hashid(record)
            data = {
                "estado_alerta": "Desaparecidos Querétaro",
                "hashid": hashid,
                **record,
            }
            upload_image_to_s3(data.get("imagen_url"), hashid)
            inserted = insert_into_db(data, hashid)
            if inserted:
                total_insertados += 1
            time.sleep(0.3)
            total_count += 1
            if TEST_LIMIT and total_count >= TEST_LIMIT:
                break
        if TEST_LIMIT and total_count >= TEST_LIMIT:
            break

    print(f"\n{'='*70}")
    print(f"✅ PROCESAMIENTO COMPLETO — Insertados: {total_insertados}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    process_all()
