import requests
from bs4 import BeautifulSoup
import re
import fitz  # PyMuPDF
import time
import psycopg2
import json
import datetime
import hashlib
# import boto3
# from botocore.exceptions import ClientError


DB_NAME = "cda_busqueda"
DB_USER = "postgres"
DB_PASSWORD = "mysecretpassword"
DB_HOST = "postgres"  # Use "postgres" when running inside Docker, "localhost" for local execution
DB_PORT = "5432"

# s3 = boto3.client("s3")
# S3_BUCKET = "cdas-2025-alertas-amber"
# S3_PREFIX = "pdf"


BASE_URL = "https://hasvistoa.fiscaliamichoacan.gob.mx"
LISTADO_URL = BASE_URL + "/desaparecidos?page={}"
HEADERS = {"User-Agent": "Mozilla/5.0"}

TOTAL_PAGES = 1  # confirmado en la paginación

# ------------------ Helpers: hash y S3 ------------------
def normalize_for_hash(value):
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip().lower()


def make_hashid(parsed_data):
    """Genera el hash a partir de: folio, nombre, edad, descripcion_hechos, senas.
    Devuelve el hashid con prefijo 1702_ (sin extensión) y el nombre de archivo 1702_<hash>.pdf"""
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
    hashid = f"1702_{h}"
    filename = f"{hashid}.pdf"
    # según tu instrucción, guardamos en subcarpeta C dentro de pdf
    # s3_key = f"{S3_PREFIX}/{filename}"  # COMMENTED OUT - S3 disabled for testing
    s3_key = None  # S3 disabled for testing
    return hashid, filename, s3_key


# def s3_object_exists(s3_client, bucket, key):
#     try:
#         s3_client.head_object(Bucket=bucket, Key=key)
#         return True
#     except ClientError as e:
#         code = e.response.get('Error', {}).get('Code', '')
#         if code in ("404", "NoSuchKey", 'NotFound'):
#             return False
#         # Para permisos u otros errores, re-lanzar
#         raise


# def upload_pdf_to_s3_if_not_exists(pdf_bytes, bucket, key):
#     s3 = boto3.client('s3')
#     if s3_object_exists(s3, bucket, key):
#         print(f"☑️ PDF already exists in S3: s3://{bucket}/{key}")
#         return False
#     try:
#         s3.put_object(Bucket=bucket, Key=key, Body=pdf_bytes, ContentType='application/pdf')
#         print(f"✅ PDF uploaded to s3://{bucket}/{key}")
#         return True
#     except Exception as e:
#         print(f"❌ Error uploading to S3: {e}")
#         return False



# -------------------------------------------------------------------
# 1) Obtener todos los links de detalle desde la paginación
# -------------------------------------------------------------------
def get_all_detail_links():
    resultados = []
    for page in range(1, TOTAL_PAGES + 1):
        url = LISTADO_URL.format(page)
        try:
            response = requests.get(url, headers=HEADERS, timeout=25)
            response.raise_for_status()
        except Exception as e:
            print(f"❌ Error en página {page}: {e}")
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        detalle_links = soup.select("a.btn.btn-custom-darkblue[href]")
        
        # Extraer también el estado de localización y la imagen de cada enlace
        for a in detalle_links:
            href = a["href"]
            if not href.startswith("http"):
                href = BASE_URL + href
            
            # Buscar el estado de localización y la imagen en el mismo contenedor
            localizado = None
            imagen_url = None
            try:
                card_container = a.find_parent("div", class_="card")
                if card_container:
                    # Buscar estado de localización
                    status_badges = card_container.find_all("span", class_=re.compile(r"badge.*badge-pill"))
                    for badge in status_badges:
                        badge_classes = badge.get("class", [])
                        if "text-bg-success" in badge_classes:
                            localizado = True
                            break
                        elif "text-bg-danger" in badge_classes:
                            localizado = False
                            break
                    
                    # Buscar imagen con id="previewImage"
                    img_tag = card_container.find("img", id="previewImage")
                    if img_tag and img_tag.get("src"):
                        imagen_url = img_tag["src"]
            except Exception:
                pass
            
            resultados.append((href, localizado, imagen_url))

        print(f"📄 Página {page} → {len(detalle_links)} enlaces encontrados.")

        time.sleep(0.3)  # para no saturar el servidor

    print(f"✅ Total de enlaces detalle encontrados: {len(resultados)}")
    return resultados


# -------------------------------------------------------------------
# 2) De cada página de detalle, obtener PDF e imagen
# -------------------------------------------------------------------
def get_pdf_url_from_detalle(detalle_url):
    try:
        response = requests.get(detalle_url, headers=HEADERS, timeout=10, allow_redirects=True)
        response.raise_for_status()
        
        content_type = response.headers.get('Content-Type', '')
        
        # Si la página de detalle devuelve directamente un PDF
        if 'application/pdf' in content_type.lower():
            return detalle_url, None, False
        
    except Exception as e:
        print(f"❌ Error al cargar detalle {detalle_url}: {e}")
        return None, None, False

    soup = BeautifulSoup(response.text, "html.parser")

    # Detectar si la persona está localizada usando múltiples métodos
    localizado = False
    try:
        # Debug: Imprimir todo el HTML para análisis
        print(f"  🔍 Analizando HTML completo para detectar estado...")
        all_text = soup.get_text().lower()
        print(f"  📄 Texto completo (primeros 500 chars): {all_text[:500]}")
        
        # Método 1: Buscar en todos los elementos span
        all_spans = soup.find_all("span")
        status_text = None
        
        print(f"  🔍 Buscando en {len(all_spans)} elementos span...")
        for i, span in enumerate(all_spans):
            text = span.get_text().strip()
            if text:  # Solo mostrar spans con texto
                print(f"    Span {i+1}: '{text}'")
            if text in ["Localizada", "Desaparecido", "Desaparecida"]:
                status_text = text
                if text == "Localizada":
                    localizado = True
                print(f"  ✅ Estado encontrado en span: '{text}' -> Localizada: {localizado}")
                break
        
        # Método 2: Si no se encontró en spans, buscar en todo el HTML
        if not status_text:
            print(f"  🔍 Buscando en texto completo...")
            if "persona localizada" in all_text:
                localizado = True
                status_text = "Persona Localizada (encontrado en texto completo)"
                print(f"  ✅ Estado encontrado en texto completo: 'persona localizada' -> Localizada: {localizado}")
            elif "localizada" in all_text:
                localizado = True
                status_text = "Localizada (encontrado en texto completo)"
                print(f"  ✅ Estado encontrado en texto completo: 'localizada' -> Localizada: {localizado}")
        
        # Método 3: Buscar en elementos con clases específicas
        if not localizado:
            print(f"  🔍 Buscando en elementos con clases específicas...")
            status_elements = soup.find_all(["div", "p", "h1", "h2", "h3", "h4", "h5", "h6"], class_=re.compile(r"status|estado|localiz", re.IGNORECASE))
            print(f"  🔍 Encontrados {len(status_elements)} elementos con clases relevantes")
            for element in status_elements:
                text = element.get_text().strip().lower()
                print(f"    Elemento {element.name}: '{text}'")
                if "localizada" in text:
                    localizado = True
                    status_text = f"Localizada (encontrado en {element.name})"
                    print(f"  ✅ Estado encontrado en {element.name}: '{text}' -> Localizada: {localizado}")
                    break
        
        # Método 4: Buscar en todos los elementos que contengan texto
        if not localizado:
            print(f"  🔍 Buscando en todos los elementos...")
            all_elements = soup.find_all()
            for element in all_elements:
                text = element.get_text().strip().lower()
                if "persona localizada" in text or "localizada" in text:
                    localizado = True
                    status_text = f"Localizada (encontrado en {element.name})"
                    print(f"  ✅ Estado encontrado en {element.name}: '{text}' -> Localizada: {localizado}")
                    break
        
        if status_text:
            print(f"  🏷️  Estado final encontrado: '{status_text}' -> Localizada: {localizado}")
        else:
            print(f"  ⚠️  No se encontró el elemento de estado en ningún método")
    except Exception as e:
        print(f"  ❌ Error detectando estado: {e}")
        localizado = False

    # Imagen - buscar por ID previewImage primero, luego por clase img-fluid
    img_tag = soup.find("img", id="previewImage")
    if not img_tag:
        img_tag = soup.find("img", class_="img-fluid")
    
    imagen_url = img_tag["src"] if img_tag else None
    if imagen_url and not imagen_url.startswith("http") and not imagen_url.startswith("data:"):
        imagen_url = BASE_URL + imagen_url
    print(f"  📷 Imagen: {imagen_url[:100]}{'...' if imagen_url and len(imagen_url) > 100 else ''}")

    # Buscar PDF de múltiples formas
    pdf_url = None
    
    # Método 1: Buscar enlaces con .pdf
    pdf_tag = soup.find("a", href=re.compile(r"\.pdf$", re.IGNORECASE))
    if pdf_tag:
        pdf_url = pdf_tag["href"]
        print(f"  ✅ PDF encontrado (método 1 - enlace directo .pdf)")
    
    # Método 2: Buscar enlaces que contengan 'pdf' en el href
    if not pdf_url:
        pdf_tag = soup.find("a", href=re.compile(r"pdf", re.IGNORECASE))
        if pdf_tag:
            pdf_url = pdf_tag["href"]
            print(f"  ✅ PDF encontrado (método 2 - contiene 'pdf')")
    
    # Método 3: Buscar botones o enlaces con atributos data-* o que contengan 'ficha'
    if not pdf_url:
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            text = link.get_text(strip=True).lower()
            classes = " ".join(link.get("class", []))
            
            # Revisar si el enlace podría ser una ficha/documento
            if any(keyword in href.lower() for keyword in ['ficha', 'documento', 'download', 'descargar', 'ver']):
                pdf_url = href
                print(f"  ✅ PDF encontrado (método 3 - href keyword): {href}")
                break
            if any(keyword in text for keyword in ['ficha', 'documento', 'pdf', 'descargar', 'ver detalle']):
                pdf_url = href
                print(f"  ✅ PDF encontrado (método 3 - texto keyword): '{text}' -> {href}")
                break
    
    # Método 4: Si no encontramos nada, intentar construir URL basada en patrones comunes
    if not pdf_url:
        print("  🔎 Intentando patrones de URL comunes...")
        # Extraer el ID encriptado de la URL
        match = re.search(r'/detalle/(.+)$', detalle_url)
        if match:
            encrypted_id = match.group(1)
            # Intentar URLs comunes para PDFs
            possible_urls = [
                f"{BASE_URL}/pdf/{encrypted_id}",
                f"{BASE_URL}/ficha/{encrypted_id}",
                f"{BASE_URL}/documento/{encrypted_id}",
                f"{BASE_URL}/descargar/{encrypted_id}",
                detalle_url.replace('/detalle/', '/pdf/'),
                detalle_url.replace('/detalle/', '/ficha/'),
            ]
            
            for test_url in possible_urls:
                try:
                    print(f"    Probando: {test_url}")
                    test_response = requests.head(test_url, headers=HEADERS, timeout=25, allow_redirects=True)
                    test_content_type = test_response.headers.get('Content-Type', '')
                    if 'application/pdf' in test_content_type.lower() or test_response.status_code == 200:
                        pdf_url = test_url
                        print(f"  ✅ PDF encontrado (método 4 - patrón): {test_url}")
                        break
                except:
                    continue
    
    if not pdf_url:
        # Listar todos los enlaces encontrados para debug
        print("  🔎 Enlaces encontrados en la página:")
        all_links = soup.find_all("a", href=True)
        for i, link in enumerate(all_links[:20], 1):  # Solo los primeros 20
            href = link["href"]
            text = link.get_text(strip=True)
            print(f"    {i}. Texto: '{text[:50]}' | Href: '{href[:80]}'")
        
        if len(all_links) > 20:
            print(f"    ... y {len(all_links) - 20} enlaces más")
        
        print(f"  ⚠️  No se encontró enlace PDF")
        return None, imagen_url, localizado

    # Normalizar URL
    if not pdf_url.startswith("http"):
        pdf_url = BASE_URL + pdf_url
    
    print(f"  📄 PDF URL final: {pdf_url}")
    return pdf_url, imagen_url, localizado


# -------------------------------------------------------------------
# 3) Descargar y extraer texto del PDF
# -------------------------------------------------------------------
def detect_localizado_from_text(text):
    """Detecta si una persona está localizada basándose en el texto del PDF"""
    if not text:
        return False
    
    text_lower = text.lower()
    
    # Palabras clave que indican que la persona está localizada
    localizada_keywords = [
        "localizada",
        "encontrada", 
        "localizado",
        "encontrado",
        "persona localizada",
        "ya fue localizada",
        "fue localizada",
        "ha sido localizada",
        "localizada el",
        "localizada en"
    ]
    
    # Buscar palabras clave en el texto
    for keyword in localizada_keywords:
        if keyword in text_lower:
            return True
    
    return False

def extract_text_and_images_from_pdf_url(pdf_url):
    try:
        response = requests.get(pdf_url, headers=HEADERS, timeout=30, allow_redirects=True)
        
        if response.status_code != 200:
            return None, None, False
        
        if not response.content.startswith(b'%PDF'):
            return None, None, False
        
        pdf_bytes = response.content
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        text = "\n".join([page.get_text("text") for page in doc])
        
        # Detectar si está localizada basándose en el texto
        localizado = detect_localizado_from_text(text)
        
        # Extraer imágenes del PDF
        imagen_url = None
        import base64
        
        for page_num in range(doc.page_count):
            page = doc[page_num]
            image_list = page.get_images()
            
            for img_index, img in enumerate(image_list):
                try:
                    xref = img[0]
                    pix = fitz.Pixmap(doc, xref)
                    
                    if pix.n - pix.alpha < 4:  # GRAY o RGB
                        img_data = pix.tobytes("png")
                        base64_data = base64.b64encode(img_data).decode('utf-8')
                        
                        if len(base64_data) > 1000:
                            imagen_url = f"data:image/png;base64,{base64_data}"
                            pix = None
                            break
                        else:
                            pix = None
                            continue
                    else:
                        # Intentar convertir CMYK a RGB
                        try:
                            pix_rgb = fitz.Pixmap(fitz.csRGB, pix)
                            img_data = pix_rgb.tobytes("png")
                            base64_data = base64.b64encode(img_data).decode('utf-8')
                            
                            if len(base64_data) > 1000:
                                imagen_url = f"data:image/png;base64,{base64_data}"
                                pix = None
                                pix_rgb = None
                                break
                            else:
                                pix = None
                                pix_rgb = None
                                continue
                        except Exception:
                            pix = None
                            continue
                    
                except Exception:
                    continue
        
        doc.close()
        return text, imagen_url, localizado, pdf_bytes
    except Exception as e:
        print(f"❌ Error al procesar PDF: {e}")
        return None, None, False


# -------------------------------------------------------------------
# 4) Parser para PDFs de Michoacán
# -------------------------------------------------------------------
def parse_pdf_data_michoacan(text):
    text = re.sub(r"\r\n|\r", "\n", text).lstrip()
    patrones = {
        "folio": r"FOLIO[:\-]?\s*([^\n]+)",
        "fecha_reporte": r"FECHA DE REPORTE[:\-]?\s*([0-9/]+)",
        "fecha_nacimiento": r"FECHA NACIMIENTO[:\-]?\s*([0-9/]+)",
        "edad": r"EDAD[:\-]?\s*([0-9]+)",
        "nacionalidad": r"NACIONALIDAD[:\-]?\s*([^\n]+)",
        "genero": r"(?:GÉNERO|SEXO)[:\-]?\s*([^\n]+)",
        "estatura": r"ESTATURA[:\-]?\s*([0-9.,]+)",
        "peso": r"PESO[:\-]?\s*([0-9.,]+)",
        "complexion": r"COMPLEXIÓN[:\-]?\s*([^\n]+)",
        "piel": r"COLOR DE PIEL[:\-]?\s*([^\n]+)",
        "cara": r"CARA[:\-]?\s*([^\n]+)",
        "frente": r"FRENTE[:\-]?\s*([^\n]+)",
        "cabello": r"CABELLO[:\-]?\s*([^\n]+)",
        "cejas": r"CEJAS[:\-]?\s*([^\n]+)",
        "ojos": r"OJOS[:\-]?\s*([^\n]+)",
        "nombre": r"NOMBRE[:\-]?\s*([^\n]+)",
        "senas": r"SEÑAS PARTICULARES[:\-]?\s*([^\n]+)",
        "tatuajes": r"TATUAJE\(S\)[:\-]?\s*([^\n]+)",
        "vestimenta": r"VESTIMENTA[:\-]?\s*([^\n]+)",
        "descripcion_hechos": r"DESCRIPCIÓN DE LOS HECHOS[:\-]?\s*([\s\S]*)"
    }
    datos = {}
    for campo, patron in patrones.items():
        match = re.search(patron, text, re.IGNORECASE)
        datos[campo] = match.group(1).strip() if match else None
    return datos


# -------------------------------------------------------------------
# 5) Insertar en la base de datos
# -------------------------------------------------------------------
def insert_into_db(data, detalle_url, hashid):
    extraction_date = datetime.date.today()
    localizado = data.get("localizado")
    conn = None
    cur = None 
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
            host=DB_HOST, port=DB_PORT
        )
        cur = conn.cursor()
        # Verificar existencia exactamente para (hashid, localizado)
        cur.execute("SELECT 1 FROM public.desaparecidos WHERE hashid = %s AND localizado = %s LIMIT 1", (hashid, localizado))
        exists = cur.fetchone() is not None
        if exists:
            print(f"✔ No hay que insertar: ya existe registro con hashid={hashid} y localizado={localizado}")
            return False

        query = "INSERT INTO public.desaparecidos (fecha_extraccion, url_origen, localizado, hashid, datos) VALUES (%s, %s, %s, %s, %s)"
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
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass


# -------------------------------------------------------------------
# 6) Flujo principal
# -------------------------------------------------------------------
def process_all():
    detalle_links = get_all_detail_links()
    print(f"\n🚀 Iniciando procesamiento de {len(detalle_links)} registros...\n")
    
    for i, (detalle_url, listing_localizado, listing_imagen_url) in enumerate(detalle_links, 1):
        print(f"\n{'='*80}")
        print(f"PROCESANDO REGISTRO {i}/{len(detalle_links)}")
        print(f"{'='*80}")
        
        pdf_url, imagen_url, html_localizado = get_pdf_url_from_detalle(detalle_url)
        if not pdf_url:
            print("  ⏭️  Saltando (sin PDF)")
            continue

        pdf_text, extracted_imagen_url, pdf_localizado, pdf_bytes = extract_text_and_images_from_pdf_url(pdf_url)
        if not pdf_text or not pdf_bytes:
            print("  ⏭️  Saltando (PDF vacío)")
            continue

        # Priorizar la imagen del listado, luego PDF, luego HTML
        if listing_imagen_url:
            final_imagen_url = listing_imagen_url
        elif extracted_imagen_url:
            final_imagen_url = extracted_imagen_url
        else:
            final_imagen_url = imagen_url

        # Priorizar el estado de la página de listado, luego PDF, luego HTML
        if listing_localizado is not None:
            final_localizado = listing_localizado
        elif pdf_localizado is not None:
            final_localizado = pdf_localizado
        elif html_localizado is not None:
            final_localizado = html_localizado
        else:
            final_localizado = False

        parsed_data = parse_pdf_data_michoacan(pdf_text)

        data = {
            "estado_alerta": "Desaparecidos Michoacán",
            "pdf_url": pdf_url,
            "imagen_url": final_imagen_url,
            "localizado": final_localizado,
            **parsed_data
        }
        # generar hashid y nombre de archivo
        hashid, filename, s3_key = make_hashid(data)
        # Subir a S3 si no existe - COMMENTED OUT FOR TESTING
        # try:
        #     if not s3_object_exists(s3, S3_BUCKET, s3_key):
        #         uploaded = upload_pdf_to_s3_if_not_exists(pdf_bytes, S3_BUCKET, s3_key)
        #     else:
        #         print(f"☑️ Ya existe en S3: s3://{S3_BUCKET}/{s3_key}")
        #         uploaded = False
        # except Exception as e:
        #     print(f"❌ Error verificando/guardando en S3: {e}")
        #     uploaded = False

         # Insertar en DB solo si no existe (hashid && localizado)
        inserted = insert_into_db(data, detalle_url, hashid)
        time.sleep(0.5)
    
    print(f"\n{'='*80}")
    print("✅ PROCESAMIENTO COMPLETO")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    process_all()