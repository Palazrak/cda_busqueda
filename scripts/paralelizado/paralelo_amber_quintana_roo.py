import os
import time
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin
import urllib3
import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
BUCKET_NAME = "cdas-2025-alertas-amber"
S3_FOLDER = "jpg/"

# Traer todos los archivos existentes en S3
def get_existing_files(bucket, prefix):
    existing = set()
    
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if "Contents" in page:
            for obj in page["Contents"]:
                existing.add(os.path.basename(obj["Key"]))

    return existing

# Verificar si el archivo ya existe en S3
def file_exists_s3(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False
    
# Deshabilitar advertencias de SSL (no recomendado para producción)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def download_image(url, existing_files):
    try:
        url = requests.utils.requote_uri(url)
        # Deshabilitamos la verificación del certificado con verify=False
        response = requests.get(url, timeout=20, verify=False)
        if response.status_code == 200:
            file_name = os.path.basename(url)
            file_name = f"2402_{file_name}" # Estado 24, segunda página
            s3_key = f"{S3_FOLDER}{file_name}"
            # verificar si ya existe
            if file_name in existing_files:
                print(f"La imagen ya existe en S3: {s3_key}")
                return
            s3.put_object(
                 Bucket=BUCKET_NAME,
                 Key=s3_key,
                 Body=response.content
                 )
            print(f"Imagen subida a S3: {s3_key}")
        else:
            print(f"Error {response.status_code} al descargar {url}")
    except Exception as e:
        print(f"Error al descargar {url}: {e}")

def main():
    start_time = time.time()
    
    base_url = "https://www.fgeqroo.gob.mx/alertas/Amber"
    total_pages = 191 #verificar número de páginas
    
    existing_files = get_existing_files(BUCKET_NAME, S3_FOLDER)
    print(f"Archivos existentes en S3: {len(existing_files)}")
    
    workers = 24  # Utilizamos 24 workers
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for page in range(1, total_pages + 1):
            page_url = f"{base_url}?page={page}"
            print(f"Procesando página {page}: {page_url}")
            try:
                # Deshabilitamos verificación SSL aquí también
                response = requests.get(page_url, timeout=20, verify=False)
                if response.status_code != 200:
                    print(f"Error al acceder a la página {page_url}")
                    continue
            except Exception as e:
                print(f"Error al solicitar {page_url}: {e}")
                continue
            
            soup = BeautifulSoup(response.content, "html.parser")
            section = soup.find("section", class_="grid-com bg-white")
            if not section:
                print(f"No se encontró la sección 'grid-com bg-white' en la página {page}")
                continue

            fichas = section.find_all("div", class_="detalle-com bg-gray-200")
            print(f"Página {page}: Se encontraron {len(fichas)} fichas")
            
            for ficha in fichas:
                img = ficha.find("img")
                if img and img.get("src"):
                    src = img["src"]
                    full_url = urljoin(page_url, src)
                    executor.submit(download_image, full_url,existing_files)
                    
    end_time = time.time()
    print(f"Tiempo total de scrappeo: {end_time - start_time:.2f} segundos.")

if __name__ == "__main__":
    main()
