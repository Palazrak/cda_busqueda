import os
import time
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin
import urllib3

# Deshabilitar advertencias de SSL (no recomendado para producción)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def download_image(url, folder):
    try:
        url = requests.utils.requote_uri(url)
        # Deshabilitamos la verificación del certificado con verify=False
        response = requests.get(url, timeout=20, verify=False)
        if response.status_code == 200:
            file_name = os.path.basename(url)
            file_path = os.path.join(folder, file_name)
            with open(file_path, 'wb') as f:
                f.write(response.content)
        else:
            print(f"Error {response.status_code} al descargar {url}")
    except Exception as e:
        print(f"Error al descargar {url}: {e}")

def main():
    start_time = time.time()
    
    base_url = "https://www.fgeqroo.gob.mx/alertas/Amber"
    total_pages = 151
    folder = "images/quintana_roo"
    os.makedirs(folder, exist_ok=True)
    
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
                    executor.submit(download_image, full_url, folder)
                    
    end_time = time.time()
    print(f"Tiempo total de scrappeo: {end_time - start_time:.2f} segundos.")

if __name__ == "__main__":
    main()
