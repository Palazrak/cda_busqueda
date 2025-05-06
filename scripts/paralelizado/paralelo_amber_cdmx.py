import os
import time
import requests
import multiprocessing
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin

def download_image(url, folder):
    try:
        # Codificar la URL (ej. espacios a %20)
        url = requests.utils.requote_uri(url)
        response = requests.get(url, timeout=20)
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
    
    # URL de la página de la Ciudad de México
    base_url = "https://www.fgjcdmx.gob.mx/nuestros-servicios/servicios-la-ciudadania/alerta-amber-df"
    response = requests.get(base_url, timeout=20)
    if response.status_code != 200:
        print("Error al acceder a la página.")
        return
    
    soup = BeautifulSoup(response.content, "html.parser")
    
    # Buscamos el enlace que despliega el contenido colapsado,
    # usando el atributo href="#collapse599" del full XPath proporcionado.
    collapse_link = soup.find("a", href="#collapse599")
    if not collapse_link:
        print("No se encontró el enlace de colapso con href='#collapse599'.")
        return

    # Ahora, buscamos el contenedor colapsado (que normalmente se genera con id="collapse599")
    collapse_container = soup.find(id="collapse599")
    if not collapse_container:
        print("No se encontró el contenedor colapsado con id 'collapse599'.")
        return

    # Dentro del contenedor, buscamos todos los <p>
    p_tags = collapse_container.find_all("p")
    print(f"Se encontraron {len(p_tags)} elementos <p> en el contenedor colapsado.")
    
    image_urls = []
    # Recorremos todos los <p> para extraer la URL de la imagen
    for p in p_tags:
        # Solo procesamos los <p> que contengan un <img>
        img_tag = p.find("img")
        if img_tag and img_tag.get("src"):
            src = img_tag["src"]
            full_url = urljoin(base_url, src)
            image_urls.append(full_url)
    
    print(f"Total de URLs de imágenes a descargar: {len(image_urls)}")
    
    # Carpeta de destino
    folder = "images/cdmx"
    os.makedirs(folder, exist_ok=True)
    
    # Configuramos la paralelización: 24 workers o el mínimo entre 24 y (nº de cores virtuales * 2)
    cores_virtuales = multiprocessing.cpu_count() * 2
    workers = min(24, cores_virtuales)
    print(f"Usando {workers} workers para la descarga de imágenes.")
    
    # Descargamos las imágenes en paralelo
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for url in image_urls:
            executor.submit(download_image, url, folder)
    
    end_time = time.time()
    print(f"Tiempo total de scrappeo: {end_time - start_time:.2f} segundos.")

if __name__ == "__main__":
    main()
