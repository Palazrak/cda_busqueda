import os
import time
from requests_html import HTMLSession
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin
import multiprocessing
import requests
import urllib3

# Deshabilitamos advertencias de SSL (solo para propósitos de scrapping)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def download_image(url, folder):
    try:
        url = requests.utils.requote_uri(url)
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
    
    base_url = "https://sg.guanajuato.gob.mx/personas-desaparecidas/"
    
    # Usamos HTMLSession para renderizar JavaScript
    session = HTMLSession()
    try:
        response = session.get(base_url, verify=False, timeout=20)
        response.html.render(timeout=20)
    except Exception as e:
        print(f"Error al renderizar la página: {e}")
        return

    # Parseamos el HTML renderizado con BeautifulSoup
    soup = BeautifulSoup(response.html.html, "html.parser")
    
    # Buscamos todas las etiquetas <img> con la clase "card-img-top img-fluid"
    imgs = soup.find_all("img", class_="card-img-top img-fluid")
    print(f"Se encontraron {len(imgs)} imágenes con la clase 'card-img-top img-fluid'")
    
    image_urls = []
    for img in imgs:
        src = img.get("src")
        # Filtramos según el patrón que identificaste, por ejemplo "desaparecidos/uploads"
        if src and "desaparecidos/uploads" in src:
            full_url = urljoin(base_url, src)
            image_urls.append(full_url)
    
    print(f"Total de URLs a descargar: {len(image_urls)}")
    
    folder = "images/guanajuato"
    os.makedirs(folder, exist_ok=True)
    
    # Configuramos hasta 24 workers (o el mínimo entre 24 y 2 * núcleos virtuales)
    workers = min(24, multiprocessing.cpu_count() * 2)
    print(f"Usando {workers} workers para la descarga de imágenes.")
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for url in image_urls:
            executor.submit(download_image, url, folder)
    
    end_time = time.time()
    print(f"Tiempo total de scrappeo: {end_time - start_time:.2f} segundos.")

if __name__ == "__main__":
    main()
