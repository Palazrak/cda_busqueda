import os
import time
import multiprocessing
import requests
from requests_html import HTMLSession
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin

def download_image(url, folder):
    try:
        # Asegurarse de que la URL esté codificada
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
    
    base_url = "https://fiscaliaenpersonasdesaparecidas.jalisco.gob.mx/alerta-amber-jalisco/"
    session = HTMLSession()
    response = session.get(base_url, timeout=20)
    
    # Renderizamos la página para ejecutar JavaScript (puede demorar unos segundos)
    response.html.render(timeout=20)
    
    # Buscamos el contenedor del carrusel usando su clase
    carousel = response.html.find('.owl-stage-outer', first=True)
    if not carousel:
        print("No se encontró el contenedor 'owl-stage-outer'.")
        return

    # Extraemos todos los enlaces (<a>) dentro del contenedor
    links = carousel.find('a')
    print(f"Se encontraron {len(links)} elementos <a> en el carrusel.")
    
    image_urls = []
    for a in links:
        # Primero, intentamos encontrar una etiqueta <img> dentro del <a>
        img = a.find('img', first=True)
        if img and 'src' in img.attrs:
            src = img.attrs['src']
            full_url = urljoin(base_url, src)
            image_urls.append(full_url)
        # Si no hay <img>, usamos el atributo href del <a>
        elif 'href' in a.attrs:
            href = a.attrs['href']
            full_url = urljoin(base_url, href)
            image_urls.append(full_url)

    print(f"Total de URLs a descargar: {len(image_urls)}")
    
    folder = "images/jalisco"
    os.makedirs(folder, exist_ok=True)
    
    cores_virtuales = multiprocessing.cpu_count() * 2
    workers = min(24, cores_virtuales)
    print(f"Usando {workers} workers para la descarga de imágenes.")
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for url in image_urls:
            executor.submit(download_image, url, folder)
    
    end_time = time.time()
    print(f"Tiempo total de scrappeo: {end_time - start_time:.2f} segundos.")

if __name__ == "__main__":
    main()
