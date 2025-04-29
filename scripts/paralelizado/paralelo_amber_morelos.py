import os
import time
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin
import multiprocessing

def download_image(url, folder):
    try:
        # Codifica la URL para evitar problemas con espacios o caracteres especiales
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

    # La URL base de la página (la página 1 es la URL base y las siguientes se agregan con el número y una barra final)
    base_url = "https://fiscaliamorelos.gob.mx/alerta-amber-morelos/"
    total_pages = 5
    folder = "images/morelos"
    os.makedirs(folder, exist_ok=True)

    # Configuramos 24 workers (o el mínimo entre 24 y 2*núcleos virtuales)
    cores_virtuales = multiprocessing.cpu_count() * 2
    workers = min(24, cores_virtuales)
    print(f"Usando {workers} workers para la descarga de imágenes.")

    with ThreadPoolExecutor(max_workers=workers) as executor:
        for page in range(1, total_pages + 1):
            if page == 1:
                page_url = base_url
            else:
                # Para las páginas 2 a 5, la URL se arma agregando el número de página y una barra final.
                page_url = urljoin(base_url, f"{page}/")
            try:
                response = requests.get(page_url, timeout=20)
                if response.status_code != 200:
                    print(f"Error al acceder a la página {page_url}")
                    continue
            except Exception as e:
                print(f"Error al solicitar {page_url}: {e}")
                continue

            soup = BeautifulSoup(response.content, "html.parser")
            # Buscamos las imágenes que son nuestras fichas.
            # En el ejemplo se observa que el src de la imagen contiene "wp-content/uploads".
            imgs = soup.find_all("img", src=lambda x: x and "wp-content/uploads" in x)

            for img in imgs:
                src = img.get("src")
                if src:
                    full_url = urljoin(page_url, src)
                    # Enviamos la tarea de descarga sin esperar a que termine para pasar a la siguiente ficha/página.
                    executor.submit(download_image, full_url, folder)

    end_time = time.time()
    print(f"Tiempo total de scrappeo: {end_time - start_time:.2f} segundos.")

if __name__ == "__main__":
    main()
