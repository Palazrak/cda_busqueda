from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ...
def scrape_with_clicks():
    base_url = "https://alertaamber.fgr.org.mx/Alerta/CarruselGB"
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(base_url)
    try:
        # Aumentamos el timeout a 30 segundos y usamos visibility_of_all_elements_located
        WebDriverWait(driver, 30).until(
            EC.visibility_of_all_elements_located((By.CSS_SELECTOR, "tr.txtestados"))
        )
    except Exception as e:
        print("Error esperando la tabla de estados:", e)
        # Para depuración, podrías imprimir parte del HTML:
        # print(driver.page_source)
        driver.quit()
        return []
    time.sleep(2)
    all_data = []
    rows = driver.find_elements(By.CSS_SELECTOR, "tr.txtestados")
    print(f"Se encontraron {len(rows)} filas de estados.")
    # Si no se encuentran filas, quizás revisar el selector
    for row in rows:
        try:
            driver.execute_script("arguments[0].click();", row)
            time.sleep(2)
            html = driver.page_source
            soup = BeautifulSoup(html, "html.parser")
            pdf_links = soup.find_all("a", href=re.compile(r'/Alerta/CreaAlertaPDFPublico\?numero_reporte=\d+'))
            print(f"En estado '{row.text.strip()}', se encontraron {len(pdf_links)} enlaces PDF.")
            for a_tag in pdf_links:
                pdf_href = a_tag.get("href")
                pdf_link = "https://alertaamber.fgr.org.mx" + pdf_href
                img_tag = a_tag.find("img", class_="carousel-image")
                if img_tag:
                    img_src = img_tag.get("src")
                    foto_url = "https://alertaamber.fgr.org.mx" + img_src
                else:
                    foto_url = None
                try:
                    pdf_response = requests.get(pdf_link, timeout=10)
                    if pdf_response.status_code != 200:
                        continue
                    pdf_data = extract_pdf_data(pdf_response.content)
                    pdf_data['imagen_url'] = foto_url
                    pdf_data['pdf_link'] = pdf_link
                    estado_text = row.text.strip()
                    pdf_data['estado'] = estado_text
                    all_data.append(pdf_data)
                except Exception as e:
                    print("Error al procesar PDF:", e)
            time.sleep(0.5)
        except Exception as e:
            print("Error al hacer click en fila:", e)
    driver.quit()
    return all_data
