"""
Comisión de Búsqueda del Estado de Coahuila de Zaragoza:
- La página inicial tiene un carrusel/listado.
- Al dar click a cada persona, se abre un HTML con la información de la ficha.
- Link listado: https://cbecz.gob.mx/le-estamos-buscando
- Prefijo hash_id: 0901_
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import re
from pathlib import Path
import sys
import time
from typing import Any, Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from utils.db_utils import insert_payload, make_hashid as make_shared_hashid
from utils.s3_utils import upload_url_if_enabled


# -------------------- Config --------------------
BASE_URL = "https://cbecz.gob.mx"
LISTING_URL_TEMPLATE = BASE_URL + "/le-estamos-buscando?page={}"
HEADERS = {"User-Agent": "Mozilla/5.0"}

INSERT_DB_DEFAULT = True

S3_FOLDER = "html/"
TEST_LIMIT = None


# -------------------- Utilidades --------------------
def normalize_ws(value: str) -> str:
    """Colapsa espacios y saltos de línea para normalizar texto."""
    return re.sub(r"\s+", " ", value or "").strip()


def clean_period(value: str) -> str:
    """Quita puntos finales comunes."""
    v = normalize_ws(value)
    v = v.rstrip(".")
    return normalize_ws(v)


def extract_detail_url_from_html(html: str) -> Optional[str]:
    """Intenta extraer la URL canónica de la ficha desde el HTML."""
    m = re.search(r'<link[^>]+rel="canonical"[^>]+href="([^"]+)"', html, re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r'<meta[^>]+property="og:url"[^>]+content="([^"]+)"', html, re.IGNORECASE)
    return m.group(1) if m else None


def extract_folio_from_detail_url(detalle_url: str) -> Optional[str]:
    m = re.search(r"/le-estamos-buscando/(\d+)", detalle_url)
    return m.group(1) if m else None


# -------------------- Parser de fichas --------------------
def find_heading_h5(soup: BeautifulSoup, needle: str) -> Optional[Any]:
    needle_norm = normalize_ws(needle).lower()
    for h5 in soup.find_all("h5"):
        h5_text = normalize_ws(h5.get_text(" ", strip=True)).lower()
        if needle_norm == h5_text:
            return h5
    return None


def extract_h4_value_after_heading(soup: BeautifulSoup, heading_text: str) -> Optional[str]:
    h5 = find_heading_h5(soup, heading_text)
    if not h5:
        return None
    h4 = h5.find_next("h4")
    if not h4:
        return None
    return clean_period(h4.get_text(" ", strip=True))


def parse_nombre(soup: BeautifulSoup) -> Optional[str]:
    # Hay más de un <h1> (header y ficha). El de la ficha está dentro de `cbl_card_main`.
    card = soup.find("div", class_=re.compile(r"\bcbl_card_main\b"))
    h1 = card.find("h1") if card else soup.find("h1")
    if not h1:
        return None
    raw = normalize_ws(h1.get_text(" ", strip=True))
    # Ej: "C. Eusebio Ontiveros Esquivel"
    raw = re.sub(r"^c\.?\s*", "", raw, flags=re.IGNORECASE)
    return raw or None


def parse_imagen_url(soup: BeautifulSoup) -> Optional[str]:
    img = soup.find("img", alt=True)
    if not img:
        return None
    src = img.get("src")
    if not src:
        return None
    return src


def parse_characteristicas(soup: BeautifulSoup) -> Dict[str, Any]:
    # Defaults: si no se encuentra, regresamos null.
    data: Dict[str, Any] = {
        "complexion": None,
        "tez": None,
        "cara": None,
        "ojos": None,
        "nariz": None,
        "boca": None,
        "labios": None,
        "cabello": None,
        "barba": False,
        "bigote": False,
        "orejas": None,
    }

    h5 = find_heading_h5(soup, "Características")
    if not h5:
        return data

    ul = h5.find_next("ul")
    if not ul:
        return data

    for li in ul.find_all("li"):
        li_text = normalize_ws(li.get_text(" ", strip=True))
        if not li_text:
            continue

        li_lower = li_text.lower()

        if li_lower.startswith("complexión") or li_lower.startswith("complexion"):
            m = re.search(r"complexi[oó]n\s*(.*)$", li_text, flags=re.IGNORECASE)
            data["complexion"] = clean_period(m.group(1) if m else li_text)
            continue

        if li_lower.startswith("tez"):
            m = re.search(r"tez\s*(.*)$", li_text, flags=re.IGNORECASE)
            data["tez"] = clean_period(m.group(1) if m else li_text)
            continue

        if li_lower.startswith("cara"):
            m = re.search(r"cara\s*(.*)$", li_text, flags=re.IGNORECASE)
            data["cara"] = clean_period(m.group(1) if m else li_text)
            continue

        if "ojos" in li_lower:
            # Normaliza a: "Ojos {tamaño}, color {color}" si viene completo.
            m = re.search(r"ojos\s*(.*)$", li_text, flags=re.IGNORECASE)
            rest = clean_period(m.group(1) if m else li_text)
            rest = rest.lstrip(",").strip()
            data["ojos"] = f"Ojos {rest}" if rest else None
            continue

        if li_lower.startswith("nariz"):
            m = re.search(r"nariz\s*(.*)$", li_text, flags=re.IGNORECASE)
            data["nariz"] = clean_period(m.group(1) if m else li_text)
            continue

        if "boca" in li_lower and "labios" in li_lower:
            boca_match = re.search(r"boca\s*([^\.]+)\.", li_text, flags=re.IGNORECASE)
            labios_match = re.search(r"labios\s*([^\.]+)\.?", li_text, flags=re.IGNORECASE)
            if boca_match:
                data["boca"] = clean_period(boca_match.group(1))
            if labios_match:
                data["labios"] = clean_period(labios_match.group(1))
            continue

        if li_lower.startswith("boca"):
            m = re.search(r"boca\s*(.*)$", li_text, flags=re.IGNORECASE)
            data["boca"] = clean_period(m.group(1) if m else li_text)
            continue

        if "cabello" in li_lower:
            m = re.search(r"cabello\s*(.*)$", li_text, flags=re.IGNORECASE)
            rest = m.group(1) if m else None
            if rest is not None:
                rest = rest.lstrip(",").strip()
            data["cabello"] = clean_period(rest or li_text)
            continue

        if "barba" in li_lower:
            data["barba"] = True
            continue

        if "bigote" in li_lower:
            data["bigote"] = True
            continue

        if li_lower.startswith("orejas"):
            m = re.search(r"orejas\s*:\s*(.*)$", li_text, flags=re.IGNORECASE)
            if not m:
                m = re.search(r"orejas\s*(.*)$", li_text, flags=re.IGNORECASE)
            data["orejas"] = clean_period(m.group(1) if m else li_text)
            continue

        # Campos no contemplados (ej. "otros") se ignoran.

    return data


def append_join(existing: Optional[str], addition: str) -> str:
    if not existing:
        return addition
    if addition in existing:
        return existing
    return existing + "\n" + addition


def parse_senas_particulares(soup: BeautifulSoup) -> Dict[str, Any]:
    # Defaults
    data: Dict[str, Any] = {
        "tatuajes": None,
        "cicatrices": None,
        "lunares": None,
        "discapacidades": None,
        "manchas": None,
    }

    h5 = find_heading_h5(soup, "Señas particulares")
    if not h5:
        return data

    ul = h5.find_next("ul")
    if not ul:
        return data

    def label_to_key(label: str) -> Optional[str]:
        l = normalize_ws(label).lower().rstrip(".")
        if l.startswith("tatuaj"):
            return "tatuajes"
        # "cicatrices" no matchea con prefijo "cicatriz" (cambio z->c).
        if l.startswith("cicatr"):
            return "cicatrices"
        if l.startswith("lunar"):
            return "lunares"
        if l.startswith("discap"):
            return "discapacidades"
        if l.startswith("manch"):
            return "manchas"
        return None

    for li in ul.find_all("li", recursive=False):
        strong = li.find("strong")
        if not strong:
            continue
        key = label_to_key(strong.get_text(" ", strip=True))
        if not key:
            continue

        ubicacion: Optional[str] = None
        caracteristicas: Optional[str] = None
        for detail_li in li.select("ul li"):
            t = normalize_ws(detail_li.get_text(" ", strip=True))
            if not t:
                continue
            t_lower = t.lower()
            if t_lower.startswith("ubicación:") or t_lower.startswith("ubicacion:"):
                ubicacion = normalize_ws(t.split(":", 1)[1])
            elif t_lower.startswith("características:") or t_lower.startswith("caracteristicas:"):
                caracteristicas = normalize_ws(t.split(":", 1)[1])

        parts: list[str] = []
        if ubicacion is not None and ubicacion != "":
            parts.append(f"Ubicación: {ubicacion}")
        if caracteristicas is not None and caracteristicas != "":
            parts.append(f"Características: {caracteristicas}")

        value = "\n".join(parts) if parts else None
        if value:
            data[key] = append_join(data[key], value)

    return data


def parse_vestimenta(soup: BeautifulSoup) -> Dict[str, Any]:
    data: Dict[str, Any] = {
        "prenda_superior": None,
        "prenda_inferior": None,
        "calzado": None,
        "prenda_interior": None,
        "prenda_exterior": None,
        "prenda_accesoria": None,
        "objetos_uso_personal": None,
    }

    h5 = find_heading_h5(soup, "Vestimenta")
    if not h5:
        return data

    # Mapa por substring visible en el <h5>.
    mapping = {
        "Prenda Superior": "prenda_superior",
        "Prenda Inferior": "prenda_inferior",
        "Calzado": "calzado",
        "Prenda interior": "prenda_interior",
        "Prenda exterior": "prenda_exterior",
        "Prenda accesoria": "prenda_accesoria",
        "Objetos de uso personal": "objetos_uso_personal",
    }

    # Recorremos h5 posteriores y tomamos los que correspondan.
    for h5_after in h5.find_all_next("h5"):
        label = normalize_ws(h5_after.get_text(" ", strip=True))
        matched_key: Optional[str] = None
        for label_sub, json_key in mapping.items():
            if label_sub.lower() in label.lower():
                matched_key = json_key
                break
        if not matched_key:
            # El contacto posterior no tiene h5 con esos labels; seguimos.
            continue

        # Buscamos el contenedor (col-*) y capturamos todos los <p> asociados.
        col = h5_after.find_parent("div", class_=re.compile(r"\bcol-(sm|md|lg)-\d+|\bcol-\d+\b"))
        if not col:
            col = h5_after.parent

        ps = []
        for p in col.find_all("p"):
            p_text = normalize_ws(p.get_text(" ", strip=True))
            if p_text:
                ps.append(p_text)
        data[matched_key] = "\n".join(ps) if ps else None

    return data


def parse_ficha_html(html: str, detalle_url: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")

    folio = extract_folio_from_detail_url(detalle_url)
    if not folio:
        raise ValueError(f"No se pudo extraer folio desde detalle_url={detalle_url}")

    nombre = parse_nombre(soup)
    visto_por_ultima_vez = extract_h4_value_after_heading(soup, "Visto por última vez")
    edad_al_desaparecer = extract_h4_value_after_heading(soup, "Edad al desaparecer")
    desaparecido_desde = extract_h4_value_after_heading(soup, "Desaparecido desde")
    estatura = extract_h4_value_after_heading(soup, "Estatura")

    caracteristicas = parse_characteristicas(soup)
    senas_particulares = parse_senas_particulares(soup)
    vestimenta = parse_vestimenta(soup)

    # Si el sitio dice "Sin información" lo normalizamos a null (para favorecer null en json).
    if estatura and estatura.lower().startswith("sin información"):
        estatura = None

    # `localizado` siempre false según la instrucción del usuario.
    localizado = False

    # Hashid usa descripcion_hechos y senas (string).
    senas_parts = [senas_particulares.get(k) for k in ("tatuajes", "cicatrices", "lunares", "discapacidades", "manchas")]
    senas = "\n".join([s for s in senas_parts if s]) if any(senas_parts) else ""

    parsed_data: Dict[str, Any] = {
        "estado_alerta": "Desaparecidos Comisión de Búsqueda Coahuila",
        "folio": folio,
        "localizado": localizado,
        "nombre": nombre,
        "visto_por_ultima_vez": visto_por_ultima_vez,
        "desaparecido_desde": desaparecido_desde,
        "edad_al_desaparecer": edad_al_desaparecer,
        "edad": edad_al_desaparecer,
        "descripcion_hechos": desaparecido_desde,
        "estatura": estatura,
        "imagen_url": parse_imagen_url(soup),
        # Características
        **caracteristicas,
        # Señas particulares
        **senas_particulares,
        # Vestimenta
        **vestimenta,
        # Campo usado para hash.
        "senas": senas,
    }

    return parsed_data


# -------------------- Hashid --------------------
def normalize_for_hash(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip().lower()


def make_hashid(parsed_data: Dict[str, Any]) -> Tuple[str, str, None]:
    """
    Genera hash a partir de:
    folio, localizado, nombre, edad, descripcion_hechos, senas
    Prefijo: 0901_
    """
    hashid = make_shared_hashid("0901_", parsed_data)
    filename = f"{hashid}.pdf"
    return hashid, filename, None


# -------------------- DB Insert --------------------
def insert_into_db(data: Dict[str, Any], detalle_url: str, hashid: str, fecha_modificacion: datetime.datetime) -> bool:
    localizado = bool(data.get("localizado", False))
    try:
        inserted = insert_payload(
            "0901_",
            data,
            detalle_url,
            localizado=localizado,
            hashid=hashid,
        )
        print(f"✅ Insertados en DB: {inserted} hashid={hashid}")
        return bool(inserted)
    except Exception as e:
        print(f"❌ Error al insertar en la base de datos: {e}")
        return False


# -------------------- Scraping: listado -> fichas --------------------
def get_total_pages_from_html(html: str) -> int:
    pages = [int(m.group(1)) for m in re.finditer(r"gotoPage\((\d+),\s*'page'\)", html)]
    return max(pages) if pages else 1


def fetch_html(url: str, timeout: int = 25) -> str:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text


def extract_detail_links_from_listing_html(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.select("a.cbl_infocard[href]"):
        href = a.get("href")
        if not href:
            continue
        if href.startswith("http"):
            links.append(href)
        else:
            links.append(BASE_URL + href)
    # dedup preserve order
    seen = set()
    unique = []
    for l in links:
        if l not in seen:
            unique.append(l)
            seen.add(l)
    return unique


def scrape_all_network(max_pages: Optional[int] = None) -> list[Dict[str, Any]]:
    # 1) Detectar total páginas con página 1 (o base listing)
    first_url = LISTING_URL_TEMPLATE.format(1)
    try:
        first_html = fetch_html(first_url)
    except Exception as e:
        print(f"❌ No se pudo obtener listado Coahuila {first_url}: {e}")
        return []
    total_pages = get_total_pages_from_html(first_html)
    if max_pages is not None:
        total_pages = min(total_pages, max_pages)

    print(f"📄 Páginas detectadas: {total_pages}")

    detail_links: list[str] = []
    # Extraer de página 1
    detail_links.extend(extract_detail_links_from_listing_html(first_html))

    for page in range(2, total_pages + 1):
        url = LISTING_URL_TEMPLATE.format(page)
        html = fetch_html(url)
        detail_links.extend(extract_detail_links_from_listing_html(html))
        time.sleep(0.3)

    # Dedup final
    seen = set()
    uniq_links = []
    for l in detail_links:
        if l in seen:
            continue
        uniq_links.append(l)
        seen.add(l)

    print(f"✅ Detalles encontrados: {len(uniq_links)}")

    scraped: list[Dict[str, Any]] = []
    for i, detalle_url in enumerate(uniq_links, 1):
        print(f"  Procesando {i}/{len(uniq_links)}: {detalle_url}")
        html = fetch_html(detalle_url, timeout=30)
        parsed_data = parse_ficha_html(html, detalle_url)
        scraped.append({"detalle_url": detalle_url, "datos": parsed_data})
        time.sleep(0.4)

    return scraped


# -------------------- Modo ejemplos (solo local) --------------------
EXAMPLES_DIR = "scripts/serial/ejemplosCoahuila"


def scrape_examples_from_local_fichas() -> Dict[str, Dict[str, Any]]:
    resultados: Dict[str, Dict[str, Any]] = {}

    for i in range(1, 6):
        ficha_path = f"{EXAMPLES_DIR}/ficha{i}"
        with open(ficha_path, "r", encoding="utf-8") as f:
            html = f.read()

        detalle_url = extract_detail_url_from_html(html) or f"{BASE_URL}/le-estamos-buscando/{i}"
        parsed_data = parse_ficha_html(html, detalle_url)

        resultados[f"ficha{i}"] = parsed_data

    return resultados


def upload_image_to_s3(url, hashid):
    if not url or url.startswith("data:"):
        return None
    try:
        from urllib.parse import urljoin
        if not url.startswith("http"):
            url = urljoin(BASE_URL, url)
        ext = os.path.splitext(url.split("?")[0])[-1]
        if not ext or len(ext) > 5:
            ext = ".jpg"
        filename = f"{hashid}{ext}"
        s3_key = f"{S3_FOLDER}{filename}"
        s3_url = upload_url_if_enabled(url, s3_key, headers=HEADERS, timeout=20)
        if s3_url:
            print(f"✅ Imagen subida: {s3_url}")
            return s3_url
        return None
    except Exception as e:
        print(f"❌ Error S3: {e}")
        return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--examples", action="store_true", help="Procesa las 5 fichas de ejemplo localmente y guarda JSON.")
    parser.add_argument("--insert-db", action="store_true", help="Compatibilidad: DB ya está habilitada por defecto.")
    parser.add_argument("--no-insert-db", action="store_true", help="Solo scrapea sin insertar en la base de datos.")
    parser.add_argument("--max-pages", type=int, default=None, help="Máximo de páginas a scrapear (red).")
    args = parser.parse_args()

    if args.examples:
        resultados = scrape_examples_from_local_fichas()
        # Guardar SOLO datos según instrucción.
        out_path = f"{EXAMPLES_DIR}/coahuila_scrape_datos.json"
        only_datos = {k: v for k, v in resultados.items()}
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(only_datos, f, ensure_ascii=False, indent=2)
        print(f"✅ Guardado en {out_path}")
        return

    # Red
    INSERT_DB = INSERT_DB_DEFAULT and not args.no_insert_db
    insert_count = 0

    scraped = scrape_all_network(max_pages=args.max_pages)
    fecha_modificacion = datetime.datetime.now()
    count = 0

    for entry in scraped:
        detalle_url = entry["detalle_url"]
        datos = entry["datos"]
        hashid, _, _ = make_hashid(datos)
        upload_image_to_s3(datos.get("imagen_url"), hashid)
        if INSERT_DB:
            if insert_into_db(datos, detalle_url, hashid, fecha_modificacion=fecha_modificacion):
                insert_count += 1
        count += 1
        if TEST_LIMIT and count >= TEST_LIMIT:
            break

    print(f"✅ Scraping finalizado. Insertados en DB: {insert_count if INSERT_DB else 0}")


if __name__ == "__main__":
    main()
