"""
Fiscalia General de Oaxaca - Personas Desaparecidas.

Fuente:
- https://portal.fgeo.gob.mx/index.php/dnol-personas-desaparecidas

Prefijo hash_id: 2101_
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
import unicodedata
from typing import Any, Dict, Iterator, Optional, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from utils.db_utils import insert_payload, make_hashid as make_shared_hashid
from utils.s3_utils import upload_url_if_enabled

BASE_URL = "https://portal.fgeo.gob.mx"
BASE_LIST_URL = "https://portal.fgeo.gob.mx/index.php/dnol-personas-desaparecidas"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; Oaxaca-scraper/1.0)"}
REQUEST_TIMEOUT = 45

INSERT_DB_DEFAULT = True

S3_FOLDER = "html/"
TEST_LIMIT = None


def normalize_ws(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def strip_accents(s: str) -> str:
    nfd = unicodedata.normalize("NFD", s or "")
    return "".join(c for c in nfd if unicodedata.category(c) != "Mn")


def normalize_for_hash(value: Any) -> str:
    if value is None:
        return ""
    return normalize_ws(value).lower()


def make_hashid(parsed_data: Dict[str, Any]) -> tuple[str, str, None]:
    hashid = make_shared_hashid("2101_", parsed_data)
    return hashid, f"{hashid}.pdf", None


def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.text


def extract_detail_links(list_html: str, page_url: str) -> list[str]:
    soup = BeautifulSoup(list_html, "html.parser")
    links: list[str] = []
    for a in soup.select("a.dnol-card-foto-link[href], a.dnol-card-btn[href]"):
        href = normalize_ws(a.get("href"))
        if not href:
            continue
        full = urljoin(page_url, href)
        if "/archivos/personas-desaparecidas/" not in full:
            continue
        links.append(full)
    return links


def get_next_page_url(list_html: str, page_url: str) -> Optional[str]:
    soup = BeautifulSoup(list_html, "html.parser")
    nxt = soup.select_one("a.next.page-numbers[href]")
    if nxt and nxt.get("href"):
        return urljoin(page_url, normalize_ws(nxt["href"]))
    return None


def iter_detail_urls(max_pages: Optional[int] = None) -> Iterator[str]:
    seen_urls: Set[str] = set()
    current = BASE_LIST_URL
    page = 0
    while current:
        page += 1
        if max_pages is not None and page > max_pages:
            break
        html = fetch_html(current)
        links = extract_detail_links(html, current)
        print(f"  listado pagina {page}: {len(links)} fichas")
        for link in links:
            if link in seen_urls:
                continue
            seen_urls.add(link)
            yield link
        current = get_next_page_url(html, current)
        time.sleep(0.15)


def parse_localizado_from_text(text: str) -> Optional[bool]:
    t = strip_accents(normalize_ws(text)).lower()
    if "no localizado" in t or "no localizada" in t:
        return False
    # cuidado: primero buscar "no localizado", ya cubierto arriba
    if "localizado" in t or "localizada" in t:
        return True
    return None


def parse_ficha_html(html: str, url_origen: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")

    nombre = None
    h2 = soup.select_one(".ficha-nombre h2")
    if h2:
        txt = normalize_ws(h2.get_text(" ", strip=True))
        txt = re.sub(r"(?i)^NOMBRE:\s*", "", txt).strip()
        nombre = txt or None
    if not nombre:
        pnombre = soup.select_one(".ficha-contenido p")
        if pnombre:
            m = re.search(r"(?i)NOMBRE:\s*(.*)", normalize_ws(pnombre.get_text(" ", strip=True)))
            if m:
                nombre = normalize_ws(m.group(1)).rstrip(".") or None

    imagen_url = None
    img = soup.select_one(".ficha-foto-container img")
    if img and img.get("src"):
        imagen_url = normalize_ws(img["src"])
    if not imagen_url:
        og = soup.select_one('meta[property="og:image"]')
        if og and og.get("content"):
            imagen_url = normalize_ws(og["content"])

    # Parseo de campos de contenido por etiquetas textuales.
    edad = None
    visto = None
    ropa = None
    media = None
    senas = None
    extras: list[str] = []
    contenido_text = []

    for p in soup.select(".ficha-contenido p"):
        txt = normalize_ws(p.get_text(" ", strip=True))
        if not txt:
            continue
        contenido_text.append(txt)
        norm = strip_accents(txt).upper()
        if norm.startswith("NOMBRE:"):
            continue
        if norm.startswith("EDAD:"):
            edad = re.sub(r"(?i)^EDAD:\s*", "", txt).strip().rstrip(".")
            continue
        if norm.startswith("FUE VISTO POR ULTIMA VEZ:"):
            visto = re.sub(r"(?i)^FUE VISTO POR[ ]+ÚLTIMA VEZ:\s*", "", txt).strip().rstrip(".")
            if not visto:
                visto = re.sub(r"(?i)^FUE VISTO POR[ ]+ULTIMA VEZ:\s*", "", txt).strip().rstrip(".")
            continue
        if norm.startswith("ROPA QUE VISTE:") or norm.startswith("ROPA QUE VESTIA:"):
            ropa = re.sub(r"(?i)^ROPA QUE VISTE:\s*", "", txt).strip().rstrip(".")
            ropa = re.sub(r"(?i)^ROPA QUE VESTÍA:\s*", "", ropa).strip().rstrip(".")
            ropa = re.sub(r"(?i)^ROPA QUE VESTIA:\s*", "", ropa).strip().rstrip(".")
            continue
        if norm.startswith("MEDIA FILIACION:"):
            media = re.sub(r"(?i)^MEDIA FILIACIÓN:\s*", "", txt).strip().rstrip(".")
            media = re.sub(r"(?i)^MEDIA FILIACION:\s*", "", media).strip().rstrip(".")
            continue
        if norm.startswith("SEÑAS PARTICULARES:") or norm.startswith("SENAS PARTICULARES:") or norm.startswith("SEÑA PARTICULAR:") or norm.startswith("SENA PARTICULAR:"):
            senas = re.sub(r"(?i)^SEÑAS PARTICULARES:\s*", "", txt).strip().rstrip(".")
            senas = re.sub(r"(?i)^SEÑAS PARTICULAR:\s*", "", senas).strip().rstrip(".")
            senas = re.sub(r"(?i)^SEÑA PARTICULAR:\s*", "", senas).strip().rstrip(".")
            senas = re.sub(r"(?i)^SENAS PARTICULARES:\s*", "", senas).strip().rstrip(".")
            senas = re.sub(r"(?i)^SENA PARTICULAR:\s*", "", senas).strip().rstrip(".")
            continue
        extras.append(txt)

    # localizado: por defecto false; true solo si explícito.
    localizado = False
    candidates = []
    for node in soup.select(".ficha-sello, .ficha-banner h1, .ficha-contenido, .ficha-nombre"):
        candidates.append(node.get_text(" ", strip=True))
    combined = " ".join(candidates)
    loc = parse_localizado_from_text(combined)
    if loc is not None:
        localizado = loc

    # folio desde slug URL.
    path = urlparse(url_origen).path.rstrip("/")
    slug = path.split("/")[-1] if path else ""
    folio = slug or url_origen

    datos: Dict[str, Any] = {
        "nombre": nombre,
        "edad": edad,
        "visto_por_ultima_vez": visto,
        "ropa_que_viste": ropa,
        "media_filiacion": media,
        "senas_particulares": senas,
        "imagen_url": imagen_url,
        "texto_adicional": "\n".join(extras) if extras else None,
        "localizado": localizado,
        "folio": folio,
        "descripcion_hechos": visto or "",
        "senas": senas or "",
        "url_origen": url_origen,
        "contenido_crudo": "\n".join(contenido_text) if contenido_text else None,
    }
    return datos


def hash_fields_from_datos(datos: Dict[str, Any], localizado: bool) -> Dict[str, Any]:
    return {
        "folio": datos.get("folio"),
        "localizado": localizado,
        "nombre": datos.get("nombre"),
        "edad": datos.get("edad"),
        "descripcion_hechos": datos.get("descripcion_hechos") or "",
        "senas": datos.get("senas") or "",
    }


def insert_into_db(
    datos: Dict[str, Any],
    url_origen: str,
    hashid: str,
    localizado: bool,
    fecha_modificacion: datetime.datetime,
) -> bool:
    try:
        inserted = insert_payload(
            "2101_",
            datos,
            url_origen,
            localizado=localizado,
            hashid=hashid,
        )
        return bool(inserted)
    except Exception as e:
        print(f"❌ Error DB: {e}")
        return False


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
    parser = argparse.ArgumentParser(description="Scraper Oaxaca Personas Desaparecidas")
    parser.add_argument("--no-insert-db", action="store_true", help="Solo scrapeo, sin inserción en DB")
    parser.add_argument("--max-pages", type=int, default=None, help="Límite de páginas del listado")
    parser.add_argument("--sleep-ms", type=int, default=0, help="Pausa extra por ficha en milisegundos")
    args = parser.parse_args()

    insert_db = INSERT_DB_DEFAULT and not args.no_insert_db
    if not insert_db:
        print("ℹ️  Modo solo lectura (--no-insert-db).")

    processed = 0
    inserted = 0
    fecha_modificacion = datetime.datetime.now()

    for detail_url in iter_detail_urls(max_pages=args.max_pages):
        try:
            ficha_html = fetch_html(detail_url)
            datos = parse_ficha_html(ficha_html, detail_url)
            localizado = bool(datos.get("localizado", False))
            hf = hash_fields_from_datos(datos, localizado)
            hashid, _, _ = make_hashid(hf)
            upload_image_to_s3(datos.get("imagen_url"), hashid)
            if insert_db and insert_into_db(datos, detail_url, hashid, localizado, fecha_modificacion):
                inserted += 1
            processed += 1
            if TEST_LIMIT and processed >= TEST_LIMIT:
                break
            if processed % 100 == 0:
                if insert_db:
                    print(f"... procesados={processed}, insertados={inserted}")
                else:
                    print(f"... procesados={processed}")
        except Exception as e:
            print(f"❌ Error procesando ficha {detail_url}: {e}")
        if args.sleep_ms > 0:
            time.sleep(args.sleep_ms / 1000.0)

    if insert_db:
        print(f"✅ Listo. Procesados={processed}, insertados={inserted}")
    else:
        print(f"✅ Listo. Procesados={processed} (sin inserción BD)")


if __name__ == "__main__":
    main()
