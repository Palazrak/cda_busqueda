"""
Alerta Amber Nuevo Leon.

Sitio: https://fiscalianl.gob.mx/AlertaAmber
Prefijo hashid: 2002_

Estrategia:
- Listado por API interna paginada: /AlertaAmber/ObtenerAlertasPaginado
- Parseo final desde HTML de detalle: /AlertaAmber/Detalle?idAlerta=<id>
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
from typing import Any, Dict, Iterator, Optional, Tuple

import requests
from bs4 import BeautifulSoup

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from utils.db_utils import insert_payload, make_hashid as make_shared_hashid
from utils.s3_utils import upload_url_if_enabled

BASE_URL = "https://fiscalianl.gob.mx"
LISTING_URL = f"{BASE_URL}/AlertaAmber"
LIST_API = f"{BASE_URL}/AlertaAmber/ObtenerAlertasPaginado"
DETAIL_URL = f"{BASE_URL}/AlertaAmber/Detalle"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; NL-Amber-scraper/1.0)"}
REQUEST_TIMEOUT = 50

INSERT_DB_DEFAULT = True
DEFAULT_PAGE_SIZE = 12

S3_FOLDER = "html/"
TEST_LIMIT = None


def normalize_ws(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_for_hash(value: Any) -> str:
    if value is None:
        return ""
    return normalize_ws(value).lower()


def make_hashid(parsed_data: Dict[str, Any]) -> Tuple[str, str, None]:
    hashid = make_shared_hashid("2002_", parsed_data)
    return hashid, f"{hashid}.pdf", None


def get_idioma_id() -> str:
    html = requests.get(LISTING_URL, headers=HEADERS, timeout=REQUEST_TIMEOUT).text
    m = re.search(r'id="txtIdIdioma"[^>]*value="([^"]+)"', html)
    return m.group(1) if m else "1"


def fetch_list_page(idioma_id: str, page: int, page_size: int) -> list[dict]:
    payload = {
        "Idioma": {"Id": idioma_id},
        "Pagina": page,
        "RegistroMostrar": page_size,
        "OrdenarRegistro": 1,
        "IdCondicion": 0,
    }
    r = requests.post(LIST_API, json=payload, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    return []


def iter_alertas(idioma_id: str, page_size: int, max_pages: Optional[int]) -> Iterator[Tuple[dict, int]]:
    page = 1
    while True:
        if max_pages is not None and page > max_pages:
            break
        items = fetch_list_page(idioma_id, page, page_size)
        print(f"  pagina {page}: {len(items)} registros")
        if not items:
            break
        for item in items:
            yield item, page
        total = int(items[0].get("TotalRegistro") or 0)
        if total > 0 and (page * page_size) >= total:
            break
        page += 1
        time.sleep(0.2)


def parse_localizado_text(text: str) -> Optional[bool]:
    t = normalize_ws(text).lower()
    if "no localizado" in t or "no localizada" in t:
        return False
    if "localizado" in t or "localizada" in t:
        return True
    return None


def parse_detail_html(html: str, fallback_item: dict) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")

    nombre = None
    h = soup.select_one("h5.page-title")
    if h:
        nombre = normalize_ws(h.get_text(" ", strip=True)) or None
    if not nombre:
        alt = soup.select_one(".card-detail--header")
        if alt:
            nombre = normalize_ws(alt.get_text(" ", strip=True)) or None
    if not nombre:
        nombre = normalize_ws(fallback_item.get("NombreCompleto")) or None

    imagen_url = None
    img = soup.select_one("div.detail-photo-container img")
    if img and img.get("src"):
        imagen_url = normalize_ws(img.get("src"))
    if not imagen_url:
        meta = soup.select_one('meta[property="og:image"]')
        if meta and meta.get("content"):
            imagen_url = normalize_ws(meta.get("content"))

    label_map: Dict[str, str] = {}
    for block in soup.select("div.info--content"):
        title = block.select_one(".info--title")
        value = block.select_one(".info--value")
        if not title or not value:
            continue
        k = normalize_ws(title.get_text(" ", strip=True)).lower().replace(":", "")
        v = normalize_ws(value.get_text(" ", strip=True))
        label_map[k] = v

    def get_by_prefix(prefix: str) -> Optional[str]:
        for k, v in label_map.items():
            if k.startswith(prefix):
                return v or None
        return None

    edad = get_by_prefix("edad de la desaparici")
    fecha = get_by_prefix("desapareci")
    visto = get_by_prefix("visto por")
    paradero = get_by_prefix("paradero")

    loc_from_ui = None
    status_nodes = soup.select("p.page-subtitle, .photo--header")
    for n in status_nodes:
        parsed = parse_localizado_text(n.get_text(" ", strip=True))
        if parsed is not None:
            loc_from_ui = parsed
            break
    if loc_from_ui is None:
        cond = normalize_ws(fallback_item.get("DesCondicion")).lower()
        loc_from_ui = not ("no localizado" in cond or "no localizada" in cond)

    return {
        "nombre": nombre,
        "imagen_url": imagen_url,
        "edad": edad or (str(fallback_item.get("EdadDesaparecido")) if fallback_item.get("EdadDesaparecido") is not None else None),
        "fecha_desaparicion": fecha or normalize_ws(fallback_item.get("FechaDesaparecido")) or None,
        "visto_por_ultima_vez": visto or normalize_ws(fallback_item.get("Visto")) or None,
        "paradero": paradero or normalize_ws(fallback_item.get("DesParadero")) or None,
        "localizado": bool(loc_from_ui),
    }


def fetch_and_parse_detail(item: dict) -> Tuple[Dict[str, Any], str]:
    alerta_id = item.get("Id")
    url = f"{DETAIL_URL}?idAlerta={alerta_id}"
    r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    parsed = parse_detail_html(r.text, item)
    return parsed, url


def build_datos(parsed: Dict[str, Any], item: dict) -> Tuple[Dict[str, Any], bool]:
    folio = normalize_ws(item.get("Folio")) or f"idAlerta:{item.get('Id')}"
    descripcion = parsed.get("visto_por_ultima_vez") or parsed.get("paradero") or ""
    datos = {
        "nombre": parsed.get("nombre"),
        "imagen_url": parsed.get("imagen_url"),
        "edad": parsed.get("edad"),
        "fecha_desaparicion": parsed.get("fecha_desaparicion"),
        "visto_por_ultima_vez": parsed.get("visto_por_ultima_vez"),
        "paradero": parsed.get("paradero"),
        "localizado": parsed.get("localizado", False),
        "folio": folio,
        "descripcion_hechos": descripcion,
        "senas": "",
        "fuente": "alerta_amber_nuevo_leon",
        "id_alerta": item.get("Id"),
    }
    return datos, bool(datos["localizado"])


def hash_fields_from_datos(d: Dict[str, Any], localizado: bool) -> Dict[str, Any]:
    return {
        "folio": d.get("folio"),
        "localizado": localizado,
        "nombre": d.get("nombre"),
        "edad": d.get("edad"),
        "descripcion_hechos": d.get("descripcion_hechos") or "",
        "senas": d.get("senas") or "",
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
            "2002_",
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
    parser = argparse.ArgumentParser(description="Scraper Alerta Amber Nuevo Leon")
    parser.add_argument("--no-insert-db", action="store_true", help="Solo scrapeo, sin insertar en BD")
    parser.add_argument("--max-pages", type=int, default=None, help="Maximo de paginas del listado")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE, help="Registros por pagina")
    parser.add_argument("--sleep-ms", type=int, default=0, help="Delay extra por registro")
    args = parser.parse_args()

    insert_db = INSERT_DB_DEFAULT and not args.no_insert_db
    if not insert_db:
        print("ℹ️  Modo solo lectura (--no-insert-db).")

    idioma_id = get_idioma_id()
    print(f"📍 Idioma detectado: {idioma_id}")
    processed = 0
    inserted = 0
    fecha_modificacion = datetime.datetime.now()

    for item, _page in iter_alertas(idioma_id, args.page_size, args.max_pages):
        try:
            parsed, url_origen = fetch_and_parse_detail(item)
        except Exception as e:
            print(f"❌ Error detalle id={item.get('Id')}: {e}")
            continue
        datos, localizado = build_datos(parsed, item)
        hf = hash_fields_from_datos(datos, localizado)
        hashid, _, _ = make_hashid(hf)
        upload_image_to_s3(datos.get("imagen_url"), hashid)
        if insert_db and insert_into_db(datos, url_origen, hashid, localizado, fecha_modificacion):
            inserted += 1
        processed += 1
        if TEST_LIMIT and processed >= TEST_LIMIT:
            break
        if processed % 100 == 0:
            if insert_db:
                print(f"… procesados={processed}, insertados={inserted}")
            else:
                print(f"… procesados={processed}")
        if args.sleep_ms > 0:
            time.sleep(args.sleep_ms / 1000)

    if insert_db:
        print(f"✅ Listo. Procesados={processed}, insertados={inserted}")
    else:
        print(f"✅ Listo. Procesados={processed} (sin insercion BD)")


if __name__ == "__main__":
    main()
