"""
Personas Desaparecidas: Fiscalia General del Estado de Guanajuato.

Scraper basado en la API usada por el carrusel del portal:
- https://portal.fgeguanajuato.gob.mx/PortalWebEstatal/PersonasDesaparecidas/Formularios/index.aspx
- JS referencia:
  - /PortalWebEstatal/PersonasDesaparecidas/Javascript/jsIndex.js
  - /PortalWebEstatal/PersonasDesaparecidas/Javascript/jsModalFichasProgresion.js

Endpoints principales:
- /personasDesaparecidas/listaCompleta?page={n}  (Fiscalia + Amber + Alba)
- /personasDesaparecidas/listaIA?page={n}        (fichas progresion edad)

Prefijo hash_id: 1301_
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
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

import requests

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from utils.db_utils import insert_payload, make_hashid as make_shared_hashid
from utils.s3_utils import upload_url_if_enabled

# -------------------- Config --------------------
API_BASE = "https://wsc.fgeguanajuato.gob.mx/pw-recursos/api/v1"
PORTAL_URL = "https://portal.fgeguanajuato.gob.mx/PortalWebEstatal/PersonasDesaparecidas/Formularios/index.aspx"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; Guanajuato-scraper/1.0)"}
REQUEST_TIMEOUT = 45

INSERT_DB_DEFAULT = True

S3_FOLDER = "html/"
TEST_LIMIT = None

DEFAULT_LIMIT = 10  # La API pagina en bloques de 10.


# -------------------- Helpers --------------------
def normalize_ws(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_for_hash(value: Any) -> str:
    if value is None:
        return ""
    return normalize_ws(value).lower()


def make_hashid(parsed_data: Dict[str, Any]) -> Tuple[str, str, None]:
    """Misma logica base que Michoacan; prefijo 1301_."""
    hashid = make_shared_hashid("1301_", parsed_data)
    return hashid, f"{hashid}.pdf", None


def api_get(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = f"{API_BASE}/{path.lstrip('/')}"
    r = requests.get(url, headers=HEADERS, params=params or {}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def image_url_for_item(item: Dict[str, Any], source: str) -> Optional[str]:
    id_persona = item.get("idPersona")
    if id_persona is None:
        return None
    endpoint = "imagenIADesaparecido" if source == "listaIA" else "imagenDesaparecido"
    return f"{API_BASE}/personasDesaparecidas/{endpoint}/{id_persona}"


def infer_source_name(tipo_lista: Any) -> str:
    if tipo_lista == 1:
        return "alba"
    if tipo_lista == 2:
        return "fiscalia"
    if tipo_lista == 3:
        return "amber"
    if tipo_lista == 4:
        return "otra"
    return "desconocida"


def infer_localizado(item: Dict[str, Any]) -> bool:
    """
    Regla pedida:
    - usar campo del API si existe,
    - fallback False.
    """
    for key in ("localizado", "esLocalizado", "estatusLocalizacion"):
        if key in item and item.get(key) is not None:
            val = item.get(key)
            if isinstance(val, bool):
                return val
            t = normalize_ws(val).lower()
            if t in ("1", "true", "si", "sí", "localizada", "localizado"):
                return True
            if t in ("0", "false", "no", "desaparecida", "desaparecido"):
                return False
    return False


def build_datos(item: Dict[str, Any], source: str) -> Tuple[Dict[str, Any], bool]:
    localizado = infer_localizado(item)
    nombre = normalize_ws(item.get("nombreCompleto")) or None
    edad_raw = item.get("edad")
    edad = None if edad_raw is None else normalize_ws(edad_raw)
    senas = normalize_ws(item.get("senasParticulares")) or None
    lugar = normalize_ws(item.get("lugarHechos")) or None
    fecha = normalize_ws(item.get("fechaDesaparicion")) or None
    imagen_url = image_url_for_item(item, source)
    folio = normalize_ws(item.get("folioCarpeta")) or None
    id_persona = item.get("idPersona")
    tipo_lista = item.get("tipoLista")

    datos: Dict[str, Any] = {
        "nombre": nombre,
        "imagen_url": imagen_url,
        "edad": edad,
        "senasParticulares": senas,
        "lugarHechos": lugar,
        "fechaDesaparicion": fecha,
        # Utiles para trazabilidad/paridad con otros scrapers:
        "localizado": localizado,
        "fuente": source,
        "subfuente": infer_source_name(tipo_lista),
        "tipoLista": tipo_lista,
        "idPersona": id_persona,
        "folio": folio,
        # Campos auxiliares de hash:
        "descripcion_hechos": lugar or "",
        "senas": senas or "",
    }
    return datos, localizado


def hash_fields_from_item(item: Dict[str, Any], datos: Dict[str, Any], localizado: bool) -> Dict[str, Any]:
    folio = normalize_ws(item.get("folioCarpeta"))
    if not folio:
        # Fallback estable si no hay folio de carpeta.
        folio = f"idPersona:{normalize_ws(item.get('idPersona'))}"
    return {
        "folio": folio,
        "localizado": localizado,
        "nombre": datos.get("nombre"),
        "edad": datos.get("edad"),
        "descripcion_hechos": datos.get("descripcion_hechos") or "",
        "senas": datos.get("senas") or "",
    }


def iter_paginated(path: str, max_pages: Optional[int] = None) -> Iterator[Tuple[Dict[str, Any], int]]:
    page = 0
    while True:
        if max_pages is not None and page >= max_pages:
            break
        try:
            payload = api_get(path, {"page": page})
        except requests.RequestException as exc:
            print(f"❌ Error consultando {path} page {page}: {exc}")
            break

        if not isinstance(payload, dict):
            break
        content = payload.get("content") or []
        total_pages = int(payload.get("totalPages") or 1)
        print(f"  {path} page {page + 1}/{total_pages} ({len(content)} registros)")
        for item in content:
            if isinstance(item, dict):
                yield item, page
        if (page + 1) >= total_pages or not content:
            break
        page += 1
        time.sleep(0.15)


def iter_all_records(max_pages: Optional[int] = None) -> Iterator[Tuple[Dict[str, Any], str, int]]:
    """
    Fusiona:
    - listaCompleta (principal: fiscalia/amber/alba)
    - listaIA (carrusel progresion)
    Dedupe por idPersona+tipoLista para evitar duplicados.
    """
    seen: Set[str] = set()
    sources = ("personasDesaparecidas/listaCompleta", "personasDesaparecidas/listaIA")
    for path in sources:
        source_name = "listaIA" if path.endswith("listaIA") else "listaCompleta"
        for item, page in iter_paginated(path, max_pages=max_pages):
            key = f"{item.get('idPersona')}::{item.get('tipoLista')}"
            if key in seen:
                continue
            seen.add(key)
            yield item, source_name, page


def make_url_origen(source_name: str, page: int, item: Dict[str, Any]) -> str:
    id_persona = item.get("idPersona")
    return f"{API_BASE}/personasDesaparecidas/{source_name}?page={page}&idPersona={id_persona}"


# -------------------- DB --------------------
def insert_into_db(
    datos: Dict[str, Any],
    url_origen: str,
    hashid: str,
    localizado: bool,
    fecha_modificacion: datetime.datetime,
) -> bool:
    try:
        inserted = insert_payload(
            "1301_",
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
        if not url.startswith("http"):
            url = API_BASE + "/" + url.lstrip("/")
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
    parser = argparse.ArgumentParser(description="Scraper Guanajuato por carrusel/API")
    parser.add_argument(
        "--no-insert-db",
        action="store_true",
        help="Solo consumir API sin insertar en PostgreSQL",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Limitar paginas por fuente para pruebas (0-index interno)",
    )
    parser.add_argument(
        "--sleep-ms",
        type=int,
        default=0,
        help="Delay extra por registro (ms) para depuracion",
    )
    args = parser.parse_args()

    insert_db = INSERT_DB_DEFAULT and not args.no_insert_db
    if not insert_db:
        print("ℹ️  Modo solo lectura (--no-insert-db): no se escribe en BD.")

    processed = 0
    inserted = 0
    fecha_modificacion = datetime.datetime.now()

    print(f"📍 Iniciando scrape: {PORTAL_URL}")
    for item, source_name, page in iter_all_records(max_pages=args.max_pages):
        datos, localizado = build_datos(item, source_name)
        hf = hash_fields_from_item(item, datos, localizado)
        hashid, _, _ = make_hashid(hf)
        url_origen = make_url_origen(source_name, page, item)
        upload_image_to_s3(datos.get("imagen_url"), hashid)

        if insert_db and insert_into_db(datos, url_origen, hashid, localizado, fecha_modificacion):
            inserted += 1
        processed += 1
        if TEST_LIMIT and processed >= TEST_LIMIT:
            break

        if processed % 200 == 0:
            if insert_db:
                print(f"... procesados={processed} insertados={inserted}")
            else:
                print(f"... procesados={processed}")
        if args.sleep_ms > 0:
            time.sleep(args.sleep_ms / 1000.0)

    if insert_db:
        print(f"✅ Listo. Procesados={processed}, insertados={inserted}")
    else:
        print(f"✅ Listo. Procesados={processed} (sin insercion BD)")


if __name__ == "__main__":
    main()
