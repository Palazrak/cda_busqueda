"""
Registro Estatal de Personas Desaparecidas (REPD Jalisco — versión pública).

- Listado y fichas vía API JSON: https://repd.jalisco.gob.mx/api/v1/version_publica/
- UI: https://version-publica-repd.jalisco.gob.mx/cedulas-de-busqueda
- Prefijo hash_id: 1601_
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
from typing import Any, Dict, List, Optional, Tuple

import requests

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from utils.db_utils import insert_payload, make_hashid as make_shared_hashid
from utils.s3_utils import upload_url_if_enabled

# -------------------- Config --------------------
API_BASE = "https://repd.jalisco.gob.mx/api/v1/version_publica"
PUBLIC_UI_BASE = "https://version-publica-repd.jalisco.gob.mx"
CEDULAS_PATH = "repd-version-publica-cedulas-busqueda"
ESTADOS_PATH = "repd-version-publica-get-estados-with-cedulas"

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; REPD-scraper/1.0)"}

INSERT_DB_DEFAULT = True
PAGE_LIMIT_DEFAULT = 12
REQUEST_TIMEOUT = 45

S3_FOLDER = "html/"
TEST_LIMIT = None


# -------------------- Utilidades --------------------
def normalize_ws(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalize_for_hash(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip().lower()


def make_hashid(parsed_data: Dict[str, Any]) -> Tuple[str, str, None]:
    """Misma lógica que serial_hasvistoa_michoacan.py; prefijo 1601_."""
    hashid = make_shared_hashid("1601_", parsed_data)
    return hashid, f"{hashid}.pdf", None


def cedula_url_publica(id_cedula: str) -> str:
    return f"{PUBLIC_UI_BASE}/cedulas-de-busqueda?id_cedula_busqueda={id_cedula}"


# -------------------- API --------------------
def api_get(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = f"{API_BASE}/{path.rstrip('/')}/"
    r = requests.get(url, headers=HEADERS, params=params or {}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def fetch_estados_con_cedulas() -> Dict[str, Any]:
    return api_get(ESTADOS_PATH)


def fetch_cedulas_page(estado_id: int, page: int, limit: int = PAGE_LIMIT_DEFAULT) -> Dict[str, Any]:
    return api_get(
        CEDULAS_PATH,
        {"estado": estado_id, "page": page, "limit": limit},
    )


# -------------------- Mapeo API -> datos_json --------------------
def _vestimenta_from_api(items: Any) -> Optional[str]:
    if not items:
        return None
    lines: List[str] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        grupo = normalize_ws(str(it.get("grupo_prenda") or ""))
        prenda = normalize_ws(str(it.get("prenda") or ""))
        color = normalize_ws(str(it.get("color") or ""))
        material = normalize_ws(str(it.get("material") or ""))
        talla = normalize_ws(str(it.get("talla") or ""))
        tipo = normalize_ws(str(it.get("tipo") or ""))
        desc = normalize_ws(str(it.get("descripcion") or ""))
        chunk = " | ".join(
            x for x in [grupo, prenda, color, material, talla, tipo, desc] if x
        )
        if chunk:
            lines.append(chunk)
    return "\n".join(lines) if lines else None


def _senas_from_api(items: Any) -> Optional[str]:
    if not items:
        return None
    lines: List[str] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        tipo = normalize_ws(str(it.get("tipo_sena") or ""))
        desc = normalize_ws(str(it.get("descripcion") or ""))
        parte = normalize_ws(str(it.get("parte_cuerpo") or ""))
        chunk = " | ".join(x for x in [tipo, parte, desc] if x and x != "None")
        if chunk:
            lines.append(chunk)
    return "\n".join(lines) if lines else None


def _lugar_from_api(item: Dict[str, Any]) -> Optional[str]:
    parts = [
        normalize_ws(str(item.get("colonia") or "")),
        normalize_ws(str(item.get("municipio") or "")),
        normalize_ws(str(item.get("estado") or "")),
    ]
    parts = [p for p in parts if p]
    return ", ".join(parts) if parts else None


def infer_localizado_con_vida(item: Dict[str, Any]) -> Tuple[bool, bool]:
    """
    Reglas alineadas al texto de la ficha:
    - Persona aún desaparecida -> localizado=False, con_vida=False
    - Localizada con vida -> localizado=True, con_vida=True
    - Localizada sin vida / otro -> localizado=True, con_vida=False
    """
    estatus = normalize_ws(str(item.get("estatus_persona_desaparecida") or "")).upper()
    condicion = normalize_ws(str(item.get("condicion_localizacion") or "")).upper()

    desaparecida = "DESAPARECID" in estatus and "LOCALIZ" not in estatus
    localizada = "LOCALIZ" in estatus

    if desaparecida:
        return False, False
    if localizada:
        con_vida = "CON VIDA" in condicion or "CON VIDA" in estatus
        return True, con_vida
    # Fallback conservador
    return False, False


def build_datos_from_api(item: Dict[str, Any]) -> Dict[str, Any]:
    localizado, con_vida = infer_localizado_con_vida(item)
    edad = item.get("edad_momento_desaparicion")
    edad_str = None
    if edad is not None:
        edad_str = str(edad) if not isinstance(edad, str) else edad

    estatura = item.get("estatura")
    estatura_str: Optional[str]
    if estatura is None:
        estatura_str = None
    else:
        estatura_str = f"{estatura} m" if isinstance(estatura, (int, float)) else str(estatura)

    cid = item.get("id_cedula_busqueda")
    imagen = item.get("ruta_foto")
    if imagen is not None:
        imagen = normalize_ws(str(imagen)) or None

    senas_str = _senas_from_api(item.get("descripcion_sena_particular"))
    lugar_str = _lugar_from_api(item)

    datos: Dict[str, Any] = {
        # Alineado con otros serial (p.ej. Coahuila): origen, folio, localizado e imagen en JSON.
        "estado_alerta": "REPD Jalisco",
        "folio": str(cid) if cid is not None else None,
        "localizado": localizado,
        "imagen_url": imagen,
        "con_vida": con_vida,
        "nombre": item.get("nombre_completo"),
        "edad_al_desaparecer": edad_str,
        "edad": edad_str,
        "fecha_desaparicion": item.get("fecha_desaparicion"),
        "descripcion_hechos": lugar_str,
        "lugar": lugar_str,
        "colonia": item.get("colonia"),
        "municipio": item.get("municipio"),
        "estado": item.get("estado"),
        "sexo": item.get("sexo"),
        "genero": item.get("genero"),
        "nacionalidad": item.get("nacionalidad"),
        "estatus_persona_desaparecida": item.get("estatus_persona_desaparecida"),
        "condicion_localizacion": item.get("condicion_localizacion"),
        "autorizacion_informacion_publica": item.get("autorizacion_informacion_publica"),
        "complexión": item.get("complexion"),
        "estatura": estatura_str,
        "tez": item.get("tez"),
        "cabello": item.get("cabello"),
        "color_ojos": item.get("ojos_color"),
        "vestimenta": _vestimenta_from_api(item.get("descripcion_vestimenta")),
        "senas_particulares": senas_str,
        "senas": senas_str or "",
    }
    return datos, localizado


def hash_fields_from_api(item: Dict[str, Any], datos: Dict[str, Any], localizado: bool) -> Dict[str, Any]:
    folio = str(item.get("id_cedula_busqueda") or "")
    return {
        "folio": folio,
        "localizado": localizado,
        "nombre": datos.get("nombre"),
        "edad": datos.get("edad_al_desaparecer"),
        "descripcion_hechos": datos.get("descripcion_hechos") or datos.get("lugar") or "",
        "senas": datos.get("senas") or datos.get("senas_particulares") or "",
    }


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
            "1601_",
            datos,
            url_origen,
            localizado=localizado,
            hashid=hashid,
        )
        print(f"✅ Insertados en DB: {inserted} hashid={hashid}")
        return bool(inserted)
    except Exception as e:
        print(f"❌ Error DB: {e}")
        return False


# -------------------- Scraping principal --------------------
def iter_all_cedulas(limit: int, max_pages_per_estado: Optional[int] = None):
    raw = fetch_estados_con_cedulas()
    estados: List[Tuple[str, int]] = []
    for nombre, info in raw.items():
        if not isinstance(info, dict):
            continue
        eid = info.get("estado_id")
        if eid is None:
            continue
        try:
            estados.append((str(nombre), int(eid)))
        except (TypeError, ValueError):
            continue

    print(f"📍 Estados con cédulas: {len(estados)} → {estados}")

    for nombre_estado, estado_id in estados:
        page = 1
        total_pages = None
        while True:
            if max_pages_per_estado is not None and page > max_pages_per_estado:
                break
            try:
                data = fetch_cedulas_page(estado_id, page, limit=limit)
            except Exception as e:
                print(f"❌ estado={nombre_estado} page={page}: {e}")
                break
            total_pages = int(data.get("total_pages") or 1)
            count = int(data.get("count") or 0)
            results = data.get("results") or []
            print(f"  {nombre_estado} página {page}/{total_pages} ({len(results)} registros, total {count})")
            for item in results:
                yield item
            if page >= total_pages or not results:
                break
            page += 1
            time.sleep(0.25)


def upload_image_to_s3(url, hashid):
    if not url or url.startswith("data:"):
        return None
    try:
        if not url.startswith("http"):
            url = PUBLIC_UI_BASE + "/" + url.lstrip("/")
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
    parser = argparse.ArgumentParser(description="Scraper REPD Jalisco (API JSON)")
    parser.add_argument(
        "--no-insert-db",
        action="store_true",
        help="Solo consultar la API sin escribir en Postgres (por defecto sí inserta)",
    )
    parser.add_argument("--limit", type=int, default=PAGE_LIMIT_DEFAULT, help="Registros por página API")
    parser.add_argument(
        "--max-pages-per-estado",
        type=int,
        default=None,
        help="Solo las primeras N páginas por estado (pruebas)",
    )
    parser.add_argument("--max-records", type=int, default=None, help="Limitar registros procesados en esta corrida")
    args = parser.parse_args()

    insert_db = INSERT_DB_DEFAULT and not args.no_insert_db
    if not insert_db:
        print("ℹ️  Modo solo lectura (--no-insert-db): no se escribe en Postgres.\n")
    fecha_modificacion = datetime.datetime.now()
    processed = 0
    inserted = 0

    for item in iter_all_cedulas(limit=args.limit, max_pages_per_estado=args.max_pages_per_estado):
        datos, localizado = build_datos_from_api(item)
        hf = hash_fields_from_api(item, datos, localizado)
        hashid, _, _ = make_hashid(hf)
        upload_image_to_s3(datos.get("imagen_url"), hashid)
        cid = str(item.get("id_cedula_busqueda") or "")
        url = cedula_url_publica(cid)
        if insert_db:
            if insert_into_db(datos, url, hashid, localizado, fecha_modificacion):
                inserted += 1
        processed += 1
        limit = args.max_records or TEST_LIMIT
        if limit and processed >= limit:
            break
        if processed % 200 == 0:
            if insert_db:
                print(f"… procesados {processed}, insertados {inserted}")
            else:
                print(f"… procesados {processed}")

    if insert_db:
        print(f"✅ Listo. Procesados: {processed}, insertados: {inserted}")
    else:
        print(f"✅ Listo. Procesados: {processed} (sin inserción en BD)")


if __name__ == "__main__":
    main()
