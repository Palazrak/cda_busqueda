import datetime
import hashlib
import json
import os
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Optional


HASH_FIELDS = (
    "folio",
    "localizado",
    "nombre",
    "edad",
    "descripcion_hechos",
    "senas",
)

COMMON_FIELDS = ("nombre", "folio", "estado_alerta", "descripcion_hechos", "senas")

FIELD_ALIASES = {
    "folio": ("folio", "reporte_num", "reporte", "numero_reporte", "id", "id_registro"),
    "descripcion_hechos": (
        "descripcion_hechos",
        "resumen_hechos",
        "hechos",
        "descripcion",
        "descripcion_del_hecho",
    ),
    "senas": (
        "senas",
        "senas_particulares",
        "señas_particulares",
        "senas_particular",
        "media_filiacion",
    ),
}


@dataclass(frozen=True)
class DesaparecidoRecord:
    fecha_extraccion: datetime.date
    url_origen: str
    localizado: Optional[bool]
    hashid: str
    datos: Optional[Dict[str, Any]]


def normalize_for_hash(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip().lower()


def normalize_common_payload(
    datos: Dict[str, Any],
    *,
    localizado: Optional[bool] = None,
) -> Dict[str, Any]:
    normalized = dict(datos or {})
    for canonical, aliases in FIELD_ALIASES.items():
        if normalized.get(canonical) in (None, ""):
            for alias in aliases:
                value = normalized.get(alias)
                if value not in (None, ""):
                    normalized[canonical] = value
                    break

    for field in COMMON_FIELDS:
        normalized.setdefault(field, None)

    if localizado is None:
        localizado = normalized.get("localizado")
    if localizado is None:
        localizado = False
    normalized["localizado"] = localizado
    return normalized


def make_hashid(prefix: str, datos: Dict[str, Any]) -> str:
    normalized = normalize_common_payload(datos)
    parts = [normalize_for_hash(normalized.get(field)) for field in HASH_FIELDS]
    digest = hashlib.sha256("||".join(parts).encode("utf-8")).hexdigest()[:10]
    return f"{prefix}{digest}"


def build_record(
    prefix: str,
    datos: Dict[str, Any],
    url_origen: str,
    *,
    localizado: Optional[bool] = None,
    hashid: Optional[str] = None,
    fecha_extraccion: Optional[datetime.date] = None,
) -> DesaparecidoRecord:
    normalized_datos = normalize_common_payload(datos, localizado=localizado)
    if localizado is None:
        localizado = normalized_datos.get("localizado")
    if localizado is None:
        localizado = False

    return DesaparecidoRecord(
        fecha_extraccion=fecha_extraccion or datetime.date.today(),
        url_origen=url_origen,
        localizado=localizado,
        hashid=hashid or make_hashid(prefix, normalized_datos),
        datos=normalized_datos,
    )


def insert_payload(
    prefix: str,
    datos: Dict[str, Any],
    url_origen: str,
    *,
    localizado: Optional[bool] = None,
    hashid: Optional[str] = None,
    connect_func: Optional[Callable[[], Any]] = None,
) -> int:
    return insert_records(
        [
            build_record(
                prefix,
                datos,
                url_origen,
                localizado=localizado,
                hashid=hashid,
            )
        ],
        connect_func=connect_func,
    )


def get_db_config() -> Dict[str, str]:
    return {
        "dbname": os.getenv("DB_NAME", "cda_busqueda"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "mysecretpassword"),
        "host": os.getenv("DB_HOST", "postgres"),
        "port": os.getenv("DB_PORT", "5432"),
    }


def get_connection():
    import psycopg2

    return psycopg2.connect(**get_db_config())


def insert_records(
    records: Iterable[DesaparecidoRecord],
    connect_func: Optional[Callable[[], Any]] = None,
) -> int:
    records = list(records)
    if not records:
        return 0

    conn = None
    cur = None
    inserted = 0
    try:
        conn = (connect_func or get_connection)()
        cur = conn.cursor()
        for record in records:
            cur.execute(
                """
                INSERT INTO public.desaparecidos
                  (fecha_extraccion, url_origen, localizado, hashid, datos)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (hashid, localizado) DO NOTHING
                """,
                (
                    record.fecha_extraccion,
                    record.url_origen,
                    record.localizado,
                    record.hashid,
                    json.dumps(record.datos, ensure_ascii=False) if record.datos is not None else None,
                ),
            )
            inserted += cur.rowcount or 0
        conn.commit()
        return inserted
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
