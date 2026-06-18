import mimetypes
import os
from typing import Mapping, Optional
from urllib.parse import urlparse


TRUE_VALUES = {"1", "true", "yes", "on"}


def s3_upload_enabled() -> bool:
    return os.getenv("S3_UPLOAD_ENABLED", "false").strip().lower() in TRUE_VALUES


def _require_s3_env() -> tuple[str, str]:
    bucket = os.getenv("S3_BUCKET", "").strip()
    region = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "")).strip()
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "").strip()
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "").strip()

    if not all([bucket, region, access_key, secret_key]):
        raise RuntimeError(
            "S3 upload enabled but AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, "
            "AWS_REGION/AWS_DEFAULT_REGION, and S3_BUCKET are not fully configured"
        )
    return bucket, region


def upload_bytes_if_enabled(
    content: bytes,
    key: str,
    *,
    content_type: Optional[str] = None,
) -> Optional[str]:
    if not s3_upload_enabled():
        return None

    bucket, region = _require_s3_env()
    import boto3

    client = boto3.client("s3", region_name=region)
    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type
    client.put_object(Bucket=bucket, Key=key, Body=content, **extra_args)
    return f"s3://{bucket}/{key}"


def upload_url_if_enabled(
    url: str,
    key: str,
    *,
    headers: Optional[Mapping[str, str]] = None,
    timeout: int = 30,
    content_type: Optional[str] = None,
) -> Optional[str]:
    if not s3_upload_enabled():
        return None

    import requests

    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()

    guessed_type = content_type or response.headers.get("content-type")
    if not guessed_type:
        guessed_type, _ = mimetypes.guess_type(urlparse(url).path)

    return upload_bytes_if_enabled(
        response.content,
        key,
        content_type=guessed_type,
    )
