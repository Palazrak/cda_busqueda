import os
from typing import Mapping, Optional


class AwsConfigError(RuntimeError):
    """Raised when an AWS-backed feature is requested without required config."""


def get_rekognition_config(env: Optional[Mapping[str, str]] = None) -> dict:
    values = env or os.environ
    access_key = values.get("AWS_ACCESS_KEY_ID", "").strip()
    secret_key = values.get("AWS_SECRET_ACCESS_KEY", "").strip()
    region = (
        values.get("AWS_REGION", "")
        or values.get("AWS_DEFAULT_REGION", "")
        or "us-east-1"
    ).strip()

    if not access_key or not secret_key:
        raise AwsConfigError(
            "AWS Rekognition is not configured. Set AWS_ACCESS_KEY_ID, "
            "AWS_SECRET_ACCESS_KEY, and AWS_REGION before using advanced "
            "face-search."
        )

    return {
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
        "region_name": region,
    }
