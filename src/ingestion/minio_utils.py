"""
SciPulse — Utilitaires MinIO partagés par les scripts d'ingestion Bronze.

Les scripts d'ingestion tournent soit sur Windows (MINIO_ENDPOINT=
http://localhost:9000, valeur par défaut), soit dans le conteneur Spark
(MINIO_ENDPOINT=http://minio:9000, défini dans docker-compose).
"""

import io
import json
import logging
import os
from datetime import datetime, timezone
from typing import Iterable

import boto3
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# Toutes les zones du data lake Medallion
BUCKETS = [
    "arxiv-raw", "hn-raw", "citations-raw",          # Bronze
    "arxiv-clean", "hn-clean", "citations-clean",    # Silver
    "results",                                        # résultats Spark (préparation Gold)
]


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    )


def ensure_bucket(s3, bucket: str) -> None:
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)
        logger.info(f"📦 Bucket '{bucket}' créé")


def timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_jsonl(s3, bucket: str, key: str, records: Iterable[dict]) -> int:
    """Écrit des enregistrements en JSON Lines, tels quels (principe Bronze)."""
    buffer = io.BytesIO()
    n = 0
    for record in records:
        buffer.write((json.dumps(record, ensure_ascii=False) + "\n").encode("utf-8"))
        n += 1
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    logger.info(f"✅ {n:,} enregistrements écrits vers s3://{bucket}/{key}")
    return n


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    client = get_s3_client()
    for name in BUCKETS:
        ensure_bucket(client, name)
    logger.info("✅ Tous les buckets Medallion sont prêts : " + ", ".join(BUCKETS))
