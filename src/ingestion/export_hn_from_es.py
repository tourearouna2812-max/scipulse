"""
SciPulse Insights — Export de l'index hn-items (module 1) vers la couche Bronze

Copie les stories de l'index Elasticsearch hn-items (alimenté en continu
par hn_poller.py) qui possèdent une URL, telles quelles, vers
s3://hn-raw/es_export/. Elles sont ensuite fusionnées en Silver avec les
stories récupérées par fetch_hn_arxiv.py.

Différence avec le TP J2 : on ne filtre pas sur un champ "arxiv_id"
(qui n'existe pas forcément dans ton index) ; l'identifiant ArXiv est
extrait de l'URL en Silver, de la même façon pour les deux sources HN.

Usage :
    python -m src.ingestion.export_hn_from_es
"""

from __future__ import annotations

import logging
import os

from elasticsearch import Elasticsearch, helpers

from src.ingestion.minio_utils import ensure_bucket, get_s3_client, timestamp, write_jsonl

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ES_HOST = os.environ.get("ES_HOST", "http://localhost:9200")
ES_INDEX = "hn-items"
BUCKET = "hn-raw"


def export_items_with_url() -> list[dict]:
    es = Elasticsearch(ES_HOST)
    query = {"query": {"exists": {"field": "url"}}}
    return [hit["_source"] for hit in helpers.scan(es, index=ES_INDEX, query=query, size=500)]


def main():
    s3 = get_s3_client()
    ensure_bucket(s3, BUCKET)

    logger.info(f"📤 Export des items '{ES_INDEX}' possédant une URL...")
    items = export_items_with_url()
    logger.info(f"   {len(items):,} items trouvés")
    write_jsonl(s3, BUCKET, f"es_export/hn_items_{timestamp()}.jsonl", items)


if __name__ == "__main__":
    main()
