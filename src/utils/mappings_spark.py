"""
SciPulse Insights — Mapping explicite de l'index Gold arxiv-papers-enriched

Crée l'index AVANT que Spark n'y écrive, pour éviter une inférence de
type inadaptée par Elasticsearch (ex. primary_category en "text" au lieu
de "keyword", ce qui casserait les agrégations Kibana).

Le mapping est dans mapping_arxiv_papers_enriched.json (réutilisé tel
quel par le DAG Airflow). "dynamic": "strict" : tout champ non prévu
fait échouer l'écriture au lieu de polluer l'index silencieusement.

Différence avec le TP : plus de input() interactif (bloquant dans un
DAG) ; on recrée l'index uniquement avec --recreate.

Usage (Windows, venv) :
    python -m src.utils.mappings_spark
    python -m src.utils.mappings_spark --recreate
"""

import argparse
import json
import logging
import os
from pathlib import Path

from elasticsearch import Elasticsearch

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ES_HOST = os.environ.get("ES_HOST", "http://localhost:9200")
INDEX_NAME = os.environ.get("ES_INDEX", "arxiv-papers-enriched")
MAPPING_FILE = Path(__file__).with_name("mapping_arxiv_papers_enriched.json")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--recreate", action="store_true", help="supprime puis recrée l'index s'il existe")
    args = parser.parse_args()

    es = Elasticsearch(ES_HOST)
    body = json.loads(MAPPING_FILE.read_text(encoding="utf-8"))

    if es.indices.exists(index=INDEX_NAME):
        if not args.recreate:
            logger.info(f"ℹ️  L'index '{INDEX_NAME}' existe déjà — conservé (utilise --recreate pour le recréer)")
            return
        es.indices.delete(index=INDEX_NAME)
        logger.info(f"🗑️  Index '{INDEX_NAME}' supprimé")

    es.indices.create(index=INDEX_NAME, settings=body["settings"], mappings=body["mappings"])
    logger.info(f"✅ Index '{INDEX_NAME}' créé avec mapping explicite ({len(body['mappings']['properties'])} champs)")


if __name__ == "__main__":
    main()
