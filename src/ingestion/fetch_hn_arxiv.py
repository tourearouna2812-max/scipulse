"""
SciPulse Insights — Ingestion Bronze Hacker News : stories pointant vers arXiv

Le poller temps réel du module 1 (hn_poller.py) ne voit que les items
publiés depuis son lancement : quelques centaines, presque tous des
commentaires sans lien arXiv. Pour mesurer l'écho d'articles publiés sur
plusieurs années, on complète par l'API publique de recherche Hacker News
(hn.algolia.com, gratuite, sans clé), en ne demandant que les stories
dont l'URL contient "arxiv".

Les résultats sont écrits TELS QUELS (Bronze) dans s3://hn-raw/algolia/.
L'extraction de l'identifiant ArXiv depuis l'URL se fait en Silver.

Usage :
    python -m src.ingestion.fetch_hn_arxiv --max-items 20000
"""

from __future__ import annotations

import argparse
import logging
import time

import requests

from src.ingestion.minio_utils import ensure_bucket, get_s3_client, timestamp, write_jsonl

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ALGOLIA_URL = "https://hn.algolia.com/api/v1/search_by_date"
BUCKET = "hn-raw"
HITS_PER_PAGE = 1000


def fetch_arxiv_stories(max_items: int) -> list[dict]:
    """Pagine à rebours dans le temps (filtre created_at_i < borne) : l'API plafonne
    la pagination classique à 1 000 résultats par requête."""
    session = requests.Session()
    hits_total, vus = [], set()
    borne = int(time.time())

    while len(hits_total) < max_items:
        params = {
            "query": "arxiv",
            "tags": "story",
            "restrictSearchableAttributes": "url",
            "hitsPerPage": HITS_PER_PAGE,
            "numericFilters": f"created_at_i<{borne}",
        }
        response = session.get(ALGOLIA_URL, params=params, timeout=60)
        response.raise_for_status()
        hits = response.json().get("hits", [])
        if not hits:
            break

        nouveaux = [h for h in hits if h["objectID"] not in vus]
        vus.update(h["objectID"] for h in nouveaux)
        hits_total.extend(nouveaux)

        nouvelle_borne = min(h["created_at_i"] for h in hits)
        if nouvelle_borne >= borne:     # sécurité anti-boucle infinie
            break
        borne = nouvelle_borne
        logger.info(f"   {len(hits_total):,} stories — jusqu'au "
                    f"{time.strftime('%Y-%m-%d', time.gmtime(borne))}")
        time.sleep(0.2)

    return hits_total[:max_items]


def main():
    parser = argparse.ArgumentParser(description="Ingestion Bronze HN (stories arXiv)")
    parser.add_argument("--max-items", type=int, default=20_000)
    args = parser.parse_args()

    s3 = get_s3_client()
    ensure_bucket(s3, BUCKET)

    logger.info("📡 Récupération des stories Hacker News pointant vers arXiv...")
    stories = fetch_arxiv_stories(args.max_items)
    write_jsonl(s3, BUCKET, f"algolia/hn_arxiv_stories_{timestamp()}.jsonl", stories)


if __name__ == "__main__":
    main()
