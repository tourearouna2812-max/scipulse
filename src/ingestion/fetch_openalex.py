"""
SciPulse Insights — Ingestion Bronze de la source de citations (OpenAlex)

Écrit des œuvres OpenAlex brutes, en JSON Lines, dans
s3://citations-raw/openalex/ (couche Bronze, aucune transformation).

Deux modes de récupération :

  --mode arxiv-ids (recommandé)
      Lit la liste d'identifiants ArXiv produite par la couche Silver
      (s3://arxiv-clean/arxiv_ids_csv/, cf. build_silver.py) et interroge
      OpenAlex par DOI ArXiv (10.48550/arxiv.<id>), par lots de 50 DOIs
      en un seul appel (filtre OR "doi:a|b|c"). On ne récupère donc QUE
      des œuvres qui correspondent à nos articles : le taux de matching
      n'est plus limité par la taille d'un extrait aléatoire.

  --mode field
      Mode du TP : parcourt par curseur les œuvres "Computer Science"
      hébergées sur arXiv (source OpenAlex arXiv, résolue dynamiquement).
      Utile si la couche Silver ArXiv n'existe pas encore.

Budget : avec une clé gratuite (1 $/jour ≈ 10 000 appels list+filter),
20 000 articles en mode arxiv-ids = 400 appels seulement.

Usage :
    python -m src.ingestion.fetch_openalex --mode arxiv-ids --limit 20000
    python -m src.ingestion.fetch_openalex --mode field --max-pages 50
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import os
import time

import requests

from src.ingestion.minio_utils import ensure_bucket, get_s3_client, timestamp, write_jsonl

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

OPENALEX_URL = "https://api.openalex.org"
API_KEY = os.environ.get("OPENALEX_API_KEY", "")
MAILTO = os.environ.get("OPENALEX_MAILTO", "")
BUCKET = "citations-raw"
PREFIX = "openalex"

PER_PAGE = 100            # maximum autorisé par l'API
DOIS_PAR_APPEL = 50       # l'API accepte jusqu'à 100 valeurs OR ; 50 garde des URLs raisonnables
ARXIV_SOURCE_FALLBACK = "S4306400194"   # "arXiv (Cornell University)" dans OpenAlex

# On ne demande que les champs utiles (cf. question J1 sur select= / _source filtering)
SELECT = ",".join([
    "id", "doi", "display_name", "publication_year", "publication_date",
    "cited_by_count", "counts_by_year", "primary_topic", "locations", "ids",
])


class OpenAlexClient:
    def __init__(self):
        self.session = requests.Session()
        self.calls = 0

    def get(self, path: str, params: dict) -> dict:
        params = dict(params)
        if API_KEY:
            params["api_key"] = API_KEY
        if MAILTO:
            params["mailto"] = MAILTO

        for tentative in range(5):
            response = self.session.get(f"{OPENALEX_URL}{path}", params=params, timeout=60)
            self.calls += 1
            if response.status_code == 429 or response.status_code >= 500:
                attente = 2 ** tentative
                logger.warning(f"   HTTP {response.status_code} — nouvelle tentative dans {attente}s")
                time.sleep(attente)
                continue
            response.raise_for_status()
            time.sleep(0.1)   # courtoisie envers l'API
            return response.json()
        raise RuntimeError(f"OpenAlex indisponible après 5 tentatives : {path}")

    def resolve_arxiv_source_id(self) -> str:
        """Retrouve l'identifiant de la source 'arXiv' dans OpenAlex (au lieu de le coder en dur)."""
        try:
            data = self.get("/sources", {"search": "arxiv", "per_page": 10,
                                         "select": "id,display_name,works_count"})
            candidats = [s for s in data["results"] if s["display_name"].lower().startswith("arxiv")]
            if candidats:
                meilleur = max(candidats, key=lambda s: s.get("works_count") or 0)
                return meilleur["id"].rsplit("/", 1)[-1]
        except Exception as exc:   # la valeur de repli suffit
            logger.warning(f"   Résolution de la source arXiv impossible ({exc}), repli sur {ARXIV_SOURCE_FALLBACK}")
        return ARXIV_SOURCE_FALLBACK


def read_arxiv_ids_from_silver(s3, limit: int, category_prefix: str) -> list[str]:
    """Lit le CSV d'IDs écrit par build_silver.py (arxiv-clean/arxiv_ids_csv/part-*.csv)."""
    objets = s3.list_objects_v2(Bucket="arxiv-clean", Prefix="arxiv_ids_csv/").get("Contents", [])
    parts = [o["Key"] for o in objets if o["Key"].endswith(".csv")]
    if not parts:
        raise FileNotFoundError(
            "Aucun fichier dans s3://arxiv-clean/arxiv_ids_csv/ — lance d'abord "
            "'.\\scripts\\spark.ps1 build_silver --source arxiv', ou utilise --mode field."
        )

    lignes = []
    for key in parts:
        body = s3.get_object(Bucket="arxiv-clean", Key=key)["Body"].read().decode("utf-8")
        lignes.extend(csv.DictReader(io.StringIO(body)))

    if category_prefix:
        lignes = [l for l in lignes if (l.get("primary_category") or "").startswith(category_prefix)]

    # Les plus récents d'abord : ce sont eux que le score de récence valorise
    lignes.sort(key=lambda l: int(l.get("published_year") or 0), reverse=True)
    return [l["arxiv_id"] for l in lignes[:limit]]


def fetch_by_arxiv_ids(client: OpenAlexClient, arxiv_ids: list[str]) -> list[dict]:
    works = []
    for debut in range(0, len(arxiv_ids), DOIS_PAR_APPEL):
        lot = arxiv_ids[debut:debut + DOIS_PAR_APPEL]
        filtre = "doi:" + "|".join(f"10.48550/arxiv.{i.lower()}" for i in lot)
        data = client.get("/works", {"filter": filtre, "per_page": PER_PAGE, "select": SELECT})
        works.extend(data["results"])
        if (debut // DOIS_PAR_APPEL) % 20 == 0:
            logger.info(f"   {debut + len(lot):,}/{len(arxiv_ids):,} IDs interrogés — "
                        f"{len(works):,} œuvres trouvées")
    return works


def fetch_by_field(client: OpenAlexClient, max_pages: int) -> list[dict]:
    source_id = client.resolve_arxiv_source_id()
    logger.info(f"   Source arXiv OpenAlex : {source_id}")
    filtre = f"locations.source.id:{source_id},primary_topic.field.id:fields/17"

    works, cursor, page = [], "*", 0
    while cursor and page < max_pages:
        data = client.get("/works", {"filter": filtre, "per_page": PER_PAGE,
                                     "cursor": cursor, "select": SELECT})
        works.extend(data["results"])
        cursor = data["meta"].get("next_cursor")
        page += 1
        logger.info(f"   Page {page}/{max_pages} — total cumulé : {len(works):,}")
    return works


def main():
    parser = argparse.ArgumentParser(description="Ingestion Bronze OpenAlex")
    parser.add_argument("--mode", choices=["arxiv-ids", "field"], default="arxiv-ids")
    parser.add_argument("--limit", type=int, default=20_000, help="mode arxiv-ids : nb max d'IDs ArXiv")
    parser.add_argument("--category-prefix", default="", help="ex. 'cs.' pour ne garder que l'informatique")
    parser.add_argument("--max-pages", type=int, default=50, help="mode field : nb max de pages de 100")
    args = parser.parse_args()

    if not API_KEY:
        logger.warning("⚠️  OPENALEX_API_KEY absente : quota limité à 0,10 $/jour (≈ 1 000 appels).")

    s3 = get_s3_client()
    ensure_bucket(s3, BUCKET)
    client = OpenAlexClient()
    debut = time.time()

    if args.mode == "arxiv-ids":
        arxiv_ids = read_arxiv_ids_from_silver(s3, args.limit, args.category_prefix)
        logger.info(f"📡 Interrogation d'OpenAlex pour {len(arxiv_ids):,} articles ArXiv...")
        works = fetch_by_arxiv_ids(client, arxiv_ids)
    else:
        logger.info("📡 Parcours des œuvres CS hébergées sur arXiv...")
        works = fetch_by_field(client, args.max_pages)

    key = f"{PREFIX}/works_{args.mode}_{timestamp()}.jsonl"
    write_jsonl(s3, BUCKET, key, works)

    logger.info("═" * 55)
    logger.info(f"  📊 Œuvres récupérées : {len(works):,}")
    logger.info(f"  📞 Appels API        : {client.calls:,} (quota gratuit ≈ 10 000/jour)")
    logger.info(f"  ⏱  Durée             : {time.time() - debut:.1f}s")
    logger.info("═" * 55)


if __name__ == "__main__":
    main()
