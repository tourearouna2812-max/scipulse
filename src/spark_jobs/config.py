"""
SciPulse Insights — Configuration centralisée du module Spark

Tous les chemins (couches Medallion) et paramètres de connexion sont
définis ici, et surchargeables par variable d'environnement. Aucun job
ne code en dur un chemin S3 ou un hôte : changer d'environnement
(conteneur Docker, poste local, CI) ne demande que des variables.
"""

import os
from datetime import date

# ── Connexions ────────────────────────────────────────────────────────
# Depuis le conteneur Spark, MinIO et Elasticsearch sont joignables par
# leur nom de service Docker ; depuis Windows, par localhost.
SPARK_S3_ENDPOINT = os.getenv("SPARK_S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

ES_NODES = os.getenv("ES_NODES", "elasticsearch")
ES_PORT = os.getenv("ES_PORT", "9200")
ES_USER = os.getenv("ES_USER", "")          # vide si la sécurité ES est désactivée
ES_PASSWORD = os.getenv("ES_PASSWORD", "")
ES_INDEX = os.getenv("ES_INDEX", "arxiv-papers-enriched")

# La version du connecteur doit correspondre à la version d'Elasticsearch
# (curl localhost:9200 → "number"). 8.13.4 convient à un cluster 8.x.
ES_SPARK_VERSION = os.getenv("ES_SPARK_VERSION", "8.13.4")
SPARK_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    f"org.elasticsearch:elasticsearch-spark-30_2.12:{ES_SPARK_VERSION}"
)

# ── Bronze (données brutes, JSON Lines) ───────────────────────────────
ARXIV_RAW = os.getenv("ARXIV_RAW_PATH", "s3a://arxiv-raw/arxiv-metadata-full.json")
HN_RAW_ALGOLIA = "s3a://hn-raw/algolia/"
HN_RAW_ES_EXPORT = "s3a://hn-raw/es_export/"
CITATIONS_RAW = "s3a://citations-raw/openalex/"

# ── Silver (nettoyé, normalisé, Parquet, une table par source) ────────
ARXIV_CLEAN = "s3a://arxiv-clean/arxiv.parquet"
ARXIV_IDS_CSV = "s3a://arxiv-clean/arxiv_ids_csv"    # liste d'IDs pour l'ingestion OpenAlex
HN_CLEAN = "s3a://hn-clean/hn_arxiv.parquet"
CITATIONS_CLEAN = "s3a://citations-clean/openalex.parquet"

# ── Résultats intermédiaires de la couche Gold (Parquet) ──────────────
RESULTS = "s3a://results"
ENRICHED = f"{RESULTS}/arxiv_enriched.parquet"
MATCH_COVERAGE = f"{RESULTS}/match_coverage.parquet"
CITATION_VELOCITY = f"{RESULTS}/citation_velocity.parquet"
TOPIC_GROWTH = f"{RESULTS}/topic_growth.parquet"
CATEGORY_GROWTH = f"{RESULTS}/category_growth.parquet"
TFIDF_VECTORS = f"{RESULTS}/tfidf_vectors.parquet"
KEYWORDS = f"{RESULTS}/arxiv_keywords.parquet"
IMPACT_SCORE = f"{RESULTS}/arxiv_impact_score.parquet"

# ── Paramètres analytiques ────────────────────────────────────────────
# Année de référence = dernière année civile complète (l'année en cours
# est partielle et sous-estimerait toutes les citations récentes).
REF_YEAR = int(os.getenv("REF_YEAR", date.today().year - 1))

# Fraction du corpus utilisée pour le fit TF-IDF (1.0 = tout le corpus).
TFIDF_SAMPLE = float(os.getenv("TFIDF_SAMPLE", "1.0"))
