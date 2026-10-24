"""
SciPulse Insights — Couche Gold : écriture vers Elasticsearch

Lit s3a://results/arxiv_impact_score.parquet et l'écrit dans l'index
arxiv-papers-enriched via le connecteur elasticsearch-hadoop (écriture
distribuée : chaque partition est envoyée en parallèle en bulk).

C'est la SEULE écriture vers Elasticsearch du module 2 (brief : couche Gold).

Prérequis : l'index doit exister avec son mapping (src/utils/mappings_spark.py).
On écrit en mode "append" : "overwrite" ferait supprimer/recréer l'index
par le connecteur et perdre le mapping explicite. es.mapping.id=arxiv_id
rend l'écriture idempotente : relancer le job met à jour les documents
au lieu de les dupliquer.
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.spark_jobs import config
from src.spark_jobs.spark_session import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Alignées une à une sur src/utils/mapping_arxiv_papers_enriched.json ("dynamic": "strict")
COLONNES_GOLD = [
    "arxiv_id", "title", "abstract", "authors", "categories", "primary_category",
    "published_date", "published_year", "doi_clean", "is_withdrawn",
    "openalex_id", "match_method", "topic_name", "subfield_name", "field_name",
    "cited_by_count", "citations_3ans", "velocite",
    "has_hn", "hn_score", "hn_comments", "hn_posts", "hn_first_posted_at", "hn_top_title",
    "keywords",
    "score_citations", "score_velocite", "score_hn", "score_recence", "score_impact",
    "gold_indexed_at",
]


def to_gold(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("gold_indexed_at", F.current_timestamp())
        # Dates en ISO 8601 : interprétation sans ambiguïté côté Elasticsearch
        .withColumn("published_date", F.date_format("published_date", "yyyy-MM-dd"))
        .withColumn("hn_first_posted_at", F.date_format("hn_first_posted_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("gold_indexed_at", F.date_format("gold_indexed_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .select(*COLONNES_GOLD)
    )


def write_to_elasticsearch(df: DataFrame, index_name: str = config.ES_INDEX) -> None:
    writer = (
        to_gold(df).write
        .format("org.elasticsearch.spark.sql")
        .option("es.nodes", config.ES_NODES)
        .option("es.port", config.ES_PORT)
        .option("es.nodes.wan.only", "true")         # Docker : ne pas chercher les IP internes des nœuds
        .option("es.resource", index_name)
        .option("es.index.auto.create", "false")     # l'index DOIT exister avec son mapping
        .option("es.mapping.id", "arxiv_id")
        .option("es.write.operation", "index")
        .option("es.batch.size.entries", "1000")
        .mode("append")
    )
    if config.ES_USER:
        writer = writer.option("es.net.http.auth.user", config.ES_USER) \
                       .option("es.net.http.auth.pass", config.ES_PASSWORD)
    writer.save()


def main():
    spark = get_spark_session("SciPulse — Write to Elasticsearch")
    df = spark.read.parquet(config.IMPACT_SCORE)

    total = df.count()
    distincts = df.select("arxiv_id").distinct().count()
    logger.info(f"📂 {total:,} lignes, {distincts:,} arxiv_id distincts")
    if total != distincts:
        logger.warning(f"⚠️  {total - distincts:,} doublons d'arxiv_id : ES n'en gardera qu'un "
                       "(même _id → la dernière écriture écrase les précédentes)")

    logger.info(f"💾 Écriture distribuée vers l'index '{config.ES_INDEX}'...")
    write_to_elasticsearch(df)
    logger.info(f"✅ Écriture terminée — vérifie : curl localhost:9200/{config.ES_INDEX}/_count")
    spark.stop()


if __name__ == "__main__":
    main()
