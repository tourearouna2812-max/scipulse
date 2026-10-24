"""
SciPulse Insights — Pipeline Spark complet Silver → Gold, en une seule SparkSession

Enchaîne jointures (J2), indicateurs temporels et score (J3), écriture
Elasticsearch (J4) en réutilisant les fonctions des autres modules : un
correctif (ex. normalisation du DOI) se fait à UN seul endroit et profite
à la fois aux scripts unitaires et au pipeline orchestré par Airflow.

Le TF-IDF (étape la plus longue) reste un job séparé, lancé à part :
ses mots-clés sont intégrés ici s'ils existent déjà.

Usage :
    .\\scripts\\spark.ps1 run_full_pipeline
"""

import logging
import time

from pyspark.sql import functions as F

from src.spark_jobs import config
from src.spark_jobs.citation_velocity import (
    compute_velocity,
    explode_counts_by_year,
    publications_growth_by_category,
    rank_topics_by_growth,
    velocity_per_article,
)
from src.spark_jobs.impact_score import compute_impact_score
from src.spark_jobs.join_sources import build_enriched, coverage_report
from src.spark_jobs.spark_session import get_spark_session, path_exists
from src.spark_jobs.write_to_elasticsearch import write_to_elasticsearch

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def etape(numero: int, titre: str) -> None:
    logger.info("═" * 60)
    logger.info(f"  ÉTAPE {numero}/4 — {titre}")
    logger.info("═" * 60)


def main():
    debut = time.time()
    spark = get_spark_session("SciPulse — Full pipeline")

    etape(1, "Jointures ArXiv × OpenAlex × HN")
    df_enriched = build_enriched(spark)
    # .cache() : df_enriched est relu 4 fois (couverture, explode, croissance, score).
    # Sans cache, Spark relirait les 3 sources et refairait les jointures à chaque action.
    df_enriched = df_enriched.cache()
    coverage_report(spark, df_enriched).coalesce(1).write.mode("overwrite").parquet(config.MATCH_COVERAGE)

    etape(2, "Indicateurs temporels (window functions)")
    df_yearly = explode_counts_by_year(df_enriched)
    df_velocity = velocity_per_article(compute_velocity(df_yearly)).cache()
    df_velocity.write.mode("overwrite").parquet(config.CITATION_VELOCITY)
    rank_topics_by_growth(df_yearly).write.mode("overwrite").parquet(config.TOPIC_GROWTH)
    publications_growth_by_category(df_enriched).write.mode("overwrite").parquet(config.CATEGORY_GROWTH)

    etape(3, "Score d'impact composite")
    if path_exists(spark, config.KEYWORDS):
        df_enriched = df_enriched.join(spark.read.parquet(config.KEYWORDS), on="arxiv_id", how="left")
    else:
        logger.warning("⚠️  Mots-clés absents (tfidf_pipeline non lancé) — champ keywords vide")
        df_enriched = df_enriched.withColumn("keywords", F.array().cast("array<string>"))
    df_score = compute_impact_score(df_enriched, df_velocity).cache()
    df_score.write.mode("overwrite").partitionBy("primary_category").parquet(config.IMPACT_SCORE)
    logger.info(f"   {df_score.count():,} articles scorés")

    etape(4, "Écriture Gold vers Elasticsearch")
    write_to_elasticsearch(df_score)

    logger.info("═" * 60)
    logger.info(f"  ✅ Pipeline terminé en {(time.time() - debut) / 60:.1f} min")
    logger.info("═" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
