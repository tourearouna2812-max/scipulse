"""
SciPulse Insights — Jointure ArXiv × Citations (OpenAlex) × Hacker News

Lit les trois tables Silver et construit la table enrichie (une ligne par
article ArXiv) :

  1. ArXiv ⨝ OpenAlex, par ordre de fiabilité décroissante :
       a. arxiv_id   (OpenAlex expose l'ID ArXiv via son DOI 10.48550/arxiv.* ou l'URL arxiv.org)
       b. doi        (DOI éditeur présent dans le dump ArXiv)
       c. doi_arxiv  (DOI DataCite construit depuis l'ID ArXiv)
       d. title      (repli : titre normalisé, ≥ 20 caractères pour limiter les faux positifs)
     Un article candidat à plusieurs méthodes garde la plus fiable
     (row_number sur la priorité) : pas de doublon d'article en sortie.
  2. Résultat ⨝ HN sur arxiv_id, après agrégation des stories HN par article.

Toutes les petites tables (OpenAlex, HN) sont broadcastées : la grande
table ArXiv n'est jamais redistribuée (pas d'Exchange sur elle).

Corrige deux pièges du code du TP J2 :
  - collision de la colonne "id" entre ArXiv et OpenAlex → clés renommées
    explicitement (arxiv_id / openalex_id) ;
  - jointure HN sur une colonne "arxiv_id" absente côté ArXiv.

Écrit :
  - s3a://results/arxiv_enriched.parquet (partitionné par primary_category)
  - s3a://results/match_coverage.parquet (taux de couverture, pour la doc)
"""

import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.spark_jobs import config
from src.spark_jobs.spark_session import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CITATION_COLS = ["openalex_id", "cited_by_count", "counts_by_year", "topic_name",
                 "subfield_name", "field_name", "oa_publication_year"]
MIN_TITLE_LENGTH = 20


def load_silver(spark: SparkSession):
    return (
        spark.read.parquet(config.ARXIV_CLEAN),
        spark.read.parquet(config.CITATIONS_CLEAN),
        spark.read.parquet(config.HN_CLEAN),
    )


def _candidates(df_arxiv: DataFrame, df_cit: DataFrame, arxiv_key: str,
                cit_key: str, method: str, priority: int) -> DataFrame:
    """Une méthode de matching = une équi-jointure broadcast sur une paire de clés."""
    right = (
        df_cit.filter(F.col(cit_key).isNotNull())
        .select(F.col(cit_key).alias("_k"), *CITATION_COLS)
    )
    left = df_arxiv.filter(F.col(arxiv_key).isNotNull()).select("arxiv_id", F.col(arxiv_key).alias("_k"))
    return (
        left.join(F.broadcast(right), on="_k", how="inner")
        .drop("_k")
        .withColumn("match_method", F.lit(method))
        .withColumn("_priority", F.lit(priority))
    )


def join_arxiv_citations(df_arxiv: DataFrame, df_cit: DataFrame) -> DataFrame:
    df_arxiv_titles = df_arxiv.filter(F.length("title_clean") >= MIN_TITLE_LENGTH)
    df_cit_titles = df_cit.filter(F.length("title_clean") >= MIN_TITLE_LENGTH)

    candidats = (
        _candidates(df_arxiv, df_cit, "arxiv_id", "arxiv_id", "arxiv_id", 1)
        .unionByName(_candidates(df_arxiv, df_cit, "doi_clean", "doi_clean", "doi", 2))
        .unionByName(_candidates(df_arxiv, df_cit, "doi_arxiv", "doi_clean", "doi_arxiv", 3))
        .unionByName(_candidates(df_arxiv_titles, df_cit_titles, "title_clean", "title_clean", "title_fallback", 4))
    )

    # Un seul match par article : méthode la plus fiable, puis l'œuvre la plus citée
    w = Window.partitionBy("arxiv_id").orderBy("_priority", F.desc("cited_by_count"))
    best = (
        candidats.withColumn("_rang", F.row_number().over(w))
        .filter("_rang = 1")
        .drop("_rang", "_priority")
    )

    # LEFT : on conserve TOUS les articles (un article viral sur HN peut n'avoir aucune citation).
    # best a au plus autant de lignes que la table OpenAlex : broadcast explicite, sinon Spark,
    # qui ne connaît pas sa taille (issue d'une window), choisirait un sort-merge join.
    return (
        df_arxiv.join(F.broadcast(best), on="arxiv_id", how="left")
        .withColumn("match_method", F.coalesce("match_method", F.lit("none")))
    )


def aggregate_hn(df_hn: DataFrame) -> DataFrame:
    """Un article peut avoir été posté plusieurs fois sur HN : on agrège par arxiv_id."""
    return df_hn.groupBy("arxiv_id").agg(
        F.max("score").alias("hn_score"),
        F.sum("comments").alias("hn_comments"),
        F.countDistinct("hn_id").alias("hn_posts"),
        F.min("posted_at").alias("hn_first_posted_at"),
        F.max_by("hn_title", "score").alias("hn_top_title"),
    )


def join_with_hn(df_enriched: DataFrame, df_hn: DataFrame) -> DataFrame:
    return (
        df_enriched.join(F.broadcast(aggregate_hn(df_hn)), on="arxiv_id", how="left")
        .withColumn("has_hn", F.col("hn_score").isNotNull())
    )


def build_enriched(spark: SparkSession) -> DataFrame:
    df_arxiv, df_cit, df_hn = load_silver(spark)
    df_arxiv = df_arxiv.cache()      # relue par les 4 méthodes de matching
    return join_with_hn(join_arxiv_citations(df_arxiv, df_cit), df_hn)


def coverage_report(spark: SparkSession, df_final: DataFrame) -> DataFrame:
    """Taux de couverture par méthode de matching + couverture HN."""
    total = df_final.count()
    rows = [(r["match_method"], r["count"], 100.0 * r["count"] / total)
            for r in df_final.groupBy("match_method").count().collect()]
    hn = df_final.filter("has_hn").count()
    both = df_final.filter("has_hn AND match_method != 'none'").count()
    rows += [("hn_signal", hn, 100.0 * hn / total),
             ("citations_and_hn", both, 100.0 * both / total),
             ("total_articles", total, 100.0)]

    logger.info("═" * 60)
    logger.info(f"  {'Méthode':<20}{'Articles':>12}{'% du corpus':>15}")
    for method, n, pct in sorted(rows, key=lambda x: -x[1]):
        logger.info(f"  {method:<20}{n:>12,}{pct:>14.2f}%")
    logger.info("═" * 60)
    return spark.createDataFrame(rows, ["metric", "articles", "pct_corpus"])


def main():
    spark = get_spark_session("SciPulse — Join sources")

    logger.info("🔗 Jointure ArXiv × OpenAlex × HN (couche Silver)...")
    df_final = build_enriched(spark).cache()

    coverage_report(spark, df_final).coalesce(1).write.mode("overwrite").parquet(config.MATCH_COVERAGE)

    logger.info("📐 Plan d'exécution (repère les BroadcastHashJoin) :")
    df_final.explain()

    df_final.write.mode("overwrite").partitionBy("primary_category").parquet(config.ENRICHED)
    logger.info(f"✅ Écrit : {config.ENRICHED}")
    spark.stop()


if __name__ == "__main__":
    main()
