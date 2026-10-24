"""
SciPulse Insights — Indicateurs temporels (window functions)

  1. Vélocité de citation par article : explode(counts_by_year), puis
       - moyenne mobile sur 3 ans           (avg sur rowsBetween(-2, 0))
       - variation annuelle (velocite)      (lag sur l'année précédente)
       - citations_3ans : moyenne annuelle des citations sur les 3 dernières
         années complètes [REF_YEAR-2 ; REF_YEAR], années manquantes = 0.
         C'est cet indicateur, robuste aux trous dans counts_by_year, qui
         alimente le score d'impact.
  2. Croissance des sujets OpenAlex (primary_topic) : citations par
     sujet et par année, taux de croissance, rang annuel.
  3. Croissance des publications par catégorie ArXiv : nombre d'articles
     par catégorie et par année, taux de croissance, rang annuel.

Entrée : s3a://results/arxiv_enriched.parquet (join_sources.py)
"""

import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.spark_jobs import config
from src.spark_jobs.spark_session import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def explode_counts_by_year(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("counts_by_year").isNotNull() & (F.size("counts_by_year") > 0))
        .select("arxiv_id", "primary_category", "topic_name",
                F.explode("counts_by_year").alias("y"))
        .select("arxiv_id", "primary_category", "topic_name",
                F.col("y.year").cast("int").alias("year"),
                F.col("y.cited_by_count").cast("int").alias("citations_annee"))
    )


def compute_velocity(df_yearly: DataFrame) -> DataFrame:
    w_mobile = Window.partitionBy("arxiv_id").orderBy("year").rowsBetween(-2, 0)
    w_ordre = Window.partitionBy("arxiv_id").orderBy("year")
    return (
        df_yearly
        .withColumn("moyenne_mobile_3ans", F.avg("citations_annee").over(w_mobile))
        .withColumn("citations_annee_precedente", F.lag("citations_annee", 1).over(w_ordre))
        .withColumn("velocite", F.col("citations_annee")
                    - F.coalesce(F.col("citations_annee_precedente"), F.lit(0)))
    )


def velocity_per_article(df_velocity: DataFrame, ref_year: int = config.REF_YEAR) -> DataFrame:
    """Une ligne par article : dernière vélocité connue + moyenne sur la fenêtre de référence."""
    w_last = Window.partitionBy("arxiv_id").orderBy(F.desc("year"))
    derniere = (
        df_velocity.withColumn("_r", F.row_number().over(w_last)).filter("_r = 1")
        .select("arxiv_id", "velocite", "moyenne_mobile_3ans", F.col("year").alias("derniere_annee_citee"))
    )
    fenetre = (
        df_velocity.filter(F.col("year").between(ref_year - 2, ref_year))
        .groupBy("arxiv_id")
        .agg((F.sum("citations_annee") / F.lit(3.0)).alias("citations_3ans"))
    )
    return (
        derniere.join(fenetre, on="arxiv_id", how="left")
        .withColumn("citations_3ans", F.coalesce("citations_3ans", F.lit(0.0)))
    )


def rank_topics_by_growth(df_yearly: DataFrame) -> DataFrame:
    par_annee = (
        df_yearly.filter(F.col("topic_name").isNotNull())
        .groupBy("topic_name", "year").agg(F.sum("citations_annee").alias("citations_totales"))
    )
    w_prec = Window.partitionBy("topic_name").orderBy("year")
    w_rang = Window.partitionBy("year").orderBy(F.desc("croissance_pct"))
    return (
        par_annee
        .withColumn("citations_annee_precedente", F.lag("citations_totales").over(w_prec))
        .withColumn("croissance_pct", F.round(100 * (F.col("citations_totales") - F.col("citations_annee_precedente"))
                                              / F.col("citations_annee_precedente"), 1))
        .withColumn("rang_croissance", F.rank().over(w_rang))
        .withColumn("rang_volume", F.rank().over(Window.partitionBy("year").orderBy(F.desc("citations_totales"))))
    )


def publications_growth_by_category(df_articles: DataFrame) -> DataFrame:
    par_annee = (
        df_articles.filter(F.col("published_year").isNotNull())
        .groupBy("primary_category", F.col("published_year").alias("year"))
        .agg(F.count("*").alias("publications"))
    )
    w_prec = Window.partitionBy("primary_category").orderBy("year")
    # On ne classe que les catégories avec un volume significatif (évite les +300 % sur 3 articles)
    w_rang = Window.partitionBy("year", "_significatif").orderBy(F.desc("croissance_pct"))
    return (
        par_annee
        .withColumn("publications_annee_precedente", F.lag("publications").over(w_prec))
        .withColumn("croissance_pct", F.round(100 * (F.col("publications") - F.col("publications_annee_precedente"))
                                              / F.col("publications_annee_precedente"), 1))
        .withColumn("_significatif", F.coalesce(F.col("publications_annee_precedente") >= 50, F.lit(False)))
        .withColumn("rang_croissance", F.when(F.col("_significatif"), F.rank().over(w_rang)))
        .drop("_significatif")
    )


def main():
    spark = get_spark_session("SciPulse — Citation velocity & growth")
    df = spark.read.parquet(config.ENRICHED)

    df_yearly = explode_counts_by_year(df).cache()
    n_articles = df.filter(F.col("counts_by_year").isNotNull()).count()
    logger.info(f"📂 {df_yearly.count():,} lignes (article, année) après explode, "
                f"pour {n_articles:,} articles avec citations")

    df_velocity = compute_velocity(df_yearly)
    exemple = df_velocity.select("arxiv_id").first()
    if exemple:
        logger.info("=== Exemple de vélocité pour un article ===")
        df_velocity.filter(F.col("arxiv_id") == exemple["arxiv_id"]).orderBy("year").show(truncate=False)

    velocity_per_article(df_velocity).write.mode("overwrite").parquet(config.CITATION_VELOCITY)
    logger.info(f"✅ Vélocité par article : {config.CITATION_VELOCITY} (année de référence {config.REF_YEAR})")

    topics = rank_topics_by_growth(df_yearly)
    logger.info(f"=== Top 5 sujets par croissance des citations en {config.REF_YEAR} ===")
    topics.filter((F.col("year") == config.REF_YEAR) & (F.col("citations_annee_precedente") >= 20)) \
        .orderBy(F.desc("croissance_pct")).show(5, truncate=60)
    topics.write.mode("overwrite").parquet(config.TOPIC_GROWTH)

    categories = publications_growth_by_category(df)
    logger.info("=== Catégories ArXiv à plus forte croissance de publications (dernière année) ===")
    derniere = categories.agg(F.max("year")).first()[0]
    categories.filter((F.col("year") == derniere) & F.col("rang_croissance").isNotNull()) \
        .orderBy("rang_croissance").show(10, truncate=False)
    categories.write.mode("overwrite").parquet(config.CATEGORY_GROWTH)

    spark.stop()


if __name__ == "__main__":
    main()
