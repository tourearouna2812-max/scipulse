"""
SciPulse Insights — Score d'impact composite

Formule (chaque composante est ramenée sur [0, 1]) :

    score_citations = log(1 + cited_by_count) / max(log(1 + cited_by_count))
    score_velocite  = log(1 + citations_3ans) / max(log(1 + citations_3ans))
    score_hn        = log(1 + hn_score)       / max(log(1 + hn_score))
    score_recence   = 2 ^ (−âge_en_années / DEMI_VIE)          (demi-vie : 3 ans)

    score_impact = 100 × (0.40·citations + 0.25·vélocité + 0.20·HN + 0.15·récence)

Justification (détaillée dans docs/MODULE2_PIPELINE_SPARK.md) :
  - log1p : citations et points HN suivent des lois à queue lourde ; sans
    log, une poignée d'articles écrase tous les autres (c'est le défaut du
    score du TP, où 0.5 × cited_by_count brut domine tout).
  - normalisation par le max : les quatre composantes deviennent
    comparables, donc les poids se lisent directement comme des parts
    d'importance, et le score final est sur 0–100.
  - récence : corrige le biais d'âge (un article de 2015 a mécaniquement
    plus de citations qu'un article de 2024) ; le brief l'exige.
  - pondérations : l'impact académique (citations + vélocité = 65 %)
    reste majoritaire car c'est le signal le plus fiable ; HN (20 %)
    apporte la dimension "écho grand public" sans pouvoir à lui seul
    propulser un article ; la récence (15 %) départage à impact égal.
    L'analyse de sensibilité ci-dessous mesure la stabilité du top 100.
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.spark_jobs import config
from src.spark_jobs.spark_session import get_spark_session, path_exists

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

POIDS_DEFAUT = {"citations": 0.40, "velocite": 0.25, "hn": 0.20, "recence": 0.15}
DEMI_VIE_ANNEES = 3.0

CONFIGURATIONS_SENSIBILITE = {
    "par_defaut": POIDS_DEFAUT,
    "hn_dominant": {"citations": 0.20, "velocite": 0.10, "hn": 0.60, "recence": 0.10},
    "citations_seules": {"citations": 1.0, "velocite": 0.0, "hn": 0.0, "recence": 0.0},
    "sans_recence": {"citations": 0.50, "velocite": 0.30, "hn": 0.20, "recence": 0.0},
}


def compute_components(df_enriched: DataFrame, df_velocity: DataFrame) -> DataFrame:
    df = (
        df_enriched.join(df_velocity, on="arxiv_id", how="left")
        .withColumn("cited_by_count", F.coalesce("cited_by_count", F.lit(0)))
        .withColumn("citations_3ans", F.coalesce("citations_3ans", F.lit(0.0)))
        .withColumn("_log_cit", F.log1p("cited_by_count"))
        .withColumn("_log_vel", F.log1p(F.greatest("citations_3ans", F.lit(0.0))))
        .withColumn("_log_hn", F.log1p(F.coalesce("hn_score", F.lit(0))))
    )
    # Maxima globaux : agrégat d'une ligne, broadcasté sur toutes les lignes
    maxima = df.agg(
        F.max("_log_cit").alias("_max_cit"), F.max("_log_vel").alias("_max_vel"), F.max("_log_hn").alias("_max_hn"),
    )
    age_annees = F.datediff(F.current_date(), F.col("published_date")) / F.lit(365.25)
    eps = F.lit(1e-9)
    return (
        df.crossJoin(F.broadcast(maxima))
        .withColumn("score_citations", F.col("_log_cit") / F.greatest("_max_cit", eps))
        .withColumn("score_velocite", F.col("_log_vel") / F.greatest("_max_vel", eps))
        .withColumn("score_hn", F.col("_log_hn") / F.greatest("_max_hn", eps))
        .withColumn("score_recence", F.coalesce(
            F.pow(F.lit(2.0), -F.greatest(age_annees, F.lit(0.0)) / F.lit(DEMI_VIE_ANNEES)), F.lit(0.0)))
        .drop("_log_cit", "_log_vel", "_log_hn", "_max_cit", "_max_vel", "_max_hn")
    )


def score_expression(poids: dict):
    return F.round(100 * (
        poids["citations"] * F.col("score_citations")
        + poids["velocite"] * F.col("score_velocite")
        + poids["hn"] * F.col("score_hn")
        + poids["recence"] * F.col("score_recence")
    ), 3)


def compute_impact_score(df_enriched: DataFrame, df_velocity: DataFrame, poids: dict = POIDS_DEFAUT) -> DataFrame:
    return compute_components(df_enriched, df_velocity).withColumn("score_impact", score_expression(poids))


def sensitivity_analysis(df_components: DataFrame, top_n: int = 100) -> None:
    """Recouvrement du top N entre la configuration par défaut et chaque alternative."""
    df_components = df_components.filter(~F.col("is_withdrawn")).cache()
    tops = {}
    for nom, poids in CONFIGURATIONS_SENSIBILITE.items():
        rows = (df_components.withColumn("_s", score_expression(poids))
                .orderBy(F.desc("_s")).select("arxiv_id", "title").limit(top_n).collect())
        tops[nom] = rows

    reference = {r["arxiv_id"] for r in tops["par_defaut"]}
    logger.info("═" * 72)
    logger.info(f"  Analyse de sensibilité des pondérations (top {top_n})")
    for nom, rows in tops.items():
        ids = {r["arxiv_id"] for r in rows}
        recouvrement = 100 * len(ids & reference) / max(len(reference), 1)
        premier = rows[0]["title"][:55] if rows else "-"
        logger.info(f"  {nom:<18} recouvrement avec défaut : {recouvrement:5.1f} %   n°1 : {premier}")
    logger.info("═" * 72)


def main():
    spark = get_spark_session("SciPulse — Impact score")

    df_enriched = spark.read.parquet(config.ENRICHED)
    df_velocity = spark.read.parquet(config.CITATION_VELOCITY)
    if path_exists(spark, config.KEYWORDS):
        df_enriched = df_enriched.join(spark.read.parquet(config.KEYWORDS), on="arxiv_id", how="left")
    else:
        logger.warning("⚠️  Pas de mots-clés (lance tfidf_pipeline pour les ajouter)")
        df_enriched = df_enriched.withColumn("keywords", F.array().cast("array<string>"))

    df_score = compute_impact_score(df_enriched, df_velocity).cache()

    logger.info("=== Top 10 articles par score d'impact ===")
    df_score.filter(~F.col("is_withdrawn")).orderBy(F.desc("score_impact")) \
        .select("arxiv_id", "title", "cited_by_count", "citations_3ans", "hn_score", "published_year", "score_impact") \
        .show(10, truncate=50)

    logger.info("=== Cas d'usage 1 : viraux sur HN mais peu cités (cited_by_count < 5, hn_score > 100) ===")
    df_score.filter((F.col("cited_by_count") < 5) & (F.col("hn_score") > 100)) \
        .select("arxiv_id", "title", "cited_by_count", "hn_score").orderBy(F.desc("hn_score")).show(10, truncate=55)

    logger.info("=== Cas d'usage 2 : très cités (> 50) mais jamais mentionnés sur HN ===")
    df_score.filter((F.col("cited_by_count") > 50) & ~F.col("has_hn")) \
        .select("arxiv_id", "title", "cited_by_count").orderBy(F.desc("cited_by_count")).show(10, truncate=55)

    logger.info("=== Cas d'usage 3 : sous-domaines en croissance (vélocité + HN combinés) ===")
    df_score.filter(F.col("subfield_name").isNotNull()).groupBy("subfield_name").agg(
        F.count("*").alias("articles"),
        F.round(F.avg("score_velocite"), 3).alias("velocite_moy"),
        F.round(F.avg("score_hn"), 3).alias("hn_moy"),
        F.round(F.avg("score_impact"), 2).alias("impact_moy"),
    ).filter("articles >= 20").orderBy((F.col("velocite_moy") + F.col("hn_moy")).desc()).show(10, truncate=45)

    sensitivity_analysis(df_score)

    df_score.write.mode("overwrite").partitionBy("primary_category").parquet(config.IMPACT_SCORE)
    logger.info(f"✅ Écrit : {config.IMPACT_SCORE}")
    spark.stop()


if __name__ == "__main__":
    main()
