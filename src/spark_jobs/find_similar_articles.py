"""
SciPulse Insights — Articles les plus proches d'un article donné (similarité cosinus)

UDF justifiée : pas d'équivalent natif simple pour une similarité cosinus
entre deux vecteurs creux arbitraires dans pyspark.sql.functions. Le
vecteur de référence est capturé par closure (identique pour toutes les
lignes) au lieu d'être passé en colonne.

Usage :
    .\\scripts\\spark.ps1 find_similar_articles               (1er article de la table)
    .\\scripts\\spark.ps1 find_similar_articles --id 1706.03762
"""

import argparse

import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from src.spark_jobs import config
from src.spark_jobs.spark_session import get_spark_session


def make_cosine_similarity_udf(vecteur_reference):
    ref = vecteur_reference.toArray()
    ref_norm = np.linalg.norm(ref)

    def cosine_similarity(v):
        if v is None:
            return 0.0
        norme = ref_norm * np.linalg.norm(v.toArray())
        return 0.0 if norme == 0 else float(np.dot(ref, v.toArray()) / norme)

    return F.udf(cosine_similarity, DoubleType())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", help="identifiant ArXiv de l'article de référence")
    parser.add_argument("--top", type=int, default=10)
    args = parser.parse_args()

    spark = get_spark_session("SciPulse — Similar articles")
    df = spark.read.parquet(config.TFIDF_VECTORS)

    reference = (df.filter(F.col("arxiv_id") == args.id.lower()).first() if args.id else df.first())
    if reference is None:
        raise SystemExit(f"Article {args.id} absent de {config.TFIDF_VECTORS}")
    print(f"📌 Référence : [{reference['arxiv_id']}] {reference['title']}")

    similarity = make_cosine_similarity_udf(reference["tfidf"])
    (df.filter(F.col("arxiv_id") != reference["arxiv_id"])
       .withColumn("similarite", similarity(F.col("tfidf")))
       .orderBy(F.desc("similarite"))
       .select("arxiv_id", "title", F.round("similarite", 3).alias("similarite"))
       .show(args.top, truncate=80))

    spark.stop()


if __name__ == "__main__":
    main()
